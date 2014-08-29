/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */
#include "ldapsecurity.ipp"
#include "ldapsecurity.hpp"
#include "build-config.h"

#ifndef _WIN32
#include <unistd.h>
#endif


//-----------------------------------------------------
//
//-----------------------------------------------------
void usage()
{
    fprintf(stdout, "\nUsage: initldap");
    fprintf(stdout, "\n\n\tinitldap creates an initial HPCC Admin user account\n\tand all HPCC organization units, using the setting entered into configmanager 'LDAPServer' component\n");
    fprintf(stdout, "\n");
}

//-----------------------------------------------------
//
//-----------------------------------------------------
int main(int argc, char* argv[])
{
#ifdef _NO_LDAP
    fprintf(stderr, "System was built with _NO_LDAP\n");
    return -1;
#endif

    for (int x = 1; x < argc; x++)
    {
        if (0==strncmp("-h", argv[x], 2))
        {
            usage();
            exit(1);
        }
        else
        {
            fprintf(stderr, "\nERROR: Unrecognized parameter : '%s', enter 'initldap -h' for help\n", argv[x]);
            exit(1);
        }
    }

    //execute configgen to query the esp.xml configuration
    StringBuffer cmdLine;
    cmdLine.append(ADMIN_DIR).append(PATHSEPCHAR).append("configgen");
    cmdLine.append(" -env ").append(CONFIG_DIR).append(PATHSEPCHAR).append("environment.xml");
    cmdLine.appendf(" -c myesp -od %ctmp%cHPCCSystems%cinitldap",PATHSEPCHAR,PATHSEPCHAR,PATHSEPCHAR);

    unsigned long runcode = 0;
    bool success = invoke_program(cmdLine.str(), runcode, true);
    if (!success)
        exit(1);

    //Get LDAP admin creds from user
    char buf[100];
    fprintf(stdout, "\nEnter the LDAP Admin User name...");
    fgets(buf, sizeof(buf), stdin);
    if (buf[strlen(buf)-1] == '\n')
        buf[strlen(buf)-1] = (char)NULL;
    StringAttr ldapUser(buf);

    fprintf(stdout, "Enter the LDAP Admin password...");
    fgets(buf, sizeof(buf), stdin);
    if (buf[strlen(buf)-1] == '\n')
        buf[strlen(buf)-1] = (char)NULL;
    StringAttr ldapPwd(buf);

    if (0==ldapUser.length() || 0==ldapPwd.length())
    {
        fprintf(stderr, "\nERROR: Invalid LDAP Account credentials entered");
        exit(1);
    }

    InitModuleObjects();
    try
    {
        StringBuffer dirName;
        dirName.appendf("%ctmp%cHPCCSystems%cinitldap",PATHSEPCHAR,PATHSEPCHAR,PATHSEPCHAR);
        Owned<IDirectoryIterator> dir = createDirectoryIterator(dirName.str(),"*");
        ForEach (*dir)
        {
            IFile &dirEntry = dir->query();
            if (dirEntry.isDirectory())
            {
                StringBuffer espXML;
                espXML.append(dirEntry.queryFilename()).append(PATHSEPCHAR).append("esp.xml");
                Owned<IPropertyTree> envFile = createPTreeFromXMLFile(espXML.str());
                Owned<IPropertyTree> espLdap;
                if (!envFile)
                    continue;
                espLdap.setown(envFile->getPropTree(".//ldapSecurity"));
                if (!espLdap)
                {
                    fprintf(stderr, "\nERROR: Unable to access ESP LDAP Configuration");
                    exit(1);
                }

                //Query LDAP Server type
                StringAttr serverType( espLdap->queryProp("@serverType") );
                if (!serverType.length())
                {
                    fprintf(stderr, "\nERROR: serverType not set in LDAPServer component");
                    exit(1);
                }

                StringBuffer hpccUser;
                StringBuffer hpccPwd;
                espLdap->getProp("@systemUser", hpccUser);
                espLdap->getProp("@systemPassword", hpccPwd);

                if (0==hpccUser.length() || 0==hpccPwd.length())
                {
                    fprintf(stderr, "\nERROR: systemUser credentials not found in configuration");
                    exit(1);
                }

                StringBuffer ldapAddress;
                espLdap->getProp("@ldapAddress", ldapAddress);

                fprintf(stdout, "\nReady to initialize HPCC LDAP Environment, using the following settings");
                fprintf(stdout, "\n\tLDAP Type       : %s", serverType.get());
                fprintf(stdout, "\n\tLDAP Server     : %s", ldapAddress.str());
                fprintf(stdout, "\n\tHPCC Admin User : %s", hpccUser.str());
                fprintf(stdout, "\nProceed?  y/n ");
                for (;;)
                {
                    int c = getchar();
                    if (c == 'y' || c == 'Y')
                        break;
                    else if (c == 'n' || c == 'N')
                        exit(0);
                }

                if (stricmp(serverType.get(),"ActiveDirectory"))
                    espLdap->setProp("@systemBasedn", "");

                //Replace system user with LDAP Admin credentials
                espLdap->setProp("@systemUser", ldapUser.get());
                espLdap->setProp("@systemCommonName", ldapUser.get());
                StringBuffer sb;
                encrypt(sb,ldapPwd.get());
                espLdap->setProp("@systemPassword", sb.str());

                //Create security manager. This creates the required OUs
                Owned<ISecManager> secMgr = newLdapSecManager("initldap", *LINK(espLdap));
                if (!secMgr)
                {
                    fprintf(stderr, "\nERROR: Unable to create security manager");
                    exit(1);
                }

                //Create HPCC Admin user
                Owned<ISecUser> user = secMgr->createUser(hpccUser.str());
                StringBuffer pwd;
                decrypt(pwd, hpccPwd.str());
                user->credentials().setPassword(pwd.str());
                bool ok = secMgr->addUser(*user.get());
                if (!ok)
                {
                    fprintf(stderr, "\nERROR: Unable to add HPCC user '%s'", hpccUser.str() );
                    exit(1);
                }

                //Add HPCC admin user to Administrators group
                CLdapSecManager* ldapSecMgr = dynamic_cast<CLdapSecManager*>(secMgr.get());
                StringAttr adminGroup;
                bool isActiveDir = true;
                if (0 == stricmp(serverType.get(),"ActiveDirectory"))
                    adminGroup.set("Administrators");
                else
                    adminGroup.set("Directory Administrators");
                ldapSecMgr->changeUserGroup("add", hpccUser.str(), adminGroup);
                fprintf(stdout, "\n\nLDAP Initialization successful\n");
            }
        }//foreach dir
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        e->errorMessage(errmsg);
        fprintf(stderr, "%s\n", errmsg.str());
    }
    catch(...)
    {
        fprintf(stderr, "Unknown exception\n");
    }

    releaseAtoms();
    return 0;
}
