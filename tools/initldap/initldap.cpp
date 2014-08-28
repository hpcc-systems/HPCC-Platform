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

#ifndef _WIN32
#include <unistd.h>
#endif

//-----------------------------------------------------
//
//-----------------------------------------------------
void usage(const char * defESPConfig)
{
    printf("\nUsage: initldap [options]");
    printf("\n\n\tinitldap creates an initial HPCC Admin user account\n\tand all HPCC organization units. Specify this user\n\tand OU in configmgr LDAPServer as the SystemUser\n");
    printf("\n  -h\t\tDisplay usage");
    printf("\n  -e=<esp.xml>\tLocation and name of esp.xml file,\n\t\tdefault %s", defESPConfig);
    printf("\n");
}

//-----------------------------------------------------
//
//-----------------------------------------------------
int main(int argc, char* argv[])
{
#ifdef _NO_LDAP
    printf("System was built with _NO_LDAP\n");
    return -1;
#endif

    StringAttr hpccESPConfig("/var/lib/HPCCSystems/myesp/esp.xml");

    if(argc > 1 && 0==strncmp("-h", argv[1], 2))
    {
        usage(hpccESPConfig.get());
        exit(-1);
    }

    for (int x = 1; x < argc; x++)
    {
        if (0==strncmp("-e=", argv[x], 3) && argv[x][3])
            hpccESPConfig.set(&argv[x][3]);
        else if (0==strncmp("-h", argv[x], 2))
        {
            usage(hpccESPConfig.get());
            exit(-1);
        }
        else
        {
            printf("Unrecognized parameter : '%s', enter 'initldap -h' for help\n", argv[x]);
            exit(-1);
        }
    }

    char buf[100];
    printf("\nEnter the LDAP Admin User name...");
    buf[0] = 0;
    scanf("%s", buf);
    StringAttr ldapUser(buf);

    printf("Enter the LDAP Admin password...");
    buf[0] = 0;
    scanf("%s", buf);
    StringAttr ldapPwd(buf);

    if (0==ldapUser.length() || 0==ldapPwd.length())
    {
        printf("\nERROR: Invalid LDAP Account credentials entered");
        exit(-1);
    }

    InitModuleObjects();
    try
    {
        //Parse ESP.XML
        printf("Accessing config file '%s'...",hpccESPConfig.get() );
        Owned<IPropertyTree> env = createPTreeFromXMLFile(hpccESPConfig.get());
        Owned<IPropertyTree> espLdap;
        if (env)
            espLdap.setown(env->getPropTree(".//ldapSecurity"));
        if (!espLdap)
        {
            printf("ERROR: Unable to access ESP config file '%s'", hpccESPConfig.get());
            exit(-1);
        }

        //Query LDAP Server type
        StringAttr serverType( espLdap->queryProp("@serverType") );
        if (!serverType.length())
        {
            printf("\nERROR: serverType not set in LDAPServer component");
            exit(-1);
        }

        StringBuffer hpccUser;
        StringBuffer hpccPwd;
        espLdap->getProp("@systemUser", hpccUser);
        espLdap->getProp("@systemPassword", hpccPwd);

        if (0==hpccUser.length() || 0==hpccPwd.length())
        {
            printf("\nERROR: systemUser credentials not found in configuration");
            exit(-1);
        }

        printf("\nReady to initialize HPCC LDAP Environment, using the following settings");
        printf("\n\tesp.xml file    : %s", hpccESPConfig.get());
        printf("\n\tLDAP Type       : %s", serverType.get());
        printf("\n\tHPCC Admin User : %s", hpccUser.str());
        printf("\nProceed?  y/n ");
        for (;;)
        {
            int c = getchar();
            if (c == 'y' || c == 'Y')
                break;
            else if (c == 'n' || c == 'N')
                exit(0);
        }


        if (stricmp(serverType.get(),"ActiveDirectory"))
        {
            espLdap->setProp("@systemCommonName", "Directory Manager");
            espLdap->setProp("@systemBasedn", "");
        }

        //Replace system user with LDAP Admin credentials
        espLdap->setProp("@systemUser", ldapUser.get());
        StringBuffer sb;
        encrypt(sb,ldapPwd.get());
        espLdap->setProp("@systemPassword", sb.str());

        //Create security manager
        Owned<ISecManager> secMgr = newLdapSecManager("initldap", *LINK(espLdap));
        if (!secMgr)
        {
            printf("\nERROR: Unable to create security manager");
            exit(-1);
        }

        //Create HPCC Admin user
        Owned<ISecUser> user = secMgr->createUser(hpccUser.str());
        user->credentials().setPassword(hpccPwd.str());
        bool ok = secMgr->addUser(*user.get());
        if (!ok)
        {
            printf("\nERROR: Unable to add HPCC user '%s'", hpccUser.str() );
            exit(-1);
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
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        e->errorMessage(errmsg);
        printf("%s\n", errmsg.str());
    }
    catch(...)
    {
        printf("Unknown exception\n");
    }

    releaseAtoms();
    return 0;
}
