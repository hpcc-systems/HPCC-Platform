/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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
void usage()
{
    fprintf(stdout, "\nUsage: initldap");
    fprintf(stdout, "\n\n\tinitldap creates an initial HPCC Admin user account\n\tand all HPCC organization units, using the setting entered into configmanager 'LDAPServer' component\n");
    fprintf(stdout, "\n");
}

//-----------------------------------------------------
//
//-----------------------------------------------------
bool initLDAP(IPropertyTree * ldapProps)
{
    StringAttr serverType( ldapProps->queryProp("@serverType") );
    if (!serverType.length())
    {
        fprintf(stderr, "\nERROR: serverType not set in LDAPServer component");
        return false;
    }

    StringBuffer hpccUser;
    StringBuffer hpccPwd;
    ldapProps->getProp("@systemUser", hpccUser);
    ldapProps->getProp("@systemPassword", hpccPwd);
    if (0==hpccUser.length() || 0==hpccPwd.length())
    {
        fprintf(stderr, "\nERROR: HPCC systemUser credentials not found in configuration");
        return false;
    }

    StringBuffer ldapAddress;
    ldapProps->getProp("@ldapAddress", ldapAddress);

    bool is389DS = (0 == strcmp(serverType.get(), "389DirectoryServer") ? true : false);

    //Get LDAP admin creds from user
    char buff[100];
    fprintf(stdout, "\nEnter the '%s' LDAP Admin User name on '%s'.%s..",serverType.get(),ldapAddress.str(),is389DS?" Please include the attribute name prefix such as uid=adminName.":"");
    do
    {
        char * line = fgets(buff, sizeof(buff), stdin);
        if (!line)
            return false;
    }
    while (buff[0] == (char)'\n');

    if (buff[strlen(buff)-1] == '\n')
        buff[strlen(buff)-1] = (char)NULL;
    StringAttr ldapUser(buff);

    fprintf(stdout, "Enter the LDAP Admin user '%s' password...",ldapUser.get());
    char * line = fgets(buff, sizeof(buff), stdin);
    if (!line)
        return false;

    if (buff[strlen(buff)-1] == '\n')
        buff[strlen(buff)-1] = (char)NULL;
    StringAttr ldapPwd(buff);
    if (0==ldapUser.length() || 0==ldapPwd.length())
    {
        fprintf(stderr, "\nERROR: Invalid LDAP Admin account credentials entered");
        return false;
    }

    fprintf(stdout, "\nReady to initialize HPCC LDAP Environment, using the following settings");
    fprintf(stdout, "\n\tLDAP Server     : %s", ldapAddress.str());
    fprintf(stdout, "\n\tLDAP Type       : %s", serverType.get());
    fprintf(stdout, "\n\tHPCC Admin User : %s", hpccUser.str());
    fprintf(stdout, "\nProceed?  y/n ");
    for (;;)
    {
        int c = getchar();
        if (c == 'y' || c == 'Y')
            break;
        else if (c == 'n' || c == 'N')
            return true;
    }

    //Replace system user with LDAP Admin credentials
    ldapProps->setProp("@systemUser", ldapUser);
    ldapProps->setProp("@systemCommonName", ldapUser);
    StringBuffer sb;
    encrypt(sb,ldapPwd);
    ldapProps->setProp("@systemPassword", sb.str());

    //Create security manager. This creates the required OUs
    Owned<ISecManager> secMgr;
    try
    {
        secMgr.setown(newLdapSecManager("initldap", *LINK(ldapProps)));
    }
    catch(IException *e)
    {
        StringBuffer buff;
        e->errorMessage(buff);
        e->Release();
        fprintf(stderr, "\nERROR: Unable to create security manager : %s", buff.str());
        return false;
    }

    //Create HPCC Admin user
    Owned<ISecUser> user = secMgr->createUser(hpccUser.str());
    StringBuffer pwd;
    decrypt(pwd, hpccPwd.str());
    user->credentials().setPassword(pwd.str());
    try { secMgr->addUser(*user.get()); }
    catch(...) {}//user may already exist, so just move on

    //Add HPCC admin user to Administrators group
    CLdapSecManager* ldapSecMgr = dynamic_cast<CLdapSecManager*>(secMgr.get());
    if (!ldapSecMgr)
    {
        fprintf(stderr, "\nERROR: Unable to access CLdapSecManager object");
        return false;
    }
    StringAttr adminGroup;
    bool isActiveDir = true;
    if (0 == stricmp(serverType.get(),"ActiveDirectory"))
        adminGroup.set("Administrators");
    else
        adminGroup.set("Directory Administrators");
    try { ldapSecMgr->changeUserGroup("add", hpccUser.str(), adminGroup); }
    catch(...) {}//user may already be in group so just move on

    fprintf(stdout, "\n\nLDAP Initialization successful\n");
    return true;
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
            exit(0);
        }
        else
        {
            fprintf(stderr, "\nERROR: Unrecognized parameter : '%s', enter 'initldap -h' for help\n", argv[x]);
            exit(1);
        }
    }

    InitModuleObjects();

    //execute configgen to query the LDAP Server configuration(s)
    StringBuffer cmd;
    cmd.appendf("%s%cconfiggen -env %s%c%s -listldapservers", hpccBuildInfo.adminDir,PATHSEPCHAR,hpccBuildInfo.configDir, PATHSEPCHAR, hpccBuildInfo.envXmlFile);

    char * configBuffer = NULL;

    //acquire LDAP configuration by executing configgen and capturing output
    {
        StringBuffer configBuff;
        Owned<IPipeProcess> pipe = createPipeProcess();
        if (pipe->run("configgen", cmd.str(), ".", false, true, true, 0))
        {
            Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
            readSimpleStream(configBuff, *pipeReader);
            pipe->closeOutput();
        }
        int retcode = pipe->wait();
        if (retcode)
        {
            fprintf(stderr, "\nERROR %d: unable to execute %s", retcode, cmd.str());
            exit(1);
        }
        configBuffer = strdup(configBuff.str());
    }

    //Using the LDAP Server parms queried from configgen, build an
    //LDAPSecurity property tree for each LDAP Server and call the LDAP
    //Security Manager to create the needed entries
    Owned<IPropertyTree> ldapProps;
    char *saveptr;
    char * pLine = strtok_r(configBuffer, "\n", &saveptr);
    while (pLine)
    {
        if (pLine && 0==strcmp(pLine, "LDAPServerProcess"))
        {
            if (ldapProps)
                initLDAP(ldapProps);
            ldapProps.clear();
            ldapProps.setown(createPTree("ldapSecurity"));
        }
        else
        {
            char * sep = strchr(pLine, ',');
            if (sep)
            {
                *sep = (char)NULL;
                ldapProps->addProp(pLine, sep+1);
            }
        }
        pLine = strtok_r(NULL, "\n", &saveptr);
    }
    if (ldapProps)
        initLDAP(ldapProps);
    if (configBuffer)
        free(configBuffer);
    ldapProps.clear();

    releaseAtoms();
    return 0;
}
