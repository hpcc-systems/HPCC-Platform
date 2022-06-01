/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.

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

#include "jmisc.hpp"
#include "jutil.hpp"
#include "ldapenvironmentService.hpp"
#include "exception_util.hpp"
#include "ldapsecurity.ipp"

#define MSG_SEC_MANAGER_IS_NULL "LDAP Security Manager is not found. Please check if the system authentication is set up correctly"

void CldapenvironmentEx::init(IPropertyTree *_cfg, const char *_process, const char *_service)
{
    cfg = _cfg;

    {
#ifdef _DEBUG
        StringBuffer sb;
        toXML(cfg, sb);
        if (!sb.isEmpty())
            DBGLOG("\n------ldapenvironment config\n%s\n------\n", sb.str());
#endif
        VStringBuffer prefix("Software/EspProcess[@name='%s']/@", _process);
        StringBuffer xpath;

        xpath.set(prefix).append("hpccRootName");
        cfg->getProp(xpath.str(), ldapRootOU);
        if (ldapRootOU.isEmpty())
            throw MakeStringException(-1, "hpccRootName must be specified in configuration (ex. 'ou=hpcc,dc=myldap,dc=com')");

        xpath.set(prefix).append("adminGroupName");
        cfg->getProp(xpath.str(), adminGroupName);
        if (adminGroupName.isEmpty())
            adminGroupName.set("HPCCAdmins");

        xpath.set(prefix).append("sharedFilesBaseDN");
        cfg->getProp(xpath.str(), sharedFilesBaseDN);
        if (sharedFilesBaseDN.isEmpty())
            throw MakeStringException(-1, "sharedFilesBaseDN must be specified in configuration (ex. 'ou=files,ou=shared,ou=hpcc,dc=myldap,dc=com')");

        xpath.set(prefix).append("sharedGroupsBaseDN");
        cfg->getProp(xpath.str(), sharedGroupsBaseDN);
        if (sharedGroupsBaseDN.isEmpty())
            throw MakeStringException(-1, "sharedGroupsBaseDN must be specified in configuration (ex. 'ou=groups,ou=shared,ou=hpcc,dc=myldap,dc=com')");

        xpath.set(prefix).append("sharedUsersBaseDN");
        cfg->getProp(xpath.str(), sharedUsersBaseDN);
        if (sharedUsersBaseDN.isEmpty())
            throw MakeStringException(-1, "sharedUsersBaseDN must be specified in configuration (ex. 'ou=users,ou=shared,ou=hpcc,dc=myldap,dc=com')");

        xpath.set(prefix).append("sharedResourcesBaseDN");
        cfg->getProp(xpath.str(), sharedResourcesBaseDN);
        if (sharedResourcesBaseDN.isEmpty())
            throw MakeStringException(-1, "sharedResourcesBaseDN must be specified in configuration (ex. 'ou=smc,ou=espservices,ou=shared,ou=hpcc,dc=myldap,dc=com')");

        xpath.set(prefix).append("sharedWorkunitsBaseDN");
        cfg->getProp(xpath.str(), sharedWorkunitsBaseDN);
        if (sharedWorkunitsBaseDN.isEmpty())
            throw MakeStringException(-1, "sharedWorkunitsBaseDN must be specified in configuration (ex. 'ou=workunits,ou=shared,ou=hpcc,dc=myldap,dc=com')");
    }
}

bool CldapenvironmentEx::onLDAPQueryDefaults(IEspContext &context, IEspLDAPQueryDefaultsRequest &req, IEspLDAPQueryDefaultsResponse &resp)
{
    resp.setHPCCRootName(ldapRootOU.str());
    resp.setAdminGroupName(adminGroupName.str());
    resp.setSharedFilesBaseDN(sharedFilesBaseDN.str());
    resp.setSharedGroupsBaseDN(sharedGroupsBaseDN.str());
    resp.setSharedUsersBaseDN(sharedUsersBaseDN.str());
    resp.setSharedResourcesBaseDN(sharedResourcesBaseDN.str());
    resp.setSharedWorkunitsBaseDN(sharedWorkunitsBaseDN.str());
    return true;
}

const char * CldapenvironmentEx::formatOUname(StringBuffer &ou, const char * envName, int mode, const char * sharedOU, const char * reqBaseDN, const char * standAloneOU)
{
    switch (mode)
    {
        case COUMode_GenerateStandAlone: // Generated Stand Alone OU
            ou.appendf("%s,ou=%s,%s", standAloneOU, envName, ldapRootOU.str());
            break;
        case COUMode_UseGlobal: // Global/Shared OU
            ou.append(sharedOU);//specified in ldapconfiguration
            break;
        case COUMode_CreateCustom: // Custom User-provided OU
            ou.append(reqBaseDN);
            break;
    }
    ou.toLowerCase();
    return ou.str();
}

bool CldapenvironmentEx::changePermissions(const char * ou, const char * userFQDN, SecAccessFlags allows, SecAccessFlags denies)
{
    // Given an OU such as "ou=BocaInsurance,ou=hpcc,dc=myldap,dc=com", the
    // 'rName' is 'BocaInsurance' and the baseDN is 'ou=hpcc,dc=myldap,dc=com"
    StringBuffer baseDN;
    StringBuffer rName;

    size_t finger = 0;
    while ('=' != ou[finger++]);//skip over "ou=" or "cn="
    while (',' != ou[finger])
        rName.append( (char)ou[finger++] );
    baseDN.append( ou + finger + 1 );

    CPermissionAction action;
    action.m_action = "update";
    action.m_basedn = baseDN;
    action.m_rname =  rName;
    action.m_rtype = RT_FILE_SCOPE;
    action.m_account_name = userFQDN;//fully qualified user DN
    action.m_account_type = USER_ACT;
    action.m_allows = allows;
    action.m_denies = denies;
    DBGLOG("Setting (%d,%d) permissions for rName %s, baseDN %s, user %s", (int)allows, (int)denies, rName.str(), baseDN.str(), userFQDN);
    bool ok = false;
    try
    {
        ok = secmgr->changePermission(action);
    }
    catch (...)
    {
    }
    return ok;
}

bool CldapenvironmentEx::createSecret(SecretType type, const char * secretName, const char * username, const char * pwd, StringBuffer & notes)
{
    StringBuffer cmdLineSafe;
    switch (type)
    {
        case ST_K8S:
            cmdLineSafe.appendf("kubectl create secret generic %s --from-literal=username=%s --from-literal=password=", secretName, username);
            break;
        case ST_AUTHN_VAULT:
            cmdLineSafe.appendf("vault kv put secret/authn/%s username=%s password=", secretName, username);
            break;
    }

    VStringBuffer cmdLine("%s%s", cmdLineSafe.str(), pwd);
    try
    {
        DBGLOG("\nExecuting '%s'\n", cmdLineSafe.str());
        long unsigned int runcode;
        bool success = invoke_program(cmdLine.str(), runcode) && (runcode==0);
        if (!success)
            notes.appendf("\nError executing '%s', please run manually before executing helm install", cmdLineSafe.str());
    }
    catch (...)
    {
        notes.appendf("\nException executing '%s', please run manually before executing helm install", cmdLineSafe.str());
    }
    return true;
}

void CldapenvironmentEx::createLDAPBaseDN(const char * baseDN, SecPermissionType pt, const char * description, StringBuffer & notes)
{
    try
    {
        secmgr->createLdapBasedn(nullptr, baseDN, pt, description);
    }
    catch(...)
    {
        notes.appendf("\nNon Fatal Error creating '%s'", baseDN);
    }
}

bool CldapenvironmentEx::onLDAPCreateEnvironment(IEspContext &context, IEspLDAPCreateEnvironmentRequest &req, IEspLDAPCreateEnvironmentResponse &resp)
{
    try
    {
        if (secmgr == nullptr)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        
        StringBuffer notes;//contains interesting but non-fatal notes about environment creation attempt
        const char * userPrefix = secmgr->getLdapServerType() == ACTIVE_DIRECTORY ? "cn=" : "uid=";
        const char * resPrefix = secmgr->getLdapServerType() == ACTIVE_DIRECTORY ? "cn=" : "ou=";

        //Verify request

        if (isEmptyString(req.getEnvName()))
            throw MakeStringException(-1, "Environment name must be specified");

        if (strpbrk(req.getEnvName(), " ~`!@#$%^&*()_-+={[}]|:;<,>.?/"))
            throw MakeStringException(-1, "Environment name cannot contain spaces or special characters");

        if (isEmptyString(req.getEnvOwnerName()))
            throw MakeStringException(-1, "Environment Owner name must be specified");
        if (isEmptyString(req.getEnvDescription()))
            throw MakeStringException(-1, "Environment Description must be specified");

        if (req.getFilesMode() == COUMode_CreateCustom  && isEmptyString(req.getCustomFilesBaseDN()))
            throw MakeStringException(-1, "CustomFilesBaseDN must be specified (ex. 'ou=files,ou=hpcc,dc=myldap,dc=com')");
        if (!req.getGroupsMode() == COUMode_CreateCustom && isEmptyString(req.getCustomGroupsBaseDN()))
            throw MakeStringException(-1, "CustomGroupsBaseDN must be specified (ex. 'ou=groups,ou=hpcc,dc=myldap,dc=com')");
        if (!req.getUsersMode() == COUMode_CreateCustom && isEmptyString(req.getCustomUsersBaseDN()))
            throw MakeStringException(-1, "CustomUsersBaseDN must be specified (ex. 'ou=users,ou=hpcc,dc=myldap,dc=com')");
        if (!req.getResourcesMode() == COUMode_CreateCustom && isEmptyString(req.getCustomResourcesBaseDN()))
            throw MakeStringException(-1, "CustomResourcesBaseDN must be specified (ex. 'ou=smc,ou=espservices,ou=hpcc,dc=myldap,dc=com')");
        if (!req.getWorkunitsMode() == COUMode_CreateCustom && isEmptyString(req.getCustomWorkunitsBaseDN()))
            throw MakeStringException(-1, "CustomWorkunitsBaseDN must be specified (ex. 'ou=workunits,ou=hpcc,dc=myldap,dc=com')");

        if (req.getCreateVaultSecrets() && isEmptyString(req.getVaultName()))
            throw MakeStringException(-1, "Vault Name must be specified to create vault secrets");

        // Create OU string names

        StringBuffer respFilesBaseDN, respGroupsBaseDN, respUsersBaseDN, respResourcesBaseDN, respWorkunitsBaseDN;
        formatOUname(respFilesBaseDN, req.getEnvName(), req.getFilesMode(), sharedFilesBaseDN.str(), req.getCustomFilesBaseDN(), "ou=files");
        formatOUname(respGroupsBaseDN, req.getEnvName(), req.getGroupsMode(), sharedGroupsBaseDN.str(), req.getCustomGroupsBaseDN(), "ou=groups");
        formatOUname(respUsersBaseDN, req.getEnvName(), req.getUsersMode(), sharedUsersBaseDN.str(), req.getCustomUsersBaseDN(), "ou=users");
        formatOUname(respResourcesBaseDN, req.getEnvName(), req.getResourcesMode(), sharedResourcesBaseDN.str(), req.getCustomResourcesBaseDN(), "ou=smc,ou=espservices");
        formatOUname(respWorkunitsBaseDN, req.getEnvName(), req.getWorkunitsMode(), sharedWorkunitsBaseDN.str(), req.getCustomWorkunitsBaseDN(), "ou=workunits");

        //Ensure environment doesn't already exist
        VStringBuffer envOU("ou=%s,%s", req.getEnvName(), ldapRootOU.str());
        if (secmgr->organizationalUnitExists(envOU.str()))
        {
            throw MakeStringException(-1, "Environment '%s' already exists, please specify a different name", envOU.str());
        }

        //Create LDAP resources OU Hierarchy
        VStringBuffer description("%s, created by %s", req.getEnvDescription(), req.getEnvOwnerName());
        if (req.getCreateLDAPEnvironment())
        {
            //Note that ESP will also try to create these OUs on startup
            createLDAPBaseDN(respFilesBaseDN.str(), PT_DEFAULT, description.str(), notes);
            createLDAPBaseDN(respGroupsBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str(), notes);
            createLDAPBaseDN(respUsersBaseDN.str(),  PT_ADMINISTRATORS_ONLY, description.str(), notes);
            createLDAPBaseDN(respResourcesBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str(), notes);
            createLDAPBaseDN(respWorkunitsBaseDN.str(), PT_DEFAULT, description.str(), notes);

            //Create HPCCAdmins Group
            try
            {
                secmgr->addGroup(adminGroupName.str(), nullptr, description.str(), respGroupsBaseDN.str());
            }
            catch(...)
            {
                notes.appendf("\nNon Fatal Error creating '%s,%s'", adminGroupName.str(), respGroupsBaseDN.str());
            }
        }
        
        //----------------------------------
        // Create HPCC Admin Username/password.
        // Attempt to create the Kubernetes secret for that user
        //----------------------------------
        VStringBuffer respHPCCAdminUser("HPCC_%s", req.getEnvName());
        StringBuffer  respHPCCAdminPwd;
        generatePassword(respHPCCAdminPwd, 10);//jutil.hpp

        if (req.getCreateLDAPEnvironment())
        {
            //Create the HPCCAdmin user
            {
                Owned<ISecUser> user = secmgr->createUser(respHPCCAdminUser.str());
                user->credentials().setPassword(respHPCCAdminPwd.str());
                try
                {
                    secmgr->addUser(*user.get(), respUsersBaseDN.str());
                }
                catch(...)
                {
                    notes.appendf("\nNon Fatal Error creating '%s'", respHPCCAdminUser.str());
                }
            }

            //Add HPCCAdmin user to HPCCAdmins group
            {
                VStringBuffer adminGrpOU("cn=%s,%s", adminGroupName.str(), respGroupsBaseDN.str());
                VStringBuffer adminUsr("%s%s,%s", userPrefix, respHPCCAdminUser.str(), respUsersBaseDN.str());
                try
                {
                    secmgr->changeGroupMember("add", adminGrpOU.str(), adminUsr.str());
                }
                catch(...)
                {
                    notes.appendf("\nNon Fatal Error adding '%s' to '%s'", adminUsr.str(), adminGrpOU.str());
                }
            }

            //Grant SmcAccess to HPCCAdmins group
            {
                try
                {
                    //Create the SmcAccess resource (OU on 389DS, CN on AD)
                    Owned<ISecUser> usr = secmgr->createUser(nullptr);
                    secmgr->addResourceEx(RT_DEFAULT, *usr.get(), "SmcAccess", PT_ADMINISTRATORS_ONLY, respResourcesBaseDN.str());
                }
                catch(...)
                {
                    notes.appendf("\nNon Fatal Error creating 'SmcAccess' resource at %s", respResourcesBaseDN.str());
                }

                //Grant the permission
                VStringBuffer adminGrp("cn=%s,%s", adminGroupName.str(), respGroupsBaseDN.str());
                CPermissionAction action;
                action.m_action = "update";
                action.m_basedn = respResourcesBaseDN.str();
                action.m_rname = "SmcAccess";
                action.m_rtype = RT_SERVICE;
                action.m_account_name = adminGrp.str();
                action.m_account_type = GROUP_ACT;
                action.m_allows = SecAccess_Full;
                action.m_denies = 0;
                try
                {
                    secmgr->changePermission(action);
                }
                catch(...)
                {
                    notes.appendf("\nNon Fatal Error setting 'SmcAccess' permission for '%s'", adminGroupName.str());
                }
            }
        }

        //Create the HPCCAdmin secret
        VStringBuffer  respHPCCAdminSecretName("hpcc-admin-%s", req.getEnvName());
        respHPCCAdminSecretName.toLowerCase();
        if (req.getCreateK8sSecrets())
            createSecret(ST_K8S, respHPCCAdminSecretName.str(), respHPCCAdminUser.str(), respHPCCAdminPwd.str(), notes);

        StringBuffer respVaultID;
        if (req.getCreateVaultSecrets())
        {
            respVaultID.set(req.getVaultName());
            createSecret(ST_AUTHN_VAULT, respHPCCAdminSecretName.str(), respHPCCAdminUser.str(), respHPCCAdminPwd.str(), notes);
        }

        //----------------------------------
        // Create LDAP Admin Username/password.
        // Attempt to create the Kubernetes secret for that user
        //----------------------------------
        VStringBuffer respLDAPAdminUser("LDAP_%s", req.getEnvName());
        StringBuffer  respLDAPAdminPwd;
        generatePassword(respLDAPAdminPwd, 10);//jutil.hpp

        if (req.getCreateLDAPEnvironment())
        {
            //Create the user
            Owned<ISecUser> user = secmgr->createUser(respLDAPAdminUser.str());
            user->credentials().setPassword(respLDAPAdminPwd.str());
            try
            {
                secmgr->addUser(*user.get(), respUsersBaseDN.str());
            }
            catch(...)
            {
                notes.appendf("\nNon Fatal Error creating '%s'", respLDAPAdminUser.str());
            }

            //Add LDAPAdmin user to Administrators group
            if (secmgr->getLdapServerType() == ACTIVE_DIRECTORY)
            {
                const char * pDC = strstr(respUsersBaseDN.str(), "dc=");
                VStringBuffer ldapadminGrpOU("cn=Administrators,cn=Builtin,%s", pDC ? pDC : "dc=local");
                VStringBuffer ldapadminUsr("%s%s,%s", userPrefix, respLDAPAdminUser.str(), respUsersBaseDN.str());
                try
                {
                    secmgr->changeGroupMember("add", ldapadminGrpOU.str(), ldapadminUsr.str());
                }
                catch(...)
                {
                    notes.appendf("\nNon Fatal Error adding '%s' to '%s'", ldapadminUsr.str(), ldapadminGrpOU.str());
                }
            }

            //Add LDAP R/W permissions for LDAPAdmin user
            //Only grant access to root of new environment (ex  ou=BocaInsurance,ou=hpcc,dc=myldap,dc=com)
            VStringBuffer ldapAdminFQDN("%s%s,%s", userPrefix, respLDAPAdminUser.str(), respUsersBaseDN.str());
            if (!changePermissions(envOU.str(), ldapAdminFQDN.str(), SecAccess_Full, SecAccess_None))
                notes.appendf("\nNon Fatal Error setting LDAPAdmin permission for %s'", envOU.str());
            notes.appendf("\nEnsure LDAPAdmin user '%s' has full access permissions to environment OU '%s', including 'This object and all descendant objects'", respLDAPAdminUser.str(), envOU.str());
        }

        //Create the LDAPAdmin secret
        VStringBuffer respLDAPAdminSecretName("ldap-admin-%s", req.getEnvName());
        respLDAPAdminSecretName.toLowerCase();
        if (req.getCreateK8sSecrets())
            createSecret(ST_K8S, respLDAPAdminSecretName.str(), respLDAPAdminUser.str(), respLDAPAdminPwd.str(), notes);
        if (req.getCreateVaultSecrets())
            createSecret(ST_AUTHN_VAULT, respLDAPAdminSecretName.str(), respLDAPAdminUser.str(), respLDAPAdminPwd.str(), notes);

        //----------------------------------
        // Set response
        //----------------------------------
        StringBuffer ldapcredskey;
        StringBuffer hpcccredskey;
        if (respVaultID.isEmpty())
        {
            //No vault, create K8s authn key name
            ldapcredskey.appendf("ldapcredskey-%s", req.getEnvName());
            hpcccredskey.appendf("hpcccredskey-%s", req.getEnvName());
        }
        else
        {
            //Vault, specify the secret name as the key
            ldapcredskey.set(respLDAPAdminSecretName.str());
            hpcccredskey.set(respHPCCAdminSecretName.str());
        }
        ldapcredskey.toLowerCase();
        hpcccredskey.toLowerCase();

        resp.setLDAPAdminUsername(respLDAPAdminUser.str());
        resp.setLDAPAdminPassword(respLDAPAdminPwd.str());
        resp.setHPCCAdminUsername(respHPCCAdminUser.str());
        resp.setHPCCAdminPassword(respHPCCAdminPwd.str());

        StringBuffer helmSecrets;
        if (req.getCreateK8sSecrets())
        {
            helmSecrets.appendf("secrets:\n"
                               "  authn:\n"
                               "    %s: %s\n"
                               "    %s: %s\n",
                               ldapcredskey.str(), respLDAPAdminSecretName.str(),
                               hpcccredskey.str(), respHPCCAdminSecretName.str());
        }
        if (req.getCreateVaultSecrets())
        {
            helmSecrets.appendf("\nvaults:\n"
                                "  authn:\n"
                                "    - name: my-authn-vault\n"
                                "      url: http://${env.VAULT_SERVICE_HOST}:${env.VAULT_SERVICE_PORT}/v1/secret/data/authn/${secret}\n"
                                "      kind: kv-v2");
        }

        VStringBuffer ldapAdminKey( !ldapcredskey.isEmpty() ?   "    ldapAdminSecretKey: %s\n" : "", ldapcredskey.str());
        VStringBuffer ldapAdminVKey(!respVaultID.isEmpty() ?    "    ldapAdminVaultId: %s\n" : "",   respVaultID.str());
        VStringBuffer hpccAdminKey( !hpcccredskey.isEmpty() ?   "    hpccAdminSecretKey: %s\n" : "", hpcccredskey.str());
        VStringBuffer hpccAdminVKey(!respVaultID.isEmpty() ?   "     hpccAdminVaultId: %s\n" : "",   respVaultID.str());

        VStringBuffer ldapHelm("\n\n"
                               "%s\n"
                               "esp:\n"
                               "- name: eclwatch\n"
                               "  auth: ldap\n"
                               "  ldap:\n"
                               "    adminGroupName: %s\n"
                               "%s%s%s%s"
                               "    filesBasedn: %s\n"
                               "    groupsBasedn: %s\n"
                               "    usersBasedn: %s\n"
                               "    resourcesBasedn: %s\n"
                               "    workunitsBasedn: %s\n"
                               "    systemBasedn: %s\n\n",
                               helmSecrets.str(),
                               adminGroupName.str(),
                               ldapAdminKey.str(), ldapAdminVKey.str(),
                               hpccAdminKey.str(), hpccAdminVKey.str(),
                               respFilesBaseDN.str(), respGroupsBaseDN.str(), respUsersBaseDN.str(), respResourcesBaseDN.str(), respWorkunitsBaseDN.str(), respUsersBaseDN.str());
        resp.setLDAPHelm(ldapHelm.str());
        if (!notes.isEmpty())
            notes.append("\n");
        resp.setNotes(notes.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
