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
    try
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
    catch (...)
    {
        throw MakeStringException(-1, "ldapenvironment: Error querying environment settings");
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
    return ou.str();
}


bool CldapenvironmentEx::onLDAPCreateEnvironment(IEspContext &context, IEspLDAPCreateEnvironmentRequest &req, IEspLDAPCreateEnvironmentResponse &resp)
{
    try
    {
        if (secmgr == nullptr)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        
        StringBuffer notes;//contains interesting but non-fatal notes about environment creation attempt
        
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

        // Create OU names

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

        if (req.getCreateLDAPEnvironment())
        {
            VStringBuffer description("%s, created by %s", req.getEnvDescription(), req.getEnvOwnerName());
            //Note that ESP will also try to create these OUs on startup
            try { secmgr->createLdapBasedn(nullptr, respFilesBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str()); }
            catch(...) { notes.appendf("\nError creating '%s'", respFilesBaseDN.str()); }
            try { secmgr->createLdapBasedn(nullptr, respGroupsBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str()); }
            catch(...) { notes.appendf("\nError creating '%s'", respGroupsBaseDN.str()); }
            try { secmgr->createLdapBasedn(nullptr, respUsersBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str()); }
            catch(...) { notes.appendf("\nError creating '%s'", respUsersBaseDN.str()); }
            try { secmgr->createLdapBasedn(nullptr, respResourcesBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str()); }
            catch(...) { notes.appendf("\nError creating '%s'", respResourcesBaseDN.str()); }
            try { secmgr->createLdapBasedn(nullptr, respWorkunitsBaseDN.str(), PT_ADMINISTRATORS_ONLY, description.str()); }
            catch(...) { notes.appendf("\nError creating '%s'", respWorkunitsBaseDN.str()); }
        }
        
        //----------------------------------
        // Create HPCC Admin Username/password.
        // Attempt to create the Kubernetes secret for that user
        // NOTE!  HPCCAdmin LDAP group and HPCCAdmin LDAP user will be
        // created by ESP when started in Admin mode
        //----------------------------------        
        VStringBuffer respHPCCAdminUser("admin_%s", req.getEnvName());

        VStringBuffer  hpccAdminK8sSecretName("hpccadminsecret-%s", req.getEnvName());
        hpccAdminK8sSecretName.toLowerCase();

        StringBuffer  respHPCCAdminPwd;
        generatePassword(respHPCCAdminPwd, 10);//jutil.hpp

        if (req.getCreateK8sSecret())
        {
            VStringBuffer cmdLineSafe("kubectl create secret generic %s --from-literal=username=%s --from-literal=password=", hpccAdminK8sSecretName.str(), respHPCCAdminUser.str());
            VStringBuffer cmdLine("%s%s", cmdLineSafe.str(), respHPCCAdminPwd.str());
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
        }

        //----------------------------------
        // Set response
        //----------------------------------
        resp.setHPCCAdminUsername(respHPCCAdminUser.str());
        resp.setHPCCAdminPassword(respHPCCAdminPwd.str());
        VStringBuffer ldapHelm("\n"
                               "secrets:\n"
                               "  authn:\n"
                               "    myhpcccreds: %s\n\n"
                               "esp:\n"
                               "- name: eclwatch\n"
                               "  auth: ldap\n"
                               "  ldap:\n"
                               "    adminGroupName: %s\n"
                               "    hpccAdminSecretKey: myhpcccreds\n"
                               "    filesBasedn: %s\n"
                               "    groupsBasedn: %s\n"
                               "    usersBasedn: %s\n"
                               "    resourcesBasedn: %s\n"
                               "    workunitsBasedn: %s\n",
                               hpccAdminK8sSecretName.str(), adminGroupName.str(), respFilesBaseDN.str(), respGroupsBaseDN.str(),
                               respUsersBaseDN.str(), respResourcesBaseDN.str(), respWorkunitsBaseDN.str());
        resp.setLDAPHelm(ldapHelm.str());
        resp.setNotes(notes.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
