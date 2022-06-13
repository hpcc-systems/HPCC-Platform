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

#ifndef _LDAPENVIRONMENT_HPP__
#define _LDAPENVIRONMENT_HPP__

#include "ldapenvironment_esp.ipp"
#include "ldapsecurity.ipp"


class CldapenvironmentEx : public Cldapenvironment  //base class name built from ECM ESPservice name
{
private:
    IPropertyTree * cfg;
    CLdapSecManager* secmgr = nullptr;
    StringBuffer    ldapRootOU;
    StringBuffer    sharedFilesBaseDN;
    StringBuffer    sharedGroupsBaseDN;
    StringBuffer    sharedUsersBaseDN;
    StringBuffer    sharedResourcesBaseDN;
    StringBuffer    sharedWorkunitsBaseDN;
    StringBuffer    adminGroupName;

    const char * formatOUname(StringBuffer &ou, const char * envName, int mode, const char * sharedOU, const char * reqBaseDN, const char * privateOU);
    void createLDAPBaseDN(const char * baseDN, SecPermissionType pt, const char * description, StringBuffer & notes);
    bool changePermissions(const char * ou, const char * userFQDN, SecAccessFlags allows, SecAccessFlags denies);

    enum SecretType : int
    {
        ST_K8S = 0,
        ST_AUTHN_VAULT = 1
    };
    bool createSecret(SecretType type, const char * secretName, const char * username, const char * pwd, StringBuffer & notes);
    bool createUser(StringBuffer &userName, const char * prefix, const char * envName, const char * baseDN, const char * pwd, StringBuffer &notes);

public:
    IMPLEMENT_IINTERFACE;
    virtual void init(IPropertyTree *_cfg, const char *_process, const char *_service);
    bool onLDAPQueryDefaults(IEspContext &context, IEspLDAPQueryDefaultsRequest &req, IEspLDAPQueryDefaultsResponse &resp);
    bool onLDAPCreateEnvironment(IEspContext &context, IEspLDAPCreateEnvironmentRequest &req, IEspLDAPCreateEnvironmentResponse &resp);
    void setSecMgr( ISecManager*sm) { secmgr = dynamic_cast<CLdapSecManager*>(sm); }
};


class CldapenvironmentSoapBindingEx : public CldapenvironmentSoapBinding  //base class name built from ECM ESPservice name
{
public:
    CldapenvironmentSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none)
        : CldapenvironmentSoapBinding(cfg, name, process, llevel)
    {
    }

    void addService(const char * name, const char * host, unsigned short port, IEspService & service) override
    {
        CldapenvironmentEx *srv = dynamic_cast<CldapenvironmentEx*>(&service);
        srv->setSecMgr(querySecManager());  //call EspHttpBinding::querySecManager();
        CEspBinding::addService(name, host, port, service);
    }
};

#endif //_LDAPENVIRONMENT_HPP__
