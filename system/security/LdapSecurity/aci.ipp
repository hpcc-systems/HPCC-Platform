/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef __ACI_IPP_
#define __ACI_IPP_
#include "ldapconnection.hpp"
#include "permissions.hpp"

class AciProcessor : public CInterface, implements IPermissionProcessor
{
protected:
    Owned<IPropertyTree> m_cfg;
    Owned<IPropertyTree> m_sidcache;
    Mutex                m_mutex;
    StringBuffer         m_server;
    ILdapClient*         m_ldap_client;
    LdapServerType       m_servertype;

public:
    IMPLEMENT_IINTERFACE;

    AciProcessor(IPropertyTree* cfg);
    
    virtual void setLdapClient(ILdapClient* client)
    {
        m_ldap_client = client;
    }

    virtual bool getPermissions(ISecUser& user, IArrayOf<CSecurityDescriptor>& sdlist, IArrayOf<ISecResource>& resources);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd);
    virtual bool retrieveUserInfo(ISecUser& user);

    virtual void getCachedSid(const char* name, MemoryBuffer& sid);
    virtual void cacheSid(const char* name, int len, const void* sidbuf);
    virtual void lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE acttype=USER_ACT);
    virtual int sdSegments(CSecurityDescriptor* sd);

    virtual StringBuffer& sec2aci(int secperm, StringBuffer& aciperm);
    virtual bool getPermissionsArray(CSecurityDescriptor *sd, IArrayOf<CPermission>& permissions);
    virtual CSecurityDescriptor* changePermission(CSecurityDescriptor* initialsd, CPermissionAction& action);
};

class CIPlanetAciProcessor : public AciProcessor
{
public:
    CIPlanetAciProcessor(IPropertyTree* cfg) : AciProcessor(cfg)
    {
        m_servertype = IPLANET;
    }

    virtual StringBuffer& sec2aci(int secperm, StringBuffer& aciperm);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd);
};

class COpenLdapAciProcessor : public AciProcessor
{
public:
    COpenLdapAciProcessor(IPropertyTree* cfg) : AciProcessor(cfg)
    {
        m_servertype = OPEN_LDAP;
    }
    
    virtual StringBuffer& sec2aci(int secperm, StringBuffer& aciperm);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd);
};

#endif
