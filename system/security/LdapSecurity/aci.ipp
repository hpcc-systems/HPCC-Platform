/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, ISecResource* resource, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, ISecResource* resource, MemoryBuffer& initial_sd);
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
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, ISecResource* resource, MemoryBuffer& initial_sd);
};

class COpenLdapAciProcessor : public AciProcessor
{
public:
    COpenLdapAciProcessor(IPropertyTree* cfg) : AciProcessor(cfg)
    {
        m_servertype = OPEN_LDAP;
    }
    
    virtual StringBuffer& sec2aci(int secperm, StringBuffer& aciperm);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, const char* name, SecPermissionType ptype);
    virtual CSecurityDescriptor* createDefaultSD(ISecUser& user, ISecResource* resource, MemoryBuffer& initial_sd);
};

#endif
