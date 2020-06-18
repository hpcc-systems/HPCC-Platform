/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef __PERMISSIONS_HPP_
#define __PERMISSIONS_HPP_

#ifndef _WIN32
#undef DWORD
typedef unsigned int DWORD;
#endif

#include "ldapconnection.hpp"

#define DEFAULT_OWNER_PERMISSION SecAccess_Full
#define DEFAULT_AUTHENTICATED_USERS_PERMISSION SecAccess_Full
#define DEFAULT_ADMINISTRATORS_PERMISSION SecAccess_Full

class CSecurityDescriptor : public CInterface, implements IInterface
{
private:
    StringAttr   m_name;
    StringAttr   m_relativeBasedn;
    MemoryBuffer m_descriptor;
    StringAttr   m_dn;
    StringAttr   m_objectClass;

public:
    IMPLEMENT_IINTERFACE;
    
    CSecurityDescriptor(const char* name);
    const char* getName();
    const char* getRelativeBasedn();
    MemoryBuffer& getDescriptor();
    void setDescriptor(unsigned len, void* buf);
    void appendDescriptor(unsigned len, void* buf);
    void setDn(const char* dn)
    {
        m_dn.set(dn);
    }

    const char* getDn()
    {
        return m_dn.get();
    }
    void setObjectClass(const char* oc)
    {
        m_objectClass.set(oc);
    }
    const char* getObjectClass()
    {
        return m_objectClass.get();
    }
};


class CMemoryBufferWrapper : public CInterface, implements IInterface
{
private:
    MemoryBuffer m_membuf;

public:
    IMPLEMENT_IINTERFACE;

    MemoryBuffer& getBuffer()
    {
        return m_membuf;
    }

    void setBuffer(unsigned len, void* buf)
    {
        m_membuf.append(len, buf);
    }
};

interface IPermissionProcessor : implements IInterface
{
    virtual void setLdapClient(ILdapClient* client) = 0;
    
    virtual bool getPermissions(ISecUser& user, IArrayOf<CSecurityDescriptor>& sdlist, IArrayOf<ISecResource>& resources) = 0;
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, SecPermissionType ptype) = 0;
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype) = 0;
    virtual CSecurityDescriptor* createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd) = 0;
    virtual bool retrieveUserInfo(ISecUser& user) = 0;

    virtual void getCachedSid(const char* name, MemoryBuffer& sid) = 0;
    virtual void cacheSid(const char* name, int len, const void* sidbuf) = 0;
    virtual void lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE acttype=USER_ACT) = 0;
    virtual int sdSegments(CSecurityDescriptor* sd) = 0;
    virtual bool getPermissionsArray(CSecurityDescriptor *sd, IArrayOf<CPermission>& permissions) = 0;
    virtual CSecurityDescriptor* changePermission(CSecurityDescriptor* initialsd, CPermissionAction& action) = 0;
};

bool toXpath(const char* from, StringBuffer& to);

#endif

