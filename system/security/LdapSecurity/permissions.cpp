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

#pragma warning(disable:4786)

#include "permissions.ipp"
#include "ldapsecurity.ipp"

#if defined(__linux__)
 #include <malloc.h>
#endif
#ifdef _WIN32
 #include <Windows.h>
 #include <aclapi.h>
#endif

#define ADS_RIGHT_GENERIC_EXECUTE 0x20000000
#define ADS_RIGHT_ACTRL_DS_LIST 0x4
#define ADS_RIGHT_DS_LIST_OBJECT 0x80

static const unsigned char authenticated_users_sid[] = 
{
    '\001', '\001', '\000', '\000', '\000', '\000', '\000', 
    '\005', '\013', '\000', '\000', '\000'
};

static const unsigned char everyone_sid[] = 
{
    '\001', '\001', '\000', '\000', '\000', '\000', '\000', 
    '\001', '\000', '\000', '\000', '\000'
};

static const unsigned char administrators_sid[] = 
{
    '\001', '\002', '\000', '\000', '\000', '\000', '\000', '\005', 
    '\040', '\000', '\000', '\000', '\040', '\002', '\000', '\000'
};

PermissionProcessor::PermissionProcessor(IPropertyTree* config)
{
    if(config == NULL)
        throw MakeStringException(-1, "PermissionProcessor() - config is NULL");
    m_cfg.set(config);

    m_sidcache.setown(createPTree());
    m_cfg->getProp(".//@ldapAddress", m_server);
    m_ldap_client = nullptr;
}

SecAccessFlags PermissionProcessor::ldap2sec(unsigned ldapperm)
{
    if((ldapperm & 0xFF) == 0xFF)
    {
        return SecAccess_Full;
    }

    unsigned permission = SecAccess_None;
    if(ldapperm & ADS_RIGHT_DS_LIST_OBJECT)
        permission |= SecAccess_Access;
    if(ldapperm & 0x14)
        permission |= SecAccess_Read;
    if(ldapperm & 0x28)
        permission |= SecAccess_Write;

    return (SecAccessFlags)permission;
}

unsigned PermissionProcessor::sec2ldap(SecAccessFlags secperm)
{
    if((secperm & SecAccess_Full) == SecAccess_Full)
    {
        return 0xF01FF;
    }

    unsigned permission = SecAccess_None;
    if((secperm & SecAccess_Access) == SecAccess_Access)
    {
        permission |=  ADS_RIGHT_DS_LIST_OBJECT;
    }

    if((secperm & SecAccess_Read) == SecAccess_Read)
    {
        permission |=  0x20014;
    }

    if((secperm & SecAccess_Write) == SecAccess_Write)
    {
        permission |= 0x28;
    }

    return permission;
}

NewSecAccessFlags PermissionProcessor::ldap2newsec(unsigned ldapperm)
{
    if((ldapperm & 0xFF) == 0xFF)
    {
        return NewSecAccess_Full;
    }

    unsigned permission = NewSecAccess_None;
    if(ldapperm & ADS_RIGHT_DS_LIST_OBJECT)
        permission |= NewSecAccess_Access;
    if(ldapperm & 0x14)
        permission |= NewSecAccess_Read;
    if(ldapperm & 0x28)
        permission |= NewSecAccess_Write;

    return (NewSecAccessFlags)permission;
}

unsigned PermissionProcessor::newsec2ldap(NewSecAccessFlags secperm)
{
    if((secperm & NewSecAccess_Full) == NewSecAccess_Full)
    {
        return 0xF01FF;
    }

    unsigned permission = SecAccess_None;
    if((secperm & SecAccess_Access) == SecAccess_Access)
    {
        permission |=  ADS_RIGHT_DS_LIST_OBJECT;
    }

    if((secperm & NewSecAccess_Read) == NewSecAccess_Read)
    {
        permission |=  0x20014;
    }

    if((secperm & NewSecAccess_Write) == NewSecAccess_Write)
    {
        permission |= 0x28;
    }

    return permission;
}

bool toXpath(const char* from, StringBuffer& to)
{
    if(from == NULL || *from == '\0')
        return false;

    int i = 0;
    char c;
    while((c = from[i++]) != '\0')
    {
        if(c == '\'')
            return false;
        else if(c == ' ')
            to.append(':');
        else
            to.append(c);
    }
    return true;
}

void PermissionProcessor::getCachedSid(const char* name, MemoryBuffer& sid)
{
    StringBuffer buf;
    if(toXpath(name, buf))
    {
        synchronized block(m_monitor);
        try
        {
            m_sidcache->getPropBin(buf.str(), sid);
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            OERRLOG("error getPropbin - %s", errmsg.str());
            e->Release();
        }
        catch(...)
        {
            OERRLOG("Unknow exception getPropBin");
        }
    }
}

void PermissionProcessor::cacheSid(const char* name, int len, const void* sidbuf)
{
    if(!name || !*name || !len)
        return;

    StringBuffer buf;
    if(toXpath(name, buf))
    {
        synchronized block(m_monitor);
        try
        {
            if(!m_sidcache->hasProp(buf.str()))
            {
                m_sidcache->setPropBin(buf.str(), len, sidbuf);
            }
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            OERRLOG("error addPropBin - %s", errmsg.str());
            e->Release();
        }
        catch(...)
        {
            OERRLOG("Unknow exception addPropBin");
        }
    }
}

void PermissionProcessor::lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE act_type)
{
    act_sid.clear();

    MemoryBuffer mb;
    getCachedSid(act_name, mb);
    if(mb.length() > 0)
    {
        act_sid.append(mb.length(), mb.toByteArray());
    }
    else
    {
#if 0
        char sidbuf[SIDLEN+1];
        PSID sid;
        DWORD sidlen = SIDLEN;
        char domain[DOMAINLEN+1];
        DWORD dlen = DOMAINLEN;
        SID_NAME_USE nameuse;
        int ret;
    
        sid = (PSID)sidbuf;
        if(m_serverOnSameDomain)
            ret = LookupAccountName(NULL, act_name, sid, &sidlen, domain, &dlen, &nameuse);
        else
            ret = LookupAccountName(m_server.str(), act_name, sid, &sidlen, domain, &dlen, &nameuse);
        if(ret == 0)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error getting SID of user %s - error code = %d", act_name, error);
        }
        else
        {
            sidlen = GetLengthSid(sid);
            cacheSid(act_name, sidlen, (const void*)sidbuf);
            act_sid.append(sidlen, sidbuf);
        }
#else
        if(m_ldap_client != NULL)
        {
            m_ldap_client->lookupSid(act_name, act_sid, act_type);
            if(act_sid.length() > 0)
                cacheSid(act_name, act_sid.length(), (const void*)act_sid.toByteArray());
        }
#endif
    }

}

bool PermissionProcessor::retrieveUserInfo(ISecUser& user)
{
    CLdapSecUser* ldapuser = (CLdapSecUser*)&user;
    const char* username = user.getName();
    if(username == NULL || strlen(username) == 0)
    {
        OWARNLOG("retrieveUserInfo : username is empty");
        return false;
    }

    MemoryBuffer umb;
    lookupSid(username, umb);
    int sidlen = umb.length();
    if(sidlen <= 0)
    {
        OWARNLOG("user %s not found", username);
        return false;
    }

    const char* usersidbuf = umb.toByteArray();
    ldapuser->setUserSid(sidlen, usersidbuf);
    unsigned uid = usersidbuf[sidlen - 4];
    int i;
    for(i = 3; i > 0; i--)
    {
        uid = (uid << 8) + usersidbuf[sidlen - i];
    }
    ldapuser->setUserID(uid);

    const char* fullname = user.getFullName();
    if((fullname == NULL || *fullname == '\0') && m_ldap_client != NULL)
    {
        return m_ldap_client->getUserInfo(user);
    }
    return true;
}

bool PermissionProcessor::retrieveGroupInfo(ISecUser& user, BufferArray& groupsids)
{
    CLdapSecUser* ldapuser = (CLdapSecUser*)&user;
    const char* username = user.getName();

    StringArray groups;
    if(m_ldap_client != NULL)
        m_ldap_client->getGroups(username, groups);
    int numgroups = groups.length(); 
    ForEachItemIn(x, groups)
    {
        const char* group = (const char*)groups.item(x);
        if(group == NULL)
            continue;

        MemoryBuffer gmb;
        lookupSid(group, gmb, GROUP_ACT);
        if(gmb.length() == 0)
        {
            DBGLOG("group %s not found", group);
            continue;
        }
        CMemoryBufferWrapper* mbuf = new CMemoryBufferWrapper;
        mbuf->setBuffer(gmb.length(), (void*)gmb.toByteArray());
        groupsids.append(*mbuf);
    }
    //automaitcaly include "Authenticated Users" and "everyone"
    CMemoryBufferWrapper* au_buf = new CMemoryBufferWrapper;
    au_buf->setBuffer(sizeof(authenticated_users_sid), (void*)authenticated_users_sid);
    groupsids.append(*au_buf);

    CMemoryBufferWrapper* everyone_buf = new CMemoryBufferWrapper;
    everyone_buf->setBuffer(sizeof(everyone_sid), (void*)everyone_sid);
    groupsids.append(*everyone_buf);

    return true;
}

int sizeofSid(PSID pSid)
{
    if(pSid == NULL)
        return 0;

    PISID pisid = (PISID)pSid;

    return sizeof(SID) + (pisid->SubAuthorityCount - 1)*4;
}

#ifndef _WIN32
bool GetSecurityDescriptorDacl(PSECURITY_DESCRIPTOR pSecurityDescriptor, int* lpbDaclPresent, PACL *pDacl, int* lpbDaclDefaulted)
{
    PISECURITY_DESCRIPTOR psd = (PISECURITY_DESCRIPTOR)pSecurityDescriptor;
    SECURITY_DESCRIPTOR_CONTROL ctrl = psd->Control;
    if(ctrl & SE_DACL_PRESENT)
        *lpbDaclPresent = 1;
    else
        *lpbDaclPresent = 0;

    if(ctrl & SE_DACL_DEFAULTED)
        *lpbDaclDefaulted = 1;
    else
        *lpbDaclDefaulted = 0;

    if(*lpbDaclPresent)
    {
        if(ctrl & SE_SELF_RELATIVE)
        {
            PISECURITY_DESCRIPTOR_RELATIVE sr_psd = (PISECURITY_DESCRIPTOR_RELATIVE)pSecurityDescriptor;
            unsigned char* bptr = (unsigned char*)pSecurityDescriptor;
            DWORD doffset = sr_psd->Dacl;
            *pDacl = (PACL)(bptr + doffset);
        }
        else
        {
            *pDacl = (PACL)psd->Dacl;
        }
    }
    else
    {
        *pDacl = NULL;
    }

    return true;
}

bool GetAclInformation(PACL pAcl, void* pAclInformation, DWORD nAclInformationLength, ACL_INFORMATION_CLASS dwAclInformationClass)
{
    if(pAcl == NULL)
    {
        DBGLOG("GetAclInformation : pAcl is NULL");
        return false;
    }

    if(nAclInformationLength < sizeof(ACL_SIZE_INFORMATION))
    {
        DBGLOG("GetAclInformation: ACL_SIZE_INFORMATION buffer size too small");
        return false;
    }

    PACL_SIZE_INFORMATION pinfo = (PACL_SIZE_INFORMATION)pAclInformation;
    pinfo->AceCount = pAcl->AceCount;

    unsigned char* ptr = (unsigned char*)pAcl + sizeof(ACL);
    int size = sizeof(ACL);
    for(int i = 0; i < pAcl->AceCount; i++)
    {
        PACCESS_ALLOWED_ACE curace = (PACCESS_ALLOWED_ACE)ptr;
        int curace_size = curace->Header.AceSize;
        size += curace_size;
        ptr += curace_size;
    }

    pinfo->AclBytesInUse = size;
    pinfo->AclBytesFree = pAcl->AclSize - pinfo->AclBytesInUse;

    return true;
}

bool GetAce (PACL pAcl, DWORD dwAceIndex, void**pAce)
{
    if(pAcl == NULL)
    {
        DBGLOG("GetAce : pAcl is NULL");
        return false;
    }

    if(dwAceIndex >= pAcl->AceCount)
    {
        return false;
    }

    unsigned char* ptr = (unsigned char*)pAcl + sizeof(ACL);
    for(int i = 0; i < dwAceIndex; i++)
    {
        PACCESS_ALLOWED_ACE curace = (PACCESS_ALLOWED_ACE)ptr;
        int curace_size = curace->Header.AceSize;
        ptr += curace_size;
    }
    
    *pAce = (void*)ptr;

    return true;
}

bool EqualSid (PSID pSid1, PSID pSid2)
{
    if(pSid1 == NULL || pSid2 == NULL)
        return false;

    int size1 = sizeofSid(pSid1);
    int size2 = sizeofSid(pSid2);

    return (size1 == size2) && (memcmp(pSid1, pSid2, size1) == 0);
}

bool InitializeSecurityDescriptor(PSECURITY_DESCRIPTOR pSecurityDescriptor, DWORD dwRevision)
{
    if(pSecurityDescriptor == NULL)
    {
        DBGLOG("InitializeSecurityDescriptor : pSecurityDescriptor is NULL");
        return false;
    }

    PISECURITY_DESCRIPTOR psd = (PISECURITY_DESCRIPTOR)pSecurityDescriptor;
    psd->Revision = dwRevision;
    psd->Control = 0;
    psd->Owner = NULL;
    psd->Group = NULL;
    psd->Dacl = NULL;
    psd->Sacl = NULL;
    return true;
}

bool InitializeAcl(PACL pAcl, DWORD nAclLength, DWORD dwAclRevision)
{
    if(pAcl == NULL)
    {
        DBGLOG("InitializeAcl : pAcl is NULL");
        return false;
    }
    
    memset(pAcl, 0xcc, nAclLength);
    pAcl->AclRevision = dwAclRevision;
    pAcl->AclSize = nAclLength;
    pAcl->AceCount = 0;
    pAcl->Sbz1 = 0;
    pAcl->Sbz2 = 0;
    return true;
}

bool AddAccessAllowedAce(PACL pAcl, DWORD dwAceRevision, DWORD AccessMask, PSID pSid)
{
    if(pAcl == NULL)
    {
        DBGLOG("AddAccessAllowedAce: PACL is NULL");
        return false;
    }

    int sidsize = sizeofSid(pSid);
    if(sidsize == 0)
    {
        DBGLOG("AddAccessAllowedAce : empty sid");
        return false;
    }

    unsigned char* ptr = (unsigned char*)pAcl + sizeof(ACL);
    int size = sizeof(ACL);
    for(int i = 0; i < pAcl->AceCount; i++)
    {
        PACCESS_ALLOWED_ACE curace = (PACCESS_ALLOWED_ACE)ptr;
        int curace_size = curace->Header.AceSize;
        size += curace_size;
        ptr += curace_size;
    }

    int newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sidsize;
    if(size + newace_size > pAcl->AclSize)
    {
        DBGLOG("AddAccessAllowedAce : not enough space to add new ace");
        return false;
    }

    PACCESS_ALLOWED_ACE newace = (PACCESS_ALLOWED_ACE)ptr;
    newace->Header.AceFlags = 0;
    newace->Header.AceSize = newace_size;
    newace->Header.AceType = ACCESS_ALLOWED_ACE_TYPE;
    newace->Mask = AccessMask;
    ptr = ptr + sizeof(ACE_HEADER)+sizeof(ACCESS_MASK);
    memcpy(ptr, pSid, sidsize);
    pAcl->AceCount++;

    return true;
}

bool AddAccessAllowedAce(PACL pNewAcl, DWORD* pNewLength, PACL pAcl, DWORD dwAceRevision, DWORD AccessMask, PSID pSid)
{
    if(pAcl == NULL)
    {
        DBGLOG("AddAccessAllowedAce: PACL is NULL");
        return false;
    }

    if(pNewAcl == NULL)
    {
        DBGLOG("AddAccessAllowedAce : pNewAcl is NULL");
        return false;
    }

    int sidsize = sizeofSid(pSid);
    if(sidsize == 0)
    {
        DBGLOG("AddAccessAllowedAce : empty sid");
        return false;
    }

    ACL_SIZE_INFORMATION acl_size_info;
    GetAclInformation(pAcl, &acl_size_info, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
    int acl_size = acl_size_info.AclBytesInUse;
    int newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sizeofSid(pSid);
    int new_acl_size = acl_size + newace_size;
    if(new_acl_size > *pNewLength)
    {
        *pNewLength = new_acl_size;
        DBGLOG("AddAccessAllowedAce : not enough buffer space to hold new pNewAcl");
        return false;
    }

    memcpy((void*)pNewAcl, (void*)pAcl, acl_size);

    pNewAcl->AclSize = *pNewLength;

    unsigned char* ptr = (unsigned char*)pNewAcl + acl_size;

    PACCESS_ALLOWED_ACE newace = (PACCESS_ALLOWED_ACE)ptr;
    newace->Header.AceFlags = 0;
    newace->Header.AceSize = newace_size;
    newace->Header.AceType = ACCESS_ALLOWED_ACE_TYPE;
    newace->Mask = AccessMask;
    ptr = ptr + sizeof(ACE_HEADER)+sizeof(ACCESS_MASK);
    memcpy(ptr, pSid, sidsize);
    pNewAcl->AceCount++;

    return true;
}

bool AddAccessDeniedAce(PACL pNewAcl, DWORD* pNewLength, PACL pAcl, DWORD dwAceRevision, DWORD AccessMask, PSID pSid)
{
    if(pAcl == NULL)
    {
        DBGLOG("AddAccessDeniedAce: PACL is NULL");
        return false;
    }

    if(pNewAcl == NULL)
    {
        DBGLOG("AddAccessDeniedAce : pNewAcl is NULL");
        return false;
    }

    int sidsize = sizeofSid(pSid);
    if(sidsize == 0)
    {
        DBGLOG("AddAccessDeniedAce : empty sid");
        return false;
    }

    ACL_SIZE_INFORMATION acl_size_info;
    GetAclInformation(pAcl, &acl_size_info, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
    int acl_size = acl_size_info.AclBytesInUse;
    int newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sizeofSid(pSid);
    int new_acl_size = acl_size + newace_size;
    if(new_acl_size > *pNewLength)
    {
        *pNewLength = new_acl_size;
        DBGLOG("AddAccessDeniedAce : not enough buffer space to hold new pNewAcl");
        return false;
    }

    memcpy((void*)pNewAcl, (void*)pAcl, acl_size);

    pNewAcl->AclSize = *pNewLength;

    unsigned char* ptr = (unsigned char*)pNewAcl + acl_size;

    PACCESS_DENIED_ACE newace = (PACCESS_DENIED_ACE)ptr;
    newace->Header.AceFlags = 0;
    newace->Header.AceSize = newace_size;
    newace->Header.AceType = ACCESS_DENIED_ACE_TYPE;
    newace->Mask = AccessMask;
    ptr = ptr + sizeof(ACE_HEADER)+sizeof(ACCESS_MASK);
    memcpy(ptr, pSid, sidsize);
    pNewAcl->AceCount++;

    return true;
}


bool SetSecurityDescriptorDacl(PSECURITY_DESCRIPTOR pSecurityDescriptor, bool bDaclPresent, PACL pDacl, bool bDaclDefaulted)
{
    PISECURITY_DESCRIPTOR pisd = (PISECURITY_DESCRIPTOR)pSecurityDescriptor;
    if(pisd->Control & SE_SELF_RELATIVE)
    {
        DBGLOG("SetSecurityDescriptorDacl : can't set dacl of self relative SD");
        return false;
    }

    pisd->Control |= SE_DACL_PRESENT;
    if(bDaclPresent)
        pisd->Control |= SE_DACL_PRESENT;
    else
        pisd->Control &= ~(SE_DACL_PRESENT);

    if(bDaclDefaulted)
        pisd->Control |= SE_DACL_DEFAULTED;
    else
        pisd->Control &= ~(SE_DACL_DEFAULTED);

    pisd->Dacl = pDacl;

    return true;
}

bool MakeSelfRelativeSD(PSECURITY_DESCRIPTOR pAbsoluteSecurityDescriptor, 
                        PSECURITY_DESCRIPTOR pSelfRelativeSecurityDescriptor, 
                        DWORD* lpdwBufferLength)
{
    int sdsize = GetSecurityDescriptorLength(pAbsoluteSecurityDescriptor);
    if(sdsize > *lpdwBufferLength)
    {
        DBGLOG("MakeSelfRelativeSD : buffer not big enough");
        *lpdwBufferLength = sdsize;
    }

    PISECURITY_DESCRIPTOR pasd = (PISECURITY_DESCRIPTOR)pAbsoluteSecurityDescriptor;
    PISECURITY_DESCRIPTOR_RELATIVE pssd = (PISECURITY_DESCRIPTOR_RELATIVE)pSelfRelativeSecurityDescriptor;
    pssd->Revision = pasd->Revision;
    pssd->Sbz1 = 0;
    pssd->Control = pasd->Control | SE_SELF_RELATIVE;

    unsigned char* bptr = (unsigned char*)pSelfRelativeSecurityDescriptor;
    
    DWORD owner_offset = 20;
    int owner_size = 0;
    if(pasd->Owner != NULL)
    {
        owner_size = sizeofSid(pasd->Owner);
        memcpy(bptr + owner_offset, pasd->Owner, owner_size);
        pssd->Owner = owner_offset;
    }
    else 
    {
        pssd->Owner = 0;
    }

    DWORD group_offset = owner_offset + owner_size;
    int group_size = 0;
    if(pasd->Group != NULL)
    {
        group_size = sizeofSid(pasd->Group);
        memcpy(bptr + group_offset, pasd->Group, group_size);
        pssd->Group = group_offset;
    }
    else
    {
        pssd->Group = 0;
    }

    DWORD sacl_offset = group_offset + group_size;
    int sacl_size = 0;
    if(pasd->Sacl != NULL)
    {
        ACL_SIZE_INFORMATION sacl_size_info;
        GetAclInformation(pasd->Sacl, &sacl_size_info, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        sacl_size = sacl_size_info.AclBytesInUse + sacl_size_info.AclBytesFree;
        memcpy(bptr + sacl_offset, pasd->Sacl, sacl_size);
        pssd->Sacl = sacl_offset;
    }
    else
    {
        pssd->Sacl = 0;
    }
    
    DWORD dacl_offset = sacl_offset + sacl_size;
    int dacl_size = 0;
    if(pasd->Dacl != NULL)
    {
        ACL_SIZE_INFORMATION dacl_size_info;
        GetAclInformation(pasd->Dacl, &dacl_size_info, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        dacl_size = dacl_size_info.AclBytesInUse + dacl_size_info.AclBytesFree;
        memcpy(bptr + dacl_offset, pasd->Dacl, dacl_size);
        pssd->Dacl = dacl_offset;
    }
    else
    {
        pssd->Dacl = 0;
    }

    return true;
}

bool MakeAbsoluteSD(PSECURITY_DESCRIPTOR pSelfRelativeSecurityDescriptor, 
                    PSECURITY_DESCRIPTOR pAbsoluteSecurityDescriptor, 
                    DWORD* lpdwAbsoluteSecurityDescriptorSize, 
                    PACL pDacl, 
                    DWORD* lpdwDaclSize, 
                    PACL pSacl,  
                    DWORD* lpdwSaclSize, 
                    PSID pOwner, 
                    DWORD* lpdwOwnerSize, 
                    PSID pPrimaryGroup, 
                    DWORD* lpdwPrimaryGroupSize)
{
    if(pSelfRelativeSecurityDescriptor == NULL)
    {
        DBGLOG("MakeAbsoluteSD : pSelfRelativeSecurityDescriptor is NULL");
        return false;
    }
    
    PISECURITY_DESCRIPTOR_RELATIVE prsd = (PISECURITY_DESCRIPTOR_RELATIVE)pSelfRelativeSecurityDescriptor;

    unsigned ok = true;
    unsigned char* ptr = (unsigned char*)pSelfRelativeSecurityDescriptor;

    if(*lpdwAbsoluteSecurityDescriptorSize < sizeof(SECURITY_DESCRIPTOR))
    {
        ok = false;
        DBGLOG("MakeAbsoluteSD : pdwAbsoluteSecurityDescriptorSize < sizeof(SECURITY_DESCRIPTOR)");
        *lpdwAbsoluteSecurityDescriptorSize = sizeof(SECURITY_DESCRIPTOR);
    }

    PSID r_pOwner = NULL;
    int owner_size = 0;
    if(prsd->Owner != 0)
    {
        r_pOwner = (PSID)(ptr + prsd->Owner);
        owner_size = sizeofSid(r_pOwner);
        if(*lpdwOwnerSize < owner_size)
        {
            ok = false;
            DBGLOG("MakeAbsoluteSD : *lpdwOwnerSize < owner_size");
            *lpdwOwnerSize = owner_size;
        }
    }

    PSID r_pGroup = NULL;
    int group_size = 0;
    if(prsd->Group != 0)
    {
        r_pGroup = (PSID)(ptr + prsd->Group);
        group_size = sizeofSid(r_pGroup);
        if(*lpdwPrimaryGroupSize < group_size)
        {
            ok = false;
            DBGLOG("MakeAbsoluteSD : *lpdwPrimaryGroupSize < group_size");
            *lpdwPrimaryGroupSize = group_size;
        }
    }

    ACL_SIZE_INFORMATION DaclSizeInfo;
    PACL r_pDacl = NULL;
    if(prsd->Dacl != 0)
    {
        r_pDacl = (PACL)(ptr + prsd->Dacl);
        GetAclInformation(r_pDacl, &DaclSizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        if(*lpdwDaclSize < DaclSizeInfo.AclBytesInUse)
        {
            ok = false;
            DBGLOG("MakeAbsoluteSD : *lpdwDaclSize < DaclSizeInfo.AclBytesInUse");
            *lpdwDaclSize = DaclSizeInfo.AclBytesInUse;
        }
    }

    ACL_SIZE_INFORMATION SaclSizeInfo;
    PACL r_pSacl = NULL;
    if(prsd->Sacl != 0)
    {
        r_pSacl = (PACL)(ptr + prsd->Sacl);
        GetAclInformation(r_pSacl, &SaclSizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        if(*lpdwSaclSize < SaclSizeInfo.AclBytesInUse)
        {
            ok = false;
            DBGLOG("MakeAbsoluteSD : *lpdwSaclSize < SaclSizeInfo.AclBytesInUse");
            *lpdwSaclSize = SaclSizeInfo.AclBytesInUse;
        }
    }

    if(!ok)
    {
        return false;
    }
    
    if(pAbsoluteSecurityDescriptor == NULL)
    {
        DBGLOG("MakeAbsoluteSD : pAbsoluteSecurityDescriptor is NULL");
        return false;
    }
    
    PISECURITY_DESCRIPTOR pasd = (PISECURITY_DESCRIPTOR)pAbsoluteSecurityDescriptor;
    pasd->Revision = prsd->Revision;
    pasd->Sbz1 = 0;
    pasd->Control = prsd->Control & (~(SE_SELF_RELATIVE));

    if(r_pOwner != NULL)
    {
        if(pOwner == NULL)
        {
            DBGLOG("MakeAbsoluteSD : pOwner is NULL");
            return false;
        }
        else
        {
            memset(pOwner, 0, *lpdwOwnerSize);
            memcpy(pOwner, r_pOwner, owner_size);
            pasd->Owner = pOwner;
        }
    }
    else
    {
        pasd->Owner = NULL;
    }

    if(r_pGroup != NULL)
    {
        if(pPrimaryGroup == NULL)
        {
            DBGLOG("MakeAbsoluteSD : pPrimaryGroup is NULL");
            return false;
        }
        else
        {
            memset(pPrimaryGroup, 0, *lpdwPrimaryGroupSize);
            memcpy(pPrimaryGroup, r_pGroup, group_size);
            pasd->Group = pPrimaryGroup;
        }
    }
    else
    {
        pasd->Group = NULL;
    }


    if(r_pDacl != NULL)
    {
        if(pDacl == NULL)
        {
            DBGLOG("MakeAbsoluteSD : pDacl is NULL");
            return false;
        }
        else
        {
            memset(pDacl, 0, *lpdwDaclSize);
            memcpy(pDacl, r_pDacl, DaclSizeInfo.AclBytesInUse);
            pasd->Dacl = pDacl;
        }
    }
    else
    {
        pasd->Dacl = NULL;
    }

    if(r_pSacl != NULL)
    {
        if(pSacl == NULL)
        {
            DBGLOG("MakeAbsoluteSD : pSacl is NULL");
            return false;
        }
        else
        {
            memset(pSacl, 0, *lpdwSaclSize);
            memcpy(pSacl, r_pSacl, SaclSizeInfo.AclBytesInUse);
            pasd->Sacl = pSacl;
        }
    }
    else
    {
        pasd->Sacl = NULL;
    }

    return true;
}

unsigned long GetSecurityDescriptorLength(PSECURITY_DESCRIPTOR pSecurityDescriptor)
{
    PISECURITY_DESCRIPTOR pisd = (PISECURITY_DESCRIPTOR)pSecurityDescriptor;
    unsigned long sdsize = 0;
    if(pisd->Control & SE_SELF_RELATIVE)
    {
        DBGLOG("GetSecurityDescriptorLength : does not support self-relative format yet");
        return 0;
    }

    sdsize += 4;
    if(pisd->Owner != NULL)
        sdsize += sizeofSid(pisd->Owner);
    if(pisd->Group != NULL)
        sdsize += sizeofSid(pisd->Group);
    if(pisd->Sacl != NULL)
    {
        ACL_SIZE_INFORMATION ASizeInfo;
        GetAclInformation(pisd->Sacl, &ASizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        sdsize += ASizeInfo.AclBytesInUse + ASizeInfo.AclBytesFree;
    }
    if(pisd->Dacl != NULL)
    {
        ACL_SIZE_INFORMATION ASizeInfo;
        GetAclInformation(pisd->Dacl, &ASizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        sdsize += ASizeInfo.AclBytesInUse + ASizeInfo.AclBytesFree;
    }

    // Self-relative SD is 16 bytes longer. Make it bigger in-order to be able to create buffer for 
    // Self-Relative SD. This seems to be the way that standard MS function does it.
    sdsize += 16;

    return sdsize;
}

bool DeleteAce(PACL pAcl, DWORD dwAceIndex)
{
    if(pAcl == NULL)
    {
        DBGLOG("DeleteAce: PACL is NULL");
        return false;
    }

    unsigned char* ptr = (unsigned char*)pAcl + sizeof(ACL);
    unsigned char* delptr = ptr;
    int total_size = sizeof(ACL);
    int delsize = 0;
    for(int i = 0; i < pAcl->AceCount; i++)
    {
        PACCESS_ALLOWED_ACE curace = (PACCESS_ALLOWED_ACE)ptr;
        int curace_size = curace->Header.AceSize;
        if(i == dwAceIndex)
        {
            delsize = curace_size;
            delptr = ptr;
        }
        total_size += curace_size;
        ptr += curace_size;
    }

    if(delsize > 0)
    {
        int movsize = total_size - (delptr - (unsigned char*)pAcl) - delsize;
        for(int i = 0; i < movsize; i++)
        {
            delptr[i] = delptr[delsize + i];
        }
        pAcl->AceCount--;
    }

    return true;
}   



#endif

bool PermissionProcessor::getPermissions(ISecUser& user, IArrayOf<CSecurityDescriptor>& sdlist, IArrayOf<ISecResource>& resources)
{
    //MTimeSection timing(NULL, "getPermissions");
    int num_resources = resources.length();
    if(num_resources <= 0)
        return true;

    const char* username = user.getName();

#ifdef USE_LOGONUSER
    // To use the NT LogonUser function, 2 conditions must be met
    // 1. The server is on the same domain as the machine that calls LogonUser, or on two domains that share user information.
    // 2. The process that calls LogonUser must have SE_TCB_NAME privilege. To get this privilege, we need to allow the user
    //    to Act As Part of the OS (set in Local Security Policy tool, under "Local Policies"/"User Rights Assignment")
    const char* password = user.credentials().getPassword();
    HANDLE usertoken;
    int ret = LogonUser((char*)username, NULL, (char*)password, LOGON32_LOGON_NETWORK, LOGON32_PROVIDER_DEFAULT, &usertoken);
    if(ret == 0)
    {
        throw MakeStringException(-1, "LogonUser %s error, error code = %d\n", username, GetLastError());
        return false;
    }
    for(int i = 0; i < num_resources; i++)
    {
        ISecResource& resource = resources.item(i);
        CSecurityDescriptor& csd = sdlist.item(i);
        MemoryBuffer& sdbuf = csd.getDescriptor();
        if(sdbuf.length() == 0)
            continue;
        PSECURITY_DESCRIPTOR psd = (PSECURITY_DESCRIPTOR)(sdbuf.toByteArray());

        GENERIC_MAPPING gmap;
        PRIVILEGE_SET pset;
        DWORD pset_len = sizeof(pset);
        DWORD granted_access = 0;
        BOOL access_status;
        ret = AccessCheck(psd, usertoken, MAXIMUM_ALLOWED, &gmap, &pset, &pset_len, &granted_access, &access_status);
        if(ret == 0)
        {
            DBGLOG("error calling AccessCheck = %d\n", GetLastError());
            granted_access = 0;
        }

        SecAccessFlags permission = ldap2sec(granted_access);
        resource.setAccessFlags(permission);
    }   
    CloseHandle(usertoken);

#else
    CLdapSecUser* ldapuser = (CLdapSecUser*)&user;

    MemoryBuffer& usersidbuf = ldapuser->getUserSid();
    if(usersidbuf.length() == 0)
        retrieveUserInfo(user);

    PSID usersid = (PSID)(ldapuser->getUserSid().toByteArray());
    BufferArray groupsids;
    retrieveGroupInfo(user, groupsids);
    int numgroups = groupsids.length();
    for(int i = 0; i < num_resources; i++)
    {
        ISecResource& resource = resources.item(i);
        CSecurityDescriptor& csd = sdlist.item(i);
        MemoryBuffer& sdbuf = csd.getDescriptor();
        if(sdbuf.length() == 0)
        {
            resource.setAccessFlags(SecAccess_Unavailable);
            continue;
        }
        PSECURITY_DESCRIPTOR psd = (PSECURITY_DESCRIPTOR)(sdbuf.toByteArray());

        PACL dacl;
        int dpresent;
        int dacldefaulted;
        GetSecurityDescriptorDacl(psd, &dpresent, &dacl, &dacldefaulted);

        ACL_SIZE_INFORMATION ASizeInfo;

        GetAclInformation(dacl, &ASizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        PACCESS_ALLOWED_ACE pAce;
        unsigned iAce;

        unsigned allows = 0;
        unsigned denies = 0;
        for(iAce = 0; iAce < ASizeInfo.AceCount; iAce++)
        {
            GetAce(dacl, iAce, (void**)&pAce);
            PSID cursid = (PSID)&(pAce->SidStart);

            BYTE AType = pAce->Header.AceType;
            if(AType == ACCESS_ALLOWED_ACE_TYPE)
            {
                if(EqualSid(usersid, cursid))
                {
                    allows |= pAce->Mask;
                }
                else
                {

                    for(int i = 0; i < numgroups; i++)
                    {
                        PSID gsid = (PSID)(groupsids.item(i).getBuffer().toByteArray());
                        if(EqualSid(gsid, cursid))
                        {
                            allows |= pAce->Mask;
                        }
                    }
                }
            }
            else if(AType == ACCESS_DENIED_ACE_TYPE)
            {
                if(EqualSid(usersid, cursid))
                {
                    denies |= pAce->Mask;
                }
                else
                {
                    for(int i = 0; i < numgroups; i++)
                    {
                        PSID gsid = (PSID)(groupsids.item(i).getBuffer().toByteArray());
                        if(EqualSid(gsid, cursid))
                        {
                            denies |= pAce->Mask;
                        }
                    }
                }
            }
        }

        unsigned ldapperm = allows & (~denies);
        SecAccessFlags permission = ldap2sec(ldapperm);
        resource.setAccessFlags(permission);
    }
#endif

    return true;
}

bool PermissionProcessor::getPermissionsArray(CSecurityDescriptor *sd, IArrayOf<CPermission>& permissions)
{
    MemoryBuffer& sdbuf = sd->getDescriptor();
    if(sdbuf.length() == 0)
    {
        throw MakeStringException(-1, "security descriptor is empty");
    }
    PSECURITY_DESCRIPTOR psd = (PSECURITY_DESCRIPTOR)(sdbuf.toByteArray());

    PACL dacl;
    int dpresent;
    int dacldefaulted;
    GetSecurityDescriptorDacl(psd, &dpresent, &dacl, &dacldefaulted);

    ACL_SIZE_INFORMATION ASizeInfo;

    GetAclInformation(dacl, &ASizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
    PACCESS_ALLOWED_ACE pAce;
    unsigned iAce;
    PSID au_psid = (PSID)(authenticated_users_sid);
    PSID everyone_psid = (PSID)(everyone_sid);
    PSID administrators_psid = (PSID)(administrators_sid);

    for(iAce = 0; iAce < ASizeInfo.AceCount; iAce++)
    {
        unsigned allows = 0;
        unsigned denies = 0;
        GetAce(dacl, iAce, (void**)&pAce);
        PSID cursid = (PSID)&(pAce->SidStart);
        ACT_TYPE act_type;

        BYTE AType = pAce->Header.AceType;
        if(AType == ACCESS_ALLOWED_ACE_TYPE)
        {
            allows = ldap2newsec(pAce->Mask);
        }
        else if(AType == ACCESS_DENIED_ACE_TYPE)
        {
            denies = ldap2newsec(pAce->Mask);
        }

        if(allows == 0 && denies == 0)
            continue;

        StringBuffer account_name;
        if(EqualSid(cursid, au_psid))
        {
            account_name.append("Authenticated Users");
            act_type = GROUP_ACT;
        }
        else if(EqualSid(cursid, everyone_psid))
        {
            account_name.append("everyone");
            act_type = GROUP_ACT;
        }
        else if(EqualSid(cursid, administrators_psid))
        {
            account_name.append("Administrators");
            act_type = GROUP_ACT;
        }
        else
        {
            MemoryBuffer sidbuf;
            int len = sizeofSid(cursid);
            sidbuf.append(len, (void*)cursid);
            if(m_ldap_client != NULL)
                m_ldap_client->lookupAccount(sidbuf, account_name, act_type);
        }
        if(account_name.length() > 0)
        {
            bool found = false;
            for(unsigned i = 0; i < permissions.length(); i++)
            {
                CPermission& curperm = permissions.item(i);
                const char* curname = curperm.getAccount_name();
                if(curname != NULL && stricmp(curname, account_name.str()) == 0)
                {
                    found = true;
                    if(allows != 0)
                        curperm.setAllows(curperm.getAllows() | allows);
                    if(denies != 0)
                        curperm.setDenies(curperm.getDenies() | denies);
                }
            }
            if(!found)
            {
                Owned<CPermission> perm = new CPermission(account_name.str(), act_type, allows, denies);
                permissions.append(*LINK(perm.get()));
            }
        }
    }
    
    return true;
}

CSecurityDescriptor* PermissionProcessor::changePermission(CSecurityDescriptor* initialsd, CPermissionAction& action)
{
    const char* initial_buf = initialsd->getDescriptor().toByteArray();
    PSECURITY_DESCRIPTOR pisd = (PSECURITY_DESCRIPTOR)initial_buf;

    PSID act_psid;
    MemoryBuffer act_sidbuf;
    if(action.m_account_type == GROUP_ACT && stricmp(action.m_account_name.str(), "Administrators") == 0)
    {
        if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
        {
            throw MakeStringException(-1, "Please don't set deny permissions for Administrators or Authenticated Users");
        }
        act_psid = (PSID)administrators_sid;
    }
    else if(action.m_account_type == GROUP_ACT && stricmp(action.m_account_name.str(), "Authenticated Users") == 0)
    {
        if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
        {
            throw MakeStringException(-1, "Please don't set deny permissions for Administrators or Authenticated Users");
        }
        act_psid = (PSID)authenticated_users_sid;
    }
    else if(action.m_account_type == GROUP_ACT && stricmp(action.m_account_name.str(), "everyone") == 0)
    {
        act_psid = (PSID)everyone_sid;
    }
    else
    {
        lookupSid(action.m_account_name.str(), act_sidbuf, action.m_account_type);
        if(act_sidbuf.length() == 0)
            throw MakeStringException(-1, "account %s's sid can't be found", action.m_account_name.str());
        act_psid = (PSID)act_sidbuf.toByteArray();
    }

    int rc = 0;

    DWORD sd_size = 0;
    DWORD dacl_size = 0;
    DWORD sacl_size = 0;
    DWORD owner_size = 0;
    DWORD pgroup_size = 0;

    MakeAbsoluteSD(pisd, NULL, &sd_size, NULL, &dacl_size, NULL, &sacl_size, NULL, &owner_size, NULL, &pgroup_size);

    unsigned char* sd_buf = (unsigned char*)alloca(sd_size+1);
    unsigned char* dacl_buf = (unsigned char*)alloca(dacl_size+1);
    unsigned char* sacl_buf = (unsigned char*)alloca(sacl_size+1);
    unsigned char* owner_buf = (unsigned char*)alloca(owner_size+1);
    unsigned char* pgroup_buf = (unsigned char*)alloca(pgroup_size+1);
    PSECURITY_DESCRIPTOR psd = (PSECURITY_DESCRIPTOR)sd_buf;
    PACL pdacl = (PACL)dacl_buf;
    PACL psacl = (PACL)sacl_buf;
    PSID owner = (PSID)owner_buf;
    PSID pgroup = (PSID)pgroup_buf;

    rc = MakeAbsoluteSD(pisd, psd, &sd_size, pdacl, &dacl_size, psacl, &sacl_size, owner, &owner_size, pgroup, &pgroup_size);
    if(rc == 0)
    {
#ifdef _WIN32
        int error = GetLastError();
        throw MakeStringException(-1, "Error MakeAbsoluteSD - error code = %d", error);
#else
        throw MakeStringException(-1, "Error MakeAbsoluteSD");
#endif
    }

    bool done = false;
    while(!done)
    {
        ACL_SIZE_INFORMATION ASizeInfo;

        GetAclInformation(pdacl, &ASizeInfo, sizeof(ACL_SIZE_INFORMATION), AclSizeInformation);
        PACCESS_ALLOWED_ACE pAce;
        unsigned iAce;

        done = true;
        for(iAce = 0; iAce < ASizeInfo.AceCount; iAce++)
        {
            GetAce(pdacl, iAce, (void**)&pAce);
            PSID cursid = (PSID)&(pAce->SidStart);

            if(EqualSid(cursid, act_psid))
            {
                if(stricmp(action.m_action.str(), "add") == 0)
                    throw MakeStringException(-1, "Permission for account %s already exists", action.m_account_name.str());
                DeleteAce(pdacl, iAce);
                if(iAce < ASizeInfo.AceCount - 1)
                    done = false;
                break;
            }
        }
    }

    PACL pnewdacl = pdacl;

#ifdef _WIN32
    EXPLICIT_ACCESS uaccess_allows;
    EXPLICIT_ACCESS uaccess_denies;

    if(stricmp(action.m_action.str(), "delete") != 0 && action.m_allows != 0)
    {
        uaccess_allows.grfAccessMode = GRANT_ACCESS;
        uaccess_allows.grfAccessPermissions = newsec2ldap((NewSecAccessFlags)action.m_allows);
        uaccess_allows.grfInheritance = NO_INHERITANCE;
        uaccess_allows.Trustee.MultipleTrusteeOperation = NO_MULTIPLE_TRUSTEE;
        uaccess_allows.Trustee.pMultipleTrustee = NULL;
        if(action.m_account_type == GROUP_ACT)
            uaccess_allows.Trustee.TrusteeType =  TRUSTEE_IS_GROUP;
        else
            uaccess_allows.Trustee.TrusteeType =  TRUSTEE_IS_USER;
        uaccess_allows.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        uaccess_allows.Trustee.ptstrName = (char*)(act_psid);

        rc = SetEntriesInAcl(1, &uaccess_allows, pdacl, &pnewdacl);
        if(rc != ERROR_SUCCESS)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error SetEntriesInAcl - error code = %d", error);
        }
    }

    pdacl = pnewdacl;

    if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
    {
        uaccess_denies.grfAccessMode = DENY_ACCESS;
        uaccess_denies.grfAccessPermissions = newsec2ldap((NewSecAccessFlags)action.m_denies);
        uaccess_denies.grfInheritance = NO_INHERITANCE ;
        uaccess_denies.Trustee.MultipleTrusteeOperation = NO_MULTIPLE_TRUSTEE;
        uaccess_denies.Trustee.pMultipleTrustee = NULL;
        if(action.m_account_type == GROUP_ACT)
                uaccess_denies.Trustee.TrusteeType =  TRUSTEE_IS_GROUP;
        else
            uaccess_denies.Trustee.TrusteeType =  TRUSTEE_IS_USER;
        uaccess_denies.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        uaccess_denies.Trustee.ptstrName = (char*)(act_psid);

        rc = SetEntriesInAcl(1, &uaccess_denies, pdacl, &pnewdacl);
        if(rc != ERROR_SUCCESS)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error SetEntriesInAcl - error code = %d", error);
        }
    }
#else
    if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
    {
        DWORD newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sizeofSid(act_psid);
        DWORD new_dacl_size = dacl_size + newace_size;
        dacl_size = new_dacl_size;
        pnewdacl = (PACL)alloca(new_dacl_size);
        rc = AddAccessDeniedAce(pnewdacl, &new_dacl_size, pdacl, ACL_REVISION, newsec2ldap((NewSecAccessFlags)action.m_denies), act_psid);
        if(rc == 0)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error AddAccessAllowedAce - error code = %d", error);
        }       
    }

    pdacl = pnewdacl;
    
    if(stricmp(action.m_action.str(), "delete") != 0 && action.m_allows != 0)
    {
        DWORD newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sizeofSid(act_psid);
        DWORD new_dacl_size = dacl_size + newace_size;
        pnewdacl = (PACL)alloca(new_dacl_size);
        rc = AddAccessAllowedAce(pnewdacl, &new_dacl_size, pdacl, ACL_REVISION, newsec2ldap((NewSecAccessFlags)action.m_allows), act_psid);
        if(rc == 0)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error AddAccessAllowedAce - error code = %d", error);
        }       
    }
#endif
    
    rc = SetSecurityDescriptorDacl(psd, true, pnewdacl, false); 
    if(rc == 0)
    {
        int error = GetLastError();
        throw MakeStringException(-1, "Error SetSecurityDescriptorDacl - error code = %d", error);
    }

    CSecurityDescriptor* csd = new CSecurityDescriptor(action.m_rname.str());   
    DWORD sdlen = GetSecurityDescriptorLength(psd);
    void* sdbuf = alloca(sdlen);
    MakeSelfRelativeSD(psd, (PSECURITY_DESCRIPTOR)sdbuf, &sdlen);
    csd->setDescriptor(sdlen, (void*)sdbuf);

#ifdef _WIN32
    if(pnewdacl != NULL)
    {
        LocalFree(pnewdacl);
    }
#endif
    return csd;
}

CSecurityDescriptor* PermissionProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, SecPermissionType ptype)
{
    return createDefaultSD(user, resource->getName(), ptype);   
}

CSecurityDescriptor* PermissionProcessor::createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype)
{
    SECURITY_DESCRIPTOR sd;
    unsigned char aclBuffer[1024];
    PACL pacl = (PACL)aclBuffer;
    PSID psid;
    PSID au_psid;

    int rc;

    InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION);
    InitializeAcl(pacl, 1024, ACL_REVISION);

    if(ptype != PT_ADMINISTRATORS_ONLY)
    {
        MemoryBuffer umb, gmb;
        if(user && DEFAULT_OWNER_PERMISSION != SecAccess_None)
        {
            //Add SD for given user
            lookupSid(user->getName(), umb);
            psid = (PSID)(umb.toByteArray());
            if(psid != NULL)
            {
                rc = AddAccessAllowedAce(pacl, ACL_REVISION, sec2ldap(DEFAULT_OWNER_PERMISSION), psid);
                if(rc == 0)
                {
                    int error = GetLastError();
                    throw MakeStringException(-1, "Error AddAccessAllowedAce - error code = %d", error);
                }
            }
        }

        if(ptype != PT_ADMINISTRATORS_AND_USER  &&  DEFAULT_AUTHENTICATED_USERS_PERMISSION != SecAccess_None)
        {
            //Add SD for Authenticated users
            au_psid = (PSID)(authenticated_users_sid);
            unsigned permission = sec2ldap(DEFAULT_AUTHENTICATED_USERS_PERMISSION);
            rc = AddAccessAllowedAce(pacl, ACL_REVISION, permission, au_psid);
            if(rc == 0)
            {
                int error = GetLastError();
                throw MakeStringException(-1, "Error AddAccessAllowedAce - error code = %d", error);
            }
        }
    }

    SetSecurityDescriptorDacl(&sd, TRUE, pacl, FALSE);

    CSecurityDescriptor* csd = new CSecurityDescriptor(name);   
    DWORD sdlen = GetSecurityDescriptorLength(&sd);
    void* sdbuf = alloca(sdlen);

    MakeSelfRelativeSD(&sd, (PSECURITY_DESCRIPTOR)sdbuf, &sdlen);
    csd->setDescriptor(sdlen, (void*)sdbuf);
    return csd;
}

CSecurityDescriptor* PermissionProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd)
{
    const char* initial_buf = initial_sd.toByteArray();
    PSECURITY_DESCRIPTOR pisd = (PSECURITY_DESCRIPTOR)initial_buf;

    int rc = 0;

    DWORD sd_size = 0;
    DWORD dacl_size = 0;
    DWORD sacl_size = 0;
    DWORD owner_size = 0;
    DWORD pgroup_size = 0;
    
    MakeAbsoluteSD(pisd, NULL, &sd_size, NULL, &dacl_size, NULL, &sacl_size, NULL, &owner_size, NULL, &pgroup_size);

    unsigned char* sd_buf = (unsigned char*)alloca(sd_size+1);
    unsigned char* dacl_buf = (unsigned char*)alloca(dacl_size+1);
    unsigned char* sacl_buf = (unsigned char*)alloca(sacl_size+1);
    unsigned char* owner_buf = (unsigned char*)alloca(owner_size+1);
    unsigned char* pgroup_buf = (unsigned char*)alloca(pgroup_size+1);
    PSECURITY_DESCRIPTOR psd = (PSECURITY_DESCRIPTOR)sd_buf;
    PACL pdacl = (PACL)dacl_buf;
    PACL psacl = (PACL)sacl_buf;
    PSID owner = (PSID)owner_buf;
    PSID pgroup = (PSID)pgroup_buf;

    rc = MakeAbsoluteSD(pisd, psd, &sd_size, pdacl, &dacl_size, psacl, &sacl_size, owner, &owner_size, pgroup, &pgroup_size);
    if(rc == 0)
    {
#ifdef _WIN32
        int error = GetLastError();
        throw MakeStringException(-1, "Error MakeAbsoluteSD - error code = %d", error);
#else
        throw MakeStringException(-1, "Error MakeAbsoluteSD");
#endif

    }

    PSID user_psid;
    PACL pnewdacl = NULL;
    MemoryBuffer umb;
    if(user && DEFAULT_OWNER_PERMISSION != SecAccess_None)
    {
        lookupSid(user->getName(), umb);
        user_psid = (PSID)(umb.toByteArray());

#ifdef _WIN32
        EXPLICIT_ACCESS uaccess;
        uaccess.grfAccessMode = GRANT_ACCESS;
        uaccess.grfAccessPermissions = sec2ldap(DEFAULT_OWNER_PERMISSION);
        uaccess.grfInheritance = OBJECT_INHERIT_ACE;
        uaccess.Trustee.MultipleTrusteeOperation = NO_MULTIPLE_TRUSTEE;
        uaccess.Trustee.pMultipleTrustee = NULL;
        uaccess.Trustee.TrusteeType =  TRUSTEE_IS_USER;
        uaccess.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        uaccess.Trustee.ptstrName = (char*)(user_psid);

        rc = SetEntriesInAcl(1, &uaccess, pdacl, &pnewdacl);
        if(rc != ERROR_SUCCESS)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error SetEntriesInAcl - error code = %d", error);
        }
        rc = SetSecurityDescriptorDacl(psd, true, pnewdacl, false); 
#else
        DWORD newace_size = sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sizeofSid(user_psid);
        DWORD new_dacl_size = dacl_size + newace_size;
        pnewdacl = (PACL)alloca(new_dacl_size);
        rc = AddAccessAllowedAce(pnewdacl, &new_dacl_size, pdacl, ACL_REVISION, sec2ldap(DEFAULT_OWNER_PERMISSION), user_psid);
        if(rc == 0)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error AddAccessAllowedAce - error code = %d", error);
        }       
        rc = SetSecurityDescriptorDacl(psd, true, pnewdacl, false); 
#endif
        if(rc == 0)
        {
            int error = GetLastError();
            throw MakeStringException(-1, "Error SetSecurityDescriptorDacl - error code = %d", error);
        }
    }
    
    CSecurityDescriptor* csd = new CSecurityDescriptor(resource->getName());    
    DWORD sdlen = GetSecurityDescriptorLength(psd);
    void* sdbuf = alloca(sdlen);
    MakeSelfRelativeSD(psd, (PSECURITY_DESCRIPTOR)sdbuf, &sdlen);
    csd->setDescriptor(sdlen, (void*)sdbuf);

#ifdef _WIN32
    if(pnewdacl != NULL)
    {
        LocalFree(pnewdacl);
    }
#endif
    return csd;
}


CSecurityDescriptor::CSecurityDescriptor(const char* name)
{
    if(name == NULL || name[0] == '\0')
        throw MakeStringException(-1, "name can't be empty for CSecurityDescriptor");
    
    const char* resourcename = name;
    if(resourcename[0] == '/')
        resourcename = resourcename + 1;

    const char* eqsign = strchr(resourcename, '=');
    const char* slash = strrchr(resourcename, '/');
    if(eqsign != NULL)
    {
        const char* comma = strchr(eqsign+1, ',');
        if(comma != NULL)
        {
            m_name.set(eqsign+1, comma - eqsign - 1);
            m_relativeBasedn.set(comma + 1);
        }
        else
        {
            m_name.set(eqsign+1);
        }
        return;
    }
    else if(slash != NULL)
    {
        m_name.set(slash + 1);
        StringBuffer basednbuf;
        basednbuf.append("ou=");
        const char* ptr = slash - 1;
        StringBuffer oneou;
        while(ptr >= resourcename)
        {
            char c = *ptr;
            if(c == '/')
            {
                oneou.reverse();
                basednbuf.append(oneou.str()).append(",ou=");
                oneou.clear();
            }
            else
            {
                oneou.append(c);
            }
            ptr--;
        }
        oneou.reverse();
        basednbuf.append(oneou.str());
        m_relativeBasedn.set(basednbuf.str());
        return;
    }
    else
        m_name.set(resourcename);
}

const char* CSecurityDescriptor::getName()
{
    return m_name.get();
}

const char* CSecurityDescriptor::getRelativeBasedn()
{
    return m_relativeBasedn.get();
}

MemoryBuffer& CSecurityDescriptor::getDescriptor()
{
    return m_descriptor;
}

void CSecurityDescriptor::setDescriptor(unsigned len, void* buf)
{
    m_descriptor.clear().append(len, buf);
}

void CSecurityDescriptor::appendDescriptor(unsigned len, void* buf)
{
    m_descriptor.append(len, buf);

    // '\0' is the separator between descriptors. used only for aci.
    m_descriptor.append('\0');
}
