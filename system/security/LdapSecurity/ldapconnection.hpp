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

#ifndef __LDAPCONNECTION_HPP
#define __LDAPCONNECTION_HPP
#include <stdlib.h>
#include "thirdparty.h"
#include "jiface.hpp"
#include "jliball.hpp"
#include "seclib.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winldap.h>
#include <winber.h>
#include <rpc.h>
#include <rpcdce.h>
#include "dsgetdc.h"
#include <lm.h>
#else
#define LDAP_DEPRECATED 1
#include <stdio.h>
#include <string.h>
#include <ldap_cdefs.h>
#include <ldap.h>
#endif

#ifdef _WIN32
    #ifndef LDAPSECURITY_EXPORTS
        #define LDAPSECURITY_API __declspec(dllimport)
    #else
        #define LDAPSECURITY_API __declspec(dllexport)
    #endif//LDAPSECURITY_EXPORTS
#else
    #define LDAPSECURITY_API
#endif //_WIN32

#ifdef _WIN32
/*from Winldap.h
WINLDAPAPI ULONG LDAPAPI ldap_compare_ext_s(
        LDAP *ld,
        const PCHAR dn,
        const PCHAR Attr,
        const PCHAR Value,            // either value or Data is not null, not both
        struct berval   *Data,
        PLDAPControlA   *ServerControls,
        PLDAPControlA   *ClientControls
        );
*/
    #define LDAP_COMPARE_EXT_S(ld,dn,attr,bval,data,svrctrls,clientctrls) ldap_compare_ext_s(ld,(const PCHAR)dn,(const PCHAR)attr,(const PCHAR)bval,(struct berval *)data,svrctrls,clientctrls)
    #define LDAP_UNBIND(ld)     ldap_unbind(ld)
    #define LDAP_INIT(host,port) ldap_init((PCHAR)host, (ULONG)port);
#else
/* from openLDAP ldap.h
ldap_compare_ext_s LDAP_P((
    LDAP            *ld,
    LDAP_CONST char *dn,
    LDAP_CONST char *attr,
    struct berval   *bvalue,
    LDAPControl    **serverctrls,
    LDAPControl    **clientctrls ));
*/
    #define LDAP_COMPARE_EXT_S(ld,dn,attr,bval,svrctrls,clientctrls,msgnum) ldap_compare_ext_s(ld,(const char*)dn,(const char*)attr,(struct berval *)bval,svrctrls,clientctrls)
    #define LDAP_UNBIND(ld)     ldap_unbind_ext(ld,0,0)
    #define LDAP_INIT(ld,uri)   ldap_initialize(ld, uri);
#endif

#ifdef _WIN32
    typedef struct l_timeval TIMEVAL;
#else
    typedef struct timeval TIMEVAL;
#endif

#define LDAPTIMEOUT 60 //20 second connection/search timeout
#define DEFAULT_LDAP_POOL_SIZE 10

// 1 for ActiveDirectory, 2 for iPlanet, 3 for openLdap
enum LdapServerType
{
    LDAPSERVER_UNKNOWN = 0,
    ACTIVE_DIRECTORY = 1,
    IPLANET = 2,
    OPEN_LDAP = 3
};

enum ACT_TYPE
{
    USER_ACT = 0,
    GROUP_ACT = 1
};

enum UserField
{
    UFUserID = 0,
    UFName = 1,
    UFFullName = 2,
    UFPasswordExpiration = 3,
    UFterm = 4,
    UFreverse = 256,
    UFnocase = 512,
    UFnumeric = 1024
};

enum GroupField
{
    GFName = 0,
    GFManagedBy = 1,
    GFDesc = 2,
    GFterm = 3,
    GFreverse = 256,
    GFnocase = 512,
    GFnumeric = 1024
};

#define RF_NONE                     0x00
#define RF_RT_FILE_SCOPE_FILE       0x01
#define RF_RT_MODULE_NO_REPOSITORY  0x02

enum ResourceField
{
    RFName = 0,
    RFDesc = 1,
    RFterm = 2,
    RFreverse = 256,
    RFnocase = 512,
    RFnumeric = 1024
};

extern LDAPSECURITY_API  const char* getUserFieldNames(UserField feild);
extern LDAPSECURITY_API  const char* getGroupFieldNames(GroupField feild);
extern LDAPSECURITY_API  const char* getResourceFieldNames(ResourceField feild);

typedef IIteratorOf<IPropertyTree> ISecItemIterator;

interface IPermissionProcessor;

interface ILdapConnection : extends IInterface
{
    virtual LDAP* getLd() = 0;
};

interface ILdapConnectionPool : extends IInterface
{
    virtual ILdapConnection* getConnection() = 0;
    virtual ILdapConnection* getSSLConnection() = 0;
};

interface ILdapConfig : extends IInterface
{
    virtual LdapServerType getServerType() = 0;
    virtual const char * getCfgServerType() const = 0;
    virtual StringBuffer& getLdapHost(StringBuffer& hostbuf) = 0;
    virtual void markDown(const char* ldaphost) = 0;
    virtual int getLdapPort() = 0;
    virtual int getLdapSecurePort() = 0;
    virtual const char* getProtocol() = 0;
    virtual const char* getBasedn() = 0;
    virtual const char* getDomain() = 0;
    virtual const char* getAuthMethod() = 0;
    virtual const char* getUserBasedn() = 0;
    virtual const char* getGroupBasedn() = 0;
    virtual const char* getResourceBasedn(SecResourceType rtype) = 0;
    virtual const char* getTemplateName() = 0;
    virtual const char* getSysUser() = 0;
    virtual const char* getSysUserDn() = 0;
    virtual const char* getSysUserCommonName() = 0;
    virtual const char* getSysUserPassword() = 0;
    virtual const char* getSysUserBasedn() = 0;
    virtual bool sysuserSpecified() = 0;
    virtual int getMaxConnections() = 0;
    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype = RT_DEFAULT) = 0;
};


class CPermission : public CInterface, implements IInterface
{
    StringBuffer m_account_name;
    ACT_TYPE     m_account_type;
    int          m_allows;
    int          m_denies;

public:
    IMPLEMENT_IINTERFACE
    
    CPermission(const char* account_name, ACT_TYPE account_type, int allows, int denies)
    {
        m_account_name.append(account_name);
        m_account_type = account_type;
        m_allows = allows;
        m_denies = denies;
    }

    const char* getAccount_name() {return m_account_name.str();}
    ACT_TYPE getAccount_type() {return m_account_type;}
    int getAllows() {return m_allows;}
    int getDenies() {return m_denies;}
    void setAllows(int allows) { m_allows = allows;}
    void setDenies(int denies) { m_denies = denies;}
};

class CPermissionAction : public CInterface, implements IInterface
{
public:
    StringBuffer m_action;

    StringBuffer m_basedn;
    SecResourceType m_rtype;
    StringBuffer m_rname;
    
    StringBuffer m_account_name;
    ACT_TYPE     m_account_type;
    int          m_allows;
    int          m_denies;

    IMPLEMENT_IINTERFACE
};

interface ILdapClient : extends IInterface
{
    virtual void init(IPermissionProcessor* pp) = 0;
    virtual LdapServerType getServerType() = 0;
    virtual bool authenticate(ISecUser& user) = 0;
    virtual bool authorize(SecResourceType rtype, ISecUser&, IArrayOf<ISecResource>& resources, const char * resName = nullptr) = 0;
    virtual bool addResources(SecResourceType rtype, ISecUser& user, IArrayOf<ISecResource>& resources, SecPermissionType ptype, const char* basedn) = 0;
    virtual bool addUser(ISecUser& user) = 0;
    virtual void getGroups(const char *user, StringArray& groups) = 0;
    virtual bool getUserInfo(ISecUser& user, const char* infotype = NULL) = 0;
    virtual ISecUser* lookupUser(unsigned uid) = 0;
    virtual bool lookupAccount(MemoryBuffer& sidbuf, StringBuffer& account_name, ACT_TYPE& act_type) = 0;
    virtual void lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE act_type) = 0;
    virtual void setPermissionProcessor(IPermissionProcessor* pp) = 0;
    virtual bool retrieveUsers(IUserArray& users) = 0;
    virtual bool retrieveUsers(const char* searchstr, IUserArray& users) = 0;
    virtual IPropertyTreeIterator* getUserIterator(const char* userName) = 0;
    virtual ISecItemIterator* getUsersSorted(const char* userName, UserField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize,
        unsigned *total, __int64 *cachehint) = 0;
    virtual void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions, const char * baseDN = nullptr) = 0;
    virtual IPropertyTreeIterator* getGroupIterator() = 0;
    virtual ISecItemIterator* getGroupsSorted(GroupField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize,
        unsigned *total, __int64 *cachehint) = 0;
    virtual IPropertyTreeIterator* getGroupMemberIterator(const char* groupName) = 0;
    virtual ISecItemIterator* getGroupMembersSorted(const char* groupName, UserField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize,
        unsigned *total, __int64 *cachehint) = 0;
    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype = RT_DEFAULT) = 0;
    virtual ILdapConfig* getLdapConfig() = 0;
    virtual bool userInGroup(const char* userdn, const char* groupdn) = 0;
    virtual bool updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword = 0) = 0;
    virtual bool updateUser(const char* type, ISecUser& user) = 0;
    virtual bool updateUserPassword(const char* username, const char* newPassword) = 0;
    virtual bool getResources(SecResourceType rtype, const char * basedn, const char* prefix, IArrayOf<ISecResource>& resources) = 0;
    virtual bool getResourcesEx(SecResourceType rtype, const char * basedn, const char* prefix, const char* searchstr, IArrayOf<ISecResource>& resources) = 0;
    virtual IPropertyTreeIterator* getResourceIterator(SecResourceType rtype, const char * basedn, const char* prefix,
        const char* resourceName, unsigned extraNameFilter) = 0;
    virtual ISecItemIterator* getResourcesSorted(SecResourceType rtype, const char * basedn, const char* resourceName, unsigned extraNameFilter,
        ResourceField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned *total, __int64 *cachehint) = 0;
    virtual bool getPermissionsArray(const char* basedn, SecResourceType rtype, const char* name, IArrayOf<CPermission>& permissions) = 0;
    virtual bool changePermission(CPermissionAction& action) = 0;
    virtual void changeUserGroup(const char* action, const char* username, const char* groupname, const char * groupDN=nullptr) = 0;
    virtual bool deleteUser(ISecUser* user) = 0;
    virtual void addGroup(const char* groupname, const char * groupOwner, const char * groupDesc) = 0;
    virtual void deleteGroup(const char* groupname, const char * groupsDN=nullptr) = 0;
    virtual void getGroupMembers(const char* groupname, StringArray & users, const char * groupsDN=nullptr) = 0;
    virtual void deleteResource(SecResourceType rtype, const char* name, const char* basedn) = 0;
    virtual void renameResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn) = 0;
    virtual void copyResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn) = 0;
    virtual void normalizeDn(const char* dn, StringBuffer& ndn) = 0;
    virtual bool isSuperUser(ISecUser* user) = 0;
    virtual int countEntries(const char* basedn, const char* objectClass, int limit) = 0;
    virtual int countUsers(const char* searchstr, int limit) = 0;
    virtual int countResources(const char* basedn, const char* searchstr, int limit) = 0;
    virtual ILdapConfig* queryConfig() = 0;
    virtual const char* getPasswordStorageScheme() = 0;
    virtual bool createUserScope(ISecUser& user) = 0;
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes) = 0;
    virtual int queryDefaultPermission(ISecUser& user) = 0;

    //Data View related interfaces
    virtual void createView(const char * viewName, const char * viewDescription) = 0;
    virtual void deleteView(const char * viewName) = 0;
    virtual void queryAllViews(StringArray & viewNames, StringArray & viewDescriptions) = 0;

    virtual void addViewColumns(const char * viewName, StringArray & files, StringArray & columns) = 0;
    virtual void removeViewColumns(const char * viewName, StringArray & files, StringArray & columns) = 0;
    virtual void queryViewColumns(const char * viewName, StringArray & files, StringArray & columns) = 0;

    virtual void addViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups) = 0;
    virtual void removeViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups) = 0;
    virtual void queryViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups) = 0;
    virtual bool userInView(const char * user, const char* viewName) = 0;
};

ILdapClient* createLdapClient(IPropertyTree* cfg);

#ifdef _WIN32
extern LDAPSECURITY_API bool verifyServerCert(LDAP* ld, PCCERT_CONTEXT pServerCert);
#endif


//--------------------------------------------
// This helper class ensures memory allocated by
// calls to ldap_get_values_len gets freed
//--------------------------------------------
class CLDAPGetValuesLenWrapper
{
private:
    struct berval** bvalues;
    unsigned numValues;
public:
    CLDAPGetValuesLenWrapper()
    {
        bvalues = NULL;
        numValues = 0;
    }
    CLDAPGetValuesLenWrapper(LDAP *ld, LDAPMessage *msg, const char * attr)
    {
        bvalues = NULL;
        retrieveBValues(ld,msg,attr);
    }

    ~CLDAPGetValuesLenWrapper()
    {
        if (bvalues)
            ldap_value_free_len(bvalues);
    }
    inline bool hasValues()         { return bvalues != NULL  && *bvalues != NULL; }
    inline berval **queryBValues()  { return bvalues; }
    inline const char * queryCharValue(unsigned which){ return which < numValues ? (*(bvalues[which])).bv_val : NULL; }

    //Delayed call to ldap_get_values_len
    void retrieveBValues(LDAP *ld, LDAPMessage *msg, const char * attr)
    {
        if (bvalues)
            ldap_value_free_len(bvalues);
#ifdef _WIN32
        bvalues = ldap_get_values_len(ld, msg, (const PCHAR)attr);
#else
        bvalues = ldap_get_values_len(ld, msg, attr);
#endif
        for (numValues = 0; bvalues && bvalues[numValues]; numValues++);
    }
};


#endif

