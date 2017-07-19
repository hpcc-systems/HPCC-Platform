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

#ifndef __LDAPSECURITY_IPP_
#define __LDAPSECURITY_IPP_

#pragma warning(disable:4786)

#include "permissions.ipp"
#include "aci.ipp"
#include "caching.hpp"
#undef new
#include <map>
#include <string>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif
#include "seclib.hpp"

#ifndef LDAPSECURITY_EXPORTS
    #define LDAPSECURITY_API DECL_IMPORT
#else
    #define LDAPSECURITY_API DECL_EXPORT
#endif

class LDAPSECURITY_API CLdapSecUser : implements ISecUser, implements ISecCredentials, public CInterface
{
private:
    StringAttr   m_realm;
    StringAttr   m_name;
    StringAttr   m_fullname;
    StringAttr   m_firstname;
    StringAttr   m_lastname;
    StringAttr   m_pw;
    StringAttr   m_employeeID;
    StringAttr   m_distinguishedName;
    StringAttr   m_Fqdn;
    StringAttr   m_Peer;
    authStatus   m_authenticateStatus;
    CDateTime    m_passwordExpiration;//local time
    unsigned     m_userid;
    MemoryBuffer m_usersid;
    BufferArray  m_groupsids;
    
    bool         m_posixenabled;
    StringAttr   m_gidnumber;
    StringAttr   m_uidnumber;
    StringAttr   m_homedirectory;
    StringAttr   m_loginshell;

    bool         m_sudoersenabled;
    bool         m_insudoers;
    StringAttr   m_sudoHost;
    StringAttr   m_sudoCommand;
    StringAttr   m_sudoOption;
    MemoryBuffer m_sessionToken;//User's ESP session token
    MemoryBuffer m_signature;//User's digital signature

public:
    IMPLEMENT_IINTERFACE

    CLdapSecUser(const char *name, const char *pw);
    virtual ~CLdapSecUser();

//non-interfaced functions
    void setUserID(unsigned userid);
    void setUserSid(int sidlen, const char* sid);
    MemoryBuffer& getUserSid();
//interface ISecUser
    const char * getName();
    bool setName(const char * name);
    virtual const char * getFullName();
    virtual bool setFullName(const char * name);
    virtual const char * getFirstName();
    virtual bool setFirstName(const char * fname);
    virtual const char * getLastName();
    virtual bool setLastName(const char * lname);
    virtual const char * getEmployeeID();
    virtual bool setEmployeeID(const char * emplID);
    virtual const char * getDistinguishedName();
    virtual bool setDistinguishedName(const char * dn);
    const char * getRealm();
    bool setRealm(const char * name);
    ISecCredentials & credentials();
    virtual unsigned getUserID();
    virtual void copyTo(ISecUser& source);

    virtual const char * getFqdn();
    virtual bool setFqdn(const char * Fqdn);
    virtual const char *getPeer();
    virtual bool setPeer(const char *Peer);


    virtual SecUserStatus getStatus(){return SecUserStatus_Unknown;}
    virtual bool setStatus(SecUserStatus Status){return false;}


   virtual CDateTime& getPasswordExpiration(CDateTime& expirationDate)
   {
       expirationDate.set(m_passwordExpiration);
       return expirationDate;
   }
   virtual bool setPasswordExpiration(CDateTime& expirationDate)
   {
       m_passwordExpiration.set(expirationDate);
       return true;
   }
   virtual int getPasswordDaysRemaining()
   {
       if (m_passwordExpiration.isNull())
           return scPasswordNeverExpires;//-2 if never expires
       CDateTime expiry(m_passwordExpiration);
       CDateTime now;
       now.setNow();
       now.adjustTime(now.queryUtcToLocalDelta());
       if (expiry <= now)
           return scPasswordExpired;//-1 if already expired
       expiry.setTime(0,0,0,0);
       now.setTime(23,59,59);
       int numDays = 0;
       while (expiry > now)
       {
           ++numDays;
           now.adjustTime(24*60);
       }
       return numDays;
   }

   authStatus getAuthenticateStatus()           { return m_authenticateStatus; }
   void setAuthenticateStatus(authStatus status){ m_authenticateStatus = status; }

   ISecUser * clone();
    virtual void setProperty(const char* name, const char* value){}
    virtual const char* getProperty(const char* name){ return "";}

    virtual void setPropertyInt(const char* name, int value){}
    virtual int getPropertyInt(const char* name){ return 0;}


//interface ISecCredentials
    bool setPassword(const char * pw);
    const char* getPassword();
    bool setEncodedPassword(SecPasswordEncoding enc, void * pw, unsigned length, void * salt, unsigned saltlen);
    void setSessionToken(const MemoryBuffer * const token);
    const MemoryBuffer & getSessionToken();
    void setSignature(const MemoryBuffer * const signature);
    const MemoryBuffer & getSignature();

// Posix specific fields
    virtual void setGidnumber(const char* gidnumber)
    {
        m_gidnumber.set(gidnumber);
    }
    virtual const char* getGidnumber()
    {
        return m_gidnumber.get();
    }
    virtual void setUidnumber(const char* uidnumber)
    {
        m_uidnumber.set(uidnumber);
    }
    virtual const char* getUidnumber()
    {
        return m_uidnumber.get();
    }
    virtual void setHomedirectory(const char* homedir)
    {
        m_homedirectory.set(homedir);
    }
    virtual const char* getHomedirectory()
    {
        return m_homedirectory.get();
    }
    virtual void setLoginshell(const char* loginshell)
    {
        m_loginshell.set(loginshell);
    }
    virtual const char* getLoginshell()
    {
        return m_loginshell.get();
    }
    virtual void setPosixenabled(bool enabled)
    {
        m_posixenabled = enabled;
    }
    virtual bool getPosixenabled()
    {
        return m_posixenabled;
    }

// Sudoers specific fields  
    virtual void setSudoersEnabled(bool enabled)
    {
        m_sudoersenabled = enabled;
    }
    virtual bool getSudoersEnabled()
    {
        return m_sudoersenabled;
    }
    virtual void setInSudoers(bool in)
    {
        m_insudoers = in;
    }
    virtual bool getInSudoers()
    {
        return m_insudoers;
    }
    virtual void setSudoHost(const char* host)
    {
        m_sudoHost.set(host);
    }
    virtual const char* getSudoHost()
    {
        return m_sudoHost.get();
    }
    virtual void setSudoCommand(const char* cmd)
    {
         m_sudoCommand.set(cmd);
    }
    virtual const char* getSudoCommand()
    {
        return m_sudoCommand.get();
    }
    virtual void setSudoOption(const char* option)
    {
        m_sudoOption.set(option);
    }
    virtual const char* getSudoOption()
    {
        return m_sudoOption.get();
    }
};


class CLdapSecResource : implements ISecResource, public CInterface
{
private:
    StringAttr         m_name;
    SecAccessFlags     m_access;
    SecAccessFlags     m_required_access;
    Owned<IProperties> m_parameters;
    StringBuffer       m_description;
    StringBuffer       m_value;
    SecResourceType    m_resourcetype;

public: 
    IMPLEMENT_IINTERFACE

    CLdapSecResource(const char *name);
    void addAccess(SecAccessFlags flags);
    void setAccessFlags(SecAccessFlags flags);
    virtual void setRequiredAccessFlags(SecAccessFlags flags);
    virtual SecAccessFlags getRequiredAccessFlags();
//interface ISecResource : extends IInterface
    virtual const char * getName();
    virtual SecAccessFlags getAccessFlags();
    virtual int addParameter(const char* name, const char* value);
    virtual const char * getParameter(const char * name);
    virtual void setDescription(const char* description);
    virtual const char* getDescription();

    virtual void setValue(const char* value);
    virtual const char* getValue();

    virtual ISecResource * clone();
    virtual SecResourceType getResourceType();
    virtual void setResourceType(SecResourceType resourcetype);
    virtual void copy(ISecResource* from);
    virtual StringBuffer& toString(StringBuffer& s)
    {
        s.appendf("%s: %s (value: %s, rqr'ed access: %d, type: %s)", m_name.get(), m_description.str(),  
                    m_value.str(), m_required_access, resTypeDesc(m_resourcetype)); 
        return s;
    }
};


class CLdapSecResourceList : implements ISecResourceList, public CInterface
{
private:
    bool m_complete;
    StringAttr m_name;
    IArrayOf<ISecResource> m_rlist;
    std::map<std::string, ISecResource*> m_rmap;  

public:
    IMPLEMENT_IINTERFACE

    CLdapSecResourceList(const char *name);

    void setAuthorizationComplete(bool value);
    IArrayOf<ISecResource>& getResourceList();

//interface ISecResourceList : extends IInterface
    bool isAuthorizationComplete();

    virtual ISecResourceList * clone();
    virtual bool copyTo(ISecResourceList& destination);

    void clear();


    ISecResource* addResource(const char * name);
    virtual void addResource(ISecResource * resource);
    bool addCustomResource(const char * name, const char * config);
    ISecResource * getResource(const char * Resource);
    virtual int count();
    virtual const char* getName();
    virtual ISecResource * queryResource(unsigned seq);
    virtual ISecPropertyIterator * getPropertyItr();
    virtual ISecProperty* findProperty(const char* name);
    virtual StringBuffer& toString(StringBuffer& s) 
    { 
        s.appendf("name=%s, count=%d.", m_name.get(), count()); 
        for (int i=0; i<count(); i++) 
        { 
            s.appendf("\nItem %d: ",i+1); 
            queryResource(i)->toString(s); 
        } 
        return s;
    }
};

class LDAPSECURITY_API CLdapSecManager : implements ISecManager, public CInterface
{
private:
    Owned<ILdapClient> m_ldap_client;
    Owned<IPermissionProcessor> m_pp;
    Owned<IPropertyTree> m_cfg;
    Owned<ISecAuthenticEvents> m_subscriber;
    StringBuffer m_server;
    void init(const char *serviceName, IPropertyTree* cfg);
    IUserArray m_user_array;
    Monitor m_monitor;
    Owned<IProperties> m_extraparams;
    Owned<CPermissionsCache> m_permissionsCache;
    bool m_cache_off[RT_SCOPE_MAX];
    bool m_usercache_off;
    bool authenticate(ISecUser* user);
    StringBuffer m_description;
    unsigned m_passwordExpirationWarningDays;
    bool m_checkViewPermissions;

public:
    IMPLEMENT_IINTERFACE

    CLdapSecManager(const char *serviceName, const char *config);
    CLdapSecManager(const char *serviceName, IPropertyTree &config);
    virtual ~CLdapSecManager();

//interface ISecManager : extends IInterface
    ISecUser * createUser(const char * user_name);
    ISecResourceList * createResourceList(const char * rlname);
    bool subscribe(ISecAuthenticEvents & events);
    bool unsubscribe(ISecAuthenticEvents & events);
    bool authorize(ISecUser& sec_user, ISecResourceList * Resources, IEspSecureContext* secureContext);
    bool authorizeEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * Resources, IEspSecureContext* secureContext = NULL);
    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename, IEspSecureContext* secureContext = NULL);
    virtual SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope);
    virtual bool authorizeFileScope(ISecUser & user, ISecResourceList * resources);
    virtual SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * wuscope);
    virtual bool authorizeViewScope(ISecUser & user, StringArray & filenames, StringArray & columnnames);
    virtual bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources);
    virtual bool addResources(ISecUser& sec_user, ISecResourceList * resources);
    virtual SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename);
    virtual bool addResourcesEx(SecResourceType rtype, ISecUser &user, ISecResourceList* resources, SecPermissionType ptype = PT_ADMINISTRATORS_ONLY, const char* basedn = NULL);
    virtual bool addResourceEx(SecResourceType rtype, ISecUser& user, const char* resourcename, SecPermissionType ptype = PT_ADMINISTRATORS_ONLY, const char* basedn = NULL);
    virtual bool updateResources(ISecUser& sec_user, ISecResourceList * resources){return false;}
    virtual bool addUser(ISecUser & user);
    virtual ISecUser * lookupUser(unsigned uid);
    virtual ISecUser * findUser(const char * username);
    virtual ISecUserIterator * getAllUsers();
    virtual void searchUsers(const char* searchstr, IUserArray& users);
    virtual ISecItemIterator* getUsersSorted(const char* userName, UserField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned* total, __int64* cacheHint);
    virtual void getAllUsers(IUserArray& users);
    virtual void setExtraParam(const char * name, const char * value);
    virtual IAuthMap * createAuthMap(IPropertyTree * authconfig);
    virtual IAuthMap * createFeatureMap(IPropertyTree * authconfig);
    virtual IAuthMap * createSettingMap(struct IPropertyTree *){return 0;}
    virtual bool updateSettings(ISecUser & User,ISecPropertyList * settings, IEspSecureContext* secureContext){return false;}
    virtual bool updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword = 0);
    virtual bool updateUser(const char* type, ISecUser& user);
    virtual bool updateUserPassword(const char* username, const char* newPassword);
    virtual bool initUser(ISecUser& user){return false;}

    virtual bool getResources(SecResourceType rtype, const char * basedn, IArrayOf<ISecResource>& resources);
    virtual bool getResourcesEx(SecResourceType rtype, const char * basedn, const char * searchstr, IArrayOf<ISecResource>& resources);
    virtual ISecItemIterator* getResourcesSorted(SecResourceType rtype, const char* basedn, const char* resourceName, unsigned extraNameFilter,
        ResourceField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned* total, __int64* cacheHint);
    virtual void cacheSwitch(SecResourceType rtype, bool on);

    virtual bool getPermissionsArray(const char* basedn, SecResourceType rtype, const char* name, IArrayOf<CPermission>& permissions);
    virtual void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions);
    virtual ISecItemIterator* getGroupsSorted(GroupField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned* total, __int64* cacheHint);
    virtual ISecItemIterator* getGroupMembersSorted(const char* groupName, UserField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned* total, __int64* cacheHint);
    virtual void getGroups(const char* username, StringArray & groups);
    virtual bool changePermission(CPermissionAction& action);
    virtual void changeUserGroup(const char* action, const char* username, const char* groupname);
    virtual bool deleteUser(ISecUser* user);
    virtual void addGroup(const char* groupname, const char * groupOwner, const char * groupDesc);
    virtual void deleteGroup(const char* groupname);
    virtual void getGroupMembers(const char* groupname, StringArray & users);
    virtual void deleteResource(SecResourceType rtype, const char * name, const char * basedn);
    virtual void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn);
    virtual void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn);

    virtual bool authorizeEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * Resources, bool doAuthentication);
    virtual SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename, bool doAuthentication);

    virtual void normalizeDn(const char* dn, StringBuffer& ndn);
    virtual bool isSuperUser(ISecUser* user);
    virtual ILdapConfig* queryConfig();

    virtual int countResources(const char* basedn, const char* searchstr, int limit);
    virtual int countUsers(const char* searchstr, int limit);
    virtual bool authTypeRequired(SecResourceType rtype) {return true;};

    virtual bool getUserInfo(ISecUser& user, const char* infotype = NULL);
    
    virtual LdapServerType getLdapServerType() 
    { 
        if(m_ldap_client) 
            return m_ldap_client->getServerType();
        else
            return ACTIVE_DIRECTORY;
    }

    virtual const char* getPasswordStorageScheme()
    {
        if(m_ldap_client) 
            return m_ldap_client->getPasswordStorageScheme();
        else
            return NULL;
    }
        
    virtual const char* getDescription()
    {
        return m_description.str();
    }

    virtual unsigned getPasswordExpirationWarningDays()
    {
        return m_passwordExpirationWarningDays;
    }

    virtual bool getCheckViewPermissions()
    {
        return m_checkViewPermissions;
    }

    virtual bool createUserScopes();
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes);
    virtual SecAccessFlags queryDefaultPermission(ISecUser& user);
    virtual bool clearPermissionsCache(ISecUser &user);
    virtual bool authenticateUser(ISecUser & user, bool * superUser);
    virtual secManagerType querySecMgrType() { return SMT_LDAP; }
    inline virtual const char* querySecMgrTypeName() { return "LdapSecurity"; }

    //Data View related interfaces
    virtual void createView(const char * viewName, const char * viewDescription);
    virtual void deleteView(const char * viewName);
    virtual void queryAllViews(StringArray & viewNames, StringArray & viewDescriptions, StringArray & viewManagedBy);

    virtual void addViewColumns(const char * viewName, StringArray & files, StringArray & columns);
    virtual void removeViewColumns(const char * viewName, StringArray & files, StringArray & columns);
    virtual void queryViewColumns(const char * viewName, StringArray & files, StringArray & columns);

    virtual void addViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups);
    virtual void removeViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups);
    virtual void queryViewMembers(const char * viewName, StringArray & viewUsers, StringArray & viewGroups);
    virtual bool userInView(const char * user, const char* viewName);
};

#endif
