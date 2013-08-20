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

#ifndef BASESECURITY_INCL
#define BASESECURITY_INCL

#pragma warning (disable : 4786)
#pragma warning (disable : 4018)

#include <stdlib.h>
#include "seclib.hpp"
#include "jliball.hpp"


#include "SecureUser.hpp"
#include "SecurityResource.hpp"
// to avoid warning about macro max/min
#undef max
#undef min
#include "caching.hpp"
#include "SecurityResourceList.hpp"

#include <map>
#include <string>


typedef MapStringTo<int> MapStrToInt;

const char* const def_ExpirationDate = "ExpirationDate";

typedef std::map<std::string, CSecurityResourceList* > MapStrToResList;

typedef std::map<std::string, bool> IPList;

class UserInfo : public CInterface
{
private:
    ISecUser* _UserInfo;
    MapStrToResList _resList;
    unsigned _timeCreated;
    unsigned _timeOut;

public:
    IMPLEMENT_IINTERFACE;

    UserInfo(ISecUser& userInfo, unsigned TimeoutPeriod=60000)
    {
        _UserInfo = &userInfo;
        if(_UserInfo)
            _UserInfo->Link();
        _timeCreated = msTick();
        _timeOut = TimeoutPeriod; 
    }

    virtual ~UserInfo()
    {
        MapStrToResList::iterator pos;
        for(pos=_resList.begin();pos!=_resList.end();){
            pos->second->Release();
            pos++;
        }
        if(_UserInfo)
            _UserInfo->Release();
    }

    ISecResourceList* queryList(const char* ListName)
    {
        return (ISecResourceList*)_resList[ListName];
    }

    void appendList(const char* ListName, ISecResourceList* list)
    {
        if(list && ListName)
            _resList[ListName] = (CSecurityResourceList*)list;
    }

    virtual bool IsValid()
    {
        return (msTick() > (_timeOut + _timeCreated)) ? false : true;
    }

    void CopyTo(ISecUser & sec_user)
    {
        if(_UserInfo)
            _UserInfo->copyTo(sec_user);
    }
};


typedef std::map<std::string, UserInfo*> MapStrToUsers;


class CBaseSecurityManager : public CInterface,
    implements ISecManager
{
private:
    Owned<ISecAuthenticEvents> m_subscriber;
    StringBuffer               m_dbserver;
    StringBuffer               m_dbuser;
    StringBuffer               m_dbpassword;
    int                        m_poolsize;
    SecPasswordEncoding        m_dbpasswordEncoding;
    MapStrToInt                m_usermap;
    Mutex                      m_usermap_mutex;
    MapStrToUsers              m_userList;  
    Owned<IProperties>         m_extraparams;
    CPermissionsCache          m_permissionsCache;
    unsigned                   m_passwordExpirationWarningDays;

protected:
    CriticalSection             crit;
    Owned<IPropertyTree>        m_config;
    CriticalSection             _cache_Section;
    IPList                      m_safeIPList;
    bool                        m_enableIPRoaming;
    bool                        m_enableOTP;
public:
    IMPLEMENT_IINTERFACE

    CBaseSecurityManager(const char *serviceName, const char *config);
    CBaseSecurityManager(const char *serviceName, IPropertyTree *config);
    virtual ~CBaseSecurityManager();

//interface ISecManager : extends IInterface
    ISecUser * createUser(const char * user_name);
    ISecResourceList * createResourceList(const char * rlname);
    bool subscribe(ISecAuthenticEvents & events);
    bool unsubscribe(ISecAuthenticEvents & events);
    bool virtual authorize(ISecUser & sec_user, ISecResourceList * Resources);
    bool authorizeEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * Resources)
    {
        return authorize(sec_user, Resources);
    }
    int authorizeEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename)
    {
        if(!resourcename || !*resourcename)
            return SecAccess_Full;

        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("resources"));
        rlist->addResource(resourcename);
        
        bool ok = authorizeEx(rtype, sec_user, rlist.get());
        if(ok)
            return rlist->queryResource(0)->getAccessFlags();
        else
            return -1;
    }   
    virtual int getAccessFlagsEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename)
    {
        UNIMPLEMENTED;
    }
    virtual int authorizeFileScope(ISecUser & user, const char * filescope)
    {
        UNIMPLEMENTED;
    }
    virtual bool authorizeFileScope(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
    }
    virtual int authorizeWorkunitScope(ISecUser & user, const char * filescope)
    {
        UNIMPLEMENTED;
    }
    virtual bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
    }
    virtual bool ValidateSourceIP(ISecUser & user,IPList& SafeIPList)
    {
        return true;
    }
    bool addResourcesEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * resources, SecPermissionType ptype = PT_DEFAULT, const char* basedn=NULL)
    {
        return addResources(sec_user, resources);
    }
    bool addResourceEx(SecResourceType rtype, ISecUser& user, const char* resourcename, SecPermissionType ptype = PT_DEFAULT, const char* basedn=NULL)
    {
        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("resources"));
        rlist->addResource(resourcename);

        return addResourcesEx(rtype, user, rlist.get(), ptype, basedn);
    }

    bool addResources(ISecUser & user, ISecResourceList * resources);
    bool updateResources(ISecUser & user, ISecResourceList * resources);
    


    virtual bool updateSettings(ISecUser &user, ISecPropertyList* resources) ;
    

    virtual bool getResources(SecResourceType rtype, const char * basedn, IArrayOf<ISecResource> & resources)
    {
        UNIMPLEMENTED;
    }
    virtual bool addUser(ISecUser & user);
    virtual ISecUser * lookupUser(unsigned uid)
    {
        return NULL;
    }
    virtual ISecUser * findUser(const char * username)
    {
        return NULL;
    }

    virtual bool initUser(ISecUser& user)
    {
        return false;
    }

    virtual ISecUserIterator * getAllUsers()
    {
        return NULL;
    }

    virtual void setExtraParam(const char * name, const char * value)
    {
        if(name == NULL || name[0] == '\0')
            return;

        if (!m_extraparams)
            m_extraparams.setown(createProperties(false));
        m_extraparams->setProp(name, value);
    }
    virtual IAuthMap * createAuthMap(IPropertyTree * authconfig) {return NULL;}
    virtual IAuthMap * createFeatureMap(IPropertyTree * authconfig) {return NULL;}
    virtual IAuthMap * createSettingMap(IPropertyTree * authconfig) {return NULL;}

    virtual bool updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword = NULL);
    virtual bool IsPasswordExpired(ISecUser& user){return false;}
    void getAllGroups(StringArray & groups) { UNIMPLEMENTED;}
    
    virtual void deleteResource(SecResourceType rtype, const char * name, const char * basedn)
    {
        UNIMPLEMENTED;
    }
    virtual void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        //UNIMPLEMENTED;
    }
    virtual void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        UNIMPLEMENTED;
    }
    virtual void cacheSwitch(SecResourceType rtype, bool on)
    {
        UNIMPLEMENTED;
    }

    virtual SecUserStatus getUserStatus(const char* StatusFlag)
    {
        UNIMPLEMENTED;
    }

    virtual SecPasswordEncoding getPasswordEncoding()
    {
        return m_dbpasswordEncoding;
    }
    virtual bool authTypeRequired(SecResourceType rtype) {return false;};

    virtual const char* getDescription()
    {
        return NULL;
    }

    virtual unsigned getPasswordExpirationWarningDays()
    {
        return m_passwordExpirationWarningDays;
    }

    virtual bool createUserScopes() {UNIMPLEMENTED; return false;}
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes) {UNIMPLEMENTED; }
    virtual int queryDefaultPermission(ISecUser& user) {UNIMPLEMENTED; }
    virtual bool clearPermissionsCache(ISecUser& user) {return false;}
    virtual bool authenticateUser(ISecUser & user, bool &superUser) {return false;}
protected:
    const char* getServer(){return m_dbserver.toCharArray();}
    const char* getUser(){return m_dbuser.toCharArray();}
    const char* getPassword(){return m_dbpassword.toCharArray();}
    int getPoolsize() { return m_poolsize;}
    void setUserMap(const char* user,int uid){synchronized block(m_usermap_mutex); m_usermap.setValue(user, uid);}
    int getUserID(ISecUser& user);

    void logon_failed(const char* user, const char* msg);
    int findUser(const char* user,const char* realm);
    void init(const char *serviceName, IPropertyTree *config);

    void EncodePassword(StringBuffer& password);
    bool ValidateResources(ISecUser & sec_user,IArrayOf<ISecResource>& rlist);
    bool updateSettings(ISecUser & sec_user,IArrayOf<ISecResource>& rlist);

    virtual bool ValidateUser(ISecUser & sec_user);
    virtual bool ValidateResources(ISecUser & sec_user, ISecResourceList * Resources);

    virtual StringBuffer& buildAuthenticateQuery(const char* user,const char* password,const char* realm, StringBuffer& SQLQuery){return SQLQuery;}

    virtual bool dbValidateResource(ISecResource& res,int usernum,const char* realm)
    {
        return false;
    }
    virtual bool dbValidateSetting(ISecResource& res,int usernum,const char* realm)
    {
        return false;
    }
    virtual bool dbValidateSetting(ISecResource& res,ISecUser& User)
    {
        return false;
    }

    virtual bool dbUpdateResource(ISecResource& res,int usernum,const char* Realm)
    {
        return false;
    }

    virtual StringBuffer& ExecuteScalar(const char* Query,const char* FieldName, StringBuffer & ReturnVal){return ReturnVal;}


    virtual bool dbauthenticate(StringBuffer &user, StringBuffer &password,StringBuffer &realm,StringBuffer& status,int& ExpirationDate)
    {
        return false;
    }

    virtual bool dbauthenticate(ISecUser& User, StringBuffer& SQLQuery)
    {
        return false;
    }

    virtual int dbLookupUser(const char* user,const char* realm)
    {
        return 0;
    }
    virtual bool dbUpdatePasswrd(const char* user,const char* realm,const char* password)
    {
        return false;
    }
    virtual StringBuffer& dbGetEffectiveAccess(int usernum, const char * resource, const char * member, const char * objclass,StringBuffer& returnValue)
    {
        returnValue.appendf("%d",-1);
        return returnValue;
    }
    bool IsIPRestricted(ISecUser& User);
    virtual bool IsPasswordValid(ISecUser& sec_user);

    virtual void dbConnect()
    {
    }
    virtual void dbDisconnect()
    {
    }
    virtual bool inHierarchy(const char* username)
    {
        return false;
    }
    virtual bool inHierarchy(int usernum)
    {
        return false;
    }

    virtual bool validateSecurityQuestion(ISecUser* user, const char* token)
    {
        return false;
    }
    virtual int validateOTP(ISecUser* user, const char* cookie)
    {
        return 0;
    }
};

#endif // BASESECURITY_INCL
//#endif
