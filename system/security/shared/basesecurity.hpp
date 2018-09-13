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

#ifndef BASESECURITY_INCL
#define BASESECURITY_INCL

#include <stdlib.h>
#include "seclib.hpp"
#include "jliball.hpp"


#include "SecureUser.hpp"
#include "SecurityResource.hpp"
#include "SecurityResourceList.hpp"

class CBaseSecurityManager : implements ISecManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE

    CBaseSecurityManager()
    {
    }


    CBaseSecurityManager(const char *serviceName, IPropertyTree *config)
    {
    }

    ~CBaseSecurityManager()
    {
    }


    //ISecManager
    ISecUser * createUser(const char * user_name)
    {
        return new CSecureUser(user_name, NULL);
    }

    ISecResourceList * createResourceList(const char * rlname)
    {
        return new CSecurityResourceList(rlname);
    }

    bool subscribe(ISecAuthenticEvents & events)
    {
        throwUnexpected();
    }

    bool unsubscribe(ISecAuthenticEvents & events)
    {
        throwUnexpected();
    }

    bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext)
    {
        throwUnexpected();
    }

    bool authorizeEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext)
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext)
    {
        throwUnexpected();
    }

    SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename)
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope)
    {
        throwUnexpected();
    }

    bool authorizeFileScope(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    bool authorizeViewScope(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    bool addResources(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    bool addResourcesEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, SecPermissionType ptype, const char * basedn)
    {
        throwUnexpected();
    }

    bool addResourceEx(SecResourceType rtype, ISecUser & user, const char * resourcename, SecPermissionType ptype, const char * basedn)
    {
        throwUnexpected();
    }

    bool getResources(SecResourceType rtype, const char * basedn, IResourceArray & resources)
    {
        throwUnexpected();
    }

    bool updateResources(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    bool updateSettings(ISecUser & user, ISecPropertyList * resources, IEspSecureContext* secureContext)
    {
        throwUnexpected();
    }

    bool addUser(ISecUser & user)
    {
        throwUnexpected();
    }

    ISecUser * findUser(const char * username)
    {
        throwUnexpected();
    }

    ISecUser * lookupUser(unsigned uid)
    {
        throwUnexpected();
    }

    ISecUserIterator * getAllUsers()
    {
        throwUnexpected();
    }

    void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions )
    {
        throwUnexpected();
    }

    bool updateUserPassword(ISecUser & user, const char * newPassword, const char* currPassword = 0)
    {
        throwUnexpected();
    }

    bool initUser(ISecUser & user)
    {
        throwUnexpected();
    }

    void setExtraParam(const char * name, const char * value)
    {
        throwUnexpected();
    }

    IAuthMap * createAuthMap(IPropertyTree * authconfig)
    {
        throwUnexpected();
    }

    IAuthMap * createFeatureMap(IPropertyTree * authconfig)
    {
        throwUnexpected();
    }

    IAuthMap * createSettingMap(IPropertyTree * authconfig)
    {
#ifdef _DEBUG
        DBGLOG("using CBaseSecurityManager::createSettingMap; override with desired behavior");
#endif
        return nullptr;
    }

    void deleteResource(SecResourceType rtype, const char * name, const char * basedn)
    {
        throwUnexpected();
    }

    void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        throwUnexpected();
    }

    void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        throwUnexpected();
    }

    void cacheSwitch(SecResourceType rtype, bool on)
    {
        throwUnexpected();
    }

    bool authTypeRequired(SecResourceType rtype)
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope)
    {
        throwUnexpected();
    }

    bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    const char * getDescription()
    {
        throwUnexpected();
    }

    unsigned getPasswordExpirationWarningDays()
    {
        throwUnexpected();
    }

    bool createUserScopes()
    {
        throwUnexpected();
    }

    aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes)
    {
        throwUnexpected();
    }

    SecAccessFlags queryDefaultPermission(ISecUser& user)
    {
        throwUnexpected();
    }

    bool clearPermissionsCache(ISecUser & user)
    {
        throwUnexpected();
    }

    bool authenticateUser(ISecUser & user, bool *superUser)
    {
        throwUnexpected();
    }

    secManagerType querySecMgrType()
    {
        throwUnexpected();
    }

    const char* querySecMgrTypeName()
    {
        throwUnexpected();
    }

    bool logoutUser(ISecUser & user)
    {
        throwUnexpected();
    }
};

#endif // BASESECURITY_INCL
//#endif
