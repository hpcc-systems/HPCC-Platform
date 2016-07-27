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

class CBaseSecurityManager : public CInterface,
    implements ISecManager
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
        UNIMPLEMENTED;
        return false;
    }

    bool unsubscribe(ISecAuthenticEvents & events)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool authorizeEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext)
    {
        UNIMPLEMENTED;
        return false;
    }

    int authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext)
    {
        UNIMPLEMENTED;
        return 0;
    }

    int getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename)
    {
        UNIMPLEMENTED;
        return 0;
    }

    int authorizeFileScope(ISecUser & user, const char * filescope)
    {
        UNIMPLEMENTED;
        return 0;
    }

    bool authorizeFileScope(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool authorizeViewScope(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool addResources(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool addResourcesEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, SecPermissionType ptype, const char * basedn)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool addResourceEx(SecResourceType rtype, ISecUser & user, const char * resourcename, SecPermissionType ptype, const char * basedn)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool getResources(SecResourceType rtype, const char * basedn, IResourceArray & resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool updateResources(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool updateSettings(ISecUser & user, ISecPropertyList * resources, IEspSecureContext* secureContext)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool addUser(ISecUser & user)
    {
        UNIMPLEMENTED;
        return false;
    }

    ISecUser * findUser(const char * username)
    {
        UNIMPLEMENTED;
        return NULL;
    }

    ISecUser * lookupUser(unsigned uid)
    {
        UNIMPLEMENTED;
        return NULL;
    }

    ISecUserIterator * getAllUsers()
    {
        UNIMPLEMENTED;
        return NULL;
    }

    void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions )
    {
        UNIMPLEMENTED;
    }

    bool updateUserPassword(ISecUser & user, const char * newPassword, const char* currPassword = 0)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool initUser(ISecUser & user)
    {
        UNIMPLEMENTED;
        return false;
    }

    void setExtraParam(const char * name, const char * value)
    {
        UNIMPLEMENTED;
    }

    IAuthMap * createAuthMap(IPropertyTree * authconfig)
    {
        UNIMPLEMENTED;
        return NULL;
    }

    IAuthMap * createFeatureMap(IPropertyTree * authconfig)
    {
        UNIMPLEMENTED;
        return NULL;
    }

    IAuthMap * createSettingMap(IPropertyTree * authconfig)
    {
        UNIMPLEMENTED;
        return NULL;
    }

    void deleteResource(SecResourceType rtype, const char * name, const char * basedn)
    {
        UNIMPLEMENTED;
    }

    void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        UNIMPLEMENTED;
    }

    void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
    {
        UNIMPLEMENTED;
    }

    void cacheSwitch(SecResourceType rtype, bool on)
    {
        UNIMPLEMENTED;
    }

    bool authTypeRequired(SecResourceType rtype)
    {
        UNIMPLEMENTED;
        return false;
    }

    int authorizeWorkunitScope(ISecUser & user, const char * filescope)
    {
        UNIMPLEMENTED;
        return 0;
    }

    bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources)
    {
        UNIMPLEMENTED;
        return false;
    }

    const char * getDescription()
    {
        UNIMPLEMENTED;
        return NULL;
    }

    unsigned getPasswordExpirationWarningDays()
    {
        UNIMPLEMENTED;
        return 0;
    }

    bool createUserScopes()
    {
        UNIMPLEMENTED;
        return false;
    }

    aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes)
    {
        UNIMPLEMENTED;
        return 0;
    }

    int queryDefaultPermission(ISecUser& user)
    {
        UNIMPLEMENTED;
        return 0;
    }

    bool clearPermissionsCache(ISecUser & user)
    {
        UNIMPLEMENTED;
        return false;
    }

    bool authenticateUser(ISecUser & user, bool &superUser)
    {
        UNIMPLEMENTED;
        return false;
    }

    secManagerType querySecMgrType()
    {
        UNIMPLEMENTED;
        return (secManagerType)0;
    }

    const char* querySecMgrTypeName()
    {
        UNIMPLEMENTED;
        return NULL;
    }

};

#endif // BASESECURITY_INCL
//#endif
