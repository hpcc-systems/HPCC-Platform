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
#include "authmap.ipp"

#include "SecureUser.hpp"
#include "SecurityResource.hpp"
#include "SecurityResourceList.hpp"

class CBaseSecurityManager : implements ISecManager, public CInterface
{
    static const SecFeatureSet s_implementedFeatureMask = SMF_CreateUser | SMF_CreateResourceList;
    static const SecFeatureSet s_safeFeatureMask = s_implementedFeatureMask | SMF_CreateSettingMap;

protected:
    void createAuthMapImpl(IAuthMap *authMap, const char *name, bool setAccessFlag, SecAccessFlags accessFlag,
        IPropertyTree *authconfig, IEspSecureContext *secureContext)
    {
        Owned<IPropertyTreeIterator> iter = authconfig->getElements(".//Location");
        ForEach(*iter)
        {
            IPropertyTree& location = iter->query();
            StringBuffer pathStr, rstr, required, description;
            location.getProp("@path", pathStr);
            location.getProp("@resource", rstr);
            location.getProp("@required", required);
            location.getProp("@description", description);

            if (pathStr.length() == 0)
                throw makeStringException(-1, "path empty in Authenticate/Location");
            if (rstr.length() == 0)
                throw makeStringException(-1, "resource empty in Authenticate/Location");

            ISecResourceList *rlist = authMap->queryResourceList(pathStr);
            if (rlist == nullptr)
            {
                rlist = createResourceList(name, secureContext);
                authMap->add(pathStr, rlist);
            }
            ISecResource *rs = rlist->addResource(rstr);
            SecAccessFlags requiredaccess = str2perm(required);
            rs->setRequiredAccessFlags(requiredaccess);
            rs->setDescription(description);
            if (setAccessFlag)
                rs->setAccessFlags(accessFlag);//grant an access to authenticated users
        }
    }

    void createFeatureMapImpl(IAuthMap *featureMap, bool setAccessFlag, SecAccessFlags accessFlag,
        IPropertyTree *authconfig, IEspSecureContext *secureContext)
    {
        Owned<IPropertyTreeIterator> iter = authconfig->getElements(".//Feature");
        ForEach(*iter)
        {
            IPropertyTree& feature = iter->query();
            StringBuffer pathStr, rstr, required, description;
            feature.getProp("@path", pathStr);
            feature.getProp("@resource", rstr);
            feature.getProp("@required", required);
            feature.getProp("@description", description);
            ISecResourceList* rlist = featureMap->queryResourceList(pathStr);
            if(rlist == nullptr)
            {
                rlist = createResourceList(pathStr, secureContext);
                featureMap->add(pathStr, rlist);
            }
            if (!rstr.isEmpty())
            {
                ISecResource* rs = rlist->addResource(rstr);
                SecAccessFlags requiredaccess = str2perm(required);
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description);
                if (setAccessFlag)
                    rs->setAccessFlags(accessFlag);//grant an access to authenticated users
            }
        }
    }

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
    SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const override
    {
        switch (level)
        {
        case SFSL_Unsafe:
            return (SMF_ALL_FEATURES & ~s_safeFeatureMask);
        case SFSL_Safe:
            return s_safeFeatureMask;
        case SFSL_Implemented:
            return s_implementedFeatureMask;
        default:
            return SMF_NO_FEATURES;
        }
    }

    ISecUser * createUser(const char * user_name, IEspSecureContext* secureContext = nullptr) override
    {
        return new CSecureUser(user_name, NULL);
    }

    ISecResourceList * createResourceList(const char * rlname, IEspSecureContext* secureContext = nullptr) override
    {
        return new CSecurityResourceList(rlname);
    }

    bool subscribe(ISecAuthenticEvents & events, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool unsubscribe(ISecAuthenticEvents & events, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authorizeEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext) override
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext) override
    {
        throwUnexpected();
    }

    SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authorizeFileScope(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authorizeViewScope(ISecUser & user, ISecResourceList * resources)
    {
        throwUnexpected();
    }

    bool addResources(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool addResourcesEx(SecResourceType rtype, ISecUser & user, ISecResourceList * resources, SecPermissionType ptype, const char * basedn, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool addResourceEx(SecResourceType rtype, ISecUser & user, const char * resourcename, SecPermissionType ptype, const char * basedn, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool getResources(SecResourceType rtype, const char * basedn, IResourceArray & resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool updateResources(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool updateSettings(ISecUser & user, ISecPropertyList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool addUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    ISecUser * findUser(const char * username, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    ISecUser * lookupUser(unsigned uid, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    ISecUserIterator * getAllUsers(IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool updateUserPassword(ISecUser & user, const char * newPassword, const char* currPassword = nullptr, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool initUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    void setExtraParam(const char * name, const char * value, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    IAuthMap * createAuthMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    IAuthMap * createFeatureMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    IAuthMap * createSettingMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) override
    {
#ifdef _DEBUG
        DBGLOG("using CBaseSecurityManager::createSettingMap; override with desired behavior");
#endif
        return nullptr;
    }

    void deleteResource(SecResourceType rtype, const char * name, const char * basedn, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    void renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    void copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    void cacheSwitch(SecResourceType rtype, bool on, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authTypeRequired(SecResourceType rtype, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    const char * getDescription() override
    {
        throwUnexpected();
    }

    unsigned getPasswordExpirationWarningDays(IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    aindex_t getManagedScopeTree(SecResourceType rtype, const char * basedn, IArrayOf<ISecResource>& scopes, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    SecAccessFlags queryDefaultPermission(ISecUser& user, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool clearPermissionsCache(ISecUser & user, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool authenticateUser(ISecUser & user, bool *superUser, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    secManagerType querySecMgrType() override
    {
        throwUnexpected();
    }

    const char* querySecMgrTypeName() override
    {
        throwUnexpected();
    }

    virtual bool logoutUser(ISecUser& user, IEspSecureContext* secureContext = nullptr) override
    {
        user.setAuthenticateStatus(AS_UNKNOWN);
        user.credentials().setSessionToken(0);
        return true;
    }

    bool retrieveUserData(ISecUser& requestedUser, ISecUser* requestingUser = nullptr, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }

    bool removeResources(ISecUser& sec_user, ISecResourceList * resources, IEspSecureContext* secureContext = nullptr) override
    {
        throwUnexpected();
    }
};

#endif // BASESECURITY_INCL
//#endif
