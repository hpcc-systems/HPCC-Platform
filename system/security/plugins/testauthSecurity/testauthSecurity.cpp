/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#pragma warning( disable : 4786 )

#include "basesecurity.hpp"

#include "authmap.ipp"
#include "testauthSecurity.hpp"

class CUserAccess : public CSimpleInterfaceOf<IInterface>
{
    StringAttr userName, password;
    SecAccessFlags defaulteECLWUScopeAccess = SecAccess_Unavailable;
    SecAccessFlags defaultFileScopeAccess = SecAccess_Unavailable;
    SecAccessFlags defaultFeaturAccess = SecAccess_Unavailable;
    MapStringTo<int> featureAccesses, eclwuScopeAccesses, fileScopeAccesses;

public:
    CUserAccess(const char* _userName, const char* _password)
        : userName(_userName), password(_password) {};

    const char* queryUserName()
    {
        return userName.get();
    }

    const char* queryPassword()
    {
        return password.get();
    }

    void setDefaultFeatureAccess(const char* access)
    {
        defaultFeaturAccess = getSecAccessFlagValue(access);
    }

    void setDefaultFileScopeAccess(const char* access)
    {
        defaultFileScopeAccess = getSecAccessFlagValue(access);
    }

    void setDefaultECLWUScopeAccess(const char* access)
    {
        defaulteECLWUScopeAccess = getSecAccessFlagValue(access);
    }

    void addFeatureAccess(const char* resource, const char* access)
    {
        SecAccessFlags flag = getSecAccessFlagValue(access);
        if (!featureAccesses.getValue(resource))
            featureAccesses.setValue(resource, flag);
    }

    void addECLWUScopeAccess(const char* scope, const char* access)
    {
        SecAccessFlags flag = getSecAccessFlagValue(access);
        if (!eclwuScopeAccesses.getValue(scope))
            eclwuScopeAccesses.setValue(scope, flag);
    }

    void addFileScopeAccess(const char* scope, const char* access)
    {
        SecAccessFlags flag = getSecAccessFlagValue(access);
        if (!fileScopeAccesses.getValue(scope))
            fileScopeAccesses.setValue(scope, flag);
    }

    SecAccessFlags queryFeatureAccess(const char* resource)
    {
        int* allow = featureAccesses.getValue(resource);
        return allow ? (SecAccessFlags) *allow : defaultFeaturAccess;
    }

    /*
    if perms set on 'scopeA::scopeB' only and lookup of 'scopeA::scopeB::scopeC::scopeD'
    need to lookup:
        'scopeA'
    no match=>continue
    match=>continue if read permissions (if no read, implies can't "see" child scopes)
        'scopeA::scopeB'
    no match=>continue
    match=>continue if read permissions (if no read, implies can't "see" child scopes)
    etc. Until full scope path checked, or no read permissions hit on ancestor scope.
    */
    SecAccessFlags queryFileScopeAccess(const char* scope)
    {
        StringArray scopes;
        parseFileScope(scope, scopes);

        SecAccessFlags allowed = defaultFileScopeAccess;
        ForEachItemIn(i, scopes)
        {
            int* allow = fileScopeAccesses.getValue(scopes.item(i));
            if (allow)
            {
                allowed = (SecAccessFlags) *allow;
                if (0 == (allowed & SecAccess_Read))
                    return allowed;
            }
        }
        return allowed;
    }

    bool parseFileScope(const char* fileScope, StringArray& scopes)
    {
        StringBuffer scope;
        const char* p = fileScope;
        while (*p)
        {
            if (*p == ':')
            {
                if (*(p+1) != ':')
                    return false;
                scopes.append(scope);
                scope.append(*(p++));
            }
            scope.append(*(p++));
        }
        scopes.append(scope);
        return true;
    }

    SecAccessFlags queryECLWUScopeAccess(const char* resource)
    {
        int* allow = eclwuScopeAccesses.getValue(resource);
        return allow ? (SecAccessFlags) *allow : defaulteECLWUScopeAccess;
    }
};

typedef CUserAccess* CUserAccessPtr;

class CTestAuthSecurityManager : public CBaseSecurityManager
{
    static const SecFeatureSet s_implementedFeatureMask = SMF_QuerySecMgrType | SMF_QuerySecMgrTypeName | SMF_CreateAuthMap |
                                                          SMF_CreateFeatureMap | SMF_LogoutUser | SMF_GetDescription | SMF_Authorize |
                                                          SMF_AuthorizeEx_Named | SMF_GetAccessFlagsEx |
                                                          SMF_AuthorizeFileScope_Named | SMF_AuthorizeWorkUnitScope_Named;
    static const SecFeatureSet s_safeFeatureMask = s_implementedFeatureMask | SMF_CreateSettingMap;

    MapStringTo<CUserAccess*> userAccessMap;

    void readUserAccess(IPropertyTree* secMgrCfg, IPropertyTree* bindConfig)
    {
        StringBuffer s;
        Owned<IPropertyTreeIterator> iter = secMgrCfg->getElements("useraccess");
        ForEach(*iter)
        {
            IPropertyTree& userSettings = iter->query();

            StringBuffer userName, password;
            if (!readUserNamePassword(userSettings, userName, password))
                continue;

            CUserAccessPtr* userAccess = userAccessMap.getValue(userName);
            if (userAccess)
                continue;

            Owned<CUserAccess> newUserAccess = new CUserAccess(userName, password);
            readDefaultAccess(userSettings, newUserAccess);
            readFeatureAccess(bindConfig->queryProp("@serviceType"), userSettings, newUserAccess);
            readFileScopeAccess(userSettings, newUserAccess);
            readECLWUScopeAccess(userSettings, newUserAccess);
            userAccessMap.setValue(userName.str(), newUserAccess.getClear());
        }
    }

    bool readUserNamePassword(IPropertyTree& userSettings, StringBuffer& userName, StringBuffer& password)
    {
        const char* secretKey = userSettings.queryProp("@secretkey");
        if (!isEmptyString(secretKey))
        {
            const char* vaultKey = userSettings.queryProp("@vaultkey");
            Owned<IPropertyTree> secretTree;
            if (isEmptyString(vaultKey))
                secretTree.setown(getSecret("authn", secretKey));
            else
                secretTree.setown(getVaultSecret("authn", vaultKey, secretKey, nullptr));
            if (!secretTree)
                throw makeStringExceptionV(-1, "Error retrieving the secret for %s.", secretKey);

            getSecretKeyValue(userName, secretTree, "username");
            getSecretKeyValue(password, secretTree, "password");
            if (userName.isEmpty() || password.isEmpty())
                throw makeStringExceptionV(-1, "Error retrieving userName/password for %s.", secretKey);
            return true;
        }

        userName.set(userSettings.queryProp("@username"));
        if (isEmptyString(userName))
            return false;

        password.set(userSettings.queryProp("@password"));
        if (isEmptyString(password))
            password.set(userName);
        return true;
    }

    void readDefaultAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        const char* defaultFeatureAccess = userSettings.queryProp("defaults/@resource");
        const char* defaulteFileScopeAccess = userSettings.queryProp("defaults/@filescope");
        const char* defaulteECLWUScopeAccess = userSettings.queryProp("defaults/@eclwuscope");
        if (!isEmptyString(defaultFeatureAccess))
            userAccess->setDefaultFeatureAccess(defaultFeatureAccess);
        if (!isEmptyString(defaulteFileScopeAccess))
            userAccess->setDefaultFileScopeAccess(defaulteFileScopeAccess);
        if (!isEmptyString(defaulteECLWUScopeAccess))
            userAccess->setDefaultECLWUScopeAccess(defaulteECLWUScopeAccess);
    }

    void readFeatureAccess(const char* serviceType, IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        if (isEmptyString(serviceType))
            return;

        VStringBuffer path("resources/%s/Features", serviceType);
        IPropertyTree* t = userSettings.queryPropTree(path.str());
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addFeatureAccess(attributes->queryName() + 1, attributes->queryValue());
    }

    void readECLWUScopeAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        IPropertyTree* t = userSettings.queryPropTree("eclwuscopes");
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addECLWUScopeAccess(attributes->queryName() + 1, attributes->queryValue());
    }

    void readFileScopeAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        IPropertyTree* t = userSettings.queryPropTree("filescopes");
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addFileScopeAccess(attributes->queryName() + 1, attributes->queryValue());
    }

public:
    CTestAuthSecurityManager(const char* serviceName, IPropertyTree* secMgrCfg, IPropertyTree* bindConfig)
        : CBaseSecurityManager(serviceName, (IPropertyTree* ) nullptr)
    {
        if (!secMgrCfg || !bindConfig)
            return;
        readUserAccess(secMgrCfg, bindConfig);
    }

    SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const override
    {
        SecFeatureSet mgrFeatures = CBaseSecurityManager::queryFeatures(level);
        switch (level)
        {
        case SFSL_Safe:
            mgrFeatures = mgrFeatures | s_safeFeatureMask;
            break;
        case SFSL_Implemented:
            mgrFeatures = mgrFeatures | s_implementedFeatureMask;
            break;
        case SFSL_Unsafe:
            mgrFeatures = mgrFeatures & ~s_safeFeatureMask;
            break;
        default:
            break;
        }
        return mgrFeatures;
    }

    secManagerType querySecMgrType() override {	return SMT_TestAuth; }
    inline virtual const char* querySecMgrTypeName() override { return "testauth"; }
    const char* getDescription() override { return "Test Auth Security Manager"; }

    IAuthMap* createAuthMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<CAuthMap> authmap = new CAuthMap();

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

            ISecResourceList* rlist = authmap->queryResourceList(pathStr);
            if (rlist == nullptr)
            {
                rlist = createResourceList("htpasswdsecurity", secureContext);
                authmap->add(pathStr, rlist);
            }
            ISecResource* rs = rlist->addResource(rstr);
            SecAccessFlags requiredaccess = str2perm(required);
            rs->setRequiredAccessFlags(requiredaccess);
            rs->setDescription(description);
        }
        return authmap.getClear();
    }

    IAuthMap* createFeatureMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<CAuthMap> authmap = new CAuthMap();

        Owned<IPropertyTreeIterator> iter = authconfig->getElements(".//Feature");
        ForEach(*iter)
        {
            IPropertyTree& feature = iter->query();
            StringBuffer pathStr, rstr, required, description;
            feature.getProp("@path", pathStr);
            feature.getProp("@resource", rstr);
            feature.getProp("@required", required);
            feature.getProp("@description", description);
            ISecResourceList* rlist = authmap->queryResourceList(pathStr);
            if(rlist == nullptr)
            {
                rlist = createResourceList(pathStr, secureContext);
                authmap->add(pathStr, rlist);
            }
            if (!rstr.isEmpty())
            {
                ISecResource* rs = rlist->addResource(rstr);
                SecAccessFlags requiredaccess = str2perm(required);
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description);
            }
        }
        return authmap.getClear();
    }

    IAuthMap* createSettingMap(IPropertyTree* authConfig, IEspSecureContext* secureContext = nullptr) override
    {
        return nullptr;
    }

    SecAccessFlags getAccessFlag(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext)
    {
        SecAccessFlags  resultFlag = SecAccess_Unavailable;
        try
        {
            if (!authenticate(user))
                return resultFlag;

            StringBuffer userN;
            userN.append(user.getName());
            CUserAccessPtr* userAccess = userAccessMap.getValue(userN);
            if (!userAccess)
                return resultFlag;

            CUserAccessPtr userAccessPtr = *userAccess;

            if(rtype == RT_FILE_SCOPE)
                resultFlag = userAccessPtr->queryFileScopeAccess(resourceName);
            else if(rtype == RT_WORKUNIT_SCOPE)
                resultFlag = userAccessPtr->queryECLWUScopeAccess(resourceName);
            else
                resultFlag = userAccessPtr->queryFeatureAccess(resourceName);
        }
        catch (IException* e)
        {
            EXCLOG(e, "CTestAuthSecurityManager::getAccessFlag(...) exception caught");
            e->Release();
        }
        catch (...)
        {
            // Need better logging
            DBGLOG("CTestAuthSecurityManager::getAccessFlag(...) exception caught");
        }

        return resultFlag;
    }

protected:

    //ISecManager
    bool authenticate(ISecUser& user)
    {
        if (user.credentials().getSessionToken() != 0 || user.getAuthenticateStatus() == AS_AUTHENTICATED)
        { //Authenticated.
            return true;
        }

        StringBuffer userN, password;
        userN.set(user.getName());
        if (userN.isEmpty())
			return false;

        CUserAccessPtr* userAccess = userAccessMap.getValue(userN);
        if (!userAccess)
            return false;

        CUserAccessPtr userAccessPtr = *userAccess;
        password.set(user.credentials().getPassword());
        return streq(password.str(), userAccessPtr->queryPassword());
    }

    bool authorize(ISecUser& user, ISecResourceList* Resources, IEspSecureContext* secureContext = nullptr) override
    {
        return authorizeEx(RT_DEFAULT, user, Resources, secureContext);
    }

    bool authorizeEx(SecResourceType rtype, ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext = nullptr) override
    {
        try
        {
            if (!authenticate(user))
                return false;

            if (!resources)
                return true;

            StringBuffer userN;
            userN.append(user.getName());
            CUserAccessPtr* userAccess = userAccessMap.getValue(userN);
            if (!userAccess)
                return false;

            CUserAccessPtr userAccessPtr = *userAccess;
            for (unsigned x = 0; x < resources->count(); x++)
            {
                ISecResource* resource = resources->queryResource(x);
                if (resource == nullptr)
                    continue;

                if(rtype == RT_FILE_SCOPE)
                    resource->setAccessFlags(userAccessPtr->queryFileScopeAccess(resource->getName()));
                else if(rtype == RT_WORKUNIT_SCOPE)
                    resource->setAccessFlags(userAccessPtr->queryECLWUScopeAccess(resource->getName()));
                else
                    resource->setAccessFlags(userAccessPtr->queryFeatureAccess(resource->getName()));
            }

        }
        catch (IException* e)
        {
            EXCLOG(e, "CTestAuthSecurityManager::authorizeEx(...) exception caught");
            e->Release();
        }
        catch (...)
        {
            // Need better logging
            DBGLOG("CTestAuthSecurityManager::authorizeEx(...) exception caught");
        }
        return true;
    }

    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext) override
    {
        if (isEmptyString(resourceName))
            return SecAccess_Full; //Copy from LdapSecurity lib. Not sure this is safe.

        return getAccessFlag(rtype, user, resourceName, secureContext);
    }

    SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext = nullptr) override
    {
        if (isEmptyString(resourceName))
            return SecAccess_Unavailable;

        return getAccessFlag(rtype, user, resourceName, secureContext);
    }

    bool authorizeFileScope(ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext) override
    {
        return authorizeEx(RT_FILE_SCOPE, user, resources, secureContext);
    }

    SecAccessFlags authorizeFileScope(ISecUser& user, const char* filescope, IEspSecureContext* secureContext = nullptr) override
    {
        if (isEmptyString(filescope))
            return SecAccess_Full; //Copy from LdapSecurity lib. Not sure this is safe.

        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("FileScope", secureContext));
        rlist->addResource(filescope );

        if(authorizeFileScope(user, rlist.get(), secureContext))
            return rlist->queryResource(0)->getAccessFlags();
        return SecAccess_Unavailable;
    }

    SecAccessFlags authorizeWorkunitScope(ISecUser& user, const char* wuscope, IEspSecureContext* secureContext)
    {
        if (isEmptyString(wuscope))
            return SecAccess_Full;

        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("WorkunitScope", secureContext));
        rlist->addResource(wuscope);

        if (authorizeWorkunitScope(user, rlist.get(), secureContext))
            return rlist->queryResource(0)->getAccessFlags();
        return SecAccess_Unavailable;
    }

    bool authorizeWorkunitScope(ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext)
    {
        return authorizeEx(RT_WORKUNIT_SCOPE, user, resources, secureContext);
    }

    bool logoutUser(ISecUser& user, IEspSecureContext* secureContext = nullptr) override
    {
        user.setAuthenticateStatus(AS_UNKNOWN);
        user.credentials().setSessionToken(0);
        return true;
    }
};

extern "C"
{
    TESTAUTHSECURITY_API ISecManager* createInstance(const char* serviceName, IPropertyTree& secMgrCfg, IPropertyTree& bndCfg)
    {
        return new CTestAuthSecurityManager(serviceName, &secMgrCfg, &bndCfg);
    }
}

