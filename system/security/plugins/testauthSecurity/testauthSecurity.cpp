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

class CUserAccess : public CInterface
{
    StringAttr userName, password;
    SecAccessFlags defaultECLWUScopeAccess = SecAccess_Unavailable;
    SecAccessFlags defaultFileScopeAccess = SecAccess_Unavailable;
    SecAccessFlags defaultFeatureAccess = SecAccess_Unavailable;
    MapStringTo<int> featureAccesses, eclwuScopeAccesses;
    Owned<IPropertyTree> fileScopeAccesses;

public:
    CUserAccess(const char* _userName, const char* _password)
        : userName(_userName), password(_password)
    {
        fileScopeAccesses.setown(createPTree("FileScopes"));
    };

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
        defaultFeatureAccess = getSecAccessFlagValue(access);
    }

    void setDefaultFileScopeAccess(const char* access)
    {
        defaultFileScopeAccess = getSecAccessFlagValue(access);
    }

    void setDefaultECLWUScopeAccess(const char* access)
    {
        defaultECLWUScopeAccess = getSecAccessFlagValue(access);
    }

    void addFeatureAccess(const char* resource, const char* access)
    {
        featureAccesses.setValue(resource, getSecAccessFlagValue(access));
    }

    void addECLWUScopeAccess(const char* scope, const char* access)
    {
        eclwuScopeAccesses.setValue(scope, getSecAccessFlagValue(access));
    }

    void addFileScopeAccess(const char* scope, const char* access)
    {
        //Convert the scope to xpath.
        StringArray nodes;
        if (!convertFileScopeToNodes(scope, true, nodes))
        {
            OWARNLOG("Invalid scope %s.", scope);
            return;
        }

        //Check duplicate
        const char* xpath = nodes.item(0);
        if (fileScopeAccesses->hasProp(xpath))
        {
            OWARNLOG("Duplicated access setting for scope %s.", scope);
            return;
        }

        //Add a tree node for the access.
        IPropertyTree* accessNode = ensurePTree(fileScopeAccesses, xpath);
        accessNode->setPropInt("@access", getSecAccessFlagValue(access));
    }

    SecAccessFlags queryFeatureAccess(const char* resource)
    {
        int* allow = featureAccesses.getValue(resource);
        return allow ? (SecAccessFlags) *allow : defaultFeatureAccess;
    }

    /* same logic as in LDAP security
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
        StringArray nodes;
        if (!convertFileScopeToNodes(scope, false, nodes))  //==>  a::b::c::d becomes nodes[a, b, c, d]
            return SecAccess_Unavailable;
        return getFileScopeAccess(fileScopeAccesses, nodes, 0, defaultFileScopeAccess, true);
    }

    bool convertFileScopeToNodes(const char* fileScope, bool fullPathOnly, StringArray& scopes)
    {
        StringBuffer scope;
        const char* p = fileScope;
        while (*p)
        {
            if (*p != ':')
            {
                scope.append(*(p++));
                continue;
            }

            if (*(p+1) != ':')
                return false;
            if (fullPathOnly)
                scope.append('/');
            else
            {
                scopes.append(scope);
                scope.clear();
            }
            p += 2;
        }
        if (scope.isEmpty())
            return false;
        if (fullPathOnly && (scope.charAt(scope.length() - 1) == '/'))
            return false;
        if (!fullPathOnly)
            return false;
        scopes.append(scope);
        return true;
    }

    SecAccessFlags getFileScopeAccess(IPropertyTree* accessTree, const StringArray& nodes, unsigned pos, SecAccessFlags access, bool first)
    {
        if (!nodes.isItem(pos))
            return access;
        const char* node = nodes.item(pos);
        if (isEmptyString(node))
            return access;
        IPropertyTree* element = accessTree->queryPropTree(node);
        if (!element)
            return access;
        if (element->hasProp("@access"))
        {
            int newAccess = element->getPropInt("@access");
            if (newAccess < SecAccess_Read)
                return (SecAccessFlags) newAccess;
            if (first || newAccess < access) //least access wins
            {
                access = (SecAccessFlags) newAccess;
                first = false;
            }
        }
        return getFileScopeAccess(element, nodes, ++pos, access, first);
    }

    SecAccessFlags queryECLWUScopeAccess(const char* resource)
    {
        int* allow = eclwuScopeAccesses.getValue(resource);
        return allow ? (SecAccessFlags) *allow : defaultECLWUScopeAccess;
    }
};

class CTestAuthSecurityManager : public CBaseSecurityManager
{
    static const SecFeatureSet s_implementedFeatureMask = SMF_QuerySecMgrType | SMF_QuerySecMgrTypeName | SMF_CreateAuthMap |
                                                          SMF_CreateFeatureMap | SMF_LogoutUser | SMF_GetDescription | SMF_Authorize |
                                                          SMF_AuthorizeEx_Named | SMF_GetAccessFlagsEx |
                                                          SMF_AuthorizeFileScope_Named | SMF_AuthorizeWorkUnitScope_Named;
    static const SecFeatureSet s_safeFeatureMask = s_implementedFeatureMask | SMF_CreateSettingMap;

    MapStringTo<CUserAccess*> userAccessMap;
    CIArrayOf<CUserAccess> userAccessList; //free up the CUserAccess in userAccessMap

    void readUserAccess(IPropertyTree* secMgrCfg, IPropertyTree* bindConfig)
    {
        StringBuffer s;
        Owned<IPropertyTreeIterator> iter = secMgrCfg->getElements("userAccess");
        ForEach(*iter)
        {
            IPropertyTree& userSettings = iter->query();

            StringBuffer userName, password;
            readUserNamePassword(userSettings, userName, password);

            CUserAccess** match = userAccessMap.getValue(userName);
            if (match)
                continue;

            Owned<CUserAccess> newUserAccess = new CUserAccess(userName, password);
            readDefaultAccess(userSettings, newUserAccess);
            readFeatureAccess(bindConfig->queryProp("@serviceType"), userSettings, newUserAccess);
            readFileScopeAccess(userSettings, newUserAccess);
            readECLWUScopeAccess(userSettings, newUserAccess);
            userAccessMap.setValue(userName.str(), newUserAccess);
            userAccessList.append(*newUserAccess.getClear());
        }
    }

    void readUserNamePassword(IPropertyTree& userSettings, StringBuffer& userName, StringBuffer& password)
    {
        userName.set(userSettings.queryProp("@userName"));
        if (isEmptyString(userName))
            throw makeStringExceptionV(-1, "Error retrieving userName.");

        const char* secretKey = userSettings.queryProp("@secretKey");
        if (!isEmptyString(secretKey))
        {
            Owned<IPropertyTree> secretTree = getSecret("authn", secretKey);
            if (!secretTree)
                throw makeStringExceptionV(-1, "Error retrieving the secret for %s.", secretKey);

            getSecretKeyValue(password, secretTree, "password");
            if (password.isEmpty())
                throw makeStringExceptionV(-1, "Error retrieving password for %s.", secretKey);
        }
        else
        {
            const char* passwordStr = userSettings.queryProp("@password");
            if (isEmptyString(passwordStr))
                password.set(userName);
            else
                decrypt(password, passwordStr);
        }
    }

    void readDefaultAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        const char* defaultFeatureAccess = userSettings.queryProp("defaults/@resource");
        const char* defaultFileScopeAccess = userSettings.queryProp("defaults/@fileScope");
        const char* defaultECLWUScopeAccess = userSettings.queryProp("defaults/@eclWUScope");
        if (!isEmptyString(defaultFeatureAccess))
            userAccess->setDefaultFeatureAccess(defaultFeatureAccess);
        if (!isEmptyString(defaultFileScopeAccess))
            userAccess->setDefaultFileScopeAccess(defaultFileScopeAccess);
        if (!isEmptyString(defaultECLWUScopeAccess))
            userAccess->setDefaultECLWUScopeAccess(defaultECLWUScopeAccess);
    }

    void readFeatureAccess(const char* serviceType, IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        if (isEmptyString(serviceType))
            return;

        VStringBuffer path("resources/%s/features", serviceType);
        IPropertyTree* t = userSettings.queryPropTree(path.str());
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addFeatureAccess(attributes->queryName() + 1, attributes->queryValue());
    }

    void readECLWUScopeAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        IPropertyTree* t = userSettings.queryPropTree("eclWUScopes");
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addECLWUScopeAccess(attributes->queryName() + 1, attributes->queryValue());
    }

    void readFileScopeAccess(IPropertyTree& userSettings, CUserAccess* userAccess)
    {
        IPropertyTree* t = userSettings.queryPropTree("fileScopes");
        if (!t)
            return;

        Owned<IAttributeIterator> attributes = t->getAttributes();
        ForEach(*attributes)
            userAccess->addFileScopeAccess(attributes->queryName() + 1, attributes->queryValue());
    }

    SecAccessFlags getAccessFlag(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext)
    {
        SecAccessFlags  resultFlag = SecAccess_Unavailable;
        try
        {
            if (!authenticate(user))
                return SecAccess_Unavailable;

            StringBuffer userN;
            userN.append(user.getName());
            CUserAccess** match = userAccessMap.getValue(userN);
            if (!match)
                return SecAccess_Unavailable;

            CUserAccess* userAccess = *match;

            if(rtype == RT_FILE_SCOPE)
                resultFlag = userAccess->queryFileScopeAccess(resourceName);
            else if(rtype == RT_WORKUNIT_SCOPE)
                resultFlag = userAccess->queryECLWUScopeAccess(resourceName);
            else
                resultFlag = userAccess->queryFeatureAccess(resourceName);
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
    bool authenticate(ISecUser& user)
    {
        if (user.credentials().getSessionToken() != 0 || user.getAuthenticateStatus() == AS_AUTHENTICATED)
        { //Authenticated.
            return true;
        }

        StringBuffer userN;
        userN.set(user.getName());
        if (userN.isEmpty())
            return false;

        CUserAccess** match = userAccessMap.getValue(userN);
        if (!match)
            return false;

        CUserAccess* userAccess = *match;
        const char* pwd = user.credentials().getPassword();
        return !isEmptyString(pwd) && streq(pwd, userAccess->queryPassword());
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

    virtual IAuthMap* createAuthMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<IAuthMap> authMap = new CAuthMap();
        createAuthMapImpl(authMap, "TestAuthSecMgr", false, SecAccess_Unavailable, authconfig, secureContext);
        return authMap.getClear();
    }

    virtual IAuthMap* createFeatureMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<IAuthMap> featureMap = new CAuthMap();
        createFeatureMapImpl(featureMap, false, SecAccess_Unavailable, authconfig, secureContext);
        return featureMap.getClear();
    }

    virtual IAuthMap* createSettingMap(IPropertyTree* authConfig, IEspSecureContext* secureContext = nullptr) override
    {
        return nullptr;
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
            CUserAccess** match = userAccessMap.getValue(userN);
            if (!match)
                return false;

            CUserAccess* userAccess = *match;
            for (unsigned x = 0; x < resources->count(); x++)
            {
                ISecResource* resource = resources->queryResource(x);
                if (resource == nullptr)
                    continue;

                if(rtype == RT_FILE_SCOPE)
                    resource->setAccessFlags(userAccess->queryFileScopeAccess(resource->getName()));
                else if(rtype == RT_WORKUNIT_SCOPE)
                    resource->setAccessFlags(userAccess->queryECLWUScopeAccess(resource->getName()));
                else
                    resource->setAccessFlags(userAccess->queryFeatureAccess(resource->getName()));
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

    virtual SecAccessFlags authorizeFileScope(ISecUser& user, const char* fileScope, IEspSecureContext* secureContext = nullptr) override
    {
        if (isEmptyString(fileScope))
            return SecAccess_Full; //Copy from LdapSecurity lib. Not sure this is safe.

        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("FileScope", secureContext));
        rlist->addResource(fileScope );

        if(authorizeFileScope(user, rlist.get(), secureContext))
            return rlist->queryResource(0)->getAccessFlags();
        return SecAccess_Unavailable;
    }

    virtual SecAccessFlags authorizeWorkunitScope(ISecUser& user, const char* wuScope, IEspSecureContext* secureContext) override
    {
        if (isEmptyString(wuScope))
            return SecAccess_Full;

        Owned<ISecResourceList> rlist;
        rlist.setown(createResourceList("WorkunitScope", secureContext));
        rlist->addResource(wuScope);

        if (authorizeWorkunitScope(user, rlist.get(), secureContext))
            return rlist->queryResource(0)->getAccessFlags();
        return SecAccess_Unavailable;
    }

    virtual bool authorizeWorkunitScope(ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext) override
    {
        return authorizeEx(RT_WORKUNIT_SCOPE, user, resources, secureContext);
    }
};

extern "C"
{
    TESTAUTHSECURITY_API ISecManager* createInstance(const char* serviceName, IPropertyTree& secMgrCfg, IPropertyTree& bndCfg)
    {
        return new CTestAuthSecurityManager(serviceName, &secMgrCfg, &bndCfg);
    }
}

