/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#include "singleUserSecurity.hpp"

class CSingleUserSecurityManager : public CBaseSecurityManager
{
public:
    CSingleUserSecurityManager(const char * serviceName, IPropertyTree * secMgrCfg, IPropertyTree * bindConfig) : CBaseSecurityManager(serviceName, (IPropertyTree *)NULL)
    {
        if (secMgrCfg)
        {
            if (secMgrCfg->hasProp("@SingleUserName"))
                secMgrCfg->getProp("@SingleUserName", m_userName);
            else
                m_userName.set("admin");
            PROGLOG("SingleUserAuth: User set '%s'", m_userName.str());

            secMgrCfg->getProp("@SingleUserPass", m_userPass);
            if (m_userPass.isEmpty())
                throw MakeStringException(-1,"SingleUserAuth: Password not supplied and could not set up security manager!");
        }
        else
           throw MakeStringException(-1, "SingleUserAuth did not receive security manager configuration!");
    }

    ~CSingleUserSecurityManager() {}

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

    secManagerType querySecMgrType() override
    {
        return SMT_SingleUser;
    }

    inline virtual const char * querySecMgrTypeName() override { return "singleuser"; }

    IAuthMap * createAuthMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<IAuthMap> authMap = new CAuthMap();
        createAuthMapImpl(authMap, "singleusersecurity", true, SecAccess_Full, authconfig, secureContext);
        authMap->shareWithManager(*this, secureContext);
        return authMap.getClear();
    }

    IAuthMap * createFeatureMap(IPropertyTree * authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        Owned<IAuthMap> featureMap = new CAuthMap();
        createFeatureMapImpl(featureMap, true, SecAccess_Full, authconfig, secureContext);
        return featureMap.getClear();
    }

    IAuthMap * createSettingMap(IPropertyTree * authConfig, IEspSecureContext* secureContext = nullptr) override
    {
        return nullptr;
    }
    bool logoutUser(ISecUser & user, IEspSecureContext* secureContext = nullptr) override { return true; }

protected:

    //ISecManager
    bool IsPasswordValid(ISecUser& sec_user)
    {
        StringBuffer username;
        username.set(sec_user.getName());
        if (0 == username.length())
            throw MakeStringException(-1, "SingleUserAuth name is empty");

        if (sec_user.credentials().getSessionToken() != 0  || !isEmptyString(sec_user.credentials().getSignature()))//Already authenticated it token or signature exist
            return true;

        if (strcmp(username.str(), m_userName.str())!=0)
        {
            WARNLOG("SingleUserAuth: Invalid credentials provided!");
            return false;
        }

        if (m_userPass.isEmpty())
            throw MakeStringException(-1, "SingleUserAuth password was not set!");

        const char * userpass = sec_user.credentials().getPassword();
        if (!userpass || !*userpass)
            throw MakeStringException(-1, "SingleUserAuth encountered empty password!");

        StringBuffer encpass;
        encrypt(encpass, userpass);
        if (strcmp(m_userPass.str(), encpass.str())!=0)
        {
            WARNLOG("SingleUserAuth: Invalid credentials provided!");
            return false;
        }
        return true;
    }

    const char * getDescription() override
    {
        return "SingleUser Security Manager";
    }

    bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext * secureContext = nullptr) override
    {
        return IsPasswordValid(user);
    }

    unsigned getPasswordExpirationWarningDays(IEspSecureContext* secureContext = nullptr) override
    {
        return -2;//never expires
    }

    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext * secureContext) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser& sec_user, const char * resourcename, IEspSecureContext* secureContext = nullptr) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope, IEspSecureContext* secureContext = nullptr) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

private:

private:
    static const SecFeatureSet s_implementedFeatureMask = SMF_QuerySecMgrType | SMF_QuerySecMgrTypeName | SMF_CreateAuthMap |
                                                          SMF_CreateFeatureMap | SMF_LogoutUser | SMF_GetDescription | SMF_Authorize |
                                                          SMF_GetPasswordExpirationDays | SMF_AuthorizeEx_Named | SMF_GetAccessFlagsEx |
                                                          SMF_AuthorizeFileScope_Named | SMF_AuthorizeWorkUnitScope_Named;
    static const SecFeatureSet s_safeFeatureMask = s_implementedFeatureMask | SMF_CreateSettingMap;
    StringBuffer    m_userPass;
    StringBuffer    m_userName;
};

extern "C"
{
    SINGLEUSERSECURITY_API ISecManager * createInstance(const char * serviceName, IPropertyTree &secMgrCfg, IPropertyTree &bndCfg)
    {
        return new CSingleUserSecurityManager(serviceName, &secMgrCfg, &bndCfg);
    }

}

