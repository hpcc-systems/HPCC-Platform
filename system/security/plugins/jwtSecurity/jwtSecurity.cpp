/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include <chrono>
#include <curl/curl.h>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "basesecurity.hpp"
#include "authmap.ipp"
#include "daldap.hpp"
#include "dasess.hpp"
#include "daliKVStore.hpp"
#include "jsecrets.hpp"

// Need to temporarily disable the verify macro because the jwt-cpp code has
// a verify() function that conflicts
#ifdef verify
    #define JWT_HAS_VERIFY_MACRO
    #define OLD_VERIFY verify
    #undef verify
#endif

#include "jwt-cpp/jwt.h"

#include "nlohmann/json.hpp"

#include "jwtCache.hpp"
#include "jwtEndpoint.hpp"
#include "jwtSecurity.hpp"

static const char* DALI_KVSTORE_STORE_NAME = "JWTAuth";
static const char* DALI_KVSTORE_NAMESPACE = "Tokens";
static const char* DALI_KVSTORE_KEY = "Token";

// The number of milliseconds between checking the key file for updates; this
// means changes to the file won't be noticed until at most
// KEY_FILE_CHECK_INTERVAL_MS milliseconds elapse
#define KEY_FILE_CHECK_INTERVAL_MS 10000

static JWTUserCache     gUserCache;

/**
 * Class used to temporarily hold scope permissions parsed from tokens.
 */
class ScopePermission
{
    public:

        ScopePermission(const std::string& _jwtClaim, const std::string& _scopePattern)
        :   scopePattern(_scopePattern), claim(_jwtClaim)
        {
        }

        bool operator< (const ScopePermission& other) const
        {
            return scopePattern < other.scopePattern;
        }

        std::string     scopePattern;
        std::string     claim;
};

typedef std::vector<ScopePermission> CollectedScopePerms;

class CJwtSecurityManager : implements IDaliLdapConnection, public CBaseSecurityManager
{
public:
    IMPLEMENT_IINTERFACE_USING(CBaseSecurityManager);

    CJwtSecurityManager(const char* serviceName, IPropertyTree* secMgrCfg, IPropertyTree* bindConfig)
    : CBaseSecurityManager(serviceName, (IPropertyTree*)nullptr), keyIsPublicKey(false)
    {
        defaultPermAccess = SecAccess_Full;
        defaultWUAccess = SecAccess_Full;
        defaultFileAccess = SecAccess_Full;

        if (secMgrCfg != nullptr)
        {
            clientID = secMgrCfg->queryProp("@clientID");
            loginEndpoint = secMgrCfg->queryProp("@loginEndpoint");
            refreshEndpoint = secMgrCfg->queryProp("@refreshEndpoint");
            secretsName = secMgrCfg->queryProp("@secretsName");
            allowSelfSignedCert = secMgrCfg->getPropBool("@allowSelfSignedCert", false);
            const char* permAccessStr = secMgrCfg->queryProp("@permDefaultAccess");
            const char* wuAccessStr = secMgrCfg->queryProp("@wuDefaultAccess");
            const char* fileAccessStr = secMgrCfg->queryProp("@fileDefaultAccess");

            if (permAccessStr && permAccessStr[0])
                defaultPermAccess = getSecAccessFlagValue(permAccessStr);

            if (wuAccessStr && wuAccessStr[0])
                defaultWUAccess = getSecAccessFlagValue(wuAccessStr);

            if (fileAccessStr && fileAccessStr[0])
                defaultFileAccess = getSecAccessFlagValue(fileAccessStr);
        }

        if (clientID.empty())
            throw makeStringException(-1, "CJwtSecurityManager: clientID not found in configuration");

        if (loginEndpoint.empty())
            throw makeStringException(-1, "CJwtSecurityManager: loginEndpoint not found in configuration");

        if (refreshEndpoint.empty())
            throw makeStringException(-1, "CJwtSecurityManager: refreshEndpoint not found in configuration");

        if (secretsName.empty())
            throw makeStringException(-1, "CJwtSecurityManager: secretsName not found in configuration");
    }

    virtual ~CJwtSecurityManager()
    {
    }

    virtual SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const override
    {
        SecFeatureSet mgrFeatures = CBaseSecurityManager::queryFeatures(level);

        switch (level)
        {
            case SFSL_Safe:
                mgrFeatures = mgrFeatures | safeFeaturesMask;
                break;

            case SFSL_Implemented:
                mgrFeatures = mgrFeatures | implementedFeaturesMask;
                break;

            case SFSL_Unsafe:
                mgrFeatures = mgrFeatures & ~safeFeaturesMask;
                break;

            default:
                break;
        }

        return mgrFeatures;
    }

    virtual secManagerType querySecMgrType() override
    {
        return SMT_JWTAuth;
    }

    inline virtual const char* querySecMgrTypeName() override
    {
        return "jwtsecmgr";
    }

    virtual IAuthMap* createAuthMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        CAuthMap* authmap = new CAuthMap();

        try
        {
            Owned<IPropertyTreeIterator> loc_iter;
            loc_iter.setown(authconfig->getElements(".//Location"));
            if (loc_iter != nullptr)
            {
                IPropertyTree* location = nullptr;
                loc_iter->first();
                while (loc_iter->isValid())
                {
                    location = &loc_iter->query();
                    if (location != nullptr)
                    {
                        StringBuffer pathstr, rstr, required, description;
                        location->getProp("@path", pathstr);
                        location->getProp("@resource", rstr);
                        location->getProp("@required", required);
                        location->getProp("@description", description);

                        if (pathstr.length() == 0)
                            throw makeStringException(-1, "CJwtSecurityManager: path empty in Authenticate/Location");
                        if (rstr.length() == 0)
                            throw makeStringException(-1, "CJwtSecurityManager: resource empty in Authenticate/Location");

                        ISecResourceList* rlist = authmap->queryResourceList(pathstr.str());

                        if (rlist == nullptr)
                        {
                            rlist = createResourceList("jwtsecmgr", secureContext);
                            authmap->add(pathstr.str(), rlist);
                        }
                        ISecResource* rs = rlist->addResource(rstr.str());
                        SecAccessFlags requiredaccess = str2perm(required.str());
                        rs->setRequiredAccessFlags(requiredaccess);
                        rs->setDescription(description.str());
                    }
                    loc_iter->next();
                }
            }
        }
        catch(...)
        {
            delete(authmap);
            throw;
        }

        return authmap;
    }

    virtual IAuthMap* createFeatureMap(IPropertyTree* authconfig, IEspSecureContext* secureContext = nullptr) override
    {
        CAuthMap* feature_authmap = new CAuthMap();

        try
        {
            Owned<IPropertyTreeIterator> feature_iter;
            feature_iter.setown(authconfig->getElements(".//Feature"));
            ForEach(*feature_iter)
            {
                IPropertyTree* feature = nullptr;
                feature = &feature_iter->query();
                if (feature != nullptr)
                {
                    StringBuffer pathstr, rstr, required, description;
                    feature->getProp("@path", pathstr);
                    feature->getProp("@resource", rstr);
                    feature->getProp("@required", required);
                    feature->getProp("@description", description);

                    ISecResourceList* rlist = feature_authmap->queryResourceList(pathstr.str());

                    if (rlist == nullptr)
                    {
                        rlist = createResourceList(pathstr.str(), secureContext);
                        feature_authmap->add(pathstr.str(), rlist);
                    }
                    if (!rstr.isEmpty())
                    {
                        ISecResource* rs = rlist->addResource(rstr.str());
                        SecAccessFlags requiredaccess = str2perm(required.str());
                        rs->setRequiredAccessFlags(requiredaccess);
                        rs->setDescription(description.str());
                    }
                }
            }
        }
        catch(...)
        {
            delete(feature_authmap);
            throw;
        }

        return feature_authmap;
    }

    virtual IAuthMap* createSettingMap(IPropertyTree* authConfig, IEspSecureContext* secureContext = nullptr) override
    {
        return nullptr;
    }

private:

    /**
     * Boolean indicating if one string is a prefix of another.  Case sensitive.
     *
     * @param   prefix      The prefix to test
     * @param   s           The string to test against
     *
     * @return  true if prefix is a case-sensitive prefix of, or exactly matches, s.
     */
    static bool isPrefixString(const std::string& prefix, const std::string& s)
    {
        auto res = std::mismatch(prefix.begin(), prefix.end(), s.begin());

        return res.first == prefix.end();
    }

    /**
     * Returns a copy of the string argument with particular whitespace
     * characters removed from the end.
     *
     * @param   s           The string to process
     *
     * @return  New string with trailing whitespace characters removed.
     */
    static std::string leadingStringTrim(const std::string& s)
    {
        size_t wsStart = s.find_first_not_of(" \n\r\t\f\v");

        if (wsStart != std::string::npos)
            return s.substr(wsStart);
        else
            return "";
    }

    /**
     * Returns a copy of the string argument with particular whitespace
     * characters removed from the end.
     *
     * @param   s           The string to process
     *
     * @return  New string with trailing whitespace characters removed.
     */
    static std::string trailingStringTrim(const std::string& s)
    {
        size_t wsStart = s.find_last_not_of(" \n\r\t\f\v");

        if (wsStart != std::string::npos)
            return s.substr(0, wsStart + 1);
        else
            return s;
    }

    /**
     * Returns a copy of the string argument with all characters
     * uppercased.
     *
     * @param   s           The string to process
     *
     * @return  New string with all characters uppercased.
     */
    static std::string strToUpper(std::string s)
    {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){return std::toupper(c);});

        return s;
    }

    /**
     * Validates an encoded JWT token (as if presented as a byte stream from an
     * external service).  Tokens are validated according to the OpenID Connect
     * specification (https://openid.net/specs/openid-connect-core-1_0.html#IDToken).
     * All REQUIRED items are, indeed, required.
     *
     * If token is validated then a copy of the token and all claims
     * are cached under the username of the current user.
     *
     * @param   token           Encoded token
     * @param   subject         Username of current user; will be compared
     *                          to the 'sub' value within the token; case-
     *                          sensitive
     * @param   refreshToken    Refresh token; will stuffed into the user
     *                          cache if token verifies without errors
     * @param   nonce           The random string supplied to the JWT
     *                          endpoint that presumably returned the
     *                          encoded token; will be compared to the
     *                          'nonce' value within the token; case-
     *                          sensitive; this may be an empty string;
     *                          if an empty string, no nonce value will
     *                          be verified
     *
     * @return  true if all validation checks pass, false otherwise
     */
    bool verifyToken(const std::string& token, const std::string& subject, const std::string& refreshToken, const std::string& nonce, const std::string& endpointURL)
    {
        bool verificationResult = false;

        try
        {
            CollectedScopePerms     wuScopePerms;
            CollectedScopePerms     fileScopePerms;

            // Decode the token string
            auto decodedToken = jwt::decode(token);

            // Create the JWT token verifier with known claims
            auto jwtVerifier = jwt::verify()
                                .with_subject(subject)
                                .with_audience(clientID);

            // Add an issuer claim only if an endpoint value was provided
            if (!endpointURL.empty())
                jwtVerifier = jwtVerifier.with_issuer(endpointURL);

            // Add a nonce claim only if a nonce value was provided
            if (!nonce.empty())
                jwtVerifier = jwtVerifier.with_claim("nonce", jwt::claim(nonce));

            // Append signature algorithm verifier
            std::string included_algo = strToUpper(decodedToken.get_algorithm());

            {
                CriticalBlock block(crit);

                ensureKeyLoaded();

                if (keyIsPublicKey)
                {
                    if (included_algo == "RS256")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::rs256(keyContents, "", "", ""));
                    else if (included_algo == "RS384")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::rs384(keyContents, "", "", ""));
                    else if (included_algo == "RS512")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::rs512(keyContents, "", "", ""));
                    else if (included_algo == "ES256")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::es256(keyContents, "", "", ""));
                    else if (included_algo == "ES384")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::es384(keyContents, "", "", ""));
                    else if (included_algo == "ES512")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::es512(keyContents, "", "", ""));
                    else if (included_algo == "PS256")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::ps256(keyContents, "", "", ""));
                    else if (included_algo == "PS384")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::ps384(keyContents, "", "", ""));
                    else if (included_algo == "PS512")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::ps512(keyContents, "", "", ""));
                    else
                        throw makeStringExceptionV(-1, "CJwtSecurityManager error: Unknown token algorithm for public key: %s", included_algo.c_str());
                }
                else
                {
                    if (included_algo == "HS256")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::hs256(keyContents));
                    else if (included_algo == "HS384")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::hs384(keyContents));
                    else if (included_algo == "HS512")
                        jwtVerifier = jwtVerifier.allow_algorithm(jwt::algorithm::hs512(keyContents));
                    else
                        throw makeStringExceptionV(-1, "CJwtSecurityManager error: Unknown token algorithm for hash key: %s", included_algo.c_str());
                }
            }

            // verify() throws an exception if it fails
            jwtVerifier.verify(decodedToken);

            // Stuff claims into a new cache object for this user
            std::shared_ptr<JWTUserInfo> userInfo(new JWTUserInfo());

            userInfo->setExpirationTime(std::chrono::system_clock::to_time_t(decodedToken.get_expires_at()));
            userInfo->setRefreshToken(refreshToken);
            userInfo->setJWTToken(token);
            for (auto& e : decodedToken.get_payload_claims())
            {
                std::string     key(e.first);

                // Don't cache key/values for standardized token keys
                if (key != "iss"
                        && key != "sub"
                        && key != "aud"
                        && key != "exp"
                        && key != "nbf"
                        && key != "iat"
                        && key != "aud"
                        && key != "jti"
                        && key != "auth_time"
                        && key != "nonce"
                        && key != "acr"
                        && key != "amr"
                        && key != "azp")
                {
                    if (isPrefixString("AllowWorkunitScope", key) || isPrefixString("DenyWorkunitScope", key))
                    {
                        // Collect permissions for later batch processing
                        if (e.second.get_type() == jwt::json::type::string)
                        {
                            wuScopePerms.push_back(ScopePermission(key, e.second.as_string()));
                        }
                        else
                        {
                            jwt::claim::set_t   valueSet = e.second.as_set();

                            for (jwt::claim::set_t::const_iterator x = valueSet.begin(); x != valueSet.end(); x++)
                            {
                                wuScopePerms.push_back(ScopePermission(key, *x));
                            }
                        }
                    }
                    else if (isPrefixString("AllowFileScope", key) || isPrefixString("DenyFileScope", key))
                    {
                        // Collect permissions for later batch processing
                        if (e.second.get_type() == jwt::json::type::string)
                        {
                            fileScopePerms.push_back(ScopePermission(key, e.second.as_string()));
                        }
                        else
                        {
                            jwt::claim::set_t   valueSet = e.second.as_set();

                            for (jwt::claim::set_t::const_iterator x = valueSet.begin(); x != valueSet.end(); x++)
                            {
                                fileScopePerms.push_back(ScopePermission(key, *x));
                            }
                        }
                    }
                    else if (e.second.get_type() == jwt::json::type::string)
                    {
                        // Feature permission where value is a single string
                        userInfo->mergeFeaturePerm(key, e.second.as_string());
                    }
                    else
                    {
                        // Feature permission where value is an array of strings
                        jwt::claim::set_t   valueSet = e.second.as_set();

                        for (jwt::claim::set_t::const_iterator x = valueSet.begin(); x != valueSet.end(); x++)
                        {
                            userInfo->mergeFeaturePerm(key, *x);
                        }
                    }
                }
            }

            // Add workunit scope permissions in alphabetical (and implicitly, length) order
            std::sort(wuScopePerms.begin(), wuScopePerms.end());
            for (CollectedScopePerms::const_iterator x = wuScopePerms.begin(); x != wuScopePerms.end(); x++)
            {
                if (x->claim == "AllowWorkunitScopeView")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Read, defaultWUAccess);
                else if (x->claim == "AllowWorkunitScopeModify")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Write, defaultWUAccess);
                else if (x->claim == "AllowWorkunitScopeDelete")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Full, defaultWUAccess);
                else if (x->claim == "DenyWorkunitScopeView")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Read, defaultWUAccess, true);
                else if (x->claim == "DenyWorkunitScopeModify")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Write, defaultWUAccess, true);
                else if (x->claim == "DenyWorkunitScopeDelete")
                    userInfo->addWUScopePerm(x->scopePattern, SecAccess_Full, defaultWUAccess, true);
            }

            // Add file scope permissions in alphabetical (and implicitly, length) order
            std::sort(fileScopePerms.begin(), fileScopePerms.end());
            for (CollectedScopePerms::const_iterator x = fileScopePerms.begin(); x != fileScopePerms.end(); x++)
            {
                if (x->claim == "AllowFileScopeAccess")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Access, defaultFileAccess);
                else if (x->claim == "AllowFileScopeRead")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Read, defaultFileAccess);
                else if (x->claim == "AllowFileScopeWrite")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Write, defaultFileAccess);
                else if (x->claim == "AllowFileScopeFull")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Full, defaultFileAccess);
                else if (x->claim == "DenyFileScopeAccess")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Access, defaultFileAccess, true);
                else if (x->claim == "DenyFileScopeRead")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Read, defaultFileAccess, true);
                else if (x->claim == "DenyFileScopeWrite")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Write, defaultFileAccess, true);
                else if (x->claim == "DenyFileScopeFull")
                    userInfo->addFileScopePerm(x->scopePattern, SecAccess_Full, defaultFileAccess, true);
            }

            gUserCache.set(subject, userInfo);

            verificationResult = true;
        }
        catch (const std::runtime_error& e)
        {
            // Most jwt-cpp exceptions are subclasses of std::runtime_error
            PROGLOG("CJwtSecurityManager::verifyToken error: %s", e.what());
        }
        catch (const std::logic_error& e)
        {
            PROGLOG("CJwtSecurityManager::logic_error: %s", e.what());
        }
        catch (const std::bad_cast& e)
        {
            PROGLOG("CJwtSecurityManager::bad_cast error: %s", e.what());
        }
        catch (...)
        {
            PROGLOG("CJwtSecurityManager::verifyToken unknown error");
            throw;
        }

        return verificationResult;
    }

    /**
     * Load the contents of the key if necessary.  Contents loaded when
     * at least KEY_FILE_CHECK_INTERVAL have passed since last check time
     *
     * Function will throw an exception on error.
     *
     * @return  true
     */
    bool ensureKeyLoaded()
    {
        if ((keyContentsTimer.elapsedMs() > KEY_FILE_CHECK_INTERVAL_MS) || keyContents.empty())
        {
            try
            {
                StringBuffer    rawKeyContents;

                getSecretValue(rawKeyContents, "esp", secretsName.c_str(), "key", true);

                if (rawKeyContents.isEmpty())
                    throw makeStringExceptionV(-1, "CJwtSecurityManager: Key at path '%s' is empty", secretsName.c_str());

                keyContents = trailingStringTrim(leadingStringTrim(rawKeyContents.str()));
                keyIsPublicKey = isPrefixString("-----BEGIN PUBLIC KEY-----", keyContents);

                keyContentsTimer.reset();
            }
            catch(IException* e)
            {
                StringBuffer msg;
                if (strncmp(e->errorMessage(msg).str(), "CJwtSecurityManager", 19) == 0)
                    throw;
                int code = e->errorCode();
                e->Release();
                throw makeStringExceptionV(code, "CJwtSecurityManager: Exception accessing key at path '%s': %s", secretsName.c_str(), msg.str());
            }
        }

        return true;
    }

    /**
     * @return  New string composed of random characters.  Size of string
     * governed by the NONCE_LEN value defined within the function.
     */
    static std::string generateNonce()
    {
        static const char   nonceLetters[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        static const size_t NONCE_LETTERS_LEN = sizeof(nonceLetters) - 1;
        static const size_t NONCE_LEN = 24;
        char                nonceBuff[NONCE_LEN];

        for (unsigned int x = 0; x < NONCE_LEN; x++)
            nonceBuff[x] = nonceLetters[rand() % NONCE_LETTERS_LEN];

        return std::string(nonceBuff, NONCE_LEN);
    }

    /**
     * Encodes regular token with refresh token in preparation for storage
     * in Dali's K/V store.
     *
     * @param   tokenStr            Value of ID token
     * @param   refreshTokenStr     Value of refresh token
     *
     * @return  New std::string representing encoded data.
     *
     * @see     decodeTokenFromDali
     */
    static std::string encodeTokenForDali(const std::string& tokenStr, const std::string& refreshTokenStr)
    {
        std::string encodedStr = tokenStr + "\t" + refreshTokenStr;

        return encodedStr;
    }

    /**
     * Decodes token data from Dali into ID and refresh tokens.  The tokenStr
     * and refreshTokenStr parameters will be updated with the extracted
     * values.
     *
     * @param   encodedStr          Value of encoded data.
     * @param   tokenStr            Out: value of ID token
     * @param   refreshTokenStr     Out: value of refresh token
     *
     * @return  True if encodedStr was successfully parsed, false otherwise.
     *
     * @see     encodeTokenForDali
     */
    static bool decodeTokenFromDali(const std::string& encodedStr, std::string& tokenStr, std::string& refreshTokenStr)
    {
        std::size_t delimPos = encodedStr.find('\t');

        if (delimPos != std::string::npos)
        {
            tokenStr = encodedStr.substr(0, delimPos);
            refreshTokenStr = encodedStr.substr(delimPos + 1, std::string::npos);

            return true;
        }
        else
        {
            tokenStr.clear();
            refreshTokenStr.clear();
        }

        return false;
    }

    /**
     * Given a response from a JWT endpoint, extract the necessary information
     * from it (including an encoded token) and attempt to verify the contents.
     *
     * Replies to a login or refresh request should follow the OpenID Connect
     * specifications:
     *
     *      On success:  https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse
     *      On error:    https://openid.net/specs/openid-connect-core-1_0.html#TokenErrorResponse
     *
     * Note that the access_token and expires_in values in success replies are ignored.
     *
     * @param   username        The username of the current user
     * @param   nonce           The nonce value generated during a login,
     *                          or empty string (used during refresh)
     * @param   apiResponse     The bytes collected from the JWT endpoint
     * @param   endpointURL     The API service endpoint URL
     *
     * @return  true if response is successfully parsed and validated, false
     *          otherwise
     */
    bool verifyAPIResponse(const std::string& username, const std::string& nonce, const std::string& apiResponse, const std::string& endpointURL)
    {
        nlohmann::json  responseJSON = nlohmann::json::parse(apiResponse);
        std::string     tokenStr = trailingStringTrim(responseJSON["id_token"].get<std::string>());
        std::string     refreshToken = responseJSON["refresh_token"].get<std::string>();

        if (tokenStr.empty())
        {
            // Check for an explicit error condition; we won't return success
            // no matter what, but we may need something to log
            std::string     errorStr = responseJSON["error"].get<std::string>();

            if (!errorStr.empty())
            {
                PROGLOG("CJwtSecurityManager::endpoint error: %s", errorStr.c_str());
            }

            return false;
        }

        if (verifyToken(tokenStr, username, refreshToken, nonce, endpointURL))
        {
            // Successful verification through a JWT endpoint means that we
            // are a 'primary provider' of sorts of the JWT token; push
            // the token to Dali's key/value store
            Owned<ISecUser>     user = createUser(username.c_str());
            std::string         encodedData = encodeTokenForDali(tokenStr, refreshToken);

            daliStore.set(DALI_KVSTORE_STORE_NAME, DALI_KVSTORE_NAMESPACE, DALI_KVSTORE_KEY, encodedData.c_str(), user, false);

            return true;
        }

        return false;
    }

    /**
     * Ensure that current user is authenticated.  Unauthenticated users will trigger
     * a call to the JWT login endpoint, submitted the user's username and
     * credentials.
     *
     * @param   user        ISecUser object containing current user information
     *
     * @return  true if user is already authenticated or has successfully
     *          authenticated against the JWT login endpoint, false otherwise.
     */
    bool authenticate(ISecUser& user)
    {
        std::string username = user.getName();

        if (username.length() == 0)
            throw makeStringException(-1, "CJwtSecurityManager: Username is empty");

        // Already authenticated if token or status set to authenticated
        if (user.credentials().getSessionToken() != 0 || user.getAuthenticateStatus() == AS_AUTHENTICATED)
            return true;

        std::string     nonce = generateNonce();
        std::string     responseStr = tokenFromLogin(loginEndpoint, allowSelfSignedCert, username, user.credentials().getPassword(), clientID, nonce);

        return verifyAPIResponse(username, nonce, responseStr, loginEndpoint);
    }

    /**
     * Retrieves cached user information keyed off of user's username.  If cached
     * information is found then it is checked for expiration according to the
     * expiration time set within JWT token.  Expired tokens trigger a call
     * to the JWT refresh endpoint.  If a token refresh call fails then the
     * user will be logged out.
     *
     * @param   user        ISecUser object containing current user information
     * @param   userInfo    Object populated with found cache information;
     *                      valid only if this method returns true
     *
     * @return  true if user information was found in cache or if the cache
     *          was successfully repopulated after a token refresh,
     *          false otherwise
     */
    std::shared_ptr<JWTUserInfo> populateUserInfoFromUser(ISecUser& user)
    {
        std::shared_ptr<JWTUserInfo> userInfo = gUserCache.get(user.getName());

        if (userInfo.get() == nullptr || !userInfo->isValid())
            return nullptr;

        if (userInfo->isExpired())
        {
            // Remove any cached entries for this user
            gUserCache.erase(user.getName());

            if (refreshEndpoint.empty() || userInfo->getRefreshToken().empty())
            {
                // No refresh endpoint defined or no refresh token; logout user
                logoutUser(user);
                return nullptr;
            }
            else
            {
                std::string apiResponse = tokenFromRefresh(refreshEndpoint, allowSelfSignedCert, clientID, userInfo->getRefreshToken());

                if (!verifyAPIResponse(user.getName(), "", apiResponse, refreshEndpoint))
                {
                    // Could not verify token; logout user
                    logoutUser(user);
                    return nullptr;
                }

                // Reread the newly-populated cache
                userInfo = gUserCache.get(user.getName());
            }
        }

        return userInfo;
    }

public:

    //--------------------------------------------------------
    // ISecManager overrides
    //--------------------------------------------------------

    virtual const char* getDescription() override
    {
        return "JWT Security Manager";
    }

    virtual bool logoutUser(ISecUser& user, IEspSecureContext* secureContext = nullptr) override
    {
        user.setAuthenticateStatus(AS_UNKNOWN);
        user.credentials().setSessionToken(0);

        return true;
    }

    virtual bool authorize(ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext) override
    {
        return authorizeEx(RT_DEFAULT, user, resources, secureContext);
    }

    virtual unsigned getPasswordExpirationWarningDays(IEspSecureContext* secureContext = nullptr) override
    {
        return -2; // never expires
    }

    SecAccessFlags _authorizeEx(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext, JWTUserInfo& userInfo)
    {
        SecAccessFlags accessFlag = SecAccess_None;

        if (resourceName != nullptr)
        {
            switch (rtype)
            {
                case RT_DEFAULT:
                    {
                        accessFlag = userInfo.getFeaturePerm(resourceName, defaultPermAccess);
                    }
                    break;

                case RT_FILE_SCOPE:
                    {
                        // Scope hpccinternal::<username> always has full access for their own scope, but
                        // explicitly denied when attempting to access someone else's
                        // hpccinternal::<username> scope
                        if (resourceName && strncmp(resourceName, "hpccinternal::", 14) == 0)
                        {
                            if (strisame(&resourceName[14], user.getName()))
                                accessFlag = SecAccess_Full;
                            else
                                accessFlag = SecAccess_None;
                        }
                        else
                        {
                            accessFlag = userInfo.matchFileScopePerm(resourceName, defaultFileAccess);
                        }
                    }
                    break;

                case RT_WORKUNIT_SCOPE:
                    {
                        if (resourceName[0] == '\0' || strisame(user.getName(), resourceName))
                            accessFlag = SecAccess_Full;
                        else
                            accessFlag = userInfo.matchWUScopePerm(resourceName, defaultWUAccess);
                    }
                    break;
            }
        }

        return accessFlag;
    }

    virtual bool authorizeEx(SecResourceType rtype, ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext) override
    {
        try
        {
            if (!authenticate(user))
                return false;

            std::shared_ptr<JWTUserInfo> userInfo = populateUserInfoFromUser(user);

            if (userInfo.get() == nullptr)
                return false;

            if (!resources)
                return true;

            for (unsigned int x = 0; x < resources->count(); x++)
            {
                ISecResource* resource = resources->queryResource(x);

                if (resource != nullptr)
                {
                    const char*     resourceName = resource->getName();
                    SecAccessFlags  accessFlag = _authorizeEx(rtype, user, resourceName, secureContext, *userInfo);

                    resource->setAccessFlags(accessFlag);
                }
            }

            return true;
        }
        catch (IException* e)
        {
            EXCLOG(e, "CJwtSecurityManager::authorizeEx(...) exception caught");
            e->Release();
        }
        catch (...)
        {
            // Need better logging
            DBGLOG("CJwtSecurityManager::authorizeEx(...) exception caught");
        }

        return false;
    }

    virtual SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser& user, const char* resourceName, IEspSecureContext* secureContext) override
    {
        SecAccessFlags  resultFlag = SecAccess_Unavailable;

        try
        {
            if (!authenticate(user))
                return SecAccess_None;

            std::shared_ptr<JWTUserInfo> userInfo = populateUserInfoFromUser(user);

            if (userInfo.get() == nullptr)
                return SecAccess_None;

            resultFlag = _authorizeEx(rtype, user, resourceName, secureContext, *userInfo);
        }
        catch (IException* e)
        {
            EXCLOG(e, "CJwtSecurityManager::authorizeEx(...) exception caught");
            e->Release();
        }
        catch (...)
        {
            // Need better logging
            DBGLOG("CJwtSecurityManager::authorizeEx(...) exception caught");
        }

        return resultFlag;
    }

    virtual SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser& user, const char* resourcename, IEspSecureContext* secureContext = nullptr) override
    {
        return authorizeEx(RT_DEFAULT, user, resourcename, secureContext);
    }

    virtual SecAccessFlags authorizeFileScope(ISecUser& user, const char* filescope, IEspSecureContext* secureContext = nullptr) override
    {
        return authorizeEx(RT_FILE_SCOPE, user, filescope, secureContext);
    }

    virtual SecAccessFlags authorizeWorkunitScope(ISecUser& user, const char* wuscope, IEspSecureContext* secureContext = nullptr) override
    {
        if(!wuscope || wuscope[0] == '\0')
            return SecAccess_Full;

        return authorizeEx(RT_WORKUNIT_SCOPE, user, wuscope, secureContext);
    }

    virtual bool authorizeWorkunitScope(ISecUser& user, ISecResourceList* resources, IEspSecureContext* secureContext = nullptr) override
    {
        return authorizeEx(RT_WORKUNIT_SCOPE, user, resources, secureContext);
    }

    virtual bool authorizeViewScope(ISecUser& user, ISecResourceList* resources)
    {
        // View scopes are not restricted by this plugin

        int numResources = resources->count();

        for (int x = 0; x < numResources; x++)
        {
            ISecResource* res = resources->queryResource(x);

            if (res != nullptr)
            {
                assertex(res->getResourceType() == RT_VIEW_SCOPE);
                res->setAccessFlags(SecAccess_Full);
            }
        }

        return true;
    }

    //--------------------------------------------------------
    // IDaliLdapConnection implementation
    //--------------------------------------------------------

    virtual SecAccessFlags getPermissions(const char* key, const char* obj, IUserDescriptor* udesc, unsigned auditflags)
    {
        SecAccessFlags  resultFlag = SecAccess_Unavailable;

        StringBuffer userName;
        udesc->getUserName(userName);

        if (!userName.isEmpty())
        {
            Owned<ISecUser>                 user = createUser(userName);
            std::shared_ptr<JWTUserInfo>    userInfo = gUserCache.get(userName.str());

            if (userInfo.get() == nullptr || !userInfo->isValid())
            {
                // This secmgr plugin running within a Dali client connection
                // does not have accesss to user passwords so it cannot
                // authenticate against the JWT endpoint; if the token
                // is not in the cache or it is invalid then we have to
                // try to load it from Dali's key/value store
                StringBuffer        foundEncodedToken;

                if (daliStore.fetch(DALI_KVSTORE_STORE_NAME, DALI_KVSTORE_NAMESPACE, DALI_KVSTORE_KEY, foundEncodedToken, user, false))
                {
                    std::string         foundToken;
                    std::string         foundRefreshToken;

                    decodeTokenFromDali(foundEncodedToken.str(), foundToken, foundRefreshToken);

                    // Validate the token, which also caches it if
                    // validation passes
                    if (verifyToken(foundToken, userName.str(), foundRefreshToken, "", ""))
                    {
                        // Load parsed token out of cache
                        userInfo = gUserCache.get(userName.str());
                    }
                    else
                    {
                        return SecAccess_Unavailable;
                    }
                }
                else
                {
                    // No token in the K/V store;
                    return SecAccess_Unavailable;
                }
            }

            if (strisame(key, "Scope"))
                resultFlag = _authorizeEx(RT_FILE_SCOPE, *user, obj, nullptr, *userInfo);
            else if (strisame(key, "workunit"))
                resultFlag = _authorizeEx(RT_WORKUNIT_SCOPE, *user, obj, nullptr, *userInfo);
        }

        return resultFlag;
    }

    virtual bool checkScopeScans()
    {
        return true;
    }

    virtual unsigned getLDAPflags()
    {
        return DLF_ENABLED | DLF_SCOPESCANS; // | DLF_SAFE;
    }

    virtual void setLDAPflags(unsigned flags)
    {
        // Do nothing
    }

    virtual bool clearPermissionsCache(IUserDescriptor *udesc)
    {
        return true;
    }

    virtual bool enableScopeScans(IUserDescriptor *udesc, bool enable, int *err)
    {
        return true;
    }


private:
    mutable CriticalSection     crit;                       //!< Protects access to secretsName contents
    std::string                 clientID;                   //!< URL or unique name of the current cluster; from configuration
    std::string                 loginEndpoint;              //!< Full URL to login endpoint; from configuration
    std::string                 refreshEndpoint;            //!< Full URL to refresh endpoint; from configuration
    bool                        allowSelfSignedCert;        //!< true = allow self-signed certificate; from configuration
    std::string                 secretsName;                //!< Secrets vault space or subdirectory name containing key; from configuration; @see ensureKeyLoaded()
    CCycleTimer                 keyContentsTimer;           //!< Timer governing reload of key; @see ensureKeyLoaded()
    SecAccessFlags              defaultPermAccess;          //!< Default permission value for permissions; from configuration
    SecAccessFlags              defaultWUAccess;            //!< Default permission value for workunit scopes; from configuration
    SecAccessFlags              defaultFileAccess;          //!< Default permission value for file scopes; from configuration
    std::string                 keyContents;                //!< Contents of secret key; @see ensureKeyLoaded()
    bool                        keyIsPublicKey;             //!< True if keyContents contains a public key, false otherwise
    CDALIKVStore                daliStore;                  //!< Handle to Dali's key/value store (external token cache)
    static const SecFeatureSet  implementedFeaturesMask = SMF_Authorize
                                                            | SMF_AuthorizeEx_Named
                                                            | SMF_AuthorizeFileScope_List
                                                            | SMF_AuthorizeFileScope_Named
                                                            | SMF_AuthorizeWorkUnitScope_List
                                                            | SMF_AuthorizeWorkUnitScope_Named
                                                            | SMF_CreateAuthMap
                                                            | SMF_CreateFeatureMap
                                                            | SMF_GetAccessFlagsEx
                                                            | SMF_GetDescription
                                                            | SMF_GetPasswordExpirationDays
                                                            | SMF_LogoutUser
                                                            | SMF_QuerySecMgrType
                                                            | SMF_QuerySecMgrTypeName;              //!< Bitmask of features implemented in this plugin
    static const SecFeatureSet  safeFeaturesMask = implementedFeaturesMask | SMF_CreateSettingMap;  //!< Bitmask of safe features implemented in this plugin
};

// Reinstate the old macro value
#ifdef JWT_HAS_VERIFY_MACRO
    #define verify OLD_VERIFY
    #undef OLD_VERIFY
    #undef JWT_HAS_VERIFY_MACRO
#endif

extern "C"
{
    JWTSECURITY_API ISecManager* createInstance(const char* serviceName, IPropertyTree& secMgrCfg, IPropertyTree& bndCfg)
    {
        return new CJwtSecurityManager(serviceName, &secMgrCfg, &bndCfg);
    }
}

// NOTE: MODULE_INIT and MODULE_EXIT code located in jwtEndpoint.cpp
