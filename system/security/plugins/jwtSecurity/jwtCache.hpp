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

#ifndef JWTCACHE_HPP_
#define JWTCACHE_HPP_

#include <map>
#include <string>

#include "basesecurity.hpp"

/**
 * Instances of JWTUserInfo represent all of the cached values for a particular
 * user.  The intention is that instances would be accessed via another
 * container, such as a map keyed off the user's username.  See JWTUserCache class.
 */
class JWTUserInfo
{
    private:

        typedef std::map<std::string, SecAccessFlags> PermissionMap;

    public:

        JWTUserInfo();

        ~JWTUserInfo();

        /**
         * @return  true if the object is valid, false otherwise
         */
        bool isValid() const;

        /**
         * @return  The expiration time for the user info as the number of
         *          seconds after epoch in UTC.
         *
         * @see     isExpired
         * @see     setExpirationTime
         */
        time_t getExpirationTime() const;

        /**
         * Sets the expiration tme for the user info.
         *
         * @param   newExpTime      Expiration time as the number of
         *                          seconds after epoch in UTC.
         *
         * @return  Reference to update object (this).
         *
         * @see     getExpirationTime
         * @see     isExpired
         */
        JWTUserInfo& setExpirationTime(time_t newExpTime);

        /**
         * @return  true if the save expiration is prior to the current time,
         *          false otherwise.
         *
         * @see     getExpirationTime
         * @see     setExpirationTime
         */
        bool isExpired() const;

        /**
         * @return  Copy of the saved refresh token.
         *
         * @see     setRefreshToken
         */
        std::string getRefreshToken() const;

        /**
         * Saves a new refresh token in the object.
         *
         * @param   newRefreshToken     The token to save
         *
         * @return  Reference to updated object (this).
         *
         * @see     getRefreshToken
         */
        JWTUserInfo& setRefreshToken(const std::string& newRefreshToken);

        /**
         * @return  Copy of the saved authentication token.  This
         *          could be used to authenticate the associated
         *          user with another JWT-aware service.
         *
         * @see     setJWTToken
         */
        std::string getJWTToken() const;

        /**
         * Saves a new authentication token in the object.
         *
         * @param   newJWTToken     The token to save
         *
         * @return  Reference to updated object (this).
         *
         * @see     getJWTToken
         */
        JWTUserInfo& setJWTToken(const std::string& newJWTToken);

        /**
         * @param   permName        A permission name/key
         *
         * @return  true if there is any kind of entry for permName in the
         *          object's table, false otherwise
         *
         * @see     getFeaturePerm
         * @see     mergeFeaturePerm
         * @see     setFeaturePerm
         */
        bool hasFeaturePerm(const std::string& permName) const;

        /**
         * Set the value for a feature permission, overwriting an
         * existing value, if any.
         *
         * @param   permName        A permission name/key
         * @param   accessFlag      The flag value to save
         *
         * @return  Reference to updated object (this).
         *
         * @see     getFeaturePerm
         * @see     hasFeaturePerm
         * @see     mergeFeaturePerm
         */
        JWTUserInfo& setFeaturePerm(const std::string& permName, SecAccessFlags accessFlag);

        /**
         * Set the value for a feature permission, overwriting an
         * existing value, if any.
         *
         * @param   permName        A permission name/key
         * @param   accessFlagName  The name of a flag value to save; must be one of
         *                          [None, Access, Read, Write, Full]; case-sensitive
         *
         * @return  Reference to updated object (this).
         *
         * @see     getFeaturePerm
         * @see     hasFeaturePerm
         * @see     mergeFeaturePerm
         */
        JWTUserInfo& setFeaturePerm(const std::string& permName, const std::string& accessFlagName);

        /**
         * Modify the value for a feature permission.  If a permission did not exist
         * previously then the permission takes on the given value; if the permission
         * did exist then the new valuee is OR'd with the old value.
         *
         * @param   permName        A permission name/key
         * @param   accessFlag      The flag value to save
         *
         * @return  Reference to updated object (this).
         *
         * @see     getFeaturePerm
         * @see     hasFeaturePerm
         * @see     setFeaturePerm
         */
        JWTUserInfo& mergeFeaturePerm(const std::string& permName, SecAccessFlags accessFlag);

        /**
         * Modify the value for a feature permission.  If a permission did not exist
         * previously then the permission takes on the given value; if the permission
         * did exist then the new valuee is OR'd with the old value.
         *
         * @param   permName        A permission name/key
         * @param   accessFlagName  The name of a flag value to save; must be one of
         *                          [None, Access, Read, Write, Full]; case-sensitive
         *
         * @return  Reference to updated object (this).
         *
         * @see     getFeaturePerm
         * @see     hasFeaturePerm
         * @see     setFeaturePerm
         */
        JWTUserInfo& mergeFeaturePerm(const std::string& permName, const std::string& accessFlagName);

        /**
         * Returns the value for a feature permission if one is found, or the given default
         * value if not found.
         *
         * @param   permName        A permission name/key
         * @param   defaultFlag     The value to return if permName is not found in the object
         *
         * @return  A previously-saved permission value or the value of defaultFlag if the
         *          permission had not been saved.
         *
         * @see     hasFeaturePerm
         * @see     mergeFeaturePerm
         * @see     setFeaturePerm
         */
        SecAccessFlags getFeaturePerm(const std::string& permName, SecAccessFlags defaultFlag) const;

        /**
         * Erase a permission value from the object.  Does nothing when the given permission
         * does not exist.
         *
         * @param   permName        A permission name/key
         *
         * @return  Reference to updated object (this).
         *
         * @see     clearFeaturePerms
         */
        JWTUserInfo& eraseFeaturePerm(const std::string& permName);

        /**
         * Erase all permission values from the object.
         *
         * @return  Reference to updated object (this).
         *
         * @see     eraseFeaturePerm
         */
        JWTUserInfo& clearFeaturePerms();

        /**
         * @return  The number of permissions currently saved.
         */
        unsigned int countFeaturePerms() const;

        /**
         * Merge the value for a workunit scope permission with the most-applicable
         * workunit scope (if any) and saves it.
         *
         * @param   scope           A workunit scope pattern
         * @param   accessFlag      The flag value to save
         * @param   defaultFlag     The default flag value; used only when asDeny
         *                          is true and no previous scope flag is found
         * @param   asDeny          If true, treat accessFlag as a denial of
         *                          that permission; if false, treat accessFlag
         *                          as a grant of that permission; defaults to
         *                          false
         *
         * @return  Reference to updated object (this).
         *
         * @see     matchWUScopePerm
         */
        JWTUserInfo& addWUScopePerm(const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny = false);

        /**
         * Merge the value for a workunit scope permission with the most-applicable
         * workunit scope (if any) and saves it.
         *
         * @param   scope           A workunit scope pattern
         * @param   accessFlagName  The name of a flag value to save; must be one of
         *                          [None, Access, Read, Write, Full]; case-sensitive
         * @param   defaultFlag     The default flag value; used only when asDeny
         *                          is true and no previous scope flag is found
         * @param   asDeny          If true, treat accessFlagName as a denial of
         *                          that permission; if false, treat accessFlagName
         *                          as a grant of that permission; defaults to
         *                          false
         *
         * @return  Reference to updated object (this).
         *
         * @see     matchWUScopePerm
         */
        JWTUserInfo& addWUScopePerm(const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny = false);

        /**
         * Returns the value for a workunit scope permission if one is found or the given default
         * value if not found.  A match is determined using the same mechanism as is used for
         * filename globbing in Linux.  In the event of multiple matches, the longest match wins.
         *
         * @param   scope           A workunit scope
         * @param   defaultFlag     The value to return if scope is not found in the object
         *
         * @return  A previously-saved permission value or the value of defaultFlag if a matching
         *          scope cannot be found.
         *
         * @see     addWUScopePerm
         */
        SecAccessFlags matchWUScopePerm(const std::string& scope, SecAccessFlags defaultFlag) const;

        /**
         * Erase a workunit scope value from the object.  Does nothing when the given scope
         * does not exist.
         *
         * @param   scope           A workunit scope pattern
         *
         * @return  Reference to updated object (this).
         *
         * @see     clearWUScopePerm
         */
        JWTUserInfo& eraseWUScopePerm(const std::string& scope);

        /**
         * Erase all workunit scope values from the object.
         *
         * @return  Reference to updated object (this).
         *
         * @see     eraseWUScopePerm
         */
        JWTUserInfo& clearWUScopePerm();

        /**
         * @return  The number of workunit scopes currently saved.
         */
        unsigned int countWUScopePerms() const;

        /**
         * @return  True if any workunit scopes have been set, false otherwise.
         */
        bool hasWUScopePerms() const;

        /**
         * Merge the value for a file scope permission with the most-applicable
         * file scope (if any) and saves it.
         *
         * @param   scope           A file scope pattern
         * @param   accessFlag      The flag value to save
         * @param   defaultFlag     The default flag value; used only when asDeny
         *                          is true and no previous scope flag is found
         * @param   asDeny          If true, treat accessFlag as a denial of
         *                          that permission; if false, treat accessFlag
         *                          as a grant of that permission; defaults to
         *                          false
         *
         * @return  Reference to updated object (this).
         *
         * @see     matchFileScopePerm
         */
        JWTUserInfo& addFileScopePerm(const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny = false);

        /**
         * Merge the value for a fil scope permission with the most-applicable
         * file scope (if any) and saves it.
         *
         * @param   scope           A file scope pattern
         * @param   accessFlagName  The name of a flag value to save; must be one of
         *                          [None, Access, Read, Write, Full]; case-sensitive
         * @param   defaultFlag     The default flag value; used only when asDeny
         *                          is true and no previous scope flag is found
         * @param   asDeny          If true, treat accessFlagName as a denial of
         *                          that permission; if false, treat accessFlagName
         *                          as a grant of that permission; defaults to
         *                          false
         *
         * @return  Reference to updated object (this).
         *
         * @see     matchFileScopePerm
         */
        JWTUserInfo& addFileScopePerm(const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny = false);

        /**
         * Returns the value for a file scope permission if one is found or the given default
         * value if not found.  A match is determined using the same mechanism as is used for
         * filename globbing in Linux.  In the event of multiple matches, the longest match wins.
         *
         * @param   scope           A file scope
         * @param   defaultFlag     The value to return if scope is not found in the object
         *
         * @return  A previously-saved permission value or the value of defaultFlag if a matching
         *          scope cannot be found.
         *
         * @see     addFileScopePerm
         */
        SecAccessFlags matchFileScopePerm(const std::string& scope, SecAccessFlags defaultFlag) const;

        /**
         * Erase a file scope value from the object.  Does nothing when the given scope
         * does not exist.
         *
         * @param   scope           A file scope pattern
         *
         * @return  Reference to updated object (this).
         *
         * @see     clearFileScopePerm
         */
        JWTUserInfo& eraseFileScopePerm(const std::string& scope);

        /**
         * Erase all file scope values from the object.
         *
         * @return  Reference to updated object (this).
         *
         * @see     eraseFileScopePerm
         */
        JWTUserInfo& clearFileScopePerm();

        /**
         * @return  The number of file scopes currently saved.
         */
        unsigned int countFileScopePerms() const;

        /**
         * @return  True if any file scopes have been set, false otherwise.
         */
        bool hasFileScopePerms() const;

    private:

        std::string _globToExample(const std::string& pattern) const;
        std::string _convertPathname(const std::string& logicalPath) const;
        JWTUserInfo& _addScopePerm(PermissionMap& scopePermMap, const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny);
        JWTUserInfo& _addScopePerm(PermissionMap& scopePermMap, const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny);
        SecAccessFlags _matchScopePerm(const PermissionMap& scopePermMap, const std::string& scope, SecAccessFlags defaultFlag) const;
        JWTUserInfo& _eraseScopePerm(PermissionMap& scopePermMap, const std::string& scope);
        JWTUserInfo& _clearScopePerm(PermissionMap& scopePermMap);
        unsigned int _countScopePerms(const PermissionMap& scopePermMap) const;

    private:

        time_t                  expireTimeUTC;      //!< Time this object expires
        std::string             refreshToken;       //!< The refresh token associated with jwtToken
        std::string             jwtToken;           //!< Raw token provided during authentication; suitable for passing to another service
        PermissionMap           featurePermMap;     //!< Map of FeatureName -> PermissionFlag
        PermissionMap           wuScopePermMap;     //!< Map of ScopePattern -> PermissionFlag
        PermissionMap           fileScopePermMap;   //!< Map of ScopePattern -> PermissionFlag
};

/**
 * This is basically a thread-safe map of username->JWTUserInfo objects.
 */
class JWTUserCache
{
    private:

        typedef std::map<std::string, std::shared_ptr<JWTUserInfo> > UserPermissionMap;

    public:

        bool has(const std::string& userName) const;

        JWTUserCache& set(const std::string& userName, std::shared_ptr<JWTUserInfo>& userInfo);

        const std::shared_ptr<JWTUserInfo> get(const std::string& userName) const;

        JWTUserCache& erase(const std::string& userName);

        JWTUserCache& clear();

        unsigned int count() const;

    private:

        mutable CriticalSection     crit;           //!< Used to prevent thread collisions during userPermMap modification
        UserPermissionMap           userPermMap;    //!< Map of UserName -> Permissions
};

#endif // JWTCACHE_HPP_
