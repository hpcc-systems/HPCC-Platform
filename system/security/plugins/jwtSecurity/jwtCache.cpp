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
#include <fnmatch.h>
#include <iostream>
#include <regex>

#include "jlog.hpp"

#include "jwtCache.hpp"

JWTUserInfo::JWTUserInfo()
    :   expireTimeUTC(0)
{
}

JWTUserInfo::~JWTUserInfo()
{
}

bool JWTUserInfo::isValid() const
{
    return expireTimeUTC > 0;
}

time_t JWTUserInfo::getExpirationTime() const
{
    return expireTimeUTC;
}

JWTUserInfo& JWTUserInfo::setExpirationTime(time_t newExpTime)
{
    expireTimeUTC = newExpTime;

    return *this;
}

bool JWTUserInfo::isExpired() const
{
    time_t  timeNow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    return isValid() && std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) > expireTimeUTC;
}

std::string JWTUserInfo::getRefreshToken() const
{
    return refreshToken;
}

JWTUserInfo& JWTUserInfo::setRefreshToken(const std::string& newRefreshToken)
{
    refreshToken = newRefreshToken;

    return *this;
}

std::string JWTUserInfo::getJWTToken() const
{
    return jwtToken;
}

JWTUserInfo& JWTUserInfo::setJWTToken(const std::string& newJWTToken)
{
    jwtToken = newJWTToken;

    return *this;
}

//-----------------------------------------------------------------------------

bool JWTUserInfo::hasFeaturePerm(const std::string& permName) const
{
    return featurePermMap.find(permName) != featurePermMap.end();
}

JWTUserInfo& JWTUserInfo::setFeaturePerm(const std::string& permName, SecAccessFlags accessFlag)
{
    featurePermMap[permName] = accessFlag;

    return *this;
}

JWTUserInfo& JWTUserInfo::setFeaturePerm(const std::string& permName, const std::string& accessFlagName)
{
    return setFeaturePerm(permName, getSecAccessFlagValue(accessFlagName.c_str()));
}

JWTUserInfo& JWTUserInfo::mergeFeaturePerm(const std::string& permName, SecAccessFlags accessFlag)
{
    PermissionMap::const_iterator   foundIter = featurePermMap.find(permName);

    if (foundIter != featurePermMap.end())
        featurePermMap[permName] = static_cast<SecAccessFlags>(foundIter->second | accessFlag);
    else
        featurePermMap[permName] = accessFlag;

    return *this;
}

JWTUserInfo& JWTUserInfo::mergeFeaturePerm(const std::string& permName, const std::string& accessFlagName)
{
    return mergeFeaturePerm(permName, getSecAccessFlagValue(accessFlagName.c_str()));
}

SecAccessFlags JWTUserInfo::getFeaturePerm(const std::string& permName, SecAccessFlags defaultFlag) const
{
    SecAccessFlags                  accessValue = defaultFlag;
    PermissionMap::const_iterator   foundIter = featurePermMap.find(permName);

    if (foundIter != featurePermMap.end())
        accessValue = foundIter->second;

    return accessValue;
}

JWTUserInfo& JWTUserInfo::eraseFeaturePerm(const std::string& permName)
{
    featurePermMap.erase(permName);

    return *this;
}

JWTUserInfo& JWTUserInfo::clearFeaturePerms()
{
    featurePermMap.clear();

    return *this;
}

unsigned int JWTUserInfo::countFeaturePerms() const
{
    return featurePermMap.size();
}

//-----------------------------------------------------------------------------

JWTUserInfo& JWTUserInfo::addWUScopePerm(const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny)
{
    return _addScopePerm(wuScopePermMap, scope, accessFlag, defaultFlag, asDeny);
}

JWTUserInfo& JWTUserInfo::addWUScopePerm(const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny)
{
    return _addScopePerm(wuScopePermMap, scope, getSecAccessFlagValue(accessFlagName.c_str()), defaultFlag, asDeny);
}

SecAccessFlags JWTUserInfo::matchWUScopePerm(const std::string& scope, SecAccessFlags defaultFlag) const
{
    return _matchScopePerm(wuScopePermMap, scope, defaultFlag);
}

JWTUserInfo& JWTUserInfo::eraseWUScopePerm(const std::string& scope)
{
    return _eraseScopePerm(wuScopePermMap, scope);
}

JWTUserInfo& JWTUserInfo::clearWUScopePerm()
{
    return _clearScopePerm(wuScopePermMap);
}

unsigned int JWTUserInfo::countWUScopePerms() const
{
    return _countScopePerms(wuScopePermMap);
}

bool JWTUserInfo::hasWUScopePerms() const
{
    return !wuScopePermMap.empty();
}

//-----------------------------------------------------------------------------

JWTUserInfo& JWTUserInfo::addFileScopePerm(const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny)
{
    return _addScopePerm(fileScopePermMap, scope, accessFlag, defaultFlag, asDeny);
}

JWTUserInfo& JWTUserInfo::addFileScopePerm(const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny)
{
    return _addScopePerm(fileScopePermMap, scope, getSecAccessFlagValue(accessFlagName.c_str()), defaultFlag, asDeny);
}

SecAccessFlags JWTUserInfo::matchFileScopePerm(const std::string& scope, SecAccessFlags defaultFlag) const
{
    return _matchScopePerm(fileScopePermMap, scope, defaultFlag);
}

JWTUserInfo& JWTUserInfo::eraseFileScopePerm(const std::string& scope)
{
    return _eraseScopePerm(fileScopePermMap, scope);
}

JWTUserInfo& JWTUserInfo::clearFileScopePerm()
{
    return _clearScopePerm(fileScopePermMap);
}

unsigned int JWTUserInfo::countFileScopePerms() const
{
    return _countScopePerms(fileScopePermMap);
}

bool JWTUserInfo::hasFileScopePerms() const
{
    return !fileScopePermMap.empty();
}

//-----------------------------------------------------------------------------
// Private methods
//-----------------------------------------------------------------------------

// regex patterns for converting a filename glob pattern
// to an example string
static const std::regex _globRegex1("\\[![0-9].+?\\]\\*?");                 // foo[!1] -> fooa, foo[!1]* -> fooa
static const std::regex _globRegex2("\\[([0-9]|![A-Za-z]).+?\\]\\*?");      // foo[1] -> foo1, foo[1]* -> foo1, foo[!m] -> foo1, foo[!m]* -> foo1
static const std::regex _globRegex3("\\[[A-Za-z].+?\\]\\*?");               // foo[m] -> fooa, foo[m]* -> fooa
static const std::regex _globRegex4("[*?]");                                // foo* -> fooa, foo? -> fooa

std::string JWTUserInfo::_globToExample(const std::string& pattern) const
{
    std::string s1 = std::regex_replace(pattern, _globRegex1, "a");
    std::string s2 = std::regex_replace(s1, _globRegex2, "1");
    std::string s3 = std::regex_replace(s2, _globRegex3, "a");
    std::string s4 = std::regex_replace(s3, _globRegex4, "a");

    return s4;
}

std::string JWTUserInfo::_convertPathname(const std::string& logicalPath) const
{
    std::string newPath(std::regex_replace(logicalPath, std::regex("::"), "/"));

    if (!newPath.empty())
    {
        if (newPath.back() == '/')
            newPath += "*";
        else if (newPath.back() != '*')
            newPath += "/*";
    }

    return newPath;
}

JWTUserInfo& JWTUserInfo::_addScopePerm(PermissionMap& scopePermMap, const std::string& scope, SecAccessFlags accessFlag, SecAccessFlags defaultFlag, bool asDeny)
{
    std::string                 rewrittenScope(_convertPathname(scope));
    PermissionMap::iterator     foundIter = scopePermMap.find(rewrittenScope);

    if (foundIter != scopePermMap.end())
    {
        // Exact match on scope; apply access to existing bitmask
        if (asDeny)
            foundIter->second = static_cast<SecAccessFlags>(foundIter->second & ~accessFlag);
        else
            foundIter->second = static_cast<SecAccessFlags>(foundIter->second | accessFlag);
    }
    else
    {
        // Find most-applicable scope access value and apply new access to that bitmask
        SecAccessFlags  existingFlags = _matchScopePerm(scopePermMap, scope, defaultFlag);

        if (asDeny)
            scopePermMap[rewrittenScope] = static_cast<SecAccessFlags>(existingFlags & ~accessFlag);
        else
            scopePermMap[rewrittenScope] = static_cast<SecAccessFlags>(existingFlags | accessFlag);
    }

    return *this;
}

JWTUserInfo& JWTUserInfo::_addScopePerm(PermissionMap& scopePermMap, const std::string& scope, const std::string& accessFlagName, SecAccessFlags defaultFlag, bool asDeny)
{
    return _addScopePerm(scopePermMap, scope, getSecAccessFlagValue(accessFlagName.c_str()), defaultFlag, asDeny);
}

SecAccessFlags JWTUserInfo::_matchScopePerm(const PermissionMap& scopePermMap, const std::string& scope, SecAccessFlags defaultFlag) const
{
    // Ideally, this function should be traversing a tree of scope permissions, with each
    // scope level a tree node; that would be much faster and would also avoid requiring
    // ordering multiple patterns prior to calling _addScopePerm()
    std::string         scopeAsExample(_convertPathname(_globToExample(scope)));
    SecAccessFlags      accessValue = defaultFlag;
    size_t              longestScopeLen = 0;

    for (PermissionMap::const_iterator x = scopePermMap.begin(); x != scopePermMap.end(); x++)
    {
        if (::fnmatch(x->first.c_str(), scopeAsExample.c_str(), (FNM_CASEFOLD | FNM_LEADING_DIR)) == 0)
        {
            // Use only the longest (most precise) scope
            if (x->first.size() > longestScopeLen)
            {
                longestScopeLen = x->first.size();
                accessValue = x->second;
            }
        }
    }

    return accessValue;
}

JWTUserInfo& JWTUserInfo::_eraseScopePerm(PermissionMap& scopePermMap, const std::string& scope)
{
    std::string rewrittenScope(_convertPathname(scope));

    scopePermMap.erase(rewrittenScope);

    return *this;
}

JWTUserInfo& JWTUserInfo::_clearScopePerm(PermissionMap& scopePermMap)
{
    scopePermMap.clear();

    return *this;
}

unsigned int JWTUserInfo::_countScopePerms(const PermissionMap& scopePermMap) const
{
    return scopePermMap.size();
}

//=============================================================================

bool JWTUserCache::has(const std::string& userName) const
{
    CriticalBlock block(crit);
    return userPermMap.find(userName) != userPermMap.end();
}

JWTUserCache& JWTUserCache::set(const std::string& userName, std::shared_ptr<JWTUserInfo>& userInfo)
{
    {
        CriticalBlock block(crit);

        userPermMap[userName] = userInfo;
    }

    return *this;
}

const std::shared_ptr<JWTUserInfo> JWTUserCache::get(const std::string& userName) const
{
    {
        CriticalBlock                       block(crit);
        UserPermissionMap::const_iterator   foundIter = userPermMap.find(userName);

        if (foundIter != userPermMap.end())
        {
            return foundIter->second;
        }
    }

    return nullptr;
}

JWTUserCache& JWTUserCache::erase(const std::string& userName)
{
    {
        CriticalBlock block(crit);

        userPermMap.erase(userName);
    }

    return *this;
}

JWTUserCache& JWTUserCache::clear()
{
    {
        CriticalBlock block(crit);

        userPermMap.clear();
    }

    return *this;
}

unsigned int JWTUserCache::count() const
{
    return userPermMap.size();
}
