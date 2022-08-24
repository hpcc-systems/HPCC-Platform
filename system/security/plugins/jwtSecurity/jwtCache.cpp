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

std::string JWTUserInfo::_convertPathname(std::string logicalPath) const
{
    if (!logicalPath.empty())
    {
        char lastChar = logicalPath.back();

        if (lastChar == ':')
        {
            logicalPath += "*";
        }
        else if (lastChar != '*' && lastChar != '?')
        {
            logicalPath += "::*";
        }
    }

    return std::regex_replace(logicalPath, std::regex("\\*"), "[^:]*");
}

unsigned int JWTUserInfo::_countDelimitersInPath(const std::string& path) const
{
    unsigned int    count = 0;
    std::string     delim("::");

    for (size_t offset = path.find(delim); offset != std::string::npos; offset = path.find(delim, offset + delim.length()))
    {
        ++count;
    }

    return count;
}

void JWTUserInfo::_clampPathByDelimCount(const std::string& path, unsigned int delimCount, std::string& clampedPath) const
{
    unsigned int    count = 0;
    std::string     delim("::");

    for (size_t offset = path.find(delim); offset != std::string::npos; offset = path.find(delim, offset + delim.length()))
    {
        if (count < delimCount)
        {
            ++count;
        }
        else
        {
            clampedPath = path.substr(0, offset);
            return;
        }
    }

    // If we got here then we never found delimCount number of delimiters
    clampedPath = path;
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
    SecAccessFlags              accessValue = defaultFlag;
    size_t                      scopeDelimCount = _countDelimitersInPath(scope);
    std::string                 clampedKey;
    size_t                      longestScopePartCount = 0;

    for (PermissionMap::const_iterator x = scopePermMap.begin(); x != scopePermMap.end(); x++)
    {
        _clampPathByDelimCount(x->first, scopeDelimCount, clampedKey);

        // Convert key to a regex pattern anchored at the beginning of the string
        std::regex scopePattern(std::string("^") + clampedKey, std::regex_constants::icase);

        if (std::regex_search(scope, scopePattern))
        {
            unsigned int partCount = _countDelimitersInPath(x->first) + 1;

            // Use only the longest (most precise?) scope
            if (partCount > longestScopePartCount)
            {
                longestScopePartCount = partCount;
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

bool JWTUserCache::has(const std::string& userName, time_t expireTime) const
{
    CriticalBlock                       block(crit);
    UserPermissionMap::const_iterator   foundIter = userPermMap.find(userName);

    if (foundIter != userPermMap.end())
    {
        time_t  savedTime;

        std::tie(savedTime, std::ignore) = foundIter->second;

        return expireTime < savedTime;
    }

    return false;
}

JWTUserCache& JWTUserCache::set(const std::string& userName, std::shared_ptr<JWTUserInfo>& userInfo)
{
    {
        CriticalBlock   block(crit);
        UserInfoTuple   info = std::make_tuple(time(nullptr), userInfo);

        userPermMap[userName] = info;
    }

    return *this;
}

const std::shared_ptr<JWTUserInfo> JWTUserCache::get(const std::string& userName, time_t expireTime) const
{
    {
        CriticalBlock                       block(crit);
        UserPermissionMap::const_iterator   foundIter = userPermMap.find(userName);

        if (foundIter != userPermMap.end())
        {
            time_t                          savedTime;
            std::shared_ptr<JWTUserInfo>    userInfo;

            std::tie(savedTime, userInfo) = foundIter->second;

            if (expireTime < savedTime)
                return userInfo;
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
