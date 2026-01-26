// filepath: /home/krowland/hpcc/src/system/security/LdapSecurity/failedAuthCache.hpp
#pragma once

#include <unordered_map>
#include <string>
#include <ctime>
#include <algorithm>

#include "jlog.hpp"
#include "jmutex.hpp"

struct FailedAuthEntry
{
    time_t firstFailureTime;
    unsigned failedAttempts;

    FailedAuthEntry() : firstFailureTime(0), failedAttempts(0) {}
    FailedAuthEntry(time_t timestamp, unsigned attempts)
        : firstFailureTime(timestamp), failedAttempts(attempts) {}
};

class FailedAuthCache
{
public:
    static constexpr unsigned defaultMaxFailedAttempts = 5;
    static constexpr unsigned defaultCacheTimeoutSeconds = 300; // 5 minutes
    static constexpr unsigned defaultMaxAllowedEntries = 25;

    FailedAuthCache(unsigned maxFailedAttempts = defaultMaxFailedAttempts,
                    unsigned cacheTimeoutSeconds = defaultCacheTimeoutSeconds,
                    unsigned maxAllowedEntries = defaultMaxAllowedEntries)
        : m_maxFailedAttempts(maxFailedAttempts),
          m_cacheTimeoutSeconds(cacheTimeoutSeconds),
          m_maxAllowedEntries(maxAllowedEntries)
    {
    }

    // Check if a user should be blocked due to failed authentication.
    // Returns true if authentication should be blocked.
    bool isUserLockedOut(const char* username)
    {
        if (!username || !*username)
            return false;

        CriticalBlock block(m_lock);

        auto it = m_cache.find(username);
        if (it == m_cache.end())
            return false;

        time_t currentTime = time(nullptr);
        time_t elapsedTime = currentTime - it->second.firstFailureTime;

        if (elapsedTime >= m_cacheTimeoutSeconds)
        {
            m_cache.erase(it);
            trimFailedAuthCache();
            return false;
        }

        if (it->second.failedAttempts >= m_maxFailedAttempts)
            return true;

        return false;
    }

    // Record a failed authentication attempt for username.
    void updateUserLockoutStatus(const char* username)
    {
        if (!username || !*username)
            return;

        CriticalBlock block(m_lock);

        auto it = m_cache.find(username);
        if (it == m_cache.end())
        {
            time_t currentTime = time(nullptr);
            m_cache[username] = FailedAuthEntry(currentTime, 1);
            trimFailedAuthCache();
        }
        else
        {
            ++(it->second.failedAttempts);
            if (it->second.failedAttempts == m_maxFailedAttempts)
                OWARNLOG("User %s locked out for %u seconds after reaching maximum failed authentication attempts of %u", username, m_cacheTimeoutSeconds, m_maxFailedAttempts);
        }
    }

    // Remove a user from the failed auth cache (e.g., on successful authentication).
    void removeUser(const char* username)
    {
        if (!username || !*username)
            return;

        CriticalBlock block(m_lock);

        auto it = m_cache.find(username);
        if (it != m_cache.end())
        {
            m_cache.erase(it);
            trimFailedAuthCache();
        }
    }

    // Optional setters/getters
    void setMaxFailedAttempts(unsigned v) { m_maxFailedAttempts = v; }
    void setCacheTimeoutSeconds(unsigned v) { m_cacheTimeoutSeconds = v; }
    void setMaxAllowedEntries(unsigned v) { m_maxAllowedEntries = v; }

    unsigned getMaxFailedAttempts() const { return m_maxFailedAttempts; }
    unsigned getCacheTimeoutSeconds() const { return m_cacheTimeoutSeconds; }
    unsigned getMaxAllowedEntries() const { return m_maxAllowedEntries; }

    // Clear entire cache
    void clear()
    {
        CriticalBlock block(m_lock);
        m_cache.clear();
    }

private:
    void trimFailedAuthCache()
    {
        // Expect lock is held by caller
        time_t currentTime = time(nullptr);

        // Remove expired entries
        for (auto it = m_cache.begin(); it != m_cache.end();)
        {
            if ((currentTime - it->second.firstFailureTime) > m_cacheTimeoutSeconds)
                it = m_cache.erase(it);
            else
                ++it;
        }

        // If still too large, remove oldest entries
        while (m_cache.size() > m_maxAllowedEntries)
        {
            auto oldestIt = std::min_element(
                m_cache.begin(),
                m_cache.end(),
                [](const auto& a, const auto& b) {
                    return a.second.firstFailureTime < b.second.firstFailureTime;
                });
            if (oldestIt != m_cache.end())
                m_cache.erase(oldestIt);
            else
                break;
        }
    }

private:
    std::unordered_map<std::string, FailedAuthEntry> m_cache;
    CriticalSection m_lock;

    unsigned m_maxFailedAttempts;
    unsigned m_cacheTimeoutSeconds;
    unsigned m_maxAllowedEntries;
};