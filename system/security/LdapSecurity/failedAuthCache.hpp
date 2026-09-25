#pragma once

#include <unordered_map>
#include <string>
#include <algorithm>
#include <limits>

#include "jlog.hpp"
#include "jmutex.hpp"
#include "jutil.hpp"

struct FailedAuthEntry
{
    unsigned firstFailureTick;
    unsigned failedAttempts;

    FailedAuthEntry() : firstFailureTick(0), failedAttempts(0) {}
    FailedAuthEntry(unsigned tick, unsigned attempts)
        : firstFailureTick(tick), failedAttempts(attempts) {}
};

// FailedAuthCache is primarily a load-reduction mechanism to prevent repeated failed
// authentication attempts from hammering the LDAP/Active Directory server with unnecessary
// traffic and round-trips. Once a configurable threshold of local failures is reached, the cache
// blocks further attempts for the configured timeout period, avoiding LDAP queries.
// NOTE: This is NOT the primary lockout mechanism; Active Directory's own account.lockout
// policy remains authoritative. This cache layer simply reduces network load during failure bursts.
// IMPORTANT: The TTL is measured from the FIRST failure, not continuously updated on each new failure.
// This allows automatic recovery: if the underlying AD condition (temporary outage, account unlock, etc.)
// is fixed within the timeout period, the entry expires and retries are allowed. This prevents permanent
// blocking of services that are retrying after a transient AD issue.
class FailedAuthCache
{
public:
    static constexpr unsigned defaultMaxFailedAttempts = 5;
    static constexpr unsigned defaultCacheTimeoutSeconds = 300; // 5 minutes
    // NOTE: maxAllowedEntries caps the cache size to bound memory use. During a credential-spray
    // attack with more distinct usernames than this limit, the oldest entries are evicted to make
    // room for newer ones. Evicted users can retry LDAP immediately, so this cap trades off memory
    // use against protection coverage. Raise this limit if spray attacks with many distinct usernames
    // are a concern. The trim cost is O(N) per insert so keep this reasonably sized.
    static constexpr unsigned defaultMaxAllowedEntries = 25;

    FailedAuthCache(unsigned maxFailedAttempts = defaultMaxFailedAttempts,
                    unsigned cacheTimeoutSeconds = defaultCacheTimeoutSeconds,
                    unsigned maxAllowedEntries = defaultMaxAllowedEntries)
        : m_maxFailedAttempts(maxFailedAttempts),
            // Clamp once at construction so ms conversion cannot overflow.
            m_cacheTimeoutSeconds(std::min(cacheTimeoutSeconds, std::numeric_limits<unsigned>::max() / 1000U)),
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

        const unsigned currentTick = msTick();
        if (isExpired(it->second, currentTick))
        {
            // Entry has expired; remove it and allow retries. No need to trim the
            // full cache here — trimming happens on every insert/update in updateUserLockoutStatus.
            m_cache.erase(it);
            trimFailedAuthCache();
            return false;
        }

        return it->second.failedAttempts >= m_maxFailedAttempts;
    }

    // Record a failed authentication attempt for username.
    void updateUserLockoutStatus(const char* username)
    {
        if (!username || !*username)
            return;

        CriticalBlock block(m_lock);

        const unsigned currentTick = msTick();
        auto it = m_cache.find(username);
        if (it == m_cache.end())
        {
            m_cache[username] = FailedAuthEntry(currentTick, 1);
            trimFailedAuthCache();
            return;
        }

        if (isExpired(it->second, currentTick))
        {
            // Timeout expired from first failure; reset to allow retries.
            // If the underlying AD condition (e.g., temporary outage, account unlock) was fixed,
            // the service can now attempt authentication again.
            it->second.firstFailureTick = currentTick;
            it->second.failedAttempts = 1;
        }
        else
            ++(it->second.failedAttempts);

        if (it->second.failedAttempts == m_maxFailedAttempts)
            OWARNLOG("User %s locked out for %u seconds after reaching maximum failed authentication attempts of %u", username, m_cacheTimeoutSeconds, m_maxFailedAttempts);
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
    // Keep timeout-ms conversion safe even if set at runtime.
    void setCacheTimeoutSeconds(unsigned v) { m_cacheTimeoutSeconds = std::min(v, std::numeric_limits<unsigned>::max() / 1000U); }
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
    unsigned queryCacheTimeoutMs() const
    {
        // Safe because constructor/setter clamp m_cacheTimeoutSeconds.
        return m_cacheTimeoutSeconds * 1000U;
    }

    // Check if a cached entry has expired based on time elapsed since FIRST failure.
    // NOTE: TTL is from firstFailureTick (the initial failure time), NOT updated to the latest failure.
    // This design prevents permanent blocking and allows recovery if the underlying condition is resolved.
    bool isExpired(const FailedAuthEntry &entry, unsigned currentTick) const
    {
        const unsigned elapsedMs = currentTick - entry.firstFailureTick; // Unsigned tick subtraction intentionally handles msTick wraparound.
        return elapsedMs >= queryCacheTimeoutMs();
    }

    void trimFailedAuthCache()
    {
        // Expect lock is held by caller.
        const unsigned currentTick = msTick();

        // Remove expired entries.
        for (auto it = m_cache.begin(); it != m_cache.end();)
        {
            if (isExpired(it->second, currentTick))
                it = m_cache.erase(it);
            else
                ++it;
        }

        // If still too large, remove oldest entries by age.
        while (m_cache.size() > m_maxAllowedEntries)
        {
            // max_element with this comparator returns the entry with the largest age (oldest).
            auto oldestIt = std::max_element(
                m_cache.begin(),
                m_cache.end(),
                [currentTick](const auto& a, const auto& b) {
                    const unsigned ageA = currentTick - a.second.firstFailureTick;
                    const unsigned ageB = currentTick - b.second.firstFailureTick;
                    return ageA < ageB;
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
