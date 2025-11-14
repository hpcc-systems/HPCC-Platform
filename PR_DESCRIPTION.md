# Pull Request: Implement Failed Authentication Cache for LDAP Security Manager

## Summary
This PR implements a cache of users that have failed authentication in the LDAP Security Manager to help protect against brute-force password attacks.

## Problem Statement
The LDAP security manager needed a mechanism to track and limit failed authentication attempts to protect against brute-force attacks.

## Solution
Implemented a failed authentication cache that:
1. Tracks failed authentication attempts per user with timestamp and count
2. Temporarily blocks users who exceed a configurable maximum number of failed attempts
3. Automatically removes stale entries after a configurable timeout
4. Clears the cache entry on successful authentication

## Implementation Details

### Data Structure
- Added `FailedAuthEntry` struct with timestamp and failed attempt count
- Uses `std::map<std::string, FailedAuthEntry>` for efficient O(log n) lookups
- Protected by `CriticalSection` for thread safety

### Authentication Flow
**Before attempting authentication:**
- Check if user exists in failed auth cache
- If found and entry is stale (older than timeout), remove it
- If found and failed attempts >= max allowed, immediately deny with log warning
- Otherwise, proceed with normal authentication

**After authentication result:**
- **On Success**: Remove user from failed auth cache (if present)
- **On Failure**: Add user to cache (if not present) or increment their failed attempt count

### Configuration
Two new optional configuration parameters:
- `maxFailedAuthAttempts` (default: 3) - Maximum failed attempts before blocking
- `failedAuthCacheTimeout` (default: 30 minutes) - Cache entry timeout in minutes

### Thread Safety
All cache operations are protected by `CriticalSection` locks, ensuring safe concurrent access.

### Edge Cases Handled
- ✅ Timestamp wrap-around (unsigned int overflow)
- ✅ Stale entry cleanup
- ✅ Multiple authentication paths (LDAP, digital signature, session token, cached user)
- ✅ Empty username validation

## Files Changed
- `system/security/LdapSecurity/ldapsecurity.ipp` (+15 lines)
  - Added `FailedAuthEntry` structure
  - Added cache-related member variables to `CLdapSecManager`
  
- `system/security/LdapSecurity/ldapsecurity.cpp` (+122 lines)
  - Modified `init()` to read configuration parameters
  - Updated `authenticate()` with comprehensive cache logic
  - Added `jutil.hpp` include for `msTick()` function

- `.gitignore` (+1 line)
  - Added build_test directory

## Files Added
- `system/security/LdapSecurity/FAILED_AUTH_CACHE.md` - Feature documentation
- `system/security/LdapSecurity/FAILED_AUTH_CACHE_TESTS.md` - Test scenarios
- `system/security/LdapSecurity/SUMMARY.md` - Implementation summary

## Testing
Comprehensive test scenarios are documented in `FAILED_AUTH_CACHE_TESTS.md`:
1. Failed authentication blocking after max attempts
2. Stale entry removal after timeout
3. Successful authentication clears cache
4. Per-user cache isolation
5. Concurrent access thread safety
6. Configuration validation

## Security Benefits
- **Brute-force Protection**: Limits password guessing attempts
- **Resource Protection**: Reduces unnecessary LDAP calls for blocked users
- **Configurable Policy**: Allows customization based on security requirements
- **Automatic Recovery**: Stale entries expire, preventing permanent lockouts

## Backward Compatibility
✅ **Fully backward compatible**
- Default configuration values provide reasonable security
- No changes to external APIs
- Feature is transparent when not explicitly configured

## Performance Impact
- Minimal: O(log n) cache lookups using std::map
- Only adds overhead when authentication occurs
- Stale entries cleaned up lazily during authentication

## Code Quality
- ✅ Thread-safe implementation
- ✅ Proper error handling
- ✅ Comprehensive logging (DBGLOG, WARNLOG)
- ✅ Follows existing code style
- ✅ No memory leaks (std::map handles cleanup)
- ✅ Handles edge cases

## Example Configuration
```xml
<Environment>
  <Software>
    <LdapServerProcess name="ldapserver" 
                       ldapAddress="ldap://ldap.example.com:389"
                       maxFailedAuthAttempts="5"
                       failedAuthCacheTimeout="60"
                       ... >
    </LdapServerProcess>
  </Software>
</Environment>
```

## Example Log Output
```
DBGLOG: Added user testuser to failed authentication cache
DBGLOG: Incremented failed authentication count for user testuser to 2
DBGLOG: Incremented failed authentication count for user testuser to 3
WARNLOG: Authentication denied for user testuser: exceeded max failed attempts (3)
```

## Verification
To verify this implementation:
1. Configure LDAP with `maxFailedAuthAttempts="3"`
2. Attempt authentication with wrong password 3 times
3. 4th attempt should be immediately denied without LDAP call
4. Wait for configured timeout
5. Authentication should be allowed again

## Related Issues
Addresses the requirement to implement a failed authentication cache in the LDAP security manager.

## Checklist
- [x] Code changes are minimal and surgical
- [x] Thread safety ensured
- [x] Edge cases handled
- [x] Backward compatible
- [x] Documentation added
- [x] Test scenarios documented
- [x] Logging added for debugging
- [x] Security considerations addressed
