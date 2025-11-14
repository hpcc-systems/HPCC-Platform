# Test Scenarios for Failed Authentication Cache

## Prerequisites
- HPCC Platform with LDAP security configured
- Access to LDAP server with test users
- Configuration with `maxFailedAuthAttempts="3"` and `failedAuthCacheTimeout="5"` (5 minutes for faster testing)

## Test Scenario 1: Failed Authentication Blocking

### Steps:
1. Configure LDAP security with `maxFailedAuthAttempts="3"`
2. Attempt to authenticate with user "testuser" using incorrect password
3. Repeat step 2 two more times (total 3 failed attempts)
4. Attempt to authenticate with user "testuser" using correct password

### Expected Results:
- First 3 attempts should fail with normal authentication failure message
- 4th attempt should be immediately denied with message: "Authentication denied for user testuser: exceeded max failed attempts (3)"
- No LDAP call should be made on the 4th attempt (can verify from logs)

## Test Scenario 2: Stale Entry Removal

### Steps:
1. Configure LDAP security with `maxFailedAuthAttempts="3"` and `failedAuthCacheTimeout="5"` (5 minutes)
2. Fail authentication 3 times with user "testuser"
3. Wait for 5+ minutes
4. Attempt to authenticate with user "testuser" using correct password

### Expected Results:
- First 3 attempts should fail normally
- After 5+ minutes, the cache entry should be stale
- Next authentication attempt should succeed with correct credentials
- Log should show: "Removed stale failed authentication entry for user testuser"

## Test Scenario 3: Successful Authentication Clears Cache

### Steps:
1. Configure LDAP security with `maxFailedAuthAttempts="3"`
2. Fail authentication 2 times with user "testuser" (incorrect password)
3. Authenticate successfully with correct password
4. Fail authentication 2 more times with incorrect password
5. Attempt to authenticate with correct password again

### Expected Results:
- First 2 failed attempts are recorded in cache
- Successful authentication clears the cache entry
- Next 2 failed attempts start a new cache entry
- Final authentication with correct password should succeed (only 2 failed attempts in current entry)

## Test Scenario 4: Different Users

### Steps:
1. Configure LDAP security with `maxFailedAuthAttempts="3"`
2. Fail authentication 3 times with user "testuser1"
3. Attempt to authenticate with user "testuser2" (different user)

### Expected Results:
- testuser1 should be blocked after 3 failed attempts
- testuser2 should be able to authenticate normally
- Cache entries are per-user, not global

## Test Scenario 5: Concurrent Access

### Steps:
1. Configure LDAP security with `maxFailedAttempts="5"`
2. Create multiple threads/processes
3. Each thread attempts to authenticate with same user but fails
4. Verify thread safety

### Expected Results:
- All failed attempts should be properly counted
- No race conditions or crashes
- After 5 total failed attempts across all threads, user should be blocked
- Critical section locks prevent corruption of cache

## Test Scenario 6: Cache Entry Limit Check

### Steps:
1. Configure LDAP security with `maxFailedAuthAttempts="3"`
2. Fail authentication 2 times with user "testuser"
3. Attempt to authenticate with correct password

### Expected Results:
- 2 failed attempts should not block the user
- Authentication with correct password should succeed
- Cache entry should be cleared after successful authentication

## Verification Methods

### Log Analysis
Check logs for:
- "Added user X to failed authentication cache"
- "Incremented failed authentication count for user X to Y"
- "Removed stale failed authentication entry for user X"
- "Authentication denied for user X: exceeded max failed attempts"

### LDAP Call Monitoring
- When user is blocked due to exceeded attempts, verify no LDAP call is made
- Use LDAP server logs or network monitoring to confirm

### Configuration Testing
Test with different configuration values:
- `maxFailedAuthAttempts="1"` - Should block immediately after first failure
- `maxFailedAuthAttempts="10"` - Should allow more attempts
- `failedAuthCacheTimeout="1"` - Entries expire quickly (1 minute)
- `failedAuthCacheTimeout="60"` - Entries persist longer (1 hour)

## Performance Testing

### Load Test
1. Simulate multiple users with failed authentication attempts
2. Monitor memory usage of failed authentication cache
3. Verify cache doesn't grow unbounded
4. Check performance impact on normal authentication

### Expected Performance:
- Minimal overhead for cache lookup (O(log n) for std::map)
- Thread-safe access with minimal contention
- Stale entries automatically cleaned up during authentication attempts
