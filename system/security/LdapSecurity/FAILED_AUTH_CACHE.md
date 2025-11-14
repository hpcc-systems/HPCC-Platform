# Failed Authentication Cache

## Overview

The LDAP Security Manager now includes a cache of users that have failed authentication. This feature helps protect against brute-force attacks by temporarily blocking users who exceed a configurable number of failed authentication attempts.

## How It Works

### Authentication Flow

When a user attempts to authenticate:

1. **Check Failed Authentication Cache**
   - If the user is found in the cache:
     - If the entry is stale (older than the configured timeout), remove it and proceed to normal authentication
     - If the user's failed attempts exceed the maximum allowed, immediately deny authentication
     - Otherwise, proceed to normal authentication

2. **Normal Authentication Process**
   - Attempt to authenticate the user through LDAP

3. **Update Cache Based on Result**
   - **On Success**: Remove the user from the failed authentication cache (if present)
   - **On Failure**: 
     - If the user is not in the cache, add them with an initial timestamp and count of 1
     - If the user is already in the cache, increment their failed attempt count

## Configuration

The failed authentication cache can be configured in the LDAP security configuration with the following parameters:

### `maxFailedAuthAttempts` (optional)
- **Type**: Integer
- **Default**: 3
- **Description**: Maximum number of failed authentication attempts before a user is temporarily blocked
- **Example**: `maxFailedAuthAttempts="5"`

### `failedAuthCacheTimeout` (optional)
- **Type**: Integer (minutes)
- **Default**: 30
- **Description**: Duration (in minutes) for which failed authentication entries are considered valid. After this timeout, the entry is considered stale and removed.
- **Example**: `failedAuthCacheTimeout="60"`

## Configuration Example

```xml
<Environment>
  <Software>
    <LdapServerProcess name="ldapserver" 
                       ldapAddress="ldap://ldap.example.com:389"
                       maxFailedAuthAttempts="3"
                       failedAuthCacheTimeout="30"
                       ... >
    </LdapServerProcess>
  </Software>
</Environment>
```

## Implementation Details

### Data Structure

The cache uses a `std::map` to store failed authentication entries:
- **Key**: Username (string)
- **Value**: `FailedAuthEntry` structure containing:
  - `timestamp`: Time of first failed attempt (in milliseconds via `msTick()`)
  - `failedAttempts`: Count of consecutive failed authentication attempts

### Thread Safety

The cache is protected by a `CriticalSection` lock (`m_failedAuthCacheLock`) to ensure thread-safe access in multi-threaded environments.

### Time Handling

The implementation uses `msTick()` for timestamps and handles potential wrap-around of the unsigned integer value when calculating elapsed time.

## Security Considerations

1. **Brute Force Protection**: The cache helps mitigate brute-force password attacks by temporarily blocking accounts after multiple failed attempts.

2. **Stale Entry Cleanup**: Entries automatically expire after the configured timeout, preventing permanent lockouts due to forgotten passwords or misconfigurations.

3. **Legitimate User Impact**: Users who exceed the maximum failed attempts must wait for the timeout period before they can authenticate again, even with correct credentials.

## Logging

The implementation includes debug and warning logs:
- `DBGLOG`: Records when entries are added, removed, or updated in the cache
- `WARNLOG`: Alerts when authentication is denied due to exceeded failed attempts

## Testing

To test the failed authentication cache:

1. Configure the LDAP security manager with desired `maxFailedAuthAttempts` and `failedAuthCacheTimeout` values
2. Attempt to authenticate with incorrect credentials multiple times
3. Verify that after exceeding `maxFailedAttempts`, authentication is immediately denied
4. Wait for the `failedAuthCacheTimeout` period to elapse
5. Verify that authentication attempts are allowed again after the timeout

## Backward Compatibility

This feature is backward compatible. If the configuration parameters are not specified, default values are used (3 attempts, 30-minute timeout), and the system behaves as before with the additional protection of the failed authentication cache.
