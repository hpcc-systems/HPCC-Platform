# Overview

These are tests for the ESP session ID moving to a 128-bit cryptographically secure format.

# Implementation

## Prerequisites and Assumptions

The tests require a running ESP with security enabled, so they are expected to be run manually. These assumptions about the environment are set as variables in the test script but can be overridden on the CLI:

1. HTTP default, but can support HTTPS
2. Target ESP host is 127.0.0.1 (port 8010)
3. Regular user ID is `hpcc_user`
4. Admin user ID is `hpcc_admin`

The user passwords are expected to be set as environmental values but can be specified on the CLI:

1. Regular user password in `$HPCC_TEST_USER_PW`
2. Admin user password in `$HPCC_TEST_ADMIN_PW`

The ESP must have WSESPControl authorized for the Admin user, and that user must belong to a group of the same name as the LDAP Admin group.

### Python Dependencies

Install required Python packages:

```bash
pip install requests
```

## Running Tests

### Basic Usage

Set environment variables and run all tests:

```bash
export HPCC_TEST_USER_PW=your_user_password
export HPCC_TEST_ADMIN_PW=your_admin_password
python3 sessionid_test.py
```

### Advanced Options

```bash
# Custom host and port
python3 sessionid_test.py --host 192.168.1.100 --port 8010

# Use HTTPS
python3 sessionid_test.py --protocol https

# Custom credentials
python3 sessionid_test.py --user myuser --user-pw mypass --admin myadmin --admin-pw adminpass

# Run specific test
python3 sessionid_test.py -t test_new_format_validation

# Verbose output
python3 sessionid_test.py -v

# Show help
python3 sessionid_test.py --help
```

## Tests

The test suite (`sessionid_test.py`) implements the following automated tests using ESP APIs. Tests pull data from response fields, headers and cookies as needed. All tests run as regular user unless otherwise specified.

### Test 1: New Format Validation
- Login via `/esp/login`
- Extract `ESPSessionID` cookie
- Validate session ID is 32-character hex string (128-bit)
- Make authenticated request to `/WsSMC/Activity`
- Verify successful authentication with new session ID format

### Test 2: Incorrect Format/Unknown Session ID Rejection
- Craft multiple malformed session IDs:
  - Empty string
  - Too short
  - Too long (33+ characters)
  - Non-hex characters
  - Wrong format (with dashes, etc.)
- Attempt authenticated requests with each invalid session ID
- Confirm expected failure (HTTP 401/403 or redirect to login)

### Test 3: ws_espcontrol Session Timeout
1. Login as regular user
2. Verify session is active with authenticated request
3. Login as admin
4. Use `/WSESPControl/SessionQuery` to find user's external session ID
5. Call `/WSESPControl/SetSessionTimeout` with `TimeoutMinutes=1`
6. Wait 90 seconds (timeout + ESP cleanup cycle buffer)
7. Confirm session has timed out (request fails or redirects)

### Test 4: Logout Session Invalidation
1. Login as user
2. Extract session ID and verify it works
3. Call `/esp/logout`
4. Attempt authenticated request with old session cookie
5. Confirm session is invalid (HTTP 401/403 or redirect)

### Test 5: Concurrent Sessions Load Test
- Use `ThreadPoolExecutor` to create multiple concurrent login sessions
- All threads login as same user simultaneously
- Capture session IDs from all successful logins
- Verify each session is active by making authenticated request to `/WsSMC/Activity`
- Confirm ESP allows multiple concurrent sessions per user
- Confirm all captured session IDs are functional

### Test 6: Session ID Uniqueness
1. Login as admin for session queries
2. Perform many sequential logins as regular user (configurable count, default 1000)
3. For each iteration:
   - Login and capture session ID from cookie
   - Use `/WSESPControl/SessionQuery` to retrieve external session ID
   - Store both session ID and external ID
   - Logout to clean up session
4. Verify all session IDs are unique (no duplicates in set)
5. Verify all external IDs are unique (no duplicates in set)
6. Verify external IDs differ from their corresponding session IDs
7. Report statistics: unique counts, duplicate counts, failed logins/queries

### Test 7: Active Session Collision Detection
1. Login as admin for session queries
2. Create many concurrent active sessions (configurable count, default 1000)
3. Keep all `ESPSession` objects alive (no logout)
4. Verify all session IDs collected are unique
5. Use `/WSESPControl/SessionQuery` to retrieve all external session IDs
6. Extract all external IDs from admin API response
7. Verify count of external IDs matches count of active sessions
8. Verify all external IDs are unique
9. Verify external IDs differ from their session IDs
10. Confirm collision detection works under realistic concurrent load
