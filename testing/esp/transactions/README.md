# Overview

This directory contains two test scripts:

| Script | Purpose | Auth backend |
|--------|---------|--------------|
| `suppress_api_redirects_test.py` | API redirect-suppression regression (`canRedirect()`) | Auth backend needed. Tested with testauthSecurity plugin. |
| `sessionid_test.py` | 128-bit session-ID format, session management, and full regression suite | Auth backend needed. Tested with LDAP. |

# suppress_api_redirects_test.py

Focused regression for the `canRedirect()` guard: verifies that API-style
requests (those carrying `rawxml_` or a `.json`/`.xml` serialization format) receive
HTTP 401 instead of an HTML login-page redirect when authentication fails.

## Conditions Required to Run

To run all the tests the following conditions must be met:

* ESP is running with security enabled
* At least one valid regular user exists and one in "password valid but expired" state.
* `authType="AuthTypeMixed"` is configured on the tested binding (e.g. `WsSMC`)
  so that both session-cookie and BasicAuth code paths are exercised

No LDAP or Active Directory is required.  The **testauthSecurity** plugin
(`libtestauthSecurity.so`) is one convenient way to satisfy these conditions
without setting up a full security backend.

For documentation on configuring the **testauthSecurity** plugin, refer to `devdoc/TestAuthSecurityManager.md` that will be located in the repo upon resolution of [issue #36811](https://github.com/risk-hsy/HPCC-Platform/issues/36811).

> Note: As of this time, 389ds LDAP is missing support for "password valid but expired" state required for test case #4. That is the reason **testauthSecurity** is recommended for this test instead of 389ds.

## Running suppress_api_redirects_test.py

Ensure dependencies are installed:

```bash
pip install requests
```

Then run the tests

```bash
# All defaults (testauthSecurity out-of-the-box, password == username):
python3 suppress_api_redirects_test.py

# Explicit credentials:
python3 suppress_api_redirects_test.py \
    --user testuser --user-pw testuser

# Custom host / port:
python3 suppress_api_redirects_test.py --host 192.168.1.100 --port 8010

# Use HTTPS:
python3 suppress_api_redirects_test.py --protocol https

# Include the optional expired-BasicAuth test (Test 4) — see note below:
python3 suppress_api_redirects_test.py \
    --expired-user testexpired --expired-user-pw testexpired

# Run a single test:
python3 suppress_api_redirects_test.py -t test_api_invalid_session_returns_401

# Verbose output:
python3 suppress_api_redirects_test.py -v
```

### Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `HPCC_TEST_USER` | `testuser` | Regular user ID |
| `HPCC_TEST_USER_PW` | *(username)* | Regular user password |
| `HPCC_TEST_EXPIRED_USER` | N/A | User with expired-but-valid password (optional) |
| `HPCC_TEST_EXPIRED_USER_PW` | N/A | Password for expired user (optional) |

## Tests in suppress_api_redirects_test.py

### Test 1: API smoke (sanity check)
- Login as regular user via `/esp/login`
- Make authenticated `GET /WsSMC/Activity?rawxml_=1`
- Verify HTTP 200 — confirms auth is working before the failure tests

### Test 2: Invalid session cookie + rawxml_ → 401
- Inject a malformed `ESPSessionID<port>` cookie
- `GET /WsSMC/Activity?rawxml_=1` with redirects disabled
- Assert HTTP 401
- Assert no `Location` header
- Assert response body is not `text/html`

### Test 3: Invalid BasicAuth credentials + rawxml_ → 401
- `GET /WsSMC/Activity?rawxml_=1` with a bogus `Authorization: Basic …` header
- Assert HTTP 401
- Assert no `Location` header
- Assert `WWW-Authenticate` header is present
- Assert response body is not `text/html`

### Test 4: Expired-but-valid BasicAuth + rawxml_ → 401 (optional)
- Requires `--expired-user` / `--expired-user-pw` (or env vars)
- Requires the security backend to produce `AS_PASSWORD_VALID_BUT_EXPIRED` for
  the test account.  With testauthSecurity, this requires configuring the test
  user with `authStatus = password_valid_but_expired` in the plugin configuration
  (requires the authstatus-config capability introduced in the issue #36811).
- A preflight check confirms the account is in the expired state (non-API
  request redirects to `/esp/updatepasswordinput`) before running the assertion.
- If the preflight does not observe the redirect, the test skips automatically.

---

# sessionid_test.py

## Prerequisites and Assumptions

The tests require a running ESP with security enabled, so they are expected to be run manually. These assumptions about the environment are set as variables in the test script but can be overridden on the CLI:

1. HTTP default, but can support HTTPS
2. Target ESP host is 127.0.0.1 (port 8010)
3. Regular user ID is `hpcc_user`
4. Admin user ID is `hpcc_admin`

### Cluster/Auth Configuration Requirements

To cover both session-cookie and BasicAuth behavior in a single run, configure the ESP auth domain used by the tested binding (for example `WsSMC`) as:

- `authType="AuthTypeMixed"` (recommended for this suite)

Notes:

- `AuthPerSessionOnly` will not exercise BasicAuth per-request paths.
- `AuthPerRequestOnly` will not exercise session-id cookie auth paths.
- `WsESPControl` must be enabled and admin-authorized for timeout and query tests.

The user passwords are expected to be set as environmental values but can be specified on the CLI:

1. Regular user password in `$HPCC_TEST_USER_PW`
2. Admin user password in `$HPCC_TEST_ADMIN_PW`

The ESP must have WSESPControl authorized for the Admin user, and that user must belong to a group of the same name as the LDAP Admin group.

### Python Dependencies

Install required Python packages:

```bash
pip install requests
```

## Running Tests (sessionid_test.py)

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

## Tests (sessionid_test.py)

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
