#!/usr/bin/env python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

"""
Regression tests for ESP API redirect suppression (canRedirect() behavior).

Verifies that API-style requests — those carrying the ``rawxml_`` query
parameter or a non-HTML serialization format — receive HTTP 401 instead of
an HTML login-page redirect when authentication fails.  This covers the
``canRedirect()`` guard introduced to prevent browser redirect loops for
programmatic clients.

To run these tests, ESP must be configured with:

* Security enabled with at least one valid regular user
* ``authType="AuthTypeMixed"`` on the tested binding so that both
  session-cookie and BasicAuth code paths are exercised
* No LDAP or Active Directory is required

The **testauthSecurity** plugin (``libtestauthSecurity.so``) is one convenient
way to satisfy these conditions without setting up a full security backend.
See the README for configuration details and alternatives.

Tests
-----
1. Smoke test  – authenticated API request succeeds (sanity check).
2. Invalid session cookie + rawxml_  → HTTP 401, no redirect, no HTML body.
3. Invalid BasicAuth credentials + rawxml_  → HTTP 401, no redirect,
   ``WWW-Authenticate`` header present, no HTML body.
4. (Optional) Expired-but-valid BasicAuth + rawxml_  → HTTP 401, no redirect.
   This test is skipped unless the security plugin is configured with
   a user whose ``authStatus`` is set to ``password_valid_but_expired``
   (e.g. with testauthSecurity, requires the authstatus-config feature
   introduced in the ``feat/36811-testauth-document-support-authstatus``
   branch).

Usage
-----
Default credentials match the testauthSecurity YAML defaults
(password == username):

    python3 suppress_api_redirects_test.py

Custom host / credentials:

    python3 suppress_api_redirects_test.py \\
        --host 192.168.1.100 --port 8010 \\
        --user testuser --user-pw testuser

Include the optional expired-BasicAuth regression test (Test 4):

    python3 suppress_api_redirects_test.py \\
        --expired-user testexpired --expired-user-pw testexpired

Run only a specific test by name:

    python3 suppress_api_redirects_test.py -t test_api_smoke

Verbose output:

    python3 suppress_api_redirects_test.py -v

Environment variables (alternatives to CLI flags):
    HPCC_TEST_USER         regular user ID      (default: testuser)
    HPCC_TEST_USER_PW      regular user password (default: testuser)
    HPCC_TEST_EXPIRED_USER user with expired-but-valid password (optional)
    HPCC_TEST_EXPIRED_USER_PW password for the expired user     (optional)
"""

__version__ = "1.0"

import argparse
import base64
import logging
import os
import sys
import unittest
from typing import Optional

try:
    import requests
except ImportError:
    print("ERROR: requests library not found. Install with: pip install requests")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Defaults — match testauthSecurity YAML convention (password == username)
# ---------------------------------------------------------------------------
DEFAULT_PROTOCOL   = "http"
DEFAULT_HOST       = "127.0.0.1"
DEFAULT_PORT       = "8010"
DEFAULT_USER       = os.getenv("HPCC_TEST_USER",  "testuser")

# Service/path exercised by the tests
_API_PATH = "/WsSMC/Activity"
_RESPONSE_FORMAT = ".json"

# ---------------------------------------------------------------------------
# Configuration and helpers
# ---------------------------------------------------------------------------

class ESPTestConfig:
    """Holds connection and credential settings for a single test run."""

    def __init__(self, protocol: str, host: str, port: str,
                 user: str, user_password: str):
        self.protocol       = protocol
        self.host           = host
        self.port           = port
        self.user           = user
        self.user_password  = user_password
        self.base_url       = f"{protocol}://{host}:{port}"
        self.session_cookie = f"ESPSessionID{port}"

    def __repr__(self):
        return (f"ESPTestConfig(base_url={self.base_url}, "
                f"user={self.user})")


def _login(config: ESPTestConfig, username: str, password: str
           ) -> Optional[requests.Session]:
    """
    POST to /esp/login and return an authenticated requests.Session on success,
    or None if login failed.
    """
    session = requests.Session()
    try:
        resp = session.post(
            f"{config.base_url}/esp/login",
            data={"username": username, "password": password},
            allow_redirects=True,
            timeout=10
        )
    except requests.RequestException as exc:
        logging.error("Login request failed: %s", exc)
        return None

    if resp.status_code != 200 or "login" in resp.url.lower():
        logging.error("Login failed (status=%s, url=%s)", resp.status_code, resp.url)
        return None

    if not session.cookies.get(config.session_cookie):
        logging.error("No %s cookie after login. Cookies: %s",
                      config.session_cookie, list(session.cookies.keys()))
        return None

    return session


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestSuppressAPIRedirects(unittest.TestCase):
    """
    Regression tests for ``canRedirect()`` — API requests must receive HTTP 401
    instead of redirect-to-login-page responses when authentication fails.

    These tests require ESP to be configured with security enabled and at least
    one valid regular user.  No LDAP or Active Directory is required; the
    testauthSecurity plugin is one convenient way to satisfy this requirement.
    """

    @classmethod
    def setUpClass(cls):
        cls.config: ESPTestConfig = getattr(cls, "_config", None)
        if not cls.config:
            raise RuntimeError("Test configuration not injected; run via main().")

    # ------------------------------------------------------------------
    # Helper assertions
    # ------------------------------------------------------------------

    def _debug_response(self, resp: requests.Response):
        if not logging.getLogger().isEnabledFor(logging.DEBUG):
            return
        logging.debug("=== Response ===")
        logging.debug("Status : %s", resp.status_code)
        logging.debug("URL    : %s", resp.url)
        logging.debug("Headers: %s", dict(resp.headers))
        logging.debug("History: %s", resp.history)
        logging.debug("Body   : %.2000s", resp.text)
        logging.debug("=== End ===")

    def _assert_api_auth_failure(self, resp: requests.Response, context: str):
        """
        Assert that *resp* represents a correct API-style authentication
        failure:

        * HTTP status is 401
        * No redirect history
        * No ``Location`` header
        * Content-Type is NOT ``text/html``
        """
        self._debug_response(resp)
        lower_headers = {k.lower() for k in resp.headers}

        self.assertEqual(
            resp.status_code, 401,
            f"{context}: expected HTTP 401, got {resp.status_code}"
        )
        self.assertEqual(
            len(resp.history), 0,
            f"{context}: response should not have redirect history; "
            f"got {resp.history}"
        )
        self.assertNotIn(
            "location", lower_headers,
            f"{context}: auth failure must not include a Location redirect header"
        )
        self.assertNotIn(
            "text/html", resp.headers.get("Content-Type", "").lower(),
            f"{context}: auth failure must not return an HTML body"
        )

    # ------------------------------------------------------------------
    # Test 1: Smoke — authenticated API request succeeds
    # ------------------------------------------------------------------

    def test_api_smoke(self):
        """Valid session cookie + rawxml_ must return HTTP 200."""
        logging.info("=== Test 1: API smoke (authenticated request succeeds) ===")

        session = _login(self.config, self.config.user, self.config.user_password)
        self.assertIsNotNone(session, "Login failed; check ESP security configuration and user credentials.")

        resp = session.get(
            f"{self.config.base_url}{_API_PATH}{_RESPONSE_FORMAT}",
            params={"rawxml_": "1"},
            allow_redirects=False,
            timeout=10
        )
        self._debug_response(resp)

        self.assertEqual(
            resp.status_code, 200,
            f"Authenticated API request should succeed; got {resp.status_code}"
        )
        logging.info("✓ Authenticated API request returned 200")

    # ------------------------------------------------------------------
    # Test 2: Invalid session cookie + rawxml_ → 401, no redirect/HTML
    # ------------------------------------------------------------------

    def test_api_invalid_session_returns_401(self):
        """
        An API-style request carrying an invalid session cookie must return
        HTTP 401 without any redirect or HTML login page.

        This is the core regression for the session-auth path of
        ``canRedirect()`` / ``authExistingSession()``.
        """
        logging.info("=== Test 2: Invalid session cookie + rawxml_ → 401 ===")

        session = requests.Session()
        session.cookies.set(
            self.config.session_cookie, "invalid_session_id",
            domain=self.config.host, path="/"
        )

        resp = session.get(
            f"{self.config.base_url}{_API_PATH}{_RESPONSE_FORMAT}",
            params={"rawxml_": "1"},
            allow_redirects=False,
            timeout=10
        )

        self._assert_api_auth_failure(
            resp,
            "API request with invalid session cookie"
        )
        logging.info("✓ Invalid session + rawxml_ returned 401 with no redirect/HTML")

    # ------------------------------------------------------------------
    # Test 3: Invalid BasicAuth + rawxml_ → 401 + WWW-Authenticate, no redirect/HTML
    # ------------------------------------------------------------------

    def test_api_invalid_basicauth_returns_401(self):
        """
        An API-style request with invalid BasicAuth credentials must return
        HTTP 401 with a ``WWW-Authenticate`` challenge header, without any
        redirect or HTML login page.

        This is the core regression for the BasicAuth / per-request auth path
        of ``canRedirect()`` / ``handleAuthFailed()``.
        """
        logging.info("=== Test 3: Invalid BasicAuth + rawxml_ → 401 ===")

        bad_creds = base64.b64encode(b"no_such_user:wrong_password").decode("ascii")

        resp = requests.get(
            f"{self.config.base_url}{_API_PATH}{_RESPONSE_FORMAT}",
            params={"rawxml_": "1"},
            headers={"Authorization": f"Basic {bad_creds}"},
            allow_redirects=False,
            timeout=10
        )

        self._assert_api_auth_failure(resp, "API request with invalid BasicAuth")

        lower_headers = {k.lower() for k in resp.headers}
        self.assertIn(
            "www-authenticate", lower_headers,
            "HTTP 401 from BasicAuth failure must include a WWW-Authenticate header"
        )
        logging.info("✓ Invalid BasicAuth + rawxml_ returned 401 + WWW-Authenticate, no redirect/HTML")

    # ------------------------------------------------------------------
    # Test 4 (optional): Expired-but-valid BasicAuth + rawxml_ → 401
    # ------------------------------------------------------------------

    def test_api_expired_basicauth_returns_401(self):
        """
        An API-style request with credentials whose password is
        *valid-but-expired* (``AS_PASSWORD_VALID_BUT_EXPIRED``) must return
        HTTP 401 without a redirect to ``/esp/updatepasswordinput``.

        This test is **skipped automatically** unless:

        * ``--expired-user`` / ``--expired-user-pw`` are supplied (or the
          corresponding ``HPCC_TEST_EXPIRED_USER`` / ``HPCC_TEST_EXPIRED_USER_PW``
          environment variables are set), **and**
        * A preflight check confirms that the same credentials cause a non-API
          request to redirect to ``/esp/updatepasswordinput`` — meaning the
          expired-password state is actually active.

        With the standard testauthSecurity plugin this test always skips
        because the plugin performs a plain password comparison and does not
        produce the ``AS_PASSWORD_VALID_BUT_EXPIRED`` status.  To exercise
        this path with testauthSecurity, configure the test user with
        ``authStatus: password_valid_but_expired`` in the plugin's YAML
        (requires the authstatus-config capability added in the
        ``feat/36811-testauth-document-support-authstatus`` branch).
        """
        logging.info("=== Test 4 (optional): Expired BasicAuth + rawxml_ → 401 ===")

        expired_user = os.getenv("HPCC_TEST_EXPIRED_USER")
        expired_pw   = os.getenv("HPCC_TEST_EXPIRED_USER_PW")
        if not expired_user or not expired_pw:
            self.skipTest(
                "Skipped: set HPCC_TEST_EXPIRED_USER and HPCC_TEST_EXPIRED_USER_PW "
                "(or --expired-user / --expired-user-pw) to run this test."
            )

        auth_header = {
            "Authorization": "Basic " + base64.b64encode(
                f"{expired_user}:{expired_pw}".encode()
            ).decode("ascii")
        }

        # Preflight: confirm the account is in the expected expired state for
        # a non-API (browser-style) request.  If it does not redirect to
        # /esp/updatepasswordinput the expired state is not active and the
        # test cannot make meaningful assertions.
        preflight = requests.get(
            f"{self.config.base_url}{_API_PATH}",
            headers=auth_header,
            allow_redirects=False,
            timeout=10
        )
        location = preflight.headers.get("Location", "")
        if (preflight.status_code not in (301, 302, 303, 307, 308)
                or "/esp/updatepasswordinput" not in location):
            self.skipTest(
                f"Skipped: expired-BasicAuth precondition not met. "
                f"Expected a redirect to /esp/updatepasswordinput for non-API "
                f"request, but got status={preflight.status_code}, "
                f"Location='{location}'. "
                f"Ensure the test account is in the valid-but-expired state "
                f"and that the testauthSecurity plugin supports authStatus "
                f"configuration."
            )

        # Now make the API-style request — it must NOT redirect.
        resp = requests.get(
            f"{self.config.base_url}{_API_PATH}{_RESPONSE_FORMAT}",
            params={"rawxml_": "1"},
            headers=auth_header,
            allow_redirects=False,
            timeout=10
        )

        self._assert_api_auth_failure(resp, "API request with expired BasicAuth")

        lower_headers = {k.lower() for k in resp.headers}
        self.assertIn(
            "www-authenticate", lower_headers,
            "HTTP 401 from expired-BasicAuth failure must include WWW-Authenticate"
        )
        logging.info("✓ Expired BasicAuth + rawxml_ returned 401 + WWW-Authenticate, no redirect/HTML")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(
        description=f"ESP API-redirect suppression regression test v{__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Conditions required to run
---------------------------
  * ESP is running with security enabled
  * At least one valid regular user exists
  * ``authType="AuthTypeMixed"`` is configured on the tested binding
  * No LDAP or Active Directory is required

  The testauthSecurity plugin (libtestauthSecurity.so) is one way to satisfy
  these conditions.  See the README for configuration details.

  Default credentials match the testauthSecurity YAML convention (password ==
  username): testuser/testuser.

Environment variables
----------------------
  HPCC_TEST_USER             Regular user ID      (default: testuser)
  HPCC_TEST_USER_PW          Regular user password (default: testuser)
  HPCC_TEST_EXPIRED_USER     User with expired-but-valid password (optional)
  HPCC_TEST_EXPIRED_USER_PW  Password for expired user            (optional)

Examples
--------
  # Run with all defaults (testauthSecurity out-of-the-box setup):
  python3 suppress_api_redirects_test.py

  # Explicit credentials:
  python3 suppress_api_redirects_test.py \\
      --user testuser --user-pw testuser

  # Include the optional expired-BasicAuth test (Test 4):
  python3 suppress_api_redirects_test.py \\
      --expired-user testexpired --expired-user-pw testexpired

  # Run a single test:
  python3 suppress_api_redirects_test.py -t test_api_invalid_session_returns_401

  # Verbose logging:
  python3 suppress_api_redirects_test.py -v
        """
    )
    parser.add_argument("--protocol", default=DEFAULT_PROTOCOL,
                        help=f"Protocol (http/https, default: {DEFAULT_PROTOCOL})")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"ESP host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", default=DEFAULT_PORT,
                        help=f"ESP port (default: {DEFAULT_PORT})")
    parser.add_argument("--user", default=DEFAULT_USER,
                        help=f"Regular user ID (default: {DEFAULT_USER})")
    parser.add_argument("--user-pw", default=None,
                        help="Regular user password "
                             "(default: $HPCC_TEST_USER_PW or username)")
    parser.add_argument("--expired-user", default=None,
                        help="User ID with expired-but-valid password for Test 4 "
                             "(default: $HPCC_TEST_EXPIRED_USER). "
                             "If not set, Test 4 is skipped.")
    parser.add_argument("--expired-user-pw", default=None,
                        help="Password for --expired-user "
                             "(default: $HPCC_TEST_EXPIRED_USER_PW)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose/debug output")
    parser.add_argument("-t", "--test", default=None,
                        help="Run only a single named test "
                             "(e.g. test_api_invalid_session_returns_401)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_arguments()

    # Resolve credentials: CLI > env > default-to-username
    user_pw    = args.user_pw    or os.getenv("HPCC_TEST_USER_PW")    or args.user
    expired_user   = args.expired_user   or os.getenv("HPCC_TEST_EXPIRED_USER")
    expired_user_pw = args.expired_user_pw or os.getenv("HPCC_TEST_EXPIRED_USER_PW")

    # Propagate resolved values so test methods can read os.getenv()
    if expired_user:
        os.environ["HPCC_TEST_EXPIRED_USER"]    = expired_user
    if expired_user_pw:
        os.environ["HPCC_TEST_EXPIRED_USER_PW"] = expired_user_pw

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    config = ESPTestConfig(
        protocol=args.protocol,
        host=args.host,
        port=args.port,
        user=args.user,
        user_password=user_pw,
    )

    logging.info("=" * 70)
    logging.info("ESP API Redirect Suppression Regression Test v%s", __version__)
    logging.info("=" * 70)
    logging.info("Config: %s", config)
    logging.info("")

    # Inject config into the test class
    TestSuppressAPIRedirects._config = config

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()

    if args.test:
        if hasattr(TestSuppressAPIRedirects, args.test):
            suite.addTest(TestSuppressAPIRedirects(args.test))
        else:
            logging.error("Test '%s' not found.", args.test)
            return 1
    else:
        suite.addTests(loader.loadTestsFromTestCase(TestSuppressAPIRedirects))

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    result = runner.run(suite)

    logging.info("")
    logging.info("=" * 70)
    if result.wasSuccessful():
        logging.info("✓ ALL TESTS PASSED")
    else:
        logging.error("✗ TESTS FAILED: %d failures, %d errors",
                      len(result.failures), len(result.errors))
    logging.info("=" * 70)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
