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
Test suite for ESP 128-bit cryptographically secure session ID implementation.

Assumes an ESP is running with:
1. ws_espcontrol service enabled
2. A normal user (hpcc_user) and admin user (hpcc_admin) exist
3. Some security is enabled (e.g. LDAP, htpasswd) with session management

Tests cover:
1. New format validation (128-bit hex string)
2. Invalid/malformed session ID rejection
3. Session timeout via ws_espcontrol
4. Logout session invalidation
5. Concurrent sessions load test
6. Session ID uniqueness over many iterations
7. Active session collision detection
"""

__version__ = "1.0"

import argparse
import logging
import os
import re
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Set, List

try:
    import requests
except ImportError:
    print("ERROR: requests library not found. Install with: pip install requests")
    sys.exit(1)


# Default configuration
DEFAULT_PROTOCOL = "http"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "8010"
DEFAULT_USER = "hpcc_user"
DEFAULT_ADMIN = "hpcc_admin"

# Session ID validation
SESSION_ID_PATTERN = re.compile(r'^[0-9a-fA-F]{32}$')  # 128-bit = 32 hex chars
# Extract "external" ID from XML/JSON WsESPControl responses
ID_EXTRACTION_PATTERN = re.compile(r'<ID>([0-9a-fA-F]+)</ID>|"ID"\s*:\s*"([0-9a-fA-F]+)"')


class ESPTestConfig:
    """Configuration for ESP session tests."""
    
    def __init__(self, protocol: str, host: str, port: str,
                 user: str, user_password: str,
                 admin: str, admin_password: str):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.user = user
        self.user_password = user_password
        self.admin = admin
        self.admin_password = admin_password
        self.base_url = f"{protocol}://{host}:{port}"
        
    def __repr__(self):
        return (f"ESPTestConfig(base_url={self.base_url}, "
                f"user={self.user}, admin={self.admin})")


class ESPSession:
    """Manages ESP session and authentication."""
    
    def __init__(self, config: ESPTestConfig):
        self.config = config
        self.session = requests.Session()
        self.session_id: Optional[str] = None
        self.external_id: Optional[str] = None
        self.cookie_name = f"ESPSessionID{config.port}"
        
    def login(self, username: str, password: str) -> bool:
        """
        Login to ESP and capture session ID.
        
        Returns:
            True if login successful, False otherwise
        """
        login_url = f"{self.config.base_url}/esp/login"
        
        try:
            response = self.session.post(
                login_url,
                data={"username": username, "password": password},
                allow_redirects=True,
                timeout=10
            )
            
            # Debug: Show all cookies received
            logging.debug(f"Login response status: {response.status_code}")
            logging.debug(f"Login response URL: {response.url}")
            logging.debug(f"All cookies after login: {dict(self.session.cookies)}")
            logging.debug(f"Expected session cookie name: {self.cookie_name}")
            
            # Check if login was successful
            if response.status_code != 200:
                logging.error(f"Login failed with status {response.status_code}")
                return False
            
            # Check response for authentication indicators
            response_text = response.text
            if "login" in response.url.lower():
                logging.error("Login appears to have failed - still on login page")
                logging.debug(f"Response: {response_text}")
                return False
            
            # Extract session ID from cookies using port-specific name
            session_cookie = self.session.cookies.get(self.cookie_name)
            
            if session_cookie:
                self.session_id = session_cookie
                logging.debug(f"Session ID captured from {self.cookie_name}: {self.session_id}")
                return True
            else:
                logging.error(f"No {self.cookie_name} cookie found after login")
                logging.error("Available cookies: " + ", ".join(self.session.cookies.keys()) if self.session.cookies else "none")
                return False
                
        except requests.RequestException as e:
            logging.error(f"Login request failed: {e}")
            return False
    
    def logout(self) -> bool:
        """Logout from ESP."""
        logout_url = f"{self.config.base_url}/esp/logout"
        
        try:
            response = self.session.get(logout_url, timeout=10)
            success = response.status_code == 200
            if success:
                logging.debug("Logout successful")
            return success
        except requests.RequestException as e:
            logging.error(f"Logout request failed: {e}")
            return False
    
    def make_authenticated_request(self, path: str, method: str = "GET", 
                                   params: Optional[Dict] = None,
                                   data: Optional[Dict] = None) -> requests.Response:
        """
        Make an authenticated request to ESP.
        
        Args:
            path: URL path (e.g., "/WsSMC/Activity")
            method: HTTP method (GET or POST)
            params: Query parameters
            data: POST data
            
        Returns:
            Response object
        """
        url = f"{self.config.base_url}{path}"
        
        try:
            if method.upper() == "POST":
                response = self.session.post(url, params=params, data=data, timeout=10)
            else:
                response = self.session.get(url, params=params, timeout=10)
            return response
        except requests.RequestException as e:
            logging.error(f"Request to {path} failed: {e}")
            raise
    
    def set_custom_session_id(self, session_id: str):
        """Set a custom session ID cookie for testing invalid sessions."""
        self.session.cookies.set(self.cookie_name, session_id, 
                                 domain=self.config.host, path='/')
        self.session_id = session_id


class WSESPControlClient:
    """Client for ws_espcontrol service."""
    
    def __init__(self, config: ESPTestConfig):
        self.config = config
        self.session = requests.Session()
        
    def login_as_admin(self) -> bool:
        """Login as admin user."""
        login_url = f"{self.config.base_url}/esp/login"
        
        try:
            response = self.session.post(
                login_url,
                data={"username": self.config.admin, "password": self.config.admin_password},
                allow_redirects=True,
                timeout=10
            )
            return response.status_code == 200
        except requests.RequestException as e:
            logging.error(f"Admin login failed: {e}")
            return False
    
    def query_sessions(self, user_id: Optional[str] = None, 
                      from_ip: Optional[str] = None) -> Optional[requests.Response]:
        """
        Query active sessions.
        
        Returns:
            Response object or None on error
        """
        url = f"{self.config.base_url}/WSESPControl/SessionQuery"
        params = {}
        if user_id:
            params['UserID'] = user_id
        if from_ip:
            params['FromIP'] = from_ip
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                # Parse response (XML or JSON depending on Accept header)
                # For simplicity, we'll work with the text response
                return response
            else:
                logging.error(f"SessionQuery failed with status {response.status_code}")
                return None
        except requests.RequestException as e:
            logging.error(f"SessionQuery request failed: {e}")
            return None
    
    def get_session_info(self, session_id: str, port: int = None) -> Optional[requests.Response]:
        """Get detailed information about a specific session."""
        url = f"{self.config.base_url}/WSESPControl/SessionInfo"
        params = {'ID': session_id}
        if port:
            params['Port'] = port
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            return response if response.status_code == 200 else None
        except requests.RequestException as e:
            logging.error(f"SessionInfo request failed: {e}")
            return None
    
    def set_session_timeout(self, session_id: str, timeout_minutes: int) -> bool:
        """
        Set timeout for a specific session.
        
        Args:
            session_id: External session ID
            timeout_minutes: Minutes until timeout (0 = immediate)
            
        Returns:
            True if successful
        """
        url = f"{self.config.base_url}/WSESPControl/SetSessionTimeout"
        params = {
            'ID': session_id,
            'TimeoutMinutes': timeout_minutes
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            success = response.status_code == 200
            if success:
                logging.debug(f"Set timeout for session {session_id} to {timeout_minutes} minutes")
            return success
        except requests.RequestException as e:
            logging.error(f"SetSessionTimeout request failed: {e}")
            return False
    
    def clean_session(self, session_id: str = None, user_id: str = None) -> bool:
        """Delete/clean a specific session or all sessions for a user.
        
        Args:
            session_id: External session ID to clean (optional)
            user_id: User ID to clean all sessions for (optional)
            
        Returns:
            True if successful
        """
        url = f"{self.config.base_url}/WSESPControl/CleanSession"
        params = {}
        if session_id:
            params['ID'] = session_id
        if user_id:
            params['UserID'] = user_id
        
        if not params:
            logging.error("CleanSession requires either session_id or user_id")
            return False
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            return response.status_code == 200
        except requests.RequestException as e:
            logging.error(f"CleanSession request failed: {e}")
            return False


class TestSessionIDFormat(unittest.TestCase):
    """Test suite for ESP session ID functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test configuration."""
        cls.config = getattr(cls, '_config', None)
        if not cls.config:
            raise RuntimeError("Test configuration not set")
    
    def tearDown(self):
        """Clean up sessions after each test."""
        self.cleanup_user_sessions()
    
    def cleanup_user_sessions(self):
        """Clean up all sessions for hpcc_user."""
        try:
            admin_client = WSESPControlClient(self.config)
            if admin_client.login_as_admin():
                if admin_client.clean_session(user_id=self.config.user):
                    logging.debug(f"Cleaned up sessions for {self.config.user}")
        except Exception as e:
            logging.warning(f"Failed to cleanup sessions: {e}")

    def debugResponseDetails(self, response: requests.Response):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug("=== Raw Response Details ===")
            logging.debug(f"Status Code: {response.status_code}")
            logging.debug(f"Reason: {response.reason}")
            logging.debug(f"URL: {response.url}")
            logging.debug(f"Headers: {dict(response.headers)}")
            logging.debug(f"Cookies: {dict(response.cookies)}")
            logging.debug(f"Encoding: {response.encoding}")
            logging.debug(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
            logging.debug(f"Content-Length: {len(response.content)} bytes")
            logging.debug("--- Response Body (first 2000 chars) ---")
            try:
                logging.debug(response.text[:2000])
            except Exception as e:
                logging.debug(f"Could not decode response text: {e}")
            logging.debug("=== End Raw Response ===")

    def getResponseFailureReasons(self, response: requests.Response) -> List[str]:
        """
        Get a list of reasons the response is a failure response.
        
        Args:
            response: The response object to check
            
        Returns:
            List of failure reasons (empty if successful)

        """
        failure_reasons = []
        
        # Check for redirects in history
        if response.history:
            redirect_chain = " -> ".join([f"{r.status_code} ({r.url})" for r in response.history])
            failure_reasons.append(f"Response was redirected: {redirect_chain} -> {response.status_code} ({response.url})")
        
        # Check final response status
        if response.status_code != 200:
            failure_reasons.append(f"Response status is {response.status_code}, expected 200")
        
        if response.text and "Authentication failed: invalid session." in response.text:
            failure_reasons.append("Response body includes: 'Authentication failed: invalid session.'")

        return failure_reasons

    def assertResponseSuccess(self, response: requests.Response, msg: str = None):
        """
        Assert that a response was successful with no redirects and status 200.
        
        Args:
            response: The response object to check
            msg: Optional custom failure message
            
        Raises:
            AssertionError: If response contains redirects, status is not 200
            or failure message in body
        """
        failure_reasons = self.getResponseFailureReasons(response)

        if failure_reasons:
            failure_msg = "; ".join(failure_reasons)
            if msg:
                failure_msg = f"{msg}: {failure_msg}"
            self.debugResponseDetails(response)
            self.fail(failure_msg)
    
    def assertResponseFailure(self, response: requests.Response, msg: str = None) -> str:
        """
        Assert that a response failed (redirect, non-200 status
        or 200 status with failure message in body).
        
        Args:
            response: The response object to check
            msg: Optional custom failure message

        Returns:
            Reason(s) for failure (string)
            
        Raises:
            AssertionError: If response is unexpectedly successful
        """
        failure_reasons = self.getResponseFailureReasons(response)
        
        if not failure_reasons:
            failure_msg = "Response was unexpectedly successful"
            if msg:
                failure_msg = f"{msg}: {failure_msg}"
            self.debugResponseDetails(response)
            self.fail(failure_msg)
        
        # Return reason for expected failure
        return "; ".join(failure_reasons)

    def test_new_format_validation(self):
        """Test 1: Validate new 128-bit session ID format after login."""
        logging.info("=== Test 1: New Format Validation ===")
        
        esp_session = ESPSession(self.config)
        
        # Login and capture session ID
        self.assertTrue(
            esp_session.login(self.config.user, self.config.user_password),
            "Failed to login as regular user"
        )
        
        session_id = esp_session.session_id
        self.assertIsNotNone(session_id, "Session ID should not be None")
        
        # Validate format: 32 hex characters (128 bits)
        self.assertRegex(
            session_id, 
            SESSION_ID_PATTERN,
            f"Session ID '{session_id}' does not match 128-bit hex format (32 hex chars)"
        )
        
        logging.info(f"✓ Session ID format valid: {session_id}")
        
        # Make an authenticated request to verify session works
        response = esp_session.make_authenticated_request("/WsSMC/Activity")
        
        # Log verbose response details
        self.debugResponseDetails(response)
        
        self.assertResponseSuccess(response, "Authenticated request with valid session ID should succeed")
        
        logging.info("✓ Authenticated request successful with valid session ID")
        
        # Logout
        esp_session.logout()
    
    def test_invalid_session_id_rejection(self):
        """Test 2: Verify that invalid/malformed session IDs are rejected."""
        logging.info("=== Test 2: Invalid Session ID Rejection ===")
        
        invalid_session_ids = [
            "invalid",  # Too short
            "",  # Empty
            "00112233445566778899aabbccddeeff00",  # Too long
            "gggggggggggggggggggggggggggggggg",  # Non-hex characters
            "00112233-4455-6677-8899-aabbccddeeff",  # With dashes
            "X" * 32,  # Wrong characters
        ]
        
        for invalid_id in invalid_session_ids:
            logging.info(f"Testing invalid session ID: '{invalid_id}'")
            
            esp_session = ESPSession(self.config)
            esp_session.set_custom_session_id(invalid_id)
            
            # Try to make an authenticated request
            response = esp_session.make_authenticated_request("/WsSMC/Activity")

            # self.debugResponseDetails(response)
            logging.debug(f"History: {response.history}")
            
            # Should fail (not 200) or redirect to login
            reason = self.assertResponseFailure(response, f"Invalid session ID '{invalid_id}' should be rejected")
            
            logging.info(f"✓ Invalid session ID '{invalid_id}' rejected ({reason})")
    
    def test_session_timeout_via_espcontrol(self):
        """Test 3: Session timeout via ws_espcontrol."""
        logging.info("=== Test 3: Session Timeout via ws_espcontrol ===")
        
        # Login as regular user
        user_session = ESPSession(self.config)
        self.assertTrue(
            user_session.login(self.config.user, self.config.user_password),
            "Failed to login as regular user"
        )
        
        user_session_id = user_session.session_id
        logging.info(f"User session ID: {user_session_id}")
        
        # Verify session works
        response = user_session.make_authenticated_request("/WsSMC/Activity")
        self.assertResponseSuccess(response, "Initial authenticated request should work")
        logging.info("✓ User session is active")
        
        # Login as admin and use ws_espcontrol
        admin_client = WSESPControlClient(self.config)
        self.assertTrue(
            admin_client.login_as_admin(),
            "Failed to login as admin"
        )
        logging.info("✓ Admin logged in")
        
        # Query sessions to find user's external session ID
        sessions_response = admin_client.query_sessions(user_id=self.config.user)
        self.assertIsNotNone(sessions_response, "Failed to query sessions")
        
        # Parse the response to extract external session ID
        # The external ID is a hash of the internal session ID
        response_text = sessions_response.text
        logging.debug(f"Session query response: {response_text[:500]}")
        
        # Extract session ID from XML/JSON response
        # For XML: <ID>hash...</ID>
        # For JSON: "ID": "hash..."
        external_id_match = re.search(ID_EXTRACTION_PATTERN, response_text)
        
        if external_id_match:
            external_id = external_id_match.group(1) or external_id_match.group(2)
            logging.info(f"✓ Found external session ID: {external_id}")
            
            # Set timeout to 1 minute (near future)
            self.assertTrue(
                admin_client.set_session_timeout(external_id, 1),
                "Failed to set session timeout"
            )
            logging.info("✓ Session timeout set to 1 minute")
            
            # Wait for session to expire (1 minute + buffer for ESP cleanup cycle)
            # ESP checks sessions periodically (every 30 seconds by default)
            wait_time = 90  # 1.5 minutes
            logging.info(f"Waiting {wait_time} seconds for session to expire...")
            time.sleep(wait_time)
            
            # Try to use the expired session
            response = user_session.make_authenticated_request("/WsSMC/Activity")
            
            # Session should be invalid (not 200 or redirect)
            reason = self.assertResponseFailure(response, "Session should be expired and invalid")
            logging.info(f"✓ Session expired as expected ({reason})")
        else:
            self.fail("Could not extract external session ID from query response")
    
    def test_logout_invalidates_session(self):
        """Test 4: Verify logout invalidates the session."""
        logging.info("=== Test 4: Logout Session Invalidation ===")
        
        esp_session = ESPSession(self.config)
        
        # Login
        self.assertTrue(
            esp_session.login(self.config.user, self.config.user_password),
            "Failed to login"
        )
        
        session_id = esp_session.session_id
        logging.info(f"Logged in with session ID: {session_id}")
        
        # Verify session works
        response = esp_session.make_authenticated_request("/WsSMC/Activity")
        self.assertResponseSuccess(response, "Session should be valid before logout")
        logging.info("✓ Session is active before logout")
        
        # Logout
        self.assertTrue(esp_session.logout(), "Logout should succeed")
        logging.info("✓ Logout successful")
        
        # Try to use the session after logout
        response = esp_session.make_authenticated_request("/WsSMC/Activity")
        
        # Session should be invalid
        reason = self.assertResponseFailure(response, "Session should be invalid after logout")
        logging.info(f"✓ Session invalidated after logout ({reason})")
    
    def test_concurrent_sessions_single_user(self):
        """Load test: multiple concurrent sessions for one user."""
        logging.info("=== Concurrent Sessions Load Test ===")
        
        max_concurrent = 10
        
        def create_session(index: int) -> tuple:
            """Create a session and return (index, session_id, success)."""
            esp_session = ESPSession(self.config)
            success = esp_session.login(self.config.user, self.config.user_password)
            return (index, esp_session.session_id if success else None, success)
        
        # Create concurrent sessions
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(create_session, i) for i in range(max_concurrent)]
            results = [future.result() for future in as_completed(futures)]
        
        successful_sessions = [(idx, sid) for idx, sid, success in results if success and sid]
        
        logging.info(f"✓ Created {len(successful_sessions)} concurrent sessions")
        for idx, sid in successful_sessions:
            logging.debug(f"  Session {idx}: {sid}")
        
        # Verify all sessions are active
        active_count = 0
        for idx, sid in successful_sessions:
            esp_session = ESPSession(self.config)
            esp_session.session_id = sid
            esp_session.set_custom_session_id(sid)
            try:
                response = esp_session.make_authenticated_request("/WsSMC/Activity")
                failures = self.getResponseFailureReasons(response)
                if failures == []:
                    active_count += 1
                else:
                    logging.warning(f"Session {idx} inactive: {', '.join(failures)}")
            except Exception as e:
                logging.debug(f"Session {idx} verification failed: {e}")
        
        logging.info(f"✓ {active_count}/{len(successful_sessions)} sessions are active")
        
        self.assertEqual(active_count, len(successful_sessions), "All successful sessions should be active")
    
    def test_session_id_uniqueness(self):
        """Generate many session IDs and verify no duplicates."""
        logging.info("=== Session ID Uniqueness Test ===")
        
        target_sessions = 1000
        session_ids: Set[str] = set()
        external_ids: Set[str] = set()
        session_pairs: list = []  # [(session_id, external_id), ...]
        failed_logins = 0
        failed_queries = 0
        
        # Login as admin to query session info
        admin_client = WSESPControlClient(self.config)
        self.assertTrue(
            admin_client.login_as_admin(),
            "Failed to login as admin for session queries"
        )
        logging.info("✓ Admin logged in for session queries")
        
        logging.info(f"Generating {target_sessions} session IDs...")
        
        for i in range(target_sessions):
            esp_session = ESPSession(self.config)
            
            if esp_session.login(self.config.user, self.config.user_password):
                if esp_session.session_id:
                    session_id = esp_session.session_id
                    session_ids.add(session_id)
                    
                    # Query to get external ID
                    sessions_response = admin_client.query_sessions(user_id=self.config.user)
                    if sessions_response:
                        # Extract external ID from response
                        external_id_match = re.search(
                            ID_EXTRACTION_PATTERN,
                            sessions_response.text
                        )
                        if external_id_match:
                            external_id = external_id_match.group(1) or external_id_match.group(2)
                            external_ids.add(external_id)
                            session_pairs.append((session_id, external_id))
                        else:
                            failed_queries += 1
                            logging.warning(f"Could not extract external ID for session {i}")
                    else:
                        failed_queries += 1
                        logging.warning(f"Failed to query session info for session {i}")
                    
                    # Logout to clean up
                    esp_session.logout()
                else:
                    failed_logins += 1
            else:
                failed_logins += 1
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                logging.info(f"  Progress: {i + 1}/{target_sessions} sessions created")
        
        unique_session_count = len(session_ids)
        unique_external_count = len(external_ids)
        session_duplicate_count = target_sessions - failed_logins - unique_session_count
        external_duplicate_count = len(session_pairs) - unique_external_count
        
        logging.info(f"✓ Generated {target_sessions} sessions")
        logging.info(f"  Unique session IDs: {unique_session_count}")
        logging.info(f"  Session ID duplicates: {session_duplicate_count}")
        logging.info(f"  Unique external IDs: {unique_external_count}")
        logging.info(f"  External ID duplicates: {external_duplicate_count}")
        logging.info(f"  Failed logins: {failed_logins}")
        logging.info(f"  Failed queries: {failed_queries}")
        
        # Check for duplicate session IDs
        self.assertEqual(
            session_duplicate_count,
            0,
            f"Found {session_duplicate_count} duplicate session IDs!"
        )
        logging.info("✓ All session IDs are unique")
        
        # Check for duplicate external IDs
        self.assertEqual(
            external_duplicate_count,
            0,
            f"Found {external_duplicate_count} duplicate external IDs!"
        )
        logging.info("✓ All external IDs are unique")
        
        # Check that external ID differs from session ID for each pair
        same_id_count = 0
        for session_id, external_id in session_pairs:
            if session_id == external_id:
                same_id_count += 1
                logging.error(f"Session ID matches external ID: {session_id}")
        
        self.assertEqual(
            same_id_count,
            0,
            f"Found {same_id_count} cases where session ID equals external ID!"
        )
        logging.info("✓ All external IDs differ from their corresponding session IDs")
    
    def test_active_session_id_collision_detection(self):
        """ID uniqueness and collision detection with many concurrent active sessions."""
        logging.info("=== Active Session ID Collision Detection ===")
        
        target_sessions = 1000
        esp_sessions = []  # Keep session objects alive
        session_ids: Set[str] = set()
        failed_logins = 0
        
        logging.info(f"Creating {target_sessions} concurrent active sessions...")
        
        # Create all sessions WITHOUT logout (keep them active)
        for i in range(target_sessions):
            esp_session = ESPSession(self.config)
            
            if esp_session.login(self.config.user, self.config.user_password):
                if esp_session.session_id:
                    session_ids.add(esp_session.session_id)
                    esp_sessions.append(esp_session)  # Keep session alive
                else:
                    failed_logins += 1
            else:
                failed_logins += 1
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                logging.info(f"  Progress: {i + 1}/{target_sessions} active sessions")
        
        successful_sessions = len(esp_sessions)
        logging.info(f"✓ Created {successful_sessions} active sessions")
        
        # Verify all session IDs are unique while sessions are active
        self.assertEqual(
            len(session_ids),
            successful_sessions,
            f"Expected {successful_sessions} unique session IDs, but got {len(session_ids)} - collision detected!"
        )
        logging.info(f"✓ All {successful_sessions} session IDs are unique (no collisions)")
        
        # Query admin API once to get all external IDs for these active sessions
        admin_client = WSESPControlClient(self.config)
        self.assertTrue(
            admin_client.login_as_admin(),
            "Failed to login as admin"
        )
        logging.info("✓ Admin logged in for session query")
        
        sessions_response = admin_client.query_sessions(user_id=self.config.user)
        self.assertIsNotNone(sessions_response, "Failed to query active sessions")
        
        # Extract ALL external IDs from the response
        external_ids = set(re.findall(
            ID_EXTRACTION_PATTERN,
            sessions_response.text
        ))
        # Flatten the tuples from findall (alternation creates groups)
        external_ids = {eid[0] or eid[1] for eid in external_ids}
        
        logging.info(f"✓ Retrieved {len(external_ids)} external IDs from admin API")
        
        # Verify count matches and all external IDs are unique
        self.assertEqual(
            len(external_ids),
            successful_sessions,
            f"Expected {successful_sessions} external IDs, but got {len(external_ids)}"
        )
        logging.info(f"✓ All {len(external_ids)} external IDs are unique")
        
        # Verify external IDs differ from session IDs
        common_ids = session_ids & external_ids
        self.assertEqual(
            len(common_ids),
            0,
            f"Found {len(common_ids)} session IDs that match their external IDs!"
        )
        logging.info("✓ All external IDs differ from their session IDs")
        
        logging.info(f"✓ Collision detection test passed: {successful_sessions} concurrent sessions, all unique")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=f"ESP Session ID Test Suite v{__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  HPCC_TEST_USER_PW     Password for regular user (required)
  HPCC_TEST_ADMIN_PW    Password for admin user (required)

Examples:
  # Run all tests with environment variables
  export HPCC_TEST_USER_PW=password1
  export HPCC_TEST_ADMIN_PW=adminpass
  python3 sessionid_test.py

  # Run specific test
  python3 sessionid_test.py -t test_new_format_validation

  # Use custom host and credentials
  python3 sessionid_test.py --host 192.168.1.100 --user myuser --user-pw mypass
        """
    )
    
    parser.add_argument('--protocol', default=DEFAULT_PROTOCOL,
                       help=f'Protocol (http or https, default: {DEFAULT_PROTOCOL})')
    parser.add_argument('--host', default=DEFAULT_HOST,
                       help=f'ESP host (default: {DEFAULT_HOST})')
    parser.add_argument('--port', default=DEFAULT_PORT,
                       help=f'ESP port (default: {DEFAULT_PORT})')
    parser.add_argument('--user', default=DEFAULT_USER,
                       help=f'Regular user ID (default: {DEFAULT_USER})')
    parser.add_argument('--user-pw',
                       help='Regular user password (default: $HPCC_TEST_USER_PW)')
    parser.add_argument('--admin', default=DEFAULT_ADMIN,
                       help=f'Admin user ID (default: {DEFAULT_ADMIN})')
    parser.add_argument('--admin-pw',
                       help='Admin user password (default: $HPCC_TEST_ADMIN_PW)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('-t', '--test', 
                       help='Run specific test (e.g., test_new_format_validation)')
    
    args = parser.parse_args()
    
    # Get passwords from environment if not provided
    user_pw = args.user_pw or os.getenv('HPCC_TEST_USER_PW')
    admin_pw = args.admin_pw or os.getenv('HPCC_TEST_ADMIN_PW')
    
    if not user_pw:
        parser.error("User password required: use --user-pw or set HPCC_TEST_USER_PW")
    if not admin_pw:
        parser.error("Admin password required: use --admin-pw or set HPCC_TEST_ADMIN_PW")
    
    return args, user_pw, admin_pw


def main():
    """Main entry point."""
    args, user_pw, admin_pw = parse_arguments()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create test configuration
    config = ESPTestConfig(
        protocol=args.protocol,
        host=args.host,
        port=args.port,
        user=args.user,
        user_password=user_pw,
        admin=args.admin,
        admin_password=admin_pw
    )
    
    logging.info("=" * 70)
    logging.info(f"ESP Session ID Test Suite v{__version__}")
    logging.info("=" * 70)
    logging.info(f"Configuration: {config}")
    logging.info("")
    
    # Set configuration for test classes
    TestSessionIDFormat._config = config
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    if args.test:
        # Run specific test
        if hasattr(TestSessionIDFormat, args.test):
            suite.addTest(TestSessionIDFormat(args.test))
        else:
            logging.error(f"Test '{args.test}' not found in available test cases.")
            return 1
    else:
        # Run all tests from TestSessionIDFormat
        suite.addTests(loader.loadTestsFromTestCase(TestSessionIDFormat))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    result = runner.run(suite)
    
    # Print summary
    logging.info("")
    logging.info("=" * 70)
    if result.wasSuccessful():
        logging.info("✓ ALL TESTS PASSED")
    else:
        logging.error(f"✗ TESTS FAILED: {len(result.failures)} failures, {len(result.errors)} errors")
    logging.info("=" * 70)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
