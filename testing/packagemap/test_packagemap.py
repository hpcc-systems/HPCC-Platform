# This is a script for testing ecl packagemaps from command line.  This script is based on
# bare-metal, but should be easily modified for a container environment.  The things to
# consider are the url for the server as well as adding username and password as well as a
# secure flag for all the ecl statements.
#
# Prerequisites:
#   - pytest and requests must be installed: pip install pytest requests
#   - The following files must exist in the ecl/ subdirectory:
#       ecl/icecream_data.ecl
#       ecl/icecream_query.ecl
#       ecl/icecream.pkg
#   - The following JSON comparison files must exist in JSONs/:
#       JSONs/Nopkg.json       — expected Roxie response with no packagemap applied
#       JSONs/Inactivepkg.json — expected Roxie response with an inactive packagemap
#       JSONs/Activepkg.json   — expected Roxie response with an active packagemap
#
# Usage:
#   See README.md for full usage instructions.

import subprocess
import json
import shutil
from pathlib import Path
import pytest
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOCALHOST = "localhost"
ROXIE_TARGET = "roxie"
THOR_TARGET = "thor"
CURL_URL = (
    f"http://{LOCALHOST}:8002/WsEcl/submit/query/roxie/icecream_query/json"
)
CURL_HEADERS = {"Content-Type": "application/json"}
CURL_PAYLOAD = {"icecream_query": {"name": "", "password": ""}}
PKG_NAME = "icecream.pkg"

# JSON comparison files are kept in ./JSONs relative to this script
JSON_DIR = Path(__file__).parent / "JSONs"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run(cmd: "list[str]", check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        safe_cmd = cmd[:]
        if "-u" in safe_cmd:
            i = safe_cmd.index("-u")
            if i + 1 < len(safe_cmd):
                safe_cmd[i + 1] = "<redacted>"
        pytest.fail(
            f"Command failed: {' '.join(safe_cmd)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return result


def roxie_query() -> dict:
    """POST the icecream_query to Roxie and return the parsed JSON response."""
    response = requests.post(CURL_URL, headers=CURL_HEADERS, json=CURL_PAYLOAD, timeout=30)
    response.raise_for_status()
    return response.json()


def load_json(filename: str) -> dict:
    """Load a local JSON file from JSONs/ and return its parsed contents."""
    file_path = JSON_DIR / filename
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Session-scoped fixture: prerequisites
# build data and then publish the query to Roxie before any tests run
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def setup_prerequisites():
    """
    Run once before the entire test session:
      1. Publish the Thor dataset.
      2. Publish the Roxie query.
    """
    if shutil.which("ecl") is None:
        pytest.skip("Skipping packagemap integration tests: 'ecl' CLI is not available on PATH")

    # Best-effort cleanup to keep the workflow idempotent when re-running locally
    run(["ecl", "packagemap", "deactivate", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST], check=False)
    run(["ecl", "packagemap", "delete", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST], check=False)

    print("\n[setup] Running icecream_data.ecl on Thor…")
    run(["ecl", "run", "-t", THOR_TARGET, "ecl/icecream_data.ecl", "-s", LOCALHOST])

    print("[setup] Publishing icecream_query.ecl to Roxie…")
    run(["ecl", "publish", "-t", ROXIE_TARGET, "ecl/icecream_query.ecl", "-s", LOCALHOST])

    try:
        yield  # tests run here
    finally:
        run(["ecl", "packagemap", "deactivate", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST], check=False)
        run(["ecl", "packagemap", "delete", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST], check=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPackagemap:
    """End-to-end packagemap workflow tests."""

    def test_01_baseline_query_matches_nopkg_json(self):
        """Verify the Roxie query response matches Nopkg.json before any packagemap is applied."""
        response = roxie_query()
        expected = load_json("Nopkg.json")
        assert response == expected, (
            "Baseline Roxie response does not match Nopkg.json.\n"
            f"Expected: {json.dumps(expected, indent=2)}\n"
            f"Got:      {json.dumps(response, indent=2)}"
        )

    def test_02_add_packagemap(self):
        """Add icecream.pkg to Roxie."""
        run(["ecl", "packagemap", "add", ROXIE_TARGET, f"ecl/{PKG_NAME}", "-s", LOCALHOST])
        run(["ecl", "roxie", "reload", ROXIE_TARGET, "-s", LOCALHOST])

    def test_03_packagemap_listed_as_inactive(self):
        """Verify icecream.pkg appears in the packagemap list and is inactive (active = 0)."""
        result = run(["ecl", "packagemap", "list", ROXIE_TARGET, "-s", LOCALHOST])
        expected_string = "Package Id = roxie::icecream.pkg  active = 0"
        assert expected_string in result.stdout, (
            f"Expected to find '{expected_string}' in packagemap list output.\n"
            f"stdout: {result.stdout}"
        )

    def test_04_inactive_packagemap_matches_inactivepkg_json(self):
        """
        The packagemap has been added but NOT activated yet.
        The Roxie query response should match Inactivepkg.json.
        """
        response = roxie_query()
        expected = load_json("Inactivepkg.json")
        assert response == expected, (
            "Roxie response with inactive packagemap does not match Inactivepkg.json.\n"
            f"Expected: {json.dumps(expected, indent=2)}\n"
            f"Got:      {json.dumps(response, indent=2)}"
        )

    def test_05_activate_packagemap(self):
        """Activate icecream.pkg on Roxie."""
        run(["ecl", "packagemap", "activate", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST])

    def test_06_packagemap_listed_as_active(self):
        """Verify icecream.pkg appears in the packagemap list and is active (active = 1)."""
        result = run(["ecl", "packagemap", "list", ROXIE_TARGET, "-s", LOCALHOST])
        expected_string = "Package Id = roxie::icecream.pkg  active = 1"
        assert expected_string in result.stdout, (
            f"Expected to find '{expected_string}' in packagemap list output.\n"
            f"stdout: {result.stdout}"
        )

    def test_07_activated_packagemap_matches_activepkg_json(self):
        """After activation the Roxie response should match Activepkg.json."""
        response = roxie_query()
        expected = load_json("Activepkg.json")
        assert response == expected, (
            "Roxie response with active packagemap does not match Activepkg.json.\n"
            f"Expected: {json.dumps(expected, indent=2)}\n"
            f"Got:      {json.dumps(response, indent=2)}"
        )

    def test_08_validate_packagemap(self):
        """Validate the packagemap — command must exit cleanly."""
        run(["ecl", "packagemap", "validate", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST])

    def test_09_deactivate_packagemap(self):
        """Deactivate icecream.pkg on Roxie."""
        run(["ecl", "packagemap", "deactivate", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST])

    def test_10_deactivated_packagemap_matches_inactivepkg_json(self):
        """After deactivation the Roxie query response should again match Inactivepkg.json."""
        response = roxie_query()
        expected = load_json("Inactivepkg.json")
        assert response == expected, (
            "Roxie response after packagemap deactivation does not match Inactivepkg.json.\n"
            f"Expected: {json.dumps(expected, indent=2)}\n"
            f"Got:      {json.dumps(response, indent=2)}"
        )

    def test_11_delete_packagemap(self):
        """Delete icecream.pkg from Roxie."""
        run(["ecl", "packagemap", "delete", ROXIE_TARGET, PKG_NAME, "-s", LOCALHOST])