#!/usr/bin/env python3
"""Integration tests for WsPackageProcess target validation and metadata operations."""

import argparse
import base64
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree


DEFAULT_PROTOCOL = "http"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8010
DEFAULT_ROXIE_TARGET = "roxie"
DEFAULT_NON_ROXIE_TARGET = "thor"
DEFAULT_PACKAGE_MAP = f"packageprocess-test-{os.getpid()}"
SOAP_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"


def default_ecl_path():
    installed = Path.home() / "runtime/opt/HPCCSystems/bin/ecl"
    return shutil.which("ecl") or str(installed)


class PackageProcessConfig:
    """Configuration shared by the package-process tests."""

    def __init__(self, args):
        self.protocol = "https" if args.https else DEFAULT_PROTOCOL
        self.host = args.host
        self.port = args.port
        self.roxie_target = args.roxie_target
        self.non_roxie_target = args.non_roxie_target
        self.package_file = args.package_file
        self.part_name = args.part_name
        self.ecl = str(Path(args.ecl).expanduser())
        self.user = args.user
        self.password = args.password
        self.accept_self_signed = args.accept_self_signed
        self.use_cli_for_known_failures = args.use_cli_for_known_failures
        self.verbose = args.verbose
        self.base_package_map = args.package_map

    @property
    def service_url(self):
        return f"{self.protocol}://{self.host}:{self.port}/WsPackageProcess/"

    def cli_options(self):
        options = [f"--server={self.host}", f"--port={self.port}"]
        if self.user:
            options.extend(
                [f"--username={self.user}", f"--password={self.password}"]
            )
        if self.protocol == "https":
            options.append("--ssl")
        if self.accept_self_signed:
            options.append("--accept-self-signed")
        return options


class TestPackageProcess(unittest.TestCase):
    """WsPackageProcess regression tests."""

    config = None

    @classmethod
    def setUpClass(cls):
        if cls.config is None:
            raise RuntimeError("Test configuration not set")

    def setUp(self):
        suffix = self._testMethodName
        if suffix.startswith("test_"):
            suffix = suffix[5:]
        suffix = suffix.replace("_", "-")
        self.package_map = f"{self.config.base_package_map}-{suffix}"
        self.temp_dir = tempfile.TemporaryDirectory(prefix="packageprocess-")
        self.package_file = self._prepare_package_file()
        self.part_name = self.config.part_name or self.package_map
        self.map_added = False

    def tearDown(self):
        try:
            if self.map_added:
                self._assert_soap_success(
                    "DeletePackage",
                    message="SOAP cleanup failed",
                )
        finally:
            self.temp_dir.cleanup()

    def _prepare_package_file(self):
        if self.config.package_file:
            return self.config.package_file

        package_file = Path(self.temp_dir.name) / "packageprocess.pkg"
        root = ElementTree.Element("RoxiePackages")
        ElementTree.SubElement(root, "Package", {"id": self.package_map})
        ElementTree.ElementTree(root).write(
            package_file,
            encoding="utf-8",
            xml_declaration=True,
        )
        return package_file

    def _run_cli(self, *command):
        full_command = [
            self.config.ecl,
            "packagemap",
            *command,
            *self.config.cli_options(),
        ]
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            check=False,
        )
        if self.config.verbose:
            safe_command = [
                "***" if item.startswith("--password=") else item
                for item in full_command
            ]
            print(f"\n$ {' '.join(safe_command)}")
            if result.stdout:
                print(result.stdout.rstrip())
            if result.stderr:
                print(result.stderr.rstrip(), file=sys.stderr)
        return result

    def _assert_cli_success(self, *command):
        result = self._run_cli(*command)
        self.assertEqual(
            result.returncode,
            0,
            f"CLI command failed:\n{result.stdout}{result.stderr}",
        )
        return result.stdout + result.stderr

    def _assert_cli_rejected(self, command, expected_message):
        result = self._run_cli(*command)
        output = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0, output)
        self.assertIn(expected_message, output)

    def _add_roxie_map(self):
        self._assert_cli_success(
            "add",
            self.config.roxie_target,
            str(self.package_file),
            f"--pmid={self.package_map}",
        )
        self.map_added = True

    def _soap_envelope(self, method):
        ElementTree.register_namespace("soap", SOAP_NAMESPACE)
        envelope = ElementTree.Element(f"{{{SOAP_NAMESPACE}}}Envelope")
        body = ElementTree.SubElement(envelope, f"{{{SOAP_NAMESPACE}}}Body")
        request = ElementTree.SubElement(body, f"{method}Request")
        ElementTree.SubElement(request, "Target").text = self.config.roxie_target
        ElementTree.SubElement(request, "PackageMap").text = self.package_map
        if method == "DeletePackage":
            ElementTree.SubElement(request, "Process").text = "*"
        ElementTree.SubElement(request, "GlobalScope").text = "0"
        if method != "DeletePackage":
            ElementTree.SubElement(request, "PartName").text = self.part_name
        return ElementTree.tostring(
            envelope,
            encoding="utf-8",
            xml_declaration=True,
        )

    def _validate_package_envelope(self):
        ElementTree.register_namespace("soap", SOAP_NAMESPACE)
        envelope = ElementTree.Element(f"{{{SOAP_NAMESPACE}}}Envelope")
        body = ElementTree.SubElement(envelope, f"{{{SOAP_NAMESPACE}}}Body")
        request = ElementTree.SubElement(body, "ValidatePackageRequest")
        ElementTree.SubElement(request, "Info").text = self.package_file.read_text(
            encoding="utf-8"
        )
        ElementTree.SubElement(request, "Target").text = (
            self.config.non_roxie_target
        )
        ElementTree.SubElement(request, "Active").text = "0"
        ElementTree.SubElement(request, "CheckDFS").text = "0"
        ElementTree.SubElement(request, "GlobalScope").text = "0"
        return ElementTree.tostring(
            envelope,
            encoding="utf-8",
            xml_declaration=True,
        )

    def _send_soap(self, method, payload=None):
        request = Request(
            self.config.service_url,
            data=payload or self._soap_envelope(method),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": method,
            },
            method="POST",
        )
        if self.config.user:
            credentials = f"{self.config.user}:{self.config.password}".encode()
            encoded = base64.b64encode(credentials).decode("ascii")
            request.add_header("Authorization", f"Basic {encoded}")

        context = None
        if self.config.protocol == "https" and self.config.accept_self_signed:
            context = ssl._create_unverified_context()

        try:
            with urlopen(request, timeout=30, context=context) as response:
                return response.status, response.read().decode(
                    "utf-8",
                    errors="replace",
                )
        except HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", errors="replace")
        except (URLError, OSError) as exc:
            self.fail(f"SOAP request failed for {method}: {exc}")

    def _assert_soap_success(self, method, message=None):
        status, response = self._send_soap(method)
        try:
            root = ElementTree.fromstring(response)
        except ElementTree.ParseError as exc:
            self.fail(f"{message or method}: invalid SOAP response: {exc}\n{response}")

        local_names = [element.tag.rsplit("}", 1)[-1] for element in root.iter()]
        self.assertLess(status, 400, message or response)
        self.assertIn(f"{method}Response", local_names, message or response)
        self.assertNotIn("Exception", local_names, message or response)
        if self.config.verbose:
            print(response.rstrip())
        return response

    def test_01_reject_non_roxie_add(self):
        """T01: add to a non-Roxie target returns 'Roxie target required'."""
        self._assert_cli_rejected(
            [
                "add",
                self.config.non_roxie_target,
                str(self.package_file),
                f"--pmid={self.package_map}",
            ],
            "Roxie target required",
        )

    def test_02_accept_roxie_add(self):
        """T02: add to a Roxie target succeeds."""
        self._add_roxie_map()

    def test_03_list_package_metadata(self):
        """T03: list returns the newly added target-scoped package map."""
        self._add_roxie_map()
        output = self._assert_cli_success("list", self.config.roxie_target)
        self.assertIn(f"{self.config.roxie_target}::{self.package_map}", output)

    def test_04_activate_package_map(self):
        """T04: activate succeeds for a target-scoped Roxie package map."""
        self._add_roxie_map()
        self._assert_cli_success(
            "activate",
            self.config.roxie_target,
            self.package_map,
        )

    def test_05_read_active_package_map(self):
        """T05: info returns the active target-scoped package map."""
        self._add_roxie_map()
        self._assert_cli_success(
            "activate",
            self.config.roxie_target,
            self.package_map,
        )
        output = self._assert_cli_success("info", self.config.roxie_target)
        self.assertIn(self.package_map, output)

    def test_06_deactivate_package_map(self):
        """T06: deactivate succeeds for a target-scoped Roxie package map."""
        self._add_roxie_map()
        self._assert_cli_success(
            "activate",
            self.config.roxie_target,
            self.package_map,
        )
        self._assert_cli_success(
            "deactivate",
            self.config.roxie_target,
            self.package_map,
        )

    def test_07_read_package_map_part(self):
        """T07: get-part succeeds with explicit target-scoped semantics."""
        self._add_roxie_map()
        if self.config.use_cli_for_known_failures:
            output = self._assert_cli_success(
                "get-part",
                self.config.roxie_target,
                self.package_map,
                self.part_name,
            )
            self.assertIn(self.part_name, output)
        else:
            response = self._assert_soap_success("GetPartFromPackageMap")
            self.assertIn(self.part_name, response)

    def test_08_remove_package_map_part(self):
        """T08: remove-part succeeds with explicit target-scoped semantics."""
        self._add_roxie_map()
        if self.config.use_cli_for_known_failures:
            self._assert_cli_success(
                "remove-part",
                self.config.roxie_target,
                self.package_map,
                self.part_name,
            )
        else:
            self._assert_soap_success("RemovePartFromPackageMap")

    def test_09_delete_package_map(self):
        """T09: delete succeeds with explicit target-scoped semantics."""
        self._add_roxie_map()
        if self.config.use_cli_for_known_failures:
            self._assert_cli_success(
                "delete",
                self.config.roxie_target,
                self.package_map,
            )
        else:
            response = self._assert_soap_success("DeletePackage")
            codes = [
                element.text
                for element in ElementTree.fromstring(response).iter()
                if element.tag.rsplit("}", 1)[-1] == "Code"
            ]
            self.assertEqual(codes, ["0"], response)
        # mark it as no longer added so that tearDown doesn't try to remove it again.
        self.map_added = False

    def test_10_reject_non_roxie_add_part(self):
        """T10: add-part to a non-Roxie target returns the target error."""
        self._assert_cli_rejected(
            [
                "add-part",
                self.config.non_roxie_target,
                self.package_map,
                str(self.package_file),
            ],
            "Roxie target required",
        )

    def test_11_reject_non_roxie_copy(self):
        """T11: copying a package map to non-Roxie returns the target error."""
        self._add_roxie_map()
        self._assert_cli_rejected(
            [
                "copy",
                f"{self.config.roxie_target}/{self.package_map}",
                self.config.non_roxie_target,
                f"--pmid={self.package_map}-copy",
            ],
            "Roxie target required",
        )

    def test_12_reject_non_roxie_validate(self):
        """T12: validating against non-Roxie returns the target error."""
        status, response = self._send_soap(
            "ValidatePackage",
            self._validate_package_envelope(),
        )
        self.assertLess(status, 500, response)
        self.assertIn("Roxie target required", response)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="WsPackageProcess regression tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tests:
  T01  Reject add to a non-Roxie target with a helpful error.
  T02  Accept add to a Roxie target.
  T03  List the added package-map metadata.
  T04  Activate the package map.
  T05  Read the active package map.
  T06  Deactivate the package map.
  T07  Read a package-map part using SOAP by default.
  T08  Remove a package-map part using SOAP by default.
  T09  Delete the package map using SOAP by default.
  T10  Reject add-part to a non-Roxie target with a helpful error.
  T11  Reject copying a package map to a non-Roxie target.
  T12  Reject package-map validation against a non-Roxie target.

Examples:
  python3 packageprocess_test.py
  python3 packageprocess_test.py -t test_01_reject_non_roxie_add
  python3 packageprocess_test.py --ecl "~/runtime/opt/HPCCSystems/bin/ecl"

Note:
  --use-cli-for-known-failures is to expose and test for known issue 37673 which,
  until fixed, will cause the affected tests to fail when using CLI. Without
  this option the test instead uses SOAP to exercise the underlying ESP services.
""",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--roxie-target", default=DEFAULT_ROXIE_TARGET)
    parser.add_argument("--non-roxie-target", default=DEFAULT_NON_ROXIE_TARGET)
    parser.add_argument("--package-map", default=DEFAULT_PACKAGE_MAP)
    parser.add_argument("--package-file", type=Path)
    parser.add_argument("--part-name")
    parser.add_argument(
        "--ecl",
        default=default_ecl_path(),
        help="Path to the ECL executable, default is " + str(default_ecl_path()),
    )
    parser.add_argument("--user", default=os.getenv("HPCC_USER"))
    parser.add_argument("--password", default=os.getenv("HPCC_PASSWORD"))
    parser.add_argument("--https", action="store_true")
    parser.add_argument("--accept-self-signed", action="store_true")
    parser.add_argument(
        "--use-cli-for-known-failures",
        action="store_true",
        help="Use CLI instead of SOAP for get-part, remove-part, and delete",
    )
    parser.add_argument("-t", "--test", help="Run one unittest method")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    ecl_path = Path(args.ecl).expanduser()
    if not ecl_path.is_file() or not os.access(ecl_path, os.X_OK):
        parser.error(f"ecl executable not found or not executable: {ecl_path}")
    args.ecl = str(ecl_path)
    if (args.user or args.password is not None) and (
        not args.user or args.password is None
    ):
        parser.error("--user and --password must be supplied together")
    if args.package_file:
        args.package_file = args.package_file.expanduser()
        if not args.package_file.is_file():
            parser.error(f"package file not found: {args.package_file}")
        if not args.part_name:
            parser.error("--part-name is required with --package-file")
    return args


def main():
    args = parse_arguments()
    TestPackageProcess.config = PackageProcessConfig(args)

    loader = unittest.TestLoader()
    if args.test:
        if not hasattr(TestPackageProcess, args.test):
            print(f"ERROR: test '{args.test}' not found", file=sys.stderr)
            return 1
        suite = unittest.TestSuite([TestPackageProcess(args.test)])
    else:
        suite = loader.loadTestsFromTestCase(TestPackageProcess)

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
