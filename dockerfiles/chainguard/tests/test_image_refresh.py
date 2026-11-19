import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


TOOLING = Path(__file__).resolve().parents[1]


class ImageRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "docker.jsonl"
        docker = self.root / "docker"
        docker.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "with open(os.environ['MOCK_DOCKER_LOG'], 'a') as log:\n"
            "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "if sys.argv[1] == 'pull':\n"
            "    sys.exit(int(os.environ.get('MOCK_PULL_STATUS', '0')))\n"
        )
        docker.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": f"{self.root}:{os.environ['PATH']}",
            "HOME": str(self.root),
            "ENV_FILE": str(self.root / "no.env"),
            "CONTAINER_RUNTIME": "docker",
            "CHAINGUARD_WORKSPACE_ROOT": str(self.root),
            "CHAINGUARD_IMAGE": "example.test/builder:configured",
            "CHAINGUARD_GLIBC": "",
            "CHAINGUARD_GCC": "",
            "CHAINGUARD_ARCH": "amd64",
            "CHAINGUARD_NO_PULL": "0",
            "CHAINGUARD_PACKAGING": "0",
            "CHAINGUARD_BUILD_DIR": "",
            "CHAINGUARD_EXTRA_MOUNTS": "",
            "NUGET_MODE": "off",
            "JFROG_REGISTRY": "example.test",
            "MOCK_DOCKER_LOG": str(self.log),
            "MOCK_PULL_STATUS": "0",
        }

    def run_script(self, script, *args, succeeds=True):
        result = subprocess.run(
            ["bash", str(TOOLING / script), *args],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
        )
        if succeeds:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_rebuild_ignores_existing_image_and_refreshes_bases(self):
        calls = self.run_script(
            "create-build-image.sh",
            "--glibc", "2.39",
            "--arch", "amd64",
            "--tag-prefix", "example.test/hpcc",
            "--base-image", "other.test/gcc:configured",
            "--rebuild",
        )
        self.assertEqual([call[0] for call in calls], ["build", "run"])
        self.assertIn("--pull", calls[0])
        self.assertIn("--no-cache", calls[0])
        self.assertIn("SYSROOT_IMAGE=docker.io/library/ubuntu:24.04", calls[0])

    def test_no_pull_still_reuses_existing_local_builder(self):
        calls = self.run_script(
            "create-build-image.sh",
            "--glibc", "2.39",
            "--arch", "amd64",
            "--no-pull",
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["image", "inspect"])

    def test_explicit_builder_pull_precedes_uncached_packager(self):
        self.env["CHAINGUARD_PACKAGING"] = "1"
        calls = self.run_script("cmake-chainguard.sh", "--version")
        self.assertEqual(
            calls[0],
            ["pull", "--platform", "linux/amd64", self.env["CHAINGUARD_IMAGE"]],
        )
        build = calls[1]
        self.assertEqual(build[0], "build")
        self.assertIn("--no-cache", build)
        self.assertNotIn("--pull", build)
        self.assertIn(f"BASE_IMAGE={self.env['CHAINGUARD_IMAGE']}", build)

    def test_explicit_builder_no_pull_preserves_local_image(self):
        self.env["CHAINGUARD_NO_PULL"] = "1"
        calls = self.run_script("cmake-chainguard.sh", "--version")
        self.assertNotIn("pull", [call[0] for call in calls])
        self.assertIn("run", [call[0] for call in calls])

    def test_explicit_builder_pull_failure_stops_build(self):
        self.env["MOCK_PULL_STATUS"] = "1"
        self.env["CHAINGUARD_PACKAGING"] = "1"
        calls = self.run_script("cmake-chainguard.sh", "--version", succeeds=False)
        self.assertEqual([call[0] for call in calls], ["pull"])


class PackagerHeaderTests(unittest.TestCase):
    def test_apk_refresh_preserves_sysroot_header_isolation(self):
        dockerfile = (TOOLING / "containers/Dockerfile.packager").read_text()
        command = dockerfile.split("RUN ", 1)[1].replace("\\\n", "")
        for sysroot in (None, "", "/opt/hpcc-sysroot"):
            with self.subTest(sysroot=sysroot), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixed = root / "include-fixed"
                fixed.mkdir()
                for name in ("limits.h", "syslimits.h", "README"):
                    (fixed / name).write_text("GCC header\n")
                apk = root / "apk"
                apk.write_text(
                    "#!/bin/sh\n"
                    'mkdir -p "$MOCK_FIXED/bits" || exit\n'
                    'printf "Wolfi header\\n" > "$MOCK_FIXED/stdio.h" || exit\n'
                    'printf "Wolfi header\\n" > "$MOCK_FIXED/bits/stdlib.h"\n'
                )
                apk.chmod(0o755)
                gcc = root / "gcc"
                gcc.write_text('#!/bin/sh\nprintf "%s\\n" "$MOCK_FIXED"\n')
                gcc.chmod(0o755)
                env = {
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "MOCK_FIXED": str(fixed),
                }
                env.pop("HPCC_SYSROOT", None)
                if sysroot is not None:
                    env["HPCC_SYSROOT"] = sysroot
                result = subprocess.run(
                    ["sh", "-c", command], env=env, capture_output=True, text=True
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = {"limits.h", "syslimits.h", "README"}
                if not sysroot:
                    expected.update(("stdio.h", "bits", "bits/stdlib.h"))
                self.assertEqual(
                    {str(path.relative_to(fixed)) for path in fixed.rglob("*")},
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
