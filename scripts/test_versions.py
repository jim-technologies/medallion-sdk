#!/usr/bin/env python3
"""Exercise version synchronization in an isolated repository fixture."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_FILES = (
    ".flox/env/manifest.toml",
    "LICENSE",
    "NOTICE",
    "VERSION",
    "README.md",
    "go.mod",
    "package.json",
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    "python/pyproject.toml",
    "python/LICENSE",
    "python/NOTICE",
    "python/src/medallion/py.typed",
    "python/uv.lock",
    "scripts/check_versions.py",
    "scripts/set_version.py",
)
VERSION_MIRRORS = (
    "VERSION",
    "package.json",
    "python/pyproject.toml",
    "python/uv.lock",
)


class VersionScriptsTest(unittest.TestCase):
    fixture: Path

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture = Path(self.temporary.name)
        for relative in FIXTURE_FILES:
            source = ROOT / relative
            destination = self.fixture / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

    def run_script(
        self, script: str, *arguments: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        process_env = os.environ.copy()
        process_env["PYTHONDONTWRITEBYTECODE"] = "1"
        if env is not None:
            process_env.update(env)
        return subprocess.run(
            [sys.executable, f"scripts/{script}", *arguments],
            cwd=self.fixture,
            env=process_env,
            check=False,
            capture_output=True,
            text=True,
        )

    def mirror_snapshot(self) -> dict[str, bytes]:
        return {
            relative: (self.fixture / relative).read_bytes()
            for relative in VERSION_MIRRORS
        }

    def test_setter_updates_every_mirror_and_tag_check(self) -> None:
        result = self.run_script("set_version.py", "0.2.3")
        self.assertEqual(result.returncode, 0, result.stderr)

        package = json.loads((self.fixture / "package.json").read_text())
        self.assertEqual((self.fixture / "VERSION").read_text().strip(), "0.2.3")
        self.assertEqual(package["version"], "0.2.3")
        self.assertIn('version = "0.2.3"', (self.fixture / "python/pyproject.toml").read_text())
        self.assertIn('version = "0.2.3"', (self.fixture / "python/uv.lock").read_text())

        tagged = self.run_script(
            "check_versions.py",
            env={"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": "v0.2.3"},
        )
        self.assertEqual(tagged.returncode, 0, tagged.stderr)
        prefixed = self.run_script(
            "check_versions.py",
            env={"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": "go/v0.2.3"},
        )
        self.assertNotEqual(prefixed.returncode, 0)
        self.assertIn("expected 'v0.2.3'", prefixed.stderr)

    def test_invalid_and_v2_versions_do_not_modify_files(self) -> None:
        before = self.mirror_snapshot()
        for version in ("01.2.3", "1.2", "2.0.0"):
            with self.subTest(version=version):
                result = self.run_script("set_version.py", version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.mirror_snapshot(), before)

    def test_checker_detects_drift(self) -> None:
        package_path = self.fixture / "package.json"
        package = json.loads(package_path.read_text())
        expected = (self.fixture / "VERSION").read_text().strip()
        major, minor, patch = (int(component) for component in expected.split("."))
        drifted = f"{major}.{minor}.{patch + 1}"
        package["version"] = drifted
        package_path.write_text(json.dumps(package, indent=2) + "\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            f"package.json has {drifted!r}; expected {expected!r}", result.stderr
        )

    def test_checker_rejects_go_toolchain_drift(self) -> None:
        go_mod = (self.fixture / "go.mod").read_text()
        go_version_match = re.search(r"^go\s+(\S+)$", go_mod, re.MULTILINE)
        self.assertIsNotNone(go_version_match)
        go_version = go_version_match.group(1)
        manifest_path = self.fixture / ".flox/env/manifest.toml"
        manifest = manifest_path.read_text()
        manifest_path.write_text(
            manifest.replace(
                f'GOTOOLCHAIN = "go{go_version}+auto"',
                'GOTOOLCHAIN = "go0.0.0+auto"',
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            ".flox/env/manifest.toml GOTOOLCHAIN", result.stderr
        )

    def test_checker_rejects_documented_go_version_drift(self) -> None:
        readme_path = self.fixture / "README.md"
        readme = readme_path.read_text()
        readme_path.write_text(
            re.sub(
                r"\bGo [0-9]+\.[0-9]+\.[0-9]+",
                "Go 0.0.0",
                readme,
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md Go toolchain versions", result.stderr)

    def test_checker_rejects_documented_pnpm_version_drift(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            re.sub(
                r"\bpnpm [0-9]+\.[0-9]+\.[0-9]+",
                "pnpm 0.0.0",
                readme_path.read_text(),
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md pnpm versions", result.stderr)

    def test_checker_rejects_missing_python_type_marker(self) -> None:
        (self.fixture / "python/src/medallion/py.typed").unlink()

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("src/medallion/py.typed", result.stderr)

    def test_checker_rejects_unsafe_npm_git_install_docs(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            readme_path.read_text().replace("npm install --allow-git=all", "npm install")
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Git npm install must pass --allow-git=all", result.stderr)

    def test_checker_rejects_stale_invariant_build_pin(self) -> None:
        workspace_path = self.fixture / "pnpm-workspace.yaml"
        workspace = workspace_path.read_text()
        workspace += (
            '  "@jim-technologies/invariant-protocol@'
            "https://codeload.github.com/jim-technologies/invariantprotocol/"
            'tar.gz/0000000000000000000000000000000000000000": true\n'
        )
        workspace_path.write_text(workspace)

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "pnpm-workspace.yaml invariantprotocol build pins", result.stderr
        )

    def test_checker_rejects_stale_invariant_lock_pin(self) -> None:
        package = json.loads((self.fixture / "package.json").read_text())
        expected_sha = package["dependencies"][
            "@jim-technologies/invariant-protocol"
        ].rsplit("#", 1)[1]
        lock_path = self.fixture / "pnpm-lock.yaml"
        lock = lock_path.read_text()
        locator_line = next(
            line.strip()
            for line in lock.splitlines()
            if line.strip().startswith("version: https://")
            and line.rstrip().endswith(expected_sha)
        )
        locator = locator_line.removeprefix("version: ")
        stale_locator = locator.removesuffix(expected_sha) + ("0" * len(expected_sha))
        lock_path.write_text(
            lock
            + f"\n  '@jim-technologies/invariant-protocol@{stale_locator}':\n"
            + f"    resolution: {{tarball: {stale_locator}}}\n"
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "pnpm-lock.yaml invariantprotocol resolutions", result.stderr
        )

    def test_checker_rejects_stale_invariant_lock_specifier(self) -> None:
        package = json.loads((self.fixture / "package.json").read_text())
        expected_specifier = package["dependencies"][
            "@jim-technologies/invariant-protocol"
        ]
        expected_sha = expected_specifier.rsplit("#", 1)[1]
        stale_specifier = expected_specifier.removesuffix(expected_sha) + (
            "0" * len(expected_sha)
        )
        lock_path = self.fixture / "pnpm-lock.yaml"
        lock_path.write_text(
            lock_path.read_text() + f"\n      specifier: {stale_specifier}\n"
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "pnpm-lock.yaml invariantprotocol specifier pins", result.stderr
        )

    def test_checker_rejects_esbuild_override_drift(self) -> None:
        workspace_path = self.fixture / "pnpm-workspace.yaml"
        workspace_path.write_text(
            workspace_path.read_text().replace(
                'esbuild: "0.28.1"',
                'esbuild: "0.0.0"',
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("esbuild overrides", result.stderr)

    def test_checker_rejects_esbuild_lock_drift(self) -> None:
        lock_path = self.fixture / "pnpm-lock.yaml"
        lock_path.write_text(
            lock_path.read_text().replace("  esbuild@0.28.1:", "  esbuild@0.0.0:", 1)
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("esbuild package versions", result.stderr)

    def test_checker_rejects_python_license_drift(self) -> None:
        (self.fixture / "python/NOTICE").write_text("stale notice\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python/NOTICE must be byte-identical", result.stderr)


if __name__ == "__main__":
    unittest.main()
