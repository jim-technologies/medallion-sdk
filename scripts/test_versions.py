#!/usr/bin/env python3
"""Exercise version synchronization and the release verb in a fixture repository."""

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
    "CHANGELOG.md",
    "LICENSE",
    "NOTICE",
    "VERSION",
    "Makefile",
    "README.md",
    "go.mod",
    "go/README.md",
    "package.json",
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    "python/pyproject.toml",
    "python/LICENSE",
    "python/NOTICE",
    "python/README.md",
    "python/src/medallion/py.typed",
    "python/src/medallion/workflows.py",
    "python/uv.lock",
    "scripts/check_versions.py",
    "scripts/release",
    "scripts/set_version.py",
)
GIT_IDENTITY = (
    "-c",
    "user.name=Version test",
    "-c",
    "user.email=version-test@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "-c",
    "tag.gpgsign=false",
)
# The release script's own git calls get an identity and no signing prompt
# from the environment, the way a maintainer's configuration supplies them.
RELEASE_GIT_ENV = {
    "GIT_AUTHOR_NAME": "Version test",
    "GIT_AUTHOR_EMAIL": "version-test@example.invalid",
    "GIT_COMMITTER_NAME": "Version test",
    "GIT_COMMITTER_EMAIL": "version-test@example.invalid",
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "tag.gpgsign",
    "GIT_CONFIG_VALUE_0": "false",
}
README_INSTALL_TAG = re.compile(r"medallion-sdk\.git#(v[0-9]+\.[0-9]+\.[0-9]+)")
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
        # The checker verifies the README install tag against the repository's
        # tags, so the fixture is a repository carrying that tag.
        self.git("init", "--quiet")
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", "Version test fixture")
        self.readme_tag = self.readme_install_tag()
        self.git("tag", self.readme_tag)

    def git(self, *arguments: str, cwd: Path | None = None) -> str:
        return subprocess.run(
            ["git", *GIT_IDENTITY, *arguments],
            cwd=self.fixture if cwd is None else cwd,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def release_changelog(self, version: str) -> None:
        changelog = self.fixture / "CHANGELOG.md"
        changelog.write_text(
            changelog.read_text().replace(
                "## [Unreleased]", f"## [{version}] - 2026-09-27", 1
            )
        )

    def release_ready_fixture(self) -> Path:
        """Commit a released CHANGELOG and push it as main of a bare origin."""

        version = (self.fixture / "VERSION").read_text().strip()
        self.release_changelog(version)
        self.git("commit", "--quiet", "--all", "-m", "Release fixture")
        origin_directory = tempfile.TemporaryDirectory()
        self.addCleanup(origin_directory.cleanup)
        origin = Path(origin_directory.name) / "origin.git"
        self.git("init", "--quiet", "--bare", str(origin))
        self.git("remote", "add", "origin", str(origin))
        self.git("push", "--quiet", "origin", "HEAD:refs/heads/main")
        return origin

    def run_release(self) -> subprocess.CompletedProcess[str]:
        process_env = os.environ.copy()
        for name in ("GITHUB_ACTIONS", "GITHUB_REF_TYPE", "GITHUB_REF_NAME"):
            process_env.pop(name, None)
        process_env.update(RELEASE_GIT_ENV)
        return subprocess.run(
            ["scripts/release"],
            cwd=self.fixture,
            env=process_env,
            check=False,
            capture_output=True,
            text=True,
        )

    def origin_tag_type(self, origin: Path, tag: str) -> str | None:
        found = subprocess.run(
            ["git", "--git-dir", str(origin), "cat-file", "-t", f"refs/tags/{tag}"],
            check=False,
            capture_output=True,
            text=True,
        )
        return found.stdout.strip() if found.returncode == 0 else None

    def readme_install_tag(self) -> str:
        match = README_INSTALL_TAG.search((self.fixture / "README.md").read_text())
        self.assertIsNotNone(match)
        assert match is not None
        return match.group(1)

    def run_script(
        self, script: str, *arguments: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        process_env = os.environ.copy()
        # Fixture checks provide their own synthetic release context. Do not
        # inherit an outer tag-triggered GitHub Actions run.
        process_env.pop("GITHUB_ACTIONS", None)
        process_env.pop("GITHUB_REF_TYPE", None)
        process_env.pop("GITHUB_REF_NAME", None)
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

    def create_fixture_tag(self, *, annotated: bool) -> str:
        version = (self.fixture / "VERSION").read_text().strip()
        tag = f"v{version}"
        if annotated:
            self.git("tag", "--annotate", "--message", "Version test tag", tag)
        else:
            self.git("tag", tag)
        return tag

    def test_setter_updates_every_mirror_and_tag_check(self) -> None:
        result = self.run_script("set_version.py", "0.2.3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.release_changelog("0.2.3")

        package = json.loads((self.fixture / "package.json").read_text())
        self.assertEqual((self.fixture / "VERSION").read_text().strip(), "0.2.3")
        self.assertEqual(package["version"], "0.2.3")
        self.assertIn(
            'version = "0.2.3"', (self.fixture / "python/pyproject.toml").read_text()
        )
        self.assertIn(
            'version = "0.2.3"', (self.fixture / "python/uv.lock").read_text()
        )

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

    def test_checker_accepts_annotated_ci_release_tag(self) -> None:
        self.release_changelog((self.fixture / "VERSION").read_text().strip())
        tag = self.create_fixture_tag(annotated=True)

        result = self.run_script(
            "check_versions.py",
            env={
                "GITHUB_ACTIONS": "true",
                "GITHUB_REF_TYPE": "tag",
                "GITHUB_REF_NAME": tag,
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_checker_rejects_lightweight_ci_release_tag(self) -> None:
        tag = self.create_fixture_tag(annotated=False)

        result = self.run_script(
            "check_versions.py",
            env={
                "GITHUB_ACTIONS": "true",
                "GITHUB_REF_TYPE": "tag",
                "GITHUB_REF_NAME": tag,
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be an annotated Git tag", result.stderr)

    def test_checker_rejects_unavailable_ci_release_tag(self) -> None:
        version = (self.fixture / "VERSION").read_text().strip()

        result = self.run_script(
            "check_versions.py",
            env={
                "GITHUB_ACTIONS": "true",
                "GITHUB_REF_TYPE": "tag",
                "GITHUB_REF_NAME": f"v{version}",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is unavailable in the CI checkout", result.stderr)

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
        self.assertIn(".flox/env/manifest.toml GOTOOLCHAIN", result.stderr)

    def test_checker_rejects_unexpected_nested_go_module(self) -> None:
        nested = self.fixture / "unexpected/go.mod"
        nested.parent.mkdir(parents=True)
        nested.write_text("module example.com/unexpected\n\ngo 1.26.5\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected nested Go modules", result.stderr)

    def test_checker_rejects_node_type_major_drift(self) -> None:
        package_path = self.fixture / "package.json"
        package = json.loads(package_path.read_text())
        package["devDependencies"]["@types/node"] = "28.1.1"
        package_path.write_text(json.dumps(package, indent=2) + "\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("@types/node major", result.stderr)

    def test_checker_rejects_gate_node_above_the_minimum(self) -> None:
        manifest_path = self.fixture / ".flox/env/manifest.toml"
        manifest_path.write_text(
            manifest_path.read_text().replace(
                'nodejs.pkg-path = "nodejs_26"', 'nodejs.pkg-path = "nodejs_28"'
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nodejs must be nodejs_26", result.stderr)

    def test_checker_rejects_minimum_node_raised_without_the_gate(self) -> None:
        package_path = self.fixture / "package.json"
        package = json.loads(package_path.read_text())
        package["engines"]["node"] = ">=27"
        package["devDependencies"]["@types/node"] = "27.0.0"
        package_path.write_text(json.dumps(package, indent=2) + "\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nodejs must be nodejs_27", result.stderr)

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

    def test_checker_rejects_temporaless_module_pin_drift(self) -> None:
        module = self.fixture / "python/src/medallion/workflows.py"
        module.write_text(
            re.sub(
                r'^TEMPORALESS_COMMIT = "[0-9a-f]{40}"$',
                'TEMPORALESS_COMMIT = "' + "0" * 40 + '"',
                module.read_text(),
                flags=re.MULTILINE,
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("medallion.workflows.TEMPORALESS_COMMIT", result.stderr)

    def test_checker_rejects_temporaless_extra_pin_drift(self) -> None:
        pyproject = self.fixture / "python/pyproject.toml"
        pyproject.write_text(
            pyproject.read_text().replace(
                "#subdirectory=core/py", "#subdirectory=core/py-moved"
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("workflows extra", result.stderr)

    def test_checker_rejects_documented_temporaless_version_drift(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            re.sub(
                r"\bTemporaless v[0-9]+\.[0-9]+\.[0-9]+",
                "Temporaless v0.0.0",
                readme_path.read_text(),
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Temporaless versions", result.stderr)

    def test_checker_rejects_missing_python_type_marker(self) -> None:
        (self.fixture / "python/src/medallion/py.typed").unlink()

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("src/medallion/py.typed", result.stderr)

    def test_checker_rejects_unsafe_npm_git_install_docs(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            readme_path.read_text().replace(
                "npm install --allow-git=all", "npm install"
            )
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
        self.assertIn("pnpm-workspace.yaml invariantprotocol build pins", result.stderr)

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
        self.assertIn("pnpm-lock.yaml invariantprotocol resolutions", result.stderr)

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
        self.assertIn("pnpm-lock.yaml invariantprotocol specifier pins", result.stderr)

    def test_checker_rejects_esbuild_override_drift(self) -> None:
        workspace_path = self.fixture / "pnpm-workspace.yaml"
        workspace_path.write_text(
            workspace_path.read_text().replace(
                'esbuild: "0.28.2"',
                'esbuild: "0.0.0"',
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("esbuild overrides", result.stderr)

    def test_checker_rejects_esbuild_lock_drift(self) -> None:
        lock_path = self.fixture / "pnpm-lock.yaml"
        lock_path.write_text(
            lock_path.read_text().replace("  esbuild@0.28.2:", "  esbuild@0.0.0:", 1)
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("esbuild package versions", result.stderr)

    def test_checker_rejects_postcss_override_drift(self) -> None:
        workspace_path = self.fixture / "pnpm-workspace.yaml"
        workspace_path.write_text(
            workspace_path.read_text().replace(
                'postcss: "8.5.26"',
                'postcss: "0.0.0"',
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("postcss overrides", result.stderr)

    def test_checker_rejects_postcss_lock_drift(self) -> None:
        lock_path = self.fixture / "pnpm-lock.yaml"
        lock_path.write_text(
            lock_path.read_text().replace(
                "  postcss@8.5.26:",
                "  postcss@0.0.0:",
                1,
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("postcss package versions", result.stderr)

    def test_checker_rejects_postcss_release_age_drift(self) -> None:
        workspace_path = self.fixture / "pnpm-workspace.yaml"
        workspace_path.write_text(
            workspace_path.read_text().replace(
                "- postcss@8.5.26",
                "- postcss@0.0.0",
            )
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("minimumReleaseAgeExclude entries", result.stderr)

    def test_checker_accepts_readme_install_tag_that_exists(self) -> None:
        result = self.run_script("check_versions.py")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_checker_rejects_readme_install_tag_that_does_not_exist(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            readme_path.read_text().replace(f"#{self.readme_tag}", "#v0.0.9")
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("installs v0.0.9, which is not a tag", result.stderr)

    def test_checker_rejects_several_readme_install_tags(self) -> None:
        self.git("tag", "v0.0.8")
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            readme_path.read_text().replace(f"#{self.readme_tag}", "#v0.0.8", 1)
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("name several release tags", result.stderr)

    def test_checker_requires_a_readme_install_tag(self) -> None:
        readme_path = self.fixture / "README.md"
        readme_path.write_text(
            readme_path.read_text().replace(f"#{self.readme_tag}", "#vX.Y.Z")
        )

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must pin one release tag", result.stderr)

    def test_checker_rejects_release_tag_with_unreleased_changes(self) -> None:
        version = (self.fixture / "VERSION").read_text().strip()
        changelog = self.fixture / "CHANGELOG.md"
        changelog.write_text(
            changelog.read_text().replace(
                "## [Unreleased]", "## [Unreleased]\n\n- Pending fixture change", 1
            )
        )

        result = self.run_script(
            "check_versions.py",
            env={"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": f"v{version}"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[Unreleased] must be empty", result.stderr)

    def test_checker_rejects_release_tag_of_another_changelog_version(self) -> None:
        self.release_changelog("0.0.1")
        version = (self.fixture / "VERSION").read_text().strip()

        result = self.run_script(
            "check_versions.py",
            env={"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": f"v{version}"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CHANGELOG.md starts at '[0.0.1] - 2026-09-27'", result.stderr)

    def test_checker_accepts_empty_unreleased_above_the_release(self) -> None:
        version = (self.fixture / "VERSION").read_text().strip()
        self.release_changelog(version)
        changelog = self.fixture / "CHANGELOG.md"
        changelog.write_text(
            changelog.read_text().replace(
                f"## [{version}]", f"## [Unreleased]\n\n## [{version}]", 1
            )
        )

        result = self.run_script(
            "check_versions.py",
            env={"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": f"v{version}"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_release_tags_and_pushes_a_ready_tree(self) -> None:
        origin = self.release_ready_fixture()
        tag = "v" + (self.fixture / "VERSION").read_text().strip()

        result = self.run_release()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"Published {tag}", result.stdout)
        self.assertEqual(self.origin_tag_type(origin, tag), "tag")
        self.assertEqual(
            self.git("rev-parse", f"refs/tags/{tag}^{{commit}}"),
            self.git("rev-parse", "HEAD"),
        )

    def test_release_refuses_a_dirty_tree(self) -> None:
        origin = self.release_ready_fixture()
        (self.fixture / "stray.txt").write_text("untracked\n")
        tag = "v" + (self.fixture / "VERSION").read_text().strip()

        result = self.run_release()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("the working tree is dirty", result.stderr)
        self.assertIsNone(self.origin_tag_type(origin, tag))

    def test_release_refuses_an_unpushed_head(self) -> None:
        origin = self.release_ready_fixture()
        self.git("commit", "--quiet", "--allow-empty", "-m", "Unpushed")
        tag = "v" + (self.fixture / "VERSION").read_text().strip()

        result = self.run_release()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("HEAD is not pushed to origin/main", result.stderr)
        self.assertIsNone(self.origin_tag_type(origin, tag))

    def test_release_refuses_a_tag_that_exists(self) -> None:
        self.release_ready_fixture()
        tag = self.create_fixture_tag(annotated=True)

        local = self.run_release()
        self.assertNotEqual(local.returncode, 0)
        self.assertIn(f"tag {tag} already exists locally", local.stderr)

        # A tag only origin has is refused too, whether the fetch brings it
        # back or the remote lookup finds it.
        self.git("push", "--quiet", "origin", f"refs/tags/{tag}")
        self.git("tag", "--delete", tag)
        remote = self.run_release()
        self.assertNotEqual(remote.returncode, 0)
        self.assertIn(f"tag {tag} already exists", remote.stderr)

    def test_release_refuses_unreleased_changelog(self) -> None:
        origin = self.release_ready_fixture()
        changelog = self.fixture / "CHANGELOG.md"
        changelog.write_text("# Changelog\n\n## [Unreleased]\n\n- pending\n")
        self.git("commit", "--quiet", "--all", "-m", "Unreleased work")
        self.git("push", "--quiet", "origin", "HEAD:refs/heads/main")
        tag = "v" + (self.fixture / "VERSION").read_text().strip()

        result = self.run_release()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[Unreleased] must be empty", result.stderr)
        self.assertIn("the version gate failed", result.stderr)
        self.assertIsNone(self.origin_tag_type(origin, tag))

    def test_checker_rejects_python_license_drift(self) -> None:
        (self.fixture / "python/NOTICE").write_text("stale notice\n")

        result = self.run_script("check_versions.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python/NOTICE must be byte-identical", result.stderr)


if __name__ == "__main__":
    unittest.main()
