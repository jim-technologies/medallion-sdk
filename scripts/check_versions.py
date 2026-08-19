#!/usr/bin/env python3
"""Verify that every Medallion SDK uses one repository release version."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SEMVER_COMPONENT = r"(?:0|[1-9][0-9]*)"
SEMVER = re.compile(rf"{SEMVER_COMPONENT}\.{SEMVER_COMPONENT}\.{SEMVER_COMPONENT}")
INVARIANT_PROTOCOL_VERSION = "0.14.0"
INVARIANT_PROTOCOL_SHA = "f924e602582402476f257571852fabb0e1e1cf43"
INVARIANT_PROTOCOL_PACKAGE = "@jim-technologies/invariant-protocol"
INVARIANT_PROTOCOL_SPEC = (
    "github:jim-technologies/invariantprotocol#" + INVARIANT_PROTOCOL_SHA
)
INVARIANT_PROTOCOL_LOCATOR_PREFIX = (
    "https://codeload.github.com/jim-technologies/invariantprotocol/tar.gz/"
)
INVARIANT_PROTOCOL_LOCATOR = INVARIANT_PROTOCOL_LOCATOR_PREFIX + INVARIANT_PROTOCOL_SHA
ESBUILD_VERSION = "0.28.1"
POSTCSS_VERSION = "8.5.24"


def read_json(path: str) -> dict[str, Any]:
    return json.loads((ROOT / path).read_text())


def read_toml(path: str) -> dict[str, Any]:
    with (ROOT / path).open("rb") as file:
        return tomllib.load(file)


def captured(path: str, pattern: str) -> str | None:
    source = ROOT / path
    if not source.is_file():
        return None
    match = re.search(pattern, source.read_text(), re.MULTILINE)
    return match.group(1) if match is not None else None


def main() -> int:
    version = (ROOT / "VERSION").read_text().strip()
    errors: list[str] = []

    if SEMVER.fullmatch(version) is None:
        errors.append(f"VERSION must be MAJOR.MINOR.PATCH, got {version!r}")
    elif int(version.split(".", 1)[0]) >= 2:
        errors.append(
            "VERSION major 2+ requires a coordinated Go /vN semantic-import "
            "path migration before release"
        )

    package = read_json("package.json")
    pyproject = read_toml("python/pyproject.toml")
    uv_lock = read_toml("python/uv.lock")
    flox_manifest = read_toml(".flox/env/manifest.toml")
    package_manager = package.get("packageManager")
    pnpm_match = (
        re.fullmatch(r"pnpm@([0-9]+\.[0-9]+\.[0-9]+)", package_manager)
        if isinstance(package_manager, str)
        else None
    )
    if pnpm_match is None:
        errors.append(
            "package.json packageManager must pin an exact pnpm patch version"
        )
        pnpm_version = None
    else:
        pnpm_version = pnpm_match.group(1)

    engines = package.get("engines", {})
    node_engine = engines.get("node") if isinstance(engines, dict) else None
    node_match = (
        re.fullmatch(r">=([1-9][0-9]*)", node_engine)
        if isinstance(node_engine, str)
        else None
    )
    if node_match is None:
        errors.append("package.json engines.node must declare one minimum major as >=N")
        minimum_node_major = None
    else:
        minimum_node_major = int(node_match.group(1))

    dev_dependencies = package.get("devDependencies", {})
    node_types = (
        dev_dependencies.get("@types/node")
        if isinstance(dev_dependencies, dict)
        else None
    )
    node_types_match = (
        SEMVER.fullmatch(node_types) if isinstance(node_types, str) else None
    )
    if node_types_match is None:
        errors.append("package.json must pin @types/node to an exact patch version")
    elif (
        minimum_node_major is not None
        and int(node_types_match.group(0).split(".", 1)[0]) != minimum_node_major
    ):
        errors.append(
            f"package.json @types/node major must match the minimum Node "
            f"runtime {minimum_node_major}"
        )

    primary_node_path = (
        flox_manifest.get("install", {}).get("nodejs", {}).get("pkg-path")
    )
    primary_node_match = (
        re.fullmatch(r"nodejs_([1-9][0-9]*)", primary_node_path)
        if isinstance(primary_node_path, str)
        else None
    )
    if minimum_node_major is not None and (
        primary_node_match is None
        or int(primary_node_match.group(1)) < minimum_node_major
    ):
        errors.append(
            ".flox/env/manifest.toml nodejs must meet the minimum Node runtime"
        )

    minimum_node_manifest_path = (
        ROOT / f".ci/node-{minimum_node_major}/.flox/env/manifest.toml"
        if minimum_node_major is not None
        else None
    )
    if minimum_node_manifest_path is None:
        minimum_node_manifest: dict[str, Any] = {}
    elif not minimum_node_manifest_path.is_file():
        errors.append(f".ci/node-{minimum_node_major} Flox environment must exist")
        minimum_node_manifest = {}
    else:
        with minimum_node_manifest_path.open("rb") as file:
            minimum_node_manifest = tomllib.load(file)
    minimum_node_path = (
        minimum_node_manifest.get("install", {}).get("nodejs", {}).get("pkg-path")
    )
    if (
        minimum_node_major is not None
        and minimum_node_path != f"nodejs_{minimum_node_major}"
    ):
        errors.append(
            f".ci/node-{minimum_node_major} Flox environment must use "
            f"nodejs_{minimum_node_major}"
        )

    dependencies = package.get("dependencies", {})
    invariant_spec = (
        dependencies.get(INVARIANT_PROTOCOL_PACKAGE)
        if isinstance(dependencies, dict)
        else None
    )
    if invariant_spec != INVARIANT_PROTOCOL_SPEC:
        errors.append(
            f"package.json pins invariantprotocol as {invariant_spec!r}; "
            f"expected {INVARIANT_PROTOCOL_SPEC!r}"
        )

    workspace = (ROOT / "pnpm-workspace.yaml").read_text()
    workspace_esbuild_versions = re.findall(
        r"^\s{2}esbuild:\s*[\"']?([0-9]+\.[0-9]+\.[0-9]+)[\"']?\s*$",
        workspace,
        re.MULTILINE,
    )
    if workspace_esbuild_versions != [ESBUILD_VERSION]:
        errors.append(
            "pnpm-workspace.yaml esbuild overrides are "
            f"{workspace_esbuild_versions!r}; expected only {[ESBUILD_VERSION]!r}"
        )
    workspace_postcss_versions = re.findall(
        r"^\s{2}postcss:\s*[\"']?([0-9]+\.[0-9]+\.[0-9]+)[\"']?\s*$",
        workspace,
        re.MULTILINE,
    )
    if workspace_postcss_versions != [POSTCSS_VERSION]:
        errors.append(
            "pnpm-workspace.yaml postcss overrides are "
            f"{workspace_postcss_versions!r}; expected only {[POSTCSS_VERSION]!r}"
        )
    release_age_section = re.search(
        r"^minimumReleaseAgeExclude:\s*\n((?:^[ \t]+.*(?:\n|$))*)",
        workspace,
        re.MULTILINE,
    )
    release_age_entries = (
        re.findall(
            r"^[ \t]*-[ \t]*[\"']?([^\"'\s]+)[\"']?[ \t]*$",
            release_age_section.group(1),
            re.MULTILINE,
        )
        if release_age_section is not None
        else []
    )
    expected_release_age_entries = [f"postcss@{POSTCSS_VERSION}"]
    if release_age_entries != expected_release_age_entries:
        errors.append(
            "pnpm-workspace.yaml minimumReleaseAgeExclude entries are "
            f"{release_age_entries!r}; expected only {expected_release_age_entries!r}"
        )
    invariant_allow_build = (
        f'"{INVARIANT_PROTOCOL_PACKAGE}@{INVARIANT_PROTOCOL_LOCATOR}": true'
    )
    if invariant_allow_build not in workspace:
        errors.append(
            "pnpm-workspace.yaml must allow the exact pinned invariantprotocol build"
        )
    workspace_invariant_shas = re.findall(
        r"^\s*[\"']?"
        + re.escape(f"{INVARIANT_PROTOCOL_PACKAGE}@{INVARIANT_PROTOCOL_LOCATOR_PREFIX}")
        + r"([0-9a-f]{40})[\"']?:",
        workspace,
        re.MULTILINE,
    )
    if workspace_invariant_shas != [INVARIANT_PROTOCOL_SHA]:
        errors.append(
            "pnpm-workspace.yaml invariantprotocol build pins are "
            f"{workspace_invariant_shas!r}; expected only {[INVARIANT_PROTOCOL_SHA]!r}"
        )

    pnpm_lock = (ROOT / "pnpm-lock.yaml").read_text()
    lock_esbuild_versions = set(
        re.findall(r"^\s{2}esbuild@([0-9]+\.[0-9]+\.[0-9]+):", pnpm_lock, re.MULTILINE)
    )
    if lock_esbuild_versions != {ESBUILD_VERSION}:
        errors.append(
            "pnpm-lock.yaml esbuild package versions are "
            f"{sorted(lock_esbuild_versions)!r}; expected only {[ESBUILD_VERSION]!r}"
        )
    lock_platform_esbuild_versions = set(
        re.findall(
            r"^\s{2}'?@esbuild/[^@']+@([0-9]+\.[0-9]+\.[0-9]+)'?:",
            pnpm_lock,
            re.MULTILINE,
        )
    )
    if lock_platform_esbuild_versions != {ESBUILD_VERSION}:
        errors.append(
            "pnpm-lock.yaml platform esbuild versions are "
            f"{sorted(lock_platform_esbuild_versions)!r}; "
            f"expected only {[ESBUILD_VERSION]!r}"
        )
    if f"  esbuild: {ESBUILD_VERSION}" not in pnpm_lock:
        errors.append(
            "pnpm-lock.yaml esbuild override does not match pnpm-workspace.yaml"
        )
    lock_postcss_versions = set(
        re.findall(
            r"^\s{2}postcss@([0-9]+\.[0-9]+\.[0-9]+):",
            pnpm_lock,
            re.MULTILINE,
        )
    )
    if lock_postcss_versions != {POSTCSS_VERSION}:
        errors.append(
            "pnpm-lock.yaml postcss package versions are "
            f"{sorted(lock_postcss_versions)!r}; "
            f"expected only {[POSTCSS_VERSION]!r}"
        )
    if f"  postcss: {POSTCSS_VERSION}" not in pnpm_lock:
        errors.append(
            "pnpm-lock.yaml postcss override does not match pnpm-workspace.yaml"
        )
    if f"specifier: {INVARIANT_PROTOCOL_SPEC}" not in pnpm_lock:
        errors.append(
            "pnpm-lock.yaml invariantprotocol specifier does not match package.json"
        )
    lock_invariant_spec_shas = set(
        re.findall(
            re.escape("github:jim-technologies/invariantprotocol#") + r"([0-9a-f]{40})",
            pnpm_lock,
        )
    )
    if lock_invariant_spec_shas != {INVARIANT_PROTOCOL_SHA}:
        errors.append(
            "pnpm-lock.yaml invariantprotocol specifier pins are "
            f"{sorted(lock_invariant_spec_shas)!r}; "
            f"expected only {[INVARIANT_PROTOCOL_SHA]!r}"
        )
    if f"version: {INVARIANT_PROTOCOL_LOCATOR}" not in pnpm_lock:
        errors.append(
            "pnpm-lock.yaml invariantprotocol resolution does not match the pin"
        )
    invariant_lock_header = (
        f"  '{INVARIANT_PROTOCOL_PACKAGE}@{INVARIANT_PROTOCOL_LOCATOR}':"
    )
    invariant_lock_start = pnpm_lock.find(invariant_lock_header)
    if invariant_lock_start == -1:
        errors.append("pnpm-lock.yaml is missing the pinned invariantprotocol package")
    else:
        invariant_lock_block = pnpm_lock[invariant_lock_start:].split("\n\n", 1)[0]
        if f"    version: {INVARIANT_PROTOCOL_VERSION}" not in invariant_lock_block:
            errors.append(
                "pnpm-lock.yaml invariantprotocol package version does not match "
                f"{INVARIANT_PROTOCOL_VERSION}"
            )
    lock_invariant_shas = set(
        re.findall(
            re.escape(INVARIANT_PROTOCOL_LOCATOR_PREFIX) + r"([0-9a-f]{40})",
            pnpm_lock,
        )
    )
    if lock_invariant_shas != {INVARIANT_PROTOCOL_SHA}:
        errors.append(
            "pnpm-lock.yaml invariantprotocol resolutions are "
            f"{sorted(lock_invariant_shas)!r}; expected only {[INVARIANT_PROTOCOL_SHA]!r}"
        )

    if package.get("private") is not True:
        errors.append("package.json must set private=true for Git-only distribution")
    if "VERSION" not in package.get("files", []):
        errors.append("package.json files must include VERSION")

    classifiers = pyproject["project"].get("classifiers", [])
    if "Private :: Do Not Upload" not in classifiers:
        errors.append("python/pyproject.toml must prohibit PyPI uploads")
    if not (ROOT / "python/src/medallion/py.typed").is_file():
        errors.append("python package must include src/medallion/py.typed")
    for license_file in ("LICENSE", "NOTICE"):
        root_license = ROOT / license_file
        python_license = ROOT / "python" / license_file
        if not python_license.is_file():
            errors.append(f"python/{license_file} must be included for Git builds")
        elif python_license.read_bytes() != root_license.read_bytes():
            errors.append(
                f"python/{license_file} must be byte-identical to {license_file}"
            )

    medallion_packages = [
        item for item in uv_lock["package"] if item.get("name") == "medallion"
    ]
    if len(medallion_packages) != 1:
        errors.append(
            "python/uv.lock must contain exactly one medallion package; "
            f"found {len(medallion_packages)}"
        )
        uv_version: object = "<missing>"
    else:
        medallion_package = medallion_packages[0]
        uv_version = medallion_package.get("version", "<missing>")
        if medallion_package.get("source") != {"editable": "."}:
            errors.append(
                'python/uv.lock medallion package must use source = { editable = "." }'
            )

    actual_versions = {
        "package.json": package.get("version", "<missing>"),
        "python/pyproject.toml": pyproject["project"].get("version", "<missing>"),
        "python/uv.lock": uv_version,
    }
    for source, actual in actual_versions.items():
        if actual != version:
            errors.append(f"{source} has {actual!r}; expected {version!r}")

    module_path = captured("go.mod", r"^module\s+(\S+)$")
    expected_module = "github.com/jim-technologies/medallion-sdk"
    if module_path != expected_module:
        errors.append(
            f"go.mod declares {module_path or '<missing>'!r}; expected {expected_module!r}"
        )
    go_version = captured("go.mod", r"^go\s+(\S+)$")
    if go_version is None or SEMVER.fullmatch(go_version) is None:
        errors.append(
            f"go.mod must declare an exact Go patch version, got {go_version!r}"
        )
    else:
        expected_toolchain = f"go{go_version}+auto"
        actual_toolchain = flox_manifest.get("vars", {}).get("GOTOOLCHAIN")
        if actual_toolchain != expected_toolchain:
            errors.append(
                f".flox/env/manifest.toml GOTOOLCHAIN is {actual_toolchain!r}; "
                f"expected {expected_toolchain!r}"
            )

    nested_go_mods: list[Path] = []
    ignored_dirs = {".flox", ".git", ".venv", "node_modules", "target"}
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in ignored_dirs]
        path = Path(directory) / "go.mod"
        if "go.mod" in files and path != ROOT / "go.mod":
            nested_go_mods.append(path.relative_to(ROOT))
    nested_go_mods.sort()
    if nested_go_mods:
        errors.append(f"unexpected nested Go modules: {nested_go_mods}")

    readme = (ROOT / "README.md").read_text()
    for marker in ("`VERSION`", "`vX.Y.Z`"):
        if marker not in readme:
            errors.append(
                f"README.md must document the lockstep release marker {marker}"
            )
    for line_number, line in enumerate(readme.splitlines(), 1):
        if line.strip().startswith("npm install") and "--allow-git=all" not in line:
            errors.append(
                f"README.md:{line_number} Git npm install must pass --allow-git=all"
            )
    if f"invariantprotocol v{INVARIANT_PROTOCOL_VERSION}" not in readme:
        errors.append(
            "README.md invariantprotocol release does not match the pinned dependency"
        )
    readme_invariant_versions = set(
        re.findall(r"\binvariantprotocol v([0-9]+\.[0-9]+\.[0-9]+)\b", readme)
    )
    if readme_invariant_versions != {INVARIANT_PROTOCOL_VERSION}:
        errors.append(
            "README.md invariantprotocol versions are "
            f"{sorted(readme_invariant_versions)!r}; "
            f"expected only {[INVARIANT_PROTOCOL_VERSION]!r}"
        )
    if go_version is not None:
        readme_go_versions = set(
            re.findall(r"\bGo ([0-9]+\.[0-9]+\.[0-9]+)(?:\+|,)", readme)
        )
        if readme_go_versions != {go_version}:
            errors.append(
                "README.md Go toolchain versions are "
                f"{sorted(readme_go_versions)!r}; expected only {[go_version]!r}"
            )
    if pnpm_version is not None:
        readme_pnpm_versions = set(
            re.findall(r"\bpnpm ([0-9]+\.[0-9]+\.[0-9]+)(?:\+|,)", readme)
        )
        if readme_pnpm_versions != {pnpm_version}:
            errors.append(
                "README.md pnpm versions are "
                f"{sorted(readme_pnpm_versions)!r}; "
                f"expected only {[pnpm_version]!r}"
            )

    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        expected_tag = f"v{version}"
        actual_tag = os.environ.get("GITHUB_REF_NAME")
        if actual_tag != expected_tag:
            errors.append(f"release tag is {actual_tag!r}; expected {expected_tag!r}")
        elif os.environ.get("GITHUB_ACTIONS") == "true":
            tag_type = subprocess.run(
                ["git", "cat-file", "-t", f"refs/tags/{actual_tag}"],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            if tag_type.returncode != 0:
                errors.append(
                    f"release tag {actual_tag!r} is unavailable in the CI checkout"
                )
            elif tag_type.stdout.strip() != "tag":
                errors.append(
                    f"release tag {actual_tag!r} must be an annotated Git tag"
                )

    if errors:
        print("version consistency check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"versions aligned: {version} (tag v{version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
