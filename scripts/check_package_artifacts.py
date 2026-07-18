#!/usr/bin/env python3
"""Verify the exact Git-install package payloads for every SDK artifact."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
PYTHON_ROOT = ROOT / "python"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_subset(actual: set[str], expected: set[str], artifact: str) -> None:
    missing = sorted(expected - actual)
    if missing:
        fail(f"{artifact} is missing required files: {missing}")


def metadata_license_fields(raw: bytes, artifact: str) -> None:
    text = raw.decode("utf-8")
    if "License-Expression: Apache-2.0\n" not in text:
        fail(f"{artifact} is missing License-Expression: Apache-2.0")
    fields = {
        line.removeprefix("License-File: ")
        for line in text.splitlines()
        if line.startswith("License-File: ")
    }
    if fields != {"LICENSE", "NOTICE"}:
        fail(f"{artifact} has unexpected License-File fields: {sorted(fields)}")


def npm_pack_description(payload: object) -> dict[str, object]:
    """Normalize npm 11's array and npm 12's package-name-keyed JSON shapes."""
    if (
        isinstance(payload, list)
        and len(payload) == 1
        and isinstance(payload[0], dict)
    ):
        return payload[0]
    if isinstance(payload, dict):
        if isinstance(payload.get("files"), list):
            return payload
        if len(payload) == 1:
            description = next(iter(payload.values()))
            if isinstance(description, dict):
                return description
    fail("npm pack did not return exactly one package description")


def check_npm() -> None:
    result = subprocess.run(
        ["npm", "pack", "--dry-run", "--json", "--ignore-scripts"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    description = npm_pack_description(payload)
    files = {
        item["path"]
        for item in description.get("files", [])
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    require_subset(
        files,
        {
            "LICENSE",
            "NOTICE",
            "README.md",
            "VERSION",
            "package.json",
            "dist/index.js",
            "dist/index.cjs",
            "dist/index.d.ts",
        },
        "npm Git package",
    )


def check_python(version: str) -> None:
    license_bytes = (ROOT / "LICENSE").read_bytes()
    notice_bytes = (ROOT / "NOTICE").read_bytes()
    if (PYTHON_ROOT / "LICENSE").read_bytes() != license_bytes:
        fail("python/LICENSE is not byte-identical to root LICENSE")
    if (PYTHON_ROOT / "NOTICE").read_bytes() != notice_bytes:
        fail("python/NOTICE is not byte-identical to root NOTICE")

    wheel = PYTHON_ROOT / "dist" / f"medallion-{version}-py3-none-any.whl"
    sdist = PYTHON_ROOT / "dist" / f"medallion-{version}.tar.gz"
    for artifact in (wheel, sdist):
        if not artifact.is_file():
            fail(f"missing Python artifact: {artifact.relative_to(ROOT)}")

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        prefix = f"medallion-{version}.dist-info"
        require_subset(
            names,
            {
                "medallion/__init__.py",
                "medallion/py.typed",
                "medallion/connect/v1/connect_pb2.py",
                f"{prefix}/licenses/LICENSE",
                f"{prefix}/licenses/NOTICE",
                f"{prefix}/METADATA",
            },
            "Python wheel",
        )
        if archive.read(f"{prefix}/licenses/LICENSE") != license_bytes:
            fail("wheel LICENSE bytes differ from root LICENSE")
        if archive.read(f"{prefix}/licenses/NOTICE") != notice_bytes:
            fail("wheel NOTICE bytes differ from root NOTICE")
        metadata_license_fields(archive.read(f"{prefix}/METADATA"), "wheel METADATA")

    distribution_root = f"medallion-{version}"
    with tarfile.open(sdist, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                fail(f"sdist contains unsafe path: {member.name!r}")
        by_name = {member.name: member for member in members}
        required = {
            f"{distribution_root}/LICENSE",
            f"{distribution_root}/NOTICE",
            f"{distribution_root}/PKG-INFO",
            f"{distribution_root}/pyproject.toml",
            f"{distribution_root}/src/medallion/py.typed",
        }
        require_subset(set(by_name), required, "Python sdist")
        for name, expected in (
            (f"{distribution_root}/LICENSE", license_bytes),
            (f"{distribution_root}/NOTICE", notice_bytes),
        ):
            member = by_name[name]
            if not member.isfile():
                fail(f"sdist {name} is not a regular file")
            extracted = archive.extractfile(member)
            if extracted is None or extracted.read() != expected:
                fail(f"sdist {name} bytes differ from the root file")
        metadata = archive.extractfile(by_name[f"{distribution_root}/PKG-INFO"])
        if metadata is None:
            fail("sdist PKG-INFO could not be read")
        metadata_license_fields(metadata.read(), "sdist PKG-INFO")

    with tempfile.TemporaryDirectory() as temporary:
        subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", temporary],
            cwd=PYTHON_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        direct_wheel = Path(temporary) / wheel.name
        if not direct_wheel.is_file():
            fail("direct Python wheel build did not produce the expected filename")
        if hashlib.sha256(direct_wheel.read_bytes()).digest() != hashlib.sha256(
            wheel.read_bytes()
        ).digest():
            fail("direct wheel differs from the wheel rebuilt through the sdist")


def main() -> int:
    try:
        version = (ROOT / "VERSION").read_text().strip()
        check_npm()
        check_python(version)
    except (AssertionError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"package artifact check failed: {error}", file=sys.stderr)
        return 1
    print("Git-install package artifacts contain the required runtime, type, and license files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
