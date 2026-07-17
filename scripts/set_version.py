#!/usr/bin/env python3
"""Synchronize every Medallion SDK mirror from one release version."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

import check_versions

ROOT = Path(__file__).resolve().parents[1]


def write_text(path: Path, value: str) -> None:
    """Replace one file atomically while preserving its permission bits."""
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(value)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def write_json(path: Path, value: dict[str, Any]) -> None:
    write_text(path, json.dumps(value, indent=2) + "\n")


def replace_once(text: str, pattern: str, replacement: str, path: Path) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"expected one version field in {path}, found {count}")
    return updated


def update_uv_lock(path: Path, version: str) -> None:
    blocks = re.split(r"(?=^\[\[package\]\]$)", path.read_text(), flags=re.MULTILINE)
    updated_blocks: list[str] = []
    found = 0

    for block in blocks:
        name = re.search(r'^name = "([^"]+)"$', block, re.MULTILINE)
        if name is not None and name.group(1) == "medallion":
            if not re.search(
                r'^source = \{ editable = "\." \}$', block, re.MULTILINE
            ):
                raise ValueError(f"{path} medallion package is not editable from '.'")
            block = replace_once(
                block,
                r'^version = "[0-9]+\.[0-9]+\.[0-9]+"$',
                f'version = "{version}"',
                path,
            )
            found += 1
        updated_blocks.append(block)

    if found != 1:
        raise ValueError(f"{path} contained {found} medallion packages; expected 1")
    write_text(path, "".join(updated_blocks))


def main() -> int:
    if len(sys.argv) != 2 or check_versions.SEMVER.fullmatch(sys.argv[1]) is None:
        print(
            "usage: python3 scripts/set_version.py MAJOR.MINOR.PATCH", file=sys.stderr
        )
        return 2
    version = sys.argv[1]
    if int(version.split(".", 1)[0]) >= 2:
        print(
            "major 2+ requires the Go /vN semantic-import-path migration first",
            file=sys.stderr,
        )
        return 2

    write_text(ROOT / "VERSION", f"{version}\n")

    package_path = ROOT / "package.json"
    package = json.loads(package_path.read_text())
    package["version"] = version
    write_json(package_path, package)

    pyproject_path = ROOT / "python/pyproject.toml"
    pyproject = replace_once(
        pyproject_path.read_text(),
        r'^version = "[0-9]+\.[0-9]+\.[0-9]+"$',
        f'version = "{version}"',
        pyproject_path,
    )
    write_text(pyproject_path, pyproject)

    update_uv_lock(ROOT / "python/uv.lock", version)

    return check_versions.main()


if __name__ == "__main__":
    raise SystemExit(main())
