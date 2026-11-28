#!/usr/bin/env bash

# Audit the locked Python graph, every extra included, and the pinned build
# backend against the OSV vulnerability database with uv's own auditor.
# Network-dependent: `make audit` only, never the gate. `uv audit` is a preview
# command in uv 0.13, hence --preview-features.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root/python"

uv audit --locked --no-dev --preview-features audit-command

# The build backend is not part of uv.lock. Audit the exact build-system pin
# as a PEP 723 script, which uv resolves together with the backend's own
# dependencies.
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
python3 - "$scratch/build-backend.py" <<'PY'
import json
import sys
import tomllib

with open("pyproject.toml", "rb") as handle:
    pyproject = tomllib.load(handle)
with open(sys.argv[1], "w") as script:
    script.write(
        "# /// script\n"
        f"# requires-python = {json.dumps(pyproject['project']['requires-python'])}\n"
        f"# dependencies = {json.dumps(pyproject['build-system']['requires'])}\n"
        "# ///\n"
    )
PY
uv audit --script "$scratch/build-backend.py" --preview-features audit-command
