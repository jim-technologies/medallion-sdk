#!/usr/bin/env bash

# Audit the exact Python runtime lock and the pinned build backend against
# the OSV vulnerability database. Network-dependent: `make audit` only, never
# the gate.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

pip_audit="pip-audit==${PIP_AUDIT_VERSION:?PIP_AUDIT_VERSION is required}"

scratch="$(mktemp -d)"
cleanup() {
  rm -rf "$scratch"
}
trap cleanup EXIT

uv export --project python --locked --no-dev --no-emit-project \
  --format requirements-txt --output-file "$scratch/runtime.txt" >/dev/null
# Requirement-mode auditing rejects direct wheel URLs. Install the exact
# hashed lock, then audit its metadata without skipping the shared Buf wheels.
uv venv --quiet "$scratch/runtime"
uv pip sync --python "$scratch/runtime/bin/python" --require-hashes "$scratch/runtime.txt"
site="$("$scratch/runtime/bin/python" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
uv tool run --from "$pip_audit" --with pip pip-audit \
  --strict --vulnerability-service osv --path "$site" --progress-spinner off \
  --format json --output "$scratch/runtime-audit.json"
"$scratch/runtime/bin/python" - "$scratch/runtime-audit.json" "$site" <<'PY'
from importlib.metadata import distributions
import json
from pathlib import Path
import re
import sys

def canonical(name):
    return re.sub(r"[-_.]+", "-", name).lower()

expected = {canonical(dist.metadata["Name"]): dist.version
            for dist in distributions(path=[sys.argv[2]])}
dependencies = json.loads(Path(sys.argv[1]).read_text())["dependencies"]
actual = {canonical(dep["name"]): dep["version"] for dep in dependencies}
if not expected or actual != expected or len(dependencies) != len(expected):
    sys.exit("audit report must include every installed locked runtime dependency exactly once")
if not all(dep.get("vulns") == [] and "skip_reason" not in dep for dep in dependencies):
    sys.exit("audit report contains a vulnerability or an unaudited dependency")
print(f"OSV audit covered all {len(expected)} installed locked runtime packages.")
PY

python3 -c 'import tomllib; data = tomllib.load(open("python/pyproject.toml", "rb")); print(*data["build-system"]["requires"], sep="\n")' \
  >"$scratch/build.txt"
uv tool run --from "$pip_audit" --with pip pip-audit \
  --strict --vulnerability-service osv \
  --requirement "$scratch/build.txt" --progress-spinner off
