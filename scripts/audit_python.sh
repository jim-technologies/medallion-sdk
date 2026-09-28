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
uv tool run --from "$pip_audit" pip-audit \
  --strict --disable-pip --vulnerability-service osv \
  --requirement "$scratch/runtime.txt" --progress-spinner off

python3 -c 'import tomllib; data = tomllib.load(open("python/pyproject.toml", "rb")); print(*data["build-system"]["requires"], sep="\n")' \
  >"$scratch/build.txt"
uv tool run --from "$pip_audit" --with pip pip-audit \
  --strict --vulnerability-service osv \
  --requirement "$scratch/build.txt" --progress-spinner off
