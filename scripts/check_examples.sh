#!/usr/bin/env bash

# Check the runnable TypeScript, Go, and Python quickstarts against the built
# SDKs: type-check, format-check, vet, lint, and byte-compile.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

pnpm check:examples

unformatted="$(gofmt -l examples)"
if [[ -n "$unformatted" ]]; then
  echo "gofmt: examples need formatting:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go vet ./examples/

ruff check --no-cache examples
PYTHONDONTWRITEBYTECODE=1 python3 -m compileall -q examples
