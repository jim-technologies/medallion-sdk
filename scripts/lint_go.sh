#!/usr/bin/env bash

# Format-check and vet the Go SDK.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

unformatted="$(find go -type f -name '*.go' -exec gofmt -l {} +)"
if [[ -n "$unformatted" ]]; then
  echo "gofmt: files need formatting:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go vet ./go/...
