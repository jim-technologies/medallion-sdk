#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

command -v buf >/dev/null || {
  echo "buf is not on PATH; run inside 'flox activate'" >&2
  exit 1
}

# The vendored contract is consumed outside this repository, so breaking-change
# detection against main runs inside make validate. The baseline is the local
# main branch when present, then origin/main, then a minimal fetch for CI
# checkouts that carry only a single shallow ref.
baseline=""
for ref in refs/heads/main refs/remotes/origin/main; do
  if git rev-parse --verify --quiet "$ref^{commit}" >/dev/null; then
    baseline="$ref"
    break
  fi
done
if [[ -z "$baseline" ]]; then
  git fetch --quiet --depth=1 origin main || {
    echo "no local main baseline and fetching origin main failed" >&2
    exit 1
  }
  baseline="FETCH_HEAD"
fi

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

git archive --format=tar "$baseline" buf.yaml proto | tar -xf - -C "$tmp"
buf breaking . --against "$tmp"
echo "Protobuf contracts are backward-compatible with main ($(git rev-parse --short "$baseline"))"
