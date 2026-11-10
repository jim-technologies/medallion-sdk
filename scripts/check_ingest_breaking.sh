#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

command -v buf >/dev/null || {
  echo "buf is not on PATH; run inside 'flox activate'" >&2
  exit 1
}

# make breaking-check already compares every retained proto file, the ingest
# contract included, with the main baseline. This check adds the one baseline
# that does not move with main and that none of that script's one-time
# exemptions can reach: the retained ingest contract must stay
# backward-compatible with the last commit before the Connect removal. A
# shallow checkout fetches that one commit by its SHA.
pinned=6a83624d573fa02bca9060b58b34e7cb38899312
ingest=proto/medallion/ingest/v1/ingest.proto
if ! git cat-file -e "$pinned^{commit}" 2>/dev/null; then
  git fetch --quiet --depth=1 origin "$pinned" || {
    echo "the pre-removal baseline $pinned is not in this clone and fetching it failed" >&2
    exit 1
  }
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/baseline"
git archive --format=tar "$pinned" buf.yaml proto | tar -xf - -C "$tmp/baseline"
buf build . --path "$ingest" -o "$tmp/current.binpb"
buf build "$tmp/baseline" --path "$tmp/baseline/$ingest" -o "$tmp/baseline.binpb"
buf breaking "$tmp/current.binpb" --against "$tmp/baseline.binpb"
echo "Retained ingest is backward-compatible with the pre-removal commit ($(git rev-parse --short "$pinned"))"
