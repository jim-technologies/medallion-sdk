#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"
pinned=6a83624d573fa02bca9060b58b34e7cb38899312
ingest=proto/medallion/ingest/v1/ingest.proto
main_ref=refs/remotes/origin/main
if ! git rev-parse --verify --quiet "$main_ref^{commit}" >/dev/null; then
  main_ref=refs/heads/main
fi
main_commit="$(git rev-parse --verify "$main_ref^{commit}")"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
buf build . --path "$ingest" -o "$tmp/current.binpb"
for baseline in "$pinned" "$main_commit"; do
  git cat-file -e "$baseline^{commit}"
  mkdir -p "$tmp/$baseline"
  git archive --format=tar "$baseline" buf.yaml proto | tar -xf - -C "$tmp/$baseline"
  buf build "$tmp/$baseline" --path "$tmp/$baseline/$ingest" -o "$tmp/baseline.binpb"
  buf breaking "$tmp/current.binpb" --against "$tmp/baseline.binpb"
  echo "Retained ingest is backward-compatible with $baseline"
done
