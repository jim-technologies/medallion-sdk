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

# One-time re-pin exemption, scoped by content hash so it expires by itself.
# The ingest contract was first vendored as a provisional sketch, authored
# before the upstream contract was released and never matching the served
# shapes. Replacing it with the released contract is deliberately breaking.
# The exemption applies only when the baseline still carries that exact
# provisional blob: it drops the file from the baseline so the re-pin reads
# as a new file. Once main carries the released contract the hash no longer
# matches, the exemption can never fire again, and every later change to the
# ingest contract is compared normally.
provisional_ingest=6c3ce1f05a2acc9b593388fef8b1632078294be9
ingest_path=proto/medallion/ingest/v1/ingest.proto
baseline_ingest="$(git rev-parse --verify --quiet "$baseline:$ingest_path" || true)"
if [[ "$baseline_ingest" == "$provisional_ingest" ]]; then
  echo "note: exempting the one-time ingest re-pin off the provisional sketch" >&2
  rm -f "$tmp/$ingest_path"
fi

buf breaking . --against "$tmp"
echo "Protobuf contracts are backward-compatible with main ($(git rev-parse --short "$baseline"))"
