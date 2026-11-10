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

# One-time Connect removal exemption, scoped by content hash the same way.
# Version 0.4.0 deletes the deprecated medallion.connect.v1 binding and the
# buf/validate shim that only it imported; byte-preserved copies move to
# archive/connect-v1 as historical evidence. FILE_NO_DELETE would otherwise
# report that intentional deletion. A file is dropped from the baseline only
# while the baseline holds its exact pre-removal blob and the file is absent
# from the tree, so every surviving file is still compared, and once main
# carries the removal the exemption can never fire again.
connect_removal=(
  proto/medallion/connect/v1/connect.proto:4251c76e9af523691793df2dbe80860ddd761912
  proto/buf/validate/validate.proto:65ef8e03dfd4d3193ce604683934eecb0d82a61d
)
for removed in "${connect_removal[@]}"; do
  removed_path="${removed%%:*}"
  removed_blob="${removed##*:}"
  baseline_blob="$(git rev-parse --verify --quiet "$baseline:$removed_path" || true)"
  if [[ "$baseline_blob" == "$removed_blob" && ! -e "$removed_path" ]]; then
    echo "note: exempting the one-time removal of $removed_path" >&2
    rm -f "$tmp/$removed_path"
  fi
done

buf breaking . --against "$tmp"
echo "Protobuf contracts are backward-compatible with main ($(git rev-parse --short "$baseline"))"
