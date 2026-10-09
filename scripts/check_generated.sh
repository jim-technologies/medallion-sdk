#!/usr/bin/env bash

set -euo pipefail

tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
root="${MEDALLION_GENERATED_ROOT:-$tool_root}"
tmp="$(mktemp -d)"

cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

cd "$root"

command -v buf >/dev/null || {
  echo "buf is not on PATH; run inside 'flox activate'" >&2
  exit 1
}

mapfile -t descriptors < <(find proto -maxdepth 1 -type f -name '*.descriptor.binpb' -print | sort)
if [[ "${descriptors[*]}" != "proto/ingest-v1.descriptor.binpb" ]]; then
  echo "proto must contain exactly the ingest v1 descriptor" >&2
  printf 'found: %s\n' "${descriptors[*]:-(none)}" >&2
  exit 1
fi

buf generate proto \
  --template "$root/buf.gen.yaml" \
  --path proto/medallion/ingest/v1/ingest.proto \
  --output "$tmp/generated"

mkdir -p "$tmp/generated/proto"
buf build proto \
  --path proto/medallion/ingest/v1/ingest.proto \
  --exclude-source-info \
  --as-file-descriptor-set \
  -o "$tmp/generated/proto/ingest-v1.descriptor.binpb"

for retired in proto/medallion/connect go/gen/medallion/connect python/src/medallion/connect src/connect-descriptor.ts; do
  if [[ -e "$retired" ]]; then
    echo "retired Connect API must not exist in active generated surfaces: $retired" >&2
    exit 1
  fi
done

generated_files=(
  "go/gen/medallion/ingest/v1/ingest.pb.go"
  "proto/ingest-v1.descriptor.binpb"
  "python/src/medallion/ingest/v1/ingest_pb2.py"
)
for relative in "${generated_files[@]}"; do
  if ! cmp -s "$relative" "$tmp/generated/$relative"; then
    echo "$relative is stale; run make proto-bindings" >&2
    diff -u "$relative" "$tmp/generated/$relative" || true
    exit 1
  fi
done

for directory in python/src/buf "$tmp/generated/python/src/buf"; do
  if [[ -d "$directory" ]] && [[ -n "$(find "$directory" ! -type d ! -path '*/__pycache__/*' -print -quit)" ]]; then
    echo "Python generation must not own the shared buf namespace" >&2
    exit 1
  fi
done

node "$tool_root/scripts/embed-ingest-descriptor.mjs" --check --root "$root"
echo "Generated protobuf bindings and descriptors are current"
