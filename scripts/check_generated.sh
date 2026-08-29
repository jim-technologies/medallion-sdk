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
if [[ "${descriptors[*]}" != "proto/external-ingestion-v1.descriptor.binpb proto/ingest-v1.descriptor.binpb" ]]; then
  echo "proto must contain exactly the external-ingestion and ingest v1 descriptors" >&2
  printf 'found: %s\n' "${descriptors[*]:-(none)}" >&2
  exit 1
fi

buf generate proto/external-ingestion-v1.descriptor.binpb \
  --template "$root/buf.gen.yaml" \
  --path medallion/connect/v1/connect.proto \
  --output "$tmp/generated"

buf generate proto \
  --template "$root/buf.gen.yaml" \
  --path proto/medallion/ingest/v1/ingest.proto \
  --output "$tmp/generated"

mkdir -p "$tmp/generated/proto"
buf build proto \
  --path proto/medallion/ingest/v1/ingest.proto \
  --exclude-source-info \
  -o "$tmp/generated/proto/ingest-v1.descriptor.binpb"

generated_files=(
  "go/gen/medallion/connect/v1/connect.pb.go"
  "go/gen/medallion/ingest/v1/ingest.pb.go"
  "proto/ingest-v1.descriptor.binpb"
  "python/src/buf/validate/validate_pb2.py"
  "python/src/medallion/connect/v1/connect_pb2.py"
  "python/src/medallion/ingest/v1/ingest_pb2.py"
)
for relative in "${generated_files[@]}"; do
  if ! cmp -s "$relative" "$tmp/generated/$relative"; then
    echo "$relative is stale; run make proto-bindings" >&2
    diff -u "$relative" "$tmp/generated/$relative" || true
    exit 1
  fi
done

node "$tool_root/scripts/embed-connect-descriptor.mjs" --check --root "$root"
echo "Generated protobuf bindings and descriptors are current"
