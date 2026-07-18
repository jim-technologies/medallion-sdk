#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
tmp="$(mktemp -d)"

cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

cd "$root"

pnpm exec buf generate \
  --template buf.gen.yaml \
  --path proto/medallion/connect/v1/connect.proto \
  --output "$tmp/generated"

generated_files=(
  "go/gen/medallion/connect/v1/connect.pb.go"
  "python/src/buf/validate/validate_pb2.py"
  "python/src/medallion/connect/v1/connect_pb2.py"
)
for relative in "${generated_files[@]}"; do
  if ! cmp -s "$relative" "$tmp/generated/$relative"; then
    echo "$relative is stale; run make proto-bindings" >&2
    diff -u "$relative" "$tmp/generated/$relative" || true
    exit 1
  fi
done

pnpm exec buf build proto \
  --as-file-descriptor-set \
  --path proto/medallion/connect/v1/connect.proto \
  -o "$tmp/medallion-connect.descriptor.binpb"
pnpm exec buf build proto \
  --as-file-descriptor-set \
  --path proto/medallion/ontology/v1/ontology.proto \
  -o "$tmp/medallion-ontology.descriptor.binpb"
pnpm exec buf build proto \
  --as-file-descriptor-set \
  --path proto/medallion/storage/v1/service.proto \
  -o "$tmp/medallion-storage.descriptor.binpb"

descriptor_pairs=(
  "proto/medallion-connect.descriptor.binpb:$tmp/medallion-connect.descriptor.binpb"
  "proto/medallion-ontology.descriptor.binpb:$tmp/medallion-ontology.descriptor.binpb"
  "proto/medallion-storage.descriptor.binpb:$tmp/medallion-storage.descriptor.binpb"
)
for pair in "${descriptor_pairs[@]}"; do
  committed="${pair%%:*}"
  generated="${pair#*:}"
  if ! cmp -s "$committed" "$generated"; then
    echo "$committed is stale; run make proto-descriptor" >&2
    exit 1
  fi
done

node scripts/embed-connect-descriptor.mjs --check
echo "Generated protobuf bindings and descriptors are current"
