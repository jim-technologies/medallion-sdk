#!/usr/bin/env bash

set -euo pipefail

required=(
  MEDALLION_SMOKE_BASE_URL
  MEDALLION_SMOKE_API_KEY
  MEDALLION_SMOKE_WORKSPACE_ID
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "::error::Missing required sdk-smoke secret: ${name}" >&2
    exit 1
  fi
done

# The tables live tier skips without a caller-provisioned target table.
if [[ -z "${MEDALLION_SMOKE_INGEST_TABLE:-}" ]]; then
  echo "note: MEDALLION_SMOKE_INGEST_TABLE is unset; the tables live tests will skip" >&2
fi

# The durable-execution live tier additionally needs a deployment that serves
# the temporaless.v1 storage services.
if [[ -z "${MEDALLION_SMOKE_WORKFLOWS:-}" ]]; then
  echo "note: MEDALLION_SMOKE_WORKFLOWS is unset; the workflows live tests will skip" >&2
fi
