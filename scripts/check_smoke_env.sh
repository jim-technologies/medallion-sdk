#!/usr/bin/env bash

set -euo pipefail

required=(
  MEDALLION_SMOKE_BASE_URL
  MEDALLION_SMOKE_API_KEY
  MEDALLION_SMOKE_WORKSPACE_ID
  MEDALLION_SMOKE_CONNECTOR_ID
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "::error::Missing required sdk-smoke secret: ${name}" >&2
    exit 1
  fi
done

# The datasets live tier is opt-in on top of the connect smoke set; without a
# target dataset it skips rather than fails.
if [[ -z "${MEDALLION_SMOKE_INGEST_DATASET:-}" ]]; then
  echo "note: MEDALLION_SMOKE_INGEST_DATASET is unset; the datasets live tests will skip" >&2
fi
