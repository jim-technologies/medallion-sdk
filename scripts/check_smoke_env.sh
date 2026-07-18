#!/usr/bin/env bash

set -euo pipefail

required=(
  MEDALLION_SMOKE_BASE_URL
  MEDALLION_SMOKE_ACCESS_TOKEN
  MEDALLION_SMOKE_ORGANIZATION_ID
  MEDALLION_SMOKE_EXPECTED_INGESTER_PRINCIPAL
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "::error::Missing required sdk-smoke secret: ${name}" >&2
    exit 1
  fi
done
