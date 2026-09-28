#!/usr/bin/env bash

# On a release-tag CI run, require the immutable external-ingestion contract
# attestation; on every other run, say why it is not required.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"

if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  node scripts/sync_external_ingestion_contract.mjs --check-release
else
  echo "contract-release-gate: not a release tag; immutable attestation not required"
fi
