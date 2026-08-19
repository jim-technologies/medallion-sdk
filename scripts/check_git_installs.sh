#!/usr/bin/env bash

set -euo pipefail

mode="${1:-all}"
if (($# > 1)); then
  echo "usage: $0 [all|typescript]" >&2
  exit 2
fi
case "$mode" in
  all | typescript) ;;
  *)
    echo "usage: $0 [all|typescript]" >&2
    exit 2
    ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
version="$(tr -d '[:space:]' <"$root/VERSION")"
tmp="$(mktemp -d)"

cleanup() {
  chmod -R u+w "$tmp" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

# Create a temporary commit from the exact non-ignored working tree so local
# release rehearsals validate pending changes as well as committed CI trees.
snapshot="$tmp/source-snapshot"
git clone --quiet --no-hardlinks "$root" "$snapshot"
if ! git -C "$root" diff --quiet HEAD -- .; then
  git -C "$root" diff --binary HEAD -- . | git -C "$snapshot" apply --binary
fi
while IFS= read -r -d '' relative; do
  mkdir -p "$snapshot/$(dirname "$relative")"
  cp -p "$root/$relative" "$snapshot/$relative"
done < <(git -C "$root" ls-files --others --exclude-standard -z)
git -C "$snapshot" add --all
git -C "$snapshot" \
  -c user.name="Medallion install check" \
  -c user.email="install-check@example.invalid" \
  commit --quiet --allow-empty -m "Exact working tree install check"

sha="$(git -C "$snapshot" rev-parse HEAD)"
tag="v${version}"
git -C "$snapshot" tag --delete "$tag" >/dev/null 2>&1 || true
git -C "$snapshot" \
  -c user.name="Medallion install check" \
  -c user.email="install-check@example.invalid" \
  tag --annotate --message="Install check ${tag}" "$tag"

# A bare clone exposes exactly what a remote Git consumer receives.
source_repo="$tmp/medallion-sdk.git"
git clone --quiet --bare "$snapshot" "$source_repo"
git --git-dir="$source_repo" cat-file -e "${sha}^{commit}"
git --git-dir="$source_repo" cat-file -e "${tag}^{commit}"

git_config="$tmp/gitconfig"
git config --file "$git_config" \
  --add "url.file://${source_repo}.insteadOf" \
  "https://github.com/jim-technologies/medallion-sdk.git"
git config --file "$git_config" \
  --add "url.file://${source_repo}.insteadOf" \
  "https://github.com/jim-technologies/medallion-sdk"

for ref in "$sha" "$tag"; do
  suffix="${ref:0:12}"
  echo "Checking Git installs from ${ref}"

  if [[ "$mode" == "all" ]]; then
    echo "==> Go"
    go_consumer="$tmp/go-consumer-$suffix"
    mkdir -p "$go_consumer" "$tmp/gopath"
    (
      cd "$go_consumer"
      go mod init example.com/medallion-git-install >/dev/null
      env \
        GIT_ALLOW_PROTOCOL=file:https \
        GIT_CONFIG_GLOBAL="$git_config" \
        GIT_CONFIG_NOSYSTEM=1 \
        GOFLAGS=-mod=mod \
        GOPATH="$tmp/gopath" \
        GOPRIVATE=github.com/jim-technologies/medallion-sdk \
        GOPROXY=https://proxy.golang.org,direct \
        go get "github.com/jim-technologies/medallion-sdk/go@${ref}"
      env \
        GIT_ALLOW_PROTOCOL=file:https \
        GIT_CONFIG_GLOBAL="$git_config" \
        GIT_CONFIG_NOSYSTEM=1 \
        GOFLAGS=-mod=readonly \
        GOPATH="$tmp/gopath" \
        GOPRIVATE=github.com/jim-technologies/medallion-sdk \
        GOPROXY=https://proxy.golang.org,direct \
        go build github.com/jim-technologies/medallion-sdk/go
      sdk_module_dir="$(
        env \
          GIT_ALLOW_PROTOCOL=file:https \
          GIT_CONFIG_GLOBAL="$git_config" \
          GIT_CONFIG_NOSYSTEM=1 \
          GOFLAGS=-mod=readonly \
          GOPATH="$tmp/gopath" \
          GOPRIVATE=github.com/jim-technologies/medallion-sdk \
          GOPROXY=https://proxy.golang.org,direct \
          go list -m -f '{{.Dir}}' github.com/jim-technologies/medallion-sdk
      )"
      mapfile -t contract_directories < <(
        find "$sdk_module_dir/proto" -mindepth 1 -maxdepth 1 -type d \
          -name '*contract*' -printf '%f\n' | sort
      )
      if [[ "${contract_directories[*]}" != "external-ingestion-contract" ]]; then
        echo "Go module archive has unexpected contract directories: ${contract_directories[*]}" >&2
        exit 1
      fi
      if [[ ! -d "$sdk_module_dir/proto/external-ingestion-contract/v1" ]]; then
        echo "Go module archive is missing the minimal ingestion contract" >&2
        exit 1
      fi
      mapfile -t descriptors < <(
        find "$sdk_module_dir/proto" -maxdepth 1 -type f \
          -name '*.descriptor.binpb' -printf '%f\n' | sort
      )
      if [[ "${descriptors[*]}" != "external-ingestion-v1.descriptor.binpb" ]]; then
        echo "Go module archive has unexpected service descriptors: ${descriptors[*]}" >&2
        exit 1
      fi
      python3 "$root/scripts/check_package_artifacts.py" \
        --scan-tree "$sdk_module_dir"
      python3 "$root/scripts/check_package_artifacts.py" \
        --check-contract-tree \
        "$sdk_module_dir/proto/external-ingestion-contract/v1"
    )

    echo "==> Python"
    python_venv="$tmp/python-venv-$suffix"
    uv venv --quiet --seed --python 3.14 "$python_venv"
    "$python_venv/bin/python" -m pip install --quiet \
      --disable-pip-version-check \
      "git+file://${source_repo}@${ref}#subdirectory=python"
    EXPECTED_VERSION="$version" "$python_venv/bin/python" - <<'PY'
import importlib.metadata
import os

import medallion

assert importlib.metadata.version("medallion") == os.environ["EXPECTED_VERSION"]
assert medallion.MedallionClient is not None
PY
    installed_python_root="$(
      "$python_venv/bin/python" - <<'PY'
from pathlib import Path

import medallion

print(Path(medallion.__file__).resolve().parent)
PY
    )"
    python3 "$root/scripts/check_package_artifacts.py" \
      --scan-tree "$installed_python_root"
  fi

  echo "==> TypeScript"
  npm_consumer="$tmp/npm-consumer-$suffix"
  mkdir -p "$npm_consumer"
  (
    cd "$npm_consumer"
    npm init --yes --silent >/dev/null
    npm_config_cache="$tmp/npm-cache" npm install --silent --allow-git=all \
      "git+file://${source_repo}#${ref}"
    EXPECTED_VERSION="$version" node --input-type=module <<'JS'
import { readFile } from "node:fs/promises";
import { MedallionClient } from "@jimtech/medallion";

const entry = import.meta.resolve("@jimtech/medallion");
const manifest = JSON.parse(await readFile(new URL("../package.json", entry)));
const packagedVersion = (await readFile(new URL("../VERSION", entry), "utf8")).trim();
if (manifest.version !== process.env.EXPECTED_VERSION) {
  throw new Error(`installed ${manifest.version}; expected ${process.env.EXPECTED_VERSION}`);
}
if (packagedVersion !== process.env.EXPECTED_VERSION) {
  throw new Error(`packaged VERSION ${packagedVersion}; expected ${process.env.EXPECTED_VERSION}`);
}
if (typeof MedallionClient !== "function") {
  throw new Error("MedallionClient export is unavailable");
}
JS
    node <<'JS'
const { MedallionClient } = require("@jimtech/medallion");

if (typeof MedallionClient !== "function") {
  throw new Error("CommonJS MedallionClient export is unavailable");
}
JS
    if [[ "$mode" == "all" ]]; then
      python3 "$root/scripts/check_package_artifacts.py" \
        --scan-tree "$npm_consumer/node_modules/@jimtech/medallion"
    fi
  )
done

if [[ "$mode" == "all" ]]; then
  echo "Git SHA and root-tag installs passed for Go, Python, and TypeScript"
else
  echo "Git SHA and root-tag installs passed for TypeScript"
fi
