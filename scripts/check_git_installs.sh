#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
sha="$(git -C "$root" rev-parse HEAD)"
version="$(tr -d '[:space:]' < "$root/VERSION")"
tmp="$(mktemp -d)"

cleanup() {
  chmod -R u+w "$tmp" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

# A bare clone exposes exactly the files committed at this revision, matching
# what a Git consumer receives without generated or ignored working-tree state.
source_repo="$tmp/medallion-sdk.git"
git clone --quiet --bare "$root" "$source_repo"
# Anchor the exact checkout as an advertised ref. GitHub pull-request jobs use
# detached synthetic commits, which need not be reachable from a branch.
git --git-dir="$source_repo" fetch --quiet "$root" \
  "${sha}:refs/heads/install-source"
git --git-dir="$source_repo" cat-file -e "${sha}^{commit}"

echo "Checking Git installs from ${sha}"

echo "==> Go"
go_consumer="$tmp/go-consumer"
mkdir -p "$go_consumer" "$tmp/gopath"
(
  cd "$go_consumer"
  go mod init example.com/medallion-git-install >/dev/null

  git_config="$tmp/gitconfig"
  git config --file "$git_config" \
    --add "url.file://${source_repo}.insteadOf" \
    "https://github.com/jim-technologies/medallion-sdk.git"
  git config --file "$git_config" \
    --add "url.file://${source_repo}.insteadOf" \
    "https://github.com/jim-technologies/medallion-sdk"

  env \
    GIT_ALLOW_PROTOCOL=file:https \
    GIT_CONFIG_GLOBAL="$git_config" \
    GIT_CONFIG_NOSYSTEM=1 \
    GOPATH="$tmp/gopath" \
    GOPRIVATE=github.com/jim-technologies/medallion-sdk \
    GOPROXY=https://proxy.golang.org,direct \
    go get "github.com/jim-technologies/medallion-sdk/go@${sha}"
  env \
    GIT_ALLOW_PROTOCOL=file:https \
    GIT_CONFIG_GLOBAL="$git_config" \
    GIT_CONFIG_NOSYSTEM=1 \
    GOPATH="$tmp/gopath" \
    GOPRIVATE=github.com/jim-technologies/medallion-sdk \
    GOPROXY=https://proxy.golang.org,direct \
    go build github.com/jim-technologies/medallion-sdk/go
)

echo "==> Python"
python_venv="$tmp/python-venv"
uv venv --quiet --seed --python 3.14 "$python_venv"
"$python_venv/bin/python" -m pip install --quiet \
  --disable-pip-version-check \
  "git+file://${source_repo}@${sha}#subdirectory=python"
EXPECTED_VERSION="$version" "$python_venv/bin/python" - <<'PY'
import importlib.metadata
import os

import medallion

assert importlib.metadata.version("medallion") == os.environ["EXPECTED_VERSION"]
assert medallion.MedallionClient is not None
PY

echo "==> TypeScript"
npm_consumer="$tmp/npm-consumer"
mkdir -p "$npm_consumer"
(
  cd "$npm_consumer"
  npm init --yes --silent >/dev/null
  npm_config_cache="$tmp/npm-cache" npm install --silent --allow-git=all \
    "git+file://${source_repo}#${sha}"
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
)

echo "Git installs passed for Go, Python, and TypeScript"
