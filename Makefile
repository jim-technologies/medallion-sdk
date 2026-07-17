PNPM ?= pnpm
GO ?= go
UV ?= uv

.DEFAULT_GOAL := help

.PHONY: help install check version-check version-set git-install-check check-public test test-version test-ts test-go test-python test-deployed build build-ts build-go build-python lint lint-ts lint-go lint-python deps proto-descriptor run clean

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Medallion SDK targets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

node_modules/.medallion-install-stamp: package.json pnpm-lock.yaml pnpm-workspace.yaml
	$(PNPM) install --frozen-lockfile
	@touch $@

install: ## Install development dependencies for all languages.
	$(PNPM) install
	$(GO) mod download
	cd python && $(UV) sync

check: version-check check-public lint test build ## Run public-surface checks, lint, tests, and builds for all languages.

version-check: ## Verify every language SDK uses the root VERSION.
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_versions.py

version-set: ## Synchronize every SDK version (usage: make version-set VERSION=X.Y.Z).
	@test -n "$(VERSION)" || { echo "VERSION is required"; exit 2; }
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/set_version.py "$(VERSION)"

git-install-check: ## Install every language SDK from the current Git commit.
	scripts/check_git_installs.sh

check-public: node_modules/.medallion-install-stamp ## Scan public SDK surfaces for private/internal references.
	$(PNPM) check:public

test: test-version test-ts test-go test-python ## Run all tests.

test-version: ## Test version synchronization and release-tag guardrails.
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_versions.py

test-ts: node_modules/.medallion-install-stamp ## Run TypeScript tests.
	$(PNPM) test

test-go: ## Run Go tests.
	$(GO) test ./go/...

test-python: ## Run Python tests.
	cd python && $(UV) run python -m unittest discover tests

test-deployed: node_modules/.medallion-install-stamp ## Run opt-in deployed smoke test against a locked-down tenant.
	$(PNPM) test:deployed

build: build-ts build-go build-python ## Build all SDK packages.

build-ts: node_modules/.medallion-install-stamp ## Build JavaScript and type declarations.
	$(PNPM) build

build-go: ## Compile Go packages.
	$(GO) build ./go/...

build-python: ## Build the Python package from the git subdirectory.
	cd python && $(UV) build

lint: lint-ts lint-go lint-python ## Run lightweight lint/type checks for all languages.

lint-ts: node_modules/.medallion-install-stamp ## Type-check the TypeScript SDK.
	$(PNPM) lint

lint-go: ## Format-check Go files.
	@test -z "$$(gofmt -l go)" || { echo "gofmt: files need formatting:"; gofmt -l go; exit 1; }

lint-python: ## Byte-compile Python sources and tests.
	cd python && $(UV) run python -m compileall -q src tests

deps: ## Refresh dependencies within declared compatibility ranges.
	$(PNPM) update
	$(GO) get -u ./go/...
	$(GO) mod tidy
	cd python && $(UV) lock --upgrade

proto-descriptor: node_modules/.medallion-install-stamp ## Regenerate TypeScript invariantprotocol descriptors.
	$(PNPM) proto:descriptor

run: build-ts ## Smoke-test importing the built TypeScript SDK.
	node --input-type=module -e 'import("./dist/index.js").then((m) => { if (!m.MedallionClient) throw new Error("missing MedallionClient export"); })'

clean: ## Remove generated build outputs and caches.
	$(PNPM) clean
	rm -rf coverage.out go/coverage.out python/dist python/.pytest_cache python/.ruff_cache
	find python -type d \( -name '__pycache__' -o -name '*.egg-info' \) -prune -exec rm -rf {} +
