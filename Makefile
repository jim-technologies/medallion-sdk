PNPM ?= corepack pnpm
GO ?= go
UV ?= uv

.DEFAULT_GOAL := help

.PHONY: help install check check-public test test-ts test-go test-python test-deployed build build-ts build-go build-python lint lint-ts lint-go lint-python proto-descriptor run clean

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Medallion SDK targets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install development dependencies for all languages.
	$(PNPM) install
	cd go && $(GO) mod download
	cd python && $(UV) sync

check: check-public lint test build ## Run public-surface checks, lint, tests, and builds for all languages.

check-public: ## Scan public SDK surfaces for private/internal references.
	$(PNPM) check:public

test: test-ts test-go test-python ## Run all tests.

test-ts: ## Run TypeScript tests.
	$(PNPM) test

test-go: ## Run Go tests.
	cd go && $(GO) test ./...

test-python: ## Run Python tests.
	cd python && $(UV) run python -m unittest discover tests

test-deployed: ## Run opt-in deployed smoke test against a locked-down tenant.
	$(PNPM) test:deployed

build: build-ts build-go build-python ## Build all SDK packages.

build-ts: ## Build JavaScript and type declarations.
	$(PNPM) build

build-go: ## Compile Go packages.
	cd go && $(GO) build ./...

build-python: ## Build the Python package from the git subdirectory.
	cd python && $(UV) build

lint: lint-ts lint-go lint-python ## Run lightweight lint/type checks for all languages.

lint-ts: ## Type-check the TypeScript SDK.
	$(PNPM) lint

lint-go: ## Format-check Go files.
	cd go && test -z "$$($(GO) fmt ./...)"

lint-python: ## Byte-compile Python sources and tests.
	cd python && $(UV) run python -m compileall -q src tests

proto-descriptor: ## Regenerate TypeScript invariantprotocol descriptors.
	$(PNPM) proto:descriptor

run: build-ts ## Smoke-test importing the built TypeScript SDK.
	node --input-type=module -e 'import("./dist/index.js").then((m) => { if (!m.MedallionClient) throw new Error("missing MedallionClient export"); })'

clean: ## Remove generated build outputs and caches.
	$(PNPM) clean
	rm -rf go/coverage.out python/dist python/.pytest_cache python/.ruff_cache
	find python -type d \( -name '__pycache__' -o -name '*.egg-info' \) -prune -exec rm -rf {} +
