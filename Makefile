SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

PNPM ?= pnpm
BUF ?= buf
GO ?= go
UV ?= uv
PYTHON ?= python3
RUFF ?= ruff
SHELLCHECK ?= shellcheck
SHFMT ?= shfmt
ACTIONLINT ?= actionlint
GITLEAKS ?= gitleaks
GOVULNCHECK_VERSION ?= v1.6.0
PIP_AUDIT_VERSION ?= 2.10.1

.DEFAULT_GOAL := help

.PHONY: help help-all install validate lock-check version-check version-set release contract-sync contract-check contract-release-check contract-release-gate generated-check breaking-check artifact-check git-install-check public-surface check-examples test test-version test-contract-sync test-package-artifacts test-ts test-go test-python test-deployed build build-ts build-go build-python lint lint-ts lint-go lint-python lint-proto lint-shell lint-workflows fmt fmt-ts fmt-go fmt-python fmt-proto fmt-shell audit audit-node audit-go audit-python secret-check deps generate proto-bindings proto-descriptor run clean

help: ## One-screen help (make help-all for every target)
	@echo "Daily:"
	@echo "  make fmt        autofix formatting, every language"
	@echo "  make test       all tests (TS, Go, Python)"
	@echo "  make validate   the full offline gate; exactly what CI runs"
	@echo "  make build      build all SDK packages"
	@echo "  make generate   regenerate schema-derived code"
	@echo ""
	@echo "Everything else: make help-all"

help-all: ## Every target with its description
	@grep -hE '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/'

node_modules/.medallion-install-stamp: package.json pnpm-lock.yaml pnpm-workspace.yaml
	$(PNPM) install --frozen-lockfile
	@touch $@

install: ## Install development dependencies for all languages.
	$(PNPM) install --frozen-lockfile
	$(GO) mod download
	cd python && $(UV) sync --locked

validate: lock-check version-check generated-check breaking-check contract-release-gate public-surface lint test build check-examples artifact-check ## Run the full local gate; exactly what CI runs.

lock-check: node_modules/.medallion-install-stamp ## Verify all language dependency locks are synchronized.
	$(GO) mod tidy -diff
	cd python && $(UV) lock --check

version-check: ## Verify every language SDK uses the root VERSION.
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/check_versions.py

version-set: ## Synchronize every SDK version (usage: make version-set VERSION=X.Y.Z).
	@test -n "$(VERSION)" || { echo "VERSION is required"; exit 2; }
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/set_version.py "$(VERSION)"

release: ## Fail-closed release stub: require a clean, pushed, version-consistent tree, then refuse.
	@status="$$(git status --porcelain)"; test -z "$$status" \
		|| { echo "release: refusing: the working tree is dirty:"; echo "$$status"; exit 1; }
	@git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' >/dev/null 2>&1 \
		|| { echo "release: refusing: the current branch has no upstream"; exit 1; }
	@git fetch --quiet \
		|| { echo "release: refusing: cannot fetch the upstream to verify the push state"; exit 1; }
	@git merge-base --is-ancestor HEAD '@{upstream}' \
		|| { echo "release: refusing: HEAD is not pushed to $$(git rev-parse --abbrev-ref '@{upstream}')"; exit 1; }
	$(MAKE) version-check
	@echo "release: the tree is clean, pushed, and version-consistent at v$$(cat VERSION)."
	@echo "release: publishing to npm, PyPI, or a public Go module tag is a pending product decision;"
	@echo "release: distribution stays git-install from the annotated v$$(cat VERSION) root tag (see CONTRIBUTING.md)."
	@exit 1

contract-sync: node_modules/.medallion-install-stamp ## Sync a sanitized SDK contract export (MEDALLION_SDK_CONTRACT_ROOT), then regenerate.
	node scripts/sync_external_ingestion_contract.mjs --sync
	$(MAKE) proto-bindings proto-descriptor
	node scripts/sync_external_ingestion_contract.mjs --check
	scripts/check_generated.sh

contract-check: node_modules/.medallion-install-stamp ## Verify vendored external-ingestion artifacts and wire parity offline.
	node scripts/sync_external_ingestion_contract.mjs --check

contract-release-check: node_modules/.medallion-install-stamp ## Require an immutable sanitized SDK contract attestation before tagging.
	node scripts/sync_external_ingestion_contract.mjs --check-release

contract-release-gate: node_modules/.medallion-install-stamp ## Require the immutable contract attestation when validating a release tag.
	@if [ "$${GITHUB_REF_TYPE:-}" = "tag" ]; then \
		node scripts/sync_external_ingestion_contract.mjs --check-release; \
	else \
		echo "contract-release-gate: not a release tag; immutable attestation not required"; \
	fi

generated-check: contract-check node_modules/.medallion-install-stamp ## Verify generated protobuf bindings and descriptors have no drift.
	scripts/check_generated.sh

breaking-check: ## Verify protobuf contracts stay backward-compatible with the main baseline.
	scripts/check_proto_breaking.sh

artifact-check: build ## Verify Git-install package payloads and exact bundled license coverage.
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/check_package_artifacts.py

git-install-check: ## Install every SDK from the exact tree by SHA and root tag.
	scripts/check_git_installs.sh

public-surface: secret-check ## Guard the public surface: tracked content, paths, and unpushed commit messages, plus a credential scan.
	scripts/public-surface-check
	scripts/public-surface-check-test

check-examples: build ## Check the runnable TypeScript, Go, and Python quickstart examples.
	$(PNPM) check:examples
	@unformatted="$$(gofmt -l examples)"; \
	test -z "$$unformatted" || { echo "gofmt: examples need formatting:"; echo "$$unformatted"; exit 1; }
	$(GO) vet ./examples/
	$(RUFF) check --no-cache examples
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m compileall -q examples

test: test-version test-contract-sync test-package-artifacts test-ts test-go test-python ## Run all tests.

test-version: ## Test version synchronization and release-tag guardrails.
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/test_versions.py

test-contract-sync: node_modules/.medallion-install-stamp ## Test external-ingestion synchronization and drift detection.
	node --test scripts/test_external_ingestion_contract.mjs

test-package-artifacts: ## Test package-artifact compatibility helpers.
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/test_package_artifacts.py
	node --test scripts/test_third_party_licenses.mjs

test-ts: node_modules/.medallion-install-stamp ## Run TypeScript tests.
	$(PNPM) test

test-go: ## Run Go tests.
	$(GO) test ./go/...

test-python: ## Run Python tests.
	cd python && $(UV) run --locked python -m unittest discover tests

test-deployed: node_modules/.medallion-install-stamp ## Run opt-in deployed smoke test against a locked-down workspace.
	$(PNPM) test:deployed

build: build-ts build-go build-python ## Build all SDK packages.

build-ts: node_modules/.medallion-install-stamp ## Build JavaScript and type declarations.
	$(PNPM) build

build-go: ## Compile Go packages.
	$(GO) build ./go/...

build-python: ## Build the Python package from the git subdirectory.
	cd python && $(UV) build

lint: lint-ts lint-go lint-python lint-proto lint-shell lint-workflows ## Run all format, lint, type, schema, shell, and workflow checks.

lint-ts: node_modules/.medallion-install-stamp ## Type-check, lint, and format-check TypeScript and JSON.
	$(PNPM) lint

lint-go: ## Format-check and vet Go code.
	@unformatted="$$(find go -type f -name '*.go' -exec gofmt -l {} +)"; \
	test -z "$$unformatted" || { echo "gofmt: files need formatting:"; echo "$$unformatted"; exit 1; }
	$(GO) vet ./go/...

lint-python: ## Lint, format-check, and byte-compile authored Python code.
	cd python && $(RUFF) check src tests
	cd python && $(RUFF) format --check src tests
	cd python && $(UV) run --locked python -m compileall -q src tests

lint-proto: ## Lint and format-check vendored protobuf contracts.
	$(BUF) lint
	$(BUF) format proto --diff --exit-code

lint-shell: ## Lint and format-check repository shell scripts.
	$(SHELLCHECK) scripts/*.sh
	$(SHFMT) -d -i 2 -ci scripts/*.sh

lint-workflows: ## Validate GitHub Actions syntax and expressions.
	$(ACTIONLINT) .github/workflows/*.yml

fmt: fmt-ts fmt-go fmt-python fmt-proto fmt-shell ## Rewrite formatting in place, every language in the repo.

fmt-ts: node_modules/.medallion-install-stamp ## Format TypeScript and JSON.
	$(PNPM) format

fmt-go: ## Format Go code.
	find go -type f -name '*.go' -exec gofmt -w {} +

fmt-python: ## Apply safe Python lint fixes and formatting.
	cd python && $(RUFF) check --fix src tests
	cd python && $(RUFF) format src tests

fmt-proto: ## Format vendored protobuf contracts.
	$(BUF) format proto --write

fmt-shell: ## Format repository shell scripts.
	$(SHFMT) -w -i 2 -ci scripts/*.sh

audit: audit-node audit-go audit-python secret-check ## Scan dependencies and the tree for known vulnerabilities and secrets.

audit-node: node_modules/.medallion-install-stamp ## Audit the locked JavaScript dependency graph.
	$(PNPM) audit --audit-level=low

audit-go: ## Audit Go packages and tests against the current vulnerability database.
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) -test ./...

audit-python: ## Audit exact Python runtime locks and the pinned build backend.
	@runtime_requirements="$$(mktemp)"; \
	build_requirements="$$(mktemp)"; \
	trap 'rm -f "$$runtime_requirements" "$$build_requirements"' EXIT; \
	$(UV) export --project python --locked --no-dev --no-emit-project \
		--format requirements-txt --output-file "$$runtime_requirements" >/dev/null; \
	$(UV) tool run --from pip-audit==$(PIP_AUDIT_VERSION) pip-audit \
		--strict --disable-pip --vulnerability-service osv \
		--requirement "$$runtime_requirements" --progress-spinner off; \
	$(PYTHON) -c 'import tomllib; data = tomllib.load(open("python/pyproject.toml", "rb")); print(*data["build-system"]["requires"], sep="\n")' >"$$build_requirements"; \
	$(UV) tool run --from pip-audit==$(PIP_AUDIT_VERSION) --with pip pip-audit \
		--strict --vulnerability-service osv \
		--requirement "$$build_requirements" --progress-spinner off

secret-check: ## Scan the working tree for committed credentials and tokens.
	$(GITLEAKS) dir . --no-banner --redact

deps: ## Refresh dependencies within declared compatibility ranges.
	$(PNPM) update
	GOFLAGS=-mod=mod $(GO) get -u ./go/...
	GOFLAGS=-mod=mod $(GO) mod tidy
	cd python && $(UV) lock --upgrade

generate: proto-bindings proto-descriptor ## Regenerate all schema-derived code; validate fails if committed output is stale.

proto-bindings: ## Regenerate public Go and Python Connect and ingest protobuf bindings.
	$(BUF) generate proto/external-ingestion-v1.descriptor.binpb --template buf.gen.yaml --path medallion/connect/v1/connect.proto
	$(BUF) generate proto --template buf.gen.yaml --path proto/medallion/ingest/v1/ingest.proto
	$(BUF) build proto --path proto/medallion/ingest/v1/ingest.proto --exclude-source-info -o proto/ingest-v1.descriptor.binpb

proto-descriptor: node_modules/.medallion-install-stamp ## Regenerate TypeScript invariantprotocol descriptors.
	node scripts/embed-connect-descriptor.mjs

run: build-ts ## Smoke-test importing the built TypeScript SDK.
	node --input-type=module -e 'import("./dist/index.js").then((m) => { if (!m.MedallionClient) throw new Error("missing MedallionClient export"); })'

clean: ## Remove generated build outputs and caches.
	$(PNPM) clean
	rm -rf coverage.out go/coverage.out python/dist python/.pytest_cache python/.ruff_cache
	find python -type d \( -name '__pycache__' -o -name '*.egg-info' \) -prune -exec rm -rf {} +
