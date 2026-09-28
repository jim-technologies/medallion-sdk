# Changelog

All notable changes to the Medallion SDK are documented in this file. Every
language SDK shares the repository-root `VERSION` and ships together from one
annotated `vX.Y.Z` tag.

## [Unreleased]

- Take the fleet `MAKEFILE-CONTRACT.md` text shared by every public
  jim-technologies repository: `make release` creates and pushes the annotated
  `v<VERSION>` tag after the same guards everywhere, and `run` and `deploy`
  are not part of a framework's contract.
- Removed the `.ci/node-26` Flox environment: CI never activated it, and the
  root environment already runs Node 26, the `engines.node` floor.
  `make version-check` now requires the root environment's Node to equal that
  floor, so the one gate always tests the oldest supported Node.
- `make release` publishes instead of refusing: after its guards (the
  immutable contract attestation, a clean tree, `HEAD` pushed to
  `origin/main`, every version mirror and the first CHANGELOG release heading
  equal to `VERSION`, the tag absent locally and on origin) it creates and
  pushes the annotated `vVERSION` tag, the repository's one distribution. The
  logic lives in `scripts/release`, and the release-tag gate now also requires
  the CHANGELOG release heading.
- Removed the fail-closed `make release` stub and `make run`, whose import
  smoke of the ESM bundle now runs in `make build-ts` inside the gate. The
  multi-line recipes of `contract-release-gate`, `check-examples`, `lint-go`,
  and `audit-python` moved to `scripts/`, so every Makefile target names one
  tool or one script.
- Point the README install commands at `v0.3.1`, the newest release tag; they
  named `v0.3.0`, which was never tagged. `make version-check` now fails when
  an install command names a tag this repository does not have, and CI checks
  out full history so tags and `main` are present offline. The changelog gains
  the `0.3.1` section that tag shipped with.
- Pace query polling instead of issuing up to 1,000 back-to-back
  `GetQueryResults` requests: `tables.query()` in TypeScript and Python waits
  the client's retry backoff between polls (200 ms doubling to 2 s by default,
  whether or not retries are enabled), and a `Retry-After` on a running answer
  replaces that wait, capped at 30 s. Cancellation (`signal`,
  `cancellation_event`) interrupts the wait. TypeScript low-level pollers get
  the same pacing from `client.ingest.waitBeforeQueryPoll()`, and the Go
  quickstart paces its caller-side loop the same way.
- `make fmt` now formats `python/tests_workflows`, which `make validate`
  already lint-checks, and `.gitignore` covers a `.ruff_cache/` at any depth.
- Keep `make validate` offline and hermetic, as `MAKEFILE-CONTRACT.md`
  requires: `make test-workflows` leaves `make test` and becomes an opt-in
  tier beside `make test-deployed`, because it installs Temporaless from
  GitHub. The offline guard that holds the Temporaless pin in every file still
  runs in the gate (`make version-check`).
- Expose Medallion as a durable-execution backend through `medallion.workflows`
  (Python): `store()` and `query_store()` return Temporaless's own
  `ConnectStore` / `ConnectQueryStore` pointed at the configured Medallion
  endpoint, with the client's credential and immutable workspace attached as
  request headers by a ConnectRPC interceptor that caller interceptors cannot
  displace. No storage client is reimplemented here and none of the
  `temporaless.v1` RPCs are restated. `capabilities()` surfaces the
  `GetStoreCapabilities` handshake and `require_capabilities()` turns a backend
  without atomic create-if-absent into a startup error. Ships as the
  `medallion[workflows]` extra, pinning Temporaless v0.10.7 by immutable
  commit; `scripts/check_versions.py` holds that pin across every file naming
  it inside the gate, and the opt-in `make test-workflows` tier runs the suite
  against a built wheel so a drifting upstream contract fails it. No operator
  client is shipped: `PutEvent`, the bounded deletions, and `Sweep` are
  enumerated in `OPERATOR_METHODS` and stay behind a separate operator
  credential and the server's per-method authorization. TypeScript parity
  waits for a real consumer; Go documents the ten-line interceptor instead of
  taking a dependency that would triple every Go consumer's module graph.
- Re-pin the `medallion.ingest.v1` surface from the provisional datasets
  sketch of 0.3.1 to the released upstream contract: `CreateTable`,
  `GetTable`, `ListTables`, `UpdateTable`, `AppendRows` (the insertAll analog
  with per-row `insert_id` passthrough, `skip_invalid_rows`, and per-row error
  surfacing), and `RunQuery`/`GetQueryResults` (the synchronous-first
  jobs.query analog with transparent poll-and-paginate). Queries pass one
  statement through verbatim in the declared ClickHouse SQL dialect; workspace
  identity comes only from the verified transport, and every write carries a
  batch idempotency key sent as both the Stripe-style `Idempotency-Key` header
  and the contract's `request_id` field. `proto/README.md` records the pin and
  the three deliberate deviations from upstream.
- Name the resource a TABLE, not a dataset, the way the contract does: a
  workspace already plays BigQuery's dataset role, so the resource one level
  down is a table with a declared schema (`BOOL`, `INT64`, `FLOAT64`,
  `STRING`, `BYTES`, `TIMESTAMP`, `DATE`, `JSON`), a `TIMESTAMP` time column,
  and an optional sort key. Schema evolution is additive only: `UpdateTable`
  takes the full desired schema and may only append new nullable columns.
- Ship the surface in all three languages: TypeScript `client.tables` and the
  low-level `client.ingest` with an async row iterator that never exposes page
  tokens; Python `client.tables` with the dataframe-first layer
  (polars/pyarrow appends, `to_polars()` collection, `medallion[polars]`
  extra); Go generated bindings with a deliberately thin `client.Ingest`.
  Runnable quickstarts land in `examples/` for every language, and live tests
  stay opt-in behind the `MEDALLION_SMOKE_*` environment
  (`MEDALLION_SMOKE_INGEST_TABLE` selects the target table).
## [0.3.1] - 2026-08-29

Versions 0.2.0 and 0.3.0 were never tagged; their changes ship here.

- Add the `medallion.ingest.v1` tabular surface as the SDK's main act:
  dataset create/get/list, `Append` (the insertAll analog with per-row
  `insert_id` passthrough and per-row error surfacing), and
  `Query`/`GetQueryResults` (the synchronous-first jobs.query analog with
  transparent poll-and-paginate). Queries pass one statement through verbatim
  in the declared ClickHouse SQL dialect; workspace identity rides only in
  request headers, and appends and dataset creation carry an automatic
  Stripe-style `Idempotency-Key` header for whole-batch replay protection.
  The vendored ingest proto awaits its first sanitized upstream export; the
  pin is recorded as pending in `proto/README.md`.
- Ship the surface in all three languages: TypeScript `client.datasets` and
  the low-level `client.ingest` with an async row iterator that never exposes
  page tokens; Python `client.datasets` with the dataframe-first layer
  (polars/pyarrow appends, `to_polars()` collection, `medallion[polars]`
  extra); Go generated bindings with a deliberately thin `client.Ingest`.
  Runnable quickstarts land in `examples/` for every language, and live tests
  stay opt-in behind the `MEDALLION_SMOKE_*` environment.
- Deprecate the `medallion.connect.v1` CDC/audit publish surface. The four
  publish/list RPCs and their clients keep working unchanged; the README is
  rewritten around getting data into and out of Medallion through datasets.
- Replace the SDK-specific public-surface script with the shared guard every
  public jim-technologies repository runs. It scans tracked content, tracked
  paths, and the commit messages a push would publish; exceptions live in
  `.public-surface-allow` and this repository's extra denials, including the
  scope names retired from the v1 contract, in `.public-surface-deny`.
- Enforce the buf conventions in the gate: comment linting on public RPCs,
  messages, and fields, breaking-change detection against the `main` baseline
  inside `make validate`, and Buf pinned in the Flox manifest instead of npm.
- Adopt the jim-technologies open-source Makefile contract
  (`MAKEFILE-CONTRACT.md`): the gate verb is `make validate`, formatting is
  `make fmt`, schema regeneration is `make generate`, and `make release` is a
  fail-closed stub while distribution stays git-install based.
- Narrow the SDK to the four customer-ingestion RPCs and vendor a sanitized,
  attested external-ingestion contract with offline drift checks.
- Scope ingestion credentials to an immutable workspace selected at client
  construction.

## [0.1.1] - 2026-07-18

- Fix release validation for the unified root tag.

## [0.1.0] - 2026-07-17

- First unified release: TypeScript, Go, and Python SDKs ship together from
  one root tag with lockstep versions.
