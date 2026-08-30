# Changelog

All notable changes to the Medallion SDK are documented in this file. Every
language SDK shares the repository-root `VERSION` and ships together from one
annotated `vX.Y.Z` tag.

## [Unreleased]

- Add the `medallion.ingest.v1` tabular surface as the SDK's main act, tracking
  the released upstream contract: `CreateTable`, `GetTable`, `ListTables`,
  `UpdateTable`, `AppendRows` (the insertAll analog with per-row `insert_id`
  passthrough, `skip_invalid_rows`, and per-row error surfacing), and
  `RunQuery`/`GetQueryResults` (the synchronous-first jobs.query analog with
  transparent poll-and-paginate). Queries pass one statement through verbatim
  in the declared ClickHouse SQL dialect; workspace identity comes only from
  the verified transport, and every write carries a batch idempotency key sent
  as both the Stripe-style `Idempotency-Key` header and the contract's
  `request_id` field. `proto/README.md` records the pin and the three
  deliberate deviations from upstream.
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
