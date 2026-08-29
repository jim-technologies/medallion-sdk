# Vendored contracts

This directory vendors the two public contract surfaces the SDK speaks:

- `medallion.ingest.v1.MedallionIngestService` — the tabular datasets, append,
  and query surface (`medallion/ingest/v1/ingest.proto`).
- `medallion.connect.v1.MedallionConnectService` — the deprecated CDC/audit
  publish surface (`medallion/connect/v1/connect.proto`).

## Ingest contract (pin pending)

`medallion/ingest/v1/ingest.proto` is the tabular ingestion and query
contract: dataset create/get/list, `Append` (the insertAll analog), and
`Query`/`GetQueryResults` (the synchronous-first jobs.query analog with
poll-and-paginate). Queries pass through one statement in the declared
ClickHouse SQL dialect. Workspace identity rides only in request headers;
no ingest message carries a workspace field, and whole-batch replay
protection uses the `Idempotency-Key` request header.

The pin for this contract is PENDING: the file is authored against the agreed
contract and awaits the first sanitized upstream export with a checksummed
bundle and release attestation, following the same flow the external-ingestion
contract uses below. Until that export lands, the upstream contract remains
authoritative — reconcile any divergence toward upstream, never the other way
— and `make contract-release-check` stays a blocker for release tags.
`proto/ingest-v1.descriptor.binpb` is built deterministically from the
vendored proto by `make generate` and embedded into the TypeScript runtime.

## External-ingestion contract

This directory also vendors the checksummed `external_ingestion_sdk_v1` contract for the
public `medallion.connect.v1.MedallionConnectService` interface. Its only RPCs
are `PublishCdcEvents`, `PublishAuditEvents`, `ListCdcEvents`, and
`ListAuditEvents`. The descriptor is dependency-closed but contains only the
messages and enums reachable from those methods.

`external-ingestion-v1.descriptor.binpb` is verified against the standalone export,
used to generate the Go and Python bindings, and embedded in the TypeScript
runtime:

```sh
make proto-bindings
make proto-descriptor
make generated-check
```

The sync input is a standalone sanitized export. Pass it explicitly with
`MEDALLION_SDK_CONTRACT_ROOT=/path/to/export make contract-sync`, or run the
target without the variable to deterministically regenerate from the committed
offline copy. SDK consumers do not need Buf or Flox.

Ordinary checks accept a consistent candidate export. `make
contract-release-check` remains blocked until that export is replaced by a
producer-issued immutable release attestation.

## Conventions

- Package names always carry a version suffix (`medallion.<domain>.v1`); a
  breaking major is a new package, never an in-place edit of an existing one.
- `buf lint` enforces the standard rules plus comment coverage on public RPCs,
  messages, and fields. The vendored `connect.proto` is exempt from the
  comment rules only because it is rendered from the sanitized descriptor,
  which must omit source comments; its documentation is reviewed in the
  contract export.
- Breaking-change detection (`buf breaking`, FILE rules) runs inside
  `make validate` against the `main` baseline because this contract is
  consumed outside the repository.
- Buf itself is pinned in `.flox/env/manifest.toml`; CI never installs it.
