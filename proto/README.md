# Vendored contracts

This directory vendors the two public contract surfaces the SDK speaks:

- `medallion.ingest.v1.MedallionIngestService` — the tabular tables, append,
  and query surface (`medallion/ingest/v1/ingest.proto`).
- `medallion.connect.v1.MedallionConnectService` — the deprecated CDC/audit
  publish surface (`medallion/connect/v1/connect.proto`).

## Ingest contract

`medallion/ingest/v1/ingest.proto` tracks the released upstream tabular ingest
and query contract. It has seven RPCs:

| RPC | What it does |
| --- | --- |
| `CreateTable` | Declares a table: an ordered schema, a `TIMESTAMP` time column, and an optional sort key. Re-declaring an identical table returns the existing one. |
| `GetTable` | Reads one table by resource name, `tables/{table}`. |
| `ListTables` | Pages the workspace's tables, ordered by table id. |
| `UpdateTable` | Evolves a schema (the `tables.patch` shape). The request carries the FULL desired schema and may only append new nullable columns at the end; resending the current schema is a no-op success. |
| `AppendRows` | Appends one batch of JSON rows or one Arrow IPC stream (the `insertAll` analog). Acknowledged rows are durable and immediately queryable. |
| `RunQuery` | Runs one read-only ClickHouse SQL statement (the `jobs.query` analog, modernized): sync-first, returning rows inline when the query finishes within `timeout_ms`, otherwise a `RUNNING` query to poll. |
| `GetQueryResults` | Polls one query and cursor-paginates its stored result set. |

The vocabulary is TABLE, not dataset: a workspace already plays the role
BigQuery gives a dataset, so the resource one level down is a table. Column
types are the BigQuery-style set `BOOL`, `INT64`, `FLOAT64`, `STRING`,
`BYTES`, `TIMESTAMP`, `DATE`, `JSON`. Workspace identity comes only from the
verified transport; no ingest message carries a workspace field. Batch replay
protection uses the AIP-155 `request_id` field, which REST callers may also
supply as a Stripe-style `Idempotency-Key` header; the SDK sends both.

`proto/ingest-v1.descriptor.binpb` is built deterministically from the
vendored proto by `make generate` and embedded into the TypeScript runtime.

### Deliberate deviations from upstream

The vendored copy is wire-identical to the upstream contract — every message,
field name, field number, reserved range, and RPC matches — with exactly three
documented deviations:

1. `option go_package` names this repository's generated Go package. The
   upstream module path is never vendored.
2. The upstream `(buf.validate.*)` field and message rules are omitted, along
   with the `buf/validate/validate.proto` import. This repository vendors only
   a narrow generated projection of those rules for the connect contract, and
   it does not carry the rule types the ingest contract uses. The bounds and
   patterns those options declared are restated in the field comments, and the
   hand-written clients enforce them as validation constants.
3. The `(google.api.http)` annotations are omitted, along with the
   `google/api/annotations.proto` import. This repository has no `buf.lock`
   and no vendored googleapis module, so the dependency is unavailable; the
   SDK speaks the canonical RPC route rather than the REST projection, so the
   annotations carry no client behaviour.

Upstream stays authoritative: reconcile any divergence toward upstream, never
the other way. `make contract-release-check` stays a blocker for release tags.

Because the first vendored copy was a provisional sketch authored before the
upstream contract was released, replacing it was deliberately breaking.
`scripts/check_proto_breaking.sh` carries a one-time exemption for exactly
that transition, scoped by the content hash of the provisional blob so it
expires by itself and every later change is compared normally.

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
