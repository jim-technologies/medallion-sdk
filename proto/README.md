# External-ingestion contract

This directory vendors the checksummed `external_ingestion_sdk_v1` contract for the
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
