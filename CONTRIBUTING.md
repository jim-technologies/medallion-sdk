# Contributing

This is a public SDK repository. Keep contributions free of secrets, private repository references, and internal deployment details.

## Development

```sh
flox activate
make install
make check
```

Consumers do not need Flox. Flox is only contributor tooling.

## Versions and Releases

All language SDKs use the repository-root `VERSION` and ship together from one
plain `vX.Y.Z` tag. Never create language-prefixed tags such as `go/vX.Y.Z`.
Prepare a future lockstep version and verify it with:

```sh
make version-set VERSION=X.Y.Z
make version-check
```

The synchronizer updates the TypeScript and Python package metadata and the
Python lockfile together. Go derives the same version from the root Git tag.
Before a v2 or later release, migrate the Go module and imports to the required
`/vN` semantic import path and update the version guard in the same change.

## Scope

The TypeScript implementation supports the broad public client surface:

- audit events
- CDC events
- datasource registration
- audit trail reads
- ontology queries
- action planning/execution
- storage upload

Keep ergonomic wrappers over the descriptor-backed low-level clients, and keep route compatibility tests in sync with public backend contracts.

Go and Python currently cover the Connect-backed server integration path. Their low-level Connect types should stay generated from the vendored public protobuf contract.

When vendored proto contracts change, regenerate the TypeScript descriptors:

```sh
pnpm proto:descriptor
```

## Public Surface Review

Public docs, examples, package metadata, vendored proto comments, and tests must not include private repository references, internal deployment topology, real private hostnames, secrets, or private customer details. Public Medallion service contract names such as `medallion-connect`, `medallion-ontology`, and `medallion-storage` are allowed when they describe the SDK API.

Run the public-surface check before changing those files:

```sh
make check-public
```

## Checks

Before opening a change, run:

```sh
make check
```
