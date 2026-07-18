# Contributing

This is a public SDK repository. Keep contributions free of secrets, private repository references, and internal deployment details.

## Development

```sh
flox activate -- make install
flox activate -- make check
flox activate -- make audit
```

Consumers do not need Flox. Flox is the sole contributor and CI toolchain
bootstrap; do not add language setup actions or host-installed quality tools.

## Versions and Releases

All language SDKs use the repository-root `VERSION` and ship together from one
plain `vX.Y.Z` tag. Never create language-prefixed tags such as `go/vX.Y.Z`.
Prepare a future lockstep version and verify it with:

```sh
flox activate -- make version-set VERSION=X.Y.Z
flox activate -- make version-check
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
flox activate -- make proto-bindings proto-descriptor generated-check
```

The vendored protos are deliberately bounded initial client subsets, not the
entire administration or internal APIs. Every shared message, field, enum, RPC
signature, and validation constraint must remain identical to its canonical
service contract, including additive response fields used by an included RPC.

## Public Surface Review

Public docs, examples, package metadata, vendored proto comments, and tests must not include private repository references, internal deployment topology, real private hostnames, secrets, or private customer details. Public Medallion service contract names such as `medallion-connect`, `medallion-ontology`, and `medallion-storage` are allowed when they describe the SDK API.

Run the public-surface check before changing those files:

```sh
flox activate -- make check-public
```

## Checks

Before opening a change, run:

```sh
flox activate -- make format
flox activate -- make check
flox activate -- make audit
```

## Release Checklist

Before creating the one root `vX.Y.Z` release tag:

1. Run `flox activate -- make version-set VERSION=X.Y.Z` and review every
   version mirror.
2. Regenerate protocol bindings and descriptors with
   `flox activate -- make proto-bindings proto-descriptor`.
3. Run `flox activate -- make check`.
4. Run `flox activate -- make git-install-check`; this installs the exact tree
   by both full commit SHA and the synthetic root release tag in clean Go,
   Python, and TypeScript consumers.
5. Run `flox activate -- make audit` to scan the locked Node, Go, and Python
   dependencies and the source tree.
6. Confirm the tree contains no secrets or unrelated generated output, commit
   it, and push it.
7. Create exactly one annotated root tag, `vX.Y.Z`, on that reviewed commit.
   Do not create language-prefixed tags and do not publish to npm, PyPI,
   crates.io, or another language registry.
