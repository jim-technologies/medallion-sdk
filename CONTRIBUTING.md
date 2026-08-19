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

Every language implementation supports the same bounded customer-ingestion
surface:

- audit events
- CDC events
- audit and CDC readback for verification and reconciliation

The generated Connect service, low-level clients, and ergonomic wrappers must
contain exactly `PublishCdcEvents`, `PublishAuditEvents`, `ListCdcEvents`, and
`ListAuditEvents`. Do not add a generic RPC dispatcher, connector provisioning,
platform administration, first-party application APIs, actions, secrets, or
storage APIs to this repository.

When vendored proto contracts change, regenerate the TypeScript descriptors:

```sh
flox activate -- make proto-bindings proto-descriptor generated-check
```

The local generated proto is the exact reachable closure of those four RPCs,
not the entire administration or internal API. Every included message, field,
enum, RPC signature, and validation constraint must remain identical to the
reviewed, sanitized external-ingestion contract export.

## Public Surface Review

Public docs, examples, package metadata, vendored proto comments, and tests must
not include private repository references, internal deployment topology, real
private hostnames, secrets, or private customer details.

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

The TypeScript CommonJS build generates `dist/THIRD_PARTY_LICENSES.txt` from
the exact package inputs in the esbuild metafile. The artifact check compares
that file to a fresh inventory and rejects missing, extra, or stale bundled
package entries. When JavaScript dependencies or bundling rules change, review
the generated inventory as part of `flox activate -- make artifact-check`; do
not maintain its package list by hand.

## Release Checklist

Before creating the one root `vX.Y.Z` release tag:

1. Run `flox activate -- make version-set VERSION=X.Y.Z` and review every
   version mirror.
2. Regenerate protocol bindings and descriptors with
   `flox activate -- make proto-bindings proto-descriptor`.
3. Sync a reviewed, immutable external-ingestion contract export and run
   `flox activate -- make contract-release-check`; a mutable or unattested
   export is never acceptable on a release tag.
4. Run `flox activate -- make check`.
5. Run `flox activate -- make git-install-check`; this installs the exact tree
   by both full commit SHA and the synthetic root release tag in clean Go,
   Python, and TypeScript consumers.
6. Run `flox activate -- make audit` to scan the locked Node, Go, and Python
   dependencies and the source tree.
7. Confirm the tree contains no secrets or unrelated generated output, commit
   it, and push it.
8. Create exactly one annotated root tag, `vX.Y.Z`, on that reviewed commit.
   Do not create language-prefixed tags and do not publish to npm, PyPI,
   crates.io, or another language registry.
