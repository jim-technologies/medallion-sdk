# Contributing

This is a public SDK repository. Keep contributions free of secrets, private repository references, and internal deployment details.

## Development

```sh
flox activate -- make install
flox activate -- make validate
flox activate -- make audit
```

Consumers do not need Flox. Flox is the sole contributor and CI toolchain
bootstrap; do not add language setup actions or host-installed quality tools.
CI runs exactly `flox activate -- make validate`, and a weekly secretless
workflow runs `flox activate -- make audit`; every toolchain comes from
`.flox/env/manifest.toml`. On a release tag the same gate additionally
requires the immutable contract attestation and the annotated-tag checks.

The deployed smoke test (`make test-deployed`) is never run by CI. Invoke it
from a maintainer machine with caller-supplied `MEDALLION_SMOKE_*` environment
variables; `scripts/check_smoke_env.sh` verifies the set is complete.

## Versions and Releases

All language SDKs use the repository-root `VERSION` and ship together from one
plain `vX.Y.Z` tag. Never create language-prefixed tags such as `go/vX.Y.Z`.
Prepare a future lockstep version and verify it with:

```sh
flox activate -- make version-set VERSION=X.Y.Z
flox activate -- make version-check
```

`make release` is a fail-closed stub: it verifies a clean, pushed,
version-consistent tree and then refuses, because publishing to npm, PyPI, or
a public Go module ecosystem is a pending product decision. Distribution stays
git-install based per the Release Checklist below.

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

When vendored proto contracts change, regenerate the language bindings and
TypeScript descriptors:

```sh
flox activate -- make generate generated-check
```

`make validate` also runs breaking-change detection against the `main`
baseline (`make breaking-check`). The published packages are version-suffixed
(`medallion.connect.v1`); a breaking major ships as a new package, never an
in-place edit of an existing one.

The local generated proto is the exact reachable closure of those four RPCs,
not the entire administration or internal API. Every included message, field,
enum, RPC signature, and validation constraint must remain identical to the
reviewed, sanitized external-ingestion contract export.

## Public Surface Review

Public docs, examples, package metadata, vendored proto comments, and tests must
not include private repository references, internal deployment topology, real
private hostnames, secrets, or private customer details.

`scripts/public-surface-check` enforces that. It scans three streams: the
content of every tracked file, every tracked path, and the commit messages a
push would publish. A finding against a commit message means the message must
be rewritten before the branch is pushed; fixing the file is not enough.

Run the public-surface guard before changing those files:

```sh
flox activate -- make public-surface
```

Findings are redacted so a public CI log never republishes what the guard
caught; run `PUBLIC_SURFACE_SHOW_MATCH=1 scripts/public-surface-check` locally
to see them in full, and `--full-history` to audit every commit rather than
the unpushed range.

A finding that is genuinely public gets one line in `.public-surface-allow`
(`category | path-glob | reason | pattern`), and the reason is mandatory. The
guard re-runs every category probe after loading those rules, so a rule broad
enough to switch a category off is rejected rather than obeyed. Denials this
repository adds on top of the fleet baseline - the scope names and backend
names retired from the v1 contract - live in `.public-surface-deny` in the
same format. `scripts/public-surface-check-test` runs alongside the guard and
fails the gate if the guard itself stops working.

## Checks

Before opening a change, run:

```sh
flox activate -- make fmt
flox activate -- make validate
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
   `flox activate -- make generate`.
3. Sync a reviewed, immutable external-ingestion contract export and run
   `flox activate -- make contract-release-check`; a mutable or unattested
   export is never acceptable on a release tag.
4. Run `flox activate -- make validate`.
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
