# Migrating to 0.4.0: CDC and audit publishing removed

0.4.0 removes the deprecated `medallion.connect.v1` CDC/audit surface from the
TypeScript, Go and Python SDKs. It is not tagged yet; the newest install tag is
`v0.3.1`, which still carries that surface. 0.4.0 also replaces the 0.3.1
datasets sketch with the released tables contract; the
[changelog](../CHANGELOG.md) lists every change since 0.3.1.

| Removed | Replacement |
| --- | --- |
| `connect`, `cdc` and `audit` clients and their publish/list helpers | Tables and the seven `medallion.ingest.v1` RPCs for new tabular integrations |
| Connector defaults in SDK constructors | Immutable workspace identity and the declared ingest request |
| CDC/audit types and generated Connect subset | Your own declared table rows, or a separately qualified connector integration |

This is not an automatic event migration. Existing CDC/audit consumers must
keep their current pinned SDK until their application's semantics,
authorization and delivery have been migrated and tested. The change does not
disable the server's CDC/audit RPCs, rewrite event ledgers or delete data.
Shared downstream CDC uses `invariant.cdc.v2`; arbitrary JSON must not be
labelled replayable row state without explicit semantics.

Tables, ingest, workspace/auth transport, retries, cancellation, errors and
Temporaless workflow factories retain their behavior. Table append retries keep
the exact batch and idempotency key. They do not inherit event-level
deduplication by changing a constructor name.

Publication waits on one blocker: an independent producer-issued immutable
**ingest** attestation, and a reviewed release verifier for it. The archived
Connect proof cannot attest ingest, even if its historical release status
changed. `make breaking-check` exempts exactly the deleted Connect proto files
by their pre-removal content hashes, and `make ingest-breaking-check` compares
the retained ingest contract with the fixed pre-removal commit and `main`;
neither check authorizes a release.

Before switching a consumer, retain its previous immutable pin and lockfile.
Reverting that pin restores its old SDK API; no server or data rollback is
implied.
