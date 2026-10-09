# 0.4 API to 0.5 source candidate

This branch prepares a breaking SDK change; 0.5.0 is not published. The 0.4.0
source candidate is also untagged. The latest actual install tag remains 0.3.1.
Owner review is required before a breaking public release.

| Removed | Replacement |
| --- | --- |
| `connect`, `cdc` and `audit` clients and their publish/list helpers | Tables and the seven `medallion.ingest.v1` RPCs for new tabular integrations |
| Connector defaults in SDK constructors | Immutable workspace identity and the declared ingest request |
| CDC/audit types and generated Connect subset | Your own declared table rows, or a separately qualified connector integration |

This is not an automatic event migration. Existing CDC/audit consumers must
keep their current pinned SDK until their application's semantics, authorization
and delivery have been migrated and tested. The change does not disable the
server's legacy connector-ingest RPCs, rewrite event ledgers or delete data.
Shared downstream CDC uses `invariant.cdc.v2`; arbitrary JSON must not be
labelled replayable row state without explicit semantics.

Tables, ingest, workspace/auth transport, retries, cancellation, errors and
Temporaless workflow factories retain their behavior. Table append retries keep
the exact batch and idempotency key. They do not inherit event-level deduplication
by changing a constructor name.

Two release blockers remain: an independent producer-issued immutable **ingest**
attestation is absent, and the unchanged full Buf breaking check reports the
intentional Connect closure removal. Archived Connect proof cannot attest
ingest, even if its historical release status is changed. Ingest compatibility
is checked separately against a fixed candidate and real main; those checks do
not authorize a release or waive the breaking review.

The review branch is reversible in Git from base `6a83624`. Before switching a
consumer, retain its previous immutable pin and lockfile. Reverting that pin
restores its old SDK API; no server or data rollback is implied.
