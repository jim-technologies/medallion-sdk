# Historical Connect v1 proof

This archive preserves the sanitized, checksummed export and exact projection
for the removed `medallion.connect.v1` CDC/audit API. It is historical evidence,
not an active generation input or a supported SDK runtime surface.

`make contract-check` still verifies the bundle checksum, artifact inventory,
producer facts, descriptor closure and the shared transport error-policy
projection. The producer's attestation is `unreleased_candidate`; moving the
files does not change that publication blocker.

This proof does not attest `medallion.ingest.v1`. Both the 0.4 candidate and the
0.5 candidate remain unpublishable until independently issued, immutable ingest
contract evidence is reviewed and its release verifier is implemented. No local
SDK-generated statement can substitute for producer evidence.
