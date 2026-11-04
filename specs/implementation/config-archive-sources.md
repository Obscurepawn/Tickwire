# Trust and source bundles in a configuration archive

To audit a configuration plan after a change in credentials, retain the trust configuration and source material that the planner actually used. `Trust` reconstructs the issuer set under which evidence was checked. `Bundle` preserves the artifact bytes and signed envelopes, including inputs that the final candidate does not directly reference. The plan archive supplies their admission time and approval context. [Plan archive](config-plan-archive.md)

## Trust identity

`Trust::canonical_bytes()` returns the exact JSON used by `Trust::new` to compute its digest. SHA-256 covers the domain separator `tickwire.config.evidence-trust.v1\0` followed by the canonical bytes. The fields are `schema=1`, `cluster_id`, `namespace_id`, `revision` and `issuers`. The revision is a decimal string in the range 1 through 2⁶³−1 without leading zeroes. Issuers are sorted by ID, each public key is a 32-byte array, and the unique kinds follow the existing `Kind` order.

`Trust::decode(bytes, expected_digest)` accepts at most 16 KiB and eight nesting levels. It rebuilds the value through the constructor and checks both the full canonical bytes and digest. It rejects unknown or duplicate keys, invalid scope or keys, unfamiliar kinds, noncanonical ordering, numbers, escapes or whitespace. A historical verifier may evaluate evidence at its recorded, verified admission time. A new deployment calls `Trust::verify` or `VerifiedEvidence::revalidate` with the currently approved trust and current time. Expiry, revocation or a start time in the future can change that decision. [Trust implementation](../../crates/tickwire-config-service/src/evidence.rs)

## Source bundle encoding

`Bundle::encode(limits)` writes the eight-byte magic `TWDSRC01`, a four-byte little-endian canonical header length, the canonical JSON header, then the raw objects in full artifact-reference order. The header includes schema, each reference and length, every envelope, the retained root and credential roots. Envelopes sort by `(Kind, reference)` and credential roots by reference. Object contents are copied byte for byte.

The encoder checks object count, individual and total sizes, header length, total bundle size and integer overflow. It bounds header serialization and checks the final size before allocating the body. Decode validates framing, hashes, uniqueness and references. Decode–encode–decode must preserve all artifact and envelope contents and root sets. A different original ordering or JSON representation can produce a new canonical bundle digest. `digest()` and `stored_bytes()` on the original instance continue to identify the original input. Persist both identities when preserving provenance.

The archive can divide a bundle into chunks of at most 16 MiB. Reassembly verifies order, lengths and digests before decoding. An envelope with a damaged signature can survive archival as a record of what was submitted. Qualification still performs signature and freshness checks. Tests cover a real Ed25519 envelope after trust reconstruction, exact budget boundaries, altered contents and expired or revoked current trust. [Bundle codec](../../crates/tickwire-config-service/src/source_bundle.rs) · [Archive acceptance](../evidence/20261028/config-archive.json)
