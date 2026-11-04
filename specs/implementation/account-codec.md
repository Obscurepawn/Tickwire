# Account baseline and recovery-record codec

The account codec assigns one canonical byte representation to a baseline and each ordered account record. Both carry SHA-256 over their entire top-level encoding. [Implementation](../../crates/tickwire-account-codec/src/lib.rs)

| Object | Top-level CBOR array | Maximum |
|---|---|---:|
| Baseline | `["tickwire.account-baseline", 1, Snapshot, Catalog]` | 2 MiB |
| Record | `["tickwire.account-record", 1, baseline_sha256, sequence, RecoveryRecord]` | 60 KiB |

The baseline digest and sequence in a record must be non-zero. The journal establishes that the first record is 1, that positions are consecutive and that they match the physical log. Fixed-size structs use fixed-arity arrays. An option is `[]` or `[value]`, and a boolean is integer 0 or 1. Integers use shortest-form CBOR, including tags 2/3 for bignums and `-1 - magnitude` for negatives down to `i128::MIN`. Floating point, indefinite lengths, unknown tags and trailing contents fail decoding. `Fixed` encodes its original `[atoms, scale]`, preserving its unit. Venue decimals retain their canonical `[coefficient, exponent]`. Embedded commands and events must pass their own canonical decoders. [CBOR](../../crates/tickwire-account-codec/src/cbor.rs) · [Record codec](../../crates/tickwire-account-codec/src/codec.rs)

## Baselines and admission images

The constructor sorts assets by wallet and asset, positions by instrument and position side, wallet availability by wallet, and catalog entries by instrument. Attributions and strategy limits sort by logical strategy. Duplicate keys fail. Risk tiers preserve their supplied order and must pass the risk rules. A valid baseline must pass `Account::from_snapshot`. The reader checks capacities before vector allocation and verifies byte-for-byte canonical re-encoding. Limits include 64 assets, 64 instruments, 128 positions, 1,024 attributions per asset or position, 1,024 strategy limits per instrument and 128 risk tiers per instrument. [Baseline checks](../../crates/tickwire-account-codec/src/validate.rs) · [Baseline tests](../../crates/tickwire-account-codec/tests/baseline.rs)

An admission image contains the full command digest, original intent key, account epoch, ledger sequence, revision set, deadline, resource and capital reservations, child order keys and increase-risk flag. The codec checks identity and command classification. Cancel, query, parent cancellation and supported keep-priority reductions follow the non-increasing branch. New orders cannot downgrade themselves by changing the image flag. Each admission may contain at most three orders and 64 resource or capital entries per collection. Vector order matters on replay. Duplicate asset, wallet or child keys fail. An image is recorded data. Replaying an admission requires the reducer to recompute its full image at the original evaluation time. [Image validation](../../crates/tickwire-account-codec/src/validate.rs) · [Non-increasing commands](../../crates/tickwire-account-codec/tests/nonincreasing.rs)

## Record tags and evidence

| Tag | Record | Tag | Record |
|---:|---|---:|---|
| 1 | Admit | 9 | IngressEvent |
| 2 | VenueEvent | 10 | Reactivated |
| 3 | Aborted | 11 | StrategyHandover |
| 4 | FeesReconciled | 12 | IngressAccountInput |
| 5 | UnattributedCashFlow | 13 | IncomeBaseline |
| 6 | ExternalPosition | 14 | AccountInputsReconciled |
| 7 | TransferAsset | 15 | IncomeObservationsAssociated |
| 8 | TransferPosition | 16 | UncreatedAlgoClosed |

Tag 9 preserves the non-zero 16-byte input stream, non-zero sequence and full canonical event. Tag 10 binds the account, source and writer epochs, preceding state, input prefix, snapshot coverage, clock validity, sending connection and ordered wallet availability. Tag 14 binds scope, account epoch, preceding ledger sequence, baseline and history digests, input stream and processed sequence, state digest and coverage digest. The enclosing baseline must match. The owner verifies actual input data, physical writer and historical prefix before publishing these records. Earlier tags retain their original bytes. Older readers reject unfamiliar tags. [Wire definitions](../../crates/tickwire-account-codec/src/codec.rs) · [Compatibility tests](../../crates/tickwire-account-codec/tests/legacy_encoding.rs)

Core validation includes independent byte and hash vectors, truncation and malformed-length cases, and Spot/USD-M file-backed replay. Decoding verifies byte shape and binding. It does not establish who produced the record, whether all prior records survived or whether an account may send. [Vector tests](../../crates/tickwire-account-codec/tests/vectors.rs) · [File replay](../../crates/tickwire-account-codec/tests/file_replay.rs)
