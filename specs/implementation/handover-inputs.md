# Account inputs at strategy handover

The `TWINP001` object in a handover closure contains the original account callback bytes. Its `InputSeal` provides an aggregate digest. Recovering the account also requires its baseline and every intervening record.

1. Verify lengths and digests in `HandoverSeal` against the actual checkpoint, inputs and account seal. The checkpoint binds target instance, generation, logical strategy, report cut, ledger and order digest.
2. Start at sequence 1 with a zero predecessor. Decode each kind 2 record as `TWAI0001` and verify wrapper length, tag and sequence. Exactly one baseline precedes all account records.
3. Decode canonical records against that baseline in sequence through the report cut. Reconstruct the seal preimage and compare its bytes to the retained object.
4. Recover the account. Compare the resulting ledger and strategy order digest with the declared cut.
5. The runtime compares the actual baseline and prior WAL byte for byte. The SDK verifies the target and report cut in authenticated READY. A replica verifies source stream, handover record and preceding record digests. Each owner enforces its object budget. The runtime includes the full input object.

Account records use the journal's `MAX_FACTS + MAX_ORDERS` bound. Other input types retain their relative order for replay. Regression tests remove a baseline or an account record and recompute the outer hashes. Both closures must fail verification.
