# L0 abstractions and scope

Transcribes `Session::admit`, `ExecutionTicket` completion, and `MockRequestStore` (`fail_next` fault injection) faithfully across serialized synchronous calls: `ArmFailure` occurs between calls, and `admit`'s initial lookup consumes `fail_next` without concurrent writers interleaving between lookup and insert.

`keys` models bounded authenticated request keys, and `commands` models collision-free command digests where command `c` carries `expected_state_version = c - 1` against bounded `current_version`. `BadCompletion` covers ticket correlation rejection without mutating `MockRequestStore` or consuming `fail_next`, while valid completion requires an issued ticket's exact key and digest.

`entries` stores recorded digests and terminal responses (`0` = absent); ghost witnesses `issued`, `registered`, `firstResult`, and `observedRetry` verify at-most-once `Execute` ticket issuance per key, non-eviction of registered entries, immutability of terminal results, and version-independent `Pending` / `Completed` replay on matching-digest retries across total capacities `1..3` and bounded pending limits.
