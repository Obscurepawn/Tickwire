# Counterexamples and review log

Zero TLC safety counterexamples occurred across all four configurations. During model review, historical `Quiesce` ACK retry quantification was widened so `closedApp` can lag a reader's subsequent post-quiesce `applied` advance, matching valid `Reader` behavior after `Quiesced` / `Detached`.

Prior source-review fixes for idempotent duplicate `JOIN_READY` responses and full historical `Quiesce` ACK comparison are documented in the [JOIN protocol specification](../../../../specs/implementation/join.md).
