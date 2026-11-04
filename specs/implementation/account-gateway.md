# Account gateway

Each gateway instance owns one account, one ordered journal, one protocol encoder and one connection. The account baseline and physical journal must match the route's risk domain, venue, account, environment and product. At creation, the writer matches the initial writer. Spot and USD-M each instantiate this contract.

## Submit and send

1. Look up the intent key before checking capacity, account health or the clock. An exact retry returns its stored status. A different command under the same key stops the sender.
2. Encode the request, then compare the command digest and target authority to the expected route. The encoder returns bytes without touching a socket.
3. Compute admission on a candidate account. The canonical record contains the command, original evaluation time and full admission image. Bind the command, admission record, account baseline and preceding journal prefix to the recovery closure.
4. Synchronize the recovery dependencies and `Admit` before publishing the candidate account and queuing the send. If the closure cannot be prepared, the operation ends before admission. If an append may have reached durable storage, retain the candidate reservation and stop sending until recovery establishes what happened.
5. Immediately before TLS accepts the first application byte, check current ownership, action permission, lease, writer, strategy generation, configuration, request deadline and admission. Read time at or after the original evaluation. New exposure also requires valid market and account inputs.

## Zero bytes, partial sends and economic facts

A matching send token and a `NoBytes` report with zero accepted bytes establish a zero-byte result. A queue failure with a guarantee of zero application output gives the same evidence. A generic I/O error does not establish how many bytes reached the connection.

Validate an abort on a candidate account. A contradictory confirmation or fill invalidates the abort. After validation, synchronize `Aborted`, then publish the resulting account and release its reservation. If TLS already accepted some bytes and the operation subsequently fails, mark the intent unknown, freeze the account and close the connection. A local flush only reports the state of local buffers.

Synchronize venue events, fees, cash flows, positions and ownership transfers before publishing new account state. Preserve the order of original input observations, including duplicates. The account gateway alone emits `Admit` and `Aborted`. If the account rejects a persisted input, keep it in the journal and freeze the account. A healthy sender can still service authorized cancels and queries.

A local fee-closure decision requires a terminal order whose reported cumulative quantity equals its booked fills. Validate that decision before appending `FeesReconciled`. Publish the fee release after the append is durable. A conflicting cumulative quantity freezes the account and leaves the journal at its preceding position. The underlying authenticated order and fill observations remain in the journal. Fee preflight reads one order and checks the ledger sequence on the reconciliation path. It allocates no account copy.

## Recovery

Validate the baseline, physical chain and sequence. For each admission, verify its original dependencies and preceding history, then replay the account. The recovered gateway starts frozen with an empty send queue and no live connection. Repeating an old intent returns its previous status.

New submissions require verified reconciliation, a synchronized `Reactivated` record and a new connection. A healthy sender under the same owner and writer can restore account eligibility using fresh reconciliation evidence. See the [gateway implementation](../../crates/tickwire-account-gateway/src/lib.rs) and [submission tests](../../crates/tickwire-account-gateway/tests/submission.rs).
