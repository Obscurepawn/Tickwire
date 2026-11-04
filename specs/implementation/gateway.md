# Durable command sending

`JournaledSender` owns the business journal, authorization service, encoder and writable connection. Its caller submits a durable order request and a canonical command. Validate that both name the same risk domain, venue, product, environment, account, intent, original execution generation, strategy generation, configuration revision, deadline, recovery object and digest. An identical retry returns the recorded status. Conflicting content for the same intent freezes the sender. [Sender implementation](../../crates/tickwire-gateway/src/lib.rs)

## Submit and send

1. Reject a frozen sender, an occupied send slot or exhausted `max_intents`. Prepare the entire immutable message and check its digest and route. An encoding failure produces neither an intent nor network output.
2. Obtain authorization and persist the command’s recovery dependencies. Verify the closure proof, then commit and synchronize the business intent. Only after a successful commit may the message enter the connection queue.
3. Drive connection setup and existing TLS output. Immediately **before** the first plaintext write, ask the current authorization service for the execution epoch, time, permissions and account gate. The transport checks expiry again after the callback. If zero application bytes were accepted, recheck on every attempt. After any acceptance, keep the original bytes and offset.
4. If the transport proves zero accepted bytes, synchronize the abort record **before** marking the intent `Aborted`. An ambiguous failure after partial acceptance, lost connection or failed journal synchronization freezes the sender and leaves unresolved intents `Unknown`. Local completion (`AwaitingResponse`) keeps the risk reservation. A venue report is still required.

On recovery, verify the physical and business logs and resolve each complete original command closure. Only a durable abort restores `Aborted`. Other unresolved intents become `Unknown`. The recovered sender has no connection and remains frozen. A new writer epoch may recover orders from an older epoch without rewriting their identity or resending them.

## Scope and verification

The authorization service supplies strategy permissions and account budget checks. A scalar `OrderRequest` is insufficient evidence of multi-asset coverage. Private fills, fees and balance updates require their own identity correlation and durable accounting. The Binance REST fixture uses an independent TLS peer to check signed Spot and USD-M requests. Bybit/OKX spot signing and execution have a separate adapter and verification path. [Spot execution](spot-execution.md) · [Lifecycle tests](../../crates/tickwire-gateway/tests/lifecycle.rs) · [Signed REST tests](../../crates/tickwire-gateway/tests/binance_rest.rs)

```sh
cargo test --locked -p tickwire-gateway
```
