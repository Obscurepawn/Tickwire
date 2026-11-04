# Binance private stream session

One `PrivateClient` owns one product, environment, account, risk domain and credential generation. A pending operation is identified by connection epoch, request number and operation kind. Its digest also binds the entire account scope. Consume a new generation even on a failed connection attempt. Counter or deadline overflow enters `Exhausted`, clears the connections and pending work, and withdraws receiving eligibility. [Session implementation](../../crates/tickwire-binance-private/src/lib.rs)

## Establish and lose a subscription

* **Spot.** Match the exact subscribe reply before entering `Receiving`. With session logon, first verify the returned API key, then subscribe. A signed subscription can authenticate and subscribe in one operation. Subscription ID zero is valid. Incoming events must match the established ID. A status response cannot establish a missing subscription.
* **USD-M.** POST for a listen key, then complete the `/private/ws/<key>` Upgrade before entering `Receiving`. A successful POST or PUT sets the local expiry to **request start plus 60 minutes**. A PUT succeeds only while the old key is valid, the request has not expired, and the returned key matches. A new POST creates a new session epoch even if the key value repeats.
* **Gap or EOF.** Withdraw receiving eligibility before delivering a termination or already-buffered old-session fact. Drain complete observations with their original session context before starting the next connection. Account recovery and risk admission require their own durable evidence.

## First application byte

`prepare` binds immutable request bytes. The signer and encoder share one parameter representation, including canonical `recvWindow` decimals. Spot supports HMAC, RSA or Ed25519 for signed subscription and Ed25519 for session logon. USD-M key management uses its fixed HTTP method/path and API-key header.

Immediately before the first write, sample the clock and verify domain, monotonic order, original source/stamp and local deadline. For signed work, run `Clock::check_signed`, obtain the required control permit, sample again and repeat the time and signature checks. After the final callback, sample once more and verify the local deadline **before** attempting plaintext output. A WouldBlock attempt accepting zero bytes repeats these gates. A permanent clock-domain fault stops the owner. [Clock contract](binance-private-clock-adapter.md)

`NoBytes` requires zero application bytes accepted by TLS. Partial acceptance followed by failure, local flush followed by disconnect, uncertain response, or an early matching reply retains `MayHaveApplied`. Check that the pending write completed before consuming a reply. Only a confirmed protocol response advances authentication or renewal.

## Poll and close

The defaults bound a request to 16 KiB, a message to 1 MiB, early events to 32 messages/1 MiB, and the control queue to eight frames. Bound network calls/bytes, decoded messages and control work per poll. If an I/O failure hides the amount consumed, treat the current I/O budget as spent. A decoder yield reschedules buffered fragments. A pong echoes the ping payload and can pass an unsent application frame. An already-started frame must finish first.

`ControlPermit` covers connection attempts, Upgrade, API weight, Pong and Close. A zero-acceptance retry keeps its charge ID. Explicit remote close first revokes receiving eligibility. Spot unsubscribes the exact subscription and logs out according to its mode. USD-M uses DELETE. If DELETE may have reached the server but its outcome is unknown, retain management ownership and `remote_close_unknown`. Drop only closes the local connection. Diagnostic formatting redacts secrets, signatures and listen keys. Explicit payload accessors expose sensitive wire bytes to their caller.

```sh
cargo test --locked -p tickwire-binance-private
```

The local TLS peers independently verify signatures and exercise reconnects, stale tokens, early replies and ambiguous closure. [Spot lifecycle](../../crates/tickwire-binance-private/tests/spot_lifecycle.rs) · [USD-M lifecycle](../../crates/tickwire-binance-private/tests/usdm_lifecycle.rs) · [Session model](../../tla/execution_models/binance_private_session/PLAN.md)
