# Stateless Command Binding and Self-Describing Client Order IDs

## Scope

This specification governs how strategy intents (`IntentRecord`, `1,024` bytes inline in shared memory) bind deterministically to venue wire orders, in-memory `OrderManager` / `AccountGateway` slots, and cold-restart Exchange REST open-order snapshots without any local write-ahead log, blob store, or recovery manifest.

## Canonical Command Identity and Wire Encoding

1. **Inline SHM Intent Transport:**
   - Strategy workers publish fixed 1,024-byte `IntentRecord`s (`tickwire_ipc::IntentRecord`) into their private SPSC intent ring (`IntentReader` in ORS). Oversize batch/RFQ payloads, when present, reside strictly in the in-memory SHM Arena (`payload_kind == 2`), never on disk.
   - Every intent carries `strat_id: u8` (`0..=254`, or `255` exclusively on administrative `CancelAll` / `CancelAllRfqQuotes`), `account_ref: u8` (`0` for default first-authorized account, or `1..=255` explicit), `msg_seqnum: u64`, `order_seqnum: u64`, `orig_order_seqnum: u64`, `tick_mono_ns: u64`, `symbol_id: u32`, integer `price_ticks: i64`, integer `qty_lots: i64`, `time_in_force: u8`, and `intent_flags: u8` (`BYPASS_RMS`, `DRY_RUN`, `CHECK_STP`, `REDUCE_ONLY`, `POST_ONLY`, `QUOTE_AMOUNT`, `PRICE_PROTECT`, `CLOSE_POSITION`).
2. **32-Character `SelfDescribingClientId` (`tw1...`):**
   - Outbound venue `clientOrderId` / `clOrdId` tokens are 32-character lowercase alphanumeric `[0-9a-z]` identifiers formatted by `tickwire_account::SelfDescribingClientId`:
     * Fixed version prefix `"tw1"`.
     * Compact base-36 encoding of `(account_ref, strat_id, epoch, order_seqnum)` plus a domain-keyed cryptographic MAC tag derived from `DomainConfig` (`risk_domain`, venue, account UID).
   - Because `SelfDescribingClientId` embeds `(account_ref, strat_id, order_seqnum)` and authenticates provenance via its domain MAC, cold ORS startup reconstructs strategy ownership, open-order holds, and per-strategy `next_order_seqnum` floors directly from Exchange REST `OpenOrders` responses—eliminating local command journals.

## Binding Rules in `AccountGateway` and `OrderManager`

Implemented across [`crates/tickwire-account-gateway/src/binding.rs`](../../crates/tickwire-account-gateway/src/binding.rs) and `tickwire_account::OrderManager`:

1. **Idempotent Client Command Binding:**
   - Within a live ORS process epoch, `(strat_id, order_seqnum)` maps deterministically to a bounded pre-allocated `OrderSlot` (`seqnum % ring_capacity`).
   - Re-presenting an already-accepted `CanonicalCommand` with identical economic parameters (`account_ref`, `symbol_id`, `side`, `order_type`, `time_in_force`, `price_ticks`, `qty_lots`, `client_order_id`) returns the existing in-memory binding; presenting a conflicting payload under the same command key fails closed with `GatewayError::CommandConflict` / `RefusalReason::DuplicateOrderSeqnum` before touching any venue socket.
2. **Cancel and Modify Target Resolution:**
   - `Cancel` and `Modify` / `AmendKeepPriority` / `CancelReplace` resolve their target order in `O(1)` via `orig_order_seqnum` (verifying `slot.strat_id == req.strat_id` and `slot.account_ref == resolved_account_ref`) or via `orig_client_id` / `venue_order_id`.
   - If a `Cancel` arrives while `NewOrder` is still `PendingAck` (no venue `orderId` yet observed, and venue requires `orderId` or serialized cancel), `OrderManager` latches `pending_cancel = true` under the per-order spinlock so `RESP` triggers immediate cancellation upon `on_ack`.
3. **`dry_run` Pre-Trade Probe Path:**
   - When `intent_flags::DRY_RUN` is set on `NewOrder`, the ORS runs the full cold-free admission pipeline—stale-order gate, runtime policy, `RmsEngine` check (without advancing live rolling-window counters when dry-run), `StpBook` check, and `OrderManager` scratch-slot reservation—then immediately releases the scratch slot and skips route selection, rate-limiter spend, and socket write (`should_send_to_venue == false`).

## Cold Restart & Sequence Continuity (`DopLogin`)

1. At ORS startup, `bootstrap_snapshots` fetches `Account + OpenOrders` per enabled `AccountSession`. Every open order carrying a valid `tw1...` `SelfDescribingClientId` updates `OrderManager`'s per-strategy `max_recovered_seqnum[strat_id]`.
2. When strategy `strat_id` connects over the Unix-domain DOP control socket and sends `DopLogin { next_source_seqnum }`, the ORS computes:
   `effective_next_seqnum = max(login.next_source_seqnum, max_recovered_seqnum[strat_id] + 1)`
   resets `last_recv_msg_seqnum[strat_id] = 0` for the fresh SHM intent session, and replies with `DopEcho { next_source_order_seqnum: effective_next_seqnum, ... }` followed by `OrsSessions`.
3. Every subsequent `IntentRecord` from `strat_id` must present strictly increasing `msg_seqnum` and `order_seqnum >= effective_next_seqnum`, preventing accidental `SelfDescribingClientId` collision after strategy or ORS restarts.
