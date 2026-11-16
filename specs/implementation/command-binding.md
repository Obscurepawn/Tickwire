# Stateless Command Binding and Self-Describing Client Order IDs

## Scope

This specification governs how strategy intents (`IntentRecord`, `1,024` bytes inline in shared memory) bind deterministically to venue wire orders, in-memory `OrderManager` / `AccountGateway` slots, and cold-restart Exchange REST open-order snapshots without any local write-ahead log, blob store, or recovery manifest.

## Canonical Command Identity and Wire Encoding

1. **Inline SHM Intent Transport:**
   - Strategy workers publish fixed 1,024-byte `IntentRecord`s (`tickwire_ipc::IntentRecord`) into their private SPSC intent ring (`IntentReader` in ORS). Oversize batch/RFQ payloads, when present, reside strictly in the in-memory SHM Arena (`payload_kind == 2`), never on disk.
   - Every intent carries `strat_id: u8` (`0..=254`, or `255` exclusively on administrative `CancelAll` / `CancelAllRfqQuotes`), `account_ref: u8` (`0` for default first-authorized account, or `1..=255` explicit), `msg_seqnum: u64`, `order_seqnum: u64`, `orig_order_seqnum: u64`, `tick_mono_ns: u64`, `symbol_id: u32`, integer `price_ticks: i64`, integer `qty_lots: i64`, `time_in_force: u8`, and `intent_flags: u8` (`BYPASS_RMS`, `DRY_RUN`, `CHECK_STP`, `REDUCE_ONLY`, `POST_ONLY`, `QUOTE_AMOUNT`, `PRICE_PROTECT`, `CLOSE_POSITION`).
2. **32-Character `SelfDescribingClientId` (`tw1...`):**
   - Outbound venue `clientOrderId` / `clOrdId` / `orderLinkId` tokens are 32-character lowercase alphanumeric `[0-9a-z]` identifiers formatted by [`tickwire_venue::SelfDescribingClientId`](../../crates/tickwire-venue/src/client_id.rs) (re-exported by `tickwire_account`):
     * Fixed version prefix `"tw1"`.
     * Compact 29-character lowercase base-36 encoding of `(logical_strategy_id, leg, epoch, intent_id)` (where ORS `strat_id` `0..=254` maps via `from_ors_order` / `ors_strat_id` as `logical_strategy_id = strat_id + 1`) plus a 30-bit domain-keyed SHA-256 MAC tag derived from `ClientIdDomain` (`DomainConfig` binding `risk_domain_id`, `venue`, `environment`, `account`, and `product`).
   - Because `SelfDescribingClientId` embeds `(strat_id, leg, epoch, order_seqnum)` and authenticates `(account, venue, product)` provenance via its domain MAC, cold ORS startup reconstructs strategy ownership, open-order holds, and per-strategy `next_order_seqnum` floors directly from Exchange REST `OpenOrders` responses—eliminating local command journals.

## Binding Rules in `AccountGateway` and `OrderManager`

Implemented across [`crates/tickwire-account-gateway/src/binding.rs`](../../crates/tickwire-account-gateway/src/binding.rs), [`crates/tickwire-account/src/order_manager.rs`](../../crates/tickwire-account/src/order_manager.rs), and [`crates/tickwire-account/src/dop_state.rs`](../../crates/tickwire-account/src/dop_state.rs):

1. **Idempotent Client Command & Order-Slot Binding:**
   - In `AccountGateway`, re-presenting an already-admitted `IntentKey { epoch, intent_id }` with an identical `CanonicalCommand::digest()` returns the existing in-memory entry idempotently, whereas presenting a conflicting digest under the same `IntentKey` fails closed with `tickwire_account_gateway::Error::Conflict` before touching any venue socket.
   - In `OrderManager<CHECK_STP>`, `(strat_id, order_seqnum)` maps in `O(1)` to a preallocated per-strategy power-of-two `OrderSlot` (`slot_idx = base + (seqnum & (cap - 1))`). Non-monotonic `msg_seqnum` or `order_seqnum < next_order_seqnum` refuses in `DopState` with `RefusalReason::UnexpectedMsgSeqnum`, while an occupied unretired slot refuses with `RefusalReason::AllocationError`.
2. **Cancel and Modify Target Resolution:**
   - `Cancel` and `Modify` / `AmendKeepPriority` / `CancelReplace` resolve their target order in `O(1)` via `orig_order_seqnum` (verifying `slot.strat_id == req.strat_id` and `slot.account_ref == resolved_account_ref`) or via `orig_client_id` / `venue_order_id`.
   - If a `Cancel` arrives while `NewOrder` is still awaiting venue acknowledgement, `OrderManager` increments `pending_cxl` under `LockedOrder` so `RESP` triggers immediate cancellation upon `on_ack`.
3. **`dry_run` Pre-Trade Probe Path:**
   - When `intent_flags::DRY_RUN` is set on `NewOrder`, the ORS runs the full cold-free admission pipeline—stale-order gate, runtime policy, `RmsEngine` check (without advancing live rolling-window counters when dry-run), atomic BBO `OrsStpBook` check, and `OrderManager` scratch-slot reservation—then immediately releases the scratch slot and skips route selection, rate-limiter spend, and socket write.

## Cold Restart & Sequence Continuity (`DopLogin`)

1. At ORS startup, `bootstrap_snapshots` fetches `Account + OpenOrders` per enabled `AccountSession`. Every open order carrying a valid `tw1...` `SelfDescribingClientId` calls `DopState::observe_recovered_order(strat_id, recovered_order_seqnum)`, advancing `next_order_seqnum` to at least `recovered_order_seqnum + 1`.
2. When strategy `strat_id` connects over the Unix-domain DOP control socket and sends `DopLogin { next_source_seqnum }`, `DopState::login` computes:
   `effective_next_seqnum = max(login.next_source_seqnum, next_order_seqnum, 1)`
   resets `last_recv_seqnum = 0` for the fresh SHM intent session, and replies with `DopEcho { next_source_order_seqnum: effective_next_seqnum, ... }` followed by `OrsSessions`.
3. Every subsequent `IntentRecord` from `strat_id` must present strictly contiguous `msg_seqnum` (establishing the baseline on the first request after login, then `+1` per message) and `order_seqnum >= next_order_seqnum` on new orders, preventing `SelfDescribingClientId` collision after strategy or ORS restarts.
