# Output (`L0_OrsSessionRouting`)

PASS. Exhaustive explicit-state verification across [L0_OrsSessionRouting.tla](02_models/L0_OrsSessionRouting.tla) (`21,488` distinct states in `L0_OrsSessionRouting.cfg` and `73,856` distinct states in `L0_OrsSessionRouting_restart.cfg`, `0 states left on queue`, 100% nonzero coverage across all 14 `Next` actions) proves:

1. **`AuthorizedSlotAttributionInv` & `AccountHoldConservationInv`**: `account_ref = 0` routes strictly to the strategy's first authorized `AccountSession`; unregistered (`INVALID_ACCOUNT_ID`) or cross-account unauthorized (`REJECT_ROUTING_ID_MISMATCH`) intents never allocate an order slot, mutate sibling `AccountSession` holds, or advance `next_order_seqnum`.
2. **`MonotonicOrderSeqFloorInv` & `UniqueActiveOrderSeqInv`**: `DopState::login` enforces `max(next_order_seqnum, login_seq, 1)` across worker disconnects while resetting `last_recv_seqnum = 0`; `check_msg_seqnum` rejects `0` and sequence gaps without advancing `last_recv_seqnum`.
3. **Per-order `RejectStaleOrder` isolation**: Intents exceeding `max_order_age_ns` are rejected individually without disabling any `(account_ref, strat_id)` pair or mutating `AccountSession` holds.
4. **`DisabledSweepCompletenessInv`**: Granular `set_strat_enabled` (`account_ref = 0` all-account wildcard, `strat_id = 255` all-strategy wildcard) paired with ascending single-slot `set_strat_enabled_collect` sweeps and `cancel_on_ack` leaves zero resting `Open` orders on disabled `(account_ref, strat_id)` pairs even when venue `Ack` messages interleave with the sweep.

Symbolic 64-bit `QF_LIA` verification (`0 .. 2^64 - 1` timestamps and sequence numbers) continues in [L1_sym_ors_session_routing](../L1_sym_ors_session_routing/05_output.md).
