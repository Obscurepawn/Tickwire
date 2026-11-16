--------------------------- MODULE L0_PoolRouter ---------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    ExclusiveSlots,
    SharedSlots,
    Txs,
    MaxShared,
    WarmupSamples,
    MaxSamples,
    SampleSet,
    MaxDisconnects

(*
Code-faithful model of pooled order routing (`OrderRouter` + `OrderConnectionPool` +
`PublishedRoutes`), `AccountGateway` multi-slot pool (`submit_on_slot`, `apply_slot_report`,
`drop_broken_slot_isolated`, `poll_pool_dials_filtered`), unified session pending-slot fencing
(`UnifiedBinanceSession` / `UnifiedUtaSession`), and multiplexed WebSocket pre-EOF frame draining.

Variable <-> Rust source mapping:
- slot_established, slot_epoch, slot_inflight, ema_us, ema_samples, last_pick_slot,
  router_pick_sound: `RouteHealth`, `OrderRoute`, `OrderRouter::{on_connected, on_disconnect,
  acquire_inflight, release_inflight, note_sample, pick}`, `PublishedRoutes::pick`, and
  `AccountGateway::pick_pool_slot_filtered`,
  crates/tickwire-transport/src/order_router.rs:130-140,
  crates/tickwire-transport/src/order_router.rs:180-194,
  crates/tickwire-transport/src/order_router.rs:220-232,
  crates/tickwire-transport/src/order_router.rs:378-445,
  crates/tickwire-transport/src/order_router.rs:466-515,
  crates/tickwire-transport/src/order_router.rs:688-745,
  crates/tickwire-account-gateway/src/pool.rs:333-365.
- baton_ready, disconnects_used: `DialBaton` (`DialState::Ready`) and `OrderConnectionPool::
  {poll_dials_filtered, disconnect_slot}` + `AccountGateway::poll_pool_dials_filtered`,
  crates/tickwire-transport/src/order_router.rs:941-1015,
  crates/tickwire-account-gateway/src/pool.rs:290-330.
- pending_send, retained_rx: `SlotIo.pending_send` and `SlotIo.retained_receive` across
  `submit_on_slot`, `apply_slot_report`, `read_slot`, and `drop_broken_slot_isolated`,
  crates/tickwire-account-gateway/src/pool.rs:506-540,
  crates/tickwire-account-gateway/src/pool.rs:690-765,
  crates/tickwire-account-gateway/src/pool.rs:792-835,
  crates/tickwire-account-gateway/src/websocket.rs:209-225.
- rest_slot_pending: per-slot exclusive in-flight request correlation and `poll_dials` drain guard
  across `AccountGateway` and unified venue sessions,
  crates/tickwire-binance-account/src/session.rs:857-930,
  crates/tickwire-spot-execution/src/session.rs:417-470,
  crates/tickwire-spot-execution/src/session.rs:937-980.
- ws_draining_eof: multiplexed WebSocket pre-EOF reply draining before isolating unanswered
  in-flight requests in doubt,
  crates/tickwire-account-gateway/src/pool.rs:749-775,
  crates/tickwire-binance-account/src/session.rs:914-955.
- tx_stage, tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked, tx_in_doubt, tx_in_query_desk,
  tx_abort_pending_reap, tx_resp_epoch: per-command gateway `Entry.status`, `accepted_bytes > 0`,
  order hold, `in_doubt` / `OrsQueryDesk` registration, pending reap flag, and matched response
  epoch witness,
  crates/tickwire-account-gateway/src/pool.rs:690-745,
  crates/tickwire-account-gateway/src/pool.rs:792-835,
  crates/tickwire-spot-execution/src/session.rs:1235-1295.
*)

Slots == ExclusiveSlots \cup SharedSlots
TxStages == {"Idle", "Queued", "MayHaveSent", "Flushed", "Aborted", "InDoubt", "Completed"}
MaxEpochBound == 1 + 2 * MaxDisconnects
MaxSlotCap == IF MaxShared > 1 THEN MaxShared ELSE 1
MaxSampleVal == CHOOSE m \in SampleSet : \A x \in SampleSet : x <= m

ASSUME
    /\ ExclusiveSlots \cap SharedSlots = {}
    /\ Slots # {}
    /\ Txs # {}
    /\ MaxShared >= 1
    /\ WarmupSamples >= 1
    /\ MaxSamples >= WarmupSamples
    /\ SampleSet # {}
    /\ MaxDisconnects >= 1

VARIABLES
    slot_established,
    slot_epoch,
    slot_inflight,
    ema_us,
    ema_samples,
    baton_ready,
    pending_send,
    retained_rx,
    rest_slot_pending,
    ws_draining_eof,
    last_pick_slot,
    router_pick_sound,
    disconnects_used,
    tx_stage,
    tx_slot,
    tx_epoch,
    tx_bytes_sent,
    tx_hold_locked,
    tx_in_doubt,
    tx_in_query_desk,
    tx_abort_pending_reap,
    tx_resp_epoch

vars == <<
    slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
    baton_ready, pending_send, retained_rx, rest_slot_pending, ws_draining_eof,
    last_pick_slot, router_pick_sound, disconnects_used,
    tx_stage, tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
    tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap, tx_resp_epoch
>>

SlotCap(s) == IF s \in ExclusiveSlots THEN 1 ELSE MaxShared

SlotMeasured(s) == ema_samples[s] >= WarmupSamples

SlotEligible(s) ==
    /\ slot_established[s]
    /\ slot_inflight[s] < SlotCap(s)
    /\ pending_send[s] = 0
    /\ (s \in ExclusiveSlots => rest_slot_pending[s] = 0)
    /\ (s \in SharedSlots => ~ws_draining_eof[s])

EligibleSlots == {s \in Slots : SlotEligible(s)}

MinSlot(S) == CHOOSE x \in S : \A y \in S : x <= y

MeasuredEligible == {s \in EligibleSlots : SlotMeasured(s)}

BetterScore(a, b) ==
    \/ ema_us[a] < ema_us[b]
    \/ (ema_us[a] = ema_us[b] /\ a < b)

BestMeasuredSlot ==
    CHOOSE x \in MeasuredEligible : \A y \in MeasuredEligible : x = y \/ BetterScore(x, y)

AfterLastSlots == {s \in EligibleSlots : last_pick_slot # 0 /\ s > last_pick_slot}

\* Exact transcription of OrderRouter::pick (crates/tickwire-transport/src/order_router.rs:466-515)
\* and AccountGateway::pick_pool_slot_filtered (crates/tickwire-account-gateway/src/pool.rs:337-365).
PickedSlot ==
    IF EligibleSlots = {} THEN 0
    ELSE
        LET first == MinSlot(EligibleSlots) IN
        IF MeasuredEligible = {} THEN
            IF last_pick_slot = 0 THEN first
            ELSE IF AfterLastSlots # {} THEN MinSlot(AfterLastSlots)
            ELSE first
        ELSE
            LET best == BestMeasuredSlot IN
            IF last_pick_slot \in MeasuredEligible /\ ema_us[last_pick_slot] * 8 <= ema_us[best] * 9
            THEN last_pick_slot
            ELSE best

\* Declarative specification of warmup round-robin and 9/8 hysteresis soundness.
PickMatchesSpec(chosen) ==
    /\ chosen \in EligibleSlots
    /\ IF \E s \in EligibleSlots : SlotMeasured(s)
       THEN
           /\ SlotMeasured(chosen)
           /\ IF last_pick_slot \in EligibleSlots /\ SlotMeasured(last_pick_slot)
                 /\ (\A m \in EligibleSlots : SlotMeasured(m) => ema_us[last_pick_slot] * 8 <= ema_us[m] * 9)
              THEN chosen = last_pick_slot
              ELSE \A m \in EligibleSlots : SlotMeasured(m) => (m = chosen \/ BetterScore(chosen, m))
       ELSE
           IF last_pick_slot = 0 \/ ~\E s \in EligibleSlots : s > last_pick_slot
           THEN \A s \in EligibleSlots : chosen <= s
           ELSE /\ chosen > last_pick_slot
                /\ \A s \in EligibleSlots : s > last_pick_slot => chosen <= s

UpdatedEma(cur_ema, cur_samples, sample) ==
    IF cur_samples = 0 THEN sample
    ELSE (cur_ema * 7 + sample) \div 8

UpdatedSamples(cur_samples) ==
    IF cur_samples < MaxSamples THEN cur_samples + 1 ELSE MaxSamples

ActiveOnCurrentEpoch(tx, s) ==
    /\ tx_slot[tx] = s
    /\ tx_epoch[tx] = slot_epoch[s]
    /\ tx_stage[tx] \in {"Queued", "MayHaveSent", "Flushed"}

FlushedOnCurEpoch(s) ==
    {tx \in Txs : tx_slot[tx] = s /\ tx_epoch[tx] = slot_epoch[s] /\ tx_stage[tx] = "Flushed"}

FlushedAnyEpochOnSlot(s) ==
    {tx \in Txs : tx_slot[tx] = s /\ tx_stage[tx] = "Flushed"}

Init ==
    /\ slot_established = [s \in Slots |-> TRUE]
    /\ slot_epoch = [s \in Slots |-> 1]
    /\ slot_inflight = [s \in Slots |-> 0]
    /\ ema_us = [s \in Slots |-> 0]
    /\ ema_samples = [s \in Slots |-> 0]
    /\ baton_ready = [s \in Slots |-> FALSE]
    /\ pending_send = [s \in Slots |-> 0]
    /\ retained_rx = [s \in Slots |-> {}]
    /\ rest_slot_pending = [s \in Slots |-> 0]
    /\ ws_draining_eof = [s \in Slots |-> FALSE]
    /\ last_pick_slot = 0
    /\ router_pick_sound = TRUE
    /\ disconnects_used = 0
    /\ tx_stage = [tx \in Txs |-> "Idle"]
    /\ tx_slot = [tx \in Txs |-> 0]
    /\ tx_epoch = [tx \in Txs |-> 0]
    /\ tx_bytes_sent = [tx \in Txs |-> FALSE]
    /\ tx_hold_locked = [tx \in Txs |-> FALSE]
    /\ tx_in_doubt = [tx \in Txs |-> FALSE]
    /\ tx_in_query_desk = [tx \in Txs |-> FALSE]
    /\ tx_abort_pending_reap = [tx \in Txs |-> FALSE]
    /\ tx_resp_epoch = [tx \in Txs |-> 0]

\* 1. Pick ready slot via OrderRouter::pick + submit_on_slot / acquire_inflight.
SubmitPickedSlot(tx) ==
    LET s == PickedSlot IN
    /\ tx_stage[tx] = "Idle"
    /\ s # 0
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] = @ + 1]
    /\ pending_send' = [pending_send EXCEPT ![s] = tx]
    /\ rest_slot_pending' = [rest_slot_pending EXCEPT ![s] = IF s \in ExclusiveSlots THEN tx ELSE @]
    /\ last_pick_slot' = s
    /\ router_pick_sound' = (router_pick_sound /\ PickMatchesSpec(s))
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "Queued"]
    /\ tx_slot' = [tx_slot EXCEPT ![tx] = s]
    /\ tx_epoch' = [tx_epoch EXCEPT ![tx] = slot_epoch[s]]
    /\ tx_bytes_sent' = [tx_bytes_sent EXCEPT ![tx] = FALSE]
    /\ tx_hold_locked' = [tx_hold_locked EXCEPT ![tx] = TRUE]
    /\ UNCHANGED <<
        slot_established, slot_epoch, ema_us, ema_samples, baton_ready,
        retained_rx, ws_draining_eof, disconnects_used,
        tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap, tx_resp_epoch
       >>

\* 2. Zero-byte SendState::NoBytes in AccountGateway::apply_slot_report
\* (crates/tickwire-account-gateway/src/pool.rs:715-735):
\* releases router inflight once and aborts reservation without marking in_doubt.
AbortZeroBytesInPoll(s) ==
    LET tx == pending_send[s] IN
    /\ tx # 0
    /\ tx_stage[tx] = "Queued"
    /\ ~tx_bytes_sent[tx]
    /\ pending_send' = [pending_send EXCEPT ![s] = 0]
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] =
        IF slot_epoch[s] = tx_epoch[tx] /\ @ > 0 THEN @ - 1 ELSE @]
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "Aborted"]
    /\ tx_hold_locked' = [tx_hold_locked EXCEPT ![tx] = FALSE]
    /\ tx_in_doubt' = [tx_in_doubt EXCEPT ![tx] = FALSE]
    /\ tx_in_query_desk' = [tx_in_query_desk EXCEPT ![tx] = FALSE]
    /\ tx_abort_pending_reap' = [tx_abort_pending_reap EXCEPT ![tx] = (s \in ExclusiveSlots)]
    /\ UNCHANGED <<
        slot_established, slot_epoch, ema_us, ema_samples, baton_ready,
        retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_bytes_sent, tx_resp_epoch
       >>

\* 3. Pre-read Status::Aborted reaping in pooled session slots
\* (crates/tickwire-account-gateway/src/pool.rs:663-685,
\* crates/tickwire-spot-execution/src/session.rs:961-980):
\* clears rest_slot_pending before reading socket bytes and WITHOUT a duplicate release_slot_inflight.
ReapAbortedRestPending(s) ==
    LET tx == rest_slot_pending[s] IN
    /\ s \in ExclusiveSlots
    /\ tx # 0
    /\ tx_stage[tx] = "Aborted"
    /\ rest_slot_pending' = [rest_slot_pending EXCEPT ![s] = 0]
    /\ tx_abort_pending_reap' = [tx_abort_pending_reap EXCEPT ![tx] = FALSE]
    /\ UNCHANGED <<
        slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
        baton_ready, pending_send, retained_rx, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_stage, tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_in_doubt, tx_in_query_desk, tx_resp_epoch
       >>

\* 4. First plaintext byte accepted into TLS/socket (SendState::MayHaveSent,
\* crates/tickwire-account-gateway/src/pool.rs:700-706).
PartialAcceptOnSlot(s) ==
    LET tx == pending_send[s] IN
    /\ tx # 0
    /\ tx_stage[tx] = "Queued"
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "MayHaveSent"]
    /\ tx_bytes_sent' = [tx_bytes_sent EXCEPT ![tx] = TRUE]
    /\ UNCHANGED <<
        slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
        baton_ready, pending_send, retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_hold_locked, tx_in_doubt, tx_in_query_desk,
        tx_abort_pending_reap, tx_resp_epoch
       >>

\* 5. Request bytes fully flushed (SendState::Flushed,
\* crates/tickwire-account-gateway/src/pool.rs:707-714):
\* clears pending_send while keeping slot_inflight acquired until response or broken-slot finalization.
FlushOnSlot(s) ==
    LET tx == pending_send[s] IN
    /\ tx # 0
    /\ tx_stage[tx] \in {"Queued", "MayHaveSent"}
    /\ pending_send' = [pending_send EXCEPT ![s] = 0]
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "Flushed"]
    /\ tx_bytes_sent' = [tx_bytes_sent EXCEPT ![tx] = TRUE]
    /\ UNCHANGED <<
        slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
        baton_ready, retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_hold_locked, tx_in_doubt, tx_in_query_desk,
        tx_abort_pending_reap, tx_resp_epoch
       >>

\* 6. Live response decoded on established slot (HTTP reply on Exclusive or out-of-order WsRequestId on Shared):
\* releases slot_inflight for matching epoch and updates integer EWMA
\* (crates/tickwire-transport/src/order_router.rs:180-194).
CompleteLiveReply(tx, sample) ==
    LET s == tx_slot[tx] IN
    /\ tx_stage[tx] = "Flushed"
    /\ slot_established[s]
    /\ slot_epoch[s] = tx_epoch[tx]
    /\ ~ws_draining_eof[s]
    /\ (s \in ExclusiveSlots => rest_slot_pending[s] = tx)
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] =
        IF slot_epoch[s] = tx_epoch[tx] /\ @ > 0 THEN @ - 1 ELSE @]
    /\ ema_us' = [ema_us EXCEPT ![s] = UpdatedEma(ema_us[s], ema_samples[s], sample)]
    /\ ema_samples' = [ema_samples EXCEPT ![s] = UpdatedSamples(ema_samples[s])]
    /\ rest_slot_pending' = [rest_slot_pending EXCEPT ![s] = IF s \in ExclusiveSlots THEN 0 ELSE @]
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "Completed"]
    /\ tx_resp_epoch' = [tx_resp_epoch EXCEPT ![tx] = slot_epoch[s]]
    /\ UNCHANGED <<
        slot_established, slot_epoch, baton_ready, pending_send, retained_rx,
        ws_draining_eof, last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap
       >>

\* 7. Peer delivers >= 1 complete reply frame(s) and closes TCP/TLS before owner drains them:
\* drop_broken_slot_isolated retains unread reply bytes in retained_rx, disconnects the slot
\* (bumping epoch & resetting router health), aborts any 0-byte Queued pending_send, and isolates any
\* MayHaveSent pending_send (crates/tickwire-account-gateway/src/pool.rs:792-835,
\* crates/tickwire-binance-account/src/session.rs:914-955).
PeerReplyThenEof(s) ==
    /\ slot_established[s]
    /\ disconnects_used < MaxDisconnects
    /\ FlushedOnCurEpoch(s) # {}
    /\ \E delivered \in (SUBSET FlushedOnCurEpoch(s)) \ {{}} :
        LET ptx == pending_send[s]
            p_zero == ptx # 0 /\ tx_stage[ptx] = "Queued" /\ ~tx_bytes_sent[ptx]
            p_ambig == ptx # 0 /\ tx_stage[ptx] = "MayHaveSent" /\ tx_bytes_sent[ptx]
        IN
        /\ slot_established' = [slot_established EXCEPT ![s] = FALSE]
        /\ slot_epoch' = [slot_epoch EXCEPT ![s] = @ + 1]
        /\ slot_inflight' = [slot_inflight EXCEPT ![s] = 0]
        /\ ema_us' = [ema_us EXCEPT ![s] = 0]
        /\ ema_samples' = [ema_samples EXCEPT ![s] = 0]
        /\ pending_send' = [pending_send EXCEPT ![s] = 0]
        /\ retained_rx' = [retained_rx EXCEPT ![s] = delivered]
        /\ ws_draining_eof' = [ws_draining_eof EXCEPT ![s] = (s \in SharedSlots)]
        /\ disconnects_used' = disconnects_used + 1
        /\ tx_stage' = [t \in Txs |->
            IF t = ptx /\ p_zero THEN "Aborted"
            ELSE IF t = ptx /\ p_ambig THEN "InDoubt"
            ELSE tx_stage[t]]
        /\ tx_hold_locked' = [t \in Txs |->
            IF t = ptx /\ p_zero THEN FALSE ELSE tx_hold_locked[t]]
        /\ tx_in_doubt' = [t \in Txs |->
            IF t = ptx /\ p_ambig THEN TRUE ELSE tx_in_doubt[t]]
        /\ tx_in_query_desk' = [t \in Txs |->
            IF t = ptx /\ p_ambig THEN TRUE ELSE tx_in_query_desk[t]]
        /\ tx_abort_pending_reap' = [t \in Txs |->
            IF t = ptx /\ p_zero /\ s \in ExclusiveSlots THEN TRUE ELSE tx_abort_pending_reap[t]]
        /\ UNCHANGED <<
            baton_ready, rest_slot_pending, last_pick_slot, router_pick_sound,
            tx_slot, tx_epoch, tx_bytes_sent, tx_resp_epoch
           >>

\* 8. Transport drops with zero unread response frames (peer reset, write error, or WS Close/expiry):
\* aborts 0-byte Queued pending_send, isolates MayHaveSent pending_send, and leaves Flushed entries
\* for post-EOF FinalizeBrokenSlotPending without freezing sibling slots
\* (crates/tickwire-account-gateway/src/pool.rs:792-835).
DisconnectWithoutReply(s) ==
    LET ptx == pending_send[s]
        p_zero == ptx # 0 /\ tx_stage[ptx] = "Queued" /\ ~tx_bytes_sent[ptx]
        p_ambig == ptx # 0 /\ tx_stage[ptx] = "MayHaveSent" /\ tx_bytes_sent[ptx]
    IN
    /\ slot_established[s]
    /\ disconnects_used < MaxDisconnects
    /\ slot_established' = [slot_established EXCEPT ![s] = FALSE]
    /\ slot_epoch' = [slot_epoch EXCEPT ![s] = @ + 1]
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] = 0]
    /\ ema_us' = [ema_us EXCEPT ![s] = 0]
    /\ ema_samples' = [ema_samples EXCEPT ![s] = 0]
    /\ pending_send' = [pending_send EXCEPT ![s] = 0]
    /\ disconnects_used' = disconnects_used + 1
    /\ tx_stage' = [t \in Txs |->
        IF t = ptx /\ p_zero THEN "Aborted"
        ELSE IF t = ptx /\ p_ambig THEN "InDoubt"
        ELSE tx_stage[t]]
    /\ tx_hold_locked' = [t \in Txs |->
        IF t = ptx /\ p_zero THEN FALSE ELSE tx_hold_locked[t]]
    /\ tx_in_doubt' = [t \in Txs |->
        IF t = ptx /\ p_ambig THEN TRUE ELSE tx_in_doubt[t]]
    /\ tx_in_query_desk' = [t \in Txs |->
        IF t = ptx /\ p_ambig THEN TRUE ELSE tx_in_query_desk[t]]
    /\ tx_abort_pending_reap' = [t \in Txs |->
        IF t = ptx /\ p_zero /\ s \in ExclusiveSlots THEN TRUE ELSE tx_abort_pending_reap[t]]
    /\ UNCHANGED <<
        baton_ready, retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, tx_slot, tx_epoch, tx_bytes_sent, tx_resp_epoch
       >>

\* 9. Off-hot-path DialWorker publishes a ready replacement connection onto DialBaton
\* (crates/tickwire-transport/src/order_router.rs:945-985), potentially BEFORE retained_rx or stale SlotPending drains!
BatonBecomeReady(s) ==
    /\ ~slot_established[s]
    /\ ~baton_ready[s]
    /\ baton_ready' = [baton_ready EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<
        slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
        pending_send, retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_stage, tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap, tx_resp_epoch
       >>

\* 10. Owner drains a buffered pre-EOF reply from retained_receive / WS decoder after socket EOF
\* (crates/tickwire-account-gateway/src/pool.rs:749-775,
\* crates/tickwire-binance-account/src/session.rs:914-955).
\* Epoch-fenced release_inflight is a safe no-op on the disconnected epoch, and response binds strictly to tx_epoch[tx].
DrainRetainedReply(s, tx) ==
    LET rem_rx == retained_rx[s] \ {tx}
        other_flushed == {t \in Txs : t # tx /\ tx_slot[t] = s /\ tx_stage[t] = "Flushed"}
    IN
    /\ ~slot_established[s]
    /\ tx \in retained_rx[s]
    /\ (s \in ExclusiveSlots => rest_slot_pending[s] = tx)
    /\ retained_rx' = [retained_rx EXCEPT ![s] = rem_rx]
    /\ rest_slot_pending' = [rest_slot_pending EXCEPT ![s] = IF s \in ExclusiveSlots THEN 0 ELSE @]
    /\ ws_draining_eof' = [ws_draining_eof EXCEPT ![s] =
        IF s \in SharedSlots THEN (rem_rx # {} \/ other_flushed # {}) ELSE FALSE]
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] =
        IF slot_epoch[s] = tx_epoch[tx] /\ @ > 0 THEN @ - 1 ELSE @]
    /\ tx_stage' = [tx_stage EXCEPT ![tx] = "Completed"]
    /\ tx_resp_epoch' = [tx_resp_epoch EXCEPT ![tx] = tx_epoch[tx]]
    /\ UNCHANGED <<
        slot_established, slot_epoch, ema_us, ema_samples, baton_ready, pending_send,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap
       >>

\* 11. Once retained_rx is empty on a disconnected slot, owner finalizes remaining broken pending
\* transactions into InDoubt + QueryDesk and clears rest_slot_pending / ws_draining_eof
\* (crates/tickwire-account-gateway/src/pool.rs:792-835,
\* crates/tickwire-spot-execution/src/session.rs:459-470).
FinalizeBrokenSlotPending(s) ==
    LET targets == FlushedAnyEpochOnSlot(s) IN
    /\ ~slot_established[s]
    /\ retained_rx[s] = {}
    /\ IF s \in ExclusiveSlots
       THEN rest_slot_pending[s] # 0 /\ tx_stage[rest_slot_pending[s]] # "Aborted"
       ELSE (ws_draining_eof[s] \/ targets # {})
    /\ rest_slot_pending' = [rest_slot_pending EXCEPT ![s] = 0]
    /\ ws_draining_eof' = [ws_draining_eof EXCEPT ![s] = FALSE]
    /\ tx_stage' = [t \in Txs |-> IF t \in targets THEN "InDoubt" ELSE tx_stage[t]]
    /\ tx_in_doubt' = [t \in Txs |-> IF t \in targets THEN TRUE ELSE tx_in_doubt[t]]
    /\ tx_in_query_desk' = [t \in Txs |-> IF t \in targets THEN TRUE ELSE tx_in_query_desk[t]]
    /\ UNCHANGED <<
        slot_established, slot_epoch, slot_inflight, ema_us, ema_samples,
        baton_ready, pending_send, retained_rx,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_abort_pending_reap, tx_resp_epoch
       >>

\* 12. Guarded poll_pool_dials_filtered (crates/tickwire-account-gateway/src/pool.rs:294-330,
\* crates/tickwire-binance-account/src/session.rs:857-910):
\* installs a ready DialBaton connection ONLY when pending_send, retained_rx, and owner pending are empty!
HarvestReadyBaton(s) ==
    /\ ~slot_established[s]
    /\ baton_ready[s]
    /\ pending_send[s] = 0
    /\ retained_rx[s] = {}
    /\ (s \in ExclusiveSlots => rest_slot_pending[s] = 0)
    /\ (s \in SharedSlots => (~ws_draining_eof[s] /\ FlushedAnyEpochOnSlot(s) = {}))
    /\ baton_ready' = [baton_ready EXCEPT ![s] = FALSE]
    /\ slot_established' = [slot_established EXCEPT ![s] = TRUE]
    /\ slot_epoch' = [slot_epoch EXCEPT ![s] = @ + 1]
    /\ slot_inflight' = [slot_inflight EXCEPT ![s] = 0]
    /\ ema_us' = [ema_us EXCEPT ![s] = 0]
    /\ ema_samples' = [ema_samples EXCEPT ![s] = 0]
    /\ UNCHANGED <<
        pending_send, retained_rx, rest_slot_pending, ws_draining_eof,
        last_pick_slot, router_pick_sound, disconnects_used,
        tx_stage, tx_slot, tx_epoch, tx_bytes_sent, tx_hold_locked,
        tx_in_doubt, tx_in_query_desk, tx_abort_pending_reap, tx_resp_epoch
       >>

Next ==
    \/ \E tx \in Txs : SubmitPickedSlot(tx)
    \/ \E s \in Slots : AbortZeroBytesInPoll(s)
    \/ \E s \in ExclusiveSlots : ReapAbortedRestPending(s)
    \/ \E s \in Slots : PartialAcceptOnSlot(s)
    \/ \E s \in Slots : FlushOnSlot(s)
    \/ \E tx \in Txs, sample \in SampleSet : CompleteLiveReply(tx, sample)
    \/ \E s \in Slots : PeerReplyThenEof(s)
    \/ \E s \in Slots : DisconnectWithoutReply(s)
    \/ \E s \in Slots : BatonBecomeReady(s)
    \/ \E s \in Slots, tx \in Txs : DrainRetainedReply(s, tx)
    \/ \E s \in Slots : FinalizeBrokenSlotPending(s)
    \/ \E s \in Slots : HarvestReadyBaton(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ slot_established \in [Slots -> BOOLEAN]
    /\ slot_epoch \in [Slots -> 1..MaxEpochBound]
    /\ slot_inflight \in [Slots -> 0..MaxSlotCap]
    /\ ema_us \in [Slots -> 0..MaxSampleVal]
    /\ ema_samples \in [Slots -> 0..MaxSamples]
    /\ baton_ready \in [Slots -> BOOLEAN]
    /\ pending_send \in [Slots -> Txs \cup {0}]
    /\ retained_rx \in [Slots -> SUBSET Txs]
    /\ rest_slot_pending \in [Slots -> Txs \cup {0}]
    /\ ws_draining_eof \in [Slots -> BOOLEAN]
    /\ last_pick_slot \in Slots \cup {0}
    /\ router_pick_sound \in BOOLEAN
    /\ disconnects_used \in 0..MaxDisconnects
    /\ tx_stage \in [Txs -> TxStages]
    /\ tx_slot \in [Txs -> Slots \cup {0}]
    /\ tx_epoch \in [Txs -> 0..MaxEpochBound]
    /\ tx_bytes_sent \in [Txs -> BOOLEAN]
    /\ tx_hold_locked \in [Txs -> BOOLEAN]
    /\ tx_in_doubt \in [Txs -> BOOLEAN]
    /\ tx_in_query_desk \in [Txs -> BOOLEAN]
    /\ tx_abort_pending_reap \in [Txs -> BOOLEAN]
    /\ tx_resp_epoch \in [Txs -> 0..MaxEpochBound]

ExclusiveSlotBound ==
    \A s \in ExclusiveSlots :
        /\ slot_inflight[s] <= 1
        /\ Cardinality({tx \in Txs : ActiveOnCurrentEpoch(tx, s)}) <= 1

SharedSlotBound ==
    \A s \in SharedSlots :
        /\ slot_inflight[s] <= MaxShared
        /\ Cardinality({tx \in Txs : ActiveOnCurrentEpoch(tx, s)}) <= MaxShared

InflightAccountingExact ==
    \A s \in Slots :
        slot_inflight[s] =
            IF slot_established[s]
            THEN Cardinality({tx \in Txs : ActiveOnCurrentEpoch(tx, s)})
            ELSE 0

NoCrossEpochResponseAliasing ==
    /\ \A tx \in Txs : tx_resp_epoch[tx] # 0 => tx_resp_epoch[tx] = tx_epoch[tx]
    /\ \A s \in Slots : slot_established[s] => retained_rx[s] = {}
    /\ \A s \in ExclusiveSlots :
        (slot_established[s] /\ rest_slot_pending[s] # 0 /\ tx_stage[rest_slot_pending[s]] # "Aborted")
            => tx_epoch[rest_slot_pending[s]] = slot_epoch[s]
    /\ \A s \in SharedSlots :
        slot_established[s] =>
            (/\ ~ws_draining_eof[s]
             /\ \A tx \in Txs :
                (tx_slot[tx] = s /\ tx_stage[tx] \in {"Queued", "MayHaveSent", "Flushed"})
                    => tx_epoch[tx] = slot_epoch[s])

AbortedNeverMarkedInDoubt ==
    \A tx \in Txs :
        tx_stage[tx] = "Aborted" =>
            /\ ~tx_bytes_sent[tx]
            /\ ~tx_hold_locked[tx]
            /\ ~tx_in_doubt[tx]
            /\ ~tx_in_query_desk[tx]
            /\ tx_resp_epoch[tx] = 0

AmbiguousSentIsolatedInDoubt ==
    /\ \A tx \in Txs :
        tx_stage[tx] = "InDoubt" =>
            /\ tx_bytes_sent[tx]
            /\ tx_hold_locked[tx]
            /\ tx_in_doubt[tx]
            /\ tx_in_query_desk[tx]
            /\ tx_resp_epoch[tx] = 0
    /\ \A tx \in Txs :
        (tx_in_doubt[tx] \/ tx_in_query_desk[tx]) => tx_stage[tx] = "InDoubt"
    /\ \A tx \in Txs :
        (tx_stage[tx] = "Flushed" /\ ~slot_established[tx_slot[tx]] /\ tx \notin retained_rx[tx_slot[tx]]) =>
            /\ tx_bytes_sent[tx]
            /\ tx_hold_locked[tx]
            /\ (tx_slot[tx] \in ExclusiveSlots => rest_slot_pending[tx_slot[tx]] = tx)
    /\ \A s \in Slots :
        (slot_established[s]
         /\ slot_inflight[s] < SlotCap(s)
         /\ pending_send[s] = 0
         /\ (s \in ExclusiveSlots => rest_slot_pending[s] = 0)
         /\ (s \in SharedSlots => ~ws_draining_eof[s]))
            => s \in EligibleSlots

HysteresisAndWarmupSoundness ==
    /\ router_pick_sound
    /\ (EligibleSlots # {} => PickMatchesSpec(PickedSlot))

=============================================================================
