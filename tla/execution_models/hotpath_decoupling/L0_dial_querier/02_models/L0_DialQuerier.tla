---------------------------- MODULE L0_DialQuerier ----------------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Capacity, TaskCount, MaxQueryCount, MaxDialGen, MaxTime

(*
Code-faithful model of off-hot-path TCP/TLS connection dialing (DialBaton + IpPool)
and bounded asynchronous HTTP querying (AsyncRestQuerier + SpscRing).

Variable <-> Rust source mapping and state/watermark denotations:
- now: discrete monotonic clock step in 0..MaxTime representing ClockFn readings,
  crates/tickwire-transport/src/dialer.rs:338-356,
  crates/tickwire-transport/src/rest_querier.rs:789-799.
- baton_state: DialBaton.state atomic state machine ("Idle", "Wanted", "Dialing",
  "Ready", "Stopped"), crates/tickwire-transport/src/dialer.rs:177-195,
  crates/tickwire-transport/src/dialer.rs:264-271.
- baton_due: DialBaton.due_mono_ns earliest step at which "tw-dialer" may claim
  Wanted -> Dialing, crates/tickwire-transport/src/dialer.rs:217-223,
  crates/tickwire-transport/src/dialer.rs:322-356,
  crates/tickwire-transport/src/dialer.rs:370-381.
- baton_failures: DialBaton.failures consecutive failed attempts in the active
  want cycle (reset to 0 on take_ready and reset),
  crates/tickwire-transport/src/dialer.rs:290-299,
  crates/tickwire-transport/src/dialer.rs:370-410.
- staged_link: DialBaton.product: TimedSpinLock<Option<DialedLink>> staged between
  deliver_ready and take_ready/reset/stop,
  crates/tickwire-transport/src/dialer.rs:264-271,
  crates/tickwire-transport/src/dialer.rs:358-420.
- active_attempt: ManagedSlot.active: Option<ActiveAttempt> owned exclusively by
  the "tw-dialer" worker thread during TCP+TLS/WS/FIX connect,
  crates/tickwire-transport/src/dialer.rs:966-985,
  crates/tickwire-transport/src/dialer.rs:1283-1339.
- leased_peer: ManagedSlot.leased_peer: Option<SocketAddr> (0 = None, 1..2 = peer)
  tracking the peer leased to the session across reconnect failover,
  crates/tickwire-transport/src/dialer.rs:983,
  crates/tickwire-transport/src/dialer.rs:1239-1248,
  crates/tickwire-transport/src/dialer.rs:1334-1338.
- pool_present, peer_in_use, peer_failed: IpPool.peers membership, PeerEntry.in_use
  lease count, and PeerEntry.last_failed penalty / zero-lease eviction on
  note_syn(peer, None) when len > 1,
  crates/tickwire-transport/src/dialer.rs:33-46,
  crates/tickwire-transport/src/dialer.rs:103-173.
- owner_link, dial_gen, was_stopped: session owner's adopted DialedLink from
  take_ready, monotone dial generation witness (0..MaxDialGen), and terminal stop
  witness, crates/tickwire-transport/src/dialer.rs:383-420.
- issued, task_queue, reply_queue: submitted RestTask IDs and lock-free SPSC
  SpscRing<RestTask> / SpscRing<RestReply> queues of capacity Capacity,
  crates/tickwire-transport/src/rest_querier.rs:226-348,
  crates/tickwire-transport/src/rest_querier.rs:459-488.
- waiting: worker-private Vec<WaitingTask> holding pulled/retryable tasks gated by
  waiting.len() + usize::from(inflight.is_some()) + shared.reply_queue.len() < config.capacity,
  crates/tickwire-transport/src/rest_querier.rs:515-520,
  crates/tickwire-transport/src/rest_querier.rs:774-799.
- inflight: worker-private Option<InflightTask> tracking the single active HTTP
  exchange and counted_send latch,
  crates/tickwire-transport/src/rest_querier.rs:522-530,
  crates/tickwire-transport/src/rest_querier.rs:1218-1398.
- last_reply, accepted_writes: consumer's last dequeued RestReply and ghost count
  of attempts where accepted_bytes > 0 on the transport,
  crates/tickwire-transport/src/rest_querier.rs:89-97,
  crates/tickwire-transport/src/rest_querier.rs:470-476,
  crates/tickwire-transport/src/rest_querier.rs:1218-1227.
*)

VARIABLES
    now,
    baton_state,
    baton_due,
    baton_failures,
    staged_link,
    active_attempt,
    leased_peer,
    pool_present,
    peer_in_use,
    peer_failed,
    owner_link,
    dial_gen,
    was_stopped,
    issued,
    task_queue,
    waiting,
    inflight,
    reply_queue,
    last_reply,
    accepted_writes

vars == <<
    now, baton_state, baton_due, baton_failures, staged_link, active_attempt,
    leased_peer, pool_present, peer_in_use, peer_failed, owner_link, dial_gen,
    was_stopped, issued, task_queue, waiting, inflight, reply_queue,
    last_reply, accepted_writes
>>

Peers == 1..2
Tasks == 1..TaskCount
MaxFailures == 2
MaxCountBound == MaxTime + MaxQueryCount + 2

BatonStates == {"Idle", "Wanted", "Dialing", "Ready", "Stopped"}
FailKinds == {"none", "Transport", "Http5xx"}
ReplyStatuses == {"none", "Ok", "Expired", "NotBuilt", "SendFail", "Transport", "HttpError"}

NoneLink == [peer |-> 0, gen |-> 0, handshaked |-> FALSE]
NoneInflight == [id |-> 0, qc |-> 0, counted |-> FALSE]
NoneReply == [id |-> 0, status |-> "none", qc |-> 0]

LinkValues ==
    {NoneLink} \cup [peer: Peers, gen: 1..MaxDialGen, handshaked: BOOLEAN]

WaitingElemValues ==
    [id: Tasks, qc: 0..MaxCountBound, next_ns: 0..(MaxTime + 1), last_fail: FailKinds]

InflightValues ==
    {NoneInflight} \cup [id: Tasks, qc: 0..MaxCountBound, counted: BOOLEAN]

ReplyElemValues ==
    [id: Tasks, status: ReplyStatuses \ {"none"}, qc: 0..MaxCountBound]

Min(a, b) == IF a <= b THEN a ELSE b

RemoveAt(seq, idx) ==
    SubSeq(seq, 1, idx - 1) \o SubSeq(seq, idx + 1, Len(seq))

\* Task 2 represents an unbuildable request kind (Ok(None) -> RestStatus::NotBuilt);
\* Tasks 1 and 2 carry deadline MaxTime while Task 3 (when configured) has end_mono_ns = 0.
\* crates/tickwire-transport/src/rest_querier.rs:40-75, crates/tickwire-transport/src/rest_querier.rs:838-854.
Buildable(t) == t # 2
EndNs(t) == IF t = 3 /\ MaxQueryCount > 0 THEN 0 ELSE MaxTime

\* IpPool::take_ip / quality_key / replace_with_unused selection order.
\* crates/tickwire-transport/src/dialer.rs:126-173.
QualityRank(p) == IF peer_failed[p] THEN 2 ELSE 1

BetterPeer(in_use_fn, p, q) ==
    \/ in_use_fn[p] < in_use_fn[q]
    \/ (in_use_fn[p] = in_use_fn[q] /\ QualityRank(p) < QualityRank(q))
    \/ (in_use_fn[p] = in_use_fn[q] /\ QualityRank(p) = QualityRank(q) /\ p < q)

PickBestPeer(candidates, in_use_fn) ==
    CHOOSE p \in candidates :
        \A q \in candidates : p = q \/ BetterPeer(in_use_fn, p, q)

InflightCount == IF inflight = NoneInflight THEN 0 ELSE 1

\* can_retry(task, query_count, next_send_ns) and reply_from_waiting_expiry(w).
\* crates/tickwire-transport/src/rest_querier.rs:683-731.
CanRetry(t, qc, next_ns) ==
    /\ (MaxQueryCount = 0 \/ qc < MaxQueryCount)
    /\ (EndNs(t) = 0 \/ next_ns < EndNs(t))

ReplyFromWaitingExpiry(w) ==
    IF w.qc = 0 THEN
        [id |-> w.id, status |-> "Expired", qc |-> 0]
    ELSE IF w.last_fail = "Http5xx" THEN
        [id |-> w.id, status |-> "HttpError", qc |-> w.qc]
    ELSE IF w.last_fail = "Transport" THEN
        [id |-> w.id, status |-> "Transport", qc |-> w.qc]
    ELSE
        [id |-> w.id, status |-> "Expired", qc |-> w.qc]

ExpiredWaiting ==
    {i \in 1..Len(waiting) : EndNs(waiting[i].id) > 0 /\ now >= EndNs(waiting[i].id)}

FirstExpiredIdx ==
    CHOOSE i \in ExpiredWaiting : \A j \in ExpiredWaiting : i <= j

EligibleWaiting ==
    {i \in 1..Len(waiting) : waiting[i].next_ns <= now}

PickWaitingIdx ==
    CHOOSE i \in EligibleWaiting :
        \A j \in EligibleWaiting :
            \/ waiting[i].next_ns < waiting[j].next_ns
            \/ (waiting[i].next_ns = waiting[j].next_ns /\ i <= j)

Init ==
    /\ now = 0
    /\ baton_state = "Idle"
    /\ baton_due = 0
    /\ baton_failures = 0
    /\ staged_link = NoneLink
    /\ active_attempt = NoneLink
    /\ leased_peer = 0
    /\ pool_present = Peers
    /\ peer_in_use = [p \in Peers |-> 0]
    /\ peer_failed = [p \in Peers |-> FALSE]
    /\ owner_link = NoneLink
    /\ dial_gen = 0
    /\ was_stopped = FALSE
    /\ issued = {}
    /\ task_queue = <<>>
    /\ waiting = <<>>
    /\ inflight = NoneInflight
    /\ reply_queue = <<>>
    /\ last_reply = NoneReply
    /\ accepted_writes = [t \in Tasks |-> 0]

\* Monotonic clock advances across backoff and request deadlines.
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<
        baton_state, baton_due, baton_failures, staged_link, active_attempt,
        leased_peer, pool_present, peer_in_use, peer_failed, owner_link,
        dial_gen, was_stopped, issued, task_queue, waiting, inflight,
        reply_queue, last_reply, accepted_writes
       >>

\* Session owner requests a fresh connection (Idle -> Wanted).
\* crates/tickwire-transport/src/dialer.rs:322-336.
OwnerWant ==
    /\ baton_state = "Idle"
    /\ dial_gen < MaxDialGen
    /\ baton_due' = now
    /\ baton_state' = "Wanted"
    /\ UNCHANGED <<
        now, baton_failures, staged_link, active_attempt, leased_peer,
        pool_present, peer_in_use, peer_failed, owner_link, dial_gen,
        was_stopped, issued, task_queue, waiting, inflight, reply_queue,
        last_reply, accepted_writes
       >>

\* "tw-dialer" claims Wanted -> Dialing when now >= due_mono_ns and leases a peer
\* via replace_with_unused on reconnect or take_ip on initial dial.
\* crates/tickwire-transport/src/dialer.rs:338-356, crates/tickwire-transport/src/dialer.rs:1239-1293.
DialerClaim ==
    /\ baton_state = "Wanted"
    /\ now >= baton_due
    /\ active_attempt = NoneLink
    /\ dial_gen < MaxDialGen
    /\ LET spares == {p \in pool_present : p # leased_peer /\ peer_in_use[p] = 0}
           released_use ==
               IF leased_peer # 0 /\ leased_peer \in pool_present
               THEN [peer_in_use EXCEPT ![leased_peer] = @ - 1]
               ELSE peer_in_use
           chosen ==
               IF leased_peer # 0 /\ spares # {}
               THEN PickBestPeer(spares, peer_in_use)
               ELSE PickBestPeer(pool_present, released_use)
           next_use == [released_use EXCEPT ![chosen] = @ + 1]
       IN /\ baton_state' = "Dialing"
          /\ dial_gen' = dial_gen + 1
          /\ leased_peer' = 0
          /\ peer_in_use' = next_use
          /\ active_attempt' = [peer |-> chosen, gen |-> dial_gen + 1, handshaked |-> FALSE]
    /\ UNCHANGED <<
        now, baton_due, baton_failures, staged_link, pool_present, peer_failed,
        owner_link, was_stopped, issued, task_queue, waiting, inflight,
        reply_queue, last_reply, accepted_writes
       >>

\* "tw-dialer" completes non-blocking TCP + TLS handshake on its private socket.
\* crates/tickwire-transport/src/dialer.rs:1399-1435.
DialerHandshakeStep ==
    /\ baton_state = "Dialing"
    /\ active_attempt # NoneLink
    /\ ~active_attempt.handshaked
    /\ active_attempt' = [active_attempt EXCEPT !.handshaked = TRUE]
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link, leased_peer,
        pool_present, peer_in_use, peer_failed, owner_link, dial_gen,
        was_stopped, issued, task_queue, waiting, inflight, reply_queue,
        last_reply, accepted_writes
       >>

\* "tw-dialer" publishes the handshaked link via deliver_ready (Dialing -> Ready).
\* crates/tickwire-transport/src/dialer.rs:358-368, crates/tickwire-transport/src/dialer.rs:1313-1339.
DialerDeliverReady ==
    /\ baton_state = "Dialing"
    /\ active_attempt # NoneLink
    /\ active_attempt.handshaked
    /\ LET p == active_attempt.peer
       IN /\ peer_failed' = [peer_failed EXCEPT ![p] = FALSE]
          /\ staged_link' = active_attempt
          /\ active_attempt' = NoneLink
          /\ leased_peer' = p
          /\ baton_state' = "Ready"
    /\ UNCHANGED <<
        now, baton_due, baton_failures, pool_present, peer_in_use, owner_link,
        dial_gen, was_stopped, issued, task_queue, waiting, inflight,
        reply_queue, last_reply, accepted_writes
       >>

\* If owner concurrently called reset() or stop(), "tw-dialer" aborts its private
\* attempt and releases the peer lease without mutating DialBaton.product.
\* crates/tickwire-transport/src/dialer.rs:1295-1311, crates/tickwire-transport/src/dialer.rs:1385-1388.
DialerAbortStale ==
    /\ active_attempt # NoneLink
    /\ baton_state # "Dialing"
    /\ LET p == active_attempt.peer
       IN /\ active_attempt' = NoneLink
          /\ peer_in_use' =
                 IF p \in pool_present
                 THEN [peer_in_use EXCEPT ![p] = @ - 1]
                 ELSE peer_in_use
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link, leased_peer,
        pool_present, peer_failed, owner_link, dial_gen, was_stopped, issued,
        task_queue, waiting, inflight, reply_queue, last_reply, accepted_writes
       >>

\* TCP/TLS connect failure or timeout: release peer, note_syn(peer, None) (evicting
\* failed peer iff in_use == 0 and len > 1), increment failures, back off to Wanted.
\* crates/tickwire-transport/src/dialer.rs:103-124, crates/tickwire-transport/src/dialer.rs:370-381,
\* crates/tickwire-transport/src/dialer.rs:1295-1311.
DialerFail ==
    /\ baton_state = "Dialing"
    /\ active_attempt # NoneLink
    /\ LET p == active_attempt.peer
           rel_use == [peer_in_use EXCEPT ![p] = @ - 1]
           evict == (p \in pool_present /\ rel_use[p] = 0 /\ Cardinality(pool_present) > 1)
       IN /\ active_attempt' = NoneLink
          /\ peer_in_use' = rel_use
          /\ peer_failed' = [peer_failed EXCEPT ![p] = TRUE]
          /\ pool_present' = IF evict THEN pool_present \ {p} ELSE pool_present
          /\ baton_failures' = Min(MaxFailures, baton_failures + 1)
          /\ baton_due' = Min(MaxTime, now + 1)
          /\ baton_state' = "Wanted"
    /\ UNCHANGED <<
        now, staged_link, leased_peer, owner_link, dial_gen, was_stopped,
        issued, task_queue, waiting, inflight, reply_queue, last_reply,
        accepted_writes
       >>

\* Session owner adopts Ready link (Ready -> Idle) and resets failures/due to 0.
\* crates/tickwire-transport/src/dialer.rs:383-396.
OwnerTakeReady ==
    /\ baton_state = "Ready"
    /\ owner_link' = staged_link
    /\ staged_link' = NoneLink
    /\ baton_failures' = 0
    /\ baton_due' = 0
    /\ baton_state' = "Idle"
    /\ UNCHANGED <<
        now, active_attempt, leased_peer, pool_present, peer_in_use,
        peer_failed, dial_gen, was_stopped, issued, task_queue, waiting,
        inflight, reply_queue, last_reply, accepted_writes
       >>

\* Session owner resets non-Stopped baton back to Idle, dropping any staged link.
\* crates/tickwire-transport/src/dialer.rs:398-410.
OwnerReset ==
    /\ baton_state # "Stopped"
    /\ (baton_state # "Idle" \/ baton_failures > 0 \/ baton_due > 0)
    /\ staged_link' = NoneLink
    /\ baton_failures' = 0
    /\ baton_due' = 0
    /\ baton_state' = "Idle"
    /\ UNCHANGED <<
        now, active_attempt, leased_peer, pool_present, peer_in_use,
        peer_failed, owner_link, dial_gen, was_stopped, issued, task_queue,
        waiting, inflight, reply_queue, last_reply, accepted_writes
       >>

\* Session owner permanently stops baton (any -> Stopped), dropping any staged link.
\* crates/tickwire-transport/src/dialer.rs:412-420.
OwnerStop ==
    /\ baton_state # "Stopped"
    /\ staged_link' = NoneLink
    /\ baton_state' = "Stopped"
    /\ was_stopped' = TRUE
    /\ UNCHANGED <<
        now, baton_due, baton_failures, active_attempt, leased_peer,
        pool_present, peer_in_use, peer_failed, owner_link, dial_gen, issued,
        task_queue, waiting, inflight, reply_queue, last_reply, accepted_writes
       >>

\* Producer enqueues a REST task onto the bounded SPSC task_queue.
\* crates/tickwire-transport/src/rest_querier.rs:262-297, crates/tickwire-transport/src/rest_querier.rs:459-468.
ProducerEnqueue ==
    /\ Len(task_queue) < Capacity
    /\ \E t \in Tasks \ issued :
        /\ issued' = issued \cup {t}
        /\ task_queue' = Append(task_queue, t)
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, waiting, inflight, reply_queue,
        last_reply, accepted_writes
       >>

\* Worker pulls a task only when waiting + inflight + reply_queue < Capacity,
\* reserving a guaranteed reply slot before accepting work.
\* crates/tickwire-transport/src/rest_querier.rs:774-786.
WorkerPullTask ==
    /\ Len(task_queue) > 0
    /\ Len(waiting) + InflightCount + Len(reply_queue) < Capacity
    /\ LET t == Head(task_queue)
       IN /\ task_queue' = Tail(task_queue)
          /\ waiting' = Append(waiting, [id |-> t, qc |-> 0, next_ns |-> 0, last_fail |-> "none"])
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, inflight, reply_queue,
        last_reply, accepted_writes
       >>

\* Waiting task whose end_mono_ns has passed posts reply unconditionally to reply_queue
\* (Expired with query_count = 0 when never sent, else last 5xx/transport error).
\* crates/tickwire-transport/src/rest_querier.rs:677-725, crates/tickwire-transport/src/rest_querier.rs:789-799.
WorkerExpireWaiting ==
    /\ ExpiredWaiting # {}
    /\ LET idx == FirstExpiredIdx
           w == waiting[idx]
       IN /\ waiting' = RemoveAt(waiting, idx)
          /\ reply_queue' = Append(reply_queue, ReplyFromWaitingExpiry(w))
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, inflight,
        last_reply, accepted_writes
       >>

\* Unbuildable request posts RestStatus::NotBuilt with query_count == 0.
\* crates/tickwire-transport/src/rest_querier.rs:815-854.
WorkerNotBuilt ==
    /\ inflight = NoneInflight
    /\ ExpiredWaiting = {}
    /\ EligibleWaiting # {}
    /\ LET idx == PickWaitingIdx
           w == waiting[idx]
       IN /\ ~Buildable(w.id)
          /\ waiting' = RemoveAt(waiting, idx)
          /\ reply_queue' = Append(reply_queue, [id |-> w.id, status |-> "NotBuilt", qc |-> w.qc])
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, inflight,
        last_reply, accepted_writes
       >>

\* Worker starts next eligible buildable task on a ready connection with counted = FALSE.
\* crates/tickwire-transport/src/rest_querier.rs:815-835, crates/tickwire-transport/src/rest_querier.rs:945-954.
WorkerStartSend ==
    /\ inflight = NoneInflight
    /\ ExpiredWaiting = {}
    /\ EligibleWaiting # {}
    /\ LET idx == PickWaitingIdx
           w == waiting[idx]
       IN /\ Buildable(w.id)
          /\ waiting' = RemoveAt(waiting, idx)
          /\ inflight' = [id |-> w.id, qc |-> w.qc, counted |-> FALSE]
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, reply_queue,
        last_reply, accepted_writes
       >>

\* Zero-byte transport failure before accepted_bytes > 0 leaves query_count unchanged.
\* crates/tickwire-transport/src/rest_querier.rs:1118-1157, crates/tickwire-transport/src/rest_querier.rs:1293-1359.
WorkerZeroByteFail ==
    /\ inflight # NoneInflight
    /\ ~inflight.counted
    /\ LET id == inflight.id
           qc == inflight.qc
           next_ns == now + 1
           final_st ==
               IF qc = 0 /\ EndNs(id) > 0 /\ (now >= EndNs(id) \/ next_ns >= EndNs(id))
               THEN "Expired"
               ELSE "SendFail"
       IN /\ inflight' = NoneInflight
          /\ IF CanRetry(id, qc, next_ns)
             THEN /\ waiting' = Append(waiting, [id |-> id, qc |-> qc, next_ns |-> next_ns, last_fail |-> "Transport"])
                  /\ UNCHANGED reply_queue
             ELSE /\ reply_queue' = Append(reply_queue, [id |-> id, status |-> final_st, qc |-> qc])
                  /\ UNCHANGED waiting
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, last_reply,
        accepted_writes
       >>

\* Transport accepts > 0 request bytes: sync_counted_send latches counted_send and
\* increments query_count exactly once for this attempt.
\* crates/tickwire-transport/src/rest_querier.rs:1218-1227, crates/tickwire-transport/src/rest_querier.rs:1299-1338.
WorkerAcceptBytes ==
    /\ inflight # NoneInflight
    /\ ~inflight.counted
    /\ inflight' = [inflight EXCEPT !.counted = TRUE, !.qc = @ + 1]
    /\ accepted_writes' = [accepted_writes EXCEPT ![inflight.id] = @ + 1]
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, waiting,
        reply_queue, last_reply
       >>

\* Post-accept transport failure or HTTP 5xx retries up to CanRetry or posts final error.
\* crates/tickwire-transport/src/rest_querier.rs:1118-1157, crates/tickwire-transport/src/rest_querier.rs:1187-1215.
WorkerRetryOrFailAfterSend ==
    /\ inflight # NoneInflight
    /\ inflight.counted
    /\ \E kind \in {"Transport", "Http5xx"} :
        LET id == inflight.id
            qc == inflight.qc
            next_ns == now + 1
            term_st == IF kind = "Http5xx" THEN "HttpError" ELSE "Transport"
        IN /\ inflight' = NoneInflight
           /\ IF CanRetry(id, qc, next_ns)
              THEN /\ waiting' = Append(waiting, [id |-> id, qc |-> qc, next_ns |-> next_ns, last_fail |-> kind])
                   /\ UNCHANGED reply_queue
              ELSE /\ reply_queue' = Append(reply_queue, [id |-> id, status |-> term_st, qc |-> qc])
                   /\ UNCHANGED waiting
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, last_reply,
        accepted_writes
       >>

\* HTTP 2xx (Ok) or non-retryable 4xx (HttpError) posts terminal reply immediately.
\* crates/tickwire-transport/src/rest_querier.rs:1172-1185, crates/tickwire-transport/src/rest_querier.rs:1205-1215.
WorkerCompleteResponse ==
    /\ inflight # NoneInflight
    /\ inflight.counted
    /\ \E st \in {"Ok", "HttpError"} :
        /\ reply_queue' = Append(reply_queue, [id |-> inflight.id, status |-> st, qc |-> inflight.qc])
        /\ inflight' = NoneInflight
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, waiting,
        last_reply, accepted_writes
       >>

\* Consumer (RESP) dequeues a completed reply; no fairness is assumed so consumer
\* may stall arbitrarily while the worker executes.
\* crates/tickwire-transport/src/rest_querier.rs:299-337, crates/tickwire-transport/src/rest_querier.rs:470-476.
ConsumerDequeue ==
    /\ Len(reply_queue) > 0
    /\ last_reply' = Head(reply_queue)
    /\ reply_queue' = Tail(reply_queue)
    /\ UNCHANGED <<
        now, baton_state, baton_due, baton_failures, staged_link,
        active_attempt, leased_peer, pool_present, peer_in_use, peer_failed,
        owner_link, dial_gen, was_stopped, issued, task_queue, waiting,
        inflight, accepted_writes
       >>

Next ==
    \/ Tick
    \/ OwnerWant
    \/ DialerClaim
    \/ DialerHandshakeStep
    \/ DialerDeliverReady
    \/ DialerAbortStale
    \/ DialerFail
    \/ OwnerTakeReady
    \/ OwnerReset
    \/ OwnerStop
    \/ ProducerEnqueue
    \/ WorkerPullTask
    \/ WorkerExpireWaiting
    \/ WorkerNotBuilt
    \/ WorkerStartSend
    \/ WorkerZeroByteFail
    \/ WorkerAcceptBytes
    \/ WorkerRetryOrFailAfterSend
    \/ WorkerCompleteResponse
    \/ ConsumerDequeue

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ now \in 0..MaxTime
    /\ baton_state \in BatonStates
    /\ baton_due \in 0..MaxTime
    /\ baton_failures \in 0..MaxFailures
    /\ staged_link \in LinkValues
    /\ active_attempt \in LinkValues
    /\ leased_peer \in 0..2
    /\ pool_present \subseteq Peers
    /\ peer_in_use \in [Peers -> 0..2]
    /\ peer_failed \in [Peers -> BOOLEAN]
    /\ owner_link \in LinkValues
    /\ dial_gen \in 0..MaxDialGen
    /\ was_stopped \in BOOLEAN
    /\ issued \subseteq Tasks
    /\ Len(task_queue) <= Capacity
    /\ \A i \in 1..Len(task_queue) : task_queue[i] \in Tasks
    /\ \A i \in 1..Len(waiting) : waiting[i] \in WaitingElemValues
    /\ inflight \in InflightValues
    /\ \A i \in 1..Len(reply_queue) : reply_queue[i] \in ReplyElemValues
    /\ last_reply \in ({NoneReply} \cup ReplyElemValues)
    /\ accepted_writes \in [Tasks -> 0..MaxCountBound]

\* Owner (Idle, Ready) and Dialer (Wanted, Dialing) never simultaneously own or mutate
\* the staged DialedLink; Stopped is permanently terminal; IpPool leases are conserved.
ExclusiveBatonOwnership ==
    /\ (staged_link # NoneLink <=> baton_state = "Ready")
    /\ (baton_state \in {"Wanted", "Dialing"} => staged_link = NoneLink)
    /\ (staged_link # NoneLink => active_attempt = NoneLink)
    /\ (owner_link # NoneLink /\ staged_link # NoneLink => owner_link.gen # staged_link.gen)
    /\ (owner_link # NoneLink /\ active_attempt # NoneLink => owner_link.gen # active_attempt.gen)
    /\ (was_stopped => baton_state = "Stopped" /\ staged_link = NoneLink)
    /\ Cardinality(pool_present) >= 1
    /\ \A p \in Peers :
        peer_in_use[p] =
            (IF active_attempt # NoneLink /\ active_attempt.peer = p THEN 1 ELSE 0)
            + (IF leased_peer = p /\ p \in pool_present THEN 1 ELSE 0)

\* Every staged or adopted connection completed TCP/TLS handshake in its dial
\* generation, and take_ready resets failures = 0 and due_mono_ns = 0 on Idle return.
FreshLinkHandoff ==
    /\ (staged_link # NoneLink =>
            /\ staged_link.handshaked
            /\ staged_link.gen = dial_gen)
    /\ (owner_link # NoneLink =>
            /\ owner_link.handshaked
            /\ owner_link.gen <= dial_gen)
    /\ (baton_state = "Idle" => baton_failures = 0 /\ baton_due = 0)

\* Worker reply-slot reservation guarantees waiting + inflight + replies <= Capacity
\* and reply_queue never overflows even when ConsumerDequeue stalls indefinitely.
ReplySlotReservation ==
    /\ Len(waiting) + InflightCount + Len(reply_queue) <= Capacity
    /\ Len(reply_queue) <= Capacity

\* query_count increments iff accepted_bytes > 0 on the transport; Expired and NotBuilt
\* replies always carry query_count = 0; bounded MaxQueryCount is never exceeded.
CountedWritesOnly ==
    /\ \A i \in 1..Len(waiting) :
        /\ waiting[i].qc = accepted_writes[waiting[i].id]
        /\ (MaxQueryCount > 0 => waiting[i].qc <= MaxQueryCount)
    /\ (inflight # NoneInflight =>
        /\ inflight.qc = accepted_writes[inflight.id]
        /\ (MaxQueryCount > 0 => inflight.qc <= MaxQueryCount))
    /\ \A i \in 1..Len(reply_queue) :
        /\ reply_queue[i].qc = accepted_writes[reply_queue[i].id]
        /\ (reply_queue[i].status \in {"Expired", "NotBuilt"} => reply_queue[i].qc = 0)
        /\ (MaxQueryCount > 0 => reply_queue[i].qc <= MaxQueryCount)
    /\ (last_reply # NoneReply =>
        /\ last_reply.qc = accepted_writes[last_reply.id]
        /\ (last_reply.status \in {"Expired", "NotBuilt"} => last_reply.qc = 0)
        /\ (MaxQueryCount > 0 => last_reply.qc <= MaxQueryCount))
=============================================================================
