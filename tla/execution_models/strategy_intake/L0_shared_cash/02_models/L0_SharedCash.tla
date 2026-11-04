------------------------- MODULE L0_SharedCash -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Cash, RestartLimit
(*
  9b2ed63. Two distinct FileStrategy logical IDs, unique clients/local IDs.
  layout[m] is the second descriptor in m's immutable queue, after its BUY.
  cursor and turn: FileStrategy.sequence + MultiStrategy.next, original queue order.
    crates/tickwire-app/src/strategy.rs:243-275 crates/tickwire-app/src/strategy.rs:628-669
  enabled: slots with a live FileStrategy. phase: live loop, restored freeze or exit.
    crates/tickwire-app/src/strategy.rs:468-496 crates/tickwire-spot-runner/src/live.rs:321-345
  wal[k]: durable admission and local abort. reserved: common quote-asset units held.
    crates/tickwire-account-gateway/src/lib.rs:421-435 crates/tickwire-account-gateway/src/lib.rs:464-511
    crates/tickwire-account/src/admission.rs:208-233
  restKey and runnerKey: two volatile pending records in different owners.
    crates/tickwire-spot-execution/src/session.rs:145-173 crates/tickwire-spot-runner/src/lib.rs:260-280
  accepted is volatile output progress, reset for each new request.
  sent[k] is an independent ghost counting completed native sends. receipts records which member actually gets its reply.
    crates/tickwire-spot-execution/src/session.rs:219-239 crates/tickwire-app/src/strategy.rs:685-695
  submittedBy[k] is a ghost recording the producing queue BEFORE reply routing.
  offender is the member whose pre-admission target failed, for a peer availability probe.
  restarts counts fault injections. On reopen both configured queues start at sequence 1.
    crates/tickwire-spot-runner/src/live/config.rs:91-106 crates/tickwire-account-gateway/src/lib.rs:357-384
*)
M == 1..2
Keys == 1..4
Other(m) == 3 - m
Owner(k) == IF k \in M THEN k ELSE k - 2
Kinds == {"repeat", "ownCancel", "foreignCancel", "spoof"}
VARIABLES layout, cursor, turn, enabled, phase, wal, reserved, runnerKey,
          restKey, accepted, sent, submittedBy, receipts, offender, restarts
vars == <<layout,cursor,turn,enabled,phase,wal,reserved,runnerKey,restKey,
          accepted,sent,submittedBy,receipts,offender,restarts>>
Init ==
    /\ layout \in [M -> Kinds] /\ cursor = [m \in M |-> 1] /\ turn \in M
    /\ enabled = M /\ phase = "run" /\ wal = [k \in Keys |-> "absent"]
    /\ reserved = 0 /\ runnerKey = 0 /\ restKey = 0 /\ accepted = FALSE
    /\ sent = [k \in Keys |-> 0] /\ submittedBy = [k \in Keys |-> 0]
    /\ receipts = {} /\ offender = 0 /\ restarts = 0

Ready == {m \in enabled : cursor[m] <= 2}
Selected == IF turn \in Ready THEN turn ELSE Other(turn)
Kind(m) == IF cursor[m] = 1 THEN "buy" ELSE layout[m]
Key(m) == IF Kind(m) \in {"buy","repeat"} THEN m ELSE 2 + m
Target(m) == IF Kind(m) = "foreignCancel" THEN Other(m) ELSE m
\* Zero-byte-aborted targets have an account order record. Its check_target may still
\* pass. Exclude that request profile, rather than equating aborted with unknown.
Supported(m) == Kind(m) \notin {"ownCancel","foreignCancel"} \/ wal[Target(m)] # "aborted"
Intake(m) == /\ Supported(m) /\ phase = "run" /\ runnerKey = 0 /\ m \in Ready /\ m = Selected
Advance(m) == /\ cursor' = [cursor EXCEPT ![m] = @ + 1] /\ turn' = Other(m)

\* Callback increments only the producing file cursor, even on refusal.
\* crates/tickwire-app/src/strategy.rs:339-353 crates/tickwire-app/src/strategy.rs:673-683
\* Exact gateway retry returns immediately, without assigning either pending slot.
\* crates/tickwire-spot-runner/src/lib.rs:256-265 crates/tickwire-account-gateway/src/lib.rs:393-400
Replay == \E m \in M :
    /\ Intake(m) /\ Kind(m) # "spoof" /\ wal[Key(m)] # "absent" /\ Advance(m)
    /\ UNCHANGED <<layout,enabled,phase,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,offender,restarts>>

\* Mislabelled descriptor drops just that file member under a successful grant withdrawal.
\* crates/tickwire-app/src/strategy.rs:644-669 crates/tickwire-app/src/strategy.rs:468-484
DropWrongMember == \E m \in M :
    /\ Intake(m) /\ Kind(m) = "spoof" /\ enabled' = enabled \ {m} /\ turn' = Other(m)
    /\ UNCHANGED <<layout,cursor,phase,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,offender,restarts>>

\* A new command from an old writer cannot be freshly admitted after restart.
\* crates/tickwire-account-gateway/src/binding.rs:33-49 crates/tickwire-spot-runner/src/lib.rs:136-147
RejectOldEpoch == \E m \in M :
    /\ Intake(m) /\ Kind(m) # "spoof" /\ wal[Key(m)] = "absent" /\ restarts > 0
    /\ Advance(m) /\ phase' = "exit"
    /\ UNCHANGED <<layout,enabled,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,offender,restarts>>

\* Known live order, wrong logical owner, or unidentified target. For the availability
\* witness only the former (with both orders already acknowledged) qualifies.
\* crates/tickwire-spot-runner/src/lib.rs:212-256 crates/tickwire-account/src/admission.rs:369-396
\* crates/tickwire-spot-runner/src/lib.rs:128-147 crates/tickwire-spot-runner/src/live.rs:330-345
RejectTarget == \E m \in M :
    /\ Intake(m) /\ Kind(m) \in {"ownCancel","foreignCancel"} /\ restarts = 0
    /\ wal[Key(m)] = "absent"
    /\ (Target(m) # m \/ wal[Target(m)] # "held")
    /\ Advance(m) /\ phase' = "exit" /\ offender' = m
    /\ UNCHANGED <<layout,enabled,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,restarts>>

\* crates/tickwire-account/src/admission.rs:16-31 crates/tickwire-account/src/admission.rs:208-233
\* crates/tickwire-spot-runner/src/lib.rs:136-146
RefuseFunds == \E m \in M :
    /\ Intake(m) /\ Kind(m) \in {"buy","repeat"} /\ wal[m] = "absent"
    /\ restarts = 0 /\ reserved = Cash /\ Advance(m)
    /\ UNCHANGED <<layout,enabled,phase,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,offender,restarts>>

\* Successful durability and object checks are one account-owner turn.
\* crates/tickwire-account-gateway/src/lib.rs:421-435 crates/tickwire-account-gateway/src/lib.rs:464-511
\* crates/tickwire-account/src/admission.rs:85-117 crates/tickwire-spot-runner/src/lib.rs:266-280
Admit == \E m \in M : LET k == Key(m) IN
    /\ Intake(m) /\ restarts = 0 /\ wal[k] = "absent"
    /\ IF k \in M THEN reserved < Cash ELSE Kind(m) = "ownCancel" /\ wal[m] = "held"
    /\ Advance(m) /\ wal' = [wal EXCEPT ![k] = "held"]
    /\ reserved' = IF k \in M THEN reserved + 1 ELSE reserved
    /\ submittedBy' = [submittedBy EXCEPT ![k] = m] /\ accepted' = FALSE /\ runnerKey' = k /\ restKey' = k
    /\ UNCHANGED <<layout,enabled,phase,sent,receipts,offender,restarts>>

\* Complete acceptance of a short native request, with final authority satisfied.
\* crates/tickwire-account-gateway/src/lib.rs:564-586 crates/tickwire-account-gateway/src/lib.rs:1137-1168
Send == /\ phase = "run" /\ restKey # 0 /\ ~accepted
        /\ accepted' = TRUE /\ sent' = [sent EXCEPT ![restKey] = @ + 1]
        /\ UNCHANGED <<layout,cursor,turn,enabled,phase,wal,reserved,runnerKey,restKey,submittedBy,receipts,offender,restarts>>
\* No accepted bytes and final grant denied. Sync the abort before releasing collateral.
\* crates/tickwire-account-gateway/src/lib.rs:1175-1198 crates/tickwire-spot-execution/src/session.rs:241-245
\* crates/tickwire-spot-runner/src/lib.rs:312-318
Abort == /\ phase = "run" /\ restKey # 0 /\ ~accepted
         /\ wal' = [wal EXCEPT ![restKey] = "aborted"]
         /\ reserved' = IF restKey \in M THEN reserved - 1 ELSE reserved
         /\ accepted' = FALSE /\ runnerKey' = 0 /\ restKey' = 0
         /\ UNCHANGED <<layout,cursor,turn,enabled,phase,accepted,sent,submittedBy,receipts,offender,restarts>>
\* Session matches native IDs against its command; runner reloads its own key from the WAL.
\* crates/tickwire-spot-execution/src/reports.rs:20-55 crates/tickwire-spot-execution/src/session.rs:219-239
\* crates/tickwire-spot-runner/src/lib.rs:290-310 crates/tickwire-app/src/strategy.rs:685-695
Ack == /\ phase = "run" /\ restKey # 0 /\ accepted
       /\ receipts' = receipts \cup {<<restKey,Owner(runnerKey)>>}
       /\ accepted' = FALSE /\ runnerKey' = 0 /\ restKey' = 0
       /\ UNCHANGED <<layout,cursor,turn,enabled,phase,wal,reserved,sent,submittedBy,offender,restarts>>

\* Reopen after a settled request or after a fully sent buy. Recovery starts frozen.
\* crates/tickwire-account-gateway/src/lib.rs:357-384 crates/tickwire-spot-runner/src/lib.rs:67-78
\* crates/tickwire-app/src/strategy.rs:252-276 crates/tickwire-spot-runner/src/live/config.rs:91-105
Restart == /\ phase = "run" /\ restarts < RestartLimit /\ (restKey = 0 \/ (restKey \in M /\ accepted))
           /\ \A k \in 3..4 : sent[k] = 0
           /\ restarts' = restarts + 1 /\ phase' = "frozen" /\ accepted' = FALSE /\ restKey' = 0 /\ runnerKey' = 0
           /\ cursor' = [m \in M |-> 1] /\ turn' \in M /\ enabled' = M
           /\ UNCHANGED <<layout,wal,reserved,sent,submittedBy,receipts,offender>>
\* Choose only the branch where every sent buy is still open, fills are empty and two
\* verified scans agree with cash/locks. Other outcomes leave the owner frozen.
\* crates/tickwire-spot-runner/src/live/account.rs:136-160
Requalify == /\ phase = "frozen" /\ phase' = "run"
             /\ UNCHANGED <<layout,cursor,turn,enabled,wal,reserved,runnerKey,restKey,accepted,sent,submittedBy,receipts,offender,restarts>>
Next == Replay \/ DropWrongMember \/ RejectOldEpoch \/ RejectTarget \/ RefuseFunds \/ Admit
        \/ Send \/ Abort \/ Ack \/ Restart \/ Requalify
Spec == Init /\ [][Next]_vars
CashSafe == reserved \in 0..Cash /\ reserved = Cardinality({k \in M : wal[k] = "held"})
BoundPending == runnerKey = restKey /\ (restKey # 0 => wal[restKey] = "held")
ExactlyOnce == \A k \in Keys : sent[k] <= 1 /\ (sent[k] > 0 => wal[k] = "held")
CorrectOwner == \A pair \in receipts : /\ submittedBy[pair[1]] = pair[2] /\ sent[pair[1]] = 1
CancelAuthority == \A m \in M : wal[2 + m] # "absent" => layout[m] = "ownCancel" /\ wal[m] = "held"
\* Diagnostic expectation: one clean pre-admission foreign-order error leaves a
\* valid, ready sibling able to continue on the same live process.
PeerCanContinue == ~(phase = "exit" /\ offender \in M /\ layout[offender] = "foreignCancel"
                    /\ layout[Other(offender)] = "ownCancel" /\ cursor[Other(offender)] = 2
                    /\ <<1,1>> \in receipts /\ <<2,2>> \in receipts)
=============================================================================
