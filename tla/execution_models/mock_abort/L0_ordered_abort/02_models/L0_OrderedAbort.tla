------------------------- MODULE L0_OrderedAbort -------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* var <-> code map; all paths relative to repository root.
\* live/durable/frozen: Engine order/send/terminal/durable + global frozen
\*   crates/tickwire-execution/src/lib.rs:138-153,276-339,351-390,407-436
\* journal: actual MockJournal.events, not grouped views
\*   crates/tickwire-mock/src/lib.rs:76-160
\* pc/cur: serial &mut MockRuntime call stack, crates/tickwire-mock/src/lib.rs:225-277
\* rebuilt/pos: local Engine and next event in restart, crates/tickwire-mock/src/lib.rs:280-291
\* used/sent: observers for finite unique IDs and Exchange.submit count
\*   crates/tickwire-mock/src/lib.rs:177-185; no extra authorization is introduced.
\* crashes: bounded tolerated interruption; journal and exchange survive restart
\*   crates/tickwire-mock/src/lib.rs:279-292
\* Constants: finite orders, positive budgets, arithmetic bound, crash bound.
\* Fixed valid request/profile/dependencies; zero fills/fees; no reauthorization.
CONSTANTS OrderCount, Capital, HeavyBudget, AmountMax, MaxCrashes
Orders == 1..OrderCount
Budget(o) == IF o = 1 THEN 1 ELSE HeavyBudget
States == {"None", "Reserved", "Pending", "Confirmed", "Abort", "Terminal"}
HeldStates == {"Reserved", "Pending", "Confirmed"}
Empty == [o \in Orders |-> "None"]
RECURSIVE HeldPrefix(_, _)
HeldPrefix(s, n) == IF n = 0 THEN 0 ELSE HeldPrefix(s, n-1) +
                   (IF s[n] \in HeldStates THEN Budget(n) ELSE 0)
Held(s) == HeldPrefix(s, OrderCount)
Event(kind, o) == [kind |-> kind, order |-> o]
Has(j, kind, o) == \E k \in 1..Len(j): j[k] = Event(kind, o)
Released(j, o) == Has(j, "Abort", o) \/ Has(j, "Terminal", o)
VARIABLES live, durable, frozen, journal, pc, cur, used, sent, rebuilt, pos, crashes
vars == <<live, durable, frozen, journal, pc, cur, used, sent, rebuilt, pos, crashes>>
Init == /\ live = Empty /\ durable = {} /\ frozen = FALSE /\ journal = <<>>
        /\ pc = "Idle" /\ cur = 1 /\ used = {} /\ sent = [o \in Orders |-> 0]
        /\ rebuilt = Empty /\ pos = 1 /\ crashes = 0

\* MockRuntime.submit + Engine.reserve: crates/tickwire-mock/src/lib.rs:225-233;
\* crates/tickwire-execution/src/lib.rs:276-299. Legal dependencies abstracted successful.
Reserve(o) == /\ pc = "Idle" /\ ~frozen /\ o \notin used
              /\ Held(live) + Budget(o) <= Capital
              /\ live' = [live EXCEPT ![o] = "Reserved"] /\ used' = used \cup {o}
              /\ pc' = "Commit" /\ cur' = o
              /\ UNCHANGED <<durable, frozen, journal, sent, rebuilt, pos, crashes>>
\* resume_submit; guards that can reach a successful send, crates/tickwire-mock/src/lib.rs:235-242.
Retry(o) == /\ pc = "Idle" /\ ~frozen /\ live[o] = "Reserved"
            /\ pc' = "Commit" /\ cur' = o
            /\ UNCHANGED <<live, durable, frozen, journal, used, sent, rebuilt, pos, crashes>>
\* MockJournal.commit failure before append: crates/tickwire-mock/src/lib.rs:131-133.
CommitFail == /\ pc = "Commit" /\ pc' = "Idle"
              /\ UNCHANGED <<live, durable, frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Idempotent exact record; no second Intent event. crates/tickwire-mock/src/lib.rs:137-147.
PersistIntent == /\ pc = "Commit"
                 /\ journal' = IF Has(journal, "Intent", cur) THEN journal
                               ELSE Append(journal, Event("Intent", cur))
                 /\ pc' = "Receipt"
                 /\ UNCHANGED <<live, durable, frozen, cur, used, sent, rebuilt, pos, crashes>>
\* Loss after persistence: crates/tickwire-mock/src/lib.rs:149-151.
LoseReceipt == /\ pc = "Receipt" /\ pc' = "Idle"
               /\ UNCHANGED <<live, durable, frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Matching LocalDurable receipt: crates/tickwire-execution/src/lib.rs:308-320.
AcceptReceipt == /\ pc = "Receipt" /\ durable' = durable \cup {cur} /\ pc' = "Begin"
                 /\ UNCHANGED <<live, frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Before handing bytes to adapter: crates/tickwire-execution/src/lib.rs:322-339.
BeginSend == /\ pc = "Begin" /\ live[cur] = "Reserved" /\ cur \in durable /\ ~frozen
             /\ live' = [live EXCEPT ![cur] = "Pending"] /\ pc' = "Dispatch"
             /\ UNCHANGED <<durable, frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Exchange + callback: crates/tickwire-mock/src/lib.rs:177-185,243-248.
Dispatch(outcome) ==
  /\ pc = "Dispatch"
  /\ sent' = [sent EXCEPT ![cur] = @ + 1]
  /\ live' = [live EXCEPT ![cur] = IF outcome = "Ack" THEN "Confirmed" ELSE "Pending"]
  /\ frozen' = (frozen \/ outcome = "Unknown")
  /\ pc' = IF outcome = "NoBytes" THEN "AbortAppend" ELSE "Idle"
  /\ UNCHANGED <<durable, journal, cur, used, rebuilt, pos, crashes>>
\* Valid zero-cumulative terminal; crates/tickwire-mock/src/lib.rs:267-277.
StartTerminal(o) == /\ pc = "Idle" /\ live[o] \in {"Pending", "Confirmed"}
                    /\ pc' = "TerminalAppend" /\ cur' = o
                    /\ UNCHANGED <<live, durable, frozen, journal, used, sent, rebuilt, pos, crashes>>
\* append_fact failure, retain hold and freeze: crates/tickwire-mock/src/lib.rs:117-120,251-255.
FactFail == /\ pc \in {"AbortAppend", "TerminalAppend"} /\ pc' = "Idle" /\ frozen' = TRUE
            /\ UNCHANGED <<live, durable, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Both fact kinds use append before apply: crates/tickwire-mock/src/lib.rs:121-123,251-258.
PersistRelease == /\ pc \in {"AbortAppend", "TerminalAppend"}
                  /\ journal' = Append(journal, Event(IF pc = "AbortAppend" THEN "Abort" ELSE "Terminal", cur))
                  /\ pc' = IF pc = "AbortAppend" THEN "AbortApply" ELSE "TerminalApply"
                  /\ UNCHANGED <<live, durable, frozen, cur, used, sent, rebuilt, pos, crashes>>
\* Exact identity is fixed in this model. NoBytes and zero-fill terminal reducers:
\* crates/tickwire-execution/src/ports.rs:76-108; crates/tickwire-execution/src/lib.rs:359-372,407-436.
ApplyRelease == /\ pc \in {"AbortApply", "TerminalApply"}
                /\ live' = [live EXCEPT ![cur] = IF pc = "AbortApply" THEN "Abort" ELSE "Terminal"]
                /\ pc' = "Idle"
                /\ UNCHANGED <<durable, frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
\* Freeze original, allocate local recovery engine: crates/tickwire-mock/src/lib.rs:279-284.
Crash == /\ crashes < MaxCrashes /\ crashes' = crashes + 1
         /\ frozen' = TRUE /\ rebuilt' = Empty /\ pos' = 1 /\ pc' = "Recover"
         /\ UNCHANGED <<live, durable, journal, cur, used, sent>>
\* Ordered replay, not grouped projections. Every event checks its current predecessor.
\* crates/tickwire-mock/src/lib.rs:285-289; crates/tickwire-execution/src/lib.rs:504-528;
\* crates/tickwire-execution/src/ports.rs:76-108.
ReplayOne ==
  /\ pc = "Recover" /\ pos <= Len(journal)
  /\ LET ev == journal[pos]
         o == ev.order
         valid == IF ev.kind = "Intent"
                  THEN rebuilt[o] = "None" /\ Held(rebuilt) + Budget(o) <= AmountMax
                  ELSE rebuilt[o] = "Pending"
     IN /\ rebuilt' = IF valid THEN [rebuilt EXCEPT ![o] = IF ev.kind = "Intent" THEN "Pending" ELSE ev.kind]
                      ELSE rebuilt
        /\ pc' = IF valid THEN "Recover" ELSE "Failed"
  /\ pos' = pos + 1
  /\ UNCHANGED <<live, durable, frozen, journal, cur, used, sent, crashes>>
\* Publish only complete reconstruction: crates/tickwire-mock/src/lib.rs:291-292.
FinishRecovery == /\ pc = "Recover" /\ pos > Len(journal)
                  /\ live' = rebuilt /\ durable' = {o \in Orders: Has(journal, "Intent", o)}
                  /\ pc' = "Idle"
                  /\ UNCHANGED <<frozen, journal, cur, used, sent, rebuilt, pos, crashes>>
Next == (\E o \in Orders: Reserve(o) \/ Retry(o) \/ StartTerminal(o))
        \/ CommitFail \/ PersistIntent \/ LoseReceipt \/ AcceptReceipt \/ BeginSend
        \/ (\E outcome \in {"Ack", "Pending", "Unknown", "NoBytes"}: Dispatch(outcome))
        \/ FactFail \/ PersistRelease \/ ApplyRelease \/ Crash \/ ReplayOne \/ FinishRecovery
Spec == Init /\ [][Next]_vars

TypeOK == /\ live \in [Orders -> States] /\ rebuilt \in [Orders -> States]
          /\ durable \subseteq Orders /\ used \subseteq Orders /\ frozen \in BOOLEAN
          /\ journal \in Seq([kind : {"Intent", "Abort", "Terminal"}, order : Orders])
          /\ Len(journal) <= 2 * OrderCount /\ cur \in Orders /\ pos \in 1..(2*OrderCount+1)
          /\ sent \in [Orders -> 0..1] /\ crashes \in 0..MaxCrashes
          /\ pc \in {"Idle", "Commit", "Receipt", "Begin", "Dispatch", "AbortAppend", "AbortApply",
                     "TerminalAppend", "TerminalApply", "Recover", "Failed"}
PersistBeforeRelease == \A o \in Orders: live[o] = "Abort" => Has(journal, "Abort", o)
SendHasIntent == \A o \in Orders: sent[o] > 0 => Has(journal, "Intent", o)
NoDoubleSend == \A o \in Orders: sent[o] <= 1
ReleaseVisible == pc = "Idle" => \A o \in Orders: Released(journal, o) => live[o] \in {"Abort", "Terminal"}
UnresolvedHeld == pc = "Idle" => \A o \in Orders:
                 (Has(journal, "Intent", o) /\ ~Released(journal, o)) => live[o] \in HeldStates
ReplayCannotFail == pc # "Failed"
RecoveryFrozen == crashes > 0 => frozen
BoundedLiability == Held(live) <= AmountMax /\ Held(rebuilt) <= AmountMax
=============================================================================
