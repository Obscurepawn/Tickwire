---------------------------- MODULE L1_CrashReceipt ----------------------------
EXTENDS Naturals, FiniteSets, TLC

\* var <-> code map; Init: crates/tickwire-mock/src/lib.rs:199-208; crates/tickwire-execution/src/lib.rs:219-232.
\* present, profile, seq, send, terminal, durable, held, nextSeq:
\*   Engine.orders/Order fields/next_sequence, crates/tickwire-execution/src/lib.rs:138
\*   seq DENOTES the identity of an intent journal record, NOT an exchange order ID.
\* objects: immutable recovery-object bytes abstracted to request profile,
\*   crates/tickwire-mock/src/lib.rs:54; request fields besides profile are fixed by intent.
\* journal, journalProfile, journalSeq: successfully appended DurableRecord content,
\*   crates/tickwire-mock/src/lib.rs:116; journal is a content set, NOT a receipt watermark.
\* journalReplica: history of truthful replica_durable receipt evidence,
\*   crates/tickwire-mock/src/lib.rs:140 (mock only; no real disk/replication proof).
\* pc, active, callProfile, receiptReplica, response: runtime stack; outcomes: crates/tickwire-execution/src/ports.rs:21-27,
\*   crates/tickwire-mock/src/lib.rs:210 and crates/tickwire-mock/src/lib.rs:220.
\* gateOpen: stored Gate::permits conjunction, crates/tickwire-execution/src/lib.rs:60.
\* frozen: Engine.frozen, crates/tickwire-execution/src/lib.rs:238.
\* alive, crashes, everRestarted: exec crash/restart test harness,
\*   crates/tickwire-mock/src/lib.rs:264; only journal, objects, exchange survive.
\* beginCount: history witness at Engine::begin_send, crates/tickwire-execution/src/lib.rs:337.
\* dispatchCount, acceptedCount: MockExchange.submit_calls/accepted,
\*   crates/tickwire-mock/src/lib.rs:163.
\* ACTION ANCHORS:
\* StartSubmit -> reserve and submission: crates/tickwire-mock/src/lib.rs:210
\* StoreDependency -> put_immutable: crates/tickwire-mock/src/lib.rs:217
\* DependencyFailure -> failed put_immutable: crates/tickwire-mock/src/lib.rs:55
\* Resume -> resume_submit: crates/tickwire-mock/src/lib.rs:220
\* CommitAndDeliver/CommitAndLoseReceipt/CommitFailure -> commit: crates/tickwire-mock/src/lib.rs:116
\* AcceptReceipt/RejectReceipt -> accept_commit: crates/tickwire-execution/src/lib.rs:308
\* BeginSend/RejectBegin -> begin_send: crates/tickwire-execution/src/lib.rs:322
\* Dispatch -> exchange.submit: crates/tickwire-mock/src/lib.rs:163
\* Respond -> result branches: crates/tickwire-mock/src/lib.rs:228
\* AbortUnsent -> abort_unsent: crates/tickwire-execution/src/lib.rs:341
\* Uncertain -> uncertain/not_found: crates/tickwire-execution/src/lib.rs:374
\* ToggleGate/Freeze -> update_gate/freeze: crates/tickwire-execution/src/lib.rs:234
\* Crash -> tolerated exec failure, crates/tickwire-mock/src/lib.rs:264
\* Restart -> freeze then recover_intent: crates/tickwire-mock/src/lib.rs:265
\*   and crates/tickwire-execution/src/lib.rs:504.
\* FAULTS: journal failure, durable-write receipt loss, unknown transport outcome,
\* exec crash at every call boundary. NOT: journal loss, forged completion, Byzantine data.
\* Full-body drift anchors: crates/tickwire-execution/src/lib.rs:60-71
\* crates/tickwire-execution/src/lib.rs:138-181
\* crates/tickwire-execution/src/lib.rs:234-239
\* crates/tickwire-execution/src/lib.rs:262-392
\* crates/tickwire-execution/src/lib.rs:504-528
\* crates/tickwire-mock/src/lib.rs:54-73
\* crates/tickwire-mock/src/lib.rs:116-145
\* crates/tickwire-mock/src/lib.rs:163-170
\* crates/tickwire-mock/src/lib.rs:210-235
\* crates/tickwire-mock/src/lib.rs:264-276

CONSTANTS Orders, Slots, Budget, Capital, MaxCrashes
Profiles == {"Local", "Replica"}
Outcomes == {"Pending", "Ack", "Unknown", "NoBytes"}
PCs == {"Idle", "Reserved", "Ready", "Receipt", "Accepted", "Started", "Respond", "Down"}
VARIABLES present, profile, seq, send, terminal, durable, held, nextSeq,
          objects, journal, journalProfile, journalSeq, journalReplica,
          pc, active, callProfile, receiptReplica, response, gateOpen, frozen,
          alive, crashes, everRestarted, beginCount, dispatchCount, acceptedCount
orderVars == <<present, profile, seq, send, terminal, durable, held, nextSeq>>
storeVars == <<objects, journal, journalProfile, journalSeq, journalReplica>>
historyVars == <<beginCount, dispatchCount, acceptedCount, everRestarted>>
vars == <<orderVars, storeVars, pc, active, callProfile, receiptReplica, response,
          gateOpen, frozen, alive, crashes, historyVars>>
TotalHeld == Cardinality({o \in Orders : held[o] = Budget}) * Budget
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init == /\ present = {} /\ profile = [o \in Orders |-> "Local"]
        /\ seq = [o \in Orders |-> 0] /\ send = [o \in Orders |-> "Unsent"]
        /\ terminal = {} /\ durable = {} /\ held = [o \in Orders |-> 0]
        /\ nextSeq = 1
        /\ objects = [o \in Orders |-> "None"] /\ journal = {}
        /\ journalProfile = [o \in Orders |-> "Local"]
        /\ journalSeq = [o \in Orders |-> 0] /\ journalReplica = {}
        /\ pc = "Idle" /\ active = "None" /\ callProfile = "Local"
        /\ receiptReplica = FALSE /\ response = "Unknown"
        /\ gateOpen = TRUE /\ frozen = FALSE /\ alive = TRUE /\ crashes = 0
        /\ everRestarted = FALSE
        /\ beginCount = [o \in Orders |-> 0]
        /\ dispatchCount = [o \in Orders |-> 0]
        /\ acceptedCount = [o \in Orders |-> 0]

StartSubmit(o, p, cp) ==
    /\ pc = "Idle" /\ o \notin present /\ Cardinality(present) < Slots
    /\ gateOpen /\ ~frozen /\ TotalHeld + Budget <= Capital
    /\ present' = present \cup {o} /\ profile' = [profile EXCEPT ![o] = p]
    /\ seq' = [seq EXCEPT ![o] = nextSeq] /\ nextSeq' = nextSeq + 1
    /\ held' = [held EXCEPT ![o] = Budget]
    /\ pc' = "Reserved" /\ active' = o /\ callProfile' = cp
    /\ UNCHANGED <<send, terminal, durable, storeVars, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

StoreDependency ==
    /\ pc = "Reserved" /\ objects[active] \in {"None", profile[active]}
    /\ objects' = [objects EXCEPT ![active] = profile[active]] /\ pc' = "Ready"
    /\ UNCHANGED <<orderVars, journal, journalProfile, journalSeq, journalReplica,
                   active, callProfile, receiptReplica, response, gateOpen, frozen,
                   alive, crashes, historyVars>>

DependencyFailure ==
    /\ pc = "Reserved" /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, callProfile, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

Resume(o, cp) ==
    /\ pc = "Idle" /\ o \in present
    /\ pc' = "Ready" /\ active' = o /\ callProfile' = cp
    /\ UNCHANGED <<orderVars, storeVars, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

WriteCommitted(replicated) ==
    /\ pc = "Ready" /\ objects[active] = profile[active]
    /\ (active \in journal =>
           (journalProfile[active] = profile[active] /\ journalSeq[active] = seq[active]))
    /\ \A o \in journal \ {active} : journalSeq[o] # seq[active]
    /\ journal' = journal \cup {active}
    /\ journalProfile' = [journalProfile EXCEPT ![active] = profile[active]]
    /\ journalSeq' = [journalSeq EXCEPT ![active] = seq[active]]
    /\ journalReplica' = IF replicated THEN journalReplica \cup {active} ELSE journalReplica
    /\ receiptReplica' = replicated
    /\ UNCHANGED <<orderVars, objects, callProfile, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
CommitAndDeliver(replicated) ==
    /\ WriteCommitted(replicated) /\ pc' = "Receipt" /\ UNCHANGED active
CommitAndLoseReceipt(replicated) ==
    /\ WriteCommitted(replicated) /\ pc' = "Idle" /\ active' = "None"

CommitFailure ==
    /\ pc = "Ready" /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, callProfile, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

ReceiptPermitted == callProfile = profile[active]
                    /\ (profile[active] = "Replica" => receiptReplica)
AcceptReceipt ==
    /\ pc = "Receipt" /\ ReceiptPermitted
    /\ durable' = durable \cup {active} /\ pc' = "Accepted"
    /\ UNCHANGED <<present, profile, seq, send, terminal, held, nextSeq, storeVars,
                   active, callProfile, receiptReplica, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
RejectReceipt ==
    /\ pc = "Receipt" /\ ~ReceiptPermitted
    /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, callProfile, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

SendPermitted == send[active] = "Unsent" /\ active \notin terminal
                 /\ active \in durable /\ gateOpen /\ ~frozen
BeginSend ==
    /\ pc = "Accepted" /\ SendPermitted
    /\ send' = [send EXCEPT ![active] = "Unknown"] /\ pc' = "Started"
    /\ beginCount' = [beginCount EXCEPT ![active] = @ + 1]
    /\ UNCHANGED <<present, profile, seq, terminal, durable, held, nextSeq, storeVars,
                   active, callProfile, receiptReplica, response, gateOpen, frozen,
                   alive, crashes, dispatchCount, acceptedCount, everRestarted>>
RejectBegin ==
    /\ pc = "Accepted" /\ ~SendPermitted
    /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, callProfile, receiptReplica, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

Dispatch(outcome) ==
    /\ pc = "Started" /\ pc' = "Respond" /\ response' = outcome
    /\ dispatchCount' = [dispatchCount EXCEPT ![active] = @ + 1]
    /\ acceptedCount' = IF outcome = "NoBytes" THEN acceptedCount
                         ELSE [acceptedCount EXCEPT ![active] = @ + 1]
    /\ UNCHANGED <<orderVars, storeVars, active, callProfile, receiptReplica,
                   gateOpen, frozen, alive, crashes, beginCount, everRestarted>>
Respond ==
    /\ pc = "Respond" /\ pc' = "Idle" /\ active' = "None"
    /\ send' = [send EXCEPT ![active] = CASE response = "Ack" -> "Confirmed"
                                            [] response = "NoBytes" -> "Unsent"
                                            [] OTHER -> @]
    /\ terminal' = IF response = "NoBytes" THEN terminal \cup {active} ELSE terminal
    /\ held' = IF response = "NoBytes" THEN [held EXCEPT ![active] = 0] ELSE held
    /\ frozen' = IF response = "Unknown" THEN TRUE ELSE frozen
    /\ UNCHANGED <<present, profile, seq, durable, nextSeq, storeVars, callProfile,
                   receiptReplica, response, gateOpen, alive, crashes, historyVars>>

AbortUnsent(o) ==
    /\ pc = "Idle" /\ o \in present /\ send[o] = "Unsent"
    /\ terminal' = terminal \cup {o} /\ held' = [held EXCEPT ![o] = 0]
    /\ UNCHANGED <<present, profile, seq, send, durable, nextSeq, storeVars, pc,
                   active, callProfile, receiptReplica, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
Uncertain(o) == /\ pc = "Idle" /\ o \in present /\ send[o] # "Unsent"
                /\ frozen' = TRUE
                /\ UNCHANGED <<orderVars, storeVars, pc, active, callProfile,
                               receiptReplica, response, gateOpen, alive, crashes, historyVars>>
ToggleGate ==
    /\ pc = "Idle" /\ gateOpen' = ~gateOpen
    /\ UNCHANGED <<orderVars, storeVars, pc, active, callProfile, receiptReplica,
                   response, frozen, alive, crashes, historyVars>>
Freeze ==
    /\ pc = "Idle" /\ ~frozen /\ frozen' = TRUE
    /\ UNCHANGED <<orderVars, storeVars, pc, active, callProfile, receiptReplica,
                   response, gateOpen, alive, crashes, historyVars>>

Crash ==
    /\ alive /\ crashes < MaxCrashes /\ alive' = FALSE /\ crashes' = crashes + 1
    /\ pc' = "Down" /\ active' = "None" /\ frozen' = TRUE
    /\ present' = {} /\ profile' = [o \in Orders |-> "Local"]
    /\ seq' = [o \in Orders |-> 0] /\ send' = [o \in Orders |-> "Unsent"]
    /\ terminal' = {} /\ durable' = {} /\ held' = [o \in Orders |-> 0] /\ nextSeq' = 1
    /\ UNCHANGED <<storeVars, callProfile, receiptReplica, response, gateOpen, historyVars>>

Restart ==
    /\ pc = "Down" /\ alive' = TRUE /\ pc' = "Idle" /\ frozen' = TRUE
    /\ present' = journal
    /\ profile' = [o \in Orders |-> IF o \in journal THEN journalProfile[o] ELSE "Local"]
    /\ seq' = [o \in Orders |-> IF o \in journal THEN journalSeq[o] ELSE 0]
    /\ send' = [o \in Orders |-> IF o \in journal THEN "Unknown" ELSE "Unsent"]
    /\ terminal' = {} /\ durable' = journal
    /\ held' = [o \in Orders |-> IF o \in journal THEN Budget ELSE 0]
    /\ nextSeq' = 1 + Max({0} \cup {journalSeq[o] : o \in journal})
    /\ everRestarted' = TRUE
    /\ UNCHANGED <<storeVars, active, callProfile, receiptReplica, response, gateOpen,
                   crashes, beginCount, dispatchCount, acceptedCount>>

Next == \/ \E o \in Orders, p \in Profiles, cp \in Profiles : StartSubmit(o, p, cp)
        \/ StoreDependency \/ DependencyFailure
        \/ \E o \in Orders, cp \in Profiles : Resume(o, cp)
        \/ \E replicated \in BOOLEAN : CommitAndDeliver(replicated) \/ CommitAndLoseReceipt(replicated)
        \/ CommitFailure \/ AcceptReceipt \/ RejectReceipt \/ BeginSend \/ RejectBegin
        \/ \E outcome \in Outcomes : Dispatch(outcome)
        \/ Respond \/ (\E o \in Orders : AbortUnsent(o) \/ Uncertain(o))
        \/ ToggleGate \/ Freeze \/ Crash \/ Restart
Spec == Init /\ [][Next]_vars

TypeOK == /\ present \subseteq Orders /\ terminal \subseteq present /\ durable \subseteq present
          /\ journal \subseteq Orders /\ journalReplica \subseteq journal
          /\ profile \in [Orders -> Profiles] /\ journalProfile \in [Orders -> Profiles]
          /\ objects \in [Orders -> (Profiles \cup {"None"})]
          /\ send \in [Orders -> {"Unsent", "Unknown", "Confirmed"}]
          /\ seq \in [Orders -> Nat] /\ journalSeq \in [Orders -> Nat] /\ nextSeq \in Nat
          /\ held \in [Orders -> {0, Budget}] /\ pc \in PCs /\ active \in Orders \cup {"None"}
          /\ callProfile \in Profiles /\ response \in Outcomes
          /\ gateOpen \in BOOLEAN /\ frozen \in BOOLEAN /\ receiptReplica \in BOOLEAN
          /\ alive \in BOOLEAN /\ crashes \in 0..MaxCrashes /\ everRestarted \in BOOLEAN
          /\ beginCount \in [Orders -> Nat] /\ dispatchCount \in [Orders -> Nat]
          /\ acceptedCount \in [Orders -> Nat]
CapacityBound == Cardinality(present) <= Slots /\ Cardinality(journal) <= Slots
DependencyClosure == \A o \in journal : objects[o] = journalProfile[o]
JournalSequenceUnique == \A a,b \in journal : a # b => journalSeq[a] # journalSeq[b]
NoOvercommitWhileActive == alive /\ ~frozen => TotalHeld <= Capital
NoRepeatedSubmit == \A o \in Orders : dispatchCount[o] <= 1 /\ beginCount[o] <= 1
DurableBeforeSubmit == \A o \in Orders : dispatchCount[o] > 0 =>
    (o \in journal /\ (journalProfile[o] = "Replica" => o \in journalReplica))
AcceptedHasRecord == \A o \in Orders : acceptedCount[o] > 0 => o \in journal
UnknownKeepsReservation == \A o \in present : send[o] = "Unknown" => held[o] = Budget
RecoveryStaysFrozen == everRestarted => frozen
=============================================================================
