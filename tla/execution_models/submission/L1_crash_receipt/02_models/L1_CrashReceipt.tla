---------------------------- MODULE L1_CrashReceipt ----------------------------
EXTENDS Naturals, FiniteSets, TLC

\* struct CommitReceipt: crates/tickwire-execution/src/lib.rs:99-102
\* fn permits: crates/tickwire-execution/src/lib.rs:61-72
\* fn update_gate: crates/tickwire-execution/src/lib.rs:229-231
\* fn freeze: crates/tickwire-execution/src/lib.rs:233-235
\* fn reserve: crates/tickwire-execution/src/lib.rs:272-296
\* fn durable_record: crates/tickwire-execution/src/lib.rs:297-303
\* fn accept_commit: crates/tickwire-execution/src/lib.rs:304-314
\* fn begin_send: crates/tickwire-execution/src/lib.rs:315-333
\* fn abort_unsent: crates/tickwire-execution/src/lib.rs:334-343
\* fn transport_rejected_without_bytes: crates/tickwire-execution/src/lib.rs:345-351
\* fn apply_no_bytes: crates/tickwire-execution/src/lib.rs:352-366
\* fn uncertain: crates/tickwire-execution/src/lib.rs:367-373
\* fn not_found: crates/tickwire-execution/src/lib.rs:374-376
\* fn ack: crates/tickwire-execution/src/lib.rs:377-385
\* fn recover_intent: crates/tickwire-execution/src/lib.rs:497-521
\* fn put_immutable: crates/tickwire-mock/src/lib.rs:54-67
\* impl Journal for MockJournal: crates/tickwire-mock/src/lib.rs:101-145
\* impl Exchange for MockExchange: crates/tickwire-mock/src/lib.rs:161-170
\* fn submit: crates/tickwire-mock/src/lib.rs:209-218
\* fn resume_submit: crates/tickwire-mock/src/lib.rs:219-234
\* fn restart: crates/tickwire-mock/src/lib.rs:264-277
\* A and B stand for distinct immutable command digests. Orders retain the original digest.
\* A completed local append can survive loss of its receipt. Restart replays the journal frozen.

CONSTANTS Orders, Slots, Budget, Capital, MaxCrashes
Digests == {"A", "B"}
Outcomes == {"Pending", "Ack", "Unknown", "NoBytes"}
PCs == {"Idle", "Reserved", "Ready", "Receipt", "Accepted", "Started", "Respond", "Down"}
VARIABLES present, digest, seq, send, terminal, durable, held, nextSeq,
          objects, journal, journalDigest, journalSeq,
          pc, active, receiptDigest, response, gateOpen, frozen,
          alive, crashes, everRestarted, beginCount, dispatchCount, acceptedCount
orderVars == <<present, digest, seq, send, terminal, durable, held, nextSeq>>
storeVars == <<objects, journal, journalDigest, journalSeq>>
historyVars == <<beginCount, dispatchCount, acceptedCount, everRestarted>>
vars == <<orderVars, storeVars, pc, active, receiptDigest, response,
          gateOpen, frozen, alive, crashes, historyVars>>
TotalHeld == Cardinality({o \in Orders : held[o] = Budget}) * Budget
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init == /\ present = {} /\ digest = [o \in Orders |-> "A"]
        /\ seq = [o \in Orders |-> 0] /\ send = [o \in Orders |-> "Unsent"]
        /\ terminal = {} /\ durable = {} /\ held = [o \in Orders |-> 0]
        /\ nextSeq = 1
        /\ objects = [o \in Orders |-> "None"] /\ journal = {}
        /\ journalDigest = [o \in Orders |-> "A"]
        /\ journalSeq = [o \in Orders |-> 0]
        /\ pc = "Idle" /\ active = "None" /\ receiptDigest = "A"
        /\ response = "Unknown"
        /\ gateOpen = TRUE /\ frozen = FALSE /\ alive = TRUE /\ crashes = 0
        /\ everRestarted = FALSE
        /\ beginCount = [o \in Orders |-> 0]
        /\ dispatchCount = [o \in Orders |-> 0]
        /\ acceptedCount = [o \in Orders |-> 0]

StartSubmit(o, p, cp) ==
    /\ pc = "Idle" /\ o \notin present /\ Cardinality(present) < Slots
    /\ gateOpen /\ ~frozen /\ TotalHeld + Budget <= Capital
    /\ present' = present \cup {o} /\ digest' = [digest EXCEPT ![o] = p]
    /\ seq' = [seq EXCEPT ![o] = nextSeq] /\ nextSeq' = nextSeq + 1
    /\ held' = [held EXCEPT ![o] = Budget]
    /\ pc' = "Reserved" /\ active' = o /\ receiptDigest' = cp
    /\ UNCHANGED <<send, terminal, durable, storeVars, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

StoreDependency ==
    /\ pc = "Reserved" /\ objects[active] \in {"None", digest[active]}
    /\ objects' = [objects EXCEPT ![active] = digest[active]] /\ pc' = "Ready"
    /\ UNCHANGED <<orderVars, journal, journalDigest, journalSeq,
                   active, receiptDigest, response, gateOpen, frozen,
                   alive, crashes, historyVars>>

DependencyFailure ==
    /\ pc = "Reserved" /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, receiptDigest, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

Resume(o, cp) ==
    /\ pc = "Idle" /\ o \in present
    /\ pc' = "Ready" /\ active' = o /\ receiptDigest' = cp
    /\ UNCHANGED <<orderVars, storeVars, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

WriteCommitted ==
    /\ pc = "Ready" /\ objects[active] = digest[active]
    /\ (active \in journal =>
           (journalDigest[active] = digest[active] /\ journalSeq[active] = seq[active]))
    /\ \A o \in journal \ {active} : journalSeq[o] # seq[active]
    /\ journal' = journal \cup {active}
    /\ journalDigest' = [journalDigest EXCEPT ![active] = digest[active]]
    /\ journalSeq' = [journalSeq EXCEPT ![active] = seq[active]]
    /\ UNCHANGED <<orderVars, objects, receiptDigest, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
CommitAndDeliver ==
    /\ WriteCommitted /\ pc' = "Receipt" /\ UNCHANGED active
CommitAndLoseReceipt ==
    /\ WriteCommitted /\ pc' = "Idle" /\ active' = "None"

CommitFailure ==
    /\ pc = "Ready" /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, receiptDigest, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

ReceiptPermitted == receiptDigest = digest[active]
AcceptReceipt ==
    /\ pc = "Receipt" /\ ReceiptPermitted
    /\ durable' = durable \cup {active} /\ pc' = "Accepted"
    /\ UNCHANGED <<present, digest, seq, send, terminal, held, nextSeq, storeVars,
                   active, receiptDigest, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
RejectReceipt ==
    /\ pc = "Receipt" /\ ~ReceiptPermitted
    /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, receiptDigest, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

SendPermitted == send[active] = "Unsent" /\ active \notin terminal
                 /\ active \in durable /\ gateOpen /\ ~frozen
BeginSend ==
    /\ pc = "Accepted" /\ SendPermitted
    /\ send' = [send EXCEPT ![active] = "Unknown"] /\ pc' = "Started"
    /\ beginCount' = [beginCount EXCEPT ![active] = @ + 1]
    /\ UNCHANGED <<present, digest, seq, terminal, durable, held, nextSeq, storeVars,
                   active, receiptDigest, response, gateOpen, frozen,
                   alive, crashes, dispatchCount, acceptedCount, everRestarted>>
RejectBegin ==
    /\ pc = "Accepted" /\ ~SendPermitted
    /\ pc' = "Idle" /\ active' = "None"
    /\ UNCHANGED <<orderVars, storeVars, receiptDigest, response,
                   gateOpen, frozen, alive, crashes, historyVars>>

Dispatch(outcome) ==
    /\ pc = "Started" /\ pc' = "Respond" /\ response' = outcome
    /\ dispatchCount' = [dispatchCount EXCEPT ![active] = @ + 1]
    /\ acceptedCount' = IF outcome = "NoBytes" THEN acceptedCount
                         ELSE [acceptedCount EXCEPT ![active] = @ + 1]
    /\ UNCHANGED <<orderVars, storeVars, active, receiptDigest, gateOpen, frozen, alive, crashes, beginCount, everRestarted>>
Respond ==
    /\ pc = "Respond" /\ pc' = "Idle" /\ active' = "None"
    /\ send' = [send EXCEPT ![active] = CASE response = "Ack" -> "Confirmed"
                                            [] response = "NoBytes" -> "Unsent"
                                            [] OTHER -> @]
    /\ terminal' = IF response = "NoBytes" THEN terminal \cup {active} ELSE terminal
    /\ held' = IF response = "NoBytes" THEN [held EXCEPT ![active] = 0] ELSE held
    /\ frozen' = IF response = "Unknown" THEN TRUE ELSE frozen
    /\ UNCHANGED <<present, digest, seq, durable, nextSeq, storeVars, receiptDigest,
                   response, gateOpen, alive, crashes, historyVars>>

AbortUnsent(o) ==
    /\ pc = "Idle" /\ o \in present /\ send[o] = "Unsent"
    /\ terminal' = terminal \cup {o} /\ held' = [held EXCEPT ![o] = 0]
    /\ UNCHANGED <<present, digest, seq, send, durable, nextSeq, storeVars, pc,
                   active, receiptDigest, response, gateOpen, frozen,
                   alive, crashes, historyVars>>
Uncertain(o) == /\ pc = "Idle" /\ o \in present /\ send[o] # "Unsent"
                /\ frozen' = TRUE
                /\ UNCHANGED <<orderVars, storeVars, pc, active, receiptDigest,
                               response, gateOpen, alive, crashes, historyVars>>
ToggleGate ==
    /\ pc = "Idle" /\ gateOpen' = ~gateOpen
    /\ UNCHANGED <<orderVars, storeVars, pc, active, receiptDigest, response, frozen, alive, crashes, historyVars>>
Freeze ==
    /\ pc = "Idle" /\ ~frozen /\ frozen' = TRUE
    /\ UNCHANGED <<orderVars, storeVars, pc, active, receiptDigest, response, gateOpen, alive, crashes, historyVars>>

Crash ==
    /\ alive /\ crashes < MaxCrashes /\ alive' = FALSE /\ crashes' = crashes + 1
    /\ pc' = "Down" /\ active' = "None" /\ frozen' = TRUE
    /\ present' = {} /\ digest' = [o \in Orders |-> "A"]
    /\ seq' = [o \in Orders |-> 0] /\ send' = [o \in Orders |-> "Unsent"]
    /\ terminal' = {} /\ durable' = {} /\ held' = [o \in Orders |-> 0] /\ nextSeq' = 1
    /\ UNCHANGED <<storeVars, receiptDigest, response, gateOpen, historyVars>>

Restart ==
    /\ pc = "Down" /\ alive' = TRUE /\ pc' = "Idle" /\ frozen' = TRUE
    /\ present' = journal
    /\ digest' = [o \in Orders |-> IF o \in journal THEN journalDigest[o] ELSE "A"]
    /\ seq' = [o \in Orders |-> IF o \in journal THEN journalSeq[o] ELSE 0]
    /\ send' = [o \in Orders |-> IF o \in journal THEN "Unknown" ELSE "Unsent"]
    /\ terminal' = {} /\ durable' = journal
    /\ held' = [o \in Orders |-> IF o \in journal THEN Budget ELSE 0]
    /\ nextSeq' = 1 + Max({0} \cup {journalSeq[o] : o \in journal})
    /\ everRestarted' = TRUE
    /\ UNCHANGED <<storeVars, active, receiptDigest, response, gateOpen,
                   crashes, beginCount, dispatchCount, acceptedCount>>

Next == \/ \E o \in Orders, p \in Digests, cp \in Digests : StartSubmit(o, p, cp)
        \/ StoreDependency \/ DependencyFailure
        \/ \E o \in Orders, cp \in Digests : Resume(o, cp)
        \/ CommitAndDeliver \/ CommitAndLoseReceipt
        \/ CommitFailure \/ AcceptReceipt \/ RejectReceipt \/ BeginSend \/ RejectBegin
        \/ \E outcome \in Outcomes : Dispatch(outcome)
        \/ Respond \/ (\E o \in Orders : AbortUnsent(o) \/ Uncertain(o))
        \/ ToggleGate \/ Freeze \/ Crash \/ Restart
Spec == Init /\ [][Next]_vars

TypeOK == /\ present \subseteq Orders /\ terminal \subseteq present /\ durable \subseteq present
          /\ journal \subseteq Orders
          /\ digest \in [Orders -> Digests] /\ journalDigest \in [Orders -> Digests]
          /\ objects \in [Orders -> (Digests \cup {"None"})]
          /\ send \in [Orders -> {"Unsent", "Unknown", "Confirmed"}]
          /\ seq \in [Orders -> Nat] /\ journalSeq \in [Orders -> Nat] /\ nextSeq \in Nat
          /\ held \in [Orders -> {0, Budget}] /\ pc \in PCs /\ active \in Orders \cup {"None"}
          /\ receiptDigest \in Digests /\ response \in Outcomes
          /\ gateOpen \in BOOLEAN /\ frozen \in BOOLEAN
          /\ alive \in BOOLEAN /\ crashes \in 0..MaxCrashes /\ everRestarted \in BOOLEAN
          /\ beginCount \in [Orders -> Nat] /\ dispatchCount \in [Orders -> Nat]
          /\ acceptedCount \in [Orders -> Nat]
CapacityBound == Cardinality(present) <= Slots /\ Cardinality(journal) <= Slots
DependencyClosure == \A o \in journal : objects[o] = journalDigest[o]
JournalSequenceUnique == \A a,b \in journal : a # b => journalSeq[a] # journalSeq[b]
NoOvercommitWhileActive == alive /\ ~frozen => TotalHeld <= Capital
NoRepeatedSubmit == \A o \in Orders : dispatchCount[o] <= 1 /\ beginCount[o] <= 1
DurableBeforeSubmit == \A o \in Orders : dispatchCount[o] > 0 =>
    o \in journal
AcceptedHasRecord == \A o \in Orders : acceptedCount[o] > 0 => o \in journal
UnknownKeepsReservation == \A o \in present : send[o] = "Unknown" => held[o] = Budget
RecoveryStaysFrozen == everRestarted => frozen
=============================================================================
