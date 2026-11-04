---------------------------- MODULE L0_CommitGate ----------------------------
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
\* A and B stand for distinct immutable command digests. Orders retain the original digest.

CONSTANTS Orders, Slots, Budget, Capital
Digests == {"A", "B"}
SendStates == {"Unsent", "Unknown", "Confirmed"}
VARIABLES present, digest, send, terminal, durable, held,
          local, gateOpen, frozen, sendCount
vars == <<present, digest, send, terminal, durable, held,
          local, gateOpen, frozen, sendCount>>
TotalHeld == Cardinality({o \in Orders : held[o] = Budget}) * Budget

Init == /\ present = {}
        /\ digest = [o \in Orders |-> "A"]
        /\ send = [o \in Orders |-> "Unsent"]
        /\ terminal = {}
        /\ durable = {}
        /\ held = [o \in Orders |-> 0]
        /\ local = {}
        /\ gateOpen = TRUE
        /\ frozen = FALSE
        /\ sendCount = [o \in Orders |-> 0]

Reserve(o, p) == /\ o \notin present
                 /\ Cardinality(present) < Slots
                 /\ gateOpen /\ ~frozen
                 /\ TotalHeld + Budget <= Capital
                 /\ present' = present \cup {o}
                 /\ digest' = [digest EXCEPT ![o] = p]
                 /\ held' = [held EXCEPT ![o] = Budget]
                 /\ UNCHANGED <<send, terminal, durable, local,
                                gateOpen, frozen, sendCount>>

PersistLocal(o) == /\ o \in present /\ o \notin local
                   /\ local' = local \cup {o}
                   /\ UNCHANGED <<present, digest, send, terminal, durable, held,
                                   gateOpen, frozen, sendCount>>
AcceptCommit(o, receiptDigest) ==
    /\ o \in present /\ receiptDigest = digest[o]
    /\ o \in local
    /\ durable' = durable \cup {o}
    /\ UNCHANGED <<present, digest, send, terminal, held, local,
                   gateOpen, frozen, sendCount>>

BeginSend(o) == /\ o \in present /\ send[o] = "Unsent"
               /\ o \notin terminal /\ o \in durable
               /\ gateOpen /\ ~frozen
               /\ send' = [send EXCEPT ![o] = "Unknown"]
               /\ sendCount' = [sendCount EXCEPT ![o] = @ + 1]
               /\ UNCHANGED <<present, digest, terminal, durable, held, local,
                               gateOpen, frozen>>

Ack(o) == /\ o \in present /\ send[o] # "Unsent"
          /\ send' = [send EXCEPT ![o] = "Confirmed"]
          /\ UNCHANGED <<present, digest, terminal, durable, held, local,
                          gateOpen, frozen, sendCount>>

Uncertain(o) == /\ o \in present /\ send[o] # "Unsent"
                /\ frozen' = TRUE
                /\ UNCHANGED <<present, digest, send, terminal, durable, held,
                               local, gateOpen, sendCount>>

AbortUnsent(o) == /\ o \in present /\ send[o] = "Unsent"
                  /\ terminal' = terminal \cup {o}
                  /\ held' = [held EXCEPT ![o] = 0]
                  /\ UNCHANGED <<present, digest, send, durable, local,
                                 gateOpen, frozen, sendCount>>

\* The transport assertion is trusted: this branch means no bytes were accepted.
NoBytes(o) == /\ o \in present /\ send[o] = "Unknown" /\ o \notin terminal
              /\ send' = [send EXCEPT ![o] = "Unsent"]
              /\ terminal' = terminal \cup {o}
              /\ held' = [held EXCEPT ![o] = 0]
              /\ UNCHANGED <<present, digest, durable, local,
                             gateOpen, frozen, sendCount>>

NoBytesRejected(o) ==
    /\ (o \notin present \/ send[o] # "Unknown" \/ o \in terminal)
    /\ frozen' = TRUE
    /\ UNCHANGED <<present, digest, send, terminal, durable, held,
                   local, gateOpen, sendCount>>

ToggleGate == /\ gateOpen' = ~gateOpen
              /\ UNCHANGED <<present, digest, send, terminal, durable, held,
                             local,  frozen, sendCount>>
Freeze == /\ ~frozen /\ frozen' = TRUE
          /\ UNCHANGED <<present, digest, send, terminal, durable, held,
                         local, gateOpen, sendCount>>

Next == \/ \E o \in Orders, p \in Digests : Reserve(o, p)
        \/ \E o \in Orders : PersistLocal(o)
        \/ \E o \in Orders, p \in Digests : AcceptCommit(o, p)
        \/ \E o \in Orders : BeginSend(o) \/ Ack(o) \/ Uncertain(o)
                              \/ AbortUnsent(o) \/ NoBytes(o) \/ NoBytesRejected(o)
        \/ ToggleGate \/ Freeze
Spec == Init /\ [][Next]_vars

TypeOK == /\ present \subseteq Orders /\ terminal \subseteq present
          /\ durable \subseteq present /\ local \subseteq present
          /\ digest \in [Orders -> Digests] /\ send \in [Orders -> SendStates]
          /\ held \in [Orders -> {0, Budget}] /\ sendCount \in [Orders -> Nat]
          /\ gateOpen \in BOOLEAN /\ frozen \in BOOLEAN
CapacityBound == Cardinality(present) <= Slots
NoOvercommit == TotalHeld <= Capital
NoRepeatedBeginSend == \A o \in Orders : sendCount[o] <= 1
DurableBeforeSend == \A o \in Orders : sendCount[o] > 0 =>
    o \in local
UnknownKeepsReservation == \A o \in present : send[o] = "Unknown" => held[o] = Budget
NoPhantomReservation == \A o \in Orders : o \notin present => held[o] = 0
=============================================================================
