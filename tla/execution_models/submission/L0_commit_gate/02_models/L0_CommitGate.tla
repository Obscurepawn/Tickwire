---------------------------- MODULE L0_CommitGate ----------------------------
EXTENDS Naturals, FiniteSets, TLC

\* var <-> code map; Init: crates/tickwire-execution/src/lib.rs:219-232 (repo-relative paths)
\* Orders, Slots: bounded Engine<N,T>.orders, crates/tickwire-execution/src/lib.rs:212
\* Budget, Capital: checked_budget and Engine.capital, crates/tickwire-execution/src/lib.rs:262
\* present, profile: orders and immutable request.commit_profile, crates/tickwire-execution/src/lib.rs:276
\* send, terminal, durable, held: Order fields/held(), crates/tickwire-execution/src/lib.rs:138
\* local, replica: trusted CommitReceipt completion evidence, crates/tickwire-execution/src/lib.rs:103
\*   These sets DENOTE completed record writes, not acknowledgements accepted by Engine.
\* gateOpen: conjunction Gate::permits, crates/tickwire-execution/src/lib.rs:60
\* frozen: Engine::freeze, crates/tickwire-execution/src/lib.rs:238
\* sendCount: history witness counting successful begin_send returns, crates/tickwire-execution/src/lib.rs:322
\* Reserve -> reserve: crates/tickwire-execution/src/lib.rs:276
\* PersistLocal/PersistReplica -> receipt environment: crates/tickwire-execution/src/lib.rs:103
\* AcceptCommit -> accept_commit exact profile/local/replica guards: crates/tickwire-execution/src/lib.rs:308
\* BeginSend -> writes MayHaveSent before returning: crates/tickwire-execution/src/lib.rs:337
\* Ack -> ack: crates/tickwire-execution/src/lib.rs:384
\* Uncertain -> uncertain and not_found: crates/tickwire-execution/src/lib.rs:374
\* AbortUnsent -> abort_unsent: crates/tickwire-execution/src/lib.rs:341
\* NoBytes -> transport_rejected_without_bytes: crates/tickwire-execution/src/lib.rs:352
\* NoBytesRejected -> error freezes without releasing: crates/tickwire-execution/src/lib.rs:352-373
\* ToggleGate -> update_gate: crates/tickwire-execution/src/lib.rs:234
\* Freeze -> freeze: crates/tickwire-execution/src/lib.rs:238
\* FAULTS: none at L0. Single-owner API calls; fills/fees/terminal reports at L2.
\* Full-body drift anchors: crates/tickwire-execution/src/lib.rs:60-71
\* crates/tickwire-execution/src/lib.rs:138-181
\* crates/tickwire-execution/src/lib.rs:234-239
\* crates/tickwire-execution/src/lib.rs:262-392

CONSTANTS Orders, Slots, Budget, Capital
Profiles == {"Local", "Replica"}
SendStates == {"Unsent", "Unknown", "Confirmed"}
VARIABLES present, profile, send, terminal, durable, held,
          local, replica, gateOpen, frozen, sendCount
vars == <<present, profile, send, terminal, durable, held,
          local, replica, gateOpen, frozen, sendCount>>
TotalHeld == Cardinality({o \in Orders : held[o] = Budget}) * Budget

Init == /\ present = {}
        /\ profile = [o \in Orders |-> "Local"]
        /\ send = [o \in Orders |-> "Unsent"]
        /\ terminal = {}
        /\ durable = {}
        /\ held = [o \in Orders |-> 0]
        /\ local = {}
        /\ replica = {}
        /\ gateOpen = TRUE
        /\ frozen = FALSE
        /\ sendCount = [o \in Orders |-> 0]

Reserve(o, p) == /\ o \notin present
                 /\ Cardinality(present) < Slots
                 /\ gateOpen /\ ~frozen
                 /\ TotalHeld + Budget <= Capital
                 /\ present' = present \cup {o}
                 /\ profile' = [profile EXCEPT ![o] = p]
                 /\ held' = [held EXCEPT ![o] = Budget]
                 /\ UNCHANGED <<send, terminal, durable, local, replica,
                                gateOpen, frozen, sendCount>>

PersistLocal(o) == /\ o \in present /\ o \notin local
                   /\ local' = local \cup {o}
                   /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                                  replica, gateOpen, frozen, sendCount>>
PersistReplica(o) == /\ o \in local /\ o \notin replica
                     /\ replica' = replica \cup {o}
                     /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                                    local, gateOpen, frozen, sendCount>>

AcceptCommit(o, requestedProfile) ==
    /\ o \in present /\ requestedProfile = profile[o]
    /\ o \in local
    /\ (profile[o] = "Replica" => o \in replica)
    /\ durable' = durable \cup {o}
    /\ UNCHANGED <<present, profile, send, terminal, held, local, replica,
                   gateOpen, frozen, sendCount>>

BeginSend(o) == /\ o \in present /\ send[o] = "Unsent"
               /\ o \notin terminal /\ o \in durable
               /\ gateOpen /\ ~frozen
               /\ send' = [send EXCEPT ![o] = "Unknown"]
               /\ sendCount' = [sendCount EXCEPT ![o] = @ + 1]
               /\ UNCHANGED <<present, profile, terminal, durable, held, local,
                              replica, gateOpen, frozen>>

Ack(o) == /\ o \in present /\ send[o] # "Unsent"
          /\ send' = [send EXCEPT ![o] = "Confirmed"]
          /\ UNCHANGED <<present, profile, terminal, durable, held, local,
                         replica, gateOpen, frozen, sendCount>>

Uncertain(o) == /\ o \in present /\ send[o] # "Unsent"
                /\ frozen' = TRUE
                /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                               local, replica, gateOpen, sendCount>>

AbortUnsent(o) == /\ o \in present /\ send[o] = "Unsent"
                  /\ terminal' = terminal \cup {o}
                  /\ held' = [held EXCEPT ![o] = 0]
                  /\ UNCHANGED <<present, profile, send, durable, local, replica,
                                 gateOpen, frozen, sendCount>>

\* The transport assertion is trusted: this branch means no bytes were accepted.
NoBytes(o) == /\ o \in present /\ send[o] = "Unknown" /\ o \notin terminal
              /\ send' = [send EXCEPT ![o] = "Unsent"]
              /\ terminal' = terminal \cup {o}
              /\ held' = [held EXCEPT ![o] = 0]
              /\ UNCHANGED <<present, profile, durable, local, replica,
                             gateOpen, frozen, sendCount>>

NoBytesRejected(o) ==
    /\ (o \notin present \/ send[o] # "Unknown" \/ o \in terminal)
    /\ frozen' = TRUE
    /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                   local, replica, gateOpen, sendCount>>

ToggleGate == /\ gateOpen' = ~gateOpen
              /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                             local, replica, frozen, sendCount>>
Freeze == /\ ~frozen /\ frozen' = TRUE
          /\ UNCHANGED <<present, profile, send, terminal, durable, held,
                         local, replica, gateOpen, sendCount>>

Next == \/ \E o \in Orders, p \in Profiles : Reserve(o, p)
        \/ \E o \in Orders : PersistLocal(o) \/ PersistReplica(o)
        \/ \E o \in Orders, p \in Profiles : AcceptCommit(o, p)
        \/ \E o \in Orders : BeginSend(o) \/ Ack(o) \/ Uncertain(o)
                              \/ AbortUnsent(o) \/ NoBytes(o) \/ NoBytesRejected(o)
        \/ ToggleGate \/ Freeze
Spec == Init /\ [][Next]_vars

TypeOK == /\ present \subseteq Orders /\ terminal \subseteq present
          /\ durable \subseteq present /\ local \subseteq present /\ replica \subseteq local
          /\ profile \in [Orders -> Profiles] /\ send \in [Orders -> SendStates]
          /\ held \in [Orders -> {0, Budget}] /\ sendCount \in [Orders -> Nat]
          /\ gateOpen \in BOOLEAN /\ frozen \in BOOLEAN
CapacityBound == Cardinality(present) <= Slots
NoOvercommit == TotalHeld <= Capital
NoRepeatedBeginSend == \A o \in Orders : sendCount[o] <= 1
DurableBeforeSend == \A o \in Orders : sendCount[o] > 0 =>
    (o \in local /\ (profile[o] = "Replica" => o \in replica))
UnknownKeepsReservation == \A o \in present : send[o] = "Unknown" => held[o] = Budget
NoPhantomReservation == \A o \in Orders : o \notin present => held[o] = 0
=============================================================================
