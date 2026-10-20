-------------------------- MODULE L0_ControlRequests --------------------------
EXTENDS Naturals, FiniteSets, TLC

\* var <-> code map
\* entries[k].digest/result: StoredRequest and MockRequestStore.entries,
\*   crates/tickwire-control/src/requests.rs:6-32,171-244.
\* fail: MockRequestStore.fail_next/maybe_fail, crates/tickwire-control/src/requests.rs:188-197.
\* issued[k]: observer counting Admission::Execute returns, crates/tickwire-control/src/requests.rs:159-167.
\* registered/firstResult: history observers of successful insertion/completion.
\* out/observedRetry: return-value observers for replay, crates/tickwire-control/src/requests.rs:119-126.
\* Admit is a complete synchronous &mut Session/&mut Mock call, crates/tickwire-control/src/requests.rs:131-168.
\* Complete/BadCompletion: ExecutionTicket::complete, crates/tickwire-control/src/requests.rs:53-107,228-243.
\* MODE: concrete Mock fault injection; fail_next consumes once before a store operation.
\* NOT modeled: persistent store, process death, bytes/crypto, session expiry (L1).
CONSTANTS Keys, Commands, Results, Capacity, PendingLimit
VARIABLES entries, fail, issued, registered, firstResult, out, observedRetry
vars == <<entries, fail, issued, registered, firstResult, out, observedRetry>>
Empty == [digest |-> 0, result |-> 0]
Existing == {k \in Keys : entries[k].digest # 0}
Pending == {k \in Existing : entries[k].result = 0}
Answer(kind,k,d,r) == [kind |-> kind, key |-> k, digest |-> d, result |-> r]
Init ==
  /\ entries = [k \in Keys |-> Empty]
  /\ fail = FALSE
  /\ issued = [k \in Keys |-> 0]
  /\ registered = {}
  /\ firstResult = [k \in Keys |-> 0]
  /\ out = Answer("None",0,0,0)
  /\ observedRetry = FALSE

\* crates/tickwire-control/src/requests.rs:188-197
ArmFailure ==
  /\ ~fail /\ fail' = TRUE
  /\ UNCHANGED <<entries,issued,registered,firstResult,out,observedRetry>>

\* crates/tickwire-control/src/requests.rs:131-168,200-226
Admit(k,c,v) ==
  LET existing == k \in Existing
      fresh == ~fail /\ ~existing /\ c-1=v
               /\ Cardinality(Existing)<Capacity /\ Cardinality(Pending)<PendingLimit
      answer == IF fail THEN "Store"
                ELSE IF existing THEN IF entries[k].digest#c THEN "Conflict"
                     ELSE IF entries[k].result=0 THEN "Pending" ELSE "Completed"
                ELSE IF c-1#v THEN "Version"
                ELSE IF ~fresh THEN "Capacity" ELSE "Execute"
  IN /\ entries' = IF fresh THEN [entries EXCEPT ![k] = [digest |-> c,result |-> 0]] ELSE entries
     /\ issued' = IF fresh THEN [issued EXCEPT ![k] = @+1] ELSE issued
     /\ registered' = IF fresh THEN registered \cup {k} ELSE registered
     /\ fail' = FALSE
     /\ firstResult' = firstResult
     /\ out' = Answer(answer,k,c,IF answer="Completed" THEN entries[k].result ELSE 0)
     /\ observedRetry' = (~fail /\ existing /\ entries[k].digest=c)

\* crates/tickwire-control/src/requests.rs:53-107,228-243
Complete(k,r) ==
  /\ issued[k]>0
  /\ LET first == ~fail /\ entries[k].result=0
         answer == IF fail THEN "Store"
                   ELSE IF entries[k].result=0 \/ entries[k].result=r THEN "Applied" ELSE "Conflict"
     IN /\ entries' = IF first THEN [entries EXCEPT ![k].result = r] ELSE entries
        /\ firstResult' = IF first THEN [firstResult EXCEPT ![k] = r] ELSE firstResult
        /\ out' = Answer(answer,k,entries[k].digest,r)
  /\ fail' = FALSE
  /\ observedRetry' = FALSE
  /\ UNCHANGED <<issued,registered>>

\* crates/tickwire-control/src/requests.rs:59-98 (no Store method called)
BadCompletion(k) ==
  /\ issued[k]>0
  /\ out' = Answer("Protocol",k,entries[k].digest,0)
  /\ observedRetry' = FALSE
  /\ UNCHANGED <<entries,fail,issued,registered,firstResult>>

Next == ArmFailure
        \/ (\E k \in Keys,c \in Commands,v \in 0..Cardinality(Commands): Admit(k,c,v))
        \/ (\E k \in Keys,r \in Results: Complete(k,r))
        \/ (\E k \in Keys: BadCompletion(k))
Spec == Init /\ [][Next]_vars
TypeOK == /\ entries \in [Keys -> [digest: Commands \cup {0}, result: Results \cup {0}]]
          /\ fail \in BOOLEAN /\ issued \in [Keys -> Nat]
          /\ firstResult \in [Keys -> Results \cup {0}]
AtMostOneTicket == \A k \in Keys: issued[k]<=1
RegistrationBeforeTicket == \A k \in Keys: issued[k]>0 => k \in Existing
NoEviction == registered = Existing
ImmutableTerminal == \A k \in Keys: firstResult[k]#0 => entries[k].result=firstResult[k]
CapacityBound == Cardinality(Existing)<=Capacity /\ Cardinality(Pending)<=PendingLimit
RetryBeforeVersion == observedRetry =>
  /\ out.kind \in {"Pending","Completed"}
  /\ out.digest = entries[out.key].digest
  /\ out.result = entries[out.key].result
=============================================================================
