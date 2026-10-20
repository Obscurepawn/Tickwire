--------------------------- MODULE L0_JoinOwner ---------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC
\* var <-> code map:
\* entries: JoinOwner::Entry; stage/base/birth/done/acks/cursor/result,
\*   crates/tickwire-join/src/lib.rs:98-111. base is content boundary S.
\* pub/payload: complete published prefix and ordinary slot sequence tags,
\*   crates/tickwire-shm/src/mapped.rs:311-346.
\* version: membership_version, crates/tickwire-shm/src/mapped.rs:205-220.
\* busy: exclusive &mut owner call, crates/tickwire-join/src/lib.rs:278-425.
\* clockExpired: JOIN deadline predicate, crates/tickwire-join/src/lib.rs:240-256.
\* rd/app/rel/held/life: actual independent cursor and live borrow,
\*   crates/tickwire-shm/src/lib.rs:417-558; app/rel are contiguous progress.
\* activatedAt/activationRel/closedRel/closedApp: cached historical results,
\*   crates/tickwire-join/src/lib.rs:533-565 and crates/tickwire-join/src/lib.rs:646-663.
\* Guard equality represents all authenticated body/descriptor identity fields;
\* raw encoding, syscalls, and CPU memory model are outside this level.
CONSTANTS Readers, Capacity, Start, MaxSeq, Wrapped
ASSUME /\ Capacity = 64 /\ Start = 4 /\ Cardinality(Readers) \in 1..2
       /\ Readers = 1..Cardinality(Readers) /\ MaxSeq >= Start
VARIABLES pub, entries, version, busy, clockExpired, payload
vars == <<pub,entries,version,busy,clockExpired,payload>>
Slot(s) == (s-1) % Capacity
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Min2(a,b) == IF a <= b THEN a ELSE b
Present == {r \in Readers : entries[r].stage \notin {"Empty","Detached"}}
Gate(r) == IF entries[r].active THEN entries[r].rel ELSE entries[r].base
GateMin == IF Present = {} THEN pub ELSE Min({Gate(r):r \in Present})
NewEntry(s) ==
 [stage |-> "Empty", base |-> s, birth |-> 0, done |-> FALSE,
  delivered |-> FALSE, attached |-> FALSE, loaded |-> FALSE, cursor |-> FALSE,
  bankAck |-> FALSE, snapAck |-> FALSE, active |-> FALSE,
  rd |-> s, app |-> s, rel |-> s, held |-> FALSE, life |-> 3,
  activatedAt |-> -1, activationRel |-> -1, closedRel |-> -1, closedApp |-> -1]
Init ==
 /\ pub = IF Wrapped THEN Start+64 ELSE Start
 /\ entries = [r \in Readers |-> IF Wrapped
      THEN [NewEntry(Start) EXCEPT !.stage="Delivered", !.birth=r+1,
        !.done=TRUE, !.delivered=TRUE, !.attached=TRUE, !.loaded=TRUE,
        !.cursor=TRUE, !.bankAck=TRUE, !.snapAck=TRUE,
        !.rd=Start+63, !.app=Start+63, !.rel=Start+63]
      ELSE NewEntry(Start)]
 /\ version = IF Wrapped THEN 1+Cardinality(Readers) ELSE 1
 /\ busy = 0 /\ clockExpired = FALSE
 /\ payload = [i \in 0..63 |-> IF Wrapped
      THEN CHOOSE s \in (Start+1)..(Start+64) : Slot(s)=i
      ELSE IF i < Start THEN i+1 ELSE 0]
\* prepare: admission is imported from control L0; fixed valid new identities.
\* crates/tickwire-join/src/lib.rs:278-380
Prepare(r) ==
 /\ busy=0 /\ ~clockExpired /\ entries[r].stage="Empty"
 /\ entries' = [entries EXCEPT ![r] = [NewEntry(pub) EXCEPT
      !.stage="Reserved", !.birth=version+1]]
 /\ version'=version+1 /\ busy'=r
 /\ UNCHANGED <<pub,clockExpired,payload>>
\* build/seal/complete success or error after reservation, crates/tickwire-join/src/lib.rs:381-424.
Build(r,ok) ==
 /\ busy=r /\ entries[r].stage="Reserved"
 /\ entries'=[entries EXCEPT ![r].stage=IF ok THEN "Prepared" ELSE "Failed", ![r].done=ok]
 /\ busy'=0 /\ UNCHANGED <<pub,version,clockExpired,payload>>
\* crates/tickwire-join/src/lib.rs:294-300 completed and pending retry do not redispatch or re-register.
RetryPrepare(r) ==
 /\ busy=0 /\ entries[r].stage # "Empty"
 /\ UNCHANGED vars
\* deliver crates/tickwire-join/src/lib.rs:426-469. Failure preserves the actual gate and registration.
Deliver(r,ok) ==
 /\ busy=0 /\ ~clockExpired /\ entries[r].stage \in {"Prepared","Delivered"}
 /\ entries'=[entries EXCEPT ![r].stage=IF ok THEN "Delivered" ELSE "Failed",
      ![r].delivered=IF ok THEN TRUE ELSE @]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* actual immutable mapping + Subscriber attach, crates/tickwire-join/src/mapped.rs:75-142.
Attach(r) ==
 /\ entries[r].delivered /\ ~entries[r].attached /\ ~clockExpired
 /\ entries'=[entries EXCEPT ![r].attached=TRUE]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* callback completion, crates/tickwire-join/src/mapped.rs:164-175; failure leaves loaded false.
Load(r,ok) ==
 /\ entries[r].attached /\ ~entries[r].loaded /\ entries[r].life=3 /\ ~clockExpired
 /\ entries'=[entries EXCEPT ![r].loaded=ok]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* receive_cursor checks fixed authenticated offer, crates/tickwire-join/src/mapped.rs:12-60.
Register(r) ==
 /\ busy=0 /\ ~clockExpired /\ entries[r].attached /\ ~entries[r].cursor
 /\ entries[r].stage \in {"Prepared","Delivered"}
 /\ entries'=[entries EXCEPT ![r].cursor=TRUE]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* read_ready crates/tickwire-join/src/lib.rs:470-515. One field may arrive before the other; duplicates commute.
ReadReady(r,bank) ==
 /\ busy=0 /\ ~clockExpired /\ entries[r].loaded
 /\ entries[r].stage \in {"Delivered","Active"}
 /\ entries'=[entries EXCEPT ![r].bankAck=IF bank THEN TRUE ELSE @,
       ![r].snapAck=IF bank THEN @ ELSE TRUE]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* real read guard, crates/tickwire-join/src/mapped.rs:261-266 and Reader::borrow.
Borrow(r) ==
 /\ entries[r].attached /\ entries[r].loaded /\ entries[r].life=3
 /\ entries[r].rd < pub /\ entries[r].rd-Min2(entries[r].app,entries[r].rel) < 1
 /\ payload[Slot(entries[r].rd+1)]=entries[r].rd+1
 /\ entries'=[entries EXCEPT ![r].rd=@+1, ![r].held=TRUE]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* Reader::release/apply, crates/tickwire-shm/src/lib.rs:498-541.
Release(r) ==
 /\ entries[r].held
 /\ entries'=[entries EXCEPT ![r].held=FALSE, ![r].rel=entries[r].rd]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
Apply(r) ==
 /\ entries[r].attached /\ entries[r].loaded /\ entries[r].app < entries[r].rd
 /\ entries'=[entries EXCEPT ![r].app=entries[r].rd]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* JoinReady and actual cursor validation, crates/tickwire-join/src/lib.rs:516-566,
\* crates/tickwire-shm/src/mapped.rs:250-299.
CanReady(r) ==
 /\ busy=0 /\ ~clockExpired /\ entries[r].stage="Delivered"
 /\ entries[r].bankAck /\ entries[r].snapAck /\ entries[r].cursor
 /\ entries[r].life=3 /\ entries[r].app>=pub
 /\ entries[r].base<=entries[r].rel /\ entries[r].rel<=entries[r].rd
 /\ entries[r].app<=entries[r].rd /\ entries[r].rd<=pub
Ready(r,birth,matching) ==
 /\ CanReady(r) /\ birth=entries[r].birth /\ matching
 /\ entries'=[entries EXCEPT ![r].stage="Active", ![r].active=TRUE,
      ![r].activatedAt=pub, ![r].activationRel=entries[r].rel]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
RetryReady(r) ==
 /\ busy=0 /\ entries[r].stage="Active"
 /\ UNCHANGED vars
RejectReady(r,birth,matching) ==
 /\ busy=0 /\ entries[r].stage # "Empty"
 /\ (~matching \/ birth # entries[r].birth \/ (~CanReady(r) /\ entries[r].stage # "Active"))
 /\ UNCHANGED vars
\* expire crates/tickwire-join/src/lib.rs:567-577 never detaches; active historical result is retained.
Expire ==
 /\ busy=0 /\ ~clockExpired
 /\ clockExpired'=TRUE
 /\ entries'=[r \in Readers |-> IF entries[r].stage \notin {"Empty","Active","Detached","Quiescing"}
       THEN [entries[r] EXCEPT !.stage="Expired"] ELSE entries[r]]
 /\ UNCHANGED <<pub,version,busy,payload>>
\* owner Quiesce request, crates/tickwire-join/src/lib.rs:578-616. No timeout-based detach.
StartQuiesce(r) ==
 /\ busy=0 /\ entries[r].stage \notin {"Empty","Detached","Reserved"}
 /\ entries'=[entries EXCEPT ![r].stage="Quiescing"]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* Consumer stops borrowing before checking outstanding guards, crates/tickwire-join/src/mapped.rs:276-309.
StopReader(r) ==
 /\ entries[r].attached /\ entries[r].stage="Quiescing" /\ entries[r].life=3
 /\ entries'=[entries EXCEPT ![r].life=4]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
Quiesced(r) ==
 /\ entries[r].attached /\ entries[r].life=4 /\ ~entries[r].held
 /\ entries[r].rd=entries[r].rel
 /\ entries'=[entries EXCEPT ![r].life=5]
 /\ UNCHANGED <<pub,version,busy,clockExpired,payload>>
\* crates/tickwire-join/src/lib.rs:617-664 plus actual registered cursor lifecycle/qseq/exact values,
\* crates/tickwire-shm/src/mapped.rs:364-405.
Detach(r) ==
 /\ busy=0 /\ entries[r].stage="Quiescing" /\ entries[r].cursor
 /\ entries[r].life=5 /\ entries[r].rel=entries[r].rd
 /\ entries[r].app<=entries[r].rd /\ entries[r].rel<=pub
 \* This input carries the matching identity and actual cursor values.
 /\ entries'=[entries EXCEPT ![r].stage="Detached",
       ![r].closedRel=entries[r].rel, ![r].closedApp=entries[r].app]
 /\ version'=version+1 /\ UNCHANGED <<pub,busy,clockExpired,payload>>
RetryDetach(r) ==
 /\ busy=0 /\ entries[r].stage="Detached"
 \* Exact original body equals saved closedRel/closedApp, regardless of later app.
 /\ UNCHANGED vars
RejectDetach(r) ==
 /\ busy=0 /\ entries[r].stage \in {"Quiescing","Detached"}
 \* Conflicting body: released+1, wrong identity, or lifecycle is not Quiesced.
 \* Full body equality and actual mapped cursor comparison reject before detach.
 /\ UNCHANGED vars
\* publish with imported conservative-min lemma; no claim/write failure introduced.
\* crates/tickwire-shm/src/mapped.rs:300-346.
Publish ==
 /\ busy=0 /\ pub<MaxSeq /\ pub+1-GateMin<=Capacity
 /\ pub'=pub+1 /\ payload'=[payload EXCEPT ![Slot(pub+1)]=pub+1]
 /\ UNCHANGED <<entries,version,busy,clockExpired>>
PublishFull ==
 /\ busy=0 /\ pub<MaxSeq /\ pub+1-GateMin>Capacity
 /\ UNCHANGED vars
NextBase ==
 \/ Publish
 \/ \E r \in Readers : Borrow(r) \/ Release(r) \/ Apply(r)
       \/ RetryPrepare(r) \/ RetryReady(r) \/ StartQuiesce(r) \/ StopReader(r) \/ Quiesced(r)
       \/ \E v \in 1..(1+Cardinality(Readers)), matching \in BOOLEAN :
              Ready(r,v,matching) \/ RejectReady(r,v,matching)
       \/ Detach(r) \/ RetryDetach(r) \/ RejectDetach(r)
 \/ Expire
Next ==
 \/ NextBase
 \/ \E r \in Readers : Prepare(r) \/ Register(r)
       \/ \E ok \in BOOLEAN : Build(r,ok) \/ Deliver(r,ok) \/ Load(r,ok)
       \/ Attach(r) \/ \E bank \in BOOLEAN : ReadReady(r,bank)
NextWrap == NextBase \/ PublishFull
TypeOK ==
 /\ pub \in Start..MaxSeq /\ busy \in Readers \cup {0}
 /\ version \in 1..(1+2*Cardinality(Readers)) /\ clockExpired \in BOOLEAN
 /\ \A r \in Readers :
    /\ entries[r].stage \in {"Empty","Reserved","Prepared","Delivered","Active","Failed","Expired","Quiescing","Detached"}
    /\ entries[r].base<=entries[r].app /\ entries[r].app<=entries[r].rd /\ entries[r].rd<=pub
    /\ entries[r].base<=entries[r].rel /\ entries[r].rel<=entries[r].rd
Safety ==
 /\ \A r \in Present : pub-Gate(r)<=Capacity
 /\ \A r \in Readers : entries[r].held => payload[Slot(entries[r].rd)]=entries[r].rd
 /\ \A r \in Present : entries[r].attached =>
      \A s \in (entries[r].rd+1)..pub : payload[Slot(s)]=s
 /\ \A r \in Readers : entries[r].active =>
      /\ entries[r].cursor /\ entries[r].bankAck /\ entries[r].snapAck
      /\ entries[r].activatedAt>=entries[r].base
      /\ entries[r].app>=entries[r].activatedAt
 /\ \A r \in Readers : entries[r].stage="Detached" =>
      /\ entries[r].cursor /\ entries[r].life=5 /\ ~entries[r].held
      /\ entries[r].closedRel=entries[r].rd
      /\ entries[r].closedApp<=entries[r].closedRel
 /\ \A r \in Present : ~entries[r].active => Gate(r)=entries[r].base
Binding ==
 /\ version=1+Cardinality({r \in Readers:entries[r].birth#0})
               +Cardinality({r \in Readers:entries[r].stage="Detached"})
 /\ \A r \in Readers : entries[r].birth#0 => entries[r].birth<=version
 /\ \A r,s \in Readers : r#s /\ entries[r].birth#0 /\ entries[s].birth#0 => entries[r].birth#entries[s].birth
=============================================================================
