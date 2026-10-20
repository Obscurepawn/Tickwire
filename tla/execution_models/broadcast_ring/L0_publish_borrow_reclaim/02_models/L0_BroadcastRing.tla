-------------------------- MODULE L0_BroadcastRing --------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC

\* var <-> code map (fixed authenticated bank and distinct subscription keys)
\* Capacity=64: crates/tickwire-shm/src/lib.rs:134
\* p.pub/life/cache/claim/plan/version/frozen: Producer fields,
\*   crates/tickwire-shm/src/lib.rs:115-128. pub is a complete-content boundary.
\* m: Member baseline/released/active, crates/tickwire-shm/src/lib.rs:82-87.
\* rdr: Reader watermarks/completion/live borrows/quiescing,
\*   crates/tickwire-shm/src/lib.rs:417-431; released and applied are independent
\*   contiguous completion prefixes. life/qseq/token are mapped cursor evidence:
\*   crates/tickwire-shm/src/mapped.rs:519-547.
\* payload: ordinary slot bytes represented by their sequence tag,
\*   crates/tickwire-shm/src/mapped.rs:326-340.
\* rejected and p.complete: history-only error/publication witnesses, no code state.
\* Join: crates/tickwire-shm/src/lib.rs:194-233
\* Ready: crates/tickwire-shm/src/mapped.rs:215-229
\* Observe: crates/tickwire-shm/src/lib.rs:251-273
\* Claim: crates/tickwire-shm/src/lib.rs:275-312 (includes Full cache mutation)
\* Write/Publish/Abort: crates/tickwire-shm/src/mapped.rs:326-341
\* Prepare/Freeze: crates/tickwire-shm/src/lib.rs:333-370
\* Borrow/BorrowIdle: crates/tickwire-shm/src/lib.rs:466-496
\* Release/Apply: crates/tickwire-shm/src/lib.rs:498-541
\* StartQuiesce/Quiesced: crates/tickwire-shm/src/mapped.rs:529-547
\* Detach/RejectForged: crates/tickwire-shm/src/mapped.rs:360-402
\* Faults: arbitrary progress stalls (no fairness), including forgotten guards.
\* Not modeled: malicious unsafe peers, CPU weak-memory litmus, bank rotation.

CONSTANTS Readers, Capacity, Window, Prefix, PrefixMember, MaxSeq,
          ReadAdvance, Batches
ASSUME /\ Capacity = 64 /\ Window \in 1..1048576
       /\ Prefix \in {0,64} /\ PrefixMember \in {0,1,2}
       /\ Batches \subseteq 1..64 /\ MaxSeq \in 0..1024
       /\ Cardinality(Readers) \in 1..32

VARIABLES p, m, rdr, payload, rejected
vars == <<p,m,rdr,payload,rejected>>
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Min2(a,b) == IF a <= b THEN a ELSE b
\* TLC's Java IntValue cannot evaluate 2^63. Every selected input/counter is
\* bounded below 1048576, hence satisfies the real <2^63 guard. No claim here
\* covers exhaustion at that production boundary.
FiniteU64(n) == n \in 0..1048576
Slot(s) == (s-1) % Capacity
Index(r,s) == (s-rdr[r].base-1) % Window
EmptyToken == [valid |-> FALSE, rd |-> 0, app |-> 0, rel |-> 0]
Token(r) == [valid |-> TRUE, rd |-> rdr[r].rd,
             app |-> rdr[r].app, rel |-> rdr[r].rel]
Fake(r) == [valid |-> TRUE, rd |-> rdr[r].base,
            app |-> rdr[r].base, rel |-> rdr[r].base]
NewReader(b) ==
  [base |-> b, rd |-> b, app |-> b, rel |-> b, seen |-> b,
   live |-> {}, slots |-> [i \in 0..(Window-1) |->
      [seq |-> 0, rel |-> FALSE, app |-> FALSE]],
   quiescing |-> FALSE, life |-> 3, qseq |-> 0, token |-> EmptyToken]
Members == {r \in Readers : m[r].status \in {1,2}}
CurrentMin == IF Members = {} THEN p.pub ELSE Min({m[r].seen : r \in Members})
Boundary == p.last = 0
ClaimMin(last) ==
  LET old == IF p.cache = -1 THEN CurrentMin ELSE p.cache
  IN IF last-old > Capacity THEN CurrentMin ELSE old
PrefixDone(r,slots,field,start) ==
  Max({n \in start..rdr[r].rd :
    \A s \in (start+1)..n :
      slots[Index(r,s)].seq = s /\ slots[Index(r,s)][field]})
CanDetach(r,t) ==
  /\ m[r].status = 2 /\ rdr[r].life = 5
  /\ t.valid /\ t.rd = rdr[r].rd /\ t.app = rdr[r].app /\ t.rel = rdr[r].rel
  /\ rdr[r].qseq = rdr[r].rd
  /\ Boundary /\ t.rd = t.rel /\ t.rd <= p.pub
  /\ t.app <= t.rd /\ t.rel >= m[r].seen
  /\ FiniteU64(p.version+1)

Init ==
  /\ p = [pub |-> Prefix, life |-> "Active",
          cache |-> IF Prefix=0 THEN -1 ELSE 0,
          first |-> 0, last |-> 0, next |-> 0, plan |-> -1,
          version |-> IF Prefix=0 THEN 1 ELSE 1+Cardinality(Readers),
          frozenPub |-> -1, frozenVersion |-> 0, complete |-> TRUE]
  /\ m = [r \in Readers |-> [status |-> PrefixMember, seen |-> 0]]
  /\ rdr = [r \in Readers |-> NewReader(0)]
  /\ payload = [i \in 0..(Capacity-1) |-> IF Prefix=0 THEN 0 ELSE i+1]
  /\ rejected = {}

Join(r) ==
  /\ Boundary /\ p.life # "Frozen" /\ m[r].status = 0
  /\ Cardinality(Members) < Cardinality(Readers)
  /\ FiniteU64(p.version+1) /\ FiniteU64(p.pub+1)
  /\ m' = [m EXCEPT ![r] = [status |-> 1, seen |-> p.pub]]
  /\ rdr' = [rdr EXCEPT ![r] = NewReader(p.pub)]
  /\ p' = [p EXCEPT !.version = @+1, !.cache = -1]
  /\ UNCHANGED <<payload,rejected>>

Ready(r) ==
  /\ Boundary /\ p.life # "Frozen" /\ m[r].status = 1
  /\ rdr[r].rel >= m[r].seen /\ rdr[r].rel <= p.pub
  /\ m' = [m EXCEPT ![r] = [status |-> 2, seen |-> rdr[r].rel]]
  /\ p' = [p EXCEPT !.cache = -1]
  /\ UNCHANGED <<rdr,payload,rejected>>

Observe(r) ==
  /\ Boundary /\ m[r].status = 2
  /\ rdr[r].rel >= m[r].seen /\ rdr[r].rel <= p.pub
  /\ m' = [m EXCEPT ![r].seen = rdr[r].rel]
  /\ UNCHANGED <<p,rdr,payload,rejected>>

Claim(n) ==
  /\ Boundary /\ p.life = "Active" /\ n \in 1..64
  /\ p.pub+n <= MaxSeq /\ FiniteU64(p.pub+n)
  /\ (p.plan = -1 \/ p.pub+n <= p.plan)
  /\ LET last == p.pub+n
         gate == ClaimMin(last)
     IN p' = IF last-gate <= Capacity
             THEN [p EXCEPT !.cache = gate, !.first = p.pub+1,
                           !.last = last, !.next = p.pub+1]
             ELSE [p EXCEPT !.cache = gate]
  /\ UNCHANGED <<m,rdr,payload,rejected>>

Write ==
  /\ ~Boundary /\ p.next <= p.last
  /\ payload' = [payload EXCEPT ![Slot(p.next)] = p.next]
  /\ p' = [p EXCEPT !.next = @+1]
  /\ UNCHANGED <<m,rdr,rejected>>

Publish ==
  /\ ~Boundary /\ p.next = p.last+1
  /\ p' = [p EXCEPT !.pub = p.last, !.first = 0, !.last = 0, !.next = 0,
              !.complete = @ /\ (\A s \in p.first..p.last : payload[Slot(s)]=s)]
  /\ UNCHANGED <<m,rdr,payload,rejected>>

Abort ==
  /\ ~Boundary
  /\ p' = [p EXCEPT !.first = 0, !.last = 0, !.next = 0]
  /\ UNCHANGED <<m,rdr,payload,rejected>>

Prepare(cutoff) ==
  /\ Boundary /\ p.life = "Active" /\ cutoff >= p.pub /\ FiniteU64(cutoff)
  /\ (p.plan = -1 \/ p.plan = cutoff)
  /\ p' = [p EXCEPT !.plan = cutoff]
  /\ UNCHANGED <<m,rdr,payload,rejected>>

Freeze ==
  /\ Boundary /\ p.plan # -1 /\ p.pub = p.plan
  /\ p' = IF p.life = "Frozen" THEN p
          ELSE [p EXCEPT !.life = "Frozen", !.frozenPub = p.pub,
                         !.frozenVersion = p.version]
  /\ UNCHANGED <<m,rdr,payload,rejected>>

Borrow(r) ==
  /\ m[r].status \in {1,2} /\ ~rdr[r].quiescing
  /\ p.pub >= rdr[r].seen /\ FiniteU64(p.pub) /\ p.pub > rdr[r].rd
  /\ rdr[r].rd < rdr[r].base+ReadAdvance
  /\ LET s == rdr[r].rd+1 IN
     /\ s-Min2(rdr[r].app,rdr[r].rel) <= Window
     /\ rdr' = [rdr EXCEPT ![r].seen = p.pub, ![r].rd = s,
          ![r].live = @ \cup {s},
          ![r].slots[Index(r,s)] = [seq |-> s, rel |-> FALSE, app |-> FALSE]]
  /\ UNCHANGED <<p,m,payload,rejected>>

BorrowIdle(r) ==
  /\ m[r].status \in {1,2} /\ ~rdr[r].quiescing
  /\ p.pub >= rdr[r].seen /\ FiniteU64(p.pub)
  /\ (p.pub = rdr[r].rd \/ rdr[r].rd+1-Min2(rdr[r].app,rdr[r].rel) > Window)
  /\ rdr' = [rdr EXCEPT ![r].seen = p.pub]
  /\ UNCHANGED <<p,m,payload,rejected>>

Release(r,s) ==
  /\ s \in rdr[r].live
  /\ s > rdr[r].base /\ s <= rdr[r].rd
  /\ rdr[r].slots[Index(r,s)].seq = s /\ ~rdr[r].slots[Index(r,s)].rel
  /\ LET slots == [rdr[r].slots EXCEPT ![Index(r,s)].rel = TRUE]
     IN rdr' = [rdr EXCEPT ![r].slots = slots, ![r].live = @ \ {s},
                   ![r].rel = PrefixDone(r,slots,"rel",rdr[r].rel)]
  /\ UNCHANGED <<p,m,payload,rejected>>

Apply(r,s) ==
  /\ s > rdr[r].base /\ s <= rdr[r].rd
  /\ (s <= rdr[r].app \/ rdr[r].slots[Index(r,s)].seq = s)
  /\ rdr' = IF s <= rdr[r].app THEN rdr
     ELSE LET slots == [rdr[r].slots EXCEPT ![Index(r,s)].app = TRUE]
          IN [rdr EXCEPT ![r].slots = slots,
                       ![r].app = PrefixDone(r,slots,"app",rdr[r].app)]
  /\ UNCHANGED <<p,m,payload,rejected>>

StartQuiesce(r) ==
  /\ m[r].status \in {1,2} /\ ~rdr[r].quiescing
  /\ rdr' = [rdr EXCEPT ![r].quiescing = TRUE, ![r].life = 4]
  /\ UNCHANGED <<p,m,payload,rejected>>

Quiesced(r) ==
  /\ m[r].status \in {1,2} /\ rdr[r].quiescing /\ rdr[r].live = {}
  /\ rdr' = [rdr EXCEPT ![r].token = Token(r), ![r].qseq = rdr[r].rd, ![r].life = 5]
  /\ UNCHANGED <<p,m,payload,rejected>>

Detach(r,t) ==
  /\ CanDetach(r,t)
  /\ m' = [m EXCEPT ![r].status = 3]
  /\ p' = [p EXCEPT !.version = @+1, !.cache = -1]
  /\ UNCHANGED <<rdr,payload,rejected>>

RejectForged(r) ==
  /\ Boundary /\ m[r].status \in {1,2} /\ ~CanDetach(r,Fake(r))
  /\ rejected' = rejected \cup {r}
  /\ UNCHANGED <<p,m,rdr,payload>>

SuffixNext ==
  \/ \E r \in Readers : Observe(r)
  \/ \E n \in Batches : Claim(n)
  \/ Write \/ Publish \/ Abort
  \/ \E c \in Prefix..MaxSeq : Prepare(c)
  \/ Freeze
  \/ \E r \in Readers : Borrow(r)
  \/ \E r \in Readers : BorrowIdle(r)
  \/ \E r \in Readers : \E s \in rdr[r].live : Release(r,s)
  \/ \E r \in Readers : \E s \in (rdr[r].base+1)..rdr[r].rd : Apply(r,s)
  \/ \E r \in Readers : StartQuiesce(r)
  \/ \E r \in Readers : Quiesced(r)
  \/ \E r \in Readers : \E t \in {rdr[r].token,Fake(r)} : Detach(r,t)
  \/ \E r \in Readers : RejectForged(r)
PendingNext == SuffixNext \/ \E r \in Readers : Ready(r)
Next == PendingNext \/ \E r \in Readers : Join(r)
Spec == Init /\ [][Next]_vars
WrapSpec == Init /\ [][SuffixNext]_vars
PendingWrapSpec == Init /\ [][PendingNext]_vars

TypeOK ==
  /\ p.pub \in Prefix..MaxSeq /\ p.life \in {"Active","Frozen"}
  /\ p.cache \in -1..MaxSeq /\ p.plan \in -1..MaxSeq
  /\ p.version \in 1..(1+2*Cardinality(Readers))
  /\ p.last \in 0..MaxSeq /\ p.first \in 0..MaxSeq /\ p.next \in 0..(MaxSeq+1)
  /\ payload \in [0..(Capacity-1) -> 0..MaxSeq] /\ rejected \subseteq Readers
  /\ \A r \in Readers :
    /\ m[r].status \in 0..3 /\ m[r].seen \in 0..MaxSeq
    /\ rdr[r].base \in 0..MaxSeq /\ rdr[r].rd \in 0..MaxSeq
    /\ rdr[r].app \in 0..MaxSeq /\ rdr[r].rel \in 0..MaxSeq
    /\ rdr[r].seen \in 0..MaxSeq /\ rdr[r].life \in {3,4,5}
    /\ rdr[r].live \subseteq 1..MaxSeq

WatermarkOrder ==
  \A r \in Readers :
    /\ rdr[r].base <= rdr[r].app /\ rdr[r].base <= rdr[r].rel
    /\ rdr[r].app <= rdr[r].rd /\ rdr[r].rel <= rdr[r].rd
    /\ rdr[r].rd <= rdr[r].seen /\ rdr[r].seen <= p.pub
    /\ \A s \in rdr[r].live : s > rdr[r].rel /\ s <= rdr[r].rd
    /\ Cardinality(rdr[r].live) <= Window
    /\ rdr[r].rd-Min2(rdr[r].app,rdr[r].rel) <= Window

ConservativeCache == p.cache = -1 \/ p.cache <= CurrentMin
CompleteBeforePublication == p.complete
MemberGate ==
  \A r \in Members :
    /\ m[r].seen <= rdr[r].rel
    /\ (m[r].status=1 => m[r].seen=rdr[r].base)
    /\ p.pub-m[r].seen <= Capacity
    /\ (~Boundary => p.last-m[r].seen <= Capacity)
NoOverwriteBorrow ==
  \A r \in Readers : \A s \in rdr[r].live : payload[Slot(s)] = s
UnreadProtected ==
  \A r \in Members : \A s \in (rdr[r].rd+1)..p.pub : payload[Slot(s)] = s
DetachedSafe ==
  \A r \in Readers : m[r].status=3 =>
    /\ rdr[r].live={} /\ rdr[r].quiescing /\ rdr[r].life=5
    /\ rdr[r].rel=rdr[r].rd /\ rdr[r].qseq=rdr[r].rd
FreezeBoundary ==
  /\ (p.life="Frozen" => Boundary /\ p.pub=p.plan /\ p.pub=p.frozenPub)
  /\ (~Boundary => p.life="Active" /\ p.first=p.pub+1 /\ p.first<=p.next
                     /\ p.next<=p.last+1 /\ (p.plan=-1 \/ p.last<=p.plan))
=======================================================================
