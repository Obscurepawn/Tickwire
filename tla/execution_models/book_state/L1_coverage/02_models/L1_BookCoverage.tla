------------------------ MODULE L1_BookCoverage ------------------------
EXTENDS Integers, FiniteSets, TLC
CONSTANTS Side, PriceCount, Capacity, MaxQuantity, MaxGeneration
ASSUME /\ Side \in {"Bid", "Ask"} /\ PriceCount >= 3
       /\ Capacity \in 1..(PriceCount-1) /\ MaxQuantity >= 1 /\ MaxGeneration >= 1

(***************************************************************************
Var/code map: crates/tickwire-book/src/lib.rs (SHA256 37beb8dc...fd04a3).
 generation/phase: Book 576-577, rebuild 712-725, snapshot 620-642, apply 644-710.
 local: one Levels side 580-581, extensional price->quantity representation;
   zero means absent. Algorithms and raw input validation are abstracted.
 coverage: side coverage 582-583; 0=Unknown, PriceCount+1=Complete, other=Through
   at that priority rank. Derived on snapshot 850-858, unchanged on apply 699-705.
 world: ghost complete side at the latest accepted sequence watermark. Its
   hidden prices are not accessible to the implementation. Snapshot accuracy /
   completeness and delivery of accepted changes are explicit adapter contracts.
 outcome: ghost API/query branch observer, never a production risk gate.
 L0 supplies identity, sequence and creation-time freshness semantics. Each L1
 apply denotes an L0 policy/clock-accepted event; ViewPermitted denotes a fresh
 L0 view. L1 does not recheck clocks or prove a formal refinement mapping.
***************************************************************************)
VARIABLES generation, phase, local, coverage, world, outcome
vars == <<generation, phase, local, coverage, world, outcome>>
core == <<generation, phase, local, coverage, world>>
Prices == 1..PriceCount
Quantities == 0..MaxQuantity
Maps == [Prices -> Quantities]
Empty == [p \in Prices |-> 0]
NoUpdate == -1
Patches == [Prices -> Quantities \cup {NoUpdate}]
Generations == 0..MaxGeneration
Rank(p) == IF Side = "Bid" THEN PriceCount+1-p ELSE p
Covered(c,p) == c = PriceCount+1 \/ (c # 0 /\ Rank(p) <= c)
Positive(m) == {p \in Prices: m[p] > 0}
Best(m) == IF Positive(m) = {} THEN 0
 ELSE CHOOSE p \in Positive(m): \A q \in Positive(m): Rank(p) <= Rank(q)
WorstRank(m) == IF Positive(m) = {} THEN 0
 ELSE CHOOSE r \in 1..PriceCount: \E p \in Positive(m):
       Rank(p)=r /\ \A q \in Positive(m): Rank(q)<=r
SnapshotMap(w, cut, complete) ==
 [p \in Prices |-> IF complete \/ Rank(p)<=cut THEN w[p] ELSE 0]
SnapshotCoverage(m, complete) == IF complete THEN PriceCount+1 ELSE WorstRank(m)
Patched(m, patch) == [p \in Prices |-> IF patch[p]=NoUpdate THEN m[p] ELSE patch[p]]
Fits(m) == Cardinality(Positive(m)) <= Capacity
Receiving == phase \in {"Bridging","Live"}
SnapshotReady == phase \in {"Awaiting","Bridging"}
ViewPermitted == phase = "Live"
\* quantity: present positive overrides coverage; absent only known in coverage: 902-914.
QuantityAnswer(p) == IF local[p]>0 \/ Covered(coverage,p) THEN local[p] ELSE -1
\* best: even a stored outside-boundary best is conservatively Unknown: 915-929.
BestAnswer == IF Best(local)#0 /\ Covered(coverage,Best(local)) THEN Best(local)
 ELSE IF Best(local)=0 /\ coverage=PriceCount+1 THEN 0 ELSE -1
Compatible(w) == \A p \in Prices:
 (local[p]>0 \/ Covered(coverage,p)) => w[p]=local[p]
Change(w,p,q) == [w EXCEPT ![p]=q]

\* new and rebuild initialize empty sides with Unknown coverage: 590-605, 712-725.
Init == /\ generation=0 /\ phase="Awaiting" /\ local=Empty
        /\ coverage=0 /\ world=Empty /\ outcome="New"
\* snapshot: valid input includes truthful complete/partial snapshot: 620-642,850-858.
SnapshotAccepted ==
 /\ SnapshotReady
 /\ \E w \in Maps, cut \in 0..PriceCount, complete \in BOOLEAN:
      LET m == SnapshotMap(w,cut,complete) IN
      /\ Fits(m) /\ world'=w /\ local'=m
      /\ coverage'=SnapshotCoverage(m,complete)
 /\ phase'="Bridging" /\ UNCHANGED generation /\ outcome'="Snapshot"
SnapshotRejected ==
 /\ SnapshotReady /\ phase'="Invalid"
 /\ UNCHANGED <<generation,local,coverage,world>> /\ outcome'="Rejected"
\* valid, continuous input plans both sides before commit. One side refined here: 685-705.
ApplyAccepted ==
 /\ Receiving
 /\ \E patch \in Patches:
      /\ Fits(Patched(local,patch))
      /\ local'=Patched(local,patch) /\ world'=Patched(world,patch)
 /\ phase'="Live" /\ UNCHANGED <<generation,coverage>> /\ outcome'="Applied"
CapacityRejected ==
 /\ Receiving
 /\ \E patch \in Patches: ~Fits(Patched(local,patch))
 /\ phase'="Invalid" /\ UNCHANGED <<generation,local,coverage,world>>
 /\ outcome'="Rejected"
\* L0 projection: sequence error / explicit invalidation; failed apply never commits: 662-666,729-735.
Invalidate ==
 /\ phase'="Invalid" /\ UNCHANGED <<generation,local,coverage,world>>
 /\ outcome'="Invalidated"
IgnoreStale == /\ Receiving /\ UNCHANGED core /\ outcome'="Stale"
RejectIdentity == /\ UNCHANGED core /\ outcome'="WrongIdentity"
Rebuild ==
 /\ \E g \in Generations: /\ g>generation /\ generation'=g
 /\ phase'="Awaiting" /\ local'=Empty /\ coverage'=0
 /\ UNCHANGED world /\ outcome'="Rebuilt"

\* Read-only branches witness the knowledge states, not an invented order-submission gate.
ObserveKnownBest ==
 /\ ViewPermitted /\ BestAnswer>0 /\ UNCHANGED core /\ outcome'="KnownBest"
ObserveHiddenBest ==
 /\ ViewPermitted /\ BestAnswer=-1 /\ Best(world)>0
 /\ local[Best(world)]=0
 /\ UNCHANGED core /\ outcome'="HiddenBest"
ObserveUnknownZero ==
 /\ ViewPermitted
 /\ \E p \in Prices: QuantityAnswer(p)=-1 /\ world[p]=0
 /\ UNCHANGED core /\ outcome'="UnknownZero"
ObserveKnownZero ==
 /\ ViewPermitted /\ \E p \in Prices: QuantityAnswer(p)=0
 /\ UNCHANGED core /\ outcome'="KnownZero"
ObserveKnownBeyondCoverage ==
 /\ ViewPermitted
 /\ \E p \in Prices: local[p]>0 /\ ~Covered(coverage,p) /\ QuantityAnswer(p)>0
 /\ UNCHANGED core /\ outcome'="KnownBeyondCoverage"
ObserveCompleteEmpty ==
 /\ ViewPermitted /\ BestAnswer=0
 /\ UNCHANGED core /\ outcome'="CompleteEmpty"
ObserveNotLive ==
 /\ ~ViewPermitted /\ UNCHANGED core /\ outcome'="NotLive"

Next == SnapshotAccepted \/ SnapshotRejected \/ ApplyAccepted \/ CapacityRejected
 \/ Invalidate \/ IgnoreStale \/ RejectIdentity \/ Rebuild
 \/ ObserveKnownBest \/ ObserveHiddenBest \/ ObserveUnknownZero \/ ObserveKnownZero
 \/ ObserveKnownBeyondCoverage \/ ObserveCompleteEmpty \/ ObserveNotLive
Spec == Init /\ [][Next]_vars
TypeOK == /\ generation \in Generations
 /\ phase \in {"Awaiting","Bridging","Live","Invalid"}
 /\ local \in Maps /\ world \in Maps /\ coverage \in 0..(PriceCount+1)
 /\ outcome \in {"New","Snapshot","Rejected","Applied","Invalidated","Stale",
    "WrongIdentity","Rebuilt","KnownBest","HiddenBest","UnknownZero","KnownZero",
    "KnownBeyondCoverage","CompleteEmpty","NotLive"}
Bounded == Fits(local)
WorldAgreesWithKnowledge == Compatible(world)
KnownQuantitySound == ViewPermitted =>
 \A p \in Prices: QuantityAnswer(p)>=0 => QuantityAnswer(p)=world[p]
KnownBestSound == ViewPermitted /\ BestAnswer>=0 => BestAnswer=Best(world)
UnknownHasTwoWorlds == ViewPermitted =>
 \A p \in Prices: QuantityAnswer(p)=-1 =>
    Compatible(Change(world,p,0)) /\ Compatible(Change(world,p,1))
NotLiveNotObserved == outcome \in {"KnownBest","HiddenBest","UnknownZero","KnownZero",
 "KnownBeyondCoverage","CompleteEmpty"} => ViewPermitted
RebuildClearsKnowledge == phase="Awaiting" => local=Empty /\ coverage=0
FailureAtomic == [][outcome' \in {"Rejected","Invalidated","Stale","WrongIdentity"}
 => <<local',coverage',world'>> = <<local,coverage,world>>]_vars
IncrementalCoverageStable == [][outcome'="Applied" => coverage'=coverage]_vars
=============================================================================
