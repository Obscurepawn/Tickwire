-------------------------- MODULE L0_BookState --------------------------
EXTENDS Integers, TLC
CONSTANTS Policy, MaxGeneration, MaxId, MaxTime, MaxPayload, MaximumAge
ASSUME /\ Policy \in {"Spot", "UsdM"}
       /\ MaxGeneration >= 1 /\ MaxId >= 2 /\ MaxTime >= 2
       /\ MaxPayload >= 1 /\ MaximumAge \in 0..MaxTime

(***************************************************************************
Var/code map (crates/tickwire-book/src/lib.rs, SHA256 37beb8dc...fd04a3):
 generation: Book.generation 576, begin_rebuild 712-725.
 phase: Book.state 577; InvalidReason variants collapse to Invalid.
 lastId/stamp: optional last_update_id/last_event_at_ns 578-579; None = -1.
 bid/ask: abstract independently distinguishable validated side contents 580-581.
   These witnesses do NOT model sorting, numeric capacity or validation internals.
 outcome: ghost return category of the last public API call.
 viewNow: ghost time argument of the latest successful view, consumed before
   another call; represents no long-lived cache or production risk gate.
 One action = one safe-Rust run-to-completion Book method. Wrong identity is
 projected to the equality check (771-779); it cannot mutate this Book.
***************************************************************************)
VARIABLES generation, phase, lastId, stamp, bid, ask, outcome, viewNow
vars == <<generation, phase, lastId, stamp, bid, ask, outcome, viewNow>>
core == <<generation, phase, lastId, stamp, bid, ask>>
data == <<lastId, stamp, bid, ask>>
No == -1
Generations == 0..MaxGeneration
Ids == 0..MaxId
Times == 0..MaxTime
Payloads == 0..MaxPayload
Phases == {"Awaiting", "Bridging", "Live", "Invalid"}
Outcomes == {"New", "Snapshot", "SnapshotError", "SnapshotIdentity", "SnapshotState",
 "Applied", "Stale", "SequenceError", "ClockError", "PayloadError", "ApplyIdentity",
 "ApplyState", "Rebuilt", "RebuildError", "Invalidated", "InvalidateIdentity",
 "View", "ViewNotLive", "ViewClock", "ViewStale"}
Receiving == phase \in {"Bridging", "Live"}
SnapshotReady == phase \in {"Awaiting", "Bridging"}

\* validate_range precedes stale, which precedes pu/gap: crates/tickwire-book/src/lib.rs:182-224.
Classify(first, final, previous) ==
 IF first > final THEN "Error"
 ELSE IF Policy = "Spot" THEN
   IF final <= lastId THEN "Stale"
   ELSE IF first > lastId + 1 THEN "Error" ELSE "Apply"
 ELSE
   IF final < lastId \/ (phase # "Bridging" /\ final = lastId) THEN "Stale"
   ELSE IF previous = No THEN "Error"
   ELSE IF (phase = "Bridging" /\ first > lastId)
        \/ (phase # "Bridging" /\ previous # lastId) THEN "Error"
   ELSE "Apply"
GoodClock(t) == stamp = No \/ t >= stamp
Result(r) == /\ outcome' = r /\ viewNow' = No

\* new: crates/tickwire-book/src/lib.rs:590-605.
Init == /\ generation = 0 /\ phase = "Awaiting"
        /\ lastId = No /\ stamp = No /\ bid = 0 /\ ask = 0
        /\ outcome = "New" /\ viewNow = No

\* install_snapshot: validation precedes both side writes, crates/tickwire-book/src/lib.rs:620-642.
SnapshotAccepted ==
 /\ SnapshotReady
 /\ \E id \in Ids, b \in Payloads, a \in Payloads:
      /\ lastId' = id /\ bid' = b /\ ask' = a
 /\ phase' = "Bridging" /\ stamp' = No
 /\ UNCHANGED generation /\ Result("Snapshot")
SnapshotRejected ==
 /\ SnapshotReady /\ phase' = "Invalid"
 /\ UNCHANGED <<generation, data>> /\ Result("SnapshotError")
SnapshotWrongIdentity ==
 /\ UNCHANGED core /\ Result("SnapshotIdentity")
SnapshotBadState ==
 /\ ~SnapshotReady /\ UNCHANGED core /\ Result("SnapshotState")

\* apply: identity/state, policy, clock, batch preparation, commit: 644-710.
ApplyAccepted ==
 /\ Receiving
 /\ \E first \in Ids, final \in Ids, previous \in Ids \cup {No}, t \in Times,
         b \in Payloads, a \in Payloads:
      /\ Classify(first, final, previous) = "Apply" /\ GoodClock(t)
      /\ final >= lastId /\ first <= final
      /\ lastId' = final /\ stamp' = t /\ bid' = b /\ ask' = a
 /\ phase' = "Live" /\ UNCHANGED generation /\ Result("Applied")
ApplyStale ==
 /\ Receiving
 /\ \E first \in Ids, final \in Ids, previous \in Ids \cup {No}:
       Classify(first, final, previous) = "Stale"
 /\ UNCHANGED core /\ Result("Stale")
ApplyBadSequence ==
 /\ Receiving
 /\ \E first \in Ids, final \in Ids, previous \in Ids \cup {No}:
       Classify(first, final, previous) = "Error"
 /\ phase' = "Invalid" /\ UNCHANGED <<generation, data>>
 /\ Result("SequenceError")
ApplyBadClock ==
 /\ Receiving
 /\ \E first \in Ids, final \in Ids, previous \in Ids \cup {No}, t \in Times:
      /\ Classify(first, final, previous) = "Apply" /\ ~GoodClock(t)
 /\ phase' = "Invalid" /\ UNCHANGED <<generation, data>>
 /\ Result("ClockError")
ApplyBadPayload ==
 /\ Receiving
 /\ \E first \in Ids, final \in Ids, previous \in Ids \cup {No}, t \in Times:
      /\ Classify(first, final, previous) = "Apply" /\ GoodClock(t)
 /\ phase' = "Invalid" /\ UNCHANGED <<generation, data>>
 /\ Result("PayloadError")
ApplyWrongIdentity == /\ UNCHANGED core /\ Result("ApplyIdentity")
ApplyBadState == /\ ~Receiving /\ UNCHANGED core /\ Result("ApplyState")

\* begin_rebuild strictly increases generation and clears both sides: 712-725.
RebuildAccepted ==
 /\ \E g \in Generations: /\ g > generation /\ generation' = g
 /\ phase' = "Awaiting" /\ lastId' = No /\ stamp' = No
 /\ bid' = 0 /\ ask' = 0 /\ Result("Rebuilt")
RebuildRejected ==
 /\ \E g \in Generations: g <= generation
 /\ UNCHANGED core /\ Result("RebuildError")
\* invalidate, including delayed old generation callbacks: 727-735.
InvalidateAccepted ==
 /\ phase' = "Invalid" /\ UNCHANGED <<generation, data>> /\ Result("Invalidated")
InvalidateWrongGeneration ==
 /\ \E g \in Generations: g # generation
 /\ UNCHANGED core /\ Result("InvalidateIdentity")

\* view freshness is checked at creation only: 737-761; age accessor 886-889.
ViewAccepted ==
 /\ phase = "Live"
 /\ \E now \in Times:
      /\ now >= stamp /\ now - stamp <= MaximumAge /\ viewNow' = now
 /\ UNCHANGED core /\ outcome' = "View"
ViewNotLive == /\ phase # "Live" /\ UNCHANGED core /\ Result("ViewNotLive")
ViewClockRegressed ==
 /\ phase = "Live" /\ \E now \in Times: now < stamp
 /\ UNCHANGED core /\ Result("ViewClock")
ViewStale ==
 /\ phase = "Live" /\ \E now \in Times: now >= stamp /\ now-stamp > MaximumAge
 /\ UNCHANGED core /\ Result("ViewStale")

Next == SnapshotAccepted \/ SnapshotRejected \/ SnapshotWrongIdentity \/ SnapshotBadState
 \/ ApplyAccepted \/ ApplyStale \/ ApplyBadSequence \/ ApplyBadClock \/ ApplyBadPayload
 \/ ApplyWrongIdentity \/ ApplyBadState \/ RebuildAccepted \/ RebuildRejected
 \/ InvalidateAccepted \/ InvalidateWrongGeneration
 \/ ViewAccepted \/ ViewNotLive \/ ViewClockRegressed \/ ViewStale
Spec == Init /\ [][Next]_vars
TypeOK == /\ generation \in Generations /\ phase \in Phases
 /\ lastId \in Ids \cup {No} /\ stamp \in Times \cup {No}
 /\ bid \in Payloads /\ ask \in Payloads /\ outcome \in Outcomes
 /\ viewNow \in Times \cup {No}
Lifecycle == /\ (phase = "Awaiting" => data = <<No, No, 0, 0>>)
 /\ (phase = "Bridging" => lastId \in Ids /\ stamp = No)
 /\ (phase = "Live" => lastId \in Ids /\ stamp \in Times)
SnapshotIsNotLive == outcome = "Snapshot" => phase = "Bridging" /\ stamp = No
RebuildClears == outcome = "Rebuilt" => phase = "Awaiting" /\ data = <<No,No,0,0>>
ViewIsCurrentAtCreation == outcome = "View" =>
 phase = "Live" /\ viewNow >= stamp /\ viewNow - stamp <= MaximumAge
FailuresInvalidate == outcome \in {"SnapshotError","SequenceError","ClockError","PayloadError"}
 => phase = "Invalid"

\* Action safety properties compare pre/post contents, without copying payload history.
NoDataChangeOnFailure == [][(outcome' \notin {"Snapshot","Applied","Rebuilt"}) => data' = data]_vars
IdentityCannotMutate == [][(outcome' \in {"SnapshotIdentity","ApplyIdentity","InvalidateIdentity"})
 => core' = core]_vars
StaleDoesNotRefresh == [][outcome' = "Stale" => core' = core]_vars
GenerationNeverDecreases == [][generation' >= generation]_vars
OnlyRebuildLeavesInvalid == [][(phase = "Invalid" /\ phase' # "Invalid")
 => outcome' = "Rebuilt"]_vars
AppliedWatermarkNeverDecreases == [][outcome' = "Applied" => lastId' >= lastId]_vars
=============================================================================
