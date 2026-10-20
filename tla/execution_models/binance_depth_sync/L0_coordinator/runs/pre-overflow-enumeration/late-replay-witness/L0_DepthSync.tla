----------------------------- MODULE L0_DepthSync -----------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS None, Policy, Capacity, MaxEpoch, MaxGeneration, MaxRequest, MaxId, MaxTime, MaximumAge
ASSUME /\ Policy \in {"Spot", "UsdM"} /\ Capacity >= 1 /\ MaxEpoch >= 2
       /\ MaxGeneration >= 2 /\ MaxRequest >= 2 /\ MaxId >= 2 /\ MaxTime >= 1

(***************************************************************************
VAR <-> CODE (all paths relative to repository root)
 s.phase/conn/lastConn/sub: State, connection, last_connection_epoch, subscribed
   crates/tickwire-binance/src/depth_sync.rs:124-136; connect 202-226.
 s.gen: Book.generation and decoder context generation; reset_book 444-461.
 s.buf/first: VecDeque<DepthUpdate> and first_u, 296-304; clear 439-443.
 s.pending/req: current (SnapshotToken,deadline) and monotonic request_id;
   request_snapshot 228-263. Token = connection epoch, generation, request id;
   BookKey is fixed. deadline is an exclusive completion bound, not exchange time.
 s.clock: last_clock, checked at callback dispatch, 477-484; no wall-clock daemon.
 s.book: concrete price's last validated qty plus id/stamp/phase/gen. qty is a
   content witness, not an unknown outer level or complete sorted book. Book
   crates/tickwire-book/src/lib.rs:620-710; policies 182-224.
 s.origin: ghost identity of snapshot accepted for current book, 325-365.
 issued: environment handles returned by request_snapshot 252-261. Old callbacks
   select an actually returned token, not a fabricated request.
 outcome/viewNow: ghost public API result and latest view call argument 428-433.
 One action = one run-to-completion &mut public call. Replay is a pure fold, so
 no other API observes an intermediate Live prefix. Decoder/payload failures,
 late HTTP callbacks, stale/duplicate deltas, gaps, overflow and disconnect are
 admitted inputs. No invented concurrent mutation or durable crash recovery.
***************************************************************************)
VARIABLES s, issued, outcome, viewNow
vars == <<s, issued, outcome, viewNow>>
No == -1
Ids == 0..MaxId
Times == 0..MaxTime
Quantities == {0,1}
Token(c,g,r) == [conn |-> c, gen |-> g, req |-> r]
EmptyBook(g) == [id |-> No, stamp |-> No, qty |-> 0, phase |-> "Awaiting", gen |-> g]
Clear(x) == [x EXCEPT !.buf = <<>>, !.first = No, !.pending = None]
Exhaust(x) == [Clear(x) EXCEPT !.phase = "Exhausted", !.conn = 0, !.sub = FALSE,
                              !.book.phase = "Invalid"]
Reset(x) == IF x.gen = MaxGeneration THEN Exhaust(x)
            ELSE [Clear(x) EXCEPT !.gen = @+1, !.phase = "Buffering", !.book = EmptyBook(x.gen+1), !.origin = None]
Clocked(x,t) == [x EXCEPT !.clock = t]
GoodClock(t) == s.clock = No \/ t >= s.clock
ReadyRequest == s.phase = "Buffering" /\ s.sub /\ s.first # No
Result(r) == /\ outcome' = r /\ viewNow' = No

\* Representative valid ranges include one overlapping batch and single IDs.
\* Independent pu choices include exact and contradictory previous watermarks.
Ranges == {[first |-> i, last |-> i, previous |-> p] : i \in Ids, p \in Ids}
          \cup {[first |-> 0, last |-> 1, previous |-> 0]}
Events(t,g) == {[first |-> r.first, last |-> r.last, previous |-> r.previous,
                stamp |-> t, gen |-> g, qty |-> q, valid |-> v]
                : r \in Ranges, q \in Quantities, v \in BOOLEAN}
\* Book range -> stale -> pu/gap -> clock -> payload order is preserved.
\* crates/tickwire-book/src/lib.rs:182-224,644-710
Classify(b,e) ==
 IF Policy = "Spot" THEN
   IF e.last <= b.id THEN "Stale" ELSE IF e.first > b.id+1 THEN "Error" ELSE "Apply"
 ELSE IF e.last < b.id \/ (b.phase # "Bridging" /\ e.last = b.id) THEN "Stale"
 ELSE IF (b.phase = "Bridging" /\ e.first > b.id)
      \/ (b.phase # "Bridging" /\ e.previous # b.id) THEN "Error" ELSE "Apply"
Apply(b,e) ==
 LET c == Classify(b,e) IN
 IF c = "Stale" THEN b
 ELSE IF c = "Error" \/ ~e.valid \/ (b.stamp # No /\ e.stamp < b.stamp)
      THEN [b EXCEPT !.phase = "Invalid"]
 ELSE [b EXCEPT !.id = e.last, !.stamp = e.stamp, !.qty = e.qty, !.phase = "Live"]
RECURSIVE Replay(_,_,_)
Replay(b,queue,applied) ==
 IF queue = <<>> THEN [book |-> b, applied |-> applied]
 ELSE LET next == Apply(b,Head(queue)) IN
      IF next.phase = "Invalid" THEN [book |-> next, applied |-> applied]
      ELSE Replay(next,Tail(queue),applied + (IF Classify(b,Head(queue))="Apply" THEN 1 ELSE 0))

\* new, crates/tickwire-binance/src/depth_sync.rs:139-200
Init == /\ s = [phase |-> "Disconnected", conn |-> 0, lastConn |-> 0, gen |-> 0,
                  sub |-> FALSE, buf |-> <<>>, first |-> No, pending |-> None,
                  req |-> 0, clock |-> No, book |-> EmptyBook(0), origin |-> None]
        /\ issued = {} /\ outcome = "Init" /\ viewNow = No

\* connect: crates/tickwire-binance/src/depth_sync.rs:202-218
Connect ==
 /\ s.phase # "Exhausted" /\ s.lastConn < MaxEpoch
 /\ s' = IF s.gen=MaxGeneration THEN Exhaust(s)
         ELSE [Reset(s) EXCEPT !.conn=s.lastConn+1, !.lastConn=@+1, !.sub=FALSE, !.clock=No]
 /\ UNCHANGED issued /\ Result(IF s.gen=MaxGeneration THEN "Exhausted" ELSE "Connect")
ConnectRejected == /\ s.phase="Exhausted" \/ s.lastConn>0
                   /\ UNCHANGED <<s,issued>> /\ Result("ConnectRejected")
\* subscription_ready: 220-226; matching token required.
Subscribe == /\ s.conn # 0 /\ s'=[s EXCEPT !.sub=TRUE]
             /\ UNCHANGED issued /\ Result("Subscribe")
\* old delta/subscribe/disconnect: 222-223,270-272,404-406.
OldConnection == /\ \E c \in 1..s.lastConn : c # s.conn
                 /\ UNCHANGED <<s,issued>> /\ Result("OldConnection")
\* current_request is checked before clock or JSON: 325-330,377-382,434-438.
OldSnapshot == /\ \E token \in issued : IF s.pending=None THEN TRUE ELSE token # s.pending.token
               /\ UNCHANGED <<s,issued>> /\ Result("OldSnapshot")

\* request_snapshot guards and exact request token: 228-263.
Request ==
 /\ ReadyRequest /\ s.pending=None
 /\ \E t \in Times, deadline \in Times :
     /\ GoodClock(t) /\ deadline>t
     /\ LET token == Token(s.conn,s.gen,s.req+1) IN
        /\ s' = IF s.req=MaxRequest THEN Exhaust(Clocked(s,t))
                ELSE [Clocked(s,t) EXCEPT !.req=@+1, !.pending=[token |-> token, deadline |-> deadline]]
        /\ issued' = IF s.req=MaxRequest THEN issued ELSE issued \cup {token}
 /\ Result(IF s.req=MaxRequest THEN "Exhausted" ELSE "Request")
RequestBadState == /\ ~ReadyRequest /\ UNCHANGED <<s,issued>> /\ Result("RequestBadState")
RequestBadDeadline == /\ ReadyRequest
 /\ \E t \in Times, deadline \in Times : /\ GoodClock(t) /\ deadline<=t /\ s'=Clocked(s,t)
 /\ UNCHANGED issued /\ Result("RequestBadDeadline")
RequestPending == /\ ReadyRequest /\ s.pending # None
 /\ \E t \in Times, deadline \in Times : /\ GoodClock(t) /\ deadline>t /\ s'=Clocked(s,t)
 /\ UNCHANGED issued /\ Result("RequestPending")

\* observe_clock failure occurs for current callbacks only; 273-275,328-330,
\* 380-382,394-396,477-484. expire_snapshot makes it reachable in any connected state.
ClockRegression == /\ s.conn # 0 /\ \E t \in Times : ~GoodClock(t)
 /\ s'=Reset(s) /\ UNCHANGED issued /\ Result("ClockRegression")
\* decoder failure or total update bound, before buffering: 276-295.
DeltaInvalid == /\ s.conn # 0
 /\ \E t \in Times : /\ GoodClock(t) /\ s'=Reset(Clocked(s,t))
 /\ UNCHANGED issued /\ Result("DeltaInvalid")
\* bounded FIFO append or overflow: 296-304.
DeltaBuffer == /\ s.conn # 0 /\ s.phase="Buffering"
 /\ \E t \in Times : \E e \in Events(t,s.gen) :
     /\ GoodClock(t)
     /\ s'=IF Len(s.buf)=Capacity THEN Reset(Clocked(s,t))
            ELSE [Clocked(s,t) EXCEPT !.buf=Append(@,e), !.first=IF @=No THEN e.first ELSE @]
 /\ UNCHANGED issued /\ Result(IF Len(s.buf)=Capacity THEN "Overflow" ELSE "Buffered")
\* current live/bridge event, exact real Book policy: 306-316.
DeltaApply == /\ s.conn # 0 /\ s.phase \in {"Bridging","Live"}
 /\ \E t \in Times : \E e \in Events(t,s.gen) :
     /\ GoodClock(t)
     /\ LET b == Apply(s.book,e) IN
        /\ s'=IF b.phase="Invalid" THEN Reset(Clocked(s,t))
               ELSE [Clocked(s,t) EXCEPT !.book=b, !.phase=b.phase]
        /\ outcome'=IF b.phase="Invalid" THEN "ApplyFailure"
                    ELSE IF Classify(s.book,e)="Stale" THEN "Stale" ELSE "Applied"
 /\ UNCHANGED issued /\ viewNow'=No

\* matching response, expiry before status/decoder: 318-337.
SnapshotExpired == /\ s.pending # None
 /\ \E t \in Times : /\ GoodClock(t) /\ t>=s.pending.deadline
                     /\ s'=[Clocked(s,t) EXCEPT !.pending=None]
 /\ UNCHANGED issued /\ Result("Expired")
SnapshotHttpFailure == /\ s.pending # None
 /\ \E t \in Times : /\ GoodClock(t) /\ t<s.pending.deadline
                     /\ s'=[Clocked(s,t) EXCEPT !.pending=None]
 /\ UNCHANGED issued /\ Result("HttpFailure")
\* snapshot decode/limit/install failure: 338-354; clearing is via reset.
SnapshotInvalid == /\ s.pending # None
 /\ \E t \in Times : /\ GoodClock(t) /\ t<s.pending.deadline
                     /\ s'=Reset([Clocked(s,t) EXCEPT !.pending=None])
 /\ UNCHANGED issued /\ Result("SnapshotInvalid")
\* Spot first-U retry, then installation + entire atomic replay: 349-370.
SnapshotResponse(delayed) == /\ s.pending # None
 /\ \E t \in Times, id \in Ids, quantity \in Quantities :
     /\ GoodClock(t) /\ t<s.pending.deadline
     /\ LET x == [Clocked(s,t) EXCEPT !.pending=None]
            old == Policy="Spot" /\ id<s.first
            b == [EmptyBook(s.gen) EXCEPT !.id=id, !.qty=quantity, !.phase="Bridging"]
            r == Replay(b,s.buf,0)
        IN /\ delayed = (IF old THEN FALSE ELSE r.book.phase="Live" /\ r.book.stamp<t)
           /\ s'=IF old THEN x
                  ELSE IF r.book.phase="Invalid" THEN Reset([x EXCEPT !.book=r.book, !.origin=s.pending.token])
                  ELSE [x EXCEPT !.book=r.book, !.phase=r.book.phase,
                                !.buf= <<>>, !.first=No, !.origin=s.pending.token]
           /\ outcome'=IF old THEN "TooOld"
                       ELSE IF r.book.phase="Invalid" THEN
                         IF r.applied>0 THEN "LateReplayFailure" ELSE "ReplayFailure"
                       ELSE IF r.book.phase="Live" THEN
                         IF delayed THEN "SnapshotDelayedLive" ELSE "SnapshotLive"
                       ELSE "SnapshotBridging"
 /\ UNCHANGED issued /\ viewNow'=No
SnapshotValid == SnapshotResponse(FALSE) /\ outcome' # "SnapshotDelayedLive"
SnapshotDelayed == SnapshotResponse(TRUE) /\ outcome' = "SnapshotDelayedLive"
\* snapshot_failed token was matched; it only cancels current request: 372-389.
SnapshotTransportFailure == /\ s.pending # None
 /\ \E t \in Times : /\ GoodClock(t) /\ s'=[Clocked(s,t) EXCEPT !.pending=None]
 /\ UNCHANGED issued /\ Result("TransportFailure")
\* timer, including no request and disconnected no-op: 390-402.
Expire ==
 /\ \E t \in Times :
      IF s.conn=0 THEN /\ s'=s /\ outcome'="ExpireNone"
      ELSE /\ GoodClock(t)
           /\ LET expired == IF s.pending=None THEN FALSE ELSE t>=s.pending.deadline IN
              /\ s'=IF expired THEN [Clocked(s,t) EXCEPT !.pending=None] ELSE Clocked(s,t)
              /\ outcome'=IF expired THEN "Expired" ELSE "ExpireNone"
 /\ UNCHANGED issued /\ viewNow'=No
\* disconnect invalidates book but retains diagnostic content: 403-414.
Disconnect == /\ s.conn # 0
 /\ s'=[Clear(s) EXCEPT !.conn=0, !.sub=FALSE, !.phase="Disconnected", !.book.phase="Invalid"]
 /\ UNCHANGED issued /\ Result("Disconnect")
\* view at creation only: depth_sync 428-433; Book 737-761.
View ==
 /\ \E t \in Times :
      /\ viewNow'=t
      /\ outcome'=IF s.phase # "Live" THEN "ViewNotLive"
                  ELSE IF t<s.book.stamp THEN "ViewClock"
                  ELSE IF t-s.book.stamp>MaximumAge THEN "ViewStale" ELSE "View"
 /\ UNCHANGED <<s,issued>>

Next == Connect \/ ConnectRejected \/ Subscribe \/ OldConnection \/ OldSnapshot
 \/ Request \/ RequestBadState \/ RequestBadDeadline \/ RequestPending
 \/ ClockRegression \/ DeltaInvalid \/ DeltaBuffer \/ DeltaApply
 \/ SnapshotExpired \/ SnapshotHttpFailure \/ SnapshotInvalid \/ SnapshotValid \/ SnapshotDelayed
 \/ SnapshotTransportFailure \/ Expire \/ Disconnect \/ View
Spec == Init /\ [][Next]_vars

TypeOK == /\ s.phase \in {"Disconnected","Buffering","Bridging","Live","Exhausted"}
 /\ s.conn \in 0..MaxEpoch /\ s.lastConn \in 0..MaxEpoch /\ s.gen \in 0..MaxGeneration
 /\ s.sub \in BOOLEAN /\ s.first \in Ids \cup {No} /\ s.req \in 0..MaxRequest
 /\ s.clock \in Times \cup {No} /\ Len(s.buf)<=Capacity
 /\ s.book.id \in Ids \cup {No} /\ s.book.stamp \in Times \cup {No}
 /\ s.book.qty \in Quantities /\ s.book.gen=s.gen
 /\ \A i \in 1..Len(s.buf): s.buf[i].gen=s.gen
 /\ viewNow \in Times \cup {No}
PendingBelongs == s.pending # None =>
 /\ s.phase="Buffering" /\ s.sub /\ s.conn # 0 /\ s.first # No
 /\ s.pending.token=Token(s.conn,s.gen,s.req) /\ s.pending.token \in issued
LiveHasCompleteReplay == s.phase="Live" =>
 /\ s.conn # 0 /\ s.sub /\ s.buf= <<>> /\ s.first=No /\ s.pending=None
 /\ s.book.phase="Live" /\ s.book.id \in Ids /\ s.book.stamp \in Times
 /\ s.origin=Token(s.conn,s.gen,s.req)
FirstIsBuffered == s.phase="Buffering" =>
 /\ s.book.phase="Awaiting" /\ s.book.id=No
 /\ (IF s.buf= <<>> THEN s.first=No ELSE s.first=Head(s.buf).first)
ClosedNotVisible == s.phase \in {"Disconnected","Exhausted"} =>
 s.conn=0 /\ ~s.sub /\ s.buf= <<>> /\ s.pending=None
FailuresClose == outcome \in {"ClockRegression","DeltaInvalid","Overflow","ApplyFailure",
 "SnapshotInvalid","LateReplayFailure","ReplayFailure","Exhausted"} => s.phase # "Live"
ViewFreshAtCreation == outcome="View" =>
 s.phase="Live" /\ viewNow>=s.book.stamp /\ viewNow-s.book.stamp<=MaximumAge

OldCallbacksCannotMutate == [][outcome' \in {"OldConnection","OldSnapshot"} => s'=s]_vars
RetryKeepsBufferedContent == [][outcome' \in {"Expired","HttpFailure","TransportFailure","TooOld"} =>
 s'.gen=s.gen /\ s'.buf=s.buf /\ s'.book=s.book /\ s'.first=s.first]_vars
StaleDoesNotRefresh == [][outcome'="Stale" => s'.book=s.book]_vars
GenerationMonotonic == [][s'.gen>=s.gen]_vars
RequestMonotonic == [][s'.req>=s.req]_vars
SnapshotRetainsArrivalStamp == [][outcome' \in {"SnapshotLive","SnapshotDelayedLive"} =>
 \E i \in 1..Len(s.buf): s'.book.stamp=s.buf[i].stamp /\ s'.book.qty=s.buf[i].qty]_vars
=============================================================================
