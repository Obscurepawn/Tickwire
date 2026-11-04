---------------------------- MODULE L0_QuotePair_v2 ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

\* Fixed runtime policy. var <-> code and actor map:
\* h      owner batch log (abstract SHM slots); crates/tickwire-market-publisher/src/spot.rs:100-159
\* t      current monotonic reading; crates/tickwire-market-source/src/shm.rs:223-237
\* base, pos accepted JOIN/member frontier and applied sequence per process;
\*        crates/tickwire-market-source/src/replica.rs:274-316 and crates/tickwire-market-source/src/replica.rs:428-443
\* callback last observed SDK emission (ghost witness); crates/tickwire-strategy-sdk/src/replay/live.rs:299-335
\* mark   latest accepted observation sequence; valid is Feed.live;
\*        crates/tickwire-runtime-policy/src/lib.rs:605-670
\* failed Sources member terminal poll bit; crates/tickwire-app/src/market.rs:206-245
\* Single owner and two independent readers. No transport failure is injected.
CONSTANT MaxBatches, LastTime, Age
VARIABLES h, t, base, pos, callback, mark, valid, failed
vars == <<h,t,base,pos,callback,mark,valid,failed>>
Readers == {"sdk","account"}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
End == 2*Len(h)
Batch(p) == (p+1) \div 2
B(l,i,u) == [live |-> l, ing |-> i, pub |-> u]
Fresh(a,n) == a <= n /\ n-a <= Age
EndLive(p) == p > 0 /\ p % 2 = 0 /\ h[Batch(p)].live
\* Stale batches carry the previous exact quote's ingress (crates/tickwire-market-publisher/src/spot.rs:58-74 and crates/tickwire-market-publisher/src/spot.rs:107-122).
Rec(p) ==
  LET b == Batch(p)
      v == h[b]
      quote == IF v.live THEN p % 2 = 1 ELSE p % 2 = 0
      prior == Max({i \in 1..b : h[i].live})
  IN [q |-> quote, live |-> v.live, stamp |-> v.pub,
      ing |-> IF quote THEN h[prior].ing ELSE v.ing]
\* snapshot crates/tickwire-snapshot/src/builder.rs:445-452 and crates/tickwire-snapshot/src/builder.rs:518-530 sorts feed before exact quote and gives local ordinals.
Slot(c,p,isQuote) ==
  LET n == Max({i \in 1..p : Rec(i).q = isQuote})
  IN [seq |-> IF n <= c THEN (IF isQuote THEN 2 ELSE 1) ELSE n, row |-> Rec(n)]
\* MarketView::complete_quote and callback_ready_at, crates/tickwire-market-source/src/replica.rs:127-174.
View(c,p) ==
  LET f == Slot(c,p,FALSE)
      q == Slot(c,p,TRUE)
      matched == f.row.live /\ q.row.live /\ q.row.ing=f.row.ing /\ q.row.stamp=f.row.stamp
      pair == matched /\ f.seq=p /\ q.seq+1=f.seq
      snap == matched /\ p>=2 /\ f.seq=1 /\ q.seq=2
  IN [continuous |-> f.row.live, pair |-> pair, exec |-> pair \/ snap,
      qtime |-> q.row.ing, received |-> f.row.ing, published |-> Rec(p).stamp]
Init ==
  /\ h = <<B(TRUE,1,1)>> /\ t=1
  /\ base=[r \in Readers |-> 0] /\ pos=base
  /\ callback=[seq |-> 0, at |-> 0] /\ mark=0 /\ valid=FALSE /\ failed=FALSE

\* PublishLive and PublishStale are the atomic batches from crates/tickwire-market-publisher/src/spot.rs:129-159 and
\* crates/tickwire-shm/src/mapped.rs:393-436. Writer rejects clock regression, crates/tickwire-market-publisher/src/spot.rs:90-98.
Publish(l) ==
  /\ Len(h)<MaxBatches
  /\ \E received \in h[Len(h)].ing..t : h'=Append(h,B(l,received,t))
  /\ UNCHANGED <<t,base,pos,callback,mark,valid,failed>>
PublishLive == Publish(TRUE)
PublishStale == Publish(FALSE)
\* Clock advances between nonblocking polls; crates/tickwire-md/src/spot_market.rs:160-169.
Tick == /\ t<LastTime /\ t'=t+1 /\ UNCHANGED <<h,base,pos,callback,mark,valid,failed>>
\* Admitted JOIN and complete prefix: crates/tickwire-md/src/server.rs:331-355,
\* crates/tickwire-market-publisher/src/lib.rs:153-203. Only the account announces
\* an initial observation (crates/tickwire-market-source/src/shm.rs:268-278); crates/tickwire-strategy-sdk/src/worker.rs:339-365 initializes the SDK.
AtJoin(r) ==
  /\ base[r]=0 /\ EndLive(End)
  /\ base'=[base EXCEPT ![r]=End] /\ pos'=[pos EXCEPT ![r]=End]
JoinSdk ==
  /\ AtJoin("sdk") /\ UNCHANGED <<h,t,callback,mark,valid,failed>>
JoinAccount ==
  /\ AtJoin("account") /\ mark'=End /\ valid'=TRUE
  /\ UNCHANGED <<h,t,callback,failed>>
\* crates/tickwire-strategy-sdk/src/worker.rs:698-733 and crates/tickwire-strategy-sdk/src/replay/live.rs:299-335 apply each row and decide once.
SdkRead ==
  LET n == pos["sdk"]+1 IN
  /\ base["sdk"]>0 /\ n<=End
  /\ pos'=[pos EXCEPT !["sdk"]=n]
  /\ LET v == View(base["sdk"],n)
     IN callback'=IF v.pair /\ Fresh(v.qtime,t) THEN [seq |-> n,at |-> t] ELSE callback
  /\ UNCHANGED <<h,t,base,mark,valid,failed>>
\* crates/tickwire-market-source/src/shm.rs:293-321 reports each noncontinuous state, even if it is the trailing quote.
\* apply_feed accepts forward observations independently of the sticky trade gate.
\* For an active cursor n is distinct from mark; JOIN supplied its initial, nonzero mark.
\* Identity is fixed by the authenticated JOIN. Sequence and publication time remain checked.
\* crates/tickwire-market-source/src/shm.rs:337-344 maps a rejected observation to error. crates/tickwire-app/src/market.rs:206-245 latches failed.
AccountRead ==
  LET n == pos["account"]+1 IN
  /\ base["account"]>0 /\ ~failed /\ n<=End
  /\ pos'=[pos EXCEPT !["account"]=n]
  /\ LET v == View(base["account"],n)
         announce == ~v.continuous \/ v.exec
         ordered == n>mark /\ v.published>=Rec(mark).stamp
     IN /\ failed'=(announce /\ ~ordered)
        /\ valid'=(valid /\ (~announce \/ (ordered /\ v.continuous)))
        /\ mark'=IF announce /\ ordered THEN n ELSE mark
  /\ UNCHANGED <<h,t,base,callback>>
\* Finite exhaustion and stopped readers may stutter. No scheduling guarantee is claimed.
Next == PublishLive \/ PublishStale \/ Tick \/ JoinSdk \/ JoinAccount \/ SdkRead \/ AccountRead
Spec == Init /\ [][Next]_vars

\* Compare Rust's reconstructed header arithmetic to the publisher's original observation,
\* independently of its snapshot ordinal heuristic.
CompleteOnly ==
  \A r \in Readers : pos[r]>0 =>
    LET p == pos[r] v == View(base[r],p) IN
      /\ v.exec = EndLive(p)
      /\ v.pair = (p>base[r] /\ EndLive(p))
CallbackSound ==
  callback.seq=0 \/ (callback.seq>base["sdk"] /\ EndLive(callback.seq)
    /\ Fresh(h[Batch(callback.seq)].ing,callback.at))
\* Real first-write additionally checks the policy trigger, lease, clock and private cut.
\* Here the SHM market predicate alone is compared to publisher ingress and the observed frontier.
MarketFresh ==
  pos["account"]>0 =>
    LET p == pos["account"] v == View(base["account"],p)
        impl == ~failed /\ v.exec /\ v.published<=t /\ Fresh(v.received,t)
    IN impl = (~failed /\ EndLive(p) /\ Fresh(h[Batch(p)].ing,t))
NoHalfMark == valid => EndLive(mark)
\* A genuine gap permanently closes the modeled generation, including when a
\* later complete pair in that generation makes the raw replica fresh again.
GenerationGate == pos["account"]>0 =>
  (valid = (\A i \in base["account"]..pos["account"] : Rec(i).live))
\* A valid unavailable pair keeps this authenticated SHM member attached.
SubscriptionRetained == ~failed
=============================================================================
