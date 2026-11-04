----------------------------- MODULE L0_SharedWindow -----------------------------
EXTENDS Naturals, FiniteSets, Sequences
CONSTANT WindowCount, RivalUnits
(*
One IP/request-weight bucket, two registered consumers, three unique grant requests,
fixed signed rule/cost per request and one trusted monotonic domain. Source: 9b2ed63.
now: discrete clock equivalence class between starts and ends; windows: committed
  immutable nonoverlapping intervals, crates/tickwire-quota/src/lib.rs:316-329, crates/tickwire-quota/src/lib.rs:345-389.
rev, healthy: durable snapshot revision and volatile coordinator poison after CAS
  error, crates/tickwire-quota/src/lib.rs:159-176, crates/tickwire-quota/src/lib.rs:331-343.
ticket: source window, request and base revision captured by prepare_issue;
  crates/tickwire-quota/src/lib.rs:490-563.
loc: issued window per request, retained: charged liability in coordinator store,
  crates/tickwire-quota/src/lib.rs:276-289, crates/tickwire-quota/src/lib.rs:519-557, crates/tickwire-quota/src/lib.rs:577-585, crates/tickwire-quota/src/lib.rs:599-626.
delivery: returned grant installed locally or unavailable after a lost reply;
  crates/tickwire-quota/src/lib.rs:163-176, crates/tickwire-quota/src/lib.rs:577-585, crates/tickwire-quota/src/lib.rs:657-678.
left, seen: local units and immutable attempts, crates/tickwire-quota/src/lib.rs:695-774.
mode: live owner, consumed owner with pending stop receipt, or reclaimed receipt;
  crates/tickwire-quota/src/lib.rs:599-626, crates/tickwire-quota/src/lib.rs:776-794.
Accounts and classes are fixed by a configured Binance action rule, and exact
request ID/content retries use one local AttemptId. See
  crates/tickwire-binance-quota/src/lib.rs:331-379.
*)
VARIABLES now, windows, rev, healthy, ticket, loc, retained, delivery, left, seen, mode
vars == <<now,windows,rev,healthy,ticket,loc,retained,delivery,left,seen,mode>>
G == 1..3
Owner(g) == IF g=1 THEN 1 ELSE 2
Normal(g) == g # 3
Units(g) == IF g=1 THEN 2 ELSE IF g=2 THEN RivalUnits ELSE 1
Budget == 3
Reserve == 1
W == 1..WindowCount
NoTicket == <<0,0,0>>
Sum3(f) == f[1]+f[2]+f[3]
Sum(w,n) == Sum3([g \in G |-> IF loc[g]=w /\ (~n \/ Normal(g)) THEN retained[g] ELSE 0])
CanPrepare(g,w) == /\ healthy /\ mode[Owner(g)]="live" /\ loc[g]=0
                   /\ w \in windows /\ now<=w
                   /\ Sum(w,FALSE)+Units(g)<=Budget
                   /\ (~Normal(g) \/ Sum(w,TRUE)+Units(g)<=Budget-Reserve)
Init == /\ now=1 /\ windows={1} /\ rev=0 /\ healthy=TRUE /\ ticket=NoTicket
        /\ loc=[g \in G |-> 0] /\ retained=loc /\ left=loc
        /\ seen=[g \in G |-> {}] /\ delivery=[g \in G |-> "absent"]
        /\ mode=[c \in 1..2 |-> "live"]

\* Trusted samples can reach the next adjacent window or advance past the last.
\* crates/tickwire-quota/src/lib.rs:316-328, crates/tickwire-quota/src/lib.rs:639-655, crates/tickwire-quota/src/lib.rs:690-694.
Tick == /\ now<=WindowCount /\ now'=now+1
        /\ UNCHANGED <<windows,rev,healthy,ticket,loc,retained,delivery,left,seen,mode>>
\* Existing windows remain accounted; add the unique successor before its end.
\* crates/tickwire-quota/src/lib.rs:355-388.
AddWindow == /\ healthy /\ WindowCount=2 /\ 2 \notin windows /\ now<=2
             /\ windows'=windows \cup {2} /\ rev'=rev+1
             /\ UNCHANGED <<now,healthy,ticket,loc,retained,delivery,left,seen,mode>>

\* Holding one uncommitted ticket still permits a different issue() on the owner.
\* crates/tickwire-quota/src/lib.rs:490-541, crates/tickwire-quota/src/lib.rs:547-562, crates/tickwire-quota/src/lib.rs:587-598.
Prepare == /\ ticket=NoTicket /\ \E g \in G,w \in W: CanPrepare(g,w) /\ ticket' = <<g,w,rev>>
           /\ UNCHANGED <<now,windows,rev,healthy,loc,retained,delivery,left,seen,mode>>
Write(g,w,reply) == /\ rev'=rev+1 /\ loc'=[loc EXCEPT ![g]=w]
                    /\ retained'=[retained EXCEPT ![g]=Units(g)]
                    /\ delivery'=[delivery EXCEPT ![g]=IF reply THEN "returned" ELSE "lost"]
                    /\ healthy'=reply
IssueOther == /\ ticket#NoTicket /\ \E g \in G,w \in W: CanPrepare(g,w)
                /\ Write(g,w,TRUE)
              /\ UNCHANGED <<now,windows,ticket,left,seen,mode>>
\* Compare revision before store CAS. After-commit loss retains the entire allocation.
\* crates/tickwire-quota/src/lib.rs:163-175, crates/tickwire-quota/src/lib.rs:331-343, crates/tickwire-quota/src/lib.rs:565-585.
Commit == /\ healthy /\ ticket#NoTicket /\ ticket[3]=rev
          /\ \E ack \in BOOLEAN: Write(ticket[1],ticket[2],ack)
          /\ ticket'=NoTicket /\ UNCHANGED <<now,windows,left,seen,mode>>
RejectStale == /\ ticket#NoTicket /\ ticket[3]#rev /\ ticket'=NoTicket
               /\ UNCHANGED <<now,windows,rev,healthy,loc,retained,delivery,left,seen,mode>>
\* A commit failure before publishing the snapshot poisons the coordinator too.
\* crates/tickwire-quota/src/lib.rs:163-171, crates/tickwire-quota/src/lib.rs:331-343.
FailBeforeCommit == /\ healthy /\ ticket#NoTicket /\ ticket[3]=rev
                    /\ healthy'=FALSE /\ ticket'=NoTicket
                    /\ UNCHANGED <<now,windows,rev,loc,retained,delivery,left,seen,mode>>

\* Grant expiry equals end of interval, lease extends beyond selected windows.
\* crates/tickwire-quota/src/lib.rs:639-678.
Install == /\ \E g \in G: /\ mode[Owner(g)]="live" /\ delivery[g]="returned" /\ now<=loc[g]
                              /\ delivery'=[delivery EXCEPT ![g]="installed"]
                              /\ left'=[left EXCEPT ![g]=Units(g)]
           /\ UNCHANGED <<now,windows,rev,healthy,ticket,loc,retained,seen,mode>>

\* One unique grant per owner/class in this bound, with unit-cost request attempts.
\* crates/tickwire-quota/src/lib.rs:690-694, crates/tickwire-quota/src/lib.rs:724-774; crates/tickwire-binance-quota/src/lib.rs:343-379.
Spend == /\ \E g \in G, a \in 1..2:
               /\ delivery[g]="installed" /\ mode[Owner(g)]="live" /\ now=loc[g]
               /\ left[g]>0 /\ a \notin seen[g]
               /\ left'=[left EXCEPT ![g]=@-1] /\ seen'=[seen EXCEPT ![g]=@ \cup {a}]
         /\ UNCHANGED <<now,windows,rev,healthy,ticket,loc,retained,delivery,mode>>
\* Exact same digest/class/cost uses the original debits and consumes zero new units.
\* crates/tickwire-quota/src/lib.rs:708-722.
Retry == /\ \E g \in G: seen[g]#{} /\ delivery[g]="installed" /\ mode[Owner(g)]="live"
                           /\ now=loc[g]
         /\ UNCHANGED vars

\* stop(self) destroys the charging capability and returns each installed grant's usage.
\* crates/tickwire-quota/src/lib.rs:776-794.
Stop == /\ \E c \in 1..2: mode[c]="live" /\ mode'=[mode EXCEPT ![c]="stopped"]
        /\ UNCHANGED <<now,windows,rev,healthy,ticket,loc,retained,delivery,left,seen>>
\* A receipt only covers installed grants; lost/undelivered grants retain full units.
\* crates/tickwire-quota/src/lib.rs:599-626.
Reclaim == /\ healthy /\ \E c \in 1..2:
              /\ mode[c]="stopped" /\ mode'=[mode EXCEPT ![c]="reclaimed"]
              /\ retained'=[g \in G |-> IF Owner(g)=c /\ delivery[g]="installed"
                                      THEN Units(g)-left[g] ELSE retained[g]]
           /\ rev'=rev+1 /\ UNCHANGED <<now,windows,healthy,ticket,loc,delivery,left,seen>>

Next == Tick \/ AddWindow \/ Prepare \/ IssueOther \/ Commit \/ RejectStale \/ FailBeforeCommit
        \/ Install \/ Spend \/ Retry \/ Stop \/ Reclaim
Spec == Init /\ [][Next]_vars
BudgetSafe == \A w \in windows: Sum(w,FALSE)<=Budget /\ Sum(w,TRUE)<=Budget-Reserve
ChargesRetained == \A g \in G: Cardinality(seen[g])<=retained[g] /\
                   (delivery[g]="installed" => left[g]+Cardinality(seen[g])=Units(g))
NoLostRefund == \A g \in G: delivery[g]="lost" => retained[g]=Units(g)
===============================================================================
