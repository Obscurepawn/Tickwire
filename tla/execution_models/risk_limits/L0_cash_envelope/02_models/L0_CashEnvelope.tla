--------------------------- MODULE L0_CashEnvelope ---------------------------
EXTENDS Integers
\* Code revision 9b2ed63. One serial spot Account, price=1, fees=0, exact same-scale atoms.
\* rest   remaining signed quantities; crates/tickwire-account/src/lib.rs:99-112,
\*        calculation.rs:138-167,438-457. Positive is buy, negative is sell (model encoding).
\* delta  per-strategy base cashflow; account/src/events.rs:214-257,296-305; quote flow=-base.
\* used   occupied logical order slots; admission.rs:121-151,233-256,297-300.
\* result, before = observable disposition, pre-refusal economic state (historical ghost);
\*        account/src/admission.rs:4-31; account-gateway/src/lib.rs:421-430.
\* frozen = post-fact account gate; account/src/events.rs:120-129. MaxQty and limits finite.
CONSTANT Base, Quote, BaseLock, QuoteLock, SecondShort, MaxQty, AbsCap, GrossCap, MemberCap
VARIABLES rest, delta, used, result, before, frozen
vars == <<rest,delta,used,result,before,frozen>>
M == {1,2}
Zero == [m \in M |-> 0]
Abs(x) == IF x<0 THEN -x ELSE x
Pos(x) == IF x>0 THEN x ELSE 0
Lo(a,b) == IF a<b THEN a ELSE b
Hi(a,b) == IF a>b THEN a ELSE b
Sum(f) == f[1]+f[2]
Start(m) == IF m=1 THEN Base ELSE -SecondShort
Residual == SecondShort
Own(d,m) == Start(m)+d[m]
BaseNow(d) == Base+Sum(d)
QuoteNow(d) == Quote-Sum(d)
Buy(r) == Pos(r[1])+Pos(r[2])
Sell(r) == Pos(-r[1])+Pos(-r[2])

\* Account::asset passes synthetic coverage=0, actual_debit=0, remaining_reserve=resource sum.
\* crates/tickwire-account/src/calculation.rs:229-265,410-436; account_inputs.rs:474-497;
\* crates/tickwire-risk/src/spot.rs:134-181 and fixed.rs:134-165 checked at one scale.
Spare(total,locked,credit,debit,hold) ==
  LET localFree == (total+credit-debit)-locked-hold
      venueCap == (total-locked)+credit-debit-Pos(hold)
      effective == total+credit-debit-locked-hold
  IN Lo(Lo(localFree,venueCap),effective)
Cash(r,d) ==
  LET cr == Pos(d[1])+Pos(d[2]) dr == Pos(-d[1])+Pos(-d[2])
  IN /\ Spare(Base,BaseLock,cr,dr,Sell(r))>=0
     /\ Spare(Quote,QuoteLock,dr,cr,Buy(r))>=0
\* crates/tickwire-account/src/calculation.rs:438-457,478-515. Bucket zero has no orders.
Limits(r,d) ==
  LET actual == BaseNow(d)
      strategy == [i \in M |-> Abs(Own(d,i))+Abs(r[i])]
      virtualGross == Abs(Residual)+Sum(strategy)
      maximum == Hi(actual+Buy(r),Abs(actual-Sell(r)))
      gross == Hi(Abs(actual)+Buy(r)+Sell(r),virtualGross)
  IN maximum<=AbsCap /\ gross<=GrossCap /\ \A m \in M : strategy[m]<=MemberCap
Allowed(r,d) == Cash(r,d) /\ Limits(r,d)
Econ == <<rest,delta,used>>
Init == /\ rest=Zero /\ delta=Zero /\ used={} /\ frozen=FALSE
        /\ result="start" /\ before = <<Zero,Zero,{}>>
\* Git source: crates/tickwire-account/src/admission.rs:4-31,143-159,233-256,297-300.
Admit ==
  \E m \in M \ used, q \in ((-MaxQty)..MaxQty) \ {0} :
    LET r == [rest EXCEPT ![m]=q] IN
    /\ ~frozen /\ Allowed(r,delta) /\ rest'=r /\ used'=used \cup {m} /\ result'="ok"
    /\ UNCHANGED <<delta,before,frozen>>
Refuse ==
  \E m \in M \ used, q \in ((-MaxQty)..MaxQty) \ {0} :
    /\ ~Allowed([rest EXCEPT ![m]=q],delta)
    /\ before'=Econ /\ result'="refused" /\ UNCHANGED <<rest,delta,used,frozen>>
\* crates/tickwire-account/src/events.rs:180-235,244-269,296-305,125-129.
Fill ==
  \E m \in M : \E lots \in 1..Abs(rest[m]) :
    LET n == IF rest[m]>0 THEN lots ELSE -lots
        d == [delta EXCEPT ![m]=@+n] r == [rest EXCEPT ![m]=@-n]
    IN /\ rest'=r /\ delta'=d /\ frozen'=(frozen \/ ~Allowed(r,d))
       /\ result'="fill" /\ UNCHANGED <<used,before>>
\* Terminal cancellation with exact cumulative records. account/src/lib.rs:99-112;
\* crates/tickwire-account/src/events.rs:119-129,363-459. No blind release on a cancel ACK.
Cancel ==
  \E m \in M :
   /\ rest[m]#0 /\ rest'=[rest EXCEPT ![m]=0] /\ result'="terminal"
   /\ UNCHANGED <<delta,used,before,frozen>>
Next == Admit \/ Refuse \/ Fill \/ Cancel
Spec == Init /\ [][Next]_vars

\* Independent semantic witnesses: enumerate every combination of future partial fills.
Moves(n) == IF n>=0 THEN 0..n ELSE n..0
AllFillsFit ==
  \A x \in Moves(rest[1]), y \in Moves(rest[2]) :
   LET b == BaseNow(delta)+x+y
       q == QuoteNow(delta)-x-y
       a1 == Own(delta,1)+x a2 == Own(delta,2)+y
   IN /\ b>=BaseLock /\ q>=QuoteLock /\ Abs(b)<=AbsCap
      /\ Abs(a1)<=MemberCap /\ Abs(a2)<=MemberCap
      /\ Abs(a1)+Abs(a2)+Abs(Residual)<=GrossCap
OwnershipConserved == Own(delta,1)+Own(delta,2)+Residual=BaseNow(delta)
RefusalAtomic == result="refused" => Econ=before
ValidPartialKeepsGate == ~frozen
=============================================================================
