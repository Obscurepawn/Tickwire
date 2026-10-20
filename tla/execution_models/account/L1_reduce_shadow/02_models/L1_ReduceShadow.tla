-------------------------- MODULE L1_ReduceShadow --------------------------
EXTENDS Integers, FiniteSets, TLC
CONSTANTS Orders, ReduceOrders, StartPosition, Wallet, Available, FeeValues
ASSUME /\ Orders = 1..Cardinality(Orders) /\ Orders # {}
       /\ ReduceOrders \subseteq Orders /\ StartPosition \in 0..2
       /\ Wallet >= StartPosition /\ Available >= 0 /\ FeeValues \subseteq 0..4
\* var <-> code: s.orders/admitted/H/credit -> Account.admissions/shadow_holds,
\* Order.shadow_fee_credit; crates/tickwire-account/src/admission.rs:268-334.
\* q/positionOwner/unattributed -> Position.quantity/ownership;
\* crates/tickwire-account/src/events.rs:413-471; crates/tickwire-account/src/external.rs:70-122.
\* filled/paid/fee/economic/confirmed/terminal/cum/closed/aborted/unknown/reduce
\* -> Order fields and facts: crates/tickwire-account/src/lib.rs:30-64;
\* crates/tickwire-account/src/events.rs:19-109; crates/tickwire-account/src/events.rs:351-410.
\* total/cashOwner/D -> cashflow/ownership/uncovered_debits;
\* crates/tickwire-account/src/events.rs:271-349.
\* frozen -> Account.frozen, gate checked in crates/tickwire-account/src/admission.rs:357-382;
\* crates/tickwire-account/src/events.rs:3-17; crates/tickwire-account/src/events.rs:473-493.
\* Every call is atomic under Account's unique &mut owner; no storage model.
VARIABLE s
vars == <<s>>
Max(a,b) == IF a > b THEN a ELSE b
Abs(a) == IF a < 0 THEN -a ELSE a
Half(n) == (n+1) \div 2
RECURSIVE Sum(_)
Sum(f) == IF DOMAIN f = {} THEN 0 ELSE LET i == CHOOSE x \in DOMAIN f:TRUE IN f[i]+Sum([j \in DOMAIN f\{i}|->f[j]])
Delta(o) == IF o \in ReduceOrders THEN -1 ELSE 1
Remaining(t,o) == IF o \notin t.orders \/ t.aborted[o] \/ (t.terminal[o] /\ t.cum[o]=t.filled[o]) THEN 0 ELSE 1-t.filled[o]
Past(t,o) == IF o \notin t.orders \/ t.aborted[o] \/ t.closed[o] THEN 0 ELSE Max(0,Half(t.filled[o])+2-t.paid[o])
Fees(t,o) == IF o \notin t.orders \/ t.aborted[o] \/ t.closed[o] THEN 0 ELSE Max(0,Half(t.filled[o])+Half(Remaining(t,o))+2-t.paid[o])
\* Concrete specialization of crates/tickwire-account/src/calculation.rs:275-400 and risk futures capital.
Capital(t) == Abs(t.q)+Sum([o\in Orders|->Remaining(t,o)+Half(Remaining(t,o))+Past(t,o)])
CashReserve(t) == Sum([o\in Orders|->Fees(t,o)])
Shadow(t) == Max(0,Available-Sum(t.hold)-t.debit)
Allocated(t) == Sum(t.reduce)
Freeze(t) == [t EXCEPT !.frozen = @ \/ Capital(t)>t.total \/ CashReserve(t)>t.total \/ Allocated(t)>Abs(t.q)]
Init == s=[orders|->{},q|->StartPosition,total|->Wallet,
 positionOwner|->[o\in Orders|->0],cashOwner|->[o\in Orders|->0],unattributed|->0,external|->FALSE,
 filled|->[o\in Orders|->0],fee|->[o\in Orders|->-1],paid|->[o\in Orders|->0],
 reduce|->[o\in Orders|->0],hold|->[o\in Orders|->0],credit|->[o\in Orders|->0],debit|->0,
 economic|->[o\in Orders|->FALSE],confirmed|->[o\in Orders|->FALSE],terminal|->[o\in Orders|->FALSE],
 cum|->[o\in Orders|->-1],closed|->[o\in Orders|->FALSE],aborted|->[o\in Orders|->FALSE],
 unknown|->[o\in Orders|->FALSE],frozen|->FALSE]
NewOrder(t,o) == [t EXCEPT !.orders=@\cup{o},!.reduce[o]=IF o\in ReduceOrders THEN 1 ELSE 0]
CanAdmit(o) == /\ ~s.frozen /\ o\notin s.orders
 /\ (o\notin ReduceOrders \/ (s.q>0 /\ Allocated(s)+1<=s.q))
 /\ Capital(NewOrder(s,o))<=s.total /\ CashReserve(NewOrder(s,o))<=s.total /\ Shadow(s)>=4
\* crates/tickwire-account/src/admission.rs:139-192; crates/tickwire-account/src/admission.rs:268-334.
Admit(o) == /\ CanAdmit(o) /\ s'=[NewOrder(s,o) EXCEPT !.hold[o]=4,!.credit[o]=3]
RejectAdmission(o) == /\ o\notin s.orders /\ ~CanAdmit(o) /\ UNCHANGED s
RetryAdmission(o) == /\ o\in s.orders /\ UNCHANGED s
ConflictAdmission(o) == /\ o\in s.orders /\ s'=[s EXCEPT !.frozen=TRUE]
\* crates/tickwire-account/src/events.rs:161-269; exact PnL=0 is provided.
Fill(o) == /\ o\in s.orders /\ s.filled[o]=0
 /\ LET t==[s EXCEPT !.filled[o]=1,!.q=@+Delta(o),!.positionOwner[o]=Delta(o),!.reduce[o]=0,
      !.economic[o]=TRUE,!.confirmed[o]=TRUE,!.aborted[o]=FALSE,!.closed[o]=FALSE,
      !.frozen=@\/s.aborted[o]\/s.closed[o]\/(s.cum[o]=0)] IN s'=Freeze(t)
ReplayFill(o) == /\ o\in s.orders /\ s.filled[o]=1 /\ UNCHANGED s
ConflictFill(o) == /\ o\in s.orders /\ s.filled[o]=1 /\ s'=[s EXCEPT !.frozen=TRUE]
\* crates/tickwire-account/src/events.rs:288-342. Zero fee is still evidence.
Fee(o,n) == /\ o\in s.orders /\ s.fee[o]=-1 /\ n\in FeeValues
 /\ LET t==[s EXCEPT !.fee[o]=n,!.paid[o]=n,!.total=@-n,!.cashOwner[o]=-n,
      !.credit[o]=Max(0,@-n),!.debit=@+Max(0,n-s.credit[o]),!.economic[o]=TRUE,
      !.aborted[o]=FALSE,!.closed[o]=FALSE,
      !.frozen=@\/s.aborted[o]\/s.closed[o]\/n>Half(s.filled[o])+2] IN s'=Freeze(t)
ReplayFee(o) == /\ o\in s.orders /\ s.fee[o]#-1 /\ UNCHANGED s
ConflictFee(o) == /\ o\in s.orders /\ s.fee[o]#-1 /\ s'=[s EXCEPT !.frozen=TRUE]
\* crates/tickwire-account/src/events.rs:351-410.
Acknowledge(o) == /\ o\in s.orders
 /\ s'=[s EXCEPT !.confirmed[o]=TRUE,!.aborted[o]=FALSE,
        !.closed[o]=IF s.aborted[o] THEN FALSE ELSE @,!.frozen=@\/s.aborted[o]]
Unknown(o) == /\ o\in s.orders /\ s'=[s EXCEPT !.unknown[o]=TRUE,!.frozen=TRUE]
Terminal(o,n) == /\ o\in s.orders /\ n\in 0..1
 /\ s'=[s EXCEPT !.terminal[o]=TRUE,!.confirmed[o]=TRUE,!.cum[o]=Max(@,n),
     !.reduce[o]=IF n=s.filled[o] THEN 0 ELSE @,!.aborted[o]=FALSE,
     !.closed[o]=IF s.aborted[o] THEN FALSE ELSE @,
     !.frozen=@\/s.aborted[o]\/n<s.filled[o]\/(s.cum[o]#-1/\s.cum[o]#n)]
ReplayFillUnknown(o) == /\ s.filled[o]=1 /\ Unknown(o)
ReplayFillTerminal(o,n) == /\ s.filled[o]=1 /\ Terminal(o,n)
\* crates/tickwire-account/src/admission.rs:434-448.
Reconcile(o) == /\ o\in s.orders /\ s.terminal[o] /\ s.cum[o]=s.filled[o]
 /\ s'=[s EXCEPT !.closed[o]=TRUE]
RejectReconcile(o) == /\ o\in s.orders /\ (~s.terminal[o]\/s.cum[o]#s.filled[o])
 /\ s'=[s EXCEPT !.frozen=TRUE]
\* crates/tickwire-account/src/admission.rs:408-431; trusted durable abort input.
Abort(o) == /\ o\in s.orders /\ ~s.aborted[o] /\ ~s.economic[o]
 /\ ~s.confirmed[o] /\ ~s.unknown[o] /\ ~s.terminal[o]
 /\ s'=[s EXCEPT !.aborted[o]=TRUE,!.closed[o]=TRUE,!.reduce[o]=0,!.hold[o]=0]
ReplayAbort(o) == /\ o\in s.orders /\ s.aborted[o] /\ UNCHANGED s
RejectAbort(o) == /\ o\in s.orders /\ ~s.aborted[o]
 /\ (s.economic[o]\/s.confirmed[o]\/s.unknown[o]\/s.terminal[o]) /\ s'=[s EXCEPT !.frozen=TRUE]
\* crates/tickwire-account/src/external.rs:70-122. Exactly one external update ID.
ExternalPosition(n) == /\ ~s.external /\ n\in 0..2
 /\ s'=[s EXCEPT !.external=TRUE,!.q=n,!.unattributed=@+n-s.q,!.frozen=TRUE]
RejectFact == /\ s.orders#{} /\ s'=[s EXCEPT !.frozen=TRUE]
Next == \/ (\E o\in Orders: Admit(o)\/RejectAdmission(o)\/RetryAdmission(o)\/ConflictAdmission(o)
            \/Fill(o)\/ReplayFill(o)\/ConflictFill(o)\/ReplayFee(o)\/ConflictFee(o)\/Acknowledge(o)\/Unknown(o)
            \/ReplayFillUnknown(o)\/Reconcile(o)\/RejectReconcile(o)\/Abort(o)\/ReplayAbort(o)\/RejectAbort(o))
        \/ (\E o\in Orders,n\in FeeValues:Fee(o,n))
        \/ (\E o\in Orders,n\in 0..1:Terminal(o,n)\/ReplayFillTerminal(o,n))
        \/ (\E n\in 0..2:ExternalPosition(n)) \/ RejectFact
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.orders\subseteq Orders /\ s.q\in (-Cardinality(Orders))..(2+Cardinality(Orders))
 /\ s.filled\in[Orders->0..1] /\ s.fee\in[Orders->{-1}\cup FeeValues]
 /\ s.reduce\in[Orders->0..1] /\ s.hold\in[Orders->{0,4}] /\ s.credit\in[Orders->0..3]
 /\ s.paid\in[Orders->0..4] /\ s.cum\in[Orders->-1..1] /\ s.debit\in 0..(Cardinality(Orders)*4)
 /\ s.frozen\in BOOLEAN /\ s.external\in BOOLEAN
 /\ \A o\in Orders:s.economic[o]\in BOOLEAN /\ s.confirmed[o]\in BOOLEAN /\ s.terminal[o]\in BOOLEAN
    /\s.closed[o]\in BOOLEAN /\s.aborted[o]\in BOOLEAN /\s.unknown[o]\in BOOLEAN
NoOpenDeficit == ~s.frozen => Capital(s)<=s.total /\ CashReserve(s)<=s.total
NoOpenOverallocation == ~s.frozen => Allocated(s)<=Abs(s.q)
AttributionConserved == /\ s.total=Wallet+Sum(s.cashOwner)
 /\ s.q=StartPosition+Sum(s.positionOwner)+s.unattributed
NoDuplicateEffects == /\ s.total=Wallet-Sum(s.paid)
 /\ \A o\in Orders:s.positionOwner[o]=s.filled[o]*Delta(o)
NoUnprovenShadowRelease == ~s.frozen => \A o\in s.orders:~s.aborted[o]=>s.hold[o]=4
NoAbortAfterEvidence == \A o\in s.orders:s.aborted[o]=>~s.economic[o]/\~s.confirmed[o]
ZeroFeeIsEvidence == \A o\in Orders:s.fee[o]=0=>s.economic[o]/\~s.aborted[o]
MissingDetailsRetained == \A o\in s.orders:s.terminal[o]/\s.cum[o]>s.filled[o]/\~s.aborted[o]
 => Remaining(s,o)=1-s.filled[o]
UnknownFreezes == \A o\in Orders:s.unknown[o]=>s.frozen
TerminalContradictionFreezes == \A o\in Orders:s.cum[o]#-1/\s.filled[o]>s.cum[o]=>s.frozen
ExternalFreezes == s.external=>s.frozen
=============================================================================
