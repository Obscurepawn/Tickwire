---------------------------- MODULE L0_Calibration ----------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
(* var/code map: one mutable Calibrator; each public call is atomic.
 c.next/pending/anchor/last/fault: crates/tickwire-clock/src/lib.rs:144-212
 tokens and Begin/Finish/Cancel: crates/tickwire-clock/src/lib.rs:214-293
 Reading/Read/Sign: crates/tickwire-clock/src/lib.rs:89-142
   crates/tickwire-clock/src/lib.rs:295-326
 exact retained SignedTime and CheckSend: crates/tickwire-clock/src/lib.rs:328-372
 integer projection: crates/tickwire-clock/src/lib.rs:395-409
 world.now/truth/epoch, capture and anchorTrusted are independent environment
 witnesses for docs/implementation/clock-interface.md continuous-rate contract.
 Calls/retained tokens/stamps are finite exploration bounds, not runtime limits.
 Injected stale input, wrong domain, backward time, malformed and jumped samples
 exercise real rejection branches; only continuous trustworthy epochs assert truth.
 No network/TLS/provider implementation or reset of this faulted instance. *)
(* Exact base-1000 representation used only to work around TLC IntValue's int32.
 Source arithmetic: crates/tickwire-clock/src/lib.rs:395-409. No time-unit scaling.
 10 limbs hold [0,10^30); selected inputs never overflow this representation. *)
Base == 1000
Digits == 10
Word == [1..Digits -> 0..(Base-1)]
Zero == [i \in 1..Digits |-> 0]
Small(n) == [i \in 1..Digits |-> CASE i=1 -> n % Base
 [] i=2 -> (n \div Base) % Base [] i=3 -> (n \div 1000000) % Base
 [] i=4 -> n \div 1000000000 [] OTHER -> 0]
One == Small(1)
Ms == Small(1000000)
Us == Small(1000)
Second == Small(1000000000)
Max64 == <<615,551,709,73,744,446,18,0,0,0>>
Lt(a,b) == \E i \in 1..Digits: a[i]<b[i] /\ \A j \in (i+1)..Digits: a[j]=b[j]
Le(a,b) == a=b \/ Lt(a,b)
MinW(a,b) == IF Le(a,b) THEN a ELSE b
MaxW(a,b) == IF Le(a,b) THEN b ELSE a
RECURSIVE Carry(_,_,_), Borrow(_,_,_)
Carry(a,b,i) == IF i=0 THEN 0 ELSE (a[i]+b[i]+Carry(a,b,i-1)) \div Base
Add(a,b) == [i \in 1..Digits |-> (a[i]+b[i]+Carry(a,b,i-1)) % Base]
Borrow(a,b,i) == IF i=0 THEN 0 ELSE IF a[i]-Borrow(a,b,i-1)<b[i] THEN 1 ELSE 0
Sub(a,b) == [i \in 1..Digits |-> LET d==a[i]-b[i]-Borrow(a,b,i-1)
                                 IN IF d<0 THEN d+Base ELSE d]
SatSub(a,b) == IF Lt(a,b) THEN Zero ELSE Sub(a,b)
Shift(a,k) == [i \in 1..Digits |-> IF i>k THEN a[i-k] ELSE 0]
Drop(a,k) == [i \in 1..Digits |-> IF i+k<=Digits THEN a[i+k] ELSE 0]
CeilDrop(a,k) == Add(Drop(a,k),IF \E i \in 1..k:a[i]#0 THEN One ELSE Zero)
Half(a) == [i \in 1..Digits |-> (a[i]+(IF i<Digits THEN Base*(a[i+1]%2) ELSE 0)) \div 2]
Mid(a,b) == Add(a,Half(Sub(b,a)))
Quantized(a,k) == Shift(Drop(a,k),k)
(* For the selected true drift fractions 0,1/1e9,1e6/1e9. *)
FractionCeil(a,d) == IF d=0 THEN Zero ELSE CeilDrop(a,IF d=1 THEN 3 ELSE 1)
Up(a,d) == Add(a,FractionCeil(a,d))
Down(a,d) == Sub(a,FractionCeil(a,d))
Scaled(a) == Shift(a,3)
DriftPart(a,d) == IF d=0 THEN Zero ELSE Shift(a,IF d=1 THEN 0 ELSE 2)
AdvanceTruth(a,d,rate) == CASE rate=-1 -> Sub(Scaled(a),DriftPart(a,d))
 [] rate=0 -> Scaled(a) [] rate=1 -> Add(Scaled(a),DriftPart(a,d))

ArithmeticOK ==
 /\ Add(<<615,551,709,73,744,446,18,0,0,0>>,One) = <<616,551,709,73,744,446,18,0,0,0>>
 /\ Sub(<<616,551,709,73,744,446,18,0,0,0>>,One) = <<615,551,709,73,744,446,18,0,0,0>>
 /\ Sub(Second,One) = <<999,999,999,0,0,0,0,0,0,0>>
 /\ Add(<<999,999,999,999,999,999,999,999,999,0>>,One) = <<0,0,0,0,0,0,0,0,0,1>>
 /\ Half(<<615,551,709,73,744,446,18,0,0,0>>) = <<807,775,854,36,372,223,9,0,0,0>>
 /\ Mid(<<598,551,709,73,744,446,18,0,0,0>>,<<615,551,709,73,744,446,18,0,0,0>>) = <<606,551,709,73,744,446,18,0,0,0>>
 /\ Up(<<0,0,0,0,0,0,0,0,0,0>>,0) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,0,0,0,0,0,0,0>>,0) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<0,0,0,0,0,0,0,0,0,0>>,1) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,0,0,0,0,0,0,0>>,1) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<0,0,0,0,0,0,0,0,0,0>>,1000000) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,0,0,0,0,0,0,0>>,1000000) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,0,0,0,0,0,0,0>>,0) = <<1,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,0,0,0,0,0,0,0>>,0) = <<1,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,0,0,0,0,0,0,0>>,1) = <<2,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,0,0,0,0,0,0,0>>,1) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,0,0,0,0,0,0,0>>,1000000) = <<2,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,0,0,0,0,0,0,0>>,1000000) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<999,0,0,0,0,0,0,0,0,0>>,0) = <<999,0,0,0,0,0,0,0,0,0>>
 /\ Down(<<999,0,0,0,0,0,0,0,0,0>>,0) = <<999,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<999,0,0,0,0,0,0,0,0,0>>,1) = <<0,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<999,0,0,0,0,0,0,0,0,0>>,1) = <<998,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<999,0,0,0,0,0,0,0,0,0>>,1000000) = <<0,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<999,0,0,0,0,0,0,0,0,0>>,1000000) = <<998,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<0,1,0,0,0,0,0,0,0,0>>,0) = <<0,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,1,0,0,0,0,0,0,0,0>>,0) = <<0,1,0,0,0,0,0,0,0,0>>
 /\ Up(<<0,1,0,0,0,0,0,0,0,0>>,1) = <<1,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,1,0,0,0,0,0,0,0,0>>,1) = <<999,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<0,1,0,0,0,0,0,0,0,0>>,1000000) = <<1,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<0,1,0,0,0,0,0,0,0,0>>,1000000) = <<999,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,1,0,0,0,0,0,0,0,0>>,0) = <<1,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,1,0,0,0,0,0,0,0,0>>,0) = <<1,1,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,1,0,0,0,0,0,0,0,0>>,1) = <<2,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,1,0,0,0,0,0,0,0,0>>,1) = <<0,1,0,0,0,0,0,0,0,0>>
 /\ Up(<<1,1,0,0,0,0,0,0,0,0>>,1000000) = <<3,1,0,0,0,0,0,0,0,0>>
 /\ Down(<<1,1,0,0,0,0,0,0,0,0>>,1000000) = <<999,0,0,0,0,0,0,0,0,0>>
 /\ Up(<<999,999,999,0,0,0,0,0,0,0>>,0) = <<999,999,999,0,0,0,0,0,0,0>>
 /\ Down(<<999,999,999,0,0,0,0,0,0,0>>,0) = <<999,999,999,0,0,0,0,0,0,0>>
 /\ Up(<<999,999,999,0,0,0,0,0,0,0>>,1) = <<0,0,0,1,0,0,0,0,0,0>>
 /\ Down(<<999,999,999,0,0,0,0,0,0,0>>,1) = <<998,999,999,0,0,0,0,0,0,0>>
 /\ Up(<<999,999,999,0,0,0,0,0,0,0>>,1000000) = <<999,999,0,1,0,0,0,0,0,0>>
 /\ Down(<<999,999,999,0,0,0,0,0,0,0>>,1000000) = <<999,999,998,0,0,0,0,0,0,0>>
 /\ Up(<<0,0,0,1,0,0,0,0,0,0>>,0) = <<0,0,0,1,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,1,0,0,0,0,0,0>>,0) = <<0,0,0,1,0,0,0,0,0,0>>
 /\ Up(<<0,0,0,1,0,0,0,0,0,0>>,1) = <<1,0,0,1,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,1,0,0,0,0,0,0>>,1) = <<999,999,999,0,0,0,0,0,0,0>>
 /\ Up(<<0,0,0,1,0,0,0,0,0,0>>,1000000) = <<0,0,1,1,0,0,0,0,0,0>>
 /\ Down(<<0,0,0,1,0,0,0,0,0,0>>,1000000) = <<0,0,999,0,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,1,0,0,0,0,0,0>>,0) = <<1,0,0,1,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,1,0,0,0,0,0,0>>,0) = <<1,0,0,1,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,1,0,0,0,0,0,0>>,1) = <<3,0,0,1,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,1,0,0,0,0,0,0>>,1) = <<999,999,999,0,0,0,0,0,0,0>>
 /\ Up(<<1,0,0,1,0,0,0,0,0,0>>,1000000) = <<2,0,1,1,0,0,0,0,0,0>>
 /\ Down(<<1,0,0,1,0,0,0,0,0,0>>,1000000) = <<0,0,999,0,0,0,0,0,0,0>>
 /\ Up(<<615,551,709,73,744,446,18,0,0,0>>,0) = <<615,551,709,73,744,446,18,0,0,0>>
 /\ Down(<<615,551,709,73,744,446,18,0,0,0>>,0) = <<615,551,709,73,744,446,18,0,0,0>>
 /\ Up(<<615,551,709,73,744,446,18,0,0,0>>,1) = <<689,295,156,92,744,446,18,0,0,0>>
 /\ Down(<<615,551,709,73,744,446,18,0,0,0>>,1) = <<541,807,262,55,744,446,18,0,0,0>>
 /\ Up(<<615,551,709,73,744,446,18,0,0,0>>,1000000) = <<167,261,783,817,190,465,18,0,0,0>>
 /\ Down(<<615,551,709,73,744,446,18,0,0,0>>,1000000) = <<63,842,635,329,297,428,18,0,0,0>>
 /\ Quantized(<<999,999,0,0,0,0,0,0,0,0>>,1) = <<0,999,0,0,0,0,0,0,0,0>>
 /\ Quantized(<<999,999,0,0,0,0,0,0,0,0>>,2) = <<0,0,0,0,0,0,0,0,0,0>>
 /\ Quantized(<<0,0,1,0,0,0,0,0,0,0>>,1) = <<0,0,1,0,0,0,0,0,0,0>>
 /\ Quantized(<<0,0,1,0,0,0,0,0,0,0>>,2) = <<0,0,1,0,0,0,0,0,0,0>>
 /\ Quantized(<<1,0,1,0,0,0,0,0,0,0>>,1) = <<0,0,1,0,0,0,0,0,0,0>>
 /\ Quantized(<<1,0,1,0,0,0,0,0,0,0>>,2) = <<0,0,1,0,0,0,0,0,0,0>>
 /\ Quantized(<<615,551,709,73,744,446,18,0,0,0>>,1) = <<0,551,709,73,744,446,18,0,0,0>>
 /\ Quantized(<<615,551,709,73,744,446,18,0,0,0>>,2) = <<0,0,709,73,744,446,18,0,0,0>>
 /\ Shift(<<709,73,744,446,18,0,0,0,0,0>>,2) = <<0,0,709,73,744,446,18,0,0,0>>
 /\ Shift(<<710,73,744,446,18,0,0,0,0,0>>,2) = <<0,0,710,73,744,446,18,0,0,0>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,0,-1) = <<0,0,0,615,551,709,73,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,0,0) = <<0,0,0,615,551,709,73,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,0,1) = <<0,0,0,615,551,709,73,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1,-1) = <<385,448,290,541,807,262,55,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1,0) = <<0,0,0,615,551,709,73,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1,1) = <<615,551,709,688,295,156,92,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1000000,-1) = <<0,0,385,63,842,635,329,297,428,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1000000,0) = <<0,0,0,615,551,709,73,744,446,18>>
 /\ AdvanceTruth(<<615,551,709,73,744,446,18,0,0,0>>,1000000,1) = <<0,0,615,166,261,783,817,190,465,18>>
 /\ Lt(Max64,Add(Max64,One))
 /\ ~Le(Add(Max64,One),Max64)
 /\ Sub(Add(Max64,One),Max64)=One
 /\ Shift(Drop(Max64,2),2)=Quantized(Max64,2)


CONSTANTS Scenario, MaxCalls, MaxTokens, MaxStamps
StartTime == IF Scenario="boundary" THEN Sub(Max64,Small(10000000)) ELSE Zero
BaseServerMs == CASE Scenario="boundary" -> Sub(Drop(Max64,2),One)
 [] Scenario="usdm" -> Small(1999) [] OTHER -> Small(1000)
Offsets == {Zero,Small(999999)}
JumpServerMs == IF Scenario="usdm" THEN Small(1000) ELSE Small(2000)
Drift == CASE Scenario="rounding" -> 1 [] Scenario="usdm" -> 1000000 [] OTHER -> 0
MaxRtt == Small(2000000)
MaxWidth == CASE Scenario="rounding" -> Small(2000002)
 [] Scenario="boundary" -> Small(30000000) [] OTHER -> Small(3000000)
MaxAge == IF Scenario="rounding" THEN Add(Second,Small(2)) ELSE Small(10000000)
Adjustment == IF Scenario="usdm" THEN Add(Second,Second) ELSE Small(5000000)
Source == IF Scenario="usdm" THEN 2 ELSE 1
FirstSerial == IF Scenario="boundary" THEN Sub(Max64,One) ELSE One
Times == CASE Scenario="rounding" -> {Zero,One,Second,Add(Second,One),Add(Second,Small(2))}
 [] Scenario="boundary" -> {StartTime,Add(StartTime,One),Add(StartTime,Ms),Max64}
 [] OTHER -> {Zero,Ms,Small(2000000),Small(10000000),Small(11000000)}
BadServers == {Zero,Small(2000),Max64}
Windows == CASE Scenario="rounding" -> {Small(2000000),Small(2001000),Small(3000000)}
 [] Scenario="boundary" -> {Ms,Second,Shift(Small(60),3)}
 [] OTHER -> {Zero,Small(2000000),Small(2001000),Small(3000000),Add(Shift(Small(60),3),Us)}
Budgets == IF Scenario="boundary" THEN {Zero,Ms,Add(MaxAge,One)}
          ELSE {Zero,Ms,MaxAge}
ASSUME /\ Drift \in {0,1,1000000} /\ Source \in {1,2}
 /\ MaxCalls>0 /\ MaxTokens>0 /\ MaxStamps>0
 /\ Times \subseteq Word /\ StartTime \in Times /\ \A t \in Times:Le(t,Max64)
 /\ Windows \subseteq Word /\ Budgets \subseteq Word /\ BadServers \subseteq Word
 /\ MaxRtt#Zero /\ MaxWidth#Zero /\ MaxAge#Zero /\ FirstSerial#Zero
 /\ \A w \in {MaxRtt,MaxWidth,MaxAge,Adjustment,FirstSerial,BaseServerMs,JumpServerMs}:Le(w,Max64)
Q == Ms
NoToken == [domain |-> 0,source |-> 0,serial |-> Zero,sent |-> Zero]
NoReading == [lo |-> Zero,hi |-> Zero,at |-> Zero,expires |-> Zero,serial |-> Zero]
NoStamp == [domain |-> 0,source |-> 0,serial |-> Zero,timestamp |-> Zero,unit |-> 2]
ForeignToken == [domain |-> 2,source |-> 3,serial |-> One,sent |-> Zero]
ForeignStamp == [domain |-> 2,source |-> 3,serial |-> One,timestamp |-> One,unit |-> 2]
EmptyCore == [next |-> FirstSerial,pending |-> NoToken,anchor |-> NoReading,last |-> Zero,
 seen |-> FALSE,fault |-> "None"]
Answer(c,e,r,s) == [core |-> c,err |-> e,reading |-> r,stamp |-> s]
Ret(c,e) == Answer(c,e,NoReading,NoStamp)
Fail(c,e) == Ret([c EXCEPT !.fault=e,!.pending=NoToken,!.anchor=NoReading],e)
Observe(c,n,domain) == IF c.fault#"None" THEN Ret(c,"Faulted")
 ELSE IF domain#1 THEN Fail(c,"Domain")
 ELSE IF c.seen /\ Lt(n,c.last) THEN Fail(c,"TimeBackwards")
 ELSE Ret([c EXCEPT !.seen=TRUE,!.last=n],"Ok")
BeginCore(c,n) == LET a==Observe(c,n,1) IN
 IF a.err#"Ok" THEN a ELSE IF a.core.pending#NoToken THEN Ret(a.core,"Busy")
 ELSE IF a.core.next=Zero THEN Fail(a.core,"Overflow")
 ELSE LET token==[domain |-> 1,source |-> Source,serial |-> a.core.next,sent |-> n]
          next==IF a.core.next=Max64 THEN Zero ELSE Add(a.core.next,One)
      IN Ret([a.core EXCEPT !.next=next,!.pending=token],"Ok")
Candidate(t,n,ms) == [lo |-> SatSub(Shift(ms,2),Q),
 hi |-> Add(Add(Shift(ms,2),Q),Up(Sub(n,t.sent),Drift)),
 at |-> n,expires |-> Add(n,MaxAge),serial |-> t.serial]
Project(r,n) == [r EXCEPT !.lo=Add(@,Down(Sub(n,r.at),Drift)),
 !.hi=Add(@,Up(Sub(n,r.at),Drift))]
ReadingFits(r) == Le(r.lo,Max64) /\ Le(r.hi,Max64) /\ Le(r.expires,Max64)
WidthOK(r) == Le(Sub(r.hi,r.lo),MaxWidth)
Gap(a,b) == MaxW(SatSub(a.lo,b.hi),SatSub(b.lo,a.hi))
FinishCore(c,t,n,domain,ms) == IF c.pending=NoToken \/ c.pending#t THEN Ret(c,"Stale")
 ELSE LET a==Observe(c,n,domain) IN IF a.err#"Ok" THEN a
 ELSE LET d==[a.core EXCEPT !.pending=NoToken] IN
 IF ms=Zero THEN Ret(d,"Invalid")
 ELSE IF Lt(MaxRtt,Sub(n,t.sent)) THEN Ret(d,"RoundTrip")
 ELSE LET candidate==Candidate(t,n,ms) IN
 IF ~Le(Shift(ms,2),Max64) \/ ~ReadingFits(candidate) THEN Fail(d,"Overflow")
 ELSE IF ~WidthOK(candidate) THEN Ret(d,"Uncertain")
 ELSE IF d.anchor#NoReading /\ Lt(n,d.anchor.expires)
 THEN LET old==Project(d.anchor,n) IN
   IF ~ReadingFits(old) THEN Fail(d,"Overflow")
   ELSE IF Lt(Adjustment,Gap(candidate,old)) THEN Fail(d,"ClockJump")
   ELSE Answer([d EXCEPT !.anchor=candidate],"Ok",candidate,NoStamp)
 ELSE Answer([d EXCEPT !.anchor=candidate],"Ok",candidate,NoStamp)
CancelCore(c,t) == IF c.pending=NoToken \/ c.pending#t THEN Ret(c,"Stale")
 ELSE Ret([c EXCEPT !.pending=NoToken],"Ok")
ReadCore(c,n,domain) == LET a==Observe(c,n,domain) IN
 IF a.err#"Ok" THEN a ELSE IF a.core.anchor=NoReading THEN Ret(a.core,"Uncalibrated")
 ELSE IF ~Lt(n,a.core.anchor.expires) THEN Ret(a.core,"Expired")
 ELSE LET r==Project(a.core.anchor,n) IN
 IF ~ReadingFits(r) THEN Fail(a.core,"Overflow")
 ELSE IF ~WidthOK(r) THEN Ret(a.core,"Uncertain")
 ELSE Answer(a.core,"Ok",r,NoStamp)
SignCore(c,n,u) == IF Source=2 /\ u=1 THEN Ret(c,"Invalid")
 ELSE LET a==ReadCore(c,n,1) IN IF a.err#"Ok" THEN a
 ELSE Answer(a.core,"Ok",a.reading,[domain |-> 1,source |-> Source,
      serial |-> a.reading.serial,timestamp |-> Drop(Mid(a.reading.lo,a.reading.hi),u),unit |-> u])
Tns(s) == Shift(s.timestamp,s.unit)
WindowOK(w) == w#Zero /\ Le(w,Shift(Small(60),3))
 /\ Quantized(w,IF Source=2 THEN 2 ELSE 1)=w
CheckCore(c,s,n,w,b) == IF s.domain#1 \/ s.source#Source THEN Ret(c,"Stale")
 ELSE IF ~WindowOK(w) THEN Ret(c,"Invalid")
 ELSE LET a==ReadCore(c,n,1) IN IF a.err#"Ok" THEN a
 ELSE IF ~Le(Add(n,b),Max64) THEN Fail(a.core,"Overflow")
 ELSE IF ~Lt(Add(n,b),a.reading.expires) THEN Ret(a.core,"Expired")
 ELSE IF ~Lt(Tns(s),Add(a.reading.lo,Second)) THEN Ret(a.core,"Future")
 ELSE LET upper==Add(a.reading.hi,Up(b,Drift)) IN
 IF ~Le(Up(b,Drift),Max64) \/ ~Le(upper,Max64) THEN Fail(a.core,"Overflow")
 ELSE IF ~Le(upper,Add(Tns(s),w)) THEN Ret(a.core,"TooOld")
 ELSE Answer(a.core,"Ok",a.reading,NoStamp)

VARIABLE p
vars == <<p>>
Init == /\ ArithmeticOK
 /\ \E offset \in Offsets:
 p=[c |-> EmptyCore,now |-> StartTime,truth |-> Scaled(Add(Shift(BaseServerMs,2),offset)),
 epoch |-> 0,jumps |-> 0,beginEpoch |-> 0,
 capture |-> [token |-> NoToken,ms |-> Zero,epoch |-> 0],anchorTrusted |-> FALSE,
 tokens |-> {},stamps |-> {},calls |-> 0,
 staleOK |-> TRUE,monotoneOK |-> TRUE,readOK |-> TRUE,signOK |-> TRUE,
 sendOK |-> TRUE,truthOK |-> TRUE,sendTruthOK |-> TRUE,oldAnchorOK |-> TRUE,
 oldExpiryOK |-> TRUE,faultRevoked |-> TRUE,lastError |-> "None"]
ValidRead(r,n) == r#NoReading /\ ReadingFits(r) /\ Le(r.lo,r.hi)
 /\ Lt(n,r.expires) /\ WidthOK(r)
Contains(r,truth) == Le(Scaled(r.lo),truth) /\ Le(truth,Scaled(r.hi))
SendRules(s,n,w,b,r) == s.domain=1 /\ s.source=Source /\ WindowOK(w)
 /\ Lt(Tns(s),Add(r.lo,Second)) /\ Le(Add(r.hi,Up(b,Drift)),Add(Tns(s),w))
 /\ Lt(Add(n,b),r.expires)
TruthSend(s,w,b,truth) == Lt(Scaled(Tns(s)),Add(truth,Scaled(Second)))
 /\ \A rate \in {-1,0,1}:Le(Add(truth,AdvanceTruth(b,Drift,rate)),Scaled(Add(Tns(s),w)))
(* Each Apply is one synchronous API method. No internal call interleaving.
 Original source branches are preserved in the pure Core operators above. *)
Apply(o,op,n,s,w,b,goodReply) ==
 LET trusted==IF o.core.anchor=NoReading THEN FALSE
              ELSE IF op="Finish" /\ o.err="Ok" THEN goodReply ELSE p.anchorTrusted
     isReading==o.err="Ok" /\ o.reading#NoReading
     newTokens==IF op="Begin" /\ o.err="Ok" THEN p.tokens \cup {o.core.pending} ELSE p.tokens
     newStamps==IF op="Sign" /\ o.err="Ok" THEN p.stamps \cup {o.stamp} ELSE p.stamps
 IN /\ p.calls<MaxCalls /\ Cardinality(newTokens)<=MaxTokens /\ Cardinality(newStamps)<=MaxStamps
 /\ p'=[p EXCEPT !.c=o.core,!.calls=@+1,!.lastError=o.err,
  !.tokens=newTokens,!.stamps=newStamps,!.anchorTrusted=trusted,
  !.beginEpoch=IF op="Begin" /\ o.err="Ok" THEN p.epoch ELSE @,
  !.capture=IF op="Begin" /\ o.err="Ok" THEN [token |-> NoToken,ms |-> Zero,epoch |-> p.epoch] ELSE @,
  !.staleOK=@ /\ (o.err#"Stale" \/ o.core=p.c),
  !.monotoneOK=@ /\ (~p.c.seen \/ ~o.core.seen \/ Le(p.c.last,o.core.last)),
  !.readOK=@ /\ (~isReading \/ ValidRead(o.reading,n)),
  !.signOK=@ /\ (op#"Sign" \/ o.err#"Ok" \/
    (o.stamp.unit \in {1,2} /\ Le(Tns(o.stamp),Mid(o.reading.lo,o.reading.hi))
     /\ Lt(Mid(o.reading.lo,o.reading.hi),Add(Tns(o.stamp),Shift(One,o.stamp.unit)))
     /\ o.stamp.timestamp=Drop(Tns(o.stamp),o.stamp.unit))),
  !.sendOK=@ /\ (op#"Check" \/ o.err#"Ok" \/ SendRules(s,n,w,b,o.reading)),
  !.truthOK=@ /\ (~isReading \/ ~trusted \/ Contains(o.reading,p.truth)),
  !.sendTruthOK=@ /\ (op#"Check" \/ o.err#"Ok" \/ ~trusted \/ TruthSend(s,w,b,p.truth)),
  !.oldAnchorOK=@ /\ (op#"Finish" \/ o.err \notin {"Invalid","RoundTrip","Uncertain"}
                          \/ o.core.anchor=p.c.anchor),
  !.oldExpiryOK=@ /\ (o.core.anchor=NoReading \/ op="Finish" \/ o.core.anchor=p.c.anchor),
  !.faultRevoked=@ /\ (p.c.fault="None" \/ o.err#"Ok")]

Advance == \E n \in Times,rate \in {-1,0,1}:
 /\ p.calls<MaxCalls /\ Lt(p.now,n)
 /\ p'=[p EXCEPT !.truth=Add(@,AdvanceTruth(Sub(n,p.now),Drift,rate)),!.now=n]
Capture == /\ p.calls<MaxCalls /\ p.c.pending#NoToken /\ p.capture.token=NoToken
 /\ p'=[p EXCEPT !.capture=[token |-> p.c.pending,ms |-> Drop(p.truth,5),epoch |-> p.epoch]]
Jump == /\ p.calls<MaxCalls /\ p.jumps=0
 /\ p'=[p EXCEPT !.truth=Scaled(Shift(JumpServerMs,2)),!.jumps=1,!.epoch=@+1,!.anchorTrusted=FALSE]
Begin == Apply(BeginCore(p.c,p.now),"Begin",p.now,NoStamp,Zero,Zero,FALSE)
FinishNormal == /\ p.capture.token#NoToken
 /\ Apply(FinishCore(p.c,p.capture.token,p.now,1,p.capture.ms),"Finish",p.now,NoStamp,Zero,Zero,
          p.capture.epoch=p.epoch /\ p.beginEpoch=p.epoch)
FinishBad == \E ms \in BadServers:
 /\ p.c.pending#NoToken
 /\ Apply(FinishCore(p.c,p.c.pending,p.now,1,ms),"Finish",p.now,NoStamp,Zero,Zero,FALSE)
FinishStale == \E t \in p.tokens \cup {ForeignToken}:
 /\ t#p.c.pending
 /\ Apply(FinishCore(p.c,t,Zero,2,Max64),"Finish",Zero,NoStamp,Zero,Zero,FALSE)
Cancel == /\ p.c.pending#NoToken
 /\ Apply(CancelCore(p.c,p.c.pending),"Cancel",p.now,NoStamp,Zero,Zero,FALSE)
CancelStale == \E t \in p.tokens \cup {ForeignToken}:
 /\ t#p.c.pending /\ Apply(CancelCore(p.c,t),"Cancel",p.now,NoStamp,Zero,Zero,FALSE)
Read == Apply(ReadCore(p.c,p.now,1),"Read",p.now,NoStamp,Zero,Zero,FALSE)
ReadBackwards == /\ p.c.seen /\ p.c.last#Zero
 /\ LET n==Sub(p.c.last,One) IN Apply(ReadCore(p.c,n,1),"Read",n,NoStamp,Zero,Zero,FALSE)
ReadWrongDomain == Apply(ReadCore(p.c,p.now,2),"Read",p.now,NoStamp,Zero,Zero,FALSE)
Sign == \E unit \in {1,2}:Apply(SignCore(p.c,p.now,unit),"Sign",p.now,NoStamp,Zero,Zero,FALSE)
CheckSend == \E s \in p.stamps,w \in Windows,b \in Budgets:
 Apply(CheckCore(p.c,s,p.now,w,b),"Check",p.now,s,w,b,FALSE)
CheckForeign == Apply(CheckCore(p.c,ForeignStamp,Zero,Zero,Max64),"Check",Zero,ForeignStamp,Zero,Max64,FALSE)
Next == Advance \/ Capture \/ Jump \/ Begin \/ FinishNormal \/ FinishBad \/ FinishStale
 \/ Cancel \/ CancelStale \/ Read \/ ReadBackwards \/ ReadWrongDomain \/ Sign \/ CheckSend \/ CheckForeign
Spec == Init /\ [][Next]_vars

TypeOK == /\ p.c.next \in Word /\ p.c.last \in Word /\ p.now \in Times /\ p.truth \in Word
 /\ p.calls \in 0..MaxCalls /\ p.jumps \in 0..1 /\ p.epoch=p.jumps
 /\ p.c.fault \in {"None","Domain","TimeBackwards","Overflow","ClockJump"}
FaultClears == p.c.fault#"None" => p.c.anchor=NoReading /\ p.c.pending=NoToken
PendingIdentity == p.c.pending#NoToken => p.c.pending.domain=1 /\ p.c.pending.source=Source
 /\ p.c.pending.serial#Zero /\ Le(p.c.pending.sent,p.c.last)
AnchorIdentity == p.c.anchor#NoReading => p.c.anchor.serial#Zero /\ ReadingFits(p.c.anchor)
 /\ Le(p.c.anchor.at,p.c.last) /\ p.c.anchor.expires=Add(p.c.anchor.at,MaxAge)
SerialNotReused == \A a,b \in p.tokens:a.serial=b.serial => a=b
StaleDoesNotTouchClock == p.staleOK
ObservedTimeMonotone == p.monotoneOK
SuccessfulReadBounded == p.readOK
ExactSignedTimestamp == p.signOK
SuccessfulSendBounded == p.sendOK
ContinuousIntervalContainsTruth == p.truthOK
ContinuousSendWithinWindow == p.sendTruthOK
RejectedSampleRetainsAnchor == p.oldAnchorOK
ReadNeverExtendsExpiry == p.oldExpiryOK
FaultRevokesCalls == p.faultRevoked

=============================================================================
