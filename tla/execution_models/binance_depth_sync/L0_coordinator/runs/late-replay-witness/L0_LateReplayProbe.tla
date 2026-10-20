-------------------------- MODULE L0_LateReplayProbe --------------------------
EXTENDS L0_DepthSync
\* Positive reachability probe; expected violation is not an implementation bug.
WitnessNotReached == outcome # "LateReplayFailure"
\* Only this witness search is restricted. All four safety cfgs are unconstrained.
\* A retained trace is also a trace of the unchanged production Next relation.
WitnessDomain == /\ s.lastConn<=1 /\ s.gen<=2 /\ s.req<=1
 /\ s.clock \in {No,0} /\ viewNow=No
 /\ \A i \in 1..Len(s.buf):
     /\ s.buf[i].stamp=0 /\ s.buf[i].first=(IF i=1 THEN 0 ELSE 2)
     /\ s.buf[i].last=(IF i=1 THEN 1 ELSE 2) /\ s.buf[i].previous=0
     /\ s.buf[i].qty=1 /\ s.buf[i].valid=(i=1)
=============================================================================
