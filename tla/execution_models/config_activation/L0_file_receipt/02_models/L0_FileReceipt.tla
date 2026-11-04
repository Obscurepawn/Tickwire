---------------------------- MODULE L0_FileReceipt ----------------------------
EXTENDS Naturals, FiniteSets

\* Source 9b2ed63. One Local owner per process/scope, fixed unique renders/operation IDs.
\* disk = count of sealed business events, omitting the constant scope header (real seq = count+1);
\* seen = prefix applied in Journal. journal.rs:74-104, replay:143-189 in tickwire-config-journal/src.
\* objects, desired, good, effective = durable file contents represented by their unique revision;
\* crates/tickwire-config-materializer/src/local.rs:80-128,145-174.
\* up, crashes = owner health, bounded process crashes / uncertain append, local.rs:69-78,176-187;
\* accepted = returned receipt sequence with header offset, or zero. Historical receipts:
\* crates/tickwire-config-journal/src/ledger.rs:70-77, journal.rs:74-104.
\* supplied = valid caller runtime fact pending sync. observed = recorded runtime witnesses,
\* crates/tickwire-config-materializer/src/local.rs:130-144 and config-journal/src/ledger.rs:122-139.
\* An observable reject/receipt/progress outcome is carried by last; previous file values in
\* retryState are ghost witnesses for the historical-retry property.
CONSTANT Operations, MaxCrashes
VARIABLES disk, seen, objects, desired, good, effective, up, crashes,
          accepted, supplied, observed, last, retryState
vars == <<disk,seen,objects,desired,good,effective,up,crashes,accepted,supplied,observed,last,retryState>>
op(n) == (n \div 4) + 1
stage(n) == (n % 4) + 1
New == up /\ seen=disk /\ disk<4*Operations
fileState == <<desired,effective>>
Init ==
  /\ disk=0 /\ seen=0 /\ objects={} /\ good={} /\ desired=0 /\ effective=0
  /\ up=TRUE /\ crashes=0 /\ accepted=0 /\ supplied=FALSE /\ observed={}
  /\ last="start" /\ retryState = <<0,0>>
\* Complete immutable projection installation before Prepared, local.rs:92-103.
PutObject ==
  /\ New /\ stage(disk)=1 /\ op(disk) \notin objects
  /\ objects'=objects \cup {op(disk)} /\ last'="file"
  /\ UNCHANGED <<disk,seen,desired,good,effective,up,crashes,accepted,supplied,observed,retryState>>
\* desired.toml is synced before its business receipt; local.rs:105-128.
PutDesired ==
  /\ New /\ stage(disk)=2 /\ desired#op(disk)
  /\ desired'=op(disk) /\ last'="file"
  /\ UNCHANGED <<disk,seen,objects,good,effective,up,crashes,accepted,supplied,observed,retryState>>
\* Caller owns runtime observation; local.rs:130-143 and config-journal/src/ledger.rs:122-139.
SupplyRuntime ==
  /\ New /\ stage(disk)=3 /\ ~supplied /\ supplied'=TRUE /\ last'="witness"
  /\ UNCHANGED <<disk,seen,objects,desired,good,effective,up,crashes,accepted,observed,retryState>>
\* write_effective installs immutable copy, then current file (local.rs:145-172).
PutGood ==
  /\ New /\ stage(disk)=4 /\ op(disk) \notin good
  /\ good'=good \cup {op(disk)} /\ last'="file"
  /\ UNCHANGED <<disk,seen,objects,desired,effective,up,crashes,accepted,supplied,observed,retryState>>
PutEffective ==
  /\ New /\ stage(disk)=4 /\ op(disk) \in good /\ effective#op(disk)
  /\ effective'=op(disk) /\ last'="file"
  /\ UNCHANGED <<disk,seen,objects,desired,good,up,crashes,accepted,supplied,observed,retryState>>
\* The journal checks the previous reference against the immediately preceding phase
\* (ledger.rs:112-158) and syncs a single event before applying its receipt (journal.rs:85-103).
Seal ==
  /\ New
  /\ CASE stage(disk)=1 -> op(disk) \in objects
       [] stage(disk)=2 -> desired=op(disk)
       [] stage(disk)=3 -> supplied
       [] stage(disk)=4 -> effective=op(disk)
  /\ observed'=IF stage(disk)=3 THEN observed \cup {op(disk)} ELSE observed
  /\ disk'=disk+1 /\ supplied'=FALSE /\ last'="sealed"
  /\ UNCHANGED <<seen,objects,desired,good,effective,up,crashes,accepted,retryState>>
\* Acknowledgment is the second half of the same serialized call. journal.rs:95-104.
Return ==
  /\ up /\ disk=seen+1 /\ seen'=disk /\ accepted'=disk+1 /\ last'="ok"
  /\ UNCHANGED <<disk,objects,desired,good,effective,up,crashes,supplied,observed,retryState>>
\* Process loss after an install or seal, including lost response. See config-materializer/tests/linux.rs
\* and config-journal/src/journal.rs:38-71,89-94. No mutation is possible while down.
Stop ==
  /\ up /\ crashes<MaxCrashes /\ up'=FALSE /\ crashes'=crashes+1
  /\ supplied'=FALSE /\ last'="stopped"
  /\ UNCHANGED <<disk,seen,objects,desired,good,effective,accepted,observed,retryState>>
\* local.rs:265-332 checks historic last-good and the current files against the durable
\* prefix. The unique pending render is allowed to exist before its own receipt.
DesiredAt(n) == (n+2) \div 4
EffectiveAt(n) == n \div 4
FilesMatch(n) ==
  /\ 1..((n+3) \div 4) \subseteq objects
  /\ 1..EffectiveAt(n) \subseteq good
  /\ (desired=DesiredAt(n) \/ (stage(n)=2 /\ desired=op(n)))
  /\ (effective=EffectiveAt(n) \/ (stage(n)=4 /\ effective=op(n)))
Reopen ==
  /\ ~up /\ FilesMatch(disk)
  /\ up'=TRUE /\ seen'=disk /\ last'="reopen"
  /\ UNCHANGED <<disk,objects,desired,good,effective,crashes,accepted,supplied,observed,retryState>>
\* Old identical step returns its original sequence, even after a higher version;
\* local.rs:92-97,108-112,149-155; ledger.rs:70-77; journal.rs:74-86.
Retry ==
  /\ up /\ disk=seen /\ seen>0 /\ ~supplied
  /\ \E k \in 1..seen : accepted'=k+1
  /\ retryState'=fileState /\ last'="retry"
  /\ UNCHANGED <<disk,seen,objects,desired,good,effective,up,crashes,supplied,observed>>
\* Preflight rejection of nonmatching base or runtime instance, before writes;
\* local.rs:80-84,130-144; ledger.rs:98-105,122-155.
Refuse ==
  /\ New /\ ~supplied /\ stage(disk) \in {1,3} /\ last'="refused"
  /\ UNCHANGED <<disk,seen,objects,desired,good,effective,up,crashes,accepted,supplied,observed,retryState>>
Next == PutObject \/ PutDesired \/ SupplyRuntime \/ PutGood \/ PutEffective \/ Seal
        \/ Return \/ Stop \/ Reopen \/ Retry \/ Refuse
Spec == Init /\ [][Next]_vars

AcknowledgedDurable == accepted=0 \/ accepted \in 2..(disk+1)
HistoricalRetryStable == last="retry" => retryState=fileState
EffectiveHasWitness == \A i \in 1..effective : i \in observed /\ i \in good /\ i \in objects
CrashRecoverable == FilesMatch(disk)
LedgerOrder == /\ seen<=disk /\ disk<=seen+1
               /\ observed=1..((disk+1) \div 4)
=============================================================================
