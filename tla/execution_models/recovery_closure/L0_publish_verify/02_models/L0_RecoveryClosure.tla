---------------------- MODULE L0_RecoveryClosure ----------------------
EXTENDS Naturals, Integers, FiniteSets, TLC
CONSTANTS Roots, Dependencies, MaxCalls, MaxCrashes
\* var <-> code, single owner with &mut Persistence and exclusive directory lock.
\* pc/mode/root/index: synchronous continuation, crates/tickwire-recovery/src/lib.rs:151-243.
\* cache/disk: visible immutable fixed keys vs synchronized contents, crates/tickwire-recovery/src/files.rs:83-148.
\* exists/parent: directory visible vs parent-dir sync complete, crates/tickwire-recovery/src/files.rs:15-60.
\* key/value: pending object/manifest/binding, crates/tickwire-recovery/src/lib.rs:158-197; crates/tickwire-recovery/src/lib.rs:254-259.
\* valid: the check() input coverage success/failure exit, crates/tickwire-recovery/src/lib.rs:223-241.
\* issued: ghost history of returned private VerifiedClosure, crates/tickwire-recovery/src/lib.rs:173-177; crates/tickwire-recovery/src/lib.rs:198-202.
\* counters: finite calls/faults only. No mutation/deletion of durable objects is exposed.
\* ACTIONS (fault injection follows PLAN; all writes are serialized):
\* Open/RootSync/ParentSync/OpenFailure: crates/tickwire-recovery/src/files.rs:15-60
\* Seed: crates/tickwire-recovery/src/lib.rs:158-164
\* BeginPublish/RejectShape: crates/tickwire-recovery/src/lib.rs:166-167; crates/tickwire-recovery/src/lib.rs:204-205
\* BeginVerify/Lookup: crates/tickwire-recovery/src/lib.rs:179-195
\* CheckObject/CheckInputs: crates/tickwire-recovery/src/lib.rs:204-242
\* PrepareBinding: crates/tickwire-recovery/src/lib.rs:171-172; crates/tickwire-recovery/src/lib.rs:197
\* Put: crates/tickwire-recovery/src/files.rs:107-118
\* TempWrite/FileSyncNew/Rename: crates/tickwire-recovery/src/files.rs:138-142
\* FileSyncExisting/DirSync: crates/tickwire-recovery/src/files.rs:112-114,142-143
\* IoFailure: Result error exits at crates/tickwire-recovery/src/files.rs:107-147
\* ReturnPut: sequential ? continuations, crates/tickwire-recovery/src/lib.rs:163-164; crates/tickwire-recovery/src/lib.rs:170-172; crates/tickwire-recovery/src/lib.rs:195-197; crates/tickwire-recovery/src/lib.rs:220-222
\* Deliver: crates/tickwire-recovery/src/lib.rs:173-177; crates/tickwire-recovery/src/lib.rs:198-202
\* Crash: tolerated loss of unsynchronized directory names, file/dir completion split
\*   crates/tickwire-recovery/src/files.rs:139-142; previously synchronized names survive.
VARIABLE s
vars == <<s>>
RootSet == 1..Roots
ObjectKey(r,i) == (r-1)*(Dependencies+1)+i
ManifestKey(r) == ObjectKey(r,Dependencies+1)
Binding == Roots*(Dependencies+1)+1
Keys == 1..Binding
Empty == [k \in Keys |-> 0]
\* A failed directory sync can leave several earlier renamed keys unconfirmed.
\* Each renamed file was file-synced before visibility; each dirty name may survive.
Dirty == {k \in Keys: s.disk[k] # s.cache[k]}
Survivors == {[k \in Keys |-> IF k \in keep THEN s.cache[k] ELSE s.disk[k]]:
              keep \in SUBSET Dirty}
ObjectKeys == {ObjectKey(r,i): r \in RootSet,i \in 1..Dependencies}
Complete(r,d) == /\ \A i \in 1..Dependencies: d[ObjectKey(r,i)]=ObjectKey(r,i)
                 /\ d[ManifestKey(r)]=ManifestKey(r) /\ d[Binding]=r
Init == s=[pc |-> "Down",mode |-> "None",root |-> 1,index |-> 1,
    cache |-> Empty,disk |-> Empty,exists |-> FALSE,parent |-> FALSE,
    key |-> 1,value |-> 1,valid |-> TRUE,issued |-> {},calls |-> 0,crashes |-> 0]

\* FileBlobStore::open repeats both directory and parent sync for existing paths.
Open == /\ s.pc="Down" /\ s'=[s EXCEPT !.pc="RootSync",!.exists=TRUE]
RootSync == /\ s.pc="RootSync" /\ s'=[s EXCEPT !.pc="ParentSync"]
ParentSync == /\ s.pc="ParentSync" /\ s'=[s EXCEPT !.pc="Idle",!.parent=TRUE]
OpenFailure == /\ s.pc \in {"RootSync","ParentSync"} /\ s'=[s EXCEPT !.pc="Down"]

\* Objects are pre-staged by put_object; no capability is returned by that API.
Seed(k) == /\ s.pc="Idle" /\ s.calls<MaxCalls /\ k \in ObjectKeys
    /\ s'=[s EXCEPT !.pc="Put",!.mode="Seed",!.key=k,!.value=k,!.calls=@+1]
BeginPublish(r,v) == /\ s.pc="Idle" /\ s.calls<MaxCalls /\ r \in RootSet /\ v \in BOOLEAN
    /\ s'=[s EXCEPT !.pc="Check",!.mode="Publish",!.root=r,!.index=1,!.valid=v,!.calls=@+1]
BeginVerify(r) == /\ s.pc="Idle" /\ s.calls<MaxCalls /\ r \in RootSet
    /\ s'=[s EXCEPT !.pc="Lookup",!.mode="Verify",!.root=r,!.index=1,!.valid=TRUE,!.calls=@+1]
RejectShape == /\ s.pc="Idle" /\ s.calls<MaxCalls /\ s'=[s EXCEPT !.calls=@+1]
Lookup == /\ s.pc="Lookup"
    /\ s'=[s EXCEPT !.pc=IF s.cache[Binding]=s.root /\ s.cache[ManifestKey(s.root)]=ManifestKey(s.root)
                           THEN "Check" ELSE "Idle"]
CheckObject == /\ s.pc="Check" /\ s.index<=Dependencies
    /\ LET k==ObjectKey(s.root,s.index) IN
       s'=[s EXCEPT !.pc=IF s.cache[k]=k THEN "Put" ELSE "Idle",!.key=k,!.value=k]
CheckInputs == /\ s.pc="Check" /\ s.index=Dependencies+1
    /\ s'=[s EXCEPT !.pc=IF s.valid THEN "Put" ELSE "Idle",
                     !.key=ManifestKey(s.root),!.value=ManifestKey(s.root)]
PrepareBinding == /\ s.pc="Check" /\ s.index=Dependencies+2
    /\ s'=[s EXCEPT !.pc="Put",!.key=Binding,!.value=s.root]

\* Existing bytes must match; failed publication never rewrites an occupied key.
Put == /\ s.pc="Put"
    /\ s'=[s EXCEPT !.pc=IF s.cache[s.key]=0 THEN "TempWrite"
                        ELSE IF s.cache[s.key]=s.value THEN "FileSyncExisting" ELSE "Idle"]
TempWrite == /\ s.pc="TempWrite" /\ s'=[s EXCEPT !.pc="FileSyncNew"]
FileSyncNew == /\ s.pc="FileSyncNew" /\ s'=[s EXCEPT !.pc="Rename"]
Rename == /\ s.pc="Rename" /\ s'=[s EXCEPT !.cache[s.key]=s.value,!.pc="DirSync"]
FileSyncExisting == /\ s.pc="FileSyncExisting" /\ s'=[s EXCEPT !.pc="DirSync"]
DirSync == /\ s.pc="DirSync" /\ s'=[s EXCEPT !.disk=s.cache,!.pc="Returned"]
IoFailure == /\ s.pc \in {"TempWrite","FileSyncNew","Rename","FileSyncExisting","DirSync"}
    /\ \E contents \in (IF s.pc="DirSync" THEN Survivors ELSE {s.disk}):
          s'=[s EXCEPT !.pc="Idle",!.disk=contents]
ReturnPut == /\ s.pc="Returned"
    /\ s'=[s EXCEPT !.pc=IF s.mode="Seed" THEN "Idle" ELSE "Check",!.index=@+1]
Deliver == /\ s.pc="Check" /\ s.index=Dependencies+3
    /\ s'=[s EXCEPT !.pc="Idle",!.issued=@ \cup {s.root}]

\* A crash preserves synchronized names and independently resolves all dirty names.
Crash == /\ s.pc#"Down" /\ s.crashes<MaxCrashes
    /\ \E contents \in Survivors, dir \in BOOLEAN:
       /\ (s.parent => dir)
       /\ s'=[s EXCEPT !.pc="Down",!.cache=IF dir THEN contents ELSE Empty,
           !.disk=IF dir THEN contents ELSE Empty,!.exists=dir,!.parent=dir,!.crashes=@+1]
Next == Open \/ RootSync \/ ParentSync \/ OpenFailure
    \/ (\E k \in ObjectKeys: Seed(k)) \/ (\E r \in RootSet,v \in BOOLEAN: BeginPublish(r,v))
    \/ (\E r \in RootSet: BeginVerify(r)) \/ RejectShape \/ Lookup \/ CheckObject \/ CheckInputs
    \/ PrepareBinding \/ Put \/ TempWrite \/ FileSyncNew \/ Rename \/ FileSyncExisting \/ DirSync
    \/ IoFailure \/ ReturnPut \/ Deliver \/ Crash
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.cache \in [Keys -> 0..Binding] /\ s.disk \in [Keys -> 0..Binding]
    /\ s.issued \subseteq RootSet /\ s.calls \in 0..MaxCalls /\ s.crashes \in 0..MaxCrashes
ProofHasDurableClosure == \A r \in s.issued: Complete(r,s.disk) /\ s.parent
NoUnopenedIO == s.pc \notin {"Down","RootSync","ParentSync"} => s.parent
ImmutableBinding == [][s.disk[Binding]#0 => s.disk'[Binding]=s.disk[Binding]]_vars
=======================================================================
