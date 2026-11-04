# Scope

The checks concern one existing worker, one remote market-data subscription change and a single authorized operation. Process launch, multi-worker scheduling, trade authorization and recovery of a process that already wrote a transition checkpoint are separate contracts.

The model fixes the approved identity, descriptor, digest and clock domain. It tracks ordered receipt phases for three authentic replies and accepts an arbitrary delay or worker exit. It projects a successful sender path into one FIFO and abstracts idle iterations, duplicate retransmissions, file contents and descriptor validation. Native tests exercise PID, descriptor, old-cut and phase mismatches. There is no progress assumption. The final successor sample can be followed by a new fault, and a host poll can span a clock tick. [Host receipt tests](04_evidence/host-tests.log) · [Candidate tests](04_evidence/worker-tests.log)
