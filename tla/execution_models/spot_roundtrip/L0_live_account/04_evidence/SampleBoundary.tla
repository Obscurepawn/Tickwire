---------------------------- MODULE SampleBoundary ----------------------------
EXTENDS L0_SpotRoundtrip
\* Stronger than the documented local sample: a second check at byte acceptance.
\* crates/tickwire-spot-runner/src/live.rs:157-163; crates/tickwire-transport/src/connection.rs:188-217
ByteRequiresCurrent == [][~FirstByte \/ Qualified]_vars
===============================================================================
