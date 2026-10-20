-------------------------- MODULE L0_LateReplayProbe --------------------------
EXTENDS L0_DepthSync
\* Reachability probe only: TLC is expected to violate this observer assertion.
\* A real first applied event followed by replay failure must be reachable.
WitnessNotReached == outcome # "LateReplayFailure"
=============================================================================
