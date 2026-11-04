# Retained subscription

**Retired, fixed.** The five-state failure no longer occurs for the two- or three-batch configuration. The scaled check includes an unavailable quote followed by another live pair on the same generation. The reader remains attached, and the policy keeps that generation closed to trading. All six invariants and all seven reported action entries were exercised across the configs. [Run records](03_run_log.md)

The fix changes the policy observer's meaning of acknowledgement. The policy still checks the current generation and the monotonic sequence and publication time. It appends accepted records to the existing bounded mark history. Invalidation uses the same mutable policy borrow as consumption. The normal publication path adds no allocations, queue hops or clock reads beyond the existing bounded history and clock checks.
