# L0 counterexample log

All four configurations verified all 14 safety invariants without counterexamples or Rust defect candidates.

During review, `nativeReads` was documented explicitly as `native_nanos` entry attempts (since `Unsupported` increments the witness without issuing an OS syscall), and `Generic` outcome classification guards were split so each generic input branch receives a distinct TLC coverage entry while preserving the exact transition relation.
