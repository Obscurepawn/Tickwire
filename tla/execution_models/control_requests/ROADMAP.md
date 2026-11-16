# Control request roadmap

| Level | Scope | Status |
| --- | --- | --- |
| `L0_registry` | Admission, idempotent retry, digest conflict, capacity/pending limits, immutable terminal results, and `MockRequestStore` errors | 3 configurations PASS |
| `L1_session` | Monotonic session lease deadlines, permanent close, lease renewal, and admission composition | 3 configurations PASS |
| `L2_owner_composition` | Composition with downstream domain-owner side effects | Planned |
