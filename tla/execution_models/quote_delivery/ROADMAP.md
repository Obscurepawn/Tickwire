# Quote model status

| Level | Evidence | Scope |
| --- | --- | --- |
| [L0 paired quotes](L0_pair/05_output.md) | Four safety invariants hold. A separate probe reproduces loss of the SHM member. | One source, independent subscriber cursors, live/stale batches and sampled freshness. |
| [L0 paired quotes v2](L0_pair_v2/05_output.md) | The subscription failure is fixed. Six safety invariants hold in both configurations. | Account observation acknowledgement and sticky same-generation trading denial. |
