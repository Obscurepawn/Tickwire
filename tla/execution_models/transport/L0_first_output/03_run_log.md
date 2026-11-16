# Run Log

Verified with the repository-pinned `tla2tools.jar` (`TLC 2.19`) via `./scripts/check-tla.sh transport` (`-workers 1 -coverage 60` per configuration in isolated temporary scratch directories).

Manual single-level reproduction from the repository root:

```bash
./scripts/check-tla.sh transport
```

| Configuration | Generated States | Distinct States | Diameter | Next Actions | Result |
|---|---:|---:|---:|---:|---|
| `primary` | 2,733 | 688 | 14 | 19 | PASS |
| `two_sends` | 14,793 | 3,928 | 20 | 19 | PASS |
| `larger_buffer` | 21,975 | 5,944 | 20 | 19 | PASS |
| `plain` | 10,183 | 2,832 | 13 | 16 | PASS |

Every configuration finishes in `< 2s` with `0 states left on queue`, and all named `Next` actions (`19` in TLS configurations, `16` in `plain`) achieve `generated > 0`. Archived evidence summary: [04_evidence/results.json](04_evidence/results.json).
