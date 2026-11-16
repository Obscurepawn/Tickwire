# L0 TLC run log (archived `v1`)

Checked with the repository-pinned `tla2tools.jar` (SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`), OpenJDK 24.0.2, single worker, and `2 GiB` heap using isolated temporary state directories.

```sh
java -Xmx2g \
  -Djava.io.tmpdir="$run_tmp/java" -XX:+UseParallelGC \
  -cp "$repo/tla/tools/tla2tools.jar" tlc2.TLC \
  -workers 1 -coverage 1 -metadir "$run_tmp/states" \
  -config L0_PrivateSession.cfg L0_PrivateSession.tla
```

| Configuration | Generated | Distinct | Depth | TLC time | Result |
| --- | ---: | ---: | ---: | --- | --- |
| `L0_PrivateSession` | 20,307 | 9,675 | 24 | 1 s | PASS |
| `L0_PrivateSession_logon` | 3,113 | 1,653 | 24 | 0 s | PASS |
| `L0_PrivateSession_usdm` | 495,148 | 242,485 | 30 | 13 s | PASS |
| `L0_PrivateSession_buffer` | 2,077 | 939 | 18 | 0 s | PASS |

The final coverage block of every log includes `Init` and 28 named `Next` actions with `generated > 0` (`SpotNext` and `UsdmNext` split product-specific transitions cleanly), verifying 15 safety invariants without counterexamples.
