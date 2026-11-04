# October 28, 2026 verification records

Use the stage report for a result and its source and binary manifests for the corresponding build. Each stage used the revision recorded in its source manifest.

| Stage | What the report records |
| --- | --- |
| [Instrument index and worker recovery](summary.json) | Five independent index benchmark runs, schema-3 handover with ordinary and 16 MiB + 1 byte strategy executables, and a bounded input-capture library. |
| [Dense book keys](dense-book.json) | Full-width instrument IDs across SBE, shared-memory publication and local book slots. This stage recorded a JSON stack overflow and an 8 MiB test workaround. |
| [Publication hot path](publisher-hot.json) | A reusable JSON/SBE workspace and real publication without heap allocation in the tested healthy path. It exercises a 2 MiB stack, cache limits, arena boundaries and publication failure. |
| [Expansion interfaces](market-expansion-core.json) | Plan validation, isolated candidate warmup, real QUIESCED cursors, re-JOIN and persistent replay across banks. Runtime assembly followed in the next stage. |
| [Live market expansion](market-expansion-runtime.json) | One catalog, the same processes, an appended high ID and three readers activated on the successor bank. The retained input chain connects S=2, the expansion observation at 161 and C=167 through schema-3 handover and fee reconciliation. |
| [Capture for declared rebuild](capture-runtime.json) | A real SDK/Host worker, full bounded capture window, nine-object closure and unsubscribe. Two fixed release worker tests and the prior schema-3 handover exercised compatibility. |
| [Rebuild installation and release](rebuild-runtime.json) | Installer and strategy factory in the same PID, continuation after the window, a separate rollback probe and coordinated schema-4 release. |
| [Configuration planning](config-planning.json) | Two qualified reads, a scoped draft, complete target actions and a bounded job. This stage produced a draft. |
| [Plan archival, completed October 29](config-archive.json) | Durable dependency archival and concurrent idempotent requests through real mTLS, three voters, leader change, snapshot installation and journal-tail recovery. Archival left the desired configuration unchanged. |

## Recheck a result

* For lookup timing, start with the five-run summary and its environment. The measurements are batch means over prebuilt queries. Other stage durations and benchmark smoke runs measure a different interval. [Lookup summary](index-bench-summary.json) · [Settings](index-benchmark-settings.json) · [First raw run](index-bench-1.csv)
* For market expansion, match 794 source inputs and eight deployed executable digests. The input audit checked the physical WAL, the initial Clock gap and the later Reactivated record. It also identified a repeated NEW status with zero filled quantity. Its script references retained paths on the original machine. [Source hashes](market-expansion-runtime-source-sha256.json) · [Binaries](market-expansion-runtime-binaries.json) · [Input audit](market-expansion-runtime-input-audit.json) · [Audit code](market-expansion-runtime-input-audit.py)
* For capture, the source manifest covers 808 build inputs. The reusable MD server and publisher ran in the parent thread and the strategy ran in a distinct process. Check the archived compatibility result against its deployed executable hashes. [Capture sources](capture-runtime-source-sha256.json) · [Compatibility result](capture-runtime-compat-result.json) · [Binaries](capture-runtime-compat-binaries.json)
* Configuration planning and archival have separate source manifests (855 and 868 inputs). The archive-specific fixture used a signed 60-second validity period and 30-second TLS session after a 10-second fixture expired during synchronous writes. [Planning sources](config-planning-source-sha256.json) · [Archive sources](config-archive-source-sha256.json)

The business scenarios used a local TLS exchange fixture. Paths under `/tmp` in JSON identify the original runs. The October 28 anchor audits checked correspondence with existing finite models. The expansion-core stage recorded a pre-existing JOIN anchor mismatch. Runtime expansion, capture and planning did not add new TLC coverage. [Expansion-core audit](market-expansion-core-anchor-audit.json) · [Runtime audit](market-expansion-runtime-anchor-audit.json) · [Capture audit](capture-runtime-anchor-audit.json) · [Planning audit](config-planning-anchor-audit.json) · [Archive audit](config-archive-anchor-audit.json)
