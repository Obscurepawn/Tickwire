# TLC and native runs

Run `python3 tla/execution_models/worker_handover/L0_market_consumer/04_evidence/check.py` from any checkout of the pinned source to regenerate the three logs. The script uses the repository TLC 2.19 jar, breadth-first search, one worker, a 256 MiB heap and fingerprint polynomial 0. The repository shortcut `./scripts/check-tla.sh worker_handover` runs both safety profiles.

| Profile | Generated | Distinct | Depth | Remaining | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| [Old watermark 1–2, exact successor cut](04_evidence/L0_MarketConsumer.log) | 12,420 | 4,668 | 28 | 0 | Three safety invariants hold |
| [Old watermark 1–4, successor up to two ahead](04_evidence/L0_MarketConsumer_ahead.log) | 63,044 | 22,968 | 30 | 0 | Three safety invariants hold |
| [Valid receipt handling](04_evidence/clock/reject.log) | 649 | 359 | 7 | 156 | Counterexample |

Each safety configuration generates successors for every modeled action, including rejecting a fresh worker receipt. The separate diagnostic stops at the first invariant violation. [Aggregate action counts](04_evidence/coverage.json)

Linux also passed the three existing host receipt tests and four SDK frontier tests with Cargo in locked, offline mode. The additional socket test exercises the receiver through its production dispatcher. [Host tests](04_evidence/host-tests.log) · [Worker tests](04_evidence/worker-tests.log) · [Socket experiment](04_evidence/clock/socket.log)
