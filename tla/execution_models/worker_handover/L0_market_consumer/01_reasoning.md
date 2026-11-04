# State and properties

The old applied watermark advances only when the worker finishes a batch against its old reader. On the approved QUIESCE it records that watermark. A commit can put the successor's cutover at that watermark or farther ahead. The worker pauses ordinary market callbacks while it joins the candidate, checks the current frontier, writes a merged checkpoint and samples the frontier again. A successful sample and a later receipt can describe different wall-clock instants. [Callback dispatch](../../../../crates/tickwire-strategy-sdk/src/worker.rs#L695) · [Commit validation](../../../../crates/tickwire-market-source/src/transition.rs#L316) · [Persistence and assignment](../../../../crates/tickwire-strategy-sdk/src/worker.rs#L785)

| Property | Observation |
| --- | --- |
| `SourceCut` | The old applied watermark stays at the captured cut once the consumer quiesces. |
| `ActivationEvidence` | Installing the new reader or dispatching a new callback implies a covered old cut, a completed checkpoint call and a successful final frontier sample. |
| `HostUsesWorkerCut` | The host sees only receipt phases sent by this worker. A commit follows observed quiescence, and host activation follows local assignment. |
| `ValidReceiptHandled` | A time-valid, correctly addressed worker receipt can be consumed without a spurious identity error. The diagnostic checks this separately. |

The host samples liveness and time at the start of one poll. Its dispatcher validates up to 16 packets with that sample. Scheduling can delay receipt creation, sending and polling independently. The model retains the writer timestamp in each of three immutable receipts and lets one more clock instant elapse during a poll. Host acceptance uses the sampled time. [Dispatcher](../../../../crates/tickwire-strategy-host/src/readiness.rs#L64) · [Model and variable map](02_models/L0_MarketConsumer.tla)
