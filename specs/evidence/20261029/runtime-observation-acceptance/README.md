# Inspect the runtime observation acceptance bundle

The acceptance manifest records two fixed-binary business scenarios, six Host process tests, and App file observations. Start with the manifest for commands, source/build identities, executable samples, signed receipts and artifact digests. [Acceptance manifest](../runtime-observation-acceptance.json)

| Evidence | Acceptance condition |
| --- | --- |
| [Handover](handover/) and [declared rebuild](rebuild/) | Match observed worker and executable identities, the six Ed25519 receipts, and the reported final account, mirror and strategy positions. The full input replay handover reached S2→C10→D285. The rebuild installed into the same PID and reached D285. |
| [Host process tests](host/process.log) | A slow observer left the worker able to consume 176 account inputs and market data. The earlier queued observation retained its full closure. A later observation exceeded the 16 MiB typed-input limit and was rejected. The worker then unsubscribed and exited normally. Other cases cover successful fresh observations, cancellation drain and a new query after timeout. |
| [App outcomes](app/outcomes/) | Match original request deadline, challenge and READY position. The two independent challenges observed READY sequence 2 and 5. Each named 14 complete dependency objects. Duplicate requests retained their result, conflicting requests were rejected and restart marked a pending request Interrupted without redispatch. |
| [Input history](app/input-history.wal) and [execution log](app/exec.stderr.log) | Rebuild and App each recorded a startup Clock gap at epoch 1, generation 2. The WAL then recorded reconciliation at epoch 2, generation 2. The actual logs place reconciliation before order submission. The cumulative gap count remains. |
| [Closure audit](app/closure-audit.json) | The Rust auditor checked the complete live recovery object graph and account semantics before archival. The repository retains its results, small account CBOR and digests. The complete recovery object store and worker ELF were omitted. |
| [Development records](development/) | Recorded failures include send backpressure leading to timeout and inherited file locks between fork and exec. These are development diagnostics. Use the final run reports for acceptance. |

The offline verifier rehashes 183 artifacts, verifies the six signatures and recorded bindings, and recomputes the account prefix from the small archived objects. The manifest includes this README’s original 2,208 bytes with SHA-256 `56f9d13ecd287c4d6ff62e9dfa46605dd9eb65b30969715c10ed431a4c187526`. The English navigation here has different bytes. Run the following against the immutable historical tree from the repository root. It needs Python 3.11+ and `cryptography`.

```sh
evidence_dir=$(mktemp -d)
git archive 4f840fcf885ff28fe51036ae2bf4f12111a82357 specs/evidence/20261029 | tar -xf - -C "$evidence_dir"
python3 "$evidence_dir/specs/evidence/20261029/verify-runtime-observation-acceptance.py"
```

These workers used the ordinary resource launcher. The resource-stage report records which delegated-cgroup tests ran. [Verifier](../verify-runtime-observation-acceptance.py) · [Resource-stage report](../process-resources.json)
