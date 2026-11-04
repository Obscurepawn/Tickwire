# October 29, 2026 process and recovery evidence

These packages record fixed release runs on WSL2 Linux with a local TLS exchange. Each stage has its own source and executable identities. For a process result, open its acceptance report. For the inputs used to build it, open that stage’s source manifest.

| Stage | Acceptance recorded in Git |
| --- | --- |
| [Managed executable images](runtime-image.json) | Full input replay handover reached S2→C10→D285. The same-PID capture/install scenario reached D291. Both reconciled fees. Samples of the live worker’s memfd executable matched the fixed binary. Two of 875 source entries changed between the acceptance and final test manifests, both test files. [Process records](runtime-image-acceptance.json) · [Acceptance sources](runtime-image-source-sha256.json) · [Final sources](runtime-image-final-source-sha256.json) |
| [Child resources](process-resources.json) | Actual affinity/inheritance and failure before exec were exercised. A normal directory failed the kernel cgroup check without launching a worker. Two positive delegated-cgroup tests required a writable leaf unavailable on the run host. [Process records](process-resources-acceptance.json) |
| [Continuous observation](runtime-observation.json) | Two full business scenarios and six Host process tests checked challenges, timeouts, cancellation and slow readers. Two independent App observations advanced READY from 2 to 5. Restart retained the historical challenge and marked the unfinished request Interrupted. [Recorded bytes and limitations](runtime-observation-acceptance/README.md) · [Acceptance manifest](runtime-observation-acceptance.json) |
| [Configuration-bound launch](projection-launch.json) | Compare projected identity, executable, OS principal and CPU/NUMA/memory constraints before accepting an intent. Tests rejected mismatches and resource failures. The ordinary spot compatibility run omitted the optional projection. A successful constrained launch still required delegated cgroup permissions. [Source manifest](projection-launch-source-sha256.json) |
| [MD cold recovery after expansion](md-restart.json) | Seventeen fixed-CLI process tests captured 22 executable samples. The archive retains a 13-snapshot expansion history and a three-snapshot startup-fault history. Restart required a new producer and fresh admissions while preserving the old dense IDs and grants. Four real SIGKILL cuts exercised distinct phases. [CLI observations](md-restart/cli-acceptance.json) · [Audited report](md-restart-audited-report.json) |
| [Projected strategy parameters](strategy-parameters.json) | Actual registered workers changed behavior with observation and bid thresholds. A queued intent waited for a fresh parameter proof. A 50 ms timeout closed the worker. The parameter tests stopped at intent delivery. [Sources](strategy-parameters-source-sha256.json) |
| [Projected strategy inputs](strategy-inputs.json) | Real MD JOIN and checkpoint inspection checked six independent challenges and six input proofs over 24 merged rows. The recorded verdict excludes gateway submission and global configuration activation. [Offline verification result](strategy-inputs-verification.json) |

## Verify retained packages

The historical run manifests hash exact bytes. In particular, the observation bundle includes the original Chinese directory README as one of its 183 artifacts. To run its verifier after this English navigation was written, extract the original Git tree to a temporary directory. Run these commands from the repository root with Python 3.11+ and `cryptography` available.

```sh
evidence_dir=$(mktemp -d)
git archive 4f840fcf885ff28fe51036ae2bf4f12111a82357 specs/evidence/20261029 | tar -xf - -C "$evidence_dir"
(
    cd "$evidence_dir/specs/evidence/20261029" || exit 1
    python3 verify-runtime-image-acceptance.py &&
    python3 verify-process-resources-acceptance.py &&
    python3 verify-runtime-observation-acceptance.py &&
    python3 verify-md-restart.py &&
    python3 verify-strategy-inputs.py &&
    python3 verify-strategy-parameters.py
)
```

All six commands check archived bytes and recorded bindings. They do not reconnect to an exchange, rerun the original workers or establish current trading authority. Historical startup Clock gaps and later reconciliation remain in the input histories. The observation archive documents the ordering for its rebuild and App scenarios. [Observation evidence](runtime-observation-acceptance/README.md)

## Instrument catalog archive gap

The Git tree has a source manifest covering 967 build inputs, an anchor audit and the offline verifier source. It does **not** contain the stage report `instrument-catalog.json` or the associated acceptance artifact directory. The source manifest identifies inputs to the historical build. It cannot substantiate a complete process result, and the archived catalog verifier cannot reproduce its verdict from this checkout. [Source manifest](instrument-catalog-source-sha256.json) · [Anchor audit](instrument-catalog-anchor-audit.json) · [Verifier source](verify-instrument-catalog.py)

The maintained catalog contract describes what a current test should assert. For example, a price string of `"1.00"` must normalize to 100 ticks on a 0.01 grid and 50 on a 0.02 grid. The shared Rust/Python format vectors have a separate, self-contained test path. [Catalog specification](../../implementation/instrument-catalog.md) · [Shared format vectors](../../contracts/instrument-catalog-v1.json) · [Format verifier](../../../scripts/verify-instrument-catalog.py)
