# Safety properties

TLC checks that every dispatched intent has a matching local record and stored dependency, journal sequences are unique, and a single intent reaches dispatch at most once. Accepted transport requests have a journal record. Restart preserves the admission freeze and unknown sends keep their reserved cash. [Run counts and domains](03_run_log.md) · [Model invariants](02_models/L1_CrashReceipt.tla)
