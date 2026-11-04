# Retaining request weight

The total retained grant units never exceeded the per-window limit. Normal reservations also stayed within the limit less the recovery reserve. Every successful charge remained accounted after a stop receipt released unused capacity. A lost grant acknowledgement retained its full allocation. These results apply to the two configurations and three properties in the [run record](03_run_log.md).

- Fidelity: 12 source-cited transitions. The configured bucket identity, domain and lease are fixed. Open implementation questions inside this subset: zero.
- Code coverage: 12 of 12 selected transitions across quota preparation, commit, installation, charging, stop and reclaim, with an advancing clock and two allowed store failure points. The denominator is the model's Next action list.
- Model coverage: all actions fired across the completed searches. [TLC action counts](04_evidence/coverage.json)
