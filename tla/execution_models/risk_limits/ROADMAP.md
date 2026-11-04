# Risk model coverage

| Level | Source boundary | Result |
| --- | --- | --- |
| L0 | Candidate account admission, spot availability and order reserves, physical and virtual limits, same-price fills and terminal release | Four invariants hold in both finite configurations. [Evidence](L0_cash_envelope/03_run_log.md) |

The production check consumes fixed-point amounts before sending a spot order. The present level checks exact integer quantities, signed per-strategy ownership and two strategies sharing one wallet. A decimal fill adds a distinct per-fill rounding boundary.
