# Terminal fee closure

At Tickwire revision `9b2ed630721911e01046822bddc9db21b2fa5aee`, can a terminal fee decision make a previously readable account journal unreplayable? A conflicting private order and REST inventory should keep trading frozen while preserving durable state for diagnosis and a later recovery attempt. Partial fills and duplicate deliveries make the two cumulative quantities observable independently.

The existing private-cut model covers connection continuity; the account economic model covers reservations. This level follows a single canceled spot order across both data sources and the fee-record completion point. See the [model and source anchors](02_models/L0_TerminalFees.tla).
