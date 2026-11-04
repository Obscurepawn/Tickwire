# Spot terminal fee decisions

The account owner receives authenticated order observations and individual executions. It compares both REST scans, checks the wallet, and proposes a fee closure for the quantity actually booked. This level checks the local decision, its journal position, and what a new writer reconstructs. It also seeds two previously written journals to check how the reader distinguishes a refused historical decision from an unexplained fee row. [Original counterexample](../L0_terminal_fees/04_counterexamples.md) · [Fixed transitions](02_models/L1_TerminalFees_v2.tla)
