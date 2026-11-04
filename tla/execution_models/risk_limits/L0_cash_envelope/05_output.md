# Verified properties

| Property | Checked behavior |
| --- | --- |
| AllFillsFit | Every combination of future partial execution quantities on admitted orders preserves both protected cash balances and the absolute, per-member and summed absolute virtual inventory bounds. |
| OwnershipConserved | The two strategies' virtual inventory and the unattributed residual add up to physical inventory after each accepted execution. |
| RefusalAtomic | Rejected candidates leave the previously committed orders, fill balances and occupied slots unchanged. |
| ValidPartialKeepsGate | A bounded, within-limit, price-one fill keeps the account's post-event risk gate open. |

The account owner serializes model actions. The checks assume exact integral notionals, current admission gates and production-compatible opening balances. Incoming exchange events with unexpected cumulative quantities, account reconciliation and futures capital follow separate event paths.
