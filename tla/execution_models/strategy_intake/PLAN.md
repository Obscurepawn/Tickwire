# File-backed strategy intake

A single cash owner in the Bybit or OKX live executable polls independent file queues in rotation. It owns both the account gateway and the outstanding REST request. A producer can make independent mistakes, retry a published command or compete for the last unit of cash. This ladder tracks which member submitted the command, where the account reserved cash and which member receives a decoded response.

[L0](L0_shared_cash/05_output.md) covers two members and one account. A future level could add same-logical-strategy manager handover and asynchronous worker reporting.
