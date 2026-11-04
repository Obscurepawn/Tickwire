# State and assumptions

The input is an already admitted base-sized spot buy with sufficient BTC/USDT balances and valid account credentials. One normalized fill buys one unit at price two and pays one quote unit of fee. A trade ID may appear on both transports, each at a new durable source cursor. The account owner applies the two deliveries serially. On an exact same-cursor/same-byte repeat the gateway returns immediately.

`terminal` retains the greatest cumulative size a terminal observation reported. `facts` contains independently booked native trade IDs. Receiving a terminal report updates the first quantity without creating a trade. REST compares its cumulative size with the second quantity. It then checks the wallet and writes a fee record. The reducer checks both quantities again when it applies the record.

The model selects a complete REST pair with a fixed local cursor and valid UID, terms and increasing time. Its reduced fingerprint distinguishes the reported cumulative, fill history and wallet agreement. Differing scans are rejected. The private reader may have reported a higher terminal quantity before the stable cut. The second configuration exercises such disagreement. An account restart replays each fee record against its preceding WAL prefix. The model permits a crash on either side of append and application.

The finite order allows zero to two fills, one terminal private row and one two-read attempt. REST fills form an ordered prefix by ID. WebSocket fills may arrive in any order. Configured bounds make SHA collision resistance, paging completeness, writer fencing, clock validation and file synchronization inputs to the model. The physical WAL model covers tearing separately. No infinite-path availability claim is checked.
