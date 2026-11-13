# Hot-path decoupling and windowed pools

Off-hot-path session dialing (`DialBaton` + `IpPool`) and asynchronous REST querying (`AsyncRestQuerier`) decouple blocking handshakes, DNS/IP rotation, and bounded HTTP retries from the single-threaded quote/execution loop. Inside `Account`, `AccountGateway`, and `AccountJournal`, fixed-capacity `WindowedTable` pools and prefix compaction bound memory across multi-day uptime while protecting live orders, unreconciled fees, duplicate-intent/fill windows, management headroom, and rolling `HistoryFrontier` continuity.
