# Binance request quotas

`tickwire-binance-quota` charges clock samples, private control, public market data and trading commands to a shared `ExecLimiter`. Each configured action supplies the matching source, bucket list, cost class and reported network weight. A missing rule or mismatched report rejects the request.

An endpoint binds the policy generation, expiration and supervisor-assigned producer ID. The attempt table retains at most 4,096 entries. Private requests also bind account, credential generation, connection generation and content digest. An exact retry checks the original authorization deadline. Conflicting identities and public 429 or 418 responses freeze the shared consumer. The supervisor reads `consumer()` to issue and renew allocations.

Trading commands consume quota within the first-byte gate and deduplicate by command digest, original intent epoch and channel. Once charged, the quota stays consumed even for a rejection, zero-byte send or uncertain result. Endpoint costs, renewal windows and distribution of authoritative feedback belong to the control plane.
