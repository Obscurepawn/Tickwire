# Binance execution adapter

`tickwire-binance-execution` binds one account, product, environment and clock domain to a sender. The command digest identifies the immutable request bytes and the original `SignedTime`. The configured table holds at most 4,096 commands. Once full, it rejects a new entry. An exact retry uses the retained signature.

`GateAuthority` delegates recovery-closure persistence to its inner authority. At the first application write it validates the original signature, lease, request deadline and remaining quota. The budget callback and policy read both precede the last clock sample. An expired signature, changed clock domain or insufficient quota prevents the write.

Unit tests exercise encoding retries, missing encodings, lease and policy changes, and a budget callback that advances the clock beyond the signing window.
