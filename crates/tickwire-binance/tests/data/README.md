# Signing fixtures

`PUBLIC_BINANCE_ED25519` comes from the public Binance FIX example. `TEST_ONLY_RSA` was generated for the test suite. OpenSSL independently produced the expected RSA signature. The fixtures belong to the signature tests. A deployed connector reads its account's credentials separately.
