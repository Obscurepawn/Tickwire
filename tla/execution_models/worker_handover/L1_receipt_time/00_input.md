# Hosted receipt dispatch

One authenticated worker publishes Armed, Quiesced and Activated receipts on an ordered local socket. The host may send the command and read the resulting reply in one poll. Either process may run between those operations. Both read the same monotonic clock domain. [Host dispatch](../../../../crates/tickwire-strategy-host/src/remote.rs#L307-L325) · [Receipt validator](../../../../crates/tickwire-market-source/src/transition.rs#L201-L222)
