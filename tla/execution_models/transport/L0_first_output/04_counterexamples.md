# Counterexamples

All four finite configurations (`primary`, `two_sends`, `larger_buffer`, `plain`) pass with zero safety counterexamples. Socket-level edge cases discovered during transport implementation — bounded plaintext decryption per call, draining buffered plaintext prior to EOF teardown, and recording `SendState::Flushed` (`mark_flushed`) before receive-side EOF — are covered by deterministic socket regression tests in [crates/tickwire-transport/tests](../../../../crates/tickwire-transport/tests).
