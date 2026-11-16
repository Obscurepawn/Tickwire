# System clock follow-up

`L0_native_owner` is complete: 4 configurations PASS, 14 safety invariants, 20 `Next` actions covered across configurations, and locked source anchors with zero open candidates.

- Planned `TimeClient` network L0: connection/request permits, zero-byte retry, sample tokens, response/`EOF` handling, cancellation, and calibration expiry.
- Private-stream `ClockAdapter` budget-callback and final-sample ordering is covered in `binance_private_session/L0_session_lifecycle` (`v2`).
