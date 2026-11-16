# L1 input

Extends `L0_registry` request admission and completion with `Session::{check_time, validate, renew}` for an established authenticated session (`now = 1`, `deadline = 1 + Lease`, `revision = 1`, `closed = false`; see `crates/tickwire-control/src/lib.rs:247-282`).

Monotonic clock validation, frame validation, and lease renewal follow `crates/tickwire-control/src/lib.rs:386-457` alongside the `requests.rs` anchors from `L0_registry`. No background thread renews or expires sessions implicitly: expiration latches permanent `closed = true` on the next `validate` or `renew` call (`check_time`).
