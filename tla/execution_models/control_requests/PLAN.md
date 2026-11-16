# Control request verification ladder

Targets `tickwire-control` ([control session specification](../../../specs/implementation/control-session.md)). `L0_registry` verifies request admission, digest deduplication, and completion correlation on `Session::admit` + `MockRequestStore` within an authenticated session; `L1_session` adds monotonic session lease deadlines, permanent closure, and lease renewal (`Session::check_time`, `validate`, `renew`).

Calls on `&mut Session` and `&mut MockRequestStore` execute synchronously without internal shared state: each `admit` or `complete` call finishes atomically before the next call may retry, mutate expected version, advance monotonic time, or arm a `MockRequestStore` error.
