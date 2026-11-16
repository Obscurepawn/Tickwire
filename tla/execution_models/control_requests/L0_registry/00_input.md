# L0 input

Models `Session::admit` and `MockRequestStore` in `tickwire-control` for requests that have already passed peer authentication and `Session::validate` framing checks.

Source boundaries: `crates/tickwire-control/src/requests.rs:6-32` (`RequestStore` records and backend contract), `crates/tickwire-control/src/requests.rs:35-108` (`ExecutionTicket` and response correlation), `crates/tickwire-control/src/requests.rs:109-169` (digest check, retry resolution, and admission), and `crates/tickwire-control/src/requests.rs:171-244` (`MockRequestStore` state and failure injection).

Each request key represents an authenticated `(instance, epoch, request_id)` tuple, and each command digest commits its `expected_state_version` so callers cannot mutate the expected version without changing the digest.
