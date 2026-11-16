# L1 abstractions and scope

Lifts all `L0_registry` admission and completion transitions and prepends `Session::check_time` on incoming requests and renewals. Both valid and invalid frames execute `check_time` first: clock regression preserves `last_now` and latches `closed = true`; `now >= deadline` updates `last_now` and latches `closed = true`; and a closed session stays permanently closed, witnessed by recording the ticket count at closure so zero new `Execute` tickets can issue afterward.

`renew` updates `deadline` and `revision` iff the session is open, `nonce` matches, and `revision` strictly increases (still running `check_time` first on bad `nonce` / `revision` without mutating `RequestStore` or prior tickets). Tickets issued before session closure may still record their completion result afterward because `ExecutionTicket` completion borrows `RequestStore` rather than `Session`.
