# JOIN owner verification plan

Targets `crates/tickwire-join` paired with live pending-cursor registration, activation, and quiescence detachment in `crates/tickwire-shm`. `L0_pending_activation` verifies preparation, `SCM_RIGHTS` delivery, catch-up, timeout expiration, and cooperative detachment under authenticated identities; `broadcast_ring` L0 and `control_requests` L0/L1 cover underlying slot reclamation and control-session request idempotency.

Owner publication and control handlers serialize on `&mut JoinOwner`, while subscribers run in separate processes and may advance their shared cursors concurrently between owner calls. `JoinOwner` adopts a subscriber's live `released` cursor watermark only after a valid `JOIN_READY` confirms catch-up; preparation, delivery, or timeout failures retain the pending baseline gate.

Ring capacity is fixed to the real minimum (`64`). Primary configurations explore preparation/delivery failures and timeouts from `S = 4`, while two `wrap` configurations start from a reachable full-ring prefix where subscribers registered at `S = 4`, the owner published `5..68`, and subscribers released/applied `5..67` while still `Pending`, testing live borrow protection at `68` and physical slot reuse at `69`.
