# Counterexamples

Neither bounded configuration violated `TypeOK`, `ExclusiveBatonOwnership`, `FreshLinkHandoff`, `ReplySlotReservation`, or `CountedWritesOnly`.

- In the single-slot configuration (`Capacity = 1, TaskCount = 2`), `"tw-rest-query"` blocks `WorkerPullTask` whenever the first task occupies `waiting`, `inflight`, or an un-popped `reply_queue` slot while `ConsumerDequeue` stalls, preventing any overflow on `post_reply`.
- Concurrent `OwnerReset` and `OwnerStop` transitions during `Dialing` trigger `DialerAbortStale`, releasing the leased `IpPool` entry without overwriting `DialBaton.product` or leaking `in_use` counts.
