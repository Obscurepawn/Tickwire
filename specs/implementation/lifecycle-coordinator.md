# Coordinated release across risk domains

The lifecycle coordinator binds an ordered member list to a release digest. Each member uses its own monotonic clock and deadline. Prepare, Commit, Release, QueryApplied and Halt carry signatures. Match each receipt to the pinned member key and current challenge before advancing the operation.

Issue GroupRelease only after every member reports Committed. Enter ActiveAll only after every member reports Released. Preserve each member's actual progress during a partial commit. An uncommitted member may submit updated readiness and must complete Prepare again. Members that already committed retain their facts.

Compare state versions and synchronize the candidate before making it visible. An uncertain sync freezes the owner. On recovery, obtain current QueryApplied receipts for this round, check the gates and explicitly resume. After partial release, send Release only to the remaining members and retain the original Committed receipts from the members already released. A halted member or changed gate invalidates the former authorization.

The physical journal stores chunked events followed by length and digest. Recovery accepts a required physical frontier and replays complete tails. Signed requests use a bounded directory transport with synchronized, atomic publication. The two-domain tests cover lost receipts, missing queries, changed gates, partial release and Halt. See the [coordinator crate](../../crates/tickwire-lifecycle/).
