# Follow-up integration notes

`JoinOwner` maintains a bounded in-memory table of at most `M` cumulative registrations (including failed, expired, and detached entries), returning `Capacity` once exhausted so prior generation entries are never silently aliased in memory.

Linux multi-process integration tests exercise `tickwire-md` and independent subscriber processes exchanging sealed bank, snapshot, and cursor `memfd` descriptors over `SCM_RIGHTS` paired with bounded in-memory request idempotency.
