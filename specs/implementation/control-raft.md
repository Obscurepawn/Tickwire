# Durable Raft state

The base control group has three full voters and uses raft 0.7.0 with Safe ReadIndex, pre-vote and quorum checks. A command may contain up to 16 MiB. The file store implements `Storage` over the physical journal. Replace its in-memory index only after synchronization. The [peer service](control-raft-service.md) adds membership changes and snapshot transfer.

Persist every Ready's entries and HardState before releasing its messages. Persist LightReady commit progress as well. Synchronize the applied control record before advancing Raft's applied watermark and returning its term, index and bytes. The business reducer records acceptance or rejection separately.

Large updates use Begin, contiguous Chunk records and End. Begin binds the expected length and SHA-256. End publishes the logical update. A new Begin abandons an incomplete tail. A disk failure stops the node and withholds the messages from that drive. Recovery verifies node identity, term/vote, contiguous entries and commit/applied positions.

The leader waits for a committed, applied barrier in its term before proposing a business command. A safe read requires its random context, majority ReadIndex and sufficient local applied progress. Check leader, term and request again when claiming the proof. Claim it once. Validate peer/from/to, the allowed message kind and Append index/term continuity before passing data to raft-rs. Heartbeat commit is bounded by the local tail, including unstable entries.

A shared `Directory` holds the snapshot lock across snapshots, staging, verification and generation storage. The last owning Arc explicitly releases the guard. A forked descriptor between fork and exec cannot prolong a released owner or unlock its successor. File tests cover three-node restart, a 100,000-byte fragmented command, minority isolation and catch-up, durable votes, six sync-failure points and malformed Append/Heartbeat inputs.
