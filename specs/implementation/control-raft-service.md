# Peer service, snapshots and membership

A single `PeerService<Replica<GenerationStore>>` owns the Raft node, business replica and result queue. Nonblocking TCP and mutually authenticated TLS 1.3 transport peer frames. The control facade exposes typed proposals, bootstrap and safe reads. Reserve result space before driving 1–128 complete Ready batches. A full result queue pauses Node input, ticks and drive until `take_applied` frees space. A failed persistent update poisons the node.

## Peer sessions

The lower node ID dials. Both sides exchange Hello/HelloAck after TLS. Bind both random challenges, identities and incarnations into a session digest. A 128-byte header carries version/length, cluster/group, from/to, incarnation, challenge and contiguous sequence. Accept only the next sequence of the active session. Validate protobuf bounds, uniqueness, entry count and lengths before decoding. ConfChangeV2 follows the membership contract, and snapshots use the separate closure transfer.

Validate CA, CRL, server name, pinned leaf digest and unique node URI. Disable early data and resumption. Revalidate trust before I/O after `replace_trust`. Limit handshake traffic to 2 MiB and enforce absolute handshake, partial-frame and idle deadlines. A new connection cannot replace a live peer by choosing a different incarnation. A disconnect discards its incomplete frame and reports the peer unreachable. The application retains committed results. Byte, frame, accept, tick and Ready budgets bound each network turn.

## Snapshot storage and transport

A `TWRS0001` manifest binds group, term/index, membership, application schema/root and sorted, unique object references. Check object lengths and SHA-256. A GenerationStore installation synchronizes and reopens the seed WAL, atomically replaces CURRENT and syncs the parent directory before installing storage in memory. Recover the complete generation selected by CURRENT, restore the application snapshot, then scan the actual applied tail. The installation receipt binds owner, node, generation, cut and root. A new owner invalidates the previous owner's receipt.

CURRENT v2 names the current and previous complete generations. GC retains them and live read/WAL/staging leases. An older v1 CURRENT conservatively preserves the unknown predecessor. At 16 generations, collect unused staging and older generations before reserving space. A per-directory mutex covers capacity, directory creation, lease registration, CURRENT selection and WAL opening. A separate lease permits one active Node. Tests cover seven object/manifest and 19 WAL/CURRENT failure points, 40 interrupted installations and concurrent GC.

Transfer Begin/Chunk/End over mTLS in manifest order. Check offsets on every chunk (up to 64 KiB). Allow one inbound closure per node and one outbound per peer, a 300-second deadline and a 16 MiB bound on an object read. Disconnect restarts transfer into fresh staging. Validate the isolated candidate, sync and reopen the seed WAL, switch CURRENT, verify the private installation receipt, then install the application. Send SnapshotApplied only when the single replica scanner reaches the cut. Synchronous disk and closure verification costs are independent of the network-frame budget. A three-node test transfers and recovers an object of 131,073 bytes.

## Membership and operation

`TWMC0001` is an 88-byte change with an operation ID, expected membership digest and AddLearner, ReplaceVoter or LeaveJoint arguments. Deduplicate by operation ID before checking the membership ETag. An exact retry returns the original term/index and ConfState. Promote a learner only when an authenticated AppendResponse and the actual raft match index both cover the committed cut. Sync ConfState, operation result and applied index before releasing messages.

`TWRS0002` adds membership history, original voters, tombstones and stable outcomes to the snapshot. Replay changes from the three original voters and compare each result. V1 keeps the fixed baseline. A group retains up to 16 node IDs and 64 membership operations. A retired identity stays retired. A joining node accepts authenticated replication before membership, then obeys the applied ConfState. A joining snapshot must establish the sender as a voter and include the receiving identity at that cut.

Run `tickwire-control-peer --config /absolute/node.json`. It pins and validates configuration FDs, private credentials, CRLs and certificates before networking. Attempt local compaction every 256 newly applied entries. `ApiOwner` drives HTTP and Raft through the same owner. A ReadIndex ticket carries context, term and a private lease. Dropping or canceling it revokes the request. Outstanding underlying work continues to count toward the limit of 1,024 until its ReadState arrives or the term changes. See the [peer executable](../../crates/tickwire-control-peer/README.md).
