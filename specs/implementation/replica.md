# Replicating a durable prefix

An optional replica consumes records exported from a synchronized physical log. Export binds the original payload, sequence and writer epoch. Pin commit mode, configuration revision and source identity to the stream. Persist per-sequence metadata, including the selected input prefix and closure root, before publishing the local frontier. Recovery reuses the original metadata.

The mirror synchronizes the referenced objects and complete recovery closure, then the contiguous batch, then publishes the prefix and issues its ACK. Bind the ACK to stream, sequence, chain digest, dependency digest, mirror identity, boot generation and assignment. A gap, conflict or failed sync leaves the confirmed frontier unchanged. A new mirror boot requires a new acknowledgement. Fixed capacities bound source tasks, results and object assembly.

Retain the raw input and writer epoch at each input sequence. Check overlap for conflicting bytes and reject generation regression. Compare handover inputs with the full preceding account WAL. A SourceRecoverySeal requires current fencing evidence, two closed writers and all dependencies, including inputs persisted ahead of the account journal. An asynchronous stream cannot generate a synchronous recovery seal.

For SyncReplica recovery, close the old mirror ingress. Mark the recovery directory in progress before writing data. On restart, verify the completion manifest and target identity, then supply the actual input and account prefixes to frozen recovery. An empty logical account prefix permits an absent account batch seal. A missing completion record requires investigation before reopening the directory. TLS uses mutual authentication. A durable mirror receipt confirms storage. The execution grant and external fencing proof have separate issuers.

For a declared rebuild, verify both installation and independent rollback roots in the v4 HandoverSeal. Re-read the complete closures, check operation/member and from/to manager against risk domain and generations, and prove continuity from the warm window through the retained tail. Publish the mirror frontier only after those checks pass.
