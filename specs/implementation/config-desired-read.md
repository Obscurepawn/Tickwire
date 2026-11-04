# Reading the committed configuration

`tickwire-config-service::request` reads the configuration head through the replica that owns the Raft node. `begin_request` opens a real ReadIndex ticket. `poll_request` waits until a majority has established the barrier and the business reducer has scanned through it, then revalidates the TLS principal and permissions in the current `CommittedHead`. [Request implementation](../../crates/tickwire-config-service/src/request.rs)

| Resource | Scope and result |
| --- | --- |
| `head` | An authorized account-scoped reader can see the revision, digest and ETag. The response omits the configuration body. |
| `desired` | Requires cluster read access. Returns the current canonical configuration. |
| `revisions/<revision>` | Requires cluster read access and a successful safe read. The current revision returns its configuration. An unknown revision returns 404 `NotFound`. |

The HTTP ETag equals the body's ETag on all three successful responses. Maximum body size is 16 MiB plus 4,096 bytes of envelope. The implemented committed head comes from signed bootstrap. `BootstrapHead` and `BootstrapDesired` record `origin=bootstrap` and the actual `raft:<term>:<index>`. There is no operation ID or commit UTC for this origin. The OpenAPI union reserves a separate branch for ordinary commits. [Control API](../contracts/control-api.openapi.json)

Without a trusted head the service returns 503 `ControlUnavailable`. Unimplemented plan, operation, effective and watch read views return 503 `UnsupportedEndpoint`. The distinction matters to clients deciding whether an authorized resource was absent or the service cannot answer the question. Three-voter, file-backed mTLS tests covered loss and restoration of quorum, response bytes, authorization, certificate revocation and unknown revisions. [Control replica](config-control-state.md)
