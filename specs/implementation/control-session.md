# Authenticated control sessions

The launcher provides the registered instances, generations, roles, logical strategies, boot and namespace IDs, architecture, trust profile and release digest in `Policy`. The kernel supplies PID/UID/GID. The launcher keeps an unreaped child handle and assigns a fresh epoch and nonce to each session.

The server checks kernel identity before accepting HELLO, then verifies registration, ABI 1.0, page/isolation/atomic widths and required features. Bits 2, 3 and 4 are required for the current profile, with bit 5 for the config-control role. WELCOME lists the supported intersection. The client checks the kernel peer, request, both instances, epoch, boot, role, trust, nonce, features and deadline. An established session accepts frames with its identity and epoch, a matching request, negotiated features, a 128-byte envelope and CRC. The maximum frame is 65,536 bytes.

Use CLOCK_MONOTONIC in the same Linux time namespace and host boot. A backward clock or `now >= deadline` closes the instance permanently. Renew with the matching nonce and a higher revision below 2^63. Deadline overflow leaves the previous valid deadline unchanged. A receiver checks freshness when handling encoded bytes.

## Request identity and completion

`Session::admit` looks up the authenticated subject, epoch and request ID. Its SHA-256 binds opcode, operation/revision, expected version and body. An exact repeat returns Pending or the exact stored terminal response. Conflicting bytes fail. Compare expected owner version on first admission.

`RequestStore` supplies lookup, atomic insert-if-absent and immutable completion. Issue one non-cloneable ExecutionTicket after the first Received record. Match completion against both identities, request, operation, revision, expected version and response opcode. StatusBody state_version equals the stored version. Repeating the same completion succeeds. Conflicting completion fails.

The in-memory test store caps pending records at 256 and has a configurable history capacity. A production owner needs durable receipt and economic-operation recovery across restart. After a lost ticket, query the actual owner state. Tests cover negotiation, storage failure, duplicate and conflicting requests, expiry and completion correlation. The [control request models](../../tla/execution_models/control_requests/PLAN.md) cover serial registration and session expiry.
