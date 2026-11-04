# Retiring a strategy subscription

The host sends an authenticated report QUIESCE and waits for the worker to detach from market data, release its report cursor and exit. A timeout keeps the successor inactive.

The subscriber's DETACH (21) carries its full JOIN identity. MD issues QUIESCE (20), and `JoinOwner::quiesced` verifies the real cursor. MD returns STATUS (49, Response|Applied) bound to the original request, control epoch, membership, ring and SHA-256 of the full QUIESCED body. Only after validating every field does the worker send report QUIESCED to its host. `detach_registered` also checks the actual report mapping and the host waits on the child it owns.

An orderly Detached peer can leave without invalidating other readers of the topic. Unexpected EOF, incomplete exit, timeout and forged messages follow the fault path. The multi-strategy scheduler stops invoking the retired member and retains its identity in bounded history.
