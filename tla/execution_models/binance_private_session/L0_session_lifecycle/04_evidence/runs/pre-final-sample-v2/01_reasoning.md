# L0 modeling rationale (archived `v1`)

Visible states correspond to synchronous `&mut PrivateClient` boundaries, with a dedicated `Gate` stage for synchronous first-accept callbacks (permitting grant, rejection, `ClockDomain` error, or `WouldBlock` without re-entering other owner APIs). Time jumps combine with `poll` expiration checks so wall-clock passage never mutates status outside `poll`.

Byte acceptance is abstracted to `{0, 1, 2}` (`NoBytes`, partial, complete), and request history retains acceptance level, first-accept time, deadline, and outcome. `Clock` adapter interval checks are modeled via their call contract while interval arithmetic belongs to the `clock` ladder.

Authentication mode is fixed as Spot signed, Spot logon, or USD-M `listenKey`. Simultaneous API reply and `EOF` revokes `Receiving` before returning; buffered events retain their original epoch and drain completely before reconnect. USD-M lease validity is anchored to confirmed request start time, explicit `DELETE` disables auto-reconnect and latches `remote_close_unknown` on ambiguous disconnect, and `SinkFailure` transitions permanently to `Stopped`.
