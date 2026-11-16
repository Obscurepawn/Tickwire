# L0 state semantics and boundaries

Tracks every physical slot of one 64-slot ring across claim, per-slot write, publication, reader borrow, out-of-order release/apply completion, and slot reuse. `published` marks the complete-payload frontier; `read` marks borrowed slots; `released` marks the contiguous prefix whose borrow guards have dropped; and `applied` marks the contiguous application prefix. Only `released` participates in publisher slot reclamation, while the reader borrow window bounds `read - min(released, applied)`.

`p` stores publisher lifecycle, active `claim`, `Freeze` plan, and cached minimum watermark. `m[r]` stores member state (`pending` / `active` / `detached`), JOIN baseline, and publisher-observed `released`. `rdr[r]` stores the subscriber's completion bitmap, live borrows, shared cursor `lifecycle`, `quiesced_seq`, and issued quiescence evidence. `payload` stores the sequence tag currently written in each of the 64 physical slots so invariants verify actual slot contents under live borrows rather than watermark arithmetic alone.

Publisher mutations serialize via `&mut self`. While a `claim` is open, subscribers may continue borrowing already-published slots and dropping guards. `Observe` (`Producer::observe_release`) is scheduled outside an open `claim` matching the mapped publisher's refresh-before-claim flow; deferring observation until after `publish` / `abort` is strictly more conservative and never weakens protection of live borrows.

## Configurations

| Config | Initial state | Bounds | Target scenarios |
| --- | --- | --- | --- |
| `primary` | Created & active, `published = 0` | 1 reader, window 2, `MaxSeq = 3` | JOIN, out-of-order release/apply, `Freeze`, and `detach` |
| `two_readers` | Created & active, `published = 0` | 2 readers, window 1, `MaxSeq = 2` | Concurrent member join, observation, and detach |
| `wrap` | `published = 64`, 1 active reader at baseline `0` | 1 reader, window 2, advance 2, `MaxSeq = 66` | Full ring, cache refresh, and sequences `65`/`66` reusing physical slots `0`/`1` |
| `pending_wrap` | `published = 64`, 1 pending reader at baseline `0` | Same as `wrap` | Unregistered pending JOIN cursor still blocks slot reuse |
| `wrap_two` | `published = 64`, 2 active readers at baseline `0` | 2 readers, window 1, advance 1, `MaxSeq = 65` | Slot `0` reuse blocked until both readers release sequence `1` |

All five configurations keep physical `Capacity = 64`. The three `wrap` configurations start from the exact state reached by creating and activating a 64-slot ring, joining 1 or 2 readers at baseline `0`, claiming and publishing batch `1..64` (`payload[i] = i + 1`, `cache = 0`, zero released records), and use `SuffixNext` for post-prefix transitions.

`Detach` enumerates both genuine cursor quiescence tokens and a forged pure-`Reader` token at the JOIN baseline; `mapped.rs` requires the registered shared cursor itself to report `lifecycle == 5`, matching watermarks, and `quiesced_seq == read`, witnessed by `RejectForged`.
