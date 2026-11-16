# L0 notes (archived `v1`)

41 native/Linux behavioral tests accompanied the private-stream network implementation and calibrated-clock integration. Independent model review separated permanent `SinkFailure` (`Stopped`) from reconnectable transport failures before recording the final `v1` runs; later cross-crate composition review added the post-callback monotonic sample verified in `v2`.
