# Numerical and interface notes

Venue server timestamps arrive in milliseconds, local clocks sample in nanoseconds, and signed payloads quantize to milliseconds or microseconds. Rescaling `1 s` to a tiny toy integer would distort outward rounding and strict future bounds, so L0 preserves exact `10^9 ns` scale via 10-limb base-1000 integers verified against Python test vectors.

Stale sample tokens are rejected before observing monotonic time so delayed stale replies cannot fault a newer calibration request, whereas active tokens observe monotonic time before consuming `pending`. `check_send` retains the original quantized timestamp `T` recorded at `sign_time` and validates it against the latest refreshed interval without mutating `T`.
