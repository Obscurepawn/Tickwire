# Interpretation

The order send transition uses the account's last observed private state. A remote socket may have disconnected before the local owner notices. Similarly, a consumed quota token records a local attempt and may precede a zero-byte expiry. The composition preserves both distinctions. Additional order types, positive/negative fees, quota resets, other authenticated account modes and exchange acceptance after ambiguous output need separate bounds.
