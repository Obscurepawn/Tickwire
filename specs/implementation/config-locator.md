# Configuration locator

`tickwire-config::locator::Locator` reads the fixed `ConfigLocator` schema from JSON or TOML. Limit input to 16 KiB and eight nesting levels. Generation is a decimal string from 1 through `2^63-1`. UUIDs use lowercase canonical form. An active root has at most 4,096 bytes and no empty path component, dot, parent traversal, doubled slash or trailing slash.

Compute the checksum from RFC 8785 bytes with the checksum field omitted. `Locator::hash` covers the complete canonical object, including that checksum. Equivalent JSON and TOML have the same full hash. The first generation uses a zero predecessor. Later generations require a nonzero predecessor and a migration ID. The manifest digest must be nonzero.

`validate_successor` requires the same cluster, the next generation, the old locator's complete digest, a changed root and a migration ID different from the previous one. Bootstrap checks the migration journal, actual file inventory and previous writer before activating the new root.
