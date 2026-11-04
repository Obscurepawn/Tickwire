# Durable configuration files on Linux

`tickwire-config-fs::Store` opens the private root relative to a trusted parent directory descriptor and acquires an exclusive kernel lock on `LOCK`. The parent belongs to root or the designated UID and grants write access to neither group nor other. Directories under the private root share a device and UID and have mode 0700. Files must be regular files with mode 0600, one hard link and at most 8 MiB. The descriptor-relative walk rejects symbolic links.

To install a version, compare SHA-256 against the existing bytes. Create a new temporary file exclusively in the target directory, write it, read it back, sync it, rename it into place, then sync the directory. Verify the inode, full contents, directory identity and lock identity before returning `DurableFile`. An exact repeat resynchronizes the file and directory. After a failure following rename, reopen the store and recover using the journal.

Recovery reads named files and ignores temporary files. Replacing a directory or lock, or changing the required permissions, invalidates the handle. Other platforms return `Unsupported`. The Linux fixture covers three unit and seven file integration cases, including injected write, sync, rename and directory-sync failures. The configuration materializer consumes the resulting receipt.
