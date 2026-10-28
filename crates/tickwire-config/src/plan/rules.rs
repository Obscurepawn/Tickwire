//! Source identity of the compiled deterministic planning rules.
use crate::Hash;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
const SOURCES: &[(&str, &[u8])] = &[
    (
        ".cargo/config.toml",
        include_bytes!("../../../../.cargo/config.toml"),
    ),
    ("Cargo.lock", include_bytes!("../../../../Cargo.lock")),
    ("Cargo.toml", include_bytes!("../../../../Cargo.toml")),
    (
        "crates/tickwire-config/Cargo.toml",
        include_bytes!("../../Cargo.toml"),
    ),
    (
        "crates/tickwire-config/src/artifacts.rs",
        include_bytes!("../artifacts.rs"),
    ),
    (
        "crates/tickwire-config/src/changes.rs",
        include_bytes!("../changes.rs"),
    ),
    (
        "crates/tickwire-config/src/delivery.rs",
        include_bytes!("../delivery.rs"),
    ),
    (
        "crates/tickwire-config/src/lib.rs",
        include_bytes!("../lib.rs"),
    ),
    (
        "crates/tickwire-config/src/patch.rs",
        include_bytes!("../patch.rs"),
    ),
    (
        "crates/tickwire-config/src/plan.rs",
        include_bytes!("../plan.rs"),
    ),
    (
        "crates/tickwire-config/src/plan/dependencies.rs",
        include_bytes!("dependencies.rs"),
    ),
    (
        "crates/tickwire-config/src/plan/encoded.rs",
        include_bytes!("encoded.rs"),
    ),
    (
        "crates/tickwire-config/src/plan/rules.rs",
        include_bytes!("rules.rs"),
    ),
    (
        "crates/tickwire-config/src/prepared.rs",
        include_bytes!("../prepared.rs"),
    ),
    (
        "crates/tickwire-config/src/projection.rs",
        include_bytes!("../projection.rs"),
    ),
    (
        "crates/tickwire-config/src/strict.rs",
        include_bytes!("../strict.rs"),
    ),
    (
        "crates/tickwire-config/src/validation.rs",
        include_bytes!("../validation.rs"),
    ),
    (
        "rust-toolchain.toml",
        include_bytes!("../../../../rust-toolchain.toml"),
    ),
    (
        "specs/contracts/control-api.openapi.json",
        include_bytes!("../../../../specs/contracts/control-api.openapi.json"),
    ),
    (
        "specs/contracts/system-config.schema.json",
        include_bytes!("../../../../specs/contracts/system-config.schema.json"),
    ),
];
/// Exact source identity, separate from existing plan content and its digest.
/// It is not a runtime build attestation or a configuration authorization.
pub fn rules_digest() -> Hash {
    static DIGEST: OnceLock<Hash> = OnceLock::new();
    *DIGEST.get_or_init(|| {
        let mut hash = Sha256::new();
        hash.update(b"tickwire.config.planning-rules.v1\0");
        hash.update((SOURCES.len() as u64).to_le_bytes());
        for (path, bytes) in SOURCES {
            hash.update((path.len() as u64).to_le_bytes());
            hash.update(path.as_bytes());
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        hash.finalize().into()
    })
}
