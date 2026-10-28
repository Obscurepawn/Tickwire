//! Ordered prefix replication with complete local closure verification.
#![forbid(unsafe_code)]
mod association;
mod cbor;
mod handover;
mod input;
mod mirror;
mod network;
mod protocol;
mod restore;
mod source;
#[cfg(test)]
mod tests;
mod tls;
mod worker;
pub use input::{INPUT_PREFIX_MAGIC, InputEntry, InputPrefix, MAX_INPUT_RECORDS};
pub mod config;
pub use mirror::*;
pub use network::*;
pub use protocol::*;
pub use source::*;
use std::io;
pub use tickwire_journal::{ClosedJournal, DurableExport};
pub use tickwire_recovery::{BlobStore, FileBlobStore, Hash, Persistence};
pub use tls::{ClientTls, ServerTls, TlsIdentity};
pub use worker::*;
#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Bounds,
    Gap { next: u64 },
    Missing,
    Conflict,
    Frozen,
    Signature,
    Io(io::Error),
    Journal(tickwire_journal::Error),
    Recovery(tickwire_recovery::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<tickwire_journal::Error> for Error {
    fn from(e: tickwire_journal::Error) -> Self {
        Self::Journal(e)
    }
}
impl From<tickwire_recovery::Error> for Error {
    fn from(e: tickwire_recovery::Error) -> Self {
        Self::Recovery(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
