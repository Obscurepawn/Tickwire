//! WAL v1 physical framing and synchronized local batches.
#![deny(unsafe_code)]

use std::io;

#[allow(unsafe_code)]
pub mod crc32c;
#[forbid(unsafe_code)]
pub mod frame;
#[forbid(unsafe_code)]
mod journal;
#[forbid(unsafe_code)]
pub mod store;
pub use journal::*;
pub use store::StorageMode;

#[forbid(unsafe_code)]
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Quarantined(&'static str),
    Poisoned,
}
#[forbid(unsafe_code)]
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
#[forbid(unsafe_code)]
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
#[forbid(unsafe_code)]
impl std::error::Error for Error {}
