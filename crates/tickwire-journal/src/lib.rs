//! WAL v1 physical framing and synchronized local batches.
#![forbid(unsafe_code)]

use std::io;

pub mod frame;
mod journal;
pub mod store;
pub use journal::*;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Quarantined(&'static str),
    Poisoned,
}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
