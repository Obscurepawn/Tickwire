//! Binance protocol encoding, authentication and response classification.
#![forbid(unsafe_code)]

pub mod auth;
mod buffer;
pub mod decimal;
pub mod depth_sync;
pub mod events;
pub mod feed;
pub mod market;
pub mod params;
pub mod request;
pub mod response;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Capacity,
    InvalidParameter,
    DuplicateParameter,
    InvalidDecimal,
    Overflow,
    InvalidKey,
    UnsupportedAlgorithm,
    SigningFailed,
    InvalidResponse,
    UnsupportedCommand,
    IdentityMismatch,
}
pub type Result<T> = core::result::Result<T, Error>;
