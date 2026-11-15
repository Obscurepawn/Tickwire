//! Pure, socket-free Binance protocol codecs, cryptographic signing (`HmacSha256`, `RsaSha256`,
//! `Ed25519` DER/PEM), full-market wire encoders (`Spot`, `Usdm`, `Coinm`, `Options`), incremental
//! depth synchronization, and deterministic `TerminalReason` classification.
#![forbid(unsafe_code)]

pub mod auth;
mod buffer;
pub mod decimal;
pub mod depth_sync;
pub mod events;
pub use events::classify_order_update_reason;
pub mod feed;
pub mod full_market;
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
