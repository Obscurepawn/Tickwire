//! File-configured executable assembly for the actual Binance runtime.
#![deny(unsafe_code)]
pub mod config;
#[allow(unsafe_code)]
pub mod hosted;
#[allow(unsafe_code)]
pub mod market;
pub mod reconciliation;
pub mod strategy;
pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn invalid(message: &str) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into()
}

pub mod control;
pub mod observation;

pub mod failover;
pub mod launch;
pub mod lifecycle;
pub mod replay;
pub mod replication;
