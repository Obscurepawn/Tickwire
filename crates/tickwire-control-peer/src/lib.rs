//! Bounded authenticated peer transport for the control consensus owner.
#![forbid(unsafe_code)]
pub mod frame;
pub mod tls;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Protocol,
    Identity,
    Authentication,
    Transport,
    Timeout,
    Closed,
    Poisoned,
}
pub type Result<T> = core::result::Result<T, Error>;

pub mod runner;
pub mod service;

mod lane;

pub mod api_runner;
pub mod config_runner;
mod snapshot_transfer;
