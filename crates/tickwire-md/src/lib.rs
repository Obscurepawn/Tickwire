//! Stateless shared-memory market-data publisher (`tickwire-md`).
//!
//! Terminates public venue WebSocket and HTTPS depth/event streams, maintains bounded
//! in-memory JOIN request idempotency ([`BoundedRequestStore`]) and in-memory public
//! rate-limit accounting ([`quota::PublicQuota`]) with zero local persistence, and
//! enforces optional hot `reactor_cpu` vs. cold `housekeeping_cpus` (`md-cold`) thread
//! affinity isolation via [`tickwire_control_sys`].
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

#[forbid(unsafe_code)]
mod catalogs;
#[forbid(unsafe_code)]
pub mod config;
#[forbid(unsafe_code)]
pub mod engine;
#[forbid(unsafe_code)]
pub mod expansion;
#[forbid(unsafe_code)]
pub mod quota;
#[forbid(unsafe_code)]
pub mod request_store;
pub mod server;
#[forbid(unsafe_code)]
pub mod spot_market;
pub mod spot_quotes;

pub use request_store::{BoundedRequestStore, DEFAULT_MAX_JOIN_REQUESTS};
pub use tickwire_control_sys::CpuSet;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

pub fn invalid(text: &str) -> Error {
    std::io::Error::other(text.to_owned()).into()
}
