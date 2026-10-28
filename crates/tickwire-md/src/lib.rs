//! Independent Binance market-data process and authenticated full-topic publisher.
#![deny(unsafe_code)]
pub mod config;
pub mod engine;
pub mod expansion;
#[allow(unsafe_code)]
pub mod server;
pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;
pub fn invalid(text: &str) -> Error {
    std::io::Error::other(text.to_owned()).into()
}
