//! Fixed-schema Binance Spot market-data decoding without dynamic reflection.
#![forbid(unsafe_code)]
mod decimal;
mod decode;
mod normalize;
pub mod schema;
pub use decimal::{DecimalUnit, decimal_to_units};
pub use decode::{Frame, Kind, LevelView, Levels, Limits, Quote, Trade, Trades, decode_frame};
pub use normalize::{
    Coverage, DepthKind, DepthUpdate, ExpectedInstrument, Identity, Level, normalize_depth,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Capacity,
    Schema,
    Version,
    Template,
    BlockLength,
    Value,
    Utf8,
    Identity,
    NotIntegral,
    Overflow,
    Kind,
}
impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = core::result::Result<T, Error>;
