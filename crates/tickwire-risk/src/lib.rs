//! Checked fixed-point risk calculations. No I/O, floating point, or exchange dependency.
#![forbid(unsafe_code)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskError {
    ArithmeticOverflow,
    InvalidDecimal,
    InvalidScale,
    ScaleMismatch,
    DivisionByZero,
    InvalidInput,
    AssetMismatch,
    InsufficientAsset,
    InsufficientCollateral,
    VenueAvailableCap,
    UnknownExposure,
    BracketSemanticsUnverified,
    InvalidBrackets,
    Unreconciled,
    DuplicateConflict,
    GateClosed,
}
impl RiskError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ArithmeticOverflow => "ARITHMETIC_OVERFLOW",
            Self::InvalidDecimal => "INVALID_DECIMAL",
            Self::InvalidScale => "INVALID_SCALE",
            Self::ScaleMismatch => "SCALE_MISMATCH",
            Self::DivisionByZero => "DIVISION_BY_ZERO",
            Self::InvalidInput => "INVALID_INPUT",
            Self::AssetMismatch => "ASSET_MISMATCH",
            Self::InsufficientAsset => "INSUFFICIENT_ASSET",
            Self::InsufficientCollateral => "INSUFFICIENT_COLLATERAL",
            Self::VenueAvailableCap => "VENUE_AVAILABLE_CAP",
            Self::UnknownExposure => "UNKNOWN_EXPOSURE",
            Self::BracketSemanticsUnverified => "BRACKET_SEMANTICS_UNVERIFIED",
            Self::InvalidBrackets => "INVALID_BRACKETS",
            Self::Unreconciled => "UNRECONCILED",
            Self::DuplicateConflict => "DUPLICATE_CONFLICT",
            Self::GateClosed => "GATE_CLOSED",
        }
    }
}
impl core::fmt::Display for RiskError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for RiskError {}
pub type Result<T> = core::result::Result<T, RiskError>;
pub mod fixed;
pub mod futures;
pub mod reservation;
pub mod settlement;
pub mod spot;
pub use fixed::{Fixed, Rounding};
