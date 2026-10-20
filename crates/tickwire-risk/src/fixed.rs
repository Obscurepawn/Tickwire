use crate::{Result, RiskError};
use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    Floor,
    Ceil,
}

pub fn floor_div(n: i128, d: i128) -> Result<i128> {
    divide(n, d, Rounding::Floor)
}
pub fn ceil_div(n: i128, d: i128) -> Result<i128> {
    divide(n, d, Rounding::Ceil)
}
fn divide(n: i128, d: i128, rounding: Rounding) -> Result<i128> {
    if d == 0 {
        return Err(RiskError::DivisionByZero);
    }
    let q = n.checked_div(d).ok_or(RiskError::ArithmeticOverflow)?;
    let rem = n.checked_rem(d).ok_or(RiskError::ArithmeticOverflow)?;
    if rem == 0 {
        return Ok(q);
    }
    match (rounding, (n < 0) == (d < 0)) {
        (Rounding::Floor, false) => q.checked_sub(1).ok_or(RiskError::ArithmeticOverflow),
        (Rounding::Ceil, true) => q.checked_add(1).ok_or(RiskError::ArithmeticOverflow),
        _ => Ok(q),
    }
}
fn power(scale: u8) -> Result<i128> {
    if scale > 38 {
        return Err(RiskError::InvalidScale);
    }
    10_i128
        .checked_pow(u32::from(scale))
        .ok_or(RiskError::ArithmeticOverflow)
}

/// Decimal value whose scale is explicit. Addition never silently converts units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fixed {
    atoms: i128,
    scale: u8,
}
impl Fixed {
    pub fn from_atoms(atoms: i128, scale: u8) -> Result<Self> {
        power(scale)?;
        Ok(Self { atoms, scale })
    }
    pub const fn atoms(self) -> i128 {
        self.atoms
    }
    pub const fn scale(self) -> u8 {
        self.scale
    }
    pub const fn zero_like(self) -> Self {
        Self {
            atoms: 0,
            scale: self.scale,
        }
    }
    pub fn zero(scale: u8) -> Result<Self> {
        Self::from_atoms(0, scale)
    }
    pub fn one(scale: u8) -> Result<Self> {
        Self::from_atoms(power(scale)?, scale)
    }
    pub fn parse(text: &str, scale: u8) -> Result<Self> {
        power(scale)?;
        if text.is_empty() || text.len() > 128 {
            return Err(RiskError::InvalidDecimal);
        }
        let (negative, body) = match text.as_bytes()[0] {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let mut split = body.split('.');
        let whole = split.next().ok_or(RiskError::InvalidDecimal)?;
        let fractional = split.next();
        if whole.is_empty() || !whole.bytes().all(|c| c.is_ascii_digit()) || split.next().is_some()
        {
            return Err(RiskError::InvalidDecimal);
        }
        let fraction = fractional.unwrap_or("");
        if fractional.is_some() && fraction.is_empty()
            || !fraction.bytes().all(|c| c.is_ascii_digit())
        {
            return Err(RiskError::InvalidDecimal);
        }
        if fraction.bytes().skip(usize::from(scale)).any(|c| c != b'0') {
            return Err(RiskError::InvalidDecimal);
        }
        let mut atoms = 0_i128;
        for digit in whole
            .bytes()
            .chain(fraction.bytes().take(usize::from(scale)))
        {
            atoms = atoms.checked_mul(10).ok_or(RiskError::ArithmeticOverflow)?;
            atoms = if negative {
                atoms.checked_sub(i128::from(digit - b'0'))
            } else {
                atoms.checked_add(i128::from(digit - b'0'))
            }
            .ok_or(RiskError::ArithmeticOverflow)?;
        }
        for _ in fraction.len().min(usize::from(scale))..usize::from(scale) {
            atoms = atoms.checked_mul(10).ok_or(RiskError::ArithmeticOverflow)?;
        }
        Self::from_atoms(atoms, scale)
    }
    pub fn ensure_same_scale(self, rhs: Self) -> Result<()> {
        if self.scale == rhs.scale {
            Ok(())
        } else {
            Err(RiskError::ScaleMismatch)
        }
    }
    pub fn nonnegative(self) -> Result<Self> {
        if self.atoms >= 0 {
            Ok(self)
        } else {
            Err(RiskError::InvalidInput)
        }
    }
    pub fn positive(self) -> Result<Self> {
        if self.atoms > 0 {
            Ok(self)
        } else {
            Err(RiskError::InvalidInput)
        }
    }
    pub fn checked_add(self, rhs: Self) -> Result<Self> {
        self.ensure_same_scale(rhs)?;
        Self::from_atoms(
            self.atoms
                .checked_add(rhs.atoms)
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
    pub fn checked_sub(self, rhs: Self) -> Result<Self> {
        self.ensure_same_scale(rhs)?;
        Self::from_atoms(
            self.atoms
                .checked_sub(rhs.atoms)
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
    pub fn checked_neg(self) -> Result<Self> {
        Self::from_atoms(
            self.atoms
                .checked_neg()
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
    pub fn checked_abs(self) -> Result<Self> {
        Self::from_atoms(
            self.atoms
                .checked_abs()
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
    pub fn checked_mul_int(self, rhs: i128) -> Result<Self> {
        Self::from_atoms(
            self.atoms
                .checked_mul(rhs)
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
    pub fn max(self, rhs: Self) -> Result<Self> {
        self.ensure_same_scale(rhs)?;
        Ok(if self.atoms >= rhs.atoms { self } else { rhs })
    }
    pub fn min(self, rhs: Self) -> Result<Self> {
        self.ensure_same_scale(rhs)?;
        Ok(if self.atoms <= rhs.atoms { self } else { rhs })
    }
    pub fn positive_part(self) -> Self {
        Self {
            atoms: self.atoms.max(0),
            scale: self.scale,
        }
    }
    pub fn rescale(self, scale: u8, rounding: Rounding) -> Result<Self> {
        power(scale)?;
        let atoms = if scale >= self.scale {
            self.atoms
                .checked_mul(power(scale - self.scale)?)
                .ok_or(RiskError::ArithmeticOverflow)?
        } else {
            divide(self.atoms, power(self.scale - scale)?, rounding)?
        };
        Self::from_atoms(atoms, scale)
    }
    pub fn mul_to(self, rhs: Self, scale: u8, rounding: Rounding) -> Result<Self> {
        product_to([self, rhs], scale, rounding)
    }
    pub fn div_to(self, rhs: Self, scale: u8, rounding: Rounding) -> Result<Self> {
        power(scale)?;
        if rhs.atoms == 0 {
            return Err(RiskError::DivisionByZero);
        }
        let exponent = i16::from(rhs.scale) + i16::from(scale) - i16::from(self.scale);
        let (mut numerator, mut denominator) = (self.atoms, rhs.atoms);
        if exponent >= 0 {
            let mut exponent = exponent as u16;
            while exponent > 0 && denominator % 10 == 0 {
                denominator /= 10;
                exponent -= 1;
            }
            for _ in 0..exponent {
                numerator = numerator
                    .checked_mul(10)
                    .ok_or(RiskError::ArithmeticOverflow)?;
            }
        } else {
            let mut exponent = (-exponent) as u16;
            while exponent > 0 && numerator % 10 == 0 {
                numerator /= 10;
                exponent -= 1;
            }
            for _ in 0..exponent {
                denominator = denominator
                    .checked_mul(10)
                    .ok_or(RiskError::ArithmeticOverflow)?;
            }
        }
        Self::from_atoms(divide(numerator, denominator, rounding)?, scale)
    }
    /// Quantize to an exchange tick/step, preserving this value's scale.
    pub fn quantize(self, quantum: Self, rounding: Rounding) -> Result<Self> {
        self.ensure_same_scale(quantum)?;
        quantum.positive()?;
        let ticks = divide(self.atoms, quantum.atoms, rounding)?;
        Self::from_atoms(
            ticks
                .checked_mul(quantum.atoms)
                .ok_or(RiskError::ArithmeticOverflow)?,
            self.scale,
        )
    }
}

/// Multiply all factors, cancelling decimal powers before checked multiplication;
/// round once in the destination unit. The fixed array does not allocate.
pub fn product_to<const N: usize>(
    factors: [Fixed; N],
    scale: u8,
    rounding: Rounding,
) -> Result<Fixed> {
    power(scale)?;
    if factors.iter().any(|x| x.atoms == 0) {
        return Fixed::zero(scale);
    }
    let source_scale: usize = factors.iter().map(|x| usize::from(x.scale)).sum();
    let mut denominator_exponent = source_scale.saturating_sub(usize::from(scale));
    let mut product = 1_i128;
    for factor in factors {
        let mut value = factor.atoms;
        while denominator_exponent > 0 && value % 10 == 0 {
            value /= 10;
            denominator_exponent -= 1;
        }
        product = product
            .checked_mul(value)
            .ok_or(RiskError::ArithmeticOverflow)?;
    }
    if source_scale < usize::from(scale) {
        for _ in source_scale..usize::from(scale) {
            product = product
                .checked_mul(10)
                .ok_or(RiskError::ArithmeticOverflow)?;
        }
    } else if denominator_exponent > 38 {
        product = match (product < 0, rounding) {
            (true, Rounding::Floor) => -1,
            (false, Rounding::Ceil) => 1,
            _ => 0,
        };
    } else {
        product = divide(product, power(denominator_exponent as u8)?, rounding)?;
    }
    Fixed::from_atoms(product, scale)
}
impl fmt::Display for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let magnitude = self.atoms.unsigned_abs();
        if self.atoms < 0 {
            f.write_str("-")?;
        }
        if self.scale == 0 {
            return write!(f, "{magnitude}");
        }
        let divisor = 10_u128.pow(u32::from(self.scale));
        write!(
            f,
            "{}.{:0width$}",
            magnitude / divisor,
            magnitude % divisor,
            width = usize::from(self.scale)
        )
    }
}

pub(crate) fn sum(values: impl IntoIterator<Item = Fixed>, scale: u8) -> Result<Fixed> {
    values
        .into_iter()
        .try_fold(Fixed::zero(scale)?, Fixed::checked_add)
}
