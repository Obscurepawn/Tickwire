use crate::{Error, Result};

/// Exact positive unit `mantissa * 10^exponent` from the trusted instrument catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalUnit {
    mantissa: i128,
    exponent: i8,
}
impl DecimalUnit {
    pub fn new(mantissa: i128, exponent: i8) -> Result<Self> {
        if mantissa <= 0 || exponent == i8::MIN {
            return Err(Error::Value);
        }
        Ok(Self { mantissa, exponent })
    }
    pub fn mantissa(self) -> i128 {
        self.mantissa
    }
    pub fn exponent(self) -> i8 {
        self.exponent
    }
}
fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}
/// Return integral units without floating point or a rounded intermediate value.
/// Cancel common factors before powers of ten, including exponent differences >38.
pub fn decimal_to_units(mantissa: i64, exponent: i8, unit: DecimalUnit) -> Result<i64> {
    if exponent == i8::MIN || mantissa == i64::MIN {
        return Err(Error::Value);
    }
    if mantissa == 0 {
        return Ok(0);
    }
    let mut n = u128::from(mantissa.unsigned_abs());
    let mut d = unit.mantissa as u128;
    let common = gcd(n, d);
    n /= common;
    d /= common;
    let delta = i16::from(exponent) - i16::from(unit.exponent);
    if delta >= 0 {
        for _ in 0..delta {
            let factor = gcd(d, 10);
            d /= factor;
            n = n.checked_mul(10 / factor).ok_or(Error::Overflow)?;
            if n > i64::MAX as u128 {
                return Err(Error::Overflow);
            }
        }
        if d != 1 {
            return Err(Error::NotIntegral);
        }
    } else {
        if d != 1 {
            return Err(Error::NotIntegral);
        }
        for _ in delta..0 {
            if n % 10 != 0 {
                return Err(Error::NotIntegral);
            }
            n /= 10;
        }
    }
    let value = i64::try_from(n).map_err(|_| Error::Overflow)?;
    Ok(if mantissa < 0 { -value } else { value })
}
