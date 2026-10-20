use crate::{Error, Result, buffer::atomic};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}
impl Decimal {
    pub fn new(mantissa: i128, scale: u8) -> Result<Self> {
        if scale > 38 {
            return Err(Error::InvalidDecimal);
        }
        Ok(Self { mantissa, scale })
    }
    pub fn parse(text: &str) -> Result<Self> {
        if text.is_empty() || text.len() > 80 {
            return Err(Error::InvalidDecimal);
        }
        let (negative, digits) = match text.strip_prefix('-') {
            Some(v) => (true, v),
            None => (false, text),
        };
        let mut value = 0u128;
        let mut scale = None;
        let mut count = 0;
        for b in digits.bytes() {
            if b == b'.' && count > 0 && scale.is_none() {
                scale = Some(0u8);
            } else if b.is_ascii_digit() {
                value = value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((b - b'0') as u128))
                    .ok_or(Error::Overflow)?;
                count += 1;
                if let Some(s) = &mut scale {
                    *s = s.checked_add(1).ok_or(Error::InvalidDecimal)?;
                }
            } else {
                return Err(Error::InvalidDecimal);
            }
        }
        if count == 0 || scale == Some(0) {
            return Err(Error::InvalidDecimal);
        }
        let mantissa = if negative {
            if value == i128::MAX as u128 + 1 {
                i128::MIN
            } else {
                -i128::try_from(value).map_err(|_| Error::Overflow)?
            }
        } else {
            i128::try_from(value).map_err(|_| Error::Overflow)?
        };
        Self::new(mantissa, scale.unwrap_or(0))
    }
    pub fn mantissa(self) -> i128 {
        self.mantissa
    }
    pub fn scale(self) -> u8 {
        self.scale
    }
    pub fn write(self, out: &mut [u8]) -> Result<usize> {
        let mut digits = [0u8; 39];
        let mut n = self.mantissa.unsigned_abs();
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        let significant = &digits[start..];
        let scale = self.scale as usize;
        atomic(out, |s| {
            if self.mantissa < 0 {
                s.byte(b'-')?;
            }
            if scale == 0 {
                return s.bytes(significant);
            }
            if scale >= significant.len() {
                s.bytes(b"0.")?;
                for _ in 0..scale - significant.len() {
                    s.byte(b'0')?;
                }
                s.bytes(significant)
            } else {
                let split = significant.len() - scale;
                s.bytes(&significant[..split])?;
                s.byte(b'.')?;
                s.bytes(&significant[split..])
            }
        })
    }
    pub fn rescale_exact(self, scale: u8) -> Result<Self> {
        if scale > 38 {
            return Err(Error::InvalidDecimal);
        }
        let mantissa = if scale > self.scale {
            self.mantissa
                .checked_mul(10i128.pow((scale - self.scale) as u32))
                .ok_or(Error::Overflow)?
        } else {
            let divisor = 10i128.pow((self.scale - scale) as u32);
            if self.mantissa % divisor != 0 {
                return Err(Error::InvalidDecimal);
            }
            self.mantissa / divisor
        };
        Self::new(mantissa, scale)
    }
}

impl<'de> serde::Deserialize<'de> for Decimal {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Decimal;
            fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str("an exact decimal string")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> core::result::Result<Decimal, E> {
                Decimal::parse(v).map_err(|_| E::custom("invalid exact decimal"))
            }
        }
        d.deserialize_str(Visitor)
    }
}
