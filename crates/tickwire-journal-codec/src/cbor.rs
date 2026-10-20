use crate::{DecodeError, MAX_ENCODED_EVENT_BYTES};

pub(crate) struct Writer(Vec<u8>);
impl Writer {
    pub(crate) fn new() -> Self {
        Self(Vec::with_capacity(MAX_ENCODED_EVENT_BYTES))
    }
    fn argument(&mut self, major: u8, value: u64) {
        let prefix = major << 5;
        match value {
            0..=23 => self.0.push(prefix | value as u8),
            24..=255 => self.0.extend_from_slice(&[prefix | 24, value as u8]),
            256..=65535 => {
                self.0.push(prefix | 25);
                self.0.extend_from_slice(&(value as u16).to_be_bytes());
            }
            65536..=0xffff_ffff => {
                self.0.push(prefix | 26);
                self.0.extend_from_slice(&(value as u32).to_be_bytes());
            }
            _ => {
                self.0.push(prefix | 27);
                self.0.extend_from_slice(&value.to_be_bytes());
            }
        }
    }
    pub(crate) fn uint(&mut self, value: u64) {
        self.argument(0, value);
    }
    pub(crate) fn int(&mut self, value: i128) {
        // Complement is -1-value for negatives, including i128::MIN.
        let magnitude = if value < 0 {
            !value as u128
        } else {
            value as u128
        };
        if let Ok(small) = u64::try_from(magnitude) {
            self.argument(u8::from(value < 0), small);
        } else {
            self.argument(6, if value < 0 { 3 } else { 2 });
            let bytes = magnitude.to_be_bytes();
            let start = (magnitude.leading_zeros() / 8) as usize;
            self.bytes(&bytes[start..]);
        }
    }
    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        self.argument(2, bytes.len() as u64);
        self.0.extend_from_slice(bytes);
    }
    pub(crate) fn text(&mut self, text: &str) {
        self.argument(3, text.len() as u64);
        self.0.extend_from_slice(text.as_bytes());
    }
    pub(crate) fn array(&mut self, len: u64) {
        self.argument(4, len);
    }
    pub(crate) fn map(&mut self, len: u64) {
        self.argument(5, len);
    }
    pub(crate) fn finish(self) -> Vec<u8> {
        self.0
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.offset.checked_add(len).ok_or(DecodeError::Truncated)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(DecodeError::Truncated)?;
        self.offset = end;
        Ok(result)
    }
    fn head(&mut self) -> Result<(u8, u64), DecodeError> {
        let head = self.take(1)?[0];
        let additional = head & 31;
        let (value, minimum) = match additional {
            0..=23 => return Ok((head >> 5, u64::from(additional))),
            24 => (u64::from(self.take(1)?[0]), 24),
            25 => (
                u64::from(u16::from_be_bytes(self.take(2)?.try_into().unwrap())),
                256,
            ),
            26 => (
                u64::from(u32::from_be_bytes(self.take(4)?.try_into().unwrap())),
                65536,
            ),
            27 => (
                u64::from_be_bytes(self.take(8)?.try_into().unwrap()),
                0x1_0000_0000,
            ),
            // Reserved arguments and indefinite lengths are not in this schema.
            _ => return Err(DecodeError::NonCanonical),
        };
        if value < minimum {
            return Err(DecodeError::NonCanonical);
        }
        Ok((head >> 5, value))
    }
    fn argument(&mut self, major: u8) -> Result<u64, DecodeError> {
        let (found, value) = self.head()?;
        if found != major {
            return Err(DecodeError::UnexpectedType);
        }
        Ok(value)
    }
    pub(crate) fn uint(&mut self) -> Result<u64, DecodeError> {
        self.argument(0)
    }
    pub(crate) fn key(&mut self, expected: u64) -> Result<(), DecodeError> {
        if self.uint()? != expected {
            return Err(DecodeError::Field);
        }
        Ok(())
    }
    pub(crate) fn map(&mut self, expected: u64) -> Result<(), DecodeError> {
        if self.argument(5)? != expected {
            return Err(DecodeError::Field);
        }
        Ok(())
    }
    pub(crate) fn bytes<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        if self.argument(2)? != N as u64 {
            return Err(DecodeError::Field);
        }
        Ok(self.take(N)?.try_into().unwrap())
    }
    pub(crate) fn int(&mut self) -> Result<i128, DecodeError> {
        let (major, value) = self.head()?;
        match (major, value) {
            (0, _) => Ok(i128::from(value)),
            (1, _) => Ok(-1 - i128::from(value)),
            (6, 2 | 3) => {
                let len = self.argument(2)?;
                if !(9..=16).contains(&len) {
                    return Err(DecodeError::NonCanonical);
                }
                let bytes = self.take(len as usize)?;
                if bytes[0] == 0 {
                    return Err(DecodeError::NonCanonical);
                }
                let mut magnitude = 0u128;
                for byte in bytes {
                    magnitude = (magnitude << 8) | u128::from(*byte);
                }
                let magnitude =
                    i128::try_from(magnitude).map_err(|_| DecodeError::IntegerOverflow)?;
                Ok(if value == 2 {
                    magnitude
                } else {
                    -1 - magnitude
                })
            }
            _ => Err(DecodeError::UnexpectedType),
        }
    }
    pub(crate) fn end(&self) -> Result<(), DecodeError> {
        if self.offset != self.bytes.len() {
            return Err(DecodeError::TrailingData);
        }
        Ok(())
    }
}
