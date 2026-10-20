use crate::{Error, Result};

pub(crate) struct Sink<'a> {
    out: Option<&'a mut [u8]>,
    len: usize,
}
impl Sink<'_> {
    pub(crate) fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self.len.checked_add(bytes.len()).ok_or(Error::Capacity)?;
        if let Some(out) = &mut self.out {
            out.get_mut(self.len..end)
                .ok_or(Error::Capacity)?
                .copy_from_slice(bytes);
        }
        self.len = end;
        Ok(())
    }
    pub(crate) fn byte(&mut self, byte: u8) -> Result<()> {
        self.bytes(&[byte])
    }
    pub(crate) fn integer(&mut self, mut number: u128) -> Result<()> {
        let mut digits = [0; 39];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (number % 10) as u8;
            number /= 10;
            if number == 0 {
                break;
            }
        }
        self.bytes(&digits[start..])
    }
}
pub(crate) fn atomic(
    out: &mut [u8],
    mut encode: impl FnMut(&mut Sink<'_>) -> Result<()>,
) -> Result<usize> {
    let mut count = Sink { out: None, len: 0 };
    encode(&mut count)?;
    if count.len > out.len() {
        return Err(Error::Capacity);
    }
    let mut write = Sink {
        out: Some(out),
        len: 0,
    };
    encode(&mut write)?;
    Ok(write.len)
}
pub(crate) fn percent(s: &mut Sink<'_>, bytes: &[u8]) -> Result<()> {
    const HEX: &[u8] = b"0123456789ABCDEF";
    for &b in bytes {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            s.byte(b)?;
        } else {
            s.bytes(&[b'%', HEX[(b >> 4) as usize], HEX[(b & 15) as usize]])?;
        }
    }
    Ok(())
}
pub(crate) fn json_string(s: &mut Sink<'_>, value: &str) -> Result<()> {
    const HEX: &[u8] = b"0123456789abcdef";
    s.byte(b'"')?;
    for b in value.bytes() {
        match b {
            b'"' => s.bytes(b"\\\"")?,
            b'\\' => s.bytes(b"\\\\")?,
            0..=31 => s.bytes(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(b >> 4) as usize],
                HEX[(b & 15) as usize],
            ])?,
            _ => s.byte(b)?,
        }
    }
    s.byte(b'"')
}
