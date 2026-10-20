use crate::*;
fn begin_valid(s: &[u8]) -> bool {
    (s.starts_with(b"FIX.") || s.starts_with(b"FIXT."))
        && s.len() <= 16
        && s.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'.')
}
pub fn encode(begin: &str, body: &[u8], max: usize) -> Result<Vec<u8>> {
    if !begin_valid(begin.as_bytes()) || body.is_empty() || body.last() != Some(&1) {
        return Err(Error::Protocol("FIX begin/body"));
    }
    let prefix = format!("8={begin}\x019={}\x01", body.len());
    let len = prefix
        .len()
        .checked_add(body.len())
        .and_then(|n| n.checked_add(7))
        .ok_or(Error::Bounds)?;
    if max > 64 * 1024 * 1024 || len > max {
        return Err(Error::Bounds);
    }
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(body);
    let sum = out.iter().fold(0u8, |n, b| n.wrapping_add(*b));
    out.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
    Ok(out)
}
pub struct Decoder {
    buffer: Vec<u8>,
    max: usize,
    failed: bool,
}
impl Decoder {
    pub fn finish(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::Closed);
        }
        self.failed = true;
        if !self.buffer.is_empty() {
            return Err(Error::Protocol("truncated FIX frame"));
        }
        Ok(())
    }
    pub fn new(max: usize) -> Result<Self> {
        if max == 0 || max > 64 * 1024 * 1024 {
            return Err(Error::Bounds);
        }
        Ok(Self {
            buffer: Vec::new(),
            max,
            failed: false,
        })
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed {
            return Err(Error::Closed);
        }
        if bytes.len() > self.max - self.buffer.len() {
            self.failed = true;
            return Err(Error::Bounds);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        if self.failed {
            return Err(Error::Closed);
        }
        let r = self.parse();
        if r.is_err() {
            self.failed = true
        }
        r
    }
    fn parse(&mut self) -> Result<Option<Vec<u8>>> {
        if self.buffer.len() < 2 {
            return Ok(None);
        }
        if !self.buffer.starts_with(b"8=") {
            return Err(Error::Protocol("FIX begin"));
        }
        let Some(first) = self.buffer.iter().position(|b| *b == 1) else {
            if self.buffer.len() > 18 {
                return Err(Error::Protocol("FIX begin length"));
            }
            return Ok(None);
        };
        if !begin_valid(&self.buffer[2..first]) {
            return Err(Error::Protocol("FIX begin"));
        }
        let rest = &self.buffer[first + 1..];
        if rest.len() < 2 {
            return Ok(None);
        }
        if !rest.starts_with(b"9=") {
            return Err(Error::Protocol("FIX BodyLength missing"));
        }
        let Some(second) = rest.iter().position(|b| *b == 1) else {
            if rest.len() > 22 {
                return Err(Error::Bounds);
            }
            return Ok(None);
        };
        let digits = &rest[2..second];
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(Error::Protocol("FIX BodyLength"));
        }
        let len = digits.iter().try_fold(0usize, |n, b| {
            n.checked_mul(10)
                .and_then(|n| n.checked_add(usize::from(b - b'0')))
                .ok_or(Error::Bounds)
        })?;
        if len == 0 {
            return Err(Error::Protocol("empty FIX body"));
        }
        let check = (first + 1 + second + 1)
            .checked_add(len)
            .ok_or(Error::Bounds)?;
        let end = check.checked_add(7).ok_or(Error::Bounds)?;
        if end > self.max {
            return Err(Error::Bounds);
        }
        if self.buffer.len() < end {
            return Ok(None);
        }
        let checksum = &self.buffer[check..end];
        if self.buffer[check - 1] != 1
            || !checksum.starts_with(b"10=")
            || checksum[6] != 1
            || !checksum[3..6].iter().all(u8::is_ascii_digit)
        {
            return Err(Error::Protocol("FIX CheckSum tag"));
        }
        let expected = checksum[3..6]
            .iter()
            .fold(0u16, |n, b| n * 10 + u16::from(b - b'0'));
        let actual = self.buffer[..check]
            .iter()
            .fold(0u8, |n, b| n.wrapping_add(*b));
        if expected != u16::from(actual) {
            return Err(Error::Protocol("FIX CheckSum"));
        }
        Ok(Some(self.buffer.drain(..end).collect()))
    }
}
