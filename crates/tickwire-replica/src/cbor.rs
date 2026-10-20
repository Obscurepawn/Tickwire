use crate::*;
pub(crate) struct Writer(pub Vec<u8>);
impl Writer {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    fn head(&mut self, m: u8, n: u64) {
        if n < 24 {
            self.0.push(m * 32 + n as u8)
        } else if n <= 255 {
            self.0.extend([m * 32 + 24, n as u8])
        } else if n <= 65535 {
            self.0.push(m * 32 + 25);
            self.0.extend((n as u16).to_be_bytes())
        } else if n <= u32::MAX as u64 {
            self.0.push(m * 32 + 26);
            self.0.extend((n as u32).to_be_bytes())
        } else {
            self.0.push(m * 32 + 27);
            self.0.extend(n.to_be_bytes())
        }
    }
    pub fn uint(&mut self, n: u64) {
        self.head(0, n)
    }
    pub fn array(&mut self, n: usize) {
        self.head(4, n as u64)
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.head(2, b.len() as u64);
        self.0.extend(b)
    }
}
pub(crate) struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self(b)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(Error::Invalid("truncated CBOR"))?;
        self.0 = b;
        Ok(a)
    }
    fn head(&mut self, m: u8) -> Result<u64> {
        let h = self.take(1)?[0];
        if h >> 5 != m {
            return Err(Error::Invalid("CBOR type"));
        }
        let (n, min) = match h & 31 {
            v @ 0..=23 => (v as u64, 0),
            24 => (self.take(1)?[0] as u64, 24),
            25 => (
                u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64,
                256,
            ),
            26 => (
                u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64,
                65536,
            ),
            27 => (
                u64::from_be_bytes(self.take(8)?.try_into().unwrap()),
                1 << 32,
            ),
            _ => return Err(Error::Invalid("CBOR indefinite")),
        };
        if n < min {
            return Err(Error::Invalid("noncanonical CBOR"));
        }
        Ok(n)
    }
    pub fn uint(&mut self) -> Result<u64> {
        self.head(0)
    }
    pub fn count(&mut self, max: usize) -> Result<usize> {
        let n = usize::try_from(self.head(4)?).map_err(|_| Error::Bounds)?;
        if n > max {
            return Err(Error::Bounds);
        }
        Ok(n)
    }
    pub fn array(&mut self, n: usize) -> Result<()> {
        if self.count(n)? != n {
            return Err(Error::Invalid("array count"));
        }
        Ok(())
    }
    pub fn bytes(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = usize::try_from(self.head(2)?).map_err(|_| Error::Bounds)?;
        if n > max {
            return Err(Error::Bounds);
        }
        self.take(n)
    }
    pub fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.bytes(N)?
            .try_into()
            .map_err(|_| Error::Invalid("fixed bytes"))
    }
    pub fn end(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Invalid("trailing CBOR"))
        }
    }
}
