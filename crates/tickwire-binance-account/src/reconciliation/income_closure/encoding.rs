use super::*;
const DOMAIN: &[u8] = b"tickwire.binance.account-input-closure.v1";
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Reference {
    pub sequence: u64,
    pub length: u64,
    pub digest: v::Hash,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selected {
    pub kind: String,
    pub id: String,
    pub length: u64,
    pub digest: v::Hash,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub original: AccountInputsReconciled,
    pub account_stream: [u8; 16],
    pub input_prefix_digest: v::Hash,
    pub writer: u64,
    pub private_epoch: u64,
    pub gap: u64,
    pub boot: [u8; 16],
    pub generation: u64,
    pub evaluated: u64,
    pub expires: u64,
    pub start: u64,
    pub end: u64,
    pub leaves: Vec<(Reference, u64, u64, u16)>,
    pub selected: Vec<Selected>,
    pub snapshots: [Reference; 3],
}
impl Manifest {
    pub fn binding(&self, digest: v::Hash) -> AccountInputsReconciled {
        let mut b = self.original.clone();
        b.coverage_digest = digest;
        b
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.leaves.is_empty()
            || self.leaves.len() + 3 > MAX_REFERENCES
            || self.selected.is_empty()
            || self.selected.len() > MAX_SELECTED
            || self.original.coverage_digest != [0; 32]
        {
            return Err(Error::Capacity);
        }
        let b = &self.original;
        let mut w = Writer(Vec::new());
        w.array(24);
        w.blob(DOMAIN);
        w.uint(1);
        w.array(4);
        w.blob(&b.scope.risk_domain_id);
        w.uint(u64::from(b.scope.venue));
        w.uint(match b.scope.environment {
            v::Environment::Production => 1,
            v::Environment::Test => 2,
        });
        w.uint(b.scope.account);
        w.uint(b.account_epoch);
        w.uint(b.ledger_sequence);
        w.blob(&b.baseline_digest);
        w.blob(&b.history_digest);
        w.blob(&b.input_stream);
        w.uint(b.through_sequence);
        w.blob(&b.state_digest);
        w.blob(&self.account_stream);
        w.blob(&self.input_prefix_digest);
        w.uint(self.writer);
        w.uint(self.private_epoch);
        w.uint(self.gap);
        w.blob(&self.boot);
        w.uint(self.generation);
        w.uint(self.evaluated);
        w.uint(self.expires);
        w.uint(self.start);
        w.uint(self.end);
        w.array(self.leaves.len());
        for (r, start, end, limit) in &self.leaves {
            w.array(6);
            w.reference(r);
            w.uint(*start);
            w.uint(*end);
            w.uint(u64::from(*limit));
        }
        w.array(self.selected.len());
        for r in &self.selected {
            w.array(4);
            w.blob(r.kind.as_bytes());
            w.blob(r.id.as_bytes());
            w.uint(r.length);
            w.blob(&r.digest);
        }
        w.array(3);
        for r in &self.snapshots {
            w.array(3);
            w.reference(r)
        }
        if w.0.len() > MAX_COVERAGE_BYTES {
            return Err(Error::Capacity);
        }
        Ok(w.0)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_COVERAGE_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(24)?;
        if r.blob(DOMAIN.len())? != DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        r.array(4)?;
        let scope = tickwire_account::Scope {
            risk_domain_id: r.fixed()?,
            venue: r.uint()?.try_into().map_err(|_| Error::Encoding)?,
            environment: match r.uint()? {
                1 => v::Environment::Production,
                2 => v::Environment::Test,
                _ => return Err(Error::Encoding),
            },
            account: r.uint()?,
        };
        let original = AccountInputsReconciled {
            scope,
            account_epoch: r.uint()?,
            ledger_sequence: r.uint()?,
            baseline_digest: r.fixed()?,
            history_digest: r.fixed()?,
            input_stream: r.fixed()?,
            through_sequence: r.uint()?,
            state_digest: r.fixed()?,
            coverage_digest: [0; 32],
        };
        let account_stream = r.fixed()?;
        let input_prefix_digest = r.fixed()?;
        let writer = r.uint()?;
        let private_epoch = r.uint()?;
        let gap = r.uint()?;
        let boot = r.fixed()?;
        let generation = r.uint()?;
        let evaluated = r.uint()?;
        let expires = r.uint()?;
        let start = r.uint()?;
        let end = r.uint()?;
        let count = r.count(MAX_REFERENCES - 3)?;
        let mut leaves = Vec::with_capacity(count);
        for _ in 0..count {
            r.array(6)?;
            leaves.push((
                r.reference()?,
                r.uint()?,
                r.uint()?,
                r.uint()?.try_into().map_err(|_| Error::Encoding)?,
            ));
        }
        let count = r.count(MAX_SELECTED)?;
        let mut selected = Vec::with_capacity(count);
        for _ in 0..count {
            r.array(4)?;
            selected.push(Selected {
                kind: r.text(64)?,
                id: r.text(64)?,
                length: r.uint()?,
                digest: r.fixed()?,
            });
        }
        r.array(3)?;
        let mut refs = Vec::with_capacity(3);
        for _ in 0..3 {
            r.array(3)?;
            refs.push(r.reference()?)
        }
        if r.pos != bytes.len() {
            return Err(Error::Encoding);
        }
        let m = Self {
            original,
            account_stream,
            input_prefix_digest,
            writer,
            private_epoch,
            gap,
            boot,
            generation,
            evaluated,
            expires,
            start,
            end,
            leaves,
            selected,
            snapshots: refs.try_into().map_err(|_| Error::Encoding)?,
        };
        if m.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(m)
    }
}
pub(crate) struct Writer(pub(crate) Vec<u8>);
impl Writer {
    fn head(&mut self, major: u8, n: u64) {
        let m = major << 5;
        match n {
            0..=23 => self.0.push(m | n as u8),
            24..=255 => self.0.extend([m | 24, n as u8]),
            256..=65535 => {
                self.0.push(m | 25);
                self.0.extend((n as u16).to_be_bytes())
            }
            65536..=4294967295 => {
                self.0.push(m | 26);
                self.0.extend((n as u32).to_be_bytes())
            }
            _ => {
                self.0.push(m | 27);
                self.0.extend(n.to_be_bytes())
            }
        }
    }
    pub(crate) fn uint(&mut self, n: u64) {
        self.head(0, n)
    }
    pub(crate) fn array(&mut self, n: usize) {
        self.head(4, n as u64)
    }
    pub(crate) fn blob(&mut self, b: &[u8]) {
        self.head(2, b.len() as u64);
        self.0.extend(b)
    }
    pub(crate) fn reference(&mut self, r: &Reference) {
        self.uint(r.sequence);
        self.uint(r.length);
        self.blob(&r.digest)
    }
}
pub(crate) struct Reader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(Error::Encoding)?;
        let b = self.bytes.get(self.pos..end).ok_or(Error::Encoding)?;
        self.pos = end;
        Ok(b)
    }
    fn head(&mut self, major: u8) -> Result<u64> {
        let b = self.take(1)?[0];
        if b >> 5 != major {
            return Err(Error::Encoding);
        }
        let (n, min) = match b & 31 {
            n @ 0..=23 => (u64::from(n), 0),
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
                4294967296,
            ),
            _ => return Err(Error::Encoding),
        };
        if n < min {
            return Err(Error::Encoding);
        }
        Ok(n)
    }
    pub(crate) fn uint(&mut self) -> Result<u64> {
        self.head(0)
    }
    pub(crate) fn array(&mut self, n: usize) -> Result<()> {
        if self.head(4)? != n as u64 {
            Err(Error::Encoding)
        } else {
            Ok(())
        }
    }
    pub(crate) fn count(&mut self, max: usize) -> Result<usize> {
        let n = self.head(4)?;
        if n > max as u64 {
            return Err(Error::Capacity);
        }
        Ok(n as usize)
    }
    pub(crate) fn blob(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.head(2)?;
        if n > max as u64 {
            return Err(Error::Capacity);
        }
        self.take(n as usize)
    }
    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.blob(N)?.try_into().map_err(|_| Error::Encoding)
    }
    pub(crate) fn text(&mut self, max: usize) -> Result<String> {
        Ok(std::str::from_utf8(self.blob(max)?)
            .map_err(|_| Error::Encoding)?
            .to_owned())
    }
    pub(crate) fn reference(&mut self) -> Result<Reference> {
        Ok(Reference {
            sequence: self.uint()?,
            length: self.uint()?,
            digest: self.fixed()?,
        })
    }
}
