use crate::*;

// This grammar has only fixed arrays, unsigned integers and byte strings.
// Reject non-shortest representations and indefinite items at the read boundary.
struct Writer(Vec<u8>);
impl Writer {
    fn head(&mut self, major: u8, n: u64) {
        if n < 24 {
            self.0.push(major * 32 + n as u8);
        } else if n <= u8::MAX as u64 {
            self.0.extend([major * 32 + 24, n as u8]);
        } else if n <= u16::MAX as u64 {
            self.0.push(major * 32 + 25);
            self.0.extend((n as u16).to_be_bytes());
        } else if n <= u32::MAX as u64 {
            self.0.push(major * 32 + 26);
            self.0.extend((n as u32).to_be_bytes());
        } else {
            self.0.push(major * 32 + 27);
            self.0.extend(n.to_be_bytes());
        }
    }
    fn uint(&mut self, n: u64) {
        self.head(0, n)
    }
    fn array(&mut self, n: usize) {
        self.head(4, n as u64)
    }
    fn bytes(&mut self, b: &[u8]) {
        self.head(2, b.len() as u64);
        self.0.extend(b)
    }
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(Error::Invalid("truncated CBOR"))?;
        self.0 = b;
        Ok(a)
    }
    fn head(&mut self, major: u8) -> Result<u64> {
        let h = self.take(1)?[0];
        if h >> 5 != major {
            return Err(Error::Invalid("CBOR type"));
        }
        let additional = h & 31;
        let (n, min) = match additional {
            0..=23 => (additional as u64, 0),
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
            _ => return Err(Error::Invalid("indefinite CBOR")),
        };
        if n < min {
            return Err(Error::Invalid("noncanonical CBOR"));
        }
        Ok(n)
    }
    fn uint(&mut self) -> Result<u64> {
        self.head(0)
    }
    fn array(&mut self, n: usize) -> Result<()> {
        if self.head(4)? != n as u64 {
            return Err(Error::Invalid("CBOR array length"));
        }
        Ok(())
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        if self.head(2)? != N as u64 {
            return Err(Error::Invalid("CBOR bytes length"));
        }
        Ok(self.take(N)?.try_into().unwrap())
    }
}
impl RecoveryManifest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.objects.len() > MAX_OBJECTS || self.edges.len() > MAX_EDGES {
            return Err(Error::Capacity);
        }
        let mut w = Writer(Vec::new());
        let b = &self.binding;
        w.array(12);
        w.bytes(b"tickwire.recovery-manifest");
        w.uint(1);
        w.array(7);
        w.bytes(&b.risk_domain_id);
        w.uint(b.intent_id);
        w.uint(b.intent_exec_epoch);
        w.bytes(&b.business_digest);
        w.uint(b.strategy_generation);
        w.uint(b.config_revision);
        w.uint(b.recovery_object);
        w.uint(self.logical_strategy_id);
        w.bytes(&self.instance_id);
        w.bytes(&self.action_grant_ref);
        w.bytes(&self.operation_ref);
        w.uint(match self.mode {
            RecoveryMode::DeterministicReplay => 1,
            RecoveryMode::DeclaredRebuild => 2,
        });
        w.bytes(&self.fault_model_id);
        w.bytes(&self.pin_id);
        let i = &self.input_boundary;
        w.array(5);
        w.bytes(&i.stream);
        w.uint(i.first_sequence);
        w.bytes(&i.previous_hash);
        w.uint(i.last_sequence);
        w.bytes(&i.last_hash);
        w.array(2);
        w.array(self.objects.len());
        for o in &self.objects {
            w.array(5);
            w.uint(o.kind as u64);
            w.bytes(&o.content_hash);
            w.uint(o.byte_length);
            w.uint(o.schema_id);
            w.bytes(&o.owner_identity);
        }
        w.array(self.edges.len());
        for &(a, b) in &self.edges {
            w.array(2);
            w.uint(a as u64);
            w.uint(b as u64);
        }
        if w.0.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        Ok(w.0)
    }
    pub fn decode(raw: &[u8]) -> Result<Self> {
        if raw.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader(raw);
        r.array(12)?;
        if r.fixed::<26>()? != *b"tickwire.recovery-manifest" || r.uint()? != 1 {
            return Err(Error::Invalid("manifest schema"));
        }
        r.array(7)?;
        let binding = ClosureBinding {
            risk_domain_id: r.fixed()?,
            intent_id: r.uint()?,
            intent_exec_epoch: r.uint()?,
            business_digest: r.fixed()?,
            strategy_generation: r.uint()?,
            config_revision: r.uint()?,
            recovery_object: r.uint()?,
        };
        let logical_strategy_id = r.uint()?;
        let instance_id = r.fixed()?;
        let action_grant_ref = r.fixed()?;
        let operation_ref = r.fixed()?;
        let mode = match r.uint()? {
            1 => RecoveryMode::DeterministicReplay,
            2 => RecoveryMode::DeclaredRebuild,
            _ => return Err(Error::Invalid("recovery mode")),
        };
        let fault_model_id = r.fixed()?;
        let pin_id = r.fixed()?;
        r.array(5)?;
        let input_boundary = InputBoundary {
            stream: r.fixed()?,
            first_sequence: r.uint()?,
            previous_hash: r.fixed()?,
            last_sequence: r.uint()?,
            last_hash: r.fixed()?,
        };
        r.array(2)?;
        let count = r.head(4)?;
        if count > MAX_OBJECTS as u64 {
            return Err(Error::Capacity);
        }
        let mut objects = Vec::with_capacity(count as usize);
        for _ in 0..count {
            r.array(5)?;
            objects.push(DependencyObject {
                kind: r.uint()?.try_into().map_err(|_| Error::Invalid("kind"))?,
                content_hash: r.fixed()?,
                byte_length: r.uint()?,
                schema_id: r.uint()?,
                owner_identity: r.fixed()?,
            });
        }
        let count = r.head(4)?;
        if count > MAX_EDGES as u64 {
            return Err(Error::Capacity);
        }
        let mut edges = Vec::with_capacity(count as usize);
        for _ in 0..count {
            r.array(2)?;
            edges.push((
                r.uint()?
                    .try_into()
                    .map_err(|_| Error::Invalid("edge index"))?,
                r.uint()?
                    .try_into()
                    .map_err(|_| Error::Invalid("edge index"))?,
            ));
        }
        if !r.0.is_empty() {
            return Err(Error::Invalid("trailing CBOR"));
        }
        Ok(Self {
            binding,
            logical_strategy_id,
            instance_id,
            action_grant_ref,
            operation_ref,
            mode,
            fault_model_id,
            pin_id,
            input_boundary,
            objects,
            edges,
        })
    }
}
