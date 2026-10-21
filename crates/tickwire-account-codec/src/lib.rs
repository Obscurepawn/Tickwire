//! Bounded immutable account recovery objects.
#![forbid(unsafe_code)]
use sha2::{Digest, Sha256};
use tickwire_account::{Catalog, RecoveryRecord, Snapshot};
mod cbor;
mod codec;
mod validate;
use codec::Codec;
pub const MAX_BASELINE_BYTES: usize = 2 * 1024 * 1024;
/// Leaves room for a begin/data/seal batch in the physical WAL's 64 KiB limit.
pub const MAX_RECORD_BYTES: usize = 60 * 1024;
pub const MAX_ADMISSION_ORDERS: usize = 3;
pub const MAX_OWNERS: usize = 1024;
pub const MAX_BRACKETS: usize = 128;
pub type Hash = [u8; 32];
/// A deterministic seal over exactly the canonical reports applied by a
/// strategy. Its digest is independent of a process's volatile receive cursor.
#[derive(Clone, Debug)]
pub struct InputSeal {
    baseline: Hash,
    position: u64,
    digest: Hash,
    object: Vec<u8>,
}
impl InputSeal {
    pub fn new(baseline: Hash) -> Result<Self, Error> {
        if baseline == [0; 32] {
            return Err(Error::Field);
        }
        let mut object = b"tickwire.account-input-seal.v1".to_vec();
        object.extend(baseline);
        let digest = Sha256::digest(&object).into();
        Ok(Self {
            baseline,
            position: 0,
            digest,
            object,
        })
    }
    pub fn apply(&mut self, record: &CanonicalRecord) -> Result<(), Error> {
        if record.baseline() != self.baseline
            || self.position.checked_add(1) != Some(record.sequence())
        {
            return Err(Error::Field);
        }
        let mut object = b"tickwire.account-input-seal.step.v1".to_vec();
        object.extend(self.digest);
        object.extend(record.sequence().to_le_bytes());
        object.extend(record.digest());
        self.digest = Sha256::digest(&object).into();
        self.position = record.sequence();
        self.object = object;
        Ok(())
    }
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn object_bytes(&self) -> &[u8] {
        &self.object
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Truncated,
    NonCanonical,
    Field,
    Text,
    IntegerOverflow,
    TrailingData,
    Schema,
    Duplicate,
    Account(tickwire_account::Error),
    Venue(tickwire_venue::Error),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRecord {
    baseline: Hash,
    sequence: u64,
    record: tickwire_account::RecoveryRecord,
    bytes: Vec<u8>,
    digest: Hash,
}
impl CanonicalRecord {
    pub fn new(baseline: Hash, sequence: u64, record: RecoveryRecord) -> Result<Self, Error> {
        if baseline == [0; 32] || sequence == 0 {
            return Err(Error::Field);
        }
        if matches!(&record, RecoveryRecord::IncomeBaseline(b) if b.baseline_digest!=baseline) {
            return Err(Error::Field);
        }
        if matches!(&record, RecoveryRecord::IncomeObservationsAssociated(b) if b.baseline_digest!=baseline)
        {
            return Err(Error::Field);
        }
        if matches!(&record, RecoveryRecord::UncreatedAlgoClosed(b) if b.baseline_digest!=baseline)
        {
            return Err(Error::Field);
        }
        if matches!(&record, RecoveryRecord::AccountInputsReconciled(b) if b.baseline_digest!=baseline)
        {
            return Err(Error::Field);
        }
        validate::record(&record)?;
        let mut w = cbor::Writer::new(MAX_RECORD_BYTES);
        w.array(5);
        w.text("tickwire.account-record");
        w.uint(1);
        baseline.write(&mut w);
        sequence.write(&mut w);
        record.write(&mut w);
        let bytes = w.finish()?;
        let digest = Sha256::digest(&bytes).into();
        Ok(Self {
            baseline,
            sequence,
            record,
            bytes,
            digest,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = cbor::Reader::new(bytes);
        r.array(5)?;
        if r.text(32)? != "tickwire.account-record" || r.uint()? != 1 {
            return Err(Error::Schema);
        }
        let baseline = Codec::read(&mut r)?;
        let sequence = Codec::read(&mut r)?;
        let record = RecoveryRecord::read(&mut r)?;
        r.end()?;
        let out = Self::new(baseline, sequence, record)?;
        if out.bytes() != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(out)
    }
    pub fn baseline(&self) -> Hash {
        self.baseline
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn record(&self) -> &tickwire_account::RecoveryRecord {
        &self.record
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalBaseline {
    snapshot: Snapshot,
    catalog: Catalog,
    bytes: Vec<u8>,
    digest: Hash,
}
impl CanonicalBaseline {
    pub fn new(mut snapshot: Snapshot, mut catalog: Catalog) -> Result<Self, Error> {
        validate::baseline(&mut snapshot, &mut catalog)?;
        tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone())
            .map_err(Error::Account)?;
        let mut w = cbor::Writer::new(MAX_BASELINE_BYTES);
        w.array(4);
        w.text("tickwire.account-baseline");
        w.uint(1);
        snapshot.write(&mut w);
        catalog.write(&mut w);
        let bytes = w.finish()?;
        let digest = Sha256::digest(&bytes).into();
        Ok(Self {
            snapshot,
            catalog,
            bytes,
            digest,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_BASELINE_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = cbor::Reader::new(bytes);
        r.array(4)?;
        if r.text(32)? != "tickwire.account-baseline" || r.uint()? != 1 {
            return Err(Error::Schema);
        }
        let snapshot = Snapshot::read(&mut r)?;
        let catalog = Catalog::read(&mut r)?;
        r.end()?;
        let out = Self::new(snapshot, catalog)?;
        if out.bytes() != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(out)
    }
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}
