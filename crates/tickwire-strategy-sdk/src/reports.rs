//! Complete durable account records referenced by the execution report ring.
use crate::{Error, Result};
use std::os::fd::OwnedFd;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_ipc::{RecordHeader, ReportBody, ReportRecord, ResourceHeader, Wire};
use tickwire_recovery::{BlobStore, FileBlobReader, StoreKey};
use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};

pub const FEATURE: u64 = 1 << 8;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdentity {
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub risk_domain: [u8; 16],
}
impl ReportIdentity {
    fn validate(&self, bank: &ResourceHeader) -> Result<()> {
        if self.logical_strategy_id == 0
            || self.instance_id == [0; 16]
            || self.strategy_generation == 0
            || self.risk_domain == [0; 16]
            || bank.resource_kind != 3
            || bank.record_bytes != 512
            || bank.required_features & FEATURE == 0
            || bank.risk_scope == 0
            || bank.consumer_instance != self.instance_id
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountReport {
    Baseline(CanonicalBaseline),
    Record {
        position: u64,
        record: CanonicalRecord,
    },
}
pub struct ReportWriter<B> {
    publisher: Publisher<1>,
    store: B,
    identity: ReportIdentity,
    baseline: Option<[u8; 32]>,
    baseline_sequence: u64,
    last_position: u64,
    last_digest: [u8; 32],
}
impl<B: BlobStore> ReportWriter<B> {
    pub fn new(publisher: Publisher<1>, store: B, identity: ReportIdentity) -> Result<Self> {
        identity.validate(publisher.header())?;
        Ok(Self {
            publisher,
            store,
            identity,
            baseline: None,
            baseline_sequence: 0,
            last_position: 0,
            last_digest: [0; 32],
        })
    }
    pub fn publisher(&mut self) -> &mut Publisher<1> {
        &mut self.publisher
    }
    pub fn last_position(&self) -> u64 {
        self.last_position
    }
    pub fn publish_baseline(&mut self, baseline: &CanonicalBaseline, now: u64) -> Result<u64> {
        if baseline.snapshot().scope.risk_domain_id != self.identity.risk_domain {
            return Err(Error::Identity);
        }
        if let Some(old) = self.baseline {
            return if old == baseline.digest() {
                Ok(self.baseline_sequence)
            } else {
                Err(Error::Identity)
            };
        }
        let sequence = self.publish(780, 11, 0, baseline.digest(), baseline.bytes(), now)?;
        self.baseline = Some(baseline.digest());
        self.baseline_sequence = sequence;
        Ok(sequence)
    }
    pub fn publish_record(
        &mut self,
        position: u64,
        record: &CanonicalRecord,
        now: u64,
    ) -> Result<u64> {
        if self.baseline != Some(record.baseline()) || position != record.sequence() {
            return Err(Error::Identity);
        }
        if position == self.last_position && record.digest() == self.last_digest {
            return Ok(self.publisher.published());
        }
        if position != self.last_position.checked_add(1).ok_or(Error::Bounds)? {
            return Err(Error::Identity);
        }
        let sequence = self.publish(781, 12, position, record.digest(), record.bytes(), now)?;
        self.last_position = position;
        self.last_digest = record.digest();
        Ok(sequence)
    }
    fn publish(
        &mut self,
        message_type: u16,
        kind: u16,
        position: u64,
        digest: [u8; 32],
        bytes: &[u8],
        now: u64,
    ) -> Result<u64> {
        self.store.put_sync(StoreKey::Object(digest), bytes)?;
        let sequence = self
            .publisher
            .published()
            .checked_add(1)
            .ok_or(Error::Bounds)?;
        let bank = self.publisher.header();
        let mut record = ReportRecord {
            header: RecordHeader {
                schema_major: 1,
                schema_minor: 0,
                message_type,
                flags: 12,
                header_bytes: 128,
                body_bytes: 384,
                sequence,
                topic_or_scope_id: bank.risk_scope,
                producer_generation: bank.producer_generation,
                publish_mono_ns: now,
                ..Default::default()
            },
            body: ReportBody {
                logical_strategy_id: self.identity.logical_strategy_id,
                instance_id: self.identity.instance_id,
                strategy_generation: self.identity.strategy_generation,
                exec_generation: bank.producer_generation,
                exec_journal_seq: position,
                report_seq: sequence,
                event_mono_ns: now,
                report_kind: kind,
                aux_id: digest[..16].try_into().unwrap(),
                aux_version: bytes.len() as u64,
                value_0: i64::from_le_bytes(digest[16..24].try_into().unwrap()),
                value_1: i64::from_le_bytes(digest[24..32].try_into().unwrap()),
                value_validity: 384,
                ..Default::default()
            },
        };
        let mut image = [0u8; 512];
        record.encode(&mut image)?;
        record.header.record_crc32c = tickwire_ipc::crc32c(&image);
        record.encode(&mut image)?;
        tickwire_ipc::decode_record(&image)?;
        Ok(self.publisher.publish(&[&image])?)
    }
}
pub struct ReportReceiver {
    reader: Subscriber<4>,
    bank: ResourceHeader,
    objects: FileBlobReader,
    identity: ReportIdentity,
    baseline: Option<[u8; 32]>,
    last_position: u64,
}
impl ReportReceiver {
    /// # Safety
    /// The authenticated execution owner retains this registered cursor and is
    /// the only writer. It follows the shared bank's borrow and release protocol.
    pub unsafe fn attach(
        fd: OwnedFd,
        bank: ResourceHeader,
        subscription: SubscriptionKey,
        objects: FileBlobReader,
        identity: ReportIdentity,
    ) -> Result<Self> {
        identity.validate(&bank)?;
        if subscription.instance.to_le_bytes() != identity.instance_id {
            return Err(Error::Identity);
        }
        let first = bank.first_seq.checked_sub(1).ok_or(Error::Bounds)?;
        // SAFETY: the caller establishes the qualified owner and exact registered gate.
        let reader = unsafe { Subscriber::attach(fd, bank.clone(), subscription, first)? };
        Ok(Self {
            reader,
            bank,
            objects,
            identity,
            baseline: None,
            last_position: 0,
        })
    }
    pub fn cursor_fd(&self) -> Result<OwnedFd> {
        Ok(self.reader.cursor_fd()?)
    }
    pub fn header(&self) -> &ResourceHeader {
        &self.bank
    }
    pub fn last_position(&self) -> u64 {
        self.last_position
    }
    pub fn start_quiesce(&self) {
        self.reader.start_quiesce();
    }
    pub fn quiesced(&self) -> Result<tickwire_shm::Quiesced> {
        Ok(self.reader.quiesced()?)
    }
    pub fn next(&mut self) -> Result<Option<AccountReport>> {
        let Some(guard) = self.reader.borrow()? else {
            return Ok(None);
        };
        let report = resolve(&self.objects, guard.bytes(), &self.bank, &self.identity)?;
        match &report {
            AccountReport::Baseline(b) => {
                if self.baseline.is_some() || self.last_position != 0 {
                    return Err(Error::Identity);
                }
                self.baseline = Some(b.digest());
            }
            AccountReport::Record { position, record } => {
                if self.baseline != Some(record.baseline())
                    || *position != self.last_position.checked_add(1).ok_or(Error::Bounds)?
                {
                    return Err(Error::Identity);
                }
                self.last_position = *position;
            }
        }
        let sequence = guard.sequence();
        drop(guard);
        self.reader.apply(sequence)?;
        Ok(Some(report))
    }
}
pub fn resolve(
    objects: &FileBlobReader,
    raw: &[u8],
    bank: &ResourceHeader,
    identity: &ReportIdentity,
) -> Result<AccountReport> {
    identity.validate(bank)?;
    let tickwire_ipc::Record::Report(record) = tickwire_ipc::decode_record(raw)? else {
        return Err(Error::Identity);
    };
    let h = &record.header;
    let b = &record.body;
    if !matches!(h.message_type, 780 | 781)
        || h.topic_or_scope_id != bank.risk_scope
        || h.producer_generation != bank.producer_generation
        || b.logical_strategy_id != identity.logical_strategy_id
        || b.instance_id != identity.instance_id
        || b.strategy_generation != identity.strategy_generation
        || b.exec_generation != bank.producer_generation
        || b.report_seq != h.sequence
        || b.event_mono_ns != h.publish_mono_ns
    {
        return Err(Error::Identity);
    }
    let mut hash = [0u8; 32];
    hash[..16].copy_from_slice(&b.aux_id);
    hash[16..24].copy_from_slice(&b.value_0.to_le_bytes());
    hash[24..].copy_from_slice(&b.value_1.to_le_bytes());
    let size = usize::try_from(b.aux_version).map_err(|_| Error::Bounds)?;
    let maximum = if h.message_type == 780 {
        tickwire_account_codec::MAX_BASELINE_BYTES
    } else {
        tickwire_account_codec::MAX_RECORD_BYTES
    };
    if size == 0 || size > maximum {
        return Err(Error::Bounds);
    }
    let bytes = objects.get_object(hash, size)?;
    if bytes.len() != size {
        return Err(Error::Identity);
    }
    if h.message_type == 780 {
        let baseline = CanonicalBaseline::decode(&bytes).map_err(|_| Error::Identity)?;
        if b.exec_journal_seq != 0
            || baseline.digest() != hash
            || baseline.snapshot().scope.risk_domain_id != identity.risk_domain
        {
            return Err(Error::Identity);
        }
        Ok(AccountReport::Baseline(baseline))
    } else {
        let record = CanonicalRecord::decode(&bytes).map_err(|_| Error::Identity)?;
        if record.digest() != hash || record.sequence() != b.exec_journal_seq {
            return Err(Error::Identity);
        }
        Ok(AccountReport::Record {
            position: b.exec_journal_seq,
            record,
        })
    }
}
