//! Mapping SDK; only this module may discharge sys memory-access contracts.
use crate::{
    BankKey, Error, Freeze, Producer, Quiesced, ReadLease, Reader, SubscriptionKey, Watermarks,
};
use sha2::{Digest, Sha256};
use std::{cell::RefCell, os::fd::OwnedFd, ptr::NonNull};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_shm_sys::{ReaderMapping, WriterMapping};

const PAGE: usize = 4096;
const SLOTS: usize = 8192;
const PUBLISHED: usize = PAGE;
const LIFECYCLE: usize = PAGE + 128;
const RELEASED: usize = PAGE + 256;

#[derive(Debug)]
pub enum MappedError {
    Protocol(Error),
    Mapping(tickwire_shm_sys::Error),
    Wire(tickwire_ipc::Error),
    Layout,
}
type Result<T> = std::result::Result<T, MappedError>;
impl From<Error> for MappedError {
    fn from(e: Error) -> Self {
        Self::Protocol(e)
    }
}
impl From<tickwire_shm_sys::Error> for MappedError {
    fn from(e: tickwire_shm_sys::Error) -> Self {
        Self::Mapping(e)
    }
}
impl From<tickwire_ipc::Error> for MappedError {
    fn from(e: tickwire_ipc::Error) -> Self {
        Self::Wire(e)
    }
}
impl std::fmt::Display for MappedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for MappedError {}

fn key(h: &ResourceHeader) -> BankKey {
    BankKey {
        resource: u128::from_le_bytes(h.resource_id),
        channel: u128::from_le_bytes(h.channel_id),
        channel_epoch: h.channel_epoch,
        ring_epoch: h.ring_epoch,
        feed_generation: h.feed_generation,
    }
}
fn hash_header(h: &mut ResourceHeader) -> Result<[u8; PAGE]> {
    let mut bytes = [0; PAGE];
    h.descriptor_hash = [0; 32];
    h.encode(&mut bytes)?;
    h.descriptor_hash = Sha256::digest(bytes).into();
    h.encode(&mut bytes)?;
    Ok(bytes)
}
fn validate_bank(h: &ResourceHeader) -> Result<()> {
    tickwire_ipc::validate_resource_shape(
        h.resource_kind,
        h.capacity,
        h.record_bytes,
        h.resource_bytes,
        None,
    )?;
    let arch = if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        0
    };
    if !(1..=3).contains(&h.resource_kind)
        || h.magic != *b"TWIPC001"
        || (h.abi_major, h.abi_minor, h.header_bytes) != (1, 0, 4096)
        || (h.endian, h.atomic_width, h.page_shift, h.isolation_shift) != (1, 8, 12, 7)
        || arch == 0
        || h.target_arch != arch
        || !key(h).valid()
        || h.producer_instance == [0; 16]
        || !(1..1 << 63).contains(&h.producer_generation)
        || h.producer_page_offset != PAGE as u64
        || h.slot_offset != SLOTS as u64
        || h.cursor_page_offset != 0
        || h.arena_offset != 0
        || h.arena_bytes != 0
        || h.arena_class_count != 0
        || h.class_descriptors.iter().any(|c| *c != Default::default())
        || h.resource_bytes != SLOTS as u64 + u64::from(h.capacity) * u64::from(h.record_bytes)
        || h.first_seq == 0
        || h.first_seq >= 1 << 63
        || h.cutover_seq != h.first_seq - 1
        || h.predecessor_ring_epoch >= h.ring_epoch
        || (h.predecessor_ring_epoch == 0 && h.cutover_seq != 0)
        || !(1..1 << 63).contains(&h.initial_membership_version)
        || h.max_consumers == 0
        || h.max_consumers > 32
        || h.required_features != 0x1c
        || h.schema_major != 1
        || h.schema_minor != 0
        || h.clock_domain != 1
    {
        return Err(MappedError::Layout);
    }
    Ok(())
}
fn cursor_header(bank: &ResourceHeader, sub: SubscriptionKey) -> Result<ResourceHeader> {
    let mut h = bank.clone();
    h.resource_kind = 4;
    h.resource_id = sub.cursor.to_le_bytes();
    h.parent_resource_id = bank.resource_id;
    h.producer_instance = sub.instance.to_le_bytes();
    h.producer_generation = sub.generation;
    h.consumer_instance = bank.producer_instance;
    h.capacity = 0;
    h.record_bytes = 0;
    h.resource_bytes = 8192;
    h.slot_offset = 0;
    h.producer_page_offset = 0;
    h.cursor_page_offset = 4096;
    h.max_consumers = 1;
    hash_header(&mut h)?;
    Ok(h)
}
fn verify_header(mapping: &ReaderMapping, expected: &ResourceHeader) -> Result<()> {
    // SAFETY: all registered writers initialize before sealing and never mutate the header.
    let actual = ResourceHeader::decode(unsafe { mapping.read_bytes(0, PAGE)? })?;
    let mut checked = actual.clone();
    hash_header(&mut checked)?;
    if actual != *expected || checked.descriptor_hash != actual.descriptor_hash {
        return Err(MappedError::Layout);
    }
    Ok(())
}

struct Cursor {
    key: SubscriptionKey,
    mapping: ReaderMapping,
}
/// A fixed-record Linux broadcast/SPSC writer. Each record starts with its u64 sequence.
/// Arena payloads and control-plane FD delivery are separate subsequent SDK modules.
pub struct Publisher<const M: usize> {
    header: ResourceHeader,
    bank: WriterMapping,
    protocol: Producer<M>,
    cursors: [Option<Cursor>; M],
}
impl<const M: usize> Publisher<M> {
    pub fn create(mut header: ResourceHeader) -> Result<Self> {
        validate_bank(&header)?;
        if header.max_consumers as usize > M {
            return Err(Error::Capacity.into());
        }
        let mut protocol =
            Producer::new(key(&header), u64::from(header.capacity), header.first_seq)?;
        protocol.membership_version = header.initial_membership_version;
        let bytes = hash_header(&mut header)?;
        let mut bank = WriterMapping::create("tickwire-bank", header.resource_bytes as usize)?;
        // SAFETY: the mapping is fresh, not shared yet; header is immutable thereafter.
        unsafe {
            bank.write_bytes(0, &bytes)?;
            bank.store_u64_release(PUBLISHED, header.first_seq - 1)?;
            bank.store_u64_release(PAGE + 384, header.initial_membership_version)?;
            bank.store_u64_release(LIFECYCLE, 3)?;
        }
        bank.seal_for_readers()?;
        protocol.activate()?;
        Ok(Self {
            header,
            bank,
            protocol,
            cursors: std::array::from_fn(|_| None),
        })
    }
    pub fn key(&self) -> BankKey {
        key(&self.header)
    }
    pub fn header(&self) -> &ResourceHeader {
        &self.header
    }
    pub fn published(&self) -> u64 {
        self.protocol.published()
    }
    pub fn reader_fd(&self) -> Result<OwnedFd> {
        Ok(self.bank.duplicate_fd()?)
    }
    pub fn join(&mut self, sub: SubscriptionKey) -> Result<u64> {
        if self.protocol.member_index(sub).is_err()
            && self.protocol.members.iter().flatten().count() >= self.header.max_consumers as usize
        {
            return Err(Error::Capacity.into());
        }
        let first = self.protocol.join(sub)?;
        // SAFETY: only this publisher writes the aligned membership atomic.
        unsafe {
            self.bank
                .store_u64_release(PAGE + 384, self.protocol.membership_version())?;
        }
        Ok(first)
    }
    /// Attach the registered consumer's independent cursor capability.
    ///
    /// # Safety
    /// The authenticated peer must have created this cursor with Subscriber::attach
    /// for this exact pending subscription and must obey its borrow/Quiesce protocol.
    /// It must never modify the immutable header or claim release of live references.
    pub unsafe fn register_cursor(&mut self, sub: SubscriptionKey, fd: OwnedFd) -> Result<()> {
        if self.cursors.iter().flatten().any(|c| c.key == sub) {
            return Err(Error::Identity.into());
        }
        let index = self.protocol.member_index(sub)?;
        let cursor = ReaderMapping::map(fd, 8192)?;
        verify_header(&cursor, &cursor_header(&self.header, sub)?)?;
        // SAFETY: validated registered cursor; this field uses only u64 atomic accesses.
        let released = unsafe { cursor.load_u64_acquire(RELEASED)? };
        self.protocol.join_ready(sub, released)?;
        self.cursors[index] = Some(Cursor {
            key: sub,
            mapping: cursor,
        });
        Ok(())
    }
    fn refresh(&mut self) -> Result<()> {
        for c in self.cursors.iter().flatten() {
            // SAFETY: cursor identity/atomic discipline established by register_cursor.
            let released = unsafe { c.mapping.load_u64_acquire(RELEASED)? };
            self.protocol.observe_release(c.key, released)?;
        }
        Ok(())
    }
    pub fn publish(&mut self, records: &[&[u8]]) -> Result<u64> {
        if records.is_empty() || records.len() > 64 {
            return Err(Error::BatchSize.into());
        }
        for (i, bytes) in records.iter().enumerate() {
            let expected = self
                .published()
                .checked_add(i as u64 + 1)
                .filter(|s| *s < 1 << 63)
                .ok_or(Error::SequenceExhausted)?;
            if bytes.len() != self.header.record_bytes as usize
                || u64::from_le_bytes(bytes[..8].try_into().unwrap()) != expected
            {
                return Err(MappedError::Layout);
            }
        }
        self.refresh()?;
        let claim = self.protocol.claim(records.len() as u8)?;
        for (i, bytes) in records.iter().enumerate() {
            let slot = self.protocol.slot(claim.first() + i as u64)?;
            let offset = SLOTS + slot * self.header.record_bytes as usize;
            // SAFETY: claim checked all readers' release-acquired watermarks. These
            // unpublished slots have no live borrows, and this is their unique writer.
            if let Err(e) = unsafe { self.bank.write_bytes(offset, bytes) } {
                self.protocol.abort(claim)?;
                return Err(e.into());
            }
        }
        // SAFETY: all batch bytes are complete, and this publisher alone writes
        // published. Release makes the entire contiguous batch visible.
        unsafe {
            self.bank.store_u64_release(PUBLISHED, claim.last())?;
        }
        Ok(self.protocol.publish(claim)?)
    }
    pub fn prepare_freeze(&mut self, successor: u64, cutoff: u64) -> Result<()> {
        Ok(self.protocol.prepare_freeze(successor, cutoff)?)
    }
    pub fn freeze(&mut self) -> Result<Freeze> {
        let frozen = self.protocol.freeze()?;
        // SAFETY: this publisher exclusively writes these atomic fields. Lifecycle
        // release follows all descriptor fields; no slot writes follow Frozen.
        unsafe {
            self.bank.store_u64_release(PAGE + 256, frozen.cutover)?;
            self.bank
                .store_u64_release(PAGE + 768, frozen.successor_ring_epoch)?;
            self.bank
                .store_u64_release(PAGE + 896, frozen.membership_version)?;
            self.bank.store_u64_release(LIFECYCLE, 4)?;
        }
        Ok(frozen)
    }
    pub fn detach(&mut self, evidence: Quiesced) -> Result<()> {
        let index = self.protocol.member_index(evidence.key())?;
        let cursor = self.cursors[index].as_ref().ok_or(Error::NotActive)?;
        // SAFETY: register_cursor bound this actual mapping to the authenticated
        // consumer. A pure state-machine token alone is not reclamation evidence.
        // Lifecycle acquire observes the consumer's final release/quiesced stores.
        unsafe {
            if cursor.mapping.load_u64_acquire(PAGE + 384)? != 5 {
                return Err(Error::NotQuiescing.into());
            }
            let actual = Watermarks {
                read: cursor.mapping.load_u64_acquire(PAGE)?,
                applied: cursor.mapping.load_u64_acquire(PAGE + 128)?,
                released: cursor.mapping.load_u64_acquire(RELEASED)?,
            };
            if actual != evidence.watermarks()
                || cursor.mapping.load_u64_acquire(PAGE + 896)? != actual.read
            {
                return Err(Error::Cursor.into());
            }
        }
        self.protocol.detach(evidence)?;
        self.cursors[index] = None;
        // SAFETY: this publisher alone writes membership.
        unsafe {
            self.bank
                .store_u64_release(PAGE + 384, self.protocol.membership_version())?;
        }
        Ok(())
    }
}

/// One fixed-thread consumer. Borrow guards cannot outlive it, and dropping a
/// guard advances release only through a fully completed prefix.
pub struct Subscriber<const B: usize> {
    header: ResourceHeader,
    bank: ReaderMapping,
    cursor: RefCell<WriterMapping>,
    protocol: RefCell<Reader<B>>,
}
impl<const B: usize> Subscriber<B> {
    /// # Safety
    /// The authenticated publisher must use the SDK's single-writer protocol and
    /// keep this exact subscription gated at baseline until this reader quiesces
    /// or its process and all registered holders exit. No writer may mutate the
    /// header, published slots, or reuse the bank for another generation.
    pub unsafe fn attach(
        fd: OwnedFd,
        header: ResourceHeader,
        sub: SubscriptionKey,
        baseline: u64,
    ) -> Result<Self> {
        validate_bank(&header)?;
        if sub.bank != key(&header) || baseline < header.first_seq - 1 {
            return Err(MappedError::Layout);
        }
        let bank = ReaderMapping::map(fd, header.resource_bytes as usize)?;
        verify_header(&bank, &header)?;
        // SAFETY: caller establishes the publisher and atomic discipline.
        let published = unsafe { bank.load_u64_acquire(PUBLISHED)? };
        if baseline > published {
            return Err(Error::Cursor.into());
        }
        let protocol = Reader::new(sub, baseline)?;
        let mut cursor = WriterMapping::create("tickwire-cursor", 8192)?;
        let mut h = cursor_header(&header, sub)?;
        let bytes = hash_header(&mut h)?;
        // SAFETY: cursor is private until initialized and sealed.
        unsafe {
            cursor.write_bytes(0, &bytes)?;
            for offset in [PAGE, PAGE + 128, RELEASED] {
                cursor.store_u64_release(offset, baseline)?;
            }
            cursor.store_u64_release(PAGE + 384, 3)?;
        }
        cursor.seal_for_readers()?;
        Ok(Self {
            header,
            bank,
            cursor: RefCell::new(cursor),
            protocol: RefCell::new(protocol),
        })
    }
    pub fn cursor_fd(&self) -> Result<OwnedFd> {
        Ok(self.cursor.borrow().duplicate_fd()?)
    }
    pub fn watermarks(&self) -> Watermarks {
        self.protocol.borrow().watermarks()
    }
    pub fn borrow(&self) -> Result<Option<RecordBorrow<'_, B>>> {
        // SAFETY: publisher exclusively writes a consistently aligned u64 atomic.
        let published = unsafe { self.bank.load_u64_acquire(PUBLISHED)? };
        let Some(lease) = self.protocol.borrow_mut().borrow_next(published)? else {
            return Ok(None);
        };
        let sequence = lease.sequence();
        let slot =
            ((sequence - self.header.first_seq) & u64::from(self.header.capacity - 1)) as usize;
        // SAFETY: acquire observed publication and this lease prevents its reuse.
        // The returned reference is exposed only through the guard's own lifetime.
        let bytes = unsafe {
            self.bank.read_bytes(
                SLOTS + slot * self.header.record_bytes as usize,
                self.header.record_bytes as usize,
            )?
        };
        if u64::from_le_bytes(bytes[..8].try_into().unwrap()) != sequence {
            self.start_quiesce();
            self.release(lease);
            return Err(MappedError::Layout);
        }
        // SAFETY: only this consumer writes its cursor u64 atomics.
        unsafe {
            self.cursor.borrow_mut().store_u64_release(PAGE, sequence)?;
            self.cursor
                .borrow_mut()
                .store_u64_release(PAGE + 512, self.protocol.borrow().outstanding() as u64)?;
        }
        Ok(Some(RecordBorrow {
            owner: self,
            lease: Some(lease),
            bytes: NonNull::from(bytes),
        }))
    }
    fn release(&self, lease: ReadLease) {
        let released = self
            .protocol
            .borrow_mut()
            .release(lease)
            .expect("SDK owns a valid live lease");
        // SAFETY: the guard has ended the last borrowed reference. Only this owner
        // writes released; release orders all ordinary reads before slot reuse.
        unsafe {
            self.cursor
                .borrow_mut()
                .store_u64_release(RELEASED, released)
        }
        .expect("validated cursor atomic offset");
        // SAFETY: diagnostics use their own aligned u64 field; never a release proof.
        unsafe {
            self.cursor
                .borrow_mut()
                .store_u64_release(PAGE + 512, self.protocol.borrow().outstanding() as u64)
        }
        .expect("validated cursor diagnostic offset");
    }
    pub fn apply(&self, sequence: u64) -> Result<u64> {
        let applied = self.protocol.borrow_mut().apply(sequence)?;
        // SAFETY: unique cursor writer; this field is exclusively an aligned u64.
        unsafe {
            self.cursor
                .borrow_mut()
                .store_u64_release(PAGE + 128, applied)?;
        }
        Ok(applied)
    }
    pub fn start_quiesce(&self) {
        if self.protocol.borrow().quiescing {
            return;
        }
        self.protocol.borrow_mut().start_quiesce();
        // SAFETY: unique consumer writer; no new borrow is allowed after this point.
        unsafe { self.cursor.borrow_mut().store_u64_release(PAGE + 384, 4) }
            .expect("validated cursor lifecycle offset");
    }
    pub fn quiesced(&self) -> Result<Quiesced> {
        let evidence = self.protocol.borrow().quiesced()?;
        // SAFETY: every guard has ended, quiescing prevents future borrows, and
        // lifecycle release follows the immutable final quiesced watermark.
        unsafe {
            let mut cursor = self.cursor.borrow_mut();
            cursor.store_u64_release(PAGE + 896, evidence.watermarks().read)?;
            cursor.store_u64_release(PAGE + 384, 5)?;
        }
        Ok(evidence)
    }
}
pub struct RecordBorrow<'a, const B: usize> {
    owner: &'a Subscriber<B>,
    lease: Option<ReadLease>,
    // Like std's read guards, retain a pointer, not an &'a [u8]: immutability
    // ends at Drop, which can occur before a by-value caller returns.
    bytes: NonNull<[u8]>,
}
impl<const B: usize> RecordBorrow<'_, B> {
    pub fn sequence(&self) -> u64 {
        self.lease.as_ref().unwrap().sequence()
    }
    /// The shared slice cannot escape the borrow guard.
    ///
    /// ```compile_fail,E0515
    /// use tickwire_shm::Subscriber;
    /// fn escape(reader: &Subscriber<64>) -> &[u8] {
    ///     let guard = reader.borrow().unwrap().unwrap();
    ///     guard.bytes()
    /// }
    /// ```
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: the live lease prevents reuse, owner keeps the mapping alive,
        // and the resulting reference is limited to this borrow of the guard.
        unsafe { self.bytes.as_ref() }
    }
}
impl<const B: usize> Drop for RecordBorrow<'_, B> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.owner.release(lease);
        }
    }
}
