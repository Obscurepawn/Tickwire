//! Exact immutable input-WAL prefix referenced by account records and final seals.
use crate::*;
use std::collections::BTreeMap;
use tickwire_recovery::digest;
pub const INPUT_PREFIX_MAGIC: &[u8; 8] = b"TWIPFX01";
pub const MAX_INPUT_RECORDS: usize = 16384;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputEntry {
    pub sequence: u64,
    pub writer_epoch: u64,
    pub object: ObjectRef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputPrefix {
    pub input_stream: [u8; 16],
    pub account_stream: [u8; 16],
    pub baseline: Hash,
    pub through_sequence: u64,
    pub entries: Vec<InputEntry>,
}
impl InputPrefix {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const HEADER: usize = 84;
        const ENTRY: usize = 56;
        if bytes.len() < HEADER || bytes.get(..8) != Some(INPUT_PREFIX_MAGIC) {
            return Err(Error::Invalid("input prefix"));
        }
        let input_stream = bytes[8..24].try_into().unwrap();
        let account_stream = bytes[24..40].try_into().unwrap();
        let baseline = bytes[40..72].try_into().unwrap();
        let through_sequence = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[80..84].try_into().unwrap()) as usize;
        if count == 0
            || count > MAX_INPUT_RECORDS
            || count as u64 != through_sequence
            || bytes.len() != HEADER + count * ENTRY
            || input_stream == [0; 16]
            || account_stream == [0; 16]
            || baseline == [0; 32]
        {
            return Err(Error::Bounds);
        }
        let mut entries = Vec::with_capacity(count);
        let mut epoch = 0;
        for (index, b) in bytes[HEADER..].as_chunks::<ENTRY>().0.iter().enumerate() {
            let sequence = u64::from_le_bytes(b[..8].try_into().unwrap());
            let writer_epoch = u64::from_le_bytes(b[8..16].try_into().unwrap());
            let object = ObjectRef {
                bytes: u64::from_le_bytes(b[16..24].try_into().unwrap()),
                hash: b[24..56].try_into().unwrap(),
            };
            if sequence != index as u64 + 1
                || writer_epoch == 0
                || writer_epoch < epoch
                || object.bytes == 0
                || object.bytes > tickwire_journal::BATCH_MAX_BYTES as u64
                || object.hash == [0; 32]
            {
                return Err(Error::Invalid("input prefix entry"));
            }
            epoch = writer_epoch;
            entries.push(InputEntry {
                sequence,
                writer_epoch,
                object,
            });
        }
        Ok(Self {
            input_stream,
            account_stream,
            baseline,
            through_sequence,
            entries,
        })
    }
    fn bind(&self, stream: &Stream) -> Result<()> {
        if !matches!(stream.format,Format::Account{baseline}if baseline==self.baseline)
            || self.account_stream != stream.journal_epoch
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
pub(crate) fn input_dependencies<S: BlobStore>(
    p: &Persistence<S>,
    stream: &Stream,
    roots: &[ObjectRef],
) -> Result<(Vec<ObjectRef>, Vec<InputPrefix>)> {
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    for root in roots {
        let bytes = p.read_object(root.hash, root.bytes as usize)?;
        if bytes.len() as u64 != root.bytes {
            return Err(Error::Conflict);
        }
        if bytes.starts_with(INPUT_PREFIX_MAGIC) {
            let prefix = InputPrefix::decode(&bytes)?;
            prefix.bind(stream)?;
            if prefixes
                .iter()
                .any(|old: &InputPrefix| old.input_stream == prefix.input_stream)
            {
                return Err(Error::Conflict);
            }
            objects.extend(prefix.entries.iter().map(|e| e.object.clone()));
            prefixes.push(prefix);
        }
    }
    Ok((objects, prefixes))
}
pub(crate) fn verify_input_dependencies<S: BlobStore>(
    p: &mut Persistence<S>,
    stream: &Stream,
    payload: &[u8],
    writer_epoch: u64,
    roots: &[ObjectRef],
) -> Result<()> {
    let (objects, prefixes) = input_dependencies(p, stream, roots)?;
    crate::source::verify_objects(p, &objects)?;
    if !matches!(stream.format, Format::Account { .. }) {
        return Ok(());
    }
    let record = tickwire_account_codec::CanonicalRecord::decode(payload)
        .map_err(|_| Error::Invalid("account record"))?;
    if let tickwire_account::RecoveryRecord::UncreatedAlgoClosed(b) = record.record() {
        let object = roots
            .iter()
            .find(|o| o.hash == b.coverage_digest)
            .ok_or(Error::Missing)?;
        let bytes = p.read_object(object.hash, object.bytes as usize)?;
        let dependencies =
            tickwire_binance_account::reconciliation::structure_closure::coverage_dependencies(
                b,
                &bytes,
                writer_epoch,
                stream.journal_epoch,
            )
            .map_err(|_| Error::Conflict)?;
        let prefix = prefixes
            .iter()
            .find(|p| {
                p.input_stream == dependencies.input_stream
                    && p.through_sequence >= dependencies.through_sequence
            })
            .ok_or(Error::Missing)?;
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        digest.update(b"tickwire.binance.input-closure-prefix.v1");
        digest.update(&dependencies.input_stream);
        digest.update(&dependencies.through_sequence.to_le_bytes());
        for e in prefix
            .entries
            .iter()
            .take(dependencies.through_sequence as usize)
        {
            digest.update(&e.sequence.to_le_bytes());
            digest.update(&e.object.bytes.to_le_bytes());
            digest.update(&e.object.hash);
        }
        if digest.finish().as_ref() != dependencies.prefix_digest {
            return Err(Error::Conflict);
        }
        for (seq, len, hash) in dependencies.references {
            let e = prefix.entries.get(seq as usize - 1).ok_or(Error::Missing)?;
            if e.object.bytes != len || e.object.hash != hash {
                return Err(Error::Conflict);
            }
        }
    }
    let required = match record.record() {
        tickwire_account::RecoveryRecord::IngressEvent {
            input_stream,
            input_sequence,
            ..
        }
        | tickwire_account::RecoveryRecord::IngressAccountInput {
            input_stream,
            input_sequence,
            ..
        } => Some((*input_stream, *input_sequence)),
        tickwire_account::RecoveryRecord::IncomeObservationsAssociated(b) => {
            Some((b.input_stream, b.through_sequence))
        }
        tickwire_account::RecoveryRecord::UncreatedAlgoClosed(b) => {
            Some((b.input_stream, b.through_sequence))
        }
        tickwire_account::RecoveryRecord::AccountInputsReconciled(b) => {
            Some((b.input_stream, b.through_sequence))
        }
        tickwire_account::RecoveryRecord::IncomeBaseline(b) => {
            Some((b.input_stream, b.through_sequence))
        }
        tickwire_account::RecoveryRecord::Reactivated(r) => {
            Some((r.input_stream, r.through_sequence))
        }
        _ => None,
    };
    if let Some((stream, sequence)) = required
        && (sequence == 0
            || !prefixes
                .iter()
                .any(|p| p.input_stream == stream && p.through_sequence >= sequence))
    {
        return Err(Error::Missing);
    }
    Ok(())
}
pub(crate) fn advance_heads<S: BlobStore>(
    p: &Persistence<S>,
    stream: &Stream,
    old: &BTreeMap<[u8; 16], InputPrefix>,
    roots: impl Iterator<Item = Vec<ObjectRef>>,
) -> Result<BTreeMap<[u8; 16], InputPrefix>> {
    let mut heads = old.clone();
    for roots in roots {
        let (_, prefixes) = input_dependencies(p, stream, &roots)?;
        for prefix in prefixes {
            if let Some(previous) = heads.get(&prefix.input_stream) {
                let common = previous.entries.len().min(prefix.entries.len());
                if previous.entries[..common] != prefix.entries[..common] {
                    return Err(Error::Conflict);
                }
                if previous.through_sequence >= prefix.through_sequence {
                    continue;
                }
            }
            heads.insert(prefix.input_stream, prefix);
            if heads.len() > 32 {
                return Err(Error::Bounds);
            }
        }
    }
    Ok(heads)
}
pub(crate) fn verify_closed_input<S: tickwire_journal::store::Store, P: BlobStore>(
    closed: &mut ClosedJournal<S>,
    p: &mut Persistence<P>,
    stream: &Stream,
    root: &ObjectRef,
) -> Result<()> {
    let (bytes, prefixes) = input_dependencies(p, stream, std::slice::from_ref(root))?;
    let prefix = prefixes.first().ok_or(Error::Missing)?;
    if prefixes.len() != 1
        || closed.config().stream_epoch != prefix.input_stream
        || closed.config().domain != stream.domain
        || closed.durable_frontier().map(|p| p.last_seq) != Some(prefix.through_sequence)
    {
        return Err(Error::Conflict);
    }
    crate::source::verify_objects(p, &bytes)?;
    let mut next = 1;
    while next <= prefix.through_sequence {
        let export = closed.export_durable(next, MAX_BATCH_RECORDS, MAX_BATCH_BYTES)?;
        if export.records().is_empty() {
            return Err(Error::Missing);
        }
        for r in export.records() {
            let e = &prefix.entries[(r.seq - 1) as usize];
            if e.writer_epoch != r.exec_epoch
                || e.object.hash != digest(&r.payload)
                || e.object.bytes != r.payload.len() as u64
            {
                return Err(Error::Conflict);
            }
            next = r.seq + 1;
        }
    }
    Ok(())
}
