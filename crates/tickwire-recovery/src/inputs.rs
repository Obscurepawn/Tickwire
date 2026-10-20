use crate::*;
const HEADER: usize = 68;
const MAX_RECORDS: u32 = 1_000_000;
pub struct InputRecord<'a> {
    pub kind: u8,
    pub payload: &'a [u8],
}
pub struct EncodedInputs {
    pub bytes: Vec<u8>,
    pub boundary: InputBoundary,
}
impl EncodedInputs {
    pub fn decode(bytes: Vec<u8>) -> Result<Self> {
        let boundary = inspect(&bytes)?;
        Ok(Self { bytes, boundary })
    }
}
fn chain(previous: Hash, row: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update(b"tickwire.input-row.v1");
    h.update(previous);
    h.update(row);
    h.finalize().into()
}
/// Called by the trusted sequencer, preserving its actual input order.
pub fn encode_inputs(
    stream: [u8; 16],
    first_sequence: u64,
    previous_hash: Hash,
    rows: &[InputRecord<'_>],
) -> Result<EncodedInputs> {
    if stream == [0; 16]
        || first_sequence == 0
        || first_sequence >= 1 << 63
        || rows.len() > MAX_RECORDS as usize
    {
        return Err(Error::Invalid("input identity or count"));
    }
    let mut size = HEADER;
    for row in rows {
        if !(1..=5).contains(&row.kind) || row.payload.len() > u32::MAX as usize {
            return Err(Error::Invalid("input row"));
        }
        size = size
            .checked_add(13)
            .and_then(|n| n.checked_add(row.payload.len()))
            .ok_or(Error::Capacity)?;
        if size > MAX_OBJECT_BYTES {
            return Err(Error::Capacity);
        }
    }
    let last_sequence = (first_sequence - 1)
        .checked_add(rows.len() as u64)
        .filter(|n| *n < 1 << 63)
        .ok_or(Error::Capacity)?;
    let mut bytes = Vec::with_capacity(size);
    bytes.extend(b"TWINP001");
    bytes.extend(stream);
    bytes.extend(first_sequence.to_be_bytes());
    bytes.extend((rows.len() as u32).to_be_bytes());
    bytes.extend(previous_hash);
    let mut last_hash = previous_hash;
    for (index, row) in rows.iter().enumerate() {
        let offset = bytes.len();
        bytes.extend((first_sequence + index as u64).to_be_bytes());
        bytes.push(row.kind);
        bytes.extend((row.payload.len() as u32).to_be_bytes());
        bytes.extend(row.payload);
        last_hash = chain(last_hash, &bytes[offset..]);
    }
    Ok(EncodedInputs {
        bytes,
        boundary: InputBoundary {
            stream,
            first_sequence,
            previous_hash,
            last_sequence,
            last_hash,
        },
    })
}
pub(crate) fn inspect(raw: &[u8]) -> Result<InputBoundary> {
    if raw.len() < HEADER || raw.len() > MAX_OBJECT_BYTES || raw[..8] != *b"TWINP001" {
        return Err(Error::Invalid("input header"));
    }
    let stream = raw[8..24].try_into().unwrap();
    let first = u64::from_be_bytes(raw[24..32].try_into().unwrap());
    let count = u32::from_be_bytes(raw[32..36].try_into().unwrap());
    let previous_hash: Hash = raw[36..68].try_into().unwrap();
    if stream == [0; 16] || first == 0 || first >= 1 << 63 || count > MAX_RECORDS {
        return Err(Error::Invalid("input identity/count"));
    }
    let last_sequence = (first - 1)
        .checked_add(count as u64)
        .filter(|v| *v < 1 << 63)
        .ok_or(Error::Capacity)?;
    let mut pos = HEADER;
    let mut last_hash = previous_hash;
    for index in 0..count {
        let row = raw
            .get(pos..pos + 13)
            .ok_or(Error::Invalid("input truncated row"))?;
        if u64::from_be_bytes(row[..8].try_into().unwrap()) != first + index as u64
            || !(1..=5).contains(&row[8])
        {
            return Err(Error::Invalid("input sequence/type"));
        }
        let size = u32::from_be_bytes(row[9..13].try_into().unwrap()) as usize;
        let end = pos
            .checked_add(13)
            .and_then(|v| v.checked_add(size))
            .ok_or(Error::Capacity)?;
        let row = raw
            .get(pos..end)
            .ok_or(Error::Invalid("input truncated payload"))?;
        last_hash = chain(last_hash, row);
        pos = end;
    }
    if pos != raw.len() {
        return Err(Error::Invalid("input trailing data"));
    }
    Ok(InputBoundary {
        stream,
        first_sequence: first,
        previous_hash,
        last_sequence,
        last_hash,
    })
}
