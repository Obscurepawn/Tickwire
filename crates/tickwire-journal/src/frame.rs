use crate::Error;
use sha2::{Digest, Sha256};
pub const HEADER_BYTES: usize = 136;
pub const TRAILER_BYTES: usize = 40;
pub const FRAME_MAX_BYTES: usize = 1_048_576;
pub const MAGIC: &[u8; 8] = b"TWAL0001";
pub const END_MAGIC: &[u8; 4] = b"TEND";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FrameType {
    BatchBegin = 1,
    DataRecord = 2,
    RaftEntry = 3,
    RaftHardState = 4,
    ControlTruncateSuffix = 5,
    SnapshotPublish = 6,
    ControlConfState = 7,
    ControlCommitWatermark = 8,
    DependencyPin = 9,
    DependencyRelease = 10,
    SourceFreezeSeal = 11,
    BatchSeal = 12,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub frame_type: FrameType,
    pub flags: u16,
    pub stream_epoch: [u8; 16],
    pub domain: [u8; 16],
    pub segment_id: u64,
    pub lsn: u64,
    pub record_seq: u64,
    pub exec_epoch: u64,
    pub batch_number: u64,
    pub previous_hash: [u8; 32],
}
#[derive(Debug)]
pub struct DecodedFrame<'a> {
    pub header: Header,
    pub payload: &'a [u8],
    pub total_len: usize,
    pub hash: [u8; 32],
}
/// Castagnoli CRC-32C, reflected polynomial; no CPU-specific implementation.
pub fn crc32c(input: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in input {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn u16_at(b: &[u8], n: usize) -> u16 {
    u16::from_le_bytes(b[n..n + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], n: usize) -> u32 {
    u32::from_le_bytes(b[n..n + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], n: usize) -> u64 {
    u64::from_le_bytes(b[n..n + 8].try_into().unwrap())
}
fn kind(value: u16) -> Result<FrameType, Error> {
    Ok(match value {
        1 => FrameType::BatchBegin,
        2 => FrameType::DataRecord,
        3 => FrameType::RaftEntry,
        4 => FrameType::RaftHardState,
        5 => FrameType::ControlTruncateSuffix,
        6 => FrameType::SnapshotPublish,
        7 => FrameType::ControlConfState,
        8 => FrameType::ControlCommitWatermark,
        9 => FrameType::DependencyPin,
        10 => FrameType::DependencyRelease,
        11 => FrameType::SourceFreezeSeal,
        12 => FrameType::BatchSeal,
        _ => return Err(Error::Quarantined("unknown frame type")),
    })
}
pub fn encode(header: &Header, payload: &[u8]) -> Result<Vec<u8>, Error> {
    if header.flags != 0 {
        return Err(Error::Invalid("unsupported flags"));
    }
    if payload.len() > FRAME_MAX_BYTES - HEADER_BYTES - TRAILER_BYTES {
        return Err(Error::Invalid("frame too large"));
    }
    let total = (HEADER_BYTES + payload.len() + TRAILER_BYTES + 7) & !7;
    let tail = total - TRAILER_BYTES;
    let mut b = vec![0u8; total];
    b[..8].copy_from_slice(MAGIC);
    b[8..10].copy_from_slice(&1u16.to_le_bytes());
    b[10..12].copy_from_slice(&(HEADER_BYTES as u16).to_le_bytes());
    b[12..16].copy_from_slice(&(total as u32).to_le_bytes());
    b[16..18].copy_from_slice(&(header.frame_type as u16).to_le_bytes());
    b[18..20].copy_from_slice(&header.flags.to_le_bytes());
    b[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    b[24..40].copy_from_slice(&header.stream_epoch);
    b[40..56].copy_from_slice(&header.domain);
    for (n, value) in [
        (56, header.segment_id),
        (64, header.lsn),
        (72, header.record_seq),
        (80, header.exec_epoch),
        (88, header.batch_number),
    ] {
        b[n..n + 8].copy_from_slice(&value.to_le_bytes());
    }
    b[96..128].copy_from_slice(&header.previous_hash);
    b[132..136].copy_from_slice(&crc32c(payload).to_le_bytes());
    let header_crc = crc32c(&b[..HEADER_BYTES]);
    b[128..132].copy_from_slice(&header_crc.to_le_bytes());
    b[HEADER_BYTES..HEADER_BYTES + payload.len()].copy_from_slice(payload);
    b[tail + 32..tail + 36].copy_from_slice(&(total as u32).to_le_bytes());
    b[tail + 36..].copy_from_slice(END_MAGIC);
    let mut hash = Sha256::new();
    hash.update(&b[..tail]);
    hash.update(&b[tail + 32..]);
    b[tail..tail + 32].copy_from_slice(&hash.finalize());
    Ok(b)
}
/// Validate the complete header even if the frame body is not yet available.
pub(crate) fn decode_header(input: &[u8]) -> Result<(Header, usize, usize), Error> {
    if input.len() < HEADER_BYTES {
        return Err(Error::Quarantined("short header"));
    }
    let b = &input[..HEADER_BYTES];
    if &b[..8] != MAGIC || u16_at(b, 8) != 1 || usize::from(u16_at(b, 10)) != HEADER_BYTES {
        return Err(Error::Quarantined("magic or version"));
    }
    let total = u32_at(b, 12) as usize;
    let payload = u32_at(b, 20) as usize;
    if !(HEADER_BYTES + TRAILER_BYTES..=FRAME_MAX_BYTES).contains(&total)
        || !total.is_multiple_of(8)
        || payload > total - HEADER_BYTES - TRAILER_BYTES
        || total - HEADER_BYTES - TRAILER_BYTES - payload >= 8
        || u16_at(b, 18) != 0
    {
        return Err(Error::Quarantined("length or flags"));
    }
    let mut crc_header = [0; HEADER_BYTES];
    crc_header.copy_from_slice(b);
    crc_header[128..132].fill(0);
    if crc32c(&crc_header) != u32_at(b, 128) {
        return Err(Error::Quarantined("header CRC"));
    }
    Ok((
        Header {
            frame_type: kind(u16_at(b, 16))?,
            flags: 0,
            stream_epoch: b[24..40].try_into().unwrap(),
            domain: b[40..56].try_into().unwrap(),
            segment_id: u64_at(b, 56),
            lsn: u64_at(b, 64),
            record_seq: u64_at(b, 72),
            exec_epoch: u64_at(b, 80),
            batch_number: u64_at(b, 88),
            previous_hash: b[96..128].try_into().unwrap(),
        },
        total,
        payload,
    ))
}
pub fn decode(input: &[u8]) -> Result<DecodedFrame<'_>, Error> {
    let (header, total, payload_len) = decode_header(input)?;
    if input.len() < total {
        return Err(Error::Quarantined("short frame"));
    }
    let b = &input[..total];
    let tail = total - TRAILER_BYTES;
    let payload = &b[HEADER_BYTES..HEADER_BYTES + payload_len];
    if b[HEADER_BYTES + payload_len..tail].iter().any(|&n| n != 0) {
        return Err(Error::Quarantined("nonzero padding"));
    }
    if crc32c(payload) != u32_at(b, 132) {
        return Err(Error::Quarantined("payload CRC"));
    }
    if u32_at(b, tail + 32) as usize != total || &b[tail + 36..] != END_MAGIC {
        return Err(Error::Quarantined("trailer length or magic"));
    }
    let mut hash = Sha256::new();
    hash.update(&b[..tail]);
    hash.update(&b[tail + 32..]);
    let hash: [u8; 32] = hash.finalize().into();
    if b[tail..tail + 32] != hash {
        return Err(Error::Quarantined("frame SHA256"));
    }
    Ok(DecodedFrame {
        header,
        payload,
        total_len: total,
        hash,
    })
}
