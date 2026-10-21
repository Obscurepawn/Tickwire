use crate::{Error, Result};
use protobuf::Message as _;
use tickwire_control_raft::{Message, MessageType};
pub const HEADER_BYTES: usize = 128;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_ENTRIES: usize = 262_144;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Kind {
    Hello = 1,
    HelloAck = 2,
    Raft = 3,
    SnapshotBegin = 4,
    SnapshotChunk = 5,
    SnapshotEnd = 6,
    SnapshotApplied = 7,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub kind: Kind,
    pub cluster: [u8; 16],
    pub group: [u8; 16],
    pub from: u64,
    pub to: u64,
    pub incarnation: [u8; 16],
    pub challenge: [u8; 16],
    pub sequence: u64,
}
impl Header {
    fn validate(&self, length: usize) -> Result<()> {
        if self.cluster == [0; 16]
            || self.group == [0; 16]
            || self.from == 0
            || self.to == 0
            || self.from == self.to
            || self.incarnation == [0; 16]
            || self.challenge == [0; 16]
        {
            return Err(Error::Identity);
        }
        let maximum = if self.kind == Kind::Raft {
            MAX_PAYLOAD_BYTES
        } else if matches!(self.kind, Kind::Hello | Kind::HelloAck) {
            4096
        } else {
            65_536 + 256
        };
        if length > maximum {
            return Err(Error::Bounds);
        }
        if (!matches!(self.kind, Kind::Hello | Kind::HelloAck)
            && (self.sequence == 0 || length == 0))
            || (matches!(self.kind, Kind::Hello | Kind::HelloAck) && self.sequence != 0)
        {
            return Err(Error::Protocol);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: Header,
    pub payload: Vec<u8>,
}
impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.header.validate(self.payload.len())?;
        let mut bytes = vec![0; HEADER_BYTES + self.payload.len()];
        bytes[..8].copy_from_slice(b"TWCP0001");
        bytes[8..10].copy_from_slice(&1u16.to_le_bytes());
        bytes[10..12].copy_from_slice(&(HEADER_BYTES as u16).to_le_bytes());
        bytes[12..16].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        bytes[16..18].copy_from_slice(&(self.header.kind as u16).to_le_bytes());
        bytes[24..40].copy_from_slice(&self.header.cluster);
        bytes[40..56].copy_from_slice(&self.header.group);
        bytes[56..64].copy_from_slice(&self.header.from.to_le_bytes());
        bytes[64..72].copy_from_slice(&self.header.to.to_le_bytes());
        bytes[72..88].copy_from_slice(&self.header.incarnation);
        bytes[88..104].copy_from_slice(&self.header.challenge);
        bytes[104..112].copy_from_slice(&self.header.sequence.to_le_bytes());
        bytes[HEADER_BYTES..].copy_from_slice(&self.payload);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (header, length) = decode_header(bytes)?;
        if bytes.len() != HEADER_BYTES + length {
            return Err(Error::Protocol);
        }
        Ok(Self {
            header,
            payload: bytes[HEADER_BYTES..].to_vec(),
        })
    }
    pub fn raft(&self) -> Result<Message> {
        self.header.validate(self.payload.len())?;
        if self.header.kind != Kind::Raft {
            return Err(Error::Protocol);
        }
        preflight(&self.payload, false)?;
        let message = Message::parse_from_bytes(&self.payload).map_err(|_| Error::Protocol)?;
        if message.from != self.header.from || message.to != self.header.to {
            return Err(Error::Identity);
        }
        if !matches!(
            message.get_msg_type(),
            MessageType::MsgAppend
                | MessageType::MsgAppendResponse
                | MessageType::MsgRequestVote
                | MessageType::MsgRequestVoteResponse
                | MessageType::MsgRequestPreVote
                | MessageType::MsgRequestPreVoteResponse
                | MessageType::MsgHeartbeat
                | MessageType::MsgHeartbeatResponse
        ) {
            return Err(Error::Protocol);
        }
        Ok(message)
    }
}
fn decode_header(bytes: &[u8]) -> Result<(Header, usize)> {
    if bytes.len() < HEADER_BYTES
        || &bytes[..8] != b"TWCP0001"
        || bytes[8..12] != [1, 0, 128, 0]
        || bytes[18..24]
            .iter()
            .chain(bytes[112..128].iter())
            .any(|b| *b != 0)
    {
        return Err(Error::Protocol);
    }
    let length =
        u32::from_le_bytes(bytes[12..16].try_into().map_err(|_| Error::Protocol)?) as usize;
    let kind = match u16::from_le_bytes(bytes[16..18].try_into().map_err(|_| Error::Protocol)?) {
        1 => Kind::Hello,
        2 => Kind::HelloAck,
        3 => Kind::Raft,
        4 => Kind::SnapshotBegin,
        5 => Kind::SnapshotChunk,
        6 => Kind::SnapshotEnd,
        7 => Kind::SnapshotApplied,
        _ => return Err(Error::Protocol),
    };
    let header = Header {
        kind,
        cluster: bytes[24..40].try_into().map_err(|_| Error::Protocol)?,
        group: bytes[40..56].try_into().map_err(|_| Error::Protocol)?,
        from: u64::from_le_bytes(bytes[56..64].try_into().map_err(|_| Error::Protocol)?),
        to: u64::from_le_bytes(bytes[64..72].try_into().map_err(|_| Error::Protocol)?),
        incarnation: bytes[72..88].try_into().map_err(|_| Error::Protocol)?,
        challenge: bytes[88..104].try_into().map_err(|_| Error::Protocol)?,
        sequence: u64::from_le_bytes(bytes[104..112].try_into().map_err(|_| Error::Protocol)?),
    };
    header.validate(length)?;
    Ok((header, length))
}
pub struct Decoder {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Decoder {
    pub fn new(maximum: usize) -> Result<Self> {
        if !(1..=MAX_PAYLOAD_BYTES).contains(&maximum) {
            return Err(Error::Bounds);
        }
        Ok(Self {
            bytes: Vec::new(),
            maximum,
        })
    }
    pub fn remaining(&self) -> usize {
        self.maximum + HEADER_BYTES - self.bytes.len()
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.remaining() {
            return Err(Error::Bounds);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub fn next_frame(&mut self) -> Result<Option<Frame>> {
        if self.bytes.len() < HEADER_BYTES {
            return Ok(None);
        }
        let (_, length) = decode_header(&self.bytes)?;
        if length > self.maximum {
            return Err(Error::Bounds);
        }
        let end = HEADER_BYTES + length;
        if self.bytes.len() < end {
            return Ok(None);
        }
        let frame = Frame::decode(&self.bytes[..end])?;
        self.bytes.drain(..end);
        Ok(Some(frame))
    }
    pub fn buffered_len(&self) -> usize {
        self.bytes.len()
    }
}
// Validate tags, cardinality and nested allocation counts before protobuf can
// allocate repeated entries. There is no recursive/unknown message field here.
fn preflight(bytes: &[u8], entry: bool) -> Result<()> {
    let mut r = Reader(bytes);
    let mut seen = 0u32;
    let mut count = 0usize;
    if bytes.is_empty() {
        return Err(Error::Protocol);
    }
    while !r.0.is_empty() {
        let tag = r.varint()?;
        let field = tag >> 3;
        let wire = tag & 7;
        let max = if entry { 6 } else { 16 };
        if field == 0 || field > max || (!entry && field == 9) {
            return Err(Error::Protocol);
        }
        let repeated = !entry && field == 7;
        let bit = 1u32 << field;
        if !repeated && seen & bit != 0 {
            return Err(Error::Protocol);
        }
        seen |= bit;
        let bytes_field = if entry {
            matches!(field, 4 | 6)
        } else {
            matches!(field, 7 | 12)
        };
        if wire != if bytes_field { 2 } else { 0 } {
            return Err(Error::Protocol);
        }
        if bytes_field {
            let value = r.bytes()?;
            let max = if entry && field == 4 {
                16 * 1024 * 1024
            } else if repeated {
                16 * 1024 * 1024 + 512
            } else {
                256
            };
            if value.len() > max {
                return Err(Error::Bounds);
            }
            if repeated {
                count += 1;
                if count > MAX_ENTRIES {
                    return Err(Error::Bounds);
                }
                preflight(value, true)?;
            }
        } else {
            let value = r.varint()?;
            if (entry && field == 1 && !matches!(value, 0 | 2))
                || ((entry && field == 5 || !entry && field == 10) && value > 1)
            {
                return Err(Error::Protocol);
            }
        }
    }
    Ok(())
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for n in 0..10 {
            let (&b, rest) = self.0.split_first().ok_or(Error::Protocol)?;
            self.0 = rest;
            if n == 9 && b > 1 {
                return Err(Error::Protocol);
            }
            value |= u64::from(b & 127) << (n * 7);
            if b < 128 {
                if n > 0 && b == 0 {
                    return Err(Error::Protocol);
                }
                return Ok(value);
            }
        }
        Err(Error::Protocol)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.varint()?).map_err(|_| Error::Bounds)?;
        let (value, rest) = self.0.split_at_checked(len).ok_or(Error::Protocol)?;
        self.0 = rest;
        Ok(value)
    }
}
