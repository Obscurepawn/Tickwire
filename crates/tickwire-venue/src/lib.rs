//! Immutable, bounded exchange commands and facts with deterministic CBOR identities.
#![forbid(unsafe_code)]

mod account;
mod cbor;
mod codec;
mod types;
mod validate;
pub use account::*;
use codec::Codec;
use sha2::{Digest, Sha256};
pub use types::*;

pub type Hash = [u8; 32];
pub const SCHEMA_VERSION: u64 = 1;
pub const MAX_CANONICAL_BYTES: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Text,
    Decimal,
    IntegerOverflow,
    Truncated,
    NonCanonical,
    Schema,
    Field,
    Enum,
    TrailingData,
    Identity,
    Instrument,
    Deadline,
    Quantity,
    Price,
    OrderCombination,
    ProductCombination,
    Duplicate,
}

/// Construction checks structure and economics of parameters, never trading authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCommand {
    command: Box<Command>,
    bytes: Box<[u8]>,
    digest: Hash,
}
impl CanonicalCommand {
    pub fn new(command: Command) -> Result<Self, Error> {
        validate::command(&command)?;
        let bytes = encode("tickwire.venue-command", &command.context, &command.body);
        if bytes.len() > MAX_CANONICAL_BYTES {
            return Err(Error::Bounds);
        }
        let digest = Sha256::digest(&bytes).into();
        Ok(Self {
            command: Box::new(command),
            bytes: bytes.into_boxed_slice(),
            digest,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (context, body) = decode("tickwire.venue-command", bytes)?;
        let result = Self::new(Command { context, body })?;
        if result.bytes() != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(result)
    }
    pub fn command(&self) -> &Command {
        &self.command
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEvent {
    event: Box<VenueEvent>,
    bytes: Box<[u8]>,
    digest: Hash,
}
impl CanonicalEvent {
    pub fn new(event: VenueEvent) -> Result<Self, Error> {
        validate::event(&event)?;
        let version = if matches!(
            event.body,
            VenueEventBody::Amendment(_) | VenueEventBody::AmendmentRejected(_)
        ) {
            3
        } else if matches!(event.body, VenueEventBody::Structure(_)) {
            2
        } else {
            1
        };
        let bytes = encode_versioned(
            "tickwire.venue-event",
            version,
            &event.identity,
            &event.body,
        );
        if bytes.len() > MAX_CANONICAL_BYTES {
            return Err(Error::Bounds);
        }
        let digest = Sha256::digest(&bytes).into();
        Ok(Self {
            event: Box::new(event),
            bytes: bytes.into_boxed_slice(),
            digest,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (version, identity, body) = decode_versioned("tickwire.venue-event", bytes)?;
        let expected = if matches!(
            body,
            VenueEventBody::Amendment(_) | VenueEventBody::AmendmentRejected(_)
        ) {
            3
        } else if matches!(body, VenueEventBody::Structure(_)) {
            2
        } else {
            1
        };
        if version != expected {
            return Err(Error::Schema);
        }
        let result = Self::new(VenueEvent { identity, body })?;
        if result.bytes() != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(result)
    }
    pub fn event(&self) -> &VenueEvent {
        &self.event
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}

fn encode<A: Codec, B: Codec>(domain: &str, a: &A, b: &B) -> Vec<u8> {
    encode_versioned(domain, SCHEMA_VERSION, a, b)
}
fn encode_versioned<A: Codec, B: Codec>(domain: &str, version: u64, a: &A, b: &B) -> Vec<u8> {
    let mut w = cbor::Writer::new();
    w.array(4);
    w.text(domain);
    w.uint(version);
    a.write(&mut w);
    b.write(&mut w);
    w.finish()
}
fn decode<A: Codec, B: Codec>(domain: &str, bytes: &[u8]) -> Result<(A, B), Error> {
    let (version, a, b) = decode_versioned(domain, bytes)?;
    if version != SCHEMA_VERSION {
        return Err(Error::Schema);
    }
    Ok((a, b))
}
fn decode_versioned<A: Codec, B: Codec>(domain: &str, bytes: &[u8]) -> Result<(u64, A, B), Error> {
    if bytes.len() > MAX_CANONICAL_BYTES {
        return Err(Error::Bounds);
    }
    let mut r = cbor::Reader::new(bytes);
    r.array(4)?;
    if r.text(32)? != domain {
        return Err(Error::Schema);
    }
    let version = r.uint()?;
    let a = A::read(&mut r)?;
    let b = B::read(&mut r)?;
    r.end()?;
    Ok((version, a, b))
}
