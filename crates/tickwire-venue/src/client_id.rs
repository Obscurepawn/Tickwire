//! Self-describing, domain-authenticated 32-character `[0-9a-z]` `ClientId` encoding.

use crate::{ClientId, CommandContext, Environment, Error, Product};
use sha2::{Digest, Sha256};

pub const SELF_DESCRIBING_CLIENT_ID_LEN: usize = 32;
pub const REPLACEMENT_ORDER_LEG: u8 = 1;
pub const PARENT_STRUCTURE_LEG: u8 = 15;

const PREFIX: &[u8; 2] = b"t1";
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
const TAG_MASK: u64 = (1u64 << 34) - 1;
const MAX_INTENT_ID: u64 = (1u64 << 63) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIdDomain {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub product: Product,
}

impl ClientIdDomain {
    pub fn from_command_context(ctx: &CommandContext) -> Self {
        let r = &ctx.request;
        Self {
            risk_domain_id: r.risk_domain_id,
            venue: r.venue,
            environment: r.environment,
            account: r.account,
            product: r.product,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfDescribingClientId {
    pub logical_strategy_id: u16,
    pub leg: u8,
    pub epoch: u32,
    pub intent_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedClientId {
    Managed(SelfDescribingClientId),
    External,
}

impl SelfDescribingClientId {
    pub fn from_command_context(ctx: &CommandContext, leg: u8) -> Result<Self, Error> {
        let logical_strategy_id =
            u16::try_from(ctx.strategy.logical_strategy_id).map_err(|_| Error::Bounds)?;
        let epoch = u32::try_from(ctx.request.intent_exec_epoch).map_err(|_| Error::Bounds)?;
        let candidate = Self {
            logical_strategy_id,
            leg,
            epoch,
            intent_id: ctx.request.intent_id,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.logical_strategy_id == 0 || self.epoch == 0 || self.intent_id == 0 {
            return Err(Error::Identity);
        }
        if self.leg > 15 || self.intent_id > MAX_INTENT_ID {
            return Err(Error::Bounds);
        }
        Ok(())
    }

    pub fn encode(&self, domain: &ClientIdDomain) -> Result<ClientId, Error> {
        self.validate()?;
        let tag_34 = compute_tag_34(domain, self);
        let upper_116 = ((self.logical_strategy_id as u128) << 100)
            | ((self.leg as u128) << 96)
            | ((self.epoch as u128) << 64)
            | (self.intent_id as u128);

        let mut buf = [0u8; SELF_DESCRIBING_CLIENT_ID_LEN];
        buf[..2].copy_from_slice(PREFIX);
        for i in 0..30 {
            let idx = if i < 23 {
                ((upper_116 >> (111 - 5 * i)) & 0x1f) as usize
            } else if i == 23 {
                ((((upper_116 & 1) as usize) << 4) | (((tag_34 >> 30) & 0x0f) as usize)) & 0x1f
            } else {
                ((tag_34 >> (25 - 5 * (i - 24))) & 0x1f) as usize
            };
            buf[2 + i] = ALPHABET[idx];
        }
        let s = core::str::from_utf8(&buf).map_err(|_| Error::Text)?;
        ClientId::new(s)
    }

    pub fn decode(client_id: &ClientId, domain: &ClientIdDomain) -> DecodedClientId {
        Self::decode_str(client_id.as_str(), domain)
    }

    pub fn decode_str(raw: &str, domain: &ClientIdDomain) -> DecodedClientId {
        let bytes = raw.as_bytes();
        if bytes.len() != SELF_DESCRIBING_CLIENT_ID_LEN || &bytes[..2] != PREFIX {
            return DecodedClientId::External;
        }
        let mut upper_116 = 0u128;
        let mut tag_34 = 0u64;
        for (i, &b) in bytes[2..].iter().enumerate() {
            let Some(v) = decode_base32_byte(b) else {
                return DecodedClientId::External;
            };
            if i < 23 {
                upper_116 |= (v as u128) << (111 - 5 * i);
            } else if i == 23 {
                upper_116 |= ((v >> 4) & 1) as u128;
                tag_34 |= ((v & 0x0f) as u64) << 30;
            } else {
                tag_34 |= (v as u64) << (25 - 5 * (i - 24));
            }
        }
        let candidate = Self {
            logical_strategy_id: ((upper_116 >> 100) & 0xffff) as u16,
            leg: ((upper_116 >> 96) & 0x0f) as u8,
            epoch: ((upper_116 >> 64) & 0xffff_ffff) as u32,
            intent_id: (upper_116 & 0xffff_ffff_ffff_ffff) as u64,
        };
        if candidate.validate().is_err() {
            return DecodedClientId::External;
        }
        if compute_tag_34(domain, &candidate) != tag_34 {
            return DecodedClientId::External;
        }
        DecodedClientId::Managed(candidate)
    }

    pub fn deterministic_order_id(&self, domain: &ClientIdDomain) -> [u8; 16] {
        let mut hasher = Sha256::new();
        hasher.update(b"tickwire.client-order-uuid.v1");
        hash_domain_and_fields(&mut hasher, domain, self);
        let digest = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[..16]);
        if out == [0; 16] {
            out[0] = 1;
        }
        out
    }
}

fn compute_tag_34(domain: &ClientIdDomain, id: &SelfDescribingClientId) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"tickwire.client-id.v1");
    hash_domain_and_fields(&mut hasher, domain, id);
    let digest = hasher.finalize();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(head) & TAG_MASK
}

fn hash_domain_and_fields(
    hasher: &mut Sha256,
    domain: &ClientIdDomain,
    id: &SelfDescribingClientId,
) {
    hasher.update(domain.risk_domain_id);
    hasher.update(domain.venue.to_le_bytes());
    hasher.update([match domain.environment {
        Environment::Production => 1,
        Environment::Test => 2,
    }]);
    hasher.update(domain.account.to_le_bytes());
    hasher.update([match domain.product {
        Product::Spot => 1,
        Product::LinearPerpetual => 2,
        Product::InversePerpetual => 3,
        Product::Option => 4,
        Product::LinearFuture => 5,
        Product::InverseFuture => 6,
    }]);
    hasher.update(id.logical_strategy_id.to_le_bytes());
    hasher.update([id.leg]);
    hasher.update(id.epoch.to_le_bytes());
    hasher.update(id.intent_id.to_le_bytes());
}

const fn decode_base32_byte(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'h' => Some(10 + (b - b'a')),
        b'j' | b'k' => Some(18 + (b - b'j')),
        b'm' | b'n' => Some(20 + (b - b'm')),
        b'p'..=b't' => Some(22 + (b - b'p')),
        b'v'..=b'z' => Some(27 + (b - b'v')),
        _ => None,
    }
}
