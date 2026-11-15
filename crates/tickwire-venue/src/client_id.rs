//! Self-describing, domain-authenticated 32-character `[0-9a-z]` `ClientId` encoding (`tw1...`).

use crate::{ClientId, CommandContext, Environment, Error, Product};
use sha2::{Digest, Sha256};

pub const SELF_DESCRIBING_CLIENT_ID_LEN: usize = 32;
pub const REPLACEMENT_ORDER_LEG: u8 = 1;
pub const PARENT_STRUCTURE_LEG: u8 = 15;
pub const ALL_ORS_STRATS: u8 = u8::MAX;

const PREFIX: &[u8; 3] = b"tw1";
const BASE36_LEN: usize = 29;
const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const TAG_MASK: u64 = (1u64 << 30) - 1;
const MAX_INTENT_ID: u64 = (1u64 << 63) - 1;
const TOP_LIMB_LIMIT: u64 = 1u64 << 18;

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

    /// Constructs a `SelfDescribingClientId` for an ORS strategy (`strat_id` in `0..=254`,
    /// rejecting `255 = ALL_ORS_STRATS`), mapping `logical_strategy_id = strat_id + 1`.
    pub fn from_ors_order(
        strat_id: u8,
        leg: u8,
        epoch: u32,
        order_seqnum: u64,
    ) -> Result<Self, Error> {
        if strat_id == ALL_ORS_STRATS {
            return Err(Error::Identity);
        }
        let candidate = Self {
            logical_strategy_id: u16::from(strat_id) + 1,
            leg,
            epoch,
            intent_id: order_seqnum,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    /// Returns the 0-based ORS `strat_id` (`0..=254`) when `logical_strategy_id` is in `1..=255`.
    #[inline]
    pub const fn ors_strat_id(&self) -> Option<u8> {
        if self.logical_strategy_id >= 1 && self.logical_strategy_id <= 255 {
            Some((self.logical_strategy_id - 1) as u8)
        } else {
            None
        }
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
        let tag_30 = compute_tag_30(domain, self);
        let upper_116 = ((self.logical_strategy_id as u128) << 100)
            | ((self.leg as u128) << 96)
            | ((self.epoch as u128) << 64)
            | (self.intent_id as u128);

        let mut limbs = [
            ((upper_116 as u64) << 30) | (tag_30 & TAG_MASK),
            ((upper_116 >> 34) & u128::from(u64::MAX)) as u64,
            (upper_116 >> 98) as u64,
        ];

        let mut buf = [0u8; SELF_DESCRIBING_CLIENT_ID_LEN];
        buf[..3].copy_from_slice(PREFIX);
        for pos in (0..BASE36_LEN).rev() {
            let mut rem = 0u128;
            for limb in limbs.iter_mut().rev() {
                let cur = (rem << 64) | u128::from(*limb);
                *limb = (cur / 36) as u64;
                rem = cur % 36;
            }
            buf[3 + pos] = ALPHABET[rem as usize];
        }
        debug_assert_eq!(limbs, [0, 0, 0]);
        let s = core::str::from_utf8(&buf).map_err(|_| Error::Text)?;
        ClientId::new(s)
    }

    pub fn decode(client_id: &ClientId, domain: &ClientIdDomain) -> DecodedClientId {
        Self::decode_str(client_id.as_str(), domain)
    }

    pub fn decode_str(raw: &str, domain: &ClientIdDomain) -> DecodedClientId {
        let bytes = raw.as_bytes();
        if bytes.len() != SELF_DESCRIBING_CLIENT_ID_LEN || &bytes[..3] != PREFIX {
            return DecodedClientId::External;
        }
        let mut limbs = [0u64; 3];
        for &b in &bytes[3..] {
            let Some(digit) = decode_base36_byte(b) else {
                return DecodedClientId::External;
            };
            let mut carry = u128::from(digit);
            for limb in &mut limbs {
                let cur = u128::from(*limb) * 36 + carry;
                *limb = cur as u64;
                carry = cur >> 64;
            }
            if carry != 0 {
                return DecodedClientId::External;
            }
        }
        if limbs[2] >= TOP_LIMB_LIMIT {
            return DecodedClientId::External;
        }
        let tag_30 = limbs[0] & TAG_MASK;
        let upper_116 = (u128::from(limbs[2]) << 98)
            | (u128::from(limbs[1]) << 34)
            | u128::from(limbs[0] >> 30);

        let candidate = Self {
            logical_strategy_id: ((upper_116 >> 100) & 0xffff) as u16,
            leg: ((upper_116 >> 96) & 0x0f) as u8,
            epoch: ((upper_116 >> 64) & 0xffff_ffff) as u32,
            intent_id: (upper_116 & 0xffff_ffff_ffff_ffff) as u64,
        };
        if candidate.validate().is_err() {
            return DecodedClientId::External;
        }
        if compute_tag_30(domain, &candidate) != tag_30 {
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

fn compute_tag_30(domain: &ClientIdDomain, id: &SelfDescribingClientId) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"tickwire.client-id.tw1");
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
    hasher.update([domain.product.code()]);
    hasher.update(id.logical_strategy_id.to_le_bytes());
    hasher.update([id.leg]);
    hasher.update(id.epoch.to_le_bytes());
    hasher.update(id.intent_id.to_le_bytes());
}

const fn decode_base36_byte(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'z' => Some(10 + (b - b'a')),
        _ => None,
    }
}
