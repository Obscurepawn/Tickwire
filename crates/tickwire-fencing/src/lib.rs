//! Authenticated attestations from a separately trusted isolation controller.
//! Verifying a signature trusts that controller's device readback; it never
//! derives isolation from a timeout, process number or an execution lease.
#![forbid(unsafe_code)]
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_clock::MonoTime;

pub type Hash = [u8; 32];
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Identity,
    Signature,
    Expired,
    Capacity,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionIdentity {
    pub host_id: [u8; 16],
    pub boot_id: [u8; 16],
    pub exec_instance_id: [u8; 16],
    pub exec_epoch: u64,
    pub network_identity: Hash,
    pub credential_access_identity: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub risk_domain: [u8; 16],
    pub venue: u32,
    pub account: u64,
    /// 1 Spot, 2 linear perpetual, 3 linear future.
    pub product: u8,
    /// 1 production, 2 test.
    pub environment: u8,
}
impl Scope {
    pub fn validate(&self) -> Result<()> {
        if self.risk_domain == [0; 16]
            || self.venue != 1
            || self.account == 0
            || !(1..=3).contains(&self.product)
            || !(1..=2).contains(&self.environment)
        {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Validity {
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub issued_ns: u64,
    pub expires_ns: u64,
}
impl Validity {
    pub fn check(&self, now: MonoTime) -> Result<()> {
        if self.clock_boot_id == [0; 16]
            || self.clock_generation == 0
            || self.issued_ns >= self.expires_ns
        {
            return Err(Error::Invalid);
        }
        if now.domain().boot_id() != self.clock_boot_id
            || now.domain().generation() != self.clock_generation
        {
            return Err(Error::Identity);
        }
        if now.nanos() < self.issued_ns || now.nanos() >= self.expires_ns {
            return Err(Error::Expired);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceClaims {
    pub schema: u32,
    pub scope: Scope,
    pub old_identity: ExecutionIdentity,
    pub controller_id: [u8; 16],
    pub operation_id: [u8; 16],
    pub control_commit: u64,
    pub all_egress_hash: Hash,
    pub device_action_id: [u8; 16],
    pub device_revision: u64,
    pub deny_tombstone_hash: Hash,
    pub readback_hash: Hash,
    pub probe_hash: Hash,
    pub all_egress_blocked: bool,
    pub existing_sessions_revoked: bool,
    pub credential_access_revoked: bool,
    pub validity: Validity,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Query,
    Cancel,
    CancelList,
    CancelAlgo,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmergencyClaims {
    pub schema: u32,
    pub scope: Scope,
    pub guardian_id: [u8; 16],
    pub controller_id: [u8; 16],
    pub grant_id: [u8; 16],
    pub control_commit: u64,
    pub precommitted_ns: u64,
    /// Explicit operator invocation is an allowed trigger, not a fence proof.
    pub manual_activation_allowed: bool,
    pub instruments: Vec<u64>,
    pub actions: Vec<Action>,
    /// Empty permits any command within the signed action/instrument scope.
    pub command_digests: Vec<Hash>,
    pub max_requests: u32,
    pub validity: Validity,
}
/// Payload bytes are signed verbatim. Parsing happens after authentication.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedDocument {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SignedDocument {
    pub fn digest(&self) -> Hash {
        let mut h = Sha256::new();
        h.update(b"tickwire.signed-control.v1");
        h.update((self.payload.len() as u64).to_le_bytes());
        h.update(&self.payload);
        h.update(&self.signature);
        h.finalize().into()
    }
}
pub struct Verifier {
    fence_key: Hash,
    emergency_key: Hash,
    controller_id: [u8; 16],
}
pub struct VerifiedFence {
    claims: FenceClaims,
    digest: Hash,
}
pub struct VerifiedEmergency {
    claims: EmergencyClaims,
    digest: Hash,
}
impl Verifier {
    pub fn new(controller_id: [u8; 16], fence_key: Hash, emergency_key: Hash) -> Result<Self> {
        if controller_id == [0; 16]
            || fence_key == [0; 32]
            || emergency_key == [0; 32]
            || fence_key == emergency_key
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            controller_id,
            fence_key,
            emergency_key,
        })
    }
    fn authenticated<T: for<'a> Deserialize<'a>>(
        &self,
        document: &SignedDocument,
        key: &Hash,
    ) -> Result<T> {
        if document.payload.is_empty()
            || document.payload.len() > 65536
            || document.signature.len() != 64
        {
            return Err(Error::Capacity);
        }
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&document.payload, &document.signature)
            .map_err(|_| Error::Signature)?;
        serde_json::from_slice(&document.payload).map_err(|_| Error::Invalid)
    }
    pub fn verify_fence(
        &self,
        document: &SignedDocument,
        scope: &Scope,
        old: &ExecutionIdentity,
        now: MonoTime,
    ) -> Result<VerifiedFence> {
        let c: FenceClaims = self.authenticated(document, &self.fence_key)?;
        c.scope.validate()?;
        c.validity.check(now)?;
        if c.schema != 1
            || c.scope != *scope
            || c.old_identity != *old
            || c.controller_id != self.controller_id
        {
            return Err(Error::Identity);
        }
        if [
            old.host_id,
            old.boot_id,
            old.exec_instance_id,
            c.operation_id,
            c.device_action_id,
        ]
        .contains(&[0; 16])
            || old.exec_epoch == 0
            || old.network_identity == [0; 32]
            || old.credential_access_identity == [0; 32]
            || c.control_commit == 0
            || c.device_revision == 0
            || [
                c.all_egress_hash,
                c.deny_tombstone_hash,
                c.readback_hash,
                c.probe_hash,
            ]
            .contains(&[0; 32])
            || !c.all_egress_blocked
            || !c.existing_sessions_revoked
            || !c.credential_access_revoked
        {
            return Err(Error::Invalid);
        }
        Ok(VerifiedFence {
            claims: c,
            digest: document.digest(),
        })
    }
    pub fn verify_emergency(
        &self,
        document: &SignedDocument,
        scope: &Scope,
        guardian: [u8; 16],
        now: MonoTime,
    ) -> Result<VerifiedEmergency> {
        let c: EmergencyClaims = self.authenticated(document, &self.emergency_key)?;
        c.scope.validate()?;
        c.validity.check(now)?;
        if c.schema != 1
            || c.scope != *scope
            || c.guardian_id != guardian
            || guardian == [0; 16]
            || c.controller_id != self.controller_id
        {
            return Err(Error::Identity);
        }
        if c.grant_id == [0; 16]
            || c.control_commit == 0
            || c.precommitted_ns > c.validity.issued_ns
            || !c.manual_activation_allowed
            || c.max_requests == 0
            || c.max_requests > 4096
            || c.instruments.is_empty()
            || c.instruments.len() > 4096
            || c.instruments.contains(&0)
            || c.instruments.windows(2).any(|w| w[0] >= w[1])
            || c.actions.is_empty()
            || c.actions.len() > 4
            || c.actions
                .iter()
                .enumerate()
                .any(|(i, a)| c.actions[..i].contains(a))
            || c.command_digests.len() > 1024
            || c.command_digests.contains(&[0; 32])
        {
            return Err(Error::Invalid);
        }
        Ok(VerifiedEmergency {
            claims: c,
            digest: document.digest(),
        })
    }
}
impl VerifiedFence {
    pub fn evidence_digest(&self) -> Hash {
        self.digest
    }
    pub fn old_identity(&self) -> &ExecutionIdentity {
        &self.claims.old_identity
    }
    pub fn risk_domain(&self) -> [u8; 16] {
        self.claims.scope.risk_domain
    }
    pub fn scope(&self) -> &Scope {
        &self.claims.scope
    }
    pub fn controller_id(&self) -> [u8; 16] {
        self.claims.controller_id
    }
    pub fn validity(&self) -> &Validity {
        &self.claims.validity
    }
    pub fn claims(&self) -> &FenceClaims {
        &self.claims
    }
    pub fn check(&self, now: MonoTime) -> Result<()> {
        self.claims.validity.check(now)
    }
}
impl VerifiedEmergency {
    pub fn evidence_digest(&self) -> Hash {
        self.digest
    }
    pub fn claims(&self) -> &EmergencyClaims {
        &self.claims
    }
    pub fn allows(
        &self,
        instrument: u64,
        action: Action,
        command: Hash,
        used: usize,
        now: MonoTime,
    ) -> Result<()> {
        self.claims.validity.check(now)?;
        if used >= self.claims.max_requests as usize
            || !self.claims.instruments.contains(&instrument)
            || !self.claims.actions.contains(&action)
            || (!self.claims.command_digests.is_empty()
                && !self.claims.command_digests.contains(&command))
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    fn pair(seed: u8) -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
    }
    #[test]
    fn verifier_requires_distinct_keys_and_exact_authenticated_claims() {
        let f = pair(1);
        let e = pair(2);
        let v = Verifier::new(
            [3; 16],
            f.public_key().as_ref().try_into().unwrap(),
            e.public_key().as_ref().try_into().unwrap(),
        )
        .unwrap();
        let scope = Scope {
            risk_domain: [4; 16],
            venue: 1,
            account: 5,
            product: 1,
            environment: 2,
        };
        let c = EmergencyClaims {
            schema: 1,
            scope: scope.clone(),
            guardian_id: [6; 16],
            controller_id: [3; 16],
            grant_id: [7; 16],
            control_commit: 1,
            precommitted_ns: 1,
            manual_activation_allowed: true,
            instruments: vec![8],
            actions: vec![Action::Cancel],
            command_digests: vec![],
            max_requests: 1,
            validity: Validity {
                clock_boot_id: [9; 16],
                clock_generation: 1,
                issued_ns: 2,
                expires_ns: 10,
            },
        };
        let payload = serde_json::to_vec(&c).unwrap();
        let mut d = SignedDocument {
            signature: e.sign(&payload).as_ref().to_vec(),
            payload,
        };
        let now = MonoTime::new(tickwire_clock::ClockDomain::new([9; 16], 1).unwrap(), 3);
        let cap = v.verify_emergency(&d, &scope, [6; 16], now).unwrap();
        assert!(cap.allows(8, Action::Cancel, [1; 32], 0, now).is_ok());
        assert!(cap.allows(8, Action::Cancel, [1; 32], 1, now).is_err());
        assert!(cap.allows(8, Action::Query, [1; 32], 0, now).is_err());
        assert!(
            v.verify_fence(
                &d,
                &scope,
                &ExecutionIdentity {
                    host_id: [1; 16],
                    boot_id: [1; 16],
                    exec_instance_id: [1; 16],
                    exec_epoch: 1,
                    network_identity: [1; 32],
                    credential_access_identity: [1; 32]
                },
                now
            )
            .is_err()
        );
        d.payload.push(b' ');
        assert!(matches!(
            v.verify_emergency(&d, &scope, [6; 16], now),
            Err(Error::Signature)
        ));
    }
    #[test]
    fn fence_requires_complete_old_identity_and_revoked_existing_sessions() {
        let f = pair(1);
        let e = pair(2);
        let v = Verifier::new(
            [3; 16],
            f.public_key().as_ref().try_into().unwrap(),
            e.public_key().as_ref().try_into().unwrap(),
        )
        .unwrap();
        let scope = Scope {
            risk_domain: [4; 16],
            venue: 1,
            account: 5,
            product: 2,
            environment: 1,
        };
        let old = ExecutionIdentity {
            host_id: [1; 16],
            boot_id: [2; 16],
            exec_instance_id: [3; 16],
            exec_epoch: 4,
            network_identity: [5; 32],
            credential_access_identity: [6; 32],
        };
        let mut claims = FenceClaims {
            schema: 1,
            scope: scope.clone(),
            old_identity: old.clone(),
            controller_id: [3; 16],
            operation_id: [7; 16],
            control_commit: 8,
            all_egress_hash: [9; 32],
            device_action_id: [10; 16],
            device_revision: 11,
            deny_tombstone_hash: [12; 32],
            readback_hash: [13; 32],
            probe_hash: [14; 32],
            all_egress_blocked: true,
            existing_sessions_revoked: true,
            credential_access_revoked: true,
            validity: Validity {
                clock_boot_id: [15; 16],
                clock_generation: 1,
                issued_ns: 20,
                expires_ns: 30,
            },
        };
        let sign = |c: &FenceClaims| {
            let payload = serde_json::to_vec(c).unwrap();
            SignedDocument {
                signature: f.sign(&payload).as_ref().to_vec(),
                payload,
            }
        };
        let now = MonoTime::new(tickwire_clock::ClockDomain::new([15; 16], 1).unwrap(), 25);
        let cap = v.verify_fence(&sign(&claims), &scope, &old, now).unwrap();
        assert_eq!(cap.old_identity(), &old);
        assert_eq!(cap.risk_domain(), scope.risk_domain);
        assert_eq!(
            cap.check(MonoTime::new(now.domain(), 30)),
            Err(Error::Expired)
        );
        let mut other = old.clone();
        other.exec_epoch += 1;
        assert!(v.verify_fence(&sign(&claims), &scope, &other, now).is_err());
        claims.existing_sessions_revoked = false;
        assert!(matches!(
            v.verify_fence(&sign(&claims), &scope, &old, now),
            Err(Error::Invalid)
        ));
    }
}
