//! Offline authorization of the first desired configuration.
use crate::{Error, Hash, Identity, ObjectKey, Result};
use ring::signature::{ED25519, UnparsedPublicKey};
use sha2::{Digest, Sha256};
use tickwire_config::{Candidate, Format, Limits, patch::Head};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTrust {
    cluster: Identity,
    namespace: Identity,
    cluster_name: String,
    public_key: Hash,
}
impl BootstrapTrust {
    pub fn new(
        cluster: Identity,
        namespace: Identity,
        cluster_name: String,
        public_key: Hash,
    ) -> Result<Self> {
        if cluster == [0; 16]
            || namespace == [0; 16]
            || public_key == [0; 32]
            || cluster_name.is_empty()
            || cluster_name.len() > 128
            || !cluster_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            cluster,
            namespace,
            cluster_name,
            public_key,
        })
    }
    pub fn digest(&self) -> Hash {
        let mut hash = Sha256::new();
        hash.update(b"tickwire.control.bootstrap-trust.v1");
        hash.update(self.cluster);
        hash.update(self.namespace);
        hash.update((self.cluster_name.len() as u32).to_le_bytes());
        hash.update(self.cluster_name.as_bytes());
        hash.update(self.public_key);
        hash.finalize().into()
    }
    pub fn signing_bytes(
        &self,
        candidate: ObjectKey,
        candidate_hash: Hash,
        nonce: Identity,
    ) -> Result<Vec<u8>> {
        if candidate == [0; 16] || nonce == [0; 16] {
            return Err(Error::Identity);
        }
        let mut bytes = b"tickwire.control.bootstrap.v1".to_vec();
        bytes.extend_from_slice(&self.digest());
        bytes.extend_from_slice(&candidate);
        bytes.extend_from_slice(&candidate_hash);
        bytes.extend_from_slice(&nonce);
        Ok(bytes)
    }
    pub(crate) fn cluster(&self) -> Identity {
        self.cluster
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootstrap {
    pub candidate: ObjectKey,
    pub candidate_hash: Hash,
    pub nonce: Identity,
    pub signature: [u8; 64],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedHead {
    candidate: Candidate,
    head: Head,
    raft_index: u64,
    raft_term: u64,
    bootstrap: Bootstrap,
    voters: [u64; 3],
}
impl CommittedHead {
    pub fn candidate(&self) -> &Candidate {
        &self.candidate
    }
    pub fn revision(&self) -> u64 {
        1
    }
    pub fn etag(&self) -> String {
        self.head.etag()
    }
    pub fn raft_index(&self) -> u64 {
        self.raft_index
    }
    pub fn raft_term(&self) -> u64 {
        self.raft_term
    }
}
#[derive(Debug)]
pub enum HeadRead {
    Pending,
    Ready(Option<Box<CommittedHead>>),
}
pub(crate) struct State {
    trust: Option<BootstrapTrust>,
    initialized: bool,
    head: Option<CommittedHead>,
}
pub(crate) struct SnapshotHead {
    pub request: Bootstrap,
    pub index: u64,
    pub term: u64,
    pub voters: [u64; 3],
}
impl State {
    pub(crate) fn new(trust: Option<BootstrapTrust>) -> Self {
        Self {
            trust,
            initialized: false,
            head: None,
        }
    }
    pub(crate) fn trust_hash(&self) -> Option<Hash> {
        self.trust.as_ref().map(BootstrapTrust::digest)
    }
    pub(crate) fn initialize(&mut self, trust_hash: Option<Hash>) -> Result<()> {
        if trust_hash != self.trust_hash() {
            return Err(Error::Identity);
        }
        self.initialized = true;
        Ok(())
    }
    pub(crate) fn apply(
        &mut self,
        request: Bootstrap,
        objects: &crate::objects::State,
        mut voters: [u64; 3],
        index: u64,
        term: u64,
    ) -> Result<crate::Change> {
        if !self.initialized {
            return Err(Error::NotReady);
        }
        let trust = self.trust.as_ref().ok_or(Error::Authorization)?;
        let message =
            trust.signing_bytes(request.candidate, request.candidate_hash, request.nonce)?;
        UnparsedPublicKey::new(&ED25519, trust.public_key)
            .verify(&message, &request.signature)
            .map_err(|_| Error::Authentication)?;
        if let Some(head) = &self.head {
            return if head.bootstrap == request {
                Ok(crate::Change::Bootstrapped { revision: 1 })
            } else {
                Err(Error::Conflict)
            };
        }
        let object = objects.object(request.candidate).ok_or(Error::NotFound)?;
        if object.spec().purpose != crate::Purpose::Candidate {
            return Err(Error::Identity);
        }
        if object.spec().hash != request.candidate_hash {
            return Err(Error::Digest);
        }
        let candidate = Candidate::parse(Format::Json, object.bytes(), Limits::default())
            .map_err(Error::Configuration)?;
        if candidate.hash() != request.candidate_hash
            || candidate.canonical_bytes() != object.bytes()
        {
            return Err(Error::Digest);
        }
        let normalized = candidate
            .normalized_configuration(Limits::default())
            .map_err(Error::Configuration)?;
        if normalized != candidate {
            return Err(Error::Digest);
        }
        candidate.validate_static().map_err(Error::Configuration)?;
        crate::authorization::RoleBindings::from_candidate(&candidate)?;
        let value = candidate.value();
        if value["identity"]["cluster_id"].as_str() != Some(trust.cluster_name.as_str())
            || value["identity"]["namespace_id"].as_str() != Some(uuid(trust.namespace).as_str())
        {
            return Err(Error::Identity);
        }
        let mut configured = value["control"]["voters"]
            .as_array()
            .ok_or(Error::Protocol)?
            .iter()
            .map(|row| row["node_id"].as_u64().ok_or(Error::Protocol))
            .collect::<Result<Vec<_>>>()?;
        configured.sort_unstable();
        voters.sort_unstable();
        if configured.as_slice() != voters {
            return Err(Error::Identity);
        }
        let head = Head::new(1, &candidate).map_err(Error::Configuration)?;
        self.head = Some(CommittedHead {
            candidate,
            head,
            raft_index: index,
            raft_term: term,
            bootstrap: request,
            voters,
        });
        Ok(crate::Change::Bootstrapped { revision: 1 })
    }
    pub(crate) fn head(&self) -> Option<CommittedHead> {
        self.head.clone()
    }
    pub(crate) fn snapshot_state(&self) -> (bool, Option<SnapshotHead>) {
        (
            self.initialized,
            self.head.as_ref().map(|head| SnapshotHead {
                request: head.bootstrap.clone(),
                index: head.raft_index,
                term: head.raft_term,
                voters: head.voters,
            }),
        )
    }
    pub(crate) fn trust(&self) -> Option<BootstrapTrust> {
        self.trust.clone()
    }
}
fn uuid(id: Identity) -> String {
    let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}
