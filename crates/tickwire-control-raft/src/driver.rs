//! The network owner drives a business replica through this interface. It never
//! owns a second RawNode or bypasses the replica's deterministic application.
use crate::{Error, Message, Result};
use protobuf::Message as _;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyBudget(usize);
impl ReadyBudget {
    pub fn new(rounds: usize) -> Result<Self> {
        if !(1..=128).contains(&rounds) {
            return Err(Error::Bounds);
        }
        Ok(Self(rounds))
    }
    pub fn rounds(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    PreCandidate,
    Leader,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverStatus {
    pub group: [u8; 16],
    pub node_id: u64,
    pub term: u64,
    pub committed: u64,
    pub raft_applied: u64,
    pub role: Role,
    pub poisoned: bool,
    /// Durable JOIN mode, only until this identity first becomes a member.
    pub joining: bool,
}

/// Derived only from the node's durable configuration, never a caller's DTO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub(crate) voters: Vec<u64>,
    pub(crate) voters_outgoing: Vec<u64>,
    pub(crate) learners: Vec<u64>,
    pub(crate) learners_next: Vec<u64>,
    pub(crate) auto_leave: bool,
    pub(crate) applied_index: u64,
    pub(crate) origin: [u64; 3],
    pub(crate) retired: Vec<u64>,
    pub(crate) operations: Vec<crate::membership::MembershipReceipt>,
}
impl Membership {
    pub fn contains(&self, id: u64) -> bool {
        self.voters.contains(&id)
            || self.voters_outgoing.contains(&id)
            || self.learners.contains(&id)
    }

    pub fn voters(&self) -> &[u64] {
        &self.voters
    }
    pub fn voters_outgoing(&self) -> &[u64] {
        &self.voters_outgoing
    }
    pub fn learners(&self) -> &[u64] {
        &self.learners
    }
    pub fn learners_next(&self) -> &[u64] {
        &self.learners_next
    }
    pub fn auto_leave(&self) -> bool {
        self.auto_leave
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"tickwire.raft.membership.v1");
        hash.update(self.applied_index.to_le_bytes());
        hash.update([u8::from(self.auto_leave)]);
        for ids in [
            &self.voters,
            &self.voters_outgoing,
            &self.learners,
            &self.learners_next,
        ] {
            hash.update((ids.len() as u64).to_le_bytes());
            for id in ids {
                hash.update(id.to_le_bytes());
            }
        }
        if !self.operations.is_empty() {
            hash.update(b"membership-history.v2");
            hash.update(self.encode_history().expect("validated membership"));
        }
        hash.finalize().into()
    }
}

/// Includes the inline value and all retained heap allocations; implementations
/// may overestimate, but must never omit an owned allocation.
pub trait RetainedSize {
    fn retained_bytes(&self) -> usize;
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutputBound {
    pub items: usize,
    pub bytes: usize,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutputCharge {
    pub applied: OutputBound,
    pub message_count: usize,
    pub message_wire_bytes: usize,
}
pub struct DriverOutput<A> {
    pub messages: Vec<Message>,
    pub applied: Vec<A>,
    pub raft_applied_through: u64,
    pub application_scanned_through: u64,
    pub more_ready: bool,
}
impl<A: RetainedSize> DriverOutput<A> {
    pub fn charge(&self) -> Result<OutputCharge> {
        let mut bytes = self
            .applied
            .capacity()
            .checked_mul(std::mem::size_of::<A>())
            .ok_or(Error::Bounds)?;
        for value in &self.applied {
            let heap = value
                .retained_bytes()
                .checked_sub(std::mem::size_of::<A>())
                .ok_or(Error::Bounds)?;
            bytes = bytes.checked_add(heap).ok_or(Error::Bounds)?;
        }
        let message_wire_bytes = self.messages.iter().try_fold(0usize, |total, message| {
            total
                .checked_add(message.compute_size() as usize)
                .ok_or(Error::Bounds)
        })?;
        Ok(OutputCharge {
            applied: OutputBound {
                items: self.applied.len(),
                bytes,
            },
            message_count: self.messages.len(),
            message_wire_bytes,
        })
    }
}

pub trait PeerDriver {
    type Error;
    type Applied: RetainedSize;
    /// A narrow facade with typed proposal/read methods, and no network or
    /// drive methods. In particular it must not expose the underlying Node.
    type Control<'a>
    where
        Self: 'a;
    fn control(&mut self) -> Self::Control<'_>;
    fn tick(&mut self) -> core::result::Result<(), Self::Error>;
    fn peer_session(
        &mut self,
        _peer: u64,
        _session: Option<[u8; 16]>,
    ) -> core::result::Result<(), Self::Error> {
        Ok(())
    }
    fn step(&mut self, peer: u64, message: Message) -> core::result::Result<(), Self::Error>;
    /// Upper bound for the immediately following drive, with no intervening
    /// input. Include Vec allocation slack and the application's result type.
    fn output_bound(&self, budget: ReadyBudget) -> core::result::Result<OutputBound, Self::Error>;
    /// Apply every returned command through the business reducer before any
    /// message/result escapes. Application failure permanently stops the owner.
    fn drive(
        &mut self,
        budget: ReadyBudget,
    ) -> core::result::Result<DriverOutput<Self::Applied>, Self::Error>;
    fn status(&self) -> DriverStatus;
    fn current_membership(&self) -> Membership;
    fn snapshot_reader(
        &self,
    ) -> core::result::Result<Option<crate::generation::VerifiedSnapshot>, Self::Error> {
        Ok(None)
    }
    fn stage_snapshot(
        &self,
    ) -> core::result::Result<Option<crate::generation::StagingSnapshot>, Self::Error> {
        Ok(None)
    }
    fn step_snapshot(
        &mut self,
        _peer: u64,
        _message: Message,
        _snapshot: crate::generation::VerifiedSnapshot,
    ) -> core::result::Result<bool, Self::Error> {
        Ok(false)
    }
    fn report_snapshot(
        &mut self,
        peer: u64,
        success: bool,
    ) -> core::result::Result<(), Self::Error> {
        if success {
            Ok(())
        } else {
            self.report_unreachable(peer)
        }
    }
    fn report_unreachable(&mut self, peer: u64) -> core::result::Result<(), Self::Error>;
}
