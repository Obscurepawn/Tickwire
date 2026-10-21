//! Explicit joint-consensus operations, bound to the durable membership.
use crate::{Error, Proposal, Result, driver::Membership};
use raft::eraftpb::ConfState;

pub const MAX_OPERATIONS: usize = 64;
pub const MAX_IDENTITIES: usize = 16;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipAction {
    AddLearner {
        node_id: u64,
    },
    ReplaceVoter {
        remove: u64,
        promote: u64,
        caught_up_through: u64,
    },
    LeaveJoint,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipRequest {
    pub operation: [u8; 16],
    pub expected_membership: [u8; 32],
    pub action: MembershipAction,
}
#[derive(Debug, Clone, PartialEq)]
pub struct MembershipReceipt {
    pub(crate) request: MembershipRequest,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) configuration: ConfState,
}
impl Eq for MembershipReceipt {}
impl MembershipReceipt {
    pub fn request(&self) -> &MembershipRequest {
        &self.request
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn configuration(&self) -> &ConfState {
        &self.configuration
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipProposal {
    Pending(Proposal),
    Applied(MembershipReceipt),
}
pub(crate) struct PeerProgress {
    pub session: [u8; 16],
    pub term: u64,
    pub index: u64,
}
impl<S: tickwire_journal::store::Store> crate::Node<S> {
    pub fn propose_membership(&mut self, request: MembershipRequest) -> Result<MembershipProposal> {
        use sha2::{Digest, Sha256};
        self.healthy()?;
        let context = request.encode()?;
        if let Some(receipt) = self.membership_receipt(request.operation) {
            return if receipt.request == request {
                Ok(MembershipProposal::Applied(receipt.clone()))
            } else {
                Err(Error::Identity)
            };
        }
        self.leader_ready()?;
        for e in self
            .raw
            .store()
            .log
            .iter()
            .chain(self.raw.raft.raft_log.unstable_entries())
        {
            if e.index > self.raw.store().applied
                && e.get_entry_type() == raft::eraftpb::EntryType::EntryConfChangeV2
            {
                let (existing, _) = parse_entry(e)?;
                if existing.operation == request.operation {
                    return if existing == request {
                        Ok(MembershipProposal::Pending(Proposal {
                            term: e.term,
                            index: e.index,
                            hash: Sha256::digest(&e.data).into(),
                        }))
                    } else {
                        Err(Error::Identity)
                    };
                }
            }
        }
        if self.raw.raft.has_pending_conf() {
            return Err(Error::NotReady);
        }
        let index = self
            .raw
            .raft
            .raft_log
            .last_index()
            .checked_add(1)
            .ok_or(Error::Bounds)?;
        self.current_membership()
            .advance(request.clone(), index, self.raw.raft.term)?;
        match request.action {
            MembershipAction::AddLearner { node_id } => {
                if !self.peer_sessions.contains_key(&node_id) {
                    return Err(Error::NotReady);
                }
            }
            MembershipAction::ReplaceVoter {
                promote,
                caught_up_through,
                ..
            } => {
                let p = self.peer_sessions.get(&promote).ok_or(Error::NotReady)?;
                if p.term != self.raw.raft.term
                    || p.index < caught_up_through
                    || caught_up_through < self.raw.store().hard.commit
                    || self
                        .raw
                        .raft
                        .prs()
                        .get(promote)
                        .is_none_or(|p| p.matched < caught_up_through)
                {
                    return Err(Error::NotReady);
                }
            }
            MembershipAction::LeaveJoint => {}
        }
        self.raw
            .propose_conf_change(context, request.change())
            .map_err(|_| Error::NotReady)?;
        let e = self
            .raw
            .raft
            .raft_log
            .unstable_entries()
            .last()
            .ok_or(Error::NotReady)?;
        if e.index != index || e.get_entry_type() != raft::eraftpb::EntryType::EntryConfChangeV2 {
            return Err(Error::NotReady);
        }
        Ok(MembershipProposal::Pending(Proposal {
            index,
            term: e.term,
            hash: Sha256::digest(&e.data).into(),
        }))
    }
    pub fn membership_receipt(&self, operation: [u8; 16]) -> Option<&MembershipReceipt> {
        self.raw
            .store()
            .member
            .operations
            .iter()
            .find(|r| r.request.operation == operation)
    }
    /// The caller is the authenticated peer owner. A new session clears prior ACK progress.
    pub fn peer_session(&mut self, peer: u64, session: Option<[u8; 16]>) -> Result<()> {
        self.healthy()?;
        if peer == 0 || peer == self.status().node_id || session == Some([0; 16]) {
            return Err(Error::Identity);
        }
        if let Some(session) = session {
            if !self.peer_sessions.contains_key(&peer) && self.peer_sessions.len() >= MAX_IDENTITIES
            {
                return Err(Error::Bounds);
            }
            if self
                .peer_sessions
                .get(&peer)
                .is_none_or(|p| p.session != session)
            {
                self.raw.report_unreachable(peer);
                if self.raw.raft.state == raft::StateRole::Leader
                    && self.raw.raft.prs().get(peer).is_some()
                {
                    self.raw.raft.send_append(peer);
                }
                self.peer_sessions.insert(
                    peer,
                    PeerProgress {
                        session,
                        term: 0,
                        index: 0,
                    },
                );
            }
        } else {
            self.peer_sessions.remove(&peer);
        }
        Ok(())
    }
    pub fn create_joining(store: S, original_config: crate::Config) -> Result<Self> {
        Self::open(crate::storage::DurableStorage::create_joining(
            store,
            original_config,
        )?)
    }
}
impl Membership {
    pub fn retired(&self) -> &[u64] {
        &self.retired
    }
}

impl MembershipRequest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        if self.operation == [0; 16] || self.expected_membership == [0; 32] {
            return Err(Error::Identity);
        }
        let (tag, a, b, c) = match self.action {
            MembershipAction::AddLearner { node_id } if node_id > 0 => (1, node_id, 0, 0),
            MembershipAction::ReplaceVoter {
                remove,
                promote,
                caught_up_through,
            } if remove > 0
                && promote > 0
                && remove != promote
                && (1..1 << 63).contains(&caught_up_through) =>
            {
                (2, remove, promote, caught_up_through)
            }
            MembershipAction::LeaveJoint => (3, 0, 0, 0),
            _ => return Err(Error::Identity),
        };
        let mut bts = b"TWMC0001".to_vec();
        bts.extend_from_slice(&self.operation);
        bts.extend_from_slice(&self.expected_membership);
        bts.push(tag);
        bts.extend_from_slice(&[0; 7]);
        for n in [a, b, c] {
            bts.extend_from_slice(&n.to_le_bytes());
        }
        Ok(bts)
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 88 || &bytes[..8] != b"TWMC0001" || bytes[57..64] != [0; 7] {
            return Err(Error::Corrupt);
        }
        let n = |o| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());
        let action = match bytes[56] {
            1 if n(72) == 0 && n(80) == 0 => MembershipAction::AddLearner { node_id: n(64) },
            2 => MembershipAction::ReplaceVoter {
                remove: n(64),
                promote: n(72),
                caught_up_through: n(80),
            },
            3 if n(64) == 0 && n(72) == 0 && n(80) == 0 => MembershipAction::LeaveJoint,
            _ => return Err(Error::Corrupt),
        };
        let s = Self {
            operation: bytes[8..24].try_into().unwrap(),
            expected_membership: bytes[24..56].try_into().unwrap(),
            action,
        };
        s.encode()?;
        Ok(s)
    }
    pub(crate) fn change(&self) -> raft::eraftpb::ConfChangeV2 {
        use raft::eraftpb::{
            ConfChangeSingle, ConfChangeTransition as X, ConfChangeType as T, ConfChangeV2,
        };
        let one = |node_id, change_type| ConfChangeSingle {
            node_id,
            change_type,
            ..Default::default()
        };
        let (transition, changes) = match self.action {
            MembershipAction::AddLearner { node_id } => {
                (X::Auto, vec![one(node_id, T::AddLearnerNode)])
            }
            MembershipAction::ReplaceVoter {
                remove, promote, ..
            } => (
                X::Explicit,
                vec![one(remove, T::RemoveNode), one(promote, T::AddNode)],
            ),
            MembershipAction::LeaveJoint => (X::Auto, vec![]),
        };
        ConfChangeV2 {
            transition,
            changes: changes.into(),
            ..Default::default()
        }
    }
}
pub(crate) fn parse_entry(
    entry: &raft::eraftpb::Entry,
) -> Result<(MembershipRequest, raft::eraftpb::ConfChangeV2)> {
    use protobuf::Message as _;
    if entry.get_entry_type() != raft::eraftpb::EntryType::EntryConfChangeV2 {
        return Err(Error::Protocol);
    }
    let req = MembershipRequest::decode(&entry.context)?;
    let cc =
        raft::eraftpb::ConfChangeV2::parse_from_bytes(&entry.data).map_err(|_| Error::Corrupt)?;
    if cc != req.change() {
        return Err(Error::Corrupt);
    }
    Ok((req, cc))
}
impl Membership {
    pub(crate) fn initial(origin: [u64; 3]) -> Self {
        Self {
            voters: origin.to_vec(),
            voters_outgoing: vec![],
            learners: vec![],
            learners_next: vec![],
            auto_leave: false,
            applied_index: 0,
            origin,
            retired: vec![],
            operations: vec![],
        }
    }
    pub(crate) fn configuration(&self) -> ConfState {
        ConfState {
            voters: self.voters.clone(),
            voters_outgoing: self.voters_outgoing.clone(),
            learners: self.learners.clone(),
            learners_next: self.learners_next.clone(),
            auto_leave: self.auto_leave,
            ..Default::default()
        }
    }
    pub(crate) fn advance(
        &self,
        request: MembershipRequest,
        index: u64,
        term: u64,
    ) -> Result<Self> {
        request.encode()?;
        if index <= self.applied_index
            || index >= 1 << 63
            || term == 0
            || term >= 1 << 63
            || self.operations.last().is_some_and(|r| term < r.term)
        {
            return Err(Error::Corrupt);
        }
        if request.expected_membership != self.digest()
            || self
                .operations
                .iter()
                .any(|r| r.request.operation == request.operation)
        {
            return Err(Error::Identity);
        }
        if self.operations.len() >= MAX_OPERATIONS {
            return Err(Error::Bounds);
        }
        let mut next = self.clone();
        match request.action {
            MembershipAction::AddLearner { node_id } => {
                if !self.voters_outgoing.is_empty()
                    || self.contains(node_id)
                    || self.retired.contains(&node_id)
                {
                    return Err(Error::Identity);
                }
                let lifetime = self
                    .voters
                    .iter()
                    .chain(&self.learners)
                    .chain(&self.retired)
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                if lifetime.len() >= MAX_IDENTITIES {
                    return Err(Error::Bounds);
                }
                next.learners.push(node_id);
                next.learners.sort_unstable();
            }
            MembershipAction::ReplaceVoter {
                remove,
                promote,
                caught_up_through,
            } => {
                if !self.voters_outgoing.is_empty()
                    || !self.voters.contains(&remove)
                    || !self.learners.contains(&promote)
                    || caught_up_through >= index
                {
                    return Err(Error::Identity);
                }
                next.voters_outgoing = self.voters.clone();
                next.voters.retain(|v| *v != remove);
                next.voters.push(promote);
                next.voters.sort_unstable();
                next.learners.retain(|n| *n != promote);
            }
            MembershipAction::LeaveJoint => {
                if self.voters_outgoing.is_empty() {
                    return Err(Error::Identity);
                }
                for id in &self.voters_outgoing {
                    if !self.voters.contains(id) {
                        next.retired.push(*id);
                    }
                }
                next.retired.sort_unstable();
                next.retired.dedup();
                next.voters_outgoing.clear();
                next.learners.extend_from_slice(&self.learners_next);
                next.learners.sort_unstable();
                next.learners_next.clear();
                next.auto_leave = false;
            }
        }
        next.applied_index = index;
        next.operations.push(MembershipReceipt {
            request,
            index,
            term,
            configuration: next.configuration(),
        });
        Ok(next)
    }
    pub(crate) fn encode_history(&self) -> Result<Vec<u8>> {
        let mut b = b"TWMH0001".to_vec();
        for n in self.origin {
            b.extend_from_slice(&n.to_le_bytes())
        }
        b.extend_from_slice(&(self.operations.len() as u16).to_le_bytes());
        b.extend_from_slice(&[0; 6]);
        for op in &self.operations {
            b.extend_from_slice(&op.request.encode()?);
            b.extend_from_slice(&op.index.to_le_bytes());
            b.extend_from_slice(&op.term.to_le_bytes());
            put_conf(&mut b, &op.configuration)?;
        }
        b.push(self.retired.len() as u8);
        b.extend_from_slice(&[0; 7]);
        for n in &self.retired {
            b.extend_from_slice(&n.to_le_bytes())
        }
        b.extend_from_slice(&self.applied_index.to_le_bytes());
        put_conf(&mut b, &self.configuration())?;
        Ok(b)
    }
    pub(crate) fn decode_history(bytes: &[u8]) -> Result<Self> {
        let mut r = Cursor(bytes);
        if r.take(8)? != b"TWMH0001" {
            return Err(Error::Corrupt);
        }
        let origin = [r.u64()?, r.u64()?, r.u64()?];
        if origin[0] == 0 || origin.windows(2).any(|w| w[0] >= w[1]) {
            return Err(Error::Corrupt);
        }
        let count = u16::from_le_bytes(r.take(2)?.try_into().unwrap()) as usize;
        if count > MAX_OPERATIONS || r.take(6)? != [0; 6] {
            return Err(Error::Bounds);
        }
        let mut m = Self::initial(origin);
        for _ in 0..count {
            let req = MembershipRequest::decode(r.take(88)?)?;
            let idx = r.u64()?;
            let term = r.u64()?;
            let after = r.conf()?;
            m = m.advance(req, idx, term)?;
            if m.configuration() != after {
                return Err(Error::Corrupt);
            }
        }
        let n = r.take(1)?[0] as usize;
        if n > MAX_IDENTITIES || r.take(7)? != [0; 7] {
            return Err(Error::Corrupt);
        }
        let mut retired = vec![];
        for _ in 0..n {
            retired.push(r.u64()?)
        }
        if retired != m.retired
            || r.u64()? != m.applied_index
            || r.conf()? != m.configuration()
            || !r.0.is_empty()
        {
            return Err(Error::Corrupt);
        }
        Ok(m)
    }
}
fn put_conf(b: &mut Vec<u8>, c: &ConfState) -> Result<()> {
    let sets = [&c.voters, &c.voters_outgoing, &c.learners, &c.learners_next];
    for s in sets {
        if s.len() > MAX_IDENTITIES || s.contains(&0) || s.windows(2).any(|w| w[0] >= w[1]) {
            return Err(Error::Corrupt);
        }
        b.push(s.len() as u8);
    }
    b.push(u8::from(c.auto_leave));
    b.extend_from_slice(&[0; 3]);
    for s in sets {
        for n in s {
            b.extend_from_slice(&n.to_le_bytes())
        }
    }
    Ok(())
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let (a, b) = self.0.split_at_checked(n).ok_or(Error::Corrupt)?;
        self.0 = b;
        Ok(a)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn conf(&mut self) -> Result<ConfState> {
        let header = self.take(8)?;
        if header[..4].iter().any(|n| *n > 16) || header[4] > 1 || header[5..] != [0; 3] {
            return Err(Error::Corrupt);
        }
        let mut sets = vec![];
        for count in &header[..4] {
            let mut s = vec![];
            for _ in 0..*count {
                s.push(self.u64()?)
            }
            sets.push(s)
        }
        Ok(ConfState {
            voters: sets.remove(0),
            voters_outgoing: sets.remove(0),
            learners: sets.remove(0),
            learners_next: sets.remove(0),
            auto_leave: header[4] == 1,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_request_literal_and_snapshot_history_are_strict() {
        let initial = Membership::initial([1, 2, 3]);
        let r = MembershipRequest {
            operation: [7; 16],
            expected_membership: initial.digest(),
            action: MembershipAction::AddLearner { node_id: 4 },
        };
        let mut literal = vec![0; 88];
        literal[..8].copy_from_slice(b"TWMC0001");
        literal[8..24].fill(7);
        literal[24..56].copy_from_slice(&initial.digest());
        literal[56] = 1;
        literal[64..72].copy_from_slice(&4u64.to_le_bytes());
        assert_eq!(r.encode().unwrap(), literal);
        assert_eq!(MembershipRequest::decode(&literal).unwrap(), r);
        for n in 0..88 {
            assert!(MembershipRequest::decode(&literal[..n]).is_err());
        }
        literal[63] = 1;
        assert!(MembershipRequest::decode(&literal).is_err());
        let after = initial.advance(r, 2, 1).unwrap();
        let encoded = after.encode_history().unwrap();
        assert_eq!(Membership::decode_history(&encoded).unwrap(), after);
        for n in 0..encoded.len() {
            assert!(Membership::decode_history(&encoded[..n]).is_err());
        }
        let mut extra = encoded;
        extra.push(0);
        assert!(Membership::decode_history(&extra).is_err());
    }
}
