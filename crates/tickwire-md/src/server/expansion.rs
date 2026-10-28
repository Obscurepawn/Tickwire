use super::*;
use sha2::{Digest, Sha256};
use tickwire_ipc::ResourceHeader;

pub(super) struct Expansion {
    topic: usize,
    operation: [u8; 16],
    participants: Vec<[u8; 32]>,
    old: ResourceHeader,
    old_set_version: u64,
    failed: bool,
    quiesced_at: Option<u64>,
    successor: Option<ResourceHeader>,
    proofs: Vec<(u32, [u8; 16], u64)>,
}
impl Expansion {
    pub(super) fn blocks(&self, topic: usize) -> bool {
        self.topic == topic && (self.failed || self.successor.is_none())
    }
}
fn canonical(admissions: &[Admission]) -> Result<Vec<[u8; 32]>> {
    let mut entries = admissions
        .iter()
        .map(|admission| Ok(Sha256::digest(serde_json::to_vec(admission)?).into()))
        .collect::<Result<Vec<_>>>()?;
    entries.sort();
    if entries.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("duplicate expansion participant"));
    }
    Ok(entries)
}
fn same_bank(a: &ResourceHeader, b: &ResourceHeader) -> bool {
    let mut a = a.clone();
    if a.descriptor_hash == [0; 32] {
        a.descriptor_hash = b.descriptor_hash;
    }
    a == *b
}
fn request_id(operation: [u8; 16], sub: SubscriptionKey) -> [u8; 16] {
    tickwire_market_source::transition::quiesce_request_id(operation, sub)
}
impl Server {
    pub fn activation_floor(&self, topic: usize, pid: u32, instance: [u8; 16]) -> Option<u64> {
        self.peers
            .iter()
            .find(|peer| {
                peer.topic == Some(topic)
                    && peer.stage == Stage::Active
                    && peer.admission.as_ref().is_some_and(|a| {
                        a.pid == pid && a.client.endpoint.local.instance == instance
                    })
            })
            .and_then(|peer| peer.activated_at)
    }
    pub fn expansion_previous_applied(
        &self,
        topic: usize,
        operation: [u8; 16],
        pid: u32,
        instance: [u8; 16],
    ) -> Result<u64> {
        self.expansions
            .iter()
            .find(|e| e.topic == topic && e.operation == operation)
            .and_then(|e| {
                e.proofs
                    .iter()
                    .find(|(p, i, _)| *p == pid && *i == instance)
            })
            .map(|(_, _, seq)| *seq)
            .ok_or_else(|| invalid("missing actual quiescence proof"))
    }
    pub(super) fn record_quiescence(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        admission: &Admission,
        applied: u64,
    ) {
        if let Some(expansion) = self
            .expansions
            .iter_mut()
            .find(|e| e.topic == topic && e.operation == operation)
        {
            expansion.proofs.push((
                admission.pid,
                admission.client.endpoint.local.instance,
                applied,
            ));
        }
    }
    pub fn topic_paused(&self, topic: usize) -> bool {
        self.blocked(topic)
    }
    pub fn abort_expansion(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        owner: &mut TopicOwner,
    ) -> Result<()> {
        if !self
            .expansions
            .iter()
            .any(|e| e.topic == topic && e.operation == operation)
        {
            return Err(invalid("unknown expansion"));
        }
        self.fail_expansion(topic, operation);
        owner.require_rebuild();
        self.invalidate_topic(topic, owner)
    }
    pub fn active_admissions(&self, topic: usize) -> Result<Vec<Admission>> {
        if topic >= 32 || self.blocked(topic) {
            return Err(invalid("topic expansion is closed"));
        }
        let mut admissions = Vec::new();
        for peer in &self.peers {
            if peer.topic.is_none() {
                return Err(invalid("unclassified pending HELLO"));
            }
            if peer.topic == Some(topic) {
                if peer.stage != Stage::Active {
                    return Err(invalid("topic has pending consumer"));
                }
                let admission = peer
                    .admission
                    .as_ref()
                    .ok_or_else(|| invalid("active admission missing"))?;
                admissions.push((serde_json::to_vec(admission)?, admission.clone()));
            }
        }
        admissions.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(admissions
            .into_iter()
            .map(|(_, admission)| admission)
            .collect())
    }
    pub fn begin_expansion(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        expected: &[Admission],
        owner: &mut TopicOwner,
        now: u64,
    ) -> Result<()> {
        if operation == [0; 16] || expected.len() > 128 || topic >= 32 {
            return Err(invalid("expansion identity or participants"));
        }
        let participants = canonical(expected)?;
        if let Some(old) = self.expansions.iter().find(|e| e.operation == operation) {
            return if old.topic == topic && old.participants == participants && !old.failed {
                Ok(())
            } else {
                Err(invalid("expansion operation reused or failed"))
            };
        }
        if self.expansions.len() >= 128 || owner.needs_bank() {
            return Err(invalid("expansion capacity or old bank fault"));
        }
        if canonical(&self.active_admissions(topic)?)? != participants {
            return Err(invalid("expansion participants changed"));
        }
        let (current, _) = owner.split();
        let old = current.header().clone();
        for peer in self.peers.iter().filter(|p| p.topic == Some(topic)) {
            let session = peer
                .session
                .as_ref()
                .ok_or_else(|| invalid("active session missing"))?;
            let sub = peer
                .subscription
                .ok_or_else(|| invalid("active subscription missing"))?;
            let admission = peer.admission.as_ref().unwrap();
            if now >= session.deadline()
                || current.stage(sub) != Some(tickwire_join::Stage::Active)
                || !same_bank(
                    &ResourceHeader::decode(&admission.client.bank_header)?,
                    &old,
                )
            {
                return Err(invalid("expansion active identity or lease changed"));
            }
        }
        if participants.is_empty() && !current.all_detached() {
            return Err(invalid("unaccounted owner gates"));
        }
        let old_set_version = current.registry().instrument_set_version;
        self.expansions.push(Expansion {
            topic,
            operation,
            participants,
            old,
            old_set_version,
            failed: false,
            quiesced_at: None,
            successor: None,
            proofs: Vec::with_capacity(expected.len()),
        });
        // Beyond this point partial revocation cannot be rolled back into an old generation.
        let result = self.queue_expansion(topic, operation, owner);
        if result.is_err() {
            self.fail_expansion(topic, operation);
            owner.require_rebuild();
        }
        result
    }
    fn queue_expansion(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        owner: &mut TopicOwner,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        for peer in self.peers.iter_mut().filter(|p| p.topic == Some(topic)) {
            let sub = peer.subscription.ok_or_else(|| invalid("subscription"))?;
            let session = peer.session.as_ref().ok_or_else(|| invalid("session"))?;
            let frame = owner
                .split()
                .0
                .quiesce(sub, session, request_id(operation, sub), 1)?;
            peer.expansion = Some(operation);
            peer.outgoing = Some(frame);
            peer.departure = None;
            peer.deadline = deadline;
            peer.stage = Stage::QuiesceQueued;
        }
        Ok(())
    }
    pub fn expansion_quiesced(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        owner: &mut TopicOwner,
    ) -> Result<bool> {
        let expansion = self
            .expansions
            .iter_mut()
            .find(|e| e.topic == topic && e.operation == operation)
            .ok_or_else(|| invalid("unknown expansion"))?;
        if expansion.failed || expansion.successor.is_some() {
            return Err(invalid("expansion not awaiting quiescence"));
        }
        let (current, _) = owner.split();
        if current.header() != &expansion.old {
            return Err(invalid("quiescence owner changed"));
        }
        if !current.all_detached() {
            return Ok(false);
        }
        expansion.quiesced_at = Some(current.published());
        Ok(true)
    }
    pub fn resume_expanded_topic(
        &mut self,
        topic: usize,
        operation: [u8; 16],
        owner: &mut TopicOwner,
    ) -> Result<()> {
        let expansion = self
            .expansions
            .iter_mut()
            .find(|e| e.topic == topic && e.operation == operation)
            .ok_or_else(|| invalid("unknown expansion"))?;
        if expansion.failed {
            return Err(invalid("expansion failed"));
        }
        let quiesced = expansion
            .quiesced_at
            .ok_or_else(|| invalid("missing actual quiescence"))?;
        let usable = !owner.needs_bank() && owner.state().live();
        let (current, _) = owner.split();
        let next = current.header();
        if let Some(done) = &expansion.successor {
            if done != next {
                return Err(invalid("resumed bank changed"));
            }
        } else {
            let old = &expansion.old;
            if next.resource_id == old.resource_id
                || next.topic_id != old.topic_id
                || next.channel_id != old.channel_id
                || next.channel_epoch != old.channel_epoch
                || next.producer_instance != old.producer_instance
                || next.producer_generation != old.producer_generation
                || next.host_boot_id != old.host_boot_id
                || next.namespace_id != old.namespace_id
                || next.predecessor_ring_epoch != old.ring_epoch
                || next.ring_epoch <= old.ring_epoch
                || next.feed_generation <= old.feed_generation
                || next.cutover_seq < quiesced
                || next.cutover_seq.checked_add(1) != Some(next.first_seq)
                || current.registry().instrument_set_version <= expansion.old_set_version
                || self.peers.iter().any(|p| p.topic == Some(topic))
            {
                return Err(invalid(
                    "expansion successor has not replaced quiesced bank",
                ));
            }
        }
        if !usable {
            expansion.failed = true;
            return Err(invalid("expansion successor is not live"));
        }
        if expansion.successor.is_none() {
            expansion.successor = Some(next.clone());
        }
        Ok(())
    }
    pub(super) fn blocked(&self, topic: usize) -> bool {
        self.expansions.iter().any(|e| e.blocks(topic))
    }
    pub(super) fn fail_expansion(&mut self, topic: usize, operation: [u8; 16]) {
        if let Some(e) = self
            .expansions
            .iter_mut()
            .find(|e| e.topic == topic && e.operation == operation)
        {
            e.failed = true;
        }
    }
}
