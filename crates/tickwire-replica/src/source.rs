use crate::{cbor::Writer, *};
use std::collections::{BTreeMap, VecDeque};
use tickwire_clock::MonoTime;
use tickwire_fencing::VerifiedFence;
use tickwire_journal::{PhysicalFrontier, store::Store};
use tickwire_recovery::{ClosureBinding, RecoveryManifest, digest};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitMode {
    LocalDurableAsync {
        max_unreplicated_age_ns: u64,
    },
    SyncReplica {
        mirror_id: [u8; 16],
        assignment_epoch: u64,
    },
}
#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub stream: Stream,
    pub profile_version: u64,
    pub mode: CommitMode,
    pub max_records: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
}
/// Actual local synchronized prefix plus verified dependencies. This is one
/// input to execution authorization, not a strategy grant or account approval.
#[derive(Clone, Debug)]
pub struct LocalPrefixProof {
    pub(crate) stream: Hash,
    pub(crate) prefix: Prefix,
    pub(crate) physical: PhysicalFrontier,
    pub(crate) profile: u64,
}
impl LocalPrefixProof {
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }
    pub fn physical(&self) -> &PhysicalFrontier {
        &self.physical
    }
    pub fn profile_version(&self) -> u64 {
        self.profile
    }
}
#[derive(Clone, Debug)]
pub struct CommitEvidence {
    local: LocalPrefixProof,
    mirror: Option<Signed>,
}
impl CommitEvidence {
    pub fn local(&self) -> &LocalPrefixProof {
        &self.local
    }
    pub fn mirror_ack(&self) -> Option<&Signed> {
        self.mirror.as_ref()
    }
}
pub struct SourceWorker<S: BlobStore> {
    config: SourceConfig,
    persistence: Persistence<S>,
    prefix: Prefix,
    entries: Vec<Entry>,
    prefixes: Vec<Prefix>,
    input_heads: BTreeMap<[u8; 16], InputPrefix>,
    pending: VecDeque<(AppendBatch, u64)>,
    pending_bytes: usize,
    restored: Option<(u64, u64)>,
    started_live: bool,
    acks: BTreeMap<u64, Signed>,
    peer: Option<([u8; 16], [u8; 16], Hash)>,
    frozen: bool,
}
impl<S: BlobStore> SourceWorker<S> {
    pub fn new(config: SourceConfig, mut persistence: Persistence<S>) -> Result<Self> {
        config.stream.validate()?;
        if config.profile_version != config.stream.profile_version
            || config.mode != config.stream.commit_mode
            || config.profile_version == 0
            || config.max_records == 0
            || config.max_records > 1_000_000
            || config.max_pending_batches == 0
            || config.max_pending_batches > 128
            || config.max_pending_bytes == 0
            || config.max_pending_bytes > 8 * 1024 * 1024
        {
            return Err(Error::Bounds);
        }
        match config.mode {
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: 0,
            }
            | CommitMode::SyncReplica {
                assignment_epoch: 0,
                ..
            } => return Err(Error::Invalid("commit profile")),
            CommitMode::SyncReplica { mirror_id, .. } if mirror_id == [0; 16] => {
                return Err(Error::Invalid("mirror identity"));
            }
            _ => {}
        }
        verify_objects(&mut persistence, &config.stream.initial_objects)?;
        if let Format::Account { baseline } = config.stream.format {
            let b = persistence.read_object(baseline, MAX_OBJECT_BYTES)?;
            let b = tickwire_account_codec::CanonicalBaseline::decode(&b)
                .map_err(|_| Error::Invalid("account baseline"))?;
            if b.snapshot().scope.risk_domain_id != config.stream.domain {
                return Err(Error::Conflict);
            }
        }
        let mut identity = Writer::new();
        identity.array(3);
        identity.bytes(&config.stream.domain);
        identity.bytes(&config.stream.journal_epoch);
        identity.uint(config.stream.source.exec_epoch);
        persistence.bind_replica_stream(&identity.0, &config.stream.bytes())?;
        let prefix = Prefix::genesis(&config.stream);
        Ok(Self {
            config,
            persistence,
            prefix,
            entries: Vec::new(),
            prefixes: vec![prefix],
            input_heads: BTreeMap::new(),
            pending: VecDeque::new(),
            pending_bytes: 0,
            restored: None,
            started_live: false,
            acks: BTreeMap::new(),
            peer: None,
            frozen: false,
        })
    }
    pub fn stream(&self) -> &Stream {
        &self.config.stream
    }
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }
    pub fn frozen(&self) -> bool {
        self.frozen
    }
    pub(crate) fn has_pending_batch_slot(&self) -> bool {
        self.pending.len() < self.config.max_pending_batches
    }
    /// Extras are immutable recovery inputs outside the intent manifest. A
    /// Reactivated account record must include its query coverage object.
    /// Startup-only replay from actual durable exports. It builds the full local
    /// prefix without consuming the bounded live queue or issuing commit evidence.
    pub fn restore_prefix(
        &mut self,
        export: &DurableExport,
        extras: &BTreeMap<u64, Vec<ObjectRef>>,
        now_ns: u64,
    ) -> Result<Prefix> {
        if self.started_live {
            return Err(Error::Invalid("restore after live ingest"));
        }
        let proof = self.ingest_inner(export, extras, now_ns, false)?;
        self.restored = Some((proof.prefix.sequence, self.restored.map_or(now_ns, |v| v.1)));
        Ok(proof.prefix)
    }
    pub fn ingest(
        &mut self,
        export: &DurableExport,
        extras: &BTreeMap<u64, Vec<ObjectRef>>,
        now_ns: u64,
    ) -> Result<LocalPrefixProof> {
        self.started_live = true;
        self.ingest_inner(export, extras, now_ns, true)
    }
    fn ingest_inner(
        &mut self,
        export: &DurableExport,
        extras: &BTreeMap<u64, Vec<ObjectRef>>,
        now_ns: u64,
        enqueue: bool,
    ) -> Result<LocalPrefixProof> {
        if self.frozen {
            return Err(Error::Frozen);
        }
        let s = &self.config.stream;
        if export.config().domain != s.domain
            || export.config().stream_epoch != s.journal_epoch
            || export.records().is_empty()
        {
            return Err(Error::Conflict);
        }
        let physical = export.frontier().ok_or(Error::Missing)?.clone();
        if self
            .entries
            .len()
            .checked_add(export.records().len())
            .is_none_or(|n| n > self.config.max_records)
            || (enqueue && self.pending.len() >= self.config.max_pending_batches)
        {
            return Err(Error::Bounds);
        }
        let mut prefix = self.prefix;
        let mut entries = Vec::new();
        for record in export.records() {
            let payload = ObjectRef::new(&record.payload)?;
            // Read-back bytes originate from the physical capability, then become
            // immutable transport objects without another business WAL format.
            self.persistence.put_object(&record.payload)?;
            let stored = match self.persistence.read_replica_entry(s.digest(), record.seq) {
                Ok(bytes) => Some(Entry::decode(&bytes)?),
                Err(tickwire_recovery::Error::Missing) => None,
                Err(e) => {
                    self.frozen = true;
                    return Err(e.into());
                }
            };
            let extra_objects = stored
                .as_ref()
                .map(|e| e.extra_objects.clone())
                .unwrap_or_else(|| extras.get(&record.seq).cloned().unwrap_or_default());
            if extra_objects.len() > 32 || extra_objects.windows(2).any(|w| w[0].hash >= w[1].hash)
            {
                return Err(Error::Bounds);
            }
            verify_objects(&mut self.persistence, &extra_objects)?;
            let (kind, binding) = classify(
                s,
                record.seq,
                record.exec_epoch,
                &record.payload,
                &extra_objects,
            )?;
            crate::input::verify_input_dependencies(
                &mut self.persistence,
                s,
                &record.payload,
                &extra_objects,
            )?;
            let manifest = if let Some(binding) = binding {
                let c = self.persistence.verify(&binding)?;
                Some(ManifestRef {
                    binding,
                    root: c.manifest_root(),
                })
            } else {
                None
            };
            let entry = Entry {
                sequence: record.seq,
                writer_epoch: record.exec_epoch,
                kind,
                payload,
                manifest,
                extra_objects,
            };
            if stored.as_ref().is_some_and(|old| old != &entry) {
                self.frozen = true;
                return Err(Error::Conflict);
            }
            crate::handover::verify(&mut self.persistence, s, &entry)?;
            if entry.kind == 11 {
                let mut history = self.entries.clone();
                history.extend(entries.iter().cloned());
                crate::handover::verify_history(&self.persistence, s, &entry, &history)?;
            }
            prefix = prefix.advance(s, &entry)?;
            entries.push(entry);
        }
        let batch = AppendBatch {
            stream_digest: s.digest(),
            previous: self.prefix,
            entries,
            end: prefix,
        };
        batch.validate(s)?;
        let bytes = batch.bytes().len();
        if enqueue
            && self
                .pending_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > self.config.max_pending_bytes)
        {
            return Err(Error::Bounds);
        }
        let input_heads = crate::input::advance_heads(
            &self.persistence,
            s,
            &self.input_heads,
            batch.entries.iter().map(|e| e.extra_objects.clone()),
        )?;
        // Persist exact dependency selection before publishing a prefix. A retry,
        // restart, or offline source seal must never choose a newer input cut
        // for an already exported record.
        for entry in &batch.entries {
            if let Err(e) =
                self.persistence
                    .put_replica_entry(s.digest(), entry.sequence, &entry.bytes())
            {
                self.frozen = true;
                return Err(e.into());
            }
        }
        self.input_heads = input_heads;
        for e in &batch.entries {
            self.prefix = self.prefix.advance(s, e)?;
            self.prefixes.push(self.prefix);
            self.entries.push(e.clone());
        }
        if enqueue {
            self.pending_bytes += bytes;
            self.pending.push_back((batch, now_ns));
        }
        Ok(LocalPrefixProof {
            stream: s.digest(),
            prefix,
            physical,
            profile: self.config.profile_version,
        })
    }
    pub fn oldest_pending_ns(&self) -> Option<u64> {
        self.restored
            .map(|v| v.1)
            .into_iter()
            .chain(self.pending.front().map(|v| v.1))
            .min()
    }
    pub fn lag_limit_ns(&self) -> Option<u64> {
        match self.config.mode {
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns,
            } => Some(max_unreplicated_age_ns),
            _ => None,
        }
    }
    pub fn extra_dependencies(&self, e: &Entry) -> Result<Vec<ObjectRef>> {
        let mut out =
            crate::input::input_dependencies(&self.persistence, self.stream(), &e.extra_objects)?.0;
        out.extend(crate::handover::dependencies(
            &self.persistence,
            self.stream(),
            e,
        )?);
        Ok(out)
    }
    pub fn next_batch(&self) -> Option<&AppendBatch> {
        self.pending.front().map(|b| &b.0)
    }
    pub fn batch_range(&self, first: u64, max: usize) -> Result<AppendBatch> {
        if first == 0 || first > self.prefix.sequence || max == 0 || max > MAX_BATCH_RECORDS {
            return Err(Error::Bounds);
        }
        let start = (first - 1) as usize;
        let end = (start + max).min(self.entries.len());
        Ok(AppendBatch {
            stream_digest: self.stream().digest(),
            previous: self.prefixes[start],
            entries: self.entries[start..end].to_vec(),
            end: self.prefixes[end],
        })
    }
    pub fn read_object(&self, o: &ObjectRef) -> Result<Vec<u8>> {
        let b = self.persistence.read_object(o.hash, o.bytes as usize)?;
        if b.len() as u64 != o.bytes {
            return Err(Error::Conflict);
        }
        Ok(b)
    }
    pub fn manifest(&self, m: &ManifestRef) -> Result<RecoveryManifest> {
        let out = self.persistence.read_manifest(&m.binding)?;
        if digest(&out.canonical_bytes()?) != m.root {
            return Err(Error::Conflict);
        }
        Ok(out)
    }
    pub(crate) fn bind_peer(&mut self, hello: &PeerHello, key: Hash) -> Result<()> {
        if hello.stream_digest != self.stream().digest() {
            return Err(Error::Conflict);
        }
        if let CommitMode::SyncReplica {
            mirror_id,
            assignment_epoch,
        } = self.config.mode
            && (hello.receiver_id != mirror_id || hello.assignment_epoch != assignment_epoch)
        {
            return Err(Error::Conflict);
        }
        if self
            .peer
            .is_some_and(|p| p != (hello.receiver_id, hello.receiver_boot_id, key))
        {
            self.acks.clear();
        }
        self.peer = Some((hello.receiver_id, hello.receiver_boot_id, key));
        Ok(())
    }
    pub fn acknowledge(&mut self, signed: Signed) -> Result<()> {
        let (id, boot, key) = self.peer.ok_or(Error::Missing)?;
        let ack = Ack::verify(&signed, key)?;
        if ack.stream_digest != self.stream().digest()
            || ack.domain != self.stream().domain
            || ack.journal_epoch != self.stream().journal_epoch
            || ack.source_exec_epoch != self.stream().source.exec_epoch
            || ack.receiver_id != id
            || ack.receiver_boot_id != boot
        {
            return Err(Error::Conflict);
        }
        if let CommitMode::SyncReplica {
            assignment_epoch, ..
        } = self.config.mode
            && ack.assignment_epoch != assignment_epoch
        {
            return Err(Error::Conflict);
        }
        if self.prefixes.get(ack.prefix.sequence as usize) != Some(&ack.prefix) {
            self.frozen = true;
            return Err(Error::Conflict);
        }
        self.acks.insert(ack.prefix.sequence, signed);
        if self.restored.is_some_and(|r| ack.prefix.sequence >= r.0) {
            self.restored = None;
        }
        while self
            .pending
            .front()
            .is_some_and(|p| p.0.end.sequence <= ack.prefix.sequence)
        {
            let b = self.pending.pop_front().unwrap();
            self.pending_bytes -= b.0.bytes().len();
        }
        Ok(())
    }
    pub fn replication_ack(&self, local: &LocalPrefixProof) -> Option<Signed> {
        if local.stream != self.stream().digest()
            || self.prefixes.get(local.prefix.sequence as usize) != Some(&local.prefix)
        {
            return None;
        }
        self.acks.get(&local.prefix.sequence).cloned()
    }
    pub fn commit_evidence(&self, local: &LocalPrefixProof, now_ns: u64) -> Result<CommitEvidence> {
        if self.frozen
            || local.stream != self.stream().digest()
            || local.profile != self.config.profile_version
            || self.prefixes.get(local.prefix.sequence as usize) != Some(&local.prefix)
        {
            return Err(Error::Frozen);
        }
        if matches!(self.config.stream.format, Format::Opaque { .. }) {
            return Err(Error::Invalid(
                "opaque replication is not transaction commit evidence",
            ));
        }
        // Old outstanding age cannot be established from a new process clock.
        // Startup must first obtain an exact ACK covering its restored prefix.
        if self.restored.is_some() {
            return Err(Error::Missing);
        }
        let mirror = match self.config.mode {
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns,
            } => {
                if self
                    .oldest_pending_ns()
                    .is_some_and(|at| now_ns < at || now_ns - at >= max_unreplicated_age_ns)
                {
                    return Err(Error::Frozen);
                }
                None
            }
            CommitMode::SyncReplica { .. } => Some(
                self.acks
                    .get(&local.prefix.sequence)
                    .cloned()
                    .ok_or(Error::Missing)?,
            ),
        };
        Ok(CommitEvidence {
            local: local.clone(),
            mirror,
        })
    }
    pub fn seal<T: Store>(
        &mut self,
        closed: &mut ClosedJournal<T>,
        fence: &VerifiedFence,
        verifier_id: [u8; 16],
        snapshot: Hash,
        now: MonoTime,
        signer: &Signer,
    ) -> Result<Signed> {
        if matches!(self.stream().format, Format::Account { .. }) {
            return Err(Error::Invalid("account seal needs closed input journal"));
        }
        self.seal_inner(
            closed,
            fence,
            verifier_id,
            snapshot,
            Vec::new(),
            now,
            signer,
        )
    }
    // Keep independent journal, ledger and fencing proofs explicit at the seal boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn seal_account<T: Store, U: Store>(
        &mut self,
        account: &mut ClosedJournal<T>,
        input: &mut ClosedJournal<U>,
        final_input: ObjectRef,
        fence: &VerifiedFence,
        verifier_id: [u8; 16],
        snapshot: Hash,
        now: MonoTime,
        signer: &Signer,
    ) -> Result<Signed> {
        let stream = self.stream().clone();
        crate::input::verify_closed_input(input, &mut self.persistence, &stream, &final_input)?;
        verify_objects(&mut self.persistence, std::slice::from_ref(&final_input))?;
        let (_, prefixes) = crate::input::input_dependencies(
            &self.persistence,
            &stream,
            std::slice::from_ref(&final_input),
        )?;
        let final_prefix = prefixes.first().ok_or(Error::Missing)?;
        if self.input_heads.iter().any(|(id, p)| {
            *id != final_prefix.input_stream || p.through_sequence > final_prefix.through_sequence
        }) {
            return Err(Error::Conflict);
        }
        crate::input::advance_heads(
            &self.persistence,
            &stream,
            &self.input_heads,
            std::iter::once(vec![final_input.clone()]),
        )?;
        self.seal_inner(
            account,
            fence,
            verifier_id,
            snapshot,
            vec![final_input],
            now,
            signer,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn seal_inner<T: Store>(
        &mut self,
        closed: &mut ClosedJournal<T>,
        fence: &VerifiedFence,
        verifier_id: [u8; 16],
        snapshot: Hash,
        final_inputs: Vec<ObjectRef>,
        now: MonoTime,
        signer: &Signer,
    ) -> Result<Signed> {
        fence
            .check(now)
            .map_err(|_| Error::Invalid("fence expired/domain"))?;
        if fence.risk_domain() != self.stream().domain
            || fence.old_identity() != &self.stream().source
            || verifier_id == [0; 16]
            || !self
                .stream()
                .initial_objects
                .iter()
                .any(|o| o.hash == snapshot)
            || closed.config().domain != self.stream().domain
            || closed.config().stream_epoch != self.stream().journal_epoch
        {
            return Err(Error::Conflict);
        }
        let end = closed.durable_frontier().map_or(0, |f| f.last_seq);
        if end != self.prefix.sequence {
            return Err(Error::Gap {
                next: self.prefix.sequence + 1,
            });
        }
        // Re-read every original byte after closing and re-establish all dependency syncs.
        let mut next = 1;
        while next <= end {
            let e = closed.export_durable(next, MAX_BATCH_RECORDS, MAX_BATCH_BYTES)?;
            if e.records().is_empty() {
                return Err(Error::Missing);
            }
            for r in e.records() {
                let meta = &self.entries[(r.seq - 1) as usize];
                if r.exec_epoch != meta.writer_epoch || digest(&r.payload) != meta.payload.hash {
                    return Err(Error::Conflict);
                }
                verify_entry(&mut self.persistence, self.config.stream.clone(), meta)?;
                next = r.seq + 1;
            }
        }
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        let seal = SourceRecoverySeal {
            stream: self.stream().clone(),
            prefix: self.prefix,
            fence_ref: fence.evidence_digest(),
            snapshot_ref: snapshot,
            verifier_id,
            final_inputs,
        };
        let signed = signer.sign(seal.bytes());
        self.persistence.put_object(&signed.bytes())?;
        self.frozen = true;
        Ok(signed)
    }
}
pub(crate) fn verify_objects<S: BlobStore>(
    p: &mut Persistence<S>,
    objects: &[ObjectRef],
) -> Result<()> {
    for o in objects {
        let bytes = p.read_object(o.hash, o.bytes as usize)?;
        if bytes.len() as u64 != o.bytes {
            return Err(Error::Conflict);
        }
        if p.put_object(&bytes)? != o.hash {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
pub(crate) fn verify_entry<S: BlobStore>(
    p: &mut Persistence<S>,
    stream: Stream,
    e: &Entry,
) -> Result<()> {
    verify_objects(p, std::slice::from_ref(&e.payload))?;
    verify_objects(p, &e.extra_objects)?;
    let bytes = p.read_object(e.payload.hash, e.payload.bytes as usize)?;
    let (kind, binding) = classify(
        &stream,
        e.sequence,
        e.writer_epoch,
        &bytes,
        &e.extra_objects,
    )?;
    crate::input::verify_input_dependencies(p, &stream, &bytes, &e.extra_objects)?;
    crate::handover::verify(p, &stream, e)?;
    if kind != e.kind || binding != e.manifest.as_ref().map(|m| m.binding) {
        return Err(Error::Conflict);
    }
    if let Some(m) = &e.manifest {
        let cap = p.verify(&m.binding)?;
        if cap.manifest_root() != m.root {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
pub(crate) fn classify(
    stream: &Stream,
    seq: u64,
    writer: u64,
    bytes: &[u8],
    extras: &[ObjectRef],
) -> Result<(u64, Option<ClosureBinding>)> {
    if writer == 0 || writer > stream.source.exec_epoch {
        return Err(Error::Conflict);
    }
    let Format::Account { baseline } = stream.format else {
        let Format::Opaque { record_type } = stream.format else {
            unreachable!()
        };
        return Ok((record_type, None));
    };
    let r = tickwire_account_codec::CanonicalRecord::decode(bytes)
        .map_err(|_| Error::Invalid("canonical account record"))?;
    if r.baseline() != baseline || r.sequence() != seq {
        return Err(Error::Conflict);
    }
    use tickwire_account::RecoveryRecord as R;
    let kind = match r.record() {
        R::Admit { command, .. } => {
            let q = &command.command().context.request;
            if q.risk_domain_id != stream.domain || q.intent_exec_epoch != writer {
                return Err(Error::Conflict);
            }
            return Ok((
                1,
                Some(ClosureBinding {
                    risk_domain_id: q.risk_domain_id,
                    intent_id: q.intent_id,
                    intent_exec_epoch: q.intent_exec_epoch,
                    business_digest: r.digest(),
                    strategy_generation: q.strategy_generation,
                    config_revision: q.config_revision,
                    recovery_object: q.recovery_object,
                }),
            ));
        }
        R::StrategyHandover(_) => 11,
        R::IngressAccountInput { .. } => 12,
        R::IncomeBaseline(b) => {
            if !extras.iter().any(|o| o.hash == b.coverage_digest) {
                return Err(Error::Missing);
            }
            13
        }
        R::VenueEvent(_) => 2,
        R::Aborted { .. } => 3,
        R::FeesReconciled { .. } => 4,
        R::UnattributedCashFlow { .. } => 5,
        R::ExternalPosition { .. } => 6,
        R::TransferAsset { .. } => 7,
        R::TransferPosition { .. } => 8,
        R::IngressEvent { .. } => 9,
        R::Reactivated(b) => {
            if b.writer_epoch != writer
                || b.scope.risk_domain_id != stream.domain
                || !extras.iter().any(|o| o.hash == b.query_coverage_digest)
            {
                return Err(Error::Missing);
            }
            10
        }
    };
    Ok((kind, None))
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRecoverySeal {
    pub stream: Stream,
    pub prefix: Prefix,
    pub fence_ref: Hash,
    pub snapshot_ref: Hash,
    pub verifier_id: [u8; 16],
    pub final_inputs: Vec<ObjectRef>,
}
impl SourceRecoverySeal {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(10);
        w.bytes(b"tickwire.replica.source-recovery-seal");
        w.uint(1);
        w.bytes(&self.stream.bytes());
        self.prefix.write(&mut w);
        w.bytes(&self.fence_ref);
        w.bytes(&self.snapshot_ref);
        w.uint(1);
        w.bytes(&self.verifier_id);
        w.bytes(b"source_freeze_seal_with_writer_closed");
        w.array(self.final_inputs.len());
        for o in &self.final_inputs {
            o.write(&mut w);
        }
        w.0
    }
    pub fn verify(
        signed: &Signed,
        key: Hash,
        fence: &VerifiedFence,
        now: MonoTime,
    ) -> Result<Self> {
        use crate::cbor::Reader;
        signed.verify(key)?;
        fence
            .check(now)
            .map_err(|_| Error::Invalid("fence validity"))?;
        let mut r = Reader::new(signed.payload());
        r.array(10)?;
        crate::protocol::tag(&mut r, b"tickwire.replica.source-recovery-seal")?;
        let stream = Stream::decode(r.bytes(65536)?)?;
        let prefix = Prefix::read(&mut r)?;
        let fence_ref = r.fixed()?;
        let snapshot_ref = r.fixed()?;
        if r.uint()? != 1 {
            return Err(Error::Invalid("verification method"));
        }
        let verifier_id = r.fixed()?;
        if r.bytes(64)? != b"source_freeze_seal_with_writer_closed" {
            return Err(Error::Invalid("source seal method"));
        }
        let n = r.count(1)?;
        let mut final_inputs = Vec::with_capacity(n);
        for _ in 0..n {
            final_inputs.push(ObjectRef::read(&mut r)?);
        }
        r.end()?;
        if matches!(stream.format, Format::Account { .. }) && final_inputs.len() != 1 {
            return Err(Error::Missing);
        }
        if stream.domain != fence.risk_domain()
            || &stream.source != fence.old_identity()
            || fence_ref != fence.evidence_digest()
            || verifier_id == [0; 16]
            || !stream
                .initial_objects
                .iter()
                .any(|o| o.hash == snapshot_ref)
        {
            return Err(Error::Conflict);
        }
        Ok(Self {
            stream,
            prefix,
            fence_ref,
            snapshot_ref,
            verifier_id,
            final_inputs,
        })
    }
}
