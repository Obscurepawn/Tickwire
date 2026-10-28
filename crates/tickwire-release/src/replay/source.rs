//! Content-bound source replay evidence. The Host additionally observes its
//! owned child and successful exit; deserialization alone proves no process ran.
use super::*;
use crate::files::ObjectFile;
use crate::{Binding, Member, Operation};
use std::collections::BTreeMap;
use std::path::PathBuf;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceJob {
    pub schema: u16,
    pub binding: Binding,
    pub source: Source,
    pub request: ObjectFile,
    pub capture: ObjectFile,
    pub anchor: ObjectFile,
    pub tail: ObjectFile,
    pub checkpoint: ObjectFile,
    pub output_directory: PathBuf,
}
impl SourceJob {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let j: Self = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        if j.schema != 1
            || j.bytes()? != bytes
            || !j.output_directory.is_absolute()
            || [&j.request, &j.capture, &j.anchor, &j.tail, &j.checkpoint]
                .iter()
                .any(|f| !f.path.is_absolute())
        {
            return Err(Error::Identity);
        }
        Ok(j)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReceipt {
    pub schema: u16,
    pub job: Object,
    pub binding: Binding,
    pub pid: u32,
    pub source: Source,
    pub capture: Object,
    pub output: Object,
    pub cut: Cut,
}
impl SourceReceipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let r: Self = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if r.schema != 1 || r.pid == 0 || r.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(r)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub operation: Operation,
    pub binding: Binding,
    pub member: Member,
    pub fixed_proof: Object,
    pub current_proof: Object,
    pub anchor: Object,
    pub tail: Object,
    pub checkpoint: Object,
    pub receipt: Object,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let c: Self = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        if c.schema != 1 || c.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(c)
    }
}
/// Complete typed content proof. Creation checks both source cuts, the exact
/// source executable/schema, actual retained input bytes, and receipt binding.
/// Runtime obtains it only from its qualified Host adapter, which also checks
/// the real Registry PID and exit. The target repeats this content verification.
pub struct VerifiedSourceReplay {
    root: Object,
    completion: Completion,
    fixed: Checkpoint,
    current: Checkpoint,
    receipt: SourceReceipt,
    objects: BTreeMap<Hash, Vec<u8>>,
}
impl VerifiedSourceReplay {
    pub fn verify(
        op: &Operation,
        member: &Member,
        root: Object,
        objects: Vec<Vec<u8>>,
    ) -> Result<Self> {
        Self::verify_at(op, member, root, objects, 0)
    }
    pub(crate) fn verify_at(
        op: &Operation,
        member: &Member,
        root: Object,
        objects: Vec<Vec<u8>>,
        depth: usize,
    ) -> Result<Self> {
        if depth >= 16 {
            return Err(Error::Bounds);
        }
        op.validate().map_err(|_| Error::Identity)?;
        if !op.members.contains(member) || objects.len() > 512 {
            return Err(Error::Bounds);
        }
        let mut total = 0usize;
        let mut map = BTreeMap::new();
        for bytes in objects {
            total = total
                .checked_add(bytes.len())
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                .ok_or(Error::Bounds)?;
            map.insert(tickwire_recovery::digest(&bytes), bytes);
        }
        let get = |o: Object| -> Result<&[u8]> {
            let b = map.get(&o.digest).ok_or(Error::Missing)?;
            o.verify(b, MAX_BYTES as u64)?;
            Ok(b)
        };
        let c = Completion::decode(get(root)?)?;
        for cut in [c.fixed_proof, c.current_proof] {
            let seal =
                tickwire_recovery::HandoverSeal::decode(get(cut)?).map_err(|_| Error::Identity)?;
            if let Some(parent) = seal.preflight_receipt_digest {
                let prior = crate::closure::verify_at(
                    parent,
                    &mut |h| map.get(&h).cloned().ok_or(Error::Missing),
                    depth + 1,
                )?;
                if prior.completion.member.new_instance != seal.instance
                    || prior.completion.member.new_generation != seal.generation
                    || prior.completion.logical_strategy != seal.logical_strategy
                {
                    return Err(Error::Identity);
                }
            }
            if seal.version == 3 {
                if seal.replay_rollback_digest.is_none() {
                    return Err(Error::Identity);
                }
                super::startup::verify_seal_at(
                    &seal,
                    get(Object {
                        digest: seal.checkpoint_digest,
                        bytes: seal.checkpoint_bytes,
                    })?,
                    get(Object {
                        digest: seal.input_digest,
                        bytes: seal.input_bytes,
                    })?,
                    &mut |h| map.get(&h).cloned().ok_or(Error::Missing),
                    depth + 1,
                )?;
            }
            if seal.version == 4 {
                if seal.rebuild_rollback_digest.is_none() {
                    return Err(Error::Identity);
                }
                crate::rebuild::install::verify_seal_at(
                    &seal,
                    get(Object {
                        digest: seal.checkpoint_digest,
                        bytes: seal.checkpoint_bytes,
                    })?,
                    get(Object {
                        digest: seal.input_digest,
                        bytes: seal.input_bytes,
                    })?,
                    &mut |h| map.get(&h).cloned().ok_or(Error::Missing),
                    depth + 1,
                )?;
            }
        }
        let binding = Binding {
            operation_id: op.operation_id,
            revision: op.operation_revision,
            logical_strategy: op.logical_strategy_id,
            members_digest: op.members_digest().map_err(|_| Error::Identity)?,
        };
        if c.operation != *op || c.binding != binding || &c.member != member {
            return Err(Error::Identity);
        }
        let read_cut = |proof: Object| -> Result<Checkpoint> {
            let seal = tickwire_recovery::HandoverSeal::decode(get(proof)?)
                .map_err(|_| Error::Identity)?;
            if seal.instance != member.old_instance
                || seal.generation != member.old_generation
                || seal.logical_strategy != op.logical_strategy_id
            {
                return Err(Error::Identity);
            }
            let raw = get(Object {
                digest: seal.checkpoint_digest,
                bytes: seal.checkpoint_bytes,
            })?;
            let inputs = get(Object {
                digest: seal.input_digest,
                bytes: seal.input_bytes,
            })?;
            let accounts = get(Object {
                digest: seal.account_input_digest,
                bytes: seal.account_input_bytes,
            })?;
            tickwire_handover::verify(&seal, raw, inputs, accounts).map_err(|_| Error::Identity)?;
            let retained = retained::decode(inputs)?;
            let cp = retained.current();
            // SDK Application snapshots contain the strategy state plus its
            // isolated views. Bind that exact business state to actual READY.
            let app: serde_json::Value =
                serde_json::from_slice(&cp.business).map_err(|_| Error::Encoding)?;
            let business: Vec<u8> =
                serde_json::from_value(app.get("strategy").cloned().ok_or(Error::Identity)?)
                    .map_err(|_| Error::Encoding)?;
            if raw.get(96..) != Some(business.as_slice())
                || cp.source.instance != seal.instance
                || cp.source.generation != seal.generation
                || cp.source.logical_strategy != seal.logical_strategy
            {
                return Err(Error::Identity);
            }
            Ok(cp.clone())
        };
        let fixed = read_cut(c.fixed_proof)?;
        let current = read_cut(c.current_proof)?;
        if fixed.source != current.source
            || get(c.anchor)? != fixed.bytes()?
            || get(c.checkpoint)? != current.bytes()?
        {
            return Err(Error::Identity);
        }
        let models = fixed
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>();
        let anchor = verify_anchor(
            get(c.anchor)?,
            &fixed.source,
            Object::of(&fixed.runtime.config.bytes),
            &models,
        )?;
        verify_tail(&anchor, get(c.tail)?, &current.cut)?;
        let receipt = SourceReceipt::decode(get(c.receipt)?)?;
        if receipt.binding != binding
            || receipt.source != fixed.source
            || receipt.output != c.checkpoint
            || receipt.cut != current.cut
        {
            return Err(Error::Identity);
        }
        let capture = CapturedTail::decode(get(receipt.capture)?)?;
        if !capture.paused
            || capture.source != fixed.source
            || capture.anchor != c.anchor
            || capture.tail != c.tail
            || capture.checkpoint != c.checkpoint
            || capture.cut != current.cut
        {
            return Err(Error::Identity);
        }
        let request = ControlInput::decode(get(capture.request)?)?;
        if request
            != (ControlInput::Capture {
                source: fixed.source.clone(),
                anchor: c.anchor,
                expected: fixed.cut.clone(),
                pause: true,
            })
        {
            return Err(Error::Identity);
        }
        let job = SourceJob::decode(get(receipt.job)?)?;
        if job.binding != binding
            || job.source != fixed.source
            || job.request.object != capture.request
            || job.capture.object != receipt.capture
            || job.anchor.object != c.anchor
            || job.tail.object != c.tail
            || job.checkpoint.object != c.checkpoint
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            root,
            completion: c,
            fixed,
            current,
            receipt,
            objects: map,
        })
    }
    pub fn root(&self) -> Object {
        self.root
    }
    pub fn completion(&self) -> &Completion {
        &self.completion
    }
    pub fn fixed(&self) -> &Checkpoint {
        &self.fixed
    }
    pub fn current(&self) -> &Checkpoint {
        &self.current
    }
    pub fn receipt(&self) -> &SourceReceipt {
        &self.receipt
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.objects.values().map(Vec::as_slice)
    }
    pub fn bind_release(&self, bundle: &crate::ReleaseBundle, op: &Operation) -> Result<()> {
        let release = bundle.verify_operation(op, &self.completion.member)?;
        if release.migration.source_proof != self.completion.fixed_proof
            || release.migration.source_schema != self.fixed.source.state_schema
        {
            return Err(Error::Identity);
        }
        let seal = tickwire_recovery::HandoverSeal::decode(
            self.objects
                .get(&self.completion.fixed_proof.digest)
                .ok_or(Error::Missing)?,
        )
        .map_err(|_| Error::Identity)?;
        if release.migration.source_checkpoint
            != (Object {
                digest: seal.checkpoint_digest,
                bytes: seal.checkpoint_bytes,
            })
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
