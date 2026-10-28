//! Versioned, bounded strategy inputs. Validation here proves byte continuity,
//! not that a strategy executed the callbacks; the SDK verifies the latter.
use crate::{Error, Hash, Object, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ROWS: usize = 65_536;
pub const MAX_STREAMS: usize = 64;
pub const MAX_TIMERS: usize = 256;
pub const MAX_EFFECTS: usize = 32;
pub mod history;
pub mod market_lineage;
pub mod retained;
pub mod rollback;
pub mod source;
pub mod startup;
const MAGIC: &[u8; 8] = b"TWINP002";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockDomain {
    pub boot_id: [u8; 16],
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub logical_strategy: u64,
    pub instance: [u8; 16],
    pub generation: u64,
    pub binary_digest: Hash,
    pub state_schema: u64,
    pub input_stream: [u8; 16],
    pub clock: ClockDomain,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamKey {
    /// Full resource/domain identity, never a process-local slot number.
    pub identity: Hash,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub stream: StreamKey,
    pub sequence: u64,
    pub content_digest: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedBytes {
    pub revision: u64,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub id: u16,
    pub value: VersionedBytes,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timer {
    pub id: u64,
    pub generation: u64,
    /// Cancelled/fired slots retain generation, so an old firing cannot revive.
    pub deadline_ns: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum Rng {
    Disabled,
    Xoshiro256StarStarV1 { state: [u64; 4], draws: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCut {
    pub count: u64,
    pub digest: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeState {
    pub now_ns: u64,
    pub config: VersionedBytes,
    pub models: Vec<Model>,
    pub markets: Vec<Cursor>,
    pub accounts: Vec<Cursor>,
    pub external: Vec<Cursor>,
    pub timers: Vec<Timer>,
    pub rng: Rng,
    pub effects: EffectCut,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cut {
    pub sequence: u64,
    pub chain: Hash,
    pub business: Object,
    pub runtime: Object,
}
/// Immutable synchronization record; publishing its hash is a separate action.
/// Schema 1 contains a cumulative tail. Schema 2 contains only the rows after
/// the previous commit's checkpoint, which is this commit's exact anchor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputCommit {
    pub schema: u16,
    pub anchor: Object,
    pub tail: Object,
    pub checkpoint: Object,
    pub previous_commit: Option<Object>,
}
impl InputCommit {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if !matches!(self.schema, 1 | 2) {
            return Err(Error::Unsupported);
        }
        for o in [&self.anchor, &self.tail, &self.checkpoint] {
            o.validate(MAX_BYTES as u64)?;
        }
        if let Some(previous) = &self.previous_commit {
            previous.validate(crate::MAX_DOCUMENT as u64)?;
        }
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let c: Self = decode(bytes)?;
        c.bytes()?;
        Ok(c)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub schema: u16,
    pub source: Source,
    pub cut: Cut,
    /// Includes strategy-owned views; a digest-only view is insufficient.
    pub business: Vec<u8>,
    pub runtime: RuntimeState,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Effect {
    pub ordinal: u32,
    pub economic_intent: [u8; 16],
    pub command: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTransitionRecord {
    pub record: Vec<u8>,
    pub payload: Vec<u8>,
    pub instrument: u64,
    pub applied_mono_ns: u64,
}
fn legacy_cursor_basis(value: &u8) -> bool {
    *value == 0
}

/// Digest of the exact producer input at a market cursor, separate from JOIN-local evidence.
pub fn market_cursor_digest(input: &Input) -> Result<Hash> {
    if let Input::MarketTransition {
        cursor_basis,
        next,
        activated_sequence,
        catchup,
        ..
    } = input
    {
        if *cursor_basis == 1 {
            use tickwire_ipc::Wire;
            let last = catchup.last().ok_or(Error::Invalid)?;
            if last.record.len() != 512 || last.payload.len() > MAX_BYTES {
                return Err(Error::Invalid);
            }
            let header = tickwire_ipc::RecordHeader::decode(&last.record[..128])
                .map_err(|_| Error::Invalid)?;
            if header.sequence != *activated_sequence
                || header.instrument_id != last.instrument
                || header.ingress_mono_ns > last.applied_mono_ns
            {
                return Err(Error::Invalid);
            }
            let frontier = Input::MarketApplied {
                stream: next.clone(),
                sequence: *activated_sequence,
                received_ns: header.ingress_mono_ns,
                record: last.record.clone(),
                payload: last.payload.clone(),
            };
            return Ok(tickwire_recovery::digest(&canonical(&frontier)?));
        }
        if *cursor_basis != 0 {
            return Err(Error::Invalid);
        }
    }
    Ok(tickwire_recovery::digest(&canonical(input)?))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum Input {
    /// A recorded trusted JOIN boundary. Content validity is not authorization.
    MarketTransition {
        #[serde(default, skip_serializing_if = "legacy_cursor_basis")]
        cursor_basis: u8,
        operation_id: [u8; 16],
        approval_digest: Hash,
        previous: Cursor,
        next: StreamKey,
        baseline_sequence: u64,
        activated_sequence: u64,
        baseline: Vec<u8>,
        catchup: Vec<MarketTransitionRecord>,
    },
    MarketApplied {
        stream: StreamKey,
        sequence: u64,
        received_ns: u64,
        record: Vec<u8>,
        payload: Vec<u8>,
    },
    MarketCallback {
        stream: StreamKey,
        instrument: u64,
        invoked: bool,
        maximum_age_ns: u64,
    },
    AccountCallback {
        stream: StreamKey,
        position: u64,
        canonical: Vec<u8>,
    },
    TimerFired {
        id: u64,
        generation: u64,
        deadline_ns: u64,
    },
    ConfigApply {
        previous: Object,
        next: VersionedBytes,
    },
    ModelApply {
        id: u16,
        previous: Object,
        next: VersionedBytes,
    },
    External {
        stream: StreamKey,
        sequence: u64,
        schema: u64,
        bytes: Vec<u8>,
    },
    Entropy {
        stream: StreamKey,
        sequence: u64,
        seed: [u64; 4],
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergedInput {
    pub sequence: u64,
    pub now_ns: u64,
    pub input: Input,
    pub effects: Vec<Effect>,
}

/// Private construction means the initial bytes and exact old code/config have
/// been checked. It does not attest to the identity of a running process.
#[derive(Clone, Debug)]
pub struct VerifiedAnchor(Checkpoint);
impl VerifiedAnchor {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.0
    }
}
#[derive(Clone, Debug)]
pub struct VerifiedTail {
    pub(crate) source: Source,
    start: Cut,
    end: Cut,
    rows: Vec<MergedInput>,
}
impl VerifiedTail {
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn start(&self) -> &Cut {
        &self.start
    }
    pub fn end(&self) -> &Cut {
        &self.end
    }
    pub fn rows(&self) -> &[MergedInput] {
        &self.rows
    }
}

pub fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Encoding)?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}
fn decode<T: for<'a> Deserialize<'a> + Serialize>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    let value = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
    if canonical(&value)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(value)
}
pub fn runtime_object(runtime: &RuntimeState) -> Result<Object> {
    Ok(Object::of(&canonical(runtime)?))
}
impl Checkpoint {
    pub fn new(
        source: Source,
        sequence: u64,
        chain: Hash,
        business: Vec<u8>,
        runtime: RuntimeState,
    ) -> Result<Self> {
        let value = Self {
            schema: 2,
            source,
            cut: Cut {
                sequence,
                chain,
                business: Object::of(&business),
                runtime: runtime_object(&runtime)?,
            },
            business,
            runtime,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let c: Self = decode(bytes)?;
        c.validate()?;
        Ok(c)
    }
    fn validate(&self) -> Result<()> {
        if self.schema != 2
            || self.source.logical_strategy == 0
            || self.source.instance == [0; 16]
            || self.source.generation == 0
            || self.source.binary_digest == [0; 32]
            || self.source.state_schema == 0
            || self.source.input_stream == [0; 16]
            || self.source.clock.boot_id == [0; 16]
            || self.source.clock.generation == 0
            || self.business.is_empty()
            || self.business.len() > MAX_BYTES
            || (self.cut.sequence == 0) != (self.cut.chain == [0; 32])
        {
            return Err(Error::Identity);
        }
        self.runtime.validate()?;
        if self.cut.business != Object::of(&self.business)
            || self.cut.runtime != runtime_object(&self.runtime)?
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
impl RuntimeState {
    pub fn validate(&self) -> Result<()> {
        version(&self.config)?;
        if self.models.len() > MAX_STREAMS || self.timers.len() > MAX_TIMERS {
            return Err(Error::Bounds);
        }
        if self.models.windows(2).any(|w| w[0].id >= w[1].id)
            || self.timers.windows(2).any(|w| w[0].id >= w[1].id)
            || self.timers.iter().any(|t| t.id == 0 || t.generation == 0)
            || matches!(
                self.rng,
                Rng::Xoshiro256StarStarV1 {
                    state: [0, 0, 0, 0],
                    ..
                }
            )
            || (self.effects.count == 0) != (self.effects.digest == [0; 32])
        {
            return Err(Error::Invalid);
        }
        for m in &self.models {
            version(&m.value)?;
        }
        for cursors in [&self.markets, &self.accounts, &self.external] {
            if cursors.len() > MAX_STREAMS {
                return Err(Error::Bounds);
            }
            if cursors.windows(2).any(|w| w[0].stream >= w[1].stream) {
                return Err(Error::Invalid);
            }
            for c in cursors {
                key(&c.stream)?;
                if c.content_digest == [0; 32] {
                    return Err(Error::Identity);
                }
            }
        }
        Ok(())
    }
}
fn version(v: &VersionedBytes) -> Result<()> {
    if v.revision == 0 || v.bytes.is_empty() || v.bytes.len() > MAX_BYTES {
        Err(Error::Bounds)
    } else {
        Ok(())
    }
}
fn key(k: &StreamKey) -> Result<()> {
    if k.identity == [0; 32] || k.generation == 0 {
        Err(Error::Identity)
    } else {
        Ok(())
    }
}
impl MergedInput {
    pub fn validate(&self) -> Result<()> {
        if self.sequence == 0 || self.effects.len() > MAX_EFFECTS {
            return Err(Error::Bounds);
        }
        for (i, e) in self.effects.iter().enumerate() {
            if e.ordinal != i as u32
                || e.economic_intent == [0; 16]
                || e.command.is_empty()
                || e.command.len() > MAX_BYTES
            {
                return Err(Error::Invalid);
            }
        }
        match &self.input {
            Input::MarketTransition {
                cursor_basis,
                operation_id,
                approval_digest,
                previous,
                next,
                baseline_sequence,
                activated_sequence,
                baseline,
                catchup,
            } => {
                if *cursor_basis > 1 {
                    return Err(Error::Invalid);
                }
                if *cursor_basis == 1 {
                    market_cursor_digest(&self.input)?;
                }
                key(&previous.stream)?;
                key(next)?;
                if *operation_id == [0; 16]
                    || *approval_digest == [0; 32]
                    || previous.content_digest == [0; 32]
                    || next.identity == previous.stream.identity
                    || next.generation <= previous.stream.generation
                    || *baseline_sequence < previous.sequence
                    || activated_sequence.checked_sub(*baseline_sequence)
                        != Some(catchup.len() as u64)
                    || baseline.is_empty()
                    || baseline.len() > MAX_BYTES
                    || catchup.len() > MAX_ROWS
                {
                    return Err(Error::Invalid);
                }
                let mut last = 0;
                let mut bytes = baseline.len();
                for row in catchup {
                    bytes = bytes
                        .checked_add(row.record.len())
                        .and_then(|n| n.checked_add(row.payload.len()))
                        .ok_or(Error::Bounds)?;
                    if row.record.len() != 512
                        || row.instrument == 0
                        || row.applied_mono_ns < last
                        || row.applied_mono_ns > self.now_ns
                        || bytes > MAX_BYTES
                    {
                        return Err(Error::Invalid);
                    }
                    last = row.applied_mono_ns;
                }
            }
            Input::MarketApplied {
                stream,
                sequence,
                received_ns,
                record,
                payload,
            } => {
                key(stream)?;
                if *sequence == 0
                    || *received_ns > self.now_ns
                    || record.len() != 512
                    || payload.len() > MAX_BYTES
                {
                    return Err(Error::Invalid);
                }
            }
            Input::MarketCallback {
                stream,
                instrument,
                maximum_age_ns,
                ..
            } => {
                key(stream)?;
                if *instrument == 0 || *maximum_age_ns == 0 {
                    return Err(Error::Invalid);
                }
            }
            Input::AccountCallback {
                stream, canonical, ..
            } => {
                key(stream)?;
                if canonical.is_empty() || canonical.len() > MAX_BYTES {
                    return Err(Error::Bounds);
                }
            }
            Input::TimerFired {
                id,
                generation,
                deadline_ns,
            } => {
                if *id == 0 || *generation == 0 || *deadline_ns > self.now_ns {
                    return Err(Error::Invalid);
                }
            }
            Input::ConfigApply { previous, next } | Input::ModelApply { previous, next, .. } => {
                previous.validate(MAX_BYTES as u64)?;
                version(next)?;
            }
            Input::External {
                stream,
                sequence,
                schema,
                bytes,
            } => {
                key(stream)?;
                if *sequence == 0 || *schema == 0 || bytes.is_empty() || bytes.len() > MAX_BYTES {
                    return Err(Error::Invalid);
                }
            }
            Input::Entropy {
                stream,
                sequence,
                seed,
            } => {
                key(stream)?;
                if *sequence == 0 || *seed == [0; 4] {
                    return Err(Error::Invalid);
                }
            }
        }
        if matches!(
            self.input,
            Input::MarketTransition { .. }
                | Input::MarketApplied { .. }
                | Input::ConfigApply { .. }
                | Input::ModelApply { .. }
                | Input::Entropy { .. }
                | Input::MarketCallback { invoked: false, .. }
        ) && !self.effects.is_empty()
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
pub fn verify_anchor(
    bytes: &[u8],
    expected: &Source,
    config: Object,
    models: &[(u16, Object)],
) -> Result<VerifiedAnchor> {
    let checkpoint = Checkpoint::decode(bytes)?;
    if &checkpoint.source != expected
        || Object::of(&checkpoint.runtime.config.bytes) != config
        || checkpoint
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>()
            != models
    {
        return Err(Error::Identity);
    }
    Ok(VerifiedAnchor(checkpoint))
}
pub fn row_chain(previous: Hash, row: &MergedInput) -> Result<Hash> {
    row.validate()?;
    let bytes = canonical(row)?;
    let mut h = Sha256::new();
    h.update(b"tickwire.strategy-input.v2");
    h.update(previous);
    h.update((bytes.len() as u32).to_be_bytes());
    h.update(bytes);
    Ok(h.finalize().into())
}
pub fn encode_tail(anchor: &VerifiedAnchor, rows: &[MergedInput]) -> Result<Vec<u8>> {
    if rows.len() > MAX_ROWS {
        return Err(Error::Bounds);
    }
    let mut out = MAGIC.to_vec();
    out.extend(anchor.0.source.input_stream);
    out.extend(
        anchor
            .0
            .cut
            .sequence
            .checked_add(1)
            .ok_or(Error::Bounds)?
            .to_be_bytes(),
    );
    out.extend(anchor.0.cut.chain);
    out.extend((rows.len() as u32).to_be_bytes());
    for row in rows {
        row.validate()?;
        let bytes = canonical(row)?;
        if out
            .len()
            .checked_add(4 + bytes.len())
            .is_none_or(|n| n > MAX_BYTES)
        {
            return Err(Error::Bounds);
        }
        out.extend((bytes.len() as u32).to_be_bytes());
        out.extend(bytes);
    }
    Ok(out)
}
pub fn verify_tail(anchor: &VerifiedAnchor, bytes: &[u8], end: &Cut) -> Result<VerifiedTail> {
    if bytes.len() < 68 || bytes.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    if &bytes[..8] != MAGIC
        || bytes[8..24] != anchor.0.source.input_stream
        || bytes[32..64] != anchor.0.cut.chain
    {
        return Err(Error::Identity);
    }
    let first = u64::from_be_bytes(bytes[24..32].try_into().map_err(|_| Error::Encoding)?);
    if first != anchor.0.cut.sequence.checked_add(1).ok_or(Error::Bounds)? {
        return Err(Error::Identity);
    }
    let count = u32::from_be_bytes(bytes[64..68].try_into().map_err(|_| Error::Encoding)?) as usize;
    if count > MAX_ROWS || count > (bytes.len() - 68) / 5 {
        return Err(Error::Bounds);
    }
    end.business.validate(MAX_BYTES as u64)?;
    end.runtime.validate(MAX_BYTES as u64)?;
    if anchor.0.cut.sequence.checked_add(count as u64) != Some(end.sequence)
        || (count == 0 && end != &anchor.0.cut)
    {
        return Err(Error::Identity);
    }
    let mut rows = Vec::with_capacity(count);
    let mut at = 68usize;
    let mut chain = anchor.0.cut.chain;
    let mut now = anchor.0.runtime.now_ns;
    for i in 0..count {
        let n_end = at.checked_add(4).ok_or(Error::Bounds)?;
        let n = u32::from_be_bytes(
            bytes
                .get(at..n_end)
                .ok_or(Error::Encoding)?
                .try_into()
                .map_err(|_| Error::Encoding)?,
        ) as usize;
        let row_end = n_end.checked_add(n).ok_or(Error::Bounds)?;
        let row: MergedInput = decode(bytes.get(n_end..row_end).ok_or(Error::Encoding)?)?;
        row.validate()?;
        if row.sequence != first.checked_add(i as u64).ok_or(Error::Bounds)? || row.now_ns < now {
            return Err(Error::Identity);
        }
        chain = row_chain(chain, &row)?;
        now = row.now_ns;
        rows.push(row);
        at = row_end;
    }
    if at != bytes.len() || chain != end.chain {
        return Err(Error::Identity);
    }
    Ok(VerifiedTail {
        source: anchor.0.source.clone(),
        start: anchor.0.cut.clone(),
        end: end.clone(),
        rows,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum ControlInput {
    Capture {
        source: Source,
        anchor: Object,
        expected: Cut,
        pause: bool,
    },
    Auxiliary {
        source: Source,
        expected: Cut,
        input: Input,
    },
    Install {
        source: Source,
        expected: Cut,
        request: Box<install::Request>,
    },
}
impl ControlInput {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
    pub fn source(&self) -> &Source {
        match self {
            Self::Capture { source, .. }
            | Self::Auxiliary { source, .. }
            | Self::Install { source, .. } => source,
        }
    }
    pub fn expected(&self) -> &Cut {
        match self {
            Self::Capture { expected, .. }
            | Self::Auxiliary { expected, .. }
            | Self::Install { expected, .. } => expected,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedTail {
    pub schema: u16,
    pub request: Object,
    pub source: Source,
    pub anchor: Object,
    pub tail: Object,
    pub checkpoint: Object,
    pub cut: Cut,
    pub paused: bool,
}
impl CapturedTail {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let x: Self = decode(bytes)?;
        if x.schema != 1 {
            return Err(Error::Unsupported);
        }
        Ok(x)
    }
}

/// Discovery only: an authenticated capture response must bind this to the
/// running process before it can participate in a release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Head {
    pub schema: u16,
    pub source: Source,
    pub commit: Object,
    pub checkpoint: Object,
    pub cut: Cut,
}
impl Head {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let h: Self = decode(bytes)?;
        if h.schema != 1 {
            return Err(Error::Unsupported);
        }
        Ok(h)
    }
}

pub mod install;
