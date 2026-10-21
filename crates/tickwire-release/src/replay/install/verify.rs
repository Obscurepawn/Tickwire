//! Content verification for an installed and continued target. Live process
//! authentication and rollback execution are separate Host completion checks.
use super::*;
use std::collections::BTreeMap;
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MarketView {
    resource: Vec<u8>,
    maximum_levels: usize,
    view: Vec<u8>,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AccountView {
    stream: StreamKey,
    view: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppState {
    schema: u16,
    strategy: Vec<u8>,
    maximum_age_ns: u64,
    markets: Vec<MarketView>,
    accounts: Vec<AccountView>,
}
fn app(checkpoint: &Checkpoint) -> Result<AppState> {
    let value: AppState = decode(&checkpoint.business)?;
    if value.schema != 1
        || value.strategy.is_empty()
        || value.maximum_age_ns == 0
        || value.markets.is_empty()
        || value.markets.len() > MAX_STREAMS
        || value.accounts.len() > MAX_STREAMS
    {
        return Err(Error::Identity);
    }
    Ok(value)
}
pub struct Verified {
    pub continued: Continued,
    pub installation: Installation,
    pub request: Request,
    pub checkpoint: Checkpoint,
    pub objects: Vec<Vec<u8>>,
}
impl Verified {
    pub fn initial_checkpoint(&self) -> Result<Checkpoint> {
        let bytes = self
            .objects
            .iter()
            .find(|b| Object::of(b) == self.installation.checkpoint)
            .ok_or(Error::Missing)?;
        Checkpoint::decode(bytes)
    }
    pub fn installed_business(&self) -> Result<Vec<u8>> {
        Ok(app(&self.initial_checkpoint()?)?.strategy)
    }
    pub fn bind_seal(
        &self,
        seal: &tickwire_recovery::HandoverSeal,
        checkpoint: &[u8],
        inputs: &[u8],
    ) -> Result<()> {
        let member = &self.request.member;
        if seal.version != 3
            || seal.instance != member.new_instance
            || seal.generation != member.new_generation
            || seal.logical_strategy != self.request.operation.logical_strategy_id
            || seal.replay_installation_digest != Some(Object::of(&self.continued.bytes()?).digest)
        {
            return Err(Error::Identity);
        }
        seal.verify_checkpoint(checkpoint)
            .map_err(|_| Error::Identity)?;
        if Object::of(inputs)
            != (Object {
                digest: seal.input_digest,
                bytes: seal.input_bytes,
            })
        {
            return Err(Error::Identity);
        }
        let retained = retained::decode(inputs)?;
        retained.tail_from(&self.checkpoint)?;
        if Object::of(&retained.anchor().checkpoint().bytes()?) != self.installation.checkpoint
            || app(retained.current())?.strategy != checkpoint[96..]
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
struct Reader<F> {
    read: F,
    objects: BTreeMap<Hash, Vec<u8>>,
    bytes: usize,
}
impl<F: FnMut(Hash) -> Result<Vec<u8>>> Reader<F> {
    fn get(&mut self, hash: Hash) -> Result<Vec<u8>> {
        if let Some(bytes) = self.objects.get(&hash) {
            return Ok(bytes.clone());
        }
        if hash == [0; 32] || self.objects.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = (self.read)(hash)?;
        if bytes.is_empty() || bytes.len() > MAX_BYTES || Object::of(&bytes).digest != hash {
            return Err(Error::Identity);
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        self.objects.insert(hash, bytes.clone());
        Ok(bytes)
    }
    fn object(&mut self, object: Object) -> Result<Vec<u8>> {
        object.validate(MAX_BYTES as u64)?;
        let bytes = self.get(object.digest)?;
        object.verify(&bytes, MAX_BYTES as u64)?;
        Ok(bytes)
    }
    fn checkpoint(&mut self, object: Object) -> Result<Checkpoint> {
        Checkpoint::decode(&self.object(object)?)
    }
    fn tail(&mut self, commit: Object) -> Result<(Checkpoint, VerifiedTail, Checkpoint)> {
        let history = history::verify(commit, MAX_ROWS + 1, |object| self.object(object))?;
        let (anchor, tail, current) = history.into_parts();
        Ok((anchor.checkpoint().clone(), tail, current))
    }
}
fn post_cut(input: &Input, source: &Checkpoint) -> Result<bool> {
    let cursor = |cursors: &[Cursor], stream: &StreamKey| {
        cursors
            .iter()
            .find(|c| c.stream == *stream)
            .map(|c| c.sequence)
    };
    Ok(match input {
        Input::MarketApplied {
            stream, sequence, ..
        } => *sequence > cursor(&source.runtime.markets, stream).ok_or(Error::Identity)?,
        Input::AccountCallback {
            stream, position, ..
        } => cursor(&source.runtime.accounts, stream).is_none_or(|c| *position > c),
        Input::External {
            stream, sequence, ..
        }
        | Input::Entropy {
            stream, sequence, ..
        } => cursor(&source.runtime.external, stream).is_none_or(|c| *sequence > c),
        Input::MarketCallback { .. } | Input::TimerFired { .. } => false,
        Input::ConfigApply { .. } | Input::ModelApply { .. } => return Err(Error::Unsupported),
    })
}
fn raw(input: &Input) -> bool {
    matches!(
        input,
        Input::MarketApplied { .. }
            | Input::AccountCallback { .. }
            | Input::External { .. }
            | Input::Entropy { .. }
    )
}
/// Verifies immutable bytes and exact C-after provenance. It does not mint
/// process ownership or rollback-probe evidence; the Host supplies those from
/// authenticated child completion and Registry observations.
pub fn verify(root: Hash, mut read: impl FnMut(Hash) -> Result<Vec<u8>>) -> Result<Verified> {
    verify_at(root, &mut read, 0)
}
pub(crate) fn verify_at(
    root: Hash,
    read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    depth: usize,
) -> Result<Verified> {
    if depth >= 16 {
        return Err(Error::Bounds);
    }
    let mut r = Reader {
        read,
        objects: BTreeMap::new(),
        bytes: 0,
    };
    let continued = Continued::decode(&r.get(root)?)?;
    let installation = Installation::decode(&r.object(continued.installation)?)?;
    let request_bytes = r.object(installation.request)?;
    let ControlInput::Install {
        source,
        expected,
        request,
    } = ControlInput::decode(&request_bytes)?
    else {
        return Err(Error::Identity);
    };
    request.bytes()?;
    if request.source_completion != installation.source_completion
        || request.target != installation.target
        || request.target.instance != source.instance
        || request.target.generation != source.generation
        || request.target.logical_strategy != source.logical_strategy
        || request.target.binary_digest != source.binary_digest
        || request.target.state_schema != source.state_schema
        || request.target.clock != source.clock
        || request.target.input_stream == source.input_stream
    {
        return Err(Error::Identity);
    }
    let mut source_objects = Vec::new();
    for o in &request.source_objects {
        source_objects.push(r.object(*o)?);
    }
    let proof = source::VerifiedSourceReplay::verify_at(
        &request.operation,
        &request.member,
        request.source_completion,
        source_objects,
        depth + 1,
    )?;
    let bundle = crate::ReleaseBundle::decode(
        &r.get(request.operation.artifacts_digest)?,
        &r.get(request.operation.checkpoint_digest)?,
        &r.get(request.operation.rollback_digest)?,
    )?;
    proof.bind_release(&bundle, &request.operation)?;
    let release = bundle.verify_operation(&request.operation, &request.member)?;
    if release
        .target
        .files
        .iter()
        .find(|f| f.role == crate::Role::Binary)
        .map(|f| f.object.digest)
        != Some(request.target.binary_digest)
        || release.target.capabilities.state_schema != request.target.state_schema
    {
        return Err(Error::Identity);
    }
    let (prior_anchor, prior_tail, prior) = r.tail(installation.previous_commit)?;
    if prior.source != source
        || prior.cut != expected
        || Object::of(&prior.bytes()?) != installation.previous_target
    {
        return Err(Error::Identity);
    }
    // Check retention lower and upper bounds before accepting any C-after bytes.
    for (cursors, begin, end) in [
        (
            &proof.current().runtime.markets,
            &prior_anchor.runtime.markets,
            &prior.runtime.markets,
        ),
        (
            &proof.current().runtime.accounts,
            &prior_anchor.runtime.accounts,
            &prior.runtime.accounts,
        ),
        (
            &proof.current().runtime.external,
            &prior_anchor.runtime.external,
            &prior.runtime.external,
        ),
    ] {
        for cursor in cursors {
            if begin
                .iter()
                .find(|c| c.stream == cursor.stream)
                .is_some_and(|c| c.sequence > cursor.sequence)
                || end
                    .iter()
                    .find(|c| c.stream == cursor.stream)
                    .is_none_or(|c| c.sequence < cursor.sequence)
            {
                return Err(Error::Identity);
            }
        }
    }
    let mut expected_inputs = Vec::new();
    for row in prior_tail.rows() {
        if post_cut(&row.input, proof.current())? {
            expected_inputs.push(row.input.clone());
        }
    }
    let inputs: Vec<Input> = decode(&r.object(continued.inputs)?)?;
    if inputs != expected_inputs {
        return Err(Error::Identity);
    }
    let initial = r.checkpoint(installation.checkpoint)?;
    let initial_commit = InputCommit::decode(&r.object(installation.commit)?)?;
    let (_, initial_tail, initial_end) = r.tail(installation.commit)?;
    if initial.source != request.target
        || initial.cut.sequence != 0
        || initial.runtime.config != request.config
        || initial_commit.anchor != installation.checkpoint
        || initial_commit.checkpoint != installation.checkpoint
        || initial_commit.previous_commit.is_some()
        || !initial_tail.rows().is_empty()
        || initial_end != initial
    {
        return Err(Error::Identity);
    }
    let applied: Input = decode(&r.object(installation.config_apply)?)?;
    if applied
        != (Input::ConfigApply {
            previous: Object::of(&proof.current().runtime.config.bytes),
            next: request.config.clone(),
        })
    {
        return Err(Error::Identity);
    }
    r.object(installation.migrated)?;
    let initial_views = app(&initial)?;
    let source_views = app(proof.current())?;
    if initial_views.markets != source_views.markets
        || initial_views.accounts != source_views.accounts
    {
        return Err(Error::Identity);
    }
    let (anchor, tail, checkpoint) = r.tail(continued.commit)?;
    if anchor != initial
        || Object::of(&checkpoint.bytes()?) != continued.checkpoint
        || checkpoint.source != request.target
        || continued.completed_ns < checkpoint.runtime.now_ns
        || tail
            .rows()
            .iter()
            .filter(|row| raw(&row.input))
            .map(|row| &row.input)
            .ne(inputs.iter())
        || tail.rows().iter().any(|row| {
            matches!(
                row.input,
                Input::ConfigApply { .. } | Input::ModelApply { .. }
            )
        })
        || checkpoint
            .runtime
            .markets
            .iter()
            .map(|c| (&c.stream, c.sequence))
            .ne(prior
                .runtime
                .markets
                .iter()
                .map(|c| (&c.stream, c.sequence)))
        || checkpoint
            .runtime
            .accounts
            .iter()
            .map(|c| (&c.stream, c.sequence))
            .ne(prior
                .runtime
                .accounts
                .iter()
                .map(|c| (&c.stream, c.sequence)))
        || checkpoint.runtime.external != prior.runtime.external
    {
        return Err(Error::Identity);
    }
    let current_views = app(&checkpoint)?;
    let prior_views = app(&prior)?;
    if current_views.markets != prior_views.markets
        || current_views.accounts != prior_views.accounts
    {
        return Err(Error::Identity);
    }
    Ok(Verified {
        continued,
        installation,
        request: *request,
        checkpoint,
        objects: r.objects.into_values().collect(),
    })
}
