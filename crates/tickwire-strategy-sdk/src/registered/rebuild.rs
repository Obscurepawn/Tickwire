use super::*;
use crate::replay::{self, rebuild as declared};

pub struct RebuildConfigured<F: UserFactory> {
    configured: Configured<F>,
    binding: declared::CompiledFactory,
    models: Vec<tickwire_release::replay::Model>,
    rng: tickwire_release::replay::Rng,
}
/// Every artifact/configuration binding is checked before user configure runs.
/// The caller has already authenticated the installation request and binary.
pub fn configure_rebuild<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &restore::Target,
    files: &LoadedFiles,
    input: &declared::VerifiedInput,
) -> Result<RebuildConfigured<R::Factory>> {
    let source = &input.plan().target;
    let contract = input.contract();
    let parameters = &input.inputs().config;
    let artifact = input
        .manifest()
        .targets
        .iter()
        .find(|t| t.member == input.plan().member)
        .ok_or(Error::Identity)?;
    if registration.id() != contract.factory
        || binding.id != contract.factory
        || binding.configuration != contract.config
        || binding.revision != parameters.revision
        || (target.instance, target.generation, target.logical_strategy)
            != (source.instance, source.generation, source.logical_strategy)
        || files.bytes(&Role::StrategyConfig) != Some(parameters.bytes.as_slice())
        || files.object(&Role::Binary).map(|o| o.digest) != Some(contract.binary)
        || artifact
            .files
            .iter()
            .any(|f| files.object(&f.role) != Some(f.object))
        || input
            .inputs()
            .models
            .iter()
            .any(|m| files.bytes(&Role::Model(m.id)) != Some(m.value.bytes.as_slice()))
    {
        return Err(Error::Identity);
    }
    let configured = configure(registration, binding, target, files)?;
    let actual = configured.capabilities();
    let expected = &artifact.capabilities;
    let declared = tickwire_release::Method::Rebuild {
        target_schema: contract.state_schema,
        contract: tickwire_release::Object::of(&contract.bytes()?).digest,
    };
    if actual.state_schema != expected.state_schema
        || actual.input_schema != expected.input_schema
        || actual.output_schema != expected.output_schema
        || actual.products != expected.products
        || actual.position_modes != expected.position_modes
        || actual.orders != expected.orders
        || expected
            .methods
            .iter()
            .any(|m| m != &declared && !actual.methods.contains(m))
    {
        return Err(Error::Identity);
    }
    Ok(RebuildConfigured {
        configured,
        binding: declared::CompiledFactory {
            id: contract.factory.clone(),
            binary: contract.binary,
            state_schema: contract.state_schema,
        },
        models: input.inputs().models.clone(),
        rng: contract.rng.clone(),
    })
}
impl<F: UserFactory> declared::Factory for RebuildConfigured<F> {
    type Running = Running<F>;
    fn binding(&self) -> declared::CompiledFactory {
        self.binding.clone()
    }
    fn begin(&self, input: &declared::FactoryInput) -> replay::Result<Self::Running> {
        if input.configuration() != &self.configured.parameters
            || input.models() != self.models
            || input.contract().rng != self.rng
        {
            return Err(replay::Error::Identity);
        }
        let running = self
            .configured
            .wrap(self.configured.factory.begin_rebuild(input)?);
        let registration = running
            .replay_registration()
            .map_err(|_| replay::Error::Identity)?
            .ok_or(replay::Error::Identity)?;
        if registration.models != self.models || registration.rng != self.rng {
            return Err(replay::Error::Identity);
        }
        Ok(running)
    }
    fn consumed_actions(&self, checkpoint: &[u8]) -> replay::Result<Vec<[u8; 16]>> {
        self.configured.factory.consumed_actions(checkpoint)
    }
    fn validate_checkpoint(&self, checkpoint: &[u8]) -> replay::Result<()> {
        self.configured
            .validate_checkpoint(checkpoint)
            .map_err(|_| replay::Error::StateMismatch)
    }
}

pub(super) fn install<R: Registration>(
    registration: &R,
    mut session: crate::capture_worker::CapturedSession,
    request: crate::capture::install::Request,
) -> Result<()> {
    session.check_install(&request)?;
    // No missing durable root can be supplied by an in-memory replacement.
    for object in &request.objects {
        session.read_object(*object)?;
        session.check_install(&request)?;
    }
    let plans =
        tickwire_release::rebuild::Plans::decode(&session.read_object(request.request.plans)?)?;
    let manifest: tickwire_release::ReleaseManifest =
        serde_json::from_slice(&session.read_object(request.request.manifest)?)?;
    let window = session.window();
    let plan = request.request.verify(
        &plans,
        window.contract(),
        window.inputs(),
        &manifest,
        window.seal(),
    )?;
    window.validate_plan(plan)?;
    let input = declared::VerifiedInput::verify(
        &request.request.operation,
        &request.request.member,
        &plans,
        window.contract(),
        window.inputs(),
        &manifest,
    )?;
    let artifacts = &manifest
        .targets
        .iter()
        .find(|t| t.member == request.request.member)
        .ok_or(Error::Identity)?
        .files;
    let files = tickwire_release::files::load(artifacts, &request.files)?;
    if crate::replay::live::executable_digest()? != session.bootstrap().identity.binary {
        return Err(Error::Identity);
    }
    session.check_install(&request)?;
    let target = target_identity(&request.bootstrap)?;
    let factory = configure_rebuild(
        registration,
        request.bootstrap.factory.as_ref().ok_or(Error::Identity)?,
        &target,
        &files,
        &input,
    )?;
    session.check_install(&request)?;
    let running = declared::execute_running(&factory, input)?;
    session.check_install(&request)?;
    files.recheck()?;
    session.check_install(&request)?;
    let directory = request.bootstrap.command_directory.join("merged-inputs");
    let running = running.persist(tickwire_recovery::FileBlobStore::open(&directory)?)?;
    session.check_install(&request)?;
    let warm_completion = running.computation().object();
    let anchor = tickwire_release::Object::of(&running.anchor().bytes()?);
    let checkpoint = tickwire_release::Object::of(&running.checkpoint().bytes()?);
    let genesis_commit = running.genesis_commit();
    let commit = running.commit();
    let head = running.head()?;
    let history_objects = running.history_objects()?;
    let live = crate::replay::live::Live::from_rebuilt(running, directory)?;
    session.check_install(&request)?;
    session.install_and_run(
        request,
        crate::capture_worker::Computed {
            live,
            warm_completion,
            anchor,
            checkpoint,
            genesis_commit,
            commit,
            head,
            history_objects,
        },
    )
}
