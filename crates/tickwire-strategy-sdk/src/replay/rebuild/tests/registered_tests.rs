use super::*;
use crate::{Strategy, registered, restore};
use std::{
    cell::Cell,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    rc::Rc,
};

struct Calls {
    configure: Cell<usize>,
    begin: Cell<usize>,
    restore: Cell<usize>,
}
#[derive(Clone)]
struct User {
    config: VersionedBytes,
    models: Vec<Model>,
    calls: Rc<Calls>,
}
struct Running {
    state: State,
    config: VersionedBytes,
    models: Vec<Model>,
}
impl Strategy for Running {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        self.state.checkpoint()
    }
    fn replay_registration(&self) -> crate::Result<Option<crate::replay::native::Registration>> {
        Ok(Some(crate::replay::native::Registration {
            schema: 15,
            config: self.config.clone(),
            models: self.models.clone(),
            rng: Rng::Disabled,
        }))
    }
    fn on_market(
        &mut self,
        view: crate::MarketView<'_>,
        now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.state.on_market(view, now)
    }
}
impl restore::RestoreFactory for User {
    type Running = Running;
    fn capabilities(&self) -> tickwire_release::Capabilities {
        Fixture::new().manifest.targets[0].capabilities.clone()
    }
    fn configuration(&self, _: &restore::Target) -> restore::Result<VersionedBytes> {
        Ok(self.config.clone())
    }
    fn validate_checkpoint(&self, b: &[u8]) -> restore::Result<()> {
        serde_json::from_slice::<State>(b)
            .map(|_| ())
            .map_err(|_| restore::Error::State)
    }
    fn restore(&self, _: &restore::VerifiedRestoreInput) -> restore::Result<Running> {
        self.calls.restore.set(self.calls.restore.get() + 1);
        Err(restore::Error::Unsupported)
    }
}
impl registered::UserFactory for User {
    fn create(&self) -> restore::Result<Running> {
        panic!("declared rebuild must not create")
    }
    fn begin_rebuild(&self, input: &FactoryInput) -> Result<Running> {
        self.calls.begin.set(self.calls.begin.get() + 1);
        Ok(Running {
            state: State {
                observations: 0,
                consumed: input.actions().iter().map(|a| a.economic_intent).collect(),
                emit: false,
            },
            config: self.config.clone(),
            models: self.models.clone(),
        })
    }
    fn consumed_actions(&self, b: &[u8]) -> Result<Vec<[u8; 16]>> {
        Ok(serde_json::from_slice::<State>(b).unwrap().consumed)
    }
}
struct Registered {
    calls: Rc<Calls>,
    models: Vec<Model>,
}
impl registered::Registration for Registered {
    type Factory = User;
    type Source = native::Factory;
    fn id(&self) -> &'static str {
        "rebuild_counter"
    }
    fn configure(&self, _: &restore::Target, config: &VersionedBytes) -> restore::Result<User> {
        self.calls.configure.set(self.calls.configure.get() + 1);
        Ok(User {
            config: config.clone(),
            models: self.models.clone(),
            calls: self.calls.clone(),
        })
    }
    fn source(&self, _: u64) -> Result<native::Factory> {
        panic!("rebuild must not restore a source")
    }
}
struct Files {
    path: std::path::PathBuf,
    loaded: tickwire_release::files::LoadedFiles,
    binding: registered::FactoryBinding,
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn files(f: &mut Fixture) -> Files {
    use tickwire_release::{Artifact, Role, files::*};
    let path = std::env::temp_dir().join(format!(
        "tw-rebuild-registered-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .unwrap();
    let mut locations = Vec::new();
    let mut artifacts = Vec::new();
    for (i, (role, bytes)) in [
        (Role::Binary, b"actual-test-binary".as_slice()),
        (Role::HostConfig, b"host".as_slice()),
        (Role::Bootstrap, b"bootstrap".as_slice()),
        (Role::StrategyConfig, f.inputs.config.bytes.as_slice()),
        (Role::Model(3), f.inputs.models[0].value.bytes.as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let file = path.join(i.to_string());
        std::fs::write(&file, bytes).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        locations.push(Location {
            role: role.clone(),
            path: file,
        });
        artifacts.push(Artifact {
            role,
            object: Object::of(bytes),
        });
    }
    f.contract.binary = artifacts[0].object.digest;
    f.plans.members[0].target.binary_digest = f.contract.binary;
    f.manifest.targets[0].files = artifacts;
    f.rebind();
    Files {
        binding: registered::FactoryBinding {
            id: "rebuild_counter".into(),
            configuration_file: locations[3].path.clone(),
            configuration: f.contract.config,
            revision: f.inputs.config.revision,
        },
        loaded: load(&f.manifest.targets[0].files, &locations).unwrap(),
        path,
    }
}
fn fixture() -> (Fixture, Registered) {
    let mut f = Fixture::new();
    f.inputs.models.push(Model {
        id: 3,
        value: VersionedBytes {
            revision: 7,
            bytes: b"actual-model".to_vec(),
        },
    });
    f.contract.models.push(ModelBinding {
        id: 3,
        value: Object::of(&f.inputs.models[0].value.bytes),
    });
    f.rebind();
    let r = Registered {
        calls: Rc::new(Calls {
            configure: Cell::new(0),
            begin: Cell::new(0),
            restore: Cell::new(0),
        }),
        models: f.inputs.models.clone(),
    };
    (f, r)
}
fn target(f: &Fixture) -> restore::Target {
    let t = &f.plans.members[0].target;
    restore::Target {
        instance: t.instance,
        generation: t.generation,
        logical_strategy: t.logical_strategy,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    }
}
#[test]
fn declared_registered_factory_preserves_configuration_models_and_actual_factory() {
    let (mut f, r) = fixture();
    let files = files(&mut f);
    let input = f.input().unwrap();
    let configured =
        registered::configure_rebuild(&r, &files.binding, &target(&f), &files.loaded, &input)
            .unwrap();
    assert_eq!(r.calls.configure.get(), 1);
    let candidate = execute_running(&configured, input).unwrap();
    assert_eq!(r.calls.begin.get(), 1);
    assert_eq!(r.calls.restore.get(), 0);
    assert_eq!(candidate.checkpoint().runtime.config, f.inputs.config);
    assert_eq!(candidate.checkpoint().runtime.models, f.inputs.models);
    assert_eq!(candidate.checkpoint().cut.sequence, 2);
}
#[test]
fn wrong_parameter_revision_or_target_is_rejected_before_configure() {
    let (mut f, r) = fixture();
    let files = files(&mut f);
    let mut binding = files.binding.clone();
    binding.revision += 1;
    assert!(
        registered::configure_rebuild(
            &r,
            &binding,
            &target(&f),
            &files.loaded,
            &f.input().unwrap()
        )
        .is_err()
    );
    let mut t = target(&f);
    t.generation += 1;
    assert!(
        registered::configure_rebuild(&r, &files.binding, &t, &files.loaded, &f.input().unwrap())
            .is_err()
    );
    assert_eq!(r.calls.configure.get(), 0);
    assert_eq!(r.calls.begin.get(), 0);
}
#[test]
fn factory_replay_registration_cannot_change_model_revision_after_configuration() {
    let (mut f, mut r) = fixture();
    let files = files(&mut f);
    r.models[0].value.revision += 1;
    let input = f.input().unwrap();
    let configured =
        registered::configure_rebuild(&r, &files.binding, &target(&f), &files.loaded, &input)
            .unwrap();
    assert!(matches!(
        execute_running(&configured, input),
        Err(Error::Identity)
    ));
    assert_eq!(r.calls.begin.get(), 1);
}

#[test]
fn a_different_valid_model_artifact_is_rejected_before_configure() {
    let (mut f, r) = fixture();
    let _original = files(&mut f);
    let input = f.input().unwrap();
    let (mut other, _) = fixture();
    other.inputs.models[0].value.bytes = b"another-model".to_vec();
    other.contract.models[0].value = Object::of(&other.inputs.models[0].value.bytes);
    let files = files(&mut other);
    assert!(
        registered::configure_rebuild(&r, &files.binding, &target(&f), &files.loaded, &input)
            .is_err()
    );
    assert_eq!(r.calls.configure.get(), 0);
}
