use super::*;
use crate::restore::RestoreFactory;
use std::{cell::Cell, os::unix::fs::PermissionsExt, rc::Rc};
#[derive(Clone)]
struct Factory {
    config: VersionedBytes,
}
impl RestoreFactory for Factory {
    type Running = crate::ObserveOnly;
    fn capabilities(&self) -> tickwire_release::Capabilities {
        restore::ObserveFactory.capabilities()
    }
    fn configuration(&self, _: &restore::Target) -> restore::Result<VersionedBytes> {
        Ok(self.config.clone())
    }
    fn validate_checkpoint(&self, b: &[u8]) -> restore::Result<()> {
        restore::ObserveFactory.validate_checkpoint(b)
    }
}
impl UserFactory for Factory {
    fn create(&self) -> restore::Result<crate::ObserveOnly> {
        Ok(crate::ObserveOnly)
    }
}
struct Registered(Rc<Cell<u32>>);
impl Registration for Registered {
    type Factory = Factory;
    type Source = crate::replay::native::Factory;
    fn id(&self) -> &'static str {
        "test_observer"
    }
    fn configure(&self, _: &restore::Target, c: &VersionedBytes) -> restore::Result<Factory> {
        self.0.set(self.0.get() + 1);
        if c.bytes != b"actual-user-parameters" {
            return Err(restore::Error::Input);
        }
        Ok(Factory { config: c.clone() })
    }
    fn source(&self, schema: u64) -> crate::replay::Result<Self::Source> {
        Ok(crate::replay::native::Factory { schema })
    }
}
struct Files {
    directory: PathBuf,
    files: LoadedFiles,
    binding: FactoryBinding,
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn fixture() -> Files {
    let directory = std::env::temp_dir().join(format!(
        "tw-registered-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut artifacts = Vec::new();
    let mut locations = Vec::new();
    for (i, role) in [
        Role::Binary,
        Role::HostConfig,
        Role::Bootstrap,
        Role::StrategyConfig,
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = if i == 3 {
            b"actual-user-parameters".to_vec()
        } else {
            vec![i as u8 + 1; 8]
        };
        let path = directory.join(format!("{i}"));
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        artifacts.push(tickwire_release::Artifact {
            role: role.clone(),
            object: tickwire_release::Object::of(&bytes),
        });
        locations.push(tickwire_release::files::Location { role, path });
    }
    let binding = FactoryBinding {
        id: "test_observer".into(),
        configuration_file: locations[3].path.clone(),
        configuration: artifacts[3].object,
        revision: 4,
    };
    let files = tickwire_release::files::load(&artifacts, &locations).unwrap();
    Files {
        directory,
        files,
        binding,
    }
}
fn target() -> restore::Target {
    restore::Target {
        instance: [1; 16],
        generation: 2,
        logical_strategy: 3,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    }
}
#[test]
fn qualified_user_configuration_executes_registered_decoder_and_retains_exact_bytes() {
    let f = fixture();
    let calls = Rc::new(Cell::new(0));
    let factory = configure(&Registered(calls.clone()), &f.binding, &target(), &f.files).unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(
        factory.parameters,
        VersionedBytes {
            revision: 4,
            bytes: b"actual-user-parameters".to_vec()
        }
    );
}
#[test]
fn wrong_registration_or_unbound_path_does_not_run_user_code() {
    let f = fixture();
    let calls = Rc::new(Cell::new(0));
    let r = Registered(calls.clone());
    let mut bad = f.binding.clone();
    bad.id = "not_registered".into();
    assert!(configure(&r, &bad, &target(), &f.files).is_err());
    bad = f.binding.clone();
    bad.configuration_file = f.directory.join("same-bytes-unbound");
    std::fs::write(&bad.configuration_file, b"actual-user-parameters").unwrap();
    assert!(configure(&r, &bad, &target(), &f.files).is_err());
    assert_eq!(calls.get(), 0);
}

#[test]
fn capture_entry_reads_only_the_static_registration_id() {
    struct CaptureRegistration;
    impl Registration for CaptureRegistration {
        type Factory = Factory;
        type Source = crate::replay::native::Factory;
        fn id(&self) -> &'static str {
            "capture-test"
        }
        fn configure(&self, _: &restore::Target, _: &VersionedBytes) -> restore::Result<Factory> {
            panic!("capture must not configure user factory")
        }
        fn source(&self, _: u64) -> crate::replay::Result<Self::Source> {
            panic!("capture must not restore a source factory")
        }
    }
    let mut config = crate::capture::tests::fixture();
    capture_entry(&CaptureRegistration, &config).unwrap();
    config.contract.factory = "another-factory".into();
    assert!(capture_entry(&CaptureRegistration, &config).is_err());
}
