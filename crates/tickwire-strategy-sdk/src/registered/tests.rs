use std::{
    cell::Cell,
    os::unix::fs::{PermissionsExt, symlink},
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

use super::*;

#[derive(Clone)]
struct TestFactory {
    schema: u64,
    config: VersionedBytes,
}

impl UserFactory for TestFactory {
    type Running = crate::ObserveOnly;

    fn state_schema(&self) -> u64 {
        self.schema
    }

    fn configuration(&self, _: &TargetIdentity) -> Result<VersionedBytes> {
        Ok(self.config.clone())
    }

    fn create(&self) -> Result<Self::Running> {
        Ok(crate::ObserveOnly)
    }
}

struct TestRegistration {
    calls: Rc<Cell<u32>>,
    schema: u64,
}

impl Registration for TestRegistration {
    type Factory = TestFactory;

    fn id(&self) -> &'static str {
        "test_observer_v1"
    }

    fn configure(&self, _target: &TargetIdentity, c: &VersionedBytes) -> Result<Self::Factory> {
        self.calls.set(self.calls.get() + 1);
        if c.bytes != b"{\"minimum_observations\":2}" {
            return Err(Error::Input);
        }
        Ok(TestFactory {
            schema: self.schema,
            config: c.clone(),
        })
    }
}

struct TempFixture {
    dir: PathBuf,
    binding: FactoryBinding,
}

impl Drop for TempFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn make_fixture() -> TempFixture {
    let dir = std::env::temp_dir().join(format!(
        "tw-stateless-reg-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let payload = b"{\"minimum_observations\":2}";
    let cfg_path = dir.join("params.json");
    std::fs::write(&cfg_path, payload).unwrap();
    std::fs::set_permissions(&cfg_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let binding = FactoryBinding {
        id: "test_observer_v1".into(),
        state_schema: 0x5457_5f4f_4253_0001,
        configuration_file: cfg_path,
        configuration: ArtifactDigest::of(payload),
        revision: 4,
    };
    TempFixture { dir, binding }
}

fn sample_target() -> TargetIdentity {
    TargetIdentity {
        logical_strategy_id: 3,
        strat_id: 7,
        instance_id: [1; 16],
        generation: 2,
        account_ref: 1,
    }
}

#[test]
fn stateless_factory_binding_verifies_sha256_and_constructs_strategy() {
    let f = make_fixture();
    let calls = Rc::new(Cell::new(0));
    let reg = TestRegistration {
        calls: Rc::clone(&calls),
        schema: 0x5457_5f4f_4253_0001,
    };
    let configured = configure_from_file(&reg, &f.binding, &sample_target()).unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(configured.parameters().revision, 4);
    assert_eq!(
        configured.parameters().bytes,
        b"{\"minimum_observations\":2}"
    );
    let _running = configured.create().unwrap();
}

#[test]
fn tampered_digest_wrong_id_or_bad_permissions_fail_before_user_configure() {
    let f = make_fixture();
    let calls = Rc::new(Cell::new(0));
    let reg = TestRegistration {
        calls: Rc::clone(&calls),
        schema: 0x5457_5f4f_4253_0001,
    };

    // Wrong registration_id fails before invoking `Registration::configure`.
    let mut wrong_id = f.binding.clone();
    wrong_id.id = "wrong_id".into();
    assert!(configure_from_file(&reg, &wrong_id, &sample_target()).is_err());
    assert_eq!(calls.get(), 0);

    // Tampered SHA-256 fails before invoking `Registration::configure`.
    let mut bad_sha = f.binding.clone();
    bad_sha.configuration.sha256[0] ^= 0xff;
    assert!(configure_from_file(&reg, &bad_sha, &sample_target()).is_err());
    assert_eq!(calls.get(), 0);

    // Symlink configuration_file is rejected by `read_trusted_bytes`.
    let link_path = f.dir.join("symlink-params.json");
    symlink(&f.binding.configuration_file, &link_path).unwrap();
    let mut symlink_binding = f.binding.clone();
    symlink_binding.configuration_file = link_path;
    assert!(configure_from_file(&reg, &symlink_binding, &sample_target()).is_err());
    assert_eq!(calls.get(), 0);

    // World-writable file is rejected by `read_trusted_bytes`.
    let writable_path = f.dir.join("writable-params.json");
    std::fs::write(&writable_path, b"{\"minimum_observations\":2}").unwrap();
    std::fs::set_permissions(&writable_path, std::fs::Permissions::from_mode(0o666)).unwrap();
    let mut writable_binding = f.binding.clone();
    writable_binding.configuration_file = writable_path;
    assert!(configure_from_file(&reg, &writable_binding, &sample_target()).is_err());
    assert_eq!(calls.get(), 0);

    // Mismatched `state_schema` fails after `configure`.
    let mut wrong_schema = f.binding.clone();
    wrong_schema.state_schema = 999;
    assert!(configure_from_file(&reg, &wrong_schema, &sample_target()).is_err());
    assert_eq!(calls.get(), 1);
}
