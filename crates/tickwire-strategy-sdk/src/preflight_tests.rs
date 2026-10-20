use super::*;
use std::os::unix::fs::PermissionsExt;
use tickwire_release::{Artifact, files::Location};
#[test]
fn every_recovery_input_is_bound_to_the_actual_template_and_artifact_path() {
    let dir = std::env::temp_dir().join(format!("tw-preflight-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    struct Clean(PathBuf);
    impl Drop for Clean {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _clean = Clean(dir.clone());
    let mut artifacts = Vec::new();
    let mut paths = Vec::new();
    for (i, role) in [
        Role::Binary,
        Role::HostConfig,
        Role::Bootstrap,
        Role::StrategyConfig,
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = vec![i as u8 + 1; 8];
        let path = dir.join(format!("a{i}"));
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        artifacts.push(Artifact {
            role: role.clone(),
            object: Object::of(&bytes),
        });
        paths.push(Location { role, path });
    }
    let template_path = dir.join("recovery.json");
    let template = crate::recovery::Template {
        dependencies: vec![crate::recovery::DependencyFile {
            kind: 11,
            schema_id: 1,
            owner: [1; 16],
            path: paths[3].path.clone(),
            digest: artifacts[3].object.digest,
        }],
        declared_rebuild: false,
        fault_model_id: [1; 32],
        pin_id: [1; 16],
        owner: [1; 16],
        input_stream: [1; 16],
        output_directory: dir.join("out"),
        maximum_input_bytes: 1024,
        order_ids: vec![[1; 16]],
    };
    let raw = serde_json::to_vec(&template).unwrap();
    std::fs::write(&template_path, &raw).unwrap();
    std::fs::set_permissions(&template_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let loaded = files::load(&artifacts, &paths).unwrap();
    assert!(
        verify_recovery_inputs(Some(&template_path), &loaded).is_err(),
        "unbound template must be rejected"
    );
    artifacts.push(Artifact {
        role: Role::RecoveryTemplate,
        object: Object::of(&raw),
    });
    paths.push(Location {
        role: Role::RecoveryTemplate,
        path: template_path.clone(),
    });
    let loaded = files::load(&artifacts, &paths).unwrap();
    assert!(verify_recovery_inputs(Some(&template_path), &loaded).is_ok());
    assert!(verify_recovery_inputs(None, &loaded).is_err());
    assert!(verify_recovery_inputs(Some(&dir.join("different-template")), &loaded).is_err());
    let mut changed = template.clone();
    changed.dependencies[0].path = dir.join("unbound-identical-bytes");
    std::fs::write(&changed.dependencies[0].path, vec![4u8; 8]).unwrap();
    let raw = serde_json::to_vec(&changed).unwrap();
    std::fs::write(&template_path, &raw).unwrap();
    artifacts[4].object = Object::of(&raw);
    let loaded = files::load(&artifacts, &paths).unwrap();
    assert!(
        verify_recovery_inputs(Some(&template_path), &loaded).is_err(),
        "equal bytes at an unbound path must be rejected"
    );
}
