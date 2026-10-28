use super::*;
use std::{
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
use tickwire_release::{
    Artifact, Role,
    files::{self, Location},
};

struct Directory(std::path::PathBuf);
impl Directory {
    fn new(replace: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-sdk-image-{}-{replace}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn wait(path: &Path) {
    let until = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < until,
            "child handshake timed out: {path:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn actual_image_child() {
    let Some(directory) = std::env::var_os("TICKWIRE_IMAGE_TEST_DIRECTORY") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    std::fs::write(directory.join("ready"), []).unwrap();
    wait(&directory.join("continue"));
    let retained = std::fs::read(directory.join("retained-artifact")).unwrap();
    let expected = Object::of(&retained);
    assert_eq!(executable_object().unwrap(), expected);
    let mut artifacts = vec![Artifact {
        role: Role::Binary,
        object: expected,
    }];
    let mut locations = vec![Location {
        role: Role::Binary,
        path: directory.join("retained-artifact"),
    }];
    for (role, name) in [
        (Role::HostConfig, "host"),
        (Role::Bootstrap, "bootstrap"),
        (Role::StrategyConfig, "parameters"),
    ] {
        let path = directory.join(name);
        std::fs::write(&path, name).unwrap();
        artifacts.push(Artifact {
            role: role.clone(),
            object: Object::of(name.as_bytes()),
        });
        locations.push(Location { role, path });
    }
    let loaded = files::load(&artifacts, &locations).unwrap();
    verify_executable(loaded.object(&Role::Binary).unwrap()).unwrap();
    let mut wrong_digest = expected;
    wrong_digest.digest[0] ^= 1;
    assert!(verify_executable(wrong_digest).is_err());
    let mut wrong_length = expected;
    wrong_length.bytes += 1;
    assert!(verify_executable(wrong_length).is_err());
    std::fs::remove_file(directory.join("retained-artifact")).unwrap();
    assert!(
        loaded.recheck().is_err(),
        "a missing recovery artifact is not replaced by the live image"
    );
}

#[test]
fn actual_running_image_survives_removed_and_replaced_launch_path() {
    for replace in [false, true] {
        let temporary = Directory::new(replace);
        let directory = &temporary.0;
        let launch = directory.join("launch-image");
        let retained = directory.join("retained-artifact");
        let original = std::env::current_exe().unwrap();
        std::fs::copy(&original, &launch).unwrap();
        std::fs::copy(&original, &retained).unwrap();
        let mut child = Command::new(&launch)
            .arg("--exact")
            .arg("replay::live::image_tests::actual_image_child")
            .arg("--nocapture")
            .env("TICKWIRE_IMAGE_TEST_DIRECTORY", directory)
            .spawn()
            .unwrap();
        wait(&directory.join("ready"));
        std::fs::remove_file(&launch).unwrap();
        if replace {
            std::fs::write(&launch, b"a different candidate is not the running image").unwrap();
        }
        std::fs::write(directory.join("continue"), []).unwrap();
        assert!(child.wait().unwrap().success(), "replacement={replace}");
    }
}
