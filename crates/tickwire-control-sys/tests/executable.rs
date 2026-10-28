#![cfg(target_os = "linux")]
use std::{fs, io::Read, path::Path, process::Stdio};
use tickwire_control_sys::{ExecutableImage, PrivateDirectory};
const MAX: u64 = 512 * 1024 * 1024;
struct Fixture(PrivateDirectory);
impl Fixture {
    fn new() -> Self {
        Self(PrivateDirectory::create_in(Path::new("/tmp"), "tw-executable").unwrap())
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.path().join("image")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(self.0.path());
    }
}
fn output(image: &ExecutableImage) -> std::process::Output {
    image
        .spawn(|c| {
            c.arg("fixed-image")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
        })
        .unwrap()
        .wait_with_output()
        .unwrap()
}
#[test]
fn prepared_image_survives_path_replacement_and_unlink() {
    let fixture = Fixture::new();
    let path = fixture.path();
    fs::copy("/bin/echo", &path).unwrap();
    let original = fs::read(&path).unwrap();
    let image = ExecutableImage::open(&path, MAX).unwrap();
    let replacement = fixture.0.path().join("replacement");
    fs::copy("/bin/false", &replacement).unwrap();
    fs::rename(replacement, &path).unwrap();
    let run = output(&image);
    assert!(run.status.success());
    assert_eq!(run.stdout, b"fixed-image\n");
    fs::remove_file(&path).unwrap();
    let run = output(&image);
    assert!(run.status.success());
    assert_eq!(run.stdout, b"fixed-image\n");
    let mut read = Vec::new();
    image.reader().unwrap().read_to_end(&mut read).unwrap();
    assert_eq!(read, original);
}
#[test]
fn prepared_image_survives_in_place_source_overwrite() {
    let fixture = Fixture::new();
    let path = fixture.path();
    fs::copy("/bin/echo", &path).unwrap();
    let image = ExecutableImage::open(&path, MAX).unwrap();
    fs::write(&path, fs::read("/bin/false").unwrap()).unwrap();
    let run = output(&image);
    assert!(run.status.success());
    assert_eq!(run.stdout, b"fixed-image\n");
}

#[test]
fn sealed_image_rejects_writes_growth_and_truncation_and_has_independent_readers() {
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    let image = ExecutableImage::open(Path::new("/bin/echo"), MAX).unwrap();
    let mut first = image.reader().unwrap();
    let mut magic = [0; 4];
    first.read_exact(&mut magic).unwrap();
    assert_eq!(&magic, b"\x7fELF");
    let mut second = image.reader().unwrap();
    second.read_exact(&mut magic).unwrap();
    assert_eq!(&magic, b"\x7fELF");
    // Restoring a write permission bit must not defeat the kernel content seals.
    first
        .set_permissions(fs::Permissions::from_mode(0o755))
        .unwrap();
    let path = format!("/proc/self/fd/{}", first.as_raw_fd());
    let mut writable = fs::OpenOptions::new().write(true).open(path).unwrap();
    assert!(writable.write_all(b"bad").is_err());
    assert!(writable.set_len(0).is_err());
    assert!(writable.set_len(image.len() + 1).is_err());
    writable.seek(SeekFrom::End(0)).unwrap();
    assert!(writable.write_all(b"x").is_err());
    drop(writable);
    first
        .set_permissions(fs::Permissions::from_mode(0o555))
        .unwrap();
    assert!(output(&image).status.success());
}
#[test]
fn only_the_fixed_sealed_image_matches_the_actual_running_elf() {
    let image = ExecutableImage::open(Path::new("/bin/sleep"), MAX).unwrap();
    let same_bytes = ExecutableImage::open(Path::new("/bin/sleep"), MAX).unwrap();
    let mut child = image
        .spawn(|c| {
            c.arg("30");
        })
        .unwrap();
    assert!(image.matches_process(child.id()).unwrap());
    assert!(!same_bytes.matches_process(child.id()).unwrap());
    assert!(!image.matches_process(std::process::id()).unwrap());
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(image.matches_process(child.id()).is_err());
}
#[test]
fn invalid_images_and_replaced_command_program_fail_before_exec() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let fixture = Fixture::new();
    let path = fixture.path();
    fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(ExecutableImage::open(&path, MAX).is_err());
    fs::remove_file(&path).unwrap();
    symlink("/bin/echo", &path).unwrap();
    assert!(ExecutableImage::open(&path, MAX).is_err());
    assert!(ExecutableImage::open(Path::new("/bin/echo"), 64).is_err());
    assert!(ExecutableImage::open(Path::new("/bin/echo"), MAX + 1).is_err());
    let image = ExecutableImage::open(Path::new("/bin/echo"), MAX).unwrap();
    assert!(
        image
            .spawn(|c| {
                *c = std::process::Command::new("/bin/true");
            })
            .is_err()
    );
}
#[test]
fn real_dynamic_test_child_preserves_argv0_and_does_not_inherit_image_descriptors() {
    let fixture = Fixture::new();
    let path = fixture.path();
    fs::copy(std::env::current_exe().unwrap(), &path).unwrap();
    let image = ExecutableImage::open(&path, MAX).unwrap();
    let _reader = image.reader().unwrap();
    let child = image
        .spawn(|c| {
            c.args(["--exact", "executable_child", "--ignored", "--nocapture"])
                .env_clear()
                .env("TW_EXECUTABLE_CHILD", "inspect")
                .env("TW_EXECUTABLE_ARGV0", &path)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
        })
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
#[ignore = "invoked by real_dynamic_test_child_preserves_argv0_and_does_not_inherit_image_descriptors"]
fn executable_child() {
    assert_eq!(std::env::var("TW_EXECUTABLE_CHILD").unwrap(), "inspect");
    assert_eq!(
        std::env::args_os().next().unwrap(),
        std::env::var_os("TW_EXECUTABLE_ARGV0").unwrap()
    );
    let actual = fs::read_link("/proc/self/exe").unwrap();
    assert!(
        actual
            .to_string_lossy()
            .contains("memfd:tickwire-executable")
    );
    assert_eq!(&fs::read("/proc/self/exe").unwrap()[..4], b"\x7fELF");
    for entry in fs::read_dir("/proc/self/fd").unwrap() {
        if let Ok(target) = fs::read_link(entry.unwrap().path()) {
            assert!(
                !target
                    .to_string_lossy()
                    .contains("memfd:tickwire-executable")
            );
        }
    }
}
