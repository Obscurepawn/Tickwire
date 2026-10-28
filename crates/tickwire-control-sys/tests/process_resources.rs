#![cfg(target_os = "linux")]
//! Requires an explicitly supplied, preconfigured, empty, delegated cgroup v2 leaf.
//! Example: TICKWIRE_RESOURCE_CGROUP=/sys/fs/cgroup/... TICKWIRE_RESOURCE_CPUS=2,3
//! TICKWIRE_RESOURCE_NUMA=0 TICKWIRE_RESOURCE_MEMORY_MAX=268435456 cargo test ... --ignored
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tickwire_control_sys::{ExecutableImage, ResourceDomain, ResourceLimits};

#[test]
#[ignore = "requires an empty delegated cgroup v2 leaf with exact limits, swap=0 and migration permission"]
fn delegated_leaf_sealed_child_is_constrained_exclusive_and_observed() {
    let path = PathBuf::from(
        std::env::var_os("TICKWIRE_RESOURCE_CGROUP").expect("explicit delegated leaf"),
    );
    let limits = ResourceLimits {
        cpus: std::env::var("TICKWIRE_RESOURCE_CPUS")
            .expect("comma separated exact CPU IDs")
            .split(',')
            .map(|v| v.parse().unwrap())
            .collect(),
        numa_node: std::env::var("TICKWIRE_RESOURCE_NUMA")
            .expect("exact NUMA node")
            .parse()
            .unwrap(),
        memory_max: std::env::var("TICKWIRE_RESOURCE_MEMORY_MAX")
            .expect("exact memory.max bytes")
            .parse()
            .unwrap(),
    };
    let mut domain = ResourceDomain::acquire(&path, limits.clone()).unwrap();
    assert!(ResourceDomain::acquire(&path, limits.clone()).is_err());
    let image = ExecutableImage::open(
        &std::fs::canonicalize("/bin/sleep").unwrap(),
        512 * 1024 * 1024,
    )
    .unwrap();
    let mut child = domain
        .spawn(&image, |c| {
            c.arg("30").stdout(Stdio::null()).stderr(Stdio::inherit());
        })
        .unwrap();
    let observed = domain.observe(child.id()).unwrap();
    assert_eq!(observed.pid(), child.id());
    assert_eq!(observed.limits(), &limits);
    assert_eq!(observed.thread_ids(), &[child.id()]);
    assert!(observed.group_inode() != 0);
    assert!(image.matches_process(child.id()).unwrap());
    assert!(
        domain
            .spawn(&image, |c| {
                c.arg("30");
            })
            .is_err()
    );
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(domain.observe(child.id()).is_err());
    drop(domain);
    // Drop never deletes or resets the preconfigured leaf; a new lease can inspect it.
    let _new = ResourceDomain::acquire(Path::new(&path), limits).unwrap();
}
