#![forbid(unsafe_code)]

use tickwire_control_sys::{CpuSet, Error, MAX_CPUS, validate_cpu_isolation};

#[test]
fn cpuset_parses_formats_and_validates_bounds_and_uniqueness() {
    let single = CpuSet::single(2).unwrap();
    assert_eq!(single.as_slice(), &[2]);
    assert_eq!(single.len(), 1);
    assert!(!single.is_empty());
    assert!(single.contains(2));
    assert!(!single.contains(0));
    assert_eq!(single.to_string(), "2");

    let max_edge = CpuSet::single(4095).unwrap();
    assert_eq!(max_edge.as_slice(), &[4095]);
    assert_eq!(max_edge.to_string(), "4095");
    assert!(matches!(CpuSet::single(4096), Err(Error::InvalidResources)));
    assert!(matches!(
        CpuSet::single(u32::MAX),
        Err(Error::InvalidResources)
    ));

    let from_unsorted = CpuSet::from_cpus([10, 4, 7, 2, 5, 6]).unwrap();
    assert_eq!(from_unsorted.as_slice(), &[2, 4, 5, 6, 7, 10]);
    assert_eq!(from_unsorted.len(), 6);
    assert!(from_unsorted.contains(2));
    assert!(from_unsorted.contains(6));
    assert!(!from_unsorted.contains(3));
    assert!(!from_unsorted.contains(4096));
    assert_eq!(from_unsorted.to_string(), "2,4-7,10");

    let parsed: CpuSet = "2,4-7,10".parse().unwrap();
    assert_eq!(parsed, from_unsorted);
    assert_eq!(parsed.to_string(), "2,4-7,10");
    assert_eq!("10,4-7,2".parse::<CpuSet>().unwrap(), from_unsorted);
    assert_eq!("4-4".parse::<CpuSet>().unwrap(), CpuSet::single(4).unwrap());
    assert_eq!("0,4095".parse::<CpuSet>().unwrap().as_slice(), &[0, 4095]);

    let full = CpuSet::from_cpus(0..MAX_CPUS as u32).unwrap();
    assert_eq!(full.len(), MAX_CPUS);
    assert_eq!(full.to_string(), "0-4095");
    assert_eq!("0-4095".parse::<CpuSet>().unwrap(), full);

    for bad_vec in [vec![], vec![2, 2], vec![5, 1, 5], vec![4096], vec![0, 4096]] {
        assert!(
            matches!(
                CpuSet::from_cpus(bad_vec.clone()),
                Err(Error::InvalidResources)
            ),
            "expected rejection for {bad_vec:?}"
        );
    }

    for bad_str in [
        "",
        ",",
        "2,",
        ",2",
        "2,,4",
        "4096",
        "4090-4096",
        "0-4294967295",
        "2,2",
        "2-5,4-7",
        "4-7,5",
        "7-4",
        "4095-0",
        "abc",
        "2--4",
        "2-4-6",
        "-2",
        "2-",
        "+2",
        " 2",
        "2 ",
        "2, 4",
        "01",
        "00-2",
    ] {
        assert!(
            matches!(bad_str.parse::<CpuSet>(), Err(Error::InvalidResources)),
            "expected rejection for {bad_str:?}"
        );
    }

    let even: CpuSet = "0,2,4,6".parse().unwrap();
    let odd: CpuSet = "1,3,5,7".parse().unwrap();
    let tail: CpuSet = "6-9".parse().unwrap();
    assert!(even.is_disjoint(&odd));
    assert!(odd.is_disjoint(&even));
    assert!(!even.is_disjoint(&tail));
    assert!(!tail.is_disjoint(&even));
}

#[test]
fn validate_cpu_isolation_checks_bounds_distinct_hot_roles_and_disjoint_housekeeping() {
    let hk: CpuSet = "4-7,10".parse().unwrap();

    // Default `None` configurations are accepted.
    validate_cpu_isolation(&[], None).unwrap();
    validate_cpu_isolation(&[("req_cpu", None), ("resp_cpu", None)], None).unwrap();
    validate_cpu_isolation(&[("req_cpu", None), ("resp_cpu", None)], Some(&hk)).unwrap();
    validate_cpu_isolation(&[("req_cpu", Some(2)), ("resp_cpu", Some(3))], None).unwrap();

    // Disjoint hot cores and housekeeping cores succeed.
    validate_cpu_isolation(
        &[
            ("req_cpu", Some(2)),
            ("resp_cpu", Some(3)),
            ("md_cpu", None),
        ],
        Some(&hk),
    )
    .unwrap();

    // Duplicate hot cores (`req_cpu == resp_cpu`) fail with `ResourceConflict`.
    assert!(matches!(
        validate_cpu_isolation(&[("req_cpu", Some(2)), ("resp_cpu", Some(2))], None),
        Err(Error::ResourceConflict)
    ));
    assert!(matches!(
        validate_cpu_isolation(&[("req_cpu", Some(2)), ("resp_cpu", Some(2))], Some(&hk)),
        Err(Error::ResourceConflict)
    ));

    // Hot core overlapping `housekeeping_cpus` fails with `ResourceConflict`.
    assert!(matches!(
        validate_cpu_isolation(&[("req_cpu", Some(2)), ("resp_cpu", Some(5))], Some(&hk)),
        Err(Error::ResourceConflict)
    ));
    assert!(matches!(
        validate_cpu_isolation(&[("req_cpu", Some(10))], Some(&hk)),
        Err(Error::ResourceConflict)
    ));

    // Out-of-range hot CPU (`>= 4096`) fails immediately with `InvalidResources`.
    assert!(matches!(
        validate_cpu_isolation(&[("req_cpu", Some(4096))], None),
        Err(Error::InvalidResources)
    ));
    assert!(matches!(
        validate_cpu_isolation(
            &[
                ("req_cpu", Some(2)),
                ("resp_cpu", Some(2)),
                ("bad_cpu", Some(4096)),
            ],
            Some(&hk)
        ),
        Err(Error::InvalidResources)
    ));
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    };
    use tickwire_control_sys::{
        CpuSet, Error, current_thread_affinity, pin_current_thread, spawn_pinned_thread,
    };

    fn current_kernel_tid() -> u32 {
        let status = fs::read_to_string("/proc/thread-self/status").unwrap();
        status
            .lines()
            .find_map(|line| line.strip_prefix("Pid:\t"))
            .expect("Pid field in /proc/thread-self/status")
            .parse()
            .unwrap()
    }

    fn proc_task_cpus_allowed(tid: u32) -> CpuSet {
        let status = fs::read_to_string(format!("/proc/self/task/{tid}/status")).unwrap();
        let raw = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
            .expect("Cpus_allowed_list field in /proc/self/task/<tid>/status");
        let parsed: CpuSet = raw.parse().unwrap();
        assert_eq!(parsed.to_string(), raw);
        parsed
    }

    fn proc_task_name(tid: u32) -> String {
        let status = fs::read_to_string(format!("/proc/self/task/{tid}/status")).unwrap();
        status
            .lines()
            .find_map(|line| line.strip_prefix("Name:\t"))
            .expect("Name field in /proc/self/task/<tid>/status")
            .to_owned()
    }

    #[test]
    fn kernel_tid_affinity_escapes_taskset_parent_or_handles_single_cpu_fallback() {
        let allowed = current_thread_affinity().unwrap();
        assert_eq!(allowed, proc_task_cpus_allowed(current_kernel_tid()));

        if allowed.len() >= 2 {
            let allowed_clone = allowed.clone();
            std::thread::spawn(move || {
                let parent_tid = current_kernel_tid();
                let hot = CpuSet::single(allowed_clone.as_slice()[0]).unwrap();
                let hk = CpuSet::from_cpus(allowed_clone.as_slice()[1..].iter().copied()).unwrap();
                assert!(hot.is_disjoint(&hk));

                // Simulate a process launched under `taskset` on `hot`.
                pin_current_thread(&hot).unwrap();
                assert_eq!(current_thread_affinity().unwrap(), hot);
                assert_eq!(proc_task_cpus_allowed(parent_tid), hot);

                // Spawn housekeeping thread with `Some(&hk)` and verify it escapes `hot` both via
                // `sched_getaffinity` and `/proc/self/task/<child_tid>/status`.
                let (tid_tx, tid_rx) = std::sync::mpsc::sync_channel::<u32>(1);
                let release = Arc::new(Barrier::new(2));
                let release_child = Arc::clone(&release);
                let expected_hk = hk.clone();
                let hk_handle = spawn_pinned_thread("tw-hk", Some(&hk), move || {
                    let child_tid = current_kernel_tid();
                    assert_eq!(current_thread_affinity().unwrap(), expected_hk);
                    assert_eq!(proc_task_cpus_allowed(child_tid), expected_hk);
                    assert_eq!(proc_task_name(child_tid), "tw-hk");
                    tid_tx.send(child_tid).unwrap();
                    release_child.wait();
                    child_tid
                })
                .unwrap();

                // Verify `/proc/self/task/<child_tid>/status` from the parent thread while child
                // is live, and confirm parent TID remained pinned to `hot`.
                let live_child_tid = tid_rx.recv().unwrap();
                assert_ne!(live_child_tid, parent_tid);
                assert_eq!(proc_task_cpus_allowed(live_child_tid), hk);
                assert_eq!(current_thread_affinity().unwrap(), hot);
                assert_eq!(proc_task_cpus_allowed(parent_tid), hot);
                release.wait();
                assert_eq!(hk_handle.join().unwrap(), live_child_tid);

                // Spawn unpinned thread (`None`) from the `hot`-pinned parent and verify clean
                // inheritance of `hot`.
                let expected_hot = hot.clone();
                let inherit_handle = spawn_pinned_thread("tw-inherit", None, move || {
                    let tid = current_kernel_tid();
                    assert_eq!(current_thread_affinity().unwrap(), expected_hot);
                    assert_eq!(proc_task_cpus_allowed(tid), expected_hot);
                    assert_eq!(proc_task_name(tid), "tw-inherit");
                })
                .unwrap();
                inherit_handle.join().unwrap();
            })
            .join()
            .unwrap();
        } else {
            // Single-CPU fallback when the host/container exposes only 1 allowed CPU.
            assert_eq!(allowed.len(), 1);
            let only = CpuSet::single(allowed.as_slice()[0]).unwrap();
            std::thread::spawn(move || {
                let parent_tid = current_kernel_tid();
                pin_current_thread(&only).unwrap();
                assert_eq!(current_thread_affinity().unwrap(), only);
                assert_eq!(proc_task_cpus_allowed(parent_tid), only);

                let expected = only.clone();
                spawn_pinned_thread("tw-single", Some(&only), move || {
                    let child_tid = current_kernel_tid();
                    assert_eq!(current_thread_affinity().unwrap(), expected);
                    assert_eq!(proc_task_cpus_allowed(child_tid), expected);
                })
                .unwrap()
                .join()
                .unwrap();

                let expected_inherit = only.clone();
                spawn_pinned_thread("tw-inherit", None, move || {
                    let child_tid = current_kernel_tid();
                    assert_eq!(current_thread_affinity().unwrap(), expected_inherit);
                    assert_eq!(proc_task_cpus_allowed(child_tid), expected_inherit);
                })
                .unwrap()
                .join()
                .unwrap();
            })
            .join()
            .unwrap();
        }
    }

    #[test]
    fn single_cpu_pinning_and_inheritance_work_on_any_linux_host() {
        let allowed = current_thread_affinity().unwrap();
        let only = CpuSet::single(allowed.as_slice()[0]).unwrap();
        std::thread::spawn(move || {
            let parent_tid = current_kernel_tid();
            pin_current_thread(&only).unwrap();
            assert_eq!(current_thread_affinity().unwrap(), only);
            assert_eq!(proc_task_cpus_allowed(parent_tid), only);

            let expected = only.clone();
            spawn_pinned_thread("tw-one", Some(&only), move || {
                let tid = current_kernel_tid();
                assert_eq!(current_thread_affinity().unwrap(), expected);
                assert_eq!(proc_task_cpus_allowed(tid), expected);
            })
            .unwrap()
            .join()
            .unwrap();

            let expected_inherit = only.clone();
            spawn_pinned_thread("tw-one-inh", None, move || {
                let tid = current_kernel_tid();
                assert_eq!(current_thread_affinity().unwrap(), expected_inherit);
                assert_eq!(proc_task_cpus_allowed(tid), expected_inherit);
            })
            .unwrap()
            .join()
            .unwrap();
        })
        .join()
        .unwrap();
    }

    struct DropTracker(Arc<AtomicBool>);
    impl Drop for DropTracker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn unreachable_or_clipped_cpu_fails_synchronously_without_executing_closure() {
        let allowed = current_thread_affinity().unwrap();
        if !allowed.contains(4095) {
            let unreachable = CpuSet::single(4095).unwrap();
            let executed = Arc::new(AtomicBool::new(false));
            let dropped = Arc::new(AtomicBool::new(false));
            let executed_child = Arc::clone(&executed);
            let guard = DropTracker(Arc::clone(&dropped));

            let result = spawn_pinned_thread("tw-unreach", Some(&unreachable), move || {
                let _keep = guard;
                executed_child.store(true, Ordering::SeqCst);
                99u64
            });
            assert!(result.is_err());
            assert!(
                !executed.load(Ordering::SeqCst),
                "closure must never run when pinning fails"
            );
            assert!(
                dropped.load(Ordering::SeqCst),
                "closure must be dropped before spawn_pinned_thread returns Err"
            );

            // Partial mask combining an online allowed CPU with unreachable CPU 4095: exercises
            // kernel silent clipping detection via mandatory `sched_getaffinity` read-back.
            let clipped = CpuSet::from_cpus([allowed.as_slice()[0], 4095]).unwrap();
            let clip_executed = Arc::new(AtomicBool::new(false));
            let clip_executed_child = Arc::clone(&clip_executed);
            let clipped_for_direct = clipped.clone();

            std::thread::spawn(move || {
                assert!(pin_current_thread(&clipped_for_direct).is_err());
            })
            .join()
            .unwrap();

            let clip_result = spawn_pinned_thread("tw-clipped", Some(&clipped), move || {
                clip_executed_child.store(true, Ordering::SeqCst);
            });
            assert!(clip_result.is_err());
            assert!(
                !clip_executed.load(Ordering::SeqCst),
                "closure must never run when kernel clips requested CPU mask"
            );
        }

        // Interior NUL thread names fail cleanly without panicking or running the closure.
        let nul_ran = Arc::new(AtomicBool::new(false));
        let nul_ran_child = Arc::clone(&nul_ran);
        assert!(matches!(
            spawn_pinned_thread("bad\0name", None, move || {
                nul_ran_child.store(true, Ordering::SeqCst);
            }),
            Err(Error::InvalidResources)
        ));
        assert!(!nul_ran.load(Ordering::SeqCst));
    }
}
