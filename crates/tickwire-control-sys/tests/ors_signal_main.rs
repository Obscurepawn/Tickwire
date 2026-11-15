#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
fn proc_sigblk_mask(tid: u32) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/self/task/{tid}/status")).unwrap();
    let hex = status
        .lines()
        .find_map(|line| line.strip_prefix("SigBlk:\t"))
        .expect("SigBlk in /proc/self/task/<tid>/status");
    u64::from_str_radix(hex.trim(), 16).unwrap()
}

#[cfg(target_os = "linux")]
fn current_tid() -> u32 {
    let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("Pid:\t"))
        .unwrap()
        .parse()
        .unwrap()
}

fn main() {
    #[cfg(target_os = "linux")]
    {
        use std::process::Command;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::time::Duration;
        use tickwire_control_sys::{
            OrsSignal, SignalWaiter, block_ors_signals_current_thread, spawn_pinned_thread,
        };

        // ORS main contract: mask signals BEFORE spawning any worker threads.
        block_ors_signals_current_thread().unwrap();
        let main_blk = proc_sigblk_mask(current_tid());
        assert_ne!(main_blk, 0);

        // Keep a live background thread running during process-directed signal delivery to prove
        // child threads inherit `SigBlk` and never preempt/terminate on SIGUSR1/SIGUSR2/SIGINT/SIGTERM.
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = Arc::clone(&stop);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let bg = spawn_pinned_thread("tw-ors-bg", None, move || {
            let blk = proc_sigblk_mask(current_tid());
            tx.send(blk).unwrap();
            while !stop_bg.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(2));
            }
        })
        .unwrap();

        let child_blk = rx.recv().unwrap();
        assert_eq!(main_blk, child_blk);

        let waiter = SignalWaiter::without_blocking().unwrap();
        assert_eq!(waiter.wait_timeout(Duration::from_millis(5)).unwrap(), None);

        let pid_str = std::process::id().to_string();
        for (flag, expected) in [
            ("-USR1", OrsSignal::SigUsr1),
            ("-USR2", OrsSignal::SigUsr2),
            ("-INT", OrsSignal::SigInt),
            ("-TERM", OrsSignal::SigTerm),
        ] {
            let status = Command::new("kill")
                .args([flag, &pid_str])
                .status()
                .unwrap();
            assert!(status.success(), "kill {flag} {pid_str} failed");
            let got = waiter
                .wait_timeout(Duration::from_secs(2))
                .unwrap()
                .expect("expected pending OrsSignal via sigtimedwait");
            assert_eq!(got, expected);
        }

        stop.store(true, Ordering::Release);
        bg.join().unwrap();
    }
}
