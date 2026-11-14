#![forbid(unsafe_code)]

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use std::{fs::File, io::Read, time::Instant};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::FileBackend;
use tickwire_app::strategy::{Config, FileStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{MockBlobStore, SharedBlobStore};
use tickwire_runtime::authority::RecoveryQueue;
use tickwire_spot_execution::{Credentials, RestSession, SpotEncoder, SpotVenue};
use tickwire_spot_runner::AccountRunner;
use tickwire_transport::IoBudget;
use tickwire_venue::Environment;

fn current_vm_data_kib() -> Option<u64> {
    let mut buf = [0u8; 4096];
    let mut f = File::open("/proc/self/status").ok()?;
    let n = f.read(&mut buf).ok()?;
    let status = std::str::from_utf8(&buf[..n]).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmData:") {
            let digits = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(digits);
        }
    }
    None
}

#[test]
fn steady_state_maybe_warm_hot_path_incurs_zero_heap_growth() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let dir = common::Directory::new();
        let (mut snapshot, catalog, mut source) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        source.context.request.venue = venue.id();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let creds = match venue {
            SpotVenue::Bybit => Credentials::bybit("paper", b"local-paper-secret"),
            SpotVenue::Okx => Credentials::okx("paper", b"local-paper-secret", "paper-pass"),
        };
        let encoder =
            SpotEncoder::new(creds, Environment::Test, 2, || Ok(1_700_000_000_000)).unwrap();
        let mut cfg = common::config(&baseline);
        cfg.route = encoder.route();
        let (connection, wire) = common::peer(&cfg.route.authority, 16384);
        let queue = RecoveryQueue::new(2, 1024 * 1024).unwrap();
        let objects = SharedBlobStore::new(MockBlobStore::default());
        let gateway = AccountGateway::create(
            FileBackend::open(&dir.0).unwrap(),
            cfg,
            baseline,
            common::MockAuthority::default(),
            encoder,
            connection,
        )
        .unwrap();
        let mut runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects)
            .with_warmup_interval_ns(500_000)
            .unwrap();

        // Settle the peer thread's accept + TLS handshake before sampling process VmData.
        let domain = ClockDomain::new([4; 16], 1).unwrap();
        let mut idle_strategy = FileStrategy::new(&Config::Observe).unwrap();
        let deadline = common::until();
        while !runner.available() {
            assert!(Instant::now() < deadline);
            runner
                .poll(
                    &mut idle_strategy,
                    LocalTime {
                        instant: Instant::now(),
                        monotonic: MonoTime::new(domain, 50),
                    },
                    IoBudget::default(),
                )
                .unwrap();
            std::thread::yield_now();
        }

        let mut t = 1_000_000u64;
        runner.record_activity(t);
        for _ in 0..64 {
            t += 500_000;
            assert!(runner.maybe_warm_hot_path(t).unwrap());
        }
        let vm_before = current_vm_data_kib();
        for _ in 0..5_000 {
            t += 500_000;
            assert!(runner.maybe_warm_hot_path(t).unwrap());
        }
        let vm_mid = current_vm_data_kib();
        for _ in 0..5_000 {
            t += 500_000;
            assert!(runner.maybe_warm_hot_path(t).unwrap());
        }
        let vm_after = current_vm_data_kib();
        if let (Some(before), Some(mid), Some(after)) = (vm_before, vm_mid, vm_after) {
            assert!(
                after <= before + 64 && after <= mid + 4,
                "unexpected VmData growth across 10,000 maybe_warm_hot_path calls: before={before} KiB, mid={mid} KiB, after={after} KiB"
            );
        }
        assert_eq!(runner.warmup_count(), 10_064);
        drop(runner);
        assert!(wire.join().unwrap().is_empty());
    }
}
