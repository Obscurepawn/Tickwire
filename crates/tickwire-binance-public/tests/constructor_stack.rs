mod common;
use common::*;
use std::{process::Command, time::Instant};
type Spot = PublicDepthClient<BinanceSpot, Clock, 5000, 5000, 64>;
type Usdm = PublicDepthClient<BinanceUsdM, Clock, 5000, 5000, 64>;
#[test]
fn production_depth_owners_construct_on_default_worker_stack() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "constructor_stack_child", "--nocapture"])
        .env("TICKWIRE_CONSTRUCTOR_STACK_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn constructor_stack_child() {
    if std::env::var_os("TICKWIRE_CONSTRUCTOR_STACK_CHILD").is_none() {
        return;
    }
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            for sbe in [false, true] {
                let mut c = config::<BinanceSpot>(Source::SpotProduction, SubscribeMode::RawUrl);
                c.depth.snapshot_limit = 5000;
                c.limits.total_buffer_bytes = 64 << 20;
                let n = NetworkConfig {
                    stream_address: "127.0.0.1:1".parse().unwrap(),
                    snapshot_address: "127.0.0.1:2".parse().unwrap(),
                    tls: certs().0,
                };
                let owner = if sbe {
                    c.depth.time_unit = TimeUnit::Microseconds;
                    Spot::new_sbe(
                        c,
                        n,
                        Clock(Instant::now()),
                        SbeAccess::ed25519_api_key("testkey".into()).unwrap(),
                    )
                    .unwrap()
                } else {
                    Spot::new(c, n, Clock(Instant::now())).unwrap()
                };
                let owner = Box::new(owner);
                assert_eq!(owner.status().phase, Phase::Idle);
            }
            let mut c = config::<BinanceUsdM>(Source::UsdmProduction, SubscribeMode::RawUrl);
            c.depth.snapshot_limit = 1000;
            c.limits.total_buffer_bytes = 64 << 20;
            let n = NetworkConfig {
                stream_address: "127.0.0.1:1".parse().unwrap(),
                snapshot_address: "127.0.0.1:2".parse().unwrap(),
                tls: certs().0,
            };
            let owner = Box::new(Usdm::new(c, n, Clock(Instant::now())).unwrap());
            assert_eq!(owner.status().phase, Phase::Idle);
        })
        .unwrap()
        .join()
        .unwrap();
}
