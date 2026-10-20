mod common;
use common::*;
fn network() -> NetworkConfig {
    NetworkConfig {
        stream_address: "127.0.0.1:1".parse().unwrap(),
        snapshot_address: "127.0.0.1:2".parse().unwrap(),
        tls: certs().0,
    }
}
fn access() -> SbeAccess {
    SbeAccess::ed25519_api_key("explicitEd25519AccessKey".into()).unwrap()
}
#[test]
fn sbe_only_accepts_exact_product_endpoint_timing_and_stream_speed() {
    let mut c = config::<BinanceSpot>(Source::SpotProduction, SubscribeMode::Explicit);
    assert!(matches!(
        PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new_sbe(
            c.clone(),
            network(),
            Clock(std::time::Instant::now()),
            access()
        ),
        Err(Error::Invalid)
    ));
    c.depth.time_unit = TimeUnit::Microseconds;
    c.speed = DepthSpeed::Ms100;
    assert!(matches!(
        PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new_sbe(
            c.clone(),
            network(),
            Clock(std::time::Instant::now()),
            access()
        ),
        Err(Error::Invalid)
    ));
    c.speed = DepthSpeed::Default;
    c.source = Source::SpotTest;
    assert!(matches!(
        PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new_sbe(
            c.clone(),
            network(),
            Clock(std::time::Instant::now()),
            access()
        ),
        Err(Error::Invalid)
    ));
    c.source = Source::SpotProduction;
    assert!(
        PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new_sbe(
            c,
            network(),
            Clock(std::time::Instant::now()),
            access()
        )
        .is_ok()
    );
    let c = config::<BinanceUsdM>(Source::UsdmProduction, SubscribeMode::RawUrl);
    assert!(matches!(
        PublicDepthClient::<BinanceUsdM, _, 8, 8, 8>::new_sbe(
            c,
            network(),
            Clock(std::time::Instant::now()),
            access()
        ),
        Err(Error::Invalid)
    ));
}
#[test]
fn api_key_rejects_empty_oversized_and_header_injection() {
    for key in [
        "".to_owned(),
        "x".repeat(513),
        "key\r\nAuthorization: x".into(),
        "a b".into(),
    ] {
        assert!(matches!(
            SbeAccess::ed25519_api_key(key),
            Err(Error::Invalid)
        ));
    }
}
