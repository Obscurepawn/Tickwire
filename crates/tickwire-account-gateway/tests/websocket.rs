mod common;
use common::*;
use std::time::Instant;
use tickwire_account_gateway::*;
use tickwire_transport::IoBudget;

#[test]
fn websocket_upgrade_is_control_only_and_does_not_create_an_admission() {
    let (baseline, command) = baseline(false);
    let mut cfg = config(&baseline);
    cfg.route.authority = "ws-api.venue.invalid".into();
    let (connection, peer) = peer(&cfg.route.authority, 16384);
    let mut g = AccountGateway::create(
        cfg,
        baseline,
        MockAuthority::default(),
        TestEncoder,
        connection,
    )
    .unwrap();
    let started = g.begin_websocket(until());
    if started.is_ok() {
        assert!(matches!(g.submit(command, 60, until()), Err(Error::Busy)));
        let deadline = until();
        while g.websocket_control_pending() {
            assert!(Instant::now() < deadline);
            g.poll_websocket(Instant::now(), IoBudget::default(), || true)
                .unwrap();
            std::thread::yield_now();
        }
    }
    assert_eq!(g.entry_count(), 0);
    drop(g);
    let bytes = peer.join().unwrap();
    started.unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.starts_with("GET /ws-api/v3 HTTP/1.1\r\n"));
    assert!(text.contains("Upgrade: websocket\r\n"));
    assert!(!text.contains("order.place"));
}
