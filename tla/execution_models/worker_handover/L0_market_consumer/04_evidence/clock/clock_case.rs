
// Test-only overlay in market_transition/tests.rs. The existing Test creates
// the approved operation, content-addressed object store and authenticated sessions.
#[test]
fn worker_receipt_created_after_host_sample_is_rejected_on_the_real_report_lane() {
    for (read_now, should_accept) in [(fixture::NOW, false), (fixture::NOW + 1, true)] {
        let mut t = Test::new();
        let host_sample = fixture::NOW;
        let worker_stamp = host_sample + 1;
        let approval = t.state.approval.as_ref().unwrap();
        let worker_time = tickwire_clock::MonoTime::new(approval.previous.domain().unwrap(), worker_stamp);
        let receipt = Receipt::armed(approval, t.state.process.pid, worker_time).unwrap();
        let bytes = serde_json::to_vec(&receipt).unwrap();
        let obj = Object::of(&bytes);
        t.store.put_sync(StoreKey::Object(obj.digest), &bytes).unwrap();
        let request = t.state.arm_request.as_ref().unwrap();
        let frame = protocol::encode_reply(&t.worker, request, obj).unwrap();
        let endpoint = t._directory.path().join("report.sock");
        let listener = tickwire_control_sys::ControlListener::bind(&endpoint).unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut writer = tickwire_control_sys::Connection::connect(&endpoint, until).unwrap();
        let reader = listener.accept(until).unwrap();
        let mut proof = crate::readiness::market_clock_fixture(t.host.peer().clone());
        let mut lane = AuthenticatedChild { connection: reader, session: t.host, process: t.state.process };
        writer.send(&frame, &[], until).unwrap();
        let outcome = proof.poll_with(&mut lane, read_now, |envelope, body| {
            t.state.observe(envelope, body, read_now)
        });
        println!("host_sample={host_sample} worker_stamp={worker_stamp} dispatcher_time={read_now} outcome={outcome:?} installed={}", t.state.receipt().is_some());
        assert_eq!(outcome.is_ok(), should_accept);
        assert_eq!(t.state.receipt().is_some(), should_accept);
        assert!(lane.connection.try_recv().unwrap().is_none());
    }
}
