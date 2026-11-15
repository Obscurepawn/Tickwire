//! Linux kernel TID `/proc/self/task/<tid>/status` verification that `"tw-dialer"`,
//! `"tw-rest-query"`, and `"tw-latency-sink"` escape an inherited single-core `taskset` mask onto
//! `housekeeping_cpus` before creating reactors or performing I/O.
#![forbid(unsafe_code)]

use std::{
    fs,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};
use tickwire_control_sys::{CpuSet, current_thread_affinity, pin_current_thread};
use tickwire_transport::{
    LatencyTracer, LatencyTracerConfig, MarkSite, ThreadTag,
    dialer::{DialBackoff, DialSecurity, DialSpec, DialTransport, Dialer, DialerConfig},
    mono_now_ns,
    rest_querier::{AsyncRestQuerier, BuiltRequest, RestQuerierConfig, RestStatus, RestTask},
};

fn parse_proc_cpus_list(spec: &str) -> CpuSet {
    spec.trim()
        .parse::<CpuSet>()
        .expect("valid Cpus_allowed_list")
}

fn read_task_cpus_allowed(tid: i32) -> CpuSet {
    let path = format!("/proc/self/task/{tid}/status");
    let content = fs::read_to_string(&path).expect("read /proc/self/task/<tid>/status");
    let line = content
        .lines()
        .find(|l| l.starts_with("Cpus_allowed_list:"))
        .expect("Cpus_allowed_list line present");
    let list_str = line
        .split_once(':')
        .map(|(_, rhs)| rhs.trim())
        .expect("Cpus_allowed_list value");
    parse_proc_cpus_list(list_str)
}

fn read_task_comm(tid: i32) -> String {
    let path = format!("/proc/self/task/{tid}/comm");
    fs::read_to_string(&path)
        .expect("read /proc/self/task/<tid>/comm")
        .trim()
        .to_owned()
}

#[test]
fn dialer_rest_querier_and_latency_sink_escape_hot_cpu_onto_housekeeping_cpus() {
    let host_allowed = current_thread_affinity().expect("query host allowed CPUs");
    let cpus: Vec<u32> = host_allowed.as_slice().to_vec();
    assert!(!cpus.is_empty(), "at least one CPU must be allowed");

    let hot_cpu = cpus[0];
    let hot_set = CpuSet::single(hot_cpu).unwrap();

    let hk_set = if cpus.len() >= 2 {
        let set = CpuSet::from_cpus(cpus[1..cpus.len().min(3)].iter().copied()).unwrap();
        assert!(hot_set.is_disjoint(&set));
        set
    } else {
        CpuSet::single(hot_cpu).unwrap()
    };

    // Launch a dedicated parent helper thread pinned to a single hot CPU (simulating `taskset -c <hot>`).
    let expected_hk = hk_set.clone();
    let helper = thread::spawn(move || {
        pin_current_thread(&hot_set).expect("pin parent helper to single hot CPU");
        let parent_tid = tickwire_control_sys::current_tid().expect("parent tid");
        assert_eq!(read_task_cpus_allowed(parent_tid), hot_set);

        // Stand up loopback TCP listeners for Dialer and AsyncRestQuerier verification.
        let dial_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let dial_addr = dial_listener.local_addr().unwrap();
        let dial_srv = thread::spawn(move || {
            let (_s, _) = dial_listener.accept().unwrap();
        });

        let rest_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let rest_addr = rest_listener.local_addr().unwrap();
        let rest_srv = thread::spawn(move || {
            let (mut sock, _) = rest_listener.accept().unwrap();
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut req_buf = [0u8; 512];
            let _ = sock.read(&mut req_buf).unwrap();
            let resp =
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}";
            sock.write_all(resp).unwrap();
        });

        // 1. Spawn Dialer with housekeeping_cpus.
        let dialer = Dialer::with_config(DialerConfig {
            housekeeping_cpus: Some(expected_hk.clone()),
            clock: std::sync::Arc::new(mono_now_ns),
        })
        .expect("spawn pinned Dialer");

        // 2. Spawn AsyncRestQuerier with housekeeping_cpus.
        let querier = AsyncRestQuerier::new(
            RestQuerierConfig {
                host: "127.0.0.1".to_owned(),
                port: rest_addr.port(),
                initial_peers: vec![rest_addr],
                local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                housekeeping_cpus: Some(expected_hk.clone()),
                capacity: 16,
                ..RestQuerierConfig::default()
            },
            |_task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
                Ok(Some(BuiltRequest {
                    method: "GET".to_owned(),
                    path: "/api/v3/openOrders".to_owned(),
                    headers: Vec::new(),
                    body: Vec::new(),
                }))
            },
        )
        .expect("spawn pinned AsyncRestQuerier");

        // 3. Spawn LatencyTracer ("tw-latency-sink") with housekeeping_cpus.
        let tracer = std::sync::Arc::new(
            LatencyTracer::new(LatencyTracerConfig {
                housekeeping_cpus: Some(expected_hk.clone()),
                ..LatencyTracerConfig::default()
            })
            .expect("spawn pinned LatencyTracer"),
        );

        let dialer_tid = dialer.worker_tid().expect("tw-dialer kernel TID");
        let rest_tid = querier.worker_tid().expect("tw-rest-query kernel TID");
        let sink_tid = tracer.sink_tid().expect("tw-latency-sink kernel TID");

        assert_eq!(read_task_comm(dialer_tid), "tw-dialer");
        assert_eq!(read_task_comm(rest_tid), "tw-rest-query");
        assert_eq!(read_task_comm(sink_tid), "tw-latency-sink");

        let dialer_cpus = read_task_cpus_allowed(dialer_tid);
        let rest_cpus = read_task_cpus_allowed(rest_tid);
        let sink_cpus = read_task_cpus_allowed(sink_tid);

        assert_eq!(
            dialer_cpus, expected_hk,
            "tw-dialer must bind housekeeping_cpus"
        );
        assert_eq!(
            rest_cpus, expected_hk,
            "tw-rest-query must bind housekeeping_cpus"
        );
        assert_eq!(
            sink_cpus, expected_hk,
            "tw-latency-sink must bind housekeeping_cpus"
        );

        if cpus.len() >= 2 {
            assert!(!dialer_cpus.contains(hot_cpu));
            assert!(!rest_cpus.contains(hot_cpu));
            assert!(!sink_cpus.contains(hot_cpu));
        }

        // Verify all 3 pinned workers execute live work cleanly on housekeeping_cpus.
        let baton = dialer
            .add_slot(
                1,
                DialSpec {
                    host: "127.0.0.1".to_owned(),
                    port: dial_addr.port(),
                    initial_peers: vec![dial_addr],
                    local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                    nic: None,
                    security: DialSecurity::Plain,
                    transport: DialTransport::PlainOrTls,
                    backoff: DialBackoff::new(10, 50),
                    ..DialSpec::default()
                },
            )
            .unwrap();
        assert!(baton.want(0));

        assert!(querier.enqueue(RestTask::cold_snapshot(1, 42)));

        let mut req_trace = tracer.thread_tracer(ThreadTag::REQ);
        req_trace.begin_iteration();
        req_trace.mark(MarkSite::SHM_POLL);
        req_trace.mark(MarkSite::SOCKET_SEND);
        req_trace.end_iteration();
        assert!(tracer.flush(Duration::from_secs(2)));

        let deadline = Instant::now() + Duration::from_secs(3);
        while baton.take_ready().is_none() {
            assert!(Instant::now() < deadline, "Dialer timed out");
            thread::sleep(Duration::from_millis(2));
        }
        let reply = loop {
            if let Some(r) = querier.dequeue() {
                break r;
            }
            assert!(Instant::now() < deadline, "AsyncRestQuerier timed out");
            thread::sleep(Duration::from_millis(2));
        };
        assert_eq!(reply.status, RestStatus::Ok);
        assert_eq!(reply.http_code, 200);
        assert_eq!(reply.body, b"{\"ok\":true}");

        // Re-verify parent thread stayed pinned to hot_cpu while children stayed on housekeeping_cpus.
        assert_eq!(read_task_cpus_allowed(parent_tid), hot_set);
        assert_eq!(read_task_cpus_allowed(dialer_tid), expected_hk);
        assert_eq!(read_task_cpus_allowed(rest_tid), expected_hk);
        assert_eq!(read_task_cpus_allowed(sink_tid), expected_hk);

        dial_srv.join().unwrap();
        rest_srv.join().unwrap();
    });

    helper.join().expect("affinity escape helper succeeded");
}
