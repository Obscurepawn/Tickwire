//! Mandatory kernel TID (`/proc/self/task/<tid>/status` `Cpus_allowed_list`) and startup CPU
//! isolation integration tests for `tickwire-runtime` (`8.2 threads.md`).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use tickwire_account::{
    OrderManagerConfig, OrsAccountDomainConfig, OrsCancelDispatch, OrsNewOrderRequest,
    OrsPlaceOutcome, OrsSymbolSpec,
};
use tickwire_account_gateway::{AccountManager, AccountSpec};
use tickwire_control_sys::{CpuSet, PrivateDirectory, current_thread_affinity, pin_current_thread};
use tickwire_quota::shared::OrsVenueProfile;
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    EncodedOrderFrame, OrderRoutingSystem, OrsCpuConfig, OrsProcess, OrsProcessConfig,
    OrsRuntimeConfig, SessionMessageEffects, SessionRestEndpoint, SessionUrlPoolSpec, UrlRole,
    VenueSession,
};
use tickwire_transport::links::{LinkKind, RequestRef};
use tickwire_transport::rest_querier::DialSecurity;
use tickwire_venue::{Environment, Product, RefusalReason};

struct IdleTestSession;

impl VenueSession for IdleTestSession {
    fn venue_profile(&self) -> OrsVenueProfile {
        OrsVenueProfile::OkxUta
    }

    fn url_pools(&self, _spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String> {
        Ok(vec![SessionUrlPoolSpec {
            url_id: String::from("order_ws"),
            role: UrlRole::ORDER_AND_STREAM,
            kind: LinkKind::Ws,
            product_mask: 0,
            host: String::new(),
            port: 0,
            path: String::from("/ws"),
            security: DialSecurity::Plain,
            initial_peers: Vec::new(),
        }])
    }

    fn rest_endpoint(&self, _spec: &AccountSpec) -> Option<SessionRestEndpoint> {
        Some(SessionRestEndpoint {
            url_id: Some(String::from("rest")),
            host: String::from("localhost"),
            port: 9,
            security: DialSecurity::Plain,
            initial_peers: Vec::new(),
        })
    }

    fn encode_new_order(
        &self,
        _spec: &AccountSpec,
        _req: &OrsNewOrderRequest,
        _placed: &OrsPlaceOutcome,
        _conn_id: u16,
        _scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        Err(RefusalReason::AbortedBeforeSend)
    }

    fn encode_cancel_order(
        &self,
        _spec: &AccountSpec,
        _cancel: &OrsCancelDispatch,
        _conn_id: u16,
        _scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        Err(RefusalReason::AbortedBeforeSend)
    }

    fn on_message(
        &self,
        _account_ref: u8,
        _url_id: &str,
        _role: UrlRole,
        _conn_id: u16,
        _hint: Option<RequestRef>,
        _payload: &[u8],
        _now_mono_ns: u64,
        _effects: &mut SessionMessageEffects,
    ) {
    }
}

fn build_test_account_manager() -> AccountManager {
    AccountManager::from_json_str(
        r#"{
            "enabled_accounts": ["acct1"],
            "acct1": {
                "account_ref": 1,
                "exchange_account_id": 100,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k", "secret": "s" },
                "urls": {
                    "order_ws": {
                        "local_ip/connection_count": [["127.0.0.1", 1]]
                    }
                }
            }
        }"#,
    )
    .unwrap()
}

fn build_test_om_config() -> OrderManagerConfig {
    let mut cfg = OrderManagerConfig::single_account_default(
        OrsAccountDomainConfig {
            account_ref: 1,
            risk_domain_id: [0x11; 16],
            venue: 1,
            environment: Environment::Test,
            exchange_account_id: 100,
            epoch: 1,
        },
        vec![OrsSymbolSpec {
            symbol_id: 0,
            instrument_id: 1001,
            product: Product::Spot,
            wallet_id: 0,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 2,
            quote_scale: 8,
            settlement_asset_id: 2,
            settlement_scale: 8,
            price_tick_atoms: 1_000_000,
            price_scale: 8,
            qty_step_atoms: 1_000,
            qty_scale: 8,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 10_000,
        }],
    );
    cfg.slots_per_strat = 64;
    cfg.max_stp_orders_per_symbol = 64;
    cfg.fill_map_exponent = 6;
    cfg
}

fn read_task_cpus_allowed_list(tid: i32) -> String {
    let path = format!("/proc/self/task/{tid}/status");
    let status =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed reading {path}: {e}"));
    status
        .lines()
        .find(|l| l.starts_with("Cpus_allowed_list:"))
        .unwrap_or_else(|| panic!("missing Cpus_allowed_list in {path}"))
        .split_once(':')
        .unwrap()
        .1
        .trim()
        .to_owned()
}

fn wait_for_tid<F: Fn() -> Option<i32>>(label: &str, f: F) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let Some(tid) = f() {
            return tid;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for kernel TID of {label}");
}

#[test]
fn duplicate_hot_cpus_and_housekeeping_overlap_fail_fast_at_startup() {
    let avail: Vec<u32> = current_thread_affinity().unwrap().as_slice().to_vec();
    let c0 = avail[0];

    // 1. `req_cpu == resp_cpu` fails fast in `OrsCpuConfig::validate` and `OrderRoutingSystem::build`.
    let dup_cfg = OrsCpuConfig {
        req_cpu: Some(c0),
        resp_cpu: Some(c0),
        housekeeping_cpus: None,
        epoll_wait_ms: 0,
    };
    assert!(dup_cfg.validate().is_err());

    let acct_mgr = build_test_account_manager();
    let rt_cfg = OrsRuntimeConfig {
        cpu: dup_cfg,
        ..OrsRuntimeConfig::default()
    };
    let err = OrderRoutingSystem::build(
        rt_cfg,
        &acct_mgr,
        &[1],
        build_test_om_config(),
        RmsEngine::new(1),
        vec![(1, Arc::new(IdleTestSession))],
    )
    .unwrap_err();
    assert!(
        err.contains("CPU isolation validation failed"),
        "unexpected error: {err}"
    );

    // 2. `req_cpu` overlapping `housekeeping_cpus` fails fast.
    let overlap_req = OrsCpuConfig {
        req_cpu: Some(c0),
        resp_cpu: None,
        housekeeping_cpus: Some(CpuSet::single(c0).unwrap()),
        epoll_wait_ms: 0,
    };
    assert!(overlap_req.validate().is_err());
    let err_req = OrderRoutingSystem::build(
        OrsRuntimeConfig {
            cpu: overlap_req,
            ..OrsRuntimeConfig::default()
        },
        &acct_mgr,
        &[1],
        build_test_om_config(),
        RmsEngine::new(1),
        vec![(1, Arc::new(IdleTestSession))],
    )
    .unwrap_err();
    assert!(err_req.contains("CPU isolation validation failed"));

    // 3. `resp_cpu` overlapping `housekeeping_cpus` fails fast.
    let overlap_resp = OrsCpuConfig {
        req_cpu: None,
        resp_cpu: Some(c0),
        housekeeping_cpus: Some(CpuSet::single(c0).unwrap()),
        epoll_wait_ms: 0,
    };
    assert!(overlap_resp.validate().is_err());
    let err_resp = OrderRoutingSystem::build(
        OrsRuntimeConfig {
            cpu: overlap_resp,
            ..OrsRuntimeConfig::default()
        },
        &acct_mgr,
        &[1],
        build_test_om_config(),
        RmsEngine::new(1),
        vec![(1, Arc::new(IdleTestSession))],
    )
    .unwrap_err();
    assert!(err_resp.contains("CPU isolation validation failed"));
}

#[test]
fn unpinned_none_defaults_work_cleanly_on_single_core_hosts() {
    let default_cpu = OrsCpuConfig::default();
    assert_eq!(default_cpu.req_cpu, None);
    assert_eq!(default_cpu.resp_cpu, None);
    assert_eq!(default_cpu.housekeeping_cpus, None);
    assert_eq!(default_cpu.epoll_wait_ms, 0);
    default_cpu.validate().unwrap();

    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "ors-d").unwrap();
    let acct_mgr = build_test_account_manager();
    let ors = OrderRoutingSystem::build(
        OrsRuntimeConfig::default(),
        &acct_mgr,
        &[1],
        build_test_om_config(),
        RmsEngine::new(1),
        vec![(1, Arc::new(IdleTestSession))],
    )
    .unwrap();

    let proc_cfg = OrsProcessConfig {
        block_signals_on_spawn: false,
        control_socket_path: Some(dir.path().join("dop.sock")),
        ..OrsProcessConfig::default()
    };
    let proc_handle = OrsProcess::spawn(ors, proc_cfg).unwrap();
    assert!(proc_handle.worker_init_error().is_none());

    let req_tid = wait_for_tid("tw-ors-req", || proc_handle.req_tid());
    let resp_tid = wait_for_tid("tw-ors-resp", || proc_handle.resp_tid());
    let ctrl_tid = wait_for_tid("tw-ors-ctrl", || proc_handle.control_server_tid());
    let dial_tid = wait_for_tid("tw-dialer", || proc_handle.dialer_tid());
    let trace_tid = wait_for_tid("tw-latency-sink", || proc_handle.latency_tracer_tid());
    assert_ne!(req_tid, resp_tid);
    assert_ne!(req_tid, ctrl_tid);
    assert_ne!(dial_tid, trace_tid);

    proc_handle.shutdown();
    assert!(proc_handle.is_shut_down());
}

#[test]
fn cold_threads_escape_single_core_parent_taskset_into_housekeeping_cpus() {
    let avail: Vec<u32> = current_thread_affinity().unwrap().as_slice().to_vec();
    if avail.len() < 2 {
        return;
    }

    let hot_parent_cpu = avail[0];
    let hk_cpu = avail[1];
    let maybe_resp_cpu = avail.get(2).copied();

    // Spawn a parent helper thread pinned exclusively to `hot_parent_cpu` (`taskset -c <hot_parent_cpu>`
    // simulation), then construct and spawn `OrsProcess` with `housekeeping_cpus = Some({hk_cpu})`.
    // Every cold thread (`ControlServer`, `Dialer`, `AsyncRestQuerier`, `LatencyTracer`) MUST
    // report `hk_cpu` in `/proc/self/task/<tid>/status` (`Cpus_allowed_list`) rather than inheriting
    // `hot_parent_cpu`!
    std::thread::spawn(move || {
        let hot_set = CpuSet::single(hot_parent_cpu).unwrap();
        let hk_set = CpuSet::single(hk_cpu).unwrap();
        pin_current_thread(&hot_set).unwrap();
        assert_eq!(current_thread_affinity().unwrap(), hot_set);

        let cpu_cfg = OrsCpuConfig {
            req_cpu: Some(hot_parent_cpu),
            resp_cpu: maybe_resp_cpu,
            housekeeping_cpus: Some(hk_set.clone()),
            epoll_wait_ms: 0,
        };
        cpu_cfg.validate().unwrap();

        let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "ors-hk").unwrap();
        let acct_mgr = build_test_account_manager();
        let ors = OrderRoutingSystem::build(
            OrsRuntimeConfig {
                cpu: cpu_cfg,
                ..OrsRuntimeConfig::default()
            },
            &acct_mgr,
            &[1],
            build_test_om_config(),
            RmsEngine::new(1),
            vec![(1, Arc::new(IdleTestSession))],
        )
        .unwrap();

        let proc_cfg = OrsProcessConfig {
            block_signals_on_spawn: false,
            control_socket_path: Some(dir.path().join("dop.sock")),
            ..OrsProcessConfig::default()
        };
        let proc_handle = OrsProcess::spawn(ors, proc_cfg).unwrap();
        assert!(proc_handle.worker_init_error().is_none());

        let req_tid = wait_for_tid("tw-ors-req", || proc_handle.req_tid());
        let resp_tid = wait_for_tid("tw-ors-resp", || proc_handle.resp_tid());
        let ctrl_tid = wait_for_tid("tw-ors-ctrl", || proc_handle.control_server_tid());
        let dial_tid = wait_for_tid("tw-dialer", || proc_handle.dialer_tid());
        let trace_tid = wait_for_tid("tw-latency-sink", || proc_handle.latency_tracer_tid());
        let rest_tid = wait_for_tid("tw-rest-query", || {
            proc_handle
                .rest_querier_tids()
                .into_iter()
                .find(|(acct, _)| *acct == 1)
                .map(|(_, tid)| tid)
        });

        let expected_hk = hk_cpu.to_string();
        assert_eq!(
            read_task_cpus_allowed_list(ctrl_tid),
            expected_hk,
            "ControlServer TID {ctrl_tid} did not pin to housekeeping_cpus"
        );
        assert_eq!(
            read_task_cpus_allowed_list(dial_tid),
            expected_hk,
            "Dialer TID {dial_tid} did not pin to housekeeping_cpus"
        );
        assert_eq!(
            read_task_cpus_allowed_list(rest_tid),
            expected_hk,
            "AsyncRestQuerier TID {rest_tid} did not pin to housekeeping_cpus"
        );
        assert_eq!(
            read_task_cpus_allowed_list(trace_tid),
            expected_hk,
            "LatencyTracer TID {trace_tid} did not pin to housekeeping_cpus"
        );

        assert_eq!(
            read_task_cpus_allowed_list(req_tid),
            hot_parent_cpu.to_string(),
            "REQ hot thread TID {req_tid} did not pin to req_cpu"
        );
        if let Some(rcpu) = maybe_resp_cpu {
            assert_eq!(
                read_task_cpus_allowed_list(resp_tid),
                rcpu.to_string(),
                "RESP hot thread TID {resp_tid} did not pin to resp_cpu"
            );
        }

        proc_handle.shutdown();
    })
    .join()
    .unwrap();
}
