use super::*;
use std::{
    cell::Cell,
    time::{Duration, Instant},
};
use tickwire_binance_quota::{Action, Class, CommandAction, Cost, Policy, Rule};
use tickwire_clock::{CalibrationConfig, Calibrator, MILLISECOND, MonoTime, SECOND};
use tickwire_quota as quota;
#[path = "../../tickwire-venue/tests/common/mod.rs"]
mod fixture;

struct Clock {
    source: Source,
    core: Calibrator,
    ns: Rc<Cell<u64>>,
}
impl Clock {
    fn local(&self) -> LocalTime {
        LocalTime {
            instant: Instant::now(),
            monotonic: mono(self.ns.get()),
        }
    }
}
impl SessionClock for Clock {
    fn source(&self) -> Source {
        self.source
    }
    fn domain(&self) -> ClockDomain {
        domain()
    }
    fn sample(&mut self) -> tickwire_binance_clock::Result<ClockReading> {
        let local = self.local();
        let signed = self
            .core
            .sign_time(local.monotonic, TimeUnit::Milliseconds)
            .map_err(tickwire_binance_clock::Error::Clock)?;
        Ok(ClockReading {
            local,
            signed: Some(signed),
        })
    }
    fn check_send_sample(
        &mut self,
        s: &SignedTime,
        w: u64,
        b: u64,
    ) -> tickwire_binance_clock::Result<LocalTime> {
        let local = self.local();
        self.core
            .check_send(s, local.monotonic, w, b)
            .map_err(tickwire_binance_clock::Error::Clock)?;
        Ok(local)
    }
}
impl tickwire_binance_private::Clock for Clock {
    fn sample(
        &mut self,
    ) -> tickwire_binance_private::Result<tickwire_binance_private::ClockSample> {
        let reading =
            SessionClock::sample(self).map_err(|_| tickwire_binance_private::Error::Clock)?;
        Ok(tickwire_binance_private::ClockSample {
            instant: reading.local.instant,
            monotonic: reading.local.monotonic,
            signing_time: reading.signed,
        })
    }
    fn check_signed(
        &mut self,
        _: &tickwire_binance_private::AccountScope,
        stamp: &SignedTime,
        now: &tickwire_binance_private::ClockSample,
        window: u64,
    ) -> tickwire_binance_private::Result<()> {
        self.core
            .check_send(stamp, now.monotonic, window, MILLISECOND)
            .map_err(|_| tickwire_binance_private::Error::Clock)
    }
}
struct Budget {
    ns: Rc<Cell<u64>>,
    advance_to: Option<u64>,
}
impl DeliveryBudget for Budget {
    fn remaining_ns(&mut self, _: &CanonicalCommand, _: &SignedTime, _: &LocalTime) -> Result<u64> {
        if let Some(n) = self.advance_to.take() {
            self.ns.set(n);
        }
        Ok(MILLISECOND)
    }
}
fn domain() -> ClockDomain {
    ClockDomain::new([1; 16], 1).unwrap()
}
fn mono(n: u64) -> MonoTime {
    MonoTime::new(domain(), n)
}
fn key() -> quota::WindowKey {
    quota::WindowKey {
        bucket: quota::BucketId {
            namespace: [2; 16],
            scope: quota::ScopeKind::Ip,
            subject: [3; 16],
            pool: 1,
        },
        window: 1,
    }
}
fn setup(
    window: u64,
    advance_to: Option<u64>,
) -> (
    SharedSessionState<Clock, Budget>,
    Rc<Cell<u64>>,
    CanonicalCommand,
) {
    setup_market(window, advance_to, false)
}
fn setup_market(
    window: u64,
    advance_to: Option<u64>,
    futures: bool,
) -> (
    SharedSessionState<Clock, Budget>,
    Rc<Cell<u64>>,
    CanonicalCommand,
) {
    setup_market_with_cap(window, advance_to, futures, 2)
}
fn setup_market_with_cap(
    window: u64,
    advance_to: Option<u64>,
    futures: bool,
    max_signed_commands: usize,
) -> (
    SharedSessionState<Clock, Budget>,
    Rc<Cell<u64>>,
    CanonicalCommand,
) {
    let source = if futures {
        Source::UsdmTest
    } else {
        Source::SpotTest
    };
    let mut c = if futures {
        account_fixture::futures_fixture().2
    } else {
        fixture::command()
    };
    c.context.request.deadline = 2 * SECOND;
    let c = CanonicalCommand::new(c).unwrap();
    let mut core = Calibrator::new(
        CalibrationConfig {
            max_round_trip_ns: SECOND,
            max_width_ns: 10 * MILLISECOND,
            max_age_ns: 10 * SECOND,
            quantization_error_ns: MILLISECOND,
            drift_ppb: 0,
            max_adjustment_ns: 10 * MILLISECOND,
        },
        source,
        domain(),
        1,
    )
    .unwrap();
    let token = core.begin(mono(0)).unwrap();
    core.finish(&token, mono(0), 1000).unwrap();
    let ns = Rc::new(Cell::new(100));
    let mut supervisor =
        quota::Coordinator::open(quota::MockQuotaStore::default(), 1, mono(0)).unwrap();
    supervisor
        .add_window(
            quota::WindowConfig {
                key: key(),
                revision: 1,
                starts_ns: 0,
                ends_ns: 20 * SECOND,
                limit: 10,
                recovery_reserve: 0,
            },
            mono(0),
        )
        .unwrap();
    let mut local = supervisor
        .register_consumer(
            quota::ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            19 * SECOND,
            mono(0),
        )
        .unwrap();
    let grant = supervisor
        .issue(
            1,
            &local,
            Class::Normal,
            &[Cost {
                window: key(),
                units: 10,
            }],
            18 * SECOND,
            mono(0),
        )
        .unwrap();
    local.install(grant, mono(0)).unwrap();
    let endpoint = Endpoint::new(
        local,
        [5; 16],
        domain(),
        Policy {
            generation: 1,
            starts_ns: 0,
            ends_ns: 15 * SECOND,
            rules: [Transport::Rest, Transport::WebSocket]
                .into_iter()
                .map(|transport| Rule {
                    source,
                    action: Action::Command {
                        transport,
                        action: CommandAction::New,
                    },
                    class: Class::Normal,
                    api_weight: 0,
                    websocket_messages: 0,
                    costs: vec![Cost {
                        window: key(),
                        units: 1,
                    }],
                })
                .chain(
                    [
                        tickwire_binance_quota::CommandSessionAction::Connect,
                        tickwire_binance_quota::CommandSessionAction::Upgrade,
                        tickwire_binance_quota::CommandSessionAction::Ping,
                        tickwire_binance_quota::CommandSessionAction::Pong,
                        tickwire_binance_quota::CommandSessionAction::Close,
                    ]
                    .into_iter()
                    .map(|kind| Rule {
                        source,
                        action: Action::CommandSession(kind),
                        class: Class::Normal,
                        api_weight: 0,
                        websocket_messages: 0,
                        costs: vec![Cost {
                            window: key(),
                            units: 1,
                        }],
                    }),
                )
                .chain(
                    [
                        tickwire_binance_quota::ReadAction::Connect,
                        tickwire_binance_quota::ReadAction::OrderQuery,
                    ]
                    .into_iter()
                    .map(|kind| Rule {
                        source,
                        action: Action::Read(kind),
                        class: Class::Normal,
                        api_weight: 0,
                        websocket_messages: 0,
                        costs: vec![Cost {
                            window: key(),
                            units: 1,
                        }],
                    }),
                )
                .collect(),
        },
    )
    .unwrap();
    let state = SharedSessionState::new(
        Clock {
            source,
            core,
            ns: ns.clone(),
        },
        endpoint,
        CommandBinding {
            owner_id: [6; 16],
            risk_domain_id: [1; 16],
            account: 2,
            product: c.command().context.request.product,
            environment: tickwire_venue::Environment::Test,
            domain: domain(),
        },
        Budget {
            ns: ns.clone(),
            advance_to,
        },
        Config {
            recv_window_micros: window,
            max_http_bytes: 4096,
            max_signed_commands,
        },
    )
    .unwrap();
    (state, ns, c)
}
fn gate() -> Gate {
    Gate {
        now: 100,
        lease_until: 2 * SECOND,
        writer_epoch: 4,
        strategy_generation: 5,
        config_revision: 6,
        owner_isolated: true,
        action_permitted: true,
        market_valid: true,
    }
}
fn encode(state: &SharedSessionState<Clock, Budget>, command: &CanonicalCommand) {
    let key = tickwire_binance::auth::SigningKey::hmac(b"unit-test-secret").unwrap();
    state
        .encoder(Credentials {
            account: 2,
            environment: tickwire_venue::Environment::Test,
            market: request::Market::Spot,
            api_key: "unit-key",
            signing_key: &key,
        })
        .encode(command)
        .unwrap();
}
#[test]
fn original_stamp_and_bytes_survive_repeat_encoding_and_gate_retry() {
    let (state, ns, c) = setup(5_000_000, None);
    encode(&state, &c);
    let original = state.signed_stamp(c.digest()).unwrap().unwrap();
    let bytes = state.0.borrow().signed[&c.digest()].bytes.clone();
    ns.set(SECOND);
    encode(&state, &c);
    assert_eq!(state.signed_stamp(c.digest()).unwrap(), Some(original));
    assert_eq!(state.0.borrow().signed[&c.digest()].bytes, bytes);
    let p = state.prepare_gate(&c).unwrap();
    assert_eq!(state.finish_gate(&c, gate(), p).unwrap().now, SECOND);
    let p = state.prepare_gate(&c).unwrap();
    state.finish_gate(&c, gate(), p).unwrap();
    assert_eq!(
        state
            .with_resources(|_, q| q.remaining(key(), Class::Normal))
            .unwrap(),
        9
    );
}
#[test]
fn original_expired_signature_is_not_replaced_with_current_calibration() {
    let (state, ns, c) = setup(100_000, None);
    encode(&state, &c);
    ns.set(200 * MILLISECOND);
    let p = state.prepare_gate(&c).unwrap();
    assert!(matches!(
        state.finish_gate(&c, gate(), p),
        Err(Error::Clock(_))
    ));
    assert_eq!(
        state
            .with_resources(|_, q| q.remaining(key(), Class::Normal))
            .unwrap(),
        10
    );
}
#[test]
fn budget_callback_expiry_is_checked_on_the_same_final_sample() {
    let (state, _, c) = setup(5_000_000, Some(3 * SECOND));
    encode(&state, &c);
    let p = state.prepare_gate(&c).unwrap();
    assert!(matches!(
        state.finish_gate(&c, gate(), p),
        Err(Error::Denied)
    ));
    assert_eq!(
        state
            .with_resources(|_, q| q.remaining(key(), Class::Normal))
            .unwrap(),
        10
    );
}
#[test]
fn unencoded_or_changed_commands_never_gain_a_send_gate() {
    let (state, _, c) = setup(5_000_000, None);
    assert!(matches!(
        state.prepare_gate(&c),
        Err(Error::MissingSignature)
    ));
    encode(&state, &c);
    let mut changed = c.command().clone();
    changed.context.request.config_revision += 1;
    let changed = CanonicalCommand::new(changed).unwrap();
    assert!(matches!(
        state.prepare_gate(&changed),
        Err(Error::MissingSignature)
    ));
}
#[test]
fn policy_and_lease_are_rechecked_after_all_callbacks() {
    let (state, ns, c) = setup(5_000_000, None);
    encode(&state, &c);
    let p = state.prepare_gate(&c).unwrap();
    ns.set(SECOND);
    let mut g = gate();
    g.lease_until = SECOND;
    assert!(matches!(state.finish_gate(&c, g, p), Err(Error::Denied)));
    let p = state.prepare_gate(&c).unwrap();
    let mut g = gate();
    g.config_revision += 1;
    assert!(matches!(state.finish_gate(&c, g, p), Err(Error::Denied)));
}

#[test]
fn windowed_eviction_reclaims_consumed_and_expired_entries_across_monotonic_windows() {
    let (state, ns, base_cmd) = setup(5_000_000, None);
    assert_eq!(
        state.retention_window_ns(),
        Some(DEFAULT_RETENTION_WINDOW_NS)
    );
    state.set_retention_window_ns(Some(500 * MILLISECOND));

    let make_cmd = |intent_id: u64, created_ns: u64, deadline_ns: u64| {
        let mut raw = base_cmd.command().clone();
        raw.context.request.intent_id = intent_id;
        raw.context.strategy.created_mono_ns = created_ns;
        raw.context.request.deadline = deadline_ns;
        if let tickwire_venue::CommandBody::New(ref mut order) = raw.body {
            order.client_id = tickwire_venue::ClientId::new(format!("win-{intent_id}")).unwrap();
        }
        CanonicalCommand::new(raw).unwrap()
    };

    // Inside a single window (`max_signed_commands = 2`), a 3rd unconsumed command fails with Capacity.
    ns.set(100 * MILLISECOND);
    let c1 = make_cmd(1, 100 * MILLISECOND, 10 * SECOND);
    let c2 = make_cmd(2, 100 * MILLISECOND, 10 * SECOND);
    let c3 = make_cmd(3, 100 * MILLISECOND, 10 * SECOND);
    encode(&state, &c1);
    encode(&state, &c2);
    let key_hmac = tickwire_binance::auth::SigningKey::hmac(b"unit-test-secret").unwrap();
    let creds = Credentials {
        account: 2,
        environment: tickwire_venue::Environment::Test,
        market: request::Market::Spot,
        api_key: "unit-key",
        signing_key: &key_hmac,
    };
    assert!(matches!(
        state.encoder(creds).encode(&c3),
        Err(tickwire_gateway::Error::Capacity)
    ));

    // Consume c1 and c2 at t = 100ms, then execute 8 more sequential requests (> 4 * max_signed_commands = 8, total 10)
    // across advancing 600ms windows.
    for cmd in [&c1, &c2] {
        let p = state.prepare_gate(cmd).unwrap();
        let mut g = gate();
        g.now = 100 * MILLISECOND;
        g.lease_until = 15 * SECOND;
        state.finish_gate(cmd, g, p).unwrap();
    }
    for step in 0..8u64 {
        let t = (700 + step * 600) * MILLISECOND;
        ns.set(t);
        let cmd = make_cmd(10 + step, t, t + 2 * SECOND);
        encode(&state, &cmd);
        let p = state.prepare_gate(&cmd).unwrap();
        let mut g = gate();
        g.now = t;
        g.lease_until = 15 * SECOND;
        state.finish_gate(&cmd, g, p).unwrap();
        assert!(state.0.borrow().signed.len() <= 2);
    }

    // Also verify WebSocket `plans` + `signed` evict past deadline + retention_window_ns across > 4 * max_signed_commands.
    let (ws_state, ws_ns, ws_base) = setup(5_000_000, None);
    ws_state.set_retention_window_ns(Some(200 * MILLISECOND));
    let (snapshot, catalog, _) = account_fixture::fixture();
    let base = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut input = tickwire_binance_account::AccountIngress::create(
        tickwire_journal::store::InMemoryStore::new(),
        tickwire_binance_account::Config {
            physical: tickwire_journal::Config::new([35; 16], [1; 16]),
            writer_epoch: 4,
            baseline: base,
            account_stream: [36; 16],
            max_records: 64,
            max_registrations: 32,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
    )
    .unwrap();
    let session = input.reserve_ws_session([41; 16], [42; 32]).unwrap();
    for i in 0..10u64 {
        let t = (100 + i * 500) * MILLISECOND;
        ws_ns.set(t);
        let mut raw = ws_base.command().clone();
        raw.context.request.intent_id = 100 + i;
        raw.context.strategy.created_mono_ns = t;
        raw.context.request.deadline = t + 200 * MILLISECOND;
        if let tickwire_venue::CommandBody::New(ref mut order) = raw.body {
            order.client_id = tickwire_venue::ClientId::new(format!("ws-win-{i}")).unwrap();
        }
        let cmd = CanonicalCommand::new(raw).unwrap();
        let plan = input.plan_ws_request(&session, cmd.clone()).unwrap();
        ws_state.prepare_websocket(plan).unwrap();
        encode(&ws_state, &cmd);
        assert!(ws_state.0.borrow().plans.len() <= 2);
        assert!(ws_state.0.borrow().signed.len() <= 2);
    }
}

use gateway_fixture::fixture as account_fixture;
#[test]
fn websocket_encoding_keeps_request_id_original_stamp_and_masked_frame() {
    let (state, ns, command) = setup(5_000_000, None);
    let (snapshot, catalog, _) = account_fixture::fixture();
    let base = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let dir = std::env::temp_dir().join(format!("tickwire-ws-encode-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut input = tickwire_binance_account::AccountIngress::create(
        tickwire_journal::store::FileStore::open(&dir).unwrap(),
        tickwire_binance_account::Config {
            physical: tickwire_journal::Config::new([31; 16], [1; 16]),
            writer_epoch: 4,
            baseline: base,
            account_stream: [32; 16],
            max_records: 32,
            max_registrations: 8,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
    )
    .unwrap();
    let session = input.reserve_ws_session([41; 16], [42; 32]).unwrap();
    let plan = input.plan_ws_request(&session, command.clone()).unwrap();
    let id = plan.request_id().as_str().to_owned();
    let result = state.prepare_websocket(plan);
    result.unwrap();
    encode(&state, &command);
    let bytes = state.0.borrow().signed[&command.digest()].bytes.clone();
    assert_eq!(bytes[0], 0x81);
    assert_ne!(bytes[1] & 0x80, 0);
    let start = if bytes[1] & 0x7f == 126 { 8 } else { 6 };
    let mask = &bytes[start - 4..start];
    let plain: Vec<u8> = bytes[start..]
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ mask[i % 4])
        .collect();
    let value: serde_json::Value = serde_json::from_slice(&plain).unwrap();
    assert_eq!(value["id"], id);
    assert_eq!(value["method"], "order.place");
    let original = state.signed_stamp(command.digest()).unwrap();
    ns.set(SECOND);
    encode(&state, &command);
    assert_eq!(state.0.borrow().signed[&command.digest()].bytes, bytes);
    assert_eq!(state.signed_stamp(command.digest()).unwrap(), original);
    let p = state.prepare_gate(&command).unwrap();
    assert!(
        state.finish_gate(&command, gate(), p).is_err(),
        "without durable binding cannot send"
    );
    drop(input);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway_fixture;
#[test]
fn websocket_gate_requires_actual_binding_frame_and_current_owner() {
    for tampered in [false, true] {
        let (state, _, command) = setup(5_000_000, None);
        let dir = gateway_fixture::Directory::new();
        let (snapshot, catalog, _) = account_fixture::fixture();
        let base = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut input = tickwire_binance_account::AccountIngress::create(
            tickwire_journal::store::FileStore::open(dir.0.join("input")).unwrap(),
            tickwire_binance_account::Config {
                physical: tickwire_journal::Config::new([31; 16], [1; 16]),
                writer_epoch: 4,
                baseline: base.clone(),
                account_stream: [2; 16],
                max_records: 32,
                max_registrations: 8,
                max_raw_bytes: 32768,
                max_retained_bytes: 1 << 20,
                income_baseline_authority: None,
                income_association_authority: None,
            },
        )
        .unwrap();
        let session = input.reserve_ws_session([41; 16], [42; 32]).unwrap();
        input.register_command(command.clone(), &[[7; 16]]).unwrap();
        let plan = input.plan_ws_request(&session, command.clone()).unwrap();
        state.prepare_websocket(plan.clone()).unwrap();
        let key = tickwire_binance::auth::SigningKey::hmac(b"unit-test-secret").unwrap();
        let encoder = state.encoder(Credentials {
            account: 2,
            environment: tickwire_venue::Environment::Test,
            market: request::Market::Spot,
            api_key: "unit-key",
            signing_key: &key,
        });
        let mut cfg = gateway_fixture::config(&base);
        cfg.route.authority = "ws-api.testnet.binance.vision".into();
        let (connection, peer) = gateway_fixture::peer(&cfg.route.authority, 16384);
        let mut gateway = tickwire_account_gateway::AccountGateway::create(
            tickwire_account_journal::FileBackend::open(dir.0.join("account")).unwrap(),
            cfg,
            base,
            state.authority(gateway_fixture::MockAuthority::default()),
            encoder,
            connection,
        )
        .unwrap();
        assert_eq!(
            gateway
                .submit(command.clone(), 60, gateway_fixture::until())
                .unwrap(),
            tickwire_account_gateway::Status::AwaitingCommit
        );
        let committed = gateway
            .committed(gateway_fixture::intent())
            .unwrap()
            .clone();
        assert!(
            !state
                .binding_ready(committed.admission(), committed.receipt())
                .unwrap()
        );
        let mut wire = state.websocket_wire(command.digest()).unwrap();
        if tampered {
            wire.frame_hash[0] ^= 1;
        }
        let bound = input.persist_ws_binding(plan, &gateway, wire).unwrap();
        let installed = state.install_websocket_binding(bound);
        if tampered {
            assert_eq!(installed, Err(Error::Identity));
            assert!(
                !state
                    .binding_ready(committed.admission(), committed.receipt())
                    .unwrap()
            );
        } else {
            installed.unwrap();
            assert!(
                state
                    .binding_ready(committed.admission(), committed.receipt())
                    .unwrap()
            );
            let p = state.prepare_gate(&command).unwrap();
            state.finish_gate(&command, gate(), p).unwrap();
            assert_eq!(
                state
                    .with_resources(|_, q| q.remaining(key_window(), Class::Normal))
                    .unwrap(),
                9
            );
            input.invalidate_ws_session();
            assert_eq!(
                state.binding_ready(committed.admission(), committed.receipt()),
                Err(Error::Identity)
            );
            let p = state.prepare_gate(&command).unwrap();
            assert!(state.finish_gate(&command, gate(), p).is_err());
        }
        drop(gateway);
        assert!(peer.join().unwrap().is_empty());
    }
}
fn key_window() -> quota::WindowKey {
    key()
}

#[test]
fn websocket_owner_real_tls_upgrade_binding_response_and_pong() {
    run_websocket_owner(false, ReplyMode::Normal);
}
#[test]
fn usdm_websocket_owner_and_rest_reconciliation_use_distinct_tls_endpoints() {
    run_websocket_owner(true, ReplyMode::Normal);
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplyMode {
    Normal,
    WrongId,
    Duplicate,
    RateLimited,
    PeerClose,
}
#[test]
fn websocket_unknown_and_duplicate_ids_preserve_raw_and_never_reapply() {
    run_websocket_owner(false, ReplyMode::WrongId);
    run_websocket_owner(false, ReplyMode::Duplicate);
}
#[test]
fn websocket_rate_limit_response_freezes_shared_quota_after_durable_raw() {
    run_websocket_owner(false, ReplyMode::RateLimited);
}
#[test]
fn websocket_peer_close_uses_only_the_control_slot() {
    run_websocket_owner(false, ReplyMode::PeerClose);
}
fn run_websocket_owner(futures: bool, reply: ReplyMode) {
    use tickwire_binance_account::{
        AccountIngress,
        rest::RestOwner,
        ws::{ControlAction, WsConfig},
    };
    let (state, _, command) = setup_market(5_000_000, None, futures);
    let (snapshot, catalog, _) = if futures {
        account_fixture::futures_fixture()
    } else {
        account_fixture::fixture()
    };
    let base = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let dir = gateway_fixture::Directory::new();
    let mut input = AccountIngress::create(
        tickwire_journal::store::FileStore::open(dir.0.join("input")).unwrap(),
        tickwire_binance_account::Config {
            physical: tickwire_journal::Config::new([31; 16], [1; 16]),
            writer_epoch: 4,
            baseline: base.clone(),
            account_stream: [2; 16],
            max_records: 32,
            max_registrations: 8,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
    )
    .unwrap();
    let session = input.reserve_ws_session([41; 16], [42; 32]).unwrap();
    state
        .websocket_control(&session, ControlAction::Connect, 1, SECOND)
        .unwrap();
    let key = tickwire_binance::auth::SigningKey::hmac(b"unit-test-secret").unwrap();
    let encoder = state.encoder(Credentials {
        account: 2,
        environment: tickwire_venue::Environment::Test,
        market: if futures {
            request::Market::Usdm
        } else {
            request::Market::Spot
        },
        api_key: "unit-key",
        signing_key: &key,
    });
    let mut config = gateway_fixture::config(&base);
    config.route.authority = if futures {
        "testnet.binancefuture.com"
    } else {
        "ws-api.testnet.binance.vision"
    }
    .into();
    let (connection, peer) = ws_peer(&config.route.authority, reply);
    let authority = gateway_fixture::MockAuthority::default();
    let recover_authority = authority.clone();
    let recovered_baseline = base.clone();
    let gateway = tickwire_account_gateway::AccountGateway::create(
        tickwire_account_journal::FileBackend::open(dir.0.join("account")).unwrap(),
        config,
        base,
        state.authority(authority),
        encoder,
        connection,
    )
    .unwrap();
    let mut owner = RestOwner::from_parts_websocket(
        input,
        gateway,
        session,
        Box::new(state.clone()),
        WsConfig {
            lifetime_ns: 10 * SECOND,
            control_timeout_ns: SECOND,
            max_messages_per_poll: 8,
        },
        Instant::now(),
        100,
    )
    .unwrap();
    let budget = tickwire_transport::IoBudget {
        max_bytes: 65536,
        max_syscalls: 64,
    };
    let end = Instant::now() + Duration::from_secs(5);
    while !owner.websocket_ready() && Instant::now() < end {
        owner.poll(Instant::now(), 100, budget).unwrap();
        std::thread::yield_now();
    }
    assert!(owner.websocket_ready());
    assert_eq!(
        owner.submit(command.clone(), &[[7; 16]], 60, end).unwrap(),
        tickwire_account_gateway::Status::AwaitingCommit
    );
    let mut outcome = None;
    while Instant::now() < end {
        let polled = owner.poll(Instant::now(), 100, budget);
        if reply == ReplyMode::Normal {
            assert!(polled.is_ok(), "{polled:?}");
        }
        if let Ok(Some(result)) = polled {
            outcome = Some(result);
        }
        if (reply == ReplyMode::RateLimited && outcome.is_some())
            || (reply != ReplyMode::Normal && owner.gateway().halted())
        {
            break;
        }
        if reply == ReplyMode::Normal
            && outcome.is_some()
            && !owner.gateway().websocket_control_pending()
            && state
                .with_resources(|_, q| q.remaining(key_window(), Class::Normal))
                .unwrap()
                == 6
        {
            break;
        }
        std::thread::yield_now();
    }
    if reply == ReplyMode::PeerClose {
        assert!(owner.gateway().halted());
        assert!(owner.account().frozen());
        assert_eq!(owner.gateway().journal_position(), 2);
        assert_eq!(owner.ingress().stored_ws_responses().count(), 1);
        drop(owner);
        peer.join().unwrap();
        return;
    }
    if reply != ReplyMode::Normal {
        if reply == ReplyMode::RateLimited {
            assert!(state.with_resources(|_, q| q.is_frozen()).unwrap());
        }
        assert!(owner.gateway().halted());
        assert!(owner.account().frozen());
        let records: Vec<_> = owner.ingress().stored_ws_responses().collect();
        assert_eq!(
            records.len(),
            if reply == ReplyMode::Duplicate { 2 } else { 1 }
        );
        let last = records.last().unwrap();
        assert!(!last.raw.is_empty());
        assert!(last.events.is_empty());
        assert_eq!(
            last.decision,
            if reply == ReplyMode::Duplicate {
                tickwire_binance_account::ws::ResponseDecision::Duplicate
            } else if reply == ReplyMode::RateLimited {
                tickwire_binance_account::ws::ResponseDecision::Matched
            } else {
                tickwire_binance_account::ws::ResponseDecision::Unmatched
            }
        );
        assert_eq!(
            owner.gateway().journal_position(),
            if reply == ReplyMode::Duplicate { 2 } else { 1 }
        );
        assert!(
            owner
                .gateway()
                .output_progress(gateway_fixture::intent())
                .unwrap()
                .accepted_bytes
                > 0
        );
        drop(owner);
        peer.join().unwrap();
        return;
    }
    let outcome = outcome.expect("response");
    assert_eq!(
        outcome.disposition,
        tickwire_binance_account::rest::RestDisposition::Observed
    );
    assert_eq!(outcome.applied_events, 1);
    assert_eq!(owner.gateway().journal_position(), 2);
    assert_eq!(owner.ingress().stored_ws_responses().count(), 1);
    assert_eq!(
        owner
            .ingress()
            .stored_ws_responses()
            .next()
            .unwrap()
            .decision,
        tickwire_binance_account::ws::ResponseDecision::Matched
    );
    let bound = owner
        .gateway()
        .committed(gateway_fixture::intent())
        .unwrap();
    assert!(
        state
            .binding_ready(bound.admission(), bound.receipt())
            .unwrap()
    );
    // A distinct authenticated REST connection performs status reconciliation;
    // the application order remains on the existing WebSocket connection.
    let scope = tickwire_binance_private::AccountScope {
        risk_domain_id: [1; 16],
        venue: 1,
        environment: tickwire_venue::Environment::Test,
        account: 2,
        market: if futures {
            request::Market::Usdm
        } else {
            request::Market::Spot
        },
        credential_generation: 1,
    };
    let read_binding = tickwire_binance_quota::ReadBinding {
        owner_id: [61; 16],
        scope: scope.clone(),
        domain: domain(),
    };
    let (read_network, read_peer) = rest_query_peer(futures);
    let mut reader = tickwire_binance_account::reconciliation::orders::ReadOnlyOrderReader::new(
        tickwire_binance_account::reconciliation::ReconcileConfig {
            scope: scope.clone(),
            address: read_network.address,
            tls: read_network.tls,
            limits: read_network.limits,
            first_connection_epoch: 1,
            request_timeout_ns: SECOND,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32768,
            max_headers: 32,
        },
    )
    .unwrap();
    let credentials = Credentials {
        account: 2,
        environment: tickwire_venue::Environment::Test,
        market: scope.market,
        api_key: "unit-key",
        signing_key: &key,
    };
    let query = tickwire_binance_account::reconciliation::orders::OrderQuery {
        intent: gateway_fixture::intent(),
        order_id: [7; 16],
        private_connection_epoch: 1,
        gap_generation: 1,
    };
    state
        .with_resources(|clock, quota| {
            reader.start(
                &mut owner,
                query,
                &credentials,
                clock,
                &mut quota.read(read_binding.clone()),
            )
        })
        .unwrap()
        .unwrap();
    let mut queried = None;
    while Instant::now() < end {
        queried = state
            .with_resources(|clock, quota| {
                reader.poll(
                    &mut owner,
                    clock,
                    &mut quota.read(read_binding.clone()),
                    budget,
                )
            })
            .unwrap()
            .unwrap();
        if queried.is_some() {
            break;
        }
        std::thread::yield_now();
    }
    let queried = queried.expect("REST status response");
    assert!(queried.accepted, "{queried:?}");
    assert_eq!(owner.gateway().journal_position(), 3);
    assert!(owner.account().frozen());
    assert_eq!(
        state
            .with_resources(|_, q| q.remaining(key_window(), Class::Normal))
            .unwrap(),
        4
    );
    let rest_request = read_peer.join().unwrap();
    assert!(rest_request.starts_with(if futures {
        "GET /fapi/v1/order?"
    } else {
        "GET /api/v3/order?"
    }));
    assert!(rest_request.contains("orderId=51"));
    assert!(!rest_request.contains("Upgrade:"));
    drop(owner);
    let request = peer.join().unwrap();
    assert_eq!(request["method"], "order.place");
    assert_eq!(request["id"].as_str().unwrap().len(), 57);
    // Opening the actual two files never recreates a live sending capability,
    // regardless of whether the prior request had a binding and a response.
    let (recovered_state, _, _) = setup_market(5_000_000, None, futures);
    let mut recovered_config = gateway_fixture::config(&recovered_baseline);
    recovered_config.route.authority = if futures {
        "testnet.binancefuture.com"
    } else {
        "ws-api.testnet.binance.vision"
    }
    .into();
    recovered_config.writer_epoch = 5;
    let recovered_gateway = tickwire_account_gateway::AccountGateway::recover(
        tickwire_account_journal::FileBackend::open(dir.0.join("account")).unwrap(),
        recovered_config,
        recovered_state.authority(recover_authority),
        recovered_state.encoder(credentials),
        None,
    )
    .unwrap();
    let recovered_input = AccountIngress::recover(
        tickwire_journal::store::FileStore::open(dir.0.join("input")).unwrap(),
        tickwire_binance_account::Config {
            physical: tickwire_journal::Config::new([31; 16], [1; 16]),
            writer_epoch: 5,
            baseline: recovered_baseline,
            account_stream: [2; 16],
            max_records: 32,
            max_registrations: 8,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
        None,
    )
    .unwrap();
    let mut recovered = RestOwner::from_parts_websocket_recovered(
        recovered_input,
        recovered_gateway,
        Box::new(recovered_state.clone()),
        tickwire_binance_account::ws::WebSocketSettings {
            execution_instance: [43; 16],
            credential_ref_digest: [42; 32],
            session: WsConfig {
                lifetime_ns: 10 * SECOND,
                control_timeout_ns: SECOND,
                max_messages_per_poll: 8,
            },
        },
    )
    .unwrap();
    recovered.replay_pending(32).unwrap();
    assert_eq!(recovered.gateway().journal_position(), 3);
    assert!(recovered.account().frozen());
    assert!(!recovered.submission_ready());
    assert_eq!(recovered.websocket_next_epoch().unwrap(), Some(2));
    assert!(
        recovered
            .gateway()
            .committed(gateway_fixture::intent())
            .is_none()
    );
    assert_eq!(
        recovered.submit(command, &[[7; 16]], 100, end).unwrap(),
        tickwire_account_gateway::Status::Unknown
    );
    assert!(
        recovered
            .gateway()
            .output_progress(gateway_fixture::intent())
            .is_none()
    );
    assert!(
        recovered
            .poll(Instant::now(), 100, budget)
            .unwrap()
            .is_none()
    );
}
fn ws_peer(
    host: &str,
    reply: ReplyMode,
) -> (
    tickwire_account_gateway::ConnectionConfig,
    std::thread::JoinHandle<serde_json::Value>,
) {
    use base64::Engine;
    use sha1::Digest;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::Arc,
    };
    use tickwire_transport::rustls;
    let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
                ),
            )
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut b = [0];
            stream.read_exact(&mut b).unwrap();
            header.push(b[0]);
            assert!(header.len() < 16384);
        }
        let header = std::str::from_utf8(&header).unwrap();
        let nonce = header
            .lines()
            .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap();
        let accept = base64::engine::general_purpose::STANDARD.encode(sha1::Sha1::digest(format!(
            "{nonce}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut h = [0; 2];
        stream.read_exact(&mut h).unwrap();
        assert_eq!(h[0], 0x81);
        assert_ne!(h[1] & 0x80, 0);
        let length = if h[1] & 0x7f == 126 {
            let mut n = [0; 2];
            stream.read_exact(&mut n).unwrap();
            u16::from_be_bytes(n) as usize
        } else {
            (h[1] & 0x7f) as usize
        };
        let mut mask = [0; 4];
        stream.read_exact(&mut mask).unwrap();
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).unwrap();
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
        let request: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let response=serde_json::to_vec(&serde_json::json!({"id":if reply==ReplyMode::WrongId {serde_json::json!("wrong-id")}else{request["id"].clone()},"status":200,"result":{"symbol":request["params"]["symbol"],"orderId":51,"clientOrderId":request["params"]["newClientOrderId"],"price":request["params"]["price"],"origQty":request["params"]["quantity"],"executedQty":"0","status":"NEW","side":request["params"]["side"],"positionSide":"BOTH"}})).unwrap();
        let response = if reply == ReplyMode::RateLimited {
            serde_json::to_vec(&serde_json::json!({"id":request["id"],"status":429,"error":{"code":-1003,"msg":"Too many requests"}})).unwrap()
        } else {
            response
        };
        if reply == ReplyMode::Normal {
            stream.write_all(&[0x89, 1, 42]).unwrap();
        }
        stream.write_all(&[0x81, 126]).unwrap();
        stream
            .write_all(&(response.len() as u16).to_be_bytes())
            .unwrap();
        stream.write_all(&response).unwrap();
        if reply == ReplyMode::Duplicate {
            stream.write_all(&[0x81, 126]).unwrap();
            stream
                .write_all(&(response.len() as u16).to_be_bytes())
                .unwrap();
            stream.write_all(&response).unwrap();
        }
        if reply == ReplyMode::PeerClose {
            stream.write_all(&[0x88, 2, 3, 232]).unwrap();
        }
        stream.flush().unwrap();
        if reply == ReplyMode::PeerClose {
            let mut close = [0; 8];
            stream.read_exact(&mut close).unwrap();
            assert_eq!(&close[..2], &[0x88, 0x82]);
            assert_eq!(close[6] ^ close[2], 3);
            assert_eq!(close[7] ^ close[3], 232);
        }
        if reply == ReplyMode::Normal {
            let mut pong = [0; 7];
            stream.read_exact(&mut pong).unwrap();
            assert_eq!(&pong[..2], &[0x8a, 0x81]);
            assert_eq!(pong[6] ^ pong[2], 42);
        }
        let mut end = [0; 1];
        let _ = stream.read(&mut end);
        request
    });
    (
        tickwire_account_gateway::ConnectionConfig {
            address,
            tls: client,
            limits: tickwire_transport::Limits {
                max_send_bytes: 16384,
                max_receive_bytes: 65536,
                tls_buffer_bytes: 65536,
                max_accept_bytes: 16384,
            },
            connect_deadline: Instant::now() + Duration::from_secs(5),
        },
        thread,
    )
}

fn rest_query_peer(
    futures: bool,
) -> (
    tickwire_account_gateway::ConnectionConfig,
    std::thread::JoinHandle<String>,
) {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::Arc,
    };
    use tickwire_transport::rustls;
    let host = if futures {
        "demo-fapi.binance.com"
    } else {
        "testnet.binance.vision"
    };
    let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
                ),
            )
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut b = [0];
            stream.read_exact(&mut b).unwrap();
            header.push(b[0]);
            assert!(header.len() < 16384);
        }
        let response = serde_json::to_vec(&serde_json::json!({"symbol":"BTCUSDT","orderId":51,"clientOrderId":"test-1","price":if futures {"100"} else {"60000"},"origQty":if futures {"1"} else {"0.001"},"executedQty":"0","status":"NEW","side":"BUY","positionSide":"BOTH"})).unwrap();
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).unwrap();
        stream.write_all(&response).unwrap();
        stream.flush().unwrap();
        String::from_utf8(header).unwrap()
    });
    (
        tickwire_account_gateway::ConnectionConfig {
            address,
            tls: client,
            limits: tickwire_transport::Limits {
                max_send_bytes: 16384,
                max_receive_bytes: 65536,
                tls_buffer_bytes: 65536,
                max_accept_bytes: 16384,
            },
            connect_deadline: Instant::now() + Duration::from_secs(5),
        },
        thread,
    )
}

#[test]
fn concurrent_websocket_plans_and_multi_ip_quota_ban_propagation() {
    use std::net::{IpAddr, Ipv4Addr};
    use tickwire_account_gateway::{AccountGateway, Authority, CommitState, Status};
    use tickwire_binance_account::{AccountIngress, ws::SessionState};
    use tickwire_binance_quota::MultiIpCommandQuota;
    use tickwire_quota::rate_limiter::IpKey;

    let (state, _, base_command) = setup_market_with_cap(5_000_000, None, false, 16);
    let ip_a = IpKey::from_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let ip_b = IpKey::from_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));

    let wrong_binding = CommandBinding {
        owner_id: [6; 16],
        risk_domain_id: [1; 16],
        account: 999,
        product: base_command.command().context.request.product,
        environment: tickwire_venue::Environment::Test,
        domain: domain(),
    };
    let wrong_mq = MultiIpCommandQuota::new(wrong_binding, &[ip_a, ip_b]).unwrap();
    assert_eq!(state.set_multi_ip_quota(wrong_mq), Err(Error::Identity));

    let valid_binding = CommandBinding {
        owner_id: [6; 16],
        risk_domain_id: [1; 16],
        account: 2,
        product: base_command.command().context.request.product,
        environment: tickwire_venue::Environment::Test,
        domain: domain(),
    };
    let mq = MultiIpCommandQuota::new(valid_binding, &[ip_a, ip_b]).unwrap();
    let mut state = state.with_multi_ip_quota(mq).unwrap();
    assert!(state.has_multi_ip_quota().unwrap());

    let (mut snapshot, catalog, _) = account_fixture::fixture();
    snapshot.assets[1].free = account_fixture::units(10_000);
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let dir = gateway_fixture::Directory::new();
    let mut input = AccountIngress::create(
        tickwire_journal::store::FileStore::open(dir.0.join("input")).unwrap(),
        tickwire_binance_account::Config {
            physical: tickwire_journal::Config::new([31; 16], [1; 16]),
            writer_epoch: 4,
            baseline: baseline.clone(),
            account_stream: [2; 16],
            max_records: 64,
            max_registrations: 32,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
    )
    .unwrap();
    let session = input.reserve_ws_session([41; 16], [42; 32]).unwrap();
    let key = tickwire_binance::auth::SigningKey::hmac(b"unit-test-secret").unwrap();
    let mut encoder = state.encoder(Credentials {
        account: 2,
        environment: tickwire_venue::Environment::Test,
        market: request::Market::Spot,
        api_key: "unit-key",
        signing_key: &key,
    });
    let mut gw_cfg = gateway_fixture::config(&baseline);
    gw_cfg.route.authority = "ws-api.testnet.binance.vision".into();
    gw_cfg.max_intents = 32;
    let mock_auth = gateway_fixture::MockAuthority::default();
    let mut gate_auth = state.authority(mock_auth.clone());
    let (conn, peer) = gateway_fixture::peer(&gw_cfg.route.authority, 16384);
    let mut gateway = AccountGateway::create(
        tickwire_account_journal::FileBackend::open(dir.0.join("account")).unwrap(),
        gw_cfg,
        baseline,
        state.authority(mock_auth),
        state.encoder(Credentials {
            account: 2,
            environment: tickwire_venue::Environment::Test,
            market: request::Market::Spot,
            api_key: "unit-key",
            signing_key: &key,
        }),
        conn,
    )
    .unwrap()
    .with_connection_pool(tickwire_account_gateway::pool::RouterConfig::default())
    .unwrap();
    let mut slot_peers = vec![peer];
    for _ in 0..4 {
        let (slot_cfg, slot_peer) = gateway_fixture::peer("ws-api.testnet.binance.vision", 16384);
        slot_peers.push(slot_peer);
        let security = tickwire_transport::Security::Tls {
            server_name: tickwire_transport::rustls::pki_types::ServerName::try_from(
                "ws-api.testnet.binance.vision",
            )
            .unwrap(),
            config: slot_cfg.tls,
        };
        let slot_conn = tickwire_transport::Connection::connect(
            slot_cfg.address,
            security,
            slot_cfg.limits,
            slot_cfg.connect_deadline,
        )
        .unwrap();
        gateway
            .add_pool_slot(
                tickwire_account_gateway::pool::OrderSlotSpec::with_connection(
                    tickwire_account_gateway::pool::SendSlot::Exclusive,
                    slot_conn,
                ),
                slot_cfg.limits,
            )
            .unwrap();
    }

    // Prepare, encode, admit, and bind 4 concurrent WS commands on the same SessionIdentity.
    let mut commands = Vec::new();
    for idx in 0..4u64 {
        let mut raw = base_command.command().clone();
        let intent_id = 10 + idx;
        raw.context.request.intent_id = intent_id;
        raw.context.request.recovery_object = intent_id;
        raw.context.strategy.strategy_intent_seq = idx + 1;
        let mut triggers = raw.context.triggers.as_slice().to_vec();
        for t in &mut triggers {
            t.sequence = intent_id;
        }
        raw.context.triggers = tickwire_venue::BoundedList::new(triggers).unwrap();
        let tickwire_venue::CommandBody::New(mut spec) = raw.body else {
            unreachable!()
        };
        spec.client_id = tickwire_venue::ClientId::new(format!("mux-ws-{idx}")).unwrap();
        raw.body = tickwire_venue::CommandBody::New(spec);
        let cmd = CanonicalCommand::new(raw).unwrap();
        let order_id = [(20 + idx) as u8; 16];
        input.register_command(cmd.clone(), &[order_id]).unwrap();
        let plan = input.plan_ws_request(&session, cmd.clone()).unwrap();
        state.prepare_websocket(plan.clone()).unwrap();
        use tickwire_account_gateway::Encoder as _;
        let _ = encoder.encode(&cmd).unwrap();
        assert_eq!(
            gateway
                .submit_on_slot(idx as u16, cmd.clone(), 60, gateway_fixture::until())
                .unwrap()
                .0,
            Status::AwaitingCommit
        );
        let ikey = tickwire_account::IntentKey {
            epoch: cmd.command().context.request.intent_exec_epoch,
            intent_id: cmd.command().context.request.intent_id,
        };
        let committed = gateway.committed(ikey).unwrap();
        // Before installing this command's binding, commit_state is Pending
        // even while earlier commands (0..idx) are already Ready!
        assert_eq!(
            gate_auth
                .commit_state(committed.admission(), committed.receipt())
                .unwrap(),
            CommitState::Pending
        );
        let wire = state.websocket_wire(cmd.digest()).unwrap();
        let bound = input.persist_ws_binding(plan, &gateway, wire).unwrap();
        state.install_websocket_binding(bound).unwrap();
        assert_eq!(
            gate_auth
                .commit_state(committed.admission(), committed.receipt())
                .unwrap(),
            CommitState::Ready
        );
        commands.push(cmd);
    }

    // All 4 concurrent commands remain Ready simultaneously on the same SessionIdentity.
    for cmd in &commands {
        let ikey = tickwire_account::IntentKey {
            epoch: cmd.command().context.request.intent_exec_epoch,
            intent_id: cmd.command().context.request.intent_id,
        };
        let committed = gateway.committed(ikey).unwrap();
        assert_eq!(
            gate_auth
                .commit_state(committed.admission(), committed.receipt())
                .unwrap(),
            CommitState::Ready
        );
    }

    // Feedback 429 freezes legacy Endpoint without banning MultiIpCommandQuota;
    // Feedback 418 bans MultiIpCommandQuota account-wide.
    SessionState::feedback(&mut state, 429).unwrap();
    assert!(
        !state
            .multi_ip_quota(|mq| mq.unwrap().is_account_banned())
            .unwrap()
    );
    SessionState::feedback(&mut state, 418).unwrap();
    assert!(
        state
            .multi_ip_quota(|mq| mq.unwrap().is_account_banned())
            .unwrap()
    );

    drop(gateway);
    for p in slot_peers {
        let _ = p.join().unwrap();
    }
}
