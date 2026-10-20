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
    core: Calibrator,
    ns: Rc<Cell<u64>>,
    instant: Instant,
}
impl Clock {
    fn local(&self) -> LocalTime {
        LocalTime {
            instant: self.instant + Duration::from_nanos(self.ns.get()),
            monotonic: mono(self.ns.get()),
        }
    }
}
impl SessionClock for Clock {
    fn source(&self) -> Source {
        Source::SpotTest
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
    let mut c = fixture::command();
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
        Source::SpotTest,
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
            rules: vec![Rule {
                source: Source::SpotTest,
                action: Action::Command {
                    transport: Transport::Rest,
                    action: CommandAction::New,
                },
                class: Class::Normal,
                api_weight: 0,
                websocket_messages: 0,
                costs: vec![Cost {
                    window: key(),
                    units: 1,
                }],
            }],
        },
    )
    .unwrap();
    let state = SharedSessionState::new(
        Clock {
            core,
            ns: ns.clone(),
            instant: Instant::now(),
        },
        endpoint,
        CommandBinding {
            owner_id: [6; 16],
            risk_domain_id: [1; 16],
            account: 2,
            product: tickwire_venue::Product::Spot,
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
            max_signed_commands: 2,
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
