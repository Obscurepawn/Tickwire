#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use std::time::Instant;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, Gate, Status};
use tickwire_account_journal::FileBackend;
use tickwire_app::strategy::{Config, FileStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{InputRecord, MockBlobStore, RecoveryMode, SharedBlobStore, encode_inputs};
use tickwire_runtime::{
    authority::{Dependency, LocalAuthority, RecoveryBundle, RecoveryQueue},
    node::Submission,
};
use tickwire_spot_execution::{Credentials, RestSession, SpotEncoder, SpotVenue};
use tickwire_spot_runner::AccountRunner;
use tickwire_transport::IoBudget;
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment};

#[test]
fn gate_denial_before_first_byte_releases_only_the_unsent_order() {
    let venue = SpotVenue::Bybit;
    let dir = common::Directory::new();
    let (mut snapshot, catalog, mut source) = fixture::fixture();
    snapshot.scope.venue = venue.id();
    source.context.request.venue = venue.id();
    source.context.request.deadline = 10_000_000_000;
    let CommandBody::New(order) = &mut source.body else {
        unreachable!()
    };
    order.client_id = ClientId::new("nopermission").unwrap();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let command = CanonicalCommand::new(source).unwrap();
    let encoder = SpotEncoder::new(
        Credentials::bybit("key", b"secret"),
        Environment::Test,
        2,
        || Ok(1_700_000_000_000),
    )
    .unwrap();
    let mut config = common::config(&baseline);
    config.route = encoder.route();
    let (connection, wire) = common::peer(&config.route.authority, 65536);
    let queue = RecoveryQueue::new(2, 1024 * 1024).unwrap();
    let objects = SharedBlobStore::new(MockBlobStore::default());
    let authority = LocalAuthority::new(
        objects.clone(),
        queue.clone(),
        |_: &CanonicalCommand, _: &tickwire_account::Admission| {
            Ok(Gate {
                now: 61,
                lease_until: 1000,
                writer_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                owner_isolated: true,
                action_permitted: false,
                market_valid: true,
            })
        },
        [2; 16],
    );
    let gateway = AccountGateway::create(
        FileBackend::open(&dir.0).unwrap(),
        config,
        baseline,
        authority,
        encoder,
        connection,
    )
    .unwrap();
    let mut runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects);
    let monotonic = MonoTime::new(ClockDomain::new([4; 16], 1).unwrap(), 60);
    let now = || LocalTime {
        monotonic,
        instant: Instant::now(),
    };

    assert_eq!(
        runner.submit(submission(&command, 9), now()).unwrap(),
        Status::Queued
    );
    assert_eq!(
        runner.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    let mut strategy = FileStrategy::new(&Config::Observe).unwrap();
    let deadline = common::until();
    while !runner.available() {
        assert!(Instant::now() < deadline);
        assert!(
            runner
                .poll(&mut strategy, now(), IoBudget::default())
                .unwrap()
                .is_none()
        );
        std::thread::yield_now();
    }
    assert_eq!(
        runner.submit(submission(&command, 9), now()).unwrap(),
        Status::Aborted
    );
    assert_eq!(
        runner.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    drop(runner);
    assert!(wire.join().unwrap().is_empty());
}

fn submission(command: &CanonicalCommand, id: u8) -> Submission {
    Submission {
        command: command.clone(),
        order_ids: vec![[id; 16]],
        deadline: common::until(),
        recovery: RecoveryBundle {
            command_digest: command.digest(),
            dependencies: (2..=11)
                .map(|kind| Dependency {
                    kind,
                    schema_id: 1,
                    owner: [9; 16],
                    bytes: vec![kind; 4],
                })
                .collect(),
            inputs: encode_inputs(
                [9; 16],
                1,
                [0; 32],
                &[InputRecord {
                    kind: 1,
                    payload: b"test quote",
                }],
            )
            .unwrap(),
            mode: RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [9; 16],
            owner: [9; 16],
        },
    }
}

#[path = "../src/paper_peer.rs"]
mod paper_peer;

use std::{cell::RefCell, collections::VecDeque, rc::Rc};
use tickwire_app::strategy::{AppStrategy, MultiStrategy};
use tickwire_runtime::node::{Health, Strategy, SubmissionResult};
use tickwire_runtime_policy::{ControlPlane, Versions};

#[derive(Default, Debug, PartialEq, Eq)]
struct Receipts {
    local: Vec<Status>,
    denials: Vec<tickwire_account::Error>,
    acknowledgements: usize,
}
struct Member {
    id: u64,
    queue: VecDeque<Submission>,
    receipts: Rc<RefCell<Receipts>>,
}
impl AppStrategy for Member {
    fn failed(&self) -> bool {
        false
    }
}
impl Strategy for Member {
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &tickwire_account::Account,
        _: Health,
        _: LocalTime,
    ) -> Option<Submission> {
        self.queue.pop_front()
    }
    fn on_submission(&mut self, c: &CanonicalCommand, r: &SubmissionResult<'_>) {
        assert_eq!(self.id, c.command().context.strategy.logical_strategy_id);
        match r {
            Ok(status) => self.receipts.borrow_mut().local.push(*status),
            Err(error) => {
                let Some(tickwire_spot_runner::Error::Session(
                    tickwire_spot_execution::SessionError::Gateway(
                        tickwire_account_gateway::Error::Account(refusal),
                    ),
                )) = error.downcast_ref()
                else {
                    panic!("unexpected submission result: {error}");
                };
                self.receipts.borrow_mut().denials.push(*refusal);
            }
        }
    }
    fn on_request_reply(&mut self, c: &CanonicalCommand, r: &tickwire_venue::RequestReply) {
        assert_eq!(self.id, c.command().context.strategy.logical_strategy_id);
        assert!(matches!(
            r,
            tickwire_venue::RequestReply::Acknowledged { .. }
        ));
        self.receipts.borrow_mut().acknowledgements += 1;
    }
}
#[test]
fn shared_cash_and_foreign_order_refusals_reach_only_the_requesting_strategy() {
    let venue = SpotVenue::Bybit;
    let dir = common::Directory::new();
    let (mut snapshot, mut catalog, mut source) = fixture::fixture();
    snapshot.scope.venue = venue.id();
    snapshot.assets[1].free = fixture::units(80);
    let cap = catalog.entries[0].max_gross_quantity;
    catalog.entries[0].strategy_limits = vec![(8, cap), (9, cap)];
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    source.context.request.venue = venue.id();
    source.context.request.deadline = 10_000_000_000;
    let domain = ClockDomain::new([4; 16], 1).unwrap();
    let control = ControlPlane::new(tickwire_runtime_policy::Config {
        scope: baseline.snapshot().scope.clone(),
        product: source.context.request.product,
        executor_id: [2; 16],
        owner_generation: 1,
        writer_epoch: source.context.request.intent_exec_epoch,
        domain,
        versions: Versions {
            config_revision: source.context.request.config_revision,
            risk_policy_version: source.context.strategy.risk_policy_version,
            order_config_version: source.context.strategy.order_config_version,
            catalog_version: source.context.instrument.catalog_version,
            catalog_hash: source.context.instrument.catalog_hash,
        },
        max_strategies: 2,
        max_feeds: 1,
        marks_per_feed: 1,
    })
    .unwrap();
    let mut members: Vec<(u64, Box<dyn AppStrategy>)> = vec![];
    let mut receipts = vec![];
    for id in [8, 9] {
        source.context.request.intent_id = id;
        source.context.request.recovery_object = id;
        source.context.strategy.instance_id = [id as u8; 16];
        source.context.strategy.logical_strategy_id = id;
        source.context.strategy.economic_intent_id = [id as u8; 16];
        let CommandBody::New(order) = &mut source.body else {
            unreachable!()
        };
        order.client_id = ClientId::new(format!("paper{id}")).unwrap();
        let c = CanonicalCommand::new(source.clone()).unwrap();
        let result = Rc::new(RefCell::new(Receipts::default()));
        let mut queue = VecDeque::from([submission(&c, id as u8)]);
        if id == 9 {
            let mut cancel = source.clone();
            cancel.context.request.intent_id = 91;
            cancel.body = CommandBody::Cancel(tickwire_venue::OrderTarget {
                order_id: [8; 16],
                expected_owner_revision: 1,
                original_intent_exec_epoch: source.context.request.intent_exec_epoch,
                client_id: Some(ClientId::new("paper8").unwrap()),
                venue_order_id: None,
            });
            let mut foreign = submission(&CanonicalCommand::new(cancel).unwrap(), 9);
            foreign.order_ids.clear();
            queue.push_back(foreign);
        }
        members.push((
            id,
            Box::new(Member {
                id,
                queue,
                receipts: result.clone(),
            }),
        ));
        receipts.push(result);
    }
    let mut group = MultiStrategy::new(members, control).unwrap();
    let (connection, wire) = paper_peer::start(venue, 1).unwrap();
    let encoder = SpotEncoder::new(
        Credentials::bybit("paper", paper_peer::SECRET),
        Environment::Test,
        2,
        || Ok(1_700_000_000_000),
    )
    .unwrap();
    let mut cfg = common::config(&baseline);
    cfg.route = encoder.route();
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
    let mut runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects);
    let health = Health {
        calibrated: true,
        market_live: true,
        private_receiving: true,
        account_frozen: false,
        sender_halted: false,
    };
    let now = || LocalTime {
        instant: Instant::now(),
        monotonic: MonoTime::new(domain, 60),
    };
    assert!(runner.poll_strategy(&mut group, health, now()).unwrap());
    let deadline = common::until();
    while receipts[0].borrow().acknowledgements == 0 {
        assert!(Instant::now() < deadline);
        runner.poll(&mut group, now(), IoBudget::default()).unwrap();
        std::thread::yield_now();
    }
    assert!(runner.poll_strategy(&mut group, health, now()).unwrap());
    assert!(runner.poll_strategy(&mut group, health, now()).unwrap());
    assert_eq!(
        *receipts[0].borrow(),
        Receipts {
            local: vec![Status::Queued],
            denials: vec![],
            acknowledgements: 1
        }
    );
    assert_eq!(
        *receipts[1].borrow(),
        Receipts {
            local: vec![],
            denials: vec![
                tickwire_account::Error::InsufficientAsset,
                tickwire_account::Error::ManagementDenied
            ],
            acknowledgements: 0
        }
    );
    assert_eq!(runner.position(), 1);
    let quote = runner.account().asset(fixture::key(2)).unwrap();
    assert_eq!(quote.reserved, fixture::units(60));
    assert_eq!(quote.available, fixture::units(20));
    assert!(!runner.account().frozen());
    drop(runner);
    wire.join().unwrap().unwrap();
}
