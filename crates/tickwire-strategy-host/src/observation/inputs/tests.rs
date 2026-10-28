use super::*;
use tickwire_release::replay::*;
use tickwire_strategy_sdk::{
    Strategy,
    replay::{Engine, ReplayableStrategy, application::Application},
};
#[path = "../../../../tickwire-account/tests/common/mod.rs"]
mod account;
mod market;

struct Fixture {
    current: configuration::Current,
    objects: Vec<Vec<u8>>,
    ready: HandoverReadyBody,
    report: ObservedReportInput,
}
impl Fixture {
    fn new() -> Self {
        let (bank, replica, original) = market::market_fixture();
        let (snapshot, catalog, _) = account::fixture();
        let baseline =
            tickwire_account_codec::CanonicalBaseline::new(snapshot.clone(), catalog.clone())
                .unwrap();
        let recovered = tickwire_account::Account::from_snapshot(snapshot, catalog).unwrap();
        let app = Application::new(
            tickwire_strategy_sdk::ObserveOnly,
            100,
            vec![(bank.clone(), replica, 4)],
        )
        .unwrap();
        let source = Source {
            logical_strategy: 8,
            instance: [2; 16],
            generation: 1,
            binary_digest: [3; 32],
            state_schema: 1,
            input_stream: [4; 16],
            clock: ClockDomain {
                boot_id: bank.host_boot_id,
                generation: 1,
            },
        };
        let runtime = RuntimeState {
            now_ns: 1_500_000_000,
            config: VersionedBytes {
                revision: 1,
                bytes: b"config".to_vec(),
            },
            models: vec![],
            markets: app.market_cursors().unwrap(),
            accounts: vec![],
            external: vec![],
            timers: vec![],
            rng: Rng::Disabled,
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        };
        let cp = Checkpoint::new(
            source.clone(),
            0,
            [0; 32],
            app.checkpoint().unwrap(),
            runtime,
        )
        .unwrap();
        let anchor = verify_anchor(
            &cp.bytes().unwrap(),
            &source,
            Object::of(&cp.runtime.config.bytes),
            &[],
        )
        .unwrap();
        let mut engine = Engine::new(&anchor, app).unwrap();
        engine
            .apply_live(
                Input::AccountCallback {
                    stream: tickwire_strategy_sdk::replay::account::AccountReplica::stream(
                        &baseline,
                    ),
                    position: 0,
                    canonical: baseline.bytes().to_vec(),
                },
                1_500_000_001,
            )
            .unwrap();
        let checkpoint = engine.checkpoint().unwrap();
        let mut account_input = b"TWAI0001".to_vec();
        account_input.push(0);
        account_input.extend(1_500_000_001u64.to_le_bytes());
        account_input.extend(0u64.to_le_bytes());
        account_input.extend((baseline.bytes().len() as u32).to_le_bytes());
        account_input.extend(baseline.bytes());
        let rows = [
            (2, account_input),
            (5, [b"TWM2A001".as_slice(), &cp.bytes().unwrap()].concat()),
            (
                5,
                [
                    b"TWM2R001".as_slice(),
                    &canonical(&engine.recorded()[0]).unwrap(),
                ]
                .concat(),
            ),
            (
                5,
                [b"TWM2C001".as_slice(), &checkpoint.bytes().unwrap()].concat(),
            ),
        ];
        let inputs = tickwire_recovery::encode_inputs(
            source.input_stream,
            1,
            [0; 32],
            &rows
                .iter()
                .map(|(kind, payload)| tickwire_recovery::InputRecord {
                    kind: *kind,
                    payload,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .bytes;
        let state = tickwire_strategy_sdk::ObserveOnly.checkpoint().unwrap();
        let mut raw = b"TWHS0001".to_vec();
        raw.extend(source.instance);
        for n in [
            source.generation,
            source.logical_strategy,
            0,
            recovered.ledger_sequence(),
        ] {
            raw.extend(n.to_le_bytes());
        }
        raw.extend(recovered.strategy_orders_digest(8));
        raw.extend((state.len() as u64).to_le_bytes());
        raw.extend(state);
        let account_seal = tickwire_account_codec::InputSeal::new(baseline.digest()).unwrap();
        let seal = tickwire_recovery::HandoverSeal {
            version: 1,
            owner: source.instance,
            instance: source.instance,
            generation: 1,
            logical_strategy: 8,
            report_position: 0,
            ledger_sequence: recovered.ledger_sequence(),
            orders_digest: recovered.strategy_orders_digest(8),
            checkpoint_digest: Object::of(&raw).digest,
            checkpoint_bytes: raw.len() as u64,
            account_input_digest: account_seal.digest(),
            account_input_bytes: account_seal.object_bytes().len() as u64,
            input_digest: Object::of(&inputs).digest,
            input_bytes: inputs.len() as u64,
            dependencies: (2..=11)
                .map(|k| {
                    (
                        k,
                        if k == 11 {
                            Object::of(&original).digest
                        } else {
                            [k as u8; 32]
                        },
                    )
                })
                .collect(),
            preflight_receipt_digest: None,
            replay_installation_digest: None,
            replay_restore_digest: None,
            replay_rollback_digest: None,
            rebuild_installation_digest: None,
            rebuild_rollback_digest: None,
        };
        let ready = HandoverReadyBody {
            instance_id: source.instance,
            logical_strategy_id: 8,
            strategy_generation: 1,
            ready_sequence: 1,
            exec_report_sequence: 0,
            report_ring_sequence: 1,
            ledger_sequence: seal.ledger_sequence,
            orders_digest: seal.orders_digest,
            checkpoint_digest: seal.checkpoint_digest,
            checkpoint_bytes: seal.checkpoint_bytes,
            input_seal_digest: Object::of(&seal.bytes().unwrap()).digest,
            input_seal_bytes: seal.bytes().unwrap().len() as u64,
        };
        let mut header = bank;
        header.resource_kind = 3;
        header.required_features |= tickwire_strategy_sdk::reports::FEATURE;
        header.consumer_instance = source.instance;
        header.record_bytes = 512;
        header.first_seq = 1;
        header.risk_scope = 1;
        let report = ObservedReportInput {
            subscription: tickwire_shm::SubscriptionKey {
                bank: tickwire_shm::BankKey {
                    resource: u128::from_le_bytes(header.resource_id),
                    channel_epoch: header.channel_epoch,
                    channel: u128::from_le_bytes(header.channel_id),
                    ring_epoch: header.ring_epoch,
                    feed_generation: header.feed_generation,
                },
                instance: u128::from_le_bytes(source.instance),
                generation: 1,
                cursor: 7,
            },
            header,
            identity: ReportIdentity {
                logical_strategy_id: 8,
                instance_id: source.instance,
                strategy_generation: 1,
                risk_domain: baseline.snapshot().scope.risk_domain_id,
            },
            sequence: 1,
        };
        Self {
            current: configuration::Current { checkpoint, seal },
            objects: vec![raw, inputs, account_seal.object_bytes().to_vec(), original],
            ready,
            report,
        }
    }
    fn read(self) -> Result<Option<ObservedInputs>> {
        extract(&self.current, &self.objects, &self.ready, self.report)
    }
}
#[test]
fn real_current_account_and_saved_join_are_exposed() {
    let f = Fixture::new();
    let c = Object::of(&f.current.checkpoint.bytes().unwrap());
    let observed = f.read().unwrap().expect("current inputs");
    assert_eq!(observed.checkpoint(), c);
    assert_eq!(observed.accounts().len(), 1);
    assert_eq!(
        observed.accounts()[0].baseline().snapshot().scope.account,
        2
    );
    assert_eq!(observed.markets()[0].instruments(), &[(1001, 5)]);
    assert_eq!(
        observed.markets()[0].join().unwrap().catalog_hash(),
        [0x22; 32]
    );
    assert_eq!(
        observed.markets()[0]
            .join()
            .unwrap()
            .instrument_set_version(),
        5
    );
    assert_eq!(observed.report().sequence(), 1);
}

#[test]
fn report_identity_scope_and_actual_cursor_must_match_current_account() {
    for case in 0..9 {
        let mut f = Fixture::new();
        match case {
            0 => f.report.identity.risk_domain = [55; 16],
            1 => f.report.identity.instance_id = [55; 16],
            2 => f.report.identity.logical_strategy_id += 1,
            3 => f.report.identity.strategy_generation += 1,
            4 => f.report.header.consumer_instance = [55; 16],
            5 => f.report.subscription.bank.resource += 1,
            6 => f.report.subscription.bank.feed_generation += 1,
            7 => f.report.sequence += 1,
            _ => f.current.seal.account_input_digest = [55; 32],
        }
        assert!(f.read().is_err(), "report mutation {case}");
    }
}

fn replace_original(f: &mut Fixture, bytes: Vec<u8>) {
    f.current.seal.dependencies.last_mut().unwrap().1 = Object::of(&bytes).digest;
    *f.objects.last_mut().unwrap() = bytes;
}
fn edit_join(
    f: &mut Fixture,
    edit: impl FnOnce(&mut tickwire_market_source::protocol::JoinBootstrap),
) {
    let original = f.objects.last().unwrap();
    let at = 20 + u32::from_le_bytes(original[16..20].try_into().unwrap()) as usize;
    let raw = &original[at..];
    let n = u32::from_le_bytes(raw[24..28].try_into().unwrap()) as usize;
    let mut join: tickwire_market_source::protocol::JoinBootstrap =
        serde_json::from_slice(&raw[28..28 + n]).unwrap();
    edit(&mut join);
    let encoded = serde_json::to_vec(&join).unwrap();
    let mut out = original[..at + 24].to_vec();
    out.extend((encoded.len() as u32).to_le_bytes());
    out.extend(encoded);
    out.extend(&raw[28 + n..]);
    replace_original(f, out);
}

#[test]
fn different_bank_cannot_reuse_old_join_and_unsupported_original_has_no_join() {
    let mut f = Fixture::new();
    edit_join(&mut f, |join| {
        let mut bank = ResourceHeader::decode(&join.bank_header).unwrap();
        bank.resource_id = [66; 16];
        join.bank_header = tickwire_market_source::protocol::wire_bytes(&bank).unwrap();
    });
    assert!(f.read().unwrap().unwrap().markets()[0].join().is_none());
    let mut f = Fixture::new();
    replace_original(&mut f, b"unrecognized recovery payload".to_vec());
    let facts = f.read().unwrap().unwrap();
    assert_eq!(facts.markets()[0].instruments(), &[(1001, 5)]);
    assert!(facts.markets()[0].join().is_none());
}

#[test]
fn named_join_cannot_substitute_catalog_members_snapshot_or_producer() {
    for case in 0..6 {
        let mut f = Fixture::new();
        edit_join(&mut f, |join| match case {
            0 => join.catalog_hash = [66; 32],
            1 => join.instrument_set_version += 1,
            2 => join.instruments[0].id += 1,
            3 => join.instruments[0].version += 1,
            4 => join.instruments[0].required_states = 128,
            _ => {
                let mut header = ResourceHeader::decode(&join.snapshot_header).unwrap();
                header.producer_instance = [66; 16];
                join.snapshot_header =
                    tickwire_market_source::protocol::wire_bytes(&header).unwrap();
            }
        });
        assert!(f.read().is_err(), "JOIN mutation {case}");
    }
    let mut f = Fixture::new();
    let mut bytes = f.objects.last().unwrap().clone();
    *bytes.last_mut().unwrap() ^= 1;
    replace_original(&mut f, bytes);
    assert!(f.read().is_err());
}

#[test]
fn valid_saved_required_states_are_reported_exactly_for_the_caller_to_approve() {
    let mut f = Fixture::new();
    edit_join(&mut f, |join| join.instruments[0].required_states = 1);
    assert_eq!(
        f.read().unwrap().unwrap().markets()[0]
            .join()
            .unwrap()
            .instruments(),
        &[(1001, 5, 1)]
    );
}

#[test]
fn unreferenced_matching_join_does_not_replace_the_named_dependency() {
    let mut f = Fixture::new();
    let correct = f.objects.last().unwrap().clone();
    replace_original(&mut f, b"rebuild seed without a saved JOIN".to_vec());
    f.objects.push(correct);
    assert!(f.read().unwrap().unwrap().markets()[0].join().is_none());
}

#[test]
fn a_valid_current_view_without_account_has_no_complete_input_facts() {
    let mut f = Fixture::new();
    let retained = tickwire_release::replay::retained::decode(&f.objects[1]).unwrap();
    f.current.checkpoint = retained.anchor().checkpoint().clone();
    assert!(f.read().unwrap().is_none());
}
