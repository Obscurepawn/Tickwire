use super::*;
use tickwire_recovery::{HandoverSeal, InputRecord, encode_inputs};
use tickwire_release::replay::{self, Checkpoint, EffectCut, Rng, RuntimeState};

struct Fixture {
    identity: Identity,
    checkpoint: Checkpoint,
    rows: Vec<Vec<u8>>,
    ready: HandoverReadyBody,
    objects: Vec<Vec<u8>>,
}
impl Fixture {
    fn new() -> Self {
        let identity = Identity {
            namespace: [1; 16],
            instance: [2; 16],
            generation: 3,
            logical_strategy: 4,
            role: 3,
            qualification_hash: [5; 32],
            manifest_hash: [6; 32],
            arch: 1,
            trust_profile: 1,
            build_hash: [7; 32],
            host_boot: [8; 16],
        };
        let checkpoint = Checkpoint::new(
            Source {
                logical_strategy: identity.logical_strategy,
                instance: identity.instance, generation: identity.generation,
                binary_digest: identity.build_hash, state_schema: 9,
                input_stream: [10; 16],
                clock: replay::ClockDomain { boot_id: identity.host_boot, generation: 11 },
            },
            0, [0; 32],
            br#"{"schema":1,"strategy":[115,116,97,116,101],"maximum_age_ns":10,"markets":[{"resource":[1],"maximum_levels":10,"view":[1]}],"accounts":[]}"#.to_vec(),
            RuntimeState {
                now_ns: 12,
                config: VersionedBytes { revision: 73, bytes: br#"{"threshold":4}"#.to_vec() },
                models: vec![], markets: vec![], accounts: vec![], external: vec![],
                timers: vec![], rng: Rng::Disabled, effects: EffectCut { count: 0, digest: [0; 32] },
            },
        ).unwrap();
        let mut f = Self {
            identity,
            checkpoint,
            rows: vec![],
            ready: Default::default(),
            objects: vec![],
        };
        f.rows = vec![
            tag(b"TWM2A001", &f.checkpoint.bytes().unwrap()),
            tag(b"TWM2C001", &f.checkpoint.bytes().unwrap()),
        ];
        f.seal(b"state");
        f
    }
    // This fixture supplies the already authenticated closure boundary. It uses
    // real checkpoint, input-chain and handover codecs, not a FreshObservation.
    fn seal(&mut self, state: &[u8]) {
        let inputs = encode_inputs(
            [10; 16],
            1,
            [0; 32],
            &self
                .rows
                .iter()
                .map(|r| InputRecord {
                    kind: 5,
                    payload: r,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .bytes;
        let mut raw = b"TWHS0001".to_vec();
        raw.extend(self.identity.instance);
        for n in [
            self.identity.generation,
            self.identity.logical_strategy,
            0,
            1,
        ] {
            raw.extend(n.to_le_bytes());
        }
        raw.extend([13; 32]);
        raw.extend((state.len() as u64).to_le_bytes());
        raw.extend(state);
        let seal = HandoverSeal {
            version: 1,
            owner: self.identity.instance,
            instance: self.identity.instance,
            generation: self.identity.generation,
            logical_strategy: self.identity.logical_strategy,
            report_position: 0,
            ledger_sequence: 1,
            orders_digest: [13; 32],
            checkpoint_digest: Object::of(&raw).digest,
            checkpoint_bytes: raw.len() as u64,
            account_input_digest: [14; 32],
            account_input_bytes: 1,
            input_digest: Object::of(&inputs).digest,
            input_bytes: inputs.len() as u64,
            dependencies: (2..=11).map(|k| (k, [k as u8; 32])).collect(),
            preflight_receipt_digest: None,
            replay_installation_digest: None,
            replay_restore_digest: None,
            replay_rollback_digest: None,
            rebuild_installation_digest: None,
            rebuild_rollback_digest: None,
        };
        let bytes = seal.bytes().unwrap();
        self.ready = HandoverReadyBody {
            instance_id: seal.instance,
            logical_strategy_id: seal.logical_strategy,
            strategy_generation: seal.generation,
            checkpoint_digest: seal.checkpoint_digest,
            checkpoint_bytes: seal.checkpoint_bytes,
            input_seal_digest: Object::of(&bytes).digest,
            input_seal_bytes: bytes.len() as u64,
            exec_report_sequence: 0,
            ledger_sequence: 1,
            orders_digest: seal.orders_digest,
            report_ring_sequence: 1,
            ready_sequence: 1,
        };
        self.objects = vec![bytes, raw, inputs];
    }
    fn read(&self) -> Result<Option<ObservedConfiguration>> {
        extract(
            &self.ready,
            &self.objects,
            &self.identity,
            self.identity.build_hash,
            tickwire_clock::ClockDomain::new(self.identity.host_boot, 11).unwrap(),
        )
    }
}
fn tag(kind: &[u8; 8], bytes: &[u8]) -> Vec<u8> {
    [kind.as_slice(), bytes].concat()
}

#[test]
fn present_merged_anchor_without_completed_cut_is_not_legacy() {
    let mut f = Fixture::new();
    f.rows.pop();
    f.seal(b"state");
    assert!(
        f.read().is_err(),
        "incomplete merged proof must fail, not become an unsupported legacy result"
    );
}

#[test]
fn complete_current_cut_exposes_exact_private_configuration() {
    let f = Fixture::new();
    let observed = f.read().unwrap().expect("complete merged configuration");
    assert_eq!(observed.revision(), 73);
    assert_eq!(observed.bytes(), br#"{"threshold":4}"#);
    assert_eq!(observed.object(), Object::of(observed.bytes()));
    assert_eq!(
        observed.checkpoint(),
        Object::of(&f.checkpoint.bytes().unwrap())
    );
    assert_eq!(observed.cut(), &f.checkpoint.cut);
    assert_eq!(observed.source(), &f.checkpoint.source);
}

#[test]
fn actual_config_callback_current_cut_wins_over_the_startup_anchor() {
    use tickwire_strategy_sdk::replay::{Engine, ReplayContext, ReplayEffects, ReplayableStrategy};
    struct Applying(Vec<u8>);
    impl ReplayableStrategy for Applying {
        fn checkpoint(&self) -> tickwire_strategy_sdk::replay::Result<Vec<u8>> {
            Ok(self.0.clone())
        }
        fn apply(
            &mut self,
            input: &replay::Input,
            context: &mut ReplayContext<'_>,
            _: &mut ReplayEffects,
        ) -> tickwire_strategy_sdk::replay::Result<()> {
            let replay::Input::ConfigApply { next, .. } = input else {
                panic!("actual configuration callback")
            };
            assert_eq!(context.config(), next);
            Ok(())
        }
    }
    let mut f = Fixture::new();
    let anchor = replay::verify_anchor(
        &f.checkpoint.bytes().unwrap(),
        &f.checkpoint.source,
        Object::of(&f.checkpoint.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let mut engine = Engine::new(&anchor, Applying(f.checkpoint.business.clone())).unwrap();
    let next = VersionedBytes {
        revision: 74,
        bytes: br#"{"threshold":9}"#.to_vec(),
    };
    engine
        .apply_live(
            replay::Input::ConfigApply {
                previous: Object::of(&f.checkpoint.runtime.config.bytes),
                next: next.clone(),
            },
            13,
        )
        .unwrap();
    f.checkpoint = engine.checkpoint().unwrap();
    f.rows = vec![
        tag(b"TWM2A001", &anchor.checkpoint().bytes().unwrap()),
        tag(
            b"TWM2R001",
            &replay::canonical(&engine.recorded()[0]).unwrap(),
        ),
        tag(b"TWM2C001", &f.checkpoint.bytes().unwrap()),
    ];
    f.seal(b"state");
    let observed = f.read().unwrap().unwrap();
    assert_eq!(observed.revision(), next.revision);
    assert_eq!(observed.bytes(), next.bytes);
    assert_eq!(observed.cut().sequence, 1);
    assert_eq!(
        observed.checkpoint(),
        Object::of(&f.checkpoint.bytes().unwrap())
    );
}

#[test]
fn rehashed_valid_cuts_cannot_switch_live_process_identity_image_or_clock() {
    for case in 0..6 {
        let mut f = Fixture::new();
        match case {
            0 => f.checkpoint.source.instance = [22; 16],
            1 => f.checkpoint.source.generation += 1,
            2 => f.checkpoint.source.logical_strategy += 1,
            3 => f.checkpoint.source.binary_digest = [23; 32],
            4 => f.checkpoint.source.clock.boot_id = [24; 16],
            _ => f.checkpoint.source.clock.generation += 1,
        }
        let bytes = f.checkpoint.bytes().unwrap();
        f.rows = vec![tag(b"TWM2A001", &bytes), tag(b"TWM2C001", &bytes)];
        f.seal(b"state");
        // The retained cut itself is valid. Only binding it to this real
        // process's expected facts must fail.
        replay::retained::decode(&f.objects[2]).unwrap();
        assert!(f.read().is_err(), "source mutation {case}");
    }
}

#[test]
fn current_business_state_must_equal_the_ready_checkpoint() {
    let mut f = Fixture::new();
    f.seal(b"another state");
    replay::retained::decode(&f.objects[2]).unwrap();
    assert!(
        f.read().is_err(),
        "two individually valid roots must not prove different business states"
    );
}

#[test]
fn named_roots_are_required_and_unrelated_or_duplicate_objects_do_not_change_the_cut() {
    let mut f = Fixture::new();
    f.objects.push(f.checkpoint.bytes().unwrap());
    f.objects.extend(f.objects.clone());
    assert_eq!(f.read().unwrap().unwrap().revision(), 73);
    for root in 0..3 {
        let mut f = Fixture::new();
        f.objects[root].push(0);
        assert!(f.read().is_err(), "corrupted named root {root}");
    }
    let mut f = Fixture::new();
    f.ready.input_seal_digest = [91; 32];
    assert!(f.read().is_err());
}

#[test]
fn only_absent_merged_history_is_legacy_not_unknown_or_partial_merged_records() {
    let mut f = Fixture::new();
    f.rows.clear();
    f.seal(b"state");
    assert!(f.read().unwrap().is_none());
    for rows in [
        vec![vec![1]],
        vec![tag(b"TWM2X001", b"unexpected")],
        vec![tag(b"TWM2C001", &f.checkpoint.bytes().unwrap())],
    ] {
        f.rows = rows;
        f.seal(b"state");
        assert!(f.read().is_err());
    }
}

#[test]
fn live_image_and_query_clock_must_match_the_approved_identity() {
    let f = Fixture::new();
    let domain = tickwire_clock::ClockDomain::new(f.identity.host_boot, 11).unwrap();
    assert!(extract(&f.ready, &f.objects, &f.identity, [92; 32], domain).is_err());
    assert!(
        extract(
            &f.ready,
            &f.objects,
            &f.identity,
            f.identity.build_hash,
            tickwire_clock::ClockDomain::new([93; 16], 11).unwrap()
        )
        .is_err()
    );
    let mut identity = f.identity.clone();
    identity.build_hash = [94; 32];
    assert!(
        extract(
            &f.ready,
            &f.objects,
            &identity,
            f.identity.build_hash,
            domain
        )
        .is_err()
    );
    identity = f.identity.clone();
    identity.host_boot = [95; 16];
    assert!(
        extract(
            &f.ready,
            &f.objects,
            &identity,
            f.identity.build_hash,
            domain
        )
        .is_err()
    );
}
