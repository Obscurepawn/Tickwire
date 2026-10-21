use serde_json::Value;
use tickwire_ipc::*;
use tickwire_snapshot::*;

fn vector(name: &str) -> Vec<u8> {
    let f: Value =
        serde_json::from_str(include_str!("../../../specs/contracts/ipc-vectors-v1.json")).unwrap();
    let h = f["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()["hex"]
        .as_str()
        .unwrap();
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
        .collect()
}
struct Fixture {
    resource: ResourceHeader,
    join: JoinBody,
    states: Vec<MarketRecord>,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
    specs: Vec<InstrumentSpec>,
}
impl Fixture {
    fn new() -> Self {
        Self {
            resource: ResourceHeader::decode(&vector("normalized_resource_header")).unwrap(),
            join: JoinBody::decode(&vector("join_normalized_snapshot")).unwrap(),
            states: [
                "normalized_state_quote",
                "normalized_state_trade",
                "normalized_state_mark",
                "normalized_state_feed",
            ]
            .iter()
            .map(|n| MarketRecord::decode(&vector(n)).unwrap())
            .collect(),
            bids: vec![DepthLevel::decode(&vector("normalized_depth_1")).unwrap()],
            asks: vec![DepthLevel::decode(&vector("normalized_depth_2")).unwrap()],
            specs: vec![InstrumentSpec {
                instrument_id: 1001,
                instrument_version: 5,
                required_states: QUOTE | TRADE | MARK_FUNDING,
            }],
        }
    }
    fn member(&self) -> InstrumentSnapshot<'_> {
        InstrumentSnapshot {
            instrument_id: 1001,
            instrument_version: 5,
            snapshot_id: [18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            at_sequence: 1000,
            last_event_sequence: 990,
            feed_generation: 3,
            catalog_version: 5,
            catalog_hash: [0x22; 32],
            created_mono_ns: 1_000_000_000,
            expires_mono_ns: 2_000_000_000,
            state_version: 990,
            coverage_depth: 1,
            coverage_flags: 3,
            states: &self.states,
            bids: &self.bids,
            asks: &self.asks,
        }
    }
    fn request<'a>(&'a self, members: &'a [InstrumentSnapshot<'a>]) -> BuildRequest<'a> {
        BuildRequest {
            resource: &self.resource,
            join: &self.join,
            catalog_hash: [0x22; 32],
            instrument_set_version: 5,
            sequence: 1000,
            created_mono_ns: 1_000_000_000,
            expires_mono_ns: 2_000_000_000,
            now_mono_ns: 1_500_000_000,
            logical_strategy_id: 0,
            instruments: &self.specs,
            members,
            byte_limit: MAX_TOPIC_SNAPSHOT_BYTES,
        }
    }
    fn clear_bindings(&mut self) {
        self.join.snapshot_id = [0; 16];
        self.join.snapshot_hash = [0; 32];
        self.join.snapshot_seq = 0;
        self.join.join_seq = 0;
        self.join.topic_instrument_set_hash = [0; 32];
        self.join.topic_instrument_set_version = 0;
    }
}
#[test]
fn constructs_the_complete_independent_golden_bundle() {
    let f = Fixture::new();
    let members = [f.member()];
    let built = build_topic(&f.request(&members)).unwrap();
    assert_eq!(built.bytes(), vector("normalized_topic_snapshot"));
    assert_eq!(built.resource(), &f.resource);
    assert_eq!(built.join(), &f.join);
    let view = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: [0x22; 32],
            instruments: &f.specs,
            join: built.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    assert_eq!(view.check_readiness(1_500_000_000), Ok(()));
}

#[test]
fn unordered_states_are_canonical_and_keep_times_and_recompute_crc() {
    let mut f = Fixture::new();
    f.clear_bindings();
    for (i, r) in f.states.iter_mut().enumerate() {
        r.header.sequence = 900 + i as u64;
        r.header.flags = 15;
        r.header.source_timestamp_ns = 55 + i as u64;
        r.header.ingress_mono_ns = 900_000_000;
        r.header.publish_mono_ns = 950_000_000;
        r.header.record_crc32c = 123; // The builder owns this derived field.
    }
    f.states.reverse();
    let before = f.states.clone();
    let members = [f.member()];
    let b = build_topic(&f.request(&members)).unwrap();
    let v = validate(
        b.bytes(),
        &JoinContext {
            expected_resource: b.resource(),
            catalog_hash: [0x22; 32],
            instruments: &f.specs,
            join: b.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    for (i, row) in v
        .member(0)
        .unwrap()
        .state_records
        .as_chunks::<512>()
        .0
        .iter()
        .enumerate()
    {
        let Record::Market(r) = decode_record(row).unwrap() else {
            panic!()
        };
        assert_eq!(r.header.sequence, i as u64 + 1);
        assert_eq!(r.header.source_timestamp_ns, 55 + i as u64);
        assert_eq!(r.header.ingress_mono_ns, 900_000_000);
        assert_eq!(r.header.publish_mono_ns, 950_000_000);
        assert_ne!(r.header.record_crc32c, 123);
    }
    assert_eq!(f.states, before);
}

#[test]
fn full_multi_instrument_cut_keeps_nonlive_missing_states_and_unknown_depth() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.specs = (0..3)
        .map(|i| InstrumentSpec {
            instrument_id: 1001 + i,
            instrument_version: 5,
            required_states: QUOTE | TRADE | MARK_FUNDING,
        })
        .collect();
    let mut states = vec![f.states.clone(); 3];
    for (i, rows) in states.iter_mut().enumerate() {
        for r in rows {
            r.header.instrument_id = 1001 + i as u64;
        }
    }
    states[1][3].body.feed_state = 3;
    states[2][3].body.feed_state = 1;
    states[2][3].header.sequence = 0;
    let mut members = Vec::new();
    for (i, rows) in states.iter().enumerate() {
        let mut m = f.member();
        m.instrument_id = 1001 + i as u64;
        m.snapshot_id = [18 + i as u8; 16];
        m.states = rows;
        if i == 2 {
            m.states = &rows[3..];
            m.last_event_sequence = 0;
            m.bids = &[];
            m.asks = &[];
            m.coverage_depth = 0;
            m.coverage_flags = 3;
        }
        members.push(m);
    }
    let b = build_topic(&f.request(&members)).unwrap();
    let v = validate(
        b.bytes(),
        &JoinContext {
            expected_resource: b.resource(),
            catalog_hash: [0x22; 32],
            instruments: &f.specs,
            join: b.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    assert_eq!(v.len(), 3);
    assert_eq!(b.bytes().len(), 5 * 4096);
    assert_eq!(v.check_readiness(1_500_000_000), Err(Error::NotReady));
    for i in 0..3 {
        let m = v.member(i).unwrap();
        assert_eq!(m.header.sequence, 1000);
        assert_eq!(m.entry.snapshot_seq, 1000);
        assert_eq!(m.entry.snapshot_offset, 8192 + i as u64 * 4096);
    }
    let m = v.member(2).unwrap();
    assert_eq!(m.entry.feed_state, 1);
    assert_eq!(m.entry.last_instrument_event_seq, 0);
    assert!(m.depth_levels.is_empty());
    assert_eq!(m.header.coverage_flags, 3);
}

#[test]
fn rejects_missing_duplicate_unordered_or_mismatched_registry_members() {
    let mut f = Fixture::new();
    f.clear_bindings();
    let one = f.member();
    assert_eq!(
        build_topic(&f.request(&[])).unwrap_err(),
        Error::ResyncRequired
    );
    let duplicate = [one.clone(), one.clone()];
    assert_eq!(
        build_topic(&f.request(&duplicate)).unwrap_err(),
        Error::ResyncRequired
    );
    for (id, version) in [(1002, 5), (1001, 6), (0, 5)] {
        let mut m = one.clone();
        m.instrument_id = id;
        m.instrument_version = version;
        assert_eq!(
            build_topic(&f.request(&[m])).unwrap_err(),
            Error::ResyncRequired
        );
    }
    f.specs[0].required_states = 8;
    let members = [f.member()];
    assert_eq!(
        build_topic(&f.request(&members)).unwrap_err(),
        Error::UnsupportedFeature
    );
}

#[test]
fn rejects_mixed_cut_generation_catalog_and_future_member_events() {
    let mut f = Fixture::new();
    f.clear_bindings();
    for case in 0..7 {
        let mut m = f.member();
        match case {
            0 => m.at_sequence -= 1,
            1 => m.last_event_sequence = 1001,
            2 => m.feed_generation += 1,
            3 => m.catalog_version += 1,
            4 => m.catalog_hash = [3; 32],
            5 => m.snapshot_id = [0; 16],
            _ => m.snapshot_id = f.resource.resource_id,
        };
        assert_eq!(
            build_topic(&f.request(&[m])).unwrap_err(),
            Error::Identity,
            "case {case}"
        );
    }
}

#[test]
fn same_snapshot_id_for_two_different_members_is_rejected() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.specs.push(InstrumentSpec {
        instrument_id: 1002,
        instrument_version: 5,
        required_states: 0,
    });
    let mut states = f.states.clone();
    for r in &mut states {
        r.header.instrument_id = 1002;
    }
    let first = f.member();
    let mut second = first.clone();
    second.instrument_id = 1002;
    second.states = &states;
    assert_eq!(
        build_topic(&f.request(&[first, second])).unwrap_err(),
        Error::Identity
    );
}

#[test]
fn sequence_limit_leaves_room_for_j_and_zero_cut_can_bootstrap() {
    let mut f = Fixture::new();
    f.clear_bindings();
    let mut m = f.member();
    m.at_sequence = (1 << 63) - 2;
    let members = [m];
    let mut request = f.request(&members);
    request.sequence = (1 << 63) - 2;
    assert_eq!(
        build_topic(&request).unwrap().join().join_seq,
        (1 << 63) - 1
    );
    for s in [(1 << 63) - 1, 1 << 63, u64::MAX] {
        let mut members = [f.member()];
        members[0].at_sequence = s;
        let mut request = f.request(&members);
        request.sequence = s;
        assert_eq!(build_topic(&request).unwrap_err(), Error::SequenceExhausted);
    }
    f.states = vec![f.states[3].clone()];
    f.states[0].header.sequence = 0;
    f.states[0].body.feed_state = 1;
    let mut m = f.member();
    m.at_sequence = 0;
    m.last_event_sequence = 0;
    m.bids = &[];
    m.asks = &[];
    let members = [m];
    let mut request = f.request(&members);
    request.sequence = 0;
    assert_eq!(build_topic(&request).unwrap().join().join_seq, 1);
}

#[test]
fn exact_page_budget_is_accepted_and_one_byte_less_is_rejected() {
    let f = Fixture::new();
    let members = [f.member()];
    let mut request = f.request(&members);
    request.byte_limit = 12288;
    assert_eq!(build_topic(&request).unwrap().bytes().len(), 12288);
    for limit in [0, 12287, MAX_TOPIC_SNAPSHOT_BYTES + 1] {
        request.byte_limit = limit;
        assert_eq!(build_topic(&request).unwrap_err(), Error::Bounds);
    }
}

#[test]
fn overlarge_depth_is_rejected_by_complete_object_budget_before_encoding() {
    let f = Fixture::new();
    let oversized = vec![f.bids[0].clone(); MAX_TOPIC_SNAPSHOT_BYTES / 16];
    let mut m = f.member();
    m.bids = &oversized;
    assert_eq!(build_topic(&f.request(&[m])).unwrap_err(), Error::Bounds);
}

#[test]
fn depth_crosses_multiple_pages_with_exact_offsets_and_zero_padding() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.bids = (0..500)
        .map(|i| DepthLevel {
            price_ticks: 6_500_000 - i,
            qty_lots: 1,
        })
        .collect();
    let members = [f.member()];
    let b = build_topic(&f.request(&members)).unwrap();
    let entry = TopicSnapshotEntry::decode(&b.bytes()[4352..4480]).unwrap();
    assert_eq!(entry.snapshot_bytes, 12288);
    let depth = SnapshotSection::decode(&b.bytes()[8512..8576]).unwrap();
    assert_eq!(depth.offset, 10624);
    assert_eq!(depth.entry_count, 501);
    let used = depth.offset as usize + depth.length as usize;
    assert!(b.bytes()[used..].iter().all(|b| *b == 0));
    assert_eq!(b.resource().capacity, 1);
    assert_eq!(b.header().manifest_member_bytes, 12288);
}

#[test]
fn invalid_depth_is_rejected_without_sanitizing_order_or_quantities() {
    let mut f = Fixture::new();
    f.clear_bindings();
    for levels in [
        vec![DepthLevel {
            price_ticks: 0,
            qty_lots: 1,
        }],
        vec![DepthLevel {
            price_ticks: 10,
            qty_lots: 0,
        }],
        vec![DepthLevel {
            price_ticks: 10,
            qty_lots: -1,
        }],
        vec![
            DepthLevel {
                price_ticks: 10,
                qty_lots: 1,
            },
            DepthLevel {
                price_ticks: 11,
                qty_lots: 1,
            },
        ],
        vec![
            DepthLevel {
                price_ticks: 10,
                qty_lots: 1
            };
            2
        ],
    ] {
        let mut m = f.member();
        m.bids = &levels;
        assert_eq!(build_topic(&f.request(&[m])).unwrap_err(), Error::Malformed);
    }
    f.asks[0].price_ticks = f.bids[0].price_ticks;
    let members = [f.member()];
    assert_eq!(
        build_topic(&f.request(&members)).unwrap_err(),
        Error::Malformed
    );
}

#[test]
fn rejects_bad_state_types_duplicates_missing_feed_and_original_sequence() {
    for case in 0..7 {
        let mut f = Fixture::new();
        f.clear_bindings();
        match case {
            0 => {
                f.states.pop();
            }
            1 => {
                f.states[1] = f.states[0].clone();
            }
            2 => f.states[0].header.message_type = 259,
            3 => f.states[0].header.feed_generation += 1,
            4 => f.states[0].header.sequence = 991,
            5 => f.states[0].header.producer_generation += 1,
            _ => f.states[0].header.topic_or_scope_id += 1,
        };
        let members = [f.member()];
        let err = build_topic(&f.request(&members)).unwrap_err();
        assert!(
            matches!(
                err,
                Error::Malformed | Error::Identity | Error::UnknownMessage
            ),
            "case {case}: {err:?}"
        );
    }
}

#[test]
fn rejects_expired_future_and_observation_time_conflicts() {
    let mut f = Fixture::new();
    f.clear_bindings();
    for case in 0..4 {
        let mut m = f.member();
        match case {
            0 => m.created_mono_ns = 1_500_000_001,
            1 => m.expires_mono_ns = 1_500_000_000,
            2 => m.created_mono_ns = 2_000_000_001,
            _ => m.expires_mono_ns = 0,
        };
        assert_eq!(build_topic(&f.request(&[m])).unwrap_err(), Error::Deadline);
    }
    f.states[0].header.flags = 2;
    f.states[0].header.ingress_mono_ns = 1_000_000_001;
    let members = [f.member()];
    assert_eq!(
        build_topic(&f.request(&members)).unwrap_err(),
        Error::Deadline
    );
}

#[test]
fn join_identity_and_existing_binding_conflicts_are_not_overwritten() {
    for case in 0..12 {
        let mut f = Fixture::new();
        f.clear_bindings();
        match case {
            0 => f.join.channel_id = [3; 16],
            1 => f.join.feed_generation += 1,
            2 => f.join.expected_catalog_version += 1,
            3 => f.join.membership_version += 1,
            4 => f.join.bank_resource_id = [0; 16],
            5 => f.join.cursor_resource_id = [0; 16],
            6 => f.join.subscription_generation = 0,
            7 => f.join.snapshot_id = [99; 16],
            8 => f.join.snapshot_seq = 999,
            9 => f.join.join_seq = 999,
            10 => f.join.snapshot_hash = [99; 32],
            _ => f.join.topic_instrument_set_hash = [99; 32],
        };
        let members = [f.member()];
        assert_eq!(
            build_topic(&f.request(&members)).unwrap_err(),
            Error::Identity,
            "case {case}"
        );
    }
}

#[test]
fn complete_registry_limit_and_manifest_alignment_are_supported() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.specs = (1..=4096)
        .map(|id| InstrumentSpec {
            instrument_id: id,
            instrument_version: 5,
            required_states: QUOTE,
        })
        .collect();
    let records = (1..=4096)
        .map(|id| {
            let mut r = f.states[3].clone();
            r.header.instrument_id = id;
            r.body.feed_state = 1;
            r
        })
        .collect::<Vec<_>>();
    let members = records
        .iter()
        .map(|r| {
            let mut m = f.member();
            m.instrument_id = r.header.instrument_id;
            m.snapshot_id = [0; 16];
            m.snapshot_id[..8].copy_from_slice(&m.instrument_id.to_le_bytes());
            m.snapshot_id[15] = 99;
            m.states = std::slice::from_ref(r);
            m.bids = &[];
            m.asks = &[];
            m
        })
        .collect::<Vec<_>>();
    let built = build_topic(&f.request(&members)).unwrap();
    assert_eq!(built.resource().capacity, 4096);
    assert_eq!(built.header().entry_count, 4096);
    let first = TopicSnapshotEntry::decode(&built.bytes()[4352..4480]).unwrap();
    assert_eq!(first.snapshot_offset, 532480);
    assert_eq!(built.bytes().len(), 532480 + 4096 * 4096);
    let view = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: [0x22; 32],
            instruments: &f.specs,
            join: built.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    assert_eq!(view.member(4095).unwrap().entry.instrument_id, 4096);
    assert_eq!(view.check_readiness(1_500_000_000), Err(Error::NotReady));
    let too_many = vec![f.specs[0]; 4097];
    let mut request = f.request(&members);
    request.instruments = &too_many;
    assert_eq!(build_topic(&request).unwrap_err(), Error::Bounds);
}

#[test]
fn budget_covers_manifest_padding_and_all_members_together() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.specs.push(InstrumentSpec {
        instrument_id: 1002,
        instrument_version: 5,
        required_states: 0,
    });
    let mut second = f.states.clone();
    for r in &mut second {
        r.header.instrument_id = 1002;
    }
    let bids = (0..256)
        .map(|i| DepthLevel {
            price_ticks: 6_500_000 - i,
            qty_lots: 1,
        })
        .collect::<Vec<_>>();
    let mut a = f.member();
    a.bids = &bids;
    let mut b = a.clone();
    b.instrument_id = 1002;
    b.snapshot_id = [19; 16];
    b.states = &second;
    let members = [a, b];
    let mut request = f.request(&members);
    request.byte_limit = 6 * 4096;
    assert_eq!(build_topic(&request).unwrap().bytes().len(), 6 * 4096);
    request.byte_limit = 5 * 4096;
    assert_eq!(build_topic(&request).unwrap_err(), Error::Bounds);
    let reversed_specs = [f.specs[1], f.specs[0]];
    let reversed_members = [members[1].clone(), members[0].clone()];
    let mut request = f.request(&reversed_members);
    request.instruments = &reversed_specs;
    assert_eq!(build_topic(&request).unwrap_err(), Error::ResyncRequired);
}

#[test]
fn profile_invalid_time_coverage_and_nonlive_states_are_checked() {
    for case in 0..8 {
        let mut f = Fixture::new();
        f.clear_bindings();
        match case {
            0 => f.resource.host_boot_id = [0; 16],
            1 => f.resource.required_features = 0,
            2 => f.resource.target_arch = 3,
            3 => f.resource.event_class = 1,
            4 => f.resource.trust_profile = 3,
            5 => f.resource.first_seq = 1 << 63,
            6 => f.join.readiness_deadline_mono_ns = 1_500_000_000,
            _ => f.resource.created_mono_ns = 1_000_000_001,
        };
        let members = [f.member()];
        let expected = match case {
            0 | 3 => Error::Identity,
            1 => Error::UnsupportedFeature,
            2 => Error::UnsupportedArchitecture,
            4 => Error::InvalidEnum,
            5 => Error::SequenceExhausted,
            _ => Error::Deadline,
        };
        assert_eq!(build_topic(&f.request(&members)).unwrap_err(), expected);
    }
    let mut f = Fixture::new();
    f.clear_bindings();
    let mut m = f.member();
    m.coverage_flags = 4;
    assert_eq!(
        build_topic(&f.request(&[m])).unwrap_err(),
        Error::InvalidEnum
    );
    for state in [1, 3, 4, 5] {
        f.states[3].body.feed_state = state;
        let members = [f.member()];
        let b = build_topic(&f.request(&members)).unwrap();
        let e = TopicSnapshotEntry::decode(&b.bytes()[4352..4480]).unwrap();
        assert_eq!(e.feed_state, state);
    }
    f.states[3].body.feed_state = 0;
    let members = [f.member()];
    assert_eq!(
        build_topic(&f.request(&members)).unwrap_err(),
        Error::InvalidEnum
    );
}

#[test]
fn heterogeneous_snapshot_preserves_exact_mark_and_requires_feature() {
    let mut f = Fixture::new();
    f.clear_bindings();
    f.resource.required_features |= EXACT_MARKET_EVENTS;
    let mut h = f.states[2].header.clone();
    h.message_type = 266;
    h.flags &= !1;
    h.source_timestamp_ns = 0;
    let mut exact = MarketExactRecord {
        header: h,
        body: MarketExactBody {
            source_id: 7,
            source_connection_epoch: 9,
            source_encoding: 1,
            feed_state: 2,
            mark_price: DecimalWire::new(1179415, -2).unwrap(),
            index_price: DecimalWire::new(1178462659091, -8).unwrap(),
            ..Default::default()
        },
    };
    exact.body.value_validity = 32;
    exact.body.funding_rate = DecimalWire::new(-3, -4).unwrap();
    let states = [
        MarketState::from(f.states[0].clone()),
        f.states[1].clone().into(),
        exact.clone().into(),
        f.states[3].clone().into(),
    ];
    let members = [f.member().with_states(&states)];
    let old = f.request(&[]);
    let req = BuildRequest {
        resource: old.resource,
        join: old.join,
        catalog_hash: old.catalog_hash,
        instrument_set_version: old.instrument_set_version,
        sequence: old.sequence,
        created_mono_ns: old.created_mono_ns,
        expires_mono_ns: old.expires_mono_ns,
        now_mono_ns: old.now_mono_ns,
        logical_strategy_id: old.logical_strategy_id,
        instruments: old.instruments,
        members: &members,
        byte_limit: old.byte_limit,
    };
    let built = build_topic(&req).unwrap();
    let view = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: req.catalog_hash,
            instruments: &f.specs,
            join: built.join(),
            now_mono_ns: req.now_mono_ns,
        },
    )
    .unwrap();
    assert!(view.check_readiness(req.now_mono_ns).is_ok());
    let rows = view.member(0).unwrap().state_records;
    let Record::MarketExact(decoded) = decode_record(&rows[1536..2048]).unwrap() else {
        panic!()
    };
    assert_eq!(decoded.body, exact.body);
    let mut unsupported = f.resource.clone();
    unsupported.required_features &= !EXACT_MARKET_EVENTS;
    let req = BuildRequest {
        resource: &unsupported,
        ..req
    };
    assert_eq!(build_topic(&req).unwrap_err(), Error::UnsupportedFeature);
}
