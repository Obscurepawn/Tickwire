use serde::Deserialize;
use tickwire_journal_codec::*;

fn order() -> OrderIdentity {
    OrderIdentity {
        venue: 1,
        account: 2,
        product: Product::Spot,
        intent_id: 3,
        intent_seq: 9,
        intent_exec_epoch: 7,
    }
}
fn prepared() -> PreparedIntent {
    PreparedIntent {
        order: order(),
        quantity: 4,
        unit_budget: 10,
        fee_budget: 2,
        strategy_generation: 8,
        config_revision: 9,
        deadline: 10,
        recovery_object: 11,
        commit_profile: CommitProfile::LocalDurable,
    }
}
fn record(name: &str) -> Record {
    let (journal_seq, event) = match name {
        "prepared" => (1, Event::PreparedIntent(prepared())),
        "fill" => (
            2,
            Event::Fill {
                order: order(),
                trade_id: 4,
                quantity: 2,
                cost: 20,
            },
        ),
        "late_fill" => (
            6,
            Event::Fill {
                order: order(),
                trade_id: 4,
                quantity: 2,
                cost: 20,
            },
        ),
        "fee" => (
            3,
            Event::Fee {
                order: order(),
                fee_id: 5,
                amount: -1,
            },
        ),
        "terminal" => (
            4,
            Event::Terminal {
                order: order(),
                status: TerminalStatus::Canceled,
                cumulative: 2,
            },
        ),
        "aborted" => (5, Event::AbortedBeforeSend { order: order() }),
        _ => panic!("unknown fixture"),
    };
    Record {
        risk_domain_id: core::array::from_fn(|i| i as u8),
        journal_epoch: core::array::from_fn(|i| 16 + i as u8),
        journal_seq,
        writer_exec_epoch: if name == "late_fill" { 8 } else { 7 },
        event,
    }
}
fn maximum() -> Record {
    let mut r = record("prepared");
    r.journal_seq = u64::MAX;
    r.writer_exec_epoch = u64::MAX;
    r.event = Event::PreparedIntent(PreparedIntent {
        order: OrderIdentity {
            venue: u32::MAX,
            account: u64::MAX,
            product: Product::UsdM,
            intent_id: u64::MAX,
            intent_seq: u64::MAX,
            intent_exec_epoch: u64::MAX,
        },
        quantity: u64::MAX,
        unit_budget: i128::MAX,
        fee_budget: i128::MIN,
        strategy_generation: u64::MAX,
        config_revision: u64::MAX,
        deadline: u64::MAX,
        recovery_object: u64::MAX,
        commit_profile: CommitProfile::SyncReplica,
    });
    r
}
fn unhex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(core::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn hash(s: &str) -> Hash {
    unhex(s).try_into().unwrap()
}
#[derive(Deserialize)]
struct Golden {
    genesis: String,
    events: Vec<GoldenEvent>,
    maximum: GoldenMaximum,
    integers: Vec<GoldenInteger>,
}
#[derive(Deserialize)]
struct GoldenEvent {
    name: String,
    wire: String,
    business_digest: String,
    logical_payload_hash: String,
    logical_hash: String,
}
#[derive(Deserialize)]
struct GoldenMaximum {
    wire: String,
    business_digest: String,
    logical_payload_hash: String,
}
#[derive(Deserialize)]
struct GoldenInteger {
    value: String,
    wire: String,
}
fn golden() -> Golden {
    serde_json::from_str(include_str!("golden.json")).unwrap()
}

#[test]
fn independent_golden_events_takeover_and_logical_prefixes() {
    let g = golden();
    let r = record("prepared");
    let mut chain = LogicalChain::new(r.risk_domain_id, r.journal_epoch);
    assert_eq!(chain.hash(), hash(&g.genesis));
    for v in g.events {
        let r = record(&v.name);
        let wire = unhex(&v.wire);
        assert_eq!(encode(&r), wire, "{}", v.name);
        assert_eq!(decode(&wire), Ok(r));
        assert_eq!(business_digest(&r), hash(&v.business_digest));
        assert_eq!(logical_payload_hash(&r), hash(&v.logical_payload_hash));
        assert_eq!(chain.append(&r), Ok(hash(&v.logical_hash)));
        assert_eq!(chain.last_seq(), r.journal_seq);
    }
}

#[test]
fn maximum_size_and_extreme_i128_are_exact() {
    let g = golden().maximum;
    let r = maximum();
    let wire = unhex(&g.wire);
    assert_eq!(wire.len(), MAX_ENCODED_EVENT_BYTES);
    assert_eq!(encode(&r), wire);
    assert_eq!(decode(&wire), Ok(r));
    assert_eq!(business_digest(&r), hash(&g.business_digest));
    assert_eq!(logical_payload_hash(&r), hash(&g.logical_payload_hash));
}

#[test]
fn rfc_integer_golden_boundaries_round_trip_in_fee_field() {
    for v in golden().integers {
        let amount = v.value.parse::<i128>().unwrap();
        let mut r = record("fee");
        r.event = Event::Fee {
            order: order(),
            fee_id: 5,
            amount,
        };
        let wire = encode(&r);
        assert!(wire.ends_with(&unhex(&v.wire)), "{}", v.value);
        assert_eq!(decode(&wire), Ok(r));
    }
}

// Build mutations from the checked-in golden bytes, never from the Rust encoder.
fn fee_wire() -> Vec<u8> {
    unhex(
        &golden()
            .events
            .into_iter()
            .find(|e| e.name == "fee")
            .unwrap()
            .wire,
    )
}
fn with_amount(bytes: &[u8]) -> Vec<u8> {
    let mut wire = fee_wire();
    assert_eq!(wire.pop(), Some(0x20));
    wire.extend_from_slice(bytes);
    wire
}

#[test]
fn rejects_noncanonical_and_out_of_range_integer_representations() {
    for bad in [
        "1800",
        "190018",
        "1a00000100",
        "1b0000000000010000",
        "3800",
        "390018",
        "3a00000100",
        "3b0000000000010000",
        "c240",
        "c24100",
        "c249000100000000000000",
        "c248ffffffffffffffff",
        "c348ffffffffffffffff",
        "d80249010000000000000000",
        "c25080000000000000000000000000000000",
        "c35080000000000000000000000000000000",
        "c2510100000000000000000000000000000000",
        "c201",
        "c301",
        "c449010000000000000000",
        "c25f4101ff",
        "fa3f800000",
        "f93c00",
        "fb3ff0000000000000",
        "f4",
        "f6",
        "ff",
    ] {
        assert!(decode(&with_amount(&unhex(bad))).is_err(), "{bad}");
    }
}

#[test]
fn rejects_schema_map_key_length_and_type_variants() {
    let base = fee_wire();
    let mut cases = Vec::new();
    for (index, replacement) in [(0, 0xbf), (0, 0xa6), (0, 0xa8), (0, 0x87), (2, 2)] {
        let mut b = base.clone();
        b[index] = replacement;
        cases.push(b);
    }
    // Overlong map length and schema version.
    let mut b = vec![0xb8, 7];
    b.extend_from_slice(&base[1..]);
    cases.push(b);
    let mut b = base[..2].to_vec();
    b.extend_from_slice(&[0x18, 1]);
    b.extend_from_slice(&base[3..]);
    cases.push(b);
    // Domain byte string: wrong length and attacker-controlled huge length.
    for bad in [
        &[0x4f][..],
        &[0x51][..],
        &[0x58, 0x10][..],
        &[0x5b, 255, 255, 255, 255, 255, 255, 255, 255][..],
    ] {
        let mut b = base[..4].to_vec();
        b.extend_from_slice(bad);
        b.extend_from_slice(&base[5..]);
        cases.push(b);
    }
    // Last payload map is a8 0001 0102 0201 0303 0409 0507 0605 0720.
    let prefix = &base[..base.len() - 17];
    for payload in [
        "a800010102020103030409050706050620", // duplicate key 6
        "a800010102020103030409050707200605", // descending keys
        "a800010102020103030409050706050820", // unknown key 8
        "a70001010202010303040905070605",     // missing amount
        "a800010102020003030409050706050720", // unknown product
        "a8001bffffffffffffffff0102020103030409050706050720", // venue overflow
        "a800010102020103030409050706050720ff", // trailing break
    ] {
        let mut b = prefix.to_vec();
        b.extend(unhex(payload));
        cases.push(b);
    }
    for (index, b) in cases.iter().enumerate() {
        assert!(decode(b).is_err(), "mutation {index}: {b:02x?}");
    }
}

#[test]
fn rejects_all_truncations_trailing_data_and_excess_length() {
    let mut vectors: Vec<_> = golden()
        .events
        .into_iter()
        .map(|v| unhex(&v.wire))
        .collect();
    vectors.push(unhex(&golden().maximum.wire));
    for wire in vectors {
        for end in 0..wire.len() {
            assert!(decode(&wire[..end]).is_err(), "prefix {end}");
        }
        let mut extra = wire;
        extra.push(0);
        assert!(decode(&extra).is_err());
    }
    assert_eq!(
        decode(&[0; MAX_ENCODED_EVENT_BYTES + 1]),
        Err(DecodeError::TooLong)
    );
}

#[test]
fn accepted_mutated_inputs_reencode_to_exactly_the_same_bytes() {
    for v in golden().events {
        let wire = unhex(&v.wire);
        for position in 0..wire.len() {
            for value in 0..=u8::MAX {
                let mut mutated = wire.clone();
                mutated[position] = value;
                if let Ok(r) = decode(&mutated) {
                    assert_eq!(encode(&r), mutated, "{} at {position}", v.name);
                }
            }
        }
    }
}

#[test]
fn business_digest_ignores_writer_and_journal_placement() {
    let original = record("prepared");
    let expected = business_digest(&original);
    let mut moved = original;
    moved.journal_seq = 700;
    moved.journal_epoch = [42; 16];
    moved.writer_exec_epoch += 1;
    assert_eq!(business_digest(&moved), expected);
    let mut mutations = Vec::new();
    let mut changed = original;
    changed.risk_domain_id[0] ^= 1;
    mutations.push(changed);
    for field in 0..14 {
        let mut p = prepared();
        match field {
            0 => p.order.venue += 1,
            1 => p.order.account += 1,
            2 => p.order.product = Product::UsdM,
            3 => p.order.intent_id += 1,
            4 => p.order.intent_seq += 1,
            5 => p.quantity += 1,
            6 => p.unit_budget += 1,
            7 => p.fee_budget += 1,
            8 => p.strategy_generation += 1,
            9 => p.config_revision += 1,
            10 => p.deadline += 1,
            11 => p.recovery_object += 1,
            12 => p.commit_profile = CommitProfile::SyncReplica,
            13 => p.order.intent_exec_epoch += 1,
            _ => unreachable!(),
        }
        let mut changed = original;
        changed.event = Event::PreparedIntent(p);
        mutations.push(changed);
    }
    mutations.push(record("fill"));
    for changed in mutations {
        assert_ne!(business_digest(&changed), expected);
    }
}

#[test]
fn sequence_identity_epoch_and_hash_failures_are_atomic() {
    let first = record("prepared");
    let second = record("fill");
    let mut chain = LogicalChain::new(first.risk_domain_id, first.journal_epoch);
    chain.append(&first).unwrap();
    let original = chain;
    let mut wrong = second;
    wrong.risk_domain_id[0] ^= 1;
    assert_eq!(chain.append(&wrong), Err(ChainError::WrongDomain));
    assert_eq!(chain, original);
    wrong = second;
    wrong.journal_epoch[0] ^= 1;
    assert_eq!(chain.append(&wrong), Err(ChainError::WrongJournalEpoch));
    assert_eq!(chain, original);
    wrong = second;
    wrong.journal_seq += 1;
    assert_eq!(chain.append(&wrong), Err(ChainError::Sequence));
    assert_eq!(chain, original);
    assert_eq!(chain.append(&first), Err(ChainError::Sequence));
    assert_eq!(chain, original);
    wrong = second;
    wrong.writer_exec_epoch = 6;
    assert_eq!(
        chain.append(&wrong),
        Err(ChainError::ExecutionEpochRollback)
    );
    assert_eq!(chain, original);
    assert_eq!(
        chain.verify_and_append(&second, [0; 32]),
        Err(ChainError::HashMismatch)
    );
    assert_eq!(chain, original);
    let expected = hash(&golden().events[1].logical_hash);
    assert_eq!(chain.verify_and_append(&second, expected), Ok(expected));
    let mut third = record("fee");
    third.writer_exec_epoch += 1;
    assert!(chain.append(&third).is_ok());
}

#[test]
fn journal_sequence_is_distinct_from_intent_sequence_and_economic_dedup() {
    let first = record("prepared");
    let mut chain = LogicalChain::new(first.risk_domain_id, first.journal_epoch);
    chain.append(&first).unwrap(); // intent_seq 9, journal_seq 1
    let second = record("fill");
    let mut repeated_fact = second;
    repeated_fact.journal_seq = 3;
    assert_eq!(business_digest(&second), business_digest(&repeated_fact));
    assert_ne!(
        chain.append(&second).unwrap(),
        chain.append(&repeated_fact).unwrap()
    );
    let mut zero = first;
    zero.journal_seq = 0;
    let mut fresh = LogicalChain::new(first.risk_domain_id, first.journal_epoch);
    assert_eq!(decode(&encode(&zero)), Ok(zero));
    assert_eq!(fresh.append(&zero), Err(ChainError::Sequence));
}

#[test]
fn same_late_fact_keeps_business_digest_across_writer_takeover() {
    let original = record("fill");
    let mut after_takeover = original;
    after_takeover.writer_exec_epoch += 1;
    after_takeover.journal_seq += 1;
    assert_eq!(business_digest(&original), business_digest(&after_takeover));
    assert_ne!(
        logical_payload_hash(&original),
        logical_payload_hash(&after_takeover)
    );
    assert_eq!(decode(&encode(&after_takeover)), Ok(after_takeover));
    assert_eq!(after_takeover.event.order().intent_exec_epoch, 7);
    let first = record("prepared");
    let mut original_chain = LogicalChain::new(first.risk_domain_id, first.journal_epoch);
    original_chain.append(&first).unwrap();
    let mut takeover_chain = original_chain;
    after_takeover.journal_seq = original.journal_seq;
    assert_ne!(
        original_chain.append(&original).unwrap(),
        takeover_chain.append(&after_takeover).unwrap()
    );
}

#[test]
fn genesis_binds_domain_and_epoch_while_business_hash_binds_all_fact_fields() {
    let r = record("prepared");
    let baseline = LogicalChain::new(r.risk_domain_id, r.journal_epoch);
    assert_ne!(
        baseline.hash(),
        LogicalChain::new([42; 16], r.journal_epoch).hash()
    );
    assert_ne!(
        baseline.hash(),
        LogicalChain::new(r.risk_domain_id, [42; 16]).hash()
    );
    for name in ["fill", "fee", "terminal", "aborted"] {
        let original = record(name);
        let mut changed = original;
        changed.event = match original.event {
            Event::Fill {
                order,
                trade_id,
                quantity,
                cost,
            } => Event::Fill {
                order,
                trade_id: trade_id + 1,
                quantity,
                cost,
            },
            Event::Fee {
                order,
                fee_id,
                amount,
            } => Event::Fee {
                order,
                fee_id: fee_id + 1,
                amount,
            },
            Event::Terminal {
                order,
                status: _,
                cumulative,
            } => Event::Terminal {
                order,
                status: TerminalStatus::Expired,
                cumulative,
            },
            Event::AbortedBeforeSend { mut order } => {
                order.intent_id += 1;
                Event::AbortedBeforeSend { order }
            }
            _ => unreachable!(),
        };
        assert_ne!(business_digest(&original), business_digest(&changed));
    }
    for status in [
        TerminalStatus::Filled,
        TerminalStatus::Canceled,
        TerminalStatus::Rejected,
        TerminalStatus::Expired,
    ] {
        let mut r = record("terminal");
        r.event = Event::Terminal {
            order: order(),
            status,
            cumulative: u64::MAX,
        };
        assert_eq!(decode(&encode(&r)), Ok(r));
    }
}
