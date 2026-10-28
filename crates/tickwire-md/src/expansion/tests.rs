use super::*;
use crate::config;
use tickwire_market_source::protocol::wire_bytes;

fn fixture() -> (Config, FeedIndex, ResourceHeader, Plan) {
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    config.feeds.truncate(1);
    config.topics.truncate(1);
    config.feeds[0].topics = vec![101];
    let mut spec: config::ResourceSpec =
        serde_json::from_str(include_str!("../../examples/topic-101.json")).unwrap();
    spec.identity.arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
    let bank = spec.header().unwrap();
    config.identity = spec.identity;
    let index = FeedIndex::new(&config.feeds).unwrap();
    let mut target = config.clone();
    let mut feed = target.feeds[0].clone();
    feed.owner_id = [80; 16];
    feed.instrument.id = u64::MAX;
    feed.instrument.symbol = "ETHUSDT".into();
    feed.instrument.base.id = 3;
    feed.instrument.base.symbol = "ETH".into();
    feed.stream_id = 9;
    target.feeds.push(feed);
    target.topics[0].instrument_set_version += 1;
    let mut next = bank.clone();
    next.resource_id = [81; 16];
    next.ring_epoch += 1;
    next.feed_generation += 1;
    next.predecessor_ring_epoch = bank.ring_epoch;
    next.first_seq = 1;
    next.cutover_seq = 0;
    next.descriptor_hash = [0; 32];
    let plan = Plan {
        schema: 1,
        operation_id: [82; 16],
        base_revision: 1,
        base_config_hash: config_hash(&config).unwrap(),
        base_bank: wire_bytes(&bank).unwrap(),
        target_revision: 2,
        target_bank: wire_bytes(&next).unwrap(),
        target_config: target,
        topic_id: 101,
        consumers: vec![],
        warmup_deadline_mono_ns: 10_000,
        maximum_age_ns: 1000,
        peak_cache_bytes: usize::MAX,
        maximum_retained_bank_bytes: bank.resource_bytes * 2,
    };
    (config, index, bank, plan)
}

#[test]
fn approved_append_prepares_an_independent_stable_index_and_full_target() {
    let (config, index, bank, plan) = fixture();
    let encoded = serde_json::to_vec(&plan).unwrap();
    let ready = Plan::decode(&encoded)
        .unwrap()
        .prepare(&config, &index, 1, &bank, 0, &[], 0, 100)
        .unwrap();
    assert_eq!(ready.added_from, 1);
    assert_eq!(ready.topic_index, 0);
    assert_eq!(
        ready.approval_digest,
        <[u8; 32]>::from(Sha256::digest(serde_json::to_vec(&ready.plan).unwrap()))
    );
    assert_eq!(
        ready
            .index
            .book_key(&ready.plan.target_config.feeds[0])
            .unwrap(),
        index.book_key(&config.feeds[0]).unwrap()
    );
    assert_eq!(
        ready
            .index
            .book_key(&ready.plan.target_config.feeds[1])
            .unwrap()
            .instrument,
        1
    );
    assert!(index.book_key(&ready.plan.target_config.feeds[1]).is_err());
}

#[test]
fn actual_bank_accepts_zero_digest_startup_admission_without_rewriting_approval() {
    let (config, index, mut bank, mut plan) = fixture();
    bank.descriptor_hash = Sha256::digest(wire_bytes(&bank).unwrap()).into();
    plan.base_bank = wire_bytes(&bank).unwrap();
    let mut previous = with_consumer(&config, &mut plan);
    let mut template = bank.clone();
    template.descriptor_hash = [0; 32];
    previous.client.bank_header = wire_bytes(&template).unwrap();
    plan.consumers[0].previous = previous.clone();
    let ready = plan
        .clone()
        .prepare(
            &config,
            &index,
            1,
            &bank,
            0,
            std::slice::from_ref(&previous),
            0,
            100,
        )
        .unwrap();
    assert_eq!(
        ready.plan().consumers[0].previous.client.bank_header,
        wire_bytes(&template).unwrap()
    );
    template.descriptor_hash = [99; 32];
    previous.client.bank_header = wire_bytes(&template).unwrap();
    plan.consumers[0].previous = previous.clone();
    assert!(
        plan.prepare(&config, &index, 1, &bank, 0, &[previous], 0, 100)
            .is_err()
    );
}

#[test]
fn wrong_base_or_changed_old_feed_is_rejected_before_candidate_installation() {
    let (config, index, bank, plan) = fixture();
    for case in 0..8 {
        let mut bad = plan.clone();
        match case {
            0 => bad.base_config_hash[0] ^= 1,
            1 => bad.base_revision += 1,
            2 => bad.target_revision += 1,
            3 => bad.target_config.feeds[0].instrument.price_tick = "0.02".into(),
            4 => {
                bad.target_config.feeds.remove(0);
            }
            5 => bad.target_config.feeds[1].instrument.catalog_hash[0] ^= 1,
            6 => bad.target_config.feeds[1].owner_id = config.feeds[0].owner_id,
            _ => bad.warmup_deadline_mono_ns = 100,
        };
        assert!(
            bad.prepare(&config, &index, 1, &bank, 0, &[], 0, 100)
                .is_err(),
            "case {case}"
        );
    }
    assert_eq!(index.book_key(&config.feeds[0]).unwrap().instrument, 0);
}

#[test]
fn bank_identity_geometry_budget_and_private_plan_encoding_are_bounded() {
    use tickwire_ipc::Wire;
    let (config, index, bank, plan) = fixture();
    for case in 0..7 {
        let mut bad = plan.clone();
        let mut target = ResourceHeader::decode(&bad.target_bank).unwrap();
        match case {
            0 => target.channel_epoch += 1,
            1 => target.resource_id = bank.resource_id,
            2 => target.ring_epoch = bank.ring_epoch,
            3 => target.first_seq = 2,
            4 => target.catalog_version += 1,
            5 => bad.maximum_retained_bank_bytes -= 1,
            _ => bad.peak_cache_bytes = 1,
        }
        bad.target_bank = wire_bytes(&target).unwrap();
        assert!(
            bad.prepare(&config, &index, 1, &bank, 0, &[], 0, 100)
                .is_err(),
            "case {case}"
        );
    }
    assert!(Plan::decode(&[]).is_err());
    assert!(Plan::decode(&vec![b' '; MAX_PLAN_BYTES + 1]).is_err());
    let mut json = serde_json::to_value(&plan).unwrap();
    json.as_object_mut()
        .unwrap()
        .insert("authorization".into(), true.into());
    assert!(Plan::decode(&serde_json::to_vec(&json).unwrap()).is_err());
}

fn with_consumer(config: &Config, plan: &mut Plan) -> Admission {
    let template: config::ClientTemplate =
        serde_json::from_str(include_str!("../../examples/exec-client.json")).unwrap();
    let mut endpoint = template.endpoint;
    endpoint.path = config.socket.clone();
    endpoint.peer = config.identity.clone();
    endpoint.local.host_boot = config.identity.host_boot;
    endpoint.local.namespace = config.identity.namespace;
    let client = tickwire_market_source::ConnectConfig {
        endpoint,
        bank_header: plan.base_bank.clone(),
        catalog_hash: config.feeds[0].instrument.catalog_hash,
        instrument_set_version: config.topics[0].instrument_set_version,
        instruments: instruments(config, plan.topic_id).unwrap(),
        cursor_id: template.cursor_id,
        subscription_generation: template.subscription_generation,
        request_id: template.request_id,
        maximum_levels: config.topics[0].maximum_levels,
        startup_timeout_ms: template.startup_timeout_ms,
        clock_generation: config.clock_generation,
    };
    let mut supervisor = config.identity.clone();
    supervisor.instance = [83; 16];
    supervisor.role = 1;
    let previous = Admission {
        client,
        pid: 123,
        uid: 1000,
        gid: 1000,
        supervisor,
        supervisor_pid: 120,
        snapshot_id: [84; 16],
    };
    let mut next = previous.clone();
    next.client.bank_header = plan.target_bank.clone();
    next.client.instrument_set_version = plan.target_config.topics[0].instrument_set_version;
    next.client.instruments = instruments(&plan.target_config, plan.topic_id).unwrap();
    next.client.cursor_id = [85; 16];
    next.client.request_id = [86; 16];
    next.client.subscription_generation += 1;
    next.client.endpoint.control_epoch += 1;
    next.client.endpoint.nonce = [87; 32];
    next.snapshot_id = [88; 16];
    plan.consumers.push(Consumer {
        previous: previous.clone(),
        next,
    });
    previous
}

#[test]
fn consumer_plan_binds_actual_principal_complete_membership_and_fresh_join_identity() {
    let (config, index, bank, mut plan) = fixture();
    let active = with_consumer(&config, &mut plan);
    let admissions = [active];
    assert!(
        plan.clone()
            .prepare(&config, &index, 1, &bank, 0, &admissions, 0, 100)
            .is_ok()
    );
    for case in 0..19 {
        let mut bad = plan.clone();
        let entry = &mut bad.consumers[0];
        match case {
            0 => entry.previous.pid += 1,
            1 => entry.next.pid += 1,
            2 => entry.next.client.endpoint.local.instance[0] ^= 1,
            3 => entry.next.client.endpoint.peer_pid += 1,
            4 => entry.next.client.instruments.pop().map(|_| ()).unwrap(),
            5 => entry.next.client.cursor_id = entry.previous.client.cursor_id,
            6 => entry.next.client.request_id = entry.previous.client.request_id,
            7 => {
                entry.next.client.subscription_generation =
                    entry.previous.client.subscription_generation
            }
            8 => entry.next.snapshot_id = entry.previous.snapshot_id,
            9 => {
                entry.next.client.endpoint.control_epoch =
                    entry.previous.client.endpoint.control_epoch
            }
            10 => entry.next.client.endpoint.nonce = entry.previous.client.endpoint.nonce,
            11 => entry.next.client.catalog_hash[0] ^= 1,
            12 => entry.next.client.clock_generation += 1,
            13 => entry.next.client.bank_header[0] ^= 1,
            14 => entry.next.snapshot_id = entry.next.client.cursor_id,
            15 => {
                entry.next.snapshot_id = ResourceHeader::decode(&entry.next.client.bank_header)
                    .unwrap()
                    .resource_id
            }
            16 => {
                entry.next.client.cursor_id = ResourceHeader::decode(&entry.next.client.bank_header)
                    .unwrap()
                    .resource_id
            }
            17 => entry.next.client.cursor_id = entry.previous.snapshot_id,
            _ => entry.next.snapshot_id = entry.previous.client.cursor_id,
        }
        assert!(
            bad.prepare(&config, &index, 1, &bank, 0, &admissions, 0, 100)
                .is_err(),
            "case {case}"
        );
    }
    assert!(
        plan.prepare(&config, &index, 1, &bank, 0, &[], 0, 100)
            .is_err()
    );
}

#[test]
fn final_descriptors_bind_the_actual_cut_without_mutating_the_approved_template() {
    let (config, index, bank, mut plan) = fixture();
    let admission = with_consumer(&config, &mut plan);
    let prepared = plan
        .prepare(&config, &index, 1, &bank, 0, &[admission], 0, 100)
        .unwrap();
    let approval = prepared.approval_digest;
    let finalized = prepared.finalize(71, 200).unwrap();
    assert_eq!(finalized.bank.cutover_seq, 71);
    assert_eq!(finalized.bank.first_seq, 72);
    assert_eq!(finalized.bank.created_mono_ns, 200);
    let mut zero = finalized.bank.clone();
    zero.descriptor_hash = [0; 32];
    assert_eq!(
        finalized.bank.descriptor_hash,
        <[u8; 32]>::from(Sha256::digest(wire_bytes(&zero).unwrap()))
    );
    assert_eq!(
        finalized.admissions[0].client.bank_header,
        wire_bytes(&finalized.bank).unwrap()
    );
    assert_eq!(prepared.bank_template.cutover_seq, 0);
    assert_eq!(prepared.approval_digest, approval);
    assert!(prepared.finalize(u64::MAX, 200).is_err());
    assert!(
        prepared
            .finalize(71, prepared.plan.warmup_deadline_mono_ns)
            .is_err()
    );
}

#[test]
fn reserved_sequence_bits_and_invalid_future_handshakes_are_rejected_before_quiescence() {
    let (config, index, bank, mut plan) = fixture();
    let admission = with_consumer(&config, &mut plan);
    for case in 0..7 {
        let mut bad = plan.clone();
        match case {
            0 | 1 => {
                let mut header = ResourceHeader::decode(&bad.target_bank).unwrap();
                if case == 0 {
                    header.ring_epoch = 1 << 63;
                } else {
                    header.feed_generation = 1 << 63;
                }
                bad.target_bank = wire_bytes(&header).unwrap();
                bad.consumers[0]
                    .next
                    .client
                    .bank_header
                    .clone_from(&bad.target_bank);
            }
            2 => {
                bad.target_config.topics[0].instrument_set_version = 1 << 63;
                bad.consumers[0].next.client.instrument_set_version = 1 << 63;
            }
            3 => bad.consumers[0].next.client.subscription_generation = 1 << 63,
            4 => bad.consumers[0].next.client.endpoint.lease_duration_ns = 0,
            5 => bad.consumers[0].next.client.endpoint.max_frame_bytes = 1,
            _ => bad.consumers[0].next.client.endpoint.supported_features = 0,
        }
        assert!(
            bad.prepare(
                &config,
                &index,
                1,
                &bank,
                0,
                std::slice::from_ref(&admission),
                0,
                100
            )
            .is_err(),
            "case {case}"
        );
    }
    let prepared = plan
        .prepare(&config, &index, 1, &bank, 0, &[admission], 0, 100)
        .unwrap();
    assert!(prepared.finalize((1 << 63) - 1, 200).is_err());
    assert!(prepared.finalize(71, 99).is_err());
}

#[test]
fn consumer_depth_capacity_is_preserved_and_covers_the_target_topic() {
    let (config, index, bank, mut plan) = fixture();
    let mut admission = with_consumer(&config, &mut plan);
    admission.client.maximum_levels = 1000;
    plan.consumers[0].previous = admission.clone();
    assert!(
        plan.clone()
            .prepare(
                &config,
                &index,
                1,
                &bank,
                0,
                std::slice::from_ref(&admission),
                0,
                100
            )
            .is_err()
    );
    plan.consumers[0].next.client.maximum_levels = 1000;
    assert!(
        plan.clone()
            .prepare(
                &config,
                &index,
                1,
                &bank,
                0,
                std::slice::from_ref(&admission),
                0,
                100
            )
            .is_err()
    );
    admission.client.maximum_levels = 10000;
    plan.consumers[0].previous = admission.clone();
    plan.consumers[0].next.client.maximum_levels = 10000;
    assert!(
        plan.prepare(&config, &index, 1, &bank, 0, &[admission], 0, 100)
            .is_ok()
    );
}
