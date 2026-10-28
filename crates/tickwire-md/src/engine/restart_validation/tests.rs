use super::*;
use crate::{
    expansion::config_hash,
    recovery::{Config as Recovery, Phase},
};
use tickwire_market_source::protocol::wire_bytes;
type Edit<T> = (&'static str, fn(&mut T));

fn fixture() -> (Snapshot, config::Config, Vec<ResourceHeader>) {
    let mut old: config::Config =
        serde_json::from_str(include_str!("../../../examples/md.json")).unwrap();
    old.topics.truncate(1);
    let catalog = (
        old.feeds[0].instrument.catalog_version,
        old.feeds[0].instrument.catalog_hash,
    );
    for feed in &mut old.feeds {
        feed.topics = vec![101];
        feed.instrument.catalog_version = catalog.0;
        feed.instrument.catalog_hash = catalog.1;
    }
    old.feeds[0].events.push(config::EventFeed {
        owner_id: [32; 16],
        stream_id: 99,
        kind: config::EventType::Trade,
        first_connection_epoch: 1,
        first_operation: 1,
        stream_address: "127.0.0.1:443".into(),
        sbe_api_key_file: None,
        fast_mark: false,
    });
    old.socket = "/tmp/md-before/md.sock".into();
    old.admission_directory = "/tmp/md-before/admissions".into();
    old.control_directory = "/tmp/md-before/control".into();
    old.topics[0].resource = "/tmp/md-before/topic.bin".into();
    old.expansion = Some(config::ExpansionControl {
        recovery: Some(Recovery {
            directory: "/tmp/md-history".into(),
            restart_from: None,
        }),
        plan_path: "/tmp/md-before/plan.json".into(),
        result_path: "/tmp/md-before/result.json".into(),
        effective_path: "/tmp/md-before/effective.json".into(),
        consumer_directory: "/tmp/md-before/consumers".into(),
        initial_revision: 1,
    });
    old.quota.consumer_instance = old.identity.instance;
    old.quota.consumer_generation = old.identity.generation;
    let mut spec: config::ResourceSpec =
        serde_json::from_str(include_str!("../../../examples/topic-101.json")).unwrap();
    spec.identity.arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
    spec.catalog_version = catalog.0;
    let mut bank = spec.header().unwrap();
    old.identity.arch = bank.target_arch;
    let previous = Snapshot {
        schema: 1,
        sequence: 4,
        previous: [81; 32],
        phase: Phase::Ready,
        revision: 2,
        config_hash: config_hash(&old).unwrap(),
        config: old.clone(),
        instrument_ids: old.feeds.iter().map(|f| f.instrument.id).collect(),
        reserved_owners: old
            .feeds
            .iter()
            .flat_map(|feed| {
                std::iter::once(feed.owner_id).chain(feed.events.iter().map(|event| event.owner_id))
            })
            .collect(),
        banks: vec![wire_bytes(&bank).unwrap()],
        operations: vec![],
    };
    let mut next = old;
    next.identity.instance = [90; 16];
    next.identity.generation += 1;
    next.quota.consumer_instance = next.identity.instance;
    next.quota.consumer_generation = next.identity.generation;
    next.socket = "/tmp/md-after/md.sock".into();
    next.admission_directory = "/tmp/md-after/admissions".into();
    next.control_directory = "/tmp/md-after/control".into();
    next.topics[0].resource = "/tmp/md-after/topic.bin".into();
    let expansion = next.expansion.as_mut().unwrap();
    expansion.plan_path = "/tmp/md-after/plan.json".into();
    expansion.result_path = "/tmp/md-after/result.json".into();
    expansion.effective_path = "/tmp/md-after/effective.json".into();
    expansion.consumer_directory = "/tmp/md-after/consumers".into();
    expansion.initial_revision = previous.revision;
    expansion.recovery.as_mut().unwrap().restart_from = Some([82; 32]);
    for (i, feed) in next.feeds.iter_mut().enumerate() {
        feed.owner_id = [100 + i as u8; 16];
        for (j, event) in feed.events.iter_mut().enumerate() {
            event.owner_id = [120 + j as u8; 16];
        }
    }
    bank.resource_id = [91; 16];
    bank.channel_id = [92; 16];
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.producer_instance = next.identity.instance;
    bank.producer_generation = next.identity.generation;
    bank.first_seq = 1;
    bank.predecessor_ring_epoch = 0;
    bank.cutover_seq = 0;
    (previous, next, vec![bank])
}

#[test]
fn restart_rejects_the_previous_producer_instance() {
    let (previous, mut next, mut banks) = fixture();
    next.identity.instance = previous.config.identity.instance;
    next.quota.consumer_instance = next.identity.instance;
    banks[0].producer_instance = next.identity.instance;
    assert!(validate(&previous, &next, &banks).is_err());
}

#[test]
fn restart_accepts_explicit_new_identity_and_a_fresh_independent_bank() {
    let (previous, next, banks) = fixture();
    validate(&previous, &next, &banks).unwrap();
}

#[test]
fn restart_rejects_unfinished_or_changed_history_and_revision() {
    let (previous, next, banks) = fixture();
    for phase in [
        Phase::Starting,
        Phase::Reserved,
        Phase::Revoking,
        Phase::Applied,
    ] {
        let mut old = previous.clone();
        old.phase = phase;
        assert!(validate(&old, &next, &banks).is_err(), "{phase:?}");
    }
    let mut old = previous.clone();
    old.config_hash[0] ^= 1;
    assert!(validate(&old, &next, &banks).is_err());
    let mut changed = next.clone();
    changed.expansion.as_mut().unwrap().initial_revision -= 1;
    assert!(validate(&previous, &changed, &banks).is_err());
    let mut changed = next.clone();
    changed
        .expansion
        .as_mut()
        .unwrap()
        .recovery
        .as_mut()
        .unwrap()
        .directory = "/tmp/other-history".into();
    assert!(validate(&previous, &changed, &banks).is_err());
    assert!(validate(&previous, &next, &[]).is_err());
}

#[test]
fn restart_preserves_all_non_launch_configuration_fields() {
    let (previous, next, banks) = fixture();
    let edits: &[Edit<config::Config>] = &[
        ("namespace", |c| c.identity.namespace[0] ^= 1),
        ("build", |c| c.identity.build_hash[0] ^= 1),
        ("qualification", |c| c.identity.qualification_hash[0] ^= 1),
        ("manifest", |c| c.identity.manifest_hash[0] ^= 1),
        ("role", |c| c.identity.role = 3),
        ("logical", |c| c.identity.logical_strategy = 1),
        ("trust", |c| c.identity.trust_profile += 1),
        ("quota directory", |c| {
            c.quota_directory = "/tmp/new-quota".into()
        }),
        ("control stream", |c| c.control_stream[0] ^= 1),
        ("control domain", |c| c.control_domain[0] ^= 1),
        ("writer epoch", |c| c.control_writer_epoch += 1),
        ("idle", |c| c.maximum_idle_ns += 1),
        ("feed source", |c| {
            c.feeds[0].source = tickwire_binance_quota::bootstrap::Source::UsdmProduction
        }),
        ("feed stream", |c| c.feeds[0].stream_id += 100),
        ("feed network", |c| {
            c.feeds[0].stream_address = "127.0.0.1:444".into()
        }),
        ("snapshot network", |c| {
            c.feeds[0].snapshot_address = "127.0.0.1:444".into()
        }),
        ("feed trust", |c| {
            c.feeds[0].trust_anchors.push("/tmp/new-root.der".into())
        }),
        ("snapshot limit", |c| c.feeds[0].snapshot_limit += 1),
        ("price", |c| {
            c.feeds[0].instrument.price_tick = "0.02".into()
        }),
        ("catalog", |c| c.feeds[0].instrument.catalog_hash[0] ^= 1),
        ("asset", |c| c.feeds[0].instrument.base.exponent += 1),
        ("topic membership", |c| c.feeds[0].topics.push(201)),
        ("event stream", |c| c.feeds[0].events[0].stream_id += 1),
        ("event network", |c| {
            c.feeds[0].events[0].stream_address = "127.0.0.1:444".into()
        }),
        ("event kind", |c| {
            c.feeds[0].events[0].kind = config::EventType::BestBidAsk
        }),
        ("topic set", |c| c.topics[0].instrument_set_version += 1),
        ("topic levels", |c| c.topics[0].maximum_levels += 1),
        ("topic cache", |c| c.topics[0].maximum_cache_bytes += 1),
        ("topic snapshot", |c| {
            c.topics[0].maximum_snapshot_bytes += 1
        }),
        ("topic delta", |c| c.topics[0].maximum_delta_bytes += 1),
    ];
    for (name, edit) in edits {
        let mut changed = next.clone();
        edit(&mut changed);
        assert!(
            validate(&previous, &changed, &banks).is_err(),
            "accepted {name}"
        );
    }
}

#[test]
fn restart_clock_nonce_and_renewed_quota_rules_are_explicit() {
    let (previous, mut next, mut banks) = fixture();
    next.clock_generation += 1;
    next.feeds[0].first_connection_epoch += 20;
    next.feeds[0].first_operation += 30;
    next.feeds[0].initial_book_generation += 40;
    next.feeds[0].events[0].first_connection_epoch += 10;
    next.feeds[0].events[0].first_operation += 10;
    next.quota.supervisor_generation += 1;
    next.quota.consumer_lease_until_ns += 10;
    next.quota.policy_generation += 1;
    for grant in &mut next.quota.grants {
        grant.request_id += 100;
    }
    banks[0].channel_epoch += 1;
    banks[0].initial_membership_version += 1;
    banks[0].created_mono_ns += 1;
    validate(&previous, &next, &banks).unwrap();
    next.clock_generation = 0;
    assert!(validate(&previous, &next, &banks).is_err());
    next.clock_generation = previous.config.clock_generation;
    next.identity.host_boot = [83; 16];
    banks[0].host_boot_id = next.identity.host_boot;
    next.clock_generation = 1;
    validate(&previous, &next, &banks).unwrap();
    next.quota.consumer_generation -= 1;
    assert!(validate(&previous, &next, &banks).is_err());
}

#[test]
fn restart_rejects_nonzero_clock_rollback_in_the_same_host_boot() {
    let (mut previous, mut next, banks) = fixture();
    previous.config.clock_generation = 3;
    previous.config_hash = config_hash(&previous.config).unwrap();
    next.clock_generation = 2;
    assert!(validate(&previous, &next, &banks).is_err());
    next.clock_generation = 3;
    validate(&previous, &next, &banks).unwrap();
}

#[test]
fn restart_rejects_zero_repeated_and_previous_depth_or_event_owners() {
    let (previous, next, banks) = fixture();
    for id in [
        [0; 16],
        previous.config.feeds[1].owner_id,
        previous.config.feeds[0].events[0].owner_id,
        next.feeds[1].owner_id,
    ] {
        let mut changed = next.clone();
        changed.feeds[0].owner_id = id;
        assert!(validate(&previous, &changed, &banks).is_err());
    }
    let mut changed = next.clone();
    changed.feeds[0].events[0].owner_id = changed.feeds[0].owner_id;
    assert!(validate(&previous, &changed, &banks).is_err());
}

#[test]
fn restart_rejects_reused_paths_and_nested_old_control_locations() {
    let (previous, next, banks) = fixture();
    let edits: &[fn(&mut config::Config)] = &[
        |c| c.socket = "/tmp/md-before/md.sock".into(),
        |c| c.socket = "/tmp/md-before/admissions/new.sock".into(),
        |c| c.admission_directory = "/tmp/md-before/admissions/child".into(),
        |c| c.control_directory = "/tmp/md-before".into(),
        |c| c.topics[0].resource = "/tmp/md-before/topic.bin".into(),
        |c| c.expansion.as_mut().unwrap().plan_path = "/tmp/md-before/result.json".into(),
        |c| c.expansion.as_mut().unwrap().effective_path = "/tmp/md-history/effective.json".into(),
        |c| c.expansion.as_mut().unwrap().consumer_directory = "/tmp/md-before/consumers".into(),
        |c| c.control_directory = c.admission_directory.clone(),
        |c| c.socket = "/tmp/md-after/../md-before/md.sock".into(),
        |c| c.socket = "relative.sock".into(),
    ];
    for edit in edits {
        let mut changed = next.clone();
        edit(&mut changed);
        assert!(validate(&previous, &changed, &banks).is_err());
    }
}

#[test]
fn restart_binds_fresh_bank_geometry_stream_and_independent_frontier() {
    let (previous, next, banks) = fixture();
    let old = ResourceHeader::decode(&previous.banks[0]).unwrap();
    let edits: &[Edit<ResourceHeader>] = &[
        ("resource zero", |h| h.resource_id = [0; 16]),
        ("channel zero", |h| h.channel_id = [0; 16]),
        ("producer", |h| h.producer_instance = [7; 16]),
        ("generation", |h| h.producer_generation += 1),
        ("host", |h| h.host_boot_id = [7; 16]),
        ("topic", |h| h.topic_id += 1),
        ("catalog", |h| h.catalog_version += 1),
        ("venue", |h| h.venue += 1),
        ("product", |h| h.product += 1),
        ("event class", |h| h.event_class += 1),
        ("shard", |h| h.feed_shard += 1),
        ("features", |h| h.optional_features ^= 1),
        ("consumers", |h| h.max_consumers -= 1),
        ("qos", |h| h.qos += 1),
        ("sequence", |h| {
            h.first_seq = 5;
            h.cutover_seq = 4;
            h.predecessor_ring_epoch = h.ring_epoch - 1;
        }),
        ("descriptor", |h| h.descriptor_hash = [17; 32]),
    ];
    for (name, edit) in edits {
        let mut changed = banks.clone();
        edit(&mut changed[0]);
        assert!(
            validate(&previous, &next, &changed).is_err(),
            "accepted {name}"
        );
    }
    for field in [0, 1, 2, 3] {
        let mut changed = banks.clone();
        match field {
            0 => changed[0].resource_id = old.resource_id,
            1 => changed[0].channel_id = old.channel_id,
            2 => changed[0].ring_epoch = old.ring_epoch,
            _ => changed[0].feed_generation = old.feed_generation,
        }
        assert!(validate(&previous, &next, &changed).is_err());
    }
    let mut actual = banks;
    actual[0].descriptor_hash = Sha256::digest(wire_bytes(&actual[0]).unwrap()).into();
    validate(&previous, &next, &actual).unwrap();
}

#[test]
fn restart_rejects_cross_topic_identity_reuse_and_bank_reordering() {
    let (mut previous, mut next, mut banks) = fixture();
    let mut old_topic = previous.config.topics[0].clone();
    old_topic.resource = "/tmp/md-before/second.bin".into();
    previous.config.topics.push(old_topic.clone());
    old_topic.resource = "/tmp/md-after/second.bin".into();
    next.topics.push(old_topic);
    for feed in &mut previous.config.feeds {
        feed.topics.push(102);
    }
    for feed in &mut next.feeds {
        feed.topics.push(102);
    }
    previous.config_hash = config_hash(&previous.config).unwrap();
    let mut old_bank = ResourceHeader::decode(&previous.banks[0]).unwrap();
    old_bank.resource_id = [71; 16];
    old_bank.channel_id = [72; 16];
    old_bank.topic_id = 102;
    previous.banks.push(wire_bytes(&old_bank).unwrap());
    let mut bank = banks[0].clone();
    bank.resource_id = [73; 16];
    bank.channel_id = [74; 16];
    bank.topic_id = 102;
    banks.push(bank);
    validate(&previous, &next, &banks).unwrap();
    let mut wrong = banks.clone();
    wrong[0].resource_id = old_bank.resource_id;
    assert!(validate(&previous, &next, &wrong).is_err());
    wrong = banks.clone();
    wrong[0].channel_id = old_bank.channel_id;
    assert!(validate(&previous, &next, &wrong).is_err());
    wrong = banks.clone();
    wrong[1].resource_id = wrong[0].resource_id;
    assert!(validate(&previous, &next, &wrong).is_err());
    wrong = banks.clone();
    wrong[1].channel_id = wrong[0].channel_id;
    assert!(validate(&previous, &next, &wrong).is_err());
    banks.swap(0, 1);
    assert!(validate(&previous, &next, &banks).is_err());
}
