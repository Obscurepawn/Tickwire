use serde_json::Value;
use sha2::{Digest, Sha256};
use tickwire_ipc::*;
use tickwire_snapshot::*;

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
struct Fixture {
    raw: Vec<u8>,
    expected: ResourceHeader,
    join: JoinBody,
    instruments: Vec<InstrumentSpec>,
}
impl Fixture {
    fn new() -> Self {
        let f: Value =
            serde_json::from_str(include_str!("../../../specs/contracts/ipc-vectors-v1.json"))
                .unwrap();
        let bytes = |name| {
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
                .collect::<Vec<_>>()
        };
        let raw = bytes("normalized_topic_snapshot");
        Self {
            expected: ResourceHeader::decode(&raw[..4096]).unwrap(),
            raw,
            join: JoinBody::decode(&bytes("join_normalized_snapshot")).unwrap(),
            instruments: vec![InstrumentSpec {
                instrument_id: 1001,
                instrument_version: 5,
                required_states: QUOTE | TRADE | MARK_FUNDING,
            }],
        }
    }
    fn context(&self, now: u64) -> JoinContext<'_> {
        JoinContext {
            expected_resource: &self.expected,
            catalog_hash: [0x22; 32],
            instruments: &self.instruments,
            join: &self.join,
            now_mono_ns: now,
        }
    }
    // Mutants remain fully hashed and structurally valid to isolate business checks.
    fn rehash(&mut self) {
        let mut r = ResourceHeader::decode(&self.raw[..4096]).unwrap();
        r.descriptor_hash = [0; 32];
        r.encode(&mut self.raw[..4096]).unwrap();
        r.descriptor_hash = hash(&self.raw[..4096]);
        r.encode(&mut self.raw[..4096]).unwrap();
        self.expected = r;
        let mut top = SnapshotHeader::decode(&self.raw[4096..4352]).unwrap();
        for i in 0..top.entry_count as usize {
            let start = 4352 + i * 128;
            let mut entry = TopicSnapshotEntry::decode(&self.raw[start..start + 128]).unwrap();
            let off = entry.snapshot_offset as usize;
            let mut child = SnapshotHeader::decode(&self.raw[off..off + 256]).unwrap();
            for section in 0..2 {
                let p = off + 256 + section * 64;
                let mut s = SnapshotSection::decode(&self.raw[p..p + 64]).unwrap();
                s.entries_hash = hash(&self.raw[s.offset as usize..(s.offset + s.length) as usize]);
                s.encode(&mut self.raw[p..p + 64]).unwrap();
            }
            child.entries_hash = hash(&self.raw[off + 256..off + 384]);
            child.encode(&mut self.raw[off..off + 256]).unwrap();
            entry.snapshot_hash = hash(&self.raw[off..off + entry.snapshot_bytes as usize]);
            entry.encode(&mut self.raw[start..start + 128]).unwrap();
        }
        top.entries_hash = hash(&self.raw[4352..4352 + top.entry_count as usize * 128]);
        top.encode(&mut self.raw[4096..4352]).unwrap();
        self.join.snapshot_hash = hash(&self.raw);
    }
    fn structural_check(&self) {
        let pairs = self
            .instruments
            .iter()
            .map(|i| (i.instrument_id, i.instrument_version))
            .collect::<Vec<_>>();
        validate_topic_bundle(
            &self.raw,
            &TopicSnapshotContext {
                expected_instruments: &pairs,
                expected_instrument_set_hash: self.join.topic_instrument_set_hash,
                expected_instrument_set_version: self.join.topic_instrument_set_version,
                boundary_sequence: self.join.snapshot_seq,
                join: &self.join,
            },
        )
        .unwrap();
    }
}

#[test]
fn golden_normalized_bundle_has_checked_members_and_deadline() {
    let f = Fixture::new();
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.bytes(), f.raw);
    assert_eq!(v.header().sequence, 1000);
    assert_eq!(v.len(), 1);
    assert!(!v.is_empty());
    let m = v.member(0).unwrap();
    assert_eq!(m.entry.last_instrument_event_seq, 990);
    assert_eq!(m.state_records.len(), 4 * 512);
    assert_eq!(m.depth_levels.len(), 2 * 16);
    assert_eq!(m.header.coverage_flags, 3);
    assert_eq!(v.member(1).unwrap_err(), Error::Bounds);
    assert_eq!(v.check_readiness(1_500_000_000), Ok(()));
    assert_eq!(v.check_readiness(1_499_999_999), Err(Error::Deadline));
    assert_eq!(v.check_readiness(2_000_000_000), Err(Error::Deadline));
}

#[test]
fn resource_join_and_top_identity_conflicts_are_rejected_despite_valid_hashes() {
    for case in 0..10 {
        let mut f = Fixture::new();
        match case {
            0 => f.join.channel_epoch += 1,
            1 => f.join.ring_epoch += 1,
            2 => f.join.feed_generation += 1,
            3 => f.join.expected_catalog_version += 1,
            4 => f.join.membership_version += 1,
            5 => f.join.topic_id += 1,
            6 => f.join.qos = 2,
            7 => f.join.channel_id = [9; 16],
            8 => {
                let mut r = f.expected.clone();
                r.channel_id = [9; 16];
                r.encode(&mut f.raw[..4096]).unwrap();
            }
            _ => {
                let mut r = f.expected.clone();
                r.resource_id = [9; 16];
                r.encode(&mut f.raw[..4096]).unwrap();
            }
        }
        f.rehash();
        f.structural_check();
        assert_eq!(
            validate(&f.raw, &f.context(1_500_000_000)).unwrap_err(),
            Error::Identity,
            "case {case}"
        );
    }
}

#[test]
fn creation_child_expiry_and_join_deadline_are_enforced() {
    for case in 0..4 {
        let mut f = Fixture::new();
        match case {
            0 => {}
            1 => f.join.readiness_deadline_mono_ns = 1_500_000_000,
            2 => {
                let mut h = SnapshotHeader::decode(&f.raw[8192..8448]).unwrap();
                h.expires_mono_ns = 1_500_000_000;
                h.encode(&mut f.raw[8192..8448]).unwrap();
            }
            _ => {
                let mut h = SnapshotHeader::decode(&f.raw[8192..8448]).unwrap();
                h.created_mono_ns = 1_500_000_001;
                h.encode(&mut f.raw[8192..8448]).unwrap();
            }
        }
        f.rehash();
        f.structural_check();
        let now = if case == 0 {
            999_999_999
        } else {
            1_500_000_000
        };
        assert_eq!(
            validate(&f.raw, &f.context(now)).unwrap_err(),
            Error::Deadline
        );
    }
}

#[test]
fn records_bind_topic_producer_and_manifest_feed_state() {
    for case in 0..3 {
        let mut f = Fixture::new();
        let off = if case == 2 { 10112 } else { 8576 };
        let mut r = MarketRecord::decode(&f.raw[off..off + 512]).unwrap();
        match case {
            0 => r.header.topic_or_scope_id += 1,
            1 => r.header.producer_generation += 1,
            _ => r.body.feed_state = 3,
        }
        r.encode(&mut f.raw[off..off + 512]).unwrap();
        f.rehash();
        f.structural_check();
        assert_eq!(
            validate(&f.raw, &f.context(1_500_000_000)).unwrap_err(),
            Error::Identity
        );
    }
}

#[test]
fn nonlive_member_remains_present_but_cannot_be_ready() {
    let mut f = Fixture::new();
    let mut e = TopicSnapshotEntry::decode(&f.raw[4352..4480]).unwrap();
    e.feed_state = 3;
    e.encode(&mut f.raw[4352..4480]).unwrap();
    let mut r = MarketRecord::decode(&f.raw[10112..10624]).unwrap();
    r.body.feed_state = 3;
    r.encode(&mut f.raw[10112..10624]).unwrap();
    f.rehash();
    f.structural_check();
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v.member(0).unwrap().entry.feed_state, 3);
    assert_eq!(v.check_readiness(1_500_000_000), Err(Error::NotReady));
}

#[test]
fn unknown_capability_catalog_and_trusted_identity_are_rejected() {
    let f = Fixture::new();
    let mut wrong = f.expected.clone();
    wrong.namespace_id = [8; 16];
    let mut c = f.context(1_500_000_000);
    c.expected_resource = &wrong;
    assert_eq!(validate(&f.raw, &c).unwrap_err(), Error::Identity);
    let mut c = f.context(1_500_000_000);
    c.catalog_hash = [9; 32];
    assert_eq!(validate(&f.raw, &c).unwrap_err(), Error::Identity);
    let bad = [InstrumentSpec {
        required_states: 8,
        ..f.instruments[0]
    }];
    let mut c = f.context(1_500_000_000);
    c.instruments = &bad;
    assert_eq!(validate(&f.raw, &c).unwrap_err(), Error::UnsupportedFeature);
}

#[test]
fn every_truncated_bundle_is_rejected_without_panicking() {
    let f = Fixture::new();
    for len in 0..f.raw.len() {
        assert!(
            validate(&f.raw[..len], &f.context(1_500_000_000)).is_err(),
            "length {len}"
        );
    }
}

#[test]
fn missing_required_state_blocks_readiness_but_preserves_the_member() {
    let mut f = Fixture::new();
    // Remove MarkFunding, keep FeedState as row three and move depth with it.
    f.raw.copy_within(10112..10624, 9600);
    f.raw.copy_within(10624..10656, 10112);
    f.raw[10144..].fill(0);
    let mut feed = MarketRecord::decode(&f.raw[9600..10112]).unwrap();
    feed.header.sequence = 3;
    feed.encode(&mut f.raw[9600..10112]).unwrap();
    let mut state = SnapshotSection::decode(&f.raw[8448..8512]).unwrap();
    state.entry_count = 3;
    state.length = 3 * 512;
    state.encode(&mut f.raw[8448..8512]).unwrap();
    let mut depth = SnapshotSection::decode(&f.raw[8512..8576]).unwrap();
    depth.offset = 10112;
    depth.encode(&mut f.raw[8512..8576]).unwrap();
    f.rehash();
    f.structural_check();
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.member(0).unwrap().state_records.len(), 3 * 512);
    assert_eq!(v.check_readiness(1_500_000_000), Err(Error::NotReady));
    f.instruments[0].required_states = QUOTE | TRADE;
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.check_readiness(1_500_000_000), Ok(()));
}

fn two_members() -> Fixture {
    let mut f = Fixture::new();
    let second = f.raw[8192..12288].to_vec();
    f.raw.extend_from_slice(&second);
    let mut child = SnapshotHeader::decode(&f.raw[12288..12544]).unwrap();
    child.snapshot_id = [33; 16];
    child.instrument_id = 1002;
    child.instrument_version = 6;
    child.entry_offset += 4096;
    child.encode(&mut f.raw[12288..12544]).unwrap();
    for offset in [12544, 12608] {
        let mut section = SnapshotSection::decode(&f.raw[offset..offset + 64]).unwrap();
        section.offset += 4096;
        section.encode(&mut f.raw[offset..offset + 64]).unwrap();
    }
    for offset in [12672, 13184, 13696, 14208] {
        let mut record = MarketRecord::decode(&f.raw[offset..offset + 512]).unwrap();
        record.header.instrument_id = 1002;
        record.header.instrument_version = 6;
        record.encode(&mut f.raw[offset..offset + 512]).unwrap();
    }
    let mut entry = TopicSnapshotEntry::decode(&f.raw[4352..4480]).unwrap();
    entry.instrument_id = 1002;
    entry.instrument_version = 6;
    entry.snapshot_id = child.snapshot_id;
    entry.snapshot_offset = 12288;
    entry.encode(&mut f.raw[4480..4608]).unwrap();
    f.instruments.push(InstrumentSpec {
        instrument_id: 1002,
        instrument_version: 6,
        required_states: QUOTE | TRADE | MARK_FUNDING,
    });
    let mut hasher = Sha256::new();
    for i in &f.instruments {
        hasher.update(i.instrument_id.to_le_bytes());
        hasher.update(i.instrument_version.to_le_bytes());
    }
    let set_hash = hasher.finalize().into();
    let mut top = SnapshotHeader::decode(&f.raw[4096..4352]).unwrap();
    top.entry_count = 2;
    top.object_bytes = f.raw.len() as u64;
    top.manifest_member_bytes = 8192;
    top.topic_instrument_set_hash = set_hash;
    top.encode(&mut f.raw[4096..4352]).unwrap();
    let mut resource = f.expected.clone();
    resource.capacity = 2;
    resource.resource_bytes = f.raw.len() as u64;
    resource.encode(&mut f.raw[..4096]).unwrap();
    f.join.topic_instrument_set_hash = set_hash;
    f.rehash();
    f.structural_check();
    f
}

#[test]
fn complete_multi_instrument_cut_requires_every_member() {
    let mut f = two_members();
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(v.member(0).unwrap().header.sequence, 1000);
    assert_eq!(v.member(1).unwrap().header.sequence, 1000);
    assert_eq!(v.member(1).unwrap().entry.instrument_id, 1002);
    assert_eq!(v.check_readiness(1_500_000_000), Ok(()));
    f.instruments.pop();
    assert_eq!(
        validate(&f.raw, &f.context(1_500_000_000)).unwrap_err(),
        Error::ResyncRequired
    );
}

#[test]
fn duplicate_member_snapshot_identity_is_not_a_valid_baseline() {
    let mut f = two_members();
    let first = TopicSnapshotEntry::decode(&f.raw[4352..4480]).unwrap();
    let mut second = TopicSnapshotEntry::decode(&f.raw[4480..4608]).unwrap();
    second.snapshot_id = first.snapshot_id;
    second.encode(&mut f.raw[4480..4608]).unwrap();
    let mut child = SnapshotHeader::decode(&f.raw[12288..12544]).unwrap();
    child.snapshot_id = first.snapshot_id;
    child.encode(&mut f.raw[12288..12544]).unwrap();
    f.rehash();
    f.structural_check();
    assert_eq!(
        validate(&f.raw, &f.context(1_500_000_000)).unwrap_err(),
        Error::Identity
    );
}

#[test]
fn local_observation_times_cannot_follow_the_snapshot_creation() {
    for bit in [2, 4] {
        let mut f = Fixture::new();
        let mut quote = MarketRecord::decode(&f.raw[8576..9088]).unwrap();
        quote.header.flags |= bit;
        if bit == 2 {
            quote.header.ingress_mono_ns = 1_000_000_001;
        } else {
            quote.header.publish_mono_ns = 1_000_000_001;
        }
        quote.encode(&mut f.raw[8576..9088]).unwrap();
        f.rehash();
        f.structural_check();
        assert_eq!(
            validate(&f.raw, &f.context(1_500_000_000)).unwrap_err(),
            Error::Deadline
        );
    }
}

#[test]
fn required_state_must_itself_be_live() {
    let mut f = Fixture::new();
    let mut quote = MarketRecord::decode(&f.raw[8576..9088]).unwrap();
    quote.body.feed_state = 3;
    quote.encode(&mut f.raw[8576..9088]).unwrap();
    f.rehash();
    f.structural_check();
    let v = validate(&f.raw, &f.context(1_500_000_000)).unwrap();
    assert_eq!(v.member(0).unwrap().entry.feed_state, 2);
    assert_eq!(v.check_readiness(1_500_000_000), Err(Error::NotReady));
}
