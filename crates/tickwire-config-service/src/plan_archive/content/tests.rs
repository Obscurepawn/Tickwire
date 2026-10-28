use super::*;
use crate::{
    Config, ObjectCommand, ObjectLimits, Purpose,
    authorization::RoleBindings,
    bootstrap::{Bootstrap, BootstrapTrust},
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use sha2::{Digest, Sha256};
use tickwire_config::{patch::Head, plan::PlanOptions};

#[path = "../../../tests/deployment_support/mod.rs"]
mod support;
pub(crate) const NOW: u64 = support::NOW;
pub(crate) struct Fixture {
    pub record: RecordPrepared,
    pub objects: objects::State,
    pub head: CommittedHead,
    pub archive: ContentArchive,
}
pub(crate) fn fixture() -> Fixture {
    fixture_kind(false)
}
pub(crate) fn fixture_at_position(index: u64, term: u64) -> Fixture {
    fixture_options_at_position(false, false, index, term)
}
fn fixture_kind(patch_input: bool) -> Fixture {
    fixture_options(patch_input, false)
}
fn fixture_options(patch_input: bool, large_bundle: bool) -> Fixture {
    fixture_options_at_position(patch_input, large_bundle, 10, 2)
}
fn fixture_options_at_position(
    patch_input: bool,
    large_bundle: bool,
    index: u64,
    term: u64,
) -> Fixture {
    let mut source = support::Fixture::new(
        |v| {
            for row in v["control"]["role_bindings"].as_array_mut().unwrap() {
                if row["role"] == "config-editor" {
                    row["cluster_scope"] = true.into();
                    row["account_ids"] = serde_json::json!([]);
                }
            }
        },
        |_, _| {},
    );
    if large_bundle {
        let bytes = vec![91; 16 * 1024 * 1024];
        let reference = format!(
            "artifact://fixture/archive-extra@{}",
            hash_text(Sha256::digest(&bytes).into())
        );
        source.artifacts.insert(&reference, &bytes).unwrap();
    }
    let config = Config {
        cluster: [19; 16],
        objects: ObjectLimits {
            max_objects: 4096,
            max_inflight: 64,
            max_payload_bytes: 512 * 1024 * 1024,
        },
    };
    let mut objects = objects::State::new(config);
    objects.initialize(config).unwrap().unwrap();
    let base_bytes = source.candidate.canonical_bytes();
    let base_spec = ObjectSpec {
        key: [99; 16],
        owner: [98; 16],
        purpose: Purpose::Candidate,
        length: base_bytes.len() as u32,
        hash: Sha256::digest(base_bytes).into(),
    };
    seal(&mut objects, &base_spec, base_bytes);
    let signer = Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap();
    let trust = BootstrapTrust::new(
        config.cluster,
        [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
        "tickwire-qualification".into(),
        signer.public_key().as_ref().try_into().unwrap(),
    )
    .unwrap();
    let command = Bootstrap {
        candidate: base_spec.key,
        candidate_hash: base_spec.hash,
        nonce: [10; 16],
        signature: signer
            .sign(
                &trust
                    .signing_bytes(base_spec.key, base_spec.hash, [10; 16])
                    .unwrap(),
            )
            .as_ref()
            .try_into()
            .unwrap(),
    };
    let mut state = crate::bootstrap::State::new(Some(trust.clone()));
    state.initialize(Some(trust.digest())).unwrap();
    state
        .apply(command, &objects, [1, 2, 3], index, term)
        .unwrap();
    let head = state.head().unwrap();
    let subject = "spiffe://tickwire.invalid/qualification/operator";
    let authority = RoleBindings::from_candidate(head.candidate())
        .unwrap()
        .historical_authority(subject)
        .unwrap();
    let patch = tickwire_config::patch::ScopedPatch::parse(
        &serde_json::to_vec(&serde_json::json!({"base_etag":head.etag(),"changes":[{
            "op":"replace","selector":{"collection":"risk_policies","id":"spot-risk"},
            "path":"/max_gross_quote","value":"100000.00"}]}))
        .unwrap(),
        tickwire_config::Limits::default(),
    );
    let full_input = if patch_input {
        None
    } else {
        Some(&source.candidate)
    };
    let input = || {
        if let Some(full) = full_input {
            Input::Full(full)
        } else {
            Input::Patch(patch.as_ref().unwrap())
        }
    };
    let prepared = PreparedCandidate::prepare(
        &Head::new(head.revision(), head.candidate()).unwrap(),
        &head.etag(),
        head.candidate(),
        input(),
        &authority,
        tickwire_config::Limits::default(),
    )
    .unwrap();
    let bundle = Bundle::decode(
        &source.bundle_bytes(),
        crate::source_bundle::Limits::default(),
    )
    .unwrap();
    let trust = source.trust(1);
    let limits = deployment::Limits::default();
    let content = source.check(NOW).unwrap();
    assert_eq!(
        source.check_reports().unwrap().expires_unix_ms(),
        content.reports().expires_unix_ms()
    );
    let delivery = ProjectionDelivery::prepare(
        head.candidate(),
        &prepared,
        bundle.sources().artifacts,
        limits.projections,
    )
    .unwrap();
    let plan = ComputedPlan::compute(
        head.revision(),
        head.candidate(),
        &prepared,
        &delivery,
        PlanOptions::default(),
    )
    .unwrap();
    let archive = ContentArchive::capture(Capture {
        id: [8; 16],
        subject,
        leaf_sha256: [7; 32],
        request_digest: [6; 32],
        if_match: &head.etag(),
        base: &head,
        prepared: &prepared,
        input: input(),
        plan: &plan,
        delivery: &delivery,
        content: &content,
        bundle: &bundle,
        trust: &trust,
        limits,
    })
    .unwrap();
    let record = RecordPrepared {
        id: [8; 16],
        subject: subject.into(),
        leaf_sha256: [7; 32],
        idempotency_key: "archive-test".into(),
        request_digest: [6; 32],
        if_match: head.etag(),
        root: archive.root().clone(),
        admitted_unix_ms: NOW + 1,
    };
    for object in archive.objects() {
        seal(&mut objects, object.spec(), object.bytes());
    }
    Fixture {
        record,
        objects,
        head,
        archive,
    }
}
pub(crate) fn seal(state: &mut objects::State, spec: &ObjectSpec, bytes: &[u8]) {
    state.apply(ObjectCommand::Begin(spec.clone())).unwrap();
    for (i, chunk) in bytes.chunks(crate::CHUNK_BYTES).enumerate() {
        state
            .apply(ObjectCommand::Chunk {
                key: spec.key,
                owner: spec.owner,
                offset: (i * crate::CHUNK_BYTES) as u32,
                bytes: chunk.to_vec(),
            })
            .unwrap();
    }
    state
        .apply(ObjectCommand::Seal {
            key: spec.key,
            owner: spec.owner,
        })
        .unwrap();
}
#[test]
fn archived_plan_reopens_from_actual_sealed_closure_and_recomputes() {
    let fixture = fixture();
    validate_record(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    let reopened =
        ContentArchive::reopen(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    assert_eq!(reopened.root(), fixture.archive.root());
    assert_eq!(
        reopened.reverify().unwrap().base_revision(),
        fixture.head.revision()
    );
    assert_eq!(
        fixture.archive.objects().last().unwrap().spec(),
        fixture.archive.root()
    );
}

fn manifest(fixture: &Fixture) -> Manifest {
    Manifest::decode(fixture.archive.objects().last().unwrap().bytes()).unwrap()
}
fn replace_root(fixture: &mut Fixture, bytes: &[u8]) {
    let hash = Sha256::digest(bytes).into();
    let spec = ObjectSpec {
        key: object_key(fixture.record.id, Purpose::PlanMetadata, hash),
        owner: fixture.record.id,
        purpose: Purpose::PlanMetadata,
        length: bytes.len() as u32,
        hash,
    };
    seal(&mut fixture.objects, &spec, bytes);
    fixture.record.root = spec;
}
fn altered_state(fixture: &Fixture, key: Identity, remove: bool) -> objects::State {
    let entries = fixture
        .objects
        .snapshot_entries()
        .filter(|(spec, _, _)| !remove || spec.key != key)
        .map(|(spec, bytes, sealed)| (spec.clone(), bytes.to_vec(), sealed && spec.key != key))
        .collect();
    objects::State::restore(
        Config {
            cluster: [19; 16],
            objects: ObjectLimits {
                max_objects: 4096,
                max_inflight: 64,
                max_payload_bytes: 512 * 1024 * 1024,
            },
        },
        true,
        entries,
    )
    .unwrap()
}
#[test]
fn every_dependency_must_be_present_and_sealed() {
    let fixture = fixture();
    for object in fixture.archive.objects() {
        for remove in [false, true] {
            let state = altered_state(&fixture, object.spec().key, remove);
            assert!(
                validate_record(&fixture.record, &state, &fixture.head).is_err(),
                "missing/unsealed {:?}, remove={remove}",
                object.spec().purpose
            );
        }
    }
}
#[test]
fn root_and_admission_are_bound_to_subject_certificate_request_and_actual_head() {
    let fixture = fixture();
    let original = &fixture.record;
    let mut cases = Vec::new();
    let mut row = original.clone();
    row.id = [9; 16];
    cases.push(row);
    let mut row = original.clone();
    row.subject.push_str("-other");
    cases.push(row);
    let mut row = original.clone();
    row.leaf_sha256 = [8; 32];
    cases.push(row);
    let mut row = original.clone();
    row.request_digest = [5; 32];
    cases.push(row);
    let mut row = original.clone();
    row.if_match.push('x');
    cases.push(row);
    let mut row = original.clone();
    row.admitted_unix_ms = NOW - 1;
    cases.push(row);
    let mut row = original.clone();
    row.admitted_unix_ms = manifest(&fixture).expires_unix_ms;
    cases.push(row);
    let mut row = original.clone();
    row.root.length += 1;
    cases.push(row);
    for record in cases {
        assert!(validate_record(&record, &fixture.objects, &fixture.head).is_err());
    }
    for field in ["base_revision", "base_index", "base_term"] {
        let mut fixture = fixture_kind(false);
        let m = manifest(&fixture);
        let mut v = serde_json::to_value(m).unwrap();
        v[field] = "123".into();
        let bytes = parse(&serde_json::to_vec(&v).unwrap(), 1024 * 1024).unwrap();
        replace_root(&mut fixture, bytes.canonical_bytes());
        assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    }
}
#[test]
fn changed_rules_preserve_readable_history_but_block_current_reverification() {
    let mut fixture = fixture();
    let mut m = manifest(&fixture);
    m.rules_digest = [17; 32];
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    validate_record(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    let reopened =
        ContentArchive::reopen(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    assert!(matches!(reopened.reverify(), Err(Error::Conflict)));
}
#[test]
fn metadata_is_strict_canonical_and_complete() {
    let mut fixture = fixture();
    let original = manifest(&fixture).bytes(1024 * 1024).unwrap();
    let mut space = original.clone();
    space.push(b' ');
    replace_root(&mut fixture, &space);
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    let duplicate = String::from_utf8(original.clone()).unwrap().replacen(
        "\"schema\":1",
        "\"schema\":1,\"schema\":1",
        1,
    );
    replace_root(&mut fixture, duplicate.as_bytes());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    let mut v: serde_json::Value = serde_json::from_slice(&original).unwrap();
    v["unknown"] = true.into();
    let bytes = parse(&serde_json::to_vec(&v).unwrap(), 1024 * 1024).unwrap();
    replace_root(&mut fixture, bytes.canonical_bytes());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    let mut m = Manifest::decode(&original).unwrap();
    m.targets.pop_first();
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    let mut m = Manifest::decode(&original).unwrap();
    m.base_revision = "01".into();
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
}
#[test]
fn original_scoped_patch_is_retained_and_reapplied() {
    let fixture = fixture_kind(true);
    let m = manifest(&fixture);
    assert!(matches!(m.input_kind, InputKind::Patch));
    assert_ne!(m.input.hash, m.candidate.hash);
    let reopened =
        ContentArchive::reopen(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    assert_eq!(
        reopened.reverify().unwrap().candidate_hash(),
        m.candidate.hash
    );
}
#[test]
fn reference_identity_and_projection_binding_cannot_be_substituted() {
    let mut fixture = fixture();
    let original = manifest(&fixture);
    for field in 0..5 {
        let mut m = original.clone();
        match field {
            0 => m.bundle[0].owner = [4; 16],
            1 => m.bundle[0].purpose = Purpose::Candidate as u8,
            2 => m.bundle[0].length += 1,
            3 => {
                m.targets
                    .first_entry()
                    .unwrap()
                    .get_mut()
                    .next
                    .projection_hash = [4; 32]
            }
            _ => m.targets.first_entry().unwrap().get_mut().old = None,
        }
        replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
        assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
    }
}
#[test]
fn builder_bounds_before_retention_and_deduplicates_only_exact_content() {
    let mut builder = Builder::new([8; 16]);
    let a = builder.add(Purpose::Candidate, b"abc").unwrap();
    assert_eq!(builder.add(Purpose::Candidate, b"abc").unwrap(), a);
    assert_eq!(builder.bytes, 3);
    assert_eq!(builder.objects.len(), 1);
    let b = builder.add(Purpose::Qualification, b"abc").unwrap();
    assert_ne!(a.key, b.key);
    builder.bytes = MAX_BYTES - 1;
    assert!(matches!(
        builder.add(Purpose::Candidate, b"new"),
        Err(Error::Capacity)
    ));
    assert_eq!(builder.objects.len(), 2);
    let oversized = vec![0; Purpose::PlanMetadata.max_bytes() as usize + 1];
    assert!(matches!(
        builder.add(Purpose::PlanMetadata, &oversized),
        Err(Error::Bounds)
    ));
    let mut builder = Builder::new([8; 16]);
    for n in 0..MAX_OBJECTS {
        builder.add(Purpose::Candidate, &n.to_le_bytes()).unwrap();
    }
    assert!(matches!(
        builder.add(Purpose::Candidate, b"extra"),
        Err(Error::Capacity)
    ));
    assert_eq!(builder.objects.len(), MAX_OBJECTS);
}
#[test]
fn source_bundle_over_one_blob_uses_ordered_bounded_sealed_chunks() {
    let mut fixture = fixture_options(false, true);
    let mut m = manifest(&fixture);
    assert!(m.bundle_length > 16 * 1024 * 1024);
    assert_eq!(m.bundle.len(), 2);
    assert_eq!(m.bundle[0].length, Purpose::Qualification.max_bytes());
    let reopened =
        ContentArchive::reopen(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    reopened.reverify().unwrap();
    m.bundle.swap(0, 1);
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
}

fn store_reference(fixture: &mut Fixture, purpose: Purpose, bytes: &[u8]) -> Reference {
    let hash = Sha256::digest(bytes).into();
    let spec = ObjectSpec {
        key: object_key(fixture.record.id, purpose, hash),
        owner: fixture.record.id,
        purpose,
        length: bytes.len() as u32,
        hash,
    };
    seal(&mut fixture.objects, &spec, bytes);
    Reference::from(&spec)
}
fn store_plan(fixture: &mut Fixture, manifest: &mut Manifest, plan: &serde_json::Value) {
    let bytes = parse(&serde_json::to_vec(plan).unwrap(), 1024 * 1024).unwrap();
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.computed-plan.v1\0");
    hash.update(bytes.canonical_bytes());
    manifest.plan_digest = hash.finalize().into();
    manifest.plan = store_reference(fixture, Purpose::PlanMetadata, bytes.canonical_bytes());
}
#[test]
fn historical_projection_payload_remains_opaque_until_current_reverification() {
    let mut fixture = fixture();
    let mut m = manifest(&fixture);
    let id = m.targets.first_key_value().unwrap().0.clone();
    let object = &m.targets[&id].next.object;
    let mut projection = parse(fixture.archive.bytes(object).unwrap(), 4 * 1024 * 1024)
        .unwrap()
        .value()
        .clone();
    projection["payload"]["historical_only_field"] = true.into();
    let payload = parse(
        &serde_json::to_vec(&projection["payload"]).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap();
    let projection_hash = payload.hash();
    projection["projection_hash"] = hash_text(projection_hash).into();
    let bytes = parse(&serde_json::to_vec(&projection).unwrap(), 4 * 1024 * 1024).unwrap();
    let reference = store_reference(&mut fixture, Purpose::Projection, bytes.canonical_bytes());
    let target = m.targets.get_mut(&id).unwrap();
    target.next.object = reference;
    target.next.projection_hash = projection_hash;
    let mut plan = parse(fixture.archive.bytes(&m.plan).unwrap(), 1024 * 1024)
        .unwrap()
        .value()
        .clone();
    plan["targets"][&id]["new_projection"]["projection_hash"] = hash_text(projection_hash).into();
    store_plan(&mut fixture, &mut m, &plan);
    m.rules_digest = [71; 32];
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    let reopened =
        ContentArchive::reopen(&fixture.record, &fixture.objects, &fixture.head).unwrap();
    assert!(matches!(reopened.reverify(), Err(Error::Conflict)));
}
#[test]
fn recomputed_object_hashes_do_not_bypass_plan_projection_global_binding() {
    let mut fixture = fixture();
    let mut m = manifest(&fixture);
    let id = m.targets.first_key_value().unwrap().0.clone();
    let mut plan = parse(fixture.archive.bytes(&m.plan).unwrap(), 1024 * 1024)
        .unwrap()
        .value()
        .clone();
    plan["targets"][&id]["new_projection"]["global_hash"] = hash_text([71; 32]).into();
    store_plan(&mut fixture, &mut m, &plan);
    replace_root(&mut fixture, &m.bytes(1024 * 1024).unwrap());
    assert!(validate_record(&fixture.record, &fixture.objects, &fixture.head).is_err());
}
