use serde_json::Value;
use sha2::{Digest, Sha256};
use tickwire_ipc::*;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../specs/contracts/ipc-vectors-v1.json")).unwrap()
}
fn vector<'a>(f: &'a Value, name: &str) -> &'a Value {
    f["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn bytes(f: &Value, name: &str) -> Vec<u8> {
    hex(vector(f, name)["hex"].as_str().unwrap())
}
fn array<const N: usize>(value: &Value) -> [u8; N] {
    hex(value.as_str().unwrap()).try_into().unwrap()
}
fn n(value: &Value, key: &str) -> u64 {
    value[key].as_u64().unwrap_or(0)
}
fn yes(value: &Value, key: &str) -> bool {
    value[key].as_bool().unwrap_or(false)
}
fn frame_rehash(raw: &mut [u8]) {
    let mut h = ControlEnvelope::decode(&raw[..128]).unwrap();
    h.body_crc32c = crc32c(&raw[128..]);
    h.header_crc32c = 0;
    h.encode(&mut raw[..128]).unwrap();
    h.header_crc32c = crc32c(&raw[..128]);
    h.encode(&mut raw[..128]).unwrap();
}

// Only the test fixture adapter uses schema names. Every assertion calls a fixed production API.
fn mutate(raw: &mut [u8], layout: &str, path: &str, value: &Value) {
    let schema: Value =
        serde_json::from_str(include_str!("../../../specs/contracts/ipc-wire-v1.json")).unwrap();
    let mut name = layout;
    let mut offset = 0;
    let parts = path.split('.').collect::<Vec<_>>();
    for (i, part) in parts.iter().enumerate() {
        if name == "ControlFrame" {
            if *part == "body" {
                offset += 128;
                name = "ConfigControlBody";
                continue;
            }
            name = "ControlEnvelope";
        }
        let l = schema["layouts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["name"] == name)
            .unwrap();
        let field = l["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == *part)
            .unwrap();
        offset += field["offset"].as_u64().unwrap() as usize;
        if i == parts.len() - 1 {
            let len = field["size"].as_u64().unwrap() as usize;
            let data = if let Some(s) = value.as_str() {
                hex(s)
            } else {
                value.as_u64().unwrap().to_le_bytes()[..len].to_vec()
            };
            raw[offset..offset + len].copy_from_slice(&data);
        } else {
            name = field["type"].as_str().unwrap();
        }
    }
}

fn config_context(c: &Value) -> ConfigContext<'_> {
    ConfigContext {
        negotiated_features: n(c, "negotiated_features"),
        authenticated_actor_kind: n(c, "authenticated_actor_kind") as u16,
        authenticated_sender_instance: array(&c["authenticated_sender_instance"]),
        authenticated_issuer_boot_id: array(&c["authenticated_issuer_boot_id"]),
        authenticated_host_id: c["authenticated_host_id"].as_str().unwrap(),
        target_instance_id: array(&c["target_instance_id"]),
        target_boot_id: array(&c["target_boot_id"]),
        target_host_boot_id: array(&c["target_host_boot_id"]),
        target_exec_generation: n(c, "target_exec_generation"),
        current_runtime_revision: n(c, "current_runtime_revision"),
        committed_config_revision: n(c, "committed_config_revision"),
        committed_base_revision: n(c, "committed_base_revision"),
        committed_global_config_hash: array(&c["committed_global_config_hash"]),
        committed_control_commit_ref: array(&c["committed_control_commit_ref"]),
        committed_target_set_hash: array(&c["committed_target_set_hash"]),
        proof_objects_prevalidated: yes(c, "proof_objects_prevalidated"),
        runtime_receipt_verified: yes(c, "runtime_receipt_verified"),
        file_receipt_verified: yes(c, "file_receipt_verified"),
        full_target_set_verified: yes(c, "full_target_set_verified"),
        completion_commit_verified: yes(c, "completion_commit_verified"),
    }
}

fn bundle(f: &Value, v: &Value, raw: &[u8], extra: &Value) -> Result<()> {
    let mut c = v["semantic_context"].clone();
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            c[k] = v.clone();
        }
    }
    let instruments = c["expected_instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (n(i, "instrument_id"), n(i, "instrument_version")))
        .collect::<Vec<_>>();
    let join = JoinBody::decode(&bytes(
        f,
        c["join_vector_name"].as_str().unwrap_or("join_snapshot"),
    ))?;
    validate_topic_bundle(
        raw,
        &TopicSnapshotContext {
            expected_instruments: &instruments,
            expected_instrument_set_hash: array(&c["expected_instrument_set_hash"]),
            expected_instrument_set_version: n(&c, "expected_instrument_set_version"),
            boundary_sequence: n(&c, "boundary_sequence"),
            join: &join,
        },
    )
}

fn run_case(name: &str) {
    let f = fixture();
    let c = f["semantic_cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap();
    let result = (|| -> Result<()> {
        if !c["resource_kind"].is_null() {
            let s = SnapshotHeader {
                snapshot_kind: n(c, "snapshot_kind") as u16,
                entry_count: n(c, "entry_count") as u32,
                entry_bytes: n(c, "record_bytes") as u16,
                ..Default::default()
            };
            return validate_resource_shape(
                n(c, "resource_kind") as u16,
                n(c, "capacity") as u32,
                n(c, "record_bytes") as u32,
                n(c, "resource_bytes"),
                Some(&s),
            );
        }
        let v = vector(&f, c["vector"].as_str().unwrap());
        let mut raw = hex(v["hex"].as_str().unwrap());
        let layout = v["layout"].as_str().unwrap();
        if let Some(map) = c["mutations"].as_object() {
            for (path, val) in map {
                mutate(&mut raw, layout, path, val);
            }
        }
        match layout {
            "MarketRecord" | "IntentRecord" | "ReportRecord" | "OwnershipRecord" => {
                decode_record(&raw).map(|_| ())
            }
            "ControlFrame" => {
                if !c["mutations"].is_null() {
                    frame_rehash(&mut raw);
                }
                let frame = ControlFrame::decode(&raw)?;
                if frame.envelope.opcode >= 80 {
                    validate_config_binding(&frame, &config_context(&c["context"])).map(|_| ())
                } else {
                    Ok(())
                }
            }
            "CompositeFile" => bundle(&f, v, &raw, &c["context"]),
            "GrantBody" => {
                let ctx = &c["context"];
                let lease =
                    ExecLeaseBody::decode(&bytes(&f, ctx["exec_lease_vector"].as_str().unwrap()))?;
                validate_grant_deadline(
                    &GrantBody::decode(&raw)?,
                    &lease,
                    n(ctx, "now_mono_ns"),
                    &array(&ctx["host_boot_id"]),
                    n(ctx, "exec_generation"),
                    n(ctx, "resource_session_deadline_mono_ns"),
                )
            }
            _ => panic!("unhandled semantic fixture {layout}"),
        }
    })();
    match c["expected_error"].as_str() {
        Some(expected) => assert_eq!(
            result.map_err(|e| format!("{e:?}")),
            Err(expected.to_owned()),
            "{name}"
        ),
        None => assert_eq!(result, Ok(()), "{name}"),
    }
}

#[test]
fn all_declared_semantic_cases() {
    let f = fixture();
    assert_eq!(f["semantic_cases"].as_array().unwrap().len(), 25);
    for case in f["semantic_cases"].as_array().unwrap() {
        run_case(case["name"].as_str().unwrap());
    }
}

#[test]
fn every_data_vector_passes_production_validation() {
    let f = fixture();
    for v in f["vectors"].as_array().unwrap() {
        if matches!(
            v["layout"].as_str().unwrap(),
            "MarketRecord" | "IntentRecord" | "ReportRecord" | "OwnershipRecord"
        ) {
            decode_record(&hex(v["hex"].as_str().unwrap()))
                .unwrap_or_else(|e| panic!("{}: {e}", v["name"]));
        }
    }
}

#[test]
fn crc_reference_and_corruption() {
    assert_eq!(crc32c(b"123456789"), 0xe3069283);
    let f = fixture();
    let raw = bytes(&f, "freeze_complete_frame");
    for i in 0..raw.len() {
        let mut corrupt = raw.clone();
        corrupt[i] ^= 1;
        assert!(ControlFrame::decode(&corrupt).is_err(), "byte {i}");
    }
}

#[test]
fn truncated_record_and_control_frame_never_panic() {
    let f = fixture();
    for name in [
        "new_limit_intent",
        "fill_report",
        "quote_one",
        "depth_inline_two_levels",
    ] {
        let raw = bytes(&f, name);
        for length in 0..raw.len() {
            assert!(decode_record(&raw[..length]).is_err(), "{name} {length}");
        }
    }
    let raw = bytes(&f, "config_apply_frame");
    for length in 0..raw.len() {
        assert!(ControlFrame::decode(&raw[..length]).is_err());
    }
}

#[test]
fn record_tags_offsets_and_unused_fields_are_rejected() {
    let f = fixture();
    for (path, value, error) in [
        ("header.message_type", 65535, Error::UnknownMessage),
        ("header.sequence", 1u64 << 63, Error::SequenceExhausted),
        ("header.payload_kind", 3, Error::InvalidEnum),
        ("body.trade_id", 1, Error::ReservedNonzero),
        ("body.feed_state", 65535, Error::InvalidEnum),
    ] {
        let mut raw = bytes(&f, "quote_one");
        mutate(&mut raw, "MarketRecord", path, &Value::from(value));
        assert_eq!(decode_record(&raw), Err(error), "{path}");
    }
    let mut raw = bytes(&f, "depth_inline_two_levels");
    mutate(
        &mut raw,
        "MarketRecord",
        "header.payload_offset",
        &Value::from(u64::MAX),
    );
    assert_eq!(decode_record(&raw), Err(Error::Malformed));
    let mut raw = bytes(&f, "new_limit_intent");
    mutate(
        &mut raw,
        "IntentRecord",
        "body.trigger_count",
        &Value::from(9),
    );
    assert_eq!(decode_record(&raw), Err(Error::Bounds));
}

#[test]
fn snapshot_truncation_hash_and_untrusted_offsets() {
    let f = fixture();
    let v = vector(&f, "normalized_topic_snapshot");
    let raw = hex(v["hex"].as_str().unwrap());
    for length in [0, 127, 4095, 4096, 4351, 8191, 8192, raw.len() - 1] {
        assert!(bundle(&f, v, &raw[..length], &Value::Null).is_err());
    }
    for index in [272, 4352, 8192, 9000, raw.len() - 1] {
        let mut corrupt = raw.clone();
        corrupt[index] ^= 1;
        assert!(bundle(&f, v, &corrupt, &Value::Null).is_err());
    }
}

#[test]
fn arena_payload_identity_bounds_order_and_padding() {
    let f = fixture();
    let mut record = MarketRecord::decode(&bytes(&f, "depth_inline_two_levels")).unwrap();
    record.header.payload_kind = 2;
    record.header.payload_length = 13 * 16;
    record.header.payload_offset = 8192 + 128;
    record.header.arena_class = 1;
    record.header.allocation_seq = record.header.sequence;
    record.body.level_count = 13;
    record.body.bid_count = 13;
    record.body.ask_count = 0;
    record.body.inline_levels = core::array::from_fn(|_| DepthLevel::default());
    let mut r = ResourceHeader {
        resource_id: [1; 16],
        resource_kind: 1,
        ring_epoch: 2,
        arena_offset: 8192,
        arena_bytes: 512,
        resource_bytes: 8704,
        arena_class_count: 1,
        ..Default::default()
    };
    r.class_descriptors[0] = ArenaClass {
        class_id: 1,
        block_bytes: 512,
        block_count: 1,
        payload_limit: 384,
        offset: 8192,
        length: 512,
    };
    let block = ArenaBlockHeader {
        allocation_seq: 1,
        payload_length: 208,
        class_id: 1,
        ring_epoch: 2,
        resource_id: [1; 16],
        payload_crc32c: 0,
    };
    let mut bank = vec![0u8; 8704];
    block.encode(&mut bank[8192..8320]).unwrap();
    for i in 0..13 {
        DepthLevel {
            price_ticks: 100 - i as i64,
            qty_lots: 1,
        }
        .encode(&mut bank[8320 + i * 16..8336 + i * 16])
        .unwrap();
    }
    assert_eq!(validate_arena_payload(&record, &bank, &r), Ok(()));
    let mut wrong = record.clone();
    wrong.header.payload_offset = u64::MAX;
    assert_eq!(
        validate_arena_payload(&wrong, &bank, &r),
        Err(Error::Bounds)
    );
    let mut corrupt = bank.clone();
    corrupt[8192] = 2;
    assert_eq!(
        validate_arena_payload(&record, &corrupt, &r),
        Err(Error::Identity)
    );
    let mut corrupt = bank.clone();
    corrupt[8703] = 1;
    assert_eq!(
        validate_arena_payload(&record, &corrupt, &r),
        Err(Error::ReservedNonzero)
    );
    let mut corrupt = bank.clone();
    corrupt[8320 + 16..8320 + 24].copy_from_slice(&101i64.to_le_bytes());
    assert_eq!(
        validate_arena_payload(&record, &corrupt, &r),
        Err(Error::Malformed)
    );
}

#[test]
fn record_crc_enabled_covers_payload_and_header() {
    let f = fixture();
    let mut raw = bytes(&f, "depth_inline_two_levels");
    let mut record = MarketRecord::decode(&raw).unwrap();
    record.header.flags = 8;
    record.header.payload_crc32c = crc32c(&raw[320..352]);
    record.encode(&mut raw).unwrap();
    // The record checksum excludes both checksum fields, including the payload checksum.
    raw[112..120].fill(0);
    record.header.record_crc32c = crc32c(&raw);
    record.encode(&mut raw).unwrap();
    assert!(decode_record(&raw).is_ok());
    raw[320] ^= 1;
    assert_eq!(decode_record(&raw), Err(Error::Checksum));
}

#[test]
fn unknown_control_enum_and_bad_fd_offer_are_rejected() {
    let f = fixture();
    let mut raw = bytes(&f, "config_apply_frame");
    mutate(
        &mut raw,
        "ControlFrame",
        "body.actor_kind",
        &Value::from(999),
    );
    frame_rehash(&mut raw);
    assert!(matches!(
        ControlFrame::decode(&raw),
        Err(Error::InvalidEnum)
    ));
    let prefix = OfferPrefix {
        descriptor_count: 2,
        descriptor_bytes: 128,
        ..Default::default()
    };
    let desc = ResourceDescriptor {
        resource_kind: 1,
        access: 1,
        seal_profile: 1,
        ..Default::default()
    };
    let mut raw = vec![0; 128 + 64 + 256];
    prefix.encode(&mut raw[128..192]).unwrap();
    desc.encode(&mut raw[192..320]).unwrap();
    desc.encode(&mut raw[320..448]).unwrap();
    let env = ControlEnvelope {
        magic: *b"TWCTL001",
        abi_major: 1,
        header_bytes: 128,
        opcode: 3,
        frame_bytes: 448,
        flags: 1,
        fd_count: 2,
        ..Default::default()
    };
    env.encode(&mut raw[..128]).unwrap();
    frame_rehash(&mut raw);
    assert!(matches!(ControlFrame::decode(&raw), Err(Error::Bounds)));
}

#[test]
fn generated_layouts_match_current_contract() {
    let digest: [u8; 32] =
        Sha256::digest(include_bytes!("../../../specs/contracts/ipc-wire-v1.json")).into();
    let contract: Value =
        serde_json::from_slice(include_bytes!("../../../specs/contracts/ipc-wire-v1.json"))
            .unwrap();
    assert_eq!(
        WIRE_LAYOUT_COUNT,
        contract["layouts"].as_array().unwrap().len()
    );
    assert_eq!(
        WIRE_CONTRACT_SHA256, digest,
        "run tools/generate.py wire after changing the contract"
    );
}

#[test]
fn feature_gate_precedes_configuration_body_parse() {
    let f = fixture();
    let mut raw = bytes(&f, "config_apply_frame");
    raw[128] ^= 1; // Even a broken body is not parsed before feature admission.
    assert!(matches!(
        ControlFrame::decode_negotiated(&raw, 28),
        Err(Error::UnsupportedFeature)
    ));
    assert!(matches!(
        ControlFrame::decode_negotiated(&raw, 60),
        Err(Error::Checksum)
    ));
}
