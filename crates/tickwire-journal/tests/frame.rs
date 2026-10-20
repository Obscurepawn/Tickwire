use sha2::{Digest, Sha256};
use tickwire_journal::frame::*;
fn header() -> Header {
    Header {
        frame_type: FrameType::DataRecord,
        flags: 0,
        stream_epoch: [1; 16],
        domain: [2; 16],
        segment_id: 3,
        lsn: 456,
        record_seq: 7,
        exec_epoch: 8,
        batch_number: 9,
        previous_hash: [10; 32],
    }
}
#[test]
fn castagnoli_known_vectors() {
    assert_eq!(crc32c(b"123456789"), 0xe3069283);
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c(&[0; 32]), 0x8a9136aa);
}
#[test]
fn exact_contract_layout_and_hash_scope() {
    let h = header();
    let bytes = encode(&h, b"abc").unwrap();
    assert_eq!(bytes.len(), 184);
    assert_eq!(&bytes[..8], b"TWAL0001");
    assert_eq!(&bytes[8..12], &[1, 0, 136, 0]);
    assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 184);
    assert_eq!(&bytes[16..24], &[2, 0, 0, 0, 3, 0, 0, 0]);
    assert_eq!(&bytes[24..40], &[1; 16]);
    assert_eq!(&bytes[40..56], &[2; 16]);
    for (offset, expected) in [(56, 3), (64, 456), (72, 7), (80, 8), (88, 9)] {
        assert_eq!(
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()),
            expected
        );
    }
    assert_eq!(&bytes[96..128], &[10; 32]);
    assert_eq!(&bytes[136..144], b"abc\0\0\0\0\0");
    assert_eq!(&bytes[176..184], &[184, 0, 0, 0, b'T', b'E', b'N', b'D']);
    let mut hash = Sha256::new();
    hash.update(&bytes[..144]);
    hash.update(&bytes[176..]);
    assert_eq!(&bytes[144..176], &hash.finalize()[..]);
    let f = decode(&bytes).unwrap();
    assert_eq!(f.header, h);
    assert_eq!(f.payload, b"abc");
}
#[test]
fn every_single_bit_error_is_detected() {
    let bytes = encode(&header(), b"economic fact").unwrap();
    for offset in 0..bytes.len() {
        for bit in 0..8 {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 1 << bit;
            assert!(decode(&corrupt).is_err(), "offset {offset} bit {bit}");
        }
    }
}
#[test]
fn truncation_and_oversize_are_errors_without_panics() {
    let bytes = encode(&header(), &[3; 31]).unwrap();
    for n in 0..bytes.len() {
        assert!(decode(&bytes[..n]).is_err());
    }
    assert!(
        encode(
            &header(),
            &vec![0; FRAME_MAX_BYTES - HEADER_BYTES - TRAILER_BYTES + 1]
        )
        .is_err()
    );
    let max = encode(
        &header(),
        &vec![0; FRAME_MAX_BYTES - HEADER_BYTES - TRAILER_BYTES],
    )
    .unwrap();
    assert_eq!(max.len(), FRAME_MAX_BYTES);
    assert!(decode(&max).is_ok());
}
#[test]
fn all_contract_types_roundtrip_and_nonzero_flags_rejected() {
    for kind in [
        FrameType::BatchBegin,
        FrameType::DataRecord,
        FrameType::RaftEntry,
        FrameType::RaftHardState,
        FrameType::ControlTruncateSuffix,
        FrameType::SnapshotPublish,
        FrameType::ControlConfState,
        FrameType::ControlCommitWatermark,
        FrameType::DependencyPin,
        FrameType::DependencyRelease,
        FrameType::SourceFreezeSeal,
        FrameType::BatchSeal,
    ] {
        let mut h = header();
        h.frame_type = kind;
        assert_eq!(decode(&encode(&h, b"").unwrap()).unwrap().header, h);
    }
    let mut h = header();
    h.flags = 1;
    assert!(encode(&h, b"").is_err());
}

#[test]
fn codec_constants_match_authoritative_contract() {
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../specs/contracts/replication-profile-v1.json"
    ))
    .unwrap();
    let wal = &contract["wal"];
    assert_eq!(wal["header_bytes"], HEADER_BYTES);
    assert_eq!(wal["trailer_bytes"], TRAILER_BYTES);
    assert_eq!(wal["frame_max_bytes"], FRAME_MAX_BYTES);
    assert_eq!(wal["alignment_bytes"], 8);
    let mut offset = 0;
    let offsets = [
        0, 8, 10, 12, 16, 18, 20, 24, 40, 56, 64, 72, 80, 88, 96, 128, 132,
    ];
    for (field, expected) in wal["header_fields"].as_array().unwrap().iter().zip(offsets) {
        assert_eq!(offset, expected);
        offset += field["bytes"].as_u64().unwrap();
    }
    assert_eq!(offset, 136);
    assert_eq!(
        wal["frame_types"]["BatchBegin"],
        FrameType::BatchBegin as u16
    );
    assert_eq!(wal["frame_types"]["BatchSeal"], FrameType::BatchSeal as u16);
}
