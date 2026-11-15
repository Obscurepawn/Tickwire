#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use tickwire_journal::crc32c::{
    crc32c_header_masked, crc32c_masked, crc32c_slicing8, crc32c_slicing8_update, crc32c_update,
};
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

#[cfg(any(feature = "crc32", not(feature = "crc32")))]
fn refresh_trailer_sha256(frame: &mut [u8]) {
    let tail = frame.len() - TRAILER_BYTES;
    let mut hash = Sha256::new();
    hash.update(&frame[..tail]);
    hash.update(&frame[tail + 32..]);
    frame[tail..tail + 32].copy_from_slice(&hash.finalize());
}

#[test]
fn castagnoli_known_vectors() {
    // RFC 3720 Appendix B.4 iSCSI / Castagnoli CRC-32C vectors
    assert_eq!(crc32c(b"123456789"), 0xe3069283);
    assert_eq!(crc32c_slicing8(b"123456789"), 0xe3069283);
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c_slicing8(b""), 0);
    assert_eq!(crc32c(&[0; 32]), 0x8a9136aa);
    assert_eq!(crc32c_slicing8(&[0; 32]), 0x8a9136aa);
    assert_eq!(crc32c(&[0xff; 32]), 0x62a8ab43);
    assert_eq!(crc32c_slicing8(&[0xff; 32]), 0x62a8ab43);

    let inc: Vec<u8> = (0u8..32).collect();
    assert_eq!(crc32c(&inc), 0x46dd794e);
    assert_eq!(crc32c_slicing8(&inc), 0x46dd794e);

    let dec: Vec<u8> = (0u8..32).rev().collect();
    assert_eq!(crc32c(&dec), 0x113fdb5c);
    assert_eq!(crc32c_slicing8(&dec), 0x113fdb5c);
}

#[test]
fn hardware_and_slicing8_agree_across_alignments_lengths_and_masks() {
    let mut raw = vec![0u8; 4096 + 32];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for byte in &mut raw {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state & 0xff) as u8;
    }

    for align in 0..16 {
        for len in (0..=512).chain([768, 1023, 1024, 1025, 2047, 2048, 3333, 4096]) {
            let slice = &raw[align..align + len];
            let hw = crc32c(slice);
            let sw = crc32c_slicing8(slice);
            assert_eq!(hw, sw, "align={align} len={len}");
        }
    }

    // Incremental update equivalence across arbitrary split points.
    let sample = &raw[3..3 + 317];
    let expected = crc32c(sample);
    for split in 0..=sample.len() {
        let (left, right) = sample.split_at(split);
        assert_eq!(
            crc32c_update(crc32c(left), right),
            expected,
            "split={split}"
        );
        assert_eq!(
            crc32c_slicing8_update(crc32c_slicing8(left), right),
            expected,
            "slicing8 split={split}"
        );
    }

    // Zero-allocation masked CRC equivalence (including WAL 136-byte header layout).
    for start in [0, 1, 7, 8, 63, 112, 116, 128, 132, 200] {
        for width in [0, 1, 4, 8, 17, 73] {
            let total = 300;
            let end = (start + width).min(total);
            let slice = &raw[..total];
            let mut zeroed = slice.to_vec();
            zeroed[start..end].fill(0);
            assert_eq!(
                crc32c_masked(slice, start..end),
                crc32c_slicing8(&zeroed),
                "mask {start}..{end}"
            );
        }
    }

    for align in 0..16 {
        let hdr: &[u8; HEADER_BYTES] = raw[align..align + HEADER_BYTES].try_into().unwrap();
        let mut zeroed = *hdr;
        zeroed[128..132].fill(0);
        assert_eq!(crc32c_header_masked(hdr), crc32c_slicing8(&zeroed));
        assert_eq!(crc32c_header_masked(hdr), crc32c_masked(hdr, 128..132));
    }
}

#[test]
fn crc32c_masked_clamps_out_of_bounds_and_inverted_ranges_without_panicking() {
    let mut raw = [0u8; 160];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(73).wrapping_add(19);
    }

    for len in [0usize, 1, 7, 8, 63, 64, 136, 160] {
        let slice = &raw[..len];
        #[allow(clippy::reversed_empty_ranges)]
        let ranges = [
            0..0,
            0..len,
            0..len + 17,
            len / 2..len + 99,
            len..len + 64,
            len + 5..len + 80,
            0..usize::MAX,
            len..usize::MAX,
            usize::MAX..usize::MAX,
            8..4,
            len + 10..4,
            usize::MAX..0,
        ];
        for r in ranges {
            let end = r.end.min(len);
            let start = r.start.min(end);
            let mut expected = slice.to_vec();
            expected[start..end].fill(0);
            assert_eq!(
                crc32c_masked(slice, r.clone()),
                crc32c(&expected),
                "clamped masked CRC mismatch at len={len}, range={r:?}"
            );
        }
    }
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

#[cfg(not(feature = "crc32"))]
#[test]
fn default_mode_writes_zero_crcs_and_skips_crc_recomputation() {
    let h = header();
    let mut bytes = encode(&h, b"abc").unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[128..132].try_into().unwrap()),
        0,
        "header_crc32c must be deterministic 0 when crc32 feature is off"
    );
    assert_eq!(
        u32::from_le_bytes(bytes[132..136].try_into().unwrap()),
        0,
        "payload_crc32c must be deterministic 0 when crc32 feature is off"
    );
    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded.header, h);
    assert_eq!(decoded.payload, b"abc");

    // Mutate payload and refresh trailer SHA-256 while leaving CRC fields at 0:
    // default mode skips CRC recomputation and accepts valid SHA-256 frames.
    bytes[136..139].copy_from_slice(b"xyz");
    refresh_trailer_sha256(&mut bytes);
    let decoded_modified = decode(&bytes).unwrap();
    assert_eq!(decoded_modified.payload, b"xyz");
}

#[cfg(feature = "crc32")]
#[test]
fn crc32_feature_computes_and_strictly_rejects_header_and_payload_crc_mismatch() {
    let h = header();
    let bytes = encode(&h, b"abc").unwrap();
    let header_crc = u32::from_le_bytes(bytes[128..132].try_into().unwrap());
    let payload_crc = u32::from_le_bytes(bytes[132..136].try_into().unwrap());
    assert_eq!(payload_crc, crc32c(b"abc"));
    assert_ne!(payload_crc, 0);
    let hdr_arr: &[u8; HEADER_BYTES] = bytes[..HEADER_BYTES].try_into().unwrap();
    assert_eq!(header_crc, crc32c_header_masked(hdr_arr));
    assert_ne!(header_crc, 0);

    // Corrupt a header byte and refresh trailer SHA-256: must fail specifically on header CRC.
    let mut bad_hdr = bytes.clone();
    bad_hdr[56] ^= 1;
    refresh_trailer_sha256(&mut bad_hdr);
    assert!(matches!(
        decode(&bad_hdr),
        Err(tickwire_journal::Error::Quarantined("header CRC"))
    ));

    // Corrupt a payload byte and refresh trailer SHA-256: header_crc32c still matches header,
    // so decode must fail specifically on payload CRC.
    let mut bad_payload = bytes;
    bad_payload[136] ^= 1;
    refresh_trailer_sha256(&mut bad_payload);
    assert!(matches!(
        decode(&bad_payload),
        Err(tickwire_journal::Error::Quarantined("payload CRC"))
    ));
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
        "../../../specs/contracts/journal-wire-v1.json"
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
