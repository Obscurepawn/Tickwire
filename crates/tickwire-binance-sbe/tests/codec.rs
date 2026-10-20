use tickwire_binance_sbe::*;
fn bytes(hex: &str) -> Vec<u8> {
    hex.trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
const SAMPLES: [&str; 4] = [
    include_str!("fixtures/trades.hex"),
    include_str!("fixtures/quote.hex"),
    include_str!("fixtures/partial.hex"),
    include_str!("fixtures/diff.hex"),
];
#[test]
fn four_independent_literal_frames_decode() {
    for sample in SAMPLES {
        decode_frame(&bytes(sample), Limits::default()).unwrap();
    }
}
#[test]
fn exact_mantissa_and_exponent_conversion() {
    let tick = DecimalUnit::new(1, -2).unwrap();
    assert_eq!(decimal_to_units(6500000, -2, tick), Ok(6500000));
    assert_eq!(
        decimal_to_units(100, -3, DecimalUnit::new(1, -3).unwrap()),
        Ok(100)
    );
}
#[test]
fn exact_large_scale_cancellation_does_not_overflow() {
    assert_eq!(
        decimal_to_units(9, 38, DecimalUnit::new(9, 38).unwrap()),
        Ok(1)
    );
    assert_eq!(
        decimal_to_units(1, -127, DecimalUnit::new(1, -127).unwrap()),
        Ok(1)
    );
}

#[test]
fn all_fields_and_group_rows_are_preserved() {
    let raw = bytes(SAMPLES[0]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    assert_eq!(f.kind(), Kind::Trades);
    assert_eq!(f.symbol(), "BTCUSDT");
    assert_eq!(f.event_time_ns(), 1_000_000_000);
    assert_eq!(f.transaction_time_ns(), Some(999_999_000));
    assert_eq!((f.price_exponent(), f.quantity_exponent()), (-2, -3));
    let t = f.trades().unwrap().iter().collect::<Vec<_>>();
    assert_eq!(
        t,
        vec![
            Trade {
                id: 7,
                price_mantissa: 6500000,
                quantity_mantissa: 100,
                is_buyer_maker: true
            },
            Trade {
                id: 8,
                price_mantissa: 6500100,
                quantity_mantissa: 200,
                is_buyer_maker: false
            }
        ]
    );
    assert!(t.iter().all(|t| t.is_best_match()));
    let raw = bytes(SAMPLES[1]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    assert_eq!(f.last_update_id(), Some(42));
    assert_eq!(
        f.quote().unwrap(),
        Quote {
            bid: LevelView {
                price_mantissa: 6500000,
                quantity_mantissa: 100
            },
            ask: LevelView {
                price_mantissa: 6500100,
                quantity_mantissa: 200
            }
        }
    );
}

#[test]
fn every_truncation_and_trailing_byte_rejected() {
    for sample in SAMPLES {
        let mut raw = bytes(sample);
        for n in 0..raw.len() {
            assert!(
                decode_frame(&raw[..n], Limits::default()).is_err(),
                "prefix {n}"
            );
        }
        raw.push(0);
        assert!(matches!(
            decode_frame(&raw, Limits::default()),
            Err(Error::Bounds)
        ));
    }
}

#[test]
fn schema_version_template_and_root_size_are_exact() {
    for (offset, value, error) in [
        (4, 2, Error::Schema),
        (6, 1, Error::Version),
        (2, 10004, Error::Template),
        (0, 27, Error::BlockLength),
    ] {
        let mut raw = bytes(SAMPLES[3]);
        raw[offset..offset + 2].copy_from_slice(&(value as u16).to_le_bytes());
        assert!(matches!(decode_frame(&raw,Limits::default()),Err(e) if e==error));
    }
}

#[test]
fn group_lengths_and_limits_are_checked_before_rows() {
    let mut raw = bytes(SAMPLES[3]);
    raw[34..36].copy_from_slice(&17u16.to_le_bytes());
    assert!(matches!(
        decode_frame(&raw, Limits::default()),
        Err(Error::BlockLength)
    ));
    let mut raw = bytes(SAMPLES[0]);
    raw[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_frame(&raw, Limits::default()),
        Err(Error::Capacity)
    ));
    let raw = bytes(SAMPLES[2]);
    assert!(matches!(
        decode_frame(
            &raw,
            Limits {
                max_depth_levels: 1,
                ..Limits::default()
            }
        ),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        decode_frame(
            &raw,
            Limits {
                max_frame_bytes: raw.len() - 1,
                ..Limits::default()
            }
        ),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        decode_frame(
            &raw,
            Limits {
                max_trades: 0,
                ..Limits::default()
            }
        ),
        Err(Error::Capacity)
    ));
}

#[test]
fn invalid_enum_null_exponent_negative_time_and_overflow_rejected() {
    let mut raw = bytes(SAMPLES[0]);
    raw[32 + 24] = 2;
    assert!(matches!(
        decode_frame(&raw, Limits::default()),
        Err(Error::Value)
    ));
    let mut raw = bytes(SAMPLES[3]);
    raw[32] = 128;
    assert!(matches!(
        decode_frame(&raw, Limits::default()),
        Err(Error::Value)
    ));
    for (time, error) in [(-1i64, Error::Value), (i64::MAX, Error::Overflow)] {
        let mut raw = bytes(SAMPLES[3]);
        raw[8..16].copy_from_slice(&time.to_le_bytes());
        assert!(matches!(decode_frame(&raw,Limits::default()),Err(e) if e==error));
    }
    let mut raw = bytes(SAMPLES[3]);
    *raw.last_mut().unwrap() = 0xff;
    assert!(matches!(
        decode_frame(&raw, Limits::default()),
        Err(Error::Utf8)
    ));
}

fn expected() -> ExpectedInstrument<'static> {
    ExpectedInstrument {
        identity: Identity {
            instrument_id: 12,
            instrument_version: 13,
            catalog_version: 14,
            catalog_hash: [5; 32],
            source_id: 1,
            generation: 2,
        },
        symbol: "BTCUSDT",
        price_tick: DecimalUnit::new(1, -2).unwrap(),
        quantity_step: DecimalUnit::new(1, -3).unwrap(),
    }
}
#[test]
fn normalized_diff_preserves_delete_sequence_and_trusted_identity() {
    let raw = bytes(SAMPLES[3]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    let n = normalize_depth::<4>(&expected(), &f, 77).unwrap();
    assert_eq!(n.identity(), expected().identity);
    assert_eq!(
        n.kind(),
        DepthKind::Diff {
            first: 43,
            last: 44
        }
    );
    assert_eq!(
        n.bids(),
        &[Level {
            price_ticks: 6500000,
            quantity_lots: 0
        }]
    );
    assert_eq!(
        n.asks(),
        &[Level {
            price_ticks: 6500100,
            quantity_lots: 500
        }]
    );
    assert_eq!(n.received_mono_ns(), 77);
    assert_eq!(n.coverage(), None);
}
#[test]
fn partial_depth_never_claims_full_or_rest_baseline_coverage() {
    let raw = bytes(SAMPLES[2]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    let n = normalize_depth::<4>(&expected(), &f, 77).unwrap();
    assert_eq!(n.kind(), DepthKind::Partial { last: 42 });
    assert_eq!(
        n.coverage(),
        Some((Coverage::Through(6499900), Coverage::Through(6500200)))
    );
    let mut empty = raw[..26].to_vec();
    empty.extend_from_slice(&[16, 0, 0, 0, 16, 0, 0, 0, 7]);
    empty.extend_from_slice(b"BTCUSDT");
    let f = decode_frame(&empty, Limits::default()).unwrap();
    assert_eq!(
        normalize_depth::<4>(&expected(), &f, 77)
            .unwrap()
            .coverage(),
        Some((Coverage::Unknown, Coverage::Unknown))
    );
    let mut over = raw;
    over[28..30].copy_from_slice(&21u16.to_le_bytes());
    assert!(matches!(
        decode_frame(&over, Limits::default()),
        Err(Error::Capacity)
    ));
}
#[test]
fn normalization_rejects_wrong_identity_capacity_grid_and_kind() {
    let raw = bytes(SAMPLES[2]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    assert!(matches!(
        normalize_depth::<1>(&expected(), &f, 77),
        Err(Error::Capacity)
    ));
    let mut e = expected();
    e.symbol = "ETHUSDT";
    assert!(matches!(
        normalize_depth::<4>(&e, &f, 77),
        Err(Error::Identity)
    ));
    e = expected();
    e.quantity_step = DecimalUnit::new(3, -3).unwrap();
    assert!(matches!(
        normalize_depth::<4>(&e, &f, 77),
        Err(Error::NotIntegral)
    ));
    let raw = bytes(SAMPLES[1]);
    let f = decode_frame(&raw, Limits::default()).unwrap();
    assert!(matches!(
        normalize_depth::<4>(&expected(), &f, 77),
        Err(Error::Kind)
    ));
}
#[test]
fn partial_sorting_positive_quantity_and_non_crossed_book() {
    for (offset, value) in [(30 + 16, 6500000i64), (30 + 8, 0), (66, 6500000)] {
        let mut raw = bytes(SAMPLES[2]);
        raw[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        let result = decode_frame(&raw, Limits::default())
            .and_then(|f| normalize_depth::<4>(&expected(), &f, 1));
        assert!(matches!(result, Err(Error::Value)));
    }
}
#[test]
fn integer_grid_conversion_matches_small_independent_rationals() {
    for m in [-1000i64, -25, -1, 0, 1, 25, 1000] {
        for e in -6i8..=6 {
            for u in [1i128, 2, 5, 10, 25] {
                for ue in -6i8..=6 {
                    let delta = i16::from(e) - i16::from(ue);
                    let (n, d) = if delta >= 0 {
                        (i128::from(m) * 10i128.pow(delta as u32), u)
                    } else {
                        (i128::from(m), u * 10i128.pow((-delta) as u32))
                    };
                    let want = if n % d == 0 {
                        Ok((n / d) as i64)
                    } else {
                        Err(Error::NotIntegral)
                    };
                    assert_eq!(
                        decimal_to_units(m, e, DecimalUnit::new(u, ue).unwrap()),
                        want,
                        "{m}e{e}/{u}e{ue}"
                    );
                }
            }
        }
    }
    assert_eq!(
        decimal_to_units(1, 127, DecimalUnit::new(1, -127).unwrap()),
        Err(Error::Overflow)
    );
    assert_eq!(
        decimal_to_units(1, -127, DecimalUnit::new(1, 127).unwrap()),
        Err(Error::NotIntegral)
    );
    assert_eq!(
        decimal_to_units(0, 127, DecimalUnit::new(1, -127).unwrap()),
        Ok(0)
    );
    assert_eq!(
        decimal_to_units(1, -128, DecimalUnit::new(1, 0).unwrap()),
        Err(Error::Value)
    );
}
#[test]
fn arbitrary_short_inputs_do_not_panic() {
    let mut state = 0x12345678u64;
    for n in 0..300 {
        let raw = (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        assert!(std::panic::catch_unwind(|| decode_frame(&raw, Limits::default())).is_ok());
    }
}
