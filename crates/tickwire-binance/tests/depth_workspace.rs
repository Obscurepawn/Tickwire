#![forbid(unsafe_code)]
mod common;
#[path = "depth_workspace/reference.rs"]
mod reference;
use tickwire_binance::{Error, feed::*, request::Market};
use tickwire_book::BookKey;

fn context() -> DepthContext {
    let mut instrument = common::command().context.instrument;
    instrument.instrument_id = u64::MAX;
    DepthContext::new(
        Market::Spot,
        instrument,
        BookKey {
            venue: 1,
            instrument: 0,
            source: 1,
        },
        7,
        TimeUnit::Microseconds,
    )
    .unwrap()
}
fn binary() -> Vec<u8> {
    include_str!("../../tickwire-binance-sbe/tests/fixtures/diff.hex")
        .trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
const JSON: &[u8] = br#"{"e":"depthUpdate","E":123,"s":"BTCUSDT","U":43,"u":44,"b":[["65000","0"]],"a":[["65001","0.5"]]}"#;

#[test]
fn reusable_workspace_decodes_json_and_binary_without_allocations() {
    let context = context();
    let raw = binary();
    let mut workspace = DepthWorkspace::new(4096).unwrap();
    let measured = allocation_counter::measure(|| {
        for _ in 0..8 {
            let update = decode_diff_into(JSON, &context, 50, 4096, &mut workspace).unwrap();
            assert_eq!(update.event().key.instrument, 0);
            assert_eq!(update.event().last_update_id, 44);
            assert_eq!(update.event().bids[0].quantity.0, 0);
            assert_eq!(update.event_time_ns(), Some(123_000));
        }
    });
    assert_eq!(measured.count_total, 0, "JSON {measured:?}");
    let measured = allocation_counter::measure(|| {
        for _ in 0..8 {
            let update = decode_sbe_diff_into(&raw, &context, 51, 4096, &mut workspace).unwrap();
            assert_eq!(update.event().asks[0].quantity.0, 50000);
            assert_eq!(update.event().last_update_id, 44);
        }
    });
    assert_eq!(measured.count_total, 0);
}

#[test]
fn workspace_constructor_rejects_unbounded_capacities() {
    for n in [0, MAX_DEPTH_LEVELS + 1, usize::MAX] {
        assert!(matches!(DepthWorkspace::new(n), Err(Error::Capacity)));
    }
}

#[test]
fn valid_escaped_keys_symbols_prices_and_unknown_values_do_not_allocate() {
    let context = context();
    let raw = br#"{"str\u0065am":"btcusdt@depth@100ms","data":{"\u0065":"depth\u0055pdate","E":123,"s":"\u0042TCUSDT","U":43,"u":44,"b":[["65\u003000","\u0030"]],"a":[["65001","0.5"]],"future":{"\u006b":["\"\\\/\b\f\n\r\t\uD83D\uDE00",null,true,false,1.25e-3,{"nested":[]}]}}}"#;
    let old = decode_diff::<8>(raw, &context, 50, 4096).unwrap();
    let mut workspace = DepthWorkspace::new(8).unwrap();
    let measured = allocation_counter::measure(|| {
        let update = decode_diff_into(raw, &context, 50, 4096, &mut workspace).unwrap();
        assert_eq!(update.event().bids, old.event().bids);
        assert_eq!(update.event().asks, old.event().asks);
        assert_eq!(update.event_time_ns(), old.event_time_ns());
    });
    assert_eq!(measured.count_total, 0);
}

fn maximum_json() -> Vec<u8> {
    let bids = (0..2048)
        .map(|i| format!("[\"{}\",\"1\"]", 65000 - i))
        .collect::<Vec<_>>()
        .join(",");
    let asks = (0..2048)
        .map(|i| format!("[\"{}\",\"2\"]", 65001 + i))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"e":"depthUpdate","E":123,"s":"BTCUSDT","U":43,"u":44,"b":[{bids}],"a":[{asks}]}}"#
    )
    .into_bytes()
}
fn maximum_binary() -> Vec<u8> {
    let original = binary();
    let mut raw = original[..34].to_vec();
    for side in 0..2 {
        raw.extend_from_slice(&16u16.to_le_bytes());
        raw.extend_from_slice(&2048u16.to_le_bytes());
        for i in 0..2048i64 {
            let price = if side == 0 {
                6500000 - i * 100
            } else {
                6500100 + i * 100
            };
            raw.extend_from_slice(&price.to_le_bytes());
            raw.extend_from_slice(&1000i64.to_le_bytes());
        }
    }
    raw.push(7);
    raw.extend_from_slice(b"BTCUSDT");
    raw
}

#[test]
fn maximum_depth_workspace_fits_a_two_mib_worker_stack() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "maximum_depth_workspace_stack_child",
            "--nocapture",
        ])
        .env("TICKWIRE_DEPTH_WORKSPACE_STACK_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
#[test]
fn maximum_depth_workspace_stack_child() {
    if std::env::var_os("TICKWIRE_DEPTH_WORKSPACE_STACK_CHILD").is_none() {
        return;
    }
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let context = context();
            let json = maximum_json();
            let binary = maximum_binary();
            let mut workspace = DepthWorkspace::new(4096).unwrap();
            assert_eq!(workspace.capacity(), 4096);
            assert_eq!(
                workspace.reserved_bytes(),
                4096 * std::mem::size_of::<tickwire_book::Level>()
            );
            let bytes = workspace.reserved_bytes();
            let measured = allocation_counter::measure(|| {
                for _ in 0..4 {
                    let update =
                        decode_diff_into(&json, &context, 50, 1 << 20, &mut workspace).unwrap();
                    assert_eq!(update.event().bids.len(), 2048);
                    assert_eq!(update.event().asks.len(), 2048);
                    assert_eq!(update.event().bids[2047].price.0, (65000 - 2047) * 100);
                    let update =
                        decode_sbe_diff_into(&binary, &context, 51, 1 << 20, &mut workspace)
                            .unwrap();
                    assert_eq!(update.event().bids.len(), 2048);
                    assert_eq!(update.event().asks[2047].price.0, (65001 + 2047) * 100);
                }
            });
            assert_eq!(measured.count_total, 0);
            assert_eq!(workspace.reserved_bytes(), bytes);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn capacity_is_combined_and_a_failed_decode_does_not_leak_a_prefix() {
    let context = context();
    let mut workspace = DepthWorkspace::new(1).unwrap();
    assert!(matches!(
        decode_diff_into(JSON, &context, 50, 4096, &mut workspace),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        decode_sbe_diff_into(&binary(), &context, 50, 4096, &mut workspace),
        Err(Error::Capacity)
    ));
    let one =
        br#"{"e":"depthUpdate","E":123,"s":"BTCUSDT","U":45,"u":45,"b":[],"a":[["65002","1"]]}"#;
    let update = decode_diff_into(one, &context, 52, 4096, &mut workspace).unwrap();
    assert!(update.event().bids.is_empty());
    assert_eq!(update.event().asks.len(), 1);
    assert_eq!(update.event().asks[0].price.0, 6500200);
    assert_eq!(update.event().last_update_id, 45);
    let empty = br#"{"e":"depthUpdate","E":123,"s":"BTCUSDT","U":46,"u":46,"b":[],"a":[]}"#;
    let update = decode_diff_into(empty, &context, 53, 4096, &mut workspace).unwrap();
    assert!(update.event().asks.is_empty());
}

#[test]
fn invalid_json_syntax_duplicate_keys_and_grid_values_remain_rejected() {
    let context = context();
    let mut workspace = DepthWorkspace::new(8).unwrap();
    let source = std::str::from_utf8(JSON).unwrap();
    let cases = [
        source.replace("\"u\":44", "\"u\":44,\"\\u0075\":45"),
        source.replace("\"E\":123", "\"E\":01"),
        source.replace("\"E\":123", "\"E\":1e0"),
        source.replace("65000", "65000.001"),
        source.replace("BTCUSDT", "ETHUSDT"),
        source.replace("65000", "\\uD800"),
        source.replace("65000", "\\x31"),
        source.replace("65000", "\\uDC00"),
        source.replace("65000", "\\uD800\\u1234"),
        source.replace("65000", "\n"),
        source.replace("\"a\":", "\"unknown\":[1,],\"a\":"),
        source.replace("\"a\":", "\"unknown\":{\"x\":1,},\"a\":"),
        source.replace("\"a\":", "\"unknown\":truefalse,\"a\":"),
        source.replace("\"a\":", "\"unknown\":1e+,\"a\":"),
        format!("{source} null"),
    ];
    for raw in cases {
        assert!(
            decode_diff_into(raw.as_bytes(), &context, 50, 4096, &mut workspace).is_err(),
            "{raw}"
        );
        assert!(
            decode_diff::<8>(raw.as_bytes(), &context, 50, 4096).is_err(),
            "{raw}"
        );
    }
    for end in 0..JSON.len() {
        assert!(decode_diff_into(&JSON[..end], &context, 50, 4096, &mut workspace).is_err());
    }
    let mut invalid_utf8 = JSON.to_vec();
    invalid_utf8[0] = 255;
    assert!(decode_diff_into(&invalid_utf8, &context, 50, 4096, &mut workspace).is_err());
    let update = decode_diff_into(JSON, &context, 50, 4096, &mut workspace).unwrap();
    assert_eq!(update.event().bids.len(), 1);
}

#[test]
fn unknown_nested_fields_are_bounded_and_cannot_change_identity() {
    let context = context();
    let mut workspace = DepthWorkspace::new(8).unwrap();
    let source = std::str::from_utf8(JSON).unwrap();
    let within = source.replace(
        "\"a\":",
        &format!("\"unknown\":{}0{},\"a\":", "[".repeat(127), "]".repeat(127)),
    );
    let measured = allocation_counter::measure(|| {
        assert_eq!(
            decode_diff_into(within.as_bytes(), &context, 50, 4096, &mut workspace)
                .unwrap()
                .event()
                .key
                .instrument,
            0
        );
    });
    assert_eq!(measured.count_total, 0);
    let beyond = source.replace(
        "\"a\":",
        &format!("\"unknown\":{}0{},\"a\":", "[".repeat(128), "]".repeat(128)),
    );
    // The former RawValue path accepts arbitrarily nested ignored fields. The new
    // bounded grammar deliberately caps total containers at 128, including root.
    assert!(reference::decode(within.as_bytes(), Market::Spot).is_ok());
    assert!(reference::decode(beyond.as_bytes(), Market::Spot).is_ok());
    assert!(matches!(
        decode_diff_into(beyond.as_bytes(), &context, 50, 4096, &mut workspace),
        Err(Error::Capacity)
    ));
}

#[test]
fn maximum_total_plus_one_rejects_both_encodings_and_memory_accounting_is_exact() {
    let context = context();
    let instrument = common::command().context.instrument;
    let expected = instrument.symbol.as_str().len() * 2
        + instrument.base.symbol.as_str().len()
        + instrument.quote.symbol.as_str().len()
        + instrument.settlement.symbol.as_str().len();
    assert_eq!(context.reserved_bytes(), expected);
    let mut workspace = DepthWorkspace::new(4096).unwrap();
    let before = workspace.reserved_bytes();
    let mut json = maximum_json();
    let split = json.len() - 2;
    json.splice(split..split, b",[\"70000\",\"1\"]".iter().copied());
    assert!(matches!(
        decode_diff_into(&json, &context, 50, 1 << 20, &mut workspace),
        Err(Error::Capacity)
    ));
    let mut binary = maximum_binary();
    let ask_header = 34 + 4 + 2048 * 16;
    binary[ask_header + 2..ask_header + 4].copy_from_slice(&2049u16.to_le_bytes());
    let mut level = [0u8; 16];
    level[..8].copy_from_slice(&7000000i64.to_le_bytes());
    level[8..].copy_from_slice(&1000i64.to_le_bytes());
    let split = binary.len() - 8;
    binary.splice(split..split, level);
    assert!(matches!(
        decode_sbe_diff_into(&binary, &context, 50, 1 << 20, &mut workspace),
        Err(Error::Capacity)
    ));
    assert_eq!(
        decode_diff_into(JSON, &context, 51, 4096, &mut workspace)
            .unwrap()
            .event()
            .asks
            .len(),
        1
    );
    assert_eq!(workspace.reserved_bytes(), before);
}

#[test]
fn bounded_json_matches_independent_serde_oracle() {
    fn compare(raw: &[u8], context: &DepthContext, market: Market) {
        let expected = reference::decode(raw, market);
        let mut workspace = DepthWorkspace::new(8).unwrap();
        let actual = decode_diff_into(raw, context, 50, 1 << 20, &mut workspace);
        assert_eq!(
            actual.is_ok(),
            expected.is_ok(),
            "acceptance: {}",
            String::from_utf8_lossy(raw)
        );
        if let (Ok(actual), Ok(expected)) = (actual, expected) {
            assert_eq!(
                reference::Facts::borrowed(&actual),
                expected,
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }
    for market in [Market::Spot, Market::Usdm] {
        let context = if market == Market::Spot {
            context()
        } else {
            let mut instrument = common::command().context.instrument;
            instrument.contract = tickwire_venue::ContractKind::Perpetual;
            DepthContext::new(
                market,
                instrument,
                BookKey {
                    venue: 1,
                    instrument: 0,
                    source: 1,
                },
                7,
                TimeUnit::Milliseconds,
            )
            .unwrap()
        };
        let mut source = std::str::from_utf8(JSON).unwrap().to_owned();
        if market == Market::Usdm {
            source = source.replace("\"E\":123", "\"E\":123,\"T\":122,\"pu\":42,\"st\":1");
        }
        compare(source.as_bytes(), &context, market);
        for escaped in [
            source.replace("\"s\":", "\"\\u0073\":"),
            source.replace("BTCUSDT", "\\u0042TCUSDT"),
            source.replace("depthUpdate", "depth\\u0055pdate"),
            source.replace("65000", "65\\u003000"),
            source.replace("65000", "65000.000000"),
            source.replace(
                "\"a\":",
                "\"future\":{\"𝄞\":[\"\\uD834\\uDD1E\",\"\\n\",-0,0e0,true,null,[]]},\"a\":",
            ),
        ] {
            compare(escaped.as_bytes(), &context, market);
        }
        for number in [
            "0",
            "-0",
            "0e0",
            "00",
            "01",
            "1.0",
            "+1",
            "-1",
            "1e+",
            "1e9999",
            "null",
            "true",
            "\"1\"",
            "18446744073709551615",
            "18446744073709551616",
        ] {
            for (field, value) in [("E", "123"), ("U", "43"), ("u", "44")] {
                let raw = source.replace(
                    &format!("\"{field}\":{value}"),
                    &format!("\"{field}\":{number}"),
                );
                compare(raw.as_bytes(), &context, market);
            }
        }
        for extra in [
            "\"T\":null",
            "\"pu\":null",
            "\"st\":null",
            "\"stream\":null",
            "\"data\":null",
            "\"stream\":null,\"data\":null",
            "\"T\":null,\"T\":null",
            "\"U\":43",
            "\"\\u0075\":44",
            "\"future\":[1,]",
            "\"future\":{\"key\":null,}",
            "\"future\":00",
            "\"future\":-0",
            "\"future\":0e0",
            "\"future\":1e9999",
            "\"future\":{\"x\":1,\"x\":2}",
        ] {
            let raw = source.replace("\"a\":", &format!("{extra},\"a\":"));
            compare(raw.as_bytes(), &context, market);
        }
        for stream in [
            "btcusdt@depth",
            "btcusdt@depth@100ms",
            "btcusdt@depth@1000ms",
            "btcusdt@depth@500ms",
            "btcusdt@depth5",
            "BTCUSDT@depth",
            "ethusdt@depth",
            "\\u0062tcusdt@depth",
        ] {
            compare(
                format!(r#"{{"stream":"{stream}","data":{source}}}"#).as_bytes(),
                &context,
                market,
            );
        }
        for stream in ["null", "false", "{}", "[]", "1"] {
            for data in ["null", "false", "{}", "[]", "1", &source] {
                compare(
                    format!(r#"{{"stream":{stream},"data":{data}}}"#).as_bytes(),
                    &context,
                    market,
                );
            }
        }
        // A deterministic grammar mutation corpus supplies independently judged invalid inputs.
        for at in 0..source.len() {
            for &byte in b" \"\\,:0e[]{}" {
                let mut raw = source.as_bytes().to_vec();
                raw[at] = byte;
                compare(&raw, &context, market);
            }
        }
    }
}
