#![forbid(unsafe_code)]

use tickwire_book::BookKey;
use tickwire_md::spot_quotes::{Publication, QuoteConfig, Session, simd_json::Backend};

const BYBIT_ACK: &[u8] =
    br#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"twbbo","conn_id":"id"}"#;
const OKX_ACK: &[u8] = br#"{"id":"twbbo","event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"connId":"id"}"#;

fn config(venue: &str, symbol: &str, price_tick: &str, qty_step: &str) -> QuoteConfig {
    serde_json::from_value(serde_json::json!({
        "venue": venue,
        "symbol": symbol,
        "instrument_id": 21,
        "instrument_version": 3,
        "price_tick": price_tick,
        "quantity_step": qty_step,
    }))
    .unwrap()
}

fn backends() -> Vec<Backend> {
    let mut list = vec![Backend::detect(), Backend::swar()];
    #[cfg(target_arch = "x86_64")]
    {
        list.push(Backend::sse2());
        if let Some(avx2) = Backend::avx2_if_supported() {
            list.push(avx2);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        list.push(Backend::neon());
    }
    list
}

fn pub_eq(a: Publication, b: Publication) -> bool {
    match (a, b) {
        (Publication::Quote(qa), Publication::Quote(qb)) => {
            qa.key == qb.key
                && qa.instrument == qb.instrument
                && qa.version == qb.version
                && qa.epoch == qb.epoch
                && qa.sequence == qb.sequence
                && qa.exchange_ns == qb.exchange_ns
                && qa.received_ns == qb.received_ns
                && qa.bid_ticks == qb.bid_ticks
                && qa.bid_lots == qb.bid_lots
                && qa.ask_ticks == qb.ask_ticks
                && qa.ask_lots == qb.ask_lots
        }
        (
            Publication::Unavailable {
                instrument: ia,
                epoch: ea,
                received_ns: ra,
            },
            Publication::Unavailable {
                instrument: ib,
                epoch: eb,
                received_ns: rb,
            },
        ) => ia == ib && ea == eb && ra == rb,
        _ => false,
    }
}

/// Runs an identical frame sequence through `decode_fast` (across every SIMD/SWAR backend)
/// and `decode_serde_oracle`, asserting exact equivalence of return values, `Publication`
/// spread/unavailability classification, and sticky session failure/ack state.
fn assert_sequence_matches_oracle(cfg: &QuoteConfig, frames: &[&[u8]]) {
    let key = BookKey {
        venue: cfg.venue.id(),
        instrument: 0,
        source: 7,
    };
    for backend in backends() {
        let mut fast = Session::with_backend(cfg, 3, backend).unwrap();
        let mut oracle = Session::with_backend(cfg, 3, backend).unwrap();
        for (idx, &frame) in frames.iter().enumerate() {
            let received = (idx as u64 + 1) * 100;
            let res_fast = fast.decode_fast(frame, received);
            let res_oracle = oracle.decode_serde_oracle(frame, received);
            assert_eq!(
                res_fast.as_ref().map_err(|_| ()),
                res_oracle.as_ref().map_err(|_| ()),
                "backend={backend:?} step={idx} frame={:?}\nfast={res_fast:?}\noracle={res_oracle:?}",
                String::from_utf8_lossy(frame)
            );
            if let (Ok(Some(qf)), Ok(Some(qo))) = (&res_fast, &res_oracle) {
                let pf = qf.publication(key).unwrap();
                let po = qo.publication(key).unwrap();
                assert!(
                    pub_eq(pf, po),
                    "publication mismatch at step={idx}: {pf:?} vs {po:?}"
                );
            }
        }
        // Probe one extra valid frame afterwards to verify `acknowledged` and `failed` match too.
        let probe: &[u8] = if cfg.venue.id() == 2 {
            br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#
        } else {
            br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["10.05","0.003","0","1"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":2}]}"#
        };
        assert_eq!(
            fast.decode_fast(probe, 9999).map_err(|_| ()),
            oracle.decode_serde_oracle(probe, 9999).map_err(|_| ()),
            "post-sequence state mismatch for backend={backend:?}"
        );
    }
}

fn assert_acked_frame_matches_oracle(cfg: &QuoteConfig, ack: &[u8], frame: &[u8]) {
    assert_sequence_matches_oracle(cfg, &[ack, frame]);
}

#[test]
fn normal_snapshots_deltas_and_simd_chunk_boundaries_match_oracle_with_zero_allocs() {
    let bybit_cfg = config("bybit", "BTCUSDT", "0.05", "0.001");
    let okx_cfg = config("okx", "BTC-USDT", "0.05", "0.001");

    // Sweep unknown padding lengths across 0..=260 so quotes, braces, and digits cross every
    // 8B (SWAR), 16B (SSE2/NEON), 32B (AVX2), and 64B (Scanner block) boundary.
    for pad_len in 0..=260 {
        let pad = "p".repeat(pad_len);
        let bybit_snapshot = format!(
            r#"{{"pad":"{pad}","topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":314,"seq":981}},"cts":1700000000122}}"#
        );
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, bybit_snapshot.as_bytes());

        // Bybit `delta` frame (rejected identically by fast path and oracle).
        let bybit_delta = format!(
            r#"{{"pad":"{pad}","topic":"orderbook.1.BTCUSDT","type":"delta","ts":1700000000123,"data":{{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[],"u":315,"seq":982}}}}"#
        );
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, bybit_delta.as_bytes());

        let okx_snapshot = format!(
            r#"{{"pad":"{pad}","arg":{{"channel":"bbo-tbt","instId":"BTC-USDT"}},"data":[{{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":482}}]}}"#
        );
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, okx_snapshot.as_bytes());
    }

    // Verify 0 steady-state heap allocations across every backend.
    let bybit_hot = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":314,"seq":981},"cts":1700000000122}"#;
    let okx_hot = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":482}]}"#;
    for backend in backends() {
        let mut sb = Session::with_backend(&bybit_cfg, 1, backend).unwrap();
        sb.on_text(BYBIT_ACK, 1).unwrap();
        let allocs_b = allocation_counter::measure(|| {
            let q = sb.on_text(bybit_hot, 2).unwrap().unwrap();
            assert_eq!(q.bid.unwrap().price_ticks, 201);
        });
        assert_eq!(allocs_b.count_total, 0, "bybit backend={backend:?}");

        let mut so = Session::with_backend(&okx_cfg, 1, backend).unwrap();
        so.on_text(OKX_ACK, 1).unwrap();
        let allocs_o = allocation_counter::measure(|| {
            let q = so.on_text(okx_hot, 2).unwrap().unwrap();
            assert_eq!(q.ask.unwrap().price_ticks, 202);
        });
        assert_eq!(allocs_o.count_total, 0, "okx backend={backend:?}");
    }
}

#[test]
fn reordered_keys_whitespace_unknown_fields_and_duplicates_match_oracle() {
    let bybit_cfg = config("bybit", "BTCUSDT", "0.05", "0.001");
    let okx_cfg = config("okx", "BTC-USDT", "0.05", "0.001");

    let bybit_cases: &[&[u8]] = &[
        // Completely reversed keys + tabs/newlines/CRLF + rich unknown nested values
        b" \r\n\t {\n \"extra_obj\" : {\"nested\":[true,false,null,-42,0,1.25e-3,\"str\"]} ,\n \"data\" : {\t\"a\" : [ [ \"10.10\" , \"0.004\" ] ] , \"u\" : 77 , \"b\" : [ [ \"10.05\" , \"0.003\" ] ] , \"seq\" : 88 , \"unknown\" : [1,2,3] , \"s\" : \"BTCUSDT\" } ,\r\n \"ts\" : 1700000000123 , \"type\" : \"snapshot\" , \"topic\" : \"orderbook.1.BTCUSDT\" } \t\r\n ",
        // Unknown 40-digit integer field
        br#"{"big":9999999999999999999999999999999999999999,"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[],"a":[],"u":1,"seq":1}}"#,
        // Duplicate unknown key allowed by serde_json
        br#"{"cts":1,"cts":2,"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#,
        // Duplicate known top-level key rejected by serde_json
        br#"{"ts":1,"ts":2,"topic":"orderbook.1.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[],"a":[],"u":1,"seq":2}}"#,
        // Duplicate known nested key rejected by serde_json
        br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","u":1,"u":2,"b":[],"a":[],"seq":2}}"#,
        // Form feed 0x0C is NOT valid JSON whitespace
        b"\x0c{\"op\":\"pong\"}",
        b"{\"op\":\"pong\"}\x0c",
        // Trailing garbage after root object
        br#"{"op":"pong"} trailing"#,
        // Trailing commas in object and array
        br#"{"op":"pong",}"#,
        br#"{"extra":[1,2,],"op":"pong"}"#,
    ];
    for case in bybit_cases {
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, case);
    }

    let okx_cases: &[&[u8]] = &[
        // Reordered OKX keys, explicit `"action":"snapshot"`, extra fields inside arg & book
        b"\n{\t\"data\" : [ { \"seqId\" : 555 , \"extra\" : {\"k\":null} , \"asks\" : [ [ \"10.10\" , \"0.004\" , \"0\" , \"1\" ] ] , \"ts\" : \"1700000000123\" , \"bids\" : [ [ \"10.05\" , \"0.003\" , \"0\" , \"2\" ] ] } ] , \"action\" : \"snapshot\" , \"arg\" : { \"instId\" : \"BTC-USDT\" , \"extra\" : true , \"channel\" : \"bbo-tbt\" } }\n",
        // `"action": null` on OKX Option<&str>
        br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"action":null,"data":[{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":9}]}"#,
        // Duplicate known key in OkxArg / OkxBook
        br#"{"arg":{"channel":"bbo-tbt","channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[],"asks":[],"ts":"1","seqId":1}]}"#,
        br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[],"asks":[],"ts":"1","seqId":1,"seqId":2}]}"#,
        // Empty `data: []` and two-element `data: [{},{}]`
        br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[]}"#,
        br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[],"asks":[],"ts":"1","seqId":1},{"bids":[],"asks":[],"ts":"1","seqId":2}]}"#,
    ];
    for case in okx_cases {
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, case);
    }
}

#[test]
fn escaped_strings_control_chars_and_utf8_cases_match_oracle() {
    let bybit_cfg = config("bybit", "BTCUSDT", "0.05", "0.001");
    let okx_cfg = config("okx", "BTC-USDT", "0.05", "0.001");

    let bybit_cases: &[&[u8]] = &[
        // Escaped strings inside borrowed `&str` fields vs unknown fields
        br#"{"note":"quote \" and slash \\ and unicode \u0031","topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#,
        br#"{"\u0074opic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#,
        br#"{"topic":"orderbook.1.BTCUSDT","type":"snap\u0073hot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#,
        br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.\u00305","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}"#,
        // Invalid JSON escape
        br#"{"note":"\x20","op":"pong"}"#,
        // Raw unescaped control chars inside strings (0x00, 0x09, 0x0A, 0x1F) vs valid DEL (0x7F)
        b"{\"note\":\"\t\",\"op\":\"pong\"}",
        b"{\"note\":\"\n\",\"op\":\"pong\"}",
        b"{\"note\":\"\x00\",\"op\":\"pong\"}",
        b"{\"note\":\"\x1f\",\"op\":\"pong\"}",
        b"{\"note\":\"\x7f\",\"op\":\"pong\"}",
        // Valid UTF-8 vs invalid UTF-8 inside unknown fields
        "{\"note\":\"🚀μ\",\"topic\":\"orderbook.1.BTCUSDT\",\"type\":\"snapshot\",\"ts\":1700000000123,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"10.05\",\"0.003\"]],\"a\":[[\"10.10\",\"0.004\"]],\"u\":1,\"seq\":2}}".as_bytes(),
        b"{\"note\":\"\xff\xfe\",\"op\":\"pong\"}",
    ];
    for case in bybit_cases {
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, case);
    }

    let okx_cases: &[&[u8]] = &[
        br#"{"note":"a\"b\\c\u0031","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":482}]}"#,
        br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["10.05","0.003","\"","2"]],"asks":[],"ts":"1700000000123","seqId":482}]}"#,
    ];
    for case in okx_cases {
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, case);
    }
}

#[test]
fn truncated_frames_at_every_byte_offset_match_oracle() {
    let bybit_cfg = config("bybit", "BTCUSDT", "0.05", "0.001");
    let okx_cfg = config("okx", "BTC-USDT", "0.05", "0.001");

    let bybit_full = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":314,"seq":981},"cts":1700000000122}"#;
    for end in 0..=bybit_full.len() {
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, &bybit_full[..end]);
    }

    let okx_full = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"action":"snapshot","data":[{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":482}]}"#;
    for end in 0..=okx_full.len() {
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, &okx_full[..end]);
    }
}

#[test]
fn numeric_overflow_malformed_decimals_spreads_and_control_messages_match_oracle() {
    let bybit_cfg = config("bybit", "BTCUSDT", "0.05", "0.001");
    let okx_cfg = config("okx", "BTC-USDT", "0.05", "0.001");

    // Exhaustive price/quantity strings tested on both Bybit and OKX.
    let bad_or_edge_levels = [
        ("", "0.001"),
        ("10.05", ""),
        (".", "0.001"),
        (".05", "0.001"),
        ("10.", "0.001"),
        ("10..05", "0.001"),
        ("+10.05", "0.001"),
        (" 10.05", "0.001"),
        ("10.05 ", "0.001"),
        ("1e2", "0.001"),
        ("0", "0.001"),
        ("0.00", "0.001"),
        ("10.05", "0"),
        ("10.05", "0.000"),
        ("-0", "0.001"),
        ("-0.00", "0.001"),
        ("-10.05", "0.001"),
        ("10.05", "-0.001"),
        ("010.05", "000.003"),  // leading zeros inside decimal string
        ("10.0500", "0.00300"), // exact trailing fractional zeros
        ("10.051", "0.003"),    // off-grid price tick
        ("10.05", "0.0035"),    // off-grid quantity lot
        // Exact i64::MAX ticks (9223372036854775807 * 0.05 = 461168601842738790.35) vs +1 overflow
        ("461168601842738790.35", "0.001"),
        ("461168601842738790.40", "0.001"),
        // u64::MAX, i128::MAX, and > i128::MAX
        ("18446744073709551615", "0.001"),
        ("170141183460469231731687303715884105727", "0.001"),
        ("170141183460469231731687303715884105728", "0.001"),
        ("999999999999999999999999999999999999999999", "0.001"),
    ];
    for (p, q) in bad_or_edge_levels {
        let bf = format!(
            r#"{{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{{"s":"BTCUSDT","b":[["{p}","{q}"]],"a":[["500000000000000000.00","0.002"]],"u":1,"seq":1}}}}"#
        );
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, bf.as_bytes());
        let of = format!(
            r#"{{"arg":{{"channel":"bbo-tbt","instId":"BTC-USDT"}},"data":[{{"bids":[["{p}","{q}","0","1"]],"asks":[["500000000000000000.00","0.002","0","1"]],"ts":"1700000000123","seqId":1}}]}}"#
        );
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, of.as_bytes());
    }

    // Crossed spread (`bid > ask`), zero spread (`bid == ask`), one-sided, and empty-sided books.
    for (bid_s, ask_s) in [
        (r#"[["10.10","0.003"]]"#, r#"[["10.05","0.004"]]"#),
        (r#"[["10.10","0.003"]]"#, r#"[["10.10","0.004"]]"#),
        (r#"[["10.05","0.003"]]"#, "[]"),
        ("[]", r#"[["10.10","0.004"]]"#),
        ("[]", "[]"),
        // Multi-level depth rejected
        (
            r#"[["10.05","0.003"],["10.00","0.002"]]"#,
            r#"[["10.10","0.004"]]"#,
        ),
        // Wrong column count rejected
        (r#"[["10.05"]]"#, r#"[["10.10","0.004"]]"#),
        (r#"[["10.05","0.003","99"]]"#, r#"[["10.10","0.004"]]"#),
    ] {
        let bf = format!(
            r#"{{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{{"s":"BTCUSDT","b":{bid_s},"a":{ask_s},"u":1,"seq":2}}}}"#
        );
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, bf.as_bytes());
    }

    // Timestamp & sequence boundary values: 0, leading zero, negative, float, u64::MAX, u64::MAX + 1, ns overflow.
    for ts_lit in [
        "0",
        "00",
        "0123",
        "-1",
        "123.0",
        "1e5",
        "18446744073709",       // max millis that does not overflow * 1_000_000
        "18446744073710",       // overflows millis * 1_000_000
        "18446744073709551615", // u64::MAX
        "18446744073709551616", // u64::MAX + 1
    ] {
        let bf = format!(
            r#"{{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":{ts_lit},"data":{{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":1,"seq":2}}}}"#
        );
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, bf.as_bytes());

        let of = format!(
            r#"{{"arg":{{"channel":"bbo-tbt","instId":"BTC-USDT"}},"data":[{{"bids":[["10.05","0.003","0","1"]],"asks":[["10.10","0.004","0","1"]],"ts":"{ts_lit}","seqId":2}}]}}"#
        );
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, of.as_bytes());

        let of_plus = format!(
            r#"{{"arg":{{"channel":"bbo-tbt","instId":"BTC-USDT"}},"data":[{{"bids":[["10.05","0.003","0","1"]],"asks":[["10.10","0.004","0","1"]],"ts":"+{ts_lit}","seqId":2}}]}}"#
        );
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, of_plus.as_bytes());
    }

    // Control frames, heartbeats, wrong channel/symbol, unacknowledged snapshots, rejected subscribes.
    let bybit_controls: &[&[u8]] = &[
        br#"{"op":"pong"}"#,
        br#"{"op":"ping","success":true,"ret_msg":"pong"}"#,
        br#"{"op":"ping","success":false,"ret_msg":"pong"}"#,
        br#"{"op":"subscribe","req_id":"twbbo","success":false}"#,
        br#"{"op":"subscribe","req_id":"other","success":true}"#,
        br#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[],"a":[],"u":1,"seq":1}}"#,
        br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"ETHUSDT","b":[],"a":[],"u":1,"seq":1}}"#,
    ];
    for ctrl in bybit_controls {
        assert_sequence_matches_oracle(&bybit_cfg, &[ctrl]);
        assert_acked_frame_matches_oracle(&bybit_cfg, BYBIT_ACK, ctrl);
    }

    let okx_controls: &[&[u8]] = &[
        b"pong",
        b"pong ",
        b" pong",
        br#"{"event":"notice","code":"64010","msg":"upgrade"}"#,
        br#"{"event":"error","code":"60012","msg":"Invalid request"}"#,
        br#"{"event":"error"}"#,
        br#"{"id":"wrong","event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}"#,
        br#"{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"bids":[],"asks":[],"ts":"1","seqId":1}]}"#,
        br#"{"arg":{"channel":"bbo-tbt","instId":"ETH-USDT"},"data":[{"bids":[],"asks":[],"ts":"1","seqId":1}]}"#,
    ];
    for ctrl in okx_controls {
        assert_sequence_matches_oracle(&okx_cfg, &[ctrl]);
        assert_acked_frame_matches_oracle(&okx_cfg, OKX_ACK, ctrl);
    }
}
