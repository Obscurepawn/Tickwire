//! Time one native JSON frame through conversion to the publisher's integer input.
use std::{hint::black_box, time::Instant};
use tickwire_book::BookKey;
use tickwire_md::spot_quotes::{QuoteConfig, Session};

const BYBIT_ACK: &[u8] = br#"{"success":true,"op":"subscribe","req_id":"twbbo","conn_id":"id"}"#;
const OKX_ACK: &[u8] = br#"{"id":"twbbo","event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"connId":"id"}"#;
const BYBIT: &str = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["31020.1","0.035"]],"a":[["31020.2","0.120"]],"u":SEQ,"seq":SEQ},"cts":1700000000122}"#;
const OKX: &str = r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["31020.1","0.035","0","2"]],"asks":[["31020.2","0.120","0","1"]],"ts":"1700000000123","seqId":SEQ}]}"#;

fn measure(label: &str, config: &str, ack: &[u8], template: &str, count: usize) {
    let config: QuoteConfig = serde_json::from_str(config).unwrap();
    let key = BookKey {
        venue: config.venue.id(),
        instrument: 0,
        source: 1,
    };
    let frames: Vec<String> = (1000..2024)
        .map(|seq| template.replace("SEQ", &seq.to_string()))
        .collect();
    let mut samples = Vec::with_capacity(count);
    for window in 0..count.div_ceil(frames.len()) + 4 {
        let mut session = Session::new(&config, (window + 1) as u64).unwrap();
        session.on_text(ack, 1).unwrap();
        for (i, frame) in frames.iter().enumerate() {
            let start = Instant::now();
            let quote = session
                .on_text(black_box(frame.as_bytes()), i as u64 + 2)
                .unwrap()
                .unwrap();
            black_box(quote.publication(key).unwrap());
            let nanos = start.elapsed().as_nanos() as u64;
            if window >= 4 && samples.len() < count {
                samples.push(nanos);
            }
        }
    }
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(1000) - 1];
    println!(
        "{label},{count},{},{},{}",
        percentile(500),
        percentile(990),
        percentile(999)
    );
}

fn main() {
    let count = std::env::var("TICKWIRE_BENCH_SAMPLES")
        .map(|s| s.parse::<usize>().expect("positive sample count"))
        .unwrap_or(100_000);
    assert!((1..=10_000_000).contains(&count));
    println!("venue,samples,p50_ns,p99_ns,p999_ns");
    measure(
        "bybit",
        include_str!("../examples/bybit-spot-quote.json"),
        BYBIT_ACK,
        BYBIT,
        count,
    );
    measure(
        "okx",
        include_str!("../examples/okx-spot-quote.json"),
        OKX_ACK,
        OKX,
        count,
    );
}
