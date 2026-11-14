#![forbid(unsafe_code)]

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use serde_json::Value;
use std::{
    fs,
    hint::black_box,
    io::{Cursor, Read, Write},
    sync::Arc,
    time::Instant,
};
use tickwire_account::StpBook;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::InMemoryBackend;
use tickwire_gateway::Encoder as _;
use tickwire_spot_execution::{
    Credentials, RestSession, SpotEncoder, SpotVenue, authority, decode_reply,
};
use tickwire_transport::{
    TlsLoopbackWarmer,
    rustls::{self, ClientConnection, RootCertStore, ServerConnection, pki_types::ServerName},
    warmup::decode_http_response_slice,
};
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment, Side};

const BYBIT_LIVE_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 82\r\n\r\n{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{\"orderId\":\"90000001\",\"orderLinkId\":\"bench1\"}}";
const OKX_LIVE_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 82\r\n\r\n{\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"90000001\",\"clOrdId\":\"bench1\",\"sCode\":\"0\"}]}";

struct LiveInMemoryTlsPair {
    client: ClientConnection,
    server: ServerConnection,
    wire_buf: [u8; 4096],
    plain_buf: [u8; 4096],
}

impl LiveInMemoryTlsPair {
    fn new(venue: SpotVenue) -> Self {
        let host = authority(venue, Environment::Test);
        let cert = rcgen::generate_simple_self_signed(vec![host.to_owned()]).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap(),
        );
        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let server_name = ServerName::try_from(host.to_owned()).unwrap();
        let mut client = ClientConnection::new(client_cfg, server_name).unwrap();
        let mut server = ServerConnection::new(server_cfg).unwrap();
        let mut tmp = [0u8; 4096];
        while client.is_handshaking() || server.is_handshaking() {
            while client.wants_write() {
                let n = client.write_tls(&mut tmp.as_mut_slice()).unwrap();
                let mut cur = Cursor::new(&tmp[..n]);
                server.read_tls(&mut cur).unwrap();
                server.process_new_packets().unwrap();
            }
            while server.wants_write() {
                let n = server.write_tls(&mut tmp.as_mut_slice()).unwrap();
                let mut cur = Cursor::new(&tmp[..n]);
                client.read_tls(&mut cur).unwrap();
                client.process_new_packets().unwrap();
            }
        }
        Self {
            client,
            server,
            wire_buf: [0u8; 4096],
            plain_buf: [0u8; 4096],
        }
    }

    #[inline(never)]
    fn roundtrip(&mut self, req_bytes: &[u8], raw_http_ack: &[u8]) -> (u16, usize) {
        self.client.writer().write_all(req_bytes).unwrap();
        let mut wire_len = 0usize;
        while self.client.wants_write() {
            let n = self
                .client
                .write_tls(&mut (&mut self.wire_buf[wire_len..]))
                .unwrap();
            wire_len += n;
        }
        let mut cur = Cursor::new(&self.wire_buf[..wire_len]);
        while (cur.position() as usize) < wire_len {
            self.server.read_tls(&mut cur).unwrap();
            self.server.process_new_packets().unwrap();
        }
        loop {
            match self.server.reader().read(&mut self.plain_buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    black_box(&self.plain_buf[..n]);
                }
            }
        }

        self.server.writer().write_all(raw_http_ack).unwrap();
        let mut ack_wire_len = 0usize;
        while self.server.wants_write() {
            let n = self
                .server
                .write_tls(&mut (&mut self.wire_buf[ack_wire_len..]))
                .unwrap();
            ack_wire_len += n;
        }
        let mut ack_cur = Cursor::new(&self.wire_buf[..ack_wire_len]);
        while (ack_cur.position() as usize) < ack_wire_len {
            self.client.read_tls(&mut ack_cur).unwrap();
            self.client.process_new_packets().unwrap();
        }
        let mut ack_len = 0usize;
        loop {
            match self.client.reader().read(&mut self.plain_buf[ack_len..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => ack_len += n,
            }
        }
        let (status, body) = decode_http_response_slice(&self.plain_buf[..ack_len]).unwrap();
        let start_offset = self.plain_buf.len() - body.len();
        let _ = start_offset;
        (status, ack_len)
    }
}

struct CacheEvictor {
    llc_buf: Vec<u64>,
    step_seed: u64,
}

impl CacheEvictor {
    fn new() -> Self {
        // 32 MiB buffer (4,194,304 x u64) exceeds L1d + L2 + 16 MiB Zen 4 L3 cache.
        let mut llc_buf = vec![0u64; 4 * 1024 * 1024];
        for (i, slot) in llc_buf.iter_mut().enumerate() {
            *slot = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
        Self {
            llc_buf,
            step_seed: 1,
        }
    }

    #[inline(never)]
    fn thrash_llc_and_icache(&mut self) -> u64 {
        self.step_seed = self.step_seed.wrapping_add(1);
        let mut acc = self.step_seed;
        // 1. Stride-8 (64-byte cache line) read-modify-write across all 32 MiB to evict L1d/L2/LLC.
        let mut idx = 0usize;
        let len = self.llc_buf.len();
        while idx < len {
            let v = self.llc_buf[idx].wrapping_add(acc);
            self.llc_buf[idx] = v.rotate_left(7);
            acc ^= v;
            idx += 8;
        }
        // 2. Exercise unrelated SHA-512 + serde_json + branching code paths to perturb L1i & BTB.
        let mut json_scratch = [0u8; 256];
        for round in 0..24u64 {
            let seed_bytes = acc.wrapping_add(round).to_le_bytes();
            let digest = ring::digest::digest(&ring::digest::SHA512, &seed_bytes);
            acc ^= u64::from_le_bytes(digest.as_ref()[0..8].try_into().unwrap());
            let sample =
                br#"{"alpha":[1,2,3,4],"beta":{"nested":true,"tag":"evict-icache","count":42}}"#;
            json_scratch[..sample.len()].copy_from_slice(sample);
            if let Ok(val) = serde_json::from_slice::<Value>(&json_scratch[..sample.len()])
                && val["beta"]["nested"].as_bool() == Some(true)
            {
                acc = acc
                    .wrapping_mul(33)
                    .wrapping_add(val["beta"]["count"].as_u64().unwrap_or(0));
            }
        }
        black_box(acc)
    }
}

#[inline(never)]
fn execute_first_order_hot_path<B, A, T, E>(
    venue: SpotVenue,
    session: &RestSession<'_, B, A, T>,
    stp_book: &StpBook,
    live_encoder: &mut SpotEncoder<'_, E>,
    command: &CanonicalCommand,
    live_tls: &mut LiveInMemoryTlsPair,
) -> usize
where
    B: tickwire_account_journal::Backend,
    A: tickwire_account_gateway::Authority,
    T: FnMut() -> Result<u64, tickwire_gateway::Error>,
    E: FnMut() -> Result<u64, tickwire_gateway::Error>,
{
    // 1. Pre-trade read-only Account + STP check.
    let acct = session.gateway().account();
    let quote_key = fixture::key(2);
    let avail = acct
        .asset(quote_key)
        .map(|a| a.available.atoms())
        .unwrap_or(0);
    let crossed = stp_book
        .self_cross(12, Side::Buy, fixture::units(60_000))
        .is_err();
    black_box((avail, crossed));

    // 2. Live SpotEncoder HMAC-SHA256 sign + HTTP/1.1 POST framing.
    let encoded = live_encoder.encode(command).unwrap();

    // 3. Live TLS 1.3 encrypt -> peer decrypt -> 200 OK encrypt -> client decrypt + reply decode.
    let raw_ack = match venue {
        SpotVenue::Bybit => BYBIT_LIVE_ACK,
        SpotVenue::Okx => OKX_LIVE_ACK,
    };
    let (status, ack_len) = live_tls.roundtrip(encoded.bytes(), raw_ack);
    let (_, ack_body) = decode_http_response_slice(&live_tls.plain_buf[..ack_len]).unwrap();
    let reply = decode_reply(command, status, ack_body).unwrap();
    black_box(reply);
    encoded.bytes().len() + ack_len
}

fn percentile_permille(sorted: &[u64], permille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len().saturating_sub(1) * permille) / 1000;
    sorted[idx]
}

fn read_cpu_model() -> String {
    if let Ok(info) = fs::read_to_string("/proc/cpuinfo") {
        for line in info.lines() {
            if let Some((_, val)) = line.split_once(':')
                && line.starts_with("model name")
            {
                return val.trim().to_owned();
            }
        }
    }
    "unknown".to_owned()
}

fn read_proc_first_line(path: &str) -> String {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.lines().next().map(|l| l.trim().to_owned()))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn format_speedup_x100(cold_ns: u64, warm_ns: u64) -> String {
    if warm_ns == 0 {
        return "n/a".to_owned();
    }
    let x100 = (cold_ns.saturating_mul(100)) / warm_ns;
    format!("{}.{:02}x", x100 / 100, x100 % 100)
}

fn format_reduction_permille(cold_ns: u64, warm_ns: u64) -> String {
    if cold_ns == 0 || warm_ns >= cold_ns {
        return "0.0%".to_owned();
    }
    let permille = ((cold_ns - warm_ns).saturating_mul(1000)) / cold_ns;
    format!("-{}.{}%", permille / 10, permille % 10)
}

fn main() {
    println!("=== Tickwire Hot-Path Cache & Offline Loopback TLS Warmer Benchmark ===");
    println!("CPU:    {}", read_cpu_model());
    println!("Kernel: {}", read_proc_first_line("/proc/version"));
    println!("Load:   {}", read_proc_first_line("/proc/loadavg"));
    println!();

    // Part 1: Isolated TlsLoopbackWarmer::warm_step_exchange microbenchmark (10,000 iterations).
    {
        let mut tls_warmer = TlsLoopbackWarmer::new().unwrap();
        let sample_req = b"POST /v5/order/create HTTP/1.1\r\nHost: api.bybit.com\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"category\":\"spot\",\"symbol\":\"BTCUSDT\",\"side\":\"Buy\",\"orderType\":\"Limit\",\"qty\":\"0.001\",\"price\":\"50000\"}";
        let mut ack_out = [0u8; 512];
        for _ in 0..500 {
            black_box(
                tls_warmer
                    .warm_step_exchange(sample_req, BYBIT_LIVE_ACK, &mut ack_out)
                    .unwrap(),
            );
        }
        let iters = if cfg!(debug_assertions) { 200 } else { 10_000 };
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t0 = Instant::now();
            let res = tls_warmer
                .warm_step_exchange(
                    black_box(sample_req),
                    black_box(BYBIT_LIVE_ACK),
                    &mut ack_out,
                )
                .unwrap();
            let dt = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            black_box(res);
            samples.push(dt);
        }
        samples.sort_unstable();
        println!(
            "TlsLoopbackWarmer::warm_step_exchange ({iters} iters): P50={} ns  P90={} ns  P99={} ns  P99.9={} ns",
            percentile_permille(&samples, 500),
            percentile_permille(&samples, 900),
            percentile_permille(&samples, 990),
            percentile_permille(&samples, 999),
        );
    }

    // Part 2: RestSession::maybe_warm_hot_path (idle-skip check vs full 4-stage dry-run)
    // and Part 3: Cold-cache vs Warmed first-order sign + frame + TLS 1.3 roundtrip + ack decode.
    let mut evictor = CacheEvictor::new();

    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let venue_label = match venue {
            SpotVenue::Bybit => "Bybit V5 Spot",
            SpotVenue::Okx => "OKX V5 Spot",
        };
        let (mut snapshot, catalog, mut source) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = 10_000_000_000;
        source.context.request.venue = venue.id();
        source.context.request.deadline = 10_000_000_000;
        let CommandBody::New(order) = &mut source.body else {
            unreachable!()
        };
        order.client_id = ClientId::new("bench1").unwrap();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let command = CanonicalCommand::new(source).unwrap();

        let creds = match venue {
            SpotVenue::Bybit => Credentials::bybit("bench-key", b"bench-live-hmac-secret"),
            SpotVenue::Okx => {
                Credentials::okx("bench-key", b"bench-live-hmac-secret", "bench-pass")
            }
        };
        let gw_encoder =
            SpotEncoder::new(creds, Environment::Test, 2, || Ok(1_700_000_000_000)).unwrap();
        let mut live_encoder =
            SpotEncoder::new(creds, Environment::Test, 2, || Ok(1_700_000_000_000)).unwrap();
        let mut cfg = common::config(&baseline);
        cfg.route = gw_encoder.route();
        let (conn, peer) = common::peer(&cfg.route.authority, 4096);
        let gateway = AccountGateway::create(
            InMemoryBackend::new(),
            cfg,
            baseline,
            common::MockAuthority::default(),
            gw_encoder,
            conn,
        )
        .unwrap();
        let mut session = RestSession::new(gateway, 1)
            .unwrap()
            .with_warmup_interval_ns(500_000)
            .unwrap();
        let stp_book = StpBook::new();
        let mut live_tls = LiveInMemoryTlsPair::new(venue);

        // 2a. Measure fast idle-skip check (`maybe_warm_hot_path` when active / not yet due).
        session.record_activity(1_000_000);
        let skip_iters = 50_000u64;
        let skip_start = Instant::now();
        for i in 0..skip_iters {
            let fired = session
                .maybe_warm_hot_path(black_box(1_000_000 + (i % 400_000)))
                .unwrap();
            black_box(fired);
        }
        let skip_total_ns = u64::try_from(skip_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let skip_per_call_ns = skip_total_ns / skip_iters;

        // 2b. Measure steady-state full 4-stage `maybe_warm_hot_path` when due (5,000 iterations).
        let mut t = 2_000_000u64;
        for _ in 0..300 {
            t += 500_000;
            assert!(session.maybe_warm_hot_path(t).unwrap());
        }
        let warm_iters = if cfg!(debug_assertions) { 200 } else { 5_000 };
        let mut warm_step_samples = Vec::with_capacity(warm_iters);
        for _ in 0..warm_iters {
            t += 500_000;
            let t0 = Instant::now();
            let fired = session.maybe_warm_hot_path(black_box(t)).unwrap();
            let dt = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            assert!(fired);
            warm_step_samples.push(dt);
        }
        warm_step_samples.sort_unstable();
        println!(
            "{venue_label} maybe_warm_hot_path: active-skip={} ns/call | full 4-stage dry-run ({warm_iters} iters): P50={} ns  P90={} ns  P99={} ns  P99.9={} ns",
            skip_per_call_ns,
            percentile_permille(&warm_step_samples, 500),
            percentile_permille(&warm_step_samples, 900),
            percentile_permille(&warm_step_samples, 990),
            percentile_permille(&warm_step_samples, 999),
        );

        // 3. Cold-cache vs Warmed first-order latency after 32 MiB LLC + L1i/BTB eviction (240 rounds).
        // Prime `live_tls` once before measurement so internal Vec capacities are settled in both modes.
        let _ = execute_first_order_hot_path(
            venue,
            &session,
            &stp_book,
            &mut live_encoder,
            &command,
            &mut live_tls,
        );

        let rounds = if cfg!(debug_assertions) { 12 } else { 240 };
        let mut cold_samples = Vec::with_capacity(rounds);
        let mut warmed_samples = Vec::with_capacity(rounds);

        for round in 0..rounds {
            if round % 2 == 0 {
                // Cold first, then Warmed.
                evictor.thrash_llc_and_icache();
                let t0 = Instant::now();
                let n = execute_first_order_hot_path(
                    venue,
                    &session,
                    &stp_book,
                    &mut live_encoder,
                    &command,
                    &mut live_tls,
                );
                let dt_cold = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                black_box(n);
                cold_samples.push(dt_cold);

                evictor.thrash_llc_and_icache();
                t += 500_000;
                assert!(session.maybe_warm_hot_path(t).unwrap());
                let t1 = Instant::now();
                let n2 = execute_first_order_hot_path(
                    venue,
                    &session,
                    &stp_book,
                    &mut live_encoder,
                    &command,
                    &mut live_tls,
                );
                let dt_warm = u64::try_from(t1.elapsed().as_nanos()).unwrap_or(u64::MAX);
                black_box(n2);
                warmed_samples.push(dt_warm);
            } else {
                // Warmed first, then Cold (interleaved to cancel drift/turbo bias).
                evictor.thrash_llc_and_icache();
                t += 500_000;
                assert!(session.maybe_warm_hot_path(t).unwrap());
                let t1 = Instant::now();
                let n2 = execute_first_order_hot_path(
                    venue,
                    &session,
                    &stp_book,
                    &mut live_encoder,
                    &command,
                    &mut live_tls,
                );
                let dt_warm = u64::try_from(t1.elapsed().as_nanos()).unwrap_or(u64::MAX);
                black_box(n2);
                warmed_samples.push(dt_warm);

                evictor.thrash_llc_and_icache();
                let t0 = Instant::now();
                let n = execute_first_order_hot_path(
                    venue,
                    &session,
                    &stp_book,
                    &mut live_encoder,
                    &command,
                    &mut live_tls,
                );
                let dt_cold = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                black_box(n);
                cold_samples.push(dt_cold);
            }
        }

        cold_samples.sort_unstable();
        warmed_samples.sort_unstable();
        let cold_p50 = percentile_permille(&cold_samples, 500);
        let cold_p99 = percentile_permille(&cold_samples, 990);
        let cold_p999 = percentile_permille(&cold_samples, 999);
        let warm_p50 = percentile_permille(&warmed_samples, 500);
        let warm_p99 = percentile_permille(&warmed_samples, 990);
        let warm_p999 = percentile_permille(&warmed_samples, 999);

        println!(
            "{venue_label} First-Order Cold  ({rounds} rounds): P50={} ns  P99={} ns  P99.9={} ns",
            cold_p50, cold_p99, cold_p999
        );
        println!(
            "{venue_label} First-Order Warmed ({rounds} rounds): P50={} ns  P99={} ns  P99.9={} ns  (P50 {} / {}, P99 {} / {})",
            warm_p50,
            warm_p99,
            warm_p999,
            format_speedup_x100(cold_p50, warm_p50),
            format_reduction_permille(cold_p50, warm_p50),
            format_speedup_x100(cold_p99, warm_p99),
            format_reduction_permille(cold_p99, warm_p99),
        );
        println!();
        drop(session);
        let _ = peer.join();
    }
    println!("Post-run Load: {}", read_proc_first_line("/proc/loadavg"));
}
