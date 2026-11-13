#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, Gate, Status};
use tickwire_account_journal::InMemoryBackend;
use tickwire_app::strategy::{Config, FileStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{
    InputRecord, RecoveryMode, RuntimeBlobStore, SharedBlobStore, encode_inputs,
};
use tickwire_runtime::{
    authority::{Dependency, LocalAuthority, RecoveryBundle, RecoveryQueue},
    node::Submission,
};
use tickwire_spot_execution::{Credentials, RestSession, SpotEncoder, SpotVenue};
use tickwire_spot_runner::AccountRunner;
use tickwire_transport::IoBudget;
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment};

#[test]
fn account_runner_in_memory_evicts_retired_aborted_origins_across_monotonic_windows() {
    let venue = SpotVenue::Bybit;
    let (mut snapshot, catalog, template) = fixture::fixture();
    snapshot.scope.venue = venue.id();
    snapshot.valid_until = u64::MAX;
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let encoder = SpotEncoder::new(
        Credentials::bybit("key", b"secret"),
        Environment::Test,
        2,
        || Ok(1_700_000_000_000),
    )
    .unwrap();
    let mut config = common::config(&baseline);
    config.route = encoder.route();
    config.max_intents = 4;
    let (connection, wire) = common::peer(&config.route.authority, 65536);
    let queue = RecoveryQueue::new(4, 1024 * 1024).unwrap();
    let objects = SharedBlobStore::new(RuntimeBlobStore::in_memory());
    let gate_now = Arc::new(AtomicU64::new(1_000));
    let gate_clock = Arc::clone(&gate_now);
    let authority = LocalAuthority::new(
        objects.clone(),
        queue.clone(),
        move |_: &CanonicalCommand, _: &tickwire_account::Admission| {
            let now = gate_clock.load(Ordering::Acquire);
            Ok(Gate {
                now,
                lease_until: now.saturating_add(1_000_000_000),
                writer_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                owner_isolated: true,
                action_permitted: false,
                market_valid: true,
            })
        },
        [2; 16],
    );
    let gateway = AccountGateway::create(
        InMemoryBackend::new(),
        config,
        baseline,
        authority,
        encoder,
        connection,
    )
    .unwrap();
    let mut runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects)
        .with_retention_window_ns(Some(1_000));
    assert_eq!(runner.retention_window_ns(), Some(1_000));

    let domain = ClockDomain::new([4; 16], 1).unwrap();
    let mut strategy = FileStrategy::new(&Config::Observe).unwrap();

    // Execute 20 sequential aborted submissions (> 4 * max_intents) across advancing windows.
    for idx in 1u64..=20 {
        let mono_ns = idx * 2_500;
        gate_now.store(mono_ns + 10, Ordering::Release);
        let mut cmd_src = template.clone();
        cmd_src.context.request.venue = venue.id();
        cmd_src.context.request.intent_id = idx;
        cmd_src.context.request.recovery_object = idx;
        cmd_src.context.request.deadline = mono_ns + 10_000_000_000;
        cmd_src.context.strategy.created_mono_ns = mono_ns;
        let mut order_id = [0u8; 16];
        order_id[..8].copy_from_slice(&idx.to_be_bytes());
        let CommandBody::New(order) = &mut cmd_src.body else {
            unreachable!()
        };
        order.client_id = ClientId::new(format!("inmemevict{idx:04}")).unwrap();
        let command = CanonicalCommand::new(cmd_src).unwrap();
        let make_sub = || Submission {
            command: command.clone(),
            order_ids: vec![order_id],
            deadline: common::until(),
            recovery: RecoveryBundle {
                command_digest: command.digest(),
                dependencies: (2..=11)
                    .map(|kind| Dependency {
                        kind,
                        schema_id: 1,
                        owner: [9; 16],
                        bytes: vec![kind; 4],
                    })
                    .collect(),
                inputs: encode_inputs(
                    [9; 16],
                    idx,
                    [0; 32],
                    &[InputRecord {
                        kind: 1,
                        payload: b"test quote",
                    }],
                )
                .unwrap(),
                mode: RecoveryMode::DeterministicReplay,
                fault_model_id: [4; 32],
                pin_id: [9; 16],
                owner: [9; 16],
            },
        };
        let now = || LocalTime {
            monotonic: MonoTime::new(domain, mono_ns),
            instant: Instant::now(),
        };
        assert_eq!(runner.submit(make_sub(), now()).unwrap(), Status::Queued);
        let deadline = common::until();
        while !runner.available() {
            assert!(Instant::now() < deadline);
            assert!(
                runner
                    .poll(&mut strategy, now(), IoBudget::default())
                    .unwrap()
                    .is_none()
            );
            std::thread::yield_now();
        }
        assert_eq!(runner.submit(make_sub(), now()).unwrap(), Status::Aborted);
    }

    assert_eq!(
        runner.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    drop(runner);
    assert!(wire.join().unwrap().is_empty());
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[test]
fn in_memory_reconcile_process_qualifies_without_creating_state_or_ephemeral_private_directories() {
    use serde_json::json;
    use std::{
        fs,
        path::Path,
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tickwire_spot_execution::{authority, private};

    let root = std::env::temp_dir().join(format!(
        "tickwire-spot-inmem-live-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    struct Cleanup<'a>(&'a Path);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
    let _cleanup = Cleanup(&root);

    let sources = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/reconcile_cli");
    assert!(
        Command::new("cc")
            .args(["-Wall", "-Werror", "-shared", "-fPIC"])
            .arg(sources.join("loopback.c"))
            .arg("-ldl")
            .arg("-o")
            .arg(root.join("local.so"))
            .status()
            .unwrap()
            .success()
    );

    let venue = SpotVenue::Bybit;
    let sample =
        CanonicalBaseline::decode(include_bytes!("../examples/baseline-template.cbor")).unwrap();
    let mut s = sample.snapshot().clone();
    s.scope.venue = venue.id();
    s.valid_until = u64::MAX;
    let mut cat = sample.catalog().clone();
    cat.entries[0].instrument.symbol = tickwire_venue::Symbol::new("BTCUSDT").unwrap();
    let base = CanonicalBaseline::new(s.clone(), cat.clone()).unwrap();
    let inst = &cat.entries[0].instrument;
    fs::write(root.join("baseline.cbor"), base.bytes()).unwrap();

    let baseline_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 60_000;
    let boot_id = [0xABu8; 16];
    let account_stream = [4u8; 16];
    let private_stream = [5u8; 16];
    let deployment = json!({
        "storage_mode": "InMemory",
        "state_directory": null,
        "baseline": root.join("baseline.cbor"),
        "expected_uid": "42",
        "baseline_utc_ms": baseline_ms,
        "account_stream": account_stream,
        "private_stream": private_stream,
        "initial_writer_epoch": 1,
        "market_sources": [],
        "strategies": [],
        "stop_file": root.join("stop"),
        "policy": {
            "scope": {
                "risk_domain_id": s.scope.risk_domain_id,
                "venue": venue.id(),
                "environment": "test",
                "account": s.scope.account
            },
            "product": "spot",
            "executor_id": boot_id,
            "owner_generation": 1,
            "writer_epoch": 1,
            "domain": { "process_instance": boot_id, "generation": 1 },
            "versions": {
                "config_revision": 1,
                "risk_policy_version": s.revisions.risk,
                "order_config_version": 1,
                "catalog_version": inst.catalog_version,
                "catalog_hash": inst.catalog_hash
            },
            "max_strategies": 2,
            "max_feeds": 2,
            "marks_per_feed": 2,
            "control_revision": 1,
            "mode": "stopped",
            "owner_lease": null,
            "strategy_grants": [],
            "management_grant": null
        }
    });
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&deployment).unwrap(),
    )
    .unwrap();

    let rest = authority(venue, Environment::Test);
    let (ws, _) = private::endpoint(venue, Environment::Test);
    let cert = rcgen::generate_simple_self_signed(vec![rest.into(), ws.into()]).unwrap();
    fs::write(root.join("ca.pem"), cert.cert.pem()).unwrap();
    fs::write(root.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();

    let py_harness = r#"
import base64, hashlib, hmac, json, os, socketserver, ssl, subprocess, sys, threading, time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

binary, directory, rest_host, ws_host = sys.argv[1:]
directory = Path(directory)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain(directory / "ca.pem", directory / "key.pem")
def accept_sni(sock, name, _ctx):
    sock.exchange_name = name
tls.set_servername_callback(accept_sni)

def digest(msg):
    return hmac.digest(b"test-secret", msg.encode(), "sha256").hex()

def frame(sock, body):
    raw = json.dumps(body).encode()
    hdr = bytes([129, len(raw)]) if len(raw) < 126 else b"\x81\x7e" + len(raw).to_bytes(2, "big")
    sock.sendall(hdr + raw)

def read_frame(f):
    hdr = f.read(2)
    if len(hdr) < 2 or (hdr[0] & 15) == 8:
        return None
    sz = hdr[1] & 127
    if sz == 126:
        sz = int.from_bytes(f.read(2), "big")
    mask, raw = f.read(4), f.read(sz)
    return bytes(x ^ mask[i % 4] for i, x in enumerate(raw))

class Peer(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with tls.wrap_socket(self.request, server_side=True) as sock, sock.makefile("rb") as f:
                sock.settimeout(20)
                first = f.readline(8192)
                if not first:
                    return
                method, target, _ = first.decode().strip().split()
                headers = {}
                while True:
                    line = f.readline(8192)
                    if not line or line == b"\r\n":
                        break
                    k, v = line.decode().split(":", 1)
                    headers[k.lower()] = v.strip()
                if headers.get("upgrade", "").lower() == "websocket":
                    seed = headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                    accept = base64.b64encode(hashlib.sha1(seed.encode()).digest()).decode()
                    sock.sendall((
                        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                        f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                    ).encode())
                    msg = read_frame(f)
                    if msg is None:
                        return
                    frame(sock, {"op": "auth", "success": True, "conn_id": "inmem"})
                    sub = json.loads(read_frame(f))
                    frame(sock, {"op": "subscribe", "success": True, "conn_id": "inmem", "req_id": sub["req_id"]})
                    while (m := read_frame(f)) is not None:
                        if json.loads(m).get("op") == "ping":
                            frame(sock, {"op": "pong", "conn_id": "inmem", "args": ["test"]})
                    return
                path, _, query = target.partition("?")
                if path == "/v5/user/query-api":
                    payload = {"retCode": 0, "result": {"userID": 42, "apiKey": "test-key"}}
                elif path == "/v5/account/info":
                    payload = {"retCode": 0, "result": {"unifiedMarginStatus": 5, "marginMode": "REGULAR_MARGIN"}}
                elif path == "/v5/spot-margin-trade/state":
                    payload = {"retCode": 0, "result": {"spotMarginMode": "0"}}
                elif path == "/v5/market/time":
                    payload = {"retCode": 0, "result": {"timeNano": str(time.time_ns())}}
                elif path == "/v5/account/wallet-balance":
                    coins = [
                        {"coin": "BTC", "equity": "1", "walletBalance": "1", "locked": "0", "spotBorrow": "0", "borrowAmount": "0"},
                        {"coin": "USDT", "equity": "1000", "walletBalance": "1000", "locked": "0", "spotBorrow": "0", "borrowAmount": "0"},
                    ]
                    payload = {"retCode": 0, "result": {"list": [{"accountType": "UNIFIED", "totalInitialMargin": "0", "totalMaintenanceMargin": "0", "totalPerpUPL": "0", "coin": coins}]}}
                elif path == "/v5/market/instruments-info":
                    cat = parse_qs(query).get("category", ["spot"])[0]
                    payload = {"retCode": 0, "result": {"category": cat, "list": [{"settleCoin": "USDT"}], "nextPageCursor": ""}}
                else:
                    cat = parse_qs(query).get("category", ["spot"])[0]
                    payload = {"retCode": 0, "result": {"category": cat, "list": [], "nextPageCursor": ""}}
                body = json.dumps(payload).encode()
                sock.sendall(f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
        except Exception:
            pass

class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True

with Server(("127.0.0.1", 0), Peer) as server:
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = dict(
        os.environ,
        TW_TEST_REST=rest_host,
        TW_TEST_WS=ws_host,
        TW_TEST_PORT=str(server.server_address[1]),
        SSL_CERT_FILE=str(directory / "ca.pem"),
        LD_PRELOAD=str(directory / "local.so"),
        TICKWIRE_SPOT_KEY="test-key",
        TICKWIRE_SPOT_SECRET="test-secret",
        TICKWIRE_SPOT_PASSPHRASE="test-pass",
    )
    for k in ("TICKWIRE_SPOT_KEY_FILE", "TICKWIRE_SPOT_SECRET_FILE", "TICKWIRE_SPOT_PASSPHRASE_FILE"):
        env.pop(k, None)
    proc = subprocess.run(
        [binary, "reconcile", str(directory / "config.json")],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    # Check before exit that no ephemeral tickwire-spot-private-* dir exists for child pid
    ephemeral_prefix = f"tickwire-spot-private-"
    leaked = [p.name for p in Path("/tmp").iterdir() if p.name.startswith(ephemeral_prefix) and ("abababab" in p.name)]
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "spot_account_reconciled" in proc.stdout, (proc.stdout, proc.stderr)
    assert not leaked, f"unexpected ephemeral private directories: {leaked}"
    server.shutdown()
"#;
    let harness_path = root.join("inmem_harness.py");
    fs::write(&harness_path, py_harness).unwrap();

    let out = Command::new("python3")
        .arg(&harness_path)
        .arg(env!("CARGO_BIN_EXE_tickwire-spot-runner"))
        .arg(&root)
        .args([rest, ws])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // Confirm only the fixture files we created exist inside `root` (no `private`, `account`, `objects`, or `reconciliation` dirs).
    for forbidden in [
        "private",
        "account",
        "objects",
        "reconciliation",
        "account-state",
    ] {
        assert!(
            !root.join(forbidden).exists(),
            "InMemory mode must not create {forbidden}"
        );
    }
}
