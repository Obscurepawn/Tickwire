#[path = "../../../tickwire-binance-public/tests/common/mod.rs"]
mod common;
use common::*;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
};

pub struct Network {
    pub stream: String,
    pub snapshot: String,
    pub subscriptions: Arc<AtomicUsize>,
    pub snapshots: Arc<AtomicUsize>,
    pub frames: Arc<AtomicUsize>,
    pub first_delta: std::path::PathBuf,
    pub first_snapshot: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}
impl Network {
    pub fn start(certificate: &Path) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec![
            "stream.testnet.binance.vision".into(),
            "testnet.binance.vision".into(),
        ])
        .unwrap();
        std::fs::write(certificate, cert.cert.der()).unwrap();
        let tls = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                        .into(),
                )
                .unwrap(),
        );
        let ws = listener();
        let http = listener();
        let stream = ws.local_addr().unwrap().to_string();
        let snapshot = http.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let snapshots = Arc::new(AtomicUsize::new(0));
        let frames = Arc::new(AtomicUsize::new(0));
        let first_delta = certificate.with_file_name("actual-first-delta.json");
        let first_snapshot = certificate.with_file_name("actual-first-snapshot.json");
        let delta_path = first_delta.clone();
        let snapshot_path = first_snapshot.clone();
        let ws_stop = stop.clone();
        let ws_tls = tls.clone();
        let subscribed = subscriptions.clone();
        let sent = frames.clone();
        let depth = std::thread::spawn(move || {
            while let Some(mut connection) = accept_until(&ws, ws_tls.clone(), &ws_stop) {
                upgrade(&mut connection, "/ws", "stream.testnet.binance.vision");
                let request: serde_json::Value =
                    serde_json::from_slice(&client_frame(&mut connection)).unwrap();
                assert_eq!(request["params"][0], "btcusdt@depth@100ms");
                frame(
                    &mut connection,
                    1,
                    &serde_json::to_vec(&serde_json::json!({"id":request["id"],"result":null}))
                        .unwrap(),
                );
                subscribed.fetch_add(1, Ordering::Relaxed);
                let mut sequence = 101;
                while !ws_stop.load(Ordering::Relaxed) {
                    // Exact source spelling is identical for both economic catalogs.
                    let data=serde_json::to_vec(&serde_json::json!({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":if sequence==101 {99}else{sequence},"u":sequence,"b":[["1.00","2"]],"a":[]})).unwrap();
                    if !send_frame(&mut connection, &data) {
                        break;
                    }
                    if sequence == 101 {
                        std::fs::write(&delta_path, &data).unwrap();
                    }
                    sent.fetch_add(1, Ordering::Relaxed);
                    sequence += 1;
                    std::thread::sleep(std::time::Duration::from_millis(30));
                }
            }
        });
        let rest_stop = stop.clone();
        let counted = snapshots.clone();
        let rest = std::thread::spawn(move || {
            while let Some(mut connection) = accept_until(&http, tls.clone(), &rest_stop) {
                let request = headers(&mut connection);
                assert!(
                    request.starts_with("GET /api/v3/depth?symbol=BTCUSDT&limit=5 "),
                    "{request}"
                );
                let snapshot =
                    br#"{"lastUpdateId":100,"bids":[["1.00","1"]],"asks":[["1.02","1"]]}"#;
                reply(&mut connection, snapshot);
                std::fs::write(&snapshot_path, snapshot).unwrap();
                counted.fetch_add(1, Ordering::Relaxed);
            }
        });
        Self {
            stream,
            snapshot,
            subscriptions,
            snapshots,
            frames,
            first_delta,
            first_snapshot,
            stop,
            threads: vec![depth, rest],
        }
    }
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
fn accept_until(
    listener: &std::net::TcpListener,
    tls: Arc<rustls::ServerConfig>,
    stop: &AtomicBool,
) -> Option<Server> {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                return Some(rustls::StreamOwned::new(
                    rustls::ServerConnection::new(tls).unwrap(),
                    stream,
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(1))
            }
            Err(e) => panic!("{e}"),
        }
    }
    None
}
fn send_frame(stream: &mut Server, bytes: &[u8]) -> bool {
    use std::io::Write;
    let mut write = || -> std::io::Result<()> {
        stream.write_all(&[0x81])?;
        if bytes.len() < 126 {
            stream.write_all(&[bytes.len() as u8])?;
        } else {
            stream.write_all(&[126])?;
            stream.write_all(&(bytes.len() as u16).to_be_bytes())?;
        }
        stream.write_all(bytes)?;
        stream.flush()
    };
    write().is_ok()
}
