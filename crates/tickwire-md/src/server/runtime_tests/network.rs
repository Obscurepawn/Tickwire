#[path = "../../../../tickwire-binance-public/tests/common/mod.rs"]
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

#[derive(Default)]
pub struct Gate {
    pub paused_events: AtomicBool,
    pub disconnect_depth: AtomicBool,
    pub depth_subscriptions: AtomicUsize,
    pub event_subscriptions: AtomicUsize,
    pub snapshots: AtomicUsize,
    pub events_sent: AtomicUsize,
}
pub struct Network {
    pub stream: String,
    pub snapshot: String,
    pub event: String,
    pub connections: Arc<AtomicUsize>,
    pub gate: Arc<Gate>,
    stopped: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}
impl Network {
    pub fn start(root: &Path, symbol: &'static str, price: u64, events: bool) -> Self {
        Self::start_inner(root, symbol, price, events, false)
    }
    pub fn start_paused_trade(root: &Path, symbol: &'static str, price: u64) -> Self {
        Self::start_inner(root, symbol, price, true, true)
    }
    fn start_inner(
        root: &Path,
        symbol: &'static str,
        price: u64,
        events: bool,
        paused_events: bool,
    ) -> Self {
        let gate = Arc::new(Gate {
            paused_events: AtomicBool::new(paused_events),
            ..Gate::default()
        });
        let cert = rcgen::generate_simple_self_signed(vec![
            "stream.binance.com".into(),
            "api.binance.com".into(),
        ])
        .unwrap();
        std::fs::write(root, cert.cert.der()).unwrap();
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
        let event = listener();
        let stream = ws.local_addr().unwrap().to_string();
        let snapshot = http.local_addr().unwrap().to_string();
        let event_address = event.local_addr().unwrap().to_string();
        let stopped = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(AtomicUsize::new(0));
        let ws_tls = tls.clone();
        let stop = stopped.clone();
        let count = connections.clone();
        let depth_gate = gate.clone();
        let depth = std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let Some(mut stream) = accept_until(&ws, ws_tls.clone(), &stop) else {
                    return;
                };
                count.fetch_add(1, Ordering::Relaxed);
                upgrade(&mut stream, "/ws", "stream.binance.com");
                let request: serde_json::Value =
                    serde_json::from_slice(&client_frame(&mut stream)).unwrap();
                assert_eq!(
                    request["params"][0],
                    format!("{}@depth@100ms", symbol.to_ascii_lowercase())
                );
                frame(
                    &mut stream,
                    1,
                    &serde_json::to_vec(&serde_json::json!({"id":request["id"],"result":null}))
                        .unwrap(),
                );
                depth_gate
                    .depth_subscriptions
                    .fetch_add(1, Ordering::Relaxed);
                let mut sequence = 101;
                while !stop.load(Ordering::Relaxed) {
                    if depth_gate.disconnect_depth.swap(false, Ordering::Relaxed) {
                        stream.sock.shutdown(std::net::Shutdown::Both).unwrap();
                        break;
                    }
                    let data=serde_json::to_vec(&serde_json::json!({"e":"depthUpdate","E":1,"s":symbol,"U":if sequence==101 {99}else{sequence},"u":sequence,"b":[[price.to_string(),"2"]],"a":[]})).unwrap();
                    if !send_frame(&mut stream, &data) {
                        break;
                    }
                    sequence += 1;
                    std::thread::sleep(std::time::Duration::from_millis(30));
                }
            }
        });
        let rest_tls = tls.clone();
        let rest_stop = stopped.clone();
        let rest_gate = gate.clone();
        let rest = std::thread::spawn(move || {
            while !rest_stop.load(Ordering::Relaxed) {
                let Some(mut connection) = accept_until(&http, rest_tls.clone(), &rest_stop) else {
                    return;
                };
                let request = headers(&mut connection);
                assert!(
                    request.starts_with(&format!("GET /api/v3/depth?symbol={symbol}&limit=5 ")),
                    "{request}"
                );
                reply(&mut connection,&serde_json::to_vec(&serde_json::json!({"lastUpdateId":100,"bids":[[price.to_string(),"1"]],"asks":[[(price+2).to_string(),"1"]]})).unwrap());
                rest_gate.snapshots.fetch_add(1, Ordering::Relaxed);
            }
        });
        let mut threads = vec![depth, rest];
        if events {
            let stop = stopped.clone();
            let count = connections.clone();
            let event_gate = gate.clone();
            threads.push(std::thread::spawn(move||{
              while !stop.load(Ordering::Relaxed) {
                let Some(mut stream)=accept_until(&event,tls.clone(),&stop) else {return;}; count.fetch_add(1,Ordering::Relaxed);
                upgrade(&mut stream,"/ws","stream.binance.com");
                let request:serde_json::Value=serde_json::from_slice(&client_frame(&mut stream)).unwrap();
                assert_eq!(request["params"][0],format!("{}@trade",symbol.to_ascii_lowercase()));
                frame(&mut stream,1,&serde_json::to_vec(&serde_json::json!({"id":request["id"],"result":null})).unwrap());
                event_gate.event_subscriptions.fetch_add(1, Ordering::Relaxed);
                let mut sequence=1;
                while !stop.load(Ordering::Relaxed) {
                    if event_gate.paused_events.load(Ordering::Relaxed) {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    if !send_frame(&mut stream,&serde_json::to_vec(&serde_json::json!({"e":"trade","E":1,"s":symbol,"t":sequence,"p":price.to_string(),"q":"1","T":1,"m":true})).unwrap()) { break; }
                    event_gate.events_sent.fetch_add(1, Ordering::Relaxed);
                    sequence+=1; std::thread::sleep(std::time::Duration::from_millis(30));
                }
              }
            }));
        }
        Self {
            stream,
            snapshot,
            event: event_address,
            connections,
            gate,
            stopped,
            threads,
        }
    }
    pub fn stop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

fn accept_until(
    listener: &std::net::TcpListener,
    tls: Arc<rustls::ServerConfig>,
    stop: &AtomicBool,
) -> Option<Server> {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
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
                assert!(std::time::Instant::now() < end);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => panic!("{error}"),
        }
    }
    None
}
fn send_frame(stream: &mut Server, bytes: &[u8]) -> bool {
    use std::io::Write;
    let mut send = || -> std::io::Result<()> {
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
    send().is_ok()
}
