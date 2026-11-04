use super::*;
use base64::Engine;
use sha1::{Digest, Sha1};
use std::io::BufRead;
use std::net::TcpListener;

fn server_frame(text: &str) -> Vec<u8> {
    let mut out = vec![0x81];
    if text.len() <= 125 {
        out.push(text.len() as u8);
    } else {
        out.push(126);
        out.extend((text.len() as u16).to_be_bytes());
    }
    out.extend(text.as_bytes());
    out
}

fn subscription(stream: &mut BufReader<TcpStream>) -> serde_json::Value {
    let mut header = [0; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], 0x81);
    assert_ne!(header[1] & 0x80, 0);
    let mut key = [0; 4];
    stream.read_exact(&mut key).unwrap();
    let mut body = vec![0; (header[1] & 0x7f) as usize];
    stream.read_exact(&mut body).unwrap();
    for (n, b) in body.iter_mut().enumerate() {
        *b ^= key[n % 4];
    }
    serde_json::from_slice(&body).unwrap()
}

#[test]
fn real_upgrade_subscribe_and_okx_snapshot_are_visible_before_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let mut s = BufReader::new(listener.accept().unwrap().0);
        let mut request = String::new();
        s.read_line(&mut request).unwrap();
        assert!(request.starts_with("GET /ws/v5/public "));
        let mut key = String::new();
        loop {
            let mut line = String::new();
            s.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(k) = line.strip_prefix("Sec-WebSocket-Key: ") {
                key = k.trim().to_string();
            }
        }
        let accept = base64::engine::general_purpose::STANDARD.encode(Sha1::digest(format!(
            "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        write!(s.get_mut(),"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        let req = subscription(&mut s);
        assert_eq!(
            req["args"][0],
            serde_json::json!({"channel":"bbo-tbt","instId":"BTC-USDT"})
        );
        s.get_mut().write_all(&server_frame(r#"{"id":"twbbo","event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}"#)).unwrap();
        s.get_mut().write_all(&server_frame(r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"action":"snapshot","data":[{"bids":[["10","0.2","0","2"]],"asks":[],"ts":"1791000000000","seqId":16}]}"#)).unwrap();
    });
    let cfg:QuoteConfig=serde_json::from_str(r#"{"venue":"okx","symbol":"BTC-USDT","price_tick":"0.1","quantity_step":"0.1","instrument_id":18,"instrument_version":2}"#).unwrap();
    let stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut output = Vec::new();
    let mut healthy = false;
    assert!(
        session(
            &cfg,
            7,
            &mut || Ok(200),
            stream,
            &mut |n| emit(&mut output, &n),
            &mut healthy
        )
        .is_err()
    );
    peer.join().unwrap();
    assert!(healthy);
    let quote: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(quote["connection_epoch"], 7);
    assert_eq!(quote["bid"]["price_ticks"], 100);
    assert_eq!(quote["type"], "quote");
}
