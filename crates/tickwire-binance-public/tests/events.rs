mod common;
use common::*;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tickwire_binance::{market::*, request::Market};
fn cfg(source: Source, kind: EventKind, mode: SubscribeMode) -> EventConfig {
    let c = if source == Source::UsdmProduction {
        config::<BinanceUsdM>(source, mode)
    } else {
        config::<BinanceSpot>(source, mode)
    };
    let i = c.depth.instrument.clone();
    EventConfig {
        owner_id: c.owner_id,
        source,
        domain: c.domain,
        first_connection_epoch: 1,
        first_operation: 1,
        context: EventContext {
            market: if source == Source::UsdmProduction {
                Market::Usdm
            } else {
                Market::Spot
            },
            identity: StreamIdentity {
                source_id: 1,
                connection_epoch: 1,
                instrument_id: i.instrument_id,
                instrument_version: i.instrument_version,
                catalog_version: i.catalog_version,
                catalog_hash: i.catalog_hash,
            },
            instrument: i,
            kind,
            time_unit: TimeUnit::Milliseconds,
        },
        encoding: EventEncoding::Json,
        subscribe: mode,
        fast_mark: kind == EventKind::MarkFunding,
        timing: c.timing,
        limits: c.limits,
        max_events: 64,
    }
}
#[derive(Default)]
struct Events {
    events: Vec<Event>,
    gaps: usize,
    usable: Vec<bool>,
}
impl EventSink for Events {
    fn on_event(&mut self, n: EventNotice<'_>) -> bool {
        match n {
            EventNotice::Event { event, raw, usable } => {
                assert!(!raw.is_empty());
                self.events.push(*event);
                self.usable.push(usable)
            }
            EventNotice::Gap(_) => self.gaps += 1,
            EventNotice::Http(_) => {}
        }
        true
    }
}
#[test]
fn actual_json_wss_trade_and_mark_are_typed() {
    for (source,kind,path,body) in [(Source::SpotProduction,EventKind::Trade,"/ws/btcusdt@trade",br#"{"e":"trade","E":123,"s":"BTCUSDT","t":7,"p":"99.01","q":"0.001","T":122,"m":true}"#.as_slice()),(Source::UsdmProduction,EventKind::MarkFunding,"/market/ws/btcusdt@markPrice@1s",br#"{"e":"markPriceUpdate","E":123,"s":"BTCUSDT","p":"11794.15","i":"11784.62659091","r":"-0.0003","T":124,"st":1}"#.as_slice())]{
 let(tls,server)=certs();let listener=listener();let address=listener.local_addr().unwrap();let(done,wait)=mpsc::channel();let host=if source==Source::SpotProduction{"stream.binance.com"}else{"fstream.binance.com"};let worker=thread::spawn(move||{let mut s=accept(&listener,server);upgrade(&mut s,path,host);frame(&mut s,1,body);wait.recv_timeout(Duration::from_secs(5)).unwrap();});
 let mut client=PublicEventClient::new(cfg(source,kind,SubscribeMode::RawUrl),EventNetworkConfig{stream_address:address,tls},Clock(Instant::now())).unwrap();let mut permit=Permit::default();client.start(&mut permit).unwrap();let mut events=Events::default();let end=Instant::now()+Duration::from_secs(4);while events.events.is_empty(){assert!(Instant::now()<end);client.poll(PollBudget::default(),&mut permit,&mut events).unwrap();thread::sleep(Duration::from_millis(1));}assert!(client.status().usable);assert_eq!(events.events[0].identity.connection_epoch,1);assert!(permit.charges.iter().any(|c|c.kind==ChargeKind::Upgrade));done.send(()).unwrap();worker.join().unwrap();}
}
#[test]
fn explicit_subscription_accepts_null_ack_and_preserves_early_data() {
    let (tls, server) = certs();
    let listener = listener();
    let address = listener.local_addr().unwrap();
    let (done, wait) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut s = accept(&listener, server);
        upgrade(&mut s, "/ws", "stream.binance.com");
        let request: serde_json::Value = serde_json::from_slice(&client_frame(&mut s)).unwrap();
        let body=br#"{"e":"trade","E":123,"s":"BTCUSDT","t":7,"p":"99.01","q":"0.001","T":122,"m":true}"#;
        frame(&mut s, 1, body);
        frame(
            &mut s,
            1,
            serde_json::to_string(&serde_json::json!({"id":request["id"],"result":null}))
                .unwrap()
                .as_bytes(),
        );
        let _ = wait.recv_timeout(Duration::from_secs(2));
    });
    let mut client = PublicEventClient::new(
        cfg(
            Source::SpotProduction,
            EventKind::Trade,
            SubscribeMode::Explicit,
        ),
        EventNetworkConfig {
            stream_address: address,
            tls,
        },
        Clock(Instant::now()),
    )
    .unwrap();
    let mut permit = Permit::default();
    client.start(&mut permit).unwrap();
    let mut events = Events::default();
    let end = Instant::now() + Duration::from_secs(1);
    while events.events.is_empty() && events.gaps == 0 && Instant::now() < end {
        client
            .poll(PollBudget::default(), &mut permit, &mut events)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    let _ = done.send(());
    worker.join().unwrap();
    assert_eq!(
        events.events.len(),
        1,
        "{:?}, gaps={}",
        client.status(),
        events.gaps
    );
    assert!(client.status().usable);
}
#[allow(dead_code)]
pub fn instrument(source: Source) -> tickwire_venue::InstrumentBinding {
    cfg(source, EventKind::Trade, SubscribeMode::RawUrl)
        .context
        .instrument
}
/// Same real network owner used by the mapped MD test; callback sees exact raw frame plus typed rows.
pub fn exercise(
    source: Source,
    kind: EventKind,
    encoding: EventEncoding,
    mut callback: impl FnMut(&EventNotice<'_>),
) -> Vec<Event> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha1::{Digest, Sha1};
    use std::io::Write;
    let (tls, server) = certs();
    let listener = listener();
    let address = listener.local_addr().unwrap();
    let (done, wait) = mpsc::channel();
    let mut config = cfg(source, kind, SubscribeMode::Explicit);
    config.encoding = encoding;
    if encoding == EventEncoding::SpotSbeV1 {
        config.context.time_unit = TimeUnit::Microseconds;
    }
    let stream = config.context.stream_name(encoding, true).unwrap();
    let host = if encoding == EventEncoding::SpotSbeV1 {
        "stream-sbe.binance.com"
    } else if source == Source::SpotProduction {
        "stream.binance.com"
    } else {
        "fstream.binance.com"
    };
    let path = if source == Source::SpotProduction {
        "/ws"
    } else if kind == EventKind::BestBidAsk {
        "/public/stream"
    } else {
        "/market/stream"
    };
    let raw=match(encoding,kind){(EventEncoding::SpotSbeV1,_)=>{let s=if kind==EventKind::Trade{include_str!("../../tickwire-binance-sbe/tests/fixtures/trades.hex")}else{include_str!("../../tickwire-binance-sbe/tests/fixtures/quote.hex")};s.trim().as_bytes().as_chunks::<2>().0.iter().map(|b|u8::from_str_radix(std::str::from_utf8(b).unwrap(),16).unwrap()).collect::<Vec<_>>()},(_,EventKind::Trade) if source==Source::SpotProduction=>br#"{"e":"trade","E":123,"s":"BTCUSDT","t":7,"p":"99.01","q":"0.001","T":122,"m":true}"#.to_vec(),(_,EventKind::Trade)=>br#"{"e":"aggTrade","E":123,"s":"BTCUSDT","a":8,"f":6,"l":8,"p":"99.01","q":"0.001","nq":"0.0005","T":122,"m":false,"st":1}"#.to_vec(),(_,EventKind::BestBidAsk) if source==Source::SpotProduction=>br#"{"u":7,"s":"BTCUSDT","b":"99.01","B":"0.001","a":"99.02","A":"0.002"}"#.to_vec(),(_,EventKind::BestBidAsk)=>br#"{"e":"bookTicker","E":123,"T":122,"u":7,"s":"BTCUSDT","b":"99.01","B":"0.001","a":"99.02","A":"0.002","st":1}"#.to_vec(),(_,EventKind::MarkFunding)=>br#"{"e":"markPriceUpdate","E":123,"s":"BTCUSDT","p":"11794.15","i":"11784.62659091","r":"-0.0003","T":124,"st":1}"#.to_vec()};
    let count = if encoding == EventEncoding::SpotSbeV1 && kind == EventKind::Trade {
        2
    } else {
        1
    };
    let worker = thread::spawn(move || {
        let mut s = accept(&listener, server);
        let h = headers(&mut s);
        assert!(h.starts_with(&format!("GET {path} HTTP/1.1")));
        assert!(h.contains(&format!("Host: {host}")));
        if encoding == EventEncoding::SpotSbeV1 {
            assert!(h.contains("X-MBX-APIKEY: sbeKey"));
        }
        let key = h
            .lines()
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                    .map(|(_, v)| v.trim())
            })
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(format!(
            "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        write!(s,"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        s.flush().unwrap();
        let req: serde_json::Value = serde_json::from_slice(&client_frame(&mut s)).unwrap();
        assert_eq!(req["params"][0], stream);
        frame(
            &mut s,
            1,
            serde_json::to_string(&serde_json::json!({"id":req["id"],"result":null}))
                .unwrap()
                .as_bytes(),
        );
        frame(
            &mut s,
            if encoding == EventEncoding::Json {
                1
            } else {
                2
            },
            &raw,
        );
        let _ = wait.recv_timeout(Duration::from_secs(4));
    });
    let network = EventNetworkConfig {
        stream_address: address,
        tls,
    };
    let mut client = if encoding == EventEncoding::SpotSbeV1 {
        PublicEventClient::new_sbe(
            config,
            network,
            Clock(Instant::now()),
            SbeAccess::ed25519_api_key("sbeKey".into()).unwrap(),
        )
        .unwrap()
    } else {
        PublicEventClient::new(config, network, Clock(Instant::now())).unwrap()
    };
    let mut permit = Permit::default();
    client.start(&mut permit).unwrap();
    struct Sink<'a, F> {
        f: &'a mut F,
        events: Vec<Event>,
    }
    impl<F: FnMut(&EventNotice<'_>)> EventSink for Sink<'_, F> {
        fn on_event(&mut self, n: EventNotice<'_>) -> bool {
            (self.f)(&n);
            if let EventNotice::Event { event, .. } = n {
                self.events.push(*event);
            }
            true
        }
    }
    let mut sink = Sink {
        f: &mut callback,
        events: Vec::new(),
    };
    let end = Instant::now() + Duration::from_secs(3);
    while sink.events.len() < count {
        assert!(Instant::now() < end, "{:?}", client.status());
        client
            .poll(
                PollBudget {
                    max_notifications: 1,
                    ..PollBudget::default()
                },
                &mut permit,
                &mut sink,
            )
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(client.status().usable);
    done.send(()).unwrap();
    worker.join().unwrap();
    sink.events
}
#[test]
fn all_event_streams_use_actual_json_or_binary_subscription() {
    for (source, kind, encoding) in [
        (
            Source::SpotProduction,
            EventKind::Trade,
            EventEncoding::SpotSbeV1,
        ),
        (
            Source::SpotProduction,
            EventKind::BestBidAsk,
            EventEncoding::SpotSbeV1,
        ),
        (
            Source::SpotProduction,
            EventKind::BestBidAsk,
            EventEncoding::Json,
        ),
        (
            Source::UsdmProduction,
            EventKind::Trade,
            EventEncoding::Json,
        ),
        (
            Source::UsdmProduction,
            EventKind::BestBidAsk,
            EventEncoding::Json,
        ),
    ] {
        let events = exercise(source, kind, encoding, |_| {});
        assert_eq!(events[0].encoding, encoding);
        if events.len() == 2 {
            assert_eq!(
                events[0].times.received_mono_ns,
                events[1].times.received_mono_ns
            );
        }
    }
}
#[test]
fn eof_drains_complete_subscription_and_trade_without_reopening_health() {
    use std::io::Write;
    let (tls, server) = certs();
    let listener = listener();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let mut s = accept(&listener, server);
        upgrade(&mut s, "/ws", "stream.binance.com");
        let request: serde_json::Value = serde_json::from_slice(&client_frame(&mut s)).unwrap();
        let ack =
            serde_json::to_vec(&serde_json::json!({"id":request["id"],"result":null})).unwrap();
        let trade = br#"{"e":"trade","E":123,"s":"BTCUSDT","t":7,"p":"99.01","q":"0.001","T":122,"m":true}"#;
        let mut packet = Vec::new();
        for body in [ack.as_slice(), trade.as_slice()] {
            packet.extend_from_slice(&[0x81, body.len() as u8]);
            packet.extend_from_slice(body);
        }
        s.write_all(&packet).unwrap();
        s.conn.send_close_notify();
        s.flush().unwrap();
    });
    let mut client = PublicEventClient::new(
        cfg(
            Source::SpotProduction,
            EventKind::Trade,
            SubscribeMode::Explicit,
        ),
        EventNetworkConfig {
            stream_address: address,
            tls,
        },
        Clock(Instant::now()),
    )
    .unwrap();
    let mut permit = Permit::default();
    client.start(&mut permit).unwrap();
    let mut events = Events::default();
    let end = Instant::now() + Duration::from_secs(2);
    while client.status().phase != Phase::Backoff {
        assert!(Instant::now() < end, "{:?}", client.status());
        client
            .poll(
                PollBudget {
                    max_notifications: 1,
                    ..PollBudget::default()
                },
                &mut permit,
                &mut events,
            )
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    worker.join().unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.gaps, 1);
    assert!(!client.status().usable);
}
