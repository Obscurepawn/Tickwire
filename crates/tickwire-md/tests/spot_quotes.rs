use tickwire_md::spot_quotes::{QuoteConfig, Session};

fn config(venue: &str, symbol: &str) -> QuoteConfig {
    serde_json::from_value(serde_json::json!({
        "venue": venue, "symbol":symbol, "instrument_id":21,
        "instrument_version":3, "price_tick":"0.05", "quantity_step":"0.001"
    }))
    .unwrap()
}

const BYBIT: &[u8] = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["10.05","0.003"]],"a":[["10.10","0.004"]],"u":314,"seq":981},"cts":1700000000122}"#;
const BYBIT_ACK: &[u8] =
    br#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"twbbo","conn_id":"id"}"#;
const OKX_ACK: &[u8] = br#"{"id":"twbbo","event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"connId":"id"}"#;
const OKX: &[u8] = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"action":"snapshot","data":[{"bids":[["10.05","0.003","0","3"]],"asks":[],"ts":"1700000000123","seqId":482}]}"#;

#[test]
fn bybit_requires_own_ack_and_a_fresh_snapshot_for_each_connection() {
    let c = config("bybit", "BTCUSDT");
    assert!(Session::new(&c, 1).unwrap().on_text(BYBIT, 11).is_err());
    let mut session = Session::new(&c, 2).unwrap();
    assert!(session.on_text(BYBIT_ACK, 21).unwrap().is_none());
    let q = session.on_text(BYBIT, 22).unwrap().unwrap();
    assert_eq!(
        (
            q.venue_id,
            q.connection_epoch,
            q.instrument_id,
            q.instrument_version
        ),
        (2, 2, 21, 3)
    );
    assert_eq!(
        (
            q.received_mono_ns,
            q.exchange_event_ns,
            q.venue_sequence,
            q.update_id
        ),
        (22, 1700000000123000000, 981, Some(314))
    );
    let bid = q.bid.unwrap();
    assert_eq!((bid.price_ticks, bid.quantity_lots), (201, 3));
    assert_eq!(q.ask.unwrap().price_ticks, 202);
}

#[test]
fn repeated_native_snapshots_replace_in_socket_order() {
    for (venue, symbol, ack, frame, native_seq) in [
        ("bybit", "BTCUSDT", BYBIT_ACK, BYBIT, 981),
        ("okx", "BTC-USDT", OKX_ACK, OKX, 482),
    ] {
        let c = config(venue, symbol);
        let mut session = Session::new(&c, 1).unwrap();
        session.on_text(ack, 10).unwrap();
        for received in [11, 12] {
            let q = session.on_text(frame, received).unwrap().unwrap();
            assert_eq!(
                (q.venue_sequence, q.received_mono_ns),
                (native_seq, received)
            );
        }
        if venue == "bybit" {
            let mut restart: serde_json::Value = serde_json::from_slice(frame).unwrap();
            restart["data"]["u"] = 1.into();
            restart["data"]["seq"] = 8.into();
            let raw = serde_json::to_vec(&restart).unwrap();
            let q = session.on_text(&raw, 13).unwrap().unwrap();
            assert_eq!(
                (q.venue_sequence, q.update_id, q.connection_epoch),
                (8, Some(1), 1)
            );
        }
    }
}

#[test]
fn okx_decodes_one_sided_snapshots_and_rejects_mismatched_or_off_grid_quotes() {
    let c = config("okx", "BTC-USDT");
    let mut s = Session::new(&c, 1).unwrap();
    assert!(
        s.on_text(
            br#"{"event":"notice","code":"64010","msg":"port change","connId":"id"}"#,
            10
        )
        .unwrap()
        .is_none()
    );
    s.on_text(OKX_ACK, 11).unwrap();
    let q = s.on_text(OKX, 12).unwrap().unwrap();
    assert_eq!(q.venue_id, 3);
    assert_eq!(q.ask, None);
    assert_eq!(q.venue_sequence, 482);
    assert_eq!(q.bid.unwrap().price_ticks, 201);
    for (a, b) in [
        ("BTC-USDT", "ETH-USDT"),
        ("10.05", "10.051"),
        ("snapshot", "update"),
        ("bbo-tbt", "books5"),
    ] {
        let mut s = Session::new(&c, 1).unwrap();
        s.on_text(OKX_ACK, 11).unwrap();
        let frame = std::str::from_utf8(OKX).unwrap().replace(a, b);
        assert!(s.on_text(frame.as_bytes(), 12).is_err());
    }
}

#[path = "common/event_join.rs"]
mod join;

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "real memfd requires Linux")]
fn snapshot_and_ring_deliver_exact_bbo_then_invalidate_without_inventing_depth() {
    use tickwire_ipc::{MarketState, MarketStateRecord};
    use tickwire_join::{JoinOwner, TopicRegistry};
    use tickwire_market_publisher::{Limits, TopicPublisher};
    use tickwire_market_source::replica::MarketReplica;
    use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};
    use tickwire_snapshot::{InstrumentSpec, JoinContext, QUOTE};
    use tickwire_venue::{AssetRef, AssetSymbol, ContractKind, InstrumentBinding, Symbol};
    for venue in [2, 3] {
        let (venue_name, symbol, ack, update) = if venue == 2 {
            (
                "bybit",
                "BTCUSDT",
                BYBIT_ACK,
                String::from_utf8(BYBIT.into()).unwrap(),
            )
        } else {
            // Production bbo-tbt frames omit the optional action field.
            ("okx", "BTC-USDT", OKX_ACK, r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["10.05","0.003","0","2"]],"asks":[["10.10","0.004","0","1"]],"ts":"1700000000123","seqId":482}]}"#.to_owned())
        };
        let cfg = config(venue_name, symbol);
        let asset = |id, symbol: &str| AssetRef {
            id,
            version: 1,
            symbol: AssetSymbol::new(symbol).unwrap(),
            exponent: -8,
        };
        let cap = 4096;
        let catalog = [4; 32];
        let i = InstrumentBinding {
            instrument_id: 21,
            instrument_version: 3,
            catalog_version: 1,
            catalog_hash: catalog,
            symbol: Symbol::new(symbol).unwrap(),
            base: asset(1, "BTC"),
            quote: asset(2, "USDT"),
            settlement: asset(2, "USDT"),
            price_tick: "0.05".parse().unwrap(),
            quantity_step: "0.001".parse().unwrap(),
            contract: ContractKind::Cash,
        };
        let instrument = InstrumentSpec {
            instrument_id: 21,
            instrument_version: 3,
            required_states: QUOTE,
        };
        let mut resource: tickwire_md::config::ResourceSpec =
            serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
        resource.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        resource.catalog_version = 1;
        resource.first_seq = 1;
        resource.capacity = 64;
        resource.arena_classes = vec![(65536, 32)];
        let bank = Publisher::<32>::create(resource.header_with_exact_events().unwrap()).unwrap();
        let bank_header = bank.header().clone();
        let reader_fd = bank.reader_fd().unwrap();
        let sub = SubscriptionKey {
            bank: bank.key(),
            instance: 55,
            generation: 1,
            cursor: 56,
        };
        let book = tickwire_book::BookKey {
            venue,
            instrument: 0,
            source: 53,
        };
        let owner = JoinOwner::new(
            bank,
            TopicRegistry {
                catalog_hash: catalog,
                instruments: vec![instrument],
                instrument_set_version: 1,
            },
        )
        .unwrap();
        let mut p = TopicPublisher::new_spot_quotes(
            owner,
            vec![(i, book)],
            Limits {
                levels_per_side: 1,
                cache_bytes: 1 << 20,
                snapshot_bytes: 1 << 20,
                max_delta_bytes: cap,
            },
        )
        .unwrap();
        let mut session = Session::new(&cfg, 1).unwrap();
        session.on_text(ack, 1).unwrap();
        let mut decoded = None;
        let allocations = allocation_counter::measure(|| {
            let quote = session.on_text(update.as_bytes(), 1).unwrap().unwrap();
            quote.publication(book).unwrap().publish(&mut p, 1).unwrap();
            decoded = Some(quote);
        });
        let mut quote = decoded.unwrap();
        assert_eq!(
            allocations.count_total, 0,
            "venue {venue}: first BBO frame to shared memory"
        );
        assert!(p.state().live());
        let (baseline, bytes, expected_resource, j) =
            join::prepare_pending(&mut p, resource.identity.identity(), sub);
        let validated = tickwire_snapshot::validate(
            &bytes,
            &JoinContext {
                expected_resource: &expected_resource,
                catalog_hash: catalog,
                instruments: &[instrument],
                join: &j,
                now_mono_ns: 1,
            },
        )
        .unwrap();
        let mut replica =
            MarketReplica::load(bank_header.topic_id, bank_header.ring_epoch, &validated, 1)
                .unwrap();
        let view = replica.view(21).unwrap();
        assert!(view.continuous());
        assert!(!view.live());
        assert!(view.best_bid().is_none());
        assert!(view.best_ask().is_none());
        assert_eq!(view.quantity(true, 201), None);
        let Some(MarketState::Exact(r)) = view.best_quote() else {
            panic!("missing BBO")
        };
        assert_eq!(
            (
                r.body.source_id,
                r.body.source_connection_epoch,
                r.body.venue_seq_last
            ),
            (53, 1, quote.venue_sequence)
        );
        assert_eq!(
            (
                r.body.bid_price_ticks,
                r.body.bid_quantity_lots,
                r.body.ask_price_ticks
            ),
            (201, 3, 202)
        );
        // SAFETY: this fd and authenticated subscription belong to the bank above.
        let reader =
            unsafe { Subscriber::<64>::attach(reader_fd, bank_header, sub, baseline).unwrap() };
        let apply_pair =
            |replica: &mut MarketReplica, last_sequence: u64, at: u64, expected: [bool; 2]| {
                for (offset, ready) in expected.into_iter().enumerate() {
                    let g = reader.borrow().unwrap().expect("two SHM records per BBO");
                    replica.apply(g.bytes(), &[]).unwrap();
                    let view = replica.view(21).unwrap();
                    assert_eq!(view.trigger().sequence, last_sequence - 1 + offset as u64);
                    assert_eq!(view.callback_ready_at(at, 1), ready);
                    assert!(!view.callback_ready_at(at + 2, 1));
                    assert!(!view.callback_ready_at(0, 1));
                    assert!(!view.live(), "a BBO never implies a depth book");
                }
                assert!(reader.borrow().unwrap().is_none());
            };
        // A healthy update after JOIN has one callback, at the aggregate commit.
        quote.received_mono_ns = 2;
        quote.venue_sequence += 1;
        quote.publication(book).unwrap().publish(&mut p, 2).unwrap();
        apply_pair(&mut replica, baseline + 2, 3, [false, true]);
        p.spot_quote_unavailable(21, 1, 3).unwrap();
        assert!(!p.state().live());
        apply_pair(&mut replica, baseline + 4, 3, [false, false]);
        let view = replica.view(21).unwrap();
        assert!(!view.continuous());
        let q = view.best_quote().unwrap();
        assert_eq!(q.feed_state(), 3);
        assert_eq!(q.header().ingress_mono_ns, 2);
        let mut fresh = quote;
        fresh.connection_epoch = 2;
        fresh.venue_sequence = 1;
        fresh.received_mono_ns = 4;
        fresh.publication(book).unwrap().publish(&mut p, 5).unwrap();
        assert!(p.state().live());
        assert!(p.spot_quote_unavailable(21, 1, 6).is_err());
        apply_pair(&mut replica, baseline + 6, 5, [false, true]);
        let v = replica.view(21).unwrap();
        assert!(v.continuous());
        let Some(MarketState::Exact(q)) = v.best_quote() else {
            panic!("missing refreshed BBO")
        };
        assert_eq!(
            (
                q.body.source_connection_epoch,
                q.body.venue_seq_last,
                q.header.ingress_mono_ns
            ),
            (2, 1, 4)
        );
        // The pending reader holds its watermark. A full ring cannot publish half
        // a BBO or leave a publisher advertising a usable topic.
        let mut last = p.split().0.published();
        for n in 0..=64 {
            fresh.received_mono_ns = n + 6;
            fresh.venue_sequence += 1;
            let event = fresh.publication(book).unwrap();
            if event.publish(&mut p, n + 6).is_err() {
                break;
            }
            last += 2;
        }
        assert!(p.needs_bank());
        assert!(!p.state().live());
        assert_eq!(p.split().0.published(), last);
        assert!(p.spot_quote_unavailable(21, 2, 80).is_err());
    }
}
