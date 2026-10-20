//! A fully local, deterministic service roundtrip. No credentials or live endpoints.
use tickwire_book::{BinanceSpot, Book, BookKey, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_execution::{ports::Journal, *};
use tickwire_ipc::{IntentBody, IntentRecord, Record, RecordHeader, Wire, decode_record};
use tickwire_mock::MockRuntime;
use tickwire_risk::{Fixed, spot::partial_buy_fill};

fn main() {
    let summary = run_scenario();
    println!(
        "market -> strategy -> risk -> IPC -> durable journal -> exchange -> fill/cancel -> recovery"
    );
    println!(
        "submit_calls={} spent={} held={} recovered_frozen={}",
        summary.0, summary.1, summary.2, summary.3
    );
}

fn run_scenario() -> (usize, i128, i128, bool) {
    let key = BookKey {
        venue: 7,
        instrument: 1001,
        source: 1,
    };
    let mut book = Book::<BinanceSpot, 8, 8>::new(key, 1);
    let level = |p, q| Level {
        price: PriceTicks(p),
        quantity: QtyLots(q),
    };
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 100,
        bids: &[level(99, 10)],
        asks: &[level(101, 10)],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key,
        generation: 1,
        first_update_id: 101,
        last_update_id: 101,
        previous_update_id: None,
        received_at_ns: 1,
        bids: &[],
        asks: &[level(101, 12)],
    })
    .unwrap();
    let view = book.view(2, 10).unwrap();
    let price = view.known_asks()[0].price.0;
    // The example strategy buys two lots with a price cap at the observed ask.
    // All amounts use scale 0 to make the reservation and economic ledger visible.
    let fixed = |n| Fixed::from_atoms(n, 0).unwrap();
    let budget = partial_buy_fill(
        fixed(2),
        fixed(price.into()),
        fixed(0),
        fixed(price.into()),
        fixed(2),
    )
    .unwrap()
    .original_reserve
    .atoms();
    let intent = IntentRecord {
        header: RecordHeader {
            sequence: 1,
            message_type: 513,
            schema_major: 1,
            header_bytes: 128,
            body_bytes: 896,
            topic_or_scope_id: 1,
            producer_generation: 1,
            instrument_id: 1001,
            instrument_version: 1,
            ..Default::default()
        },
        body: IntentBody {
            logical_strategy_id: 1,
            instance_id: [1; 16],
            strategy_generation: 1,
            exec_generation: 1,
            authorization_epoch: 1,
            intent_seq: 1,
            command_kind: 1,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            price_ticks: price,
            qty_lots: 2,
            ..Default::default()
        },
    };
    let mut bytes = [0; 1024];
    intent.encode(&mut bytes).unwrap();
    let Record::Intent(wire) = decode_record(&bytes).unwrap() else {
        panic!("intent record required")
    };
    let domain = Domain {
        venue: key.venue,
        account: 9,
        product: Product::Spot,
    };
    let gate = Gate {
        epoch: 1,
        strategy_generation: 1,
        config_revision: 1,
        now: 2,
        lease_until: 100,
        enabled: true,
        market_valid: true,
        account_valid: true,
        recovered: true,
        owner_isolated: true,
    };
    let mut services = MockRuntime::<8, 8>::new(domain, 1000, gate).unwrap();
    let request = OrderRequest {
        key: OrderKey {
            venue: domain.venue,
            account: domain.account,
            product: domain.product,
            intent: wire.body.intent_seq,
        },
        quantity: wire.body.qty_lots.try_into().unwrap(),
        unit_budget: wire.body.price_ticks.into(),
        fee_budget: 2,
        epoch: wire.body.exec_generation,
        strategy_generation: wire.body.strategy_generation,
        config_revision: 1,
        deadline: 90,
        recovery_object: 1,
        commit_profile: CommitProfile::LocalDurable,
    };
    services
        .submit(request, CommitProfile::LocalDurable)
        .unwrap();
    assert_eq!(services.engine.held(), budget);
    assert_eq!(services.journal.committed().len(), 1);
    let fill = Fill {
        trade_id: 102,
        quantity: 1,
        cost: 100,
    };
    services.record_fill(1, fill).unwrap();
    services.record_fill(1, fill).unwrap();
    services.record_terminal(1, Terminal::Canceled, 1).unwrap();
    services.record_fee(1, 1, 1).unwrap();
    services.restart().unwrap();
    assert_eq!(services.engine.spent(), 101);
    assert_eq!(services.engine.held(), 1);
    assert_eq!(services.exchange.submit_calls, 1);
    assert!(services.engine.frozen());
    (
        services.exchange.submit_calls,
        services.engine.spent(),
        services.engine.held(),
        services.engine.frozen(),
    )
}

#[test]
fn roundtrip_is_deterministic_and_recovery_preserves_accounting() {
    assert_eq!(run_scenario(), run_scenario());
    assert_eq!(run_scenario(), (1, 101, 1, true));
}
