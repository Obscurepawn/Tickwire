use super::{
    common::{ID, KEY},
    venue,
};
use tickwire_binance::market::*;
use tickwire_binance_public::{DepthEncoding, Notice, SyncOutcome};
use tickwire_book::{BinanceSpot, Book, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};

pub fn event(kind: EventKind, n: u64) -> Event {
    let instrument = venue::command().context.instrument;
    Event {
        identity: StreamIdentity {
            source_id: 1,
            connection_epoch: 1,
            instrument_id: ID,
            instrument_version: instrument.instrument_version,
            catalog_version: instrument.catalog_version,
            catalog_hash: instrument.catalog_hash,
        },
        times: EventTimes {
            received_mono_ns: n,
            exchange_event_ns: None,
            transaction_ns: None,
        },
        encoding: EventEncoding::Json,
        value: match kind {
            EventKind::Trade => EventValue::Trade(Trade {
                trade_id: n,
                raw_trade_range: None,
                price_ticks: 15000,
                quantity_lots: 100,
                normal_quantity_lots: None,
                buyer_is_maker: false,
                aggregate: false,
            }),
            EventKind::BestBidAsk => EventValue::BestBidAsk(BestBidAsk {
                update_id: n,
                bid_price_ticks: 10000,
                bid_quantity_lots: 100,
                ask_price_ticks: 20000,
                ask_quantity_lots: 100,
            }),
            EventKind::MarkFunding => EventValue::MarkFunding(MarkFunding {
                mark_price: venue::d(15000, -2),
                index_price: venue::d(14900, -2),
                estimated_settlement_price: None,
                moving_average: None,
                funding_rate: Some(venue::d(-3, -4)),
                next_funding_utc_ns: None,
            }),
        },
    }
}

pub fn depth_book(count: usize, sequence: u64) -> Book<BinanceSpot, 4096, 1> {
    let asks = (0..count)
        .map(|i| Level {
            price: PriceTicks(20000 + i as i64),
            quantity: QtyLots(100),
        })
        .collect::<Vec<_>>();
    let mut book = Book::new(KEY, 1);
    book.install_snapshot(Snapshot {
        key: KEY,
        generation: 1,
        last_update_id: sequence - 1,
        bids: &[Level {
            price: PriceTicks(10000),
            quantity: QtyLots(100),
        }],
        asks: &asks,
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key: KEY,
        generation: 1,
        first_update_id: sequence,
        last_update_id: sequence,
        previous_update_id: None,
        received_at_ns: sequence,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    book
}

pub fn notice<'a, const L: usize, const U: usize>(
    book: &'a Book<BinanceSpot, L, U>,
    raw: Option<&'a [u8]>,
    encoding: DepthEncoding,
    n: u64,
) -> Notice<'a> {
    Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: Some(n),
        raw_delta: raw,
        encoding,
        outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
            last_update_id: n,
            became_live: true,
        }),
        view: Some(book.view(n, 100).unwrap()),
    }
}

pub fn json(count: usize, sequence: u64) -> Vec<u8> {
    let asks = (0..count)
        .map(|i| {
            let price = 20000 + i;
            [
                format!("{}.{:02}", price / 100, price % 100),
                "0.001".to_owned(),
            ]
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({"e":"depthUpdate", "s":"BTCUSDT", "E":1,
        "U":sequence, "u":sequence, "b":[], "a":asks}))
    .unwrap()
}

/// Fixed SBE 1.0 depth-diff layout, matching the independent literal fixture.
pub fn sbe(count: usize, sequence: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    for v in [26u16, 10003, 1, 0] {
        bytes.extend(v.to_le_bytes());
    }
    bytes.extend(1_000_000i64.to_le_bytes());
    bytes.extend(sequence.to_le_bytes());
    bytes.extend(sequence.to_le_bytes());
    bytes.extend([(-2i8) as u8, (-5i8) as u8]);
    bytes.extend(16u16.to_le_bytes());
    bytes.extend(0u16.to_le_bytes());
    bytes.extend(16u16.to_le_bytes());
    bytes.extend(u16::try_from(count).unwrap().to_le_bytes());
    for i in 0..count {
        bytes.extend((20000 + i as i64).to_le_bytes());
        bytes.extend(100i64.to_le_bytes());
    }
    bytes.push(7);
    bytes.extend(b"BTCUSDT");
    bytes
}
