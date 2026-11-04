use super::shared::Fixture;
use serde_json::json;
use tickwire_book::BookKey;
use tickwire_market_publisher::{Limits, TopicPublisher};
use tickwire_md::spot_quotes::{QuoteConfig, Session};
use tickwire_venue::InstrumentBinding;

fn key(venue: u32) -> BookKey {
    BookKey {
        venue,
        instrument: 0,
        source: venue,
    }
}
pub fn setup(f: &mut Fixture, binding: &mut InstrumentBinding, venue: u32) -> QuoteConfig {
    let (name, symbol) = match venue {
        2 => ("bybit", "BTCUSDT"),
        3 => ("okx", "BTC-USDT"),
        _ => panic!("unknown spot venue"),
    };
    binding.symbol = tickwire_venue::Symbol::new(symbol).unwrap();
    let mut resource: tickwire_md::config::ResourceSpec =
        serde_json::from_str(include_str!("../../../tickwire-md/examples/topic-101.json")).unwrap();
    resource.identity = f.config.identity.clone();
    resource.capacity = 64;
    resource.arena_classes = vec![(65536, 32)];
    let registry = tickwire_join::TopicRegistry {
        catalog_hash: binding.catalog_hash,
        instrument_set_version: 1,
        instruments: vec![tickwire_snapshot::InstrumentSpec {
            instrument_id: binding.instrument_id,
            instrument_version: binding.instrument_version,
            required_states: tickwire_snapshot::QUOTE,
        }],
    };
    let owner = tickwire_join::JoinOwner::new(
        tickwire_shm::Publisher::<32>::create(resource.header_with_exact_events().unwrap())
            .unwrap(),
        registry,
    )
    .unwrap();
    f.topics[0] = TopicPublisher::new_spot_quotes(
        owner,
        vec![(binding.clone(), key(venue))],
        Limits {
            levels_per_side: 1,
            cache_bytes: 1 << 20,
            snapshot_bytes: 1 << 20,
            max_delta_bytes: 4096,
        },
    )
    .unwrap();
    serde_json::from_value(json!({"venue":name,"symbol":symbol,"instrument_id":binding.instrument_id,"instrument_version":binding.instrument_version,
        "price_tick":binding.price_tick.as_plain_string(),"quantity_step":binding.quantity_step.as_plain_string()})).unwrap()
}
pub fn subscribe(cfg: &QuoteConfig) -> Session<'_> {
    let mut session = Session::new(cfg, 1).unwrap();
    let ack = if cfg.venue.id() == 2 {
        json!({"op":"subscribe","success":true,"req_id":"twbbo"})
    } else {
        json!({"id":"twbbo","event":"subscribe","arg":{"channel":"bbo-tbt","instId":cfg.symbol}})
    };
    session
        .on_text(&serde_json::to_vec(&ack).unwrap(), 0)
        .unwrap();
    session
}
pub fn publish(
    f: &mut Fixture,
    session: &mut Session<'_>,
    binding: &InstrumentBinding,
    venue: u32,
    sequence: u64,
    bid: i64,
    now: u64,
) {
    let price = |p: i64| {
        tickwire_venue::Decimal::new(
            i128::from(p) * binding.price_tick.coefficient(),
            binding.price_tick.exponent(),
        )
        .unwrap()
        .as_plain_string()
    };
    let lots = binding.quantity_step.as_plain_string();
    let s = binding.symbol.as_str();
    let snapshot = if venue == 2 {
        json!({"topic":format!("orderbook.1.{s}"),"type":"snapshot","ts":1793764800000u64,"data":{"s":s,"b":[[price(bid),lots]],"a":[[price(bid+1),lots]],"u":sequence,"seq":sequence}})
    } else {
        json!({"arg":{"channel":"bbo-tbt","instId":s},"data":[{"bids":[[price(bid),lots,"0","1"]],"asks":[[price(bid+1),lots,"0","1"]],"ts":"1793764800000","seqId":sequence}]})
    };
    let quote = session
        .on_text(&serde_json::to_vec(&snapshot).unwrap(), now)
        .unwrap()
        .unwrap();
    quote
        .publication(key(venue))
        .unwrap()
        .publish(&mut f.topics[0], now)
        .unwrap();
}
