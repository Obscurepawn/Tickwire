#![allow(dead_code)]
pub mod venue;
use tickwire_account::*;
use tickwire_venue as v;
pub fn f(n: i128) -> Fixed {
    Fixed::from_atoms(n, 8).unwrap()
}
pub fn units(n: i128) -> Fixed {
    f(n * 100_000_000)
}
pub fn key(id: u64) -> AssetKey {
    AssetKey {
        wallet_id: 1,
        asset_id: id,
    }
}
pub fn fixture() -> (Snapshot, Catalog, v::Command) {
    let cmd = venue::command();
    let inst = cmd.context.instrument.clone();
    let snapshot = Snapshot {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
        },
        account_epoch: 1,
        ledger_sequence: 0,
        revisions: Revisions {
            risk: 10,
            fee: 1,
            bracket: 1,
            collateral: 1,
            valuation: 1,
        },
        valid_until: 1000,
        assets: vec![
            AssetSnapshot {
                key: key(1),
                asset: inst.base.clone(),
                free: units(1),
                locked: f(0),
                nontrade_locked: f(0),
                exclusive_fee_pool: false,
                ownership: vec![],
            },
            AssetSnapshot {
                key: key(2),
                asset: inst.quote.clone(),
                free: units(1000),
                locked: f(0),
                nontrade_locked: f(0),
                exclusive_fee_pool: false,
                ownership: vec![],
            },
        ],
        positions: vec![],
        venue_available: vec![],
    };
    let catalog = Catalog {
        entries: vec![InstrumentPolicy {
            instrument: inst,
            product: v::Product::Spot,
            wallet_id: 1,
            fees: FeePolicy {
                rates: [f(0); 3],
                maximum_fills: 10,
                exclusive_pool: None,
            },
            futures: None,
            max_absolute_position: units(100),
            max_gross_quantity: units(100),
            strategy_limits: vec![(8, units(100))],
        }],
    };
    (snapshot, catalog, cmd)
}
pub fn canonical(cmd: v::Command) -> v::CanonicalCommand {
    v::CanonicalCommand::new(cmd).unwrap()
}

pub fn setup() -> (Account, v::Command) {
    let (s, c, cmd) = fixture();
    (Account::from_snapshot(s, c).unwrap(), cmd)
}
pub fn fill(cmd: &v::Command, trade: &str, q: &str, price: &str, quote: &str) -> v::VenueEvent {
    let mut e = venue::event();
    e.identity.product = cmd.context.request.product;
    let v::VenueEventBody::Fill(ref mut f) = e.body else {
        unreachable!()
    };
    let v::CommandBody::New(ref o) = cmd.body else {
        unreachable!()
    };
    f.observation.order.original_intent_id = cmd.context.request.intent_id;
    f.observation.order.original_intent_exec_epoch = cmd.context.request.intent_exec_epoch;
    f.observation.order.client_id = Some(o.client_id.clone());
    f.observation.order.venue_order_id = None;
    f.observation.side = o.side;
    f.observation.position_side = o.position_side;
    f.observation.original_quantity = match o.quantity {
        v::Quantity::Base(q) => Some(q),
        _ => None,
    };
    f.observation.status = v::OrderStatus::PartiallyFilled;
    f.observation.cumulative_quantity = None;
    f.observation.cumulative_quote = None;
    f.observation.leaves_quantity = None;
    f.last_quantity = q.parse().unwrap();
    f.last_price = price.parse().unwrap();
    f.last_quote = Some(quote.parse().unwrap());
    f.realized_pnl = None;
    f.trade_id = venue::text(trade);
    f.fees = v::BoundedList::new(vec![]).unwrap();
    e
}
pub fn event(e: v::VenueEvent) -> v::CanonicalEvent {
    v::CanonicalEvent::new(e).unwrap()
}
pub fn fee(cmd: &v::Command, id: &str, asset: v::AssetRef, amount: &str) -> v::VenueEvent {
    let f = fill(cmd, "unused", "0.001", "60000", "60");
    let v::VenueEventBody::Fill(fill) = f.body else {
        unreachable!()
    };
    v::VenueEvent {
        identity: f.identity,
        body: v::VenueEventBody::Fee {
            order: fill.observation.order,
            fee_id: venue::text(id),
            deltas: v::BoundedList::new(vec![v::AssetDelta {
                asset,
                amount: amount.parse().unwrap(),
            }])
            .unwrap(),
        },
    }
}
pub fn terminal(cmd: &v::Command, cumulative: &str) -> v::VenueEvent {
    let mut f = fill(cmd, "unused", "0.001", "60000", "60");
    let v::VenueEventBody::Fill(ref mut fill) = f.body else {
        unreachable!()
    };
    fill.observation.status = v::OrderStatus::Canceled;
    fill.observation.cumulative_quantity = Some(cumulative.parse().unwrap());
    f.body = v::VenueEventBody::Order(Box::new(fill.observation.clone()));
    f
}
pub fn futures_fixture() -> (Snapshot, Catalog, v::Command) {
    let (mut s, mut c, mut cmd) = fixture();
    cmd.context.request.product = v::Product::LinearPerpetual;
    cmd.context.instrument.contract = v::ContractKind::Perpetual;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.mode = v::ProductMode::Derivative {
        position_mode: v::PositionMode::OneWay,
        margin_mode: v::MarginMode::Cross,
    };
    o.price = Some(venue::d(100, 0));
    o.quantity = v::Quantity::Base(venue::d(1, 0));
    c.entries[0].instrument = cmd.context.instrument.clone();
    c.entries[0].product = v::Product::LinearPerpetual;
    c.entries[0].futures = Some(FuturesPolicy {
        position_mode: v::PositionMode::OneWay,
        margin_mode: v::MarginMode::Cross,
        stress_low: units(100),
        stress_high: units(100),
        configured_leverage: units(10),
        venue_leverage: units(10),
        brackets: vec![tickwire_risk::futures::Bracket {
            floor: units(0),
            cap: units(1000000),
            rate: f(5_000_000),
            cumulative: units(0),
            max_initial_leverage: units(10),
        }],
        maintenance_addon: units(0),
        exit_fee_rate: f(0),
        funding_rate_abs_upper: f(0),
        funding_settlements: 0,
        pending_outbound: units(0),
        capital_buffer: units(0),
    });
    s.venue_available = vec![(1, units(1000))];
    (s, c, cmd)
}
