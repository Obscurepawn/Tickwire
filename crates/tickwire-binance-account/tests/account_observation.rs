#[path = "../../tickwire-account/tests/common/mod.rs"]
mod common;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance::{events::*, feed::TimeUnit, request::Market};
use tickwire_binance_account::account_facts::bind_observation;
use tickwire_venue as v;
fn context() -> SessionContext {
    SessionContext::new(
        SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            connection_epoch: 7,
        },
        Market::Usdm,
        PrivateStream::UsdmListenKey,
        TimeUnit::Milliseconds,
    )
    .unwrap()
}
#[test]
fn sparse_funding_observation_preserves_raw_cash_fields_without_local_order() {
    let (s, c, _) = common::futures_fixture();
    let b = CanonicalBaseline::new(s, c).unwrap();
    let ctx = context();
    let raw=br#"{"e":"ACCOUNT_UPDATE","E":100,"T":99,"a":{"m":"FUNDING_FEE","S":"BTCUSDT","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}}"#;
    let event = decode_private::<128>(&ctx, raw, 4096, 2).unwrap();
    let input = bind_observation(
        &b,
        ctx.identity(),
        event.body(),
        event.event_time_ns(),
        event.transaction_time_ns(),
    )
    .unwrap();
    let v::AccountInputBody::Observation(o) = &input.input().body else {
        panic!("observation")
    };
    assert!(o.positions.as_slice().is_empty());
    assert_eq!(o.event_time_ms, 100);
    assert_eq!(o.transaction_time_ms, Some(99));
    assert_eq!(
        o.balances.as_slice()[0].balance_change,
        "-2".parse().unwrap()
    );
    let mut account =
        tickwire_account::Account::from_snapshot(b.snapshot().clone(), b.catalog().clone())
            .unwrap();
    account.apply_account_input(&input).unwrap();
    assert_eq!(
        account.asset(common::key(2)).unwrap().total,
        common::units(1000)
    );
    assert_eq!(
        account.asset(common::key(2)).unwrap().available,
        common::units(998)
    );
    assert!(account.frozen());
}
#[test]
fn observation_rejects_wrong_account_and_unknown_asset() {
    let (s, c, _) = common::futures_fixture();
    let b = CanonicalBaseline::new(s, c).unwrap();
    let ctx = context();
    let raw=br#"{"e":"ACCOUNT_UPDATE","E":100,"T":99,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}}"#;
    let event = decode_private::<128>(&ctx, raw, 4096, 2).unwrap();
    let mut id = ctx.identity().clone();
    id.account += 1;
    assert!(
        bind_observation(
            &b,
            &id,
            event.body(),
            event.event_time_ns(),
            event.transaction_time_ns()
        )
        .is_err()
    );
    let raw=br#"{"e":"ACCOUNT_UPDATE","E":100,"T":99,"a":{"m":"FUNDING_FEE","B":[{"a":"UNKNOWN","wb":"998","cw":"998","bc":"-2"}]}}"#;
    let event = decode_private::<128>(&ctx, raw, 4096, 2).unwrap();
    assert!(
        bind_observation(
            &b,
            ctx.identity(),
            event.body(),
            event.event_time_ns(),
            event.transaction_time_ns()
        )
        .is_err()
    );
}
