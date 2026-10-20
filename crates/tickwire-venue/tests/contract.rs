mod common;
use common::*;
use tickwire_venue::*;
#[test]
fn complete_command_is_immutable_and_canonical_roundtrip() {
    let source = command();
    let prepared = CanonicalCommand::new(source.clone()).unwrap();
    assert_eq!(prepared.command(), &source);
    assert_eq!(
        CanonicalCommand::decode(prepared.bytes()).unwrap(),
        prepared
    );
}
#[test]
fn economic_fields_change_the_digest() {
    let original = CanonicalCommand::new(command()).unwrap();
    let mut changed = command();
    if let CommandBody::New(ref mut order) = changed.body {
        order.side = Side::Sell;
    }
    assert_ne!(
        original.digest(),
        CanonicalCommand::new(changed).unwrap().digest()
    );
}
#[test]
fn market_with_price_is_rejected() {
    let mut changed = command();
    if let CommandBody::New(ref mut order) = changed.body {
        order.order_type = OrderType::Market;
    }
    assert_eq!(
        CanonicalCommand::new(changed).unwrap_err(),
        Error::OrderCombination
    );
}
#[test]
fn decimal_normalization_and_minimum_integer() {
    assert_eq!(d(1200, -3), d(12, -1));
    assert_eq!(d(1200, -3).as_plain_string(), "1.2");
    assert_eq!(d(i128::MIN, 0).as_plain_string(), i128::MIN.to_string());
}

#[test]
fn unicode_catalog_symbols_are_preserved_without_folding() {
    let mut c = command();
    c.context.instrument.symbol = Symbol::new("１２３４５６").unwrap();
    c.context.instrument.base.symbol = AssetSymbol::new("币").unwrap();
    let canonical = CanonicalCommand::new(c.clone()).unwrap();
    assert_eq!(
        CanonicalCommand::decode(canonical.bytes())
            .unwrap()
            .command(),
        &c
    );
}
