use super::*;
use crate::gateway_fixture as gateway;
use crate::reconciliation::income_import::tests::Memory;

#[test]
fn actual_nested_rest_amend_result_is_durable_before_reducing_the_original_reservation() {
    let (_directory, original, mut g, _auth, peer) = gateway::make(false, 16384);
    let cfg = Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline: g.baseline().clone(),
        account_stream: [2; 16],
        max_records: 64,
        max_registrations: 8,
        max_raw_bytes: 32768,
        max_retained_bytes: 1 << 20,
        income_association_authority: None,
        income_baseline_authority: None,
    };
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    input
        .submit(&mut g, original.clone(), &[[7; 16]], 60, gateway::until())
        .unwrap();
    gateway::drive(&mut g, Status::Flushed);
    let ack=br#"{"symbol":"BTCUSDT","orderId":21,"orderListId":-1,"clientOrderId":"test-1","status":"NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"}"#;
    input
        .persist_rest(
            &mut g,
            original.clone(),
            7,
            61,
            200,
            ack.to_vec(),
            Some(ack),
        )
        .unwrap();
    let mut c = original.command().clone();
    c.context.request.intent_id = 4;
    c.context.request.recovery_object = 8;
    c.body = v::CommandBody::AmendKeepPriority {
        target: v::OrderTarget {
            order_id: [7; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(v::ClientId::new("test-1").unwrap()),
            venue_order_id: Some(v::VenueOrderId::new("21").unwrap()),
        },
        new_quantity: "0.0008".parse().unwrap(),
        new_client_id: Some(v::ClientId::new("test-1").unwrap()),
    };
    let request = CanonicalCommand::new(c).unwrap();
    input
        .submit(&mut g, request.clone(), &[], 62, gateway::until())
        .unwrap();
    assert_eq!(
        g.account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60)
    );
    let body=br#"{"transactTime":1741926410255,"executionId":75,"amendedOrder":{"symbol":"BTCUSDT","orderId":21,"orderListId":-1,"origClientOrderId":"test-1","clientOrderId":"test-1","price":"60000","qty":"0.0008","executedQty":"0","preventedQty":"0","quoteOrderQty":"0","cumulativeQuoteQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","workingTime":1741926410255,"selfTradePreventionMode":"NONE"}}"#;
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend_from_slice(body);
    let result = input
        .persist_rest(&mut g, request, 7, 63, 200, raw, Some(body))
        .unwrap();
    assert_eq!(result.disposition, RestDisposition::Observed);
    assert_eq!(result.applied_events, 1);
    assert_eq!(
        g.account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(48)
    );
    assert!(matches!(
        input.stored_rest().last().unwrap().events[0].event().body,
        v::VenueEventBody::Amendment(_)
    ));
    drop(g);
    assert!(!peer.join().unwrap().is_empty());
}

#[test]
fn explicit_amend_rejection_is_durable_and_preserves_the_original_order() {
    let (_directory, original, mut g, _auth, peer) = gateway::make(false, 16384);
    let cfg = Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline: g.baseline().clone(),
        account_stream: [2; 16],
        max_records: 64,
        max_registrations: 8,
        max_raw_bytes: 32768,
        max_retained_bytes: 1 << 20,
        income_association_authority: None,
        income_baseline_authority: None,
    };
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    input
        .submit(&mut g, original.clone(), &[[7; 16]], 60, gateway::until())
        .unwrap();
    gateway::drive(&mut g, Status::Flushed);
    let ack=br#"{"symbol":"BTCUSDT","orderId":21,"orderListId":-1,"clientOrderId":"test-1","status":"NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"}"#;
    input
        .persist_rest(
            &mut g,
            original.clone(),
            7,
            61,
            200,
            ack.to_vec(),
            Some(ack),
        )
        .unwrap();
    let mut c = original.command().clone();
    c.context.request.intent_id = 4;
    c.context.request.recovery_object = 8;
    c.body = v::CommandBody::AmendKeepPriority {
        target: v::OrderTarget {
            order_id: [7; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(v::ClientId::new("test-1").unwrap()),
            venue_order_id: Some(v::VenueOrderId::new("21").unwrap()),
        },
        new_quantity: "0.0008".parse().unwrap(),
        new_client_id: Some(v::ClientId::new("test-1").unwrap()),
    };
    let request = CanonicalCommand::new(c).unwrap();
    input
        .submit(&mut g, request.clone(), &[], 62, gateway::until())
        .unwrap();
    assert_eq!(
        g.account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60)
    );
    let body = br#"{"code":-2038,"msg":"Order amend request rejected."}"#;
    let mut raw = format!(
        "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    raw.extend_from_slice(body);
    let result = input
        .persist_rest(&mut g, request, 7, 63, 400, raw, Some(body))
        .unwrap();
    assert_eq!(result.disposition, RestDisposition::Rejected);
    assert_eq!(
        result.applied_events, 1,
        "matching rejection must end only this pending modification"
    );
    assert_eq!(
        g.account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60)
    );
    assert!(
        !g.account()
            .order_keys()
            .any(|key| g.account().order(key).unwrap().aborted)
    );
    drop(g);
    assert!(!peer.join().unwrap().is_empty());
}
