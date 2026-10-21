use super::*;
use crate::gateway_fixture as gateway;
use crate::reconciliation::income_import::tests::Memory;

#[test]
fn list_response_persists_parent_and_each_real_report_without_parent_release() {
    let (_directory, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let mut command = command.command().clone();
    let v::CommandBody::New(first) = command.body.clone() else {
        unreachable!()
    };
    let mut second = first.clone();
    second.client_id = v::ClientId::new("pending").unwrap();
    command.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [71; 16],
        client_id: v::ClientId::new("list-parent").unwrap(),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![first, second]).unwrap(),
    });
    let command = CanonicalCommand::new(command).unwrap();
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
        .submit(
            &mut g,
            command.clone(),
            &[[7; 16], [8; 16]],
            60,
            gateway::until(),
        )
        .unwrap();
    let before = g.account().asset(gateway::fixture::key(2)).unwrap();
    let body=br#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":"list-parent","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}],"orderReports":[{"symbol":"BTCUSDT","orderId":21,"orderListId":55,"clientOrderId":"test-1","status":"NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"},{"symbol":"BTCUSDT","orderId":22,"orderListId":55,"clientOrderId":"pending","status":"PENDING_NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"}]}"#;
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend_from_slice(body);
    let result = input
        .persist_rest(&mut g, command, 7, 80, 200, raw, Some(body))
        .unwrap();
    assert_eq!(result.disposition, RestDisposition::Observed);
    assert_eq!(result.applied_events, 3, "parent plus two actual reports");
    assert_eq!(g.account().asset(gateway::fixture::key(2)).unwrap(), before);
    let parent = g.account().structure(&[71; 16]).unwrap();
    assert!(parent.terminal);
    assert!(
        parent
            .legs
            .iter()
            .all(|k| !g.account().order(k).unwrap().terminal)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn parent_query_persists_exact_identity_before_child_queries() {
    let (_directory, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let mut command = command.command().clone();
    let v::CommandBody::New(first) = command.body.clone() else {
        unreachable!()
    };
    let mut second = first.clone();
    second.client_id = v::ClientId::new("pending").unwrap();
    command.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [71; 16],
        client_id: v::ClientId::new("list-parent").unwrap(),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![first, second]).unwrap(),
    });
    let command = CanonicalCommand::new(command).unwrap();
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
        .submit(
            &mut g,
            command.clone(),
            &[[7; 16], [8; 16]],
            60,
            gateway::until(),
        )
        .unwrap();
    let before = g.account().asset(gateway::fixture::key(2)).unwrap();
    let body=br#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":"list-parent","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}],"orderReports":[{"symbol":"BTCUSDT","orderId":21,"orderListId":55,"clientOrderId":"test-1","status":"NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"},{"symbol":"BTCUSDT","orderId":22,"orderListId":55,"clientOrderId":"pending","status":"PENDING_NEW","side":"BUY","origQty":"0.001","executedQty":"0","price":"60000"}]}"#;
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend_from_slice(body);
    let result = input
        .persist_structure_query(
            &mut g,
            crate::reconciliation::structures::StructureQuery {
                intent: intent(&command),
                structure_id: [71; 16],
                private_connection_epoch: 2,
                gap_generation: 3,
            },
            7,
            tickwire_clock::ClockDomain::new([42; 16], 1).unwrap(),
            80,
            200,
            raw,
            Some(body),
        )
        .unwrap();
    assert_eq!(result.disposition, RestDisposition::Observed);
    assert_eq!(
        result.applied_events, 1,
        "parent query is not a child status query"
    );
    let recorded = input
        .records
        .iter()
        .find_map(|r| {
            if let Record::Rest(r) = r {
                Some(r.clone())
            } else {
                None
            }
        })
        .unwrap();
    let bytes = codec::encode(&Record::Rest(recorded.clone())).unwrap();
    assert_eq!(bytes[8], 21);
    assert_eq!(
        codec::encode(&codec::decode(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut altered = recorded.clone();
    let raw = String::from_utf8(altered.raw)
        .unwrap()
        .replace("ALL_DONE", "REJECTED");
    altered.raw = raw.into_bytes();
    assert!(
        input.validate_rest(&altered).is_err(),
        "canonical event must remain bound to its actual parent query body"
    );

    assert_eq!(g.account().asset(gateway::fixture::key(2)).unwrap(), before);
    let parent = g.account().structure(&[71; 16]).unwrap();
    assert!(parent.terminal);
    assert!(
        parent
            .legs
            .iter()
            .all(|k| !g.account().order(k).unwrap().terminal)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
