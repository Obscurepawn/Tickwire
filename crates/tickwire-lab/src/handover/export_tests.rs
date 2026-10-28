use super::*;
use tickwire_account::{Account, ManagerIdentity, RecoveryRecord, StrategyHandover};
use tickwire_account_codec::CanonicalRecord;
use tickwire_venue as v;

fn fill(command: &v::CanonicalCommand, trade: u64) -> v::CanonicalEvent {
    let c = command.command();
    let v::CommandBody::New(order) = &c.body else {
        panic!("ordinary laboratory order")
    };
    v::CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: c.context.request.risk_domain_id,
            venue: c.context.request.venue,
            environment: c.context.request.environment,
            account: c.context.request.account,
            product: c.context.request.product,
            instrument_id: c.context.instrument.instrument_id,
            instrument_version: c.context.instrument.instrument_version,
            source_connection_epoch: 1,
            source_sequence: Some(trade),
            event_id: v::BoundedText::new(format!("fill-{trade}")).unwrap(),
            venue_time_ns: Some(10 + trade),
            received_mono_ns: 10 + trade,
        },
        body: v::VenueEventBody::Fill(Box::new(v::FillEvent {
            observation: v::OrderObservation {
                order: v::OrderIdentity {
                    order_id: [9; 16],
                    original_intent_id: c.context.request.intent_id,
                    original_intent_exec_epoch: c.context.request.intent_exec_epoch,
                    client_id: Some(order.client_id.clone()),
                    venue_order_id: None,
                    parent_order_id: None,
                    list_id: None,
                    venue_algo_id: None,
                    venue_list_id: None,
                    list_client_id: None,
                    algo_client_id: None,
                },
                status: v::OrderStatus::PartiallyFilled,
                side: order.side,
                position_side: order.position_side,
                original_quantity: Some(v::Decimal::new(1, -3).unwrap()),
                cumulative_quantity: None,
                cumulative_quote: None,
                leaves_quantity: None,
                order_price: order.price,
            },
            trade_id: v::VenueTradeId::new(trade.to_string()).unwrap(),
            last_quantity: v::Decimal::new(5, -4).unwrap(),
            last_price: v::Decimal::new(60000, 0).unwrap(),
            last_quote: Some(v::Decimal::new(30, 0).unwrap()),
            realized_pnl: None,
            liquidity: v::Liquidity::Maker,
            fees: v::BoundedList::new(vec![]).unwrap(),
        })),
    })
    .unwrap()
}

fn history(early_fill: bool, ingress: bool) -> Vec<CanonicalRecord> {
    let (baseline, command) = crate::model::model(1, 1000).unwrap();
    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone()).unwrap();
    let admission = account.admit(&command, 2).unwrap();
    let mut records = vec![RecoveryRecord::Admit {
        command: Box::new(command.clone()),
        evaluated_at: 2,
        expected: admission.image(),
    }];
    let event_record = |trade| {
        let event = Box::new(fill(&command, trade));
        if ingress {
            RecoveryRecord::IngressEvent {
                input_stream: [8; 16],
                input_sequence: trade,
                event,
            }
        } else {
            RecoveryRecord::VenueEvent(event)
        }
    };
    if early_fill {
        account.apply_event(&fill(&command, 1)).unwrap();
        records.push(event_record(1));
    }
    let handover = StrategyHandover {
        scope: baseline.snapshot().scope.clone(),
        account_epoch: baseline.snapshot().account_epoch,
        writer_epoch: command.command().context.request.intent_exec_epoch,
        operation_id: [140; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        expected_ledger_sequence: account.ledger_sequence(),
        expected_orders_digest: account.strategy_orders_digest(8),
        from: ManagerIdentity {
            instance: [2; 16],
            generation: 5,
        },
        to: ManagerIdentity {
            instance: NEW_INSTANCE,
            generation: GENERATION,
        },
        checkpoint_digest: [72; 32],
        input_seal_digest: [73; 32],
        evaluated_at: 20,
        valid_until: 1000,
    };
    account.apply_strategy_handover(&handover).unwrap();
    records.push(RecoveryRecord::StrategyHandover(Box::new(handover)));
    if !early_fill {
        records.push(event_record(1));
    }
    records.push(event_record(2));
    // Both orders of events are legitimate account histories; only the latter
    // meets this laboratory scenario's delayed-fill acceptance contract.
    Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &records,
    )
    .unwrap();
    records
        .into_iter()
        .enumerate()
        .map(|(i, record)| {
            let encoded = CanonicalRecord::new(baseline.digest(), i as u64 + 1, record).unwrap();
            CanonicalRecord::decode(encoded.bytes()).unwrap()
        })
        .collect()
}

#[test]
fn export_rejects_early_fill_even_when_another_fill_follows_handover() {
    for ingress in [false, true] {
        let result = check_handover_records(history(true, ingress).into_iter().map(Ok));
        assert_eq!(
            result
                .expect_err("a later fill cannot hide a fill before handover")
                .to_string(),
            "expected one old admission followed by handover and late fill"
        );
    }
}

#[test]
fn export_accepts_multiple_fills_after_the_unique_handover() {
    for ingress in [false, true] {
        let records = history(false, ingress);
        let expected = (records[1].sequence(), records[1].digest());
        assert_eq!(
            check_handover_records(records.into_iter().map(Ok)).unwrap(),
            expected
        );
    }
}
