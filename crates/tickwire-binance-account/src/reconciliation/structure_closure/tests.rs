use super::*;
use crate::gateway_fixture::{self as gateway, fixture};
use crate::reconciliation::income_import::tests::http;
use tickwire_account::RecoveryRecord;
use tickwire_account_codec::CanonicalRecord;
use tickwire_account_journal::{AccountJournal, FileBackend, RecoveryVerifier, VerificationError};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_journal::store::FileStore;
use tickwire_recovery::{FileBlobStore, SharedBlobStore};

type Input = AccountIngress<FileStore>;
fn fixture_data() -> (
    gateway::Directory,
    Input,
    Account,
    AccountJournal<FileBackend>,
    ClosureRequest,
) {
    let directory = gateway::Directory::new();
    let (snapshot, catalog, mut command) = fixture::futures_fixture();
    let v::CommandBody::New(mut order) = command.body.clone() else {
        unreachable!()
    };
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: v::Decimal::new(110, 0).unwrap(),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    command.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [78; 16],
        order,
    });
    let command = CanonicalCommand::new(command).unwrap();
    let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
    let mut account = Account::from_snapshot(snapshot, catalog).unwrap();
    let mut journal = AccountJournal::create(
        FileBackend::open(directory.0.join("account")).unwrap(),
        gateway::config(&baseline).journal,
        baseline.clone(),
    )
    .unwrap();
    let admission = account.admit(&command, 60).unwrap();
    journal
        .append(RecoveryRecord::Admit {
            command: Box::new(command.clone()),
            evaluated_at: 60,
            expected: admission.image(),
        })
        .unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline,
        account_stream: [2; 16],
        max_records: 128,
        max_registrations: 8,
        max_raw_bytes: 32768,
        max_retained_bytes: 1 << 20,
        income_baseline_authority: None,
        income_association_authority: None,
    };
    let mut input = Input::create(FileStore::open(directory.0.join("input")).unwrap(), config)
        .unwrap()
        .with_closure_store(SharedBlobStore::new(
            FileBlobStore::open(directory.0.join("objects")).unwrap(),
        ));
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    input
        .append(Record::BeginReconciliation {
            epoch: 7,
            generation: 3,
        })
        .unwrap();
    let reg = &input.registry[&intent(&command)];
    let body=serde_json::to_vec(&serde_json::json!({"algoId":800,"clientAlgoId":reg.orders[0].client_id.as_ref().unwrap().as_str(),"algoType":"CONDITIONAL","orderType":"STOP","symbol":"BTCUSDT","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"1","algoStatus":"CANCELED","actualOrderId":"","actualQty":"0","actualPrice":"0","triggerTime":0,"triggerPrice":"110","price":"100","workingType":"CONTRACT_PRICE","reduceOnly":false,"closePosition":false})).unwrap();
    let event = tickwire_binance::events::bind_structure_response(
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            account: 2,
            environment: v::Environment::Test,
            connection_epoch: 8,
        },
        Market::Usdm,
        &command,
        &reg.orders,
        &body,
        70,
    )
    .unwrap();
    let domain = ClockDomain::new([8; 16], 1).unwrap();
    let record = RestRecord {
        amendment_query: None,
        command: command.clone(),
        query: None,
        order_query: None,
        structure_query: Some(structures::StructureQuery {
            intent: intent(&command),
            structure_id: [78; 16],
            private_connection_epoch: 7,
            gap_generation: 3,
        }),
        structure_domain: Some(domain),
        epoch: 8,
        received: 70,
        status: 200,
        raw: http(&body),
        disposition: RestDisposition::Observed,
        events: vec![event.clone()],
    };
    input.validate_rest(&record).unwrap();
    let parent_query = input.append(Record::Rest(record)).unwrap().sequence;
    registry::learn(&mut input.registry, &event).unwrap();
    let key = input
        .append(Record::RestEffect {
            response: parent_query,
            index: 0,
        })
        .unwrap();
    account.apply_event(&event).unwrap();
    journal
        .append(RecoveryRecord::IngressEvent {
            input_stream: key.stream,
            input_sequence: key.sequence,
            event: Box::new(event),
        })
        .unwrap();
    let assets:Vec<_>=[("BTC","1"),("USDT","1000")].into_iter().map(|(asset,n)|serde_json::json!({"asset":asset,"walletBalance":n,"availableBalance":n,"marginBalance":n,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossWalletBalance":n,"crossUnPnl":"0"})).collect();
    let body=serde_json::to_vec(&serde_json::json!({"totalInitialMargin":"0","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]})).unwrap();
    let mut snapshots = [0; 3];
    for (i, kind) in [
        bootstrap::SnapshotKind::Account,
        bootstrap::SnapshotKind::OpenOrders,
        bootstrap::SnapshotKind::ConditionalOrders,
    ]
    .into_iter()
    .enumerate()
    {
        snapshots[i] = input
            .append(Record::Snapshot(bootstrap::SnapshotRecord {
                query: bootstrap::SnapshotQuery {
                    kind,
                    private_connection_epoch: 7,
                    gap_generation: 3,
                },
                epoch: 9 + i as u64,
                received: 71 + i as u64,
                clock_boot_id: domain.boot_id(),
                clock_generation: domain.generation(),
                ledger: account.ledger_sequence(),
                status: 200,
                complete: true,
                raw: http(if i == 0 { &body } else { b"[]" }),
            }))
            .unwrap()
            .sequence;
    }
    input.cursor = input.records.len();
    let request = ClosureRequest {
        parent_query,
        snapshots,
        cut: ActivationCut {
            private_connection_epoch: 7,
            gap_generation: 3,
            through_sequence: input.position(),
            now: MonoTime::new(domain, 75),
            valid_until: MonoTime::new(domain, 100),
            writer_epoch: 4,
            execution_connection_epoch: 1,
        },
    };
    (directory, input, account, journal, request)
}
struct Verifier<'a>(&'a Input);
impl RecoveryVerifier for Verifier<'_> {
    fn verify_admission(
        &mut self,
        _: &CanonicalBaseline,
        _: &CanonicalRecord,
        _: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        Ok(())
    }
    fn verify_uncreated_algo(
        &mut self,
        b: &CanonicalBaseline,
        r: &CanonicalRecord,
        h: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::UncreatedAlgoClosed(binding) = r.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            b.snapshot().clone(),
            b.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.0
            .verify_uncreated_algo(b, &account, h, binding, writer)
            .map_err(|_| VerificationError::Invalid)
    }
}
#[test]
fn exact_parent_query_and_three_snapshots_close_then_reopen_both_real_wals() {
    let (directory, input, mut account, mut journal, request) = fixture_data();
    let history = journal.history_frontier().clone();
    let before = account.asset(fixture::key(2)).unwrap();
    let shadow = account.venue_shadow(1).unwrap();
    let proof = input
        .prepare_uncreated_algo(&account, &history, request)
        .unwrap();
    let bytes = input
        .uncreated_algo_coverage_object(proof.binding())
        .unwrap();
    assert_eq!(hash(&bytes), proof.binding().coverage_digest);
    assert_eq!(
        account.close_uncreated_algo(proof.binding()).unwrap(),
        Applied::New
    );
    assert_eq!(account.asset(fixture::key(2)).unwrap().total, before.total);
    assert_eq!(account.venue_shadow(1).unwrap(), shadow);
    assert!(account.frozen());
    journal
        .append(RecoveryRecord::UncreatedAlgoClosed(Box::new(
            proof.binding.clone(),
        )))
        .unwrap();
    let old_ledger = account.ledger_sequence();
    let cfg = journal.config().clone();
    let mut input_config = input.config.clone();
    input_config.writer_epoch = 5;
    drop(journal);
    drop(input);
    let mut input = Input::recover(
        FileStore::open(directory.0.join("input")).unwrap(),
        input_config,
        None,
    )
    .unwrap()
    .with_closure_store(SharedBlobStore::new(
        FileBlobStore::open(directory.0.join("objects")).unwrap(),
    ));
    let closure = input.closure_store.take();
    assert!(
        AccountJournal::recover(
            FileBackend::open(directory.0.join("account")).unwrap(),
            cfg.clone(),
            5,
            None,
            &mut Verifier(&input)
        )
        .is_err(),
        "retained digest cannot manufacture missing closure evidence"
    );
    input.closure_store = closure;
    let recovered = AccountJournal::recover(
        FileBackend::open(directory.0.join("account")).unwrap(),
        cfg,
        5,
        None,
        &mut Verifier(&input),
    )
    .unwrap();
    assert_eq!(recovered.account.ledger_sequence(), old_ledger);
    assert!(recovered.account.frozen());
    assert_eq!(
        recovered.account.uncreated_algo_closure(&[78; 16]),
        Some(proof.binding())
    );
    assert_eq!(
        recovered
            .account
            .order(&recovered.account.structure(&[78; 16]).unwrap().legs[0])
            .unwrap()
            .remaining_quantity
            .atoms(),
        0
    );
}
#[test]
fn nochild_requires_exact_raw_query_cut_domain_and_complete_snapshots() {
    for failure in 0..7 {
        let (_dir, mut input, account, journal, mut request) = fixture_data();
        match failure {
            0 => request.cut.writer_epoch += 1,
            1 => request.cut.gap_generation += 1,
            2 => request.cut.now = MonoTime::new(request.cut.now.domain(), 100),
            3 => {
                let Record::Rest(r) = &mut input.records[request.parent_query as usize - 1] else {
                    unreachable!()
                };
                r.structure_domain = Some(ClockDomain::new([19; 16], 1).unwrap())
            }
            4 => {
                let Record::Snapshot(s) = &mut input.records[request.snapshots[0] as usize - 1]
                else {
                    unreachable!()
                };
                s.ledger += 1
            }
            5 => {
                let Record::Rest(r) = &mut input.records[request.parent_query as usize - 1] else {
                    unreachable!()
                };
                r.raw = http(br#"{"actualOrderId":"","actualQty":"1"}"#)
            }
            6 => input.closure_store = None,
            _ => unreachable!(),
        }
        assert!(
            input
                .prepare_uncreated_algo(&account, journal.history_frontier(), request)
                .is_err(),
            "case {failure}"
        );
        assert!(
            account
                .order(&account.structure(&[78; 16]).unwrap().legs[0])
                .unwrap()
                .remaining_quantity
                .atoms()
                > 0
        );
    }
}
fn fresh_snapshots(
    input: &mut Input,
    account: &Account,
    request: &mut ClosureRequest,
    received: u64,
) {
    let copies = request.snapshots.map(|seq| {
        let Record::Snapshot(s) = &input.records[seq as usize - 1] else {
            unreachable!()
        };
        s.clone()
    });
    for (i, mut s) in copies.into_iter().enumerate() {
        s.ledger = account.ledger_sequence();
        s.epoch += 20;
        s.received = received + i as u64;
        request.snapshots[i] = input.append(Record::Snapshot(s)).unwrap().sequence;
    }
    input.cursor = input.records.len();
    request.cut.through_sequence = input.position();
    request.cut.now = MonoTime::new(request.cut.now.domain(), received + 3);
    request.cut.valid_until = MonoTime::new(request.cut.now.domain(), received + 40);
}
fn second_parent(
    input: &mut Input,
    account: &mut Account,
    journal: &mut AccountJournal<FileBackend>,
    triggered: bool,
) -> u64 {
    let original = input
        .registry
        .values()
        .next()
        .unwrap()
        .command
        .command()
        .clone();
    let mut c = original;
    c.context.request.intent_id = 4;
    let v::CommandBody::Algo(ref mut a) = c.body else {
        unreachable!()
    };
    a.parent_order_id = [79; 16];
    a.order.client_id = v::ClientId::new("second-algo").unwrap();
    let c = CanonicalCommand::new(c).unwrap();
    let admission = account.admit(&c, 61).unwrap();
    journal
        .append(RecoveryRecord::Admit {
            command: Box::new(c.clone()),
            evaluated_at: 61,
            expected: admission.image(),
        })
        .unwrap();
    input.register_command(c.clone(), &[[9; 16]]).unwrap();
    let old = input
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
    let raw = String::from_utf8(old.raw.clone()).unwrap();
    let body = raw
        .split("\r\n\r\n")
        .nth(1)
        .unwrap()
        .replace("800", "801")
        .replace("test-1", "second-algo");
    let body = if triggered {
        body.replace("\"actualOrderId\":\"\"", "\"actualOrderId\":\"99\"")
    } else {
        body
    };
    let reg = &input.registry[&intent(&c)];
    let event = tickwire_binance::events::bind_structure_response(
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            account: 2,
            environment: v::Environment::Test,
            connection_epoch: 18,
        },
        Market::Usdm,
        &c,
        &reg.orders,
        body.as_bytes(),
        74,
    )
    .unwrap();
    let mut r = old;
    r.command = c.clone();
    r.epoch = 18;
    r.received = 74;
    r.raw = http(body.as_bytes());
    r.events = vec![event.clone()];
    r.structure_query.as_mut().unwrap().intent = intent(&c);
    r.structure_query.as_mut().unwrap().structure_id = [79; 16];
    input.validate_rest(&r).unwrap();
    let seq = input.append(Record::Rest(r)).unwrap().sequence;
    registry::learn(&mut input.registry, &event).unwrap();
    let key = input
        .append(Record::RestEffect {
            response: seq,
            index: 0,
        })
        .unwrap();
    account.apply_event(&event).unwrap();
    journal
        .append(RecoveryRecord::IngressEvent {
            input_stream: key.stream,
            input_sequence: key.sequence,
            event: Box::new(event),
        })
        .unwrap();
    seq
}
#[test]
fn two_uncreated_parents_need_individual_records_and_fresh_snapshots_between_them() {
    let (_dir, mut input, mut account, mut journal, mut request) = fixture_data();
    let second = second_parent(&mut input, &mut account, &mut journal, false);
    fresh_snapshots(&mut input, &account, &mut request, 75);
    let first = input
        .prepare_uncreated_algo(
            &account,
            journal.history_frontier(),
            ClosureRequest {
                parent_query: request.parent_query,
                snapshots: request.snapshots,
                cut: request.cut,
            },
        )
        .unwrap();
    account.close_uncreated_algo(first.binding()).unwrap();
    journal
        .append(RecoveryRecord::UncreatedAlgoClosed(Box::new(
            first.binding.clone(),
        )))
        .unwrap();
    assert!(
        account
            .order(&account.structure(&[78; 16]).unwrap().legs[0])
            .unwrap()
            .uncreated
    );
    assert_eq!(
        account
            .order(&account.structure(&[79; 16]).unwrap().legs[0])
            .unwrap()
            .remaining_quantity,
        fixture::units(1)
    );
    request.parent_query = second;
    assert!(
        input
            .prepare_uncreated_algo(
                &account,
                journal.history_frontier(),
                ClosureRequest {
                    parent_query: second,
                    snapshots: request.snapshots,
                    cut: request.cut
                }
            )
            .is_err()
    );
    fresh_snapshots(&mut input, &account, &mut request, 81);
    let second = input
        .prepare_uncreated_algo(&account, journal.history_frontier(), request)
        .unwrap();
    account.close_uncreated_algo(second.binding()).unwrap();
    assert!(
        account
            .order(&account.structure(&[79; 16]).unwrap().legs[0])
            .unwrap()
            .uncreated
    );
}
#[test]
fn another_parent_with_missing_query_or_actual_child_blocks_empty_order_projection() {
    for triggered in [false, true] {
        let (_dir, mut input, mut account, mut journal, mut request) = fixture_data();
        let second = second_parent(&mut input, &mut account, &mut journal, triggered);
        if !triggered {
            let Record::Rest(r) = &mut input.records[second as usize - 1] else {
                unreachable!()
            };
            r.raw = http(b"{}");
        }
        fresh_snapshots(&mut input, &account, &mut request, 75);
        assert!(
            input
                .prepare_uncreated_algo(&account, journal.history_frontier(), request)
                .is_err()
        );
        for id in [[78; 16], [79; 16]] {
            assert_eq!(
                account
                    .order(&account.structure(&id).unwrap().legs[0])
                    .unwrap()
                    .remaining_quantity,
                fixture::units(1)
            );
        }
    }
}
#[test]
fn known_live_algo_snapshot_retains_potential_child_and_unknown_parent_is_rejected() {
    let (_dir, mut input, _, _, request) = fixture_data();
    let reg = input.registry.values().next().unwrap().clone();
    let mut account = Account::from_snapshot(
        input.config.baseline.snapshot().clone(),
        input.config.baseline.catalog().clone(),
    )
    .unwrap();
    account.admit(&reg.command, 60).unwrap();
    let Record::Rest(r) = &input.records[request.parent_query as usize - 1] else {
        unreachable!()
    };
    let raw = String::from_utf8(r.raw.clone()).unwrap();
    let body = raw
        .split("\r\n\r\n")
        .nth(1)
        .unwrap()
        .replace("CANCELED", "NEW");
    let event = tickwire_binance::events::bind_structure_response(
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            account: 2,
            environment: v::Environment::Test,
            connection_epoch: 8,
        },
        Market::Usdm,
        &reg.command,
        &reg.orders,
        body.as_bytes(),
        70,
    )
    .unwrap();
    account.apply_event(&event).unwrap();
    registry::learn(&mut input.registry, &event).unwrap();
    let evidence = activation::evidence_at(&input, input.position()).unwrap();
    let conditional = format!("[{body}]");
    assert!(
        snapshot::verify_snapshot_with_orders(
            Market::Usdm,
            &input.config.baseline,
            &account,
            evidence[0].body(),
            b"[]",
            conditional.as_bytes(),
            &input.registry
        )
        .is_ok()
    );
    assert_eq!(
        account
            .order(&account.structure(&[78; 16]).unwrap().legs[0])
            .unwrap()
            .remaining_quantity,
        fixture::units(1)
    );
    assert!(
        snapshot::verify_snapshot_with_orders(
            Market::Usdm,
            &input.config.baseline,
            &account,
            evidence[0].body(),
            b"[]",
            b"[]",
            &input.registry
        )
        .is_err()
    );
    let wrong = conditional.replace("800", "801");
    assert!(
        snapshot::verify_snapshot_with_orders(
            Market::Usdm,
            &input.config.baseline,
            &account,
            evidence[0].body(),
            b"[]",
            wrong.as_bytes(),
            &input.registry
        )
        .is_err()
    );
}
#[test]
fn spot_list_snapshot_matches_every_original_leg_including_pending_new() {
    let (snapshot, catalog, mut c) = fixture::fixture();
    let v::CommandBody::New(first) = c.body.clone() else {
        unreachable!()
    };
    let mut second = first.clone();
    second.client_id = v::ClientId::new("pending-leg").unwrap();
    c.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [77; 16],
        client_id: v::ClientId::new("parent-list").unwrap(),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![first, second]).unwrap(),
    });
    let c = CanonicalCommand::new(c).unwrap();
    let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
    let mut account = Account::from_snapshot(snapshot, catalog).unwrap();
    account.admit(&c, 60).unwrap();
    let mut input = AccountIngress::create(
        super::super::income_import::tests::Memory::default(),
        Config {
            physical: tickwire_journal::Config::new([81; 16], [1; 16]),
            writer_epoch: 4,
            baseline: baseline.clone(),
            account_stream: [2; 16],
            max_records: 64,
            max_registrations: 8,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_baseline_authority: None,
            income_association_authority: None,
        },
    )
    .unwrap();
    input
        .register_command(c.clone(), &[[7; 16], [8; 16]])
        .unwrap();
    let body=br#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"EXEC_STARTED","listOrderStatus":"EXECUTING","listClientOrderId":"parent-list","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending-leg"}]}"#;
    let event = tickwire_binance::events::bind_structure_response(
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            account: 2,
            environment: v::Environment::Test,
            connection_epoch: 8,
        },
        Market::Spot,
        &c,
        &input.registry[&intent(&c)].orders,
        body,
        70,
    )
    .unwrap();
    account.apply_event(&event).unwrap();
    registry::learn(&mut input.registry, &event).unwrap();
    let ordinary=br#"[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1","status":"NEW","side":"BUY","type":"LIMIT","origQty":"0.001","executedQty":"0","price":"60000","timeInForce":"GTC","orderListId":55},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending-leg","status":"PENDING_NEW","side":"BUY","type":"LIMIT","origQty":"0.001","executedQty":"0","price":"60000","timeInForce":"GTC","orderListId":55}]"#;
    let balances=br#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1","locked":"0"},{"asset":"USDT","free":"880","locked":"120"}]}"#;
    let parents = format!("[{}]", std::str::from_utf8(body).unwrap());
    let before = account.asset(fixture::key(2)).unwrap();
    snapshot::verify_snapshot_with_orders(
        Market::Spot,
        &baseline,
        &account,
        balances,
        ordinary,
        parents.as_bytes(),
        &input.registry,
    )
    .unwrap();
    assert_eq!(account.asset(fixture::key(2)).unwrap(), before);
    assert!(
        snapshot::verify_snapshot_with_orders(
            Market::Spot,
            &baseline,
            &account,
            balances,
            b"[]",
            parents.as_bytes(),
            &input.registry
        )
        .is_err()
    );
    let wrong = std::str::from_utf8(ordinary)
        .unwrap()
        .replace("\"orderListId\":55", "\"orderListId\":56");
    assert!(
        snapshot::verify_snapshot_with_orders(
            Market::Spot,
            &baseline,
            &account,
            balances,
            wrong.as_bytes(),
            parents.as_bytes(),
            &input.registry
        )
        .is_err()
    );
}
