#![forbid(unsafe_code)]

use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tickwire_account::{
    AssetSnapshot, Catalog, FeePolicy, InstrumentPolicy, OrderKey, ProjectedFill,
    ProjectedPriceInput, RecoveryRecord, Revisions, Scope, Snapshot,
};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_ipc::{Record, ResourceHeader, Wire, crc32c, decode_record};
use tickwire_recovery::{FileBlobReader, FileBlobStore};
use tickwire_risk::Fixed;
use tickwire_strategy_sdk::{
    CommandStore, DecodedReport, Error, FillWatermarkTracker, MarketView, ModifyRequest,
    OrderClassificationHints, Strategy, mint_self_describing_client_id,
    outbound::{encode_intent, resolve_record_from_store},
    recovery::order_count,
    reports::{AccountReport, ReportIdentity, in_process_report_pair},
};
#[cfg(feature = "runtime")]
use tickwire_strategy_sdk::{OutboundWriter, runtime::in_process_outbound_pair};
use tickwire_venue::{
    AssetRef, AssetSymbol, BoundedList, BoundedMessage, BoundedText, CanonicalCommand,
    CanonicalEvent, ClientId, ClientIdDomain, Command, CommandBody, CommandContext, ContractKind,
    Decimal, DecodedClientId, Environment, EventIdentity, FillEvent, InstrumentBinding, IntentKey,
    Liquidity, ModifyObservation, ModifyRejection, OrderIdentity, OrderObservation, OrderSpec,
    OrderStatus, OrderTarget, OrderType, PositionSide, Product, ProductMode, Quantity,
    REPLACEMENT_ORDER_LEG, RequestIdentity, SelfDescribingClientId, SelfTradePrevention, Side,
    StrategyContext, Symbol, TerminalReason, TimeInForce, TriggerRef, VenueEvent, VenueEventBody,
    venue_id,
};

fn dec(coeff: i128, exp: i8) -> Decimal {
    Decimal::new(coeff, exp).unwrap()
}

fn fixed(atoms: i128, scale: u8) -> Fixed {
    Fixed::from_atoms(atoms, scale).unwrap()
}

fn temp_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("tw-sdk-{prefix}-{}-{stamp}", std::process::id()))
}

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        panic!("unsupported test architecture")
    }
}

fn asset(id: u64, symbol: &str, exponent: i8) -> AssetRef {
    AssetRef {
        id,
        version: 1,
        symbol: AssetSymbol::new(symbol).unwrap(),
        exponent,
    }
}

fn sample_context(intent_id: u64, intent_seq: u64) -> CommandContext {
    CommandContext {
        request: RequestIdentity {
            risk_domain_id: [1; 16],
            venue: venue_id::BINANCE,
            environment: Environment::Test,
            account: 2,
            product: Product::Spot,
            intent_id,
            intent_exec_epoch: 4,
            strategy_generation: 5,
            config_revision: 6,
            deadline: 1_000,
            recovery_object: 7,
        },
        strategy: StrategyContext {
            logical_strategy_id: 8,
            instance_id: [2; 16],
            strategy_intent_seq: intent_seq,
            authorization_epoch: 9,
            risk_policy_version: 10,
            order_config_version: 11,
            created_mono_ns: 50,
            economic_intent_id: [3; 16],
            hedge_group_id: [0; 16],
            leg_id: 0,
            recovery_manifest_hash: [4; 32],
        },
        instrument: InstrumentBinding {
            instrument_id: 12,
            instrument_version: 13,
            catalog_version: 14,
            catalog_hash: [5; 32],
            symbol: Symbol::new("BTCUSDT").unwrap(),
            base: asset(1, "BTC", -8),
            quote: asset(2, "USDT", -8),
            settlement: asset(2, "USDT", -8),
            price_tick: dec(1, -2),
            quantity_step: dec(1, -5),
            contract: ContractKind::Cash,
        },
        triggers: BoundedList::new(vec![TriggerRef {
            topic_id: 1,
            feed_generation: 1,
            ring_epoch: 1,
            sequence: 1,
            instrument_id: 12,
            instrument_version: 13,
            observed_mono_ns: 49,
        }])
        .unwrap(),
    }
}

fn intent_bank_header(producer_instance: [u8; 16], consumer_instance: [u8; 16]) -> ResourceHeader {
    let capacity = 1024u32;
    let record_bytes = 1024u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 2,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [11; 16],
        channel_id: [12; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation: 5,
        consumer_instance,
        host_boot_id: [13; 16],
        namespace_id: [14; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        cutover_seq: 0,
        predecessor_ring_epoch: 0,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | 128,
        schema_major: 1,
        schema_minor: 0,
        clock_domain: 1,
        qos: 1,
        event_class: 8,
        ..Default::default()
    }
}

fn report_bank_header(producer_instance: [u8; 16], consumer_instance: [u8; 16]) -> ResourceHeader {
    let capacity = 1024u32;
    let record_bytes = 512u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 3,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [21; 16],
        channel_id: [22; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation: 7,
        consumer_instance,
        host_boot_id: [13; 16],
        namespace_id: [14; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        cutover_seq: 0,
        predecessor_ring_epoch: 0,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::reports::FEATURE,
        schema_major: 1,
        schema_minor: 0,
        clock_domain: 1,
        qos: 1,
        event_class: 9,
        ..Default::default()
    }
}

fn sample_baseline() -> CanonicalBaseline {
    let inst = sample_context(1, 1).instrument;
    let snapshot = Snapshot {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: venue_id::BINANCE,
            environment: Environment::Test,
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
        valid_until: 10_000,
        assets: vec![
            AssetSnapshot {
                key: tickwire_account::AssetKey {
                    wallet_id: 1,
                    asset_id: 1,
                },
                asset: inst.base.clone(),
                free: fixed(100_000_000, 8),
                locked: fixed(0, 8),
                nontrade_locked: fixed(0, 8),
                exclusive_fee_pool: false,
                ownership: vec![],
            },
            AssetSnapshot {
                key: tickwire_account::AssetKey {
                    wallet_id: 1,
                    asset_id: 2,
                },
                asset: inst.quote.clone(),
                free: fixed(100_000_000_000, 8),
                locked: fixed(0, 8),
                nontrade_locked: fixed(0, 8),
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
            product: Product::Spot,
            wallet_id: 1,
            fees: FeePolicy {
                rates: [fixed(0, 8); 3],
                maximum_fills: 10,
                exclusive_pool: None,
            },
            futures: None,
            max_absolute_position: fixed(10_000_000_000, 8),
            max_gross_quantity: fixed(10_000_000_000, 8),
            strategy_limits: vec![(8, fixed(10_000_000_000, 8))],
        }],
    };
    CanonicalBaseline::new(snapshot, catalog).unwrap()
}

fn event_identity(venue: u32, event_seq: u64) -> EventIdentity {
    EventIdentity {
        risk_domain_id: [1; 16],
        venue,
        environment: Environment::Test,
        account: 2,
        product: Product::Spot,
        instrument_id: 12,
        instrument_version: 13,
        source_connection_epoch: 1,
        source_sequence: Some(event_seq),
        event_id: BoundedText::new(format!("ev-{event_seq}")).unwrap(),
        venue_time_ns: Some(100 + event_seq),
        received_mono_ns: 200 + event_seq,
    }
}

fn sample_target() -> OrderTarget {
    OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(ClientId::new("orig-client-1").unwrap()),
        venue_order_id: Some(BoundedText::new("987654321").unwrap()),
    }
}

fn sample_order_identity() -> OrderIdentity {
    OrderIdentity {
        order_id: [9; 16],
        original_intent_id: 3,
        original_intent_exec_epoch: 4,
        client_id: Some(ClientId::new("orig-client-1").unwrap()),
        venue_order_id: Some(BoundedText::new("987654321").unwrap()),
        parent_order_id: None,
        list_id: None,
        venue_algo_id: None,
        venue_list_id: None,
        list_client_id: None,
        algo_client_id: None,
    }
}

#[test]
fn outbound_modify_roundtrip_encodes_kind_5_message_517_and_zero_order_count() {
    let ctx = sample_context(42, 1);
    let target = sample_target();

    // Reject non-positive quantity and price up front before touching storage or SPSC.
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(0, -5), dec(60_000, 0))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Quantity))
    ));
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(-5, -5), dec(60_000, 0))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Quantity))
    ));
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(25, -3), dec(0, -2))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Price))
    ));
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(25, -3), dec(-60_000, 0))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Price))
    ));

    // Mint a self-describing replacement client ID and verify recovery order_count == 0.
    let request = ModifyRequest::new(target.clone(), Side::Buy, dec(25, -3), dec(6_012_525, -2))
        .with_minted_client_id(&ctx, REPLACEMENT_ORDER_LEG)
        .unwrap();
    let preview_cmd = request.clone().build_command(ctx.clone()).unwrap();
    assert_eq!(order_count(&preview_cmd), 0);

    let domain = ClientIdDomain::from_command_context(&ctx);
    let CommandBody::Modify(ref modify_spec) = preview_cmd.command().body else {
        panic!("expected CommandBody::Modify");
    };
    let minted = modify_spec.new_client_id.as_ref().unwrap();
    assert_eq!(
        SelfDescribingClientId::decode(minted, &domain),
        DecodedClientId::Managed(SelfDescribingClientId {
            logical_strategy_id: 8,
            leg: REPLACEMENT_ORDER_LEG,
            epoch: 4,
            intent_id: 42,
        })
    );

    // Verify wire encoding directly (`command_kind == 5`, `message_type == 517`) and via blob store.
    let mut mem_store = CommandStore::new(tickwire_recovery::RuntimeBlobStore::in_memory());
    let receipt = mem_store.put(&preview_cmd).unwrap();
    let mut wire_rec = encode_intent(&preview_cmd, &receipt, 1, 75, 900).unwrap();
    assert_eq!(wire_rec.body.command_kind, 5);
    assert_eq!(wire_rec.header.message_type, 517);

    let mut raw_image = [0u8; 1024];
    wire_rec.encode(&mut raw_image).unwrap();
    wire_rec.header.record_crc32c = crc32c(&raw_image);
    wire_rec.encode(&mut raw_image).unwrap();
    let Record::Intent(decoded_wire) = decode_record(&raw_image).unwrap() else {
        panic!("expected IntentRecord");
    };
    assert_eq!(decoded_wire.body.command_kind, 5);
    assert_eq!(decoded_wire.header.message_type, 517);

    // End-to-end SPSC shared-memory OutboundWriter -> OutboundReader roundtrip.
    let header = intent_bank_header([2; 16], [77; 16]);
    let resolved_from_mem_store =
        resolve_record_from_store(&mem_store_inner(&preview_cmd), &raw_image, &header, [1; 16])
            .unwrap();
    assert_eq!(resolved_from_mem_store.digest(), preview_cmd.digest());

    #[cfg(feature = "runtime")]
    {
        let dir = temp_dir("outbound-modify");
        let file_store = FileBlobStore::open(&dir).unwrap();
        let uid = std::fs::metadata(&dir).unwrap().uid();
        let file_reader = FileBlobReader::open(&dir, uid).unwrap();
        let mut cmd_store = CommandStore::new(file_store);

        let (mut writer, mut reader): (OutboundWriter, _) =
            in_process_outbound_pair(header, [77; 16], 1, [88; 16], file_reader, [1; 16], 8)
                .unwrap();

        let (published_cmd, ring_seq) = writer
            .publish_modify(ctx, request, &mut cmd_store, 75)
            .unwrap();
        assert_eq!(ring_seq, 1);
        assert_eq!(published_cmd.bytes(), preview_cmd.bytes());
        assert_eq!(order_count(&published_cmd), 0);

        let bank_header = reader.header().clone();
        let resolved_mem = resolve_record_from_store(
            &tickwire_recovery::RuntimeBlobStore::in_memory(),
            &raw_image,
            &bank_header,
            [1; 16],
        );
        assert!(resolved_mem.is_err());

        let received = reader
            .next_command()
            .unwrap()
            .expect("modify command must arrive");
        assert_eq!(received.digest(), published_cmd.digest());
        assert_eq!(received.bytes(), published_cmd.bytes());
        assert_eq!(order_count(&received), 0);
        assert!(reader.next_command().unwrap().is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

fn mem_store_inner(cmd: &CanonicalCommand) -> tickwire_recovery::RuntimeBlobStore {
    use tickwire_recovery::{BlobStore, StoreKey};
    let mut store = tickwire_recovery::RuntimeBlobStore::in_memory();
    store
        .put_sync(StoreKey::Object(cmd.digest()), cmd.bytes())
        .unwrap();
    store
}

#[test]
fn report_decoding_surfaces_modified_modify_rejected_aborted_and_terminal_reasons() {
    let baseline = sample_baseline();
    let dir = temp_dir("report-modify");
    let store = FileBlobStore::open(&dir).unwrap();
    let uid = std::fs::metadata(&dir).unwrap().uid();
    let reader = FileBlobReader::open(&dir, uid).unwrap();

    let identity = ReportIdentity {
        logical_strategy_id: 8,
        instance_id: [2; 16],
        strategy_generation: 5,
        risk_domain: [1; 16],
    };
    let (mut report_writer, mut report_reader) = in_process_report_pair(
        report_bank_header([99; 16], [2; 16]),
        1,
        [55; 16],
        store,
        reader,
        identity,
    )
    .unwrap();

    // 0. Publish baseline (780) over real SHM report bank.
    report_writer.publish_baseline(&baseline, 100).unwrap();
    let base_report = report_reader.next().unwrap().unwrap();
    assert!(matches!(base_report.decode(), DecodedReport::Baseline(_)));
    assert_eq!(base_report.terminal_reason(), None);

    // 1. VenueEventBody::Modified (tag 8) over real SHM report bank (781).
    let mod_obs = ModifyObservation {
        target: sample_target(),
        amendment_id: Some(BoundedText::new("am-1").unwrap()),
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 10,
        }),
        prior_client_id: Some(ClientId::new("orig-client-1").unwrap()),
        new_client_id: Some(ClientId::new("new-client-2").unwrap()),
        venue_order_id: Some(BoundedText::new("987654321").unwrap()),
        new_quantity: dec(5, -2),
        new_price: dec(60_100, 0),
        cumulative_filled: Some(dec(2, -2)),
    };
    let ev_mod = CanonicalEvent::new(VenueEvent {
        identity: event_identity(venue_id::BINANCE, 1),
        body: VenueEventBody::Modified(Box::new(mod_obs.clone())),
    })
    .unwrap();
    let rec_1 = CanonicalRecord::new(
        baseline.digest(),
        1,
        RecoveryRecord::VenueEvent(Box::new(ev_mod)),
    )
    .unwrap();
    report_writer.publish_record(1, &rec_1, 110).unwrap();
    let rep_1 = report_reader.next().unwrap().unwrap();
    assert_eq!(rep_1.modified(), Some(&mod_obs));
    assert_eq!(rep_1.terminal_reason(), None);
    assert!(!rep_1.decode().is_order_lifecycle_terminal());

    // Modify down to already-filled quantity -> immediately terminal Filled.
    let mut mod_filled = mod_obs.clone();
    mod_filled.new_quantity = dec(2, -2);
    let ev_mod_filled = CanonicalEvent::new(VenueEvent {
        identity: event_identity(venue_id::BINANCE, 2),
        body: VenueEventBody::Modified(Box::new(mod_filled)),
    })
    .unwrap();
    let rep_mod_filled = AccountReport::Record {
        position: 2,
        record: CanonicalRecord::new(
            baseline.digest(),
            2,
            RecoveryRecord::VenueEvent(Box::new(ev_mod_filled)),
        )
        .unwrap(),
    };
    assert_eq!(
        rep_mod_filled.terminal_reason(),
        Some(TerminalReason::Filled)
    );
    assert!(rep_mod_filled.decode().is_order_lifecycle_terminal());

    // 2. VenueEventBody::ModifyRejected (tag 9) over real SHM report bank (781).
    for (seq, code, reason) in [
        (2u64, Some(-2013), TerminalReason::OrderNotOpen),
        (3u64, Some(-5027), TerminalReason::NoChange),
        (4u64, Some(-5022), TerminalReason::PostOnlyWouldTake),
        (5u64, None, TerminalReason::SelfTradePreventedByVenue),
        (6u64, None, TerminalReason::TooManyPendingActions),
        (7u64, None, TerminalReason::CrossesBook),
    ] {
        let rej = ModifyRejection {
            target: sample_target(),
            amendment_id: None,
            intent: Some(IntentKey {
                epoch: 4,
                intent_id: 20 + seq,
            }),
            new_client_id: None,
            venue_order_id: Some(BoundedText::new("987654321").unwrap()),
            code,
            reason,
        };
        let ev_rej = CanonicalEvent::new(VenueEvent {
            identity: event_identity(venue_id::BINANCE, 10 + seq),
            body: VenueEventBody::ModifyRejected(Box::new(rej.clone())),
        })
        .unwrap();
        let rec = CanonicalRecord::new(
            baseline.digest(),
            seq,
            RecoveryRecord::VenueEvent(Box::new(ev_rej)),
        )
        .unwrap();
        report_writer.publish_record(seq, &rec, 120 + seq).unwrap();
        let rep = report_reader.next().unwrap().unwrap();
        assert_eq!(rep.modify_rejected(), Some(&rej));
        assert_eq!(rep.terminal_reason(), Some(reason));
        assert!(!rep.decode().is_order_lifecycle_terminal());
    }

    // 3. RecoveryRecord::Aborted -> TerminalReason::AbortedBeforeSend.
    let rec_aborted = CanonicalRecord::new(
        baseline.digest(),
        8,
        RecoveryRecord::Aborted {
            intent: IntentKey {
                epoch: 4,
                intent_id: 99,
            },
            command_digest: [7; 32],
        },
    )
    .unwrap();
    report_writer.publish_record(8, &rec_aborted, 140).unwrap();
    let rep_aborted = report_reader.next().unwrap().unwrap();
    assert_eq!(
        rep_aborted.terminal_reason(),
        Some(TerminalReason::AbortedBeforeSend)
    );
    assert!(matches!(
        rep_aborted.decode(),
        DecodedReport::Aborted {
            reason: TerminalReason::AbortedBeforeSend,
            ..
        }
    ));

    // 4. Venue Rejected & Order terminal statuses across Binance, Bybit, and OKX.
    let make_rejected_report = |venue: u32, code: &str, msg: &str| {
        let ev = CanonicalEvent::new(VenueEvent {
            identity: event_identity(venue, 50),
            body: VenueEventBody::Rejected {
                client_id: Some(ClientId::new("orig-client-1").unwrap()),
                venue_code: BoundedText::new(code).unwrap(),
                message: BoundedMessage::new(msg).unwrap(),
            },
        })
        .unwrap();
        AccountReport::Record {
            position: 9,
            record: CanonicalRecord::new(
                baseline.digest(),
                9,
                RecoveryRecord::VenueEvent(Box::new(ev)),
            )
            .unwrap(),
        }
    };

    assert_eq!(
        make_rejected_report(
            venue_id::BINANCE,
            "-5022",
            "Due to the order could not be executed as maker, the Post Only order will be rejected."
        )
        .terminal_reason(),
        Some(TerminalReason::PostOnlyWouldTake)
    );
    assert_eq!(
        make_rejected_report(
            venue_id::BYBIT,
            "EC_PostOnlyWillTakeLiquidity",
            "Post only order would take liquidity"
        )
        .terminal_reason(),
        Some(TerminalReason::PostOnlyWouldTake)
    );
    assert_eq!(
        make_rejected_report(venue_id::BINANCE, "EXPIRED_IN_MATCH", "STP prevented match")
            .terminal_reason(),
        Some(TerminalReason::SelfTradePreventedByVenue)
    );
    assert_eq!(
        make_rejected_report(venue_id::BINANCE, "-2013", "Order does not exist.").terminal_reason(),
        Some(TerminalReason::OrderNotOpen)
    );
    assert_eq!(
        make_rejected_report(venue_id::OKX, "51509", "Order not modified").terminal_reason(),
        Some(TerminalReason::NoChange)
    );
    assert_eq!(
        make_rejected_report(venue_id::BINANCE, "CROSSES_BOOK", "local stp guard")
            .terminal_reason(),
        Some(TerminalReason::CrossesBook)
    );

    // Order expired with PostOnly hint vs IOC hint.
    let expired_obs = OrderObservation {
        order: sample_order_identity(),
        status: OrderStatus::Expired,
        side: Side::Buy,
        position_side: PositionSide::Both,
        original_quantity: Some(dec(5, 0)),
        cumulative_quantity: Some(dec(0, 0)),
        cumulative_quote: Some(dec(0, 0)),
        leaves_quantity: Some(dec(0, 0)),
        order_price: Some(dec(60_000, 0)),
    };
    let expired_rep = AccountReport::Record {
        position: 10,
        record: CanonicalRecord::new(
            baseline.digest(),
            10,
            RecoveryRecord::VenueEvent(Box::new(
                CanonicalEvent::new(VenueEvent {
                    identity: event_identity(venue_id::BINANCE, 60),
                    body: VenueEventBody::Order(Box::new(expired_obs)),
                })
                .unwrap(),
            )),
        )
        .unwrap(),
    };
    assert_eq!(
        expired_rep.terminal_reason(),
        Some(TerminalReason::ExpiredIocOrFok)
    );
    assert_eq!(
        expired_rep.terminal_reason_with_hints(
            OrderClassificationHints::new().with_time_in_force(TimeInForce::PostOnly)
        ),
        Some(TerminalReason::PostOnlyWouldTake)
    );
    assert_eq!(
        expired_rep.terminal_reason_with_hints(
            OrderClassificationHints::new().with_stp_mode_or_reason("EXPIRE_TAKER")
        ),
        Some(TerminalReason::SelfTradePreventedByVenue)
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

fn make_order_key(intent_id: u64) -> OrderKey {
    OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id,
        },
        client_id: ClientId::new(format!("order-{intent_id}")).unwrap(),
    }
}

fn make_status_obs(status: OrderStatus, cum_qty: i128, cum_quote: i128) -> OrderObservation {
    OrderObservation {
        order: sample_order_identity(),
        status,
        side: Side::Buy,
        position_side: PositionSide::Both,
        original_quantity: Some(dec(10, 0)),
        cumulative_quantity: Some(dec(cum_qty, 0)),
        cumulative_quote: Some(dec(cum_quote, 0)),
        leaves_quantity: Some(dec(10 - cum_qty, 0)),
        order_price: Some(dec(100, 0)),
    }
}

fn make_fill_event(trade_id: &str, last_qty: i128, cum_qty: i128, px: i128) -> FillEvent {
    FillEvent {
        observation: make_status_obs(
            if cum_qty >= 10 {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            },
            cum_qty,
            cum_qty * px,
        ),
        trade_id: BoundedText::new(trade_id).unwrap(),
        last_quantity: dec(last_qty, 0),
        last_price: dec(px, 0),
        last_quote: Some(dec(last_qty * px, 0)),
        realized_pnl: None,
        liquidity: Liquidity::Maker,
        fees: BoundedList::new(vec![]).unwrap(),
    }
}

#[test]
fn fill_watermark_tracker_prevents_double_counting_across_all_interleavings() {
    let base_scale = 0u8;
    let price_scale = 2u8;

    // 1. Status (cum_qty = 5) BEFORE Trade (3 + 2):
    //    Skew advances +5 on status, +0 on trade 1, +0 on trade 2;
    //    confirmed_filled == 5 == projected_filled.
    let mut tracker = FillWatermarkTracker::new();
    let order_a = make_order_key(1);

    let proj_a = tracker
        .on_order_observation(
            &order_a,
            &make_status_obs(OrderStatus::PartiallyFilled, 5, 500),
            base_scale,
            price_scale,
        )
        .unwrap()
        .expect("status cum_qty=5 must emit +5 projection");
    assert!(proj_a.is_projected);
    assert_eq!(proj_a.delta_quantity, fixed(5, base_scale));
    assert_eq!(proj_a.estimated_price, Some(fixed(10_000, price_scale)));
    assert_eq!(
        tracker.confirmed_filled(&order_a),
        Some(fixed(0, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_a),
        Some(fixed(5, base_scale))
    );

    let t1_a = tracker
        .on_fill(
            &order_a,
            &make_fill_event("t-a1", 3, 3, 100),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert!(!t1_a.duplicate);
    assert_eq!(t1_a.already_projected, fixed(3, base_scale));
    assert_eq!(t1_a.unprojected_delta, fixed(0, base_scale));
    assert_eq!(t1_a.effective_delta_quantity, fixed(0, base_scale));
    assert_eq!(t1_a.into_effective_delta(), None);
    assert_eq!(
        tracker.confirmed_filled(&order_a),
        Some(fixed(3, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_a),
        Some(fixed(5, base_scale))
    );

    let t2_a = tracker
        .on_fill(
            &order_a,
            &make_fill_event("t-a2", 2, 5, 100),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert_eq!(t2_a.already_projected, fixed(2, base_scale));
    assert_eq!(t2_a.unprojected_delta, fixed(0, base_scale));
    assert_eq!(t2_a.effective_delta_quantity, fixed(0, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_a),
        Some(fixed(5, base_scale))
    );
    assert_eq!(
        tracker.confirmed_filled(&order_a),
        tracker.projected_filled(&order_a)
    );

    // 2. Trade (3) BEFORE Status (cum_qty = 5) THEN Trade (2):
    //    Skew advances +3 on trade 1, +2 on status, +0 on trade 2.
    let order_b = make_order_key(2);
    let t1_b = tracker
        .on_fill(
            &order_b,
            &make_fill_event("t-b1", 3, 3, 101),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert_eq!(t1_b.already_projected, fixed(0, base_scale));
    assert_eq!(t1_b.unprojected_delta, fixed(3, base_scale));
    assert_eq!(t1_b.effective_delta_quantity, fixed(3, base_scale));
    assert_eq!(
        t1_b.into_effective_delta().unwrap().delta_quantity,
        fixed(3, base_scale)
    );
    assert_eq!(
        tracker.confirmed_filled(&order_b),
        Some(fixed(3, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_b),
        Some(fixed(3, base_scale))
    );

    let proj_b = tracker
        .on_order_observation(
            &order_b,
            &make_status_obs(OrderStatus::PartiallyFilled, 5, 505),
            base_scale,
            price_scale,
        )
        .unwrap()
        .expect("status cum_qty=5 after fill=3 must project remaining +2");
    assert!(proj_b.is_projected);
    assert_eq!(proj_b.delta_quantity, fixed(2, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_b),
        Some(fixed(3, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_b),
        Some(fixed(5, base_scale))
    );

    // 3. Stale out-of-order Status (cum_qty = 2 after 5) -> None (+0 skew).
    let stale_b = tracker
        .on_order_observation(
            &order_b,
            &make_status_obs(OrderStatus::PartiallyFilled, 2, 202),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert_eq!(stale_b, None);
    assert_eq!(
        tracker.projected_filled(&order_b),
        Some(fixed(5, base_scale))
    );

    let t2_b = tracker
        .on_fill(
            &order_b,
            &make_fill_event("t-b2", 2, 5, 101),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert_eq!(t2_b.already_projected, fixed(2, base_scale));
    assert_eq!(t2_b.unprojected_delta, fixed(0, base_scale));
    assert_eq!(t2_b.effective_delta_quantity, fixed(0, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_b),
        Some(fixed(5, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_b),
        Some(fixed(5, base_scale))
    );

    // 4a. Single Trade (6) exceeding prior projection (4) -> skew advances +4 then +2.
    let order_c = make_order_key(3);
    let proj_c = tracker
        .on_cumulative_quantity(
            &order_c,
            fixed(4, base_scale),
            ProjectedPriceInput::AveragePrice {
                avg_price: fixed(10_200, price_scale),
            },
            Some(fixed(10_200, price_scale)),
            price_scale,
        )
        .unwrap()
        .unwrap();
    assert_eq!(proj_c.delta_quantity, fixed(4, base_scale));
    assert_eq!(
        tracker.projected_filled(&order_c),
        Some(fixed(4, base_scale))
    );

    let t1_c = tracker
        .on_fill(
            &order_c,
            &make_fill_event("t-c1", 6, 6, 102),
            base_scale,
            price_scale,
        )
        .unwrap();
    assert_eq!(t1_c.already_projected, fixed(4, base_scale));
    assert_eq!(t1_c.unprojected_delta, fixed(2, base_scale));
    assert_eq!(t1_c.effective_delta_quantity, fixed(2, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_c),
        Some(fixed(6, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_c),
        Some(fixed(6, base_scale))
    );

    // 4b. Multi-fill case from spec: projection 0 -> 5 (+5), Fill(3) (+0), Fill(4) -> 7 > 5 (+2).
    let order_d = make_order_key(4);
    let pf_d = ProjectedFill {
        order: order_d.clone(),
        instrument_id: 12,
        strategy: 8,
        side: Side::Buy,
        position_side: PositionSide::Both,
        gap_quantity: fixed(5, base_scale),
        gap_price: fixed(10_500, price_scale),
        order_filled: fixed(0, base_scale),
        order_projected: fixed(5, base_scale),
        projected: true,
    };
    let d0 = tracker.on_projected_fill(&pf_d).unwrap().unwrap();
    assert_eq!(d0.delta_quantity, fixed(5, base_scale));
    let d1 = tracker
        .on_fill_fixed(
            &order_d,
            Some("t-d1"),
            fixed(3, base_scale),
            fixed(10_500, price_scale),
        )
        .unwrap();
    assert_eq!(d1.already_projected, fixed(3, base_scale));
    assert_eq!(d1.unprojected_delta, fixed(0, base_scale));
    assert_eq!(d1.effective_delta_quantity, fixed(0, base_scale));

    // Duplicate trade_id replay is a strict no-op.
    let d1_dup = tracker
        .on_fill_fixed(
            &order_d,
            Some("t-d1"),
            fixed(3, base_scale),
            fixed(10_500, price_scale),
        )
        .unwrap();
    assert!(d1_dup.duplicate);
    assert_eq!(d1_dup.effective_delta_quantity, fixed(0, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_d),
        Some(fixed(3, base_scale))
    );

    let d2 = tracker
        .on_fill_fixed(
            &order_d,
            Some("t-d2"),
            fixed(4, base_scale),
            fixed(10_500, price_scale),
        )
        .unwrap();
    assert_eq!(d2.already_projected, fixed(2, base_scale));
    assert_eq!(d2.unprojected_delta, fixed(2, base_scale));
    assert_eq!(d2.effective_delta_quantity, fixed(2, base_scale));
    assert_eq!(
        tracker.confirmed_filled(&order_d),
        Some(fixed(7, base_scale))
    );
    assert_eq!(
        tracker.projected_filled(&order_d),
        Some(fixed(7, base_scale))
    );
}

#[test]
fn bounded_capacity_evicts_retired_slots_and_protects_active_slots() {
    let mut tracker = FillWatermarkTracker::with_capacity(2, 4).unwrap();
    let o1 = make_order_key(101);
    let o2 = make_order_key(102);
    let o3 = make_order_key(103);

    tracker
        .register_order(o1.clone(), 0, 2, Some(fixed(10_000, 2)))
        .unwrap();
    tracker
        .register_order(o2.clone(), 0, 2, Some(fixed(10_000, 2)))
        .unwrap();

    // Both slots are active -> 3rd registration rejected with Error::Bounds.
    assert!(matches!(
        tracker.register_order(o3.clone(), 0, 2, Some(fixed(10_000, 2))),
        Err(Error::Bounds)
    ));

    // Mark o1 retired -> o3 evicts o1 cleanly while preserving o2.
    assert!(tracker.mark_retired(&o1));
    tracker
        .register_order(o3.clone(), 0, 2, Some(fixed(10_000, 2)))
        .unwrap();
    assert_eq!(tracker.confirmed_filled(&o1), None);
    assert_eq!(tracker.confirmed_filled(&o2), Some(fixed(0, 0)));
    assert_eq!(tracker.confirmed_filled(&o3), Some(fixed(0, 0)));
}

#[test]
fn strategy_trait_default_and_custom_on_projected_fill_compile_and_execute() {
    #[derive(Default)]
    struct ExistingLegacyStrategy;
    impl Strategy for ExistingLegacyStrategy {
        fn on_market(
            &mut self,
            _view: MarketView<'_>,
            _now_ns: u64,
        ) -> tickwire_strategy_sdk::Result<Option<CanonicalCommand>> {
            Ok(None)
        }
    }

    struct SkewingMarketMaker {
        tracker: FillWatermarkTracker,
        inventory_skew: Fixed,
    }
    impl Strategy for SkewingMarketMaker {
        fn on_projected_fill(
            &mut self,
            fill: &ProjectedFill,
            _now_ns: u64,
        ) -> tickwire_strategy_sdk::Result<Option<CanonicalCommand>> {
            if let Some(delta) = self.tracker.on_projected_fill(fill)? {
                self.inventory_skew = self.inventory_skew.checked_add(delta.delta_quantity)?;
            }
            Ok(None)
        }

        fn on_market(
            &mut self,
            _view: MarketView<'_>,
            _now_ns: u64,
        ) -> tickwire_strategy_sdk::Result<Option<CanonicalCommand>> {
            Ok(None)
        }
    }

    let order = make_order_key(77);
    let pf = ProjectedFill {
        order: order.clone(),
        instrument_id: 12,
        strategy: 8,
        side: Side::Buy,
        position_side: PositionSide::Both,
        gap_quantity: fixed(5, 0),
        gap_price: fixed(6_000_000, 2),
        order_filled: fixed(0, 0),
        order_projected: fixed(5, 0),
        projected: true,
    };

    // Existing strategy compiles untouched and returns Ok(None).
    let mut legacy = ExistingLegacyStrategy;
    assert_eq!(legacy.on_projected_fill(&pf, 500).unwrap(), None);

    // Custom strategy updates skew immediately via FillWatermarkTracker without double-counting.
    let mut mm = SkewingMarketMaker {
        tracker: FillWatermarkTracker::new(),
        inventory_skew: fixed(0, 0),
    };
    assert_eq!(mm.on_projected_fill(&pf, 500).unwrap(), None);
    assert_eq!(mm.inventory_skew, fixed(5, 0));

    // Repeating the same cumulative projection adds 0 skew.
    assert_eq!(mm.on_projected_fill(&pf, 505).unwrap(), None);
    assert_eq!(mm.inventory_skew, fixed(5, 0));

    // Settled trade fill of 5 adds 0 additional skew.
    let settled = mm
        .tracker
        .on_fill_fixed(&order, Some("tr-77"), fixed(5, 0), fixed(6_000_000, 2))
        .unwrap();
    if let Some(extra) = settled.into_effective_delta() {
        mm.inventory_skew = mm.inventory_skew.checked_add(extra.delta_quantity).unwrap();
    }
    assert_eq!(mm.inventory_skew, fixed(5, 0));
    assert_eq!(mm.tracker.confirmed_filled(&order), Some(fixed(5, 0)));
    assert_eq!(mm.tracker.projected_filled(&order), Some(fixed(5, 0)));

    // Ensure unused helper imports stay exercised cleanly.
    let ctx = sample_context(78, 2);
    let (cid, _) = mint_self_describing_client_id(&ctx, 0).unwrap();
    let new_cmd = CanonicalCommand::new(Command {
        context: ctx,
        body: CommandBody::New(OrderSpec {
            client_id: cid,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Quantity::Base(dec(1, -3)),
            price: Some(dec(60_000, 0)),
            time_in_force: Some(TimeInForce::Gtc),
            position_side: PositionSide::Both,
            mode: ProductMode::Spot,
            reduce_only: false,
            self_trade_prevention: SelfTradePrevention::Default,
            trigger: None,
            trailing: None,
            iceberg_quantity: None,
        }),
    })
    .unwrap();
    assert_eq!(order_count(&new_cmd), 1);
}

#[test]
fn fill_watermark_tracker_gap_price_tracks_quote_and_avg_denominators_across_limit_fallback_and_settled_first_fills()
 {
    let mut tracker = FillWatermarkTracker::new();
    let order_a = make_order_key(901);

    // 1. Settled fill (3 @ 100.00) arrives BEFORE CumulativeQuote (cum=5, quote=500.00 -> 500/5 = 100.00),
    // followed by LimitFallback (cum=7, limit=120.00) and CumulativeQuote (cum=8, quote=830.00 -> (830-500)/(8-5) = 110.00).
    let s1 = tracker
        .on_fill_fixed(&order_a, Some("tr-a1"), fixed(3, 0), fixed(10_000, 2))
        .unwrap();
    assert_eq!(s1.effective_delta_quantity, fixed(3, 0));

    let p1 = tracker
        .on_cumulative_quantity(
            &order_a,
            fixed(5, 0),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: fixed(50_000, 2),
            },
            Some(fixed(12_000, 2)),
            2,
        )
        .unwrap()
        .unwrap();
    assert_eq!(p1.delta_quantity, fixed(2, 0));
    assert_eq!(p1.estimated_price, Some(fixed(10_000, 2)));

    let p2 = tracker
        .on_cumulative_quantity(
            &order_a,
            fixed(7, 0),
            ProjectedPriceInput::LimitFallback,
            Some(fixed(12_000, 2)),
            2,
        )
        .unwrap()
        .unwrap();
    assert_eq!(p2.delta_quantity, fixed(2, 0));
    assert_eq!(p2.estimated_price, Some(fixed(12_000, 2)));

    let p3 = tracker
        .on_cumulative_quantity(
            &order_a,
            fixed(8, 0),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: fixed(83_000, 2),
            },
            Some(fixed(12_000, 2)),
            2,
        )
        .unwrap()
        .unwrap();
    assert_eq!(p3.delta_quantity, fixed(1, 0));
    assert_eq!(p3.estimated_price, Some(fixed(11_000, 2)));

    // 2. LimitFallback first (cum=3), then CumulativeQuote (cum=4, quote=400.00 -> 400/4 = 100.00)
    // and AveragePrice transitions.
    let order_b = make_order_key(902);
    let _ = tracker
        .on_cumulative_quantity(
            &order_b,
            fixed(3, 0),
            ProjectedPriceInput::LimitFallback,
            Some(fixed(12_000, 2)),
            2,
        )
        .unwrap()
        .unwrap();
    let pb = tracker
        .on_cumulative_quantity(
            &order_b,
            fixed(4, 0),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: fixed(40_000, 2),
            },
            Some(fixed(12_000, 2)),
            2,
        )
        .unwrap()
        .unwrap();
    assert_eq!(pb.delta_quantity, fixed(1, 0));
    assert_eq!(pb.estimated_price, Some(fixed(10_000, 2)));
}
