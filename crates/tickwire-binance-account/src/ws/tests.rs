use super::*;
use crate::gateway_fixture;
use crate::reconciliation::income_import::tests::Memory;
use gateway_fixture::fixture;

fn config() -> (Config, CanonicalCommand) {
    let (snapshot, catalog, command) = fixture::fixture();
    (
        Config {
            physical: tickwire_journal::Config::new([81; 16], [1; 16]),
            writer_epoch: 4,
            baseline: tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap(),
            account_stream: [2; 16],
            max_records: 32,
            max_registrations: 8,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
            income_association_authority: None,
            income_baseline_authority: None,
        },
        fixture::canonical(command),
    )
}

#[test]
fn reservation_is_durable_and_reconnection_never_reuses_a_request_id() {
    let (cfg, command) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    let first = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    assert_eq!(input.position(), 2);
    assert_eq!(first.identity().connection_epoch, 1);
    let request = input.plan_ws_request(&first, command.clone()).unwrap();
    assert_eq!(request.input().sequence, 3);
    let second = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    assert_eq!(second.identity().connection_epoch, 2);
    let next = input.plan_ws_request(&second, command).unwrap();
    assert_ne!(request.request_id(), next.request_id());
    assert!(next.request_id().as_str().len() <= MAX_REQUEST_ID_BYTES);
    assert!(
        input
            .plan_ws_request(&first, next.command().clone())
            .is_err()
    );
}

#[test]
fn a_session_from_another_input_owner_cannot_mint_a_plan() {
    let (cfg, command) = config();
    let mut first = AccountIngress::create(Memory::default(), cfg.clone()).unwrap();
    let mut second = AccountIngress::create(Memory::default(), cfg).unwrap();
    let old = first.reserve_ws_session([9; 16], [8; 32]).unwrap();
    second.reserve_ws_session([9; 16], [8; 32]).unwrap();
    assert!(second.plan_ws_request(&old, command).is_err());
}

#[test]
fn a_plan_checks_current_writer_scope_and_baseline_catalog() {
    let (cfg, command) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    for field in 0..4 {
        let mut c = command.command().clone();
        match field {
            0 => c.context.request.intent_exec_epoch += 1,
            1 => c.context.request.account += 1,
            2 => c.context.instrument.instrument_version += 1,
            _ => c.context.request.risk_domain_id = [99; 16],
        }
        let changed = CanonicalCommand::new(c).unwrap();
        assert!(
            input.plan_ws_request(&session, changed).is_err(),
            "field {field}"
        );
    }
    assert_eq!(input.position(), 2);
}

struct PendingAuthority(gateway_fixture::MockAuthority);
impl tickwire_account_gateway::Authority for PendingAuthority {
    fn prepare(
        &mut self,
        r: tickwire_account_gateway::AdmissionRequest<'_>,
    ) -> std::result::Result<
        tickwire_recovery::VerifiedClosure,
        tickwire_account_gateway::AuthorityError,
    > {
        self.0.prepare(r)
    }
    fn verify(
        &mut self,
        r: tickwire_account_gateway::AdmissionRequest<'_>,
    ) -> std::result::Result<
        tickwire_recovery::VerifiedClosure,
        tickwire_account_gateway::AuthorityError,
    > {
        self.0.verify(r)
    }
    fn current_gate(
        &mut self,
        c: &CanonicalCommand,
        a: &tickwire_account::Admission,
    ) -> std::result::Result<tickwire_account_gateway::Gate, tickwire_account_gateway::AuthorityError>
    {
        self.0.current_gate(c, a)
    }
    fn commit_state(
        &mut self,
        _: &tickwire_account::Admission,
        _: &tickwire_account_journal::AppendReceipt,
    ) -> std::result::Result<
        tickwire_account_gateway::CommitState,
        tickwire_account_gateway::AuthorityError,
    > {
        Ok(tickwire_account_gateway::CommitState::Pending)
    }
}
fn wire() -> SignedWire {
    SignedWire {
        source: tickwire_clock::Source::SpotTest,
        domain: tickwire_clock::ClockDomain::new([5; 16], 1).unwrap(),
        timestamp: 1_000,
        unit: tickwire_clock::TimeUnit::Milliseconds,
        window_ns: 5_000_000_000,
        request_hash: [4; 32],
        frame_hash: [5; 32],
        request_bytes: 100,
        frame_bytes: 106,
    }
}
#[test]
fn binding_is_synced_using_local_admission_while_remote_commit_is_pending() {
    use tickwire_account_gateway::{AccountGateway, Status};
    let (cfg, command) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg.clone()).unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    let plan = input.plan_ws_request(&session, command.clone()).unwrap();
    let retry = plan.clone();
    let authority = gateway_fixture::MockAuthority::default();
    let gw_config = gateway_fixture::config(&cfg.baseline);
    let (connection, peer) = gateway_fixture::peer(&gw_config.route.authority, 16384);
    let dir = gateway_fixture::Directory::new();
    let mut gateway = AccountGateway::create(
        tickwire_account_journal::FileBackend::open(&dir.0).unwrap(),
        gw_config,
        cfg.baseline.clone(),
        PendingAuthority(authority.clone()),
        gateway_fixture::TestEncoder,
        connection,
    )
    .unwrap();
    assert_eq!(
        gateway
            .submit(command, 60, gateway_fixture::until())
            .unwrap(),
        Status::AwaitingCommit
    );
    let committed = gateway.committed(gateway_fixture::intent()).unwrap();
    assert_eq!(committed.receipt().sequence(), 1);
    assert_eq!(
        committed.admission().command_digest(),
        plan.command().digest()
    );
    assert_eq!(
        gateway
            .output_progress(gateway_fixture::intent())
            .unwrap()
            .accepted_bytes,
        0
    );
    let bound = input.persist_ws_binding(plan, &gateway, wire()).unwrap();
    assert_eq!(bound.account_sequence(), 1);
    assert_eq!(bound.input().sequence, 4);
    assert_eq!(input.position(), 4);
    assert_eq!(
        input
            .persist_ws_binding(retry.clone(), &gateway, wire())
            .unwrap()
            .input(),
        bound.input()
    );
    assert_eq!(input.position(), 4);
    let record = input.ws_bindings().next().unwrap().clone();
    let encoded = encode_binding(&record).unwrap();
    assert!(decode_binding(&encoded).unwrap() == record);
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_binding(&trailing).is_err());
    for changed in 0..4 {
        let mut altered = record.clone();
        match changed {
            0 => altered.record_digest[0] ^= 1,
            1 => altered.history[127] ^= 1,
            2 => altered.physical.seal_hash[0] ^= 1,
            _ => altered.account_sequence += 1,
        }
        assert!(input.verify_ws_binding(&gateway, &altered).is_err());
    }
    let original = gateway
        .admission_origin(gateway_fixture::intent())
        .unwrap()
        .clone();
    gateway
        .record_fact(tickwire_account::RecoveryRecord::UnattributedCashFlow {
            id: v::BoundedText::new("ws-test-cashflow").unwrap(),
            delta: tickwire_account::AssetAmount {
                key: tickwire_account::AssetKey {
                    wallet_id: 1,
                    asset_id: 2,
                },
                amount: tickwire_risk::Fixed::from_atoms(-1, 8).unwrap(),
            },
        })
        .unwrap();
    assert_eq!(gateway.journal_position(), 2);
    assert_eq!(
        gateway
            .export_durable(1, 2, 65536)
            .unwrap()
            .record_frontier(1),
        Some(original.receipt().physical_frontier())
    );
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let mut gc = gateway_fixture::config(&cfg.baseline);
    gc.writer_epoch = 5;
    let mut gateway = AccountGateway::recover(
        tickwire_account_journal::FileBackend::open(&dir.0).unwrap(),
        gc,
        PendingAuthority(authority),
        gateway_fixture::TestEncoder,
        None,
    )
    .unwrap();
    assert_eq!(
        gateway.admission_origin(gateway_fixture::intent()),
        Some(&original)
    );
    assert!(gateway.committed(gateway_fixture::intent()).is_none());
    let mut recovered_cfg = cfg;
    recovered_cfg.writer_epoch = 5;
    let mut input =
        AccountIngress::recover(input.journal.into_store(), recovered_cfg, None).unwrap();
    input.replay_pending(&mut gateway, 32).unwrap();
    assert!(input.persist_ws_binding(retry, &gateway, wire()).is_err());
    assert!(
        input
            .plan_ws_request(&session, fixture::canonical(fixture::fixture().2))
            .is_err()
    );
    let next = input.reserve_ws_session([10; 16], [8; 32]).unwrap();
    assert_eq!(next.identity().connection_epoch, 2);
    assert_eq!(next.identity().writer_epoch, 5);
    assert!(gateway.halted());
}

#[test]
fn request_id_encodes_all_four_components_at_integer_boundaries() {
    use base64::Engine;
    let (cfg, _) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    let mut identity = session.identity().clone();
    identity.writer_epoch = u64::MAX;
    identity.connection_epoch = u64::MAX;
    let id = request_id(&identity, u64::MAX);
    assert_eq!(id.as_str().len(), 57);
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&id.as_str()[3..])
        .unwrap();
    assert_eq!(&bytes[..16], identity.input_stream);
    for chunk in bytes[16..].as_chunks::<8>().0 {
        assert_eq!(u64::from_be_bytes(*chunk), u64::MAX);
    }
    let mut ids = std::collections::BTreeSet::new();
    ids.insert(id);
    for field in 0..4 {
        let mut changed = identity.clone();
        let seq = match field {
            0 => {
                changed.input_stream[0] ^= 1;
                u64::MAX
            }
            1 => {
                changed.writer_epoch -= 1;
                u64::MAX
            }
            2 => {
                changed.connection_epoch -= 1;
                u64::MAX
            }
            _ => u64::MAX - 1,
        };
        assert!(ids.insert(request_id(&changed, seq)));
    }
}
#[test]
fn input_owner_drop_and_explicit_disconnect_revoke_live_plans() {
    let (cfg, command) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg).unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    let plan = input.plan_ws_request(&session, command.clone()).unwrap();
    assert!(plan.is_current());
    input.invalidate_ws_session();
    assert!(!plan.is_current());
    assert!(input.plan_ws_request(&session, command.clone()).is_err());
    let next = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    let plan = input.plan_ws_request(&next, command).unwrap();
    assert!(plan.is_current());
    drop(input);
    assert!(!plan.is_current());
}

#[test]
fn websocket_reply_binds_exact_id_and_preserves_cumulative_without_inventing_a_fill() {
    use tickwire_account_gateway::AccountGateway;
    let (cfg, command) = config();
    let mut input = AccountIngress::create(Memory::default(), cfg.clone()).unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    let plan = input.plan_ws_request(&session, command.clone()).unwrap();
    let gc = gateway_fixture::config(&cfg.baseline);
    let (connection, peer) = gateway_fixture::peer(&gc.route.authority, 16384);
    let dir = gateway_fixture::Directory::new();
    let mut gateway = AccountGateway::create(
        tickwire_account_journal::FileBackend::open(&dir.0).unwrap(),
        gc,
        cfg.baseline,
        PendingAuthority(gateway_fixture::MockAuthority::default()),
        gateway_fixture::TestEncoder,
        connection,
    )
    .unwrap();
    gateway
        .submit(command, 60, gateway_fixture::until())
        .unwrap();
    let bound = input.persist_ws_binding(plan, &gateway, wire()).unwrap();
    let raw=serde_json::to_vec(&serde_json::json!({"id":bound.request_id().as_str(),"status":200,"result":{"symbol":"BTCUSDT","orderId":51,"clientOrderId":"test-1","price":"60000","origQty":"0.001","executedQty":"0.001","cummulativeQuoteQty":"60","status":"FILLED","side":"BUY","fills":[{"price":"60000","qty":"0.001","commission":"0.00001","commissionAsset":"BTC","tradeId":21}]}})).unwrap();
    let reply = response::derive(&input, &gateway, &bound.record, 70, &raw);
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let (disposition, events) = reply.unwrap();
    assert_eq!(
        disposition,
        crate::rest::RestDisposition::NeedsReconciliation
    );
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0].event().body,
        v::VenueEventBody::Order(_)
    ));
}

#[path = "../../../tickwire-account-gateway/tests/common/fault_backend.rs"]
mod fault_backend;
#[test]
fn websocket_raw_and_effect_replay_after_account_append_is_torn() {
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };
    use tickwire_account_gateway::AccountGateway;
    let (cfg, command) = config();
    let input_dir = gateway_fixture::Directory::new();
    let account_dir = gateway_fixture::Directory::new();
    let mut input = AccountIngress::create(
        tickwire_journal::store::FileStore::open(&input_dir.0).unwrap(),
        cfg.clone(),
    )
    .unwrap();
    let session = input.reserve_ws_session([9; 16], [8; 32]).unwrap();
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    let plan = input.plan_ws_request(&session, command.clone()).unwrap();
    let gc = gateway_fixture::config(&cfg.baseline);
    let (connection, peer) = gateway_fixture::peer(&gc.route.authority, 16384);
    let failure = Arc::new(AtomicU8::new(0));
    let authority = gateway_fixture::MockAuthority::default();
    let backend = fault_backend::FaultBackend {
        inner: tickwire_account_journal::FileBackend::open(&account_dir.0).unwrap(),
        next: failure.clone(),
    };
    let mut gateway = AccountGateway::create(
        backend,
        gc,
        cfg.baseline.clone(),
        PendingAuthority(authority.clone()),
        gateway_fixture::TestEncoder,
        connection,
    )
    .unwrap();
    gateway
        .submit(command, 60, gateway_fixture::until())
        .unwrap();
    let bound = input.persist_ws_binding(plan, &gateway, wire()).unwrap();
    let raw=serde_json::to_vec(&serde_json::json!({"id":bound.request_id().as_str(),"status":200,"result":{"symbol":"BTCUSDT","orderId":51,"clientOrderId":"test-1","price":"60000","origQty":"0.001","executedQty":"0.001","cummulativeQuoteQty":"60","status":"FILLED","side":"BUY"}})).unwrap();
    let (disposition, events) =
        response::derive(&input, &gateway, &bound.record, 70, &raw).unwrap();
    let response = response::ResponseRecord {
        session: bound.record.session.clone(),
        received: 70,
        binding: Some(bound.input().sequence),
        raw: raw.clone(),
        decision: ResponseDecision::Matched,
        disposition,
        events,
    };
    failure.store(fault_backend::TORN_APPEND, Ordering::SeqCst);
    assert!(input.commit_ws_response(&mut gateway, response).is_err());
    assert_eq!(input.position(), 6);
    assert_eq!(gateway.journal_position(), 1);
    assert_eq!(input.stored_ws_responses().next().unwrap().raw, raw);
    drop(input);
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let mut gc = gateway_fixture::config(&cfg.baseline);
    gc.writer_epoch = 5;
    let mut gateway = AccountGateway::recover(
        tickwire_account_journal::FileBackend::open(&account_dir.0).unwrap(),
        gc,
        PendingAuthority(authority),
        gateway_fixture::TestEncoder,
        None,
    )
    .unwrap();
    let mut cfg = cfg;
    cfg.writer_epoch = 5;
    let mut input = AccountIngress::recover(
        tickwire_journal::store::FileStore::open(&input_dir.0).unwrap(),
        cfg,
        None,
    )
    .unwrap();
    input.replay_pending(&mut gateway, 32).unwrap();
    assert_eq!(gateway.journal_position(), 2);
    assert_eq!(input.applied_position(), 6);
    assert!(gateway.halted());
    assert!(gateway.account().frozen());
    assert!(gateway.committed(gateway_fixture::intent()).is_none());
    let recorded = gateway.ingress_events().collect::<Vec<_>>();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].1, 6);
    assert!(matches!(
        recorded[0].2.event().body,
        v::VenueEventBody::Order(_)
    ));
    assert_eq!(input.replay_pending(&mut gateway, 32).unwrap(), 0);
    assert_eq!(gateway.journal_position(), 2);
}
