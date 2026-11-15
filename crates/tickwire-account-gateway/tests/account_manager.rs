mod common;
use common::*;
use std::net::{IpAddr, Ipv4Addr};
use tickwire_account_gateway::*;
use tickwire_quota::rate_limiter::{IpKey, RateReject};
use tickwire_quota::shared::{
    InstrumentScope, IpBudgetSpec, OrsAccountQuota, OrsVenueProfile, SharedRateLimiter,
};
use tickwire_transport::{Connection, Security};
use tickwire_venue::{RefusalReason, Side};

#[test]
fn credentials_redact_debug_and_validate_non_empty() {
    let creds = Credentials::new("ak-live-secret-key", "sk-super-secret", "pp-top-secret");
    let dbg = format!("{creds:?}");
    assert!(!dbg.contains("ak-live-secret-key"));
    assert!(!dbg.contains("sk-super-secret"));
    assert!(!dbg.contains("pp-top-secret"));
    assert!(dbg.contains("[REDACTED]"));
    assert!(creds.validate().is_ok());

    let empty = Credentials::new("   ", "secret", "");
    assert_eq!(empty.validate(), Err(AccountManagerError::EmptyCredentials));
}

#[test]
fn account_manager_loads_enabled_accounts_order_url_binds_and_routes_account_ref() {
    let json = r#"{
        "enabled_accounts": ["acct_beta", "acct_alpha"],
        "acct_alpha": {
            "account_ref": 1,
            "exchange_account_id": 1001,
            "strategies": [2, 5],
            "key": "k-alpha",
            "secret": "s-alpha",
            "local_ip": "10.0.10.1",
            "nic": "eno1",
            "urls": {
                "order": {},
                "stream": {
                    "local_ip/connection_count": [["10.0.10.2", 1], ["", 3]]
                }
            },
            "rate_limit": {
                "orders_10s": 75,
                "orders_min": 600
            }
        },
        "acct_beta": {
            "account_ref": 2,
            "exchange_account_id": 2002,
            "strategies": [5, 9],
            "credentials": {
                "key": "k-beta",
                "secret": "s-beta",
                "passphrase": "p-beta"
            },
            "urls": {
                "order": {
                    "local_ip/connection_count": [["10.0.20.1", 2]]
                }
            }
        },
        "acct_disabled": {
            "account_ref": 3,
            "strategies": [99],
            "key": "k-off",
            "secret": "s-off",
            "urls": { "order": {} }
        }
    }"#;

    let mgr = AccountManager::from_json_str(json).unwrap();
    assert_eq!(mgr.specs().len(), 2);
    // Strict enabled_accounts order: acct_beta (ref 2) precedes acct_alpha (ref 1).
    assert_eq!(mgr.specs()[0].name, "acct_beta");
    assert_eq!(mgr.specs()[0].account_ref, 2);
    assert_eq!(mgr.specs()[1].name, "acct_alpha");
    assert_eq!(mgr.specs()[1].account_ref, 1);
    assert!(mgr.by_account_ref(3).is_none());

    // Strategy 5 is authorized on BOTH accounts; account_ref == 0 routes to the FIRST in enabled_accounts order (ref 2).
    assert_eq!(mgr.accounts_for_strategy(5), &[2, 1]);
    assert_eq!(mgr.resolve_account_ref(0, 5).unwrap().account_ref, 2);
    assert_eq!(mgr.resolve_account_ref(0, 2).unwrap().account_ref, 1);

    // Explicit account_ref routing & refusal codes (human-docs/8.3):
    assert_eq!(mgr.resolve_account_ref(1, 5).unwrap().account_ref, 1);
    assert_eq!(
        mgr.resolve_account_ref(0, 42).unwrap_err().refusal_reason(),
        RefusalReason::InvalidStratId
    );
    assert_eq!(
        mgr.resolve_account_ref(0, 255)
            .unwrap_err()
            .refusal_reason(),
        RefusalReason::InvalidStratId
    );
    assert_eq!(
        mgr.resolve_account_ref(99, 5).unwrap_err().refusal_reason(),
        RefusalReason::InvalidAccountId
    );
    assert_eq!(
        mgr.resolve_account_ref(2, 2).unwrap_err().refusal_reason(),
        RefusalReason::RejectRoutingIdMismatch
    );

    // Default 2-socket fallback on empty local_ip/connection_count + inheritance of account local_ip/nic:
    let alpha = mgr.by_account_ref(1).unwrap();
    let order_binds = alpha.effective_url_binds("order").unwrap();
    assert_eq!(order_binds.len(), 1);
    assert_eq!(order_binds[0].count, DEFAULT_URL_CONNECTIONS);
    assert_eq!(
        order_binds[0].local_ip,
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 10, 1)))
    );
    assert_eq!(order_binds[0].nic.as_deref(), Some("eno1"));

    let stream_binds = alpha.effective_url_binds("stream").unwrap();
    assert_eq!(stream_binds.len(), 2);
    assert_eq!(
        stream_binds[0].local_ip,
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 10, 2)))
    );
    assert_eq!(stream_binds[0].count, 1);
    // Empty "" IP inherits account local_ip:
    assert_eq!(
        stream_binds[1].local_ip,
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 10, 1)))
    );
    assert_eq!(stream_binds[1].count, 3);

    // query_local_ip uses unanimous URL bind IP when uniform, else account local_ip:
    let beta = mgr.by_account_ref(2).unwrap();
    assert_eq!(
        beta.query_local_ip(Some("order")),
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 20, 1)))
    );
    assert_eq!(
        alpha.query_local_ip(Some("stream")),
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 10, 1)))
    );

    // Rate-limit overrides mapped cleanly:
    assert_eq!(alpha.rate_limit.primary_order_limit, Some(75));
    assert_eq!(alpha.rate_limit.secondary_order_limit, Some(600));
}

#[test]
fn submit_pooled_with_quota_nacks_immediately_on_picked_ip_without_retrying_sibling_slot() {
    let (snap, cat, raw_base) = fixture::fixture();
    let b = tickwire_account_codec::CanonicalBaseline::new(snap, cat).unwrap();
    let base_cmd = fixture::canonical(raw_base);
    let cfg = config(&b);
    let auth = MockAuthority::default();
    let mut g = AccountGateway::create_unconnected(cfg, b, auth, TestEncoder)
        .unwrap()
        .with_connection_pool(RouterConfig::new(1, 4, 1))
        .unwrap();

    let ip0 = IpAddr::V4(Ipv4Addr::new(10, 0, 50, 1));
    let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 50, 2));
    let mut peers = Vec::new();
    for ip in [ip0, ip1] {
        let (pcfg, handle) = peer(SPOT_HOST, 16384);
        let server_name = SPOT_HOST.to_owned().try_into().unwrap();
        let conn = Connection::connect(
            pcfg.address,
            Security::Tls {
                server_name,
                config: pcfg.tls,
            },
            pcfg.limits,
            pcfg.connect_deadline,
        )
        .unwrap();
        let mut spec = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn);
        spec.local_ip = Some(ip);
        g.add_pool_slot(spec, pcfg.limits).unwrap();
        peers.push(handle);
    }

    // Calibrate slot 0 faster (200 us) than slot 1 (900 us) so router deterministically picks slot 0 (ip0).
    g.note_slot_sample(0, 1, SampleKind::Order, 200).unwrap();
    g.note_slot_sample(1, 1, SampleKind::Order, 900).unwrap();

    // Configure ip0 with ip_weight_min = 2 and ip1 with ip_weight_min = 1000.
    let limiter = SharedRateLimiter::new();
    limiter
        .install_venue_ip_budgets(
            OrsVenueProfile::BinanceSpot,
            &[IpBudgetSpec::new(ip0, 2), IpBudgetSpec::new(ip1, 1_000)],
        )
        .unwrap();
    let quota = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceSpot, 2).unwrap();
    let scope = InstrumentScope::Symbol(12);

    // Request weight = 5 exceeds ip0's budget (2) while ip1 has plenty (1000).
    // Must NACK immediately on ip0 without falling back to slot 1 or reserving funds!
    let mut cmd = base_cmd.command().clone();
    cmd.context.request.intent_id = 88;
    cmd.context.request.recovery_object += 88;
    if let tickwire_venue::CommandBody::New(ref mut o) = cmd.body {
        o.side = Side::Buy;
        o.client_id = fixture::venue::text("quota-nack-0");
    }
    let res = g.submit_pooled_with_quota(
        0,
        &limiter,
        &quota,
        scope,
        5,
        fixture::canonical(cmd),
        1_000_000_000,
        until(),
    );
    match res {
        Err(Error::RateLimited(RateReject::Exhausted { id, .. })) => {
            assert_eq!(id.local_ip, IpKey::from_ip(ip0));
        }
        other => panic!("expected RateLimited on ip0, got {other:?}"),
    }
    // Zero admission created, zero funds reserved, and slot 1 was never touched!
    assert_eq!(g.entry_count(), 0);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    assert_eq!(g.slot_route(0).unwrap().health.inflight(), 0);
    assert_eq!(g.slot_route(1).unwrap().health.inflight(), 0);

    drop(g);
    for p in peers {
        let _ = p.join().unwrap();
    }
}
