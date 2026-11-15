use crate::{
    AccountAuthSpec, ControlPlane, Mode, OrsRuntimePolicy, PolicyAction, PolicyConfig,
    PolicyConfigError,
};
use tickwire_venue::{ALL_ORS_STRATS, RefusalReason};

fn sample_policy() -> OrsRuntimePolicy {
    let cfg = PolicyConfig {
        initial_mode: Mode::Trading,
        strategies: vec![1, 2, 7],
        enabled_accounts: vec![
            AccountAuthSpec::new(2, "acct_primary", vec![1, 7]),
            AccountAuthSpec::new(5, "acct_secondary", vec![1, 2]),
        ],
        default_active: true,
        default_enabled: true,
    };
    OrsRuntimePolicy::from_config(&cfg).expect("valid policy config")
}

#[test]
fn account_ref_zero_routes_to_first_authorized_in_enabled_order() {
    let policy = sample_policy();
    assert_eq!(policy.enabled_accounts(), &[2, 5]);

    // Strategy 1 is in both acct 2 (first) and acct 5 (second) -> 0 resolves to 2!
    assert_eq!(policy.resolve_account_ref(0, 1), Ok(2));
    assert_eq!(policy.resolve_account_ref(2, 1), Ok(2));
    assert_eq!(policy.resolve_account_ref(5, 1), Ok(5));

    // Strategy 2 is only in acct 5 -> 0 resolves to 5; explicit 2 is routing mismatch!
    assert_eq!(policy.resolve_account_ref(0, 2), Ok(5));
    assert_eq!(
        policy.resolve_account_ref(2, 2),
        Err(RefusalReason::RejectRoutingIdMismatch)
    );
    assert_eq!(policy.resolve_account_ref(5, 2), Ok(5));

    // Strategy 3 is unknown -> 0 gives InvalidStratId; known acct gives RejectRoutingIdMismatch; unknown acct gives InvalidAccountId.
    assert_eq!(
        policy.resolve_account_ref(0, 3),
        Err(RefusalReason::InvalidStratId)
    );
    assert_eq!(
        policy.resolve_account_ref(2, 3),
        Err(RefusalReason::RejectRoutingIdMismatch)
    );
    assert_eq!(
        policy.resolve_account_ref(99, 1),
        Err(RefusalReason::InvalidAccountId)
    );
    assert_eq!(
        policy.resolve_account_ref(0, ALL_ORS_STRATS),
        Err(RefusalReason::InvalidStratId)
    );
}

#[test]
fn granular_account_strat_enable_isolates_accounts_and_keeps_cancels_open() {
    let policy = sample_policy();

    // Initially strategy 1 can trade on both account 2 and account 5.
    assert_eq!(policy.evaluate(PolicyAction::NewOrder, 0, 1), Ok(2));
    assert_eq!(policy.evaluate(PolicyAction::NewOrder, 5, 1), Ok(5));

    // Disable strategy 1 ONLY on account 2: account 2 refuses NewOrder/Modify/RfqQuote with AdminAction,
    // while Cancel on account 2 and NewOrder on account 5 continue succeeding!
    assert_eq!(policy.set_strat_enabled(2, 1, false), Ok(true));
    assert!(!policy.is_strat_enabled(2, 1));
    assert!(policy.is_strat_enabled(5, 1));
    assert!(policy.is_global_strat_enabled(1));

    assert_eq!(
        policy.evaluate(PolicyAction::NewOrder, 2, 1),
        Err(RefusalReason::AdminAction)
    );
    assert_eq!(
        policy.evaluate(PolicyAction::Modify, 2, 1),
        Err(RefusalReason::AdminAction)
    );
    assert_eq!(
        policy.evaluate(PolicyAction::RfqQuote, 2, 1),
        Err(RefusalReason::AdminAction)
    );
    assert_eq!(policy.evaluate(PolicyAction::Cancel, 2, 1), Ok(2));
    assert_eq!(policy.evaluate(PolicyAction::NewOrder, 5, 1), Ok(5));

    // Global strategy disable refuses both accounts with AdminAction while still allowing Cancel.
    assert_eq!(policy.set_global_strat_enabled(1, false), Ok(true));
    assert_eq!(
        policy.evaluate(PolicyAction::NewOrder, 5, 1),
        Err(RefusalReason::AdminAction)
    );
    assert_eq!(policy.evaluate(PolicyAction::Cancel, 5, 1), Ok(5));
}

#[test]
fn mode_transitions_and_active_flag_enforce_exact_refusal_reasons() {
    let policy: ControlPlane = sample_policy();
    assert_eq!(policy.mode(), Mode::Trading);

    // Suspend -> ManageOnly / RestrictedCancelOnly
    assert_eq!(policy.suspend(), Mode::Trading);
    assert_eq!(policy.mode(), Mode::RestrictedCancelOnly);
    assert_eq!(
        policy.evaluate(PolicyAction::NewOrder, 0, 1),
        Err(RefusalReason::RestrictedCancelOnly)
    );
    assert_eq!(policy.evaluate(PolicyAction::Cancel, 0, 1), Ok(2));
    assert_eq!(policy.evaluate(PolicyAction::Query, 5, 2), Ok(5));

    // Resume -> Trading
    assert_eq!(policy.resume(), Mode::ManageOnly);
    assert_eq!(policy.evaluate(PolicyAction::NewOrder, 0, 1), Ok(2));

    // SetActive(false) -> refuses with InvalidStratId while keeping DOP login intact
    assert_eq!(policy.try_login(1), Ok(()));
    assert_eq!(policy.try_login(1), Err(RefusalReason::InvalidStratId));
    assert!(policy.is_connected(1));
    assert_eq!(policy.set_active(1, false), Ok(true));
    assert!(policy.is_connected(1));
    assert_eq!(
        policy.evaluate(PolicyAction::NewOrder, 0, 1),
        Err(RefusalReason::InvalidStratId)
    );
    assert_eq!(policy.set_active(1, true), Ok(false));
    assert_eq!(policy.evaluate(PolicyAction::NewOrder, 0, 1), Ok(2));
    assert!(policy.logout(1));
    assert!(!policy.is_connected(1));
    assert_eq!(policy.try_login(1), Ok(()));

    // Stop -> refuses strategy orders and cancels, allows AdminCancel
    assert_eq!(policy.stop(), Mode::Trading);
    assert_eq!(
        policy.evaluate(PolicyAction::NewOrder, 0, 1),
        Err(RefusalReason::MarketRejectMarketDown)
    );
    assert_eq!(
        policy.evaluate(PolicyAction::Cancel, 0, 1),
        Err(RefusalReason::MarketRejectMarketDown)
    );
    assert_eq!(
        policy.evaluate(PolicyAction::AdminCancel, 0, ALL_ORS_STRATS),
        Ok(0)
    );
}

#[test]
fn config_validation_rejects_bad_refs_names_and_strats() {
    assert_eq!(
        PolicyConfig::default().validate(),
        Err(PolicyConfigError::EmptyEnabledAccounts)
    );

    let bad_zero = PolicyConfig {
        enabled_accounts: vec![AccountAuthSpec::new(0, "acct0", vec![1])],
        ..PolicyConfig::default()
    };
    assert_eq!(
        bad_zero.validate(),
        Err(PolicyConfigError::InvalidAccountRef(0))
    );

    let bad_dot = PolicyConfig {
        enabled_accounts: vec![AccountAuthSpec::new(1, "acct.bad", vec![1])],
        ..PolicyConfig::default()
    };
    assert_eq!(
        bad_dot.validate(),
        Err(PolicyConfigError::InvalidAccountName("acct.bad".into()))
    );

    let bad_all_strats = PolicyConfig {
        enabled_accounts: vec![AccountAuthSpec::new(1, "acct1", vec![ALL_ORS_STRATS])],
        ..PolicyConfig::default()
    };
    assert_eq!(
        bad_all_strats.validate(),
        Err(PolicyConfigError::ReservedStratId(ALL_ORS_STRATS))
    );

    let bad_unwhitelisted = PolicyConfig {
        strategies: vec![1],
        enabled_accounts: vec![AccountAuthSpec::new(1, "acct1", vec![1, 2])],
        ..PolicyConfig::default()
    };
    assert_eq!(
        bad_unwhitelisted.validate(),
        Err(PolicyConfigError::UnwhitelistedAccountStratId {
            account_ref: 1,
            strat_id: 2,
        })
    );
}
