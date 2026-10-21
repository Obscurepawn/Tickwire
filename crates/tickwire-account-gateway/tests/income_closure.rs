mod common;
#[path = "common/fault_backend.rs"]
mod fault_backend;
use common::*;
use fault_backend::*;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use tickwire_account::{
    Account, AccountInputsReconciled, Applied, IncomeBaseline, Reactivation, RecoveryRecord,
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::*;
use tickwire_account_journal::{AccountJournal, FileBackend, HistoryFrontier};
use tickwire_venue as v;
struct VerifiedFixture {
    allow: bool,
    calls: usize,
}
impl ReactivationVerifier for VerifiedFixture {
    fn verify(
        &mut self,
        _: &CanonicalBaseline,
        _: &Account,
        _: &HistoryFrontier,
        _: &Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
    fn verify_income_baseline(
        &mut self,
        b: &CanonicalBaseline,
        a: &Account,
        h: &HistoryFrontier,
        r: &IncomeBaseline,
    ) -> std::result::Result<(), AuthorityError> {
        if b.digest() == r.baseline_digest
            && a.ledger_sequence() == r.ledger_sequence
            && h.record_count() == 0
        {
            Ok(())
        } else {
            Err(AuthorityError::Denied)
        }
    }
    fn verify_account_inputs_closure_live(
        &mut self,
        b: &CanonicalBaseline,
        a: &Account,
        h: &HistoryFrontier,
        r: &AccountInputsReconciled,
        _writer: u64,
    ) -> std::result::Result<(), AuthorityError> {
        self.calls += 1;
        if self.allow
            && b.digest() == r.baseline_digest
            && h.digest() == r.history_digest
            && a.ledger_sequence() == r.ledger_sequence
        {
            Ok(())
        } else {
            Err(AuthorityError::Denied)
        }
    }
}
fn setup() -> (
    Directory,
    AccountGateway<FaultBackend, MockAuthority, TestEncoder>,
    Arc<AtomicU8>,
    VerifiedFixture,
) {
    let dir = Directory::new();
    let (b, c) = baseline(true);
    let cfg = config(&b);
    drop(
        AccountJournal::create(
            FileBackend::open(&dir.0).unwrap(),
            cfg.journal.clone(),
            b.clone(),
        )
        .unwrap(),
    );
    let next = Arc::new(AtomicU8::new(0));
    let mut g = AccountGateway::recover(
        FaultBackend {
            inner: FileBackend::open(&dir.0).unwrap(),
            next: next.clone(),
        },
        cfg,
        MockAuthority::default(),
        TestEncoder,
        None,
    )
    .unwrap();
    let mut verifier = VerifiedFixture {
        allow: true,
        calls: 0,
    };
    g.install_income_baseline(
        IncomeBaseline {
            scope: b.snapshot().scope.clone(),
            account_epoch: 1,
            ledger_sequence: 0,
            wallet_id: 1,
            through_time_ms: 100,
            coverage_digest: [5; 32],
            baseline_digest: b.digest(),
            input_stream: [9; 16],
            through_sequence: 2,
            covered: vec![],
        },
        &mut verifier,
    )
    .unwrap();
    let input = v::CanonicalAccountInput::new(v::AccountInput {
        identity: v::AccountIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            market: v::AccountMarket::Usdm,
            wallet_id: 1,
        },
        body: v::AccountInputBody::Income(v::IncomeRecord {
            income_type: fixture::venue::text("FUNDING_FEE"),
            transaction_id: fixture::venue::text("1"),
            delta: v::AssetDelta {
                asset: c.command().context.instrument.settlement.clone(),
                amount: "-2".parse().unwrap(),
            },
            symbol: None,
            trade_id: None,
            venue_time_ms: 101,
            info: None,
        }),
    })
    .unwrap();
    g.record_account_input([9; 16], 3, input).unwrap();
    (dir, g, next, verifier)
}
fn binding(
    g: &AccountGateway<FaultBackend, MockAuthority, TestEncoder>,
) -> AccountInputsReconciled {
    AccountInputsReconciled {
        scope: g.baseline().snapshot().scope.clone(),
        account_epoch: 1,
        ledger_sequence: g.account().ledger_sequence(),
        baseline_digest: g.baseline().digest(),
        history_digest: g.history_frontier().digest(),
        input_stream: [9; 16],
        through_sequence: 9,
        state_digest: g
            .account()
            .account_inputs_state_digest(g.baseline().digest(), g.history_frontier().digest())
            .unwrap(),
        coverage_digest: [7; 32],
    }
}
#[test]
fn closure_requires_verifier_sync_then_publishes_and_exact_retry_skips_current_time() {
    let (_dir, mut g, _next, mut verifier) = setup();
    let b = binding(&g);
    let before = g.account().asset(fixture::key(2)).unwrap();
    verifier.allow = false;
    assert!(
        g.reconcile_account_inputs(b.clone(), &mut verifier)
            .is_err()
    );
    assert_eq!(g.journal_position(), 2);
    verifier.allow = true;
    assert_eq!(
        g.reconcile_account_inputs(b.clone(), &mut verifier)
            .unwrap(),
        Applied::New
    );
    assert_eq!(g.journal_position(), 3);
    assert_eq!(g.account().unresolved_account_inputs(), 0);
    assert!(g.account().frozen());
    assert_eq!(g.account().asset(fixture::key(2)).unwrap(), before);
    let calls = verifier.calls;
    verifier.allow = false;
    assert_eq!(
        g.reconcile_account_inputs(b.clone(), &mut verifier)
            .unwrap(),
        Applied::Duplicate
    );
    assert_eq!(verifier.calls, calls);
    assert_eq!(g.journal_position(), 3);
    assert!(
        g.record_fact(RecoveryRecord::AccountInputsReconciled(Box::new(b)))
            .is_err()
    );
}
#[test]
fn closure_sync_failure_keeps_the_previous_account_and_freezes() {
    for fault in [TORN_APPEND, LOST_SYNC_REPLY] {
        let (_dir, mut g, next, mut verifier) = setup();
        let b = binding(&g);
        let before = g.account().asset(fixture::key(2)).unwrap();
        next.store(fault, Ordering::SeqCst);
        assert!(g.reconcile_account_inputs(b, &mut verifier).is_err());
        assert_eq!(next.load(Ordering::SeqCst), 0);
        assert_eq!(g.journal_position(), 2);
        assert_eq!(g.account().ledger_sequence(), 2);
        assert_eq!(g.account().unresolved_account_inputs(), 1);
        assert_eq!(g.account().asset(fixture::key(2)).unwrap(), before);
        assert!(g.halted() && g.account().frozen());
    }
}
