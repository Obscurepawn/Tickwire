mod common;
use common::*;
use tickwire_account::*;
use tickwire_account_codec::*;
use tickwire_account_journal::{
    AccountJournal, Config, FileBackend, HistoryFrontier, RecoveryVerifier, VerificationError,
};
use tickwire_venue as v;
struct Verify {
    binding: AccountInputsReconciled,
    calls: usize,
}
impl RecoveryVerifier for Verify {
    fn verify_admission(
        &mut self,
        _: &CanonicalBaseline,
        _: &CanonicalRecord,
        _: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
    fn verify_income_baseline(
        &mut self,
        b: &CanonicalBaseline,
        r: &CanonicalRecord,
        _: &HistoryFrontier,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        match r.record() {
            RecoveryRecord::IncomeBaseline(v)
                if prior.is_empty() && v.baseline_digest == b.digest() =>
            {
                Ok(())
            }
            _ => Err(VerificationError::Invalid),
        }
    }
    fn verify_account_inputs_closure(
        &mut self,
        b: &CanonicalBaseline,
        r: &CanonicalRecord,
        h: &HistoryFrontier,
        prior: &[CanonicalRecord],
        _writer: u64,
    ) -> std::result::Result<(), VerificationError> {
        if r.record() != &RecoveryRecord::AccountInputsReconciled(Box::new(self.binding.clone()))
            || h.digest() != self.binding.history_digest
        {
            return Err(VerificationError::Invalid);
        }
        let a = Account::recover(
            b.snapshot().clone(),
            b.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        if a.account_inputs_state_digest(b.digest(), h.digest())
            .map_err(|_| VerificationError::Invalid)?
            != self.binding.state_digest
        {
            return Err(VerificationError::Invalid);
        }
        self.calls += 1;
        Ok(())
    }
}
#[test]
fn durable_closure_retry_and_reopen_keep_the_exact_original_prefix() {
    let path = std::env::temp_dir().join(format!("tickwire-income-closure-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    let (s, c, cmd) = futures_fixture();
    let baseline = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: s.scope.clone(),
    };
    let mut j = AccountJournal::create(
        FileBackend::open(&path).unwrap(),
        config.clone(),
        baseline.clone(),
    )
    .unwrap();
    j.append(RecoveryRecord::IncomeBaseline(Box::new(IncomeBaseline {
        scope: s.scope.clone(),
        account_epoch: 1,
        ledger_sequence: 0,
        wallet_id: 1,
        through_time_ms: 100,
        coverage_digest: [5; 32],
        baseline_digest: baseline.digest(),
        input_stream: [9; 16],
        through_sequence: 2,
        covered: vec![],
    })))
    .unwrap();
    let income = v::CanonicalAccountInput::new(v::AccountInput {
        identity: v::AccountIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            market: v::AccountMarket::Usdm,
            wallet_id: 1,
        },
        body: v::AccountInputBody::Income(v::IncomeRecord {
            income_type: venue::text("FUNDING_FEE"),
            transaction_id: venue::text("1"),
            delta: v::AssetDelta {
                asset: cmd.context.instrument.settlement,
                amount: "-2".parse().unwrap(),
            },
            symbol: None,
            trade_id: None,
            venue_time_ms: 101,
            info: None,
        }),
    })
    .unwrap();
    j.append(RecoveryRecord::IngressAccountInput {
        input_stream: [9; 16],
        input_sequence: 3,
        input: Box::new(income),
    })
    .unwrap();
    let a = Account::recover(
        s,
        c,
        &j.records()
            .iter()
            .map(|r| r.record().clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let binding = AccountInputsReconciled {
        scope: config.scope.clone(),
        account_epoch: 1,
        ledger_sequence: a.ledger_sequence(),
        baseline_digest: baseline.digest(),
        history_digest: j.history_frontier().digest(),
        input_stream: [9; 16],
        through_sequence: 9,
        state_digest: a
            .account_inputs_state_digest(baseline.digest(), j.history_frontier().digest())
            .unwrap(),
        coverage_digest: [7; 32],
    };
    let record = RecoveryRecord::AccountInputsReconciled(Box::new(binding.clone()));
    let first = j.append(record.clone()).unwrap();
    assert_eq!(j.append(record.clone()).unwrap(), first);
    assert_eq!(j.position(), 3);
    drop(j);
    let mut verifier = Verify {
        binding: binding.clone(),
        calls: 0,
    };
    let mut recovered = AccountJournal::recover(
        FileBackend::open(&path).unwrap(),
        config,
        5,
        None,
        &mut verifier,
    )
    .unwrap();
    assert_eq!(verifier.calls, 1);
    assert_eq!(recovered.account.ledger_sequence(), 3);
    assert!(recovered.account.frozen());
    assert_eq!(recovered.account.asset(key(2)).unwrap().total, units(998));
    assert_eq!(
        recovered.journal.append(record).unwrap().record_digest(),
        first.record_digest()
    );
    assert_eq!(recovered.journal.position(), 3);
    let mut conflict = binding;
    conflict.coverage_digest = [8; 32];
    assert!(
        recovered
            .journal
            .append(RecoveryRecord::AccountInputsReconciled(Box::new(conflict)))
            .is_err()
    );
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}
