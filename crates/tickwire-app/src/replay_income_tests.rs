//! Real file completion boundary: input approval is synchronized before its
//! account effect. The test key is a dedicated, explicitly trusted risk admin.
use super::*;
use ring::signature::{Ed25519KeyPair, KeyPair};
use tickwire_binance_account::reconciliation::income_import::{
    IncomeBaselineAuthority, IncomeImportProposal,
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_journal::Journal;

#[path = "../../tickwire-account/tests/common/mod.rs"]
pub(crate) mod common;

const ACCOUNT_STREAM: [u8; 16] = [2; 16];
const INPUT_STREAM: [u8; 16] = [81; 16];

fn blob(out: &mut Vec<u8>, b: &[u8]) {
    out.extend((b.len() as u32).to_le_bytes());
    out.extend(b);
}
fn frame(tag: u8) -> Vec<u8> {
    let mut out = b"TWAI0001".to_vec();
    out.push(tag);
    out
}
fn numbers(out: &mut Vec<u8>, values: &[u64]) {
    for n in values {
        out.extend(n.to_le_bytes());
    }
}
fn http(body: &[u8]) -> Vec<u8> {
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend(body);
    raw
}

struct Fixture {
    root: Scratch,
    input_config: tickwire_binance_account::Config,
    account_config: tickwire_account_journal::Config,
    binding: tickwire_account::IncomeBaseline,
}
impl Fixture {
    fn interrupted() -> Self {
        let root = Scratch::new().unwrap();
        let (s, c, _) = common::futures_fixture();
        let baseline = CanonicalBaseline::new(s, c).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
        let input_config = tickwire_binance_account::Config {
            income_association_authority: None,
            income_baseline_authority: Some(IncomeBaselineAuthority {
                controller_id: [31; 16],
                public_key: key.public_key().as_ref().try_into().unwrap(),
            }),
            physical: tickwire_journal::Config::new(INPUT_STREAM, [1; 16]),
            writer_epoch: 4,
            baseline: baseline.clone(),
            account_stream: ACCOUNT_STREAM,
            max_records: 64,
            max_registrations: 16,
            max_raw_bytes: 32768,
            max_retained_bytes: 1 << 20,
        };
        let account_config = tickwire_account_journal::Config {
            physical: tickwire_journal::Config::new(ACCOUNT_STREAM, [1; 16]),
            baseline_digest: baseline.digest(),
            initial_writer_epoch: 4,
            scope: baseline.snapshot().scope.clone(),
        };
        drop(
            AccountJournal::create(
                FileBackend::open(root.0.join("account")).unwrap(),
                account_config.clone(),
                baseline.clone(),
            )
            .unwrap(),
        );
        drop(FileBlobStore::open(root.0.join("recovery")).unwrap());
        drop(
            AccountIngress::create(
                FileStore::open(root.0.join("input")).unwrap(),
                input_config.clone(),
            )
            .unwrap(),
        );
        let (mut input, _) =
            Journal::recover(FileStore::open(root.0.join("input")).unwrap(), None).unwrap();

        // Independently encode the documented input records. Recovery must
        // revalidate the HTTP evidence, canonical manifest and real signature.
        let mut begin = frame(10);
        numbers(&mut begin, &[7, 3]);
        input.append_batch(4, &[&begin]).unwrap();
        let mut income = frame(11);
        numbers(&mut income, &[1, 100, 7, 3, 8, 10, 1]);
        income.extend(2u16.to_le_bytes());
        income.push(1); // BaselineCoverage, not a cash-flow effect.
        income.extend([8; 16]);
        income.extend(200u16.to_le_bytes());
        income.push(1);
        blob(&mut income, &http(b"[]"));
        income.push(0);
        input.append_batch(4, &[&income]).unwrap();
        let assets: Vec<_> = [("BTC", "1"), ("USDT", "1000")].into_iter().map(|(asset,n)|
            json!({"asset":asset,"walletBalance":n,"availableBalance":n,"marginBalance":n,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossWalletBalance":n,"crossUnPnl":"0"})).collect();
        let body = serde_json::to_vec(&json!({"totalInitialMargin":"0","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]})).unwrap();
        let mut references = vec![(3u64, tickwire_recovery::digest(&income))];
        for i in 0..3u8 {
            let mut snapshot = frame(8);
            snapshot.push(i + 1);
            numbers(&mut snapshot, &[7, 3, 9 + u64::from(i), 11 + u64::from(i)]);
            snapshot.extend([8; 16]);
            numbers(&mut snapshot, &[1, 0]);
            snapshot.extend(200u16.to_le_bytes());
            snapshot.push(1);
            blob(&mut snapshot, &http(if i == 0 { &body } else { b"[]" }));
            input.append_batch(4, &[&snapshot]).unwrap();
            references.push((4 + u64::from(i), tickwire_recovery::digest(&snapshot)));
        }
        let mut manifest = b"TWIMNF01".to_vec();
        manifest.extend(baseline.digest());
        manifest.extend(ACCOUNT_STREAM);
        manifest.extend(INPUT_STREAM);
        numbers(&mut manifest, &[1, 0, 1, 6, 7, 3, 100]);
        manifest.extend(0u16.to_le_bytes()); // Empty queried income history.
        manifest.extend((references.len() as u16).to_le_bytes());
        for (sequence, hash) in references {
            numbers(&mut manifest, &[sequence]);
            manifest.extend(hash);
        }
        let proposal = IncomeImportProposal::decode(&manifest).unwrap();
        let domain = ClockDomain::new([8; 16], 1).unwrap();
        let payload = proposal
            .approval_payload(
                [31; 16],
                [99; 16],
                4,
                MonoTime::new(domain, 14),
                MonoTime::new(domain, 100),
            )
            .unwrap();
        let mut approval = frame(13);
        numbers(&mut approval, &[4, 15]);
        blob(&mut approval, proposal.bytes());
        blob(&mut approval, &payload);
        approval.extend(key.sign(&payload).as_ref());
        input.append_batch(4, &[&approval]).unwrap();
        drop(input); // Stop exactly after input sync, before account tag 13.
        let binding = tickwire_account::IncomeBaseline {
            scope: baseline.snapshot().scope.clone(),
            account_epoch: 1,
            ledger_sequence: 0,
            wallet_id: 1,
            through_time_ms: 100,
            coverage_digest: proposal.digest(),
            baseline_digest: baseline.digest(),
            input_stream: INPUT_STREAM,
            through_sequence: 7,
            covered: vec![],
        };
        Self {
            root,
            input_config,
            account_config,
            binding,
        }
    }
    fn reopen(
        &self,
    ) -> (
        AccountIngress<FileStore>,
        tickwire_account_journal::Recovered<FileBackend>,
    ) {
        let mut config = self.input_config.clone();
        config.writer_epoch = 5;
        let input = AccountIngress::recover(
            FileStore::open(self.root.0.join("input")).unwrap(),
            config,
            None,
        )
        .unwrap();
        assert_eq!(
            input.income_import_status([99; 16]),
            Some(self.binding.coverage_digest)
        );
        assert!(input.income_coverage_object(&self.binding).is_ok());
        let mut verifier = Historical {
            persistence: Persistence::new(SharedBlobStore::new(
                FileBlobStore::open(self.root.0.join("recovery")).unwrap(),
            )),
            input: &input,
        };
        let account = AccountJournal::recover(
            FileBackend::open(self.root.0.join("account")).unwrap(),
            self.account_config.clone(),
            5,
            None,
            &mut verifier,
        )
        .unwrap();
        drop(verifier);
        (input, account)
    }
}

#[test]
fn synchronized_income_import_is_pending_until_exact_account_baseline_is_durable() {
    let f = Fixture::interrupted();
    let (input, recovered) = f.reopen();
    assert!(recovered.account.frozen());
    assert_eq!(recovered.journal.position(), 0);
    assert_eq!(input.position(), 7);
    assert_eq!(
        pair_input(&input, &recovered.journal).unwrap(),
        1,
        "durable risk-admin import must be reported while its account effect is absent"
    );
    let recovered = offline_report(input, recovered);
    assert_eq!(report(&recovered).unwrap()["input_facts_not_in_account"], 1);
    let Recovered {
        input,
        mut journal,
        account,
        ..
    } = recovered;
    input
        .activation_verifier()
        .verify_income_baseline(
            journal.baseline(),
            &account,
            journal.history_frontier(),
            &f.binding,
        )
        .unwrap();
    journal
        .append(RecoveryRecord::IncomeBaseline(Box::new(f.binding.clone())))
        .unwrap();
    drop(journal);
    drop(input);
    let (input, recovered) = f.reopen();
    assert_eq!(recovered.journal.position(), 1);
    assert_eq!(recovered.account.ledger_sequence(), 1);
    assert_eq!(recovered.account.income_baseline(1), Some(&f.binding));
    assert_eq!(pair_input(&input, &recovered.journal).unwrap(), 0);
    assert_eq!(
        report(&offline_report(input, recovered)).unwrap()["input_facts_not_in_account"],
        0
    );
}

fn offline_report(
    input: AccountIngress<FileStore>,
    recovered: tickwire_account_journal::Recovered<FileBackend>,
) -> Recovered {
    let pending_inputs = pair_input(&input, &recovered.journal).unwrap();
    Recovered {
        input,
        journal: recovered.journal,
        account: recovered.account,
        pending_inputs,
        account_tail: recovered.discarded_tail_bytes,
        input_tail: 0,
        scratch: Scratch::new().unwrap(),
        _locks: vec![],
    }
}
