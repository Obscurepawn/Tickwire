use super::*;
use crate::replay::income_tests::common;
use tickwire_release::{
    rebuild::{AccountPrefix, TimedInput},
    replay::{ClockDomain, Input, StreamKey, VersionedBytes},
};

struct Fixture {
    baseline: CanonicalBaseline,
    inputs: Inputs,
    actions: Vec<ActionBinding>,
    records: Vec<CanonicalRecord>,
    command: tickwire_venue::Command,
}
impl Fixture {
    fn new() -> Self {
        let (snapshot, catalog, command) = common::fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let mut account = tickwire_account::Account::from_snapshot(snapshot, catalog).unwrap();
        let accepted = common::canonical(command.clone());
        let admission = account.admit(&accepted, 50).unwrap();
        let record = CanonicalRecord::new(
            baseline.digest(),
            1,
            RecoveryRecord::Admit {
                command: Box::new(accepted.clone()),
                evaluated_at: 50,
                expected: admission.image().clone(),
            },
        )
        .unwrap();
        let inputs = Inputs {
            schema: 1,
            clock: ClockDomain {
                boot_id: [3; 16],
                generation: 1,
            },
            start_ns: 50,
            config: VersionedBytes {
                revision: 1,
                bytes: b"config".to_vec(),
            },
            models: vec![],
            markets: vec![],
            accounts: vec![AccountPrefix {
                baseline: baseline.bytes().to_vec(),
                records: vec![record.bytes().to_vec()],
            }],
            warm: vec![],
        };
        let actions = vec![ActionBinding {
            economic_intent: command.context.strategy.economic_intent_id,
            admitted_command: Object::of(accepted.bytes()),
        }];
        Self {
            baseline,
            inputs,
            actions,
            records: vec![record],
            command,
        }
    }
    fn check(&self) -> Result<()> {
        history(
            &self.inputs,
            &self.actions,
            8,
            &self.baseline,
            self.records.iter(),
        )
    }
    fn fill(&mut self) {
        self.records.push(
            CanonicalRecord::new(
                self.baseline.digest(),
                self.records.len() as u64 + 1,
                RecoveryRecord::VenueEvent(Box::new(common::event(common::fill(
                    &self.command,
                    "fill-1",
                    "0.001",
                    "60000",
                    "60",
                )))),
            )
            .unwrap(),
        );
    }
}

#[test]
fn later_admit_cannot_expand_the_fixed_action_set() {
    let mut f = Fixture::new();
    f.check().unwrap();
    let mut other = f.command.clone();
    other.context.request.intent_id += 1;
    other.context.strategy.economic_intent_id = [4; 16];
    other.context.strategy.strategy_intent_seq += 1;
    let tickwire_venue::CommandBody::New(order) = &mut other.body else {
        unreachable!()
    };
    order.client_id = common::venue::text("second");
    let accepted = common::canonical(other);
    let mut account = tickwire_account::Account::from_snapshot(
        f.baseline.snapshot().clone(),
        f.baseline.catalog().clone(),
    )
    .unwrap();
    account
        .admit(&common::canonical(f.command.clone()), 50)
        .unwrap();
    let admission = account.admit(&accepted, 50).unwrap();
    f.records.push(
        CanonicalRecord::new(
            f.baseline.digest(),
            2,
            RecoveryRecord::Admit {
                command: Box::new(accepted),
                evaluated_at: 50,
                expected: admission.image().clone(),
            },
        )
        .unwrap(),
    );
    assert!(
        f.check().is_err(),
        "a signed window cannot absorb a newly accepted responsibility"
    );
}

#[test]
fn real_prefix_allows_later_fill_but_rejects_missing_or_substituted_rows() {
    let mut f = Fixture::new();
    f.fill();
    f.check().unwrap();
    let stream = StreamKey {
        identity: f.baseline.digest(),
        generation: f.baseline.snapshot().account_epoch,
    };
    f.inputs.warm.push(TimedInput {
        now_ns: 60,
        input: Input::AccountCallback {
            stream,
            position: 2,
            canonical: f.records[1].bytes().to_vec(),
        },
    });
    f.check().unwrap();
    let row = f.records.pop().unwrap();
    assert!(
        f.check().is_err(),
        "a window cannot claim an unobserved account row"
    );
    f.records.push(row);
    f.inputs.accounts[0].records[0][0] ^= 1;
    assert!(f.check().is_err());
}
