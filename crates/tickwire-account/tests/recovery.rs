mod common;
use common::*;
use tickwire_account::*;
#[test]
fn replay_recomputes_and_checks_the_complete_admission_then_returns_frozen_unknown() {
    let (s, c, cmd) = fixture();
    let mut live = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let command = canonical(cmd.clone());
    let admission = live.admit(&command, 60).unwrap();
    let fill = event(fill(&cmd, "t1", "0.0004", "50000", "20"));
    live.apply_event(&fill).unwrap();
    let log = vec![
        RecoveryRecord::Admit {
            command: Box::new(command),
            evaluated_at: 60,
            expected: admission.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(fill)),
    ];
    let restored = Account::recover(s, c, &log).unwrap();
    assert!(restored.frozen());
    assert_eq!(restored.asset(key(2)).unwrap(), live.asset(key(2)).unwrap());
    assert!(restored.order(&admission.orders()[0]).unwrap().unknown);
    assert_eq!(
        restored.check_admission(restored.admission(admission.intent()).unwrap(), 60),
        Err(Error::Frozen)
    );
}
#[test]
fn tampered_multi_asset_budget_is_not_a_recovery_capability() {
    let (s, c, cmd) = fixture();
    let mut a = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let command = canonical(cmd);
    let admission = a.admit(&command, 60).unwrap();
    let mut image = admission.image();
    image.resources[0].amount = units(0);
    assert!(matches!(
        Account::recover(
            s,
            c,
            &[RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: image
            }]
        ),
        Err(Error::Conflict)
    ));
}
#[test]
fn ordered_abort_is_replayed_before_recovery_freezes_unresolved_requests() {
    let (s, c, cmd) = fixture();
    let mut a = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let command = canonical(cmd);
    let admission = a.admit(&command, 60).unwrap();
    let records = [
        RecoveryRecord::Admit {
            command: Box::new(command),
            evaluated_at: 60,
            expected: admission.image(),
        },
        RecoveryRecord::Aborted {
            intent: admission.intent(),
            command_digest: admission.command_digest(),
        },
    ];
    let recovered = Account::recover(s, c, &records).unwrap();
    assert!(recovered.frozen());
    assert_eq!(recovered.asset(key(2)).unwrap().reserved, units(0));
    assert!(recovered.order(&admission.orders()[0]).unwrap().aborted);
}
