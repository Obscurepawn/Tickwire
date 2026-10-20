mod common;
use common::*;
use tickwire_account::*;

fn transfer(a: &Account, s: &Snapshot, cmd: &tickwire_venue::Command) -> StrategyHandover {
    StrategyHandover {
        scope: s.scope.clone(),
        account_epoch: s.account_epoch,
        writer_epoch: cmd.context.request.intent_exec_epoch,
        operation_id: [70; 16],
        operation_revision: 1,
        logical_strategy_id: cmd.context.strategy.logical_strategy_id,
        expected_ledger_sequence: a.ledger_sequence(),
        expected_orders_digest: a.strategy_orders_digest(cmd.context.strategy.logical_strategy_id),
        from: ManagerIdentity {
            instance: cmd.context.strategy.instance_id,
            generation: cmd.context.request.strategy_generation,
        },
        to: ManagerIdentity {
            instance: [71; 16],
            generation: cmd.context.request.strategy_generation + 1,
        },
        checkpoint_digest: [72; 32],
        input_seal_digest: [73; 32],
        evaluated_at: 70,
        valid_until: 100,
    }
}
#[test]
fn transfer_preserves_reservations_and_origin_and_replays_without_releasing_unknown() {
    let (s, c, cmd) = fixture();
    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let command = canonical(cmd.clone());
    let admission = account.admit(&command, 60).unwrap();
    let key = &admission.orders()[0];
    let old = account.order(key).unwrap();
    let assets = [
        account.asset(common::key(1)).unwrap(),
        account.asset(common::key(2)).unwrap(),
    ];
    let handover = transfer(&account, &s, &cmd);
    assert_eq!(account.apply_strategy_handover(&handover), Ok(Applied::New));
    let next = account.order(key).unwrap();
    assert_eq!(next.origin, old.origin);
    assert_eq!(next.manager, handover.to);
    assert_eq!(next.owner_revision, old.owner_revision + 1);
    assert_eq!(next.filled_quantity, old.filled_quantity);
    assert_eq!(next.remaining_quantity, old.remaining_quantity);
    assert_eq!(account.asset(common::key(1)).unwrap(), assets[0]);
    assert_eq!(account.asset(common::key(2)).unwrap(), assets[1]);
    let sequence = account.ledger_sequence();
    assert_eq!(
        account.apply_strategy_handover(&handover),
        Ok(Applied::Duplicate)
    );
    assert_eq!(account.ledger_sequence(), sequence);
    let restored = Account::recover(
        s,
        c,
        &[
            RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::StrategyHandover(Box::new(handover)),
        ],
    )
    .unwrap();
    assert_eq!(restored.order(key).unwrap().manager, next.manager);
    assert!(restored.order(key).unwrap().unknown);
    assert_eq!(restored.asset(common::key(2)).unwrap(), assets[1]);
}
#[test]
fn fill_after_capture_rejects_transfer_before_any_manager_changes() {
    let (s, c, cmd) = fixture();
    let mut a = Account::from_snapshot(s.clone(), c).unwrap();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let h = transfer(&a, &s, &cmd);
    a.apply_event(&event(fill(&cmd, "late", "0.0004", "50000", "20")))
        .unwrap();
    let before = a.order(&admission.orders()[0]).unwrap();
    assert_eq!(a.apply_strategy_handover(&h), Err(Error::Stale));
    assert_eq!(a.order(&admission.orders()[0]).unwrap(), before);
}
