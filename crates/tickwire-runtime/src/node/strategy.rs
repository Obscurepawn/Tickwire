//! Account-side interface to strategy workers.
use super::Health;
use crate::authority::RecoveryBundle;
use std::time::Instant;
use tickwire_account::Account;
use tickwire_account_gateway::{Authority, Encoder, Status as SubmitStatus};
use tickwire_account_journal::Backend;
use tickwire_binance_account::{Outcome as InputOutcome, rest::RestOutcome};
use tickwire_binance_clock::LocalTime;
use tickwire_binance_public as public;
use tickwire_venue::CanonicalCommand;

/// A gateway-local acceptance status or a borrowed connector failure. A strategy callback
/// never infers a venue order state from either outcome.
pub type SubmissionResult<'a> =
    std::result::Result<SubmitStatus, &'a (dyn std::error::Error + 'static)>;
pub struct Submission {
    pub command: CanonicalCommand,
    pub order_ids: Vec<[u8; 16]>,
    pub deadline: Instant,
    pub recovery: RecoveryBundle,
}
/// Immutable durable account history. Each strategy owns its delivery cursor.
pub trait AccountHistory {
    fn baseline(&self) -> &tickwire_account_codec::CanonicalBaseline;
    fn account(&self) -> &Account;
    fn position(&self) -> u64;
    fn record(&self, position: u64) -> Option<&tickwire_account_codec::CanonicalRecord>;
}
impl<B: Backend, A: Authority, E: Encoder> AccountHistory
    for tickwire_account_gateway::AccountGateway<B, A, E>
{
    fn baseline(&self) -> &tickwire_account_codec::CanonicalBaseline {
        self.baseline()
    }
    fn account(&self) -> &Account {
        self.account()
    }
    fn position(&self) -> u64 {
        self.journal_position()
    }
    fn record(&self, position: u64) -> Option<&tickwire_account_codec::CanonicalRecord> {
        self.journal_records_after(position.checked_sub(1)?, 1)
            .ok()?
            .next()
            .map(|(_, record)| record)
    }
}
/// The runtime polls this owner for strategy commands and delivers account history and results.
/// Hosted workers and file queues implement the same account-side interface.
pub trait Strategy {
    fn on_account_history(&mut self, history: &dyn AccountHistory) {
        if !self.on_account_baseline(history.baseline(), history.account()) {
            return;
        }
        let Some(mut position) = self.account_report_position() else {
            return;
        };
        for _ in 0..32 {
            let Some(next) = position.checked_add(1) else {
                return;
            };
            let Some(record) = history.record(next) else {
                return;
            };
            if !self.on_account_record(next, record, history.baseline(), history.account()) {
                return;
            }
            position = next;
        }
    }
    fn on_market(&mut self, notice: &public::Notice<'_>) -> bool;
    fn next(&mut self, account: &Account, health: Health, now: LocalTime) -> Option<Submission>;
    fn on_submission(&mut self, command: &CanonicalCommand, result: &SubmissionResult<'_>);
    /// Synchronous venue reply for this request. Durable account records carry economic effects.
    fn on_request_reply(
        &mut self,
        _command: &CanonicalCommand,
        _reply: &tickwire_venue::RequestReply,
    ) {
    }
    fn on_account_baseline(
        &mut self,
        _baseline: &tickwire_account_codec::CanonicalBaseline,
        _account: &Account,
    ) -> bool {
        true
    }
    fn account_report_position(&self) -> Option<u64> {
        None
    }
    fn on_account_record(
        &mut self,
        _position: u64,
        _record: &tickwire_account_codec::CanonicalRecord,
        _baseline: &tickwire_account_codec::CanonicalBaseline,
        _account: &Account,
    ) -> bool {
        true
    }
    fn on_private(&mut self, _outcome: InputOutcome, _account: &Account) {}
    fn on_rest(&mut self, _outcome: RestOutcome, _account: &Account) {}
    fn on_health(&mut self, _health: Health, _now: LocalTime) {}
}
impl<T: Strategy + ?Sized> Strategy for Box<T> {
    fn on_account_history(&mut self, history: &dyn AccountHistory) {
        (**self).on_account_history(history)
    }
    fn on_market(&mut self, notice: &public::Notice<'_>) -> bool {
        (**self).on_market(notice)
    }
    fn next(&mut self, account: &Account, health: Health, now: LocalTime) -> Option<Submission> {
        (**self).next(account, health, now)
    }
    fn on_submission(&mut self, command: &CanonicalCommand, result: &SubmissionResult<'_>) {
        (**self).on_submission(command, result)
    }
    fn on_request_reply(
        &mut self,
        command: &CanonicalCommand,
        reply: &tickwire_venue::RequestReply,
    ) {
        (**self).on_request_reply(command, reply)
    }
    fn on_account_baseline(
        &mut self,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        account: &Account,
    ) -> bool {
        (**self).on_account_baseline(baseline, account)
    }
    fn account_report_position(&self) -> Option<u64> {
        (**self).account_report_position()
    }
    fn on_account_record(
        &mut self,
        position: u64,
        record: &tickwire_account_codec::CanonicalRecord,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        account: &Account,
    ) -> bool {
        (**self).on_account_record(position, record, baseline, account)
    }
    fn on_private(&mut self, outcome: InputOutcome, account: &Account) {
        (**self).on_private(outcome, account)
    }
    fn on_rest(&mut self, outcome: RestOutcome, account: &Account) {
        (**self).on_rest(outcome, account)
    }
    fn on_health(&mut self, health: Health, now: LocalTime) {
        (**self).on_health(health, now)
    }
}
