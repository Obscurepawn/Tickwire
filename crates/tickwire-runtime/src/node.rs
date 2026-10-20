use crate::authority::{RecoveryBundle, RecoveryQueue};
use std::time::{Duration, Instant};
use tickwire_account::Account;
use tickwire_account_gateway::{Authority, Encoder, Status as SubmitStatus};
use tickwire_account_journal::Backend;
use tickwire_binance_account::{
    Outcome as InputOutcome,
    rest::{RestOutcome, RestOwner},
};
use tickwire_binance_clock::{self as clock, BinanceTimeClient, LocalTime, MonotonicClock};
use tickwire_binance_execution::{DeliveryBudget, SharedSessionState};
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_binance_quota as quota;
use tickwire_binance_session::ClockAdapter;
use tickwire_journal::store::Store;
use tickwire_transport::{IoBudget, mio};
use tickwire_venue::CanonicalCommand;

#[derive(Debug)]
pub enum Error {
    Config,
    Clock(clock::Error),
    Shared(tickwire_binance_execution::Error),
    Market(public::Error),
    Private(private::Error),
    Account(tickwire_binance_account::Error),
    Recovery(tickwire_account_gateway::AuthorityError),
    Io(std::io::Error),
    Strategy,
    Stopped,
}
pub type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Copy, Debug, Default)]
pub struct Health {
    pub calibrated: bool,
    pub market_live: bool,
    pub private_receiving: bool,
    pub account_frozen: bool,
    pub sender_halted: bool,
}
impl Health {
    pub fn ready(&self) -> bool {
        self.calibrated
            && self.market_live
            && self.private_receiving
            && !self.account_frozen
            && !self.sender_halted
    }
}
pub struct Submission {
    pub command: CanonicalCommand,
    pub order_ids: Vec<[u8; 16]>,
    pub deadline: Instant,
    pub recovery: RecoveryBundle,
}
/// Immutable durable history; each strategy owns its independent delivery cursor.
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
/// The same interface can be implemented by an in-process strategy or an
/// authenticated strategy-IPC consumer. It never receives mutable account state.
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
    fn on_submission(
        &mut self,
        command: &CanonicalCommand,
        result: &std::result::Result<SubmitStatus, tickwire_binance_account::Error>,
    );
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
    fn on_submission(
        &mut self,
        command: &CanonicalCommand,
        result: &std::result::Result<SubmitStatus, tickwire_binance_account::Error>,
    ) {
        (**self).on_submission(command, result)
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
/// Trusted runtime notifications for the control owner. A Receiving state alone
/// does not acknowledge reconciliation or authorize account activation.
pub trait Observer {
    /// The continuous, fully processed input-WAL prefix, including non-event
    /// records and REST child effects. Notifications may advance by a batch.
    fn input_applied(
        &mut self,
        _input: tickwire_binance_account::InputKey,
        _account: &Account,
    ) -> bool {
        true
    }
    fn private_state(&mut self, _state: private::SessionStatus) -> bool {
        true
    }
    fn private_gap(&mut self, _gap: &private::GapNotice) -> bool {
        true
    }
    fn private_source(&mut self, _gap: &private::GapToken) -> bool {
        true
    }
    fn private_applied(
        &mut self,
        _connection_epoch: u64,
        _outcome: InputOutcome,
        _account: &Account,
    ) -> bool {
        true
    }
    fn health(&mut self, _health: Health, _now: LocalTime) -> bool {
        true
    }
}
pub struct NoObserver;
impl Observer for NoObserver {}
/// An observer is optional when the strategy executes in this process.
pub struct NoPublisher;
impl public::PublicSink for NoPublisher {
    fn on_notice(&mut self, _: public::Notice<'_>) -> bool {
        true
    }
}
struct Fanout<'a, S, O> {
    strategy: &'a mut S,
    output: &'a mut O,
}
impl<S: Strategy, O: public::PublicSink> public::PublicSink for Fanout<'_, S, O> {
    fn on_notice(&mut self, n: public::Notice<'_>) -> bool {
        self.strategy.on_market(&n) && self.output.on_notice(n)
    }
}
#[derive(Clone)]
pub struct Bindings {
    pub time: quota::TimeBinding,
    pub private: quota::PrivateBinding,
    pub public: quota::PublicBinding,
}
#[derive(Clone, Copy)]
pub struct Config {
    pub calibration_interval_ns: u64,
    pub calibration_retry_ns: u64,
    pub maximum_book_age_ns: u64,
    pub private_delivery_budget_ns: u64,
    /// Total I/O capacity is split equally across the four network owners.
    pub io: IoBudget,
    pub public: public::PollBudget,
    pub private: private::PollBudget,
    pub maximum_commands_per_turn: usize,
    /// Zero enables a busy polling loop; positive values cap reactor sleep.
    pub maximum_idle_wait_ns: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Report {
    pub health: Health,
    pub market_messages: usize,
    pub private_messages: usize,
    pub submissions: usize,
    pub rest_completed: bool,
    pub runnable: bool,
    pub next_deadline_ns: Option<u64>,
}
pub struct CoreNode<
    'a,
    P,
    MC,
    TC,
    DB,
    IS,
    AS,
    A,
    E,
    S,
    O,
    const L: usize,
    const U: usize,
    const B: usize,
> where
    P: public::DepthPolicy,
    MC: MonotonicClock,
    TC: MonotonicClock,
    DB: DeliveryBudget,
    IS: Store,
    AS: Backend,
    A: Authority,
    E: Encoder,
    S: Strategy,
    O: public::PublicSink,
{
    market: public::PublicDepthClient<P, MC, L, U, B>,
    private: private::PrivateClient<'a>,
    rest: RestOwner<IS, AS, A, E>,
    session: SharedSessionState<BinanceTimeClient<TC>, DB>,
    recovery: RecoveryQueue,
    strategy: S,
    output: O,
    observer: Box<dyn Observer + 'a>,
    config: Config,
    bindings: Bindings,
    next_calibration: u64,
    registered_time: Option<u64>,
    registered_rest: bool,
    stopped: bool,
}
impl<'a, P, MC, TC, DB, IS, AS, A, E, S, O, const L: usize, const U: usize, const B: usize>
    CoreNode<'a, P, MC, TC, DB, IS, AS, A, E, S, O, L, U, B>
where
    P: public::DepthPolicy,
    MC: MonotonicClock,
    TC: MonotonicClock,
    DB: DeliveryBudget,
    IS: Store,
    AS: Backend,
    A: Authority,
    E: Encoder,
    S: Strategy,
    O: public::PublicSink,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        market: public::PublicDepthClient<P, MC, L, U, B>,
        private: private::PrivateClient<'a>,
        rest: RestOwner<IS, AS, A, E>,
        session: SharedSessionState<BinanceTimeClient<TC>, DB>,
        recovery: RecoveryQueue,
        strategy: S,
        output: O,
        observer: Box<dyn Observer + 'a>,
        config: Config,
        bindings: Bindings,
    ) -> Result<Self> {
        if config.calibration_interval_ns == 0
            || config.calibration_retry_ns == 0
            || config.maximum_book_age_ns == 0
            || config.private_delivery_budget_ns == 0
            || config.io.max_syscalls < 8
            || config.io.max_syscalls > 1024
            || config.io.max_bytes < 4
            || config.io.max_bytes > 16 * 1024 * 1024
            || config.maximum_commands_per_turn == 0
            || config.maximum_commands_per_turn > 32
            || config.maximum_idle_wait_ns > 1_000_000_000
            || bindings.time.domain != bindings.private.domain
            || bindings.time.domain != bindings.public.domain
            || bindings.time.source != bindings.public.source
            || private.current_gap().scope() != &bindings.private.scope
        {
            return Err(Error::Config);
        }
        let bound = session
            .with_resources(|t, _| {
                t.domain() == bindings.time.domain && t.source() == bindings.time.source
            })
            .map_err(Error::Shared)?;
        if !bound {
            return Err(Error::Config);
        }
        Ok(Self {
            market,
            private,
            rest,
            session,
            recovery,
            strategy,
            output,
            observer,
            config,
            bindings,
            next_calibration: 0,
            registered_time: None,
            registered_rest: false,
            stopped: false,
        })
    }
    fn time(&self) -> Result<clock::ClockReading> {
        self.session
            .with_resources(|t, _| t.sample())
            .map_err(Error::Shared)?
            .map_err(Error::Clock)
    }
    pub fn account(&self) -> &Account {
        self.rest.account()
    }
    pub fn strategy(&self) -> &S {
        &self.strategy
    }
    pub fn strategy_mut(&mut self) -> &mut S {
        &mut self.strategy
    }
    pub fn rest(&self) -> &RestOwner<IS, AS, A, E> {
        &self.rest
    }
    pub fn session(&self) -> &SharedSessionState<BinanceTimeClient<TC>, DB> {
        &self.session
    }
    /// Supplies the actual account, calibrated clock and shared limiter to a
    /// reconciliation owner without creating a second mutable session owner.
    pub fn with_account_resources<T>(
        &mut self,
        operation: impl FnOnce(
            &mut RestOwner<IS, AS, A, E>,
            &mut BinanceTimeClient<TC>,
            &mut quota::Endpoint,
        ) -> T,
    ) -> Result<T> {
        let rest = &mut self.rest;
        self.session
            .with_resources(|time, quota| operation(rest, time, quota))
            .map_err(Error::Shared)
    }
    pub fn local_time(&self) -> Result<LocalTime> {
        self.time().map(|reading| reading.local)
    }
    pub fn private_status(&self) -> private::SessionStatus {
        self.private.status()
    }
    pub fn private_gap(&self) -> &private::GapToken {
        self.private.current_gap()
    }
    /// Call after the account owner installs a fresh REST connection.
    pub fn connection_replaced(&mut self) {
        self.registered_rest = false;
    }
    pub fn replay_pending(&mut self, maximum: usize) -> Result<usize> {
        self.rest.replay_pending(maximum).map_err(Error::Account)
    }
    fn step_inner(&mut self) -> Result<Report> {
        let mut report = Report::default();
        let reading = self.time()?;
        let now = reading.local.monotonic.nanos();
        let io = IoBudget {
            max_syscalls: self.config.io.max_syscalls / 4,
            max_bytes: self.config.io.max_bytes / 4,
        };
        let binding = self.bindings.time;
        let interval = self.config.calibration_interval_ns;
        let retry = self.config.calibration_retry_ns;
        let next = &mut self.next_calibration;
        self.session
            .with_resources(|time, quota| -> Result<()> {
                let mut permit = quota.time(binding);
                if time.in_flight().is_none() && now >= *next {
                    *next = now.checked_add(retry).ok_or(Error::Config)?;
                    match time.start(&mut permit) {
                        Ok(_) => {}
                        Err(clock::Error::Denied | clock::Error::Transport) => return Ok(()),
                        Err(e) => return Err(Error::Clock(e)),
                    }
                }
                if time.in_flight().is_some() {
                    let polled = time.poll(&mut permit, io).map_err(Error::Clock)?;
                    if let Some(outcome) = polled.outcome {
                        *next = now
                            .checked_add(if outcome.calibration.is_ok() {
                                interval
                            } else {
                                retry
                            })
                            .ok_or(Error::Config)?;
                    }
                }
                Ok(())
            })
            .map_err(Error::Shared)??;
        let binding = self.bindings.public;
        let market = &mut self.market;
        let mut sink = Fanout {
            strategy: &mut self.strategy,
            output: &mut self.output,
        };
        let mut budget = self.config.public;
        budget.io = io;
        let md = self
            .session
            .with_resources(|_, quota| -> Result<public::PollReport> {
                let mut permit = quota.public(binding);
                if market.status().phase == public::Phase::Idle {
                    match market.start(&mut permit) {
                        Ok(()) | Err(public::Error::Denied | public::Error::Transport) => {}
                        Err(e) => return Err(Error::Market(e)),
                    }
                }
                market
                    .poll(&mut permit, budget, &mut sink)
                    .map_err(Error::Market)
            })
            .map_err(Error::Shared)??;
        report.market_messages = md.decoded;
        report.runnable |= md.runnable;
        report.next_deadline_ns = md.next_deadline_ns;
        let binding = self.bindings.private.clone();
        let scope = binding.scope.clone();
        let delivery_ns = self.config.private_delivery_budget_ns;
        let mut delivery =
            move |_: &private::AccountScope, _: &private::SignedTime, _: &private::ClockSample| {
                Ok(delivery_ns)
            };
        let mut budget = self.config.private;
        budget.io = io;
        let private = &mut self.private;
        let rest = &mut self.rest;
        let strategy = &mut self.strategy;
        let observer = &mut self.observer;
        let mut input_error = None;
        let pd = self
            .session
            .with_resources(|time, quota| -> Result<private::PollReport> {
                let mut adapter =
                    ClockAdapter::new(time, scope, &mut delivery).map_err(Error::Private)?;
                let mut permit = quota.private(binding);
                private
                    .poll(&mut adapter, budget, &mut permit, |notice| {
                        let result = match notice {
                            private::Delivery::Event(event) => {
                                let epoch = event.session().identity().connection_epoch;
                                rest.ingest_private(&event).and_then(|outcome| {
                                    if rest.latest_applied_input().is_some_and(|input| {
                                        !observer.input_applied(input, rest.account())
                                    }) {
                                        return Err(tickwire_binance_account::Error::Identity);
                                    }
                                    if !observer.private_applied(epoch, outcome, rest.account()) {
                                        return Err(tickwire_binance_account::Error::Identity);
                                    }
                                    strategy.on_private(outcome, rest.account());
                                    Ok(())
                                })
                            }
                            private::Delivery::Gap(gap) => {
                                if !observer.private_gap(&gap) {
                                    return Err(private::Error::Sink);
                                }
                                rest.source_gap(&gap).map(|_| ())
                            }
                            private::Delivery::State(state) => {
                                if !observer.private_state(state) {
                                    return Err(private::Error::Sink);
                                }
                                Ok(())
                            }
                            private::Delivery::Control(_) => Ok(()),
                        };
                        if let Err(error) = result {
                            input_error = Some(error);
                            return Err(private::Error::Sink);
                        }
                        Ok(())
                    })
                    .map_err(Error::Private)
            })
            .map_err(Error::Shared)?;
        if let Some(error) = input_error {
            return Err(Error::Account(error));
        }
        let pd = pd?;
        if !self.observer.private_source(self.private.current_gap()) {
            return Err(Error::Private(private::Error::Sink));
        }
        report.private_messages = pd.messages;
        report.runnable |= pd.yielded;
        let fresh = self.time()?;
        if let Some(outcome) = self
            .rest
            .poll(fresh.local.instant, fresh.local.monotonic.nanos(), io)
            .map_err(Error::Account)?
        {
            report.rest_completed = true;
            if self
                .rest
                .latest_applied_input()
                .is_some_and(|input| !self.observer.input_applied(input, self.rest.account()))
            {
                return Err(Error::Strategy);
            }
            self.strategy.on_rest(outcome, self.rest.account());
        }
        if self
            .rest
            .latest_applied_input()
            .is_some_and(|input| !self.observer.input_applied(input, self.rest.account()))
        {
            return Err(Error::Strategy);
        }
        self.strategy.on_account_history(self.rest.gateway());
        report.health = Health {
            calibrated: fresh.signed.is_some(),
            market_live: self.market.view(self.config.maximum_book_age_ns).is_ok(),
            private_receiving: self.private.status().phase == private::Phase::Receiving,
            account_frozen: self.rest.account().frozen(),
            sender_halted: self.rest.gateway().halted(),
        };
        self.strategy.on_health(report.health, fresh.local);
        if !self.observer.health(report.health, fresh.local) {
            return Err(Error::Strategy);
        }
        for _ in 0..self.config.maximum_commands_per_turn {
            if self.rest.busy() {
                break;
            }
            let Some(submission) =
                self.strategy
                    .next(self.rest.account(), report.health, fresh.local)
            else {
                break;
            };
            if submission.command.digest() != submission.recovery.command_digest {
                return Err(Error::Config);
            }
            let digest = submission.command.digest();
            self.recovery
                .stage(submission.recovery)
                .map_err(Error::Recovery)?;
            let result = self.rest.submit(
                submission.command.clone(),
                &submission.order_ids,
                fresh.local.monotonic.nanos(),
                submission.deadline,
            );
            self.strategy.on_submission(&submission.command, &result);
            // Any successful prepare persisted the complete closure before Admit.
            // A denied submit leaves no new send capability, so the staged copy is disposable.
            self.recovery.remove(digest).map_err(Error::Recovery)?;
            report.submissions += 1;
        }
        report.next_deadline_ns = Some(
            report
                .next_deadline_ns
                .map_or(self.next_calibration, |d| d.min(self.next_calibration)),
        );
        Ok(report)
    }
    pub fn step(&mut self) -> Result<Report> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        let result = self.step_inner();
        if result.is_err() {
            self.stop();
        }
        result
    }
    pub fn stop(&mut self) {
        self.stopped = true;
        self.rest.stop_sending();
        let _ = self.private.stop(private::StopMode::Disconnect);
        let mut sink = Fanout {
            strategy: &mut self.strategy,
            output: &mut self.output,
        };
        let _ = self.market.stop(&mut sink);
    }
    fn registrations(&mut self, registry: &mio::Registry) -> Result<()> {
        self.market
            .reregister(
                registry,
                public::IoTokens {
                    stream: mio::Token(1),
                    snapshot: mio::Token(2),
                },
            )
            .map_err(Error::Market)?;
        self.private
            .reregister(
                registry,
                private::RegistrationTokens {
                    stream: mio::Token(3),
                    management: mio::Token(4),
                },
            )
            .map_err(Error::Private)?;
        let registered = &mut self.registered_time;
        self.session
            .with_resources(|t, _| -> Result<()> {
                if let Some(progress) = t.in_flight() {
                    if *registered == Some(progress.connection_epoch) {
                        t.reregister(registry, mio::Token(5))
                            .map_err(Error::Clock)?;
                    } else {
                        t.register(registry, mio::Token(5)).map_err(Error::Clock)?;
                        *registered = Some(progress.connection_epoch);
                    }
                }
                Ok(())
            })
            .map_err(Error::Shared)??;
        if !self.rest.gateway().halted() {
            if self.registered_rest {
                self.rest.reregister(registry, mio::Token(6))
            } else {
                self.rest.register(registry, mio::Token(6))
            }
            .map_err(Error::Account)?;
            self.registered_rest = true;
        }
        Ok(())
    }
    /// Runs the actual network owners on one event-loop thread. The caller controls
    /// shutdown and can stop after a decision, a duration, or a supervisor signal.
    pub fn run(&mut self, mut keep_running: impl FnMut(&Self, &Report) -> bool) -> Result<()> {
        self.run_with_maintenance(|_, _| Ok(()), |node, report| keep_running(node, report))
    }
    /// Maintenance runs after inputs have been persisted and before the reactor
    /// sleeps. It may drive read-only reconciliation and register its connection
    /// with token values 7 and above.
    pub fn run_with_maintenance(
        &mut self,
        mut maintenance: impl FnMut(&mut Self, &mio::Registry) -> Result<()>,
        mut keep_running: impl FnMut(&Self, &Report) -> bool,
    ) -> Result<()> {
        let mut reactor = mio::Poll::new().map_err(Error::Io)?;
        let mut events = mio::Events::with_capacity(16);
        loop {
            let report = self.step()?;
            if let Err(error) = maintenance(self, reactor.registry()) {
                self.stop();
                return Err(error);
            }
            if !keep_running(self, &report) {
                self.stop();
                return Ok(());
            }
            self.registrations(reactor.registry())?;
            let now = self.time()?.local;
            let mut wait = self.config.maximum_idle_wait_ns;
            if report.runnable {
                wait = 0;
            }
            if let Some(deadline) = report.next_deadline_ns {
                wait = wait.min(deadline.saturating_sub(now.monotonic.nanos()));
            }
            if let Some(deadline) = self.private.next_deadline() {
                wait = wait.min(
                    deadline
                        .saturating_duration_since(now.instant)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                );
            }
            reactor
                .poll(&mut events, Some(Duration::from_nanos(wait)))
                .map_err(Error::Io)?;
        }
    }
}
