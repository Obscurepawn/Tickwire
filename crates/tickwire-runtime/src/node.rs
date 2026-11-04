use crate::authority::RecoveryQueue;
use std::time::{Duration, Instant};
use tickwire_account::Account;
use tickwire_account_gateway::{Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_binance_account::{Outcome as InputOutcome, rest::RestOwner};
use tickwire_binance_clock::{self as clock, BinanceTimeClient, LocalTime, MonotonicClock};
use tickwire_binance_execution::{DeliveryBudget, SharedSessionState};
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_binance_quota as quota;
use tickwire_binance_session::ClockAdapter;
use tickwire_journal::store::Store;
use tickwire_transport::{IoBudget, mio};
mod strategy;
pub use strategy::{AccountHistory, Strategy, Submission, SubmissionResult};

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
/// The intake check schedules a command for account review. The account gateway
/// owns the durable admission and the final authorization at the socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intake {
    Wait,
    Expired,
    DeadlineOverflow,
    Ready(Instant),
}
impl Health {
    pub fn intake(&self, command: &tickwire_venue::Command, now: LocalTime) -> Intake {
        use tickwire_venue::CommandBody::{Cancel, CancelAlgo, CancelList, Query};
        if !self.calibrated || self.sender_halted {
            return Intake::Wait;
        }
        let Some(remaining) = command
            .context
            .request
            .deadline
            .checked_sub(now.monotonic.nanos())
        else {
            return Intake::Expired;
        };
        if remaining == 0 {
            return Intake::Expired;
        }
        let administrative = matches!(
            command.body,
            Cancel(_) | CancelAlgo(_) | CancelList(_) | Query(_)
        );
        if !administrative && !self.ready() {
            return Intake::Wait;
        }
        match now.instant.checked_add(Duration::from_nanos(remaining)) {
            Some(deadline) => Intake::Ready(deadline),
            None => Intake::DeadlineOverflow,
        }
    }

    pub fn ready(&self) -> bool {
        self.calibrated
            && self.market_live
            && self.private_receiving
            && !self.account_frozen
            && !self.sender_halted
    }
}
/// Trusted runtime notifications for the control owner. A Receiving state alone
/// does not acknowledge reconciliation or authorize account activation.
pub trait Observer {
    fn market_observed(&mut self, _observation: crate::market::FeedObservation) -> bool {
        true
    }
    fn market_invalidated(&mut self, _invalidation: crate::market::Invalidation) -> bool {
        true
    }
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
struct MarketObserver<'a>(&'a mut dyn Observer);
impl crate::market::MarketSink for MarketObserver<'_> {
    fn observed(&mut self, observation: crate::market::FeedObservation) -> bool {
        self.0.market_observed(observation)
    }
    fn invalidated(&mut self, invalidation: crate::market::Invalidation) -> bool {
        self.0.market_invalidated(invalidation)
    }
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
    external_market: Option<Box<dyn crate::market::MarketSource + 'a>>,
    external_market_failed: bool,
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
            external_market: None,
            external_market_failed: false,
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
    /// Installs an independently authenticated MD subscription before the first
    /// turn. The local public client remains unstarted and opens no connection.
    pub fn set_market_source(
        &mut self,
        source: Box<dyn crate::market::MarketSource + 'a>,
    ) -> Result<()> {
        if self.next_calibration != 0
            || self.stopped
            || self.external_market.is_some()
            || source.domain() != self.bindings.time.domain
        {
            return Err(Error::Config);
        }
        self.external_market = Some(source);
        Ok(())
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
    /// Reconnect may sample the shared clock/quota itself; do not hold its RefCell borrow here.
    pub fn with_trading_owner<T>(
        &mut self,
        operation: impl FnOnce(&mut RestOwner<IS, AS, A, E>) -> T,
    ) -> T {
        operation(&mut self.rest)
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
    pub fn with_strategy_and_account_resources<T>(
        &mut self,
        operation: impl FnOnce(
            &mut S,
            &mut RestOwner<IS, AS, A, E>,
            &mut BinanceTimeClient<TC>,
            &mut quota::Endpoint,
        ) -> T,
    ) -> Result<T> {
        let strategy = &mut self.strategy;
        let rest = &mut self.rest;
        self.session
            .with_resources(|time, quota| operation(strategy, rest, time, quota))
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
        let io = IoBudget {
            max_syscalls: self.config.io.max_syscalls / 4,
            max_bytes: self.config.io.max_bytes / 4,
        };
        self.poll_clock(reading.local.monotonic.nanos(), io)?;
        self.poll_market(reading.local, io, &mut report)?;
        self.poll_private(io, &mut report)?;
        self.poll_account(io, &mut report)?;
        self.poll_strategies(&mut report)?;
        report.next_deadline_ns = Some(
            report
                .next_deadline_ns
                .map_or(self.next_calibration, |d| d.min(self.next_calibration)),
        );
        Ok(report)
    }
    fn poll_clock(&mut self, now: u64, io: IoBudget) -> Result<()> {
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
        Ok(())
    }
    fn poll_market(&mut self, now: LocalTime, io: IoBudget, report: &mut Report) -> Result<()> {
        if let Some(market) = &mut self.external_market {
            if !self.external_market_failed {
                let mut sink = MarketObserver(self.observer.as_mut());
                match market.poll(
                    now.monotonic,
                    self.config.maximum_book_age_ns,
                    self.config.public.max_notifications,
                    &mut sink,
                ) {
                    Ok(md) => {
                        report.market_messages = md.records;
                        report.runnable |= md.runnable;
                        report.next_deadline_ns = md.next_deadline_ns;
                    }
                    Err(_) => {
                        // A market failure closes its dependent risk gates while
                        // the account continues processing facts and cancellations.
                        self.external_market_failed = true;
                        market.stop(&mut sink).map_err(|_| Error::Strategy)?;
                    }
                }
            }
        } else {
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
        }
        Ok(())
    }
    fn poll_private(&mut self, io: IoBudget, report: &mut Report) -> Result<()> {
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
        Ok(())
    }
    fn poll_account(&mut self, io: IoBudget, report: &mut Report) -> Result<()> {
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
        Ok(())
    }
    fn poll_strategies(&mut self, report: &mut Report) -> Result<()> {
        // Polling the account sender may run an authorization callback that
        // samples a newer trusted time. Health must follow that callback with
        // a fresh sample instead of presenting the pre-poll time as current.
        let fresh = self.time()?;
        report.health = Health {
            calibrated: fresh.signed.is_some(),
            market_live: if let Some(market) = &self.external_market {
                !self.external_market_failed
                    && market.is_live(fresh.local.monotonic, self.config.maximum_book_age_ns)
            } else {
                self.market.view(self.config.maximum_book_age_ns).is_ok()
            },
            private_receiving: self.private.status().phase == private::Phase::Receiving,
            account_frozen: self.rest.account().frozen(),
            sender_halted: self.rest.gateway().halted(),
        };
        self.strategy.on_health(report.health, fresh.local);
        if !self.observer.health(report.health, fresh.local) {
            return Err(Error::Strategy);
        }
        for _ in 0..self.config.maximum_commands_per_turn {
            if !self.rest.submission_ready() {
                break;
            }
            // Prior submissions may synchronize recovery data and sample the
            // policy clock. Each new callback starts with a current sample.
            let fresh = self.time()?;
            let Some(submission) =
                self.strategy
                    .next(self.rest.account(), report.health, fresh.local)
            else {
                break;
            };
            let digest = submission.command.digest();
            if digest != submission.recovery.command_digest {
                return Err(Error::Config);
            }
            self.recovery
                .stage(submission.recovery)
                .map_err(Error::Recovery)?;
            let submitted_at = self.time()?;
            let result = self.rest.submit(
                submission.command.clone(),
                &submission.order_ids,
                submitted_at.local.monotonic.nanos(),
                submission.deadline,
            );
            self.strategy.on_submission(
                &submission.command,
                &result
                    .as_ref()
                    .copied()
                    .map_err(|e| e as &(dyn std::error::Error + 'static)),
            );
            // Any successful prepare persisted the complete closure before Admit.
            // A denied submit leaves no new send capability, so the staged copy is disposable.
            self.recovery.remove(digest).map_err(Error::Recovery)?;
            report.submissions += 1;
        }
        Ok(())
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
        if let Some(market) = &mut self.external_market {
            let _ = market.stop(&mut MarketObserver(self.observer.as_mut()));
        } else {
            let mut sink = Fanout {
                strategy: &mut self.strategy,
                output: &mut self.output,
            };
            let _ = self.market.stop(&mut sink);
        }
    }
    fn registrations(&mut self, registry: &mio::Registry) -> Result<()> {
        if self.external_market.is_none() {
            self.market
                .reregister(
                    registry,
                    public::IoTokens {
                        stream: mio::Token(1),
                        snapshot: mio::Token(2),
                    },
                )
                .map_err(Error::Market)?;
        }
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
