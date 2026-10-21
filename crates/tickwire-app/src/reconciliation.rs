use serde::Deserialize;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_binance::request::Credentials;
use tickwire_binance_account::reconciliation::{
    ReadOnlyReconciler, ReconcileConfig, TradeQuery,
    amendments::{AmendmentQuery, ReadOnlyAmendmentReader},
    bootstrap::{AccountSnapshotReader, SnapshotEvidence, SnapshotKind, SnapshotQuery},
    orders::{OrderQuery, ReadOnlyOrderReader},
    structures::{ReadOnlyStructureReader, StructureQuery},
};
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_binance_quota::ReadBinding;
use tickwire_binance_session::ClockAdapter;
use tickwire_clock::MonoTime;
use tickwire_runtime::{
    authority::GateProvider,
    files::FileNode,
    node::{self, Strategy},
};
use tickwire_runtime_policy::{ControlPlane, ReconciliationRequest};
use tickwire_transport::{IoBudget, Limits, mio, rustls};
pub mod association;
mod files;
pub mod income;
#[cfg(test)]
mod income_network_tests;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub reader_id: [u8; 16],
    pub first_request_epoch: u64,
    pub first_execution_connection_epoch: u64,
    pub retry_ns: u64,
    pub valid_for_ns: u64,
    #[serde(default = "default_fee_maintenance_interval_ns")]
    pub fee_maintenance_interval_ns: u64,
    #[serde(default)]
    pub income_baseline_import: Option<income::Config>,
    #[serde(default)]
    pub income_account_scan: Option<income::AccountConfig>,
    #[serde(default)]
    pub income_association: Option<association::Config>,
}

fn default_fee_maintenance_interval_ns() -> u64 {
    1_000_000_000
}

#[derive(Default)]
struct OrderWork {
    orders: VecDeque<OrderQuery>,
    structures: VecDeque<StructureQuery>,
    amendments: VecDeque<AmendmentQuery>,
    loaded: bool,
    observed: bool,
    next_trade: u64,
    maintenance_at: u64,
}
impl OrderWork {
    fn reset(&mut self) {
        self.orders.clear();
        self.structures.clear();
        self.amendments.clear();
        self.loaded = false;
        self.observed = false;
        self.next_trade = 0;
    }
    fn load(&mut self, orders: impl Iterator<Item = OrderQuery>) {
        if !self.loaded {
            self.orders = orders.collect();
            self.loaded = true;
        }
    }
    fn completed(&mut self, now: u64, interval: u64) {
        self.reset();
        self.maintenance_at = now.saturating_add(interval);
    }
    fn should_start(
        &self,
        healthy: bool,
        sender_busy: bool,
        now: u64,
        terminal_fees_pending: bool,
        valid_until: u64,
        valid_for: u64,
    ) -> bool {
        // The durable account deadline is authoritative; local polling, retries
        // and busy senders cannot move it. Fee throttling must not defer renewal.
        let renewal_due = now >= valid_until.saturating_sub(valid_for / 2);
        !healthy
            || (!sender_busy
                && (renewal_due || (now >= self.maintenance_at && terminal_fees_pending)))
    }
}
pub struct Controller<'a> {
    reader: AccountSnapshotReader,
    order_reader: ReadOnlyOrderReader,
    amendment_reader: ReadOnlyAmendmentReader,
    structure_reader: ReadOnlyStructureReader,
    trades: ReadOnlyReconciler,
    income: Option<income::Work>,
    association: Option<association::Work>,
    work: OrderWork,
    config: Config,
    binding: ReadBinding,
    plane: ControlPlane,
    credentials: Credentials<'a>,
    scope: private::AccountScope,
    snapshots: Vec<SnapshotEvidence>,
    cut: Option<(u64, u64)>,
    registered: Option<(u8, u64)>,
    retry_at: u64,
    delivery_ns: u64,
    writer_epoch: u64,
    execution_epoch: u64,
    address: SocketAddr,
    tls: Arc<rustls::ClientConfig>,
    limits: Limits,
    connect_ns: u64,
}
impl<'a> Controller<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        reader_config: ReconcileConfig,
        binding: ReadBinding,
        plane: ControlPlane,
        credentials: Credentials<'a>,
        writer_epoch: u64,
        delivery_ns: u64,
        connect_ns: u64,
    ) -> crate::Result<Self> {
        if config.reader_id == [0; 16]
            || binding.owner_id != config.reader_id
            || config.retry_ns == 0
            || config.valid_for_ns == 0
            || config.fee_maintenance_interval_ns == 0
            || config.first_execution_connection_epoch == 0
            || reader_config.first_connection_epoch != config.first_request_epoch
            || writer_epoch == 0
            || delivery_ns == 0
            || connect_ns == 0
        {
            return Err(crate::invalid("reconciliation configuration"));
        }
        let scope = reader_config.scope.clone();
        let address = reader_config.address;
        let tls = reader_config.tls.clone();
        let limits = reader_config.limits;
        let order_reader = ReadOnlyOrderReader::new(reader_config.clone())?;
        let amendment_reader = ReadOnlyAmendmentReader::new(reader_config.clone())?;
        let structure_reader = ReadOnlyStructureReader::new(reader_config.clone())?;
        let trades = ReadOnlyReconciler::new(reader_config.clone())?;
        let income =
            if config.income_baseline_import.is_some() || config.income_account_scan.is_some() {
                Some(income::Work::new(
                    config.income_baseline_import.clone(),
                    config.income_account_scan.clone(),
                    reader_config.clone(),
                )?)
            } else {
                None
            };
        let association = config
            .income_association
            .clone()
            .map(association::Work::new)
            .transpose()?;
        let reader = AccountSnapshotReader::new(reader_config)?;
        Ok(Self {
            reader,
            order_reader,
            amendment_reader,
            structure_reader,
            trades,
            income,
            association,
            work: OrderWork::default(),
            execution_epoch: config.first_execution_connection_epoch,
            config,
            binding,
            plane,
            credentials,
            scope,
            snapshots: Vec::with_capacity(3),
            cut: None,
            registered: None,
            retry_at: 0,
            delivery_ns,
            writer_epoch,
            address,
            tls,
            limits,
            connect_ns,
        })
    }
    /// Only the execution reconnect target changes; all read-only REST readers
    /// retain the original authenticated REST endpoint.
    pub fn with_execution_network(
        mut self,
        network: &tickwire_account_gateway::ConnectionConfig,
    ) -> Self {
        self.address = network.address;
        self.tls = network.tls.clone();
        self.limits = network.limits;
        self
    }
    fn retry(&mut self, now: u64, message: &str) {
        self.reader.cancel();
        self.order_reader.cancel();
        self.amendment_reader.cancel();
        self.structure_reader.cancel();
        self.trades.cancel();
        if let Some(income) = &mut self.income {
            income.reset();
        }
        if let Some(association) = &mut self.association {
            association.reset();
        }
        self.work.reset();
        self.snapshots.clear();
        self.registered = None;
        self.retry_at = now.saturating_add(self.config.retry_ns);
        eprintln!(
            "{}",
            serde_json::json!({"event":"reconciliation_wait","reason":message})
        );
    }
    pub fn drive<P, G, S, O, const L: usize, const U: usize, const B: usize>(
        &mut self,
        node: &mut FileNode<'_, P, G, S, O, L, U, B>,
        registry: &mio::Registry,
    ) -> node::Result<()>
    where
        P: public::DepthPolicy,
        G: GateProvider,
        S: Strategy,
        O: public::PublicSink,
    {
        if !self.config.enabled {
            return Ok(());
        }
        let state = node.private_status();
        let gap = node.private_gap().clone();
        let cut = (state.connection_epoch, gap.generation());
        if self.cut != Some(cut) {
            self.reader.cancel();
            self.order_reader.cancel();
            self.amendment_reader.cancel();
            self.structure_reader.cancel();
            self.trades.cancel();
            if let Some(income) = &mut self.income {
                income.reset();
            }
            if let Some(association) = &mut self.association {
                association.reset();
            }
            self.work.reset();
            self.snapshots.clear();
            self.registered = None;
            self.retry_at = 0;
            self.cut = Some(cut);
        }
        if state.phase != private::Phase::Receiving {
            return Ok(());
        }
        let now = node.local_time()?;
        let frontier = self
            .plane
            .private_frontier()
            .map_err(|_| node::Error::Strategy)?;
        let healthy =
            frontier.reconciled && !node.account().frozen() && !node.rest().gateway().halted();
        let terminal_fees_pending = node.account().order_keys().any(|key| {
            node.account()
                .order(key)
                .is_some_and(|order| order.terminal && !order.aborted && !order.fee_reconciled)
        });
        if !self.work.should_start(
            healthy,
            node.rest().busy(),
            now.monotonic.nanos(),
            terminal_fees_pending
                || self.income.as_ref().is_some_and(|w| {
                    w.due(
                        node.account().income_baseline(w.wallet_id()).is_some(),
                        now.monotonic.nanos(),
                    )
                }),
            node.account().valid_until(),
            self.config.valid_for_ns,
        ) {
            return Ok(());
        }
        if now.monotonic.nanos() < self.retry_at {
            return Ok(());
        }
        let calibrated = node
            .session()
            .with_resources(|time, _| time.sample().map(|r| r.signed))
            .map_err(node::Error::Shared)?
            .map_err(node::Error::Clock)?;
        let Some(calibrated) = calibrated else {
            return Ok(());
        };
        let income_end_ms = match calibrated.unit() {
            tickwire_clock::TimeUnit::Milliseconds => calibrated.timestamp(),
            tickwire_clock::TimeUnit::Microseconds => calibrated.timestamp() / 1000,
        };
        if !self.work.loaded {
            let input = node
                .with_account_resources(|owner, _, _| owner.begin_reconciliation(&gap))?
                .map_err(node::Error::Account)?;
            self.plane
                .input_applied(input.stream, node.rest().applied_position())
                .map_err(|_| node::Error::Strategy)?;
            self.work.structures = node.rest().reconciliation_structures(cut.0, cut.1).into();
            self.work.amendments = node.rest().reconciliation_amendments(cut.0, cut.1).into();
            self.work.load(
                node.rest()
                    .reconciliation_orders()
                    .into_iter()
                    .map(|order| OrderQuery {
                        intent: order.intent,
                        order_id: order.order_id,
                        private_connection_epoch: cut.0,
                        gap_generation: cut.1,
                    }),
            );
        }
        if let Some(income) = &mut self.income
            && let Some(query) = income
                .query(
                    cut,
                    node.account().income_baseline(income.wallet_id()).is_some(),
                    node.account().ledger_sequence(),
                    income_end_ms,
                )
                .map_err(node::Error::Account)?
        {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let credentials = &self.credentials;
            let result = node.with_account_resources(|owner, time, quota| {
                let mut budget =
                    move |_: &private::AccountScope,
                          _: &private::SignedTime,
                          _: &private::ClockSample| Ok(delivery);
                let mut clock = ClockAdapter::new(time, scope, &mut budget)
                    .map_err(tickwire_binance_account::Error::Clock)?;
                let mut permit = quota.read(binding);
                if !income.reader.busy() {
                    income
                        .reader
                        .start(owner, query, credentials, &mut clock, &mut permit)?;
                }
                income.reader.poll(
                    owner,
                    &mut clock,
                    &mut permit,
                    IoBudget {
                        max_syscalls: 16,
                        max_bytes: 64 * 1024,
                    },
                )
            })?;
            match result {
                Ok(Some(outcome)) => {
                    self.registered = None;
                    self.plane
                        .input_applied(outcome.input.stream, outcome.through_sequence)
                        .map_err(|_| node::Error::Strategy)?;
                    let Some(evidence) = outcome.evidence else {
                        self.retry(now.monotonic.nanos(), "income query unavailable");
                        return Ok(());
                    };
                    income
                        .scan
                        .as_mut()
                        .expect("active income scan")
                        .accept(&evidence)
                        .map_err(node::Error::Account)?;
                }
                Ok(None) => {}
                Err(tickwire_binance_account::Error::Stopped) => {
                    self.retry(
                        now.monotonic.nanos(),
                        "income query permission or deadline expired",
                    );
                    return Ok(());
                }
                Err(e) => return Err(node::Error::Account(e)),
            }
            if let Some(epoch) = income.reader.request_epoch() {
                if self.registered == Some((4, epoch)) {
                    income.reader.reregister(registry, mio::Token(7))
                } else {
                    income.reader.register(registry, mio::Token(7))
                }
                .map_err(node::Error::Account)?;
                self.registered = Some((4, epoch));
            }
            return Ok(());
        }
        if let Some(query) = self.work.structures.front().copied() {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let credentials = &self.credentials;
            let reader = &mut self.structure_reader;
            let result = node.with_account_resources(|owner, time, quota| {
                let mut budget =
                    move |_: &private::AccountScope,
                          _: &private::SignedTime,
                          _: &private::ClockSample| Ok(delivery);
                let mut clock = ClockAdapter::new(time, scope, &mut budget)
                    .map_err(tickwire_binance_account::Error::Clock)?;
                let mut permit = quota.read(binding);
                if !reader.busy() {
                    reader.start(owner, query, credentials, &mut clock, &mut permit)?;
                }
                reader.poll(
                    owner,
                    &mut clock,
                    &mut permit,
                    IoBudget {
                        max_syscalls: 16,
                        max_bytes: 64 * 1024,
                    },
                )
            })?;
            match result {
                Ok(Some(outcome)) => {
                    self.registered = None;
                    self.plane
                        .input_applied(outcome.input.stream, outcome.through_sequence)
                        .map_err(|_| node::Error::Strategy)?;
                    if !outcome.accepted {
                        self.retry(now.monotonic.nanos(), "parent structure query unavailable");
                        return Ok(());
                    }
                    self.work.structures.pop_front();
                    let absent = node.rest().uncreated_algo_queries(cut.0, cut.1);
                    let waiting = node.rest().waiting_algo_intents(cut.0, cut.1);
                    self.work.orders.retain(|q| {
                        !absent.iter().any(|(intent, _)| *intent == q.intent)
                            && !waiting.contains(&q.intent)
                    });
                }
                Ok(None) => {}
                Err(tickwire_binance_account::Error::Stopped) => {
                    self.retry(
                        now.monotonic.nanos(),
                        "parent query permission or deadline expired",
                    );
                    return Ok(());
                }
                Err(error) => return Err(node::Error::Account(error)),
            }
            if let Some(epoch) = self.structure_reader.request_epoch() {
                if self.registered == Some((5, epoch)) {
                    self.structure_reader.reregister(registry, mio::Token(7))
                } else {
                    self.structure_reader.register(registry, mio::Token(7))
                }
                .map_err(node::Error::Account)?;
                self.registered = Some((5, epoch));
            }
            return Ok(());
        }
        if let Some(query) = self.work.amendments.front().copied() {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let credentials = &self.credentials;
            let reader = &mut self.amendment_reader;
            let result = node.with_account_resources(|owner, time, quota| {
                let mut budget =
                    move |_: &private::AccountScope,
                          _: &private::SignedTime,
                          _: &private::ClockSample| Ok(delivery);
                let mut clock = ClockAdapter::new(time, scope, &mut budget)
                    .map_err(tickwire_binance_account::Error::Clock)?;
                let mut permit = quota.read(binding);
                if !reader.busy() {
                    reader.start(owner, query, credentials, &mut clock, &mut permit)?;
                }
                reader.poll(
                    owner,
                    &mut clock,
                    &mut permit,
                    IoBudget {
                        max_syscalls: 16,
                        max_bytes: 64 * 1024,
                    },
                )
            })?;
            match result {
                Ok(Some(page)) => {
                    self.registered = None;
                    self.plane
                        .input_applied(page.input.stream, page.through_sequence)
                        .map_err(|_| node::Error::Strategy)?;
                    if !page.accepted {
                        self.retry(
                            now.monotonic.nanos(),
                            "amendment history unavailable or ambiguous",
                        );
                        return Ok(());
                    }
                    if page.page_exhausted {
                        self.work.amendments.pop_front();
                    } else {
                        self.work.amendments.front_mut().unwrap().from_execution_id =
                            page.next_execution_id;
                    }
                }
                Ok(None) => {}
                Err(tickwire_binance_account::Error::Stopped) => {
                    self.retry(
                        now.monotonic.nanos(),
                        "amendment history permission or deadline expired",
                    );
                    return Ok(());
                }
                Err(error) => return Err(node::Error::Account(error)),
            }
            if let Some(epoch) = self.amendment_reader.request_epoch() {
                if self.registered == Some((6, epoch)) {
                    self.amendment_reader.reregister(registry, mio::Token(7))
                } else {
                    self.amendment_reader.register(registry, mio::Token(7))
                }
                .map_err(node::Error::Account)?;
                self.registered = Some((6, epoch));
            }
            return Ok(());
        }
        if let Some(query) = self.work.orders.front().copied() {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let credentials = &self.credentials;
            if !self.work.observed {
                let reader = &mut self.order_reader;
                let result = node.with_account_resources(|owner, time, quota| {
                    let mut budget =
                        move |_: &private::AccountScope,
                              _: &private::SignedTime,
                              _: &private::ClockSample| Ok(delivery);
                    let mut clock = ClockAdapter::new(time, scope, &mut budget)
                        .map_err(tickwire_binance_account::Error::Clock)?;
                    let mut permit = quota.read(binding);
                    if !reader.busy() {
                        reader.start(owner, query, credentials, &mut clock, &mut permit)?;
                    }
                    reader.poll(
                        owner,
                        &mut clock,
                        &mut permit,
                        IoBudget {
                            max_syscalls: 16,
                            max_bytes: 64 * 1024,
                        },
                    )
                })?;
                match result {
                    Ok(Some(outcome)) => {
                        self.registered = None;
                        self.plane
                            .input_applied(outcome.input.stream, outcome.through_sequence)
                            .map_err(|_| node::Error::Strategy)?;
                        if !outcome.accepted {
                            self.retry(now.monotonic.nanos(), "order query unavailable");
                            return Ok(());
                        }
                        self.work.observed = true;
                    }
                    Ok(None) => {}
                    Err(tickwire_binance_account::Error::Stopped) => {
                        self.retry(
                            now.monotonic.nanos(),
                            "order query permission or deadline expired",
                        );
                        return Ok(());
                    }
                    Err(error) => return Err(node::Error::Account(error)),
                }
                if let Some(epoch) = self.order_reader.request_epoch() {
                    if self.registered == Some((1, epoch)) {
                        self.order_reader.reregister(registry, mio::Token(7))
                    } else {
                        self.order_reader.register(registry, mio::Token(7))
                    }
                    .map_err(node::Error::Account)?;
                    self.registered = Some((1, epoch));
                }
            } else {
                let reader = &mut self.trades;
                let trade = TradeQuery {
                    intent: query.intent,
                    order_id: query.order_id,
                    from_id: self.work.next_trade,
                    limit: 16,
                    private_connection_epoch: cut.0,
                    gap_generation: cut.1,
                };
                let result = node.with_account_resources(|owner, time, quota| {
                    let mut budget =
                        move |_: &private::AccountScope,
                              _: &private::SignedTime,
                              _: &private::ClockSample| Ok(delivery);
                    let mut clock = ClockAdapter::new(time, scope, &mut budget)
                        .map_err(tickwire_binance_account::Error::Clock)?;
                    let mut permit = quota.read(binding);
                    if !reader.busy() {
                        reader.start(owner, trade, credentials, &mut clock, &mut permit)?;
                    }
                    reader.poll(
                        owner,
                        &mut clock,
                        &mut permit,
                        IoBudget {
                            max_syscalls: 16,
                            max_bytes: 64 * 1024,
                        },
                    )
                })?;
                match result {
                    Ok(Some(page)) => {
                        self.registered = None;
                        self.plane
                            .input_applied(page.input.stream, page.through_sequence)
                            .map_err(|_| node::Error::Strategy)?;
                        if !page.accepted {
                            self.retry(now.monotonic.nanos(), "trade history page unavailable");
                            return Ok(());
                        }
                        if page.page_exhausted {
                            match node.with_account_resources(|owner, _, _| {
                                owner.complete_trade_reconciliation(query)
                            })? {
                                Ok(()) => {
                                    self.work.orders.pop_front();
                                    self.work.observed = false;
                                    self.work.next_trade = 0;
                                }
                                Err(
                                    tickwire_binance_account::Error::Conflict
                                    | tickwire_binance_account::Error::Identity,
                                ) => {
                                    self.retry(
                                        now.monotonic.nanos(),
                                        "trade history does not cover the observed order",
                                    );
                                    return Ok(());
                                }
                                Err(error) => return Err(node::Error::Account(error)),
                            }
                        } else {
                            self.work.next_trade = page.next_from_id.ok_or(node::Error::Config)?;
                        }
                    }
                    Ok(None) => {}
                    Err(tickwire_binance_account::Error::Stopped) => {
                        self.retry(
                            now.monotonic.nanos(),
                            "trade history permission or deadline expired",
                        );
                        return Ok(());
                    }
                    Err(error) => return Err(node::Error::Account(error)),
                }
                if let Some(epoch) = self.trades.request_epoch() {
                    if self.registered == Some((2, epoch)) {
                        self.trades.reregister(registry, mio::Token(7))
                    } else {
                        self.trades.register(registry, mio::Token(7))
                    }
                    .map_err(node::Error::Account)?;
                    self.registered = Some((2, epoch));
                }
            }
            return Ok(());
        }
        if self.snapshots.len() < 3 {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let reader = &mut self.reader;
            let credentials = &self.credentials;
            let next_kind = match self.snapshots.len() {
                0 => SnapshotKind::Account,
                1 => SnapshotKind::OpenOrders,
                _ => SnapshotKind::ConditionalOrders,
            };
            let polled = node.with_account_resources(|owner, time, quota| {
                let mut budget =
                    move |_: &private::AccountScope,
                          _: &private::SignedTime,
                          _: &private::ClockSample| Ok(delivery);
                let mut clock = ClockAdapter::new(time, scope, &mut budget)
                    .map_err(tickwire_binance_account::Error::Clock)?;
                let mut permit = quota.read(binding);
                if !reader.busy() {
                    reader.start(
                        owner,
                        SnapshotQuery {
                            kind: next_kind,
                            private_connection_epoch: cut.0,
                            gap_generation: cut.1,
                        },
                        credentials,
                        &mut clock,
                        &mut permit,
                    )?;
                }
                reader.poll(
                    owner,
                    &mut clock,
                    &mut permit,
                    IoBudget {
                        max_syscalls: 16,
                        max_bytes: 64 * 1024,
                    },
                )
            })?;
            let outcome = match polled {
                Ok(outcome) => outcome,
                Err(tickwire_binance_account::Error::Stopped) => {
                    self.retry(now.monotonic.nanos(), "read permission or request expired");
                    return Ok(());
                }
                Err(error) => return Err(node::Error::Account(error)),
            };
            if let Some(outcome) = outcome {
                self.registered = None;
                self.plane
                    .input_applied(outcome.input.stream, outcome.through_sequence)
                    .map_err(|_| node::Error::Strategy)?;
                let Some(evidence) = outcome.evidence else {
                    self.retry(now.monotonic.nanos(), "remote snapshot unavailable");
                    return Ok(());
                };
                self.snapshots.push(evidence);
            }
            if let Some(epoch) = self.reader.request_epoch() {
                if self.registered == Some((3, epoch)) {
                    self.reader.reregister(registry, mio::Token(7))
                } else {
                    self.reader.register(registry, mio::Token(7))
                }
                .map_err(node::Error::Account)?;
                self.registered = Some((3, epoch));
            }
        }
        if self.snapshots.len() != 3 {
            return Ok(());
        }
        let fresh = node.local_time()?;
        if let Some(income) = &mut self.income
            && node.account().income_baseline(income.wallet_id()).is_none()
        {
            let request = income
                .request(cut, &self.snapshots)
                .map_err(|_| node::Error::Config)?;
            let proposal = node
                .rest()
                .propose_income_import(request)
                .map_err(node::Error::Account)?;
            let controller = node
                .rest()
                .ingress()
                .income_baseline_authority()
                .ok_or(node::Error::Config)?
                .controller_id;
            let approval = income
                .files(proposal, controller, self.writer_epoch, fresh.monotonic)
                .map_err(|_| node::Error::Config)?;
            let Some((proposal, approval)) = approval else {
                return Ok(());
            };
            let accepted = node.local_time()?.monotonic;
            node.with_account_resources(|owner, _, _| {
                owner.import_income_baseline(proposal, approval, accepted)
            })?
            .map_err(node::Error::Account)?;
            let input = node
                .rest()
                .latest_applied_input()
                .ok_or(node::Error::Config)?;
            self.plane
                .input_applied(input.stream, input.sequence)
                .map_err(|_| node::Error::Strategy)?;
            // Installing coverage advances the ledger. Fresh snapshots must bind
            // that new ledger before the normal activation protocol can run.
            self.snapshots.clear();
            income.reset();
            eprintln!(
                "{}",
                serde_json::json!({"event":"income_baseline_imported","input_sequence":input.sequence,"ledger_sequence":node.account().ledger_sequence()})
            );
            return Ok(());
        }
        if node
            .account()
            .unexplained_account_observations()
            .next()
            .is_some()
        {
            let Some(association) = &mut self.association else {
                self.retry(
                    fresh.monotonic.nanos(),
                    "unexplained account observations require dedicated association approval",
                );
                return Ok(());
            };
            if !association
                .poll_selection(fresh.monotonic.nanos())
                .map_err(|_| node::Error::Config)?
            {
                return Ok(());
            }
            let Some(income) = &mut self.income else {
                return Err(node::Error::Config);
            };
            let prepared =
                node.with_account_resources(|owner, _, _| owner.prepare_association_policy())?;
            let input = match prepared {
                Ok(v) => v,
                Err(tickwire_binance_account::Error::Identity) => {
                    self.retry(
                        fresh.monotonic.nanos(),
                        "association authority is not active",
                    );
                    return Ok(());
                }
                Err(e) => return Err(node::Error::Account(e)),
            };
            self.plane
                .input_applied(input.stream, input.sequence)
                .map_err(|_| node::Error::Strategy)?;
            let current = node.local_time()?.monotonic;
            if association
                .cached(
                    node.account().ledger_sequence(),
                    node.rest().gateway().history_frontier().digest(),
                    node.rest().applied_position(),
                    current,
                )
                .is_none()
            {
                let until = current
                    .nanos()
                    .checked_add(self.config.valid_for_ns)
                    .ok_or(node::Error::Config)?;
                let cut = tickwire_binance_account::reconciliation::activation::ActivationCut {
                    private_connection_epoch: cut.0,
                    gap_generation: cut.1,
                    through_sequence: node.rest().applied_position(),
                    now: current,
                    valid_until: MonoTime::new(current.domain(), until),
                    writer_epoch: self.writer_epoch,
                    execution_connection_epoch: node.rest().execution_connection_epoch(),
                };
                let query = income
                    .closure_request(cut, &self.snapshots)
                    .map_err(|_| node::Error::Config)?;
                let (request, evidence) = association
                    .request(query)
                    .map_err(|_| node::Error::Config)?;
                let certificate = request.certificate.clone();
                node.rest()
                    .publish_association_evidence(&evidence)
                    .map_err(node::Error::Account)?;
                let proposal = match node.rest().propose_income_association(request) {
                    Ok(v) => v,
                    Err(
                        tickwire_binance_account::Error::Identity
                        | tickwire_binance_account::Error::Conflict
                        | tickwire_binance_account::Error::Unsupported,
                    ) => {
                        self.retry(
                            current.nanos(),
                            "association selection does not explain the actual account prefix",
                        );
                        return Ok(());
                    }
                    Err(e) => return Err(node::Error::Account(e)),
                };
                association
                    .publish(proposal, certificate)
                    .map_err(|_| node::Error::Config)?;
            }
            let Some((proposal, approval, audit)) = association
                .approval(current.nanos())
                .map_err(|_| node::Error::Config)?
            else {
                return Ok(());
            };
            node.rest()
                .publish_association_evidence(&audit)
                .map_err(node::Error::Account)?;
            let accepted = node.local_time()?.monotonic;
            match node.with_account_resources(|owner, _, _| {
                owner.accept_income_association(proposal, approval, accepted)
            })? {
                Ok(_) => {}
                Err(
                    tickwire_binance_account::Error::Identity
                    | tickwire_binance_account::Error::Conflict
                    | tickwire_binance_account::Error::Unsupported,
                ) => {
                    self.retry(accepted.nanos(), "association approval is invalid or stale");
                    return Ok(());
                }
                Err(e) => return Err(node::Error::Account(e)),
            }
            let input = node
                .rest()
                .latest_applied_input()
                .ok_or(node::Error::Config)?;
            self.plane
                .input_applied(input.stream, input.sequence)
                .map_err(|_| node::Error::Strategy)?;
            // The funding rows retain their Booked state. Keep the completed scan,
            // but discard snapshots so tag14 must bind this new ledger.
            self.snapshots.clear();
            association.reset();
            eprintln!(
                "{}",
                serde_json::json!({"event":"income_observations_associated","ledger_sequence":node.account().ledger_sequence(),"input_sequence":input.sequence})
            );
            return Ok(());
        }
        if node.account().account_inputs_to_reconcile().is_ok() {
            let Some(income) = &mut self.income else {
                self.retry(
                    fresh.monotonic.nanos(),
                    "income queries required for account closure",
                );
                return Ok(());
            };
            let until = fresh
                .monotonic
                .nanos()
                .checked_add(self.config.valid_for_ns)
                .ok_or(node::Error::Config)?;
            let cut = tickwire_binance_account::reconciliation::activation::ActivationCut {
                private_connection_epoch: cut.0,
                gap_generation: cut.1,
                through_sequence: node.rest().applied_position(),
                now: fresh.monotonic,
                valid_until: MonoTime::new(fresh.monotonic.domain(), until),
                writer_epoch: self.writer_epoch,
                execution_connection_epoch: node.rest().execution_connection_epoch(),
            };
            let request = match income.closure_request(cut, &self.snapshots) {
                Ok(v) => v,
                Err(_) => {
                    self.retry(
                        fresh.monotonic.nanos(),
                        "complete account income scan required",
                    );
                    return Ok(());
                }
            };
            match node
                .with_account_resources(|owner, _, _| owner.reconcile_account_inputs(request))?
            {
                Ok(_) => {}
                Err(
                    tickwire_binance_account::Error::Identity
                    | tickwire_binance_account::Error::Conflict
                    | tickwire_binance_account::Error::Unsupported,
                ) => {
                    self.retry(
                        fresh.monotonic.nanos(),
                        "income rows and snapshots do not close account state",
                    );
                    return Ok(());
                }
                Err(e) => return Err(node::Error::Account(e)),
            }
            // Tag14 advances the ledger, so none of these old observations can
            // be used to publish Reactivated. The next polls gather all three again.
            income.after_closure(node.account().ledger_sequence());
            self.snapshots.clear();
            eprintln!(
                "{}",
                serde_json::json!({"event":"account_inputs_reconciled","ledger_sequence":node.account().ledger_sequence()})
            );
            return Ok(());
        }
        let until = fresh
            .monotonic
            .nanos()
            .checked_add(self.config.valid_for_ns)
            .ok_or(node::Error::Config)?;
        let through = node.rest().applied_position();
        if let Some((_, parent_query)) = node
            .rest()
            .uncreated_algo_queries(cut.0, cut.1)
            .first()
            .copied()
        {
            let request =
                tickwire_binance_account::reconciliation::structure_closure::ClosureRequest {
                    parent_query,
                    snapshots: std::array::from_fn(|i| self.snapshots[i].input().sequence),
                    cut: tickwire_binance_account::reconciliation::activation::ActivationCut {
                        private_connection_epoch: cut.0,
                        gap_generation: cut.1,
                        through_sequence: through,
                        now: fresh.monotonic,
                        valid_until: MonoTime::new(fresh.monotonic.domain(), until),
                        writer_epoch: self.writer_epoch,
                        execution_connection_epoch: node.rest().execution_connection_epoch(),
                    },
                };
            match node.with_trading_owner(|owner| owner.close_uncreated_algo(request)) {
                Ok(_) => {}
                Err(
                    tickwire_binance_account::Error::Identity
                    | tickwire_binance_account::Error::Conflict
                    | tickwire_binance_account::Error::Unsupported,
                ) => {
                    self.retry(
                        fresh.monotonic.nanos(),
                        "parent query and snapshots do not prove an uncreated child",
                    );
                    return Ok(());
                }
                Err(e) => return Err(node::Error::Account(e)),
            }
            self.snapshots.clear();
            eprintln!(
                "{}",
                serde_json::json!({"event":"uncreated_algo_closed","ledger_sequence":node.account().ledger_sequence()})
            );
            return Ok(());
        }
        let evidence: [SnapshotEvidence; 3] = std::mem::take(&mut self.snapshots)
            .try_into()
            .map_err(|_| node::Error::Config)?;
        let replace_connection = node.rest().gateway().halted();
        let connect_deadline = Instant::now()
            .checked_add(Duration::from_nanos(self.connect_ns))
            .ok_or(node::Error::Config)?;
        let connection = replace_connection.then(|| tickwire_account_gateway::ConnectionConfig {
            address: self.address,
            tls: self.tls.clone(),
            limits: self.limits,
            connect_deadline,
        });
        let activation = tickwire_binance_account::reconciliation::activation::ActivationCut {
            private_connection_epoch: cut.0,
            gap_generation: cut.1,
            through_sequence: through,
            now: fresh.monotonic,
            valid_until: MonoTime::new(fresh.monotonic.domain(), until),
            writer_epoch: self.writer_epoch,
            execution_connection_epoch: if replace_connection {
                node.rest()
                    .websocket_next_epoch()
                    .map_err(node::Error::Account)?
                    .unwrap_or(self.execution_epoch)
            } else {
                node.rest().execution_connection_epoch()
            },
        };
        let proof =
            node.with_trading_owner(|owner| owner.reactivate(evidence, activation, connection));
        let mut proof = match proof {
            Ok(proof) => proof,
            Err(
                tickwire_binance_account::Error::Identity
                | tickwire_binance_account::Error::Conflict
                | tickwire_binance_account::Error::Unsupported,
            ) => {
                self.retry(
                    fresh.monotonic.nanos(),
                    "remote account does not close the local recovery state",
                );
                return Ok(());
            }
            Err(error) => return Err(node::Error::Account(error)),
        };
        let request = ReconciliationRequest::from(&proof);
        self.plane
            .input_applied(request.input_stream, request.through_sequence)
            .map_err(|_| node::Error::Strategy)?;
        self.plane
            .complete_reconciliation(request, &mut proof)
            .map_err(|_| node::Error::Strategy)?;
        if replace_connection {
            node.connection_replaced();
            self.execution_epoch = proof
                .binding()
                .execution_connection_epoch
                .checked_add(1)
                .ok_or(node::Error::Config)?;
        }
        if let Some(income) = &mut self.income {
            income.completed(fresh.monotonic.nanos());
        }
        self.work.completed(
            fresh.monotonic.nanos(),
            self.config.fee_maintenance_interval_ns,
        );
        eprintln!(
            "{}",
            serde_json::json!({"event":"account_reconciled","connection_epoch":cut.0,"gap_generation":cut.1,"ledger_sequence":node.account().ledger_sequence()})
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_terminal_fee_maintenance_is_bounded_and_idle_only() {
        let work = OrderWork {
            maintenance_at: 100,
            ..OrderWork::default()
        };
        assert!(!work.should_start(true, false, 100, false, 1000, 100));
        assert!(!work.should_start(true, true, 100, true, 1000, 100));
        assert!(!work.should_start(true, false, 99, true, 1000, 100));
        assert!(work.should_start(true, false, 100, true, 1000, 100));
        assert!(work.should_start(false, true, 99, false, 1000, 100));
    }

    #[test]
    fn healthy_empty_account_renews_from_durable_deadline() {
        let work = OrderWork {
            maintenance_at: 1000,
            ..OrderWork::default()
        };
        assert!(!work.should_start(true, false, 149, false, 200, 100));
        assert!(work.should_start(true, false, 150, false, 200, 100));
        assert!(!work.should_start(true, true, 150, false, 200, 100));
        assert!(work.should_start(true, false, 200, false, 200, 100));
        // Busy only defers work. It cannot extend the account's deadline.
        assert!(!work.should_start(true, true, 200, false, 200, 100));
    }

    #[test]
    fn renewal_deadline_arithmetic_keeps_exact_expiry_and_bounds() {
        let work = OrderWork::default();
        assert!(!work.should_start(true, false, u64::MAX - 51, false, u64::MAX, 100));
        assert!(work.should_start(true, false, u64::MAX - 50, false, u64::MAX, 100));
        assert!(work.should_start(true, false, 0, false, 1, u64::MAX));
        // A one-nanosecond validity has no representable half-window before expiry.
        assert!(!work.should_start(true, false, 9, false, 10, 1));
        assert!(work.should_start(true, false, 10, false, 10, 1));
    }

    #[test]
    fn successful_attempt_reloads_new_orders_in_the_same_private_cut() {
        let mut work = OrderWork::default();
        work.load(std::iter::empty());
        assert!(work.loaded);
        work.completed(100, 50);
        assert!(!work.loaded);
        assert_eq!(work.maintenance_at, 150);
        let query = OrderQuery {
            intent: tickwire_account::IntentKey {
                epoch: 4,
                intent_id: 3,
            },
            order_id: [7; 16],
            private_connection_epoch: 1,
            gap_generation: 1,
        };
        work.load(std::iter::once(query));
        assert_eq!(work.orders.front(), Some(&query));
        work.observed = true;
        work.next_trade = 34;
        work.reset();
        assert!(work.orders.is_empty());
        assert!(!work.loaded && !work.observed);
        assert_eq!(work.next_trade, 0);
    }
}
