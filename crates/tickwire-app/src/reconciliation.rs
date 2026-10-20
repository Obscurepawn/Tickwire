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
    bootstrap::{AccountSnapshotReader, SnapshotEvidence, SnapshotKind, SnapshotQuery},
    orders::{OrderQuery, ReadOnlyOrderReader},
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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub reader_id: [u8; 16],
    pub first_request_epoch: u64,
    pub first_execution_connection_epoch: u64,
    pub retry_ns: u64,
    pub valid_for_ns: u64,
}
pub struct Controller<'a> {
    reader: AccountSnapshotReader,
    order_reader: ReadOnlyOrderReader,
    trades: ReadOnlyReconciler,
    orders: VecDeque<OrderQuery>,
    orders_loaded: bool,
    order_observed: bool,
    next_trade: u64,
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
        let trades = ReadOnlyReconciler::new(reader_config.clone())?;
        let reader = AccountSnapshotReader::new(reader_config)?;
        Ok(Self {
            reader,
            order_reader,
            trades,
            orders: VecDeque::new(),
            orders_loaded: false,
            order_observed: false,
            next_trade: 0,
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
    fn retry(&mut self, now: u64, message: &str) {
        self.reader.cancel();
        self.order_reader.cancel();
        self.trades.cancel();
        self.orders.clear();
        self.orders_loaded = false;
        self.order_observed = false;
        self.next_trade = 0;
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
            self.trades.cancel();
            self.orders.clear();
            self.orders_loaded = false;
            self.order_observed = false;
            self.next_trade = 0;
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
        if frontier.reconciled && !node.account().frozen() && !node.rest().gateway().halted() {
            return Ok(());
        }
        if now.monotonic.nanos() < self.retry_at {
            return Ok(());
        }
        let calibrated = node
            .session()
            .with_resources(|time, _| time.sample().map(|r| r.signed.is_some()))
            .map_err(node::Error::Shared)?
            .map_err(node::Error::Clock)?;
        if !calibrated {
            return Ok(());
        }
        if !self.orders_loaded {
            let input = node
                .with_account_resources(|owner, _, _| owner.begin_reconciliation(&gap))?
                .map_err(node::Error::Account)?;
            self.plane
                .input_applied(input.stream, node.rest().applied_position())
                .map_err(|_| node::Error::Strategy)?;
            self.orders = node
                .rest()
                .reconciliation_orders()
                .into_iter()
                .map(|order| OrderQuery {
                    intent: order.intent,
                    order_id: order.order_id,
                    private_connection_epoch: cut.0,
                    gap_generation: cut.1,
                })
                .collect();
            self.orders_loaded = true;
        }
        if let Some(query) = self.orders.front().copied() {
            let scope = self.scope.clone();
            let binding = self.binding.clone();
            let delivery = self.delivery_ns;
            let credentials = &self.credentials;
            if !self.order_observed {
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
                        self.order_observed = true;
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
                    from_id: self.next_trade,
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
                                    self.orders.pop_front();
                                    self.order_observed = false;
                                    self.next_trade = 0;
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
                            self.next_trade = page.next_from_id.ok_or(node::Error::Config)?;
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
        if self.snapshots.len() != 3 {
            return Ok(());
        }
        let fresh = node.local_time()?;
        let until = fresh
            .monotonic
            .nanos()
            .checked_add(self.config.valid_for_ns)
            .ok_or(node::Error::Config)?;
        let through = node.rest().applied_position();
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
                self.execution_epoch
            } else {
                node.rest().execution_connection_epoch()
            },
        };
        let proof = node.with_account_resources(|owner, _, _| {
            owner.reactivate(evidence, activation, connection)
        })?;
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
            self.execution_epoch = self
                .execution_epoch
                .checked_add(1)
                .ok_or(node::Error::Config)?;
        }
        eprintln!(
            "{}",
            serde_json::json!({"event":"account_reconciled","connection_epoch":cut.0,"gap_generation":cut.1,"ledger_sequence":node.account().ledger_sequence()})
        );
        Ok(())
    }
}
