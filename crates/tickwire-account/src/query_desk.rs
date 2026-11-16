//! Preallocated per-order status and trade reconciliation scheduler (`QueryDesk`).
//!
//! Coalesces at most one `StatusAsk` per [`OrderKey`] across up to [`MAX_STATUS_ASKS`]
//! (`MAX_ORDERS + 32`) preallocated arena slots and dispatches at most [`MAX_REST_SLOTS`] (`32`)
//! concurrent wire calls without post-initialization heap growth.

use crate::{
    OrderKey,
    pool::{PoolFull, WindowedTable},
    types::MAX_ORDERS,
};
use tickwire_venue as v;

pub const MAX_REST_SLOTS: usize = 32;
pub const MAX_STATUS_ASKS: usize = MAX_ORDERS + MAX_REST_SLOTS;

pub const DEFAULT_STATUS_QUERY_DELAY_NS: u64 = 6_000_000_000;
pub const DEFAULT_PLACE_RETRY_NS: u64 = 1_000_000_000;
pub const DEFAULT_CANCEL_INITIAL_RETRY_NS: u64 = 1_000_000_000;
pub const DEFAULT_CANCEL_MAX_RETRY_NS: u64 = 64_000_000_000;
pub const DEFAULT_MAX_CANCEL_ASKS: u8 = 8;
pub const DEFAULT_NOT_FOUND_GRACE_NS: u64 = 6_000_000_000;
pub const DEFAULT_MAX_QUERY_COUNT: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryConfig {
    pub status_query_delay_ns: u64,
    pub place_retry_ns: u64,
    pub cancel_initial_retry_ns: u64,
    pub cancel_max_retry_ns: u64,
    pub max_cancel_asks: u8,
    pub not_found_grace_ns: u64,
    pub max_query_count: u8,
    pub max_in_flight_slots: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            status_query_delay_ns: DEFAULT_STATUS_QUERY_DELAY_NS,
            place_retry_ns: DEFAULT_PLACE_RETRY_NS,
            cancel_initial_retry_ns: DEFAULT_CANCEL_INITIAL_RETRY_NS,
            cancel_max_retry_ns: DEFAULT_CANCEL_MAX_RETRY_NS,
            max_cancel_asks: DEFAULT_MAX_CANCEL_ASKS,
            not_found_grace_ns: DEFAULT_NOT_FOUND_GRACE_NS,
            max_query_count: DEFAULT_MAX_QUERY_COUNT,
            max_in_flight_slots: MAX_REST_SLOTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AskKind {
    Place,
    Cancel,
    Amend,
    Trades,
}

/// Owned wire-call ticket; copies all identifiers by value and holds no arena references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTicket {
    pub slot: u8,
    pub generation: u64,
    pub order: OrderKey,
    pub instrument_id: u64,
    pub native_order_id: Option<v::VenueOrderId>,
    pub kind: AskKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotSummary {
    pub confirmed: bool,
    pub terminal: bool,
    pub cumulative_atoms: i128,
    pub local_filled_atoms: i128,
    pub amend_resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResolution {
    Resolved {
        order: OrderKey,
    },
    RetryScheduled {
        order: OrderKey,
        next_due_mono_ns: u64,
    },
    CloseUncreated {
        order: OrderKey,
        instrument_id: u64,
    },
    FetchTradesBeforeClose {
        order: OrderKey,
        instrument_id: u64,
    },
    CancelRejectedStillOpen {
        order: OrderKey,
        remaining_pending_cancels: u8,
    },
    CancelGivenUp {
        order: OrderKey,
        given_up_cancels: u8,
    },
    FreezeAccount {
        order: OrderKey,
        issued_queries: u8,
    },
}

#[derive(Debug, Clone)]
struct StatusAsk {
    instrument_id: u64,
    native_order_id: Option<v::VenueOrderId>,
    kind: AskKind,
    place_unacked: bool,
    pending_cancels: u8,
    cancel_calls: u8,
    cancel_after: bool,
    trades_after: bool,
    known_terminal: bool,
    issued_query_count: u8,
    first_doubt_mono_ns: u64,
    due_mono_ns: u64,
    sequence: u64,
    in_flight: Option<(u8, u64)>,
}

#[derive(Debug, Clone)]
struct SlotState {
    generation: u64,
    order: OrderKey,
    kind: AskKind,
}

#[derive(Debug, Clone)]
pub struct QueryDesk {
    config: QueryConfig,
    asks: WindowedTable<OrderKey, StatusAsk>,
    slots: [Option<SlotState>; MAX_REST_SLOTS],
    next_generation: u64,
    next_sequence: u64,
}

impl Default for QueryDesk {
    fn default() -> Self {
        Self::new(QueryConfig::default())
    }
}

impl QueryDesk {
    pub fn new(mut config: QueryConfig) -> Self {
        config.max_in_flight_slots = config.max_in_flight_slots.clamp(1, MAX_REST_SLOTS);
        Self {
            config,
            asks: WindowedTable::with_capacity(MAX_STATUS_ASKS),
            slots: std::array::from_fn(|_| None),
            next_generation: 1,
            next_sequence: 1,
        }
    }

    #[inline]
    pub fn config(&self) -> &QueryConfig {
        &self.config
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.asks.capacity()
    }

    #[inline]
    pub fn slots_addr(&self) -> usize {
        self.asks.slots_addr()
    }

    #[inline]
    pub fn buckets_addr(&self) -> usize {
        self.asks.buckets_addr()
    }

    #[inline]
    pub fn has_ask(&self, order: &OrderKey) -> bool {
        self.asks.contains_key(order)
    }

    #[inline]
    pub fn ask_kind(&self, order: &OrderKey) -> Option<AskKind> {
        self.asks.get(order).map(|a| a.kind)
    }

    #[inline]
    pub fn issued_query_count(&self, order: &OrderKey) -> Option<u8> {
        self.asks.get(order).map(|a| a.issued_query_count)
    }

    #[inline]
    pub fn in_flight_count(&self) -> usize {
        self.slots[..self.config.max_in_flight_slots]
            .iter()
            .filter(|s| s.is_some())
            .count()
    }

    #[inline]
    pub fn waiting_count(&self) -> usize {
        self.asks.values().filter(|a| a.in_flight.is_none()).count()
    }

    #[inline]
    pub fn has_pending_cancel(&self, order: &OrderKey) -> bool {
        self.asks.get(order).is_some_and(|a| {
            !a.known_terminal
                && (a.kind == AskKind::Cancel || a.cancel_after || a.pending_cancels > 0)
        })
    }

    #[inline]
    pub fn is_ticket_in_flight(&self, ticket: &QueryTicket) -> bool {
        let slot_idx = ticket.slot as usize;
        slot_idx < self.config.max_in_flight_slots
            && self.slots[slot_idx].as_ref().is_some_and(|s| {
                s.generation == ticket.generation
                    && s.order == ticket.order
                    && s.kind == ticket.kind
            })
            && self
                .asks
                .get(&ticket.order)
                .is_some_and(|a| a.in_flight == Some((ticket.slot, ticket.generation)))
    }

    pub fn remove_order(&mut self, order: &OrderKey) {
        if let Some(ask) = self.asks.remove(order)
            && let Some((slot, generation)) = ask.in_flight
            && let Some(slot_state) = self.slots.get_mut(slot as usize)
            && slot_state
                .as_ref()
                .is_some_and(|s| s.generation == generation)
        {
            *slot_state = None;
        }
    }

    pub fn next_deadline_mono_ns(&self) -> Option<u64> {
        self.asks
            .values()
            .filter(|a| a.in_flight.is_none())
            .map(|a| a.due_mono_ns)
            .min()
    }

    fn alloc_sequence(&mut self) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        seq
    }

    fn cancel_backoff_ns(&self, call_index: u8) -> u64 {
        let shift = u32::from(call_index.min(62));
        let scaled = self
            .config
            .cancel_initial_retry_ns
            .checked_shl(shift)
            .unwrap_or(u64::MAX);
        scaled.min(self.config.cancel_max_retry_ns)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_ask(
        &mut self,
        order: OrderKey,
        instrument_id: u64,
        native_order_id: Option<v::VenueOrderId>,
        kind: AskKind,
        place_unacked: bool,
        now_mono_ns: u64,
        is_live: impl Fn(&OrderKey) -> bool,
    ) -> Result<(), PoolFull> {
        let next_seq = self.next_sequence;
        if let Some(ask) = self.asks.get_mut(&order) {
            if native_order_id.is_some() {
                ask.native_order_id = native_order_id;
            }
            ask.place_unacked |= place_unacked;
            if ask.in_flight.is_none() {
                match kind {
                    AskKind::Cancel => {
                        if !ask.known_terminal {
                            ask.pending_cancels = ask.pending_cancels.saturating_add(1);
                            if ask.kind == AskKind::Trades {
                                ask.cancel_after = true;
                            } else {
                                ask.kind = AskKind::Cancel;
                                ask.cancel_calls = 0;
                                ask.due_mono_ns = ask.due_mono_ns.max(
                                    now_mono_ns.saturating_add(self.config.status_query_delay_ns),
                                );
                                ask.sequence = next_seq;
                                self.next_sequence = self.next_sequence.saturating_add(1);
                            }
                        }
                    }
                    AskKind::Trades => {
                        if ask.kind == AskKind::Cancel && !ask.known_terminal {
                            ask.cancel_after = true;
                            ask.pending_cancels = ask.pending_cancels.max(1);
                        }
                        ask.kind = AskKind::Trades;
                        ask.trades_after = false;
                        ask.due_mono_ns = now_mono_ns;
                        ask.sequence = next_seq;
                        self.next_sequence = self.next_sequence.saturating_add(1);
                    }
                    AskKind::Amend => {
                        if ask.kind == AskKind::Place && !ask.place_unacked {
                            ask.kind = AskKind::Amend;
                        }
                    }
                    AskKind::Place => {}
                }
            } else if kind == AskKind::Cancel {
                if !ask.known_terminal {
                    ask.pending_cancels = ask.pending_cancels.saturating_add(1);
                    ask.cancel_after = true;
                }
            } else if kind == AskKind::Trades {
                ask.trades_after = true;
            }
            return Ok(());
        }

        if self.asks.len() >= MAX_STATUS_ASKS {
            self.asks
                .retain(|k, ask| ask.in_flight.is_some() || is_live(k));
            if self.asks.len() >= MAX_STATUS_ASKS {
                return Err(PoolFull);
            }
        }

        let due_mono_ns = if kind == AskKind::Trades {
            now_mono_ns
        } else {
            now_mono_ns.saturating_add(self.config.status_query_delay_ns)
        };
        let sequence = self.alloc_sequence();
        let ask = StatusAsk {
            instrument_id,
            native_order_id,
            kind,
            place_unacked,
            pending_cancels: u8::from(kind == AskKind::Cancel),
            cancel_calls: 0,
            cancel_after: false,
            trades_after: false,
            known_terminal: false,
            issued_query_count: 0,
            first_doubt_mono_ns: now_mono_ns,
            due_mono_ns,
            sequence,
            in_flight: None,
        };
        self.asks
            .insert_or_evict(order, ask, None, now_mono_ns, None)
            .map(|_| ())
    }

    pub fn poll_due(
        &mut self,
        now_mono_ns: u64,
        mut should_query: impl FnMut(&OrderKey, AskKind) -> bool,
    ) -> Option<QueryTicket> {
        loop {
            let best = self
                .asks
                .iter()
                .filter(|(_, ask, _)| ask.in_flight.is_none() && now_mono_ns >= ask.due_mono_ns)
                .min_by_key(|(_, ask, _)| (ask.due_mono_ns, ask.sequence))
                .map(|(k, ask, _)| (k.clone(), ask.kind));
            let (order, kind) = best?;
            if !should_query(&order, kind) {
                if kind == AskKind::Trades
                    && self.asks.get(&order).is_some_and(|a| {
                        !a.known_terminal && (a.cancel_after || a.pending_cancels > 0)
                    })
                {
                    let seq = self.alloc_sequence();
                    let delay = self.config.status_query_delay_ns;
                    if let Some(ask) = self.asks.get_mut(&order) {
                        ask.kind = AskKind::Cancel;
                        ask.trades_after = false;
                        ask.cancel_calls = 0;
                        ask.cancel_after = false;
                        ask.pending_cancels = ask.pending_cancels.max(1);
                        ask.due_mono_ns = now_mono_ns.saturating_add(delay);
                        ask.sequence = seq;
                    }
                    continue;
                }
                self.asks.remove(&order);
                continue;
            }
            let slot_idx = self.slots[..self.config.max_in_flight_slots]
                .iter()
                .position(Option::is_none)?;
            let generation = self.next_generation;
            self.next_generation = self.next_generation.saturating_add(1);
            let slot = slot_idx as u8;
            let ask = self.asks.get_mut(&order)?;
            ask.in_flight = Some((slot, generation));
            let instrument_id = ask.instrument_id;
            let native_order_id = ask.native_order_id.clone();
            self.slots[slot_idx] = Some(SlotState {
                generation,
                order: order.clone(),
                kind,
            });
            return Some(QueryTicket {
                slot,
                generation,
                order,
                instrument_id,
                native_order_id,
                kind,
            });
        }
    }

    fn claim_ticket(&mut self, ticket: &QueryTicket) -> bool {
        let slot_idx = ticket.slot as usize;
        if slot_idx >= self.config.max_in_flight_slots {
            return false;
        }
        let matches_slot = self.slots[slot_idx].as_ref().is_some_and(|s| {
            s.generation == ticket.generation && s.order == ticket.order && s.kind == ticket.kind
        });
        if !matches_slot {
            return false;
        }
        self.slots[slot_idx] = None;
        let Some(ask) = self.asks.get_mut(&ticket.order) else {
            return false;
        };
        if ask.in_flight != Some((ticket.slot, ticket.generation)) {
            return false;
        }
        ask.in_flight = None;
        true
    }

    pub fn on_not_sent(
        &mut self,
        ticket: &QueryTicket,
        now_mono_ns: u64,
    ) -> Option<QueryResolution> {
        if !self.claim_ticket(ticket) {
            return None;
        }
        let (ask_kind, cancel_calls, cancel_after, trades_after, known_terminal) = {
            let ask = self.asks.get(&ticket.order)?;
            (
                ask.kind,
                ask.cancel_calls,
                ask.cancel_after,
                ask.trades_after,
                ask.known_terminal,
            )
        };
        if trades_after && ask_kind != AskKind::Trades {
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            if !known_terminal
                && (ask_kind == AskKind::Cancel || cancel_after || ask.pending_cancels > 0)
            {
                ask.cancel_after = true;
                ask.pending_cancels = ask.pending_cancels.max(1);
            }
            ask.kind = AskKind::Trades;
            ask.trades_after = false;
            ask.due_mono_ns = now_mono_ns;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: now_mono_ns,
            });
        }
        if cancel_after && !known_terminal && ask_kind != AskKind::Cancel {
            let next_due = now_mono_ns.saturating_add(self.config.status_query_delay_ns);
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.kind = AskKind::Cancel;
            ask.cancel_calls = 0;
            ask.cancel_after = false;
            ask.trades_after = false;
            ask.pending_cancels = ask.pending_cancels.max(1);
            ask.due_mono_ns = next_due;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: next_due,
            });
        }
        let delay = if ask_kind == AskKind::Cancel {
            self.cancel_backoff_ns(cancel_calls.saturating_sub(1))
        } else {
            self.config.place_retry_ns
        };
        let next_due = now_mono_ns.saturating_add(delay);
        let seq = self.alloc_sequence();
        let ask = self.asks.get_mut(&ticket.order)?;
        ask.trades_after = false;
        ask.due_mono_ns = next_due;
        ask.sequence = seq;
        Some(QueryResolution::RetryScheduled {
            order: ticket.order.clone(),
            next_due_mono_ns: next_due,
        })
    }

    pub fn on_transport_failure(
        &mut self,
        ticket: &QueryTicket,
        now_mono_ns: u64,
    ) -> Option<QueryResolution> {
        if !self.claim_ticket(ticket) {
            return None;
        }
        let max_query_count = self.config.max_query_count;
        let max_cancel_asks = self.config.max_cancel_asks;
        let place_retry_ns = self.config.place_retry_ns;
        let status_delay_ns = self.config.status_query_delay_ns;

        let counts_toward_query_budget =
            ticket.kind != AskKind::Cancel || self.asks.get(&ticket.order)?.place_unacked;
        if counts_toward_query_budget {
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.issued_query_count = ask.issued_query_count.saturating_add(1);
            let issued_queries = ask.issued_query_count;
            if max_query_count > 0 && issued_queries >= max_query_count {
                self.asks.remove(&ticket.order);
                return Some(QueryResolution::FreezeAccount {
                    order: ticket.order.clone(),
                    issued_queries,
                });
            }
        }

        if self.asks.get(&ticket.order)?.trades_after && ticket.kind != AskKind::Trades {
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            if !ask.known_terminal
                && (ticket.kind == AskKind::Cancel || ask.cancel_after || ask.pending_cancels > 0)
            {
                ask.cancel_after = true;
                ask.pending_cancels = ask.pending_cancels.max(1);
            }
            ask.kind = AskKind::Trades;
            ask.trades_after = false;
            ask.due_mono_ns = now_mono_ns;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: now_mono_ns,
            });
        }

        if ticket.kind == AskKind::Cancel {
            let cancel_after = self.asks.get(&ticket.order)?.cancel_after;
            if cancel_after {
                let next_due = now_mono_ns.saturating_add(status_delay_ns);
                let seq = self.alloc_sequence();
                let ask = self.asks.get_mut(&ticket.order)?;
                ask.kind = AskKind::Cancel;
                ask.cancel_calls = 0;
                ask.cancel_after = false;
                ask.due_mono_ns = next_due;
                ask.sequence = seq;
                return Some(QueryResolution::RetryScheduled {
                    order: ticket.order.clone(),
                    next_due_mono_ns: next_due,
                });
            }

            let new_cancel_calls = {
                let ask = self.asks.get_mut(&ticket.order)?;
                ask.cancel_calls = ask.cancel_calls.saturating_add(1);
                ask.cancel_calls
            };
            if new_cancel_calls >= max_cancel_asks {
                let (given_up_cancels, place_unacked) = {
                    let ask = self.asks.get(&ticket.order)?;
                    (ask.pending_cancels.max(1), ask.place_unacked)
                };
                if place_unacked {
                    let next_due = now_mono_ns.saturating_add(place_retry_ns);
                    let seq = self.alloc_sequence();
                    let ask = self.asks.get_mut(&ticket.order)?;
                    ask.kind = AskKind::Place;
                    ask.pending_cancels = 0;
                    ask.cancel_calls = 0;
                    ask.cancel_after = false;
                    ask.due_mono_ns = next_due;
                    ask.sequence = seq;
                } else {
                    self.asks.remove(&ticket.order);
                }
                return Some(QueryResolution::CancelGivenUp {
                    order: ticket.order.clone(),
                    given_up_cancels,
                });
            }

            let delay = self.cancel_backoff_ns(new_cancel_calls.saturating_sub(1));
            let next_due = now_mono_ns.saturating_add(delay);
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.due_mono_ns = next_due;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: next_due,
            });
        }

        let cancel_after = self.asks.get(&ticket.order)?.cancel_after;
        let next_due = if cancel_after {
            now_mono_ns.saturating_add(status_delay_ns)
        } else {
            now_mono_ns.saturating_add(place_retry_ns)
        };
        let seq = self.alloc_sequence();
        let ask = self.asks.get_mut(&ticket.order)?;
        if cancel_after {
            ask.kind = AskKind::Cancel;
            ask.cancel_calls = 0;
            ask.cancel_after = false;
        }
        ask.due_mono_ns = next_due;
        ask.sequence = seq;
        Some(QueryResolution::RetryScheduled {
            order: ticket.order.clone(),
            next_due_mono_ns: next_due,
        })
    }

    pub fn on_not_found(
        &mut self,
        ticket: &QueryTicket,
        reply_mono_ns: u64,
        order_confirmed: bool,
    ) -> Option<QueryResolution> {
        if !self.claim_ticket(ticket) {
            return None;
        }
        if !order_confirmed {
            let grace_deadline = self
                .asks
                .get(&ticket.order)?
                .first_doubt_mono_ns
                .saturating_add(self.config.not_found_grace_ns);
            if reply_mono_ns < grace_deadline {
                let max_query_count = self.config.max_query_count;
                let place_retry_ns = self.config.place_retry_ns;
                let status_delay_ns = self.config.status_query_delay_ns;
                let (issued_queries, trades_after, cancel_after, known_terminal, ask_kind) = {
                    let ask = self.asks.get_mut(&ticket.order)?;
                    ask.issued_query_count = ask.issued_query_count.saturating_add(1);
                    (
                        ask.issued_query_count,
                        ask.trades_after,
                        ask.cancel_after,
                        ask.known_terminal,
                        ask.kind,
                    )
                };
                if max_query_count > 0 && issued_queries >= max_query_count {
                    self.asks.remove(&ticket.order);
                    return Some(QueryResolution::FreezeAccount {
                        order: ticket.order.clone(),
                        issued_queries,
                    });
                }
                if trades_after && ask_kind != AskKind::Trades {
                    let seq = self.alloc_sequence();
                    let ask = self.asks.get_mut(&ticket.order)?;
                    if !known_terminal
                        && (ask_kind == AskKind::Cancel || cancel_after || ask.pending_cancels > 0)
                    {
                        ask.cancel_after = true;
                        ask.pending_cancels = ask.pending_cancels.max(1);
                    }
                    ask.kind = AskKind::Trades;
                    ask.trades_after = false;
                    ask.due_mono_ns = reply_mono_ns;
                    ask.sequence = seq;
                    return Some(QueryResolution::RetryScheduled {
                        order: ticket.order.clone(),
                        next_due_mono_ns: reply_mono_ns,
                    });
                }
                let next_due = if cancel_after && !known_terminal {
                    reply_mono_ns.saturating_add(status_delay_ns)
                } else {
                    reply_mono_ns.saturating_add(place_retry_ns)
                };
                let seq = self.alloc_sequence();
                let ask = self.asks.get_mut(&ticket.order)?;
                if cancel_after && !known_terminal {
                    ask.kind = AskKind::Cancel;
                    ask.cancel_calls = 0;
                    ask.cancel_after = false;
                    ask.pending_cancels = ask.pending_cancels.max(1);
                }
                ask.due_mono_ns = next_due;
                ask.sequence = seq;
                return Some(QueryResolution::RetryScheduled {
                    order: ticket.order.clone(),
                    next_due_mono_ns: next_due,
                });
            }
            let instrument_id = self.asks.get(&ticket.order)?.instrument_id;
            self.asks.remove(&ticket.order);
            return Some(QueryResolution::CloseUncreated {
                order: ticket.order.clone(),
                instrument_id,
            });
        }

        let max_query_count = self.config.max_query_count;
        let issued_queries = {
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.issued_query_count = ask.issued_query_count.saturating_add(1);
            ask.issued_query_count
        };
        if max_query_count > 0 && issued_queries >= max_query_count {
            self.asks.remove(&ticket.order);
            return Some(QueryResolution::FreezeAccount {
                order: ticket.order.clone(),
                issued_queries,
            });
        }

        let seq = self.alloc_sequence();
        let ask = self.asks.get_mut(&ticket.order)?;
        let instrument_id = ask.instrument_id;
        ask.kind = AskKind::Trades;
        ask.trades_after = false;
        ask.known_terminal = true;
        ask.place_unacked = false;
        ask.cancel_after = false;
        ask.pending_cancels = 0;
        ask.cancel_calls = 0;
        ask.due_mono_ns = reply_mono_ns;
        ask.sequence = seq;
        Some(QueryResolution::FetchTradesBeforeClose {
            order: ticket.order.clone(),
            instrument_id,
        })
    }

    pub fn on_status_snapshot(
        &mut self,
        ticket: &QueryTicket,
        reply_mono_ns: u64,
        snap: SnapshotSummary,
    ) -> Option<QueryResolution> {
        if !self.claim_ticket(ticket) {
            return None;
        }
        let need_trades = {
            let ask = self.asks.get_mut(&ticket.order)?;
            if snap.confirmed {
                ask.place_unacked = false;
            }
            if snap.terminal {
                ask.known_terminal = true;
                ask.cancel_after = false;
                ask.pending_cancels = 0;
            }
            snap.cumulative_atoms > snap.local_filled_atoms || ask.trades_after
        };
        if need_trades {
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            let instrument_id = ask.instrument_id;
            if !snap.terminal && !ask.known_terminal {
                if ticket.kind == AskKind::Cancel {
                    ask.pending_cancels = ask.pending_cancels.saturating_sub(1);
                    ask.cancel_after = ask.cancel_after || ask.pending_cancels > 0;
                } else if ask.pending_cancels > 0 {
                    ask.cancel_after = true;
                }
            } else {
                ask.cancel_after = false;
                ask.pending_cancels = 0;
            }
            ask.kind = AskKind::Trades;
            ask.trades_after = false;
            ask.place_unacked = false;
            ask.cancel_calls = 0;
            ask.due_mono_ns = reply_mono_ns;
            ask.sequence = seq;
            return Some(QueryResolution::FetchTradesBeforeClose {
                order: ticket.order.clone(),
                instrument_id,
            });
        }
        if ticket.kind == AskKind::Cancel && !snap.terminal {
            let status_delay_ns = self.config.status_query_delay_ns;
            let (remaining, should_rearm) = {
                let ask = self.asks.get_mut(&ticket.order)?;
                ask.pending_cancels = ask.pending_cancels.saturating_sub(1);
                (
                    ask.pending_cancels,
                    ask.cancel_after || ask.pending_cancels > 0,
                )
            };
            if should_rearm {
                let seq = self.alloc_sequence();
                let ask = self.asks.get_mut(&ticket.order)?;
                ask.kind = AskKind::Cancel;
                ask.cancel_calls = 0;
                ask.cancel_after = false;
                ask.due_mono_ns = reply_mono_ns.saturating_add(status_delay_ns);
                ask.sequence = seq;
            } else {
                self.asks.remove(&ticket.order);
            }
            return Some(QueryResolution::CancelRejectedStillOpen {
                order: ticket.order.clone(),
                remaining_pending_cancels: remaining,
            });
        }
        let cancel_after = self.asks.get(&ticket.order)?.cancel_after;
        if cancel_after && !snap.terminal {
            let next_due = reply_mono_ns.saturating_add(self.config.status_query_delay_ns);
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.kind = AskKind::Cancel;
            ask.cancel_calls = 0;
            ask.cancel_after = false;
            ask.due_mono_ns = next_due;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: next_due,
            });
        }
        if (ticket.kind == AskKind::Amend && !snap.amend_resolved && !snap.terminal)
            || (!snap.confirmed && !snap.terminal)
        {
            let next_due = reply_mono_ns.saturating_add(self.config.place_retry_ns);
            let seq = self.alloc_sequence();
            let ask = self.asks.get_mut(&ticket.order)?;
            ask.due_mono_ns = next_due;
            ask.sequence = seq;
            return Some(QueryResolution::RetryScheduled {
                order: ticket.order.clone(),
                next_due_mono_ns: next_due,
            });
        }
        self.asks.remove(&ticket.order);
        Some(QueryResolution::Resolved {
            order: ticket.order.clone(),
        })
    }

    pub fn on_trades_applied(
        &mut self,
        ticket: &QueryTicket,
        reply_mono_ns: u64,
        all_fills_caught_up: bool,
    ) -> Option<QueryResolution> {
        if !self.claim_ticket(ticket) {
            return None;
        }
        if all_fills_caught_up {
            let rearm_cancel = self
                .asks
                .get(&ticket.order)
                .is_some_and(|a| !a.known_terminal && (a.cancel_after || a.pending_cancels > 0));
            if rearm_cancel {
                let next_due = reply_mono_ns.saturating_add(self.config.status_query_delay_ns);
                let seq = self.alloc_sequence();
                let ask = self.asks.get_mut(&ticket.order)?;
                ask.kind = AskKind::Cancel;
                ask.trades_after = false;
                ask.cancel_calls = 0;
                ask.cancel_after = false;
                ask.pending_cancels = ask.pending_cancels.max(1);
                ask.due_mono_ns = next_due;
                ask.sequence = seq;
                return Some(QueryResolution::RetryScheduled {
                    order: ticket.order.clone(),
                    next_due_mono_ns: next_due,
                });
            }
            self.asks.remove(&ticket.order);
            return Some(QueryResolution::Resolved {
                order: ticket.order.clone(),
            });
        }
        let immediate = self.asks.get(&ticket.order)?.trades_after;
        let next_due = if immediate {
            reply_mono_ns
        } else {
            reply_mono_ns.saturating_add(self.config.place_retry_ns)
        };
        let seq = self.alloc_sequence();
        let ask = self.asks.get_mut(&ticket.order)?;
        ask.kind = AskKind::Trades;
        ask.trades_after = false;
        ask.due_mono_ns = next_due;
        ask.sequence = seq;
        Some(QueryResolution::RetryScheduled {
            order: ticket.order.clone(),
            next_due_mono_ns: next_due,
        })
    }
}
