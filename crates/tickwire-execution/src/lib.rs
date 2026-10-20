//! Single-owner, venue-independent execution primitives. No I/O or allocation on transitions.
#![forbid(unsafe_code)]

pub mod ports;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    Spot,
    UsdM,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Domain {
    pub venue: u32,
    pub account: u64,
    pub product: Product,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    pub venue: u32,
    pub account: u64,
    pub product: Product,
    pub intent: u64,
}
impl OrderKey {
    pub fn domain(self) -> Domain {
        Domain {
            venue: self.venue,
            account: self.account,
            product: self.product,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRequest {
    pub key: OrderKey,
    pub quantity: u64,
    pub unit_budget: i128,
    pub fee_budget: i128,
    pub epoch: u64,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub deadline: u64,
    pub recovery_object: u64,
    pub commit_profile: CommitProfile,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    pub epoch: u64,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub now: u64,
    pub lease_until: u64,
    pub enabled: bool,
    pub market_valid: bool,
    pub account_valid: bool,
    pub recovered: bool,
    pub owner_isolated: bool,
}
impl Gate {
    fn permits(self, r: OrderRequest) -> bool {
        self.enabled
            && self.market_valid
            && self.account_valid
            && self.recovered
            && self.owner_isolated
            && self.now < self.lease_until
            && self.now < r.deadline
            && self.epoch == r.epoch
            && self.strategy_generation == r.strategy_generation
            && self.config_revision == r.config_revision
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendState {
    NotSubmitted,
    MayHaveSent,
    Confirmed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    Filled,
    Canceled,
    Rejected,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelState {
    None,
    Requested,
    Rejected,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitProfile {
    LocalDurable,
    SyncReplica,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableRecord {
    pub request: OrderRequest,
    pub sequence: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    pub record: DurableRecord,
    pub local_durable: bool,
    pub replica_durable: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    pub trade_id: u64,
    pub quantity: u64,
    pub cost: i128,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EconomicEvent {
    Fill(Fill),
    Fee { id: u64, amount: i128 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    InvalidEvent,
    Overflow,
    WrongDomain,
    DuplicateIntent,
    Capacity,
    InsufficientCapital,
    GateClosed,
    NotFound,
    NotDurable,
    ReceiptMismatch,
    AlreadySubmitted,
    MayHaveSent,
    ConflictingEvent,
    EventCapacity,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order<const T: usize> {
    pub request: OrderRequest,
    pub sequence: u64,
    pub durable: bool,
    pub send: SendState,
    pub terminal: Option<Terminal>,
    pub cancel: CancelState,
    pub filled: u64,
    pub observed_filled: u64,
    pub spent: i128,
    pub fees_paid: i128,
    pub fee_released: bool,
    events: [Option<EconomicEvent>; T],
}
impl<const T: usize> Order<T> {
    fn new(request: OrderRequest, sequence: u64) -> Self {
        Self {
            request,
            sequence,
            durable: false,
            send: SendState::NotSubmitted,
            terminal: None,
            cancel: CancelState::None,
            filled: 0,
            observed_filled: 0,
            spent: 0,
            fees_paid: 0,
            fee_released: false,
            events: [None; T],
        }
    }
    fn held(&self) -> i128 {
        let remaining = if self.terminal.is_some() {
            self.observed_filled.saturating_sub(self.filled)
        } else {
            self.request.quantity - self.filled
        };
        // reserve validated quantity * unit + fee, so both components fit.
        i128::from(remaining) * self.request.unit_budget
            + if self.fee_released {
                0
            } else {
                (self.request.fee_budget - self.fees_paid).max(0)
            }
    }
    fn record_event(&mut self, event: EconomicEvent) -> Result<bool, Error> {
        for prior in self.events.iter().flatten() {
            let same = match (*prior, event) {
                (EconomicEvent::Fill(a), EconomicEvent::Fill(b)) => a.trade_id == b.trade_id,
                (EconomicEvent::Fee { id: a, .. }, EconomicEvent::Fee { id: b, .. }) => a == b,
                _ => false,
            };
            if same {
                return if *prior == event {
                    Ok(false)
                } else {
                    Err(Error::ConflictingEvent)
                };
            }
        }
        let slot = self
            .events
            .iter_mut()
            .find(|s| s.is_none())
            .ok_or(Error::EventCapacity)?;
        *slot = Some(event);
        Ok(true)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Engine<const N: usize, const T: usize> {
    domain: Domain,
    capital: i128,
    gate: Gate,
    orders: [Option<Order<T>>; N],
    next_sequence: u64,
    spent: i128,
    held: i128,
    frozen: bool,
}
impl<const N: usize, const T: usize> Engine<N, T> {
    pub fn new(domain: Domain, capital: i128, gate: Gate) -> Result<Self, Error> {
        if capital < 0 || N == 0 || T == 0 {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            domain,
            capital,
            gate,
            orders: [None; N],
            next_sequence: 1,
            spent: 0,
            held: 0,
            frozen: false,
        })
    }
    pub fn update_gate(&mut self, gate: Gate) {
        self.gate = gate;
    }
    /// Halt admission when an external service cannot safely deliver/apply facts.
    pub fn freeze(&mut self) {
        self.frozen = true;
    }
    pub fn frozen(&self) -> bool {
        self.frozen
    }
    pub fn available(&self) -> i128 {
        self.capital - self.spent - self.held
    }
    pub fn held(&self) -> i128 {
        self.held
    }
    pub fn spent(&self) -> i128 {
        self.spent
    }
    fn index(&self, id: u64) -> Result<usize, Error> {
        self.orders
            .iter()
            .position(|s| s.is_some_and(|o| o.request.key.intent == id))
            .ok_or(Error::NotFound)
    }
    pub fn order(&self, id: u64) -> Result<&Order<T>, Error> {
        Ok(self.orders[self.index(id)?].as_ref().unwrap())
    }
    fn checked_budget(r: OrderRequest) -> Result<i128, Error> {
        if r.quantity == 0
            || r.unit_budget <= 0
            || r.fee_budget < 0
            || r.key.intent == 0
            || r.recovery_object == 0
        {
            return Err(Error::InvalidRequest);
        }
        i128::from(r.quantity)
            .checked_mul(r.unit_budget)
            .and_then(|n| n.checked_add(r.fee_budget))
            .ok_or(Error::Overflow)
    }
    pub fn reserve(&mut self, r: OrderRequest) -> Result<(), Error> {
        if r.key.domain() != self.domain {
            return Err(Error::WrongDomain);
        }
        if self.index(r.key.intent).is_ok() {
            return Err(Error::DuplicateIntent);
        }
        let budget = Self::checked_budget(r)?;
        if self.frozen || !self.gate.permits(r) {
            return Err(Error::GateClosed);
        }
        if budget > self.available() {
            return Err(Error::InsufficientCapital);
        }
        let slot = self
            .orders
            .iter()
            .position(Option::is_none)
            .ok_or(Error::Capacity)?;
        let next = self.next_sequence.checked_add(1).ok_or(Error::Overflow)?;
        self.orders[slot] = Some(Order::new(r, self.next_sequence));
        self.next_sequence = next;
        self.held += budget;
        Ok(())
    }
    pub fn durable_record(&self, id: u64) -> Result<DurableRecord, Error> {
        let o = self.order(id)?;
        Ok(DurableRecord {
            request: o.request,
            sequence: o.sequence,
        })
    }
    pub fn accept_commit(&mut self, r: CommitReceipt, profile: CommitProfile) -> Result<(), Error> {
        let i = self.index(r.record.request.key.intent)?;
        if self.durable_record(r.record.request.key.intent)? != r.record {
            return Err(Error::ReceiptMismatch);
        }
        if profile != r.record.request.commit_profile {
            return Err(Error::ReceiptMismatch);
        }
        if !r.local_durable || (profile == CommitProfile::SyncReplica && !r.replica_durable) {
            return Err(Error::NotDurable);
        }
        self.orders[i].as_mut().unwrap().durable = true;
        Ok(())
    }
    pub fn begin_send(&mut self, id: u64) -> Result<OrderRequest, Error> {
        let i = self.index(id)?;
        let o = self.orders[i].as_mut().unwrap();
        if o.send != SendState::NotSubmitted {
            return Err(Error::AlreadySubmitted);
        }
        if o.terminal.is_some() {
            return Err(Error::InvalidEvent);
        }
        if !o.durable {
            return Err(Error::NotDurable);
        }
        if self.frozen || !self.gate.permits(o.request) {
            return Err(Error::GateClosed);
        }
        // Happens BEFORE calling any adapter/TLS buffer: loss of its callback stays Unknown.
        o.send = SendState::MayHaveSent;
        Ok(o.request)
    }
    pub fn abort_unsent(&mut self, id: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        let mut o = self.orders[i].unwrap();
        if o.send != SendState::NotSubmitted {
            return Err(Error::MayHaveSent);
        }
        o.terminal = Some(Terminal::Rejected);
        o.fee_released = true;
        self.replace(i, o)
    }
    /// Only the trusted transport adapter may assert it accepted no output-capable bytes.
    pub fn transport_rejected_without_bytes(&mut self, id: u64) -> Result<(), Error> {
        let result = self.apply_no_bytes(id);
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    fn apply_no_bytes(&mut self, id: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        let mut o = self.orders[i].unwrap();
        if o.send != SendState::MayHaveSent
            || o.filled != 0
            || o.observed_filled != 0
            || o.terminal.is_some()
        {
            return Err(Error::InvalidEvent);
        }
        o.send = SendState::NotSubmitted;
        o.terminal = Some(Terminal::Rejected);
        o.fee_released = true;
        self.replace(i, o)
    }
    pub fn uncertain(&mut self, id: u64) -> Result<(), Error> {
        if self.order(id)?.send == SendState::NotSubmitted {
            return Err(Error::InvalidEvent);
        }
        self.frozen = true;
        Ok(())
    }
    pub fn not_found(&mut self, id: u64) -> Result<(), Error> {
        self.uncertain(id)
    }
    pub fn ack(&mut self, id: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        let o = self.orders[i].as_mut().unwrap();
        if o.send == SendState::NotSubmitted {
            return Err(Error::InvalidEvent);
        }
        o.send = SendState::Confirmed;
        Ok(())
    }
    pub fn request_cancel(&mut self, id: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        let o = self.orders[i].as_mut().unwrap();
        if o.send == SendState::NotSubmitted || o.terminal.is_some() {
            return Err(Error::InvalidEvent);
        }
        o.cancel = CancelState::Requested;
        Ok(())
    }
    pub fn cancel_rejected(&mut self, id: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        self.orders[i].as_mut().unwrap().cancel = CancelState::Rejected;
        Ok(())
    }
    pub fn terminal(&mut self, id: u64, status: Terminal, cumulative: u64) -> Result<(), Error> {
        let result = self.apply_terminal(id, status, cumulative);
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    fn apply_terminal(&mut self, id: u64, status: Terminal, cumulative: u64) -> Result<(), Error> {
        let i = self.index(id)?;
        let mut o = self.orders[i].unwrap();
        if o.send == SendState::NotSubmitted
            || cumulative > o.request.quantity
            || (status == Terminal::Filled && cumulative != o.request.quantity)
        {
            return Err(Error::InvalidEvent);
        }
        o.observed_filled = o.observed_filled.max(cumulative).max(o.filled);
        if o.terminal.is_none() {
            o.terminal = Some(status);
        }
        if o.observed_filled == o.request.quantity {
            o.terminal = Some(Terminal::Filled);
        }
        o.send = SendState::Confirmed;
        self.replace(i, o)
    }
    pub fn fill(&mut self, id: u64, fill: Fill) -> Result<(), Error> {
        let result = self.apply_fill(id, fill);
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    fn apply_fill(&mut self, id: u64, fill: Fill) -> Result<(), Error> {
        let i = self.index(id)?;
        let mut o = self.orders[i].unwrap();
        if o.send == SendState::NotSubmitted || fill.quantity == 0 || fill.cost < 0 {
            return Err(Error::InvalidEvent);
        }
        if !o.record_event(EconomicEvent::Fill(fill))? {
            return Ok(());
        }
        let qty = o.filled.checked_add(fill.quantity).ok_or(Error::Overflow)?;
        if qty > o.request.quantity {
            return Err(Error::InvalidEvent);
        }
        o.spent = o.spent.checked_add(fill.cost).ok_or(Error::Overflow)?;
        o.filled = qty;
        o.observed_filled = o.observed_filled.max(qty);
        o.send = SendState::Confirmed;
        if qty == o.request.quantity {
            o.terminal = Some(Terminal::Filled);
        }
        self.replace(i, o)
    }
    pub fn fee(&mut self, id: u64, fee_id: u64, amount: i128) -> Result<(), Error> {
        let result = self.apply_fee(id, fee_id, amount);
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    fn apply_fee(&mut self, id: u64, fee_id: u64, amount: i128) -> Result<(), Error> {
        let i = self.index(id)?;
        let mut o = self.orders[i].unwrap();
        if o.send == SendState::NotSubmitted || amount < 0 {
            return Err(Error::InvalidEvent);
        }
        if !o.record_event(EconomicEvent::Fee { id: fee_id, amount })? {
            return Ok(());
        }
        o.send = SendState::Confirmed;
        o.fees_paid = o.fees_paid.checked_add(amount).ok_or(Error::Overflow)?;
        o.spent = o.spent.checked_add(amount).ok_or(Error::Overflow)?;
        self.replace(i, o)
    }
    fn replace(&mut self, i: usize, o: Order<T>) -> Result<(), Error> {
        let old = self.orders[i].unwrap();
        let spent = self
            .spent
            .checked_sub(old.spent)
            .and_then(|v| v.checked_add(o.spent))
            .ok_or(Error::Overflow)?;
        let held = self
            .held
            .checked_sub(old.held())
            .and_then(|v| v.checked_add(o.held()))
            .ok_or(Error::Overflow)?;
        // Keep even a deficit representable so available() cannot overflow.
        let liability = spent.checked_add(held).ok_or(Error::Overflow)?;
        self.orders[i] = Some(o);
        self.spent = spent;
        self.held = held;
        self.frozen |= liability > self.capital;
        Ok(())
    }
    /// Replay journal order, introducing each intent before its facts; recovery never submits.
    pub fn recover_intent(&mut self, r: DurableRecord) -> Result<(), Error> {
        if r.request.key.domain() != self.domain {
            return Err(Error::WrongDomain);
        }
        if self.index(r.request.key.intent).is_ok() {
            return Err(Error::DuplicateIntent);
        }
        let budget = Self::checked_budget(r.request)?;
        let held = self.held.checked_add(budget).ok_or(Error::Overflow)?;
        self.spent.checked_add(held).ok_or(Error::Overflow)?;
        let next = r.sequence.checked_add(1).ok_or(Error::Overflow)?;
        let i = self
            .orders
            .iter()
            .position(Option::is_none)
            .ok_or(Error::Capacity)?;
        let mut o = Order::new(r.request, r.sequence);
        o.durable = true;
        o.send = SendState::MayHaveSent;
        self.orders[i] = Some(o);
        self.held = held;
        self.next_sequence = self.next_sequence.max(next);
        self.frozen = true;
        Ok(())
    }
}
