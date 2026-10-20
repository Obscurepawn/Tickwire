//! Shared original-signature state for REST encoding and the first-output gate.
#![forbid(unsafe_code)]
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
use tickwire_account::Admission;
use tickwire_account_gateway::{AdmissionRequest, Authority, AuthorityError, Gate};
use tickwire_binance::request::{self, Credentials, RequestOptions, Timestamp, Transport};
use tickwire_binance_clock::{BinanceTimeClient, ClockReading, LocalTime, MonotonicClock};
use tickwire_binance_quota::{CommandBinding, Endpoint};
use tickwire_clock::{ClockDomain, SignedTime, Source, TimeUnit};
use tickwire_gateway::{EncodedRequest, Encoder};
use tickwire_recovery::VerifiedClosure;
use tickwire_transport::http;
use tickwire_venue::{CanonicalCommand, Hash};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Busy,
    Identity,
    Capacity,
    MissingSignature,
    Denied,
    Clock(tickwire_binance_clock::Error),
    Quota(tickwire_binance_quota::Error),
    Encoding,
}
pub type Result<T> = core::result::Result<T, Error>;

/// A trusted calibrated source. Implementations check the original stamp and
/// return the very sample used by that check, with no intervening callback.
pub trait SessionClock {
    fn source(&self) -> Source;
    fn domain(&self) -> ClockDomain;
    fn sample(&mut self) -> tickwire_binance_clock::Result<ClockReading>;
    fn check_send_sample(
        &mut self,
        stamp: &SignedTime,
        recv_window_ns: u64,
        budget_ns: u64,
    ) -> tickwire_binance_clock::Result<LocalTime>;
}
impl<C: MonotonicClock> SessionClock for BinanceTimeClient<C> {
    fn source(&self) -> Source {
        self.source()
    }
    fn domain(&self) -> ClockDomain {
        self.domain()
    }
    fn sample(&mut self) -> tickwire_binance_clock::Result<ClockReading> {
        self.sample()
    }
    fn check_send_sample(
        &mut self,
        stamp: &SignedTime,
        window: u64,
        budget: u64,
    ) -> tickwire_binance_clock::Result<LocalTime> {
        self.check_send_sample(stamp, window, budget)
    }
}
pub trait DeliveryBudget {
    /// Conservative remaining time from the final gate to venue timestamp validation.
    fn remaining_ns(
        &mut self,
        command: &CanonicalCommand,
        stamp: &SignedTime,
        now: &LocalTime,
    ) -> Result<u64>;
}
impl<F: FnMut(&CanonicalCommand, &SignedTime, &LocalTime) -> Result<u64>> DeliveryBudget for F {
    fn remaining_ns(&mut self, c: &CanonicalCommand, s: &SignedTime, n: &LocalTime) -> Result<u64> {
        self(c, s, n)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub recv_window_micros: u64,
    pub max_http_bytes: usize,
    pub max_signed_commands: usize,
}
struct SignedRequest {
    stamp: SignedTime,
    window_ns: u64,
    authority: String,
    bytes: Vec<u8>,
}
struct PreparedGate {
    stamp: SignedTime,
    window_ns: u64,
    budget_ns: u64,
    before: LocalTime,
}
struct State<C, B> {
    clock: C,
    quota: Endpoint,
    binding: CommandBinding,
    budget: B,
    config: Config,
    signed: BTreeMap<Hash, SignedRequest>,
}
/// Clones share one runtime state. It cannot cross threads or mint send authority.
pub struct SharedSessionState<C, B>(Rc<RefCell<State<C, B>>>);
impl<C, B> Clone for SharedSessionState<C, B> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}
impl<C: SessionClock, B: DeliveryBudget> SharedSessionState<C, B> {
    pub fn new(
        clock: C,
        quota: Endpoint,
        binding: CommandBinding,
        budget: B,
        config: Config,
    ) -> Result<Self> {
        if config.recv_window_micros == 0
            || config.recv_window_micros > 60_000_000
            || config.max_http_bytes == 0
            || config.max_http_bytes > 16 * 1024 * 1024
            || config.max_signed_commands == 0
            || config.max_signed_commands > 4096
        {
            return Err(Error::Invalid);
        }
        if binding.owner_id == [0; 16]
            || binding.risk_domain_id == [0; 16]
            || binding.account == 0
            || binding.domain != clock.domain()
            || binding.domain != quota.consumer().clock_domain()
            || source(&binding)? != clock.source()
        {
            return Err(Error::Identity);
        }
        Ok(Self(Rc::new(RefCell::new(State {
            clock,
            quota,
            binding,
            budget,
            config,
            signed: BTreeMap::new(),
        }))))
    }
    pub fn encoder<'a>(&self, credentials: Credentials<'a>) -> RestEncoder<'a, C, B> {
        RestEncoder {
            shared: self.clone(),
            credentials,
        }
    }
    pub fn authority<A: Authority>(&self, inner: A) -> GateAuthority<A, C, B> {
        GateAuthority {
            inner,
            shared: self.clone(),
        }
    }
    /// The runtime owner drives calibration and borrows permits sequentially here.
    /// Reentrant use fails with Busy instead of panicking or bypassing the gate.
    pub fn with_resources<T>(&self, f: impl FnOnce(&mut C, &mut Endpoint) -> T) -> Result<T> {
        let mut state = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let State { clock, quota, .. } = &mut *state;
        Ok(f(clock, quota))
    }
    pub fn signed_stamp(&self, digest: Hash) -> Result<Option<SignedTime>> {
        Ok(self
            .0
            .try_borrow()
            .map_err(|_| Error::Busy)?
            .signed
            .get(&digest)
            .map(|r| r.stamp))
    }
    fn encode(
        &self,
        credentials: &Credentials<'_>,
        command: &CanonicalCommand,
    ) -> Result<EncodedRequest> {
        let mut state = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        state.bind(command)?;
        let expected_market = match state.binding.product {
            tickwire_venue::Product::Spot => request::Market::Spot,
            _ => request::Market::Usdm,
        };
        if credentials.account != state.binding.account
            || credentials.environment != state.binding.environment
            || credentials.market != expected_market
        {
            return Err(Error::Identity);
        }
        let digest = command.digest();
        if let Some(prior) = state.signed.get(&digest) {
            return EncodedRequest::new(digest, prior.authority.clone(), prior.bytes.clone())
                .map_err(|_| Error::Encoding);
        }
        if state.signed.len() >= state.config.max_signed_commands {
            return Err(Error::Capacity);
        }
        let reading = state.clock.sample().map_err(Error::Clock)?;
        let stamp = reading.signed.ok_or(Error::MissingSignature)?;
        if stamp.source() != source(&state.binding)?
            || stamp.domain() != state.binding.domain
            || reading.local.monotonic.domain() != state.binding.domain
            || reading.local.monotonic.nanos() >= command.command().context.request.deadline
        {
            return Err(Error::Identity);
        }
        let timestamp = match stamp.unit() {
            TimeUnit::Milliseconds => Timestamp::Milliseconds(stamp.timestamp()),
            TimeUnit::Microseconds => Timestamp::Microseconds(stamp.timestamp()),
        };
        let prepared = request::prepare(
            command,
            credentials,
            RequestOptions {
                transport: Transport::Rest,
                timestamp,
                recv_window_micros: state.config.recv_window_micros,
                request_id: "rest",
            },
            request::MAX_REQUEST_BYTES,
        )
        .map_err(|_| Error::Encoding)?;
        let bytes = http::encode_request(
            prepared.method().as_str(),
            prepared.path(),
            &[
                ("Host", prepared.authority()),
                (
                    "X-MBX-APIKEY",
                    prepared.api_key_header().ok_or(Error::Encoding)?,
                ),
            ],
            prepared.payload(),
            state.config.max_http_bytes,
        )
        .map_err(|_| Error::Encoding)?;
        let authority = prepared.authority().to_owned();
        let encoded = EncodedRequest::new(digest, authority.clone(), bytes.clone())
            .map_err(|_| Error::Encoding)?;
        let window_ns = state
            .config
            .recv_window_micros
            .checked_mul(1000)
            .ok_or(Error::Invalid)?;
        state.signed.insert(
            digest,
            SignedRequest {
                stamp,
                window_ns,
                authority,
                bytes,
            },
        );
        Ok(encoded)
    }
    fn prepare_gate(&self, command: &CanonicalCommand) -> Result<PreparedGate> {
        let mut state = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        state.bind(command)?;
        let saved = state
            .signed
            .get(&command.digest())
            .ok_or(Error::MissingSignature)?;
        let (stamp, window_ns) = (saved.stamp, saved.window_ns);
        let before = state.clock.sample().map_err(Error::Clock)?.local;
        if before.monotonic.domain() != state.binding.domain {
            return Err(Error::Identity);
        }
        let budget_ns = state.budget.remaining_ns(command, &stamp, &before)?;
        Ok(PreparedGate {
            stamp,
            window_ns,
            budget_ns,
            before,
        })
    }
    /// Runs after all budget and policy callbacks, inside the first-accept gate.
    fn finish_gate(
        &self,
        command: &CanonicalCommand,
        mut gate: Gate,
        prepared: PreparedGate,
    ) -> Result<Gate> {
        let mut state = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        state.bind(command)?;
        let r = &command.command().context.request;
        if !gate.owner_isolated
            || !gate.action_permitted
            || gate.writer_epoch != r.intent_exec_epoch
            || gate.strategy_generation != r.strategy_generation
            || gate.config_revision != r.config_revision
        {
            return Err(Error::Denied);
        }
        let saved = state
            .signed
            .get(&command.digest())
            .ok_or(Error::MissingSignature)?;
        if saved.stamp != prepared.stamp || saved.window_ns != prepared.window_ns {
            return Err(Error::Identity);
        }
        let final_time = state
            .clock
            .check_send_sample(&prepared.stamp, prepared.window_ns, prepared.budget_ns)
            .map_err(Error::Clock)?;
        let now = final_time.monotonic;
        if now.domain() != state.binding.domain
            || now.nanos() < prepared.before.monotonic.nanos()
            || final_time.instant < prepared.before.instant
            || now.nanos() < gate.now
            || now.nanos() >= gate.lease_until
            || now.nanos() >= r.deadline
        {
            return Err(Error::Denied);
        }
        let binding = state.binding;
        state
            .quota
            .consume_command(&binding, command, Transport::Rest, now)
            .map_err(Error::Quota)?;
        gate.now = now.nanos();
        Ok(gate)
    }
}
impl<C: SessionClock, B> State<C, B> {
    fn bind(&self, command: &CanonicalCommand) -> Result<()> {
        let r = &command.command().context.request;
        if self.clock.domain() != self.binding.domain
            || self.clock.source() != source(&self.binding)?
            || r.venue != 1
            || r.risk_domain_id != self.binding.risk_domain_id
            || r.account != self.binding.account
            || r.product != self.binding.product
            || r.environment != self.binding.environment
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub struct RestEncoder<'a, C, B> {
    shared: SharedSessionState<C, B>,
    credentials: Credentials<'a>,
}
impl<C: SessionClock, B: DeliveryBudget> Encoder for RestEncoder<'_, C, B> {
    fn encode(&mut self, command: &CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        self.shared
            .encode(&self.credentials, command)
            .map_err(|e| match e {
                Error::Identity => tickwire_gateway::Error::Identity,
                Error::Capacity => tickwire_gateway::Error::Capacity,
                Error::Busy => tickwire_gateway::Error::Busy,
                _ => tickwire_gateway::Error::Encoding,
            })
    }
}
pub struct GateAuthority<A, C, B> {
    inner: A,
    shared: SharedSessionState<C, B>,
}
impl<A: Authority, C: SessionClock, B: DeliveryBudget> Authority for GateAuthority<A, C, B> {
    fn verify_handover(
        &mut self,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        record: &tickwire_account_codec::CanonicalRecord,
        prior: &[tickwire_account_codec::CanonicalRecord],
    ) -> std::result::Result<(), AuthorityError> {
        self.inner.verify_handover(baseline, record, prior)
    }
    fn commit_state(
        &mut self,
        admission: &tickwire_account::Admission,
        receipt: &tickwire_account_journal::AppendReceipt,
    ) -> core::result::Result<tickwire_account_gateway::CommitState, AuthorityError> {
        self.inner.commit_state(admission, receipt)
    }
    fn prepare(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> core::result::Result<VerifiedClosure, AuthorityError> {
        self.inner.prepare(request)
    }
    fn verify(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> core::result::Result<VerifiedClosure, AuthorityError> {
        self.inner.verify(request)
    }
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> core::result::Result<Gate, AuthorityError> {
        let prepared = self
            .shared
            .prepare_gate(command)
            .map_err(|_| AuthorityError::Denied)?;
        let gate = self.inner.current_gate(command, admission)?;
        self.shared
            .finish_gate(command, gate, prepared)
            .map_err(|_| AuthorityError::Denied)
    }
}
fn source(b: &CommandBinding) -> Result<Source> {
    use tickwire_venue::{Environment, Product};
    match (b.product, b.environment) {
        (Product::Spot, Environment::Production) => Ok(Source::SpotProduction),
        (Product::Spot, Environment::Test) => Ok(Source::SpotTest),
        (Product::LinearPerpetual | Product::LinearFuture, Environment::Production) => {
            Ok(Source::UsdmProduction)
        }
        (Product::LinearPerpetual | Product::LinearFuture, Environment::Test) => {
            Ok(Source::UsdmTest)
        }
        _ => Err(Error::Identity),
    }
}

#[cfg(test)]
mod tests;
