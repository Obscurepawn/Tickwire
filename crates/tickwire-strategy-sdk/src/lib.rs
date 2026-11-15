//! Stateless strategy worker SDK for the 3-process shared-memory topology (`tickwire-md` +
//! strategy workers + unified ORS execution process).
//!
//! Each strategy worker connects directly to `tickwire-md` shared-memory broadcast rings
//! ([`MarketReplica`], [`MarketView`]) at an independent reader cursor, authenticates with the
//! ORS execution process over the Direct Order Entry (`DOP`) handshake ([`DopSession`]:
//! `DopLogin` -> `DopEcho` + `OrsSessions`, `DopHeartBeat`, `DopLogout`), encodes outbound orders,
//! cancels, modifies, cancel-alls, and RFQ quotes directly into 1,024-byte `IntentRecord`
//! shared-memory slots ([`IntentWriter`], [`OutboundIntent`]), and decodes inbound 512-byte
//! `ReportRecord` shared-memory replies in place ([`ReportReceiver`], [`ExecutionReport`]) with
//! zero local WAL, journal, blob-store, or checkpoint state.
//!
//! Optional `worker_cpu` and `housekeeping_cpus` isolation is enforced at cold startup via
//! [`tickwire_control_sys::validate_cpu_isolation`], spawning `"strategy-cold"` housekeeping onto
//! `housekeeping_cpus` before pinning the hot polling thread to `worker_cpu`.

pub mod arena;
pub mod bootstrap;
pub mod catalog;
pub mod dop;
pub mod market;
pub mod microstructure;
pub mod outbound;
pub mod registered;
pub mod reports;
pub mod worker;

pub use arena::{ArenaSliceMeta, ShmArenaReader, ShmArenaWriter};
pub use bootstrap::{ArtifactDigest, Bootstrap, MAX_BOOTSTRAP_BYTES, MAX_PARAM_BYTES};
pub use dop::{
    DEFAULT_MAX_ORDER_AGE_NS, DopConfig, DopControlEvent, DopSession, accept_dop_login,
    local_seqpacket_pair,
};
pub use market::{DenseId, MarketReplica, MarketView};
pub use microstructure::{
    EffectiveFillDelta, FeeAmendmentOutcome, FillWatermarkTracker, OrderClassificationHints,
    OrderFillWatermarks, ReportFillOutcome, SettledFillDelta, classify_order_status,
    classify_venue_rejection, decimal_to_fixed_exact, decimal_to_fixed_half_even,
};
pub use outbound::{
    IntentOpcode, IntentReader, IntentWriter, ModifyRequest, OutboundIntent, OutboundReader,
    OutboundWriter, PublishedIntent, build_modify_command, encode_intent_record,
    in_process_intent_pair, in_process_intent_pair_with_arena, mint_self_describing_client_id,
    ticks_from_decimal,
};
pub use reports::{
    ExecutionReport, ExecutionReportKind, FillFee, FillKind, FillReport, RateLimitReport,
    ReportIdentity, ReportReceiver, ReportWriter, in_process_report_pair,
    in_process_report_pair_with_arena,
};
use tickwire_venue::{CanonicalCommand, TerminalReason};
pub use worker::{WorkerDriver, WorkerStepOutcome};

#[derive(Debug)]
pub enum Error {
    Context {
        stage: &'static str,
        source: Box<Error>,
    },
    Source(tickwire_market_source::Error),
    Wire(tickwire_ipc::Error),
    Join(tickwire_join::Error),
    Ring(tickwire_shm::MappedError),
    ShmSys(tickwire_shm_sys::Error),
    Command(tickwire_venue::Error),
    Catalog(tickwire_instrument_catalog::Error),
    Risk(tickwire_risk::RiskError),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    Clock(tickwire_clock_sys::Error),
    DopRefused {
        reason: TerminalReason,
        status: u16,
    },
    UnauthorizedAccount {
        account_ref: u8,
    },
    Identity,
    Sequence,
    Bounds,
    Stale,
    NotReady,
    Deadline,
    Unsupported,
    Input,
}

impl Error {
    #[forbid(unsafe_code)]
    pub(crate) fn context(self, stage: &'static str) -> Self {
        Self::Context {
            stage,
            source: Box::new(self),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    #[forbid(unsafe_code)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

macro_rules! from {
    ($t:ty, $v:ident) => {
        impl From<$t> for Error {
            #[forbid(unsafe_code)]
            fn from(e: $t) -> Self {
                Self::$v(e)
            }
        }
    };
}

from!(tickwire_market_source::Error, Source);
from!(tickwire_ipc::Error, Wire);
from!(tickwire_join::Error, Join);
from!(tickwire_shm::MappedError, Ring);
from!(tickwire_shm_sys::Error, ShmSys);
from!(tickwire_venue::Error, Command);
from!(tickwire_instrument_catalog::Error, Catalog);
from!(tickwire_risk::RiskError, Risk);
from!(tickwire_control::Error, Control);
from!(tickwire_control_sys::Error, Transport);
from!(std::io::Error, Io);
from!(serde_json::Error, Json);
from!(tickwire_clock_sys::Error, Clock);

/// Stateless strategy decision interface invoked by [`MarketReplica::drain`] and [`WorkerDriver`].
///
/// Return `Some(OutboundIntent)` to publish an inline 1,024-byte `IntentRecord` onto the ORS
/// shared-memory ring, or `None` to observe without emitting an order.
///
/// ```
/// use tickwire_ipc::MarketState;
/// use tickwire_strategy_sdk::{MarketView, OutboundIntent, Result, Strategy};
///
/// #[derive(Default)]
/// struct Spread {
///     last_spread_ticks: Option<i64>,
/// }
///
/// impl Strategy for Spread {
///     fn on_market(
///         &mut self,
///         view: MarketView<'_>,
///         _now_ns: u64,
///     ) -> Result<Option<OutboundIntent>> {
///         self.last_spread_ticks = match view.best_quote() {
///             Some(MarketState::Exact(q)) if q.body.feed_state == 2 => {
///                 Some(q.body.ask_price_ticks - q.body.bid_price_ticks)
///             }
///             _ => None,
///         };
///         Ok(None)
///     }
/// }
/// ```
pub trait Strategy {
    /// Invoked on each drained market update once the instrument view is ready.
    fn on_market(&mut self, view: MarketView<'_>, now_ns: u64) -> Result<Option<OutboundIntent>>;

    /// Invoked on each inbound 512-byte `ReportRecord` (`FrontAck`, `Ack`, `Nack`, `CancelNack`,
    /// `Fill` [`Execution`, `FeeAmendment`, `Projected`], `Canceled`, `Modified`,
    /// `ModifyRejected`, `ExchRateLimit`, `OrsShutDown`, `OrsRestricted`, `RfqQuoteAck`,
    /// `RfqQuoteNack`).
    fn on_report(
        &mut self,
        _report: &ExecutionReport,
        _now_ns: u64,
    ) -> Result<Option<OutboundIntent>> {
        Ok(None)
    }

    /// Optional volatile cumulative-quantity fill projection callback.
    fn on_projected_fill(
        &mut self,
        _fill: &tickwire_account::ProjectedFill,
        _now_ns: u64,
    ) -> Result<Option<OutboundIntent>> {
        Ok(None)
    }
}

/// Built-in stateless observer strategy that never emits orders.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObserveOnly;

impl Strategy for ObserveOnly {
    #[forbid(unsafe_code)]
    fn on_market(&mut self, _: MarketView<'_>, _: u64) -> Result<Option<OutboundIntent>> {
        Ok(None)
    }
}

/// Explicit opt-in single-shot demonstration strategy. Complete order parameters are supplied by
/// the static startup configuration; this strategy never invents credentials or persists disk state.
#[derive(Clone, Debug)]
pub struct FixedDemo {
    command: Option<CanonicalCommand>,
    strat_id: u8,
    account_ref: u8,
    minimum_bid_ticks: i64,
    enabled: bool,
}

impl FixedDemo {
    #[forbid(unsafe_code)]
    pub fn new(
        command: CanonicalCommand,
        strat_id: u8,
        account_ref: u8,
        minimum_bid_ticks: i64,
        enabled: bool,
    ) -> Self {
        Self {
            command: Some(command),
            strat_id,
            account_ref,
            minimum_bid_ticks,
            enabled,
        }
    }
}

impl Strategy for FixedDemo {
    #[forbid(unsafe_code)]
    fn on_market(&mut self, view: MarketView<'_>, now: u64) -> Result<Option<OutboundIntent>> {
        if !self.enabled || !view.live() {
            return Ok(None);
        }
        let Some(template) = &self.command else {
            return Ok(None);
        };
        if template.command().context.instrument.instrument_id != view.instrument()
            || view
                .best_bid()
                .is_none_or(|l| l.price_ticks < self.minimum_bid_ticks)
            || now >= template.command().context.request.deadline
        {
            return Ok(None);
        }
        let mut command = template.command().clone();
        command.context.strategy.created_mono_ns = now;
        command.context.triggers = tickwire_venue::BoundedList::new(vec![view.trigger_at(now)])?;
        let canonical = CanonicalCommand::new(command)?;
        let intent = OutboundIntent::from_canonical_command(
            &canonical,
            self.strat_id,
            self.account_ref,
            view.tick_mono_ns(),
        )?;
        self.command = None;
        Ok(Some(intent))
    }
}

#[cfg(test)]
mod test_command;
