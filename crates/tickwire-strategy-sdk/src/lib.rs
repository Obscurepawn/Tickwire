//! Strategy-owned market replica and canonical-command SPSC publication.
#![deny(unsafe_code)]
pub mod bootstrap;
pub mod capture;
mod capture_worker;
pub mod checkpoint;
pub mod handover;
pub mod market;
pub mod market_transition;
pub mod outbound;
pub mod preflight;
pub mod recovery;
pub mod registered;
pub mod replay;
#[allow(unsafe_code)]
pub mod reports;
pub mod restore;
pub mod retirement;
#[cfg(feature = "runtime")]
#[allow(unsafe_code)]
pub mod runtime;
#[cfg(test)]
#[path = "../tests/common/market_transition.rs"]
mod transition_fixture;
#[cfg(test)]
use tickwire_market_source::transition;
#[allow(unsafe_code)]
pub mod worker;
pub use market::{MarketReplica, MarketView};
pub use outbound::{CommandStore, IntentWriter, StoredCommand};
use tickwire_venue::CanonicalCommand;

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
    Storage(tickwire_recovery::Error),
    Command(tickwire_venue::Error),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    Clock(tickwire_clock_sys::Error),
    Release(tickwire_release::Error),
    Restore(restore::Error),
    Replay(replay::Error),
    Identity,
    Bounds,
    Stale,
    NotReady,
    Deadline,
}
impl Error {
    pub(crate) fn context(self, stage: &'static str) -> Self {
        Self::Context {
            stage,
            source: Box::new(self),
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
macro_rules! from {
    ($t:ty,$v:ident) => {
        impl From<$t> for Error {
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
from!(tickwire_recovery::Error, Storage);
from!(tickwire_venue::Error, Command);
from!(tickwire_control::Error, Control);
from!(tickwire_control_sys::Error, Transport);
from!(std::io::Error, Io);
from!(serde_json::Error, Json);
from!(tickwire_clock_sys::Error, Clock);
from!(tickwire_release::Error, Release);
from!(restore::Error, Restore);
from!(replay::Error, Replay);
pub trait Strategy {
    /// A registered target factory executes migration at the verified old cut.
    /// Returning a candidate grants neither installation nor output authority.
    fn prepare_replayed(
        &self,
        _release: &tickwire_release::MemberRelease<'_>,
        _target: restore::Target,
        _proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        _source: tickwire_release::replay::Source,
        _config: tickwire_release::replay::VersionedBytes,
    ) -> replay::Result<replay::install::Candidate<Self>>
    where
        Self: Sized,
    {
        Err(replay::Error::Unsupported)
    }

    /// None preserves the old callback API, without claiming complete-tail replay.
    fn replay_registration(&self) -> Result<Option<replay::native::Registration>> {
        Ok(None)
    }
    /// Deterministic callback variants used by the versioned input owner.
    fn on_market_recorded(
        &mut self,
        view: MarketView<'_>,
        context: &mut replay::ReplayContext<'_>,
    ) -> Result<Option<CanonicalCommand>> {
        self.on_market(view, context.now_ns())
    }
    fn on_report_recorded(
        &mut self,
        report: &reports::AccountReport,
        context: &mut replay::ReplayContext<'_>,
    ) -> Result<Option<CanonicalCommand>> {
        self.on_report(report, context.now_ns())
    }
    fn on_timer(
        &mut self,
        _id: u64,
        _generation: u64,
        _context: &mut replay::ReplayContext<'_>,
    ) -> Result<Option<CanonicalCommand>> {
        Err(Error::NotReady)
    }
    fn on_external(
        &mut self,
        _source: &tickwire_release::replay::StreamKey,
        _schema: u64,
        _bytes: &[u8],
        _context: &mut replay::ReplayContext<'_>,
    ) -> Result<Option<CanonicalCommand>> {
        Err(Error::NotReady)
    }
    fn apply_config(
        &mut self,
        _bytes: &[u8],
        _context: &mut replay::ReplayContext<'_>,
    ) -> Result<()> {
        Err(Error::NotReady)
    }
    fn apply_model(
        &mut self,
        _id: u16,
        _bytes: &[u8],
        _context: &mut replay::ReplayContext<'_>,
    ) -> Result<()> {
        Err(Error::NotReady)
    }
    /// Exact initial strategy state for its approved replay/checkpoint schema.
    fn checkpoint(&self) -> Result<Vec<u8>> {
        Err(Error::NotReady)
    }
    /// Ordered execution-owned account baseline and durable records. The worker
    /// retains the exact canonical object as a recovery input before this call.
    fn on_report(
        &mut self,
        _report: &crate::reports::AccountReport,
        _now_ns: u64,
    ) -> Result<Option<CanonicalCommand>> {
        Ok(None)
    }
    fn on_market(&mut self, view: MarketView<'_>, now_ns: u64) -> Result<Option<CanonicalCommand>>;
}
#[derive(Default)]
pub struct ObserveOnly;
impl Strategy for ObserveOnly {
    fn prepare_replayed(
        &self,
        release: &tickwire_release::MemberRelease<'_>,
        target: restore::Target,
        proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        source: tickwire_release::replay::Source,
        config: tickwire_release::replay::VersionedBytes,
    ) -> replay::Result<replay::install::Candidate<Self>> {
        replay::install::prepare(
            &restore::ObserveFactory,
            release,
            target,
            proof,
            source,
            config,
        )
    }
    fn apply_config(
        &mut self,
        bytes: &[u8],
        context: &mut replay::ReplayContext<'_>,
    ) -> Result<()> {
        replay::native::apply_observe(bytes, context)
    }
    fn replay_registration(&self) -> Result<Option<replay::native::Registration>> {
        Ok(Some(replay::native::observe()))
    }
    fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(b"TWOBS001".to_vec())
    }
    fn on_market(&mut self, _: MarketView<'_>, _: u64) -> Result<Option<CanonicalCommand>> {
        Ok(None)
    }
}
/// Explicit opt-in, single-shot demonstration. The complete order and authority
/// context are supplied by the launcher; this strategy never invents credentials.
pub struct FixedDemo {
    command: Option<CanonicalCommand>,
    original: CanonicalCommand,
    minimum_bid_ticks: i64,
    enabled: bool,
}
impl FixedDemo {
    pub fn new(command: CanonicalCommand, minimum_bid_ticks: i64, enabled: bool) -> Self {
        Self {
            original: command.clone(),
            command: Some(command),
            minimum_bid_ticks,
            enabled,
        }
    }
}
impl Strategy for FixedDemo {
    fn apply_config(
        &mut self,
        bytes: &[u8],
        context: &mut replay::ReplayContext<'_>,
    ) -> Result<()> {
        replay::native::apply_demo(self, true, bytes, context)
    }
    fn replay_registration(&self) -> Result<Option<replay::native::Registration>> {
        Ok(Some(replay::native::demo(self, restore::DEMO_V1_SCHEMA)?))
    }
    fn checkpoint(&self) -> Result<Vec<u8>> {
        let mut b = b"TWFD0001".to_vec();
        b.push(u8::from(self.enabled && self.command.is_some()));
        b.extend(self.minimum_bid_ticks.to_le_bytes());
        let cmd = self.command.as_ref().unwrap_or(&self.original);
        b.extend((cmd.bytes().len() as u32).to_le_bytes());
        b.extend(cmd.bytes());
        Ok(b)
    }
    fn on_market(&mut self, view: MarketView<'_>, now: u64) -> Result<Option<CanonicalCommand>> {
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
        let out = CanonicalCommand::new(command)?;
        self.command = None;
        Ok(Some(out))
    }
}

#[cfg(test)]
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[cfg(test)]
mod test_command;
