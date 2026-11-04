//! Checkpoint schemas and configuration for registered strategies.
use crate::{FixedDemo, Strategy};
use tickwire_release::Hash;
use tickwire_venue::CanonicalCommand;
pub const OBSERVE_SCHEMA: u64 = 0x54574f420001;
pub const DEMO_V1_SCHEMA: u64 = 0x545746440001;
pub const DEMO_SCHEMA: u64 = 0x545746440002;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Unsupported,
    Identity,
    State,
    Input,
    Bounds,
    Callback,
}
pub type Result<T> = core::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// Worker identity and command settings from its launch configuration.
#[derive(Clone)]
pub struct Target {
    pub instance: [u8; 16],
    pub generation: u64,
    pub logical_strategy: u64,
    pub command: Option<CanonicalCommand>,
    pub minimum_bid_ticks: i64,
    pub enabled: bool,
}
pub trait RestoreFactory {
    type Running: Strategy;
    fn state_schema(&self) -> u64;
    /// Return the parameters bound to this worker.
    fn configuration(&self, _target: &Target) -> Result<tickwire_release::replay::VersionedBytes> {
        Err(Error::Unsupported)
    }
    fn validate_checkpoint(&self, state: &[u8]) -> Result<()>;
}

pub struct RestoredDemo {
    pub(crate) inner: FixedDemo,
    pub(crate) action: Hash,
}
impl Strategy for RestoredDemo {
    fn apply_config(
        &mut self,
        bytes: &[u8],
        context: &mut crate::replay::ReplayContext<'_>,
    ) -> crate::Result<()> {
        crate::replay::native::apply_demo(&mut self.inner, false, bytes, context)
    }
    fn replay_registration(&self) -> crate::Result<Option<crate::replay::native::Registration>> {
        Ok(Some(crate::replay::native::demo(&self.inner, DEMO_SCHEMA)?))
    }
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        let mut bytes = b"TWFD0002".to_vec();
        bytes.push(u8::from(self.inner.command.is_none()));
        bytes.extend(self.action);
        Ok(bytes)
    }
    fn on_market(
        &mut self,
        view: crate::MarketView<'_>,
        now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.inner.on_market(view, now)
    }
    fn on_report(
        &mut self,
        r: &crate::reports::AccountReport,
        n: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.inner.on_report(r, n)
    }
}
