//! Qualified same-process capture, installation, and supervised live owner.
use super::*;
use std::collections::BTreeMap;
use tickwire_release::{Hash, Object};
use tickwire_runtime::node::{InstalledRebuild, PreparedRebuild, RebuildInput, Strategy};
use tickwire_strategy_sdk::capture::install;
mod delegate;
mod finish;
mod input;
mod promote;

pub struct Launch {
    pub capture: capture::Launch,
    pub bootstrap: Bootstrap,
    pub publication_directory: PathBuf,
}
/// A private deployment request to fix the actual account cut. It grants no
/// trading, installation, or publication authority.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealTarget {
    pub schema: u16,
    pub capture: [u8; 16],
    pub account_position: u64,
}
pub struct Hosted<R> {
    capture: Option<capture::Owner>,
    live: Option<RemoteHostedStrategy<R>>,
    resolver: Option<R>,
    policy: ControlPlane,
    bootstrap: Bootstrap,
    spec: LaunchSpec,
    resources: Option<ProcessResources>,
    publication: PathBuf,
    error: Option<Error>,
    next_scan: Instant,
    installation: Option<install::Request>,
    captured_bootstrap: tickwire_strategy_sdk::capture::Bootstrap,
}
impl<R: RecoveryResolver + 'static> Hosted<R> {
    /// Samples the same Registry Entry before and after same-process promotion.
    pub fn observe_resources(&mut self) -> Result<tickwire_supervisor::LiveResourceObservation> {
        if let Some(owner) = &mut self.capture {
            return owner.observe_resources();
        }
        self.live
            .as_mut()
            .ok_or(Error::NotReady)?
            .observe_resources()
    }

    /// # Safety
    /// Same qualified SHM mapping/cursor obligations as `capture::Owner::launch`.
    /// The supervisor retains the actual child throughout capture and live use.
    #[allow(unsafe_code)]
    pub unsafe fn launch(config: Launch, resolver: R, policy: ControlPlane) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::symlink_metadata(&config.publication_directory)?;
        if !config.publication_directory.is_absolute()
            || !meta.is_dir()
            || meta.mode() & 0o077 != 0
            || std::fs::canonicalize(&config.publication_directory)? != config.publication_directory
            || !config.bootstrap.handover_enabled
            || config.bootstrap.report.path != config.capture.bootstrap.report.path
            || config.bootstrap.market_admission_ready.as_ref()
                != Some(&config.capture.bootstrap.market_admission_ready)
            || config.bootstrap.report_header != config.capture.bootstrap.report_header
            || config.bootstrap.report.peer_pid != std::process::id()
        {
            return Err(Error::Invalid);
        }
        let spec = config.capture.spec.clone();
        let resources = config.capture.resources.clone();
        // SAFETY: preserves the caller's qualified-build and immutable-mapping contract.
        let owner = unsafe { capture::Owner::launch_mode(config.capture, true)? };
        let captured_bootstrap = owner.bootstrap().clone();
        Ok(Self {
            capture: Some(owner),
            live: None,
            resolver: Some(resolver),
            policy,
            bootstrap: config.bootstrap,
            spec,
            resources,
            publication: config.publication_directory,
            error: None,
            next_scan: Instant::now(),
            installation: None,
            captured_bootstrap,
        })
    }
    pub fn instance(&self) -> ([u8; 16], u64) {
        (self.spec.identity.instance, self.spec.identity.generation)
    }
    pub fn process(&mut self) -> Result<ProcessIdentity> {
        if let Some(owner) = &mut self.capture {
            owner.current()?;
            return Ok(owner.process());
        }
        self.live
            .as_mut()
            .ok_or(Error::NotReady)?
            .registry
            .process(self.spec.identity.instance)
            .map_err(Into::into)
    }
    pub fn capture_bootstrap(&self) -> &tickwire_strategy_sdk::capture::Bootstrap {
        &self.captured_bootstrap
    }
    pub fn captured(&self) -> Option<&capture::Captured> {
        self.capture.as_ref()?.captured()
    }
    pub fn seal_capture(&mut self) -> Result<&capture::Captured> {
        let owner = self.capture.as_mut().ok_or(Error::NotReady)?;
        owner.seal()?;
        while owner.captured().is_none() {
            owner.poll()?;
            std::thread::sleep(Duration::from_millis(1));
        }
        owner.captured().ok_or(Error::NotReady)
    }
    /// Drive the bounded control lane without dispatching a trading intent.
    pub fn poll(&mut self) -> Result<()> {
        let result = if let Some(live) = &mut self.live {
            live.maintain()
        } else {
            self.poll_capture()
        };
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                let message = e.to_string();
                self.close::<()>(Err(e));
                Err(Error::Io(std::io::Error::other(message)))
            }
        }
    }
    pub fn set_shadow(&mut self) {
        if let Some(live) = &mut self.live {
            live.set_shadow();
        }
    }
    pub fn failed(&self) -> bool {
        self.error.is_some()
            || self.capture.as_ref().is_some_and(capture::Owner::failed)
            || self.live.as_ref().is_some_and(RemoteHostedStrategy::failed)
    }
    pub fn stop(&mut self) -> Result<()> {
        if let Some(owner) = &mut self.capture {
            return owner.stop().map(|_| ());
        }
        let live = self.live.as_mut().ok_or(Error::NotReady)?;
        live.graceful_retire()
    }
    fn close<T>(&mut self, result: Result<T>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(e) => {
                if let Some(live) = &mut self.live {
                    live.fail(e);
                } else {
                    if let Some(owner) = &mut self.capture {
                        let _ = owner.registry.terminate(self.spec.identity.instance);
                        owner.failed = true;
                    }
                    self.error = Some(e);
                }
                None
            }
        }
    }
    fn poll_capture(&mut self) -> Result<()> {
        let owner = self.capture.as_mut().ok_or(Error::NotReady)?;
        owner.poll()?;
        if owner.request.is_some() || Instant::now() < self.next_scan {
            return Ok(());
        }
        self.next_scan = Instant::now() + Duration::from_millis(10);
        let path = self.publication.join("capture-seal.json");
        let target: SealTarget = match worker::read_config(&path, 4096) {
            Ok(v) => v,
            Err(tickwire_strategy_sdk::Error::Io(e))
                if e.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        if target.schema != 1
            || target.capture != owner.bootstrap.identity.capture
            || target.account_position < owner.reports.last_position()
        {
            return Err(Error::Invalid);
        }
        if target.account_position == owner.reports.last_position() {
            owner.seal()?;
        }
        Ok(())
    }
    pub fn market_consumer(&self) -> Option<(u32, [u8; 16], u64)> {
        self.live.as_ref()?.market_consumer()
    }
    pub fn arm_market(&mut self, a: &tickwire_market_source::transition::Approval) -> Result<()> {
        self.live.as_mut().ok_or(Error::NotReady)?.arm_market(a)
    }
    pub fn commit_market(&mut self, c: &tickwire_market_source::transition::Commit) -> Result<()> {
        self.live.as_mut().ok_or(Error::NotReady)?.commit_market(c)
    }
    pub fn market_transition(&self) -> Option<&tickwire_market_source::transition::Receipt> {
        self.live.as_ref()?.market_transition()
    }
    pub fn market_rejection(&self) -> Option<u32> {
        self.live.as_ref()?.market_rejection()
    }
}
fn root(map: &BTreeMap<Hash, Vec<u8>>, hash: Hash) -> Result<Object> {
    map.get(&hash).map(|v| Object::of(v)).ok_or(Error::Invalid)
}
fn insert(map: &mut BTreeMap<Hash, Vec<u8>>, bytes: Vec<u8>) -> Result<Object> {
    let object = Object::of(&bytes);
    object.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
    if let Some(prior) = map.get(&object.digest) {
        if prior != &bytes {
            return Err(Error::Invalid);
        }
        return Ok(object);
    }
    if map.len() >= 512
        || map
            .values()
            .map(Vec::len)
            .sum::<usize>()
            .checked_add(bytes.len())
            .is_none_or(|n| n > tickwire_recovery::MAX_CLOSURE_BYTES as usize)
    {
        return Err(Error::Invalid);
    }
    map.insert(object.digest, bytes);
    Ok(object)
}
