//! Linux local trusted-directory observation requests; never configuration Applied.
//!
//! `Deployment.observation` explicitly approves three distinct 0700 directories,
//! a fixed UID and 1..=128 unique nonzero query-capable worker build hashes.
//! Request `<32-lowercase-hex-id>.json` is a bounded 0600, single-link regular file:
//! `{schema:1,request_id,session_id,binding,target:{logical_strategy_id,instance,
//! generation},valid_until_mono_ns,require_resources}`. IDs are 16-byte arrays.
//! A new durable session is published as `outbox/session.json` at every open.
//! The session is a replay boundary, not a secret or a network credential.
//!
//! Received is synchronized before dispatch. Terminal references an already synced
//! CAS object, followed by atomic publication of the same result in the outbox.
//! Shared verified dependency paths are deduplicated by digest only after checking
//! that duplicate roots name exactly the same bytes; stored roots remain unique.
//! Results retain the entire original request, its digest and deadline; `historical`
//! explicitly identifies recovered results. Expiry is always checked by consumers.
//! Recovery never re-dispatches: unfinished requests become Interrupted. Storage or
//! permission failures close only this provider. No runtime/global revision or
//! projection Applied is inferred from an observation.
//! A conflicting request cancels any unfinished original and permanently closes
//! the ID; its rejection names the conflicting digest. Completed originals remain
//! immutable historical facts. Every WAL operation checks that the held lock still
//! matches the private LOCK pathname; replacement closes the provider. The unique
//! WAL owner explicitly unlocks on drop, so a validation clone or pre-exec inherited
//! descriptor cannot prolong that owner after its lifetime has ended.
use crate::{Result, control::Binding, invalid, strategy::AppStrategy};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_release::Object;
use tickwire_runtime_policy::{ControlPlane, bootstrap as policy};
mod files;
mod inbox;
mod store;
mod types;
pub use types::*;

const MAX_REQUEST: usize = 32 * 1024;
const MAX_RESULT: usize = 256 * 1024;
const MAX_TOTAL: u64 = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 8192;
const SCAN_INTERVAL: u64 = 10_000_000;
const MAX_LIFETIME: u64 = 30_000_000_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub inbox_directory: PathBuf,
    pub outbox_directory: PathBuf,
    pub journal_directory: PathBuf,
    pub stream_epoch: [u8; 16],
    pub owner_uid: u32,
    pub allowed_build_hashes: Vec<[u8; 32]>,
    pub maximum_requests: usize,
    pub maximum_pending: usize,
    pub maximum_per_poll: usize,
    pub recover: bool,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.stream_epoch == [0; 16]
            || !(1..=1024).contains(&self.maximum_requests)
            || !(1..=32).contains(&self.maximum_pending)
            || self.maximum_pending > self.maximum_requests
            || !(1..=16).contains(&self.maximum_per_poll)
            || !(1..=128).contains(&self.allowed_build_hashes.len())
            || self
                .allowed_build_hashes
                .iter()
                .enumerate()
                .any(|(i, h)| *h == [0; 32] || self.allowed_build_hashes[..i].contains(h))
            || self.inbox_directory == self.outbox_directory
            || self.inbox_directory == self.journal_directory
            || self.outbox_directory == self.journal_directory
        {
            return Err(invalid("observation configuration bounds or identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub schema: u16,
    pub session_id: [u8; 16],
    pub session_number: u64,
    pub binding: Binding,
    pub started_mono_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Session {
        config_digest: [u8; 32],
        session: Session,
    },
    Received {
        request: Request,
        admitted_mono_ns: u64,
    },
    Terminal {
        request_id: [u8; 16],
        request_digest: [u8; 32],
        result: Object,
    },
    Conflict {
        request_id: [u8; 16],
        result: Object,
    },
}
struct Entry {
    request: Request,
    digest: [u8; 32],
    terminal: Option<Object>,
    conflict: Option<Object>,
    pending: bool,
}

pub struct ObservationInbox {
    config: Config,
    binding: Binding,
    binding_bytes: Vec<u8>,
    domain: ClockDomain,
    plane: ControlPlane,
    session: Session,
    inbox: files::Directory,
    outbox: files::Directory,
    storage: store::Storage,
    entries: BTreeMap<[u8; 16], Entry>,
    next_scan: u64,
    scan_cursor: usize,
    pending_cursor: usize,
    last_now: u64,
    closed: Option<String>,
}
impl ObservationInbox {
    pub fn session(&self) -> &Session {
        &self.session
    }
    pub fn error(&self) -> Option<&str> {
        self.closed.as_deref()
    }
    pub fn open(
        config: Config,
        policy: &policy::Config,
        plane: ControlPlane,
        clock: &mut tickwire_binance_clock::SystemClock,
    ) -> Result<Self> {
        Self::open_with(config, policy, plane, || {
            clock
                .sample()
                .map(|v| v.monotonic)
                .map_err(|e| invalid(&format!("observation clock: {e:?}")))
        })
    }
    pub fn poll(
        &mut self,
        strategy: &mut dyn AppStrategy,
        clock: &mut tickwire_binance_clock::SystemClock,
    ) -> Result<usize> {
        self.poll_with(strategy, || {
            clock
                .sample()
                .map(|v| v.monotonic)
                .map_err(|e| invalid(&format!("observation clock: {e:?}")))
        })
    }
}
#[cfg(all(test, target_os = "linux"))]
mod tests;
