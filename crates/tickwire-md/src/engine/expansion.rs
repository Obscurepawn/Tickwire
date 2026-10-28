//! Single in-flight, launcher-authorized expansion driven beside the live owners.
use super::*;
use crate::expansion::{Plan, Prepared};
use crate::server::Admission;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_market_publisher::TopicExpansion;
use tickwire_market_source::transition::{Approval, Commit, MAX_CONTROL_BYTES, Phase, Receipt};
mod install;
mod io;
mod warming;

const SCAN_NS: u64 = 10_000_000;
#[derive(Clone, Serialize, Deserialize)]
pub struct Effective {
    pub schema: u16,
    pub revision: u64,
    pub config_hash: [u8; 32],
    pub config: Config,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub schema: u16,
    pub operation_id: [u8; 16],
    pub approval_digest: [u8; 32],
    pub revision: u64,
    pub config_hash: [u8; 32],
    pub status: String,
    pub runtime_applied: bool,
    pub effective_installed: bool,
    pub bank_descriptor_hash: Option<[u8; 32]>,
    pub error: Option<String>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Building,
    Warming,
    Arming,
    Quiescing,
    Installing,
    Committing,
    Rejoining,
}
#[derive(Clone, Copy)]
enum Action {
    Plan,
    Build,
    Arm,
    Armed,
    Install,
    Commit,
    Activated,
    Report,
}
struct Flight {
    started_at: u64,
    prepared: Option<Prepared>,
    plan: Plan,
    topic: usize,
    registry: TopicRegistry,
    candidate: Option<TopicExpansion>,
    additions: Vec<owners::Built>,
    approvals: Vec<Approval>,
    next_admissions: Vec<Admission>,
    old_routes: Vec<(usize, InstrumentSlot)>,
    old_event_routes: Vec<(usize, InstrumentSlot)>,
    stage: Stage,
    irreversible: bool,
    record: Record,
}
pub(super) struct Control {
    config: config::ExpansionControl,
    worker: cold::Worker,
    action: Option<Action>,
    flight: Option<Flight>,
    reporting: Option<Record>,
    history: BTreeMap<[u8; 16], Record>,
    last_bytes: Option<[u8; 32]>,
    next_scan: u64,
}
fn encode<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > maximum {
        return Err(invalid("MD expansion file capacity"));
    }
    Ok(bytes)
}
impl Control {
    pub(super) fn new(config: config::ExpansionControl, current: &Config) -> Result<Self> {
        config.validate()?;
        match config::read(&config.effective_path, crate::expansion::MAX_PLAN_BYTES) {
            Ok(bytes) => {
                let effective: Effective = serde_json::from_slice(&bytes)?;
                if effective.schema != 1
                    || effective.revision != config.initial_revision
                    || effective.config_hash != crate::expansion::config_hash(current)?
                    || crate::expansion::config_hash(&effective.config)? != effective.config_hash
                {
                    return Err(invalid("MD effective startup identity"));
                }
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        Ok(Self {
            config,
            worker: cold::Worker::new()?,
            action: None,
            flight: None,
            reporting: None,
            history: BTreeMap::new(),
            last_bytes: None,
            next_scan: 0,
        })
    }
    fn submit(&mut self, action: Action, task: cold::Task) -> Result<()> {
        self.worker.submit(task)?;
        self.action = Some(action);
        Ok(())
    }
    pub(super) fn poll(&mut self, runtime: &mut Runtime, now: u64) -> Result<bool> {
        if let Some(result) = self.worker.poll()? {
            let action = self
                .action
                .take()
                .ok_or_else(|| invalid("MD cold reply without request"))?;
            if let Err(error) = self.reply(action, result, runtime, now) {
                self.fail(runtime, error.to_string())?;
            }
        }
        if let Some(mut flight) = self.flight.take() {
            let result = if now >= flight.plan.warmup_deadline_mono_ns {
                Err(invalid("MD expansion deadline"))
            } else {
                self.advance(&mut flight, runtime, now)
            };
            self.flight = Some(flight);
            if let Err(error) = result {
                self.fail(runtime, error.to_string())?;
            }
        }
        if !self.worker.busy() {
            if let Some(record) = &self.reporting {
                self.submit(
                    Action::Report,
                    cold::Task::Write(vec![(
                        self.config.result_path.clone(),
                        encode(record, MAX_CONTROL_BYTES)?,
                    )]),
                )?;
            } else if self.flight.is_none() && now >= self.next_scan {
                self.next_scan = now.saturating_add(SCAN_NS);
                self.submit(
                    Action::Plan,
                    cold::Task::Read(vec![(
                        self.config.plan_path.clone(),
                        crate::expansion::MAX_PLAN_BYTES,
                    )]),
                )?;
            }
        }
        Ok(false)
    }
    fn fail(&mut self, runtime: &mut Runtime, message: String) -> Result<()> {
        if let Some(mut flight) = self.flight.take() {
            if flight.irreversible {
                runtime.server.abort_expansion(
                    flight.topic,
                    flight.plan.operation_id,
                    &mut runtime.topics[flight.topic],
                )?;
                flight.record.status = "failed_closed".into();
            } else {
                flight.record.status = "rejected".into();
            }
            flight.record.error = Some(message);
            self.history
                .insert(flight.plan.operation_id, flight.record.clone());
            self.reporting = Some(flight.record);
        } else {
            // Invalid requests have no runtime effect; never report them as applied.
            eprintln!(
                "{}",
                serde_json::json!({"event":"md_expansion_rejected","error":message})
            );
            if self.reporting.is_some() {
                return Err(invalid("MD expansion result persistence failed"));
            }
        }
        Ok(())
    }
    fn advance(&mut self, flight: &mut Flight, runtime: &mut Runtime, now: u64) -> Result<()> {
        if !flight.record.runtime_applied && flight.stage != Stage::Building {
            warming::poll(flight, runtime)?;
        }
        let now = runtime.clock.sample()?.monotonic.nanos().max(now);
        if self.worker.busy() {
            return Ok(());
        }
        match flight.stage {
            Stage::Warming
                if flight
                    .candidate
                    .as_ref()
                    .is_some_and(|c| c.ready(now, flight.plan.maximum_age_ns)) =>
            {
                let files = flight
                    .approvals
                    .iter()
                    .zip(&flight.plan.consumers)
                    .map(|(a, c)| {
                        Ok((
                            self.config.consumer_path(
                                c.previous.pid,
                                a.previous.endpoint.local.instance,
                                "arm",
                            ),
                            encode(a, MAX_CONTROL_BYTES)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                flight.stage = Stage::Arming;
                self.submit(Action::Arm, cold::Task::Write(files))?;
            }
            Stage::Arming if now >= self.next_scan => {
                self.read_receipts(flight, Action::Armed, now)?
            }
            Stage::Quiescing => {
                if runtime.server.expansion_quiesced(
                    flight.topic,
                    flight.plan.operation_id,
                    &mut runtime.topics[flight.topic],
                )? {
                    self.install(flight, runtime, now)?;
                }
            }
            Stage::Rejoining if now >= self.next_scan => {
                self.read_receipts(flight, Action::Activated, now)?
            }
            _ => {}
        }
        Ok(())
    }
    fn read_receipts(&mut self, flight: &Flight, action: Action, now: u64) -> Result<()> {
        self.next_scan = now.saturating_add(SCAN_NS);
        let files = flight
            .plan
            .consumers
            .iter()
            .map(|c| {
                (
                    self.config.consumer_path(
                        c.previous.pid,
                        c.previous.client.endpoint.local.instance,
                        "receipt",
                    ),
                    MAX_CONTROL_BYTES,
                )
            })
            .collect();
        self.submit(action, cold::Task::Read(files))
    }
}
