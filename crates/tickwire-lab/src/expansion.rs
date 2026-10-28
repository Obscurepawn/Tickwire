//! Explicit loopback experiment driver; production MD and consumers produce all proofs.
use crate::{Result, Scenario};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    os::unix::fs::DirBuilderExt,
    path::Path,
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    protocol::{Instrument, wire_bytes},
    transition::{Approval, Commit, Phase, Receipt},
};
use tickwire_md::{
    config::{Config, ExpansionControl, FeedIndex},
    expansion::{Consumer, Plan, config_hash},
    server::Admission,
};
use tickwire_recovery::digest;
fn md<T>(result: tickwire_md::Result<T>) -> Result<T> {
    result.map_err(|e| e.to_string().into())
}
fn read<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err("expansion file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("pending");
    crate::generate::write(&temporary, bytes)?;
    fs::rename(temporary, path)?;
    File::open(path.parent().ok_or("expansion parent")?)?.sync_all()?;
    Ok(())
}
fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    atomic(path, &serde_json::to_vec(value)?)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn now(s: &Scenario) -> Result<tickwire_clock::MonoTime> {
    Ok(tickwire_clock_sys::SystemClock::new(
        tickwire_clock::ClockDomain::new(s.boot, 1).map_err(|_| "expansion clock")?,
    )?
    .sample()?
    .monotonic)
}
pub(crate) fn configure(s: &Scenario) -> Result<()> {
    for name in [
        "expansion",
        "expansion/consumers",
        "expansion/consumer-effective",
    ] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(s.root.join(name))?;
    }
    let mut config: Config = read(&s.md_config)?;
    config.expansion = Some(ExpansionControl {
        recovery: None,
        plan_path: s.root.join("expansion/plan.json"),
        result_path: s.root.join("expansion/result.json"),
        effective_path: s.root.join("expansion/md-effective.json"),
        consumer_directory: s.root.join("expansion/consumers"),
        initial_revision: 1,
    });
    write(&s.md_config, &config)?;
    let mut app: Value = read(&s.app_config)?;
    app["market_transition"] = json!({"consumer_directory":s.root.join("expansion/consumers"),"effective_directory":s.root.join("expansion/consumer-effective"),"maximum_operations":16,"maximum_per_poll":16});
    write(&s.app_config, &app)?;
    let path = s.root.join("strategy/recovery-template.json");
    let mut template: tickwire_strategy_sdk::recovery::Template = read(&path)?;
    let dependency = template
        .dependencies
        .iter_mut()
        .find(|d| d.kind == 9)
        .ok_or("source configuration dependency")?;
    let bytes = serde_json::to_vec(&app)?;
    atomic(&dependency.path, &bytes)?;
    dependency.digest = digest(&bytes);
    write(&path, &template)?;
    Ok(())
}
#[derive(Clone, Serialize)]
struct Process {
    pid: u32,
    start_ticks: u64,
}
impl Process {
    fn read(pid: u32) -> Result<Self> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let tail = stat
            .rsplit_once(')')
            .ok_or("process stat")?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        if tail.first() == Some(&"Z") {
            return Err("expansion process became zombie".into());
        }
        Ok(Self {
            pid,
            start_ticks: tail.get(19).ok_or("process start time")?.parse()?,
        })
    }
    fn unchanged(&self) -> Result<()> {
        if Self::read(self.pid)?.start_ticks != self.start_ticks {
            return Err("expansion process restarted".into());
        }
        Ok(())
    }
}
pub(crate) struct Driver {
    scenario: Scenario,
    md: Process,
    exec: Process,
    workers: Vec<Process>,
    plan: Option<Plan>,
    evidence: Option<Value>,
}
impl Driver {
    pub fn new(s: &Scenario, exec: u32, md: u32) -> Result<Self> {
        Ok(Self {
            scenario: s.clone(),
            md: Process::read(md)?,
            exec: Process::read(exec)?,
            workers: vec![],
            plan: None,
            evidence: None,
        })
    }
    pub fn evidence(&self) -> Result<&Value> {
        self.evidence
            .as_ref()
            .ok_or_else(|| "market expansion has not been verified".into())
    }
    pub fn poll(&mut self) -> Result<()> {
        if self.evidence.is_some() || !self.scenario.root.join("ARM").exists() {
            return Ok(());
        }
        self.md.unchanged()?;
        self.exec.unchanged()?;
        if self.plan.is_none() {
            self.publish_plan()?;
            return Ok(());
        }
        for p in &self.workers {
            p.unchanged()?;
        }
        let plan = self.plan.as_ref().unwrap();
        let control = plan
            .target_config
            .expansion
            .as_ref()
            .ok_or("expansion control")?;
        if !control.result_path.is_file() {
            return Ok(());
        }
        let record: tickwire_md::engine::expansion::Record = read(&control.result_path)?;
        if record.status != "complete" {
            return Err(format!(
                "market expansion {}: {}",
                record.status,
                record.error.unwrap_or_default()
            )
            .into());
        }
        let effective: tickwire_md::engine::expansion::Effective = read(&control.effective_path)?;
        let hash = digest(&serde_json::to_vec(plan)?);
        let target = md(config_hash(&plan.target_config))?;
        if record.schema != 1
            || record.operation_id != plan.operation_id
            || record.approval_digest != hash
            || record.revision != 2
            || record.config_hash != target
            || !record.runtime_applied
            || !record.effective_installed
            || effective.schema != 1
            || effective.revision != record.revision
            || effective.config_hash != target
            || md(config_hash(&effective.config))? != target
        {
            return Err("market expansion runtime/effective identity".into());
        }
        let bank = ResourceHeader::decode(&fs::read(md(
            tickwire_md::config::active_resource_path(&effective.config.topics[0].resource),
        )?)?)?;
        if record.bank_descriptor_hash != Some(bank.descriptor_hash)
            || bank.ring_epoch != 2
            || bank.feed_generation != 2
        {
            return Err("market expansion actual bank".into());
        }
        let at = now(&self.scenario)?;
        let mut receipts = Vec::new();
        let mut frontier = 0;
        for c in &plan.consumers {
            let stem = format!(
                "{}-{}",
                c.previous.pid,
                hex(&c.previous.client.endpoint.local.instance)
            );
            let approval: Approval =
                read(&control.consumer_directory.join(format!("{stem}.arm.json")))?;
            let receipt: Receipt = read(
                &control
                    .consumer_directory
                    .join(format!("{stem}.receipt.json")),
            )?;
            approval.validate(&c.previous.client, at)?;
            approval.validate_receipt(&receipt, c.previous.pid, Phase::Activated, at)?;
            if approval.approval_digest != hash
                || approval.operation_id != plan.operation_id
                || serde_json::to_vec(&approval.next_template)?
                    != serde_json::to_vec(&c.next.client)?
                || receipt.next_descriptor_hash != Some(bank.descriptor_hash)
            {
                return Err("consumer activation approval".into());
            }
            let local: Value = read(
                &self
                    .scenario
                    .root
                    .join(format!("expansion/consumer-effective/{stem}.json")),
            )?;
            let commit: Commit = serde_json::from_value(local["commit"].clone())?;
            if local["schema"] != 1
                || local["approval_hash"] != json!(approval.digest()?)
                || local["receipt"] != json!(receipt)
                || commit.validate(
                    &approval,
                    receipt.previous_applied.ok_or("old watermark")?,
                    at,
                )? != bank
            {
                return Err("consumer effective differs from actual activation".into());
            }
            frontier = frontier.max(receipt.activated_sequence.ok_or("activation sequence")?);
            receipts.push(json!({"pid":c.previous.pid,"instance":c.previous.client.endpoint.local.instance,"receipt":receipt,"effective":local}));
        }
        let Some(source) = self.source_frontier(&bank, frontier)? else {
            return Ok(());
        };
        let evidence = json!({"schema":1,"operation_id":plan.operation_id,"md_record":record,"md_effective":effective,"consumer_evidence":receipts,"md_process":self.md,"exec_process":self.exec,"strategy_processes":self.workers,"same_processes":true,"activated_frontier":frontier,"source_before_c":source,"new_instrument_id":u64::MAX,"source_s_precedes_expansion":true});
        write(&self.scenario.root.join("market-expanded.json"), &evidence)?;
        self.evidence = Some(evidence);
        Ok(())
    }
    fn publish_plan(&mut self) -> Result<()> {
        let s = &self.scenario;
        let config: Config = read(&s.md_config)?;
        let mut expected = vec![(self.exec.pid, [4; 16])];
        for (dir, instance, generation) in [
            ("exec", [2; 16], 5),
            ("next", crate::handover::NEW_INSTANCE, 6),
        ] {
            let receipt: Value = read(&s.root.join(format!(
                "{dir}/reports/CONSUMED-{}-{generation}.json",
                hex(&instance)
            )))?;
            if receipt["host_pid"] != self.exec.pid
                || receipt["instance"] != json!(instance)
                || receipt["generation"] != generation
                || receipt["host_boot"] != json!(s.boot)
            {
                return Err("expansion actual worker identity".into());
            }
            let pid = u32::try_from(receipt["worker_pid"].as_u64().ok_or("worker PID")?)?;
            self.workers.push(Process::read(pid)?);
            expected.push((pid, instance));
        }
        let mut admissions = vec![];
        for entry in fs::read_dir(&config.admission_directory)? {
            let path = entry?.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let a: Admission = read(&path)?;
            if expected.contains(&(a.pid, a.client.endpoint.local.instance)) {
                admissions.push(a);
            }
        }
        if admissions.len() != 3
            || expected.iter().any(|pair| {
                admissions
                    .iter()
                    .filter(|a| (a.pid, a.client.endpoint.local.instance) == *pair)
                    .count()
                    != 1
            })
        {
            return Err("expansion requires three exact current admissions".into());
        }
        let bank = ResourceHeader::decode(&fs::read(md(
            tickwire_md::config::active_resource_path(&config.topics[0].resource),
        )?)?)?;
        let plan = make_plan(&config, &bank, admissions, now(s)?.nanos())?;
        write(
            &config
                .expansion
                .as_ref()
                .ok_or("missing MD expansion control")?
                .plan_path,
            &plan,
        )?;
        self.plan = Some(plan);
        Ok(())
    }
    fn source_frontier(&self, bank: &ResourceHeader, minimum: u64) -> Result<Option<Value>> {
        let root = &self.scenario.root;
        let ready: Value =
            read(&root.join(format!("exec/reports/CONSUMED-{}-5.json", hex(&[2; 16]))))?;
        if ready["worker_pid"] != self.workers[0].pid || ready["host_pid"] != self.exec.pid {
            return Err("expansion source callback identity".into());
        }
        let hash: [u8; 32] = serde_json::from_value(ready["input_seal_digest"].clone())?;
        let object = |hash: [u8; 32]| -> Result<Vec<u8>> {
            let bytes = fs::read(root.join(format!("strategy/recovery/{}.bin", hex(&hash))))?;
            if digest(&bytes) != hash {
                return Err("expansion source object digest".into());
            }
            Ok(bytes)
        };
        let seal = tickwire_recovery::HandoverSeal::decode(&object(hash)?)?;
        if seal.instance != [2; 16]
            || seal.generation != 5
            || ready["checkpoint_digest"] != json!(seal.checkpoint_digest)
            || ready["account_position"] != seal.report_position
        {
            return Err("expansion source seal".into());
        }
        let bytes = object(seal.input_digest)?;
        if bytes.len() as u64 != seal.input_bytes {
            return Err("expansion source input length".into());
        }
        let retained = tickwire_release::replay::retained::decode(&bytes)?;
        let stream = tickwire_strategy_sdk::replay::application::market_stream(bank)?;
        let Some(cursor) = retained
            .current()
            .runtime
            .markets
            .iter()
            .find(|c| c.stream == stream && c.sequence >= minimum)
        else {
            return Ok(None);
        };
        let fixed: Value = read(&root.join("handover/source-consumed.json"))?;
        let fixed_hash: [u8; 32] = serde_json::from_value(fixed["input_seal_digest"].clone())?;
        let fixed_seal = tickwire_recovery::HandoverSeal::decode(&object(fixed_hash)?)?;
        let fixed_inputs = object(fixed_seal.input_digest)?;
        let fixed_retained = tickwire_release::replay::retained::decode(&fixed_inputs)?;
        let previous =
            ResourceHeader::decode(&self.plan.as_ref().ok_or("source expansion plan")?.base_bank)?;
        let previous_stream = tickwire_strategy_sdk::replay::application::market_stream(&previous)?;
        if fixed_seal.instance != [2; 16]
            || fixed_seal.generation != 5
            || fixed_retained.current().cut.sequence >= retained.current().cut.sequence
            || !fixed_retained
                .current()
                .runtime
                .markets
                .iter()
                .any(|c| c.stream == previous_stream)
        {
            return Err("source S does not precede the actual bank transition".into());
        }
        proof::verify_prefix(fixed_retained.current(), &retained)?;
        Ok(Some(
            json!({"ready":ready,"seal_digest":hash,"seal":tickwire_release::Object::of(&object(hash)?),"checkpoint":tickwire_release::Object::of(&retained.current().bytes()?),"replay_cut":retained.current().cut,"market_cursor":cursor,"source_s":{"seal_digest":fixed_hash,"seal":tickwire_release::Object::of(&object(fixed_hash)?),"checkpoint":tickwire_release::Object::of(&fixed_retained.current().bytes()?),"replay_cut":fixed_retained.current().cut,"market_cursors":fixed_retained.current().runtime.markets}}),
        ))
    }
}
fn make_plan(
    config: &Config,
    bank: &ResourceHeader,
    admissions: Vec<Admission>,
    at: u64,
) -> Result<Plan> {
    let mut target = config.clone();
    let mut feed = target.feeds[0].clone();
    feed.owner_id = [210; 16];
    feed.stream_id = 2;
    feed.instrument.id = u64::MAX;
    feed.instrument.symbol = "ETHUSDT".into();
    feed.instrument.base.id = 3;
    feed.instrument.base.symbol = "ETH".into();
    target.feeds.push(feed);
    target.topics[0].instrument_set_version = 2;
    let mut header = bank.clone();
    header.resource_id = [211; 16];
    header.ring_epoch += 1;
    header.feed_generation += 1;
    header.predecessor_ring_epoch = bank.ring_epoch;
    header.created_mono_ns = 0;
    header.cutover_seq = 0;
    header.first_seq = 1;
    header.descriptor_hash = [0; 32];
    let template = wire_bytes(&header)?;
    let members = vec![
        Instrument {
            id: config.feeds[0].instrument.id,
            version: config.feeds[0].instrument.version,
            required_states: 1,
        },
        Instrument {
            id: u64::MAX,
            version: config.feeds[0].instrument.version,
            required_states: 1,
        },
    ];
    let consumers = admissions
        .iter()
        .enumerate()
        .map(|(i, old)| {
            let mut next = old.clone();
            next.client.bank_header = template.clone();
            next.client.instruments = members.clone();
            next.client.instrument_set_version = 2;
            next.client.subscription_generation += 1;
            next.client.endpoint.control_epoch += 1;
            next.client.endpoint.nonce = [220 + i as u8; 32];
            next.client.cursor_id = [230 + i as u8; 16];
            next.client.request_id = [240 + i as u8; 16];
            next.snapshot_id = [250 + i as u8; 16];
            Consumer {
                previous: old.clone(),
                next,
            }
        })
        .collect();
    let plan = Plan {
        schema: 1,
        operation_id: [213; 16],
        base_revision: 1,
        base_config_hash: md(config_hash(config))?,
        base_bank: wire_bytes(bank)?,
        target_revision: 2,
        target_bank: template,
        target_config: target,
        topic_id: 101,
        consumers,
        warmup_deadline_mono_ns: at + 30_000_000_000,
        maximum_age_ns: 10_000_000_000,
        peak_cache_bytes: 2 * config.topics[0].maximum_cache_bytes,
        maximum_retained_bank_bytes: 2 * bank.resource_bytes,
    };
    md(plan.clone().prepare(
        config,
        &md(FeedIndex::new(&config.feeds))?,
        1,
        bank,
        0,
        &admissions,
        0,
        at,
    ))?;
    Ok(plan)
}
pub(crate) mod proof;
#[cfg(test)]
mod tests;
