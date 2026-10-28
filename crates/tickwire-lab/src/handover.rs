//! Laboratory scheduling around the production signed coordinator and Host.
#[cfg(test)]
use crate::ScenarioKind;
use crate::{Result, Scenario};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_lifecycle::{
    AppliedPhase, ControlAction, Member, Operation, Readiness, SignedControlRequest, SignedReceipt,
};
use tickwire_recovery::digest;
use tickwire_strategy_sdk::bootstrap::Bootstrap;

pub(crate) const NEW_INSTANCE: [u8; 16] = [6; 16];
const GENERATION: u64 = 6;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn read(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    if fs::metadata(path)?.len() > maximum {
        return Err("laboratory input capacity".into());
    }
    Ok(fs::read(path)?)
}
fn json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&read(path, 2 * 1024 * 1024)?)?)
}
fn atomic_json(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("pending");
    crate::generate::json_file(&temporary, value)?;
    fs::rename(&temporary, path)?;
    File::open(path.parent().ok_or("directory")?)?.sync_all()?;
    Ok(())
}
fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        if read(path, 8 * 1024 * 1024)? != bytes {
            return Err("laboratory immutable relay conflict".into());
        }
        return Ok(());
    }
    let tmp = path.with_extension("pending");
    crate::generate::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    File::open(path.parent().ok_or("directory")?)?.sync_all()?;
    Ok(())
}
pub(crate) fn configure(s: &mut Scenario) -> Result<()> {
    let r = &s.root;
    for name in [
        "next",
        "next/commands",
        "next/recovery",
        "next/reports",
        "next/runtime",
        "next/control",
        "next/dependencies",
        "rollback",
        "rollback/commands",
        "rollback/reports",
        "rollback/runtime",
        "rollback/control",
        "rollback/recovery",
        "handover",
        "handover/launch",
        "handover/launch-wal",
        "handover/requests",
        "handover/staging",
        "handover/receipts",
        "handover/exec-wal",
        "handover/audit",
    ] {
        fs::DirBuilder::new().mode(0o700).create(r.join(name))?;
    }
    let mut keys = vec![];
    for name in ["coordinator", "exec-lifecycle"] {
        let key =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| "lifecycle key")?;
        let pair = Ed25519KeyPair::from_pkcs8(key.as_ref()).map_err(|_| "lifecycle key")?;
        keys.push(<[u8; 32]>::try_from(pair.public_key().as_ref()).unwrap());
        crate::generate::write(&r.join(format!("keys/{name}.pk8")), key.as_ref())?;
    }
    let mut app = json(&s.app_config)?;
    let old_strategy = app["strategy"].clone();
    let mut host = old_strategy["config"].clone();
    let mut bootstrap: Bootstrap =
        serde_json::from_slice(&read(&s.worker_bootstrap, 2 * 1024 * 1024)?)?;
    let target_binary = s.bin_directory.join(crate::flavor::binary(s.kind, false));
    let target_hash = digest(&fs::read(&target_binary)?);
    for endpoint in [
        &mut bootstrap.market,
        &mut bootstrap.intent,
        &mut bootstrap.report,
    ] {
        endpoint.local.build_hash = target_hash;
        endpoint.local.instance = NEW_INSTANCE;
        endpoint.local.generation = GENERATION;
        endpoint.control_epoch = 2;
        endpoint.nonce = [129; 32];
    }
    let external = bootstrap.external_market.as_mut().ok_or("external MD")?;
    external.endpoint.local = bootstrap.market.local.clone();
    external.endpoint.control_epoch = 2;
    external.endpoint.nonce = [130; 32];
    external.cursor_id = [126; 16];
    external.request_id = [127; 16];
    let mut intent = ResourceHeader::decode(&bootstrap.intent_header)?;
    intent.resource_id = [120; 16];
    intent.channel_id = [121; 16];
    intent.producer_instance = NEW_INSTANCE;
    intent.producer_generation = GENERATION;
    let mut report = ResourceHeader::decode(&bootstrap.report_header)?;
    report.resource_id = [122; 16];
    report.channel_id = [123; 16];
    report.consumer_instance = NEW_INSTANCE;
    tickwire_shm::validate_bank(&intent)?;
    tickwire_shm::validate_bank(&report)?;
    bootstrap.intent_header = tickwire_market_source::protocol::wire_bytes(&intent)?;
    bootstrap.report_header = tickwire_market_source::protocol::wire_bytes(&report)?;
    bootstrap.intent_cursor_id = [124; 16];
    bootstrap.report_cursor_id = [125; 16];
    bootstrap.command_directory = r.join("next/commands");
    bootstrap.report_directory = r.join("next/reports");
    bootstrap.join_file = r.join("next/join.json");
    bootstrap.recovery_template = Some(r.join("next/recovery-template.json"));
    bootstrap.demo_command_file = Some(r.join("next/command.bin"));
    bootstrap.handover_enabled = true;
    bootstrap.preflight_job = Some(r.join("next/preflight.json"));
    // Shadow must not consume its one-shot command while the original ARM is set.
    bootstrap.demo_minimum_bid_ticks = 6_000_100;
    let original =
        tickwire_venue::CanonicalCommand::decode(&read(&r.join("strategy/command.bin"), 65536)?)
            .map_err(|e| format!("command: {e:?}"))?;
    let command = shadow_command(&original)?;
    crate::generate::write(&r.join("next/command.bin"), command.bytes())?;
    crate::flavor::configure(s.kind, &mut bootstrap, &r.join("next"), &command, 6_000_100)?;
    crate::generate::json_file(
        &r.join("next/bootstrap.json"),
        &serde_json::to_value(&bootstrap)?,
    )?;
    host["executable"] = json!(target_binary);
    host["shadow"] = json!(true);
    host["bootstrap"] = json!(r.join("next/bootstrap.json"));
    host["directory_parent"] = json!(r.join("next/runtime"));
    host["control_directory"] = json!(r.join("next/control"));
    host["control_stream"] = json!(([131; 16]));
    host["control_domain"] = json!(([132; 16]));
    host["snapshot_id"] = json!(([133; 16]));
    host["market_cursor_id"] = json!(([126; 16]));
    host["recovery_directory"] = json!(r.join("next/recovery"));
    crate::generate::json_file(&r.join("next/host.json"), &host)?;
    let mut grant = app["policy"]["strategy_grants"][0].clone();
    grant["instance_id"] = json!(NEW_INSTANCE);
    grant["strategy_generation"] = json!(GENERATION);
    grant["authorization_epoch"] = json!(10);
    crate::generate::json_file(&r.join("next/grant.json"), &grant)?;
    let mut template: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&read(&r.join("strategy/recovery-template.json"), 65536)?)?;
    template.owner = NEW_INSTANCE;
    template.input_stream = [134; 16];
    template.pin_id = [135; 16];
    template.output_directory = r.join("next/recovery");
    template.order_ids = vec![[8; 16]];
    for d in &mut template.dependencies {
        d.owner = NEW_INSTANCE;
        let bytes = match d.kind {
            4 => serde_json::to_vec(&grant)?,
            7 => serde_json::to_vec(
                &json!({"scenario":"spot-handover","instance":NEW_INSTANCE,"generation":GENERATION}),
            )?,
            9 => serde_json::to_vec(&host)?,
            8 => fs::read(&target_binary)?,
            11 => fs::read(crate::flavor::config_path(s.kind, &r.join("next")))?,
            _ => read(&d.path, 64 * 1024 * 1024)?,
        };
        d.path = r.join(format!("next/dependencies/{}.bin", d.kind));
        d.digest = digest(&bytes);
        crate::generate::write(&d.path, &bytes)?;
    }
    crate::generate::json_file(
        &r.join("next/recovery-template.json"),
        &serde_json::to_value(template)?,
    )?;
    app["strategy"] = json!({"mode":"multi","members":[{"logical_strategy_id":8,"shadow":false,"strategy":old_strategy}]});
    app["shadow_loader"] = json!({"inbox_directory":r.join("handover/launch"),"journal_directory":r.join("handover/launch-wal"),"stream_epoch":([136;16]),"recover":false,"segment_max_bytes":8388608,"maximum_requests":16,"maximum_per_poll":1});
    app["lifecycle"] = json!({"requests_directory":r.join("handover/requests"),"receipts_directory":r.join("handover/receipts"),"journal_directory":r.join("handover/exec-wal"),"stream_epoch":([137;16]),"recover":false,"segment_max_bytes":8388608,"coordinator_id":([138;16]),"coordinator_public_key":keys[0],"signing_key_pkcs8":r.join("keys/exec-lifecycle.pk8"),"maximum_requests":32,"maximum_per_poll":4});
    let deployment: tickwire_app::config::Deployment = serde_json::from_value(app.clone())?;
    let baseline = tickwire_account_codec::CanonicalBaseline::decode(&read(
        &r.join("baseline.bin"),
        2 * 1024 * 1024,
    )?)
    .map_err(|e| format!("baseline: {e:?}"))?;
    deployment.assemble(baseline).map_err(|e| e.to_string())?;
    atomic_json(&s.app_config, &app)?;
    crate::generate::json_file(
        &r.join("handover/keys.json"),
        &json!({"coordinator":keys[0],"exec":keys[1]}),
    )?;
    Ok(())
}

fn shadow_command(
    original: &tickwire_venue::CanonicalCommand,
) -> Result<tickwire_venue::CanonicalCommand> {
    let mut command = original.command().clone();
    command.context.request.intent_id = 4;
    command.context.request.strategy_generation = GENERATION;
    command.context.strategy.instance_id = NEW_INSTANCE;
    command.context.strategy.authorization_epoch = 10;
    // Restoration preserves the consumed economic action; only the new authority
    // and transport identity change. The migrated callback must not emit it again.
    if let tickwire_venue::CommandBody::New(order) = &mut command.body {
        order.client_id =
            tickwire_venue::BoundedText::new("lab-spot-2").map_err(|e| format!("client: {e:?}"))?;
    }
    tickwire_venue::CanonicalCommand::new(command).map_err(|e| format!("command: {e:?}").into())
}

pub(crate) fn member(deployment: &tickwire_app::config::Deployment, valid_until_ns: u64) -> Member {
    Member {
        risk_domain: deployment.policy.scope.risk_domain_id,
        exec_instance: deployment.owner_id,
        exec_epoch: deployment.policy.owner_generation,
        old_instance: [2; 16],
        old_generation: 5,
        new_instance: NEW_INSTANCE,
        new_generation: GENERATION,
        clock_boot_id: deployment.boot_id,
        clock_generation: deployment.clock_generation,
        valid_until_ns,
    }
}

pub(crate) struct Driver {
    scenario: Scenario,
    coordinator: Option<Child>,
    operation: Option<Operation>,
    requests: BTreeMap<[u8; 16], SignedControlRequest>,
    replay: Option<tickwire_lifecycle::ReplayPrepared>,
    armed: bool,
    receipts: BTreeMap<u8, tickwire_lifecycle::AppliedReceipt>,
    keys: Value,
    launched: bool,
    grant_published: bool,
    released: bool,
    halt_pending: bool,
    old_pid: Option<u32>,
    exec_pid: u32,
}
impl Drop for Driver {
    fn drop(&mut self) {
        if let Some(c) = &mut self.coordinator {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
impl Driver {
    pub(crate) fn new(s: &Scenario, exec_pid: u32, md_pid: u32) -> Result<Self> {
        let path = s.root.join("next/bootstrap.json");
        let mut next = json(&path)?;
        next["external_market"]["endpoint"]["peer_pid"] = json!(md_pid);
        atomic_json(&path, &next)?;
        Ok(Self {
            scenario: s.clone(),
            coordinator: None,
            operation: None,
            requests: BTreeMap::new(),
            replay: None,
            armed: false,
            receipts: BTreeMap::new(),
            keys: json(&s.root.join("handover/keys.json"))?,
            launched: false,
            grant_published: false,
            released: false,
            halt_pending: false,
            old_pid: None,
            exec_pid,
        })
    }
    fn now(&self) -> Result<MonoTime> {
        let d = ClockDomain::new(self.scenario.boot, 1).map_err(|_| "clock domain")?;
        Ok(tickwire_clock_sys::SystemClock::new(d)?.sample()?.monotonic)
    }
    pub(crate) fn poll(&mut self) -> Result<()> {
        let root = self.scenario.root.clone();
        let r = &root;
        if !self.launched {
            let old_path = r.join(format!("exec/reports/CONSUMED-{}-5.json", hex(&[2; 16])));
            if !old_path.is_file() {
                return Ok(());
            }
            let old = json(&old_path)?;
            if old["account_position"].as_u64().is_none_or(|v| v < 1) {
                return Ok(());
            }
            if old["host_pid"] != self.exec_pid
                || old["host_boot"] != json!(self.scenario.boot)
                || old["instance"] != json!(([2; 16]))
                || old["generation"] != 5
            {
                return Err("preflight source process identity".into());
            }
            self.operation = Some(crate::preflight::prepare(&self.scenario, &old)?);
            let request = tickwire_app::launch::Request {
                version: 1,
                sequence: 1,
                request_id: ([139; 16]),
                exec_instance: ([4; 16]),
                writer_epoch: 4,
                logical_strategy_id: 8,
                instance_id: NEW_INSTANCE,
                strategy_generation: GENERATION,
                host_config: r.join("next/host.json"),
                host_config_digest: digest(&read(&r.join("next/host.json"), 131072)?),
                bootstrap_digest: digest(&read(&r.join("next/bootstrap.json"), 2 * 1024 * 1024)?),
            };
            publish(
                &r.join("handover/launch/00000000000000000001.json"),
                &serde_json::to_vec(&request)?,
            )?;
            self.launched = true;
        }
        if !self.armed {
            let path = r.join(format!(
                "next/reports/CONSUMED-{}-6.json",
                hex(&NEW_INSTANCE)
            ));
            if !path.is_file() {
                return Ok(());
            }
            let next = json(&path)?;
            if next["host_pid"] != self.exec_pid || next["host_boot"] != json!(self.scenario.boot) {
                return Err("startup READY host identity".into());
            }
            let startup = crate::preflight::verify_restored(
                &self.scenario,
                &next,
                self.operation.as_ref().ok_or("startup operation")?,
            )?;
            crate::generate::json_file(&r.join("handover/startup-verified.json"), &startup)?;
            crate::generate::write(
                &r.join("ARM"),
                b"actual unspent source S and restored target acknowledged\n",
            )?;
            self.armed = true;
        }
        if self.scenario.kind == crate::ScenarioKind::SpotMarketExpansion
            && !r.join("market-expanded.json").is_file()
        {
            return Ok(());
        }
        if self.coordinator.is_none() {
            let old_path = r.join(format!("exec/reports/CONSUMED-{}-5.json", hex(&[2; 16])));
            let next_path = r.join(format!(
                "next/reports/CONSUMED-{}-6.json",
                hex(&NEW_INSTANCE)
            ));
            if !old_path.is_file() || !next_path.is_file() {
                return Ok(());
            }
            let old = json(&old_path)?;
            let next = json(&next_path)?;
            if !same_cut(&old, &next) {
                return Ok(());
            }
            for (receipt, instance, generation) in [(&old, [2; 16], 5), (&next, NEW_INSTANCE, 6)] {
                if receipt["host_pid"] != self.exec_pid
                    || receipt["instance"] != json!(instance)
                    || receipt["generation"] != generation
                    || receipt["host_boot"] != json!(self.scenario.boot)
                {
                    return Err("handover receipt process identity".into());
                }
            }
            for (receipt, path) in [
                (&old, &self.scenario.worker_bootstrap),
                (&next, &r.join("next/bootstrap.json")),
            ] {
                let b: Bootstrap = serde_json::from_slice(&read(path, 2 * 1024 * 1024)?)?;
                let pid = u32::try_from(receipt["worker_pid"].as_u64().ok_or("worker pid")?)?;
                let client = b.external_market.ok_or("external MD")?;
                let path = tickwire_md::server::admission_path(
                    &r.join("md/admissions"),
                    pid,
                    client.request_id,
                )
                .map_err(|e| e.to_string())?;
                let admission: tickwire_md::server::Admission =
                    serde_json::from_slice(&read(&path, 2 * 1024 * 1024)?)?;
                if admission.pid != pid
                    || admission.supervisor_pid != self.exec_pid
                    || admission.client.endpoint.local.identity() != b.market.local.identity()
                {
                    return Err("handover READY differs from admitted child".into());
                }
            }
            if !consumed_ack(r, &next)? {
                return Ok(());
            }
            self.old_pid = Some(u32::try_from(
                old["worker_pid"].as_u64().ok_or("old worker pid")?,
            )?);
            crate::generate::json_file(&r.join("handover/old-prepared.json"), &old)?;
            crate::generate::json_file(&r.join("handover/new-prepared.json"), &next)?;
            let deployment = serde_json::from_value(json(&self.scenario.app_config)?)?;
            let member = member(&deployment, self.scenario.valid_until_ns);
            let proof = Readiness {
                checkpoint_digest: serde_json::from_value(next["checkpoint_digest"].clone())?,
                exec_report_sequence: next["account_position"]
                    .as_u64()
                    .ok_or("account position")?,
                orders_digest: serde_json::from_value(next["orders_digest"].clone())?,
                ledger_sequence: next["ledger_sequence"].as_u64().ok_or("ledger")?,
                input_seal_digest: serde_json::from_value(next["input_seal_digest"].clone())?,
            };
            let operation = self.operation.clone().ok_or("fixed release operation")?;
            if operation.schema != 3 || operation.members != [member.clone()] {
                return Err("preflight operation changed after launch".into());
            }
            let config = json!({"coordinator":{"coordinator_id":([138;16]),"coordinator_key":self.keys["coordinator"],"operation":operation,"members":[{"member":member,"public_key":self.keys["exec"],"readiness":proof}],"audit":{"domain":([1;16]),"stream_epoch":([141;16]),"segment_max_bytes":8388608}},"writer_epoch":1,"recover":false,"resume_after_queries":false,"signer_file":r.join("keys/coordinator.pk8"),"audit_directory":r.join("handover/audit"),"receipts_directory":r.join("handover/receipts"),"requests_directory":r.join("handover/staging"),"readiness_directory":null,"maximum_wait_ns":30_000_000_000u64,"poll_interval_ns":1_000_000});
            crate::generate::json_file(&r.join("handover/coordinator.json"), &config)?;
            self.coordinator = Some(
                Command::new(self.scenario.bin_directory.join("tickwire-lifecycle"))
                    .arg("--config")
                    .arg(r.join("handover/coordinator.json"))
                    .stdout(Stdio::from(File::create(
                        r.join("logs/coordinator.stdout.log"),
                    )?))
                    .stderr(Stdio::from(File::create(
                        r.join("logs/coordinator.stderr.log"),
                    )?))
                    .spawn()?,
            );
            self.operation = Some(operation);
        }
        self.relay()?;
        self.collect_receipts()?;
        if self.halt_pending {
            // The coordinator may exit immediately after persisting Halt.
            // Keep the normal scenario loop alive for its real applied ACK;
            // the existing scenario deadline still bounds this wait.
            return Ok(());
        }
        if !self.released
            && let Some(child) = &mut self.coordinator
            && let Some(status) = child.try_wait()?
        {
            if !status.success() || (self.receipts.len() != 2 || self.replay.is_none()) {
                return Err("coordinator did not finish all applied handover phases".into());
            }
            let output = String::from_utf8(read(
                &self.scenario.root.join("logs/coordinator.stdout.log"),
                65536,
            )?)?;
            if !output
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .any(|v| v["stage"] == "active_all")
            {
                return Err("coordinator is not ActiveAll".into());
            }
            let old = self.old_pid.ok_or("old child identity")?;
            let log = String::from_utf8(read(
                &self.scenario.root.join("logs/exec.stderr.log"),
                16 * 1024 * 1024,
            )?)?;
            let retired = log
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .any(|v| {
                    v["event"] == "strategy_retired"
                        && v["worker_pid"] == old
                        && v["instance"] == json!(([2; 16]))
                        && v["generation"] == 5
                });
            if !retired || Path::new(&format!("/proc/{old}")).exists() {
                return Err("Released receipt preceded old child retirement".into());
            }
            let next = json(&r.join(format!(
                "next/reports/CONSUMED-{}-6.json",
                hex(&NEW_INSTANCE)
            )))?;
            let completed = crate::preflight::verify_completed(
                &self.scenario,
                &next,
                self.operation.as_ref().ok_or("operation")?,
                self.replay.as_ref().ok_or("replay Prepared")?,
                crate::flavor::ProgressPhase::Installed,
            )?;
            crate::generate::json_file(&r.join("handover/preflight-verified.json"), &completed)?;
            crate::generate::write(
                &self.scenario.root.join("RELEASE_FILL"),
                b"signed release and old child retirement\n",
            )?;
            self.released = true;
        }
        Ok(())
    }
    fn relay(&mut self) -> Result<()> {
        let r = &self.scenario.root;
        let dir = r.join("handover/staging").join(hex(&[1; 16]));
        if !dir.is_dir() {
            return Ok(());
        }
        for path in json_paths(&dir)? {
            let bytes = read(&path, 8 * 1024 * 1024)?;
            let signed: SignedControlRequest = serde_json::from_slice(&bytes)?;
            let operation = self.operation.as_ref().ok_or("operation")?;
            let verified = tickwire_lifecycle::verify_control_request(
                &signed,
                serde_json::from_value(self.keys["coordinator"].clone())?,
                [138; 16],
                &operation.members[0],
                self.now()?,
            )?;
            let request = verified.request();
            if request.operation != *operation {
                return Err("handover request operation".into());
            }
            if self.requests.contains_key(&request.request_id) {
                continue;
            }
            if request.action == ControlAction::Commit && !self.grant_published {
                let app: tickwire_app::config::Deployment =
                    serde_json::from_value(json(&self.scenario.app_config)?)?;
                let command = tickwire_app::control::Command {
                    schema: 1,
                    sequence: 1,
                    command_id: ([142; 16]),
                    binding: tickwire_app::control::Binding::from_config(&app.policy),
                    action: tickwire_app::control::Action::StrategyGrant {
                        grant: serde_json::from_value(json(&r.join("next/grant.json"))?)?,
                    },
                };
                publish(
                    &r.join("exec/control-inbox/00000000000000000001.json"),
                    &serde_json::to_vec(&command)?,
                )?;
                self.grant_published = true;
            }
            if !matches!(
                request.action,
                ControlAction::Prepare
                    | ControlAction::Commit
                    | ControlAction::Release
                    | ControlAction::Halt
            ) {
                return Err("unexpected lifecycle action".into());
            }
            // Exact signed bytes; in particular Release can only arrive after the
            // real Committed response and a subsequent application control turn.
            publish(
                &r.join("handover/requests")
                    .join(path.file_name().ok_or("request filename")?),
                &bytes,
            )?;
            self.requests.insert(request.request_id, signed.clone());
            if request.action == ControlAction::Halt {
                self.halt_pending = true;
            }
        }
        Ok(())
    }
    fn collect_receipts(&mut self) -> Result<()> {
        for path in json_paths(&self.scenario.root.join("handover/receipts"))? {
            let signed: SignedReceipt = serde_json::from_slice(&read(&path, 8 * 1024 * 1024)?)
                .map_err(|_| "exec rejected lifecycle request; inspect receipt")?;
            if serde_json::from_slice::<tickwire_lifecycle::ReplayPrepared>(&signed.payload).is_ok()
            {
                let prepared = tickwire_lifecycle::verify_replay_prepared(
                    &tickwire_lifecycle::SignedReplayPrepared {
                        payload: signed.payload,
                        signature: signed.signature,
                    },
                    serde_json::from_value(self.keys["exec"].clone())?,
                    self.operation.as_ref().ok_or("operation")?,
                    self.now()?,
                )?;
                let challenge: tickwire_lifecycle::ControlRequest =
                    serde_json::from_slice(&prepared.challenge.payload)?;
                if self.requests.get(&challenge.request_id) != Some(&prepared.challenge)
                    || prepared.fixed.sequence >= prepared.current.sequence
                    || prepared.fixed.checkpoint == prepared.current.checkpoint
                {
                    return Err(
                        "replay Prepared does not prove the requested S-to-C transition".into(),
                    );
                }
                if self.replay.as_ref().is_some_and(|old| old != &prepared) {
                    return Err("conflicting replay preparation".into());
                }
                self.replay = Some(prepared);
                continue;
            }
            let receipt = tickwire_lifecycle::verify_receipt(
                &signed,
                serde_json::from_value(self.keys["exec"].clone())?,
                self.operation.as_ref().ok_or("operation")?,
                self.now()?,
            )?;
            let (phase, action) = match receipt.phase {
                AppliedPhase::Prepared => return Err("schema3 requires ReplayPrepared".into()),
                AppliedPhase::Committed => (2, ControlAction::Commit),
                AppliedPhase::Released => (3, ControlAction::Release),
                AppliedPhase::Halted => (4, ControlAction::Halt),
            };
            if self
                .requests
                .get(&receipt.control_request_id)
                .is_none_or(|r| {
                    serde_json::from_slice::<tickwire_lifecycle::ControlRequest>(&r.payload)
                        .map_or(true, |r| r.action != action)
                })
            {
                return Err("receipt challenge mismatch".into());
            }
            if let Some(old) = self.receipts.insert(phase, receipt.clone())
                && old != receipt
            {
                return Err("different handover applied receipt".into());
            }
            if receipt.phase == AppliedPhase::Halted {
                // A requested Halt is a valid protocol action and still makes
                // this success-path scenario fail, after the actual owner ACK.
                publish(
                    &self.scenario.root.join("handover/halted.json"),
                    &serde_json::to_vec(&signed)?,
                )?;
                self.halt_pending = false;
                return Err("handover halted after applied receipt".into());
            }
        }
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if !self.released || (self.receipts.len() != 2 || self.replay.is_none()) {
            return Err("handover incomplete".into());
        }
        Ok(())
    }
}
fn json_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    for entry in fs::read_dir(path)? {
        let p = entry?.path();
        if p.extension().is_some_and(|e| e == "json") {
            paths.push(p);
            if paths.len() > 64 {
                return Err("handover directory capacity".into());
            }
        }
    }
    paths.sort();
    Ok(paths)
}
fn same_cut(old: &Value, new: &Value) -> bool {
    old["account_position"].as_u64().is_some_and(|v| v > 1)
        && [
            "account_position",
            "account_input_digest",
            "orders_digest",
            "ledger_sequence",
        ]
        .iter()
        .all(|k| old[*k] == new[*k] && !old[*k].is_null())
}
fn check_handover_records(
    records: impl IntoIterator<Item = Result<tickwire_account_codec::CanonicalRecord>>,
) -> Result<(u64, [u8; 32])> {
    let mut handover = None;
    let mut first_fill_position = None;
    let mut admits = 0;
    for record in records {
        let record = record?;
        let position = record.sequence();
        match record.record() {
            tickwire_account::RecoveryRecord::StrategyHandover(h) => {
                if handover.is_some()
                    || h.from.instance != [2; 16]
                    || h.from.generation != 5
                    || h.to.instance != NEW_INSTANCE
                    || h.to.generation != GENERATION
                    || h.operation_id != [140; 16]
                {
                    return Err("account handover binding".into());
                }
                handover = Some((position, record.digest()));
            }
            tickwire_account::RecoveryRecord::Admit { .. } => admits += 1,
            tickwire_account::RecoveryRecord::IngressEvent { event, .. }
            | tickwire_account::RecoveryRecord::VenueEvent(event) => {
                if matches!(event.event().body, tickwire_venue::VenueEventBody::Fill(_)) {
                    first_fill_position.get_or_insert(position);
                }
            }
            _ => {}
        }
    }
    let (position, hash) = handover.ok_or("missing applied account handover")?;
    if admits != 1 || first_fill_position.is_none_or(|fill| fill <= position) {
        return Err("expected one old admission followed by handover and late fill".into());
    }
    Ok((position, hash))
}

pub(crate) fn check_export(
    s: &Scenario,
    exported: &Path,
    report: &Value,
    count: u64,
) -> Result<()> {
    let order = report["orders"]
        .as_array()
        .and_then(|orders| orders.first())
        .ok_or("order")?;
    if order["origin_instance"] != hex(&[2; 16])
        || order["origin_generation"] != 5
        || order["manager_instance"] != hex(&NEW_INSTANCE)
        || order["manager_generation"] != GENERATION
    {
        return Err("late fill did not retain the old origin and new manager".into());
    }
    let records = (1..=count).map(|position| {
        tickwire_account_codec::CanonicalRecord::decode(&read(
            &exported
                .join("records")
                .join(format!("{position:020}.cbor")),
            1024 * 1024,
        )?)
        .map_err(|e| format!("record: {e:?}").into())
    });
    let (position, hash) = check_handover_records(records)?;
    let coordinator: Value = json(&s.root.join("handover/coordinator.json"))?;
    let operation: Operation =
        serde_json::from_value(coordinator["coordinator"]["operation"].clone())?;
    let public =
        serde_json::from_value(coordinator["coordinator"]["members"][0]["public_key"].clone())?;
    let now =
        tickwire_clock_sys::SystemClock::new(ClockDomain::new(s.boot, 1).map_err(|_| "clock")?)?
            .sample()?
            .monotonic;
    let mut phases = vec![];
    let mut replay_prepared = None;
    for path in json_paths(&s.root.join("handover/receipts"))? {
        let signed: SignedReceipt = serde_json::from_slice(&read(&path, 8 * 1024 * 1024)?)?;
        if serde_json::from_slice::<tickwire_lifecycle::ReplayPrepared>(&signed.payload).is_ok() {
            let prepared = tickwire_lifecycle::verify_replay_prepared(
                &tickwire_lifecycle::SignedReplayPrepared {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                public,
                &operation,
                now,
            )?;
            if prepared.fixed.sequence >= prepared.current.sequence {
                return Err("replay had no actual tail".into());
            }
            replay_prepared = Some(prepared);
            continue;
        }
        let receipt = tickwire_lifecycle::verify_receipt(&signed, public, &operation, now)?;
        if matches!(
            receipt.phase,
            AppliedPhase::Committed | AppliedPhase::Released
        ) && (receipt.handover_sequence != position || receipt.handover_digest != hash)
        {
            return Err("signed receipt differs from durable handover record".into());
        }
        phases.push(receipt.phase);
    }
    if replay_prepared.is_none()
        || [AppliedPhase::Committed, AppliedPhase::Released]
            .iter()
            .any(|p| !phases.contains(p))
    {
        return Err("missing signed applied phase".into());
    }
    let ready = json(&s.root.join(format!(
        "next/reports/CONSUMED-{}-6.json",
        hex(&NEW_INSTANCE)
    )))?;
    let final_proof = crate::preflight::verify_completed(
        s,
        &ready,
        &operation,
        replay_prepared.as_ref().ok_or("replay Prepared")?,
        crate::flavor::ProgressPhase::Final,
    )?;
    if final_proof["target_final_sequence"].as_u64()
        <= final_proof["target_install_sequence"].as_u64()
    {
        return Err("new target has no retained C-to-D callback tail".into());
    }
    crate::generate::json_file(
        &s.root.join("handover/preflight-final-verified.json"),
        &final_proof,
    )?;
    Ok(())
}

/// Inspect the actual account records retained in the new child's durable inputs.
/// Receipt watermarks alone cannot distinguish admission from the later REST ACK.
fn consumed_ack(root: &Path, ready: &Value) -> Result<bool> {
    consumed_ack_in(&root.join("next/recovery"), ready)
}
fn consumed_ack_in(directory: &Path, ready: &Value) -> Result<bool> {
    let hash: [u8; 32] = serde_json::from_value(ready["input_seal_digest"].clone())?;
    let object = |hash: [u8; 32]| -> Result<Vec<u8>> {
        let bytes = read(
            &directory.join(format!("{}.bin", hex(&hash))),
            64 * 1024 * 1024,
        )?;
        if digest(&bytes) != hash {
            return Err("handover recovery object hash".into());
        }
        Ok(bytes)
    };
    let seal = tickwire_recovery::HandoverSeal::decode(&object(hash)?)?;
    let input = tickwire_recovery::EncodedInputs::decode(object(seal.input_digest)?)?;
    let bytes = &input.bytes;
    let count = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
    let mut offset = 68;
    let mut ack = false;
    for _ in 0..count {
        let kind = bytes[offset + 8];
        let len = u32::from_be_bytes(bytes[offset + 9..offset + 13].try_into().unwrap()) as usize;
        let payload = &bytes[offset + 13..offset + 13 + len];
        offset += 13 + len;
        if kind != 2 || payload.len() < 29 || &payload[..8] != b"TWAI0001" || payload[8] != 1 {
            continue;
        }
        let record = tickwire_account_codec::CanonicalRecord::decode(&payload[29..])
            .map_err(|e| format!("account report: {e:?}"))?;
        if let tickwire_account::RecoveryRecord::IngressEvent { event, .. }
        | tickwire_account::RecoveryRecord::VenueEvent(event) = record.record()
        {
            match &event.event().body {
                tickwire_venue::VenueEventBody::Order(o)
                    if o.status == tickwire_venue::OrderStatus::New =>
                {
                    ack = true
                }
                tickwire_venue::VenueEventBody::Fill(_) => {
                    return Err("old fill arrived before handover".into());
                }
                _ => {}
            }
        }
    }
    Ok(ack)
}

#[cfg(test)]
#[path = "handover/export_tests.rs"]
mod export_tests;

#[cfg(test)]
#[path = "handover/relay_tests.rs"]
mod relay_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shadow_command_preserves_action_and_binds_fresh_authority() {
        let (_, old) = crate::model::model(1, 1000).unwrap();
        let new = shadow_command(&old).unwrap();
        let a = &old.command().context;
        let b = &new.command().context;
        assert_eq!(b.request.risk_domain_id, a.request.risk_domain_id);
        assert_eq!(
            b.strategy.logical_strategy_id,
            a.strategy.logical_strategy_id
        );
        assert_eq!(
            (
                b.strategy.instance_id,
                b.request.strategy_generation,
                b.strategy.authorization_epoch
            ),
            (NEW_INSTANCE, GENERATION, 10)
        );
        assert_ne!(a.request.intent_id, b.request.intent_id);
        assert_eq!(a.strategy.economic_intent_id, b.strategy.economic_intent_id);
        assert_ne!(old.digest(), new.digest());
        let tickwire_venue::CommandBody::New(order) = &new.command().body else {
            panic!("new")
        };
        assert_eq!(order.client_id.as_str(), "lab-spot-2");
    }
    #[test]
    fn shadow_requires_the_same_complete_account_prefix() {
        let old = json!({"account_position":4,"account_input_digest":([1;32]),"orders_digest":([2;32]),"ledger_sequence":3});
        assert!(same_cut(&old, &old));
        for key in [
            "account_position",
            "account_input_digest",
            "orders_digest",
            "ledger_sequence",
        ] {
            let mut new = old.clone();
            new[key] = Value::Null;
            assert!(!same_cut(&old, &new));
        }
    }
}
