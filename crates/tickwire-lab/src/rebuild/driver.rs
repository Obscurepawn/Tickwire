use super::*;
use crate::handover::json_paths;
use std::{
    collections::BTreeMap,
    fs::File,
    process::{Child, Command, Stdio},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_lifecycle::{
    AppliedPhase, ControlAction, Operation, SignedControlRequest, SignedReceipt,
};
pub(super) fn now(s: &Scenario) -> Result<MonoTime> {
    Ok(
        tickwire_clock_sys::SystemClock::new(ClockDomain::new(s.boot, 1).map_err(|_| "clock")?)?
            .sample()?
            .monotonic,
    )
}
pub(crate) struct Driver {
    scenario: Scenario,
    exec_pid: u32,
    md_pid: u32,
    old_pid: Option<u32>,
    capture_pid: Option<u32>,
    coordinator: Option<Child>,
    operation: Option<Operation>,
    requests: BTreeMap<[u8; 16], SignedControlRequest>,
    prepared: Option<tickwire_lifecycle::RebuildPrepared>,
    receipts: BTreeMap<u8, tickwire_lifecycle::AppliedReceipt>,
    keys: Value,
    armed: bool,
    launched: bool,
    grant_published: bool,
    released: bool,
    halt_pending: bool,
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
        Ok(Self {
            scenario: s.clone(),
            exec_pid,
            md_pid,
            old_pid: None,
            capture_pid: None,
            coordinator: None,
            operation: None,
            requests: BTreeMap::new(),
            prepared: None,
            receipts: BTreeMap::new(),
            keys: json(&s.root.join("handover/keys.json"))?,
            armed: false,
            launched: false,
            grant_published: false,
            released: false,
            halt_pending: false,
        })
    }
    fn now(&self) -> Result<MonoTime> {
        now(&self.scenario)
    }
    pub(crate) fn poll(&mut self) -> Result<()> {
        let r = self.scenario.root.clone();
        if !self.launched {
            let path = r.join(format!("exec/reports/CONSUMED-{}-5.json", hex(&[2; 16])));
            if !path.is_file() {
                return Ok(());
            }
            let old = json(&path)?;
            if old["host_pid"] != self.exec_pid
                || old["host_boot"] != json!(self.scenario.boot)
                || old["instance"] != json!(([2; 16]))
                || old["generation"] != 5
            {
                return Err("rebuild old process identity".into());
            }
            if !self.armed {
                crate::generate::write(
                    &r.join("ARM"),
                    b"actual old source READY; allow the original admission\n",
                )?;
                self.armed = true;
            }
            if !crate::handover::consumed_ack_in(&r.join("strategy/recovery"), &old)? {
                return Ok(());
            }
            let position = old["account_position"].as_u64().ok_or("account position")?;
            self.old_pid = Some(u32::try_from(old["worker_pid"].as_u64().ok_or("old pid")?)?);
            publish(
                &r.join("handover/old-before-capture.json"),
                &serde_json::to_vec(&old)?,
            )?;
            let command = material::admitted_command(&self.scenario, &old)?;
            configure::launch(
                &self.scenario,
                self.exec_pid,
                self.md_pid,
                position,
                &command,
            )?;
            self.launched = true;
        }
        if self.coordinator.is_none() {
            let Some(captured) = material::captured(&self.scenario, self.exec_pid)? else {
                return Ok(());
            };
            self.capture_pid = Some(captured.receipt.pid);
            let operation = material::prepare(&self.scenario, &captured)?;
            let member = operation.members[0].clone();
            let config = json!({"coordinator":{"coordinator_id":([138;16]),"coordinator_key":self.keys["coordinator"],"operation":operation,"members":[{"member":member,"public_key":self.keys["exec"],"readiness":null}],"audit":{"domain":([1;16]),"stream_epoch":([141;16]),"segment_max_bytes":8388608}},"writer_epoch":1,"recover":false,"resume_after_queries":false,"signer_file":r.join("keys/coordinator.pk8"),"audit_directory":r.join("handover/audit"),"receipts_directory":r.join("handover/receipts"),"requests_directory":r.join("handover/staging"),"readiness_directory":null,"maximum_wait_ns":30_000_000_000u64,"poll_interval_ns":1_000_000});
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
            return Ok(());
        }
        if !self.released
            && let Some(status) = self.coordinator.as_mut().ok_or("coordinator")?.try_wait()?
        {
            if !status.success() || self.receipts.len() != 2 || self.prepared.is_none() {
                return Err("schema4 coordinator did not finish all phases".into());
            }
            let text = String::from_utf8(read(&r.join("logs/coordinator.stdout.log"), 65536)?)?;
            if !text
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .any(|v| v["stage"] == "active_all")
            {
                return Err("schema4 coordinator not ActiveAll".into());
            }
            let old = self.old_pid.ok_or("old pid")?;
            let log = String::from_utf8(read(&r.join("logs/exec.stderr.log"), 16 * 1024 * 1024)?)?;
            if Path::new(&format!("/proc/{old}")).exists()
                || !log
                    .lines()
                    .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                    .any(|v| {
                        v["event"] == "strategy_retired"
                            && v["worker_pid"] == old
                            && v["instance"] == json!(([2; 16]))
                            && v["generation"] == 5
                    })
            {
                return Err("schema4 Released preceded real old retirement".into());
            }
            let proof = proof::verify(
                &self.scenario,
                self.prepared.as_ref().ok_or("prepared")?,
                self.capture_pid.ok_or("capture pid")?,
                false,
            )?;
            crate::generate::json_file(&r.join("handover/preflight-verified.json"), &proof)?;
            crate::generate::write(
                &r.join("RELEASE_FILL"),
                b"actual schema4 Release and old PID retirement\n",
            )?;
            self.released = true;
        }
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if !self.released || self.receipts.len() != 2 || self.prepared.is_none() {
            return Err("schema4 publication incomplete".into());
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
            if serde_json::from_slice::<tickwire_lifecycle::RebuildPrepared>(&signed.payload)
                .is_ok()
            {
                let prepared = tickwire_lifecycle::verify_rebuild_prepared(
                    &tickwire_lifecycle::SignedRebuildPrepared {
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
                    || self.prepared.as_ref().is_some_and(|v| v != &prepared)
                {
                    return Err("rebuild Prepared challenge or roots conflict".into());
                }
                self.prepared = Some(prepared);
                continue;
            }
            let receipt = tickwire_lifecycle::verify_receipt(
                &signed,
                serde_json::from_value(self.keys["exec"].clone())?,
                self.operation.as_ref().ok_or("operation")?,
                self.now()?,
            )?;
            let (phase, action) = match receipt.phase {
                AppliedPhase::Prepared => return Err("schema4 requires RebuildPrepared".into()),
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
}
