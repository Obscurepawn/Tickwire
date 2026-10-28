//! Process owner for the explicit laboratory deployment.
use crate::{Result, Scenario};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    os::unix::{fs::MetadataExt, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for c in self.0.iter_mut().rev() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
/// Spawned before admission is published. exec preserves the PID held by the launcher.
pub fn child_exec(config: &Path, gate: &Path, binary: &Path) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(30);
    while !gate.is_file() {
        if Instant::now() >= until {
            return Err("admission gate timeout".into());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Err(Command::new(binary)
        .args(["run", "--config"])
        .arg(config)
        .exec()
        .into())
}
pub fn run(s: &Scenario) -> Result<()> {
    if !cfg!(target_os = "linux") {
        return Err("the process scenario requires the Linux shared-memory backend".into());
    }
    let domain = tickwire_clock::ClockDomain::new(s.boot, 1).map_err(|_| "clock domain")?;
    let now = tickwire_clock_sys::SystemClock::new(domain)?
        .sample()?
        .monotonic
        .nanos();
    if now >= s.valid_until_ns {
        return Err("scenario authorization expired; generate a new directory".into());
    }
    if s.root.join("STARTED").exists() {
        return Err("scenario directories are single-use".into());
    }
    crate::generate::write(&s.root.join("STARTED"), b"local laboratory\n")?;
    let scenario = s.root.join("scenario.json");
    let mut children = Children(vec![]);
    let mut fixture = Command::new(s.bin_directory.join("tickwire-lab"));
    fixture.arg("serve").arg("--config").arg(&scenario);
    children.0.push(spawn(fixture, s, "fixture")?);
    wait_file(
        &s.root.join("fixture.ready"),
        &mut children,
        Duration::from_secs(10),
    )?;
    let ready = spawn_mirror(s, &mut children)?;
    ready
        .recv_timeout(Duration::from_secs(10))
        .map_err(|_| "mirror readiness timeout")??;
    let mut md = Command::new(s.bin_directory.join("tickwire-md"));
    md.arg("--config").arg(&s.md_config);
    children.0.push(spawn(md, s, "md")?);
    let md_pid = children.0.last().ok_or("MD child")?.id();
    wait_file(
        &s.root.join("md/market.sock"),
        &mut children,
        Duration::from_secs(30),
    )?;
    let mut app: Value = serde_json::from_slice(&fs::read(&s.app_config)?)?;
    for source in app["market_sources"].as_array_mut().ok_or("MD sources")? {
        source["endpoint"]["peer_pid"] = json!(md_pid);
    }
    replace_json(&s.app_config, &app)?;
    let mut worker: Value = serde_json::from_slice(&fs::read(&s.worker_bootstrap)?)?;
    worker["external_market"]["endpoint"]["peer_pid"] = json!(md_pid);
    replace_json(&s.worker_bootstrap, &worker)?;
    let gate = s.root.join("exec.gate");
    let mut exec = Command::new(s.bin_directory.join("tickwire-lab"));
    exec.arg("child-exec")
        .arg(&s.app_config)
        .arg(&gate)
        .arg(s.bin_directory.join("tickwire-app"));
    exec.stdout(Stdio::from(File::create(
        s.root.join("logs/exec.stdout.log"),
    )?))
    .stderr(Stdio::piped())
    .current_dir(&s.root);
    let mut child = exec.spawn()?;
    let pid = child.id();
    let stderr = child.stderr.take().ok_or("exec stderr")?;
    let arm = s.root.join("ARM");
    let auto_arm = s.kind == crate::ScenarioKind::SpotFill;
    let log = s.root.join("logs/exec.stderr.log");
    let exec_log = std::thread::spawn(move || {
        let Ok(mut out) = File::create(log) else {
            return;
        };
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            let _ = writeln!(out, "{line}");
            if auto_arm
                && serde_json::from_str::<Value>(&line)
                    .ok()
                    .is_some_and(|v| v["event"] == "account_reconciled")
                && !arm.exists()
            {
                let _ = crate::generate::write(&arm, b"account_reconciled\n");
            }
        }
    });
    children.0.push(child);
    let client: tickwire_market_source::ConnectConfig =
        serde_json::from_value(app["market_sources"][0].clone())?;
    let supervisor = serde_json::from_value(if s.kind.handover() {
        app["strategy"]["members"][0]["strategy"]["config"]["supervisor"].clone()
    } else {
        app["strategy"]["config"]["supervisor"].clone()
    })?;
    let metadata = fs::metadata(&s.root)?;
    let admission = tickwire_md::server::Admission {
        client,
        pid,
        uid: metadata.uid(),
        gid: metadata.gid(),
        supervisor,
        supervisor_pid: std::process::id(),
        snapshot_id: [67; 16],
    };
    tickwire_md::server::register_admission(&s.root.join("md/admissions"), &admission)
        .map_err(|e| e.to_string())?;
    crate::generate::write(&gate, b"admitted\n")?;
    let mut handover = if s.kind.handover() {
        Some(crate::handover::Driver::new(s, pid, md_pid)?)
    } else {
        None
    };
    let mut expansion = if s.kind == crate::ScenarioKind::SpotMarketExpansion {
        Some(crate::expansion::Driver::new(s, pid, md_pid)?)
    } else {
        None
    };
    let until = Instant::now() + Duration::from_secs(90);
    let exec_index = children.0.len() - 1;
    let status = loop {
        if let Some(driver) = &mut expansion {
            driver.poll()?;
        }
        if let Some(driver) = &mut handover {
            driver.poll()?;
        }
        if let Some(status) = children.0[exec_index].try_wait()? {
            break status;
        }
        for (i, c) in children.0.iter_mut().enumerate() {
            if i != exec_index
                && let Some(status) = c.try_wait()?
            {
                return Err(format!("required process {i} exited: {status}").into());
            }
        }
        if Instant::now() >= until {
            let _ = crate::generate::write(&s.root.join("STOP"), b"deadline\n");
            return Err("scenario process timeout".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = crate::generate::write(&s.root.join("STOP"), b"complete\n");
    if !status.success() {
        return Err(format!("exec exited: {status}; see logs/exec.stderr.log").into());
    }
    if let Some(driver) = &handover {
        driver.finish()?;
    }
    // All live writer handles are closed before offline recovery reads private copies.
    drop(children);
    exec_log.join().map_err(|_| "execution log reader failed")?;
    let deployment =
        tickwire_app::config::Deployment::read(&s.app_config).map_err(|e| e.to_string())?;
    let exported = s.root.join("export");
    let report = tickwire_app::replay::export(&deployment, &exported).map_err(|e| e.to_string())?;
    check_account(&report)?;
    let mut mirror = tickwire_replica::config::ReceiverConfig::from_file(&s.mirror_config)?;
    mirror.create = false;
    let mirror = mirror.open_store()?;
    let count = report["account_records"]
        .as_u64()
        .ok_or("account record count")?;
    if mirror.prefix().sequence != count {
        return Err("mirror does not contain the full account prefix".into());
    }
    for first in (1..=count).step_by(tickwire_replica::MAX_BATCH_RECORDS) {
        for record in mirror.original_records(first, tickwire_replica::MAX_BATCH_RECORDS)? {
            let expected = fs::read(
                exported
                    .join("records")
                    .join(format!("{:020}.cbor", record.seq)),
            )?;
            if record.payload != expected {
                return Err("mirror canonical record differs from account WAL".into());
            }
        }
    }
    if s.kind.handover() {
        let mut final_s = s.clone();
        final_s.worker_bootstrap = s.root.join("next/bootstrap.json");
        check_reports(&final_s, &exported, &report, pid, count)?;
        crate::handover::check_export(s, &exported, &report, count)?;
    } else {
        check_reports(s, &exported, &report, pid, count)?;
    }
    check_fee_closure(s, &exported, count)?;
    crate::generate::json_file(&s.root.join("account-report.json"), &report)?;
    let mut result = json!({"event":if s.kind.handover(){"laboratory_spot_handover_complete"}else{"laboratory_spot_fill_complete"},"account_fill_verified":true,"mirror_records_verified":count,"worker_report_position_verified":count,"account_report":s.root.join("account-report.json"),"fixture_audit":s.root.join("fixture.jsonl"),"account_wal":s.root.join("exec/account"),"input_wal":s.root.join("exec/input"),"mirror_wal":s.root.join("mirror/journal"),"logs":s.root.join("logs")});
    if s.kind.handover() {
        result["preflight"] = serde_json::from_slice(&fs::read(
            s.root.join("handover/preflight-final-verified.json"),
        )?)?;
    }
    if let Some(driver) = &expansion {
        let evidence = driver.evidence()?;
        let observed = evidence["source_before_c"]["replay_cut"]["sequence"]
            .as_u64()
            .ok_or("expansion source frontier")?;
        let fixed = evidence["source_before_c"]["source_s"]["replay_cut"]["sequence"]
            .as_u64()
            .ok_or("expansion source S")?;
        if result["preflight"]["source_c_sequence"]
            .as_u64()
            .is_none_or(|c| c < observed)
            || result["preflight"]["source_s_sequence"] != fixed
            || result["preflight"]["expansion_prefix"]["fixed_proof"]
                != evidence["source_before_c"]["source_s"]["seal"]
            || result["preflight"]["expansion_prefix"]["fixed_checkpoint"]
                != evidence["source_before_c"]["source_s"]["checkpoint"]
            || result["preflight"]["expansion_prefix"]["observed_proof"]
                != evidence["source_before_c"]["seal"]
            || result["preflight"]["expansion_prefix"]["observed_checkpoint"]
                != evidence["source_before_c"]["checkpoint"]
            || result["preflight"]["expansion_prefix"]["prefix_verified"] != true
        {
            return Err("final replay cut does not cover the verified market expansion".into());
        }
        result["market_expansion"] = evidence.clone();
        result["event"] = json!("laboratory_spot_market_expansion_complete");
    }
    crate::generate::json_file(&s.root.join("run-result.json"), &result)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
fn spawn(mut command: Command, s: &Scenario, name: &str) -> Result<Child> {
    command
        .stdout(Stdio::from(File::create(
            s.root.join(format!("logs/{name}.stdout.log")),
        )?))
        .stderr(Stdio::from(File::create(
            s.root.join(format!("logs/{name}.stderr.log")),
        )?))
        .current_dir(&s.root);
    Ok(command.spawn()?)
}
fn spawn_mirror(
    s: &Scenario,
    children: &mut Children,
) -> Result<mpsc::Receiver<std::result::Result<(), String>>> {
    let mut command = Command::new(s.bin_directory.join("tickwire-replica-receiver"));
    command
        .arg(&s.mirror_config)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(File::create(
            s.root.join("logs/mirror.stderr.log"),
        )?));
    let mut c = command.spawn()?;
    let output = c.stdout.take().ok_or("mirror stdout")?;
    let path = s.root.join("logs/mirror.stdout.log");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let Ok(mut file) = File::create(path) else {
            let _ = tx.send(Err("mirror log".into()));
            return;
        };
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            let _ = writeln!(file, "{line}");
            if line.starts_with("replica receiver listening on ") {
                let _ = tx.send(Ok(()));
            }
        }
    });
    children.0.push(c);
    Ok(rx)
}
fn wait_file(path: &Path, children: &mut Children, timeout: Duration) -> Result<()> {
    let until = Instant::now() + timeout;
    while !path.exists() {
        for c in &mut children.0 {
            if let Some(status) = c.try_wait()? {
                return Err(format!("child {} exited {status}", c.id()).into());
            }
        }
        if Instant::now() >= until {
            return Err(format!("readiness timeout: {}", path.display()).into());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}
fn replace_json(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("updated.json");
    crate::generate::json_file(&temporary, value)?;
    fs::rename(temporary, path)?;
    File::open(path.parent().ok_or("parent")?)?.sync_all()?;
    Ok(())
}

fn hash_field(value: &Value, key: &str) -> Result<[u8; 32]> {
    Ok(serde_json::from_value(value[key].clone())?)
}
fn check_reports(
    s: &Scenario,
    exported: &Path,
    report: &Value,
    exec_pid: u32,
    count: u64,
) -> Result<()> {
    let bootstrap: tickwire_strategy_sdk::bootstrap::Bootstrap =
        serde_json::from_slice(&fs::read(&s.worker_bootstrap)?)?;
    let identity = &bootstrap.report.local;
    let name = identity
        .instance
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let receipt_path = bootstrap
        .report_directory
        .join(format!("CONSUMED-{name}-{}.json", identity.generation));
    let bytes = fs::read(&receipt_path)?;
    if bytes.len() > 16 * 1024 {
        return Err("consumption receipt capacity".into());
    }
    let receipt: Value = serde_json::from_slice(&bytes)?;
    let pid = receipt["worker_pid"]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .filter(|p| *p > 0)
        .ok_or("worker pid")?;
    let market = bootstrap
        .external_market
        .as_ref()
        .ok_or("external market admission")?;
    let admission_path =
        tickwire_md::server::admission_path(&s.root.join("md/admissions"), pid, market.request_id)
            .map_err(|e| e.to_string())?;
    let admitted =
        tickwire_md::config::read(&admission_path, 2 * 1024 * 1024).map_err(|e| e.to_string())?;
    let admission: tickwire_md::server::Admission = serde_json::from_slice(&admitted)?;
    if admission.pid != pid
        || admission.supervisor_pid != exec_pid
        || admission.client.endpoint.local.identity() != identity.identity()
    {
        return Err("worker admission differs from authenticated consumption receipt".into());
    }
    let baseline = tickwire_account_codec::CanonicalBaseline::decode(&fs::read(
        exported.join("baseline.cbor"),
    )?)
    .map_err(|e| format!("baseline {e:?}"))?;
    let mut input = tickwire_account_codec::InputSeal::new(baseline.digest())
        .map_err(|e| format!("input {e:?}"))?;
    let mut records = Vec::new();
    for position in 1..=count {
        let record = tickwire_account_codec::CanonicalRecord::decode(&fs::read(
            exported
                .join("records")
                .join(format!("{position:020}.cbor")),
        )?)
        .map_err(|e| format!("record {e:?}"))?;
        input.apply(&record).map_err(|e| format!("input {e:?}"))?;
        records.push(record.record().clone());
    }
    let account = tickwire_account::Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &records,
    )
    .map_err(|e| format!("worker mirror {e:?}"))?;
    let expected = json!({"schema":1,"host_pid":exec_pid,"instance":identity.instance,"generation":identity.generation,"logical_strategy":identity.logical_strategy,"host_boot":identity.host_boot,"build_hash":identity.build_hash,
        "exec_instance":bootstrap.report.peer.instance,"exec_generation":bootstrap.report.peer.generation,"worker_uid":admission.uid,"worker_gid":admission.gid,
        "account_position":count,"account_input_digest":input.digest(),"ledger_sequence":account.ledger_sequence(),"orders_digest":account.strategy_orders_digest(identity.logical_strategy),"cursor_resource":bootstrap.report_cursor_id,"subscription_generation":bootstrap.report_subscription_generation});
    for (key, value) in expected.as_object().ok_or("receipt expected")? {
        if receipt[key] != *value {
            return Err(format!("worker report confirmation mismatch: {key}").into());
        }
    }
    if receipt["ledger_sequence"] != report["ledger_sequence"]
        || receipt["report_sequence"].as_u64()
            != bootstrap
                .report_resource()
                .map_err(|e| e.to_string())?
                .first_seq
                .checked_add(count)
    {
        return Err("worker terminal report cut differs from account WAL".into());
    }
    let seal_hash = hash_field(&receipt, "input_seal_digest")?;
    let recovery = bootstrap
        .recovery_template
        .as_ref()
        .ok_or("recovery template")?;
    let template: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&fs::read(recovery)?)?;
    let object = |h: [u8; 32]| -> Result<Vec<u8>> {
        let filename = h.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let b = fs::read(template.output_directory.join(format!("{filename}.bin")))?;
        if tickwire_recovery::digest(&b) != h {
            return Err("worker durable object hash".into());
        }
        Ok(b)
    };
    let seal = tickwire_recovery::HandoverSeal::decode(&object(seal_hash)?)?;
    if seal.account_input_digest != input.digest()
        || seal.report_position != count
        || seal.instance != identity.instance
        || seal.generation != identity.generation
        || seal.ledger_sequence != account.ledger_sequence()
        || seal.orders_digest != account.strategy_orders_digest(identity.logical_strategy)
        || seal.checkpoint_digest != hash_field(&receipt, "checkpoint_digest")?
    {
        return Err("worker final durable checkpoint cut".into());
    }
    seal.verify_checkpoint(&object(seal.checkpoint_digest)?)?;
    if object(seal.account_input_digest)? != input.object_bytes() {
        return Err("worker canonical account input seal".into());
    }
    let mut last = None;
    for line in BufReader::new(File::open(s.root.join("logs/exec.stderr.log"))?).lines() {
        let line = line?;
        if line.len() > 64 * 1024 {
            return Err("execution log line capacity".into());
        }
        if let Ok(v) = serde_json::from_str::<Value>(&line)
            && v["event"] == "strategy_ready"
            && v["worker_pid"] == pid
        {
            last = Some(v);
        }
    }
    let last = last.ok_or("missing authenticated worker terminal READY")?;
    for key in [
        "ready_sequence",
        "checkpoint_digest",
        "orders_digest",
        "ledger_sequence",
        "input_seal_digest",
    ] {
        if last[key] != receipt[key] {
            return Err("last authenticated READY was not confirmed by actual cursor".into());
        }
    }
    if last["exec_report_sequence"] != count {
        return Err("worker did not finish the final account report".into());
    }
    Ok(())
}
fn fixed_equals(value: &Value, numerator: i128, denominator: i128) -> bool {
    let Some(atoms) = value["atoms"].as_str().and_then(|s| s.parse::<i128>().ok()) else {
        return false;
    };
    let Some(scale) = value["scale"].as_u64().filter(|s| *s <= 18) else {
        return false;
    };
    atoms.checked_mul(denominator) == numerator.checked_mul(10i128.pow(scale as u32))
}
fn check_account(report: &Value) -> Result<()> {
    if report["frozen"] != true
        || report["input_facts_not_in_account"] != 0
        || !report["rejected_event_sha256"].is_null()
    {
        return Err("account recovery has unapplied or rejected facts".into());
    }
    let orders = report["orders"].as_array().ok_or("orders")?;
    if orders.len() != 1
        || orders[0]["client_id"] != "lab-spot-1"
        || orders[0]["terminal"] != true
        || orders[0]["aborted"] != false
        || orders[0]["fee_reconciled"] != true
        || orders[0]["unknown"] != false
        || !fixed_equals(&orders[0]["filled_quantity"], 1, 1000)
        || !fixed_equals(&orders[0]["remaining_quantity"], 0, 1)
    {
        return Err("expected filled laboratory order is missing".into());
    }
    let assets = report["assets"].as_array().ok_or("assets")?;
    for (symbol, n, d) in [("BTC", 1001, 1000), ("USDT", 940, 1)] {
        let row = assets
            .iter()
            .find(|r| r["symbol"] == symbol)
            .ok_or("asset")?;
        if !fixed_equals(&row["total"], n, d) {
            return Err(format!("unexpected recovered {symbol} balance").into());
        }
    }
    Ok(())
}
fn check_fee_closure(s: &Scenario, exported: &Path, count: u64) -> Result<()> {
    let mut fees = false;
    let mut activations = 0;
    for position in 1..=count {
        let raw = fs::read(
            exported
                .join("records")
                .join(format!("{position:020}.cbor")),
        )?;
        let record = tickwire_account_codec::CanonicalRecord::decode(&raw)
            .map_err(|e| format!("record: {e:?}"))?;
        match record.record() {
            tickwire_account::RecoveryRecord::FeesReconciled { .. } => fees = true,
            tickwire_account::RecoveryRecord::Reactivated(_) => activations += 1,
            _ => {}
        }
    }
    let audit = fs::read_to_string(s.root.join("fixture.jsonl"))?;
    let requests = audit
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect::<Vec<_>>();
    if !fees
        || activations < 2
        || !["/api/v3/order", "/api/v3/myTrades"].iter().all(|path| {
            requests.iter().any(|v| {
                v["event"] == "http"
                    && v["method"] == "GET"
                    && v["path"] == *path
                    && v["status"] == 200
            })
        })
    {
        return Err(
            "terminal fees lack the complete actual query and durable reconciliation chain".into(),
        );
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn result_check_requires_exact_economic_fill() {
        let mut report = json!({"frozen":true,"input_facts_not_in_account":0,"rejected_event_sha256":null,"orders":[{"client_id":"lab-spot-1","terminal":true,"aborted":false,"fee_reconciled":true,"unknown":false,"filled_quantity":{"atoms":"100000","scale":8},"remaining_quantity":{"atoms":"0","scale":8}}],"assets":[{"symbol":"BTC","total":{"atoms":"100100000","scale":8}},{"symbol":"USDT","total":{"atoms":"94000000000","scale":8}}]});
        check_account(&report).unwrap();
        for field in ["fee_reconciled", "unknown"] {
            let mut incomplete = report.clone();
            incomplete["orders"][0][field] = json!(field == "unknown");
            assert!(check_account(&incomplete).is_err());
        }
        report["orders"][0]["filled_quantity"]["atoms"] = json!("0");
        assert!(check_account(&report).is_err());
    }
}
