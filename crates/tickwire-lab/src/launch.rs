//! Stateless 3-process laboratory orchestrator (`tickwire-md` + `tickwire-app` +
//! `tickwire-strategy-worker`).
//!
//! Verifies sealed `memfd` market broadcast, cold Exchange REST `Account + OpenOrders` + `tw1...`
//! `SelfDescribingClientId` recovery, `DOP` sequence-floor continuity (`order_seqnum = 3 -> 4`),
//! Ed25519 `/ws-api/v3` order placement & commission fill settlement, `SIGUSR2` emergency cancel of
//! the cold-recovered resting order, `SIGTERM` graceful `OrsShutDown` worker exit, and exact
//! `Fixed` post-fill balance accounting—with zero local WAL, journal, or disk blob files.

#![forbid(unsafe_code)]

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tickwire_app::OrsShutdownSummary;
use tickwire_market_source::protocol::IdentityConfig;

use crate::model::{LAB_COLD_OPEN_SEQNUM, expected_post_fill_balances};
use crate::{Result, Scenario};

struct ManagedChildren(Vec<(String, Child)>);

impl Drop for ManagedChildren {
    fn drop(&mut self) {
        for (_, child) in self.0.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Spawned before `tickwire-md` one-shot admission is written; `exec` preserves the exact OS PID
/// recorded in `md/admissions/` when launching `tickwire-strategy-worker --bootstrap <config>`.
pub fn child_exec(config: &Path, gate: &Path, binary: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !gate.is_file() {
        if Instant::now() >= deadline {
            return Err("strategy worker admission gate timed out".into());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Err(Command::new(binary)
        .arg("--bootstrap")
        .arg(config)
        .exec()
        .into())
}

/// Executes a generated single-use loopback [`Scenario`] end-to-end on Linux.
pub fn run(s: &Scenario) -> Result<()> {
    if !cfg!(target_os = "linux") {
        return Err("tickwire-lab 3-process execution requires Linux memfd + AF_UNIX".into());
    }
    s.validate()?;

    let domain = tickwire_clock::ClockDomain::new(s.boot, 1).map_err(|_| "clock domain")?;
    let now_mono_ns = tickwire_clock_sys::SystemClock::new(domain)?
        .sample()?
        .monotonic
        .nanos();
    if now_mono_ns >= s.valid_until_ns {
        return Err("scenario authorization window expired; generate a fresh directory".into());
    }
    if s.root.join("STARTED").exists() {
        return Err("scenario directories are single-use".into());
    }
    crate::generate::write_private(&s.root.join("STARTED"), b"started\n")?;

    let scenario_path = s.root.join("scenario.json");
    let mut children = ManagedChildren(Vec::new());

    // 1. Start deterministic TLS exchange fixture.
    let mut fixture_cmd = Command::new(s.bin_directory.join("tickwire-lab"));
    fixture_cmd.arg("serve").arg("--config").arg(&scenario_path);
    children
        .0
        .push(("fixture".into(), spawn_logged(fixture_cmd, s, "fixture")?));
    wait_for_path(
        &s.root.join("fixture.ready"),
        &mut children,
        Duration::from_secs(10),
    )?;

    // 2. Start `tickwire-md` public Spot depth publisher.
    let mut md_cmd = Command::new(s.bin_directory.join("tickwire-md"));
    md_cmd.arg("--config").arg(&s.md_config);
    children
        .0
        .push(("md".into(), spawn_logged(md_cmd, s, "md")?));
    let md_pid = children.0.last().ok_or("missing md child")?.1.id();
    wait_for_path(
        &s.root.join("md/market.sock"),
        &mut children,
        Duration::from_secs(20),
    )?;

    // 3. Start `tickwire-app` stateless ORS process (cold-bootstraps from TLS REST `account` +
    //    `openOrders`, binds `dop.sock` + `intent.sock` + `report.sock`, logs onto `/ws-api/v3`
    //    with Ed25519, and subscribes to private user-data events).
    let mut ors_cmd = Command::new(s.bin_directory.join("tickwire-app"));
    ors_cmd.args(["run", "--config"]).arg(&s.app_config);
    children
        .0
        .push(("ors".into(), spawn_logged(ors_cmd, s, "ors")?));
    let ors_pid = children.0.last().ok_or("missing ors child")?.1.id();
    for required in ["ors/dop.sock", "ors/intent.sock", "ors/report.sock"] {
        wait_for_path(
            &s.root.join(required),
            &mut children,
            Duration::from_secs(20),
        )?;
    }

    // 4. Patch `md_pid` and `ors_pid` into `strategy/bootstrap.json`, register the one-shot
    //    `tickwire-md` admission ticket for `worker_pid`, and release `worker.gate`.
    let mut worker_json: Value = serde_json::from_slice(&fs::read(&s.worker_bootstrap)?)?;
    worker_json["market"]["endpoint"]["peer_pid"] = json!(md_pid);
    worker_json["intent"]["peer_pid"] = json!(ors_pid);
    worker_json["report"]["peer_pid"] = json!(ors_pid);
    replace_private_json(&s.worker_bootstrap, &worker_json)?;

    let gate_path = s.root.join("worker.gate");
    let mut worker_cmd = Command::new(s.bin_directory.join("tickwire-lab"));
    worker_cmd
        .arg("child-exec")
        .arg(&s.worker_bootstrap)
        .arg(&gate_path)
        .arg(s.bin_directory.join("tickwire-strategy-worker"));
    children
        .0
        .push(("worker".into(), spawn_logged(worker_cmd, s, "worker")?));
    let worker_pid = children.0.last().ok_or("missing worker child")?.1.id();

    let connect_cfg: tickwire_market_source::ConnectConfig =
        serde_json::from_value(worker_json["market"].clone())?;
    let supervisor_id: IdentityConfig =
        serde_json::from_slice(&fs::read(s.root.join("supervisor.json"))?)?;
    let root_meta = fs::metadata(&s.root)?;
    let admission_path = tickwire_md::server::admission_path(
        &s.root.join("md/admissions"),
        worker_pid,
        connect_cfg.request_id,
    )
    .map_err(|e| e.to_string())?;
    tickwire_md::server::register_admission(
        &s.root.join("md/admissions"),
        &tickwire_md::server::Admission {
            client: connect_cfg,
            pid: worker_pid,
            uid: root_meta.uid(),
            gid: root_meta.gid(),
            supervisor: supervisor_id,
            supervisor_pid: std::process::id(),
            snapshot_id: [67; 16],
        },
    )
    .map_err(|e| e.to_string())?;
    crate::generate::write_private(&gate_path, b"admitted\n")?;

    // Wait for `tickwire-strategy-worker` to complete `JOIN` + `DOP` + SHM lane attachment and for
    // `"tw-ors-resp"` to complete `/ws-api/v3` Ed25519 `session.logon` + `userDataStream.subscribe`,
    // then arm the 60,000.00 BTCUSDT bid in `fixture`.
    let _ = admission_path;
    wait_for_path(
        &s.root.join("PRIVATE_SUBSCRIBED"),
        &mut children,
        Duration::from_secs(20),
    )?;
    std::thread::sleep(Duration::from_millis(150));
    ensure_all_alive(&mut children)?;
    crate::generate::write_private(&s.root.join("ARM"), b"armed\n")?;

    // 5. Wait for `fixture.jsonl` to record `"private_fill_sent"`, then trigger `SIGUSR2`
    //    (`EmergencyCancelAll` canceling the cold-recovered resting order `19`) followed by
    //    `SIGTERM` (`OrsShutDown` -> clean `tickwire-strategy-worker` & `tickwire-app` exit 0).
    let audit_path = s.root.join("fixture.jsonl");
    wait_for_audit_event(
        &audit_path,
        "private_fill_sent",
        &mut children,
        Duration::from_secs(20),
    )?;
    std::thread::sleep(Duration::from_millis(120));

    send_posix_signal("USR2", ors_pid)?;
    wait_for_audit_event(
        &audit_path,
        "cold_order_canceled",
        &mut children,
        Duration::from_secs(10),
    )?;
    std::thread::sleep(Duration::from_millis(80));

    send_posix_signal("TERM", ors_pid)?;

    // Both `worker` (on `OrsShutDown`) and `ors` (after writing `account-report.json`) must exit 0!
    wait_clean_exit(&mut children, "worker", Duration::from_secs(10))?;
    wait_clean_exit(&mut children, "ors", Duration::from_secs(10))?;

    let _ = crate::generate::write_private(&s.root.join("STOP"), b"complete\n");
    drop(children);

    // 6. Verify `account-report.json` (`OrsShutdownSummary`) and `fixture.jsonl` audit trail.
    let summary_bytes = fs::read(s.root.join("account-report.json"))?;
    let summary: OrsShutdownSummary = serde_json::from_slice(&summary_bytes)?;
    verify_shutdown_summary(&summary)?;
    let (elapsed_ns, verified_client_id) = verify_fixture_audit(&audit_path)?;

    let result = json!({
        "event": "laboratory_spot_fill_complete",
        "topology": "stateless_3_process_shm",
        "account_fill_verified": true,
        "cold_rest_recovery_verified": true,
        "cold_recovered_seqnum": LAB_COLD_OPEN_SEQNUM,
        "live_order_seqnum": LAB_COLD_OPEN_SEQNUM + 1,
        "live_client_id": verified_client_id,
        "eligible_quote_to_order_ns": elapsed_ns,
        "open_orders_at_shutdown": summary.open_order_and_quote_count,
        "account_report": s.root.join("account-report.json"),
        "fixture_audit": audit_path,
        "logs": s.root.join("logs")
    });
    crate::generate::write_json(&s.root.join("run-result.json"), &result)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn verify_shutdown_summary(summary: &OrsShutdownSummary) -> Result<()> {
    if summary.venue != "binance" {
        return Err(format!("expected venue 'binance', got '{}'", summary.venue).into());
    }
    if summary.open_order_and_quote_count != 0 {
        return Err(format!(
            "expected 0 open orders after SIGUSR2 + fill, got {}",
            summary.open_order_and_quote_count
        )
        .into());
    }
    let (expected_btc, expected_usdt) = expected_post_fill_balances();
    let btc = summary
        .balances
        .iter()
        .find(|b| b.wallet_id == 1 && b.asset_id == 1)
        .ok_or("missing BTC balance in account-report.json")?;
    let usdt = summary
        .balances
        .iter()
        .find(|b| b.wallet_id == 1 && b.asset_id == 2)
        .ok_or("missing USDT balance in account-report.json")?;

    if btc.free_atoms != expected_btc.atoms().to_string() || btc.locked_atoms != "0" {
        return Err(format!(
            "unexpected post-fill BTC balance: free={} locked={} (expected free={})",
            btc.free_atoms,
            btc.locked_atoms,
            expected_btc.atoms()
        )
        .into());
    }
    if usdt.free_atoms != expected_usdt.atoms().to_string() || usdt.locked_atoms != "0" {
        return Err(format!(
            "unexpected post-fill USDT balance: free={} locked={} (expected free={})",
            usdt.free_atoms,
            usdt.locked_atoms,
            expected_usdt.atoms()
        )
        .into());
    }
    Ok(())
}

fn verify_fixture_audit(audit_path: &Path) -> Result<(u64, String)> {
    let file = File::open(audit_path)?;
    let mut saw_cold = false;
    let mut saw_logon = false;
    let mut saw_fill = false;
    let mut saw_cancel = false;
    let mut elapsed_ns: Option<u64> = None;
    let mut live_cid: Option<String> = None;

    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let val: Value = serde_json::from_str(&line)?;
        match val["event"].as_str() {
            Some("cold_bootstrap_served") => saw_cold = true,
            Some("ws_session_logon_verified") => saw_logon = true,
            Some("eligible_quote_to_order") => {
                elapsed_ns = val["elapsed_ns"].as_u64();
            }
            Some("order_accepted") => {
                if val["order_seqnum"].as_u64() != Some(LAB_COLD_OPEN_SEQNUM + 1) {
                    return Err("live order did not advance past cold-recovered seqnum".into());
                }
                live_cid = val["client_id"].as_str().map(str::to_owned);
            }
            Some("private_fill_sent") => saw_fill = true,
            Some("cold_order_canceled") => saw_cancel = true,
            _ => {}
        }
    }

    if !(saw_cold && saw_logon && saw_fill && saw_cancel) {
        return Err(format!(
            "incomplete fixture audit trail (cold={saw_cold}, logon={saw_logon}, fill={saw_fill}, cancel={saw_cancel})"
        )
        .into());
    }
    Ok((
        elapsed_ns.ok_or("missing eligible_quote_to_order elapsed_ns")?,
        live_cid.ok_or("missing order_accepted client_id")?,
    ))
}

fn spawn_logged(mut cmd: Command, s: &Scenario, name: &str) -> Result<Child> {
    cmd.stdout(Stdio::from(File::create(
        s.root.join(format!("logs/{name}.stdout.log")),
    )?))
    .stderr(Stdio::from(File::create(
        s.root.join(format!("logs/{name}.stderr.log")),
    )?))
    .current_dir(&s.root);
    Ok(cmd.spawn()?)
}

fn ensure_all_alive(children: &mut ManagedChildren) -> Result<()> {
    for (name, child) in &mut children.0 {
        if let Some(status) = child.try_wait()? {
            return Err(format!("process '{name}' exited prematurely with {status}").into());
        }
    }
    Ok(())
}

fn wait_for_path(path: &Path, children: &mut ManagedChildren, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        ensure_all_alive(children)?;
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {}", path.display()).into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn wait_for_audit_event(
    audit_path: &Path,
    expected_event: &str,
    children: &mut ManagedChildren,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(raw) = fs::read_to_string(audit_path) {
            for line in raw.lines() {
                if let Ok(val) = serde_json::from_str::<Value>(line)
                    && val["event"].as_str() == Some(expected_event)
                {
                    return Ok(());
                }
            }
        }
        ensure_all_alive(children)?;
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for fixture event '{expected_event}'").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_clean_exit(
    children: &mut ManagedChildren,
    target_name: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let (_, child) = children
        .0
        .iter_mut()
        .find(|(n, _)| n == target_name)
        .ok_or_else(|| format!("unknown child '{target_name}'"))?;
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                return Err(format!("process '{target_name}' exited with {status}").into());
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for '{target_name}' clean exit").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn send_posix_signal(signal_name: &str, pid: u32) -> Result<()> {
    let status = Command::new("kill")
        .args(["-s", signal_name, &pid.to_string()])
        .status()?;
    if !status.success() {
        return Err(format!("kill -s {signal_name} {pid} failed with {status}").into());
    }
    Ok(())
}

fn replace_private_json(path: &Path, value: &Value) -> Result<()> {
    let tmp = path.with_extension("updated.json");
    crate::generate::write_json(&tmp, value)?;
    fs::rename(&tmp, path)?;
    File::open(path.parent().ok_or("missing parent dir")?)?.sync_all()?;
    Ok(())
}
