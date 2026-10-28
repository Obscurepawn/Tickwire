use super::*;
use std::os::unix::fs::DirBuilderExt;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_release::{rebuild::*, replay};
pub(crate) fn configure(s: &Scenario) -> Result<()> {
    for name in ["capture", "publication"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(s.root.join(name))?;
    }
    let mut b: Bootstrap = serde_json::from_value(json(&s.root.join("next/bootstrap.json"))?)?;
    b.preflight_job = None;
    b.market = b
        .external_market
        .as_ref()
        .ok_or("external MD")?
        .endpoint
        .clone();
    b.report.path = s.root.join("capture/report.sock");
    b.intent.path = s.root.join("next/intent.sock");
    b.market_admission_ready = Some(s.root.join("capture/md-ready.json"));
    let mut header = ResourceHeader::decode(&b.report_header)?;
    header.descriptor_hash = [0; 32];
    header.descriptor_hash =
        tickwire_recovery::digest(&tickwire_market_source::protocol::wire_bytes(&header)?);
    b.report_header = tickwire_market_source::protocol::wire_bytes(&header)?;
    atomic_json(
        &s.root.join("next/bootstrap.json"),
        &serde_json::to_value(&b)?,
    )?;
    let mut h: tickwire_app::hosted::HostConfig =
        serde_json::from_value(json(&s.root.join("next/host.json"))?)?;
    h.capture = Some(tickwire_app::hosted::CaptureConfig {
        bootstrap: s.root.join("capture/bootstrap.json"),
        publication_directory: s.root.join("publication"),
    });
    atomic_json(&s.root.join("next/host.json"), &serde_json::to_value(&h)?)?;
    refresh_template(s, &b, &h)
}
fn refresh_template(
    s: &Scenario,
    b: &Bootstrap,
    h: &tickwire_app::hosted::HostConfig,
) -> Result<()> {
    let path = b.recovery_template.as_ref().ok_or("template")?;
    let mut t: tickwire_strategy_sdk::recovery::Template = serde_json::from_value(json(path)?)?;
    t.declared_rebuild = true;
    for d in &mut t.dependencies {
        if let Some(path) = match d.kind {
            8 => Some(h.executable.clone()),
            9 => Some(s.root.join("next/host.json")),
            11 => Some(crate::flavor::config_path(s.kind, &s.root.join("next"))),
            _ => None,
        } {
            d.digest = tickwire_recovery::digest(&read(
                &path,
                tickwire_recovery::MAX_OBJECT_BYTES as u64,
            )?);
            d.path = path;
        }
    }
    atomic_json(path, &serde_json::to_value(t)?)
}
pub(super) fn launch(
    s: &Scenario,
    exec_pid: u32,
    md_pid: u32,
    account_position: u64,
    command: &tickwire_venue::CanonicalCommand,
) -> Result<()> {
    let mut b: Bootstrap = serde_json::from_value(json(&s.root.join("next/bootstrap.json"))?)?;
    let external = b.external_market.as_mut().ok_or("external MD")?;
    external.endpoint.peer_pid = md_pid;
    b.market = external.endpoint.clone();
    b.report.peer_pid = exec_pid;
    b.intent.peer_pid = exec_pid;
    let h: tickwire_app::hosted::HostConfig =
        serde_json::from_value(json(&s.root.join("next/host.json"))?)?;
    b.report.peer_uid = h.uid;
    b.report.peer_gid = h.gid;
    b.intent.peer_uid = h.uid;
    b.intent.peer_gid = h.gid;
    let f = b.factory.as_ref().ok_or("registered factory")?;
    let config = replay::VersionedBytes {
        revision: f.revision,
        bytes: read(&f.configuration_file, 2 * 1024 * 1024)?,
    };
    let mut bank =
        ResourceHeader::decode(&b.external_market.as_ref().ok_or("external")?.bank_header)?;
    bank.descriptor_hash = [0; 32];
    bank.descriptor_hash =
        tickwire_recovery::digest(&tickwire_market_source::protocol::wire_bytes(&bank)?);
    let contract = Contract {
        schema: 1,
        factory: f.id.clone(),
        binary: b.market.local.build_hash,
        state_schema: tickwire_strategy_example::V2,
        config: Object::of(&config.bytes),
        models: vec![],
        ordering: Ordering::ObservedRebuildWindow,
        markets: vec![MarketRequirement {
            stream: tickwire_strategy_sdk::replay::application::market_stream(&bank)?,
            instrument: 12,
            category: 1,
            minimum_observations: 2,
            known_bid_prices: vec![],
            known_ask_prices: vec![],
        }],
        actions: vec![ActionBinding {
            economic_intent: command.command().context.strategy.economic_intent_id,
            admitted_command: Object::of(command.bytes()),
        }],
        minimum_span_ns: 500_000_000,
        maximum_span_ns: 15_000_000_000,
        maximum_age_ns: b.maximum_market_age_ns,
        maximum_rows: 4096,
        maximum_bytes: 16 * 1024 * 1024,
        rng: replay::Rng::Disabled,
    };
    let capture = tickwire_strategy_sdk::capture::Bootstrap {
        schema: 1,
        identity: window::Identity {
            capture: [151; 16],
            logical_strategy: 8,
            risk_domain: [1; 16],
            instance: NEW_INSTANCE,
            generation: 6,
            binary: contract.binary,
            clock: replay::ClockDomain {
                boot_id: s.boot,
                generation: 1,
            },
        },
        contract,
        config,
        models: vec![],
        market: b.external_market.clone().ok_or("external")?,
        market_admission_ready: b.market_admission_ready.clone().ok_or("admission")?,
        report: b.report.clone(),
        report_header: b.report_header.clone(),
        report_cursor_id: b.report_cursor_id,
        report_subscription_generation: b.report_subscription_generation,
        report_directory: b.report_directory.clone(),
        output_directory: s.root.join("capture/output"),
        initial_account_position: account_position,
        deadline_mono_ns: s.valid_until_ns,
        poll_records: b.poll_records,
        idle_sleep_us: b.idle_sleep_us,
    };
    capture.validate(super::driver::now(s)?)?;
    // Canonical bytes become immutable launch/manifest roots before spawning.
    replace(
        &s.root.join("next/bootstrap.json"),
        &serde_json::to_vec(&b)?,
    )?;
    replace(&s.root.join("next/host.json"), &serde_json::to_vec(&h)?)?;
    refresh_template(s, &b, &h)?;
    replace(&s.root.join("capture/bootstrap.json"), &capture.bytes()?)?;
    let request = tickwire_app::launch::Request {
        version: 1,
        sequence: 1,
        request_id: [139; 16],
        exec_instance: [4; 16],
        writer_epoch: 4,
        logical_strategy_id: 8,
        instance_id: NEW_INSTANCE,
        strategy_generation: 6,
        host_config: s.root.join("next/host.json"),
        host_config_digest: tickwire_recovery::digest(&fs::read(s.root.join("next/host.json"))?),
        bootstrap_digest: tickwire_recovery::digest(&fs::read(s.root.join("next/bootstrap.json"))?),
    };
    publish(
        &s.root.join("handover/launch/00000000000000000001.json"),
        &serde_json::to_vec(&request)?,
    )?;
    publish(
        &s.root.join("publication/capture-seal.json"),
        &serde_json::to_vec(
            &json!({"schema":1,"capture":([151;16]),"account_position":account_position}),
        )?,
    )
}
pub(super) fn replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("pending");
    crate::generate::write(&tmp, bytes)?;
    fs::rename(tmp, path)?;
    fs::File::open(path.parent().ok_or("parent")?)?.sync_all()?;
    Ok(())
}
