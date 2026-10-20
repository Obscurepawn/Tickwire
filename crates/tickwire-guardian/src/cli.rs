//! Standalone deployment adapter. No execution-process RPC is used.
use super::*;
use serde::Deserialize;
use std::{
    fs::{self, File},
    io::Read,
    net::ToSocketAddrs,
    path::{Path, PathBuf},
};
use tickwire_binance::auth::SigningKey;
use tickwire_binance_clock::{CalibrationConfig, Config as TimeConfig, NetworkConfig, SystemClock};
use tickwire_journal::store::FileStore;

type CliResult<T> = core::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub guardian: Config,
    pub recover: bool,
    pub audit_directory: PathBuf,
    pub controller_id: [u8; 16],
    pub fence_key: Hash,
    pub emergency_key: Hash,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub calibration: Calibration,
    pub time_address: String,
    pub rest_address: String,
    pub trust_anchors: Vec<PathBuf>,
    pub api_key_file: PathBuf,
    pub signing_key_file: PathBuf,
    pub algorithm: Algorithm,
    pub quota: tickwire_binance_quota::bootstrap::Config,
    pub authorization_file: PathBuf,
    pub commands: Vec<PathBuf>,
    pub maximum_runtime_ns: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Calibration {
    pub max_round_trip_ns: u64,
    pub max_width_ns: u64,
    pub max_age_ns: u64,
    pub quantization_error_ns: u64,
    pub drift_ppb: u64,
    pub max_adjustment_ns: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    Hmac,
    RsaPkcs8,
    Ed25519Pkcs8,
}
pub fn main_entry() -> CliResult<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 || args[1] != "--config" {
        return Err("usage: tickwire-guardian --config /absolute/config.json".into());
    }
    let deployment: Deployment =
        serde_json::from_slice(&read(&PathBuf::from(&args[2]), 4 * 1024 * 1024, false)?)?;
    run(deployment)
}
pub fn run(d: Deployment) -> CliResult<()> {
    d.guardian.validate()?;
    if !d.audit_directory.is_absolute()
        || d.commands.is_empty()
        || d.commands.len() > 4096
        || d.trust_anchors.is_empty()
        || d.trust_anchors.len() > 128
        || d.maximum_runtime_ns == 0
        || d.maximum_runtime_ns > 3_600_000_000_000
    {
        return Err(Error::Invalid.into());
    }
    // Guardian spends only the explicitly assigned recovery pool, including
    // calibration and connection charges. This backend models the external
    // allocator; actual local consumption remains the real Endpoint.
    use tickwire_binance_quota::bootstrap::Class;
    if d.quota
        .rules
        .iter()
        .any(|r| !matches!(r.class, Class::Recovery))
        || d.quota
            .grants
            .iter()
            .any(|g| !matches!(g.class, Class::Recovery))
        || d.quota.windows.iter().any(|w| w.recovery_reserve == 0)
    {
        return Err(Error::Denied.into());
    }
    let domain =
        ClockDomain::new(d.clock_boot_id, d.clock_generation).map_err(|_| Error::Identity)?;
    let mut system = SystemClock::new(domain).map_err(|_| Error::Clock)?;
    let started = system.sample().map_err(|_| Error::Clock)?;
    let quota = d
        .quota
        .build(tickwire_quota::MockQuotaStore::default(), started.monotonic)
        .map_err(|_| Error::Denied)?;
    let controller = Verifier::new(d.controller_id, d.fence_key, d.emergency_key)
        .map_err(|_| Error::Identity)?;
    let authorization: Authorization =
        serde_json::from_slice(&read(&d.authorization_file, 256 * 1024, false)?)?;
    // Validate all command schemas before opening a remote connection.
    let commands = d
        .commands
        .iter()
        .map(|p| {
            CanonicalCommand::decode(&read(p, 65536, false)?)
                .map_err(|_| Box::<dyn std::error::Error>::from(Error::Encoding))
        })
        .collect::<CliResult<Vec<_>>>()?;
    for command in &commands {
        d.guardian.command(command)?;
    }
    let api = String::from_utf8(read(&d.api_key_file, 256, true)?)?
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let key_bytes = read(&d.signing_key_file, 16384, true)?;
    let key = match d.algorithm {
        Algorithm::Hmac => SigningKey::hmac(&key_bytes),
        Algorithm::RsaPkcs8 => SigningKey::rsa_pkcs8(&key_bytes),
        Algorithm::Ed25519Pkcs8 => SigningKey::ed25519_pkcs8(&key_bytes),
    }
    .map_err(|_| Error::Encoding)?;
    let credentials = Credentials {
        account: d.guardian.scope.account,
        environment: environment(d.guardian.scope.environment)?,
        market: market(&d.guardian.scope),
        api_key: &api,
        signing_key: &key,
    };
    let mut roots = rustls::RootCertStore::empty();
    for path in &d.trust_anchors {
        roots.add(rustls::pki_types::CertificateDer::from(read(
            path,
            1024 * 1024,
            false,
        )?))?;
    }
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let resolve = |s: &str| -> CliResult<SocketAddr> {
        s.to_socket_addrs()?
            .next()
            .ok_or_else(|| "network address has no result".into())
    };
    let network = Network {
        address: resolve(&d.rest_address)?,
        tls: tls.clone(),
        limits: Limits::default(),
    };
    let c = d.calibration;
    let clock = BinanceTimeClient::new(
        TimeConfig {
            source: source(&d.guardian.scope)?,
            domain,
            calibration: CalibrationConfig {
                max_round_trip_ns: c.max_round_trip_ns,
                max_width_ns: c.max_width_ns,
                max_age_ns: c.max_age_ns,
                quantization_error_ns: c.quantization_error_ns,
                drift_ppb: c.drift_ppb,
                max_adjustment_ns: c.max_adjustment_ns,
            },
            unit: TimeUnit::Milliseconds,
            first_sample_serial: 1,
            first_connection_epoch: 1,
            connect_timeout_ns: d.guardian.request_timeout_ns,
            request_timeout_ns: d.guardian.request_timeout_ns,
            max_response_bytes: 65536,
            max_headers: 64,
            io: Limits::default(),
        },
        NetworkConfig {
            address: resolve(&d.time_address)?,
            tls,
        },
        system,
    )?;
    let store = FileStore::open(&d.audit_directory)?;
    let mut owner = if d.recover {
        Guardian::recover(
            store,
            d.guardian,
            controller,
            clock,
            quota.endpoint,
            network,
            None,
        )?
    } else {
        Guardian::create(
            store,
            d.guardian,
            controller,
            clock,
            quota.endpoint,
            network,
        )?
    };
    owner.start_quota_run(
        d.quota.producer_id,
        d.quota.consumer_instance,
        d.quota.consumer_generation,
    )?;
    let stop = Instant::now()
        .checked_add(Duration::from_nanos(d.maximum_runtime_ns))
        .ok_or(Error::Capacity)?;
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(8);
    for command in commands {
        let digest = command.digest();
        if !owner.contains(digest) {
            owner.start_calibration()?;
            owner.register_clock(poll.registry(), mio::Token(0))?;
            while owner.calibration_in_flight() {
                if Instant::now() >= stop {
                    return Err(Error::Clock.into());
                }
                poll.poll(&mut events, Some(Duration::from_millis(1)))?;
                if let Some(outcome) = owner.poll_calibration(IoBudget::default())?.outcome {
                    outcome.calibration?;
                }
            }
            owner.submit(command, authorization.clone(), &credentials)?;
            if owner.busy() {
                owner.register(poll.registry(), mio::Token(1))?;
            }
            while owner.busy() {
                if Instant::now() >= stop {
                    return Err(Error::Transport.into());
                }
                poll.poll(&mut events, Some(Duration::from_millis(1)))?;
                owner.poll(IoBudget::default())?;
            }
        }
        let outcome = owner.outcome(digest);
        println!(
            "{}",
            serde_json::json!({
                "command_sha256":hex(&digest),
                "state":outcome.map_or("unknown",|o|match o.kind{ResultKind::Unknown=>"unknown",ResultKind::NoBytes=>"no_bytes",ResultKind::Response=>"response",ResultKind::Rejected=>"rejected",ResultKind::NotFound=>"not_found",ResultKind::RateLimited=>"rate_limited",ResultKind::Authentication=>"authentication"}),
                "http_status":outcome.map(|o|o.status),"accepted_bytes":outcome.map(|o|o.accepted_bytes),
                "response_bytes":outcome.map(|o|o.raw_response.len()),"response_sha256":outcome.map(|o|hex(&o.raw_digest)),
                "audit_position":owner.audit_position(),"audit_degraded":true,"account_safe":false
            })
        );
    }
    Ok(())
}
fn read(path: &Path, limit: usize, secret: bool) -> CliResult<Vec<u8>> {
    if !path.is_absolute() {
        return Err(Error::Invalid.into());
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > limit as u64 {
        return Err(Error::Capacity.into());
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Denied.into());
        }
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(Error::Capacity.into());
    }
    Ok(bytes)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
