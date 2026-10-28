use crate::{Result, invalid};
use serde::Deserialize;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance::{
    auth::SigningKey,
    request::{Credentials, Market},
};
use tickwire_binance_clock as time;
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_clock::{ClockDomain, Source};
use tickwire_runtime::{files, node};
use tickwire_transport::{Limits, rustls};
use tickwire_venue::{Environment, Product};

pub const PUBLIC_BOOK_LEVELS: usize = 5000;
pub const PUBLIC_DEPTH_UPDATES: usize = 5000;
pub const PUBLIC_BUFFERED_EVENTS: usize = 64;

fn public_limits(io: Limits) -> public::Limits {
    public::Limits {
        io,
        request_bytes: 16 * 1024,
        message_bytes: 1024 * 1024,
        http_bytes: 4 * 1024 * 1024,
        http_headers: 64,
        control_frames: 16,
        timestamp_segments: 1024,
        total_buffer_bytes: 64 * 1024 * 1024,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub baseline: PathBuf,
    pub directories: Directories,
    pub recover: bool,
    pub product: ProductName,
    pub boot_id: [u8; 16],
    pub clock_generation: u64,
    pub owner_id: [u8; 16],
    pub writer_epoch: u64,
    pub initial_writer_epoch: u64,
    pub account_stream: [u8; 16],
    pub input_stream: [u8; 16],
    pub first_connection_epoch: u64,
    #[serde(default)]
    pub trading_websocket: Option<TradingWebSocket>,
    pub instrument_id: u64,
    pub book_source: u32,
    pub credential: CredentialConfig,
    pub network: Network,
    pub clock: ClockConfig,
    pub tuning: Tuning,
    pub policy: tickwire_runtime_policy::bootstrap::Config,
    pub quota: tickwire_binance_quota::bootstrap::Config,
    pub strategy: crate::strategy::Config,
    /// Authenticated direct MD subscriptions. Empty selects the local public client.
    #[serde(default)]
    pub market_sources: Vec<tickwire_market_source::ConnectConfig>,
    #[serde(default)]
    pub replication: Option<crate::replication::Config>,
    #[serde(default)]
    pub lifecycle: Option<crate::lifecycle::Config>,
    #[serde(default)]
    pub shadow_loader: Option<crate::launch::Config>,
    pub reconciliation: crate::reconciliation::Config,
    #[serde(default)]
    pub income_baseline_authority:
        Option<tickwire_binance_account::reconciliation::income_import::IncomeBaselineAuthority>,
    #[serde(default)]
    pub income_association_authority:
        Option<tickwire_binance_account::reconciliation::income_association::AssociationAuthority>,
    #[serde(default)]
    pub control: Option<crate::control::Config>,
    pub stop_file: PathBuf,
    pub maximum_runtime_ns: u64,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingWebSocket {
    pub lifetime_ns: u64,
    pub control_timeout_ns: u64,
    pub max_messages_per_poll: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Directories {
    pub account: PathBuf,
    pub input: PathBuf,
    pub recovery: PathBuf,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductName {
    Spot,
    Usdm,
    UsdmFuture,
}
impl ProductName {
    pub fn market(self) -> Market {
        match self {
            Self::Spot => Market::Spot,
            Self::Usdm | Self::UsdmFuture => Market::Usdm,
        }
    }
    pub fn product(self) -> Product {
        match self {
            Self::Spot => Product::Spot,
            Self::Usdm => Product::LinearPerpetual,
            Self::UsdmFuture => Product::LinearFuture,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialConfig {
    pub api_key_file: PathBuf,
    pub signing_key_file: PathBuf,
    pub algorithm: KeyAlgorithm,
    pub generation: u64,
    pub spot_logon: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyAlgorithm {
    Hmac,
    RsaPkcs8,
    Ed25519Pkcs8,
}
pub struct CredentialMaterial {
    pub api_key: String,
    pub signing_key: SigningKey,
}
impl CredentialConfig {
    pub fn load(&self) -> Result<CredentialMaterial> {
        let raw = read_file(&self.api_key_file, 256)?;
        let api_key = String::from_utf8(raw)?
            .trim_end_matches(['\r', '\n'])
            .to_owned();
        if api_key.is_empty() {
            return Err(invalid("empty API key"));
        }
        let secret = read_file(&self.signing_key_file, 16 * 1024)?;
        let key = match self.algorithm {
            KeyAlgorithm::Hmac => SigningKey::hmac(&secret),
            KeyAlgorithm::RsaPkcs8 => SigningKey::rsa_pkcs8(&secret),
            KeyAlgorithm::Ed25519Pkcs8 => SigningKey::ed25519_pkcs8(&secret),
        }
        .map_err(|_| invalid("invalid signing key"))?;
        Ok(CredentialMaterial {
            api_key,
            signing_key: key,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Network {
    /// DNS name:port or pinned IP:port. Protocol authority/SNI is fixed by product.
    pub rest: String,
    #[serde(default)]
    pub trading_websocket: Option<String>,
    pub time: String,
    pub public_stream: String,
    pub snapshot: String,
    pub private_stream: String,
    pub private_management: Option<String>,
    /// DER trust anchors; hostname validation remains enabled.
    pub trust_anchors: Vec<PathBuf>,
}
impl Network {
    pub fn resolve(&self, connect_ns: u64) -> Result<files::Networks> {
        if self.trust_anchors.is_empty() || self.trust_anchors.len() > 128 {
            return Err(invalid("trust anchor count"));
        }
        let mut roots = rustls::RootCertStore::empty();
        for path in &self.trust_anchors {
            roots.add(rustls::pki_types::CertificateDer::from(read_file(
                path,
                1024 * 1024,
            )?))?;
        }
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        fn resolve(name: &str) -> Result<SocketAddr> {
            name.to_socket_addrs()?
                .next()
                .ok_or_else(|| invalid("address has no DNS results"))
        }
        Ok(files::Networks {
            websocket: self
                .trading_websocket
                .as_ref()
                .map(|name| -> Result<_> {
                    Ok(tickwire_account_gateway::ConnectionConfig {
                        address: resolve(name)?,
                        tls: tls.clone(),
                        limits: Limits::default(),
                        connect_deadline: Instant::now()
                            .checked_add(Duration::from_nanos(connect_ns))
                            .ok_or_else(|| invalid("connect timeout"))?,
                    })
                })
                .transpose()?,
            rest: tickwire_account_gateway::ConnectionConfig {
                address: resolve(&self.rest)?,
                tls: tls.clone(),
                limits: Limits::default(),
                connect_deadline: Instant::now()
                    .checked_add(Duration::from_nanos(connect_ns))
                    .ok_or_else(|| invalid("connect timeout"))?,
            },
            time: time::NetworkConfig {
                address: resolve(&self.time)?,
                tls: tls.clone(),
            },
            public: public::NetworkConfig {
                stream_address: resolve(&self.public_stream)?,
                snapshot_address: resolve(&self.snapshot)?,
                tls: tls.clone(),
            },
            private: private::NetworkConfig {
                stream_address: resolve(&self.private_stream)?,
                management_address: self
                    .private_management
                    .as_deref()
                    .map(resolve)
                    .transpose()?,
                tls,
            },
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockConfig {
    pub max_round_trip_ns: u64,
    pub max_width_ns: u64,
    pub max_age_ns: u64,
    pub quantization_error_ns: u64,
    pub drift_ppb: u64,
    pub max_adjustment_ns: u64,
    pub calibration_interval_ns: u64,
    pub calibration_retry_ns: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tuning {
    pub connect_timeout_ns: u64,
    pub request_timeout_ns: u64,
    pub delivery_budget_ns: u64,
    pub recv_window_micros: u64,
    pub maximum_book_age_ns: u64,
    pub maximum_idle_wait_ns: u64,
    pub snapshot_limit: usize,
    pub maximum_intents: usize,
    pub maximum_input_records: usize,
    pub maximum_input_bytes: usize,
    pub wal_segment_bytes: u64,
}
pub fn read_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    if !path.is_absolute() {
        return Err(invalid("configuration paths must be absolute"));
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(invalid("file type or size"));
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() > maximum {
        return Err(invalid("file exceeds configured bound"));
    }
    Ok(bytes)
}
impl Deployment {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&read_file(path, 4 * 1024 * 1024)?)?)
    }
    pub fn domain(&self) -> Result<ClockDomain> {
        ClockDomain::new(self.boot_id, self.clock_generation).map_err(|_| invalid("clock domain"))
    }
    pub fn baseline(&self) -> Result<CanonicalBaseline> {
        CanonicalBaseline::decode(&read_file(&self.baseline, 16 * 1024 * 1024)?)
            .map_err(|_| invalid("canonical baseline"))
    }
    pub fn source(&self, b: &CanonicalBaseline) -> Source {
        match (self.product, b.snapshot().scope.environment) {
            (ProductName::Spot, Environment::Production) => Source::SpotProduction,
            (ProductName::Spot, Environment::Test) => Source::SpotTest,
            (ProductName::Usdm | ProductName::UsdmFuture, Environment::Production) => {
                Source::UsdmProduction
            }
            (ProductName::Usdm | ProductName::UsdmFuture, Environment::Test) => Source::UsdmTest,
        }
    }
    pub fn credentials<'a>(
        &self,
        b: &CanonicalBaseline,
        key: &'a CredentialMaterial,
    ) -> Credentials<'a> {
        Credentials {
            api_key: &key.api_key,
            signing_key: &key.signing_key,
            market: self.product.market(),
            account: b.snapshot().scope.account,
            environment: b.snapshot().scope.environment,
        }
    }
    pub fn authentication<'a>(&self, key: &'a CredentialMaterial) -> private::Authentication<'a> {
        match self.product {
            ProductName::Usdm | ProductName::UsdmFuture => private::Authentication::UsdmApiKey {
                api_key: &key.api_key,
            },
            ProductName::Spot if self.credential.spot_logon => private::Authentication::SpotLogon {
                api_key: &key.api_key,
                signing_key: &key.signing_key,
            },
            ProductName::Spot => private::Authentication::SpotSignature {
                api_key: &key.api_key,
                signing_key: &key.signing_key,
            },
        }
    }
    fn public_instrument(
        &self,
        baseline: &CanonicalBaseline,
    ) -> Result<(tickwire_venue::InstrumentBinding, tickwire_book::BookKey)> {
        if self.instrument_id == 0 {
            return Err(invalid("instrument id must be nonzero"));
        }
        // CanonicalBaseline fixes the complete catalog order (at most 64 IDs).
        // This slot is local to that owner's catalog, while the binding retains
        // the full external ID for normalized events and strategy inputs.
        let (slot, entry) = baseline
            .catalog()
            .entries
            .iter()
            .enumerate()
            .find(|(_, entry)| {
                entry.instrument.instrument_id == self.instrument_id
                    && entry.product == self.product.product()
            })
            .ok_or_else(|| invalid("instrument not in baseline catalog"))?;
        Ok((
            entry.instrument.clone(),
            tickwire_book::BookKey {
                venue: baseline.snapshot().scope.venue,
                instrument: slot
                    .try_into()
                    .map_err(|_| invalid("local book slot exceeds u32"))?,
                source: self.book_source,
            },
        ))
    }
    pub fn assemble(&self, baseline: CanonicalBaseline) -> Result<files::Configuration> {
        if self.trading_websocket.is_some() != self.network.trading_websocket.is_some() {
            return Err(invalid(
                "WS settings and WS address must be configured together",
            ));
        }
        let scope = baseline.snapshot().scope.clone();
        let (instrument, book_key) = self.public_instrument(&baseline)?;
        let domain = self.domain()?;
        if self
            .policy
            .domain()
            .map_err(|_| invalid("policy clock domain"))?
            != domain
            || self.policy.scope() != scope
            || self.policy.product() != self.product.product()
            || self.policy.writer_epoch != self.writer_epoch
            || self.policy.executor_id != self.owner_id
            || self.account_stream == self.input_stream
        {
            return Err(invalid("deployment identity mismatch"));
        }
        let source = self.source(&baseline);
        let market = self.product.market();
        let private_scope = private::AccountScope {
            risk_domain_id: scope.risk_domain_id,
            venue: scope.venue,
            account: scope.account,
            environment: scope.environment,
            market,
            credential_generation: self.credential.generation,
        };
        let io = Limits::default();
        if self
            .income_association_authority
            .as_ref()
            .is_some_and(|a| a.execution_instance != self.owner_id)
        {
            return Err(invalid("income association executor identity mismatch"));
        }
        let t = &self.tuning;
        let physical = |stream| tickwire_journal::Config {
            domain: scope.risk_domain_id,
            stream_epoch: stream,
            segment_max_bytes: t.wal_segment_bytes,
        };
        let authority = match (source, self.trading_websocket.is_some()) {
            (Source::SpotProduction, true) => "ws-api.binance.com",
            (Source::SpotTest, true) => "ws-api.testnet.binance.vision",
            (Source::UsdmProduction, true) => "ws-fapi.binance.com",
            (Source::UsdmTest, true) => "testnet.binancefuture.com",
            (Source::SpotProduction, _) => "api.binance.com",
            (Source::SpotTest, _) => "testnet.binance.vision",
            (Source::UsdmProduction, _) => "fapi.binance.com",
            (Source::UsdmTest, _) => "demo-fapi.binance.com",
        };
        let mut private = private::SessionConfig::new(private_scope.clone());
        private.timing.connect_ns = t.connect_timeout_ns;
        private.timing.request_ns = t.request_timeout_ns;
        private.timing.recv_window_micros = t.recv_window_micros;
        let public = public::Config {
            owner_id: self.owner_id,
            source,
            domain,
            first_connection_epoch: self.first_connection_epoch,
            first_operation: 1,
            depth: tickwire_binance::depth_sync::Config {
                instrument,
                key: book_key,
                time_unit: tickwire_binance::feed::TimeUnit::Milliseconds,
                snapshot_limit: t.snapshot_limit,
                max_message_bytes: 1024 * 1024,
                initial_book_generation: 1,
                previous_connection_epoch: self
                    .first_connection_epoch
                    .checked_sub(1)
                    .ok_or_else(|| invalid("connection epoch"))?,
            },
            subscribe: public::SubscribeMode::RawUrl,
            speed: public::DepthSpeed::Ms100,
            timing: public::Timing {
                connect_ns: t.connect_timeout_ns,
                request_ns: t.request_timeout_ns,
                first_event_ns: 30_000_000_000,
                heartbeat_ns: 600_000_000_000,
                pong_ns: 5_000_000_000,
                rotate_ns: 82_800_000_000_000,
                backoff_ns: 100_000_000,
                max_backoff_ns: 30_000_000_000,
            },
            limits: public_limits(io),
        };
        let c = &self.clock;
        Ok(files::Configuration {
            websocket: self
                .trading_websocket
                .map(|w| tickwire_binance_account::ws::WsConfig {
                    lifetime_ns: w.lifetime_ns,
                    control_timeout_ns: w.control_timeout_ns,
                    max_messages_per_poll: w.max_messages_per_poll,
                }),
            directories: files::Directories {
                account: self.directories.account.clone(),
                input: self.directories.input.clone(),
                recovery: self.directories.recovery.clone(),
            },
            mode: if self.recover {
                files::OpenMode::RecoverFrozen {
                    account_required: None,
                    input_required: None,
                }
            } else {
                files::OpenMode::Create
            },
            account: tickwire_account_gateway::Config {
                journal: tickwire_account_journal::Config {
                    physical: physical(self.account_stream),
                    baseline_digest: baseline.digest(),
                    initial_writer_epoch: self.initial_writer_epoch,
                    scope: scope.clone(),
                },
                writer_epoch: self.writer_epoch,
                route: tickwire_account_gateway::Route {
                    venue: scope.venue,
                    account: scope.account,
                    environment: scope.environment,
                    product: self.product.product(),
                    authority: authority.to_owned(),
                },
                max_intents: t.maximum_intents,
            },
            input: tickwire_binance_account::Config {
                income_baseline_authority: self.income_baseline_authority.clone(),
                income_association_authority: self.income_association_authority.clone(),
                physical: physical(self.input_stream),
                writer_epoch: self.writer_epoch,
                baseline,
                account_stream: self.account_stream,
                max_records: t.maximum_input_records,
                max_registrations: t.maximum_intents,
                max_raw_bytes: 32 * 1024,
                max_retained_bytes: t.maximum_input_bytes,
            },
            rest: tickwire_binance_account::rest::RestConfig {
                connection_epoch: self.first_connection_epoch,
                max_response_bytes: 32 * 1024,
                max_headers: 64,
            },
            time: time::Config {
                source,
                domain,
                calibration: time::CalibrationConfig {
                    max_round_trip_ns: c.max_round_trip_ns,
                    max_width_ns: c.max_width_ns,
                    max_age_ns: c.max_age_ns,
                    quantization_error_ns: c.quantization_error_ns,
                    drift_ppb: c.drift_ppb,
                    max_adjustment_ns: c.max_adjustment_ns,
                },
                unit: time::TimeUnit::Milliseconds,
                first_sample_serial: 1,
                first_connection_epoch: self.first_connection_epoch,
                connect_timeout_ns: t.connect_timeout_ns,
                request_timeout_ns: t.request_timeout_ns,
                max_response_bytes: 16 * 1024,
                max_headers: 64,
                io,
            },
            market: public,
            private,
            private_first_connection_epoch: self.first_connection_epoch,
            command: tickwire_binance_execution::Config {
                recv_window_micros: t.recv_window_micros,
                max_http_bytes: 16 * 1024,
                max_signed_commands: t.maximum_intents,
            },
            command_binding: tickwire_binance_quota::CommandBinding {
                owner_id: self.owner_id,
                risk_domain_id: scope.risk_domain_id,
                account: scope.account,
                product: self.product.product(),
                environment: scope.environment,
                domain,
            },
            command_delivery_budget_ns: t.delivery_budget_ns,
            runtime: node::Config {
                calibration_interval_ns: c.calibration_interval_ns,
                calibration_retry_ns: c.calibration_retry_ns,
                maximum_book_age_ns: t.maximum_book_age_ns,
                private_delivery_budget_ns: t.delivery_budget_ns,
                io: tickwire_transport::IoBudget {
                    max_syscalls: 64,
                    max_bytes: 256 * 1024,
                },
                public: public::PollBudget::default(),
                private: private::PollBudget::default(),
                maximum_commands_per_turn: 1,
                maximum_idle_wait_ns: t.maximum_idle_wait_ns,
            },
            bindings: node::Bindings {
                time: tickwire_binance_quota::TimeBinding {
                    owner_id: self.owner_id,
                    source,
                    domain,
                },
                private: tickwire_binance_quota::PrivateBinding {
                    owner_id: self.owner_id,
                    scope: private_scope,
                    domain,
                },
                public: tickwire_binance_quota::PublicBinding {
                    owner_id: self.owner_id,
                    source,
                    domain,
                },
            },
            maximum_staged_commands: 32,
            maximum_staged_bytes: 128 * 1024 * 1024,
        })
    }
}

#[cfg(test)]
#[path = "../../tickwire-binance/tests/common/mod.rs"]
mod venue_fixture;

#[cfg(test)]
#[path = "config_instrument_tests.rs"]
mod instrument_tests;

#[cfg(test)]
mod tests {
    use super::*;

    struct UnusedClock;
    impl time::MonotonicClock for UnusedClock {
        fn sample(&mut self) -> time::Result<time::LocalTime> {
            panic!("constructing the public owner must not sample or perform I/O");
        }
    }

    fn construct<P: public::DepthPolicy>(source: Source, snapshot_limit: usize) {
        let mut instrument = venue_fixture::command().context.instrument;
        if P::MARKET == Market::Usdm {
            instrument.contract = tickwire_venue::ContractKind::Perpetual;
        }
        let config = public::Config {
            owner_id: [1; 16],
            source,
            domain: ClockDomain::new([2; 16], 1).unwrap(),
            first_connection_epoch: 1,
            first_operation: 1,
            depth: public::DepthConfig {
                instrument,
                key: tickwire_book::BookKey {
                    venue: 1,
                    instrument: 12,
                    source: 1,
                },
                time_unit: public::TimeUnit::Milliseconds,
                snapshot_limit,
                max_message_bytes: 1024 * 1024,
                initial_book_generation: 1,
                previous_connection_epoch: 0,
            },
            subscribe: public::SubscribeMode::RawUrl,
            speed: public::DepthSpeed::Ms100,
            timing: public::Timing {
                connect_ns: 1_000_000_000,
                request_ns: 1_000_000_000,
                first_event_ns: 30_000_000_000,
                heartbeat_ns: 600_000_000_000,
                pong_ns: 5_000_000_000,
                rotate_ns: 82_800_000_000_000,
                backoff_ns: 100_000_000,
                max_backoff_ns: 30_000_000_000,
            },
            limits: public_limits(Limits::default()),
        };
        let network = || public::NetworkConfig {
            stream_address: "127.0.0.1:1".parse().unwrap(),
            snapshot_address: "127.0.0.1:2".parse().unwrap(),
            tls: Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth(),
            ),
        };
        type Client<P> = public::PublicDepthClient<
            P,
            UnusedClock,
            PUBLIC_BOOK_LEVELS,
            PUBLIC_DEPTH_UPDATES,
            PUBLIC_BUFFERED_EVENTS,
        >;
        let mut undersized = config.clone();
        undersized.limits.total_buffer_bytes = 16 * 1024 * 1024;
        assert!(matches!(
            Client::<P>::new(undersized, network(), UnusedClock),
            Err(public::Error::Bounds)
        ));
        assert!(Client::<P>::new(config, network(), UnusedClock).is_ok());
    }

    #[test]
    fn public_owner_constructs_at_both_products_maximum_snapshot_depth() {
        // The production owner is large; avoid the test harness's 2 MiB stack.
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                construct::<tickwire_book::BinanceSpot>(Source::SpotTest, 5000);
                construct::<tickwire_book::BinanceUsdM>(Source::UsdmTest, 1000);
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
