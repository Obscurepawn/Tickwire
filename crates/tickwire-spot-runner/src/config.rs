//! Static JSON configuration schema for the stateless Unified Trading Account ORS runner
//! (`Bybit V5` and `OKX V5`).
//!
//! Processes load immutable static configuration once at cold startup—zero dynamic configuration patch
//! files, zero write-ahead logs, and zero local disk recovery state.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tickwire_account::{OrderManagerConfig, OrsSymbolSpec};
use tickwire_account_gateway::{AccountManager, AccountManagerConfig};
use tickwire_control_sys::CpuSet;
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    DEFAULT_MAX_ORDER_AGE_NS, OrsCpuConfig, OrsProcessConfig, OrsRuntimeConfig,
    SessionRestEndpoint, build_rms_engine_from_file,
};
use tickwire_spot_execution::network::OwnedCredentials;
use tickwire_spot_execution::rest_bootstrap::{UtaAssetBinding, UtaSymbolBinding};
use tickwire_spot_execution::rfq::{DEFAULT_MAX_OPEN_RFQ_QUOTES, UtaRfqQuoteBook};
use tickwire_spot_execution::{Environment, SpotVenue, UtaTransportMode};
use tickwire_transport::rest_querier::DialSecurity;
use tickwire_transport::rustls;
use tickwire_venue::{AssetKey, Product};

use crate::session::{UtaEndpointSpec, UtaVenueSession, UtaVenueSessionConfig};

fn default_max_order_age_ns() -> u64 {
    DEFAULT_MAX_ORDER_AGE_NS
}

fn default_exec_generation() -> u64 {
    1
}

fn default_instrument_version() -> u64 {
    1
}

fn default_ws_ping_interval_ms() -> u64 {
    15_000
}

fn default_status_query_delay_ms() -> u64 {
    1_000
}

fn default_ip_order_weight() -> u32 {
    1
}

fn default_slots_per_strat() -> usize {
    1_024
}

fn default_initial_margin_bps() -> u32 {
    10_000
}

fn default_contract_size_atoms() -> i128 {
    1
}

fn default_true() -> bool {
    true
}

/// Serde-friendly UTA venue selector (`"bybit"` or `"okx"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UtaVenueKind {
    Bybit,
    Okx,
}

impl UtaVenueKind {
    #[inline]
    pub const fn to_spot_venue(self) -> SpotVenue {
        match self {
            Self::Bybit => SpotVenue::Bybit,
            Self::Okx => SpotVenue::Okx,
        }
    }
}

/// Serde-friendly exchange environment (`"production"` or `"test"`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UtaEnvironmentKind {
    Production,
    #[default]
    Test,
}

impl UtaEnvironmentKind {
    #[inline]
    pub const fn to_environment(self) -> Environment {
        match self {
            Self::Production => Environment::Production,
            Self::Test => Environment::Test,
        }
    }
}

/// Serde-friendly outbound order transport mode (`"ws"` or `"https"`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UtaTransportKind {
    #[default]
    Ws,
    Https,
}

impl UtaTransportKind {
    #[inline]
    pub const fn to_transport_mode(self) -> UtaTransportMode {
        match self {
            Self::Ws => UtaTransportMode::WebSocketTrade,
            Self::Https => UtaTransportMode::PooledHttps,
        }
    }
}

/// Serde-friendly canonical product selector across all 6 UTA product classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UtaProductKind {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    LinearFuture,
    InverseFuture,
    Option,
}

impl UtaProductKind {
    #[inline]
    pub const fn to_product(self) -> Product {
        match self {
            Self::Spot => Product::Spot,
            Self::LinearPerpetual => Product::LinearPerpetual,
            Self::InversePerpetual => Product::InversePerpetual,
            Self::LinearFuture => Product::LinearFuture,
            Self::InverseFuture => Product::InverseFuture,
            Self::Option => Product::Option,
        }
    }
}

/// CPU affinity and epoll wait configuration in static JSON.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UtaCpuSpec {
    #[serde(default)]
    pub req_cpu: Option<u32>,
    #[serde(default)]
    pub resp_cpu: Option<u32>,
    #[serde(default)]
    pub housekeeping_cpus: Vec<u32>,
    #[serde(default)]
    pub epoll_wait_ms: i32,
}

impl UtaCpuSpec {
    pub fn to_ors_cpu_config(&self) -> Result<OrsCpuConfig, String> {
        let housekeeping_cpus = if self.housekeeping_cpus.is_empty() {
            None
        } else {
            Some(
                CpuSet::from_cpus(self.housekeeping_cpus.iter().copied())
                    .map_err(|e| format!("invalid housekeeping_cpus: {e}"))?,
            )
        };
        let cfg = OrsCpuConfig {
            req_cpu: self.req_cpu,
            resp_cpu: self.resp_cpu,
            housekeeping_cpus,
            epoll_wait_ms: self.epoll_wait_ms,
        };
        cfg.validate()
            .map_err(|e| format!("OrsCpuConfig validation failed: {e}"))?;
        Ok(cfg)
    }
}

/// Static network endpoint descriptor in JSON configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UtaEndpointConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub path: String,
    #[serde(default = "default_true")]
    pub tls: bool,
    #[serde(default)]
    pub ca_pem_path: Option<PathBuf>,
    #[serde(default)]
    pub initial_peers: Vec<SocketAddr>,
}

impl UtaEndpointConfig {
    pub fn build_dial_security(&self) -> Result<DialSecurity, String> {
        if !self.tls {
            return Ok(DialSecurity::Plain);
        }
        let server_name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|e| format!("invalid TLS ServerName '{}': {e}", self.host))?;
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca_path) = &self.ca_pem_path {
            let pem = std::fs::read(ca_path)
                .map_err(|e| format!("failed to read CA certificate {}: {e}", ca_path.display()))?;
            let certs = collect_pem_certs(&pem)?;
            for cert in certs {
                roots
                    .add(cert)
                    .map_err(|e| format!("RootCertStore::add failed: {e}"))?;
            }
        }
        let client_cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(DialSecurity::Tls {
            server_name,
            config: Arc::new(client_cfg),
        })
    }

    pub fn to_endpoint_spec(&self, default_path: &str) -> Result<UtaEndpointSpec, String> {
        let security = self.build_dial_security()?;
        let path = if self.path.is_empty() {
            default_path.to_owned()
        } else {
            self.path.clone()
        };
        Ok(UtaEndpointSpec {
            host: self.host.clone(),
            port: self.port,
            path,
            security,
            initial_peers: self.initial_peers.clone(),
        })
    }

    pub fn to_rest_endpoint(&self) -> Result<SessionRestEndpoint, String> {
        let security = self.build_dial_security()?;
        Ok(SessionRestEndpoint {
            url_id: None,
            host: self.host.clone(),
            port: self.port,
            security,
            initial_peers: self.initial_peers.clone(),
        })
    }
}

/// Static asset binding entry in JSON configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UtaAssetConfig {
    pub currency: String,
    #[serde(default)]
    pub wallet_id: u16,
    pub asset_id: u64,
    pub scale: u8,
}

impl UtaAssetConfig {
    pub fn to_binding(&self) -> Result<UtaAssetBinding, String> {
        UtaAssetBinding::new(
            &self.currency,
            AssetKey {
                wallet_id: self.wallet_id,
                asset_id: self.asset_id,
            },
            self.scale,
        )
        .map_err(|e| format!("invalid UtaAssetConfig '{}': {e}", self.currency))
    }
}

/// Static symbol binding entry in JSON configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UtaSymbolConfig {
    pub symbol_id: u32,
    #[serde(default)]
    pub instrument_id: Option<u64>,
    pub symbol: String,
    pub product: UtaProductKind,
    #[serde(default)]
    pub wallet_id: u16,
    pub base_asset_id: u64,
    pub base_scale: u8,
    pub quote_asset_id: u64,
    pub quote_scale: u8,
    pub settlement_asset_id: u64,
    pub settlement_scale: u8,
    pub price_tick_atoms: i128,
    pub price_scale: u8,
    pub qty_step_atoms: i128,
    pub qty_scale: u8,
    #[serde(default = "default_contract_size_atoms")]
    pub contract_size_atoms: i128,
    #[serde(default)]
    pub contract_scale: u8,
    #[serde(default = "default_initial_margin_bps")]
    pub initial_margin_bps: u32,
}

impl UtaSymbolConfig {
    pub fn to_binding(&self) -> Result<UtaSymbolBinding, String> {
        let spec = OrsSymbolSpec {
            symbol_id: self.symbol_id,
            instrument_id: self.instrument_id.unwrap_or(u64::from(self.symbol_id)),
            product: self.product.to_product(),
            wallet_id: self.wallet_id,
            base_asset_id: self.base_asset_id,
            base_scale: self.base_scale,
            quote_asset_id: self.quote_asset_id,
            quote_scale: self.quote_scale,
            settlement_asset_id: self.settlement_asset_id,
            settlement_scale: self.settlement_scale,
            price_tick_atoms: self.price_tick_atoms,
            price_scale: self.price_scale,
            qty_step_atoms: self.qty_step_atoms,
            qty_scale: self.qty_scale,
            contract_size_atoms: self.contract_size_atoms,
            contract_scale: self.contract_scale,
            initial_margin_bps: self.initial_margin_bps,
        };
        UtaSymbolBinding::new(self.symbol_id, &self.symbol, spec)
            .map_err(|e| format!("invalid UtaSymbolConfig '{}': {e}", self.symbol))
    }
}

/// Complete immutable startup configuration for a stateless UTA ORS runner process.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotRunnerConfig {
    pub venue: UtaVenueKind,
    #[serde(default)]
    pub environment: UtaEnvironmentKind,
    #[serde(default)]
    pub transport: UtaTransportKind,
    #[serde(default)]
    pub cpu: UtaCpuSpec,
    #[serde(default = "default_max_order_age_ns")]
    pub max_order_age_ns: u64,
    #[serde(default = "default_true")]
    pub rms_on_dry_run: bool,
    #[serde(default = "default_exec_generation")]
    pub exec_generation: u64,
    #[serde(default = "default_instrument_version")]
    pub instrument_version: u64,
    #[serde(default = "default_ws_ping_interval_ms")]
    pub ws_ping_interval_ms: u64,
    #[serde(default = "default_status_query_delay_ms")]
    pub status_query_delay_ms: u64,
    #[serde(default = "default_ip_order_weight")]
    pub ip_order_weight: u32,
    #[serde(default = "default_slots_per_strat")]
    pub slots_per_strat: usize,
    #[serde(default)]
    pub enable_rfq_quotes: bool,
    pub allowed_strategies: Vec<u8>,
    pub account_manager: AccountManagerConfig,
    pub symbols: Vec<UtaSymbolConfig>,
    pub assets: Vec<UtaAssetConfig>,
    #[serde(default)]
    pub order_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub stream_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub rest_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub rms_config_path: Option<PathBuf>,
    #[serde(default)]
    pub control_socket_path: Option<PathBuf>,
}

/// Validated, pre-built artifacts ready to construct `OrderRoutingSystem<UtaVenueSession>` or
/// `OrsProcess<UtaVenueSession>`.
pub struct PreparedSpotRunner {
    pub runtime_config: OrsRuntimeConfig,
    pub account_manager: AccountManager,
    pub allowed_strategies: Vec<u8>,
    pub order_manager_config: OrderManagerConfig,
    pub rms_engine: RmsEngine,
    pub sessions: Vec<(u8, Arc<UtaVenueSession>)>,
    pub process_config: OrsProcessConfig,
}

impl SpotRunnerConfig {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config {}: {e}", path.display()))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("invalid SpotRunnerConfig JSON {}: {e}", path.display()))
    }

    pub fn prepare(&self) -> Result<PreparedSpotRunner, String> {
        if self.allowed_strategies.is_empty() {
            return Err(String::from("allowed_strategies must not be empty"));
        }
        if self.symbols.is_empty() {
            return Err(String::from("symbols must not be empty"));
        }
        let cpu = self.cpu.to_ors_cpu_config()?;
        let account_manager = AccountManager::from_config(&self.account_manager)
            .map_err(|e| format!("AccountManager validation failed: {e}"))?;

        let spot_venue = self.venue.to_spot_venue();
        let env = self.environment.to_environment();
        let transport_mode = self.transport.to_transport_mode();

        let symbol_bindings: Vec<UtaSymbolBinding> = self
            .symbols
            .iter()
            .map(UtaSymbolConfig::to_binding)
            .collect::<Result<_, _>>()?;
        let asset_bindings: Vec<UtaAssetBinding> = self
            .assets
            .iter()
            .map(UtaAssetConfig::to_binding)
            .collect::<Result<_, _>>()?;

        let max_sym_id = symbol_bindings
            .iter()
            .map(|s| usize::from(u16::try_from(s.symbol_id).unwrap_or(u16::MAX)))
            .max()
            .unwrap_or(0);
        let symbol_slots = max_sym_id.saturating_add(1);

        let rms_engine = if let Some(rms_path) = &self.rms_config_path {
            let (engine, _) = build_rms_engine_from_file(symbol_slots, rms_path)
                .map_err(|e| format!("RMS config load failed: {e}"))?;
            engine
        } else {
            let engine = RmsEngine::new(symbol_slots);
            let mut default_table = String::new();
            for b in &symbol_bindings {
                default_table.push_str(&format!(
                    "symbol_id={} enabled=true max_qty=1000000000 max_notional=1000000000000000 new_count=10000/50000/100000 order_qty=1000000000/5000000000/10000000000\n",
                    b.symbol_id
                ));
            }
            engine
                .reload_handle()
                .reload_limits(&default_table)
                .map_err(|e| format!("default RmsEngine setup failed: {e}"))?;
            engine
        };

        let default_order_path = match (spot_venue, transport_mode) {
            (SpotVenue::Bybit, UtaTransportMode::WebSocketTrade) => "/v5/trade",
            (SpotVenue::Bybit, UtaTransportMode::PooledHttps) => "/v5/order/create",
            (SpotVenue::Okx, UtaTransportMode::WebSocketTrade) => "/ws/v5/private",
            (SpotVenue::Okx, UtaTransportMode::PooledHttps) => "/api/v5/trade/order",
        };
        let default_stream_path = match spot_venue {
            SpotVenue::Bybit => "/v5/private",
            SpotVenue::Okx => "/ws/v5/private",
        };

        let order_ep = match &self.order_endpoint {
            Some(ep) => ep.to_endpoint_spec(default_order_path)?,
            None => UtaEndpointSpec::manual(default_order_path),
        };
        let stream_ep = match &self.stream_endpoint {
            Some(ep) => Some(ep.to_endpoint_spec(default_stream_path)?),
            None => None,
        };
        let rest_ep = match &self.rest_endpoint {
            Some(ep) => Some(ep.to_rest_endpoint()?),
            None => None,
        };

        let mut account_domains = Vec::with_capacity(account_manager.specs().len());
        let mut sessions = Vec::with_capacity(account_manager.specs().len());
        for spec in account_manager.specs() {
            account_domains.push(spec.to_domain_config(spot_venue.venue_id(), env));
            let creds_cfg = spec
                .credentials
                .as_ref()
                .ok_or_else(|| format!("account '{}' is missing credentials", spec.name))?;
            let owned_creds = match spot_venue {
                SpotVenue::Bybit => OwnedCredentials::bybit(creds_cfg.key(), creds_cfg.secret()),
                SpotVenue::Okx => OwnedCredentials::okx(
                    creds_cfg.key(),
                    creds_cfg.secret(),
                    creds_cfg.passphrase(),
                ),
            }
            .map_err(|e| format!("invalid credentials for '{}': {e}", spec.name))?;
            let rfq_hook: Option<Box<dyn tickwire_spot_execution::RfqQuoteHook>> =
                if self.enable_rfq_quotes {
                    Some(Box::new(UtaRfqQuoteBook::new(DEFAULT_MAX_OPEN_RFQ_QUOTES)))
                } else {
                    None
                };
            let session = UtaVenueSession::new(UtaVenueSessionConfig {
                venue: spot_venue,
                environment: env,
                exchange_uid: spec.exchange_account_id,
                risk_domain_id: spec.risk_domain_id,
                credentials: owned_creds,
                transport_mode,
                order_endpoint: order_ep.clone(),
                stream_endpoint: stream_ep.clone(),
                rest_endpoint: rest_ep.clone(),
                symbols: symbol_bindings.clone(),
                assets: asset_bindings.clone(),
                ip_order_weight: self.ip_order_weight,
                status_query_delay_ms: self.status_query_delay_ms,
                okx_ws_auto_subscribe_private: true,
                rfq_hook,
                clock_utc_ms: None,
                bootstrap_override: None,
            })?;
            sessions.push((spec.account_ref, Arc::new(session)));
        }

        let first_dom = account_domains
            .first()
            .copied()
            .ok_or_else(|| String::from("at least one enabled account is required"))?;
        let mut order_manager_config = OrderManagerConfig::single_account_default(
            first_dom,
            symbol_bindings.iter().map(|b| b.spec).collect(),
        );
        order_manager_config.accounts = account_domains;
        order_manager_config.slots_per_strat = self.slots_per_strat;

        let runtime_config = OrsRuntimeConfig {
            cpu,
            max_order_age_ns: self.max_order_age_ns,
            rms_on_dry_run: self.rms_on_dry_run,
            exec_generation: self.exec_generation,
            instrument_version: self.instrument_version,
            ws_ping_interval_ms: self.ws_ping_interval_ms,
            ..OrsRuntimeConfig::default()
        };

        let process_config = OrsProcessConfig {
            control_socket_path: self.control_socket_path.clone(),
            rms_config_path: self.rms_config_path.clone(),
            ..OrsProcessConfig::default()
        };

        Ok(PreparedSpotRunner {
            runtime_config,
            account_manager,
            allowed_strategies: self.allowed_strategies.clone(),
            order_manager_config,
            rms_engine,
            sessions,
            process_config,
        })
    }
}

/// Parses PEM-encoded X.509 `CERTIFICATE` blocks into DER [`rustls::pki_types::CertificateDer`]
/// without extra PEM crate dependencies.
pub fn collect_pem_certs(
    pem: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    let text = std::str::from_utf8(pem).map_err(|e| format!("CA PEM is not UTF-8: {e}"))?;
    let mut certs = Vec::new();
    let mut in_cert = false;
    let mut b64_acc = String::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "-----BEGIN CERTIFICATE-----" {
            in_cert = true;
            b64_acc.clear();
        } else if trimmed == "-----END CERTIFICATE-----" {
            if in_cert && !b64_acc.is_empty() {
                let der = STANDARD
                    .decode(&b64_acc)
                    .map_err(|e| format!("invalid base64 in PEM certificate: {e}"))?;
                certs.push(rustls::pki_types::CertificateDer::from(der));
            }
            in_cert = false;
            b64_acc.clear();
        } else if in_cert {
            b64_acc.push_str(trimmed);
        }
    }

    if certs.is_empty() {
        return Err(String::from("no CERTIFICATE block found in PEM"));
    }
    Ok(certs)
}
