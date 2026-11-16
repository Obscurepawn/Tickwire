//! Immutable static JSON configuration schema (`OrsAppConfig`) for the unified stateless ORS
//! execution process (`tickwire-app`, `1 ORS process = 1 Venue`).
//!
//! Supports `Binance` (`Spot`, `Usdm`, `Coinm`, `Options`), `Bybit` (`UTA`), and `Okx` (`UTA`)
//! with optional static `InstrumentCatalog` digest/binding verification, static RMS configuration,
//! DOP `ControlServer` socket configuration, and external Unix `SOCK_SEQPACKET` strategy lane
//! descriptors—with zero local WAL, journal, lease, or dynamic configuration patch state.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_account::{OrderManagerConfig, OrsSymbolSpec};
use tickwire_account_gateway::{AccountManager, AccountManagerConfig};
use tickwire_binance::auth::SigningKey;
use tickwire_binance::full_market::BinanceVenueMarket;
use tickwire_binance_account::{
    BinanceAssetBinding, BinanceBootstrapCatalog, BinanceSymbolBinding,
    DEFAULT_ORDER_RECV_WINDOW_MS, DEFAULT_STATUS_QUERY_DELAY_MS,
};
use tickwire_instrument_catalog::Catalog;
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    ControlServerConfig, DEFAULT_MAX_ORDER_AGE_NS, OrsProcessConfig, OrsRuntimeConfig,
    build_rms_engine_from_file,
};
use tickwire_spot_execution::SpotVenue;
use tickwire_spot_execution::network::OwnedCredentials;
use tickwire_spot_execution::rest_bootstrap::{UtaAssetBinding, UtaSymbolBinding};
use tickwire_spot_execution::rfq::{DEFAULT_MAX_OPEN_RFQ_QUOTES, UtaRfqQuoteBook};
use tickwire_spot_runner::{
    UtaAssetConfig, UtaCpuSpec, UtaEndpointConfig, UtaEndpointSpec, UtaEnvironmentKind,
    UtaSymbolConfig, UtaTransportKind, UtaVenueSession, UtaVenueSessionConfig,
};
use tickwire_venue::{AssetKey, Product, venue_id};

use crate::binance_session::{
    BinanceMarketEndpoint, BinanceVenueSession, BinanceVenueSessionConfig,
};
use crate::external_lanes::ExternalStrategyLaneConfig;
use crate::venue::UnifiedVenueSession;

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

fn default_slots_per_strat() -> usize {
    1_024
}

fn default_true() -> bool {
    true
}

fn default_contract_size_atoms() -> i128 {
    1
}

fn default_initial_margin_bps() -> u32 {
    10_000
}

fn default_order_recv_window_ms() -> u32 {
    DEFAULT_ORDER_RECV_WINDOW_MS
}

fn default_binance_status_delay_ms() -> u64 {
    DEFAULT_STATUS_QUERY_DELAY_MS
}

/// Venue selector for `tickwire-app` (`1 ORS process = 1 Venue`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrsVenueKind {
    Binance,
    Bybit,
    Okx,
}

impl OrsVenueKind {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
        }
    }

    #[inline]
    pub const fn numeric_venue_id(self) -> u32 {
        match self {
            Self::Binance => venue_id::BINANCE,
            Self::Bybit => SpotVenue::Bybit.venue_id(),
            Self::Okx => SpotVenue::Okx.venue_id(),
        }
    }
}

/// Serde-friendly Binance trading product family (`"spot"`, `"usdm"`, `"coinm"`, `"options"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BinanceMarketKind {
    Spot,
    Usdm,
    Coinm,
    Options,
}

impl BinanceMarketKind {
    #[inline]
    pub const fn to_market(self) -> BinanceVenueMarket {
        match self {
            Self::Spot => BinanceVenueMarket::Spot,
            Self::Usdm => BinanceVenueMarket::Usdm,
            Self::Coinm => BinanceVenueMarket::Coinm,
            Self::Options => BinanceVenueMarket::Options,
        }
    }

    #[inline]
    pub const fn default_product(self) -> Product {
        match self {
            Self::Spot => Product::Spot,
            Self::Usdm => Product::UsdM,
            Self::Coinm => Product::CoinM,
            Self::Options => Product::Option,
        }
    }
}

/// Per-market endpoint and optional Ed25519 key configuration for Binance accounts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceMarketConfigBlock {
    pub market: BinanceMarketKind,
    #[serde(default)]
    pub ed25519_pem_path: Option<PathBuf>,
    #[serde(default)]
    pub order_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub stream_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub rest_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub stream_colo: bool,
}

/// Static Binance asset binding entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceAssetConfig {
    pub market: BinanceMarketKind,
    pub asset_code: String,
    #[serde(default)]
    pub wallet_id: u16,
    pub asset_id: u64,
    pub scale: u8,
}

impl BinanceAssetConfig {
    pub fn to_binding(&self) -> BinanceAssetBinding {
        BinanceAssetBinding {
            market: self.market.to_market(),
            asset_code: self.asset_code.clone(),
            key: AssetKey {
                wallet_id: self.wallet_id,
                asset_id: self.asset_id,
            },
            scale: self.scale,
        }
    }
}

/// Static Binance symbol binding entry (`symbol_id` must form a dense `0..N` sequence).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceSymbolConfig {
    pub symbol_id: u32,
    #[serde(default)]
    pub instrument_id: Option<u64>,
    pub market: BinanceMarketKind,
    pub venue_symbol: String,
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
    #[serde(default)]
    pub default_fee_asset_code: Option<String>,
}

impl BinanceSymbolConfig {
    pub fn to_binding(&self) -> Result<BinanceSymbolBinding, String> {
        let spec = OrsSymbolSpec {
            symbol_id: self.symbol_id,
            instrument_id: self.instrument_id.unwrap_or(u64::from(self.symbol_id)),
            product: self.market.default_product(),
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
        spec.validate()
            .map_err(|r| format!("invalid OrsSymbolSpec for '{}': {r:?}", self.venue_symbol))?;
        Ok(BinanceSymbolBinding {
            symbol_id: self.symbol_id,
            market: self.market.to_market(),
            venue_symbol: self.venue_symbol.clone(),
            spec,
            default_fee_asset_code: self.default_fee_asset_code.clone(),
        })
    }
}

/// Optional static `InstrumentCatalog` JSON verification block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogVerificationConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub expected_catalog_version: Option<u64>,
    #[serde(default)]
    pub expected_sha256_hex: Option<String>,
}

/// Complete immutable startup configuration for `tickwire-app` (`1 ORS process = 1 Venue`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrsAppConfig {
    pub venue: OrsVenueKind,
    #[serde(default)]
    pub environment: UtaEnvironmentKind,
    #[serde(default)]
    pub uta_transport: UtaTransportKind,
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
    #[serde(default = "default_slots_per_strat")]
    pub slots_per_strat: usize,
    #[serde(default)]
    pub ors_instance: Option<[u8; 16]>,
    pub allowed_strategies: Vec<u8>,
    pub account_manager: AccountManagerConfig,

    // Binance-specific blocks (when `venue == "binance"`):
    #[serde(default)]
    pub binance_markets: Vec<BinanceMarketConfigBlock>,
    #[serde(default)]
    pub binance_symbols: Vec<BinanceSymbolConfig>,
    #[serde(default)]
    pub binance_assets: Vec<BinanceAssetConfig>,
    #[serde(default = "default_order_recv_window_ms")]
    pub binance_order_recv_window_ms: u32,
    #[serde(default = "default_binance_status_delay_ms")]
    pub binance_status_query_delay_ms: u64,

    // Bybit / OKX UTA-specific blocks (when `venue == "bybit" | "okx"`):
    #[serde(default)]
    pub uta_symbols: Vec<UtaSymbolConfig>,
    #[serde(default)]
    pub uta_assets: Vec<UtaAssetConfig>,
    #[serde(default)]
    pub uta_order_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub uta_stream_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub uta_rest_endpoint: Option<UtaEndpointConfig>,
    #[serde(default)]
    pub uta_enable_rfq_quotes: bool,

    // Optional Cold-Path Catalog, RMS, DOP Control Socket, Strategy Lanes, and Summary Output:
    #[serde(default)]
    pub instrument_catalog: Option<CatalogVerificationConfig>,
    #[serde(default)]
    pub rms_config_path: Option<PathBuf>,
    #[serde(default)]
    pub control_socket_path: Option<PathBuf>,
    #[serde(default)]
    pub dop_hb_interval_ms: Option<u32>,
    #[serde(default)]
    pub external_strategy_lanes: Vec<ExternalStrategyLaneConfig>,
    #[serde(default)]
    pub summary_output_path: Option<PathBuf>,
}

/// Prepared cold-startup components ready to spawn `OrsProcess<UnifiedVenueSession>`.
pub struct PreparedOrsApp {
    pub venue: OrsVenueKind,
    pub runtime_config: OrsRuntimeConfig,
    pub account_manager: AccountManager,
    pub allowed_strategies: Vec<u8>,
    pub order_manager_config: OrderManagerConfig,
    pub rms_engine: RmsEngine,
    pub sessions: Vec<(u8, Arc<UnifiedVenueSession>)>,
    pub process_config: OrsProcessConfig,
    pub external_lanes: Vec<ExternalStrategyLaneConfig>,
    pub catalog_version: Option<u64>,
    pub summary_output_path: Option<PathBuf>,
}

type PreparedSessions = (Vec<OrsSymbolSpec>, Vec<(u8, Arc<UnifiedVenueSession>)>);

fn parse_sha256_hex(hex_str: &str) -> Result<[u8; 32], String> {
    if hex_str.len() != 64 {
        return Err(format!(
            "expected 64-character SHA-256 hex digest, got {} characters",
            hex_str.len()
        ));
    }
    let mut out = [0u8; 32];
    for (idx, chunk) in hex_str.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let s = core::str::from_utf8(chunk).map_err(|_| String::from("non-ASCII hex"))?;
        out[idx] = u8::from_str_radix(s, 16)
            .map_err(|_| format!("invalid hex byte '{s}' in catalog digest"))?;
    }
    Ok(out)
}

fn verify_static_catalog(cfg: &CatalogVerificationConfig) -> Result<u64, String> {
    let bytes = std::fs::read(&cfg.path)
        .map_err(|e| format!("failed to read catalog {}: {e}", cfg.path.display()))?;
    let expected_hash: [u8; 32] = match &cfg.expected_sha256_hex {
        Some(hex_str) => parse_sha256_hex(hex_str)?,
        None => Sha256::digest(&bytes).into(),
    };
    let catalog = Catalog::decode(&bytes, expected_hash)
        .map_err(|e| format!("Catalog::decode failed on {}: {e:?}", cfg.path.display()))?;
    if let Some(expected_ver) = cfg.expected_catalog_version
        && catalog.catalog_version() != expected_ver
    {
        return Err(format!(
            "catalog version mismatch: expected {expected_ver}, found {}",
            catalog.catalog_version()
        ));
    }
    Ok(catalog.catalog_version())
}

impl OrsAppConfig {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read OrsAppConfig {}: {e}", path.display()))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("invalid OrsAppConfig JSON {}: {e}", path.display()))
    }

    pub fn prepare(&self) -> Result<PreparedOrsApp, String> {
        if self.allowed_strategies.is_empty() {
            return Err(String::from("allowed_strategies must not be empty"));
        }
        let catalog_version = match &self.instrument_catalog {
            Some(cat_cfg) => Some(verify_static_catalog(cat_cfg)?),
            None => None,
        };

        let cpu = self.cpu.to_ors_cpu_config()?;
        let account_manager = AccountManager::from_config(&self.account_manager)
            .map_err(|e| format!("AccountManager validation failed: {e}"))?;
        let env = self.environment.to_environment();

        let (symbol_specs, sessions) = match self.venue {
            OrsVenueKind::Binance => self.prepare_binance_sessions(&account_manager)?,
            OrsVenueKind::Bybit => self.prepare_uta_sessions(SpotVenue::Bybit, &account_manager)?,
            OrsVenueKind::Okx => self.prepare_uta_sessions(SpotVenue::Okx, &account_manager)?,
        };

        let max_sym_id = symbol_specs
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
            for s in &symbol_specs {
                default_table.push_str(&format!(
                    "symbol_id={} enabled=true max_qty=1000000000 max_notional=1000000000000000 new_count=10000/50000/100000 order_qty=1000000000/5000000000/10000000000\n",
                    s.symbol_id
                ));
            }
            engine
                .reload_handle()
                .reload_limits(&default_table)
                .map_err(|e| format!("default RmsEngine setup failed: {e}"))?;
            engine
        };

        let account_domains: Vec<_> = account_manager
            .specs()
            .iter()
            .map(|spec| spec.to_domain_config(self.venue.numeric_venue_id(), env))
            .collect();
        let first_dom = account_domains
            .first()
            .copied()
            .ok_or_else(|| String::from("at least one enabled account is required"))?;
        let mut order_manager_config =
            OrderManagerConfig::single_account_default(first_dom, symbol_specs);
        order_manager_config.accounts = account_domains;
        order_manager_config.slots_per_strat = self.slots_per_strat;

        let runtime_config = OrsRuntimeConfig {
            cpu,
            max_order_age_ns: self.max_order_age_ns,
            rms_on_dry_run: self.rms_on_dry_run,
            exec_generation: self.exec_generation,
            instrument_version: catalog_version.unwrap_or(self.instrument_version),
            ws_ping_interval_ms: self.ws_ping_interval_ms,
            ..OrsRuntimeConfig::default()
        };

        let mut control_config = ControlServerConfig::default();
        if let Some(ors_inst) = self.ors_instance {
            control_config.ors_instance = ors_inst;
        }
        if let Some(hb_ms) = self.dop_hb_interval_ms {
            control_config.hb_interval_ms = hb_ms;
        }
        let process_config = OrsProcessConfig {
            control_socket_path: self.control_socket_path.clone(),
            control_config,
            rms_config_path: self.rms_config_path.clone(),
            ..OrsProcessConfig::default()
        };

        Ok(PreparedOrsApp {
            venue: self.venue,
            runtime_config,
            account_manager,
            allowed_strategies: self.allowed_strategies.clone(),
            order_manager_config,
            rms_engine,
            sessions,
            process_config,
            external_lanes: self.external_strategy_lanes.clone(),
            catalog_version,
            summary_output_path: self.summary_output_path.clone(),
        })
    }

    fn prepare_binance_sessions(
        &self,
        account_manager: &AccountManager,
    ) -> Result<PreparedSessions, String> {
        if self.binance_markets.is_empty() {
            return Err(String::from(
                "binance_markets must not be empty when venue == 'binance'",
            ));
        }
        if self.binance_symbols.is_empty() {
            return Err(String::from(
                "binance_symbols must not be empty when venue == 'binance'",
            ));
        }
        let sym_bindings: Vec<BinanceSymbolBinding> = self
            .binance_symbols
            .iter()
            .map(BinanceSymbolConfig::to_binding)
            .collect::<Result<_, _>>()?;
        let specs: Vec<OrsSymbolSpec> = sym_bindings.iter().map(|b| b.spec).collect();
        let asset_bindings: Vec<BinanceAssetBinding> = self
            .binance_assets
            .iter()
            .map(BinanceAssetConfig::to_binding)
            .collect();
        let catalog = Arc::new(
            BinanceBootstrapCatalog::new(sym_bindings, asset_bindings)
                .map_err(|e| format!("BinanceBootstrapCatalog::new failed: {e:?}"))?,
        );

        let env = self.environment.to_environment();
        let mut sessions = Vec::with_capacity(account_manager.specs().len());
        for acct_spec in account_manager.specs() {
            let creds = acct_spec.credentials.as_ref().ok_or_else(|| {
                format!(
                    "Binance account '{}' is missing credentials",
                    acct_spec.name
                )
            })?;
            let mut markets = Vec::with_capacity(self.binance_markets.len());
            for m_blk in &self.binance_markets {
                let m = m_blk.market.to_market();
                let default_order_path = m.ws_order_path().unwrap_or("/eapi/v1/order");
                let order_ep = match &m_blk.order_endpoint {
                    Some(ep) => ep.to_endpoint_spec(default_order_path)?,
                    None => UtaEndpointSpec::manual(default_order_path),
                };
                let stream_ep = match &m_blk.stream_endpoint {
                    Some(ep) => Some(ep.to_endpoint_spec("/ws")?),
                    None => None,
                };
                let rest_ep = match &m_blk.rest_endpoint {
                    Some(ep) => Some(ep.to_rest_endpoint()?),
                    None => None,
                };
                let ed25519_signer = match &m_blk.ed25519_pem_path {
                    Some(pem_path) => {
                        let pem = std::fs::read_to_string(pem_path).map_err(|e| {
                            format!("failed to read Ed25519 PEM {}: {e}", pem_path.display())
                        })?;
                        Some(SigningKey::ed25519_pem(&pem).map_err(|e| {
                            format!("invalid Ed25519 PEM {}: {e:?}", pem_path.display())
                        })?)
                    }
                    None => None,
                };
                let hmac_signer = SigningKey::hmac(creds.secret().as_bytes())
                    .map_err(|e| format!("invalid HMAC secret on '{}': {e:?}", acct_spec.name))?;
                markets.push(BinanceMarketEndpoint {
                    market: m,
                    api_key: creds.key().to_owned(),
                    ed25519_signer,
                    hmac_signer,
                    order_endpoint: order_ep,
                    stream_endpoint: stream_ep,
                    rest_endpoint: rest_ep,
                    stream_colo: m_blk.stream_colo,
                });
            }

            let mut s_cfg = BinanceVenueSessionConfig::new(
                env,
                acct_spec.exchange_account_id,
                acct_spec.risk_domain_id,
                Arc::clone(&catalog),
                markets,
            );
            s_cfg.order_recv_window_ms = self.binance_order_recv_window_ms;
            s_cfg.status_query_delay_ms = self.binance_status_query_delay_ms;
            let session = BinanceVenueSession::new(s_cfg)?;
            sessions.push((
                acct_spec.account_ref,
                Arc::new(UnifiedVenueSession::Binance(session)),
            ));
        }

        Ok((specs, sessions))
    }

    fn prepare_uta_sessions(
        &self,
        spot_venue: SpotVenue,
        account_manager: &AccountManager,
    ) -> Result<PreparedSessions, String> {
        if self.uta_symbols.is_empty() {
            return Err(format!(
                "uta_symbols must not be empty when venue == '{}'",
                spot_venue.as_str()
            ));
        }
        let sym_bindings: Vec<UtaSymbolBinding> = self
            .uta_symbols
            .iter()
            .map(UtaSymbolConfig::to_binding)
            .collect::<Result<_, _>>()?;
        let specs: Vec<OrsSymbolSpec> = sym_bindings.iter().map(|b| b.spec).collect();
        let asset_bindings: Vec<UtaAssetBinding> = self
            .uta_assets
            .iter()
            .map(UtaAssetConfig::to_binding)
            .collect::<Result<_, _>>()?;

        let env = self.environment.to_environment();
        let transport_mode = self.uta_transport.to_transport_mode();
        let default_order_path = match (spot_venue, transport_mode) {
            (SpotVenue::Bybit, tickwire_spot_execution::UtaTransportMode::WebSocketTrade) => {
                "/v5/trade"
            }
            (SpotVenue::Bybit, tickwire_spot_execution::UtaTransportMode::PooledHttps) => {
                "/v5/order/create"
            }
            (SpotVenue::Okx, tickwire_spot_execution::UtaTransportMode::WebSocketTrade) => {
                "/ws/v5/private"
            }
            (SpotVenue::Okx, tickwire_spot_execution::UtaTransportMode::PooledHttps) => {
                "/api/v5/trade/order"
            }
        };
        let default_stream_path = match spot_venue {
            SpotVenue::Bybit => "/v5/private",
            SpotVenue::Okx => "/ws/v5/private",
        };
        let order_ep = match &self.uta_order_endpoint {
            Some(ep) => ep.to_endpoint_spec(default_order_path)?,
            None => UtaEndpointSpec::manual(default_order_path),
        };
        let stream_ep = match &self.uta_stream_endpoint {
            Some(ep) => Some(ep.to_endpoint_spec(default_stream_path)?),
            None => None,
        };
        let rest_ep = match &self.uta_rest_endpoint {
            Some(ep) => Some(ep.to_rest_endpoint()?),
            None => None,
        };

        let mut sessions = Vec::with_capacity(account_manager.specs().len());
        for acct_spec in account_manager.specs() {
            let creds_cfg = acct_spec.credentials.as_ref().ok_or_else(|| {
                format!("UTA account '{}' is missing credentials", acct_spec.name)
            })?;
            let owned_creds = match spot_venue {
                SpotVenue::Bybit => OwnedCredentials::bybit(creds_cfg.key(), creds_cfg.secret()),
                SpotVenue::Okx => OwnedCredentials::okx(
                    creds_cfg.key(),
                    creds_cfg.secret(),
                    creds_cfg.passphrase(),
                ),
            }
            .map_err(|e| format!("invalid credentials on '{}': {e}", acct_spec.name))?;
            let rfq_hook: Option<Box<dyn tickwire_spot_execution::RfqQuoteHook>> =
                if self.uta_enable_rfq_quotes {
                    Some(Box::new(UtaRfqQuoteBook::new(DEFAULT_MAX_OPEN_RFQ_QUOTES)))
                } else {
                    None
                };
            let session = UtaVenueSession::new(UtaVenueSessionConfig {
                venue: spot_venue,
                environment: env,
                exchange_uid: acct_spec.exchange_account_id,
                risk_domain_id: acct_spec.risk_domain_id,
                credentials: owned_creds,
                transport_mode,
                order_endpoint: order_ep.clone(),
                stream_endpoint: stream_ep.clone(),
                rest_endpoint: rest_ep.clone(),
                symbols: sym_bindings.clone(),
                assets: asset_bindings.clone(),
                ip_order_weight: 1,
                status_query_delay_ms: 1_000,
                okx_ws_auto_subscribe_private: true,
                rfq_hook,
                clock_utc_ms: None,
                bootstrap_override: None,
            })?;
            let wrapped = match spot_venue {
                SpotVenue::Bybit => UnifiedVenueSession::BybitUta(session),
                SpotVenue::Okx => UnifiedVenueSession::OkxUta(session),
            };
            sessions.push((acct_spec.account_ref, Arc::new(wrapped)));
        }

        Ok((specs, sessions))
    }
}
