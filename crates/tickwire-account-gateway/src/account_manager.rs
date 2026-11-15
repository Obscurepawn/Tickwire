//! Multi-account ORS `AccountSpec` & `AccountManager` configuration loader, redacted credentials,
//! `[local_ip, count]` URL pool descriptors, and deterministic `account_ref` routing (`8.3`).

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use tickwire_account::OrsAccountDomainConfig;
use tickwire_quota::shared::AccountRateOverrides;
use tickwire_transport::UrlBindPair;
use tickwire_venue::{ALL_ORS_STRATS, Environment, RefusalReason};

/// Default number of persistent sockets opened per URL when `local_ip/connection_count` is empty.
pub const DEFAULT_URL_CONNECTIONS: u16 = 2;

/// Maximum `1`-based account reference (`1..=255`).
pub const MAX_ACCOUNT_REF: u8 = 255;

/// Maximum valid strategy ID (`0..=254`; `255` is reserved for `ALL_ORS_STRATS`).
pub const MAX_STRAT_ID: u8 = 254;

const MAX_NIC_LEN: usize = 15;

/// Exchange API credentials with mandatory `fmt::Debug` redaction.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    key: String,
    secret: String,
    #[serde(default)]
    passphrase: String,
}

impl Credentials {
    pub fn new(
        key: impl Into<String>,
        secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            secret: secret.into(),
            passphrase: passphrase.into(),
        }
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[inline]
    pub fn secret(&self) -> &str {
        &self.secret
    }

    #[inline]
    pub fn passphrase(&self) -> &str {
        &self.passphrase
    }

    pub fn validate(&self) -> Result<(), AccountManagerError> {
        if self.key.trim().is_empty() || self.secret.trim().is_empty() {
            return Err(AccountManagerError::EmptyCredentials);
        }
        Ok(())
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("key", &"[REDACTED]")
            .field("secret", &"[REDACTED]")
            .field("passphrase", &"[REDACTED]")
            .finish()
    }
}

/// Serde-friendly per-account rate-limit overrides (`ACCOUNT_MANAGER.<name>.rate_limit`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRateLimitConfig {
    #[serde(default, alias = "orders_10s")]
    pub primary_order_limit: Option<u64>,
    #[serde(default, alias = "orders_min")]
    pub secondary_order_limit: Option<u64>,
    #[serde(default, alias = "requests_min", alias = "requests_30s")]
    pub account_request_limit: Option<u64>,
    #[serde(default)]
    pub per_symbol_order_limit: Option<u64>,
    #[serde(default)]
    pub per_symbol_cancel_limit: Option<u64>,
    #[serde(default)]
    pub per_product_order_limit: Option<u64>,
    #[serde(default)]
    pub per_product_cancel_limit: Option<u64>,
}

impl From<AccountRateLimitConfig> for AccountRateOverrides {
    fn from(cfg: AccountRateLimitConfig) -> Self {
        Self {
            primary_order_limit: cfg.primary_order_limit,
            secondary_order_limit: cfg.secondary_order_limit,
            account_request_limit: cfg.account_request_limit,
            per_symbol_order_limit: cfg.per_symbol_order_limit,
            per_symbol_cancel_limit: cfg.per_symbol_cancel_limit,
            per_product_order_limit: cfg.per_product_order_limit,
            per_product_cancel_limit: cfg.per_product_cancel_limit,
        }
    }
}

impl From<AccountRateOverrides> for AccountRateLimitConfig {
    fn from(ov: AccountRateOverrides) -> Self {
        Self {
            primary_order_limit: ov.primary_order_limit,
            secondary_order_limit: ov.secondary_order_limit,
            account_request_limit: ov.account_request_limit,
            per_symbol_order_limit: ov.per_symbol_order_limit,
            per_symbol_cancel_limit: ov.per_symbol_cancel_limit,
            per_product_order_limit: ov.per_product_order_limit,
            per_product_cancel_limit: ov.per_product_cancel_limit,
        }
    }
}

/// Source-IP + socket-count descriptor supporting both `[local_ip, count]` tuple syntax
/// (`["10.0.0.9", 2]` or `["", 2]`) and structured `{ local_ip, nic, count, colo }` syntax.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UrlBindSpec {
    pub local_ip: Option<IpAddr>,
    pub nic: Option<String>,
    pub count: u16,
    pub colo: bool,
}

impl UrlBindSpec {
    pub fn new(local_ip: Option<IpAddr>, count: u16) -> Self {
        Self {
            local_ip,
            nic: None,
            count,
            colo: false,
        }
    }

    pub fn with_nic(mut self, nic: impl Into<String>) -> Self {
        self.nic = Some(nic.into());
        self
    }

    pub fn with_colo(mut self, colo: bool) -> Self {
        self.colo = colo;
        self
    }

    pub fn to_bind_pair(&self) -> UrlBindPair {
        let mut pair = UrlBindPair::new(self.local_ip, self.count).with_colo(self.colo);
        if let Some(ref nic) = self.nic {
            pair = pair.with_nic(nic.clone());
        }
        pair
    }
}

impl<'de> Deserialize<'de> for UrlBindSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct MapForm {
            #[serde(default)]
            local_ip: Option<String>,
            #[serde(default)]
            nic: Option<String>,
            count: u16,
            #[serde(default)]
            colo: bool,
        }

        struct BindVisitor;

        impl<'de> Visitor<'de> for BindVisitor {
            type Value = UrlBindSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a [local_ip, count] pair or { local_ip, nic, count, colo } object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let ip_raw: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let count: u16 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                if seq.next_element::<serde_json::Value>()?.is_some() {
                    return Err(de::Error::invalid_length(3, &self));
                }
                let local_ip = parse_non_zero_ip(&ip_raw).map_err(de::Error::custom)?;
                Ok(UrlBindSpec {
                    local_ip,
                    nic: None,
                    count,
                    colo: false,
                })
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let parsed = MapForm::deserialize(de::value::MapAccessDeserializer::new(map))?;
                let local_ip = match parsed.local_ip.as_deref() {
                    Some(raw) => parse_non_zero_ip(raw).map_err(de::Error::custom)?,
                    None => None,
                };
                Ok(UrlBindSpec {
                    local_ip,
                    nic: parsed.nic,
                    count: parsed.count,
                    colo: parsed.colo,
                })
            }
        }

        deserializer.deserialize_any(BindVisitor)
    }
}

fn parse_non_zero_ip(raw: &str) -> Result<Option<IpAddr>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let addr: IpAddr = trimmed
        .parse()
        .map_err(|_| format!("invalid IP address '{trimmed}'"))?;
    if addr.is_unspecified() {
        return Err(format!(
            "unspecified zero IP address '{trimmed}' is not allowed"
        ));
    }
    Ok(Some(addr))
}

/// Per-`UrlId` socket-pool block inside `AccountSpec.urls`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UrlPoolConfig {
    #[serde(default, rename = "local_ip/connection_count", alias = "binds")]
    pub binds: Vec<UrlBindSpec>,
}

fn default_epoch() -> u32 {
    1
}

fn default_risk_domain_id() -> [u8; 16] {
    [1u8; 16]
}

fn serialize_rate_overrides<S: serde::Serializer>(
    ov: &AccountRateOverrides,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    AccountRateLimitConfig::from(*ov).serialize(serializer)
}

/// Validated runtime specification for one `AccountSession` (`1 AccountSession = 1 Exchange UID`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountSpec {
    pub account_ref: u8,
    pub name: String,
    pub exchange_account_id: u64,
    pub strategies: Vec<u8>,
    pub credentials: Option<Credentials>,
    pub local_ip: Option<IpAddr>,
    pub nic: Option<String>,
    pub urls: BTreeMap<String, UrlPoolConfig>,
    #[serde(serialize_with = "serialize_rate_overrides")]
    pub rate_limit: AccountRateOverrides,
    pub epoch: u32,
    pub risk_domain_id: [u8; 16],
}

impl AccountSpec {
    /// Returns `true` when `strat_id` (`0..=254`) is authorized to route onto this account.
    #[inline]
    pub fn permits(&self, strat_id: u8) -> bool {
        strat_id != ALL_ORS_STRATS && self.strategies.contains(&strat_id)
    }

    /// Resolves the `[local_ip, count]` bind pairs for `url_id`, falling back to
    /// [`DEFAULT_URL_CONNECTIONS`] (`2`) on the account's `nic` / `local_ip` when empty.
    pub fn effective_url_binds(&self, url_id: &str) -> Option<Vec<UrlBindPair>> {
        let pool = self.urls.get(url_id)?;
        if pool.binds.is_empty() {
            let mut pair = UrlBindPair::new(self.local_ip, DEFAULT_URL_CONNECTIONS);
            if let Some(ref nic) = self.nic {
                pair = pair.with_nic(nic.clone());
            }
            return Some(vec![pair]);
        }
        Some(
            pool.binds
                .iter()
                .map(|b| {
                    let effective_ip = b.local_ip.or(self.local_ip);
                    let mut pair = UrlBindPair::new(effective_ip, b.count).with_colo(b.colo);
                    if let Some(nic) = b.nic.as_ref().or(self.nic.as_ref()) {
                        pair = pair.with_nic(nic.clone());
                    }
                    pair
                })
                .collect(),
        )
    }

    /// Bind IP used for REST queries / listen-key keepalives on `url_id`: if every socket on
    /// `url_id` shares a single `local_ip`, returns that IP; otherwise returns `self.local_ip`.
    pub fn query_local_ip(&self, url_id: Option<&str>) -> Option<IpAddr> {
        if let Some(id) = url_id
            && let Some(pool) = self.urls.get(id)
            && !pool.binds.is_empty()
        {
            let first = pool.binds[0].local_ip;
            if first.is_some() && pool.binds.iter().all(|b| b.local_ip == first) {
                return first;
            }
        }
        self.local_ip
    }

    /// Builds an [`OrsAccountDomainConfig`] for `tickwire_account::OrderManager`.
    pub fn to_domain_config(&self, venue: u32, environment: Environment) -> OrsAccountDomainConfig {
        OrsAccountDomainConfig {
            account_ref: self.account_ref,
            risk_domain_id: self.risk_domain_id,
            venue,
            environment,
            exchange_account_id: self.exchange_account_id,
            epoch: self.epoch,
        }
    }
}

/// Raw JSON/TOML block for one named account inside `ACCOUNT_MANAGER`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawAccountBlock {
    pub account_ref: u8,
    #[serde(default)]
    pub exchange_account_id: Option<u64>,
    pub strategies: Vec<u8>,
    #[serde(default)]
    pub credentials: Option<Credentials>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub passphrase: Option<String>,
    #[serde(default)]
    pub local_ip: Option<String>,
    #[serde(default)]
    pub nic: Option<String>,
    #[serde(default)]
    pub urls: BTreeMap<String, UrlPoolConfig>,
    #[serde(default)]
    pub rate_limit: AccountRateLimitConfig,
    #[serde(default = "default_epoch")]
    pub epoch: u32,
    #[serde(default = "default_risk_domain_id")]
    pub risk_domain_id: [u8; 16],
}

/// Top-level `ORS.ACCOUNT_MANAGER` configuration block (`human-docs/8.3`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountManagerConfig {
    pub enabled_accounts: Vec<String>,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub mongo_ip: Option<String>,
    /// Allow synthetic test/sim venues with empty `urls`.
    #[serde(default)]
    pub allow_empty_urls: bool,
    #[serde(flatten)]
    pub accounts: BTreeMap<String, RawAccountBlock>,
}

/// Validation failure while loading [`AccountManager`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountManagerError {
    EmptyEnabledAccounts,
    InvalidAccountName(String),
    DuplicateEnabledAccount(String),
    MissingAccountBlock(String),
    InvalidAccountRef {
        name: String,
        account_ref: u8,
    },
    DuplicateAccountRef(u8),
    DuplicateExchangeAccountId(u64),
    EmptyStrategies(String),
    InvalidStratId {
        name: String,
        strat_id: u8,
    },
    DuplicateStratId {
        name: String,
        strat_id: u8,
    },
    EmptyCredentials,
    MissingServerNameForRemoteCredentials(String),
    InvalidLocalIp {
        name: String,
        detail: String,
    },
    InvalidNic {
        name: String,
        nic: String,
    },
    EmptyUrls(String),
    InvalidBindCount {
        name: String,
        url_id: String,
    },
    DuplicateBindIp {
        name: String,
        url_id: String,
        ip: IpAddr,
    },
    Json(String),
}

impl fmt::Display for AccountManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AccountManagerError {}

/// Hot-path account routing refusal (`8.3 account-manager.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountRoutingError {
    InvalidStratId,
    InvalidAccountId,
    RoutingIdMismatch,
}

impl AccountRoutingError {
    #[inline]
    pub const fn refusal_reason(self) -> RefusalReason {
        match self {
            Self::InvalidStratId => RefusalReason::InvalidStratId,
            Self::InvalidAccountId => RefusalReason::InvalidAccountId,
            Self::RoutingIdMismatch => RefusalReason::RejectRoutingIdMismatch,
        }
    }
}

/// Validated multi-account lookup table preserving `enabled_accounts` order.
#[derive(Debug, Clone)]
pub struct AccountManager {
    specs: Vec<AccountSpec>,
    by_ref: [Option<usize>; 256],
    strats_to_refs: Vec<Vec<u8>>,
}

impl AccountManager {
    /// Parses and validates an `ACCOUNT_MANAGER` JSON document.
    pub fn from_json_str(raw: &str) -> Result<Self, AccountManagerError> {
        let cfg: AccountManagerConfig =
            serde_json::from_str(raw).map_err(|e| AccountManagerError::Json(e.to_string()))?;
        Self::from_config(&cfg)
    }

    /// Validates `cfg` and constructs `AccountSpec`s strictly in `enabled_accounts` order.
    pub fn from_config(cfg: &AccountManagerConfig) -> Result<Self, AccountManagerError> {
        if cfg.enabled_accounts.is_empty() {
            return Err(AccountManagerError::EmptyEnabledAccounts);
        }

        let mut seen_names = BTreeSet::new();
        let mut seen_refs = BTreeSet::new();
        let mut seen_uids = BTreeSet::new();
        let mut specs = Vec::with_capacity(cfg.enabled_accounts.len());
        let mut by_ref = [None; 256];
        let mut strats_to_refs = vec![Vec::new(); 255];

        for name in &cfg.enabled_accounts {
            if name.is_empty() || name.contains('.') {
                return Err(AccountManagerError::InvalidAccountName(name.clone()));
            }
            if !seen_names.insert(name.clone()) {
                return Err(AccountManagerError::DuplicateEnabledAccount(name.clone()));
            }
            let block = cfg
                .accounts
                .get(name)
                .ok_or_else(|| AccountManagerError::MissingAccountBlock(name.clone()))?;

            if block.account_ref == 0 {
                return Err(AccountManagerError::InvalidAccountRef {
                    name: name.clone(),
                    account_ref: 0,
                });
            }
            if !seen_refs.insert(block.account_ref) {
                return Err(AccountManagerError::DuplicateAccountRef(block.account_ref));
            }

            let exchange_account_id = block
                .exchange_account_id
                .unwrap_or(u64::from(block.account_ref));
            if exchange_account_id == 0 || !seen_uids.insert(exchange_account_id) {
                return Err(AccountManagerError::DuplicateExchangeAccountId(
                    exchange_account_id,
                ));
            }

            if block.strategies.is_empty() {
                return Err(AccountManagerError::EmptyStrategies(name.clone()));
            }
            let mut seen_block_strats = BTreeSet::new();
            for &sid in &block.strategies {
                if sid == ALL_ORS_STRATS {
                    return Err(AccountManagerError::InvalidStratId {
                        name: name.clone(),
                        strat_id: sid,
                    });
                }
                if !seen_block_strats.insert(sid) {
                    return Err(AccountManagerError::DuplicateStratId {
                        name: name.clone(),
                        strat_id: sid,
                    });
                }
            }

            let credentials = if let Some(ref creds) = block.credentials {
                creds.validate()?;
                Some(creds.clone())
            } else if block.key.is_some() || block.secret.is_some() || block.passphrase.is_some() {
                let creds = Credentials::new(
                    block.key.clone().unwrap_or_default(),
                    block.secret.clone().unwrap_or_default(),
                    block.passphrase.clone().unwrap_or_default(),
                );
                creds.validate()?;
                Some(creds)
            } else {
                if cfg
                    .server_name
                    .as_deref()
                    .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(AccountManagerError::MissingServerNameForRemoteCredentials(
                        name.clone(),
                    ));
                }
                None
            };

            let local_ip = match block.local_ip.as_deref() {
                Some(raw) => parse_non_zero_ip(raw).map_err(|detail| {
                    AccountManagerError::InvalidLocalIp {
                        name: name.clone(),
                        detail,
                    }
                })?,
                None => None,
            };

            if let Some(ref nic) = block.nic
                && (nic.is_empty()
                    || nic.len() > MAX_NIC_LEN
                    || !nic.bytes().all(|b| b.is_ascii_graphic()))
            {
                return Err(AccountManagerError::InvalidNic {
                    name: name.clone(),
                    nic: nic.clone(),
                });
            }

            if !cfg.allow_empty_urls && block.urls.is_empty() {
                return Err(AccountManagerError::EmptyUrls(name.clone()));
            }
            for (url_id, pool) in &block.urls {
                let mut seen_bind_ips = BTreeSet::new();
                for bind in &pool.binds {
                    if bind.count == 0 {
                        return Err(AccountManagerError::InvalidBindCount {
                            name: name.clone(),
                            url_id: url_id.clone(),
                        });
                    }
                    if let Some(ref nic) = bind.nic
                        && (nic.is_empty()
                            || nic.len() > MAX_NIC_LEN
                            || !nic.bytes().all(|b| b.is_ascii_graphic()))
                    {
                        return Err(AccountManagerError::InvalidNic {
                            name: name.clone(),
                            nic: nic.clone(),
                        });
                    }
                    if let Some(ip) = bind.local_ip
                        && !seen_bind_ips.insert(ip)
                    {
                        return Err(AccountManagerError::DuplicateBindIp {
                            name: name.clone(),
                            url_id: url_id.clone(),
                            ip,
                        });
                    }
                }
            }

            let idx = specs.len();
            by_ref[usize::from(block.account_ref)] = Some(idx);
            for &sid in &block.strategies {
                strats_to_refs[usize::from(sid)].push(block.account_ref);
            }

            specs.push(AccountSpec {
                account_ref: block.account_ref,
                name: name.clone(),
                exchange_account_id,
                strategies: block.strategies.clone(),
                credentials,
                local_ip,
                nic: block.nic.clone(),
                urls: block.urls.clone(),
                rate_limit: block.rate_limit.into(),
                epoch: block.epoch.max(1),
                risk_domain_id: block.risk_domain_id,
            });
        }

        Ok(Self {
            specs,
            by_ref,
            strats_to_refs,
        })
    }

    /// Loaded account specifications in exact `enabled_accounts` order.
    #[inline]
    pub fn specs(&self) -> &[AccountSpec] {
        &self.specs
    }

    /// Consumes the manager and returns its `AccountSpec`s in `enabled_accounts` order.
    pub fn into_specs(self) -> Vec<AccountSpec> {
        self.specs
    }

    /// `O(1)` lookup by `1`-based `account_ref` (`0` always returns `None`).
    #[inline]
    pub fn by_account_ref(&self, account_ref: u8) -> Option<&AccountSpec> {
        if account_ref == 0 {
            return None;
        }
        let idx = self.by_ref[usize::from(account_ref)]?;
        self.specs.get(idx)
    }

    /// Returns all `account_ref`s authorizing `strat_id`, in `enabled_accounts` order.
    #[inline]
    pub fn accounts_for_strategy(&self, strat_id: u8) -> &[u8] {
        if strat_id == ALL_ORS_STRATS {
            return &[];
        }
        &self.strats_to_refs[usize::from(strat_id)]
    }

    /// Resolves `requested_account_ref` (`0` = first block listing `strat_id`; `1..=255` = explicit
    /// block) per `human-docs/8.3`.
    #[inline]
    pub fn resolve_account_ref(
        &self,
        requested_account_ref: u8,
        strat_id: u8,
    ) -> Result<&AccountSpec, AccountRoutingError> {
        if strat_id == ALL_ORS_STRATS {
            return Err(AccountRoutingError::InvalidStratId);
        }
        if requested_account_ref == 0 {
            let &first_ref = self.strats_to_refs[usize::from(strat_id)]
                .first()
                .ok_or(AccountRoutingError::InvalidStratId)?;
            let idx =
                self.by_ref[usize::from(first_ref)].ok_or(AccountRoutingError::InvalidStratId)?;
            return Ok(&self.specs[idx]);
        }

        let Some(idx) = self.by_ref[usize::from(requested_account_ref)] else {
            return Err(AccountRoutingError::InvalidAccountId);
        };
        let spec = &self.specs[idx];
        if !spec.permits(strat_id) {
            return Err(AccountRoutingError::RoutingIdMismatch);
        }
        Ok(spec)
    }

    /// Builds `OrsAccountDomainConfig` entries for `OrderManagerConfig`.
    pub fn ors_domain_configs(
        &self,
        venue: u32,
        environment: Environment,
    ) -> Vec<OrsAccountDomainConfig> {
        self.specs
            .iter()
            .map(|s| s.to_domain_config(venue, environment))
            .collect()
    }
}
