//! Static startup configuration and validation for [`crate::OrsRuntimePolicy`].

use crate::{MAX_STRAT_ID, Mode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use tickwire_venue::ALL_ORS_STRATS;

/// Per-account strategy authorization descriptor in `enabled_accounts` order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountAuthSpec {
    /// Unique `1`-based account reference (`1..=255`).
    pub account_ref: u8,
    /// Non-empty logical account block name (cannot contain `.`).
    pub name: String,
    /// Non-empty list of strategy IDs (`0..=254`) permitted to route orders onto this account.
    pub strategies: Vec<u8>,
}

impl AccountAuthSpec {
    pub fn new(account_ref: u8, name: impl Into<String>, strategies: Vec<u8>) -> Self {
        Self {
            account_ref,
            name: name.into(),
            strategies,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Static startup configuration for [`crate::OrsRuntimePolicy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Initial ORS process mode (defaults to [`Mode::Trading`]).
    #[serde(default)]
    pub initial_mode: Mode,
    /// Optional explicit process-wide strategy whitelist (`0..=254`). When empty, the union of all
    /// `enabled_accounts[*].strategies` forms the configured whitelist; when non-empty, every
    /// per-account strategy ID must appear in `strategies`.
    #[serde(default)]
    pub strategies: Vec<u8>,
    /// Enabled accounts in deterministic configuration order (`account_ref = 0` picks the first
    /// entry whose `strategies` contains `strat_id`).
    pub enabled_accounts: Vec<AccountAuthSpec>,
    /// Whether configured strategies start `active = true` (default `true`).
    #[serde(default = "default_true")]
    pub default_active: bool,
    /// Whether configured `(account_ref, strat_id)` pairs start `enabled = true` (default `true`).
    #[serde(default = "default_true")]
    pub default_enabled: bool,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            initial_mode: Mode::Trading,
            strategies: Vec::new(),
            enabled_accounts: Vec::new(),
            default_active: true,
            default_enabled: true,
        }
    }
}

/// Startup validation failure for [`PolicyConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyConfigError {
    EmptyEnabledAccounts,
    InvalidAccountRef(u8),
    DuplicateAccountRef(u8),
    InvalidAccountName(String),
    DuplicateAccountName(String),
    EmptyAccountStrategies(u8),
    ReservedStratId(u8),
    DuplicateWhitelistStratId(u8),
    DuplicateAccountStratId { account_ref: u8, strat_id: u8 },
    UnwhitelistedAccountStratId { account_ref: u8, strat_id: u8 },
}

impl fmt::Display for PolicyConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyEnabledAccounts => f.write_str("enabled_accounts must not be empty"),
            Self::InvalidAccountRef(r) => {
                write!(f, "account_ref {r} must be in 1..=255")
            }
            Self::DuplicateAccountRef(r) => {
                write!(f, "duplicate account_ref {r} in enabled_accounts")
            }
            Self::InvalidAccountName(name) => {
                write!(
                    f,
                    "invalid account name '{name}' (must be non-empty and contain no '.')"
                )
            }
            Self::DuplicateAccountName(name) => {
                write!(f, "duplicate account name '{name}' in enabled_accounts")
            }
            Self::EmptyAccountStrategies(r) => {
                write!(f, "account_ref {r} must authorize at least one strategy")
            }
            Self::ReservedStratId(s) => {
                write!(f, "strat_id {s} exceeds MAX_STRAT_ID ({MAX_STRAT_ID})")
            }
            Self::DuplicateWhitelistStratId(s) => {
                write!(f, "duplicate strat_id {s} in strategies whitelist")
            }
            Self::DuplicateAccountStratId {
                account_ref,
                strat_id,
            } => write!(
                f,
                "duplicate strat_id {strat_id} in account_ref {account_ref}"
            ),
            Self::UnwhitelistedAccountStratId {
                account_ref,
                strat_id,
            } => write!(
                f,
                "account_ref {account_ref} authorizes strat_id {strat_id} absent from strategies whitelist"
            ),
        }
    }
}

impl std::error::Error for PolicyConfigError {}

impl PolicyConfig {
    /// Validates account references, names, and strategy allow-lists.
    pub fn validate(&self) -> Result<(), PolicyConfigError> {
        if self.enabled_accounts.is_empty() {
            return Err(PolicyConfigError::EmptyEnabledAccounts);
        }

        let mut whitelist = BTreeSet::new();
        for &sid in &self.strategies {
            if sid == ALL_ORS_STRATS {
                return Err(PolicyConfigError::ReservedStratId(sid));
            }
            if !whitelist.insert(sid) {
                return Err(PolicyConfigError::DuplicateWhitelistStratId(sid));
            }
        }

        let mut seen_refs = BTreeSet::new();
        let mut seen_names = BTreeSet::new();
        for acct in &self.enabled_accounts {
            if acct.account_ref == 0 {
                return Err(PolicyConfigError::InvalidAccountRef(0));
            }
            if !seen_refs.insert(acct.account_ref) {
                return Err(PolicyConfigError::DuplicateAccountRef(acct.account_ref));
            }
            if acct.name.is_empty() || acct.name.contains('.') {
                return Err(PolicyConfigError::InvalidAccountName(acct.name.clone()));
            }
            if !seen_names.insert(acct.name.clone()) {
                return Err(PolicyConfigError::DuplicateAccountName(acct.name.clone()));
            }
            if acct.strategies.is_empty() {
                return Err(PolicyConfigError::EmptyAccountStrategies(acct.account_ref));
            }

            let mut acct_strats = BTreeSet::new();
            for &sid in &acct.strategies {
                if sid == ALL_ORS_STRATS {
                    return Err(PolicyConfigError::ReservedStratId(sid));
                }
                if !acct_strats.insert(sid) {
                    return Err(PolicyConfigError::DuplicateAccountStratId {
                        account_ref: acct.account_ref,
                        strat_id: sid,
                    });
                }
                if !whitelist.is_empty() && !whitelist.contains(&sid) {
                    return Err(PolicyConfigError::UnwhitelistedAccountStratId {
                        account_ref: acct.account_ref,
                        strat_id: sid,
                    });
                }
            }
        }
        Ok(())
    }
}
