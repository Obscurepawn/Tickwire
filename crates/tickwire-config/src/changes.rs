//! Server-computed change classes. These values do not authorize a deployment.
use crate::{Candidate, Error, Result, validation::decimal_le};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Hot,
    Quiesce,
    RollingRestart,
    ForbiddenRuntime,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Viewer,
    Editor,
    Operator,
    RiskAdmin,
    ClusterAdmin,
}
/// The service constructs this from current authenticated role bindings.
/// Account scope stays attached to the role that grants it.
pub struct Authority {
    grants: Vec<RoleGrant>,
}
pub struct RoleGrant {
    pub role: Role,
    pub accounts: BTreeSet<String>,
    pub cluster_scope: bool,
}
impl Authority {
    pub fn from_grants(grants: Vec<RoleGrant>) -> Self {
        Self { grants }
    }
    fn covers(&self, roles: &[Role], accounts: &BTreeSet<String>, cluster: bool) -> bool {
        let grants: Vec<_> = self
            .grants
            .iter()
            .filter(|g| roles.contains(&g.role))
            .collect();
        if grants.iter().any(|g| g.cluster_scope) {
            return true;
        }
        if cluster || grants.is_empty() {
            return false;
        }
        let granted: BTreeSet<_> = grants.iter().flat_map(|g| g.accounts.iter()).collect();
        accounts.iter().all(|id| granted.contains(id))
    }
}
#[derive(Debug, Clone)]
pub struct Change {
    path: Vec<String>,
    before: Option<Value>,
    after: Option<Value>,
    class: Class,
    accounts: BTreeSet<String>,
    risk_relaxation: bool,
    cluster_change: bool,
}
impl Change {
    pub fn path(&self) -> String {
        self.path.join("/")
    }
    pub fn before(&self) -> Option<&Value> {
        self.before.as_ref()
    }
    pub fn after(&self) -> Option<&Value> {
        self.after.as_ref()
    }
    pub fn class(&self) -> Class {
        self.class
    }
    pub fn accounts(&self) -> &BTreeSet<String> {
        &self.accounts
    }
    pub fn requires_risk_admin(&self) -> bool {
        self.risk_relaxation
    }
}
#[derive(Debug)]
pub struct ChangeSet {
    changes: Vec<Change>,
    class: Class,
    accounts: BTreeSet<String>,
    delivery: BTreeSet<String>,
    risk: bool,
    cluster: bool,
}
impl ChangeSet {
    pub fn between(old: &Candidate, new: &Candidate) -> Result<Self> {
        old.validate_static()?;
        new.validate_static()?;
        let mut raw = Vec::new();
        diff(
            Some(old.value()),
            Some(new.value()),
            &mut Vec::new(),
            &mut raw,
        );
        if raw.len() > 16384 {
            return Err(Error::Capacity);
        }
        let mut changes = Vec::new();
        let mut all_accounts = BTreeSet::new();
        let mut class = Class::Hot;
        let mut risk = false;
        let mut cluster = false;
        for (path, before, after) in raw {
            let (kind, relaxed, global) = classify(&path, before.as_ref(), after.as_ref())?;
            let accounts = scope(&path, old.value(), new.value());
            let global = global || accounts.is_empty();
            all_accounts.extend(accounts.iter().cloned());
            class = class.max(kind);
            risk |= relaxed;
            cluster |= global;
            changes.push(Change {
                path,
                before,
                after,
                class: kind,
                accounts,
                risk_relaxation: relaxed,
                cluster_change: global,
            });
        }
        let delivery = rows(old.value(), "processes")
            .chain(rows(new.value(), "processes"))
            .filter_map(|p| p["id"].as_str().map(str::to_owned))
            .collect();
        Ok(Self {
            changes,
            class,
            accounts: all_accounts,
            delivery,
            risk,
            cluster,
        })
    }
    pub fn class(&self) -> Class {
        self.class
    }
    pub fn requires_risk_admin(&self) -> bool {
        self.risk
    }
    pub fn requires_cluster_admin(&self) -> bool {
        self.cluster
    }
    pub fn accounts(&self) -> &BTreeSet<String> {
        &self.accounts
    }
    pub fn delivery_targets(&self) -> &BTreeSet<String> {
        &self.delivery
    }
    /// Authorization precedes returning before/after values to an API caller.
    pub fn visible_changes(&self, authority: &Authority) -> Result<Vec<&Change>> {
        let roles = [
            Role::Viewer,
            Role::Editor,
            Role::Operator,
            Role::RiskAdmin,
            Role::ClusterAdmin,
        ];
        if !authority.covers(&roles, &BTreeSet::new(), false) {
            return Err(auth_error());
        }
        Ok(self
            .changes
            .iter()
            .filter(|c| authority.covers(&roles, &c.accounts, c.cluster_change))
            .collect())
    }
    pub fn authorize_plan(&self, authority: &Authority) -> Result<()> {
        self.runtime_allowed()?;
        if !authority.covers(&[Role::Editor], &self.accounts, self.cluster) {
            return Err(auth_error());
        }
        Ok(())
    }
    pub fn authorize_commit(&self, authority: &Authority) -> Result<()> {
        self.runtime_allowed()?;
        if !authority.covers(&[Role::Operator], &self.accounts, self.cluster) {
            return Err(auth_error());
        }
        for change in &self.changes {
            if change.risk_relaxation
                && !authority.covers(&[Role::RiskAdmin], &change.accounts, change.cluster_change)
            {
                return Err(auth_error());
            }
            if change.cluster_change
                && !authority.covers(&[Role::ClusterAdmin], &change.accounts, true)
            {
                return Err(auth_error());
            }
        }
        Ok(())
    }
    fn runtime_allowed(&self) -> Result<()> {
        if self.class == Class::ForbiddenRuntime {
            return Err(Error::Semantic("runtime change forbidden"));
        }
        Ok(())
    }
}

fn auth_error() -> Error {
    Error::Semantic("authorization")
}
fn rows<'a>(root: &'a Value, name: &str) -> impl Iterator<Item = &'a Value> {
    root[name].as_array().into_iter().flatten()
}
fn named<'a>(root: &'a Value, group: &str, key: &str, id: &str) -> Option<&'a Value> {
    rows(root, group).find(|v| v[key].as_str() == Some(id))
}
fn entity_key(path: &[String]) -> Option<&'static str> {
    match path {
        [group]
            if matches!(
                group.as_str(),
                "hosts" | "processes" | "venues" | "accounts" | "risk_policies"
            ) =>
        {
            Some("id")
        }
        [group] if group == "strategies" => Some("process_id"),
        [group, field] if group == "ipc" && field == "topics" => Some("id"),
        [group, _, field] if group == "strategies" && field == "risk_budgets" => Some("account_id"),
        _ => None,
    }
}
fn entity_id(v: &Value, key: &str) -> Option<String> {
    v[key]
        .as_str()
        .map(str::to_owned)
        .or_else(|| v[key].as_u64().map(|v| v.to_string()))
}
type RawChange = (Vec<String>, Option<Value>, Option<Value>);
fn diff(
    old: Option<&Value>,
    new: Option<&Value>,
    path: &mut Vec<String>,
    out: &mut Vec<RawChange>,
) {
    if old == new {
        return;
    }
    match (old, new) {
        (Some(Value::Object(a)), Some(Value::Object(b))) => {
            let keys: BTreeSet<_> = a.keys().chain(b.keys()).collect();
            for key in keys {
                path.push(key.clone());
                diff(a.get(key), b.get(key), path, out);
                path.pop();
            }
        }
        (Some(Value::Array(a)), Some(Value::Array(b))) if entity_key(path).is_some() => {
            let key = entity_key(path).unwrap();
            let am: Option<BTreeMap<_, _>> =
                a.iter().map(|v| Some((entity_id(v, key)?, v))).collect();
            let bm: Option<BTreeMap<_, _>> =
                b.iter().map(|v| Some((entity_id(v, key)?, v))).collect();
            if let (Some(am), Some(bm)) = (am, bm) {
                let previous_order: Vec<_> = a
                    .iter()
                    .filter_map(|v| entity_id(v, key))
                    .filter(|id| bm.contains_key(id))
                    .collect();
                let next_order: Vec<_> = b
                    .iter()
                    .filter_map(|v| entity_id(v, key))
                    .filter(|id| am.contains_key(id))
                    .collect();
                for id in am.keys().chain(bm.keys()).collect::<BTreeSet<_>>() {
                    path.push(format!("@{id}"));
                    diff(am.get(id).copied(), bm.get(id).copied(), path, out);
                    path.pop();
                }
                // Relative order is an independent change, including when rows
                // are also edited, added or removed.
                if previous_order != next_order {
                    out.push((path.clone(), old.cloned(), new.cloned()));
                }
            } else {
                out.push((path.clone(), old.cloned(), new.cloned()));
            }
        }
        (Some(Value::Array(a)), Some(Value::Array(b))) if a.len() == b.len() => {
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                path.push(i.to_string());
                diff(Some(a), Some(b), path, out);
                path.pop();
            }
        }
        _ => out.push((path.clone(), old.cloned(), new.cloned())),
    }
}
fn compare(a: &Value, b: &Value) -> Result<core::cmp::Ordering> {
    match (a, b) {
        (Value::String(a), Value::String(b)) => {
            let le = decimal_le(a, b)?;
            let ge = decimal_le(b, a)?;
            Ok(if le && ge {
                core::cmp::Ordering::Equal
            } else if le {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Greater
            })
        }
        (Value::Number(a), Value::Number(b)) => Ok(a
            .as_u64()
            .ok_or(Error::Scalar)?
            .cmp(&b.as_u64().ok_or(Error::Scalar)?)),
        _ => Err(Error::Scalar),
    }
}
fn increases(old: Option<&Value>, new: Option<&Value>) -> Result<bool> {
    match (old, new) {
        (Some(a), Some(b)) => Ok(compare(a, b)?.is_lt()),
        _ => Ok(true),
    }
}
fn classify(
    path: &[String],
    old: Option<&Value>,
    new: Option<&Value>,
) -> Result<(Class, bool, bool)> {
    let group = path.first().map(String::as_str).ok_or(Error::Schema)?;
    let field = path.last().map(String::as_str).ok_or(Error::Schema)?;
    let missing = old.is_none() || new.is_none();
    if group == "identity"
        || (group == "accounts"
            && matches!(
                field,
                "id" | "venue_id"
                    | "risk_domain_id"
                    | "account_identity_ref"
                    | "position_mode"
                    | "margin_mode"
                    | "asset_mode"
            ))
        || (group == "venues" && matches!(field, "id" | "venue" | "product" | "environment"))
        || (group == "risk_policies" && matches!(field, "quote_asset" | "expiry_resets_positions"))
        || (group == "ipc"
            && matches!(
                field,
                "abi_major"
                    | "architecture"
                    | "page_bytes"
                    | "isolation_bytes"
                    | "market_record_bytes"
                    | "intent_record_bytes"
                    | "report_record_bytes"
            ))
    {
        return Ok((Class::ForbiddenRuntime, false, true));
    }
    let risk_field = group == "risk_policies"
        || (group == "strategies" && path.get(2).is_some_and(|p| p == "risk_budgets"));
    if risk_field {
        if missing {
            return Ok((Class::Quiesce, true, path.len() <= 2));
        }
        let lower = matches!(
            field,
            "max_order_quote"
                | "max_gross_quote"
                | "max_open_orders"
                | "max_daily_loss_quote"
                | "max_market_age_us"
        );
        let higher = matches!(
            field,
            "fee_buffer_rate" | "margin_buffer_rate" | "delivery_guard_seconds"
        );
        if lower || higher {
            let cmp = compare(old.unwrap(), new.unwrap())?;
            let relaxed = if lower { cmp.is_lt() } else { cmp.is_gt() };
            return Ok((
                if relaxed { Class::Quiesce } else { Class::Hot },
                relaxed,
                false,
            ));
        }
        if field == "quote_asset" {
            return Ok((Class::ForbiddenRuntime, false, true));
        }
        let relaxed = field != "allow_external_mutation" || new == Some(&Value::Bool(true));
        return Ok((Class::Quiesce, relaxed, false));
    }
    if group == "accounts"
        && path
            .get(2)
            .is_some_and(|p| matches!(p.as_str(), "instrument_overrides" | "allowed_symbols"))
    {
        return Ok((Class::Quiesce, true, false));
    }
    if group == "strategies" && missing && path.len() <= 2 {
        return Ok((Class::RollingRestart, true, true));
    }
    let result = match group {
        "hosts" | "processes" => (Class::RollingRestart, false, true),
        "local_config" => (Class::RollingRestart, false, true),
        "ipc" => (Class::RollingRestart, false, true),
        "observability" if matches!(field, "diagnostic_level" | "sample_per_million") => {
            (Class::Hot, false, true)
        }
        "observability" => (Class::RollingRestart, false, true),
        "journal" if field == "commit_profile" => (
            Class::Quiesce,
            new.and_then(Value::as_str) == Some("LocalDurable+AsyncReplica"),
            true,
        ),
        "journal" if matches!(field, "disk_warn_percent" | "disk_freeze_percent") => {
            (Class::RollingRestart, increases(old, new)?, true)
        }
        "journal" => (Class::RollingRestart, false, true),
        "control" if matches!(field, "listen_uri") || path.iter().any(|s| s == "voters") => {
            (Class::RollingRestart, false, true)
        }
        "control" => (
            Class::Quiesce,
            matches!(
                field,
                "exec_grant_lifetime_ms"
                    | "strategy_grant_lifetime_ms"
                    | "strategy_suspect_after_ms"
            ) && increases(old, new)?,
            true,
        ),
        "replication" => (
            Class::Quiesce,
            matches!(
                field,
                "lag_warn_ms" | "lag_freeze_ms" | "batch_max_delay_us"
            ) && increases(old, new)?,
            true,
        ),
        "timeouts" => (
            Class::Quiesce,
            matches!(
                field,
                "intent_ttl_us"
                    | "private_stream_stale_ms"
                    | "clock_max_offset_us"
                    | "unknown_order_query_ms"
                    | "fencing_deadline_ms"
                    | "takeover_deadline_ms"
            ) && increases(old, new)?,
            true,
        ),
        "qualification" => (Class::Quiesce, false, true),
        "venues" if missing || field == "transport" => (Class::RollingRestart, false, true),
        "venues" if field == "recv_window_ms" => (Class::Quiesce, increases(old, new)?, false),
        "venues" => (Class::Quiesce, false, false),
        "accounts"
            if missing
                || matches!(
                    field,
                    "exec_process_id" | "standby_exec_process_id" | "guardian_process_ids"
                ) =>
        {
            (Class::RollingRestart, false, true)
        }
        "accounts" => (
            Class::Quiesce,
            matches!(
                field,
                "risk_policy_id" | "rate_limit_policy_ref" | "instrument_overrides"
            ),
            false,
        ),
        "strategies"
            if path.len() == 3
                && matches!(
                    field,
                    "process_id"
                        | "logical_strategy_id"
                        | "state_schema_ref"
                        | "recovery_contract_ref"
                        | "trust_profile"
                ) =>
        {
            (Class::RollingRestart, false, true)
        }
        "strategies" => (Class::Quiesce, false, false),
        "risk_policies" => (Class::Quiesce, true, missing),
        _ => return Err(Error::Semantic("unknown change class")),
    };
    Ok(result)
}
fn scope(path: &[String], old: &Value, new: &Value) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    let group = path.first().map(String::as_str).unwrap_or("");
    let id = path.get(1).and_then(|s| s.strip_prefix('@'));
    for root in [old, new] {
        for account in rows(root, "accounts") {
            let aid = account["id"].as_str().unwrap_or("");
            let affected = match (group, id) {
                ("accounts", Some(id)) => id == aid,
                ("risk_policies", Some(id)) => account["risk_policy_id"] == id,
                ("venues", Some(id)) => account["venue_id"] == id,
                ("strategies", Some(id)) => named(root, "strategies", "process_id", id)
                    .is_some_and(|s| {
                        if path.get(2).is_some_and(|p| p == "risk_budgets")
                            && let Some(account) = path.get(3).and_then(|v| v.strip_prefix('@'))
                        {
                            return account == aid;
                        }
                        s["account_ids"]
                            .as_array()
                            .is_some_and(|a| a.iter().any(|a| a == aid))
                    }),
                _ => true,
            };
            if affected {
                result.insert(aid.to_owned());
            }
        }
    }
    result
}
