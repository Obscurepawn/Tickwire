//! Deterministic least-privilege payloads from a content-pinned policy.
use crate::{Candidate, Error, Format, Hash, Limits, Result, strict};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::OnceLock};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema: u32,
    revision: u64,
    targets: Vec<Target>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    process_id: String,
    account_ids: Vec<String>,
    venue_ids: Vec<String>,
    topic_ids: Vec<u64>,
    secret_purposes: Vec<String>,
}
#[derive(Debug)]
pub struct PinnedPolicy {
    reference: String,
    document: Document,
}
impl PinnedPolicy {
    pub fn decode(reference: &str, bytes: &[u8]) -> Result<Self> {
        let (path, digest) = reference.rsplit_once("@sha256:").ok_or(err())?;
        let hash: String = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if !path.starts_with("artifact://") || path.len() <= 11 || digest != hash {
            return Err(err());
        }
        let candidate = Candidate::parse(
            Format::Json,
            bytes,
            Limits {
                max_bytes: 4 * 1024 * 1024,
                max_depth: 8,
            },
        )?;
        let document: Document =
            serde_json::from_value(candidate.value().clone()).map_err(|_| err())?;
        if document.schema != 1
            || document.revision == 0
            || document.revision > 9_007_199_254_740_991
            || document.targets.is_empty()
            || document.targets.len() > 4096
        {
            return Err(err());
        }
        unique(document.targets.iter().map(|t| &t.process_id))?;
        for t in &document.targets {
            if t.process_id.is_empty()
                || t.account_ids.len() > 4096
                || t.venue_ids.len() > 4096
                || t.topic_ids.len() > 4096
            {
                return Err(err());
            }
            unique(t.account_ids.iter())?;
            unique(t.venue_ids.iter())?;
            unique(t.topic_ids.iter())?;
            unique(t.secret_purposes.iter())?;
            if t.secret_purposes.iter().any(|v| {
                !matches!(
                    v.as_str(),
                    "execution"
                        | "guardian"
                        | "market-data"
                        | "control"
                        | "replication"
                        | "metrics"
                )
            }) {
                return Err(err());
            }
        }
        Ok(Self {
            reference: reference.into(),
            document,
        })
    }
    pub fn revision(&self) -> u64 {
        self.document.revision
    }
    pub fn reference(&self) -> &str {
        &self.reference
    }
}
#[derive(Debug)]
pub struct Projection {
    value: Value,
    canonical: Vec<u8>,
    hash: Hash,
    global_hash: Hash,
    policy_revision: u64,
}
impl Projection {
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn global_hash(&self) -> Hash {
        self.global_hash
    }
    pub fn policy_revision(&self) -> u64 {
        self.policy_revision
    }
}
fn err() -> Error {
    Error::Semantic("projection policy")
}
fn unique<'a, T: Ord + 'a>(values: impl Iterator<Item = &'a T>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for v in values {
        if !seen.insert(v) {
            return Err(err());
        }
    }
    Ok(())
}
fn array<'a>(v: &'a Value, key: &str) -> Result<&'a [Value]> {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or(err())
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key).and_then(Value::as_str).ok_or(err())
}
fn ids(values: &[Value]) -> Result<BTreeSet<&str>> {
    values.iter().map(|v| v.as_str().ok_or(err())).collect()
}
fn named<'a>(root: &'a Value, collection: &str, id: &str) -> Result<&'a Value> {
    array(root, collection)?
        .iter()
        .find(|v| v["id"] == id)
        .ok_or(err())
}
fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../../../specs/contracts/system-config.schema.json"
        ))
        .expect("fixed schema")
    })
}
fn view(source: &Value, name: &str) -> Result<Value> {
    let fields = schema()["$defs"][name]["properties"]
        .as_object()
        .ok_or(err())?;
    Ok(Value::Object(
        fields
            .keys()
            .filter_map(|key| source.get(key).map(|v| (key.clone(), v.clone())))
            .collect(),
    ))
}
fn purpose_allowed(role: &str, purpose: &str) -> bool {
    if purpose == "metrics" {
        return role != "strategy";
    }
    matches!(
        (role, purpose),
        ("exec", "execution")
            | ("guardian", "guardian")
            | ("md", "market-data")
            | ("supervisor", "control")
            | ("replicator", "replication")
    )
}
fn add_secret(bindings: &mut Vec<Value>, purpose: &str, reference: &Value, account: Option<&str>) {
    let mut b = json!({"purpose":purpose,"ref":reference});
    if let Some(account) = account {
        b["account_id"] = account.into();
    }
    if !bindings.contains(&b) {
        bindings.push(b);
    }
}
fn check_secret_references(value: &Value, allowed: &BTreeSet<&str>) -> Result<()> {
    match value {
        Value::String(s) if s.contains("secret://") => {
            if !allowed.contains(s.as_str()) {
                return Err(err());
            }
        }
        Value::Array(values) => {
            for v in values {
                check_secret_references(v, allowed)?;
            }
        }
        Value::Object(values) => {
            for v in values.values() {
                check_secret_references(v, allowed)?;
            }
        }
        _ => {}
    }
    Ok(())
}
/// Produces a data object; control commit and runtime authority are separate.
pub fn project(
    candidate: &Candidate,
    process_id: &str,
    policy: &PinnedPolicy,
) -> Result<Projection> {
    candidate.validate_static()?;
    let root = candidate.value();
    if root["local_config"]["projection_policy_ref"] != policy.reference
        || root["local_config"]["projection_policy_revision"].as_u64() != Some(policy.revision())
    {
        return Err(err());
    }
    let target = policy
        .document
        .targets
        .iter()
        .find(|t| t.process_id == process_id)
        .ok_or(err())?;
    let process = named(root, "processes", process_id)?;
    let host = named(root, "hosts", text(process, "host_id")?)?;
    let role = text(process, "role")?;
    if target
        .secret_purposes
        .iter()
        .any(|p| !purpose_allowed(role, p))
    {
        return Err(err());
    }
    let granted: BTreeSet<&str> = target.account_ids.iter().map(String::as_str).collect();
    let strategies = array(root, "strategies")?;
    let strategy = strategies.iter().find(|v| v["process_id"] == process_id);
    let assigned: BTreeSet<&str> = match role {
        "strategy" => ids(array(strategy.ok_or(err())?, "account_ids")?)?,
        "exec" | "guardian" => array(root, "accounts")?
            .iter()
            .filter(|a| {
                if role == "exec" {
                    a["exec_process_id"] == process_id || a["standby_exec_process_id"] == process_id
                } else {
                    a["guardian_process_ids"]
                        .as_array()
                        .is_some_and(|v| v.iter().any(|s| s == process_id))
                }
            })
            .map(|a| text(a, "id"))
            .collect::<Result<_>>()?,
        "md" => BTreeSet::new(),
        _ => granted.clone(),
    };
    if assigned != granted {
        return Err(err());
    }
    let accounts: Vec<&Value> = target
        .account_ids
        .iter()
        .map(|id| named(root, "accounts", id))
        .collect::<Result<_>>()?;
    let venues: Vec<&Value> = target
        .venue_ids
        .iter()
        .map(|id| named(root, "venues", id))
        .collect::<Result<_>>()?;
    if matches!(role, "strategy" | "exec" | "guardian") {
        let required = accounts
            .iter()
            .map(|a| text(a, "venue_id"))
            .collect::<Result<BTreeSet<_>>>()?;
        if required != target.venue_ids.iter().map(String::as_str).collect() {
            return Err(err());
        }
    }
    let topics: Vec<Value> = target
        .topic_ids
        .iter()
        .map(|id| {
            array(&root["ipc"], "topics")?
                .iter()
                .find(|v| v["id"].as_u64() == Some(*id))
                .cloned()
                .ok_or(err())
        })
        .collect::<Result<_>>()?;
    if role == "strategy" {
        let expected: BTreeSet<u64> = array(strategy.ok_or(err())?, "topic_ids")?
            .iter()
            .map(|v| v.as_u64().ok_or(err()))
            .collect::<Result<_>>()?;
        if expected != target.topic_ids.iter().copied().collect() {
            return Err(err());
        }
    }
    if role == "md"
        && (topics.is_empty()
            || topics.iter().any(|t| {
                (t["publisher_process_id"] != process_id
                    && t["standby_publisher_process_id"] != process_id)
                    || !venues.iter().any(|v| v["product"] == t["product"])
            })
            || venues
                .iter()
                .any(|v| !topics.iter().any(|t| t["product"] == v["product"])))
    {
        return Err(err());
    }
    if matches!(role, "exec" | "guardian")
        && topics
            .iter()
            .any(|t| !venues.iter().any(|v| v["product"] == t["product"]))
    {
        return Err(err());
    }
    let mut out = Map::new();
    out.insert("identity".into(), root["identity"].clone());
    out.insert("process".into(), process.clone());
    out.insert("host".into(), host.clone());
    for key in ["journal", "local_config", "timeouts"] {
        out.insert(key.into(), root[key].clone());
    }
    out.insert(
        "observability".into(),
        view(&root["observability"], "ObservabilityView")?,
    );
    if let Some(strategy) = strategy.filter(|_| role == "strategy") {
        out.insert("strategy".into(), strategy.clone());
    }
    if !topics.is_empty() {
        let mut ipc = root["ipc"].clone();
        ipc["topics"] = topics.into();
        out.insert("ipc".into(), ipc);
    }
    out.insert(
        "account_views".into(),
        Value::Array(
            accounts
                .iter()
                .map(|a| view(a, "AccountView"))
                .collect::<Result<_>>()?,
        ),
    );
    out.insert(
        "venue_views".into(),
        Value::Array(
            venues
                .iter()
                .map(|v| view(v, "VenueView"))
                .collect::<Result<_>>()?,
        ),
    );
    let policy_ids = accounts
        .iter()
        .map(|a| text(a, "risk_policy_id"))
        .collect::<Result<BTreeSet<_>>>()?;
    out.insert(
        "risk_policies".into(),
        Value::Array(
            array(root, "risk_policies")?
                .iter()
                .filter(|p| p["id"].as_str().is_some_and(|id| policy_ids.contains(id)))
                .cloned()
                .collect(),
        ),
    );
    if matches!(role, "exec" | "guardian") {
        out.insert(
            "account_identity_refs".into(),
            Value::Array(
                accounts
                    .iter()
                    .map(|a| json!({"account_id":a["id"],"ref":a["account_identity_ref"]}))
                    .collect(),
            ),
        );
    }
    let mut bindings = Vec::new();
    for purpose in &target.secret_purposes {
        match purpose.as_str() {
            "execution" | "guardian" => {
                for account in &accounts {
                    let key = if purpose == "execution" {
                        "credential_ref"
                    } else {
                        "guardian_credential_ref"
                    };
                    add_secret(
                        &mut bindings,
                        purpose,
                        &account[key],
                        Some(text(account, "id")?),
                    );
                }
            }
            "market-data" => {
                for venue in &venues {
                    add_secret(&mut bindings, purpose, &venue["credential_ref"], None);
                }
            }
            "control" => {
                out.insert("control".into(), root["control"].clone());
                add_secret(
                    &mut bindings,
                    purpose,
                    &root["control"]["mtls_identity_ref"],
                    None,
                );
            }
            "replication" => {
                out.insert("replication".into(), root["replication"].clone());
                add_secret(
                    &mut bindings,
                    purpose,
                    &root["replication"]["mtls_identity_ref"],
                    None,
                );
            }
            "metrics" => add_secret(
                &mut bindings,
                purpose,
                &root["observability"]["metrics_identity_ref"],
                None,
            ),
            _ => return Err(err()),
        }
    }
    out.insert("secret_bindings".into(), bindings.into());
    let value = Value::Object(out);
    let allowed = array(&value, "secret_bindings")?
        .iter()
        .map(|v| text(v, "ref"))
        .collect::<Result<BTreeSet<_>>>()?;
    check_secret_references(&value, &allowed)?;
    static PAYLOAD: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
    let validator = PAYLOAD
        .get_or_init(|| {
            let mut doc = schema().clone();
            doc["$ref"] = "#/$defs/ProjectionPayload".into();
            doc.as_object_mut().ok_or(())?.remove("required");
            doc.as_object_mut().ok_or(())?.remove("properties");
            doc.as_object_mut()
                .ok_or(())?
                .remove("additionalProperties");
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .should_validate_formats(true)
                .build(&doc)
                .map_err(|_| ())
        })
        .as_ref()
        .map_err(|_| Error::Schema)?;
    if !validator.is_valid(&value) {
        return Err(Error::Schema);
    }
    let canonical = strict::canonical(&value)?;
    if canonical.len() > 4 * 1024 * 1024
        || canonical.len() as u64
            > root["ipc"]["config_projection_max_bytes"]
                .as_u64()
                .ok_or(Error::Schema)?
    {
        return Err(Error::Capacity);
    }
    let hash = Sha256::digest(&canonical).into();
    Ok(Projection {
        value,
        canonical,
        hash,
        global_hash: candidate.hash(),
        policy_revision: policy.revision(),
    })
}
