//! Account-scoped edits of an existing candidate. The result still requires
//! deployment validation and a committed control operation.
use crate::{
    Candidate, Error, Format, Hash, Limits, Result,
    changes::{Authority, ChangeSet, Role, scope},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    revision: u64,
    hash: Hash,
}
impl Head {
    pub fn new(revision: u64, candidate: &Candidate) -> Result<Self> {
        if !(1..1 << 63).contains(&revision) {
            return Err(Error::Revision);
        }
        Ok(Self {
            revision,
            hash: candidate.hash(),
        })
    }
    pub fn etag(&self) -> String {
        let hash: String = self.hash.iter().map(|b| format!("{b:02x}")).collect();
        format!("\"cfg-{}-{hash}\"", self.revision)
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
}
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    Add,
    Replace,
    Remove,
}
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Collection {
    Accounts,
    RiskPolicies,
    Strategies,
    StrategyRiskBudgets,
    Venues,
    Processes,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
    collection: Collection,
    id: String,
    account_id: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    op: Op,
    selector: Selector,
    path: String,
    #[serde(default, deserialize_with = "present")]
    value: Option<Value>,
}
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> core::result::Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    base_etag: String,
    changes: Vec<Change>,
}
#[derive(Debug)]
pub struct ScopedPatch {
    request: Request,
    canonical: Vec<u8>,
    hash: Hash,
}
impl ScopedPatch {
    pub fn parse(bytes: &[u8], limits: Limits) -> Result<Self> {
        let candidate = Candidate::parse(
            Format::Json,
            bytes,
            Limits {
                max_bytes: limits.max_bytes.min(4 * 1024 * 1024),
                max_depth: limits.max_depth,
            },
        )?;
        static SCHEMA: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
        let schema = SCHEMA
            .get_or_init(|| {
                let api: Value = serde_json::from_str(include_str!(
                    "../../../specs/contracts/control-api.openapi.json"
                ))
                .map_err(|_| ())?;
                let schemas = &api["components"]["schemas"];
                let schema = json!({
                    "$ref": "#/components/schemas/ScopedPatch",
                    "components": {"schemas": {
                        "ScopedPatch": schemas["ScopedPatch"],
                        "PatchChange": schemas["PatchChange"],
                        "PatchSelector": schemas["PatchSelector"],
                    }}
                });
                jsonschema::options()
                    .with_draft(jsonschema::Draft::Draft202012)
                    .build(&schema)
                    .map_err(|_| ())
            })
            .as_ref()
            .map_err(|_| Error::Schema)?;
        if !schema.is_valid(candidate.value()) {
            return Err(Error::Schema);
        }
        let request: Request =
            serde_json::from_value(candidate.value().clone()).map_err(|_| Error::Schema)?;
        for change in &request.changes {
            let s = &change.selector;
            if s.id.is_empty()
                || s.id.len() > 128
                || s.account_id
                    .as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 128)
                || (s.collection == Collection::StrategyRiskBudgets) != s.account_id.is_some()
                || change.path.len() > 2048
                || change.path.split('/').count() > 33
            {
                return Err(Error::Capacity);
            }
        }
        Ok(Self {
            request,
            canonical: candidate.canonical_bytes().to_vec(),
            hash: candidate.hash(),
        })
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn base_etag(&self) -> &str {
        &self.request.base_etag
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn apply(
        &self,
        head: &Head,
        if_match: &str,
        base: &Candidate,
        authority: &Authority,
    ) -> Result<Candidate> {
        base.validate_static()?;
        // Authorize every selector before resolving a selected object or
        // returning a missing-object error.
        for change in &self.request.changes {
            change.selector.authorize(base.value(), authority)?;
        }
        if head.hash != base.hash() || if_match != head.etag() || self.request.base_etag != if_match
        {
            return Err(Error::Revision);
        }
        let mut value = base.value().clone();
        for change in &self.request.changes {
            // A preceding edit may alter dependencies or replace/reorder a
            // budget array. Recompute authority and resolve by identity.
            change.selector.authorize(&value, authority)?;
            let mut parts = change.path.trim_start_matches('/').split('/').peekable();
            let first = parts.peek().copied().ok_or(Error::Schema)?;
            if first == change.selector.identity_field() {
                return Err(Error::Semantic("selector identity"));
            }
            let mut selected = change.selector.resolve(&mut value)?;
            while let Some(part) = parts.next() {
                let object = selected
                    .as_object_mut()
                    .ok_or(Error::Semantic("patch object path"))?;
                if parts.peek().is_some() {
                    selected = object
                        .get_mut(part)
                        .ok_or(Error::Semantic("patch property missing"))?;
                    continue;
                }
                match change.op {
                    Op::Add => {
                        if object.contains_key(part) {
                            return Err(Error::Semantic("patch property exists"));
                        }
                        object.insert(part.to_owned(), change.value.clone().ok_or(Error::Schema)?);
                    }
                    Op::Replace => {
                        let existing = object
                            .get_mut(part)
                            .ok_or(Error::Semantic("patch property missing"))?;
                        *existing = change.value.clone().ok_or(Error::Schema)?;
                    }
                    Op::Remove => {
                        object
                            .remove(part)
                            .ok_or(Error::Semantic("patch property missing"))?;
                    }
                }
                break;
            }
        }
        let candidate = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&value).map_err(|_| Error::Encoding)?,
            Limits::default(),
        )?;
        // This also checks every dependency introduced by the resulting
        // candidate, including accounts outside the initial selector scope.
        ChangeSet::between(base, &candidate)?.authorize_plan(authority)?;
        Ok(candidate)
    }
}

impl Selector {
    fn group(&self) -> &'static str {
        match self.collection {
            Collection::Accounts => "accounts",
            Collection::RiskPolicies => "risk_policies",
            Collection::Strategies | Collection::StrategyRiskBudgets => "strategies",
            Collection::Venues => "venues",
            Collection::Processes => "processes",
        }
    }
    fn identity_field(&self) -> &'static str {
        match self.collection {
            Collection::Strategies => "process_id",
            Collection::StrategyRiskBudgets => "account_id",
            _ => "id",
        }
    }
    fn authorize(&self, value: &Value, authority: &Authority) -> Result<()> {
        let accounts = match self.collection {
            Collection::Accounts => [self.id.clone()].into(),
            Collection::StrategyRiskBudgets => {
                [self.account_id.clone().ok_or(Error::Schema)?].into()
            }
            _ => scope(
                &[self.group().to_owned(), format!("@{}", self.id)],
                value,
                value,
            ),
        };
        let cluster = accounts.is_empty() || self.collection == Collection::Processes;
        if !authority.covers(&[Role::Editor], &accounts, cluster) {
            return Err(Error::Semantic("authorization"));
        }
        Ok(())
    }
    fn resolve<'a>(&self, value: &'a mut Value) -> Result<&'a mut Value> {
        let key = if self.group() == "strategies" {
            "process_id"
        } else {
            "id"
        };
        let row = value
            .get_mut(self.group())
            .and_then(Value::as_array_mut)
            .and_then(|rows| rows.iter_mut().find(|row| row[key] == self.id))
            .ok_or(Error::Semantic("patch selector missing"))?;
        if self.collection != Collection::StrategyRiskBudgets {
            return Ok(row);
        }
        let account = self.account_id.as_deref().ok_or(Error::Schema)?;
        if !row["account_ids"]
            .as_array()
            .is_some_and(|ids| ids.iter().any(|id| id == account))
        {
            return Err(Error::Semantic("patch selector missing"));
        }
        row.get_mut("risk_budgets")
            .and_then(Value::as_array_mut)
            .and_then(|rows| rows.iter_mut().find(|row| row["account_id"] == account))
            .ok_or(Error::Semantic("patch selector missing"))
    }
}
