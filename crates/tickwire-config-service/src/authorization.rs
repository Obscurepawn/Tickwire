//! RBAC data from a validated configuration, applied to a TLS-derived principal.
use crate::{
    Error, Hash, Result,
    auth::{AuthenticatedPrincipal, ServerTls, valid_subject},
};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate,
    changes::{Authority, Role, RoleGrant},
};

pub struct RoleBindings {
    config_hash: Hash,
    subjects: BTreeMap<String, Vec<Grant>>,
}
struct Grant {
    role: Role,
    accounts: BTreeSet<String>,
    cluster_scope: bool,
}
impl RoleBindings {
    /// This validates RBAC data. The service supplies its committed candidate;
    /// constructing this value does not prove that candidate was committed.
    pub fn from_candidate(candidate: &Candidate) -> Result<Self> {
        candidate.validate_static().map_err(Error::Configuration)?;
        let rows = candidate.value()["control"]["role_bindings"]
            .as_array()
            .ok_or(Error::Protocol)?;
        if rows.is_empty() || rows.len() > 4096 {
            return Err(Error::Bounds);
        }
        let mut subjects: BTreeMap<String, Vec<Grant>> = BTreeMap::new();
        for row in rows {
            let subject = row["subject"].as_str().ok_or(Error::Protocol)?;
            if !valid_subject(subject) {
                return Err(Error::Identity);
            }
            let role = match row["role"].as_str() {
                Some("viewer") => Role::Viewer,
                Some("config-editor") => Role::Editor,
                Some("config-operator") => Role::Operator,
                Some("risk-admin") => Role::RiskAdmin,
                Some("cluster-admin") => Role::ClusterAdmin,
                _ => return Err(Error::Protocol),
            };
            let accounts: BTreeSet<_> = row["account_ids"]
                .as_array()
                .ok_or(Error::Protocol)?
                .iter()
                .map(|a| a.as_str().map(str::to_owned).ok_or(Error::Protocol))
                .collect::<Result<_>>()?;
            let cluster_scope = row["cluster_scope"].as_bool().unwrap_or(false);
            if cluster_scope && !accounts.is_empty() {
                return Err(Error::Identity);
            }
            if cluster_scope || !accounts.is_empty() {
                subjects.entry(subject.to_owned()).or_default().push(Grant {
                    role,
                    accounts,
                    cluster_scope,
                });
            }
        }
        Ok(Self {
            config_hash: candidate.hash(),
            subjects,
        })
    }
    pub fn config_hash(&self) -> Hash {
        self.config_hash
    }
    pub fn authorize(
        &self,
        trust: &ServerTls,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Authority> {
        trust.revalidate(principal)?;
        let grants = self
            .subjects
            .get(principal.subject())
            .ok_or(Error::Authorization)?;
        Ok(Authority::from_grants(
            grants
                .iter()
                .map(|g| RoleGrant {
                    role: g.role,
                    accounts: g.accounts.clone(),
                    cluster_scope: g.cluster_scope,
                })
                .collect(),
        ))
    }
}
