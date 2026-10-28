use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
    changes::{Authority, Role, RoleGrant},
    patch::Head,
    prepared::{Input, PreparedCandidate},
};

pub(super) fn parse(v: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .normalized_configuration(Limits::default())
    .unwrap()
}
pub(super) fn base() -> Value {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone()
}
pub(super) fn policy(v: &Value) -> Value {
    let targets: Vec<_> = v["processes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            let id = p["id"].as_str().unwrap();
            let role = p["role"].as_str().unwrap();
            let strategy = v["strategies"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["process_id"] == id);
            let accounts: Vec<_> = v["accounts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|a| match role {
                    "strategy" => strategy.unwrap()["account_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&a["id"]),
                    "exec" => a["exec_process_id"] == id || a["standby_exec_process_id"] == id,
                    "guardian" => a["guardian_process_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&p["id"]),
                    _ => false,
                })
                .collect();
            let topics: Vec<_> = v["ipc"]["topics"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|t| match role {
                    "strategy" => strategy.unwrap()["topic_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&t["id"]),
                    "md" => {
                        t["publisher_process_id"] == id || t["standby_publisher_process_id"] == id
                    }
                    _ => false,
                })
                .collect();
            let venues: Vec<_> = v["venues"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|venue| {
                    accounts.iter().any(|a| a["venue_id"] == venue["id"])
                        || topics.iter().any(|t| t["product"] == venue["product"])
                })
                .map(|v| v["id"].clone())
                .collect();
            let purposes: &[&str] = match role {
                "supervisor" => &["control"],
                "replicator" => &["replication"],
                "exec" => &["execution"],
                "guardian" => &["guardian"],
                "md" => &["market-data"],
                _ => &[],
            };
            json!({
                "process_id":id,
                "account_ids":accounts.iter().map(|a|a["id"].clone()).collect::<Vec<_>>(),
                "venue_ids":venues,
                "topic_ids":topics.iter().map(|t|t["id"].clone()).collect::<Vec<_>>(),
                "secret_purposes":purposes
            })
        })
        .collect();
    json!({"schema":1,"revision":1,"targets":targets})
}
pub(super) fn catalog() -> ArtifactCatalog {
    ArtifactCatalog::new(CatalogLimits::default()).unwrap()
}
pub(super) fn pinned(mut value: Value, policy: Value, catalog: &mut ArtifactCatalog) -> Candidate {
    let bytes = serde_json::to_vec(&policy).unwrap();
    let hash: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let reference = format!("artifact://fixture/config/projection-v1@sha256:{hash}");
    catalog.insert(&reference, &bytes).unwrap();
    value["local_config"]["projection_policy_ref"] = reference.into();
    value["local_config"]["projection_policy_revision"] = policy["revision"].clone();
    parse(&value)
}
pub(super) fn prepared(base: &Candidate, next: &Candidate) -> PreparedCandidate {
    let head = Head::new(1, base).unwrap();
    let authority = Authority::from_grants(vec![RoleGrant {
        role: Role::Editor,
        accounts: Default::default(),
        cluster_scope: true,
    }]);
    PreparedCandidate::prepare(
        &head,
        &head.etag(),
        base,
        Input::Full(next),
        &authority,
        Limits::default(),
    )
    .unwrap()
}
