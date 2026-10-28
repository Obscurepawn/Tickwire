//! Static dependency closure, evaluated on both old and new projections/configurations.
use crate::{Candidate, changes::Change, projection::Projection};
use serde_json::Value;
pub(super) fn rows<'a>(v: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    v[key].as_array().into_iter().flatten()
}
fn has(v: &Value, group: &str, id: Option<&str>) -> bool {
    rows(v, group).any(|v| {
        id.is_none_or(|id| {
            v["id"].as_str() == Some(id) || v["id"].as_u64().is_some_and(|n| n.to_string() == id)
        })
    })
}
fn matches(v: &Value, id: Option<&str>, key: &str) -> bool {
    !v.is_null() && id.is_none_or(|id| v[key].as_str() == Some(id))
}
pub(super) fn affects(
    change: &Change,
    old: Option<&Projection>,
    new: Option<&Projection>,
    base: &Candidate,
    next: &Candidate,
) -> bool {
    [old, new].into_iter().flatten().any(|p| {
        [base, next]
            .into_iter()
            .any(|root| affected(change, p.value(), root.value()))
    })
}
fn affected(change: &Change, p: &Value, root: &Value) -> bool {
    let path = change.parts();
    let group = path.first().map(String::as_str).unwrap_or("");
    let id = path.get(1).and_then(|s| s.strip_prefix('@'));
    match group {
        "identity" | "control" | "replication" | "qualification" => true,
        "hosts" => matches(&p["host"], id, "id"),
        "processes" => process_dependency(p, root, id),
        "strategies" => strategy_dependency(change, p, root, id),
        "accounts" => has(p, "account_views", id),
        "venues" => has(p, "venue_views", id),
        "risk_policies" => has(p, "risk_policies", id),
        "ipc" => {
            if path.get(1).is_some_and(|s| s == "topics") {
                has(
                    &p["ipc"],
                    "topics",
                    path.get(2).and_then(|s| s.strip_prefix('@')),
                )
            } else {
                true
            }
        }
        "journal" | "local_config" | "timeouts" => true,
        "observability" => path.get(1).is_none_or(|field| {
            p["observability"].get(field).is_some()
                || field == "metrics_identity_ref"
                    && rows(p, "secret_bindings").any(|s| s["purpose"] == "metrics")
        }),
        _ => false,
    }
}
fn strategy_dependency(change: &Change, p: &Value, root: &Value, id: Option<&str>) -> bool {
    if matches(&p["strategy"], id, "process_id") {
        return true;
    }
    let path = change.parts();
    let accounts = path.len() <= 2
        || path
            .get(2)
            .is_some_and(|s| matches!(s.as_str(), "risk_budgets" | "account_ids" | "topic_ids"));
    if accounts
        && rows(p, "account_views").any(|a| {
            a["id"]
                .as_str()
                .is_some_and(|id| change.accounts().contains(id))
        })
    {
        return true;
    }
    let topics = path.len() <= 2 || path.get(2).is_some_and(|s| s == "topic_ids");
    topics
        && rows(root, "strategies")
            .filter(|s| matches(s, id, "process_id"))
            .any(|s| {
                rows(&p["ipc"], "topics").any(|t| {
                    s["topic_ids"]
                        .as_array()
                        .is_some_and(|ids| ids.contains(&t["id"]))
                })
            })
}
fn process_dependency(p: &Value, root: &Value, id: Option<&str>) -> bool {
    if matches(&p["process"], id, "id") {
        return true;
    }
    rows(root, "processes")
        .filter(|v| matches(v, id, "id"))
        .any(|process| {
            if p["process"]["role"] == "supervisor" && process["host_id"] == p["process"]["host_id"]
            {
                return true;
            }
            let id = &process["id"];
            rows(root, "accounts").any(|a| {
                let owns = a["exec_process_id"] == *id
                    || a["standby_exec_process_id"] == *id
                    || a["guardian_process_ids"]
                        .as_array()
                        .is_some_and(|v| v.contains(id));
                owns && (has(p, "account_views", a["id"].as_str())
                    || p["process"]["role"] == "replicator")
            }) || rows(&root["ipc"], "topics").any(|t| {
                (t["publisher_process_id"] == *id || t["standby_publisher_process_id"] == *id)
                    && rows(&p["ipc"], "topics").any(|own| own["id"] == t["id"])
            })
        })
}
