use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
    capacity::{CapacityAssessment, RetainedResources},
};

fn parse(v: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn base() -> Value {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone()
}
fn inventory(v: &Value) -> Value {
    let hosts: Vec<_> = v["hosts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            let count = h["physical_cores"].as_u64().unwrap();
            let cpus: Vec<_> = (0..count)
                .map(|cpu| {
                    let node = v["processes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|p| {
                            p["host_id"] == h["id"]
                                && p["cpus"].as_array().unwrap().contains(&cpu.into())
                        })
                        .map(|p| p["numa_node"].as_u64().unwrap())
                        .unwrap_or(if cpu == count - 1 {
                            h["numa_nodes"].as_u64().unwrap() - 1
                        } else {
                            0
                        });
                    json!({"cpu_id":cpu,"package_id":0,"core_id":cpu,"numa_node":node})
                })
                .collect();
            json!({
                "id":h["id"],"failure_domain":h["failure_domain"],
                "memory_bytes":h["memory_bytes"].as_u64().unwrap()+1073741824u64,
                "system_reserved_bytes":1073741824u64,
                "shm_capacity_bytes":h["shm_budget_bytes"],"cpus":cpus
            })
        })
        .collect();
    json!({"schema":1,"cluster_id":v["identity"]["cluster_id"],"namespace_id":v["identity"]["namespace_id"],"hosts":hosts})
}
fn retained_value(v: &Value) -> Value {
    json!({"schema":1,"cluster_id":v["identity"]["cluster_id"],"namespace_id":v["identity"]["namespace_id"],"revision":"17","allocations":[]})
}
fn retained(v: &Value) -> RetainedResources {
    RetainedResources::decode(&serde_json::to_vec(v).unwrap()).unwrap()
}
fn pinned(mut v: Value, inventory: &Value) -> (Candidate, ArtifactCatalog, [u8; 32]) {
    let bytes = serde_json::to_vec(inventory).unwrap();
    let hash: [u8; 32] = Sha256::digest(&bytes).into();
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    let reference = format!("artifact://fixture/deployment/hosts@sha256:{hex}");
    let mut catalog = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
    catalog.insert(&reference, &bytes).unwrap();
    v["qualification"]["host_inventory_ref"] = reference.into();
    (parse(&v), catalog, hash)
}
fn allocation(shared: u64, private: u64, cpus: Value) -> Value {
    json!({"resource_id":"retained-previous","host_id":"host-a","shared_bytes":shared,"private_bytes":private,"cpus":cpus})
}

#[test]
fn sparse_os_cpu_ids_use_actual_numa_and_physical_core_mapping() {
    let mut v = base();
    let mut i = inventory(&v);
    for host in v["hosts"].as_array_mut().unwrap() {
        for cpu in host["housekeeping_cpus"].as_array_mut().unwrap() {
            *cpu = (cpu.as_u64().unwrap() + 1024).into();
        }
    }
    for process in v["processes"].as_array_mut().unwrap() {
        for cpu in process["cpus"].as_array_mut().unwrap() {
            *cpu = (cpu.as_u64().unwrap() + 1024).into();
        }
    }
    for host in i["hosts"].as_array_mut().unwrap() {
        for cpu in host["cpus"].as_array_mut().unwrap() {
            cpu["cpu_id"] = (cpu["cpu_id"].as_u64().unwrap() + 1024).into();
        }
    }
    // Sparse OS IDs are legal even before the deployment inventory is loaded.
    assert!(parse(&v).validate_static().is_ok());
    let r = retained(&retained_value(&v));
    let (c, catalog, _) = pinned(v, &i);
    assert!(CapacityAssessment::check(&c, &catalog, &r).is_ok());
}
#[test]
fn sparse_numa_ids_are_checked_by_membership_in_the_actual_inventory() {
    let mut v = base();
    let mut i = inventory(&v);
    for process in v["processes"].as_array_mut().unwrap() {
        process["numa_node"] = (process["numa_node"].as_u64().unwrap() + 8).into();
    }
    for host in i["hosts"].as_array_mut().unwrap() {
        for cpu in host["cpus"].as_array_mut().unwrap() {
            cpu["numa_node"] = (cpu["numa_node"].as_u64().unwrap() + 8).into();
        }
    }
    assert!(parse(&v).validate_static().is_ok());
    let r = retained(&retained_value(&v));
    let (c, catalog, _) = pinned(v, &i);
    assert!(CapacityAssessment::check(&c, &catalog, &r).is_ok());
}
#[test]
fn capacity_binds_candidate_inventory_and_retained_bytes() {
    let v = base();
    let r = retained(&retained_value(&v));
    let (c, catalog, inventory_hash) = pinned(v.clone(), &inventory(&v));
    let a = CapacityAssessment::check(&c, &catalog, &r).unwrap();
    assert_eq!(a.candidate_hash(), c.hash());
    assert_eq!(a.inventory_hash(), inventory_hash);
    assert_eq!(a.retained_hash(), r.hash());
    assert_ne!(a.retained_hash(), [0; 32]);
    assert_eq!(a.hosts().len(), 3);
    assert_eq!(a.hosts()["host-a"].shared_bytes, 6_449_856_512);
    assert_eq!(a.hosts()["ctl-c"].shared_bytes, 0);
    let private: u64 = v["processes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["host_id"] == "host-a")
        .map(|p| p["memory_limit_bytes"].as_u64().unwrap())
        .sum();
    assert_eq!(a.hosts()["host-a"].private_bytes, private);
    assert_eq!(
        a.hosts()["host-a"].memory_commitment_bytes,
        private + 8_589_934_592
    );
}
#[test]
fn missing_extra_wrong_identity_and_duplicate_inventory_are_rejected() {
    let v = base();
    let r = retained(&retained_value(&v));
    for kind in 0..8 {
        let mut i = inventory(&v);
        match kind {
            0 => {
                i["hosts"].as_array_mut().unwrap().pop();
            }
            1 => {
                let mut extra = i["hosts"][0].clone();
                extra["id"] = "unknown-host".into();
                i["hosts"].as_array_mut().unwrap().push(extra);
            }
            2 => i["cluster_id"] = "another-cluster".into(),
            3 => i["hosts"][0]["failure_domain"] = "different-zone".into(),
            4 => i["hosts"][0]["cpus"][8]["cpu_id"] = 0.into(),
            5 => i["hosts"][0]["unexpected"] = true.into(),
            6 => i["hosts"][0]["cpus"]
                .as_array_mut()
                .unwrap()
                .retain(|c| c["cpu_id"] != 8),
            _ => i["namespace_id"] = "00000000-0000-4000-8000-000000000002".into(),
        }
        let (c, catalog, _) = pinned(v.clone(), &i);
        assert!(
            CapacityAssessment::check(&c, &catalog, &r).is_err(),
            "kind {kind}"
        );
    }
    let (c, _, _) = pinned(v.clone(), &inventory(&v));
    let empty = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
    assert!(CapacityAssessment::check(&c, &empty, &r).is_err());
}
#[test]
fn numa_mismatch_smt_hot_loop_and_housekeeping_conflicts_are_rejected() {
    let v = base();
    let r = retained(&retained_value(&v));
    for kind in 0..3 {
        let mut i = inventory(&v);
        match kind {
            0 => i["hosts"][0]["cpus"][8]["numa_node"] = 1.into(),
            1 => i["hosts"][0]["cpus"][9]["core_id"] = 8.into(),
            _ => i["hosts"][0]["cpus"][8]["core_id"] = 0.into(),
        }
        // Keep the declared physical-core count available so the collision
        // itself, rather than a smaller host, explains this failure.
        i["hosts"][0]["cpus"]
            .as_array_mut()
            .unwrap()
            .push(json!({"cpu_id":2000,"package_id":0,"core_id":2000,"numa_node":0}));
        let (c, catalog, _) = pinned(v.clone(), &i);
        assert!(
            CapacityAssessment::check(&c, &catalog, &r).is_err(),
            "kind {kind}"
        );
    }
}
#[test]
fn extra_retained_shared_private_and_cpu_resources_cannot_disappear() {
    let v = base();
    let (c, catalog, _) = pinned(v.clone(), &inventory(&v));
    let shared_remaining = 8_589_934_592 - 6_449_856_512;
    let mut r = retained_value(&v);
    r["allocations"] = json!([allocation(shared_remaining, 0, json!([]))]);
    let exact = CapacityAssessment::check(&c, &catalog, &retained(&r)).unwrap();
    assert_eq!(exact.hosts()["host-a"].shared_bytes, 8_589_934_592);
    r["allocations"][0]["shared_bytes"] = (shared_remaining + 1).into();
    assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_err());
    let remaining = v["hosts"][0]["memory_bytes"].as_u64().unwrap()
        - exact.hosts()["host-a"].memory_commitment_bytes;
    r["allocations"] = json!([allocation(0, remaining, json!([]))]);
    assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_ok());
    r["allocations"][0]["private_bytes"] = (remaining + 1).into();
    assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_err());
    for cpus in [json!([8]), json!([0]), json!([4095])] {
        r["allocations"] = json!([allocation(0, 0, cpus)]);
        assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_err());
    }
    r["allocations"] = json!([allocation(0, 0, json!([62]))]);
    assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_ok());
}
#[test]
fn actual_memory_reserve_and_shared_capacity_constrain_configured_budgets() {
    let v = base();
    let r = retained(&retained_value(&v));
    for kind in 0..3 {
        let mut i = inventory(&v);
        match kind {
            0 => i["hosts"][0]["memory_bytes"] = v["hosts"][0]["memory_bytes"].clone(),
            1 => i["hosts"][0]["shm_capacity_bytes"] = 1.into(),
            _ => i["hosts"][0]["system_reserved_bytes"] = i["hosts"][0]["memory_bytes"].clone(),
        }
        let (c, catalog, _) = pinned(v.clone(), &i);
        assert!(
            CapacityAssessment::check(&c, &catalog, &r).is_err(),
            "kind {kind}"
        );
    }
}
#[test]
fn retained_snapshot_rejects_ambiguous_identity_and_unbounded_shapes() {
    let v = base();
    for kind in 0..7 {
        let mut r = retained_value(&v);
        r["allocations"] = json!([allocation(1, 1, json!([62]))]);
        match kind {
            0 => {
                r["allocations"]
                    .as_array_mut()
                    .unwrap()
                    .push(allocation(1, 1, json!([61])));
            }
            1 => r["allocations"][0]["cpus"] = json!([62, 62]),
            2 => r["revision"] = "9223372036854775808".into(),
            3 => r["revision"] = "017".into(),
            4 => r["allocations"][0]["resource_id"] = "../bad".into(),
            5 => r["unexpected"] = true.into(),
            _ => r["allocations"] = Value::Array(vec![allocation(0, 0, json!([])); 4097]),
        }
        assert!(
            RetainedResources::decode(&serde_json::to_vec(&r).unwrap()).is_err(),
            "kind {kind}"
        );
    }
    let duplicate = br#"{"schema":1,"schema":1,"cluster_id":"c","namespace_id":"n","revision":"1","allocations":[]}"#;
    assert!(RetainedResources::decode(duplicate).is_err());
    let (c, catalog, _) = pinned(v.clone(), &inventory(&v));
    for kind in 0..3 {
        let mut r = retained_value(&v);
        match kind {
            0 => r["cluster_id"] = "another-cluster".into(),
            1 => r["namespace_id"] = "00000000-0000-4000-8000-000000000002".into(),
            _ => {
                r["allocations"] = json!([allocation(0, 0, json!([]))]);
                r["allocations"][0]["host_id"] = "unknown-host".into();
            }
        }
        assert!(CapacityAssessment::check(&c, &catalog, &retained(&r)).is_err());
    }
}
