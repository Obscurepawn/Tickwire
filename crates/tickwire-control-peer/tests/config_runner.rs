use serde_json::{Value, json};
use tickwire_control_peer::config_runner::{Configuration, EvidenceTrust};
fn config() -> Value {
    json!({"schema":1,"peer_config_file":"/private/peer.json","http":{"listen":"127.0.0.1:9443","identity":{"chain_der":["/private/http.der"],"private_key_der":"/private/http.pk8","roots_der":["/private/clients-ca.der"],"crls_der":["/private/clients.crl"]},"clients":[{"subject":"spiffe://tickwire.test/control/editor","leaf_sha256":vec![3;32]}],"max_connections":8,"max_body_bytes":4194304,"timeout_ms":5000},"deployment":{"bundle_file":"/private/sources.bin","evidence_trust_file":"/private/evidence.json","max_bundle_bytes":67108864,"max_jobs":2,"account_max_age_ms":5000,"credential_max_age_ms":5000,"resource_max_age_ms":5000},"stop_file":"/private/STOP"})
}
fn trust() -> Value {
    json!({"schema":1,"cluster_id":"cluster-a","namespace_id":"11111111-2222-3333-4444-555555555555","revision":1,"issuers":[{"id":"observer","public_key":vec![7;32],"kinds":["host_inventory","account_mode"]}]})
}
#[test]
fn explicit_independent_https_sources_and_issuer_configuration_parse() {
    let c = Configuration::parse(&serde_json::to_vec(&config()).unwrap()).unwrap();
    assert_eq!(c.http.timeout_ms, 5000);
    assert_ne!(
        EvidenceTrust::parse(&serde_json::to_vec(&trust()).unwrap())
            .unwrap()
            .digest(),
        [0; 32]
    );
}
#[test]
fn invalid_limits_paths_duplicate_authorities_and_schema_are_rejected() {
    for n in 0..7 {
        let mut c = config();
        match n {
            0 => c["schema"] = 2.into(),
            1 => c["peer_config_file"] = "relative.json".into(),
            2 => c["http"]["timeout_ms"] = 0.into(),
            3 => c["deployment"]["max_jobs"] = 9.into(),
            4 => c["deployment"]["account_max_age_ms"] = 60001.into(),
            5 => {
                let p = c["http"]["clients"][0].clone();
                c["http"]["clients"].as_array_mut().unwrap().push(p);
            }
            _ => c["http"]["unknown"] = true.into(),
        };
        assert!(
            Configuration::parse(&serde_json::to_vec(&c).unwrap()).is_err(),
            "case {n}"
        );
    }
    for n in 0..4 {
        let mut t = trust();
        match n {
            0 => t["schema"] = 2.into(),
            1 => t["revision"] = 0.into(),
            2 => {
                let k = t["issuers"][0]["kinds"][0].clone();
                t["issuers"][0]["kinds"].as_array_mut().unwrap().push(k);
            }
            _ => t["issuer_from_bundle"] = true.into(),
        };
        assert!(EvidenceTrust::parse(&serde_json::to_vec(&t).unwrap()).is_err());
    }
}
