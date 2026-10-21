use super::*;
use tickwire_control_raft::driver::PeerDriver;

fn check_schema(name: &str, body: &serde_json::Value) {
    let document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../specs/contracts/control-api.openapi.json"
    ))
    .unwrap();
    let system_schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../specs/contracts/system-config.schema.json"
    ))
    .unwrap();
    let registry = jsonschema::Registry::new()
        .add(
            "https://tickwire.invalid/contracts/system-config.schema.json",
            system_schema,
        )
        .unwrap()
        .prepare()
        .unwrap();
    let schema = serde_json::json!({
        "$ref":format!("#/components/schemas/{name}"),
        "components":document["components"],
    });
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .with_base_uri("https://tickwire.invalid/contracts/control-api.openapi.json")
        .with_registry(&registry)
        .offline()
        .build(&schema)
        .unwrap()
        .validate(body)
        .unwrap();
}

#[test]
fn desired_returns_only_actual_head_after_quorum_with_exact_content_and_etag() {
    let f = Fixture::new(&[SUBJECT], false);
    let candidate = api_candidate(SUBJECT, true);
    let expected = candidate.value().clone();
    let hash = format!(
        "sha256:{}",
        candidate
            .hash()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let mut cluster = ApiCluster::new(&candidate);
    let (status, body) = api_exchange(
        &f,
        "/v1/config/desired",
        None,
        move |trust, stream, request| {
            let before = cluster.nodes[0].scanned_through();
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(true);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            for _ in 0..6 {
                cluster.nodes[0].tick().unwrap();
            }
            cluster.pump(false);
            cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap()
                .write(stream)
                .unwrap();
            assert_eq!(cluster.nodes[0].scanned_through(), before);
        },
    );
    assert_eq!(status, 200);
    assert_eq!(body["config"], expected);
    assert_eq!(body["config_hash"], hash);
    assert_eq!(body["origin"], "bootstrap");
    assert_eq!(body["revision"], "1");
    assert!(
        body["control_commit_ref"]
            .as_str()
            .unwrap()
            .starts_with("raft:")
    );
    assert!(body.get("operation_id").is_none());
    assert!(body.get("committed_at").is_none());
    check_schema("Desired", &body);
}

#[test]
fn scoped_head_has_no_configuration_and_matches_the_bootstrap_contract() {
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, false));
    let (status, body) = api_exchange(
        &f,
        "/v1/config/head",
        None,
        move |trust, stream, request| {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(false);
            cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap()
                .write(stream)
                .unwrap();
        },
    );
    assert_eq!(status, 200);
    assert!(body.get("config").is_none());
    assert!(!body.to_string().contains("spot-main"));
    check_schema("Head", &body);
}

#[test]
fn desired_rechecks_full_scope_and_certificate_before_returning_any_content() {
    for (scope, revoked, expected) in [(false, false, 403), (true, true, 401)] {
        let f = Fixture::new(&[SUBJECT], false);
        let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, scope));
        let revoked_trust = ServerTls::new(f.material(true, false), vec![f.peer()]).unwrap();
        let (status, body) = api_exchange(
            &f,
            "/v1/config/desired",
            None,
            move |trust, stream, request| {
                let mut pending = cluster.nodes[0]
                    .control()
                    .begin_request(trust, stream.principal(), request)
                    .unwrap();
                cluster.pump(false);
                let trust = if revoked { &revoked_trust } else { trust };
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .unwrap()
                    .write(stream)
                    .unwrap();
            },
        );
        assert_eq!(status, expected);
        assert!(!body.to_string().contains("spot-main"));
    }
}

#[test]
fn actual_revision_absence_is_distinct_from_unimplemented_views() {
    for (path, status, code) in [
        ("/v1/config/revisions/1", 200, ""),
        ("/v1/config/revisions/2", 404, "NotFound"),
        ("/v1/config/effective", 503, "UnsupportedEndpoint"),
        (
            "/v1/config/plans/00000000-0000-4000-8000-000000000001",
            503,
            "UnsupportedEndpoint",
        ),
        (
            "/v1/config/operations/00000000-0000-4000-8000-000000000001",
            503,
            "UnsupportedEndpoint",
        ),
    ] {
        let f = Fixture::new(&[SUBJECT], false);
        let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
        let (actual, body) = api_exchange(&f, path, None, move |trust, stream, request| {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(false);
            cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap()
                .write(stream)
                .unwrap();
        });
        assert_eq!(actual, status, "{path}");
        if status == 200 {
            check_schema("Desired", &body);
        } else {
            assert_eq!(body["code"], code, "{path}");
        }
    }
}
