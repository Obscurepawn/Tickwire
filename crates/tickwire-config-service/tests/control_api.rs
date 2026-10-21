use serde_json::json;
use tickwire_config_service::api::*;
const ID: &str = "01234567-89ab-4cde-8fab-0123456789ab";
const ETAG: &str = "\"cfg-9-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"";
fn request(method: &str, path: &str, headers: &str, body: &str) -> Vec<u8> {
    format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
    .into_bytes()
}
fn post(path: &str, body: &str, persistent: bool) -> Vec<u8> {
    let mut headers = format!("Content-Type: application/json\r\nIf-Match: {ETAG}\r\n");
    if persistent {
        headers += &format!("Idempotency-Key: {ID}\r\n");
    }
    request("POST", path, &headers, body)
}
fn plan_body() -> String {
    json!({
        "candidate":{"format":"toml","document":"[identity]\nmode='test'"},
        "reason":"test",
        "apply_ceiling":"Quiesce",
        "exposure_policy":"reject_if_exceeded"
    })
    .to_string()
}
#[test]
fn every_declared_get_route_has_an_exact_bounded_identity() {
    for (path, expected) in [
        ("/v1/config/head".into(), Endpoint::Head),
        ("/v1/config/desired".into(), Endpoint::Desired),
        ("/v1/config/schema".into(), Endpoint::Schema),
        ("/v1/config/watch".into(), Endpoint::Watch),
        ("/v1/config/effective".into(), Endpoint::Effective(None)),
        (
            "/v1/config/effective?process_id=exec-a".into(),
            Endpoint::Effective(Some("exec-a".into())),
        ),
        ("/v1/config/revisions/9".into(), Endpoint::Revision(9)),
        (format!("/v1/config/plans/{ID}"), Endpoint::Plan(ID.into())),
        (
            format!("/v1/config/plans/{ID}/diff"),
            Endpoint::PlanDiff(ID.into()),
        ),
        (
            format!("/v1/config/operations/{ID}"),
            Endpoint::Operation(ID.into()),
        ),
        (
            format!("/v1/config/operations?idempotency_key={ID}"),
            Endpoint::OperationByKey(ID.into()),
        ),
    ] {
        let got = Request::parse(&request("GET", &path, "", ""), 4096).unwrap();
        assert_eq!(got.endpoint(), &expected);
        assert!(got.body().is_none());
    }
}
#[test]
fn fixed_write_schemas_and_required_concurrency_headers_are_enforced() {
    let plan = plan_body();
    for (path, body, persistent, endpoint) in [
        ("/v1/config/preview".into(), plan.clone(),false,Endpoint::Preview),
        ("/v1/config/plans".into(), plan,true,Endpoint::Plans),
        ("/v1/config/commits".into(),json!({"plan_id":ID,"candidate_hash":format!("sha256:{}","ab".repeat(32)),"accepted_actions":[],"reason":"test"}).to_string(),true,Endpoint::Commit),
        ("/v1/config/rollback-plans".into(),json!({"restore_revision":"3","reason":"test","apply_ceiling":"Quiesce","exposure_policy":"reject_if_exceeded"}).to_string(),true,Endpoint::RollbackPlan),
        (format!("/v1/config/operations/{ID}/resume"),json!({"reason":"test"}).to_string(),true,Endpoint::Resume(ID.into())),
    ] {
        let got = Request::parse(&post(&path,&body,persistent),4096).unwrap();
        assert_eq!(got.endpoint(),&endpoint);
        assert_eq!(got.if_match(),Some(ETAG));
        assert_eq!(got.idempotency_key(),persistent.then_some(ID));
    }
    let validation =
        json!({"candidate":{"format":"toml","document":"[identity]\nmode='test'"},"validation_level":"structural"})
            .to_string();
    let bytes = request(
        "POST",
        "/v1/config/validate",
        "Content-Type: application/json\r\n",
        &validation,
    );
    assert_eq!(
        Request::parse(&bytes, 4096).unwrap().endpoint(),
        &Endpoint::Validate
    );
}
#[test]
fn request_identity_uses_canonical_json_and_binds_the_endpoint() {
    let body = plan_body();
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    let pretty = serde_json::to_string_pretty(&value).unwrap();
    let a = Request::parse(&post("/v1/config/plans", &body, true), 4096).unwrap();
    let b = Request::parse(&post("/v1/config/plans", &pretty, true), 4096).unwrap();
    let preview = Request::parse(&post("/v1/config/preview", &body, false), 4096).unwrap();
    assert_eq!(a.digest(), b.digest());
    assert_ne!(a.digest(), preview.digest());
}
#[test]
fn duplicated_headers_transfer_encoding_and_unsupported_body_encodings_fail() {
    for headers in [
        "Content-Length: 0\r\n",
        "Transfer-Encoding: chunked\r\n",
        "Content-Encoding: gzip\r\n",
        "Host: other\r\n",
        " host: folded\r\n",
    ] {
        assert!(Request::parse(&request("GET", "/v1/config/head", headers, ""), 4096).is_err());
    }
    let bad = post("/v1/config/plans", &plan_body(), true);
    let text = String::from_utf8(bad).unwrap();
    for text in [
        text.replace(&format!("If-Match: {ETAG}\r\n"), ""),
        text.replace(ETAG, "*"),
        text.replace(ETAG, &format!("W/{ETAG}")),
        text.replace(&format!("Idempotency-Key: {ID}\r\n"), ""),
        text.replace(
            &format!("Idempotency-Key: {ID}"),
            "Idempotency-Key: not-a-uuid",
        ),
    ] {
        assert!(Request::parse(text.as_bytes(), 4096).is_err());
    }
}
#[test]
fn unknown_parameters_escaped_paths_invalid_revisions_and_extra_bytes_fail() {
    for path in [
        "/v1/config/head?subject=admin",
        "/v1/config/effective?process_id=a&process_id=b",
        "/v1/config/operations",
        "/v1/config/revisions/09",
        "/v1/config/revisions/0",
        "/v1/config/revisions/9223372036854775808",
        "/v1/config/%68ead",
        "/v1/config/../head",
        "/v1/config/head/",
    ] {
        assert!(Request::parse(&request("GET", path, "", ""), 4096).is_err());
    }
    let mut bytes = request("GET", "/v1/config/head", "", "");
    bytes.extend_from_slice(b"GET /v1/config/head HTTP/1.1\r\n\r\n");
    assert!(Request::parse(&bytes, 4096).is_err());
}
#[test]
fn schema_strict_json_and_size_limits_apply_before_any_business_action() {
    let body = plan_body();
    let mut value: serde_json::Value = serde_json::from_str(&body).unwrap();
    value["principal"] = "admin".into();
    for body in [
        serde_json::to_string(&value).unwrap(),
        body.replace(
            "\"reason\":\"test\"",
            "\"reason\":\"test\",\"reason\":\"other\"",
        ),
        body.replace("\"reason\":\"test\"", "\"reason\":1.5"),
    ] {
        assert!(Request::parse(&post("/v1/config/plans", &body, true), 4096).is_err());
    }
    assert!(Request::parse(&post("/v1/config/plans", &plan_body(), true), 16).is_err());
    assert!(
        Request::parse(
            &request(
                "GET",
                "/v1/config/head",
                &format!("X-Fill: {}\r\n", "a".repeat(MAX_HEADER_BYTES)),
                ""
            ),
            4096
        )
        .is_err()
    );
    assert!(Request::parse(&request("GET", "/v1/config/head", "", ""), 0).is_err());
}
#[test]
fn embedded_json_candidate_uses_the_pinned_external_system_schema() {
    let base = tickwire_config::Candidate::parse(
        tickwire_config::Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        tickwire_config::Limits::default(),
    )
    .unwrap();
    let mut body = json!({
        "candidate": {"format":"json", "document":base.value()},
        "validation_level":"structural"
    });
    let parse = |body: &serde_json::Value| {
        Request::parse(
            &request(
                "POST",
                "/v1/config/validate",
                "Content-Type: application/json\r\n",
                &body.to_string(),
            ),
            MAX_BODY_BYTES,
        )
    };
    assert_eq!(parse(&body).unwrap().endpoint(), &Endpoint::Validate);
    body["candidate"]["document"]["identity"]["extra_unknown_field"] = true.into();
    assert_eq!(parse(&body).unwrap_err().code, Code::Schema);
    body["candidate"]["document"] = json!({});
    assert_eq!(parse(&body).unwrap_err().code, Code::Schema);
}
