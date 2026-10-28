use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{Candidate, Error, Hash, artifacts::validate_parameter_document};

fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
fn validate(schema: &[u8], parameters: &[u8]) -> tickwire_config::Result<Candidate> {
    validate_parameter_document(schema, hash(schema), parameters)
}
fn schema() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object", "additionalProperties":false,
        "required":["observations", "minimum_bid"],
        "$defs":{"count":{"type":"integer", "minimum":1, "maximum":4}},
        "properties":{
            "observations":{"$ref":"#/$defs/count"},
            "minimum_bid":{"type":"string", "pattern":"^[0-9]+[.][0-9]{2}$"}
        }
    }))
    .unwrap()
}
#[test]
fn returns_canonical_values_without_rewriting_factory_bytes() {
    let schema = schema();
    let raw = br#" { "observations" : 2, "minimum_bid" : "10.25" } "#.to_vec();
    let before = raw.clone();
    let first = validate(&schema, &raw).unwrap();
    let second = validate(&schema, br#"{"minimum_bid":"10.25","observations":2}"#).unwrap();
    assert_eq!(raw, before);
    assert_eq!(first, second);
    assert_eq!(
        first.canonical_bytes(),
        br#"{"minimum_bid":"10.25","observations":2}"#
    );
    assert_eq!(first.value()["observations"], 2);
    assert_ne!(
        first,
        validate(&schema, br#"{"minimum_bid":"10.25","observations":3}"#).unwrap()
    );
}
#[test]
fn schema_identity_is_the_digest_of_exact_raw_bytes() {
    let raw = b" true \n";
    let parameters = br#"{"count":1}"#;
    validate(raw, parameters).unwrap();
    assert!(validate_parameter_document(raw, hash(b"true"), parameters).is_err());
    assert!(validate_parameter_document(raw, [0; 32], parameters).is_err());
    assert!(validate_parameter_document(b"false", hash(raw), parameters).is_err());
}
#[test]
fn schema_constraints_and_local_references_are_enforced() {
    let schema = schema();
    for input in [
        br#"{"minimum_bid":"10.25","observations":0}"#.as_slice(),
        br#"{"minimum_bid":"10.25","observations":5}"#,
        br#"{"minimum_bid":"10.25","observations":"2"}"#,
        br#"{"minimum_bid":10,"observations":2}"#,
        br#"{"minimum_bid":"not-a-price","observations":2}"#,
        br#"{"minimum_bid":"10.25"}"#,
        br#"{"minimum_bid":"10.25","observations":2,"extra":true}"#,
    ] {
        assert!(validate(&schema, input).is_err(), "{input:?}");
    }
    assert!(validate(b"false", br#"{"x":1}"#).is_err());
}
#[test]
fn parameters_require_one_to_256_top_level_properties() {
    for input in [
        b"{}".as_slice(),
        b"[]",
        b"null",
        b"true",
        b"1",
        br#""text""#,
    ] {
        assert!(validate(b"true", input).is_err());
    }
    for count in [1, 256, 257] {
        let object: serde_json::Map<String, Value> =
            (0..count).map(|n| (format!("key{n}"), json!(n))).collect();
        let bytes = serde_json::to_vec(&object).unwrap();
        assert_eq!(validate(b"true", &bytes).is_ok(), count <= 256);
    }
}
#[test]
fn duplicate_keys_are_rejected_in_parameters_and_schema() {
    for input in [
        br#"{"x":1,"x":1}"#.as_slice(),
        br#"{"x":1,"\u0078":2}"#,
        br#"{"x":{"v":1,"v":2}}"#,
    ] {
        assert_eq!(validate(b"true", input), Err(Error::Duplicate));
    }
    for schema in [
        br#"{"type":"object","type":"object"}"#.as_slice(),
        br#"{"$defs":{"x":true,"\u0078":false}}"#,
    ] {
        assert_eq!(validate(schema, br#"{"x":1}"#), Err(Error::Duplicate));
    }
}
#[test]
fn unsafe_numbers_trailing_input_and_invalid_encoding_are_rejected() {
    for raw in [
        br#"{"x":1.0}"#.as_slice(),
        br#"{"x":1e0}"#,
        br#"{"x":-0.0}"#,
        br#"{"x":9007199254740992}"#,
        br#"{"x":-9007199254740992}"#,
        br#"{"x":18446744073709551616}"#,
        br#"{"x":1} {}"#,
        br#"{"x":01}"#,
        br#"{"x":"\ud800"}"#,
        b"{\"x\":\"\xff\"}",
    ] {
        assert!(validate(b"true", raw).is_err(), "{raw:?}");
    }
    let input = br#"{"x":-9007199254740991,"y":9007199254740991,"z":-0}"#;
    let value = validate(b"true", input).unwrap();
    assert_eq!(value.value()["z"], 0);
}
fn nested(depth: usize) -> Value {
    let mut value = json!(0);
    for _ in 0..depth {
        value = json!({"nested":value});
    }
    value
}
#[test]
fn parameter_and_schema_container_depth_have_independent_limits() {
    for depth in [16, 17] {
        let bytes = serde_json::to_vec(&nested(depth)).unwrap();
        assert_eq!(validate(b"true", &bytes).is_ok(), depth == 16);
    }
    for depth in [32, 33] {
        // An extension annotation remains an unknown schema keyword; its nested
        // data still must obey the schema document's parser budget.
        let schema = serde_json::to_vec(&json!({"annotation":nested(depth - 1)})).unwrap();
        assert_eq!(validate(&schema, br#"{"x":1}"#).is_ok(), depth == 32);
    }
}
#[test]
fn raw_schema_and_parameter_byte_limits_are_enforced_at_the_boundary() {
    let mut schema = b"true".to_vec();
    schema.resize(1024 * 1024, b' ');
    validate(&schema, br#"{"x":1}"#).unwrap();
    schema.push(b' ');
    assert_eq!(validate(&schema, br#"{"x":1}"#), Err(Error::Capacity));
    let maximum = 2 * 1024 * 1024;
    let parameters = format!("{{\"x\":\"{}\"}}", "v".repeat(maximum - 8));
    assert_eq!(parameters.len(), maximum);
    validate(b"true", parameters.as_bytes()).unwrap();
    assert_eq!(
        validate(b"true", format!("{parameters} ").as_bytes()),
        Err(Error::Capacity)
    );
}
#[test]
fn schema_retrieval_draft_and_regex_policy_match_existing_validation() {
    for schema in [
        json!({"$ref":"https://127.0.0.1:1/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
        json!({"$ref":"relative.json#x"}),
        json!({"$dynamicRef":"https://example.invalid/schema#x"}),
        json!({"$schema":"http://json-schema.org/draft-07/schema#"}),
        json!({"type":17}),
        json!({"type":"object","properties":{"x":{"pattern":"^(?=a)"}}}),
        json!({"type":"object","properties":{"x":{"pattern":"(a{1000}){1000}"}}}),
    ] {
        let bytes = serde_json::to_vec(&schema).unwrap();
        assert!(validate(&bytes, br#"{"x":"a"}"#).is_err(), "{schema}");
    }
    let formatted = br#"{"properties":{"email":{"type":"string","format":"email"}}}"#;
    assert!(validate(formatted, br#"{"email":"not an email"}"#).is_err());
    validate(formatted, br#"{"email":"a@example.test"}"#).unwrap();
}
