use tickwire_config::{Candidate, Format, Limits};

fn parse(input: &str) -> Candidate {
    Candidate::parse(Format::Json, input.as_bytes(), Limits::default()).unwrap()
}

#[test]
fn toml_and_json_share_exact_canonical_bytes_and_content_identity() {
    let json = parse(r#"{"strategy":{"count":42,"enabled":true,"label":"é"},"names":["b","a"]}"#);
    let toml = Candidate::parse(
        Format::Toml,
        b"names = ['b', 'a']\n[strategy]\nlabel = '\xc3\xa9'\nenabled = true\ncount = 42\n",
        Limits::default(),
    )
    .unwrap();
    assert_eq!(json, toml);
    assert_eq!(
        json.canonical_bytes(),
        "{\"names\":[\"b\",\"a\"],\"strategy\":{\"count\":42,\"enabled\":true,\"label\":\"é\"}}"
            .as_bytes()
    );
    assert_ne!(json.hash(), parse(r#"{"names":["a","b"]}"#).hash());
}

#[test]
fn canonical_key_order_uses_utf16_not_utf8() {
    // U+1F600 begins with D83D in UTF-16 and must precede U+E000.
    let input = parse("{\"\\ue000\":1,\"😀\":2,\"a\":3}");
    assert_eq!(
        input.canonical_bytes(),
        "{\"a\":3,\"😀\":2,\"\u{e000}\":1}".as_bytes()
    );
}

#[test]
fn integer_negative_zero_normalizes_without_accepting_float_notation() {
    assert_eq!(parse(r#"{"x":-0}"#), parse(r#"{"x":0}"#));
    assert_ne!(parse(r#"{"x":"-0"}"#), parse(r#"{"x":"0"}"#));
    for input in [r#"{"x":-0.0}"#, r#"{"x":-0e0}"#, r#"{"x":-01}"#] {
        assert!(Candidate::parse(Format::Json, input.as_bytes(), Limits::default()).is_err());
    }
}

#[test]
fn json_duplicate_keys_are_rejected_before_their_values_are_lost() {
    for value in [
        r#"{"mode":"live","mode":"qualification"}"#,
        r#"{"a":{"limit":1,"limit":2}}"#,
        r#"{"a":1,"\u0061":2}"#,
    ] {
        assert!(Candidate::parse(Format::Json, value.as_bytes(), Limits::default()).is_err());
    }
}

#[test]
fn unsafe_numbers_float_syntax_and_datetime_are_rejected() {
    for input in [
        r#"{"x":9007199254740992}"#,
        r#"{"x":-9007199254740992}"#,
        r#"{"x":1.0}"#,
        r#"{"x":1e0}"#,
        r#"{"x":1e999}"#,
    ] {
        assert!(
            Candidate::parse(Format::Json, input.as_bytes(), Limits::default()).is_err(),
            "accepted forbidden number"
        );
    }
    for input in [
        "x=1.0",
        "x=nan",
        "x=inf",
        "x=1979-05-27T07:32:00Z",
        "x=9007199254740992",
    ] {
        assert!(
            Candidate::parse(Format::Toml, input.as_bytes(), Limits::default()).is_err(),
            "accepted forbidden TOML scalar"
        );
    }
    assert!(
        parse(r#"{"x":9007199254740991,"y":-9007199254740991}"#)
            .value()
            .is_object()
    );
}

#[test]
fn input_size_and_container_depth_are_enforced() {
    let limits = Limits {
        max_bytes: 16,
        max_depth: 2,
    };
    assert!(Candidate::parse(Format::Json, b"{\"x\":\"0123456789\"}", limits).is_err());
    assert!(Candidate::parse(Format::Json, b"{\"a\":{\"b\":{}}}", limits).is_err());
    assert!(Candidate::parse(Format::Json, b"{\"a\":{}}", limits).is_ok());
    assert!(Candidate::parse(Format::Toml, b"[a.b]\nx=1", limits).is_err());
}

#[test]
fn fixed_schema_accepts_reference_and_rejects_unknown_fields_and_template() {
    let reference = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap();
    reference.validate_structural().unwrap();
    let mut invalid = reference.value().clone();
    invalid["identity"]["unsupported"] = true.into();
    let invalid = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&invalid).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert!(invalid.validate_structural().is_err());
    let template = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/production.template.toml"),
        Limits::default(),
    )
    .unwrap();
    assert!(template.validate_structural().is_err());
}
