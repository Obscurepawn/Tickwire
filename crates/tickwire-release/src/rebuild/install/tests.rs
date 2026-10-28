use super::*;
fn request() -> Request {
    let (mut operation, manifest, _, _, plans) = super::super::tests::fixture();
    operation.schema = 4;
    Request {
        schema: 1,
        member: operation.members[0].clone(),
        operation,
        plans: Object::of(&plans.bytes().unwrap()),
        manifest: Object::of(&crate::encode(&manifest).unwrap()),
        capture_completion: Object::of(b"completion"),
        capture_seal: Object::of(b"seal"),
        capture_receipt: Object::of(b"receipt"),
        target_bootstrap: Object::of(b"bootstrap"),
        deadline_mono_ns: 99,
    }
}
#[test]
fn install_request_is_bound_to_schema4_and_exact_operation_roots() {
    let request = request();
    assert_eq!(Request::decode(&request.bytes().unwrap()).unwrap(), request);
    for field in 0..7 {
        let mut bad = request.clone();
        match field {
            0 => bad.operation.schema = 3,
            1 => bad.plans = Object::of(b"other"),
            2 => bad.manifest = Object::of(b"other"),
            3 => bad.member.new_generation += 1,
            4 => bad.capture_receipt.bytes = 0,
            5 => bad.deadline_mono_ns = 101,
            _ => bad.deadline_mono_ns = 0,
        }
        assert!(bad.bytes().is_err(), "mutation {field}");
    }
}
#[test]
fn install_request_requires_canonical_bounded_bytes() {
    let request = request();
    let mut bytes = request.bytes().unwrap();
    bytes.push(b' ');
    assert!(Request::decode(&bytes).is_err());
    assert!(Request::decode(&vec![b' '; replay::MAX_BYTES + 1]).is_err());
}
