use serde_json::Value;
use sha2::{Digest, Sha256};
use tickwire_instrument_catalog::{
    Catalog, Hash, MAX_ASSETS, MAX_BYTES, MAX_DEPTH, MAX_INSTRUMENTS,
};

fn hash(text: &str) -> Hash {
    assert_eq!(text.len(), 64);
    std::array::from_fn(|index| u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).unwrap())
}

#[test]
fn shared_independent_canonical_vectors_and_rehashed_rejections() {
    let contract: Value = serde_json::from_str(include_str!(
        "../../../specs/contracts/instrument-catalog-v1.json"
    ))
    .unwrap();
    assert_eq!(contract["schema"], 1);
    assert_eq!(contract["limits"]["max_bytes"], MAX_BYTES);
    assert_eq!(contract["limits"]["max_depth"], MAX_DEPTH);
    assert_eq!(contract["limits"]["max_assets"], MAX_ASSETS);
    assert_eq!(contract["limits"]["max_instruments"], MAX_INSTRUMENTS);
    let domain = b"tickwire.instrument-catalog.v1\0";
    assert_eq!(
        contract["domain_hex"],
        domain
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    for case in contract["vectors"].as_array().unwrap() {
        let raw = case["raw"].as_str().unwrap().as_bytes();
        let artifact = hash(case["artifact_sha256"].as_str().unwrap());
        let catalog = Catalog::decode(raw, artifact).unwrap();
        assert_eq!(catalog.artifact_hash(), artifact, "{}", case["name"]);
        assert_eq!(catalog.raw_bytes(), raw, "{}", case["name"]);
        assert_eq!(
            catalog.canonical_bytes(),
            case["canonical"].as_str().unwrap().as_bytes(),
            "{}",
            case["name"]
        );
        assert_eq!(
            catalog.catalog_hash(),
            hash(case["catalog_sha256"].as_str().unwrap()),
            "{}",
            case["name"]
        );
        let recoded = catalog.canonical_bytes();
        let next = Catalog::decode(recoded, Sha256::digest(recoded).into()).unwrap();
        assert_eq!(next.catalog_hash(), catalog.catalog_hash());
        assert_eq!(next.instruments(), catalog.instruments());
    }
    for case in contract["rejections"].as_array().unwrap() {
        let raw = case["raw"].as_str().unwrap().as_bytes();
        let artifact = hash(case["artifact_sha256"].as_str().unwrap());
        assert_eq!(<Hash>::from(Sha256::digest(raw)), artifact);
        assert!(Catalog::decode(raw, artifact).is_err(), "{}", case["name"]);
    }
}
