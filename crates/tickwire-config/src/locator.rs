//! Content identity of the fixed bootstrap locator; no activation authority.
use crate::{Candidate, Error, Format, Hash, Limits, Result, strict};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub const MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fields {
    pub generation: u64,
    pub cluster_id: String,
    pub active_root: String,
    pub migration_id: [u8; 16],
    pub manifest_hash: Hash,
    pub previous_locator_hash: Hash,
}
#[derive(Debug)]
pub struct Locator {
    fields: Fields,
    canonical: Vec<u8>,
    hash: Hash,
}
impl Locator {
    pub fn build(fields: Fields) -> Result<Self> {
        validate_fields(&fields)?;
        let mut value = json!({
            "locator_version":1,
            "generation":fields.generation.to_string(),
            "cluster_id":fields.cluster_id,
            "active_root":fields.active_root,
            "migration_id":uuid(fields.migration_id),
            "manifest_hash":hex(fields.manifest_hash),
            "previous_locator_hash":hex(fields.previous_locator_hash),
        });
        let checksum: Hash = Sha256::digest(strict::canonical(&value)?).into();
        value["checksum"] = hex(checksum).into();
        Self::decode(Format::Json, &strict::canonical(&value)?)
    }
    pub fn decode(format: Format, bytes: &[u8]) -> Result<Self> {
        let candidate = Candidate::parse(
            format,
            bytes,
            Limits {
                max_bytes: MAX_BYTES,
                max_depth: 8,
            },
        )?;
        let value = candidate.value();
        schema(value)?;
        let fields = Fields {
            generation: text(value, "generation")?
                .parse()
                .map_err(|_| Error::Revision)?,
            cluster_id: text(value, "cluster_id")?.into(),
            active_root: text(value, "active_root")?.into(),
            migration_id: parse_uuid(text(value, "migration_id")?)?,
            manifest_hash: parse_hash(text(value, "manifest_hash")?)?,
            previous_locator_hash: parse_hash(text(value, "previous_locator_hash")?)?,
        };
        validate_fields(&fields)?;
        let checksum = parse_hash(text(value, "checksum")?)?;
        let mut body = value.clone();
        body.as_object_mut()
            .ok_or(Error::Schema)?
            .remove("checksum");
        let actual: Hash = Sha256::digest(strict::canonical(&body)?).into();
        if actual != checksum {
            return Err(Error::Semantic("locator checksum"));
        }
        Ok(Self {
            fields,
            canonical: candidate.canonical_bytes().to_vec(),
            hash: candidate.hash(),
        })
    }
    pub fn validate_successor(&self, next: &Self) -> Result<()> {
        let old = &self.fields;
        let new = &next.fields;
        if old.generation.checked_add(1) != Some(new.generation)
            || old.cluster_id != new.cluster_id
            || old.active_root == new.active_root
            || new.previous_locator_hash != self.hash
            || new.migration_id == [0; 16]
            || old.migration_id == new.migration_id
        {
            return Err(Error::Semantic("locator successor"));
        }
        Ok(())
    }
    pub fn fields(&self) -> &Fields {
        &self.fields
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn toml(&self) -> Result<Vec<u8>> {
        let value: Value = serde_json::from_slice(&self.canonical).map_err(|_| Error::Encoding)?;
        let bytes = toml::to_string(&value)
            .map_err(|_| Error::Encoding)?
            .into_bytes();
        if bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
}
fn validate_fields(fields: &Fields) -> Result<()> {
    let root = &fields.active_root;
    if fields.generation == 0 || fields.generation >= 1 << 63 {
        return Err(Error::Revision);
    }
    if fields.manifest_hash == [0; 32]
        || !(1..=63).contains(&fields.cluster_id.len())
        || (fields.generation == 1 && fields.previous_locator_hash != [0; 32])
        || (fields.generation > 1
            && (fields.previous_locator_hash == [0; 32] || fields.migration_id == [0; 16]))
        || !root.starts_with('/')
        || root.len() > 4096
        || root
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(Error::Semantic("locator fields"));
    }
    Ok(())
}
fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name].as_str().ok_or(Error::Schema)
}
fn hex(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N]> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Encoding);
    }
    let mut result = [0; N];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::Encoding)?;
    }
    Ok(result)
}
fn parse_hash(value: &str) -> Result<Hash> {
    decode_hex(value.strip_prefix("sha256:").ok_or(Error::Encoding)?)
}
fn uuid(value: [u8; 16]) -> String {
    let raw = value.iter().map(|b| format!("{b:02x}")).collect::<String>();
    format!(
        "{}-{}-{}-{}-{}",
        &raw[..8],
        &raw[8..12],
        &raw[12..16],
        &raw[16..20],
        &raw[20..]
    )
}
fn parse_uuid(value: &str) -> Result<[u8; 16]> {
    if value.len() != 36
        || [8, 13, 18, 23]
            .into_iter()
            .any(|i| value.as_bytes()[i] != b'-')
    {
        return Err(Error::Encoding);
    }
    let raw: String = value.chars().filter(|c| *c != '-').collect();
    let bytes = decode_hex(&raw)?;
    if uuid(bytes) != value {
        return Err(Error::Encoding);
    }
    Ok(bytes)
}
fn schema(value: &Value) -> Result<()> {
    static SCHEMA: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
    let validator = SCHEMA.get_or_init(|| {
        let source: Value = serde_json::from_str(include_str!("../../../specs/contracts/system-config.schema.json")).map_err(|_| ())?;
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .offline()
            .build(&json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"#/$defs/ConfigLocator","$defs":source["$defs"]}))
            .map_err(|_| ())
    }).as_ref().map_err(|_| Error::Schema)?;
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
