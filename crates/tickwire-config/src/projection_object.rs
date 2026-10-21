//! Content-bound projection transport objects, without commit authority.
use crate::{
    Candidate, Error, Format, Hash, Limits, Result,
    delivery::{DeliveryKind, ProjectionDelivery},
    projection::Projection,
    strict,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectBinding {
    pub revision: u64,
    pub global_hash: Hash,
    pub policy_revision: u64,
    pub process_id: String,
    pub projection_hash: Hash,
    pub object_hash: Hash,
}
pub struct ProjectionObject {
    bytes: Vec<u8>,
    payload: Value,
    binding: ObjectBinding,
}
impl ProjectionObject {
    pub fn for_delivery(
        revision: u64,
        delivery: &ProjectionDelivery,
        process_id: &str,
        max_bytes: usize,
    ) -> Result<Self> {
        let target = delivery
            .targets()
            .get(process_id)
            .ok_or(Error::Semantic("projection delivery target"))?;
        let projection = if target.kind() == DeliveryKind::Retired {
            target.old_projection()
        } else {
            target.new_projection()
        }
        .ok_or(Error::Semantic("projection delivery payload"))?;
        Self::wrap(revision, projection, delivery.candidate_hash(), max_bytes)
    }
    pub fn build(revision: u64, projection: &Projection, max_bytes: usize) -> Result<Self> {
        Self::wrap(revision, projection, projection.global_hash(), max_bytes)
    }
    fn wrap(
        revision: u64,
        projection: &Projection,
        global_hash: Hash,
        max_bytes: usize,
    ) -> Result<Self> {
        bound(max_bytes, 0)?;
        valid_revision(revision)?;
        valid_revision(projection.policy_revision())?;
        let process_id = projection.value()["process"]["id"]
            .as_str()
            .ok_or(Error::Schema)?;
        let value = json!({
            "projection_schema_version":1,
            "global_revision":revision.to_string(),
            "global_config_hash":hex(global_hash),
            "projection_policy_revision":projection.policy_revision().to_string(),
            "target_process_id":process_id,
            "projection_hash":hex(projection.hash()),
            "payload":projection.value()
        });
        let bytes = strict::canonical(&value)?;
        bound(max_bytes, bytes.len())?;
        let binding = ObjectBinding {
            revision,
            global_hash,
            policy_revision: projection.policy_revision(),
            process_id: process_id.to_owned(),
            projection_hash: projection.hash(),
            object_hash: Sha256::digest(&bytes).into(),
        };
        Self::decode(&bytes, &binding, max_bytes)
    }
    pub fn decode(bytes: &[u8], expected: &ObjectBinding, max_bytes: usize) -> Result<Self> {
        bound(max_bytes, bytes.len())?;
        valid_revision(expected.revision)?;
        valid_revision(expected.policy_revision)?;
        let digest: Hash = Sha256::digest(bytes).into();
        if digest != expected.object_hash {
            return Err(Error::Semantic("projection object digest"));
        }
        let candidate = Candidate::parse(
            Format::Json,
            bytes,
            Limits {
                max_bytes,
                max_depth: 32,
            },
        )?;
        let value = candidate.value();
        schema(value)?;
        if parsed_revision(&value["global_revision"]) != Some(expected.revision)
            || value["global_config_hash"] != hex(expected.global_hash)
            || parsed_revision(&value["projection_policy_revision"])
                != Some(expected.policy_revision)
            || value["target_process_id"] != expected.process_id
            || value["projection_hash"] != hex(expected.projection_hash)
            || value["payload"]["process"]["id"] != expected.process_id
        {
            return Err(Error::Semantic("projection object binding"));
        }
        let payload = value["payload"].clone();
        let actual: Hash = Sha256::digest(strict::canonical(&payload)?).into();
        if actual != expected.projection_hash {
            return Err(Error::Semantic("projection payload digest"));
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            payload,
            binding: expected.clone(),
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn binding(&self) -> &ObjectBinding {
        &self.binding
    }
}
fn valid_revision(revision: u64) -> Result<()> {
    if revision == 0 || revision >= 1 << 63 {
        Err(Error::Revision)
    } else {
        Ok(())
    }
}
fn parsed_revision(value: &Value) -> Option<u64> {
    value.as_str()?.parse().ok()
}
fn bound(limit: usize, length: usize) -> Result<()> {
    if !(1..=4 * 1024 * 1024).contains(&limit) || length > limit {
        Err(Error::Capacity)
    } else {
        Ok(())
    }
}
fn hex(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn schema(value: &Value) -> Result<()> {
    static SCHEMA: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
    let schema = SCHEMA
        .get_or_init(|| {
            let source: Value = serde_json::from_str(include_str!(
                "../../../specs/contracts/system-config.schema.json"
            ))
            .map_err(|_| ())?;
            let document = json!({
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "$ref":"#/$defs/ProjectionObject",
                "$defs":source["$defs"]
            });
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .should_validate_formats(true)
                .offline()
                .build(&document)
                .map_err(|_| ())
        })
        .as_ref()
        .map_err(|_| Error::Schema)?;
    if schema.is_valid(value) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
