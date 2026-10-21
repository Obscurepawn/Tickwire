//! Permission-scoped TOML content. No commit, runtime or persistence authority.
use crate::{Candidate, Error, Format, Hash, Limits, Result, projection_object::ProjectionObject};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Desired,
    Effective,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    pub kind: Kind,
    pub operation_id: String,
    pub control_commit_ref: String,
    pub process_boot_id: String,
    pub applied_event_seq: u64,
    pub journal_receipt_ref: String,
    pub instance_slot_id: String,
}
pub const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
pub struct ProjectionFile {
    bytes: Vec<u8>,
    render_hash: Hash,
    value: Value,
}
impl ProjectionFile {
    pub fn render(object: &ProjectionObject, stamp: &Stamp, max_bytes: usize) -> Result<Self> {
        bound(max_bytes, 0)?;
        let value = expected_value(object, stamp)?;
        schema(&value)?;
        let bytes = toml::to_string(&value)
            .map_err(|_| Error::Encoding)?
            .into_bytes();
        bound(max_bytes, bytes.len())?;
        Self::decode(
            &bytes,
            object,
            stamp,
            Sha256::digest(&bytes).into(),
            max_bytes,
        )
    }
    pub fn decode(
        bytes: &[u8],
        object: &ProjectionObject,
        stamp: &Stamp,
        render_hash: Hash,
        max_bytes: usize,
    ) -> Result<Self> {
        bound(max_bytes, bytes.len())?;
        let actual: Hash = Sha256::digest(bytes).into();
        if actual != render_hash {
            return Err(Error::Semantic("materialized file digest"));
        }
        let expected = expected_value(object, stamp)?;
        let parsed = Candidate::parse(
            Format::Toml,
            bytes,
            Limits {
                max_bytes,
                max_depth: 32,
            },
        )?;
        schema(parsed.value())?;
        if parsed.value() != &expected {
            return Err(Error::Semantic("materialized file binding"));
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            render_hash,
            value: parsed.value().clone(),
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn render_hash(&self) -> Hash {
        self.render_hash
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
}
fn bound(limit: usize, actual: usize) -> Result<()> {
    if !(1..=MAX_FILE_BYTES).contains(&limit) || actual > limit {
        Err(Error::Capacity)
    } else {
        Ok(())
    }
}
fn expected_value(object: &ProjectionObject, stamp: &Stamp) -> Result<Value> {
    const ZERO: &str = "00000000-0000-0000-0000-000000000000";
    if stamp.operation_id == ZERO
        || stamp.instance_slot_id == ZERO
        || stamp.operation_id.len() != 36
        || stamp.process_boot_id.len() != 36
        || stamp.instance_slot_id.len() != 36
        || stamp.control_commit_ref.len() > 1024
        || stamp.journal_receipt_ref.len() > 1024
        || stamp.applied_event_seq >= 1 << 63
        || (stamp.kind == Kind::Desired && stamp.applied_event_seq != 0)
        || (stamp.kind == Kind::Effective
            && (stamp.process_boot_id == ZERO || stamp.applied_event_seq == 0))
    {
        return Err(Error::Semantic("materialization stamp"));
    }
    let binding = object.binding();
    Ok(json!({
        "projection_schema_version":1,
        "payload":object.payload(),
        "_materialization":{
            "revision":binding.revision.to_string(),
            "projection_kind":match stamp.kind {Kind::Desired=>"desired",Kind::Effective=>"effective"},
            "target_process_id":binding.process_id,
            "operation_id":stamp.operation_id,
            "control_commit_ref":stamp.control_commit_ref,
            "process_boot_id":stamp.process_boot_id,
            "applied_event_seq":stamp.applied_event_seq.to_string(),
            "journal_receipt_ref":stamp.journal_receipt_ref,
            "render_version":1,
            "global_config_hash":hex(binding.global_hash),
            "projection_hash":hex(binding.projection_hash),
            "projection_policy_revision":binding.policy_revision.to_string(),
            "instance_slot_id":stamp.instance_slot_id
        }
    }))
}
fn hex(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn schema(value: &Value) -> Result<()> {
    static SCHEMA: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
    let schema = SCHEMA.get_or_init(|| {
        let source: Value = serde_json::from_str(include_str!("../../../specs/contracts/system-config.schema.json")).map_err(|_| ())?;
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .offline()
            .build(&json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"#/$defs/ProcessProjection","$defs":source["$defs"]}))
            .map_err(|_| ())
    }).as_ref().map_err(|_|Error::Schema)?;
    if schema.is_valid(value) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
