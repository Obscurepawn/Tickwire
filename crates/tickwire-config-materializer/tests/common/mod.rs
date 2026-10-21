use serde_json::json;
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_config_journal::{Config, Prepared, RuntimeFact, Scope, Version};
pub fn hash(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}
pub fn hex(h: [u8; 32]) -> String {
    h.iter().map(|v| format!("{v:02x}")).collect()
}
pub fn config() -> Config {
    let mut namespace = [0; 16];
    namespace[6] = 0x40;
    namespace[8] = 0x80;
    namespace[15] = 1;
    Config {
        scope: Scope {
            cluster: "tickwire-qualification".into(),
            namespace,
            host: "host-a".into(),
            process: "strategy-01-active".into(),
            slot: [3; 16],
            locator_generation: 1,
        },
        stream_epoch: [4; 16],
        writer_epoch: 1,
        max_operations: 8,
        max_recorded_bytes: 65536,
    }
}
pub fn object(revision: u64) -> ProjectionObject {
    let bytes = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{
        "process_id":"strategy-01-active","account_ids":["spot-main","usdm-main"],
        "venue_ids":["binance-spot","binance-usdm"],"topic_ids":[1,2],"secret_purposes":[]
    }]}))
    .unwrap();
    let reference = format!(
        "artifact://fixture/config/policy@sha256:{}",
        hex(hash(&bytes))
    );
    let mut value = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    value["local_config"]["projection_policy_ref"] = reference.clone().into();
    let c = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let policy = PinnedPolicy::decode(&reference, &bytes).unwrap();
    ProjectionObject::build(
        revision,
        &project(&c, "strategy-01-active", &policy).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap()
}
pub fn prepared(object: &ProjectionObject, base: Option<Version>) -> Prepared {
    let b = object.binding();
    Prepared {
        operation: [b.revision as u8; 16],
        base,
        next: Version {
            revision: b.revision,
            global_hash: b.global_hash,
            projection_hash: b.projection_hash,
            policy_revision: b.policy_revision,
            object_hash: b.object_hash,
        },
        control_commit_ref: [5; 32],
        target_set_hash: [6; 32],
        expected_instance: [7; 16],
        expected_boot: [8; 16],
        expected_host_boot: [9; 16],
        exec_generation: 2,
        projection_object_bytes: object.bytes().len() as u64,
    }
}
pub fn runtime() -> RuntimeFact {
    RuntimeFact {
        instance: [7; 16],
        boot: [8; 16],
        host_boot: [9; 16],
        exec_generation: 2,
        applied_event_seq: 100,
        witness_ref: [10; 32],
    }
}

pub struct LoseSync<S> {
    pub store: S,
    pub fail: std::rc::Rc<std::cell::Cell<bool>>,
}
impl<S: tickwire_journal::store::Store> tickwire_journal::store::Store for LoseSync<S> {
    fn read_manifest(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.store.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.store.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> std::io::Result<Vec<u64>> {
        self.store.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> std::io::Result<Vec<u8>> {
        self.store.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> std::io::Result<()> {
        self.store.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        self.store.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> std::io::Result<()> {
        self.store.sync_segment(id)?;
        if self.fail.replace(false) {
            Err(std::io::Error::other("lost actual journal sync return"))
        } else {
            Ok(())
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> std::io::Result<()> {
        self.store.truncate_segment(id, len)
    }
}
