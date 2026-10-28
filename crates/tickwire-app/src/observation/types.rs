use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub logical_strategy_id: u64,
    pub instance: [u8; 16],
    pub generation: u64,
}
impl Target {
    pub(super) fn valid(self) -> bool {
        self.logical_strategy_id != 0 && self.instance != [0; 16] && self.generation != 0
    }
    pub(super) fn matches(self, p: &tickwire_supervisor::LiveChildObservation) -> bool {
        p.identity().logical_strategy == self.logical_strategy_id
            && p.identity().instance == self.instance
            && p.identity().generation == self.generation
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: u16,
    pub request_id: [u8; 16],
    pub session_id: [u8; 16],
    pub binding: Binding,
    pub target: Target,
    pub valid_until_mono_ns: u64,
    pub require_resources: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Observed,
    Rejected,
    Expired,
    Busy,
    Unsupported,
    Interrupted,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub schema: u16,
    pub request: Request,
    pub request_digest: [u8; 32],
    pub status: Status,
    pub completed_mono_ns: u64,
    pub historical: bool,
    pub observation: Option<Observation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub instance: [u8; 16],
    pub generation: u64,
    pub logical_strategy_id: u64,
    pub role: u16,
    pub build_hash: [u8; 32],
    pub qualification_hash: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
    pub host_boot: [u8; 16],
    pub approved_host_boot: [u8; 16],
    pub namespace: [u8; 16],
    pub arch: u16,
    pub trust_profile: u8,
    pub connected_epoch: Option<u64>,
    pub start_ticks: u64,
    pub image_bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpus: Vec<u32>,
    pub numa_node: u32,
    pub memory_max: u64,
    pub thread_ids: Vec<u32>,
    pub group_device: u64,
    pub group_inode: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub objects: Vec<Object>,
    pub process: Process,
    pub resources: Option<Resources>,
    pub challenge: [u8; 16],
    pub control_epoch: u64,
    pub report_sequence_floor: u64,
    pub ready_sequence: u64,
    pub account_position: u64,
    pub report_ring_sequence: u64,
    pub ledger_sequence: u64,
    pub account_input_digest: [u8; 32],
    pub orders_digest: [u8; 32],
    pub checkpoint: Object,
    pub input_seal: Object,
    pub cursor_resource: [u8; 16],
    pub subscription_generation: u64,
    pub bank_resource: [u8; 16],
    pub ring_epoch: u64,
    pub started_mono_ns: u64,
    pub observed_mono_ns: u64,
}
impl Observation {
    pub(super) fn verified(value: &tickwire_strategy_host::FreshObservation) -> Self {
        let p = value.process();
        let i = p.identity();
        let os = p.process();
        let r = value.ready();
        let sub = value.subscription();
        Self {
            objects: Vec::new(),
            process: Process {
                instance: i.instance,
                generation: i.generation,
                logical_strategy_id: i.logical_strategy,
                role: i.role,
                build_hash: p.image_hash(),
                qualification_hash: i.qualification_hash,
                manifest_hash: i.manifest_hash,
                pid: os.pid,
                uid: os.uid,
                gid: os.gid,
                host_boot: p.host_boot(),
                approved_host_boot: i.host_boot,
                namespace: i.namespace,
                arch: i.arch,
                trust_profile: i.trust_profile,
                connected_epoch: p.connected_epoch(),
                start_ticks: p.start_ticks(),
                image_bytes: p.image_bytes(),
            },
            resources: value.resources().map(|r| Resources {
                cpus: r.cpus().to_vec(),
                numa_node: r.numa_node(),
                memory_max: r.memory_max(),
                thread_ids: r.thread_ids().to_vec(),
                group_device: r.group_device(),
                group_inode: r.group_inode(),
            }),
            challenge: value.challenge(),
            control_epoch: value.control_epoch(),
            report_sequence_floor: value.report_sequence_floor(),
            ready_sequence: r.ready_sequence,
            account_position: r.exec_report_sequence,
            report_ring_sequence: r.report_ring_sequence,
            ledger_sequence: r.ledger_sequence,
            account_input_digest: value.account_input_digest(),
            orders_digest: r.orders_digest,
            checkpoint: Object {
                digest: r.checkpoint_digest,
                bytes: r.checkpoint_bytes,
            },
            input_seal: Object {
                digest: r.input_seal_digest,
                bytes: r.input_seal_bytes,
            },
            cursor_resource: sub.cursor.to_le_bytes(),
            subscription_generation: sub.generation,
            bank_resource: sub.bank.resource.to_le_bytes(),
            ring_epoch: sub.bank.ring_epoch,
            started_mono_ns: value.started_at().nanos(),
            observed_mono_ns: value.observed_at().nanos(),
        }
    }
}
pub(super) fn failure(error: &tickwire_strategy_host::Error) -> Status {
    use tickwire_strategy_host::Error;
    match error {
        Error::ObservationExpired => Status::Expired,
        Error::Busy | Error::ObservationCapacity => Status::Busy,
        Error::ObservationUnsupported => Status::Unsupported,
        _ => Status::Rejected,
    }
}
