//! Durable actual strategy inputs and complete recovery descriptors, before intent publication.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use tickwire_recovery::{InputRecord, MAX_OBJECT_BYTES, digest, encode_inputs};
use tickwire_venue::{CanonicalCommand, CommandBody, SpotAdvancedCommand};
pub const CHECKPOINT_SCHEMA: u64 = 0x545753430001;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyFile {
    pub kind: u8,
    pub schema_id: u64,
    pub owner: [u8; 16],
    pub path: PathBuf,
    pub digest: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub dependencies: Vec<DependencyFile>,
    pub declared_rebuild: bool,
    pub fault_model_id: [u8; 32],
    pub pin_id: [u8; 16],
    pub owner: [u8; 16],
    pub input_stream: [u8; 16],
    pub output_directory: PathBuf,
    pub maximum_input_bytes: usize,
    pub order_ids: Vec<[u8; 16]>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryFiles {
    pub dependencies: Vec<DependencyFile>,
    pub inputs: PathBuf,
    pub input_digest: [u8; 32],
    pub declared_rebuild: bool,
    pub fault_model_id: [u8; 32],
    pub pin_id: [u8; 16],
    pub owner: [u8; 16],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub command_digest: [u8; 32],
    pub order_ids: Vec<[u8; 16]>,
    pub recovery: RecoveryFiles,
}
struct Row {
    kind: u8,
    bytes: Vec<u8>,
}
pub trait InputJournal {
    fn market(&mut self, record: &[u8], payload: &[u8]) -> Result<()>;
    fn decision(&mut self, instrument: u64, now: u64) -> Result<()>;
}
pub struct NoJournal;
impl InputJournal for NoJournal {
    fn market(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn decision(&mut self, _: u64, _: u64) -> Result<()> {
        Ok(())
    }
}
pub struct RecoveryWriter {
    template: Template,
    dependencies: Vec<DependencyFile>,
    rows: Vec<Row>,
    bytes: usize,
    active: bool,
}
pub fn order_count(c: &CanonicalCommand) -> usize {
    match &c.command().body {
        CommandBody::Cancel(_)
        | CommandBody::Query(_)
        | CommandBody::CancelList(_)
        | CommandBody::CancelAlgo(_)
        | CommandBody::AmendKeepPriority { .. }
        | CommandBody::Modify(_) => 0,
        CommandBody::SpotList(l) => l.legs.as_slice().len(),
        CommandBody::SpotAdvanced(SpotAdvancedCommand::List(l)) => l.legs.as_slice().len(),
        _ => 1,
    }
}
fn read(path: &Path, max: usize) -> Result<Vec<u8>> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    if !f.metadata()?.is_file() || f.metadata()?.len() > max as u64 {
        return Err(Error::Bounds);
    }
    let mut b = Vec::new();
    f.take(max as u64 + 1).read_to_end(&mut b)?;
    if b.len() > max {
        return Err(Error::Bounds);
    }
    Ok(b)
}
fn immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_OBJECT_BYTES {
        return Err(Error::Bounds);
    }
    match OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
    {
        Ok(mut f) => {
            f.write_all(bytes)?;
            f.sync_all()?;
            File::open(path.parent().ok_or(Error::Identity)?)?.sync_all()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if read(path, MAX_OBJECT_BYTES)? != bytes {
                return Err(Error::Identity);
            }
            File::open(path)?.sync_all()?;
            File::open(path.parent().ok_or(Error::Identity)?)?.sync_all()?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
fn name(h: [u8; 32]) -> String {
    h.iter().map(|v| format!("{v:02x}")).collect()
}
fn object(root: &Path, bytes: &[u8]) -> Result<(PathBuf, [u8; 32])> {
    let h = digest(bytes);
    let p = root.join(format!("{}.bin", name(h)));
    immutable(&p, bytes)?;
    Ok((p, h))
}
pub fn snapshot_payload(
    join: &crate::bootstrap::JoinBootstrap,
    snapshot: &[u8],
    now: u64,
    maximum_levels: usize,
) -> Result<Vec<u8>> {
    let j = serde_json::to_vec(join)?;
    let mut b = b"TWSB0001".to_vec();
    b.extend(now.to_le_bytes());
    b.extend((maximum_levels as u64).to_le_bytes());
    b.extend((j.len() as u32).to_le_bytes());
    b.extend(j);
    b.extend(snapshot);
    if b.len() > MAX_OBJECT_BYTES {
        return Err(Error::Bounds);
    }
    Ok(b)
}
impl RecoveryWriter {
    /// Exact account callback inputs, in the same order in which strategy code sees them.
    pub fn account_report(
        &mut self,
        report: &crate::reports::AccountReport,
        now: u64,
    ) -> Result<()> {
        let (tag, position, object) = match report {
            crate::reports::AccountReport::Baseline(b) => (0u8, 0u64, b.bytes()),
            crate::reports::AccountReport::Record { position, record } => {
                (1, *position, record.bytes())
            }
        };
        let mut bytes = b"TWAI0001".to_vec();
        bytes.push(tag);
        bytes.extend(now.to_le_bytes());
        bytes.extend(position.to_le_bytes());
        bytes.extend(
            u32::try_from(object.len())
                .map_err(|_| Error::Bounds)?
                .to_le_bytes(),
        );
        bytes.extend(object);
        self.push(2, bytes)
    }
    pub fn new(
        template: Template,
        strategy_state: &[u8],
        snapshot: &[u8],
        maximum_age: u64,
    ) -> Result<Self> {
        if template.dependencies.len() != 10
            || template.owner == [0; 16]
            || template.input_stream == [0; 16]
            || template.pin_id == [0; 16]
            || template.fault_model_id == [0; 32]
            || strategy_state.is_empty()
            || !template.output_directory.is_absolute()
            || template.maximum_input_bytes > MAX_OBJECT_BYTES
            || template.maximum_input_bytes < 1024
            || maximum_age == 0
        {
            return Err(Error::Identity);
        }
        match std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&template.output_directory)
        {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        File::open(template.output_directory.parent().ok_or(Error::Identity)?)?.sync_all()?;
        let mut dependencies = Vec::new();
        for (i, d) in template.dependencies.iter().enumerate() {
            if d.kind != i as u8 + 2 || d.schema_id == 0 || d.owner == [0; 16] {
                return Err(Error::Identity);
            }
            let original = read(&d.path, MAX_OBJECT_BYTES)?;
            if original.is_empty() || digest(&original) != d.digest {
                return Err(Error::Identity);
            }
            let bytes = if d.kind == 11 && !template.declared_rebuild {
                if d.schema_id != CHECKPOINT_SCHEMA || d.owner != template.owner {
                    return Err(Error::Identity);
                }
                let mut checkpoint = b"TWSC0001".to_vec();
                checkpoint.extend(maximum_age.to_le_bytes());
                checkpoint.extend((strategy_state.len() as u32).to_le_bytes());
                checkpoint.extend(strategy_state);
                checkpoint.extend(snapshot);
                checkpoint
            } else {
                original
            };
            let (path, hash) = object(&template.output_directory, &bytes)?;
            dependencies.push(DependencyFile {
                path,
                digest: hash,
                ..d.clone()
            });
        }
        let mut out = Self {
            template,
            dependencies,
            rows: Vec::new(),
            bytes: 68,
            active: false,
        };
        // Actual configuration consumed by this strategy instance; snapshot is also
        // present for declared-rebuild audit instead of being an invented market marker.
        out.push(4, strategy_state.to_vec())?;
        if out.template.declared_rebuild {
            out.push(1, snapshot.to_vec())?;
        }
        Ok(out)
    }
    pub fn set_active(&mut self) {
        self.active = true;
    }
    fn push(&mut self, kind: u8, bytes: Vec<u8>) -> Result<()> {
        let size = self
            .bytes
            .checked_add(13)
            .and_then(|n| n.checked_add(bytes.len()))
            .ok_or(Error::Bounds)?;
        if size > self.template.maximum_input_bytes || self.rows.len() >= 1_000_000 {
            return Err(Error::Bounds);
        }
        self.rows.push(Row { kind, bytes });
        self.bytes = size;
        Ok(())
    }
    /// Returns only after dependency files, complete ordered inputs, and the exact
    /// command-digest descriptor are synchronized. IntentWriter is called afterward.
    pub fn prepare(&mut self, command: &CanonicalCommand) -> Result<()> {
        if !self.active
            || self.template.order_ids.len() != order_count(command)
            || self.template.order_ids.iter().any(|v| *v == [0; 16])
        {
            return Err(Error::Identity);
        }
        let rows = self
            .rows
            .iter()
            .map(|r| InputRecord {
                kind: r.kind,
                payload: &r.bytes,
            })
            .collect::<Vec<_>>();
        let inputs = encode_inputs(self.template.input_stream, 1, [0; 32], &rows)?;
        let (path, input_digest) = object(&self.template.output_directory, &inputs.bytes)?;
        let descriptor = Descriptor {
            command_digest: command.digest(),
            order_ids: self.template.order_ids.clone(),
            recovery: RecoveryFiles {
                dependencies: self.dependencies.clone(),
                inputs: path,
                input_digest,
                declared_rebuild: self.template.declared_rebuild,
                fault_model_id: self.template.fault_model_id,
                pin_id: self.template.pin_id,
                owner: self.template.owner,
            },
        };
        let bytes = serde_json::to_vec(&descriptor)?;
        let path = self
            .template
            .output_directory
            .join(format!("{}.json", name(command.digest())));
        immutable(&path, &bytes)
    }
}
impl InputJournal for RecoveryWriter {
    fn market(&mut self, record: &[u8], payload: &[u8]) -> Result<()> {
        let mut bytes = b"TWMI0001".to_vec();
        bytes.extend((record.len() as u32).to_le_bytes());
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend(record);
        bytes.extend(payload);
        self.push(1, bytes)
    }
    fn decision(&mut self, instrument: u64, now: u64) -> Result<()> {
        let mut bytes = b"TWDI0001".to_vec();
        bytes.push(u8::from(self.active));
        bytes.extend(instrument.to_le_bytes());
        bytes.extend(now.to_le_bytes());
        self.push(3, bytes)
    }
}
impl InputJournal for Option<RecoveryWriter> {
    fn market(&mut self, r: &[u8], p: &[u8]) -> Result<()> {
        if let Some(w) = self {
            w.market(r, p)
        } else {
            Ok(())
        }
    }
    fn decision(&mut self, i: u64, n: u64) -> Result<()> {
        if let Some(w) = self {
            w.decision(i, n)
        } else {
            Ok(())
        }
    }
}
/// Replays the bundled FixedDemo from its actual initial JOIN cut and ordered
/// inputs. No exchange update, timer value, or decision input is synthesized.
pub fn replay_fixed_demo(checkpoint: &[u8], inputs: Vec<u8>) -> Result<Vec<CanonicalCommand>> {
    use crate::Strategy;
    let inputs = tickwire_recovery::EncodedInputs::decode(inputs)?;
    if checkpoint.len() < 20 || &checkpoint[..8] != b"TWSC0001" {
        return Err(Error::Identity);
    }
    let maximum_age = u64::from_le_bytes(checkpoint[8..16].try_into().unwrap());
    let state_len = u32::from_le_bytes(checkpoint[16..20].try_into().unwrap()) as usize;
    let end = 20usize.checked_add(state_len).ok_or(Error::Bounds)?;
    let state = checkpoint.get(20..end).ok_or(Error::Bounds)?;
    if state.len() < 21 || &state[..8] != b"TWFD0001" || state[8] > 1 {
        return Err(Error::Identity);
    }
    let minimum = i64::from_le_bytes(state[9..17].try_into().unwrap());
    let command_len = u32::from_le_bytes(state[17..21].try_into().unwrap()) as usize;
    if state.len() != 21 + command_len {
        return Err(Error::Bounds);
    }
    let command = CanonicalCommand::decode(&state[21..])?;
    let mut strategy = crate::FixedDemo::new(command, minimum, state[8] == 1);
    let snapshot = checkpoint.get(end..).ok_or(Error::Bounds)?;
    if snapshot.len() < 28 || &snapshot[..8] != b"TWSB0001" {
        return Err(Error::Identity);
    }
    let now = u64::from_le_bytes(snapshot[8..16].try_into().unwrap());
    let levels = usize::try_from(u64::from_le_bytes(snapshot[16..24].try_into().unwrap()))
        .map_err(|_| Error::Bounds)?;
    let join_len = u32::from_le_bytes(snapshot[24..28].try_into().unwrap()) as usize;
    let boundary = 28usize.checked_add(join_len).ok_or(Error::Bounds)?;
    let join: crate::bootstrap::JoinBootstrap =
        serde_json::from_slice(snapshot.get(28..boundary).ok_or(Error::Bounds)?)?;
    let (_bank, resource, j, registry) = join.decode()?;
    let topic = tickwire_snapshot::validate(
        snapshot.get(boundary..).ok_or(Error::Bounds)?,
        &tickwire_snapshot::JoinContext {
            expected_resource: &resource,
            catalog_hash: registry.catalog_hash,
            instruments: &registry.instruments,
            join: &j,
            now_mono_ns: now,
        },
    )?;
    let mut replica = crate::MarketReplica::load(j.topic_id, j.ring_epoch, &topic, levels)?;
    let mut pos = 68;
    let mut output = Vec::new();
    let count = u32::from_be_bytes(inputs.bytes[32..36].try_into().unwrap());
    for _ in 0..count {
        let kind = inputs.bytes[pos + 8];
        let len = u32::from_be_bytes(inputs.bytes[pos + 9..pos + 13].try_into().unwrap()) as usize;
        let payload = &inputs.bytes[pos + 13..pos + 13 + len];
        pos += 13 + len;
        match kind {
            1 => {
                if payload.len() < 16 || &payload[..8] != b"TWMI0001" {
                    return Err(Error::Identity);
                }
                let r = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
                let p = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
                if r != 512 || payload.len() != 16 + r + p {
                    return Err(Error::Bounds);
                }
                replica.apply(&payload[16..16 + r], &payload[16 + r..])?;
            }
            2 => {
                if payload.len() < 29 || &payload[..8] != b"TWAI0001" {
                    return Err(Error::Identity);
                }
                let now = u64::from_le_bytes(payload[9..17].try_into().unwrap());
                let position = u64::from_le_bytes(payload[17..25].try_into().unwrap());
                let len = u32::from_le_bytes(payload[25..29].try_into().unwrap()) as usize;
                if payload.len() != 29 + len {
                    return Err(Error::Bounds);
                }
                let report = match payload[8] {
                    0 if position == 0 => crate::reports::AccountReport::Baseline(
                        tickwire_account_codec::CanonicalBaseline::decode(&payload[29..])
                            .map_err(|_| Error::Identity)?,
                    ),
                    1 => {
                        let record =
                            tickwire_account_codec::CanonicalRecord::decode(&payload[29..])
                                .map_err(|_| Error::Identity)?;
                        if position != record.sequence() {
                            return Err(Error::Identity);
                        }
                        crate::reports::AccountReport::Record { position, record }
                    }
                    _ => return Err(Error::Identity),
                };
                if let Some(command) = strategy.on_report(&report, now)? {
                    output.push(command);
                }
            }
            3 => {
                if payload.len() != 25 || &payload[..8] != b"TWDI0001" || payload[8] > 1 {
                    return Err(Error::Identity);
                }
                if payload[8] == 0 {
                    continue;
                }
                let id = u64::from_le_bytes(payload[9..17].try_into().unwrap());
                let now = u64::from_le_bytes(payload[17..25].try_into().unwrap());
                let view = replica.view(id).ok_or(Error::Identity)?;
                let received = view.received_mono_ns();
                if view.live()
                    && now >= received
                    && now - received <= maximum_age
                    && let Some(command) = strategy.on_market(view, now)?
                {
                    output.push(command);
                }
            }
            4 if payload == state => {}
            _ => return Err(Error::Identity),
        }
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "runtime")]
    #[test]
    fn execution_callback_preserves_canonical_record_position_and_actual_time() {
        let record = tickwire_account_codec::CanonicalRecord::new(
            [8; 32],
            9,
            tickwire_account::RecoveryRecord::Aborted {
                intent: tickwire_account::IntentKey {
                    epoch: 7,
                    intent_id: 3,
                },
                command_digest: [6; 32],
            },
        )
        .unwrap();
        let template = Template {
            dependencies: vec![],
            declared_rebuild: false,
            fault_model_id: [1; 32],
            pin_id: [2; 16],
            owner: [3; 16],
            input_stream: [4; 16],
            output_directory: PathBuf::from("/unused"),
            maximum_input_bytes: 4096,
            order_ids: vec![],
        };
        let mut writer = RecoveryWriter {
            template,
            dependencies: vec![],
            rows: vec![],
            bytes: 68,
            active: true,
        };
        writer
            .account_report(
                &crate::reports::AccountReport::Record {
                    position: 9,
                    record: record.clone(),
                },
                12345,
            )
            .unwrap();
        assert_eq!(writer.rows[0].kind, 2);
        let bytes = &writer.rows[0].bytes;
        assert_eq!(u64::from_le_bytes(bytes[9..17].try_into().unwrap()), 12345);
        assert_eq!(u64::from_le_bytes(bytes[17..25].try_into().unwrap()), 9);
        assert_eq!(
            tickwire_account_codec::CanonicalRecord::decode(&bytes[29..]).unwrap(),
            record
        );
        writer.template.maximum_input_bytes = writer.bytes;
        assert!(
            writer
                .account_report(
                    &crate::reports::AccountReport::Record {
                        position: 9,
                        record
                    },
                    12346
                )
                .is_err()
        );
        assert_eq!(writer.rows.len(), 1);
    }
    #[test]
    fn administrative_commands_need_no_new_order_identity() {
        let mut c = crate::test_command::command();
        c.body = CommandBody::Query(tickwire_venue::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(tickwire_venue::ClientId::new("old").unwrap()),
            venue_order_id: None,
        });
        assert_eq!(order_count(&CanonicalCommand::new(c).unwrap()), 0);
    }
    #[test]
    fn input_payloads_keep_complete_ring_and_external_arena_bytes() {
        let template = Template {
            dependencies: vec![],
            declared_rebuild: false,
            fault_model_id: [1; 32],
            pin_id: [2; 16],
            owner: [3; 16],
            input_stream: [4; 16],
            output_directory: PathBuf::from("/unused"),
            maximum_input_bytes: 4096,
            order_ids: vec![],
        };
        let mut writer = RecoveryWriter {
            template,
            dependencies: vec![],
            rows: vec![],
            bytes: 68,
            active: false,
        };
        writer.market(&[7; 512], &[9; 1024]).unwrap();
        let payload = &writer.rows[0].bytes;
        assert_eq!(&payload[16..528], &[7; 512]);
        assert_eq!(&payload[528..], &[9; 1024]);
        let encoded =
            encode_inputs([1; 16], 1, [0; 32], &[InputRecord { kind: 1, payload }]).unwrap();
        assert_eq!(&encoded.bytes[81..], payload.as_slice());
        assert_eq!(
            tickwire_recovery::EncodedInputs::decode(encoded.bytes)
                .unwrap()
                .boundary
                .last_sequence,
            1
        );
    }
}
