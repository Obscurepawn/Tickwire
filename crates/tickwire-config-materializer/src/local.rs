use super::*;
use tickwire_config::{
    materialized::{Kind, MAX_FILE_BYTES, ProjectionFile, Stamp},
    projection_object::ObjectBinding,
};
use tickwire_config_journal::{Event, OperationView, Phase, Scope};
const OBJECT_BYTES: usize = 4 * 1024 * 1024;
// Includes referenced projection objects and retained last-good files. Reserve
// two mutable files and one not-yet-recorded last-good file at their hard caps.
const RETAINED_BYTES: u64 = 512 * 1024 * 1024;
#[derive(Clone)]
struct Operation {
    prepared: Prepared,
    prepared_receipt: Receipt,
    desired: Option<(FileFact, Receipt)>,
    runtime: Option<(RuntimeFact, Receipt)>,
    effective: Option<(FileFact, Receipt)>,
}
impl Operation {
    fn from(view: OperationView<'_>) -> Self {
        let prepared = view.prepared().clone();
        let prepared_receipt = view
            .receipt(Phase::Prepared)
            .expect("validated ledger")
            .clone();
        let desired = match view.event(Phase::Desired) {
            Some(Event::Desired { file, .. }) => Some((
                file.clone(),
                view.receipt(Phase::Desired)
                    .expect("validated ledger")
                    .clone(),
            )),
            _ => None,
        };
        let runtime = match view.event(Phase::RuntimeObserved) {
            Some(Event::RuntimeObserved { fact, .. }) => Some((
                fact.clone(),
                view.receipt(Phase::RuntimeObserved)
                    .expect("validated ledger")
                    .clone(),
            )),
            _ => None,
        };
        let effective = match view.event(Phase::Effective) {
            Some(Event::Effective { file, .. }) => Some((
                file.clone(),
                view.receipt(Phase::Effective)
                    .expect("validated ledger")
                    .clone(),
            )),
            _ => None,
        };
        Self {
            prepared,
            prepared_receipt,
            desired,
            runtime,
            effective,
        }
    }
    fn file(&self, kind: Kind) -> Option<&(FileFact, Receipt)> {
        match kind {
            Kind::Desired => self.desired.as_ref(),
            Kind::Effective => self.effective.as_ref(),
        }
    }
}
impl<F: Files, S: JournalStore> Local<F, S> {
    pub fn open(files: F, journal: Journal<S>) -> Result<Self> {
        let mut local = Self {
            files,
            journal,
            poisoned: false,
        };
        local.verify_all()?;
        local.files.create_directory("objects")?;
        local.files.create_directory("last-good")?;
        Ok(local)
    }
    pub fn prepare(&mut self, prepared: Prepared, object: &ProjectionObject) -> Result<Receipt> {
        self.healthy()?;
        let event = Event::Prepared(Box::new(prepared.clone()));
        self.journal.validate_event(&event)?;
        validate_object(self.journal.scope(), &prepared, object)?;
        let existing = self
            .journal
            .operation(prepared.operation)
            .map(Operation::from);
        if existing.is_none() {
            self.capacity(Some(&prepared))?;
        }
        self.perform(|local| {
            local.verify_all()?;
            if let Some(existing) = existing {
                return Ok(existing.prepared_receipt);
            }
            local.files.install(
                &object_path(prepared.next.object_hash),
                object.bytes(),
                None,
            )?;
            Ok(local.journal.record(&event)?)
        })
    }
    pub fn write_desired(&mut self, id: Identity) -> Result<Receipt> {
        self.healthy()?;
        let op = self.operation(id)?;
        self.perform(|local| {
            local.verify_all()?;
            if let Some((_, receipt)) = &op.desired {
                return Ok(receipt.clone());
            }
            let file = local.render(&op, Kind::Desired)?;
            let previous = local
                .latest(Kind::Desired)
                .and_then(|o| o.desired.map(|f| f.0.render_hash));
            let fact = local
                .files
                .install("desired.toml", file.bytes(), previous)?;
            if fact.render_hash != file.render_hash() || fact.bytes != file.bytes().len() as u64 {
                return Err(Error::Drift);
            }
            Ok(local.journal.record(&Event::Desired {
                operation: id,
                prepared_ref: op.prepared_receipt.record_hash(),
                file: fact,
            })?)
        })
    }
    pub fn observe_runtime(&mut self, id: Identity, fact: RuntimeFact) -> Result<Receipt> {
        self.healthy()?;
        let op = self.operation(id)?;
        let desired = op.desired.as_ref().ok_or(Error::Conflict)?;
        let event = Event::RuntimeObserved {
            operation: id,
            desired_ref: desired.1.record_hash(),
            fact,
        };
        self.journal.validate_event(&event)?;
        self.perform(|local| {
            local.verify_all()?;
            Ok(local.journal.record(&event)?)
        })
    }
    pub fn write_effective(&mut self, id: Identity) -> Result<Receipt> {
        self.healthy()?;
        let op = self.operation(id)?;
        let runtime = op.runtime.as_ref().ok_or(Error::Conflict)?;
        let runtime_ref = runtime.1.record_hash();
        self.perform(|local| {
            local.verify_all()?;
            if let Some((_, receipt)) = &op.effective {
                return Ok(receipt.clone());
            }
            let file = local.render(&op, Kind::Effective)?;
            local
                .files
                .install(&last_good_path(&op.prepared), file.bytes(), None)?;
            let previous = local
                .latest(Kind::Effective)
                .and_then(|o| o.effective.map(|f| f.0.render_hash));
            let fact = local
                .files
                .install("effective.toml", file.bytes(), previous)?;
            if fact.render_hash != file.render_hash() || fact.bytes != file.bytes().len() as u64 {
                return Err(Error::Drift);
            }
            Ok(local.journal.record(&Event::Effective {
                operation: id,
                runtime_ref,
                file: fact,
            })?)
        })
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn perform<T>(&mut self, action: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let result = action(self);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn operation(&self, id: Identity) -> Result<Operation> {
        self.journal
            .operation(id)
            .map(Operation::from)
            .ok_or(Error::Conflict)
    }
    fn latest(&self, kind: Kind) -> Option<Operation> {
        self.journal
            .operations()
            .map(Operation::from)
            .filter(|o| o.file(kind).is_some())
            .max_by_key(|o| o.file(kind).expect("filtered").1.sequence())
    }
    fn capacity(&self, additional: Option<&Prepared>) -> Result<()> {
        let mut bytes = 3 * MAX_FILE_BYTES as u64;
        for op in self.journal.operations().map(Operation::from) {
            bytes = bytes
                .checked_add(op.prepared.projection_object_bytes)
                .and_then(|n| n.checked_add(op.effective.map_or(0, |e| e.0.bytes)))
                .ok_or(Error::Invalid("retained file capacity"))?;
        }
        if let Some(p) = additional {
            bytes = bytes
                .checked_add(p.projection_object_bytes)
                .ok_or(Error::Invalid("retained file capacity"))?;
        }
        if bytes > RETAINED_BYTES {
            return Err(Error::Invalid("retained file capacity"));
        }
        Ok(())
    }
    fn object(&mut self, p: &Prepared) -> Result<ProjectionObject> {
        let bytes = self
            .files
            .read(&object_path(p.next.object_hash), OBJECT_BYTES)?
            .ok_or(Error::Drift)?;
        let scope = self.journal.scope();
        let binding = ObjectBinding {
            revision: p.next.revision,
            global_hash: p.next.global_hash,
            policy_revision: p.next.policy_revision,
            process_id: scope.process.clone(),
            projection_hash: p.next.projection_hash,
            object_hash: p.next.object_hash,
        };
        let object = ProjectionObject::decode(&bytes, &binding, OBJECT_BYTES)?;
        validate_object(scope, p, &object)?;
        Ok(object)
    }
    fn render(&mut self, op: &Operation, kind: Kind) -> Result<ProjectionFile> {
        let object = self.object(&op.prepared)?;
        let (boot, seq, journal_hash) = match kind {
            Kind::Desired => (
                op.prepared.expected_boot,
                0,
                op.prepared_receipt.record_hash(),
            ),
            Kind::Effective => {
                let (fact, receipt) = op.runtime.as_ref().ok_or(Error::Conflict)?;
                (fact.boot, fact.applied_event_seq, receipt.record_hash())
            }
        };
        Ok(ProjectionFile::render(
            &object,
            &Stamp {
                kind,
                operation_id: uuid(op.prepared.operation),
                control_commit_ref: reference("control", op.prepared.control_commit_ref),
                process_boot_id: uuid(boot),
                applied_event_seq: seq,
                journal_receipt_ref: reference("journal", journal_hash),
                instance_slot_id: uuid(self.journal.scope().slot),
            },
            MAX_FILE_BYTES,
        )?)
    }
    fn verify_all(&mut self) -> Result<()> {
        self.capacity(None)?;
        let operations: Vec<_> = self.journal.operations().map(Operation::from).collect();
        for op in &operations {
            self.object(&op.prepared)?;
            if let Some((fact, _)) = &op.effective {
                let file = self.render(op, Kind::Effective)?;
                let path = last_good_path(&op.prepared);
                let bytes = self
                    .files
                    .read(&path, MAX_FILE_BYTES)?
                    .ok_or(Error::Drift)?;
                if bytes != file.bytes()
                    || fact.render_hash != file.render_hash()
                    || fact.bytes != bytes.len() as u64
                {
                    return Err(Error::Drift);
                }
                // This exact-byte path only synchronizes the existing file. It
                // cannot recreate a missing historical last-good snapshot.
                self.files.install(&path, &bytes, Some(fact.render_hash))?;
            }
        }
        self.verify_current(Kind::Desired)?;
        self.verify_current(Kind::Effective)
    }
    fn verify_current(&mut self, kind: Kind) -> Result<()> {
        let path = match kind {
            Kind::Desired => "desired.toml",
            Kind::Effective => "effective.toml",
        };
        let actual = self.files.read(path, MAX_FILE_BYTES)?;
        let latest = self.latest(kind);
        if actual.is_none() {
            return if latest.is_none() {
                Ok(())
            } else {
                Err(Error::Drift)
            };
        }
        let actual = actual.expect("checked presence");
        if let Some(op) = latest {
            let expected = self.render(&op, kind)?;
            if actual == expected.bytes() {
                let (fact, _) = op.file(kind).expect("latest matching phase");
                let actual_fact = self.files.install(path, &actual, Some(fact.render_hash))?;
                return if &actual_fact == fact {
                    Ok(())
                } else {
                    Err(Error::Drift)
                };
            }
        }
        let pending = self
            .journal
            .active_operation()
            .map(|id| self.operation(id))
            .transpose()?;
        if let Some(op) = pending {
            let eligible = match kind {
                Kind::Desired => op.desired.is_none(),
                Kind::Effective => op.runtime.is_some() && op.effective.is_none(),
            };
            if eligible && actual == self.render(&op, kind)?.bytes() {
                return Ok(());
            }
        }
        Err(Error::Drift)
    }
}
fn validate_object(scope: &Scope, p: &Prepared, object: &ProjectionObject) -> Result<()> {
    let b = object.binding();
    let v = &p.next;
    if b.process_id != scope.process
        || b.revision != v.revision
        || b.global_hash != v.global_hash
        || b.projection_hash != v.projection_hash
        || b.object_hash != v.object_hash
        || b.policy_revision != v.policy_revision
        || object.bytes().len() as u64 != p.projection_object_bytes
        || object.payload()["identity"]["cluster_id"] != scope.cluster
        || object.payload()["identity"]["namespace_id"] != uuid(scope.namespace)
        || object.payload()["process"]["host_id"] != scope.host
    {
        return Err(Error::Invalid("projection scope or binding"));
    }
    Ok(())
}
fn hex(h: Hash) -> String {
    h.iter().map(|v| format!("{v:02x}")).collect()
}
fn object_path(hash: Hash) -> String {
    format!("objects/{}.json", hex(hash))
}
fn last_good_path(p: &Prepared) -> String {
    format!(
        "last-good/{}-{}.toml",
        p.next.revision,
        hex(p.next.global_hash)
    )
}
fn reference(kind: &str, hash: Hash) -> String {
    format!("artifact://config/{kind}/record@sha256:{}", hex(hash))
}
fn uuid(id: Identity) -> String {
    let s: String = id.iter().map(|v| format!("{v:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &s[..8],
        &s[8..12],
        &s[12..16],
        &s[16..20],
        &s[20..]
    )
}
