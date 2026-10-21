//! One owned immutable closure, bound to the complete authenticated READY and
//! its live control session. It carries content, never publication authority.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Context {
    pub identity: Identity,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
    pub epoch: u64,
    pub deadline: u64,
}
impl Context {
    pub fn from_lane(lane: &AuthenticatedChild) -> Self {
        Self {
            identity: lane.session.peer().clone(),
            pid: lane.process.pid,
            uid: lane.process.uid,
            gid: lane.process.gid,
            epoch: lane.session.epoch(),
            deadline: lane.session.deadline(),
        }
    }
    pub fn live(&self, now: u64) -> bool {
        self.pid != 0 && self.epoch != 0 && now < self.deadline
    }
}
struct Entry {
    directory: PathBuf,
    context: Context,
    body: HandoverReadyBody,
    account: [u8; 32],
    objects: Vec<Vec<u8>>,
}
#[derive(Default)]
pub(super) struct Cache {
    entry: Option<Entry>,
}
impl Cache {
    pub fn clear(&mut self) {
        self.entry = None;
    }
    pub fn load(
        &mut self,
        directory: &Path,
        context: &Context,
        body: &HandoverReadyBody,
        now: u64,
    ) -> Result<([u8; 32], Vec<Vec<u8>>)> {
        if !context.live(now)
            || context.identity.instance != body.instance_id
            || context.identity.generation != body.strategy_generation
            || context.identity.logical_strategy != body.logical_strategy_id
        {
            self.clear();
            return Err(Error::Invalid);
        }
        if let Some(entry) = &self.entry
            && entry.directory == directory
            && entry.context == *context
            && entry.body == *body
        {
            return Ok((entry.account, entry.objects.clone()));
        }
        self.clear();
        #[cfg(test)]
        LOADS.with(|n| n.set(n.get() + 1));
        let (account, objects) = tickwire_strategy_sdk::handover::objects(directory, body)?;
        check_bounds(&objects)?;
        self.entry = Some(Entry {
            directory: directory.to_owned(),
            context: context.clone(),
            body: body.clone(),
            account,
            objects,
        });
        let entry = self.entry.as_ref().ok_or(Error::Invalid)?;
        Ok((entry.account, entry.objects.clone()))
    }
}
fn check_bounds(objects: &[Vec<u8>]) -> Result<()> {
    if objects.len() > 512 {
        return Err(Error::Invalid);
    }
    let mut total = 0usize;
    for b in objects {
        if b.is_empty() || b.len() > tickwire_recovery::MAX_OBJECT_BYTES {
            return Err(Error::Invalid);
        }
        total = total
            .checked_add(b.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Invalid)?;
    }
    Ok(())
}
#[cfg(test)]
thread_local! { static LOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires actual fourth-round objects via TICKWIRE_READY_FIXTURE"]
    fn actual_ready_closure_is_loaded_once_per_body_and_context() {
        let root = PathBuf::from(std::env::var_os("TICKWIRE_READY_FIXTURE").unwrap());
        let raw: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                root.join("next/reports/CONSUMED-06060606060606060606060606060606-6.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let get = |key: &str| serde_json::from_value::<[u8; 32]>(raw[key].clone()).unwrap();
        let seal_digest = get("input_seal_digest");
        let name = seal_digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let seal_bytes = std::fs::read(root.join(format!("next/recovery/{name}.bin"))).unwrap();
        let seal = tickwire_recovery::HandoverSeal::decode(&seal_bytes).unwrap();
        let body = HandoverReadyBody {
            instance_id: seal.instance,
            logical_strategy_id: seal.logical_strategy,
            strategy_generation: seal.generation,
            checkpoint_digest: seal.checkpoint_digest,
            checkpoint_bytes: seal.checkpoint_bytes,
            input_seal_digest: seal_digest,
            input_seal_bytes: seal_bytes.len() as u64,
            exec_report_sequence: seal.report_position,
            ledger_sequence: seal.ledger_sequence,
            orders_digest: seal.orders_digest,
            report_ring_sequence: raw["report_sequence"].as_u64().unwrap(),
            ready_sequence: raw["ready_sequence"].as_u64().unwrap(),
        };
        let identity = Identity {
            instance: seal.instance,
            generation: seal.generation,
            logical_strategy: seal.logical_strategy,
            role: 3,
            namespace: [8; 16],
            qualification_hash: [8; 32],
            manifest_hash: [8; 32],
            arch: 1,
            trust_profile: 1,
            build_hash: get("build_hash"),
            host_boot: serde_json::from_value(raw["host_boot"].clone()).unwrap(),
        };
        let ctx = Context {
            identity,
            pid: raw["worker_pid"].as_u64().unwrap() as u32,
            uid: raw["worker_uid"].as_u64().unwrap() as u32,
            gid: raw["worker_gid"].as_u64().unwrap() as u32,
            epoch: 1,
            deadline: u64::MAX,
        };
        let mut cache = Cache::default();
        LOADS.with(|n| n.set(0));
        let start = Instant::now();
        let first = cache
            .load(&root.join("next/recovery"), &ctx, &body, 1)
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                cache
                    .load(&root.join("next/recovery"), &ctx, &body, 1)
                    .unwrap(),
                first
            );
        }
        let count = LOADS.with(|n| n.get());
        eprintln!(
            "actual READY: {} objects, {} bytes, {count} complete loads, {:?}",
            first.1.len(),
            first.1.iter().map(Vec::len).sum::<usize>(),
            start.elapsed()
        );
        assert_eq!(
            count, 1,
            "one immutable authenticated body must not reload the complete closure"
        );

        // Work only on a private copy. A previously verified owned Vec remains
        // its original content after a file changes; new bodies must reverify.
        let directory = std::env::temp_dir().join(format!(
            "tickwire-ready-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        for bytes in &first.1 {
            let hex = tickwire_recovery::digest(bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            std::fs::write(directory.join(format!("{hex}.bin")), bytes).unwrap();
        }
        assert_eq!(cache.load(&directory, &ctx, &body, 1).unwrap(), first);
        let changed = first.1.iter().max_by_key(|b| b.len()).unwrap();
        let hex = tickwire_recovery::digest(changed)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = directory.join(format!("{hex}.bin"));
        std::fs::write(&path, b"different bytes under the original digest name").unwrap();
        assert_eq!(cache.load(&directory, &ctx, &body, 1).unwrap(), first);
        let mut newer = body.clone();
        newer.ready_sequence += 1;
        assert!(cache.load(&directory, &ctx, &newer, 1).is_err());
        assert!(cache.entry.is_none());
        assert!(
            cache.load(&directory, &ctx, &body, 1).is_err(),
            "failed replacement cannot recover the evicted older entry"
        );
        std::fs::write(&path, changed).unwrap();
        assert_eq!(cache.load(&directory, &ctx, &body, 1).unwrap(), first);
        let mut other = ctx.clone();
        other.epoch += 1;
        std::fs::write(&path, b"invalid after session replacement").unwrap();
        assert!(cache.load(&directory, &other, &body, 1).is_err());
        std::fs::write(&path, changed).unwrap();
        cache.load(&directory, &ctx, &body, 1).unwrap();
        assert!(cache.load(&directory, &ctx, &body, ctx.deadline).is_err());
        assert!(cache.entry.is_none());
        let mut wrong = ctx;
        wrong.identity.instance = [9; 16];
        assert!(cache.load(&directory, &wrong, &body, 1).is_err());
    }

    #[test]
    fn closure_capacity_rejects_empty_and_too_many_objects() {
        assert!(check_bounds(&[vec![]]).is_err());
        assert!(check_bounds(&vec![vec![1]; 513]).is_err());
        assert!(check_bounds(&vec![vec![1]; 512]).is_ok());
        assert!(check_bounds(&[vec![0; tickwire_recovery::MAX_OBJECT_BYTES + 1]]).is_err());
    }
}
