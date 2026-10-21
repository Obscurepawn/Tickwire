//! Actual callback completion bound to ordered reports and synchronized strategy inputs.
use crate::{Error, Result, recovery, reports::AccountReport};
use std::path::Path;
use tickwire_ipc::HandoverReadyBody;
pub(crate) use tickwire_recovery::HandoverSeal as InputSeal;
use tickwire_recovery::{MAX_OBJECT_BYTES, digest};
#[derive(Default)]
pub struct AccountMirror {
    baseline: Option<tickwire_account_codec::CanonicalBaseline>,
    records: Vec<tickwire_account::RecoveryRecord>,
    position: u64,
    bytes: usize,
    seal: Option<tickwire_account_codec::InputSeal>,
}
impl AccountMirror {
    pub fn position(&self) -> Option<u64> {
        self.baseline.as_ref().map(|_| self.position)
    }
    pub fn observe(&mut self, report: &AccountReport) -> Result<()> {
        match report {
            AccountReport::Baseline(b) => {
                if self.baseline.is_some() {
                    return Err(Error::Identity);
                }
                self.baseline = Some(b.clone());
                self.bytes = b.bytes().len();
                self.seal = Some(
                    tickwire_account_codec::InputSeal::new(b.digest())
                        .map_err(|_| Error::Identity)?,
                );
            }
            AccountReport::Record { position, record } => {
                if self.baseline.as_ref().map(|b| b.digest()) != Some(record.baseline())
                    || *position != self.position.checked_add(1).ok_or(Error::Bounds)?
                {
                    return Err(Error::Identity);
                }
                self.bytes = self
                    .bytes
                    .checked_add(record.bytes().len())
                    .filter(|n| *n <= MAX_OBJECT_BYTES)
                    .ok_or(Error::Bounds)?;
                self.seal
                    .as_mut()
                    .ok_or(Error::NotReady)?
                    .apply(record)
                    .map_err(|_| Error::Identity)?;
                self.records.push(record.record().clone());
                self.position = *position;
            }
        }
        Ok(())
    }
    pub fn ready(
        &self,
        identity: &crate::bootstrap::IdentityConfig,
        ring_sequence: u64,
        ready_sequence: u64,
        writer: &recovery::RecoveryWriter,
        state: &[u8],
    ) -> Result<HandoverReadyBody> {
        let b = self.baseline.as_ref().ok_or(Error::NotReady)?;
        let account = tickwire_account::Account::recover(
            b.snapshot().clone(),
            b.catalog().clone(),
            &self.records,
        )
        .map_err(|_| Error::Identity)?;
        writer.seal_handover(
            HandoverReadyBody {
                instance_id: identity.instance,
                logical_strategy_id: identity.logical_strategy,
                strategy_generation: identity.generation,
                exec_report_sequence: self.position,
                report_ring_sequence: ring_sequence,
                ledger_sequence: account.ledger_sequence(),
                orders_digest: account.strategy_orders_digest(identity.logical_strategy),
                ready_sequence,
                ..Default::default()
            },
            state,
            self.seal.as_ref().ok_or(Error::NotReady)?,
        )
    }
}
fn object(root: &Path, hash: [u8; 32], length: Option<u64>) -> Result<Vec<u8>> {
    if hash == [0; 32] || length.is_some_and(|n| n == 0 || n > MAX_OBJECT_BYTES as u64) {
        return Err(Error::Bounds);
    }
    let name = hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let bytes = recovery::read(&root.join(format!("{name}.bin")), MAX_OBJECT_BYTES)?;
    if digest(&bytes) != hash || length.is_some_and(|n| n != bytes.len() as u64) {
        return Err(Error::Identity);
    }
    Ok(bytes)
}
/// Validate the full immutable closure named by the authenticated worker. The
/// host separately checks actual report cursor progress and the requested cut.
pub fn objects(root: &Path, b: &HandoverReadyBody) -> Result<([u8; 32], Vec<Vec<u8>>)> {
    if b.instance_id == [0; 16]
        || b.logical_strategy_id == 0
        || b.strategy_generation == 0
        || b.ready_sequence == 0
        || b.report_ring_sequence == 0
        || b.orders_digest == [0; 32]
    {
        return Err(Error::Identity);
    }
    let bytes = object(root, b.input_seal_digest, Some(b.input_seal_bytes))?;
    let seal = InputSeal::decode(&bytes)?;
    if seal.owner != b.instance_id
        || seal.instance != b.instance_id
        || seal.generation != b.strategy_generation
        || seal.logical_strategy != b.logical_strategy_id
        || seal.report_position != b.exec_report_sequence
        || seal.ledger_sequence != b.ledger_sequence
        || seal.orders_digest != b.orders_digest
        || seal.checkpoint_digest != b.checkpoint_digest
        || seal.checkpoint_bytes != b.checkpoint_bytes
        || seal.dependencies.len() != 10
    {
        return Err(Error::Identity);
    }
    let checkpoint = object(root, b.checkpoint_digest, Some(b.checkpoint_bytes))?;
    seal.verify_checkpoint(&checkpoint)?;
    let inputs = object(root, seal.input_digest, Some(seal.input_bytes))?;
    let account = object(
        root,
        seal.account_input_digest,
        Some(seal.account_input_bytes),
    )?;
    tickwire_handover::verify(&seal, &checkpoint, &inputs, &account)
        .map_err(|_| Error::Identity)?;
    let mut out = vec![bytes, checkpoint, inputs, account];
    for (i, (kind, hash)) in seal.dependencies.iter().enumerate() {
        if *kind != i as u16 + 2 {
            return Err(Error::Identity);
        }
        out.push(object(root, *hash, None)?);
    }
    if let Some(preflight) = seal.preflight_receipt_digest {
        let verified = tickwire_release::closure::verify(preflight, |h| {
            object(root, h, None).map_err(|_| tickwire_release::Error::Missing)
        })?;
        if verified.completion.member.new_instance != seal.instance
            || verified.completion.member.new_generation != seal.generation
            || verified.completion.logical_strategy != seal.logical_strategy
        {
            return Err(Error::Identity);
        }
        out.extend(verified.objects);
    }
    if seal.version == 3 {
        let objects =
            tickwire_release::replay::startup::verify_seal(&seal, &out[1], &out[2], |h| {
                object(root, h, None).map_err(|_| tickwire_release::Error::Missing)
            })?;
        out.extend(objects);
    }

    if out.iter().map(Vec::len).sum::<usize>() > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
        return Err(Error::Bounds);
    }
    Ok((seal.account_input_digest, out))
}
pub fn verify(root: &Path, b: &HandoverReadyBody) -> Result<[u8; 32]> {
    Ok(objects(root, b)?.0)
}
#[cfg(feature = "runtime")]
pub fn matches(b: &HandoverReadyBody, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
    b.instance_id == r.instance_id
        && b.strategy_generation == r.strategy_generation
        && b.checkpoint_digest == r.checkpoint_digest
        && b.exec_report_sequence == r.exec_report_sequence
        && b.orders_digest == r.orders_digest
        && b.ledger_sequence == r.ledger_sequence
        && b.input_seal_digest == r.input_seal_digest
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_fixture as fixture;
    #[test]
    fn objects_rejects_rehashed_inputs_without_the_account_callback() {
        let directory = std::env::temp_dir().join(format!(
            "tickwire-handover-objects-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let put = |bytes: &[u8]| recovery::object(&directory, bytes).unwrap().1;
        let (snapshot, catalog, _) = fixture::fixture();
        let account =
            tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        let account_input = tickwire_account_codec::InputSeal::new(baseline.digest()).unwrap();
        let mut body = HandoverReadyBody {
            instance_id: [1; 16],
            logical_strategy_id: 8,
            strategy_generation: 2,
            exec_report_sequence: 0,
            report_ring_sequence: 1,
            ready_sequence: 1,
            ledger_sequence: account.ledger_sequence(),
            orders_digest: account.strategy_orders_digest(8),
            ..Default::default()
        };
        let mut checkpoint = b"TWHS0001".to_vec();
        checkpoint.extend(body.instance_id);
        for v in [
            body.strategy_generation,
            body.logical_strategy_id,
            body.exec_report_sequence,
            body.ledger_sequence,
        ] {
            checkpoint.extend(v.to_le_bytes());
        }
        checkpoint.extend(body.orders_digest);
        checkpoint.extend(1u64.to_le_bytes());
        checkpoint.push(1);
        body.checkpoint_digest = put(&checkpoint);
        body.checkpoint_bytes = checkpoint.len() as u64;
        let mut callback = b"TWAI0001".to_vec();
        callback.push(0);
        callback.extend(70u64.to_le_bytes());
        callback.extend(0u64.to_le_bytes());
        callback.extend((baseline.bytes().len() as u32).to_le_bytes());
        callback.extend(baseline.bytes());
        let inputs = tickwire_recovery::encode_inputs(
            [3; 16],
            1,
            [0; 32],
            &[tickwire_recovery::InputRecord {
                kind: 2,
                payload: &callback,
            }],
        )
        .unwrap();
        let dep = put(baseline.bytes());
        let mut seal = InputSeal {
            preflight_receipt_digest: None,
            replay_installation_digest: None,
            replay_restore_digest: None,
            replay_rollback_digest: None,
            version: 1,
            owner: body.instance_id,
            instance: body.instance_id,
            generation: body.strategy_generation,
            logical_strategy: body.logical_strategy_id,
            report_position: body.exec_report_sequence,
            ledger_sequence: body.ledger_sequence,
            orders_digest: body.orders_digest,
            checkpoint_digest: body.checkpoint_digest,
            checkpoint_bytes: body.checkpoint_bytes,
            account_input_digest: put(account_input.object_bytes()),
            account_input_bytes: account_input.object_bytes().len() as u64,
            input_digest: put(&inputs.bytes),
            input_bytes: inputs.bytes.len() as u64,
            dependencies: (2..=11).map(|kind| (kind, dep)).collect(),
        };
        let bytes = seal.bytes().unwrap();
        body.input_seal_digest = put(&bytes);
        body.input_seal_bytes = bytes.len() as u64;
        assert_eq!(objects(&directory, &body).unwrap().1.len(), 14);
        let missing = tickwire_recovery::encode_inputs([3; 16], 1, [0; 32], &[]).unwrap();
        seal.input_digest = put(&missing.bytes);
        seal.input_bytes = missing.bytes.len() as u64;
        let bytes = seal.bytes().unwrap();
        body.input_seal_digest = put(&bytes);
        body.input_seal_bytes = bytes.len() as u64;
        assert!(objects(&directory, &body).is_err());
    }
    #[test]
    fn account_mirror_seals_ordered_callbacks_and_rejects_missing_or_replayed_records() {
        let (snapshot, catalog, _) = fixture::fixture();
        let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        let record = tickwire_account_codec::CanonicalRecord::new(
            baseline.digest(),
            1,
            tickwire_account::RecoveryRecord::Aborted {
                intent: tickwire_account::IntentKey {
                    epoch: 7,
                    intent_id: 3,
                },
                command_digest: [6; 32],
            },
        )
        .unwrap();
        let report = AccountReport::Record {
            position: 1,
            record: record.clone(),
        };
        let mut mirror = AccountMirror::default();
        assert!(mirror.observe(&report).is_err());
        mirror
            .observe(&AccountReport::Baseline(baseline.clone()))
            .unwrap();
        let mut expected = tickwire_account_codec::InputSeal::new(baseline.digest()).unwrap();
        assert_eq!(mirror.seal.as_ref().unwrap().digest(), expected.digest());
        mirror.observe(&report).unwrap();
        expected.apply(&record).unwrap();
        assert_eq!(mirror.seal.as_ref().unwrap().digest(), expected.digest());
        assert!(mirror.observe(&report).is_err());
        assert_eq!(mirror.position, 1);
        assert_eq!(mirror.records.len(), 1);
        assert!(mirror.observe(&AccountReport::Baseline(baseline)).is_err());
    }
    #[test]
    fn checkpoint_requires_exact_instance_cut_and_nonempty_strategy_state() {
        let b = HandoverReadyBody {
            instance_id: [1; 16],
            logical_strategy_id: 2,
            strategy_generation: 3,
            exec_report_sequence: 4,
            ledger_sequence: 5,
            orders_digest: [6; 32],
            ..Default::default()
        };
        let mut bytes = b"TWHS0001".to_vec();
        bytes.extend(b.instance_id);
        for n in [
            b.strategy_generation,
            b.logical_strategy_id,
            b.exec_report_sequence,
            b.ledger_sequence,
        ] {
            bytes.extend(n.to_le_bytes());
        }
        bytes.extend(b.orders_digest);
        bytes.extend(3u64.to_le_bytes());
        bytes.extend([1, 2, 3]);
        let seal = InputSeal {
            preflight_receipt_digest: None,
            replay_installation_digest: None,
            replay_restore_digest: None,
            replay_rollback_digest: None,
            version: 1,
            owner: b.instance_id,
            instance: b.instance_id,
            generation: b.strategy_generation,
            logical_strategy: b.logical_strategy_id,
            report_position: b.exec_report_sequence,
            ledger_sequence: b.ledger_sequence,
            orders_digest: b.orders_digest,
            checkpoint_digest: digest(&bytes),
            checkpoint_bytes: bytes.len() as u64,
            account_input_digest: [7; 32],
            account_input_bytes: 64,
            input_digest: [8; 32],
            input_bytes: 100,
            dependencies: (2..=11).map(|kind| (kind, [kind as u8; 32])).collect(),
        };
        assert!(seal.verify_checkpoint(&bytes).is_ok());
        for offset in [0, 8, 24, 32, 40, 48, 56, 88] {
            let mut wrong = bytes.clone();
            wrong[offset] ^= 1;
            assert!(seal.verify_checkpoint(&wrong).is_err());
        }
        bytes.truncate(96);
        bytes[88..96].copy_from_slice(&0u64.to_le_bytes());
        assert!(seal.verify_checkpoint(&bytes).is_err());
    }
}
