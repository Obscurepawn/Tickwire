//! Preserve the actual warmed instance's history without executing callbacks twice.
use super::*;
use crate::reports::AccountReport;
use tickwire_release::{Object, rebuild::install, replay};
impl RecoveryWriter {
    pub fn from_rebuild<S: crate::Strategy>(
        template: Template,
        capture: &crate::replay::rebuild::capture::VerifiedWindow,
        live: &crate::replay::live::Live<S>,
        installation: &install::Verified,
        objects: &[Vec<u8>],
    ) -> Result<(Self, crate::handover::AccountMirror)> {
        let installed = installation.installation();
        let (checkpoint, commit) = live.committed().ok_or(Error::Identity)?;
        let anchor = live.anchor_bytes()?.ok_or(Error::Identity)?;
        if !template.declared_rebuild
            || template.owner != installed.target.instance
            || template.input_stream != installed.target.input_stream
            || checkpoint != installation.checkpoint()
            || commit != installed.commit
            || Object::of(&anchor) != installed.anchor
            || Object::of(&capture.seal().bytes()?) != installed.capture_seal
            || capture.seal().identity.instance != installed.target.instance
            || capture.seal().identity.generation != installed.target.generation
            || capture.inputs().accounts.len() != 1
        {
            return Err(Error::Identity);
        }
        let anchor_checkpoint = replay::Checkpoint::decode(&anchor)?;
        let state = crate::replay::application::business_bytes(&anchor_checkpoint.business)?;
        let mut writer = Self::new(
            template,
            &state,
            &capture.seed().bytes()?,
            capture.contract().maximum_age_ns,
        )?;
        let mut mirror = crate::handover::AccountMirror::default();
        let prefix = &capture.seed().accounts[0];
        let baseline = tickwire_account_codec::CanonicalBaseline::decode(&prefix.baseline)
            .map_err(|_| Error::Identity)?;
        let stream = replay::StreamKey {
            identity: baseline.digest(),
            generation: baseline.snapshot().account_epoch,
        };
        let report = AccountReport::Baseline(baseline);
        writer.account_report(&report, capture.seed().start_ns)?;
        mirror.observe(&report)?;
        for bytes in &prefix.records {
            let record = tickwire_account_codec::CanonicalRecord::decode(bytes)
                .map_err(|_| Error::Identity)?;
            let report = AccountReport::Record {
                position: record.sequence(),
                record,
            };
            writer.account_report(&report, capture.seed().start_ns)?;
            mirror.observe(&report)?;
        }
        for row in &capture.inputs().warm {
            if let replay::Input::AccountCallback {
                stream: actual,
                position,
                canonical,
            } = &row.input
            {
                if actual != &stream || *position == 0 {
                    return Err(Error::Identity);
                }
                let record = tickwire_account_codec::CanonicalRecord::decode(canonical)
                    .map_err(|_| Error::Identity)?;
                let report = AccountReport::Record {
                    position: *position,
                    record,
                };
                writer.account_report(&report, row.now_ns)?;
                mirror.observe(&report)?;
            }
        }
        if mirror.position() != Some(installation.completion().account_position) {
            return Err(Error::Identity);
        }
        writer.merged_anchor(&anchor)?;
        let rows = live.recorded_after(0);
        if rows.len() != capture.inputs().warm.len()
            || rows.last().map(|r| r.sequence) != Some(checkpoint.cut.sequence)
        {
            return Err(Error::Identity);
        }
        for row in rows {
            writer.merged_row(row)?;
        }
        writer.merged_checkpoint(checkpoint)?;
        let mut closure = std::collections::BTreeMap::<[u8; 32], &[u8]>::new();
        let mut total = 0usize;
        for bytes in installation
            .objects()
            .chain(objects.iter().map(Vec::as_slice))
            .chain(capture.objects())
        {
            if bytes.is_empty() || bytes.len() > MAX_OBJECT_BYTES {
                return Err(Error::Bounds);
            }
            let key = digest(bytes);
            if !closure.contains_key(&key) && closure.len() >= 512 {
                return Err(Error::Bounds);
            }
            if let std::collections::btree_map::Entry::Vacant(entry) = closure.entry(key) {
                total = total
                    .checked_add(bytes.len())
                    .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                    .ok_or(Error::Bounds)?;
                entry.insert(bytes);
            }
        }
        // Verify from the complete bytes that will actually be copied.
        install::verify(installation.root(), |hash| {
            closure
                .get(&hash)
                .map(|b| b.to_vec())
                .ok_or(tickwire_release::Error::Missing)
        })?;
        for bytes in closure.values() {
            object(&writer.template.output_directory, bytes)?;
        }
        writer.rebuild_installation = Some(installation.root().digest);
        writer.set_active();
        Ok((writer, mirror))
    }
    pub(crate) fn install_rebuild_rollback(
        &mut self,
        installed: &install::Verified,
        completed: &tickwire_release::rebuild::probe::Verified,
    ) -> Result<()> {
        if !self.active
            || self.rebuild_installation != Some(installed.root().digest)
            || installed.installation().target.instance != self.template.owner
            || completed.job().operation != installed.request().operation
            || completed.job().member != installed.request().member
            || self
                .rebuild_rollback
                .is_some_and(|root| root != completed.root().digest)
        {
            return Err(Error::Identity);
        }
        for bytes in completed.objects() {
            object(&self.template.output_directory, bytes)?;
        }
        self.rebuild_rollback = Some(completed.root().digest);
        Ok(())
    }
}
