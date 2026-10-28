use super::*;
use tickwire_market_source::protocol::wire_bytes;

impl Control {
    pub(super) fn install(
        &mut self,
        flight: &mut Flight,
        runtime: &mut Runtime,
        now: u64,
    ) -> Result<()> {
        let owner = &mut runtime.topics[flight.topic];
        let published = owner.split().0.published();
        let prepared = flight
            .prepared
            .as_ref()
            .ok_or_else(|| invalid("missing MD prepared plan"))?;
        let finalization = prepared.finalize(published, now)?;
        let bank_bytes = wire_bytes(&finalization.bank)?;
        let next = JoinOwner::new(
            Publisher::<32>::create(finalization.bank.clone())?,
            flight.registry.clone(),
        )?;
        if wire_bytes(next.header())? != bank_bytes {
            return Err(invalid("MD final descriptor mismatch"));
        }
        // Serialize the exact final artifacts before freezing the old owner.
        let target = &flight.plan.target_config;
        let effective = Effective {
            schema: 1,
            revision: flight.plan.target_revision,
            config_hash: flight.record.config_hash,
            config: target.clone(),
        };
        let files = vec![
            (
                target.topics[flight.topic].resource.clone(),
                bank_bytes.clone(),
            ),
            (
                config::active_resource_path(&target.topics[flight.topic].resource)?,
                bank_bytes,
            ),
            (
                self.config.effective_path.clone(),
                encode(&effective, crate::expansion::MAX_PLAN_BYTES)?,
            ),
        ];
        let candidate = flight
            .candidate
            .take()
            .ok_or_else(|| invalid("MD missing warmed candidate"))?;
        let old = owner.commit_expansion(
            candidate,
            next,
            now,
            flight.plan.maximum_age_ns,
            &mut Faults,
        )?;
        runtime.retired[flight.topic].push(old);
        for &(index, slot) in &flight.old_routes {
            for (topic, old) in &mut runtime.feeds[index].topics {
                if *topic == flight.topic {
                    *old = slot;
                }
            }
        }
        for &(index, slot) in &flight.old_event_routes {
            for (topic, old) in &mut runtime.event_feeds[index].topics {
                if *topic == flight.topic {
                    *old = slot;
                }
            }
        }
        for built in flight.additions.drain(..) {
            runtime.feeds.push(built.feed);
            runtime.event_feeds.extend(built.events);
        }
        let prepared = flight
            .prepared
            .take()
            .ok_or_else(|| invalid("MD prepared commit ownership"))?;
        let (config, index) = prepared.into_runtime();
        runtime.config = config;
        runtime.index = index;
        runtime.revision = flight.plan.target_revision;
        flight.next_admissions = finalization.admissions;
        flight.record.runtime_applied = true;
        flight.record.bank_descriptor_hash = Some(finalization.bank.descriptor_hash);
        flight.record.status = "applied".into();
        flight.stage = Stage::Installing;
        let task = cold::Task::Install {
            directory: runtime.config.admission_directory.clone(),
            admissions: flight.next_admissions.clone(),
            files,
        };
        if self.durable.is_some() {
            flight.installation = Some(task);
            self.persist(After::Applied, RecoveryPhase::Applied, runtime)
        } else {
            self.submit(Action::Install, task)
        }
    }
}
