use crate::{
    Config, Error, Event, Identity, Phase, Receipt, Result, Version,
    codec::{self, Body},
    ledger::Ledger,
    store::Bounded,
};
use tickwire_journal::{PhysicalFrontier, store::Store};

pub struct Journal<S: Store> {
    physical: tickwire_journal::Journal<Bounded<S>>,
    config: Config,
    scope_hash: crate::Hash,
    ledger: Ledger,
    poisoned: bool,
}
impl<S: Store> Journal<S> {
    pub fn create(store: S, config: Config) -> Result<Self> {
        let physical_config = codec::physical(&config)?;
        let scope_hash = codec::scope_hash(&config.scope)?;
        let bytes = codec::encode(
            scope_hash,
            Body::Header {
                scope: config.scope.clone(),
            },
        )?;
        let ledger = Ledger::new(&config, bytes.len())?;
        let mut physical =
            tickwire_journal::Journal::create(Bounded::new(store, &config), physical_config)?;
        physical.append_batch(config.writer_epoch, &[&bytes])?;
        Ok(Self {
            physical,
            config,
            scope_hash,
            ledger,
            poisoned: false,
        })
    }
    pub fn recover(store: S, config: Config, required: Option<&PhysicalFrontier>) -> Result<Self> {
        let expected = codec::physical(&config)?;
        let scope_hash = codec::scope_hash(&config.scope)?;
        let mut replayed = None;
        let physical = tickwire_journal::Journal::recover_checked(
            Bounded::new(store, &config),
            required,
            |persisted, recovery| {
                replayed = Some(replay(&config, &expected, persisted, recovery, scope_hash));
                match &replayed {
                    Some(Ok(_)) => Ok(()),
                    _ => Err(tickwire_journal::Error::Quarantined(
                        "configuration application prefix",
                    )),
                }
            },
        );
        let ledger = match replayed {
            Some(Ok(ledger)) => ledger,
            Some(Err(error)) => return Err(error),
            None => {
                return Err(physical
                    .err()
                    .map(Error::Physical)
                    .unwrap_or(Error::Invalid("missing recovery validation")));
            }
        };
        let (physical, _) = physical?;
        Ok(Self {
            physical,
            config,
            scope_hash,
            ledger,
            poisoned: false,
        })
    }
    pub fn record(&mut self, event: &Event) -> Result<Receipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let bytes = codec::encode(
            self.scope_hash,
            Body::Event {
                event: event.clone(),
            },
        )?;
        if let Some(receipt) = self.ledger.check(event, &bytes)? {
            return Ok(receipt);
        }
        let physical = match self
            .physical
            .append_batch(self.config.writer_epoch, &[&bytes])
        {
            Ok(receipt) => receipt,
            Err(error) => {
                self.poisoned = true;
                return Err(error.into());
            }
        };
        let receipt = Receipt {
            sequence: physical.frontier().last_seq,
            record_hash: codec::hash(&bytes),
            frontier: physical.frontier().clone(),
        };
        self.ledger.apply(event, bytes, receipt.clone());
        Ok(receipt)
    }
    /// Checks the current ledger without reserving capacity or writing bytes.
    pub fn validate_event(&self, event: &Event) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let bytes = codec::encode(
            self.scope_hash,
            Body::Event {
                event: event.clone(),
            },
        )?;
        self.ledger.check(event, &bytes).map(|_| ())
    }
    pub fn phase(&self, operation: Identity) -> Option<Phase> {
        self.ledger.phase(operation)
    }
    pub fn effective_record(&self) -> Option<&Version> {
        self.ledger.effective()
    }
    pub fn durable_frontier(&self) -> Option<&PhysicalFrontier> {
        self.physical.durable_frontier()
    }
    pub fn scope(&self) -> &crate::Scope {
        &self.config.scope
    }
    pub fn operation(&self, id: Identity) -> Option<crate::OperationView<'_>> {
        self.ledger.operation(id)
    }
    pub fn operations(&self) -> impl Iterator<Item = crate::OperationView<'_>> {
        self.ledger.operations()
    }
    pub fn active_operation(&self) -> Option<Identity> {
        self.ledger.active_operation()
    }
}
fn replay(
    config: &Config,
    expected: &tickwire_journal::Config,
    persisted: &tickwire_journal::Config,
    recovery: &tickwire_journal::Recovery,
    scope_hash: crate::Hash,
) -> Result<Ledger> {
    if expected != persisted
        || recovery.frontier.is_none()
        || recovery.observed_exec_epoch() > config.writer_epoch
    {
        return Err(Error::Invalid("physical scope or writer epoch"));
    }
    let first = recovery
        .records
        .first()
        .ok_or(Error::Invalid("missing scope header"))?;
    if codec::decode(scope_hash, &first.payload)?
        != (Body::Header {
            scope: config.scope.clone(),
        })
    {
        return Err(Error::Invalid("scope header"));
    }
    let mut ledger = Ledger::new(config, first.payload.len())?;
    for record in &recovery.records {
        let frontier = recovery
            .record_frontier(record.seq)
            .ok_or(Error::Invalid("missing physical seal"))?;
        if frontier.last_seq != record.seq || frontier.batch_number != record.seq {
            return Err(Error::Invalid(
                "configuration records require individual batches",
            ));
        }
        if record.seq == 1 {
            continue;
        }
        let Body::Event { event } = codec::decode(scope_hash, &record.payload)? else {
            return Err(Error::Invalid("repeated scope header"));
        };
        if ledger.check(&event, &record.payload)?.is_some() {
            return Err(Error::Invalid("duplicate physical operation step"));
        }
        ledger.apply(
            &event,
            record.payload.clone(),
            Receipt {
                sequence: record.seq,
                record_hash: codec::hash(&record.payload),
                frontier: frontier.clone(),
            },
        );
    }
    Ok(ledger)
}
