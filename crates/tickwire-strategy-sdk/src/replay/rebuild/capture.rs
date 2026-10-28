//! Bounded input capture. No user factory is reachable from this owner.
use super::*;
use tickwire_clock::MonoTime;
use tickwire_release::rebuild::window::{Identity, Seal, next_chain};

type ObservationKey = (StreamKey, u64, u8);

pub struct CaptureOnly {
    identity: Identity,
    contract: Contract,
    seed: Object,
    inputs: Inputs,
    start: Frontier,
    runtime: RuntimeState,
    views: application::Application<crate::ObserveOnly>,
    chain: Hash,
    latest: BTreeMap<ObservationKey, (u64, u64)>,
    observed: BTreeSet<(ObservationKey, u64)>,
    counts: Vec<u32>,
}
pub struct Captured {
    seal: Seal,
    inputs: Inputs,
}
impl Captured {
    pub fn seal(&self) -> &Seal {
        &self.seal
    }
    pub fn inputs(&self) -> &Inputs {
        &self.inputs
    }
}
fn frontier(runtime: &RuntimeState, rows: u64) -> Frontier {
    Frontier {
        now_ns: runtime.now_ns,
        rows,
        markets: runtime.markets.clone(),
        accounts: runtime.accounts.clone(),
        external: runtime.external.clone(),
    }
}
fn same_clock(clock: &ClockDomain, now: MonoTime) -> bool {
    clock.boot_id == now.domain().boot_id() && clock.generation == now.domain().generation()
}
impl CaptureOnly {
    pub fn new(
        identity: Identity,
        contract: Contract,
        seed: Inputs,
        now: MonoTime,
    ) -> Result<Self> {
        identity.validate()?;
        contract.validate()?;
        let seed_bytes = seed.bytes()?;
        if identity.binary != contract.binary
            || identity.clock != seed.clock
            || !same_clock(&seed.clock, now)
            || now.nanos() != seed.start_ns
            || !seed.warm.is_empty()
            || Object::of(&seed.config.bytes) != contract.config
            || seed.models.len() != contract.models.len()
            || seed
                .models
                .iter()
                .zip(&contract.models)
                .any(|(actual, expected)| {
                    actual.id != expected.id || Object::of(&actual.value.bytes) != expected.value
                })
        {
            return Err(Error::Identity);
        }
        if contract.ordering != Ordering::ObservedRebuildWindow {
            return Err(Error::Unsupported);
        }
        if seed_bytes.len() as u64 > contract.maximum_bytes {
            return Err(Error::Capacity);
        }
        for market in &seed.markets {
            let resource = ResourceHeader::decode(&market.resource).map_err(|_| Error::Identity)?;
            if resource.host_boot_id != seed.clock.boot_id || resource.clock_domain != 1 {
                return Err(Error::Time);
            }
        }
        let work = QualifiedViews::verify(
            identity.logical_strategy,
            identity.risk_domain,
            &contract,
            &seed,
        )?;
        let mut views = application::Application::new(
            crate::ObserveOnly,
            contract.maximum_age_ns,
            work.markets,
        )?;
        views.seed_rebuild_account(work.account)?;
        let runtime = RuntimeState {
            now_ns: seed.start_ns,
            config: seed.config.clone(),
            models: seed.models.clone(),
            markets: views.market_cursors()?,
            accounts: vec![work.account_cursor],
            external: vec![],
            timers: vec![],
            rng: contract.rng.clone(),
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        };
        runtime.validate()?;
        Ok(Self {
            identity,
            counts: vec![0; contract.markets.len()],
            contract,
            seed: Object::of(&seed_bytes),
            inputs: seed,
            start: frontier(&runtime, 0),
            runtime,
            views,
            chain: [0; 32],
            latest: BTreeMap::new(),
            observed: BTreeSet::new(),
        })
    }
    pub fn push(&mut self, input: Input, now: MonoTime) -> Result<()> {
        if !same_clock(&self.identity.clock, now) || now.nanos() < self.runtime.now_ns {
            return Err(Error::Time);
        }
        if !matches!(
            input,
            Input::MarketApplied { .. }
                | Input::MarketCallback { .. }
                | Input::AccountCallback { .. }
        ) {
            return Err(Error::Unsupported);
        }
        if self.inputs.warm.len() >= self.contract.maximum_rows as usize
            || now.nanos() - self.inputs.start_ns > self.contract.maximum_span_ns
        {
            return Err(Error::Capacity);
        }
        let row = TimedInput {
            now_ns: now.nanos(),
            input,
        };
        let mut inputs = self.inputs.clone();
        inputs.warm.push(row.clone());
        if inputs.bytes()?.len() as u64 > self.contract.maximum_bytes {
            return Err(Error::Capacity);
        }
        if let Input::AccountCallback { canonical, .. } = &row.input {
            let record = CanonicalRecord::decode(canonical).map_err(|_| Error::Identity)?;
            if let RecoveryRecord::Admit { command, .. } = record.record() {
                covered_action(&self.contract, self.identity.logical_strategy, command)?;
            }
        }
        // Materialize a candidate of SDK-owned views. These internal bytes contain
        // only the fixed observer and are never exported as a user checkpoint.
        let mut views = application::Application::restore(&self.views.checkpoint()?, |_| {
            Ok(crate::ObserveOnly)
        })?;
        let mut runtime = self.runtime.clone();
        runtime.now_ns = now.nanos();
        super::super::advance(&mut runtime, &row.input)?;
        views.apply(
            &row.input,
            &mut ReplayContext {
                views: None,
                runtime: &mut runtime,
            },
            &mut ReplayEffects {
                allowed: false,
                commands: vec![],
            },
        )?;
        runtime.validate()?;
        let mut latest = self.latest.clone();
        let mut observed = self.observed.clone();
        let mut counts = self.counts.clone();
        match &row.input {
            Input::MarketApplied { stream, record, .. } => {
                let h = RecordHeader::decode(record.get(..128).ok_or(Error::Identity)?)
                    .map_err(|_| Error::Identity)?;
                let category = if h.message_type == 259 {
                    Some(3)
                } else {
                    tickwire_ipc::market_state_category(h.message_type)
                        .ok()
                        .filter(|c| *c <= 4)
                };
                if let Some(category) = category {
                    latest.insert(
                        (stream.clone(), h.instrument_id, category),
                        (h.sequence, h.ingress_mono_ns),
                    );
                }
            }
            Input::MarketCallback {
                stream,
                instrument,
                invoked: true,
                ..
            } => {
                for (i, requirement) in self
                    .contract
                    .markets
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| &r.stream == stream && r.instrument == *instrument)
                {
                    view_valid(
                        &views,
                        requirement,
                        now.nanos(),
                        self.contract.maximum_age_ns,
                    )?;
                    let key = (stream.clone(), *instrument, requirement.category);
                    if let Some((sequence, received)) = latest.get(&key) {
                        if now
                            .nanos()
                            .checked_sub(*received)
                            .is_none_or(|age| age > self.contract.maximum_age_ns)
                        {
                            return Err(Error::Time);
                        }
                        if observed.insert((key, *sequence)) {
                            counts[i] = counts[i].checked_add(1).ok_or(Error::Capacity)?;
                        }
                    }
                }
            }
            _ => {}
        }
        let chain = next_chain(self.chain, inputs.warm.len() as u64, &row)?;
        self.inputs = inputs;
        self.runtime = runtime;
        self.views = views;
        self.latest = latest;
        self.observed = observed;
        self.counts = counts;
        self.chain = chain;
        Ok(())
    }
    pub fn finish(self) -> Result<Captured> {
        if self.inputs.warm.is_empty()
            || self.runtime.now_ns - self.inputs.start_ns < self.contract.minimum_span_ns
        {
            return Err(Error::Capacity);
        }
        for (requirement, count) in self.contract.markets.iter().zip(&self.counts) {
            view_valid(
                &self.views,
                requirement,
                self.runtime.now_ns,
                self.contract.maximum_age_ns,
            )?;
            if *count < requirement.minimum_observations {
                return Err(Error::StateMismatch);
            }
        }
        let (_, account_input_digest) =
            self.views.account_seal(&self.runtime.accounts[0].stream)?;
        let seal = Seal {
            schema: 1,
            identity: self.identity,
            contract: Object::of(&self.contract.bytes()?),
            seed: self.seed,
            inputs: Object::of(&self.inputs.bytes()?),
            start: self.start,
            end: frontier(&self.runtime, self.inputs.warm.len() as u64),
            input_chain: self.chain,
            account_input_digest,
        };
        seal.bytes()?;
        Ok(Captured {
            seal,
            inputs: self.inputs,
        })
    }
}
