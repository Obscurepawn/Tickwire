//! Execution-process endpoint for actual SPSC canonical command references.
use crate::{Error, Result};
use std::{os::fd::OwnedFd, time::Duration};
use tickwire_ipc::ResourceHeader;
use tickwire_runtime::{
    authority::RecoveryBundle,
    node::{Health, Submission},
};
use tickwire_shm::{Subscriber, SubscriptionKey};
use tickwire_venue::CanonicalCommand;

pub struct ResolvedRecovery {
    pub order_ids: Vec<[u8; 16]>,
    pub bundle: RecoveryBundle,
}
/// Resolves actual retained strategy inputs and dependency objects. None means
/// still unavailable; the endpoint retains the command and retries without dequeueing another.
pub trait RecoveryResolver {
    fn resolve(&mut self, command: &CanonicalCommand) -> Result<Option<ResolvedRecovery>>;
    fn on_submission(
        &mut self,
        _command: &CanonicalCommand,
        _result: &std::result::Result<
            tickwire_account_gateway::Status,
            tickwire_binance_account::Error,
        >,
    ) {
    }
}
pub struct IntentReceiver {
    reader: Subscriber<4>,
    bank: ResourceHeader,
    objects: tickwire_recovery::FileBlobReader,
    risk_domain: [u8; 16],
    logical_strategy: u64,
    last_intent: u64,
}
impl IntentReceiver {
    /// # Safety
    /// The bank's authenticated strategy process uses the qualified single-writer
    /// SDK, retains this exact registered consumer gate, and obeys shared-memory lifetime rules.
    pub unsafe fn attach(
        fd: OwnedFd,
        bank: ResourceHeader,
        subscription: SubscriptionKey,
        objects: tickwire_recovery::FileBlobReader,
        risk_domain: [u8; 16],
        logical_strategy: u64,
    ) -> Result<Self> {
        if bank.resource_kind != 2
            || bank.required_features & 128 == 0
            || bank.risk_scope == 0
            || logical_strategy == 0
            || risk_domain == [0; 16]
        {
            return Err(Error::Identity);
        }
        let baseline = bank.first_seq.checked_sub(1).ok_or(Error::Bounds)?;
        // SAFETY: caller supplies the registered qualified writer and exact gate.
        let reader = unsafe { Subscriber::attach(fd, bank.clone(), subscription, baseline)? };
        Ok(Self {
            reader,
            bank,
            objects,
            risk_domain,
            logical_strategy,
            last_intent: 0,
        })
    }
    pub fn cursor_fd(&self) -> Result<OwnedFd> {
        Ok(self.reader.cursor_fd()?)
    }
    pub fn header(&self) -> &ResourceHeader {
        &self.bank
    }
    pub fn next_command(&mut self) -> Result<Option<CanonicalCommand>> {
        let Some(guard) = self.reader.borrow()? else {
            return Ok(None);
        };
        let command = crate::outbound::resolve_record(
            &self.objects,
            guard.bytes(),
            &self.bank,
            self.risk_domain,
        )?;
        let c = &command.command().context;
        if c.strategy.logical_strategy_id != self.logical_strategy
            || c.strategy.strategy_intent_seq <= self.last_intent
        {
            return Err(Error::Identity);
        }
        let sequence = guard.sequence();
        drop(guard);
        self.reader.apply(sequence)?;
        self.last_intent = c.strategy.strategy_intent_seq;
        Ok(Some(command))
    }
    pub fn start_quiesce(&self) {
        self.reader.start_quiesce();
    }
    pub fn quiesced(&self) -> Result<tickwire_shm::Quiesced> {
        Ok(self.reader.quiesced()?)
    }
}
/// `market` publishes public notices to the same real market owner used by the child.
/// Errors are sticky and observable to the supervisor through `error()`.
pub struct RuntimeStrategy<R, N> {
    receiver: IntentReceiver,
    resolver: R,
    market: N,
    pending: Option<CanonicalCommand>,
    error: Option<Error>,
}
impl<R: RecoveryResolver, N: FnMut(&tickwire_binance_public::Notice<'_>) -> bool>
    RuntimeStrategy<R, N>
{
    pub fn new(receiver: IntentReceiver, resolver: R, market: N) -> Self {
        Self {
            receiver,
            resolver,
            market,
            pending: None,
            error: None,
        }
    }
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
    pub fn start_quiesce(&self) {
        self.receiver.start_quiesce();
    }
    pub fn resolver_mut(&mut self) -> &mut R {
        &mut self.resolver
    }
    /// Drain shadow decisions without resolving/admitting or submitting them.
    /// The worker already persisted canonical bytes and actual decision inputs.
    pub fn drain_shadow(&mut self, budget: usize) -> Result<usize> {
        if budget == 0 || budget > 4096 {
            return Err(Error::Bounds);
        }
        let mut count = 0;
        while count < budget {
            let command = if let Some(c) = self.pending.take() {
                c
            } else {
                let Some(c) = self.receiver.next_command()? else {
                    break;
                };
                c
            };
            eprintln!(
                "{}",
                serde_json::json!({"event":"shadow_intent","instance":command.command().context.strategy.instance_id,"digest":command.digest()})
            );
            count += 1;
        }
        Ok(count)
    }
    fn next_inner(
        &mut self,
        health: Health,
        now: tickwire_binance_clock::LocalTime,
    ) -> Result<Option<Submission>> {
        if self.pending.is_none() {
            self.pending = self.receiver.next_command()?;
        }
        let Some(command) = self.pending.as_ref() else {
            return Ok(None);
        };
        let administrative = matches!(
            command.command().body,
            tickwire_venue::CommandBody::Cancel(_)
                | tickwire_venue::CommandBody::Query(_)
                | tickwire_venue::CommandBody::CancelList(_)
                | tickwire_venue::CommandBody::CancelAlgo(_)
        );
        if !(if administrative {
            health.calibrated && !health.sender_halted
        } else {
            health.ready()
        }) {
            return Ok(None);
        }
        let expires = command.command().context.request.deadline;
        let remaining = expires
            .checked_sub(now.monotonic.nanos())
            .filter(|v| *v > 0)
            .ok_or(Error::Stale)?;
        let Some(recovery) = self.resolver.resolve(command)? else {
            return Ok(None);
        };
        if recovery.bundle.command_digest != command.digest() {
            return Err(Error::Identity);
        }
        let deadline = now
            .instant
            .checked_add(Duration::from_nanos(remaining))
            .ok_or(Error::Bounds)?;
        Ok(Some(Submission {
            command: self.pending.take().ok_or(Error::NotReady)?,
            order_ids: recovery.order_ids,
            deadline,
            recovery: recovery.bundle,
        }))
    }
}
impl<R: RecoveryResolver, N: FnMut(&tickwire_binance_public::Notice<'_>) -> bool>
    tickwire_runtime::node::Strategy for RuntimeStrategy<R, N>
{
    fn on_market(&mut self, notice: &tickwire_binance_public::Notice<'_>) -> bool {
        (self.market)(notice)
    }
    fn next(
        &mut self,
        _account: &tickwire_account::Account,
        health: Health,
        now: tickwire_binance_clock::LocalTime,
    ) -> Option<Submission> {
        if self.error.is_some() {
            return None;
        }
        match self.next_inner(health, now) {
            Ok(v) => v,
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
    fn on_submission(
        &mut self,
        command: &CanonicalCommand,
        result: &std::result::Result<
            tickwire_account_gateway::Status,
            tickwire_binance_account::Error,
        >,
    ) {
        self.resolver.on_submission(command, result);
    }
}
