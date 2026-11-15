//! Stateless strategy worker readiness (`ReadyProof`, `WorkerReadiness`) evaluated strictly from
//! live process liveness, SHM ring attachment state, and [`OrsRuntimePolicy`].
#![forbid(unsafe_code)]

use tickwire_ipc::ALL_STRATS;
use tickwire_runtime_policy::{Mode, OrsRuntimePolicy};
use tickwire_strategy_sdk::Bootstrap;

use crate::{Error, ProcessResources, Result};

/// Immutable startup identity and control-lane proof established when a supervised worker connects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyProof {
    strat_id: u8,
    logical_strategy_id: u64,
    instance_id: [u8; 16],
    generation: u64,
    default_account_ref: u8,
    intent_epoch: u64,
    report_epoch: u64,
    resources_constrained: bool,
}

impl ReadyProof {
    /// Construct a [`ReadyProof`] from a validated [`Bootstrap`] and optional [`ProcessResources`].
    pub fn new(bootstrap: &Bootstrap, resources: Option<&ProcessResources>) -> Result<Self> {
        bootstrap.validate()?;
        if bootstrap.strat_id == ALL_STRATS
            || bootstrap.instance_id == [0; 16]
            || bootstrap.generation == 0
        {
            return Err(Error::Invalid);
        }
        if let Some(res) = resources {
            res.validate()?;
        }
        let intent_ep = bootstrap.intent.as_ref().ok_or(Error::Invalid)?;
        let report_ep = bootstrap.report.as_ref().ok_or(Error::Invalid)?;
        if intent_ep.control_epoch == 0 || report_ep.control_epoch == 0 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            strat_id: bootstrap.strat_id,
            logical_strategy_id: bootstrap.logical_strategy_id,
            instance_id: bootstrap.instance_id,
            generation: bootstrap.generation,
            default_account_ref: bootstrap.account_ref,
            intent_epoch: intent_ep.control_epoch,
            report_epoch: report_ep.control_epoch,
            resources_constrained: resources.is_some(),
        })
    }

    #[inline]
    pub const fn strat_id(&self) -> u8 {
        self.strat_id
    }

    #[inline]
    pub const fn logical_strategy_id(&self) -> u64 {
        self.logical_strategy_id
    }

    #[inline]
    pub const fn instance_id(&self) -> [u8; 16] {
        self.instance_id
    }

    #[inline]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[inline]
    pub const fn default_account_ref(&self) -> u8 {
        self.default_account_ref
    }

    #[inline]
    pub const fn intent_epoch(&self) -> u64 {
        self.intent_epoch
    }

    #[inline]
    pub const fn report_epoch(&self) -> u64 {
        self.report_epoch
    }

    #[inline]
    pub const fn resources_constrained(&self) -> bool {
        self.resources_constrained
    }
}

/// Stateless point-in-time readiness snapshot combining process liveness, SHM ring watermarks,
/// and [`OrsRuntimePolicy`] admission flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerReadiness {
    pub proof: ReadyProof,
    pub resolved_account_ref: u8,
    pub process_alive: bool,
    pub lanes_attached: bool,
    pub mode: Mode,
    pub dop_logged_in: bool,
    pub active: bool,
    pub account_enabled: bool,
    pub intent_last_msg_seqnum: u64,
    pub report_next_seq: u64,
}

impl WorkerReadiness {
    /// Evaluate [`WorkerReadiness`] for `proof` against `policy` and live lane watermarks.
    pub fn evaluate(
        proof: &ReadyProof,
        policy: &OrsRuntimePolicy,
        account_ref_override: Option<u8>,
        process_alive: bool,
        lanes_attached: bool,
        intent_last_msg_seqnum: u64,
        report_next_seq: u64,
    ) -> Result<Self> {
        let requested_ref = account_ref_override.unwrap_or(proof.default_account_ref);
        let resolved_account_ref = policy
            .resolve_account_ref(requested_ref, proof.strat_id)
            .map_err(|_| Error::Invalid)?;
        Ok(Self {
            proof: proof.clone(),
            resolved_account_ref,
            process_alive,
            lanes_attached,
            mode: policy.mode(),
            dop_logged_in: policy.is_connected(proof.strat_id),
            active: policy.is_active(proof.strat_id),
            account_enabled: policy.is_strat_enabled(resolved_account_ref, proof.strat_id),
            intent_last_msg_seqnum,
            report_next_seq,
        })
    }

    /// Returns `true` iff the worker process is alive, SHM lanes are attached, ORS is in
    /// [`Mode::Trading`], and the strategy is both `active` and `enabled` on `resolved_account_ref`.
    #[inline]
    pub const fn is_ready_to_trade(&self) -> bool {
        self.process_alive
            && self.lanes_attached
            && matches!(self.mode, Mode::Trading)
            && self.active
            && self.account_enabled
    }

    /// Returns `true` iff the worker process is alive, SHM lanes are attached, and ORS is not
    /// [`Mode::Stopped`] (cancels remain permitted in [`Mode::ManageOnly`]).
    #[inline]
    pub const fn can_manage_orders(&self) -> bool {
        self.process_alive && self.lanes_attached && !matches!(self.mode, Mode::Stopped)
    }
}
