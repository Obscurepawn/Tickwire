use crate::{Result, config::read_file, invalid};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
use tickwire_account::Account;
use tickwire_binance_clock::LocalTime;
use tickwire_binance_public::Notice;
use tickwire_recovery::{EncodedInputs, RecoveryMode};
use tickwire_runtime::{
    authority::{Dependency, RecoveryBundle},
    node::{Health, Strategy, Submission},
};
use tickwire_venue::CanonicalCommand;
pub trait AppStrategy: Strategy {
    /// Historical first-launch binding for the exact current instance. This
    /// does not replace a fresh observation or a complete config Applied receipt.
    fn projection_launch(
        &self,
        _target: crate::observation::Target,
    ) -> Option<&crate::hosted::profile::LaunchBindingProof> {
        None
    }
    fn observation_process(
        &mut self,
        _target: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<tickwire_supervisor::LiveChildObservation> {
        Err(tickwire_strategy_host::Error::ObservationUnsupported)
    }
    fn begin_observation(
        &mut self,
        _target: crate::observation::Target,
        _request: tickwire_strategy_host::ObservationRequest,
    ) -> tickwire_strategy_host::Result<()> {
        Err(tickwire_strategy_host::Error::ObservationUnsupported)
    }
    fn poll_observation(
        &mut self,
        _target: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>> {
        Err(tickwire_strategy_host::Error::ObservationUnsupported)
    }
    fn cancel_observation(
        &mut self,
        _target: crate::observation::Target,
        _id: [u8; 16],
    ) -> tickwire_strategy_host::Result<()> {
        Err(tickwire_strategy_host::Error::ObservationUnsupported)
    }

    fn market_consumers(&self, _out: &mut Vec<crate::control::market::Consumer>) {}
    fn arm_market(
        &mut self,
        _consumer: crate::control::market::Consumer,
        _approval: &tickwire_market_source::transition::Approval,
    ) -> Result<()> {
        Err(invalid("strategy has no remote market consumer"))
    }
    fn commit_market(
        &mut self,
        _consumer: crate::control::market::Consumer,
        _commit: &tickwire_market_source::transition::Commit,
    ) -> Result<()> {
        Err(invalid("strategy has no remote market consumer"))
    }
    fn market_transition(
        &self,
        _consumer: crate::control::market::Consumer,
    ) -> Option<&tickwire_market_source::transition::Receipt> {
        None
    }
    fn market_rejection(&self, _consumer: crate::control::market::Consumer) -> Option<u32> {
        None
    }

    fn instance(&self) -> Option<([u8; 16], u64)> {
        None
    }
    fn set_shadow(&mut self) -> bool {
        false
    }
    fn add_shadow(
        &mut self,
        _logical_strategy_id: u64,
        _strategy: Box<dyn AppStrategy>,
    ) -> Result<()> {
        Err(invalid(
            "strategy does not support dynamic shadow instances",
        ))
    }
    fn discard_shadow(&mut self, _instance: [u8; 16], _generation: u64) -> bool {
        false
    }
    fn failed(&self) -> bool;
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
// Deserialized once during startup; keep the existing public configuration shape.
#[allow(clippy::large_enum_variant)]
pub enum Config {
    Observe,
    Multi {
        members: Vec<MemberConfig>,
    },
    Hosted {
        config: crate::hosted::HostConfig,
    },
    /// Each immutable numbered descriptor contains a complete canonical command
    /// and its actual recovery dependencies. Producers publish with atomic rename.
    FileQueue {
        directory: PathBuf,
        first_sequence: u64,
        enabled: bool,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberConfig {
    #[serde(default)]
    pub shadow: bool,
    pub logical_strategy_id: u64,
    pub strategy: Box<Config>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandFile {
    pub command: PathBuf,
    pub command_digest: [u8; 32],
    pub order_ids: Vec<[u8; 16]>,
    pub recovery: RecoveryFiles,
}
#[derive(Deserialize)]
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyFile {
    pub kind: u8,
    pub schema_id: u64,
    pub owner: [u8; 16],
    pub path: PathBuf,
    pub digest: [u8; 32],
}
fn checked(path: &std::path::Path, digest: [u8; 32], max: usize) -> Result<Vec<u8>> {
    let bytes = read_file(path, max)?;
    if tickwire_recovery::digest(&bytes) != digest {
        return Err(invalid("dependency digest mismatch"));
    }
    Ok(bytes)
}
impl RecoveryFiles {
    pub fn load(&self, digest: [u8; 32]) -> Result<RecoveryBundle> {
        if self.dependencies.len() != 10 {
            return Err(invalid("recovery requires dependency kinds 2 through 11"));
        }
        let mut total = 0usize;
        let mut dependencies = Vec::with_capacity(10);
        for (index, item) in self.dependencies.iter().enumerate() {
            if item.kind != index as u8 + 2 {
                return Err(invalid("recovery dependency order"));
            }
            let bytes = checked(&item.path, item.digest, tickwire_recovery::MAX_OBJECT_BYTES)?;
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("dependency size"))?;
            if total > 128 * 1024 * 1024 {
                return Err(invalid("dependency bytes"));
            }
            dependencies.push(Dependency {
                kind: item.kind,
                schema_id: item.schema_id,
                owner: item.owner,
                bytes,
            });
        }
        let inputs = EncodedInputs::decode(checked(
            &self.inputs,
            self.input_digest,
            tickwire_recovery::MAX_OBJECT_BYTES,
        )?)?;
        Ok(RecoveryBundle {
            command_digest: digest,
            dependencies,
            inputs,
            mode: if self.declared_rebuild {
                RecoveryMode::DeclaredRebuild
            } else {
                RecoveryMode::DeterministicReplay
            },
            fault_model_id: self.fault_model_id,
            pin_id: self.pin_id,
            owner: self.owner,
        })
    }
}
pub struct FileStrategy {
    directory: Option<PathBuf>,
    sequence: u64,
    enabled: bool,
    failed: bool,
    pending: bool,
    last_health: Option<(bool, bool, bool, bool, bool)>,
}
impl FileStrategy {
    pub fn new(config: &Config) -> Result<Self> {
        let (directory, sequence, enabled) = match config {
            Config::Observe => (None, 1, false),
            Config::Hosted { .. } | Config::Multi { .. } => {
                return Err(invalid("hosted strategy requires its qualified launcher"));
            }
            Config::FileQueue {
                directory,
                first_sequence,
                enabled,
            } => {
                if !directory.is_absolute() || *first_sequence == 0 {
                    return Err(invalid("file queue identity"));
                }
                (Some(directory.clone()), *first_sequence, *enabled)
            }
        };
        Ok(Self {
            directory,
            sequence,
            enabled,
            failed: false,
            pending: false,
            last_health: None,
        })
    }
    pub fn failed(&self) -> bool {
        self.failed
    }
    fn load(&self, now: LocalTime) -> Result<Option<Submission>> {
        let Some(directory) = &self.directory else {
            return Ok(None);
        };
        let path = directory.join(format!("{:020}.json", self.sequence));
        match std::fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        let file: CommandFile = serde_json::from_slice(&read_file(&path, 1024 * 1024)?)?;
        let command =
            CanonicalCommand::decode(&checked(&file.command, file.command_digest, 1024 * 1024)?)
                .map_err(|_| invalid("invalid canonical command"))?;
        let remaining = command
            .command()
            .context
            .request
            .deadline
            .checked_sub(now.monotonic.nanos())
            .filter(|n| *n > 0)
            .ok_or_else(|| invalid("queued command expired"))?;
        let deadline = now
            .instant
            .checked_add(Duration::from_nanos(remaining))
            .ok_or_else(|| invalid("command deadline"))?;
        let recovery = file.recovery.load(command.digest())?;
        Ok(Some(Submission {
            command,
            order_ids: file.order_ids,
            deadline,
            recovery,
        }))
    }
}
impl Strategy for FileStrategy {
    fn on_market(&mut self, _: &Notice<'_>) -> bool {
        !self.failed
    }
    fn next(&mut self, _: &Account, health: Health, now: LocalTime) -> Option<Submission> {
        if !self.enabled
            || self.failed
            || self.pending
            || !health.calibrated
            || health.sender_halted
        {
            return None;
        }
        match self.load(now) {
            Ok(Some(command)) => {
                let administrative = matches!(
                    command.command.command().body,
                    tickwire_venue::CommandBody::Cancel(_)
                        | tickwire_venue::CommandBody::Query(_)
                        | tickwire_venue::CommandBody::CancelList(_)
                        | tickwire_venue::CommandBody::CancelAlgo(_)
                );
                if !administrative && !health.ready() {
                    return None;
                }
                self.pending = true;
                Some(command)
            }
            Ok(None) => None,
            Err(error) => {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"strategy_error","sequence":self.sequence,"error":error.to_string()})
                );
                self.failed = true;
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
        eprintln!(
            "{}",
            serde_json::json!({"event":"submission","sequence":self.sequence,"intent":command.command().context.request.intent_id,"result":format!("{result:?}")})
        );
        self.pending = false;
        if let Some(next) = self.sequence.checked_add(1) {
            self.sequence = next;
        } else {
            self.failed = true;
        }
    }
    fn on_rest(&mut self, outcome: tickwire_binance_account::rest::RestOutcome, _: &Account) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"rest","intent":outcome.request.intent_id,"disposition":format!("{:?}",outcome.disposition),"applied_events":outcome.applied_events})
        );
    }
    fn on_health(&mut self, h: Health, _: LocalTime) {
        let value = (
            h.calibrated,
            h.market_live,
            h.private_receiving,
            h.account_frozen,
            h.sender_halted,
        );
        if self.last_health != Some(value) {
            eprintln!(
                "{}",
                serde_json::json!({"event":"health","calibrated":h.calibrated,"market_live":h.market_live,"private_receiving":h.private_receiving,"account_frozen":h.account_frozen,"sender_halted":h.sender_halted})
            );
            self.last_health = Some(value);
        }
    }
}
impl AppStrategy for FileStrategy {
    fn failed(&self) -> bool {
        self.failed()
    }
}

struct Member {
    id: u64,
    shadow: bool,
    retired: bool,
    instance: Option<([u8; 16], u64)>,
    strategy: Option<Box<dyn AppStrategy>>,
}
/// Bounded independent strategy owners sharing one execution/account owner.
/// A failed child loses its grant and process; other children keep their cursors.
pub struct MultiStrategy {
    members: Vec<Member>,
    next: usize,
    policy: tickwire_runtime_policy::ControlPlane,
    fatal: bool,
    seen_instances: std::collections::BTreeSet<[u8; 16]>,
    seen_generations: std::collections::BTreeSet<(u64, u64)>,
    highest_generation: std::collections::BTreeMap<u64, u64>,
}
impl MultiStrategy {
    fn observation_member(
        &mut self,
        target: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<&mut Box<dyn AppStrategy>> {
        self.members
            .iter_mut()
            .find(|m| {
                !m.retired
                    && m.id == target.logical_strategy_id
                    && m.strategy.as_ref().and_then(|s| s.instance())
                        == Some((target.instance, target.generation))
            })
            .and_then(|m| m.strategy.as_mut())
            .ok_or(tickwire_strategy_host::Error::Invalid)
    }

    pub fn new(
        members: Vec<(u64, Box<dyn AppStrategy>)>,
        policy: tickwire_runtime_policy::ControlPlane,
    ) -> Result<Self> {
        Self::new_modes(
            members.into_iter().map(|(id, s)| (id, false, s)).collect(),
            policy,
        )
    }
    pub fn new_modes(
        mut members: Vec<(u64, bool, Box<dyn AppStrategy>)>,
        policy: tickwire_runtime_policy::ControlPlane,
    ) -> Result<Self> {
        if members.is_empty() || members.len() > 32 {
            return Err(invalid("multi requires 1..32 strategies"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (id, shadow, strategy) in &mut members {
            if *id == 0 || !seen.insert((*id, *shadow)) {
                return Err(invalid("multi strategy identities/modes"));
            }
            if *shadow && (strategy.instance().is_none() || !strategy.set_shadow()) {
                return Err(invalid("shadow requires qualified hosted instance"));
            }
        }
        for (id, shadow, strategy) in &members {
            if *shadow {
                let (instance, generation) = strategy
                    .instance()
                    .ok_or_else(|| invalid("shadow instance"))?;
                let active = members
                    .iter()
                    .find(|(other, shadow, _)| other == id && !*shadow)
                    .ok_or_else(|| invalid("shadow requires an active peer"))?;
                let (old, old_generation) = active
                    .2
                    .instance()
                    .ok_or_else(|| invalid("active handover instance"))?;
                if old == instance || old_generation >= generation {
                    return Err(invalid("shadow generation must follow active instance"));
                }
            }
        }
        let mut out = Self {
            members: vec![],
            next: 0,
            policy,
            fatal: false,
            seen_instances: Default::default(),
            seen_generations: Default::default(),
            highest_generation: Default::default(),
        };
        for (id, shadow, strategy) in members {
            let instance = strategy.instance();
            if let Some(identity) = instance {
                out.remember(id, identity)?;
            }
            out.members.push(Member {
                id,
                shadow,
                retired: false,
                instance,
                strategy: Some(strategy),
            });
        }
        Ok(out)
    }
    fn remember(&mut self, id: u64, (instance, generation): ([u8; 16], u64)) -> Result<()> {
        if instance == [0; 16]
            || generation == 0
            || self.seen_instances.len() >= 4096
            || self.seen_instances.contains(&instance)
            || self.seen_generations.contains(&(id, generation))
        {
            return Err(invalid(
                "strategy instance reused or history capacity exhausted",
            ));
        }
        self.seen_instances.insert(instance);
        self.seen_generations.insert((id, generation));
        self.highest_generation
            .entry(id)
            .and_modify(|g| *g = (*g).max(generation))
            .or_insert(generation);
        Ok(())
    }
    fn refresh_instance(&mut self, index: usize) {
        let Some(strategy) = self.members[index].strategy.as_ref() else {
            return;
        };
        let actual = strategy.instance();
        if actual == self.members[index].instance {
            return;
        }
        let id = self.members[index].id;
        let valid = actual.is_some_and(|new| {
            self.members[index]
                .instance
                .is_some_and(|old| new.1 > old.1)
        });
        if !valid || actual.is_some_and(|new| self.remember(id, new).is_err()) {
            if let Some((instance, generation)) = actual
                && self
                    .policy
                    .stop_strategy_instance(id, instance, generation)
                    .is_err()
            {
                self.fatal = true;
            }
            self.stop(index);
            return;
        }
        if let Some((instance, generation)) = self.members[index].instance
            && self
                .policy
                .stop_strategy_instance(id, instance, generation)
                .is_err()
        {
            self.fatal = true;
        }
        self.members[index].instance = actual;
    }
    fn stop(&mut self, index: usize) {
        let member = &mut self.members[index];
        if member.strategy.is_none() {
            return;
        }
        let stop = if let Some((instance, generation)) = member.instance {
            self.policy
                .stop_strategy_instance(member.id, instance, generation)
        } else {
            self.policy.stop_strategy(member.id)
        };
        if stop.is_err() {
            self.fatal = true;
        }
        // Dropping HostedStrategy drops its Registry, which terminates and waits
        // for its owned Child. It never kills a recovered bare PID.
        member.strategy.take();
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_stopped","logical_strategy_id":member.id,"instance":member.instance,"retired":member.retired})
        );
    }
    fn failed_member(&mut self, index: usize) {
        self.refresh_instance(index);
        if self.members[index]
            .strategy
            .as_ref()
            .is_some_and(|s| s.failed())
        {
            self.stop(index);
        }
    }
}
impl AppStrategy for MultiStrategy {
    fn projection_launch(
        &self,
        target: crate::observation::Target,
    ) -> Option<&crate::hosted::profile::LaunchBindingProof> {
        self.members
            .iter()
            .filter(|member| !member.retired)
            .filter_map(|member| member.strategy.as_ref())
            .find_map(|strategy| strategy.projection_launch(target))
    }
    fn observation_process(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<tickwire_supervisor::LiveChildObservation> {
        self.observation_member(t)?.observation_process(t)
    }
    fn begin_observation(
        &mut self,
        t: crate::observation::Target,
        r: tickwire_strategy_host::ObservationRequest,
    ) -> tickwire_strategy_host::Result<()> {
        self.observation_member(t)?.begin_observation(t, r)
    }
    fn poll_observation(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>> {
        self.observation_member(t)?.poll_observation(t)
    }
    fn cancel_observation(
        &mut self,
        t: crate::observation::Target,
        id: [u8; 16],
    ) -> tickwire_strategy_host::Result<()> {
        self.observation_member(t)?.cancel_observation(t, id)
    }

    fn market_consumers(&self, out: &mut Vec<crate::control::market::Consumer>) {
        for m in self.members.iter().filter(|m| !m.retired) {
            if let Some(s) = &m.strategy {
                s.market_consumers(out);
            }
        }
    }
    fn arm_market(
        &mut self,
        c: crate::control::market::Consumer,
        a: &tickwire_market_source::transition::Approval,
    ) -> Result<()> {
        self.members
            .iter_mut()
            .find(|m| !m.retired && m.instance == Some((c.instance, c.generation)))
            .and_then(|m| m.strategy.as_mut())
            .ok_or_else(|| invalid("remote market strategy identity"))?
            .arm_market(c, a)
    }
    fn commit_market(
        &mut self,
        c: crate::control::market::Consumer,
        v: &tickwire_market_source::transition::Commit,
    ) -> Result<()> {
        self.members
            .iter_mut()
            .find(|m| !m.retired && m.instance == Some((c.instance, c.generation)))
            .and_then(|m| m.strategy.as_mut())
            .ok_or_else(|| invalid("remote market strategy identity"))?
            .commit_market(c, v)
    }
    fn market_transition(
        &self,
        c: crate::control::market::Consumer,
    ) -> Option<&tickwire_market_source::transition::Receipt> {
        self.members
            .iter()
            .find(|m| !m.retired && m.instance == Some((c.instance, c.generation)))?
            .strategy
            .as_ref()?
            .market_transition(c)
    }
    fn market_rejection(&self, c: crate::control::market::Consumer) -> Option<u32> {
        self.members
            .iter()
            .find(|m| !m.retired && m.instance == Some((c.instance, c.generation)))?
            .strategy
            .as_ref()?
            .market_rejection(c)
    }

    fn discard_shadow(&mut self, instance: [u8; 16], generation: u64) -> bool {
        let Some(index) = self
            .members
            .iter()
            .position(|m| m.shadow && !m.retired && m.instance == Some((instance, generation)))
        else {
            return false;
        };
        self.stop(index);
        self.members[index].retired = true;
        true
    }
    fn add_shadow(&mut self, id: u64, mut strategy: Box<dyn AppStrategy>) -> Result<()> {
        if self.fatal || id == 0 {
            return Err(invalid("multi is stopped"));
        }
        for index in 0..self.members.len() {
            self.failed_member(index);
        }
        if self.fatal {
            return Err(invalid("multi is stopped"));
        }
        let actual = strategy
            .instance()
            .ok_or_else(|| invalid("shadow requires qualified hosted instance"))?;
        if !self
            .members
            .iter()
            .any(|m| m.id == id && !m.shadow && !m.retired && m.strategy.is_some())
            || self
                .members
                .iter()
                .any(|m| m.id == id && m.shadow && !m.retired && m.strategy.is_some())
            || self
                .highest_generation
                .get(&id)
                .is_none_or(|old| actual.1 <= *old)
        {
            return Err(invalid(
                "shadow needs one active predecessor and a newer generation",
            ));
        }
        let free = self
            .members
            .iter()
            .position(|m| m.retired || m.strategy.is_none());
        if free.is_none() && self.members.len() >= 32 {
            return Err(invalid("multi capacity"));
        }
        if !strategy.set_shadow() {
            return Err(invalid("shadow forwarding control"));
        }
        self.remember(id, actual)?;
        let new = Member {
            id,
            shadow: true,
            retired: false,
            instance: Some(actual),
            strategy: Some(strategy),
        };
        if let Some(index) = free {
            self.members[index] = new;
        } else {
            self.members.push(new);
        }
        Ok(())
    }
    fn failed(&self) -> bool {
        self.fatal
    }
}
impl Strategy for MultiStrategy {
    fn handover_rebuild_input(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
    ) -> Option<tickwire_runtime::node::RebuildInput> {
        self.members
            .iter_mut()
            .find(|m| {
                m.id == operation.logical_strategy_id
                    && m.instance == Some((member.new_instance, member.new_generation))
                    && m.shadow
                    && !m.retired
            })?
            .strategy
            .as_mut()?
            .handover_rebuild_input(operation, member)
    }
    fn handover_install_rebuild(
        &mut self,
        input: tickwire_runtime::node::RebuildInput,
    ) -> Option<tickwire_runtime::node::InstalledRebuild> {
        let request = input.request();
        self.members
            .iter_mut()
            .find(|m| {
                m.id == request.operation.logical_strategy_id
                    && m.instance
                        == Some((request.member.new_instance, request.member.new_generation))
                    && m.shadow
                    && !m.retired
            })?
            .strategy
            .as_mut()?
            .handover_install_rebuild(input)
    }
    fn handover_finish_rebuild(
        &mut self,
        installed: tickwire_runtime::node::InstalledRebuild,
    ) -> Option<tickwire_runtime::node::PreparedRebuild> {
        let request = installed.input().request();
        self.members
            .iter_mut()
            .find(|m| {
                m.id == request.operation.logical_strategy_id
                    && m.instance
                        == Some((request.member.new_instance, request.member.new_generation))
                    && m.shadow
                    && !m.retired
            })?
            .strategy
            .as_mut()?
            .handover_finish_rebuild(installed)
    }
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        let request = &installed.verified().request;
        self.members
            .iter_mut()
            .filter(|m| !m.retired)
            .find(|m| {
                m.id == request.operation.logical_strategy_id
                    && m.strategy.as_ref().and_then(|s| s.instance())
                        == Some((request.member.new_instance, request.member.new_generation))
            })?
            .strategy
            .as_mut()?
            .handover_finish_replay(installed)
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        let c = source.completion();
        self.members
            .iter_mut()
            .filter(|m| !m.retired)
            .find(|m| {
                m.id == c.operation.logical_strategy_id
                    && m.strategy.as_ref().and_then(|s| s.instance())
                        == Some((c.member.new_instance, c.member.new_generation))
            })?
            .strategy
            .as_mut()?
            .handover_install_replay(source)
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        self.members
            .iter_mut()
            .filter(|m| !m.retired)
            .find(|m| {
                m.id == operation.logical_strategy_id
                    && m.strategy.as_ref().and_then(|s| s.instance())
                        == Some((member.old_instance, member.old_generation))
            })?
            .strategy
            .as_mut()?
            .handover_replay_source(operation, member, fixed)
    }

    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        self.members
            .iter_mut()
            .filter(|m| !m.retired)
            .find(|m| {
                m.strategy.as_ref().and_then(|s| s.instance()) == Some((instance, generation))
            })?
            .strategy
            .as_mut()?
            .handover_checkpoint(instance, generation)
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        self.members
            .iter_mut()
            .filter(|m| !m.retired)
            .find(|m| {
                m.strategy.as_ref().and_then(|s| s.instance()) == Some((instance, generation))
            })?
            .strategy
            .as_mut()?
            .release_preflight(instance, generation, roots)
    }
    fn handover_ready(&mut self, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
        self.members
            .iter_mut()
            .find(|m| m.instance == Some((r.instance_id, r.strategy_generation)))
            .and_then(|m| m.strategy.as_mut())
            .is_some_and(|s| s.handover_ready(r))
    }
    fn handover_objects(
        &mut self,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        self.members
            .iter_mut()
            .find(|m| m.instance == Some((r.instance_id, r.strategy_generation)))?
            .strategy
            .as_mut()?
            .handover_objects(r)
    }
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        let old = self.members.iter().position(|m| {
            m.id == c.logical_strategy_id && m.instance == Some((c.old_instance, c.old_generation))
        });
        let new = self.members.iter().position(|m| {
            m.id == c.logical_strategy_id && m.instance == Some((c.new_instance, c.new_generation))
        });
        let (Some(old), Some(new)) = (old, new) else {
            return false;
        };
        if old == new || c.new_generation <= c.old_generation || self.members[new].retired {
            return false;
        }
        self.members[old].shadow = true;
        self.members[new].shadow = true;
        let a = self.members[old]
            .strategy
            .as_mut()
            .is_some_and(|s| s.handover_quiesce(c));
        let b = self.members[new]
            .strategy
            .as_mut()
            .is_some_and(|s| s.handover_quiesce(c));
        a && b
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        let old = self.members.iter().position(|m| {
            m.id == c.logical_strategy_id && m.instance == Some((c.old_instance, c.old_generation))
        });
        let new = self.members.iter().position(|m| {
            m.id == c.logical_strategy_id && m.instance == Some((c.new_instance, c.new_generation))
        });
        let (Some(old), Some(new)) = (old, new) else {
            return false;
        };
        if old == new || c.new_generation <= c.old_generation || self.members[new].retired {
            return false;
        }
        let old_stopped = self.members[old].retired
            || self.members[old]
                .strategy
                .as_mut()
                .is_some_and(|s| s.handover_activate(c));
        self.members[old].shadow = true;
        if !old_stopped {
            return false;
        }
        self.members[old].retired = true;
        self.members[old].strategy.take();
        let activated = self.members[new]
            .strategy
            .as_mut()
            .is_some_and(|s| s.handover_activate(c));
        if activated {
            self.members[new].shadow = false;
        }
        activated
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        let mut found = false;
        let mut ok = true;
        for m in self.members.iter_mut().filter(|m| m.id == id) {
            found = true;
            m.shadow = true;
            if let Some(s) = &mut m.strategy {
                ok &= s.handover_halt(id);
            }
        }
        found && ok
    }
    fn on_market(&mut self, notice: &Notice<'_>) -> bool {
        if self.fatal {
            return false;
        }
        for i in 0..self.members.len() {
            if self.members[i]
                .strategy
                .as_mut()
                .is_some_and(|s| !s.on_market(notice))
            {
                self.stop(i);
            }
            self.failed_member(i);
        }
        !self.fatal
    }
    fn next(&mut self, account: &Account, health: Health, now: LocalTime) -> Option<Submission> {
        if self.fatal {
            return None;
        }
        for _ in 0..self.members.len() {
            let i = self.next;
            self.next = (self.next + 1) % self.members.len();
            self.failed_member(i);
            let Some(strategy) = self.members[i].strategy.as_mut() else {
                continue;
            };
            let command = strategy.next(account, health, now);
            self.failed_member(i);
            if self.members[i].strategy.is_none() {
                continue;
            }
            if let Some(command) = command {
                if command
                    .command
                    .command()
                    .context
                    .strategy
                    .logical_strategy_id
                    != self.members[i].id
                    || self.members[i]
                        .instance
                        .is_some_and(|(instance, generation)| {
                            command.command.command().context.strategy.instance_id != instance
                                || command
                                    .command
                                    .command()
                                    .context
                                    .request
                                    .strategy_generation
                                    != generation
                        })
                {
                    self.stop(i);
                    continue;
                }
                if self.members[i].shadow {
                    continue;
                }
                return Some(command);
            }
        }
        None
    }
    fn on_submission(
        &mut self,
        command: &CanonicalCommand,
        result: &std::result::Result<
            tickwire_account_gateway::Status,
            tickwire_binance_account::Error,
        >,
    ) {
        if let Some(i) = self.members.iter().position(|m| {
            m.id == command.command().context.strategy.logical_strategy_id
                && m.instance.is_none_or(|(instance, generation)| {
                    instance == command.command().context.strategy.instance_id
                        && generation == command.command().context.request.strategy_generation
                })
        }) {
            if let Some(s) = &mut self.members[i].strategy {
                s.on_submission(command, result);
            }
            self.failed_member(i);
        }
    }
    fn on_account_history(&mut self, history: &dyn tickwire_runtime::node::AccountHistory) {
        for i in 0..self.members.len() {
            if let Some(s) = &mut self.members[i].strategy {
                s.on_account_history(history);
            }
            self.failed_member(i);
        }
    }
    fn on_private(&mut self, outcome: tickwire_binance_account::Outcome, account: &Account) {
        for i in 0..self.members.len() {
            if let Some(s) = &mut self.members[i].strategy {
                s.on_private(outcome, account);
            }
            self.failed_member(i);
        }
    }
    fn on_rest(&mut self, outcome: tickwire_binance_account::rest::RestOutcome, account: &Account) {
        for i in 0..self.members.len() {
            if let Some(s) = &mut self.members[i].strategy {
                s.on_rest(outcome, account);
            }
            self.failed_member(i);
        }
    }
    fn on_health(&mut self, health: Health, now: LocalTime) {
        for i in 0..self.members.len() {
            if let Some(s) = &mut self.members[i].strategy {
                s.on_health(health, now);
            }
            self.failed_member(i);
        }
    }
}

#[cfg(test)]
mod multi_tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};
    fn policy() -> tickwire_runtime_policy::ControlPlane {
        use tickwire_runtime_policy::{Config, Versions};
        tickwire_runtime_policy::ControlPlane::new(Config {
            scope: tickwire_account::Scope {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: tickwire_venue::Environment::Test,
                account: 1,
            },
            product: tickwire_venue::Product::Spot,
            executor_id: [2; 16],
            owner_generation: 1,
            writer_epoch: 1,
            domain: tickwire_clock::ClockDomain::new([3; 16], 1).unwrap(),
            versions: Versions {
                config_revision: 1,
                risk_policy_version: 1,
                order_config_version: 1,
                catalog_version: 1,
                catalog_hash: [4; 32],
            },
            max_strategies: 32,
            max_feeds: 2,
            marks_per_feed: 2,
        })
        .unwrap()
    }
    struct Child {
        calls: Rc<Cell<u32>>,
        keep_running: bool,
    }
    impl AppStrategy for Child {
        fn failed(&self) -> bool {
            false
        }
    }
    impl Strategy for Child {
        fn on_market(&mut self, _: &Notice<'_>) -> bool {
            self.calls.set(self.calls.get() + 1);
            self.keep_running
        }
        fn next(&mut self, _: &Account, _: Health, _: LocalTime) -> Option<Submission> {
            None
        }
        fn on_submission(
            &mut self,
            _: &CanonicalCommand,
            _: &std::result::Result<
                tickwire_account_gateway::Status,
                tickwire_binance_account::Error,
            >,
        ) {
        }
    }
    fn child(calls: Rc<Cell<u32>>, keep_running: bool) -> Box<dyn AppStrategy> {
        Box::new(Child {
            calls,
            keep_running,
        })
    }
    #[test]
    fn a_failed_child_loses_its_grant_without_stopping_the_healthy_child() {
        use tickwire_runtime_policy::{Actions, StrategyGrant};
        let plane = policy();
        let grant = |id| StrategyGrant {
            logical_strategy_id: id,
            instance_id: [id as u8; 16],
            strategy_generation: 1,
            authorization_epoch: 3,
            config_revision: 1,
            valid_until_ns: 1000,
            actions: Actions {
                submit: false,
                cancel: true,
                query: true,
                amend: false,
            },
            required_feeds: vec![],
        };
        plane.install_strategy_grant(grant(1)).unwrap();
        plane.install_strategy_grant(grant(2)).unwrap();
        let failed = Rc::new(Cell::new(0));
        let healthy = Rc::new(Cell::new(0));
        let mut multi = MultiStrategy::new(
            vec![
                (1, child(failed.clone(), false)),
                (2, child(healthy.clone(), true)),
            ],
            plane.clone(),
        )
        .unwrap();
        let notice = Notice::Gap(tickwire_binance_public::GapNotice {
            connection_epoch: 1,
            book_generation: 1,
            reason: tickwire_binance_public::Error::Closed,
        });
        assert!(multi.on_market(&notice));
        assert!(multi.on_market(&notice));
        assert_eq!(failed.get(), 1);
        assert_eq!(healthy.get(), 2);
        assert!(!multi.failed());
        assert_eq!(
            plane.install_strategy_grant(grant(1)),
            Err(tickwire_runtime_policy::Error::Stale)
        );
        assert_eq!(plane.install_strategy_grant(grant(2)), Ok(()));
    }
    #[test]
    fn duplicate_logical_owners_and_unbounded_members_are_rejected() {
        let calls = Rc::new(Cell::new(0));
        assert!(
            MultiStrategy::new(
                vec![
                    (1, child(calls.clone(), true)),
                    (1, child(calls.clone(), true))
                ],
                policy()
            )
            .is_err()
        );
        assert!(
            MultiStrategy::new(
                (1..=33).map(|i| (i, child(calls.clone(), true))).collect(),
                policy()
            )
            .is_err()
        );
    }
    struct Qualified {
        id: Rc<Cell<([u8; 16], u64)>>,
        shadow: Rc<Cell<bool>>,
        keep_running: bool,
        activates: Rc<Cell<u32>>,
        retire_ok: bool,
    }
    impl AppStrategy for Qualified {
        fn instance(&self) -> Option<([u8; 16], u64)> {
            Some(self.id.get())
        }
        fn set_shadow(&mut self) -> bool {
            self.shadow.set(true);
            true
        }
        fn failed(&self) -> bool {
            false
        }
    }
    impl Strategy for Qualified {
        fn on_market(&mut self, _: &Notice<'_>) -> bool {
            self.keep_running
        }
        fn next(&mut self, _: &Account, _: Health, _: LocalTime) -> Option<Submission> {
            None
        }
        fn on_submission(
            &mut self,
            _: &CanonicalCommand,
            _: &std::result::Result<
                tickwire_account_gateway::Status,
                tickwire_binance_account::Error,
            >,
        ) {
        }
        fn handover_quiesce(&mut self, _: &tickwire_runtime::node::StrategySwitch) -> bool {
            self.shadow.set(true);
            true
        }
        fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
            self.activates.set(self.activates.get() + 1);
            if self.id.get() == (c.old_instance, c.old_generation) && !self.retire_ok {
                return false;
            }
            self.shadow
                .set(self.id.get() != (c.new_instance, c.new_generation));
            true
        }
        fn handover_halt(&mut self, _: u64) -> bool {
            self.shadow.set(true);
            true
        }
    }
    fn qualified(
        id: ([u8; 16], u64),
        keep_running: bool,
        shadow: Rc<Cell<bool>>,
        activates: Rc<Cell<u32>>,
    ) -> Box<dyn AppStrategy> {
        Box::new(Qualified {
            id: Rc::new(Cell::new(id)),
            keep_running,
            shadow,
            activates,
            retire_ok: true,
        })
    }
    #[test]
    fn failed_old_retirement_keeps_the_new_instance_paused() {
        let old_shadow = Rc::new(Cell::new(false));
        let new_shadow = Rc::new(Cell::new(false));
        let new_activates = Rc::new(Cell::new(0));
        let mut multi = MultiStrategy::new_modes(
            vec![
                (
                    1,
                    false,
                    Box::new(Qualified {
                        id: Rc::new(Cell::new(([1; 16], 1))),
                        shadow: old_shadow.clone(),
                        keep_running: true,
                        activates: Rc::new(Cell::new(0)),
                        retire_ok: false,
                    }) as Box<dyn AppStrategy>,
                ),
                (
                    1,
                    true,
                    qualified(
                        ([2; 16], 2),
                        true,
                        new_shadow.clone(),
                        new_activates.clone(),
                    ),
                ),
            ],
            policy(),
        )
        .unwrap();
        let change = tickwire_runtime::node::StrategySwitch {
            logical_strategy_id: 1,
            old_instance: [1; 16],
            old_generation: 1,
            new_instance: [2; 16],
            new_generation: 2,
        };
        assert!(multi.handover_quiesce(&change));
        assert!(!multi.handover_activate(&change));
        assert_eq!(new_activates.get(), 0);
        assert!(old_shadow.get() && new_shadow.get());
        assert!(!multi.members[0].retired);
        assert!(multi.handover_halt(1));
        assert!(new_shadow.get());
    }

    #[test]
    fn exact_instances_switch_and_repeated_activation_does_not_restop_retired_child() {
        let old = Rc::new(Cell::new(false));
        let new = Rc::new(Cell::new(false));
        let stops = Rc::new(Cell::new(0));
        let starts = Rc::new(Cell::new(0));
        let mut multi = MultiStrategy::new_modes(
            vec![
                (
                    1,
                    false,
                    qualified(([1; 16], 1), true, old.clone(), stops.clone()),
                ),
                (
                    1,
                    true,
                    qualified(([2; 16], 2), true, new.clone(), starts.clone()),
                ),
            ],
            policy(),
        )
        .unwrap();
        assert!(!old.get());
        assert!(new.get());
        let mut change = tickwire_runtime::node::StrategySwitch {
            logical_strategy_id: 1,
            old_instance: [1; 16],
            old_generation: 1,
            new_instance: [2; 16],
            new_generation: 2,
        };
        change.new_generation = 3;
        assert!(!multi.handover_quiesce(&change));
        assert!(!old.get());
        change.new_generation = 2;
        assert!(multi.handover_quiesce(&change));
        assert!(old.get());
        assert!(new.get());
        assert!(multi.handover_activate(&change));
        assert!(old.get());
        assert!(!new.get());
        assert!(multi.handover_activate(&change));
        assert_eq!(stops.get(), 1);
        assert_eq!(starts.get(), 2);
        assert!(multi.members[0].retired);
        assert!(!multi.members[1].shadow);
        assert!(multi.handover_halt(1));
        assert!(new.get());
    }
    #[test]
    fn failed_shadow_does_not_revoke_the_other_instance_of_the_same_strategy() {
        use tickwire_runtime_policy::{Actions, StrategyGrant};
        let p = policy();
        let grant = StrategyGrant {
            logical_strategy_id: 1,
            instance_id: [1; 16],
            strategy_generation: 1,
            authorization_epoch: 3,
            config_revision: 1,
            valid_until_ns: 1000,
            actions: Actions {
                submit: false,
                cancel: true,
                query: true,
                amend: false,
            },
            required_feeds: vec![],
        };
        p.install_strategy_grant(grant.clone()).unwrap();
        let mut multi = MultiStrategy::new_modes(
            vec![
                (
                    1,
                    false,
                    qualified(
                        ([1; 16], 1),
                        true,
                        Rc::new(Cell::new(false)),
                        Rc::new(Cell::new(0)),
                    ),
                ),
                (
                    1,
                    true,
                    qualified(
                        ([2; 16], 2),
                        false,
                        Rc::new(Cell::new(false)),
                        Rc::new(Cell::new(0)),
                    ),
                ),
            ],
            p.clone(),
        )
        .unwrap();
        assert!(
            multi.on_market(&Notice::Gap(tickwire_binance_public::GapNotice {
                connection_epoch: 1,
                book_generation: 1,
                reason: tickwire_binance_public::Error::Closed,
            }))
        );
        assert!(multi.members[0].strategy.is_some());
        assert!(multi.members[1].strategy.is_none());
        assert_eq!(p.install_strategy_grant(grant), Ok(()));
        assert!(!multi.failed());
    }
    #[test]
    fn next_shadow_reuses_retired_slot_without_reusing_identity_or_generation() {
        let make = |(byte, generation)| {
            qualified(
                ([byte; 16], generation),
                true,
                Rc::new(Cell::new(false)),
                Rc::new(Cell::new(0)),
            )
        };
        let mut multi = MultiStrategy::new_modes(
            vec![(1, false, make((1, 1))), (1, true, make((2, 2)))],
            policy(),
        )
        .unwrap();
        let first = tickwire_runtime::node::StrategySwitch {
            logical_strategy_id: 1,
            old_instance: [1; 16],
            old_generation: 1,
            new_instance: [2; 16],
            new_generation: 2,
        };
        assert!(multi.handover_quiesce(&first));
        assert!(multi.handover_activate(&first));
        assert!(multi.add_shadow(1, make((1, 3))).is_err());
        assert!(multi.add_shadow(1, make((3, 2))).is_err());
        multi.add_shadow(1, make((3, 3))).unwrap();
        assert_eq!(multi.members.len(), 2);
        assert!(multi.add_shadow(1, make((4, 4))).is_err());
        let next = tickwire_runtime::node::StrategySwitch {
            logical_strategy_id: 1,
            old_instance: [2; 16],
            old_generation: 2,
            new_instance: [3; 16],
            new_generation: 3,
        };
        assert!(multi.handover_quiesce(&next));
        assert!(multi.handover_activate(&next));
        assert!(
            multi
                .members
                .iter()
                .any(|m| m.instance == Some(([3; 16], 3)) && !m.shadow)
        );
    }
    #[test]
    fn host_rotation_refreshes_cached_identity_before_the_next_switch() {
        let identity = Rc::new(Cell::new(([1; 16], 1)));
        let strategy = Box::new(Qualified {
            id: identity.clone(),
            keep_running: true,
            shadow: Rc::new(Cell::new(false)),
            activates: Rc::new(Cell::new(0)),
            retire_ok: true,
        });
        let mut multi = MultiStrategy::new_modes(vec![(1, false, strategy)], policy()).unwrap();
        identity.set(([2; 16], 2));
        let notice = Notice::Gap(tickwire_binance_public::GapNotice {
            connection_epoch: 1,
            book_generation: 1,
            reason: tickwire_binance_public::Error::Closed,
        });
        assert!(multi.on_market(&notice));
        assert_eq!(multi.members[0].instance, Some(([2; 16], 2)));
        assert_eq!(multi.highest_generation.get(&1), Some(&2));
        assert!(
            multi
                .add_shadow(
                    1,
                    qualified(
                        ([3; 16], 2),
                        true,
                        Rc::new(Cell::new(false)),
                        Rc::new(Cell::new(0))
                    )
                )
                .is_err()
        );
        multi
            .add_shadow(
                1,
                qualified(
                    ([3; 16], 3),
                    true,
                    Rc::new(Cell::new(false)),
                    Rc::new(Cell::new(0)),
                ),
            )
            .unwrap();
        let switch = tickwire_runtime::node::StrategySwitch {
            logical_strategy_id: 1,
            old_instance: [2; 16],
            old_generation: 2,
            new_instance: [3; 16],
            new_generation: 3,
        };
        assert!(multi.handover_quiesce(&switch));
    }
}
