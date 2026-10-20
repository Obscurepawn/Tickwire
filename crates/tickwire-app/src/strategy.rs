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
    fn failed(&self) -> bool;
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
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
    strategy: Option<Box<dyn AppStrategy>>,
}
/// Bounded independent strategy owners sharing one execution/account owner.
/// A failed child loses its grant and process; other children keep their cursors.
pub struct MultiStrategy {
    members: Vec<Member>,
    next: usize,
    policy: tickwire_runtime_policy::ControlPlane,
    fatal: bool,
}
impl MultiStrategy {
    pub fn new(
        members: Vec<(u64, Box<dyn AppStrategy>)>,
        policy: tickwire_runtime_policy::ControlPlane,
    ) -> Result<Self> {
        if members.is_empty() || members.len() > 32 {
            return Err(invalid("multi requires 1..32 strategies"));
        }
        let mut seen = std::collections::BTreeSet::new();
        if members.iter().any(|(id, _)| *id == 0 || !seen.insert(*id)) {
            return Err(invalid("multi strategy identities"));
        }
        Ok(Self {
            members: members
                .into_iter()
                .map(|(id, strategy)| Member {
                    id,
                    strategy: Some(strategy),
                })
                .collect(),
            next: 0,
            policy,
            fatal: false,
        })
    }
    fn stop(&mut self, index: usize) {
        let member = &mut self.members[index];
        if member.strategy.is_none() {
            return;
        }
        if self.policy.stop_strategy(member.id).is_err() {
            self.fatal = true;
        }
        // Dropping HostedStrategy drops its Registry, which terminates and waits
        // for its owned Child. It never kills a recovered bare PID.
        member.strategy.take();
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_stopped","logical_strategy_id":member.id})
        );
    }
    fn failed_member(&mut self, index: usize) {
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
    fn failed(&self) -> bool {
        self.fatal
    }
}
impl Strategy for MultiStrategy {
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
            if strategy.failed() {
                self.stop(i);
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
                {
                    self.stop(i);
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
        if let Some(i) = self
            .members
            .iter()
            .position(|m| m.id == command.command().context.strategy.logical_strategy_id)
        {
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
}
