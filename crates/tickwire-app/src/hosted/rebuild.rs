//! Explicitly configured capture-only launch and same-process target installation.
use super::*;
pub(super) fn build(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
    bootstrap: Bootstrap,
) -> Result<Box<dyn AppStrategy>> {
    use std::os::unix::fs::MetadataExt;
    let capture = config
        .capture
        .as_ref()
        .ok_or_else(|| invalid("capture launch missing"))?;
    let directory = config
        .md_admission_directory
        .as_ref()
        .ok_or_else(|| invalid("capture MD admission directory missing"))?;
    for path in [
        &capture.bootstrap,
        &capture.publication_directory,
        directory,
        &config.executable,
        &config.working_directory,
        &config.directory_parent,
        &config.recovery_directory,
    ] {
        if !path.is_absolute() {
            return Err(invalid("capture launcher paths must be absolute"));
        }
    }
    let metadata = std::fs::symlink_metadata(&capture.publication_directory)?;
    if !config.shadow
        || config.rotation_directory.is_some()
        || !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || std::fs::canonicalize(&capture.publication_directory)? != capture.publication_directory
    {
        return Err(invalid("capture publication directory or shadow mode"));
    }
    let input: tickwire_strategy_sdk::capture::Bootstrap =
        serde_json::from_slice(&read_file(&capture.bootstrap, 2 * 1024 * 1024)?)?;
    let external = bootstrap
        .external_market
        .as_ref()
        .ok_or_else(|| invalid("capture requires external MD runtime template"))?;
    let factory = bootstrap
        .factory
        .as_ref()
        .ok_or_else(|| invalid("capture requires a registered factory"))?;
    if external.domain()? != runtime.time.domain
        || input.market.domain()? != runtime.time.domain
        || input.identity.risk_domain != runtime.command_binding.risk_domain_id
        || bootstrap.risk_domain != runtime.command_binding.risk_domain_id
        || input.market.endpoint.local.identity() != external.endpoint.local.identity()
        || input.market.endpoint.local.identity() != bootstrap.market.local.identity()
        || factory.id != input.contract.factory
        || !bootstrap.handover_enabled
        || factory.revision != input.config.revision
        || factory.configuration != tickwire_release::Object::of(&input.config.bytes)
        || read_file(&factory.configuration_file, 2 * 1024 * 1024)? != input.config.bytes
    {
        return Err(invalid("capture runtime identity or configuration differs"));
    }
    let launch = tickwire_strategy_host::rebuild::Launch {
        capture: tickwire_strategy_host::capture::Launch {
            resources: config.resources.clone(),
            spec: tickwire_supervisor::LaunchSpec {
                identity: input.market.endpoint.local.identity(),
                executable: config.executable.clone(),
                arguments: vec![],
                working_directory: config.working_directory.clone(),
                environment: BTreeMap::new(),
                uid: config.uid,
                gid: config.gid,
                inherit_output: true,
            },
            bootstrap: input,
            directory_parent: config.directory_parent.clone(),
            supervisor: config.supervisor.identity(),
            parent_process: tickwire_control::ProcessIdentity {
                pid: std::process::id(),
                uid: config.uid,
                gid: config.gid,
            },
            md_admission_directory: directory.clone(),
            snapshot_id: config.snapshot_id,
        },
        bootstrap,
        publication_directory: capture.publication_directory.clone(),
    };
    // SAFETY: startup approval names qualified builds and immutable channel
    // identities; Registry retains and authenticates the same capture child.
    let mut hosted = unsafe {
        tickwire_strategy_host::rebuild::Hosted::launch(
            launch,
            Files {
                directory: config.recovery_directory.clone(),
            },
            policy,
        )?
    };
    hosted.set_shadow();
    Ok(Box::new(RebuildStrategy(
        hosted,
        serde_json::to_vec(config)?,
    )))
}
struct RebuildStrategy(tickwire_strategy_host::rebuild::Hosted<Files>, Vec<u8>);
impl tickwire_runtime::node::Strategy for RebuildStrategy {
    fn handover_rebuild_input(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
    ) -> Option<tickwire_runtime::node::RebuildInput> {
        let input = tickwire_runtime::node::Strategy::handover_rebuild_input(
            &mut self.0,
            operation,
            member,
        )?;
        let target = input
            .manifest()
            .targets
            .iter()
            .find(|t| &t.member == member)?;
        let root = |role| {
            target
                .files
                .iter()
                .find(|f| f.role == role)
                .map(|f| f.object)
        };
        if root(tickwire_release::Role::HostConfig) != Some(tickwire_release::Object::of(&self.1))
            || root(tickwire_release::Role::Bootstrap) != Some(input.request().target_bootstrap)
        {
            return None;
        }
        Some(input)
    }
    fn handover_install_rebuild(
        &mut self,
        input: tickwire_runtime::node::RebuildInput,
    ) -> Option<tickwire_runtime::node::InstalledRebuild> {
        tickwire_runtime::node::Strategy::handover_install_rebuild(&mut self.0, input)
    }
    fn handover_finish_rebuild(
        &mut self,
        installed: tickwire_runtime::node::InstalledRebuild,
    ) -> Option<tickwire_runtime::node::PreparedRebuild> {
        tickwire_runtime::node::Strategy::handover_finish_rebuild(&mut self.0, installed)
    }
    fn on_account_history(&mut self, history: &dyn tickwire_runtime::node::AccountHistory) {
        tickwire_runtime::node::Strategy::on_account_history(&mut self.0, history)
    }
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        tickwire_runtime::node::Strategy::handover_finish_replay(&mut self.0, installed)
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        tickwire_runtime::node::Strategy::handover_install_replay(&mut self.0, source)
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        tickwire_runtime::node::Strategy::handover_replay_source(
            &mut self.0,
            operation,
            member,
            fixed,
        )
    }
    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        tickwire_runtime::node::Strategy::handover_checkpoint(&mut self.0, instance, generation)
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::release_preflight(
            &mut self.0,
            instance,
            generation,
            roots,
        )
    }
    fn handover_ready(&mut self, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
        tickwire_runtime::node::Strategy::handover_ready(&mut self.0, r)
    }
    fn handover_objects(
        &mut self,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::handover_objects(&mut self.0, r)
    }
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_quiesce(&mut self.0, c)
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_activate(&mut self.0, c)
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        tickwire_runtime::node::Strategy::handover_halt(&mut self.0, id)
    }

    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        tickwire_runtime::node::Strategy::on_market(&mut self.0, n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        tickwire_runtime::node::Strategy::next(&mut self.0, a, h, n)
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_baseline(&mut self.0, b, a)
    }
    fn account_report_position(&self) -> Option<u64> {
        tickwire_runtime::node::Strategy::account_report_position(&self.0)
    }
    fn on_account_record(
        &mut self,
        p: u64,
        r: &tickwire_account_codec::CanonicalRecord,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_record(&mut self.0, p, r, b, a)
    }
    fn on_submission(
        &mut self,
        c: &CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.0, c, r);
    }
}
impl AppStrategy for RebuildStrategy {
    fn market_consumers(&self, out: &mut Vec<crate::control::market::Consumer>) {
        if let Some((pid, instance, generation)) = self.0.market_consumer() {
            out.push(crate::control::market::Consumer {
                pid,
                instance,
                generation,
            });
        }
    }
    fn arm_market(
        &mut self,
        c: crate::control::market::Consumer,
        a: &tickwire_market_source::transition::Approval,
    ) -> Result<()> {
        if self.0.market_consumer() != Some((c.pid, c.instance, c.generation)) {
            return Err(invalid("remote market process identity"));
        }
        self.0
            .arm_market(a)
            .map_err(|e| invalid(&format!("remote market arm: {e:?}")))
    }
    fn commit_market(
        &mut self,
        c: crate::control::market::Consumer,
        v: &tickwire_market_source::transition::Commit,
    ) -> Result<()> {
        if self.0.market_consumer() != Some((c.pid, c.instance, c.generation)) {
            return Err(invalid("remote market process identity"));
        }
        self.0
            .commit_market(v)
            .map_err(|e| invalid(&format!("remote market commit: {e:?}")))
    }
    fn market_transition(
        &self,
        c: crate::control::market::Consumer,
    ) -> Option<&tickwire_market_source::transition::Receipt> {
        (self.0.market_consumer() == Some((c.pid, c.instance, c.generation)))
            .then(|| self.0.market_transition())
            .flatten()
    }
    fn market_rejection(&self, c: crate::control::market::Consumer) -> Option<u32> {
        (self.0.market_consumer() == Some((c.pid, c.instance, c.generation)))
            .then(|| self.0.market_rejection())
            .flatten()
    }

    fn instance(&self) -> Option<([u8; 16], u64)> {
        Some(self.0.instance())
    }
    fn set_shadow(&mut self) -> bool {
        self.0.set_shadow();
        true
    }
    fn failed(&self) -> bool {
        self.0.failed()
    }
}
