use super::*;
impl<R: RecoveryResolver + 'static> Strategy for Hosted<R> {
    fn handover_rebuild_input(
        &mut self,
        op: &tickwire_release::Operation,
        m: &tickwire_release::Member,
    ) -> Option<RebuildInput> {
        let r = self.input(op, m);
        self.close(r)
    }
    fn handover_install_rebuild(&mut self, input: RebuildInput) -> Option<InstalledRebuild> {
        let r = self.promote(input);
        self.close(r)
    }
    fn handover_finish_rebuild(&mut self, installed: InstalledRebuild) -> Option<PreparedRebuild> {
        let r = self.finish(installed);
        self.close(r)
    }
    fn handover_checkpoint(
        &mut self,
        i: [u8; 16],
        g: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        self.live.as_mut()?.handover_checkpoint(i, g)
    }
    fn handover_objects(
        &mut self,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        self.live.as_mut()?.handover_objects(r)
    }
    fn handover_ready(&mut self, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
        self.live.as_mut().is_some_and(|s| s.handover_ready(r))
    }
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        if let Some(s) = &mut self.live {
            s.handover_quiesce(c)
        } else {
            self.instance() == (c.new_instance, c.new_generation)
                && self.spec.identity.logical_strategy == c.logical_strategy_id
                && !self.failed()
        }
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        self.live.as_mut().is_some_and(|s| s.handover_activate(c))
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        self.live.as_mut().is_some_and(|s| s.handover_halt(id))
    }
    fn handover_replay_source(
        &mut self,
        o: &tickwire_release::Operation,
        m: &tickwire_release::Member,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        self.live.as_mut()?.handover_replay_source(o, m, r)
    }
    fn handover_install_replay(
        &mut self,
        s: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        self.live.as_mut()?.handover_install_replay(s)
    }
    fn handover_finish_replay(
        &mut self,
        i: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        self.live.as_mut()?.handover_finish_replay(i)
    }
    fn release_preflight(&mut self, i: [u8; 16], g: u64, r: [[u8; 32]; 3]) -> Option<Vec<Vec<u8>>> {
        self.live.as_mut()?.release_preflight(i, g, r)
    }
    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        if let Some(s) = &mut self.live {
            s.on_market(n)
        } else {
            !self.failed()
        }
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        if let Some(s) = &mut self.live {
            return s.next(a, h, n);
        }
        if !self.failed() {
            let r = self.poll_capture();
            self.close(r);
        }
        None
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        if let Some(s) = &mut self.live {
            return s.on_account_baseline(b, a);
        }
        let Some(o) = &mut self.capture else {
            return false;
        };
        if o.request.is_some() {
            return false;
        }
        let r = o.publish_baseline(b).map(|_| ());
        self.close(r).is_some()
    }
    fn account_report_position(&self) -> Option<u64> {
        self.live
            .as_ref()
            .map(Strategy::account_report_position)
            .unwrap_or_else(|| self.capture.as_ref().map(|o| o.reports.last_position()))
    }
    fn on_account_record(
        &mut self,
        p: u64,
        r: &tickwire_account_codec::CanonicalRecord,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        if let Some(s) = &mut self.live {
            return s.on_account_record(p, r, b, a);
        }
        let Some(o) = &mut self.capture else {
            return false;
        };
        if o.request.is_some() {
            return false;
        }
        let result = o.publish_record(p, r).map(|_| ());
        self.close(result).is_some()
    }
    fn on_submission(
        &mut self,
        c: &tickwire_venue::CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        if let Some(s) = &mut self.live {
            s.on_submission(c, r)
        }
    }
}
