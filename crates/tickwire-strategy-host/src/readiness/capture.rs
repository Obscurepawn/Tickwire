//! Per-call announcement watermarks. Only the challenge-bound final reply is
//! eligible to become a verified READY; this value never escapes capture().
use super::*;

pub(super) struct Progress {
    identity: Identity,
    last: Option<HandoverReadyBody>,
}
impl Progress {
    pub fn new(identity: Identity, last: Option<HandoverReadyBody>) -> Self {
        Self { identity, last }
    }
    pub fn accept(
        &mut self,
        body: &HandoverReadyBody,
        sealed: bool,
        verify: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        if body.instance_id != self.identity.instance
            || body.strategy_generation != self.identity.generation
            || body.logical_strategy_id != self.identity.logical_strategy
        {
            return Err(Error::Invalid);
        }
        if let Some(last) = &self.last
            && body != last
            && (body.ready_sequence <= last.ready_sequence
                || body.exec_report_sequence < last.exec_report_sequence
                || body.report_ring_sequence < last.report_ring_sequence
                || body.ledger_sequence < last.ledger_sequence)
        {
            return Err(Error::Invalid);
        }
        if sealed {
            verify()?;
        }
        self.last = Some(body.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests;
