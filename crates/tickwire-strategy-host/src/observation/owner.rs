use super::*;
impl<R: RecoveryResolver + 'static> RemoteHostedStrategy<R> {
    pub fn observe_process(&mut self) -> Result<LiveChildObservation> {
        if self.failed() {
            return Err(Error::NotReady);
        }
        Ok(self.registry.observe(self.identity.instance)?)
    }
    /// Caller must approve mode-2 support for the actual observed SDK build.
    /// Accepted request IDs are single-use (4096 per owner); capacity exhaustion
    /// rejects observations without changing trading permission.
    pub fn begin_observation(&mut self, request: ObservationRequest) -> Result<()> {
        if self.failed() {
            return Err(Error::NotReady);
        }
        if self.market_transition.busy() {
            return Err(Error::Busy);
        }
        self.maintain()?;
        self.readiness.begin_observation(
            request,
            &mut self.report,
            &self.reports,
            &mut self.registry,
            &mut self.clock,
        )
    }
    /// Explicit cold path: may read recovery objects and kernel process facts.
    pub fn poll_observation(&mut self) -> Result<Option<FreshObservation>> {
        if !self.readiness.observing() {
            return Ok(None);
        }
        if self.failed() {
            return Err(Error::NotReady);
        }
        self.maintain()?;
        self.readiness.finish_observation(
            &self.report,
            &self.reports,
            self.report_sub,
            &mut self.registry,
            &mut self.clock,
        )
    }
    /// Sent cancellations retain channel exclusion until reply or original expiry.
    pub fn cancel_observation(&mut self, request_id: [u8; 16]) -> Result<()> {
        self.readiness.cancel_observation(request_id)
    }
}
impl<R: RecoveryResolver + 'static, const M: usize> HostedStrategy<R, M> {
    pub fn observe_process(&mut self) -> Result<LiveChildObservation> {
        if self.failed() {
            return Err(Error::NotReady);
        }
        let s = self.started.as_mut().ok_or(Error::NotReady)?;
        Ok(s.child.registry.observe(s.child.instance)?)
    }
    /// Caller must approve mode-2 support for the actual observed SDK build.
    /// Accepted IDs are single-use and bounded to 4096 per owner.
    pub fn begin_observation(&mut self, request: ObservationRequest) -> Result<()> {
        if self.failed() {
            return Err(Error::NotReady);
        }
        let s = self.started.as_mut().ok_or(Error::NotReady)?;
        let mut clock = SystemClock::new(s.child.domain)?;
        s.readiness
            .poll(&mut s.child.report, clock.sample()?.monotonic.nanos())?;
        s.readiness.begin_observation(
            request,
            &mut s.child.report,
            &s.reports,
            &mut s.child.registry,
            &mut clock,
        )
    }
    pub fn poll_observation(&mut self) -> Result<Option<FreshObservation>> {
        if self.failed() {
            return Err(Error::NotReady);
        }
        let s = self.started.as_mut().ok_or(Error::NotReady)?;
        if !s.readiness.observing() {
            return Ok(None);
        }
        let mut clock = SystemClock::new(s.child.domain)?;
        s.readiness
            .poll(&mut s.child.report, clock.sample()?.monotonic.nanos())?;
        s.readiness.finish_observation(
            &s.child.report,
            &s.reports,
            s.report_sub,
            &mut s.child.registry,
            &mut clock,
        )
    }
    pub fn cancel_observation(&mut self, request_id: [u8; 16]) -> Result<()> {
        self.started
            .as_mut()
            .ok_or(Error::NotReady)?
            .readiness
            .cancel_observation(request_id)
    }
}
