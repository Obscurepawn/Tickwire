use super::*;
impl Owner {
    /// Real MD/report detach followed by the same child's successful exit.
    /// Abnormal termination is deliberately not returned as graceful retirement.
    pub fn stop(&mut self) -> Result<tickwire_supervisor::ExitObservation> {
        let result = self.retire();
        if result.is_ok() {
            self.stopped = true;
            self.outgoing = None;
        }
        self.close(result)
    }
    fn retire(&mut self) -> Result<tickwire_supervisor::ExitObservation> {
        let now = self.current()?;
        if self.captured.is_none() {
            return Err(Error::NotReady);
        }
        let until = now
            .nanos()
            .checked_add(4_000_000_000)
            .ok_or(Error::Invalid)?
            .min(self.bootstrap.deadline_mono_ns)
            .min(self.report.session.deadline());
        let deadline = Instant::now() + Duration::from_nanos(until - now.nanos());
        let bank = self.reports.publisher().header().clone();
        let membership = self.reports.publisher().membership_version();
        let mut challenge = self.bootstrap.identity.capture;
        challenge[0] ^= 0x60;
        if challenge == [0; 16] {
            challenge[0] = 1;
        }
        let raw = tickwire_strategy_sdk::retirement::request(
            &self.report.session,
            &bank,
            self.subscription,
            membership,
            challenge,
        )?;
        let request = ControlEnvelope::decode(&raw[..128])?;
        let expected = tickwire_ipc::QuiesceBody::decode(&raw[128..])?;
        self.report.connection.send(&raw, &[], deadline)?;
        let packet = self.report.connection.recv(deadline)?;
        if !packet.fds().is_empty() {
            return Err(Error::Invalid);
        }
        let now = self.clock.sample()?.monotonic.nanos();
        if now >= until {
            return Err(Error::NotReady);
        }
        let frame = self.report.session.validate(packet.bytes(), now)?;
        if frame.envelope.status != 0 {
            return Err(Error::Invalid);
        }
        let proof = tickwire_strategy_sdk::retirement::check_reply(
            &frame.envelope,
            frame.body,
            &request,
            &expected,
        )?;
        self.reports.publisher().detach_registered(
            self.subscription,
            tickwire_shm::Watermarks {
                read: proof.released_seq,
                released: proof.released_seq,
                applied: proof.applied_seq,
            },
        )?;
        loop {
            if self.clock.sample()?.monotonic.nanos() >= until || Instant::now() >= deadline {
                return Err(Error::NotReady);
            }
            if let Some(exit) = self.registry.observe_exits()?.into_iter().next() {
                if exit.instance() != self.bootstrap.identity.instance
                    || exit.generation() != self.bootstrap.identity.generation
                    || exit.pid() != self.child.pid
                    || !exit.status().success()
                {
                    return Err(Error::Invalid);
                }
                return Ok(exit);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
