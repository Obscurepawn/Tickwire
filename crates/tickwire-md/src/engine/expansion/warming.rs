use super::*;
struct Depth<'a> {
    candidate: &'a mut TopicExpansion,
    instrument: u64,
    clock: &'a mut SystemClock,
    error: Option<crate::Error>,
}
impl public::PublicSink for Depth<'_> {
    fn on_notice(&mut self, notice: public::Notice<'_>) -> bool {
        let result = (|| {
            let now = self.clock.sample()?.monotonic.nanos();
            self.candidate.warm_notice(self.instrument, &notice, now)?;
            Ok(())
        })();
        if let Err(error) = result {
            self.error = Some(error);
            return false;
        }
        true
    }
}
struct Event<'a> {
    candidate: &'a mut TopicExpansion,
    instrument: u64,
    clock: &'a mut SystemClock,
    error: Option<crate::Error>,
}
impl public::EventSink for Event<'_> {
    fn on_event(&mut self, notice: public::EventNotice<'_>) -> bool {
        let result = (|| {
            let now = self.clock.sample()?.monotonic.nanos();
            self.candidate
                .warm_event_notice(self.instrument, &notice, now)?;
            Ok(())
        })();
        if let Err(error) = result {
            self.error = Some(error);
            return false;
        }
        true
    }
}
pub(super) fn poll(flight: &mut Flight, runtime: &mut Runtime) -> Result<()> {
    let candidate = flight
        .candidate
        .as_mut()
        .ok_or_else(|| invalid("MD expansion candidate"))?;
    for owner in &mut flight.additions {
        let feed = &mut owner.feed;
        let mut sink = Depth {
            candidate,
            instrument: feed.instrument,
            clock: &mut runtime.clock,
            error: None,
        };
        let report = feed
            .client
            .poll(&mut runtime.quota.endpoint.public(feed.binding), &mut sink)?;
        if let Some(error) = sink.error {
            return Err(error);
        }
        if report.needs_registration {
            feed.client
                .register(runtime.poll.registry(), feed.tokens, true)?;
        }
        for event in &mut owner.events {
            let mut sink = Event {
                candidate,
                instrument: event.instrument,
                clock: &mut runtime.clock,
                error: None,
            };
            let report = event.client.poll(
                public::PollBudget::default(),
                &mut runtime.quota.endpoint.public(event.binding),
                &mut sink,
            )?;
            if let Some(error) = sink.error {
                return Err(error);
            }
            if report.needs_registration {
                event
                    .client
                    .reregister(runtime.poll.registry(), event.token)?;
            }
        }
    }
    Ok(())
}
