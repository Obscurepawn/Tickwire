//! Direct execution subscriptions; each source keeps its own cursor and JOIN.
use crate::{Result, invalid};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_market_source::{
    Client, ConnectConfig, MarketSink, MarketSource, PollReport, ShmSource,
};

struct Member {
    source: ShmSource,
    failed: bool,
}
pub struct Sources {
    domain: ClockDomain,
    members: Vec<Member>,
    next: usize,
}
impl Sources {
    pub fn connect(
        configs: &[ConnectConfig],
        domain: ClockDomain,
        executor: [u8; 16],
        owner_generation: u64,
    ) -> Result<Self> {
        use tickwire_ipc::Wire;
        if configs.is_empty() || configs.len() > 32 {
            return Err(invalid("execution requires 1 through 32 market sources"));
        }
        let mut topics = std::collections::BTreeSet::new();
        let mut members = Vec::with_capacity(configs.len());
        let mut clock = tickwire_binance_clock::SystemClock::new(domain)
            .map_err(|_| invalid("market clock"))?;
        for config in configs {
            let h = tickwire_ipc::ResourceHeader::decode(&config.bank_header)
                .map_err(|_| invalid("market bank header"))?;
            if config.domain()? != domain
                || config.endpoint.local.role != 4
                || config.endpoint.local.logical_strategy != 0
                || config.endpoint.local.instance != executor
                || config.endpoint.local.generation != owner_generation
                || !topics.insert(h.topic_id)
            {
                return Err(invalid("execution market identity or duplicate topic"));
            }
            // SAFETY: configuration pins the qualified MD identity, exact bank
            // descriptor and peer OS credentials; Client verifies authenticated
            // offers before mapping and completes the actual cursor/JOIN handshake.
            let client = unsafe {
                Client::connect(config, &mut || {
                    clock
                        .sample()
                        .map(|v| v.monotonic)
                        .map_err(|_| tickwire_market_source::Error::Identity)
                })
            }?;
            members.push(Member {
                source: ShmSource::new(client),
                failed: false,
            });
        }
        Ok(Self {
            domain,
            members,
            next: 0,
        })
    }
}
impl MarketSource for Sources {
    fn domain(&self) -> ClockDomain {
        self.domain
    }
    fn poll(
        &mut self,
        now: MonoTime,
        maximum_age_ns: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> tickwire_market_source::Result<PollReport> {
        if now.domain() != self.domain || budget == 0 {
            return Err(tickwire_market_source::Error::Bounds);
        }
        let mut report = PollReport::default();
        let count = self.members.len();
        let per_member = (budget / count).max(1);
        for offset in 0..count {
            if report.records >= budget {
                break;
            }
            let member = &mut self.members[(self.next + offset) % count];
            if member.failed {
                continue;
            }
            match member.source.poll(
                now,
                maximum_age_ns,
                per_member.min(budget - report.records),
                sink,
            ) {
                Ok(r) => {
                    report.records += r.records;
                    report.runnable |= r.runnable;
                    if let Some(d) = r.next_deadline_ns {
                        report.next_deadline_ns =
                            Some(report.next_deadline_ns.map_or(d, |old| old.min(d)));
                    }
                }
                Err(error) => {
                    member.failed = true;
                    member.source.stop(sink)?;
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event":"market_subscription_failed",
                            "topic":member.source.client().bank.topic_id,
                            "error":error.to_string()
                        })
                    );
                }
            }
        }
        self.next = (self.next + 1) % count;
        report.runnable |= report.records == budget;
        Ok(report)
    }
    fn is_live(&self, now: MonoTime, maximum_age_ns: u64) -> bool {
        self.members
            .iter()
            .all(|m| !m.failed && m.source.is_live(now, maximum_age_ns))
    }
    fn stop(&mut self, sink: &mut dyn MarketSink) -> tickwire_market_source::Result<()> {
        let mut error = None;
        for member in &mut self.members {
            member.failed = true;
            if let Err(e) = member.source.stop(sink) {
                error = Some(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
}
