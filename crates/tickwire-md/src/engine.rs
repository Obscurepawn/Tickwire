//! Single MD process, independent bounded network owner per instrument.
mod cold;
#[cfg(test)]
mod dense_tests;
pub mod expansion;
mod owners;
mod restart_validation;
mod tokens;
use owners::{EventFeed, Feed};
type Quota = tickwire_binance_quota::bootstrap::Built<tickwire_quota::files::FileQuotaStore>;
use crate::{
    Result,
    config::{self, Config},
    invalid,
    server::{Server, TopicOwner},
};
use std::{collections::BTreeSet, time::Duration};
use tickwire_binance_public as public;
use tickwire_clock_sys::SystemClock;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_join::{JoinOwner, TopicRegistry};
use tickwire_market_publisher::{Fault, FaultSink, InstrumentConfig, InstrumentSlot};
use tickwire_shm::Publisher;
pub(crate) const LEVELS: usize = 5000;
pub(crate) const UPDATES: usize = 5000;
pub(crate) const BUFFER: usize = 64;
struct EventFanout<'a> {
    topics: &'a mut [TopicOwner],
    targets: &'a [(usize, InstrumentSlot)],
    instrument: u64,
    clock: &'a mut SystemClock,
}
impl public::EventSink for EventFanout<'_> {
    fn on_event(&mut self, n: public::EventNotice<'_>) -> bool {
        let now = match self.clock.sample() {
            Ok(t) => t.monotonic.nanos(),
            Err(_) => return false,
        };
        for &(i, slot) in self.targets {
            if let Err(e) = self.topics[i].on_event_slot(slot, &n, now, &mut Faults) {
                self.topics[i].require_rebuild();
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"md_event_publish_error","instrument":self.instrument,"error":e.to_string()})
                );
            }
        }
        true
    }
}
struct Faults;
impl FaultSink for Faults {
    fn invalidate(&mut self, f: Fault) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"md_topic_invalidated","topic":f.topic,"instrument":f.instrument,"generation":f.feed_generation,"sequence":f.published})
        );
    }
}
struct Fanout<'a> {
    topics: &'a mut [TopicOwner],
    targets: &'a [(usize, InstrumentSlot)],
    instrument: u64,
    clock: &'a mut SystemClock,
}
impl public::PublicSink for Fanout<'_> {
    fn on_notice(&mut self, n: public::Notice<'_>) -> bool {
        let now = match self.clock.sample() {
            Ok(s) => s.monotonic.nanos(),
            Err(_) => return false,
        };
        for &(i, slot) in self.targets {
            if let Err(e) = self.topics[i].on_notice_slot(slot, &n, now, &mut Faults) {
                self.topics[i].require_rebuild();
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"md_publish_error","instrument":self.instrument,"error":e.to_string()})
                );
            }
        }
        true
    }
}
fn topic_registry(
    c: &Config,
    h: &ResourceHeader,
    index: &config::FeedIndex,
    t: &config::Topic,
) -> Result<(TopicRegistry, Vec<InstrumentConfig>)> {
    let mut specs = Vec::new();
    let mut catalog = None;
    for f in &c.feeds {
        if f.topics.contains(&h.topic_id) {
            let (source, _) = (f.source.build(), ());
            let instrument = f.instrument.build(source)?;
            if instrument.catalog_version != h.catalog_version
                || catalog.is_some_and(|x| x != instrument.catalog_hash)
            {
                return Err(invalid("topic catalog binding"));
            }
            catalog = Some(instrument.catalog_hash);
            specs.push(InstrumentConfig {
                instrument,
                key: index.book_key(f)?,
                market: match source {
                    public::Source::SpotProduction | public::Source::SpotTest => {
                        tickwire_binance::request::Market::Spot
                    }
                    _ => tickwire_binance::request::Market::Usdm,
                },
                time_unit: if f.sbe_api_key_file.is_some() {
                    public::TimeUnit::Microseconds
                } else {
                    public::TimeUnit::Milliseconds
                },
            });
        }
    }
    specs.sort_by_key(|x| x.instrument.instrument_id);
    if specs.is_empty()
        || specs
            .windows(2)
            .any(|x| x[0].instrument.instrument_id == x[1].instrument.instrument_id)
    {
        return Err(invalid("topic membership"));
    }
    Ok((
        TopicRegistry {
            catalog_hash: catalog.ok_or_else(|| invalid("catalog"))?,
            instrument_set_version: t.instrument_set_version,
            instruments: specs
                .iter()
                .map(|s| tickwire_snapshot::InstrumentSpec {
                    instrument_id: s.instrument.instrument_id,
                    instrument_version: s.instrument.instrument_version,
                    required_states: c
                        .feeds
                        .iter()
                        .find(|f| {
                            f.instrument.id == s.instrument.instrument_id
                                && f.topics.contains(&h.topic_id)
                        })
                        .map(|f| {
                            f.events
                                .iter()
                                .fold(tickwire_snapshot::QUOTE, |bits, e| bits | e.kind.state())
                        })
                        .unwrap_or(tickwire_snapshot::QUOTE),
                })
                .collect(),
        },
        specs,
    ))
}
/// Runs public network owners and authenticated JOIN service until a fatal local
/// configuration/storage/clock error. Network reconnects remain per instrument.
pub struct Runtime {
    config: Config,
    index: config::FeedIndex,
    domain: tickwire_clock::ClockDomain,
    clock: SystemClock,
    quota: Quota,
    topics: Vec<TopicOwner>,
    poll: public::mio::Poll,
    events: public::mio::Events,
    feeds: Vec<Feed>,
    event_feeds: Vec<EventFeed>,
    server: Server,
    retired: Vec<Vec<JoinOwner<32>>>,
    first: usize,
    tokens: tokens::Tokens,
    revision: u64,
    expansion: Option<expansion::Control>,
}
#[derive(Clone, Copy, Debug)]
pub struct PollReport {
    pub runnable: bool,
    pub next_deadline_ns: Option<u64>,
    pub wait_ns: u64,
}
fn canonical_bank(mut header: ResourceHeader) -> Result<ResourceHeader> {
    use sha2::{Digest, Sha256};
    tickwire_shm::validate_bank(&header)?;
    let expected = header.descriptor_hash;
    header.descriptor_hash = [0; 32];
    header.descriptor_hash =
        Sha256::digest(tickwire_market_source::protocol::wire_bytes(&header)?).into();
    if expected != [0; 32] && expected != header.descriptor_hash {
        return Err(invalid("MD resource descriptor digest"));
    }
    Ok(header)
}
impl Runtime {
    pub fn new(c: Config) -> Result<Self> {
        if c.feeds.is_empty()
            || c.feeds.len() > 32
            || c.topics.is_empty()
            || c.topics.len() > 32
            || c.maximum_idle_ns == 0
            || c.maximum_idle_ns > 100_000_000
        {
            return Err(invalid("MD owner bounds"));
        }
        let banks = c
            .topics
            .iter()
            .map(|topic| {
                canonical_bank(ResourceHeader::decode(&config::read(
                    &topic.resource,
                    4096,
                )?)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let (expansion, index) = if let Some(control) = c.expansion.clone() {
            let (control, index) = expansion::Control::new(control, &c, &banks)?;
            (Some(control), index)
        } else {
            (None, config::FeedIndex::new(&c.feeds)?)
        };
        let revision = c.expansion.as_ref().map_or(1, |c| c.initial_revision);
        let domain = tickwire_clock::ClockDomain::new(c.identity.host_boot, c.clock_generation)
            .map_err(|e| invalid(&format!("clock {e:?}")))?;
        let mut clock = SystemClock::new(domain)?;
        let store = tickwire_quota::files::FileQuotaStore::open(&c.quota_directory)
            .map_err(|e| invalid(&format!("quota store {e:?}")))?;
        let mut quota = c
            .quota
            .build(store, clock.sample()?.monotonic)
            .map_err(|e| invalid(&format!("quota {e:?}")))?;
        let mut topics = Vec::new();
        let mut topic_ids = BTreeSet::new();
        for (t, h) in c.topics.iter().zip(banks) {
            if h.producer_instance != c.identity.instance
                || h.producer_generation != c.identity.generation
                || h.host_boot_id != c.identity.host_boot
                || h.namespace_id != c.identity.namespace
                || !topic_ids.insert(h.topic_id)
            {
                return Err(invalid("MD topic resource identity"));
            }
            let (registry, instruments) = topic_registry(&c, &h, &index, t)?;
            let owner = JoinOwner::new(Publisher::<32>::create(h)?, registry)?;
            config::write_bytes(
                &config::active_resource_path(&t.resource)?,
                &tickwire_market_source::protocol::wire_bytes(owner.header())?,
            )?;
            topics.push(TopicOwner::new(
                owner,
                instruments,
                tickwire_market_publisher::Limits {
                    levels_per_side: t.maximum_levels,
                    cache_bytes: t.maximum_cache_bytes,
                    snapshot_bytes: t.maximum_snapshot_bytes,
                    max_delta_bytes: t.maximum_delta_bytes,
                },
            )?);
        }
        let poll = public::mio::Poll::new()?;
        let events = public::mio::Events::with_capacity(256);
        let mut feeds = Vec::new();
        let mut event_feeds = Vec::new();
        let mut identities = BTreeSet::new();
        let mut tokens = tokens::Tokens::default();
        for f in &c.feeds {
            if !identities.insert(f.owner_id)
                || f.topics.is_empty()
                || f.topics.iter().collect::<BTreeSet<_>>().len() != f.topics.len()
                || f.topics.iter().any(|t| !topic_ids.contains(t))
            {
                return Err(invalid("MD feed owner/topics"));
            }
            let mut kinds = BTreeSet::new();
            let mut sources = BTreeSet::new();
            for event in &f.events {
                if !identities.insert(event.owner_id)
                    || !kinds.insert(event.kind.state())
                    || !sources.insert(event.stream_id)
                {
                    return Err(invalid("duplicate event owner/kind/source"));
                }
            }
            let reserved = owners::Reserved::new(&mut tokens, f.events.len())?;
            let mut built = owners::build(f, domain, &index, reserved)?;
            built.attach(f, &mut topics)?;
            built.start(poll.registry(), &mut quota)?;
            feeds.push(built.feed);
            event_feeds.extend(built.events);
        }
        let server = Server::new(&c)?;
        // Keep old mapped owners alive: outstanding guards/cursors may still refer
        // to them. Explicit control supplies each successor; never recycle in place.
        let retired: Vec<Vec<JoinOwner<32>>> = (0..topics.len()).map(|_| Vec::new()).collect();
        Ok(Self {
            config: c,
            index,
            domain,
            clock,
            quota,
            topics,
            poll,
            events,
            feeds,
            event_feeds,
            server,
            retired,
            first: 0,
            tokens,
            revision,
            expansion,
        })
    }
    /// Drive one round over network owners, authenticated JOIN, and expansion control.
    pub fn poll_once(&mut self) -> Result<PollReport> {
        let mut report = self.poll_live()?;
        if let Some(mut control) = self.expansion.take() {
            let now = self.clock.sample()?.monotonic.nanos();
            let result = control.poll(self, now);
            self.expansion = Some(control);
            report.runnable |= result?;
            report.wait_ns = if report.runnable {
                0
            } else {
                report.wait_ns.min(1_000_000)
            };
        }
        Ok(report)
    }
    fn poll_live(&mut self) -> Result<PollReport> {
        let Self {
            config: c,
            clock,
            quota,
            topics,
            poll,
            feeds,
            event_feeds,
            server,
            retired,
            first,
            expansion,
            ..
        } = self;

        let mut runnable = false;
        let mut next = None::<u64>;
        for offset in 0..feeds.len() {
            let i = (*first + offset) % feeds.len();
            let f = &mut feeds[i];
            let mut sink = Fanout {
                topics,
                targets: &f.topics,
                instrument: f.instrument,
                clock,
            };
            let r = f
                .client
                .poll(&mut quota.endpoint.public(f.binding), &mut sink)?;
            runnable |= r.runnable;
            if let Some(t) = r.next_deadline_ns {
                next = Some(next.map_or(t, |old| old.min(t)));
            }
            if r.needs_registration {
                f.client.register(poll.registry(), f.tokens, true)?;
            }
        }
        for offset in 0..event_feeds.len() {
            let i = (*first + offset) % event_feeds.len();
            let f = &mut event_feeds[i];
            let mut sink = EventFanout {
                topics,
                targets: &f.topics,
                instrument: f.instrument,
                clock,
            };
            let report = f.client.poll(
                public::PollBudget::default(),
                &mut quota.endpoint.public(f.binding),
                &mut sink,
            )?;
            runnable |= report.runnable;
            if let Some(t) = report.next_deadline_ns {
                next = Some(next.map_or(t, |old| old.min(t)));
            }
            if report.needs_registration {
                f.client.reregister(poll.registry(), f.token)?;
            }
        }
        *first = (*first + 1) % feeds.len();
        let now = clock.sample()?.monotonic.nanos();
        server.poll(topics, now)?;
        for i in 0..topics.len() {
            if topics[i].needs_bank() && !server.topic_paused(i) {
                server.invalidate_topic(i, &mut topics[i])?;
                if !server.has_peers(i) && topics[i].state().cached_live() {
                    let old = topics[i].split().0.header().clone();
                    let mut h = canonical_bank(ResourceHeader::decode(&config::read(
                        &c.topics[i].resource,
                        4096,
                    )?)?)?;
                    if h.resource_id != old.resource_id
                        && h.ring_epoch > old.ring_epoch
                        && h.feed_generation > old.feed_generation
                    {
                        if retired[i].len() >= 32 {
                            return Err(invalid(
                                "retained bank capacity; restart with fresh process identity",
                            ));
                        }
                        h.predecessor_ring_epoch = old.ring_epoch;
                        h.cutover_seq = topics[i].state().sequence();
                        h.first_seq = h
                            .cutover_seq
                            .checked_add(1)
                            .ok_or_else(|| invalid("sequence exhausted"))?;
                        h.descriptor_hash = [0; 32];
                        let h = canonical_bank(h)?;
                        if let Some(control) = expansion.as_mut()
                            && !control.reserve_bank(i, &h)?
                        {
                            continue;
                        }
                        let registry = topics[i].split().0.registry().clone();
                        let replacement = JoinOwner::new(Publisher::<32>::create(h)?, registry)?;
                        retired[i].push(topics[i].replace_bank(replacement, now)?);
                        config::write_bytes(
                            &config::active_resource_path(&c.topics[i].resource)?,
                            &tickwire_market_source::protocol::wire_bytes(
                                topics[i].split().0.header(),
                            )?,
                        )?;
                    }
                }
            }
        }
        let wait = if runnable {
            0
        } else {
            next.map_or(c.maximum_idle_ns, |n| {
                n.saturating_sub(now).min(c.maximum_idle_ns)
            })
        };
        Ok(PollReport {
            runnable,
            next_deadline_ns: next,
            wait_ns: wait,
        })
    }
    pub fn wait(&mut self, report: PollReport) -> Result<()> {
        self.poll
            .poll(&mut self.events, Some(Duration::from_nanos(report.wait_ns)))?;
        Ok(())
    }
}
/// Runs the same Runtime driven by integration tests and the production binary.
pub fn run(c: Config) -> Result<()> {
    let mut runtime = Runtime::new(c)?;
    loop {
        let report = runtime.poll_once()?;
        runtime.wait(report)?;
    }
}
