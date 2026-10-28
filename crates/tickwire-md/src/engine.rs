//! Single MD process, independent bounded network owner per instrument.
#[cfg(test)]
mod dense_tests;
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
type Spot = public::PublicDepthClient<public::BinanceSpot, SystemClock, LEVELS, UPDATES, BUFFER>;
type Usdm = public::PublicDepthClient<public::BinanceUsdM, SystemClock, LEVELS, UPDATES, BUFFER>;
enum Client {
    Spot(Box<Spot>),
    Usdm(Box<Usdm>),
}
impl Client {
    fn start(&mut self, p: &mut impl public::PublicPermit) -> public::Result<()> {
        match self {
            Self::Spot(c) => c.start(p),
            Self::Usdm(c) => c.start(p),
        }
    }
    fn poll(
        &mut self,
        p: &mut impl public::PublicPermit,
        s: &mut impl public::PublicSink,
    ) -> public::Result<public::PollReport> {
        match self {
            Self::Spot(c) => c.poll(p, public::PollBudget::default(), s),
            Self::Usdm(c) => c.poll(p, public::PollBudget::default(), s),
        }
    }
    fn register(
        &mut self,
        r: &public::mio::Registry,
        t: public::IoTokens,
        again: bool,
    ) -> public::Result<()> {
        match self {
            Self::Spot(c) => {
                if again {
                    c.reregister(r, t)
                } else {
                    c.register(r, t)
                }
            }
            Self::Usdm(c) => {
                if again {
                    c.reregister(r, t)
                } else {
                    c.register(r, t)
                }
            }
        }
    }
}
struct Feed {
    client: Client,
    binding: tickwire_binance_quota::PublicBinding,
    instrument: u64,
    topics: Vec<(usize, InstrumentSlot)>,
}
struct EventFeed {
    client: public::PublicEventClient<SystemClock>,
    binding: tickwire_binance_quota::PublicBinding,
    instrument: u64,
    topics: Vec<(usize, InstrumentSlot)>,
}
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
    let t = c
        .topics
        .iter()
        .find(|t| {
            config::read(&t.resource, 4096)
                .ok()
                .and_then(|b| ResourceHeader::decode(&b).ok())
                .is_some_and(|x| x.topic_id == h.topic_id)
        })
        .ok_or_else(|| invalid("topic config"))?;
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
pub fn run(c: Config) -> Result<()> {
    if c.feeds.is_empty()
        || c.feeds.len() > 32
        || c.topics.is_empty()
        || c.topics.len() > 32
        || c.maximum_idle_ns == 0
        || c.maximum_idle_ns > 100_000_000
    {
        return Err(invalid("MD owner bounds"));
    }
    let index = config::FeedIndex::new(&c.feeds)?;
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
    for t in &c.topics {
        let h = ResourceHeader::decode(&config::read(&t.resource, 4096)?)?;
        if h.producer_instance != c.identity.instance
            || h.producer_generation != c.identity.generation
            || h.host_boot_id != c.identity.host_boot
            || h.namespace_id != c.identity.namespace
            || !topic_ids.insert(h.topic_id)
        {
            return Err(invalid("MD topic resource identity"));
        }
        let (registry, instruments) = topic_registry(&c, &h, &index)?;
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
    let mut poll = public::mio::Poll::new()?;
    let mut events = public::mio::Events::with_capacity(256);
    let mut feeds = Vec::new();
    let mut event_feeds = Vec::new();
    let mut owners = BTreeSet::new();
    for f in &c.feeds {
        if !owners.insert(f.owner_id)
            || f.topics.is_empty()
            || f.topics.iter().collect::<BTreeSet<_>>().len() != f.topics.len()
            || f.topics.iter().any(|t| !topic_ids.contains(t))
        {
            return Err(invalid("MD feed owner/topics"));
        }
        let (config, network) = f.build(domain, &index)?;
        let binding = tickwire_binance_quota::PublicBinding {
            owner_id: f.owner_id,
            source: f.source.build(),
            domain,
        };
        let mut client = match config.source {
            public::Source::SpotProduction | public::Source::SpotTest => {
                let clock = SystemClock::new(domain)?;
                let client = if let Some(path) = &f.sbe_api_key_file {
                    let bytes = config::read(path, 512)?;
                    let key = std::str::from_utf8(&bytes)
                        .map_err(|_| invalid("SBE API key encoding"))?
                        .trim()
                        .to_owned();
                    Spot::new_sbe(
                        config,
                        network,
                        clock,
                        public::SbeAccess::ed25519_api_key(key)?,
                    )?
                } else {
                    Spot::new(config, network, clock)?
                };
                Client::Spot(Box::new(client))
            }
            _ => Client::Usdm(Box::new(Usdm::new(
                config,
                network,
                SystemClock::new(domain)?,
            )?)),
        };
        client.start(&mut quota.endpoint.public(binding))?;
        client.register(
            poll.registry(),
            public::IoTokens {
                stream: public::mio::Token(feeds.len() * 2),
                snapshot: public::mio::Token(feeds.len() * 2 + 1),
            },
            false,
        )?;
        let targets = topics
            .iter_mut()
            .enumerate()
            .filter_map(|(i, t)| {
                f.topics
                    .contains(&t.split().0.header().topic_id)
                    .then(|| t.instrument_slot(f.instrument.id).map(|slot| (i, slot)))
            })
            .collect::<tickwire_market_publisher::Result<Vec<_>>>()?;
        let mut event_kinds = BTreeSet::new();
        let mut event_sources = BTreeSet::new();
        if f.events.len() > 3 {
            return Err(invalid("event owner capacity"));
        }
        for event in &f.events {
            if !owners.insert(event.owner_id)
                || !event_kinds.insert(event.kind.state())
                || !event_sources.insert(event.stream_id)
            {
                return Err(invalid("duplicate event owner/kind/source"));
            }
            let (event_config, network) = event.build(f, domain, &index)?;
            for &(i, _) in &targets {
                topics[i].configure_event(f.instrument.id, event.kind.kind(), event.stream_id)?;
            }
            let binding = tickwire_binance_quota::PublicBinding {
                owner_id: event.owner_id,
                source: f.source.build(),
                domain,
            };
            let clock = SystemClock::new(domain)?;
            let mut event_client = if let Some(path) = &event.sbe_api_key_file {
                let bytes = config::read(path, 512)?;
                let key = std::str::from_utf8(&bytes)
                    .map_err(|_| invalid("SBE event API key encoding"))?
                    .trim()
                    .to_owned();
                public::PublicEventClient::new_sbe(
                    event_config,
                    network,
                    clock,
                    public::SbeAccess::ed25519_api_key(key)?,
                )?
            } else {
                public::PublicEventClient::new(event_config, network, clock)?
            };
            event_client.start(&mut quota.endpoint.public(binding))?;
            event_client.register(
                poll.registry(),
                public::mio::Token(c.feeds.len() * 2 + event_feeds.len()),
            )?;
            event_feeds.push(EventFeed {
                client: event_client,
                binding,
                instrument: f.instrument.id,
                topics: targets.clone(),
            });
        }
        feeds.push(Feed {
            client,
            binding,
            instrument: f.instrument.id,
            topics: targets,
        });
    }
    let mut server = Server::new(&c)?;
    // Keep old mapped owners alive: outstanding guards/cursors may still refer
    // to them. Explicit control supplies each successor; never recycle in place.
    let mut retired: Vec<Vec<JoinOwner<32>>> = (0..topics.len()).map(|_| Vec::new()).collect();
    let mut first = 0;
    loop {
        let mut runnable = false;
        let mut next = None::<u64>;
        for offset in 0..feeds.len() {
            let i = (first + offset) % feeds.len();
            let f = &mut feeds[i];
            let mut sink = Fanout {
                topics: &mut topics,
                targets: &f.topics,
                instrument: f.instrument,
                clock: &mut clock,
            };
            let r = f
                .client
                .poll(&mut quota.endpoint.public(f.binding), &mut sink)?;
            runnable |= r.runnable;
            if let Some(t) = r.next_deadline_ns {
                next = Some(next.map_or(t, |old| old.min(t)));
            }
            if r.needs_registration {
                f.client.register(
                    poll.registry(),
                    public::IoTokens {
                        stream: public::mio::Token(i * 2),
                        snapshot: public::mio::Token(i * 2 + 1),
                    },
                    true,
                )?;
            }
        }
        for offset in 0..event_feeds.len() {
            let i = (first + offset) % event_feeds.len();
            let f = &mut event_feeds[i];
            let mut sink = EventFanout {
                topics: &mut topics,
                targets: &f.topics,
                instrument: f.instrument,
                clock: &mut clock,
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
                f.client
                    .reregister(poll.registry(), public::mio::Token(c.feeds.len() * 2 + i))?;
            }
        }
        first = (first + 1) % feeds.len();
        let now = clock.sample()?.monotonic.nanos();
        server.poll(&mut topics, now)?;
        for i in 0..topics.len() {
            if topics[i].needs_bank() {
                server.invalidate_topic(i, &mut topics[i])?;
                if !server.has_peers(i) && topics[i].state().cached_live() {
                    let old = topics[i].split().0.header().clone();
                    let mut h =
                        ResourceHeader::decode(&config::read(&c.topics[i].resource, 4096)?)?;
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
        poll.poll(&mut events, Some(Duration::from_nanos(wait)))?;
    }
}
