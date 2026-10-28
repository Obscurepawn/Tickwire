use super::*;
use tokens::Tokens;
type Spot = public::PublicDepthClient<public::BinanceSpot, SystemClock, LEVELS, UPDATES, BUFFER>;
type Usdm = public::PublicDepthClient<public::BinanceUsdM, SystemClock, LEVELS, UPDATES, BUFFER>;
pub(super) enum Client {
    Spot(Box<Spot>),
    Usdm(Box<Usdm>),
}
impl Client {
    pub(super) fn start(&mut self, p: &mut impl public::PublicPermit) -> public::Result<()> {
        match self {
            Self::Spot(c) => c.start(p),
            Self::Usdm(c) => c.start(p),
        }
    }
    pub(super) fn poll(
        &mut self,
        p: &mut impl public::PublicPermit,
        s: &mut impl public::PublicSink,
    ) -> public::Result<public::PollReport> {
        match self {
            Self::Spot(c) => c.poll(p, public::PollBudget::default(), s),
            Self::Usdm(c) => c.poll(p, public::PollBudget::default(), s),
        }
    }
    pub(super) fn register(
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
pub(super) struct Feed {
    pub(super) client: Client,
    pub(super) tokens: public::IoTokens,
    pub(super) binding: tickwire_binance_quota::PublicBinding,
    pub(super) instrument: u64,
    pub(super) topics: Vec<(usize, InstrumentSlot)>,
}
pub(super) struct EventFeed {
    pub(super) client: public::PublicEventClient<SystemClock>,
    pub(super) token: public::mio::Token,
    pub(super) kind: public::EventKind,
    pub(super) source: u64,
    pub(super) binding: tickwire_binance_quota::PublicBinding,
    pub(super) instrument: u64,
    pub(super) topics: Vec<(usize, InstrumentSlot)>,
}

pub(super) struct Reserved {
    depth: public::IoTokens,
    events: Vec<public::mio::Token>,
}
impl Reserved {
    pub(super) fn new(tokens: &mut Tokens, events: usize) -> Result<Self> {
        if events > 3 {
            return Err(invalid("event owner capacity"));
        }
        Ok(Self {
            depth: tokens.depth()?,
            events: (0..events).map(|_| tokens.event()).collect::<Result<_>>()?,
        })
    }
}
pub(super) struct Built {
    pub(super) feed: Feed,
    pub(super) events: Vec<EventFeed>,
}
impl Built {
    pub(super) fn start(
        &mut self,
        registry: &public::mio::Registry,
        quota: &mut Quota,
    ) -> Result<()> {
        self.feed
            .client
            .start(&mut quota.endpoint.public(self.feed.binding))?;
        self.feed
            .client
            .register(registry, self.feed.tokens, false)?;
        for event in &mut self.events {
            event
                .client
                .start(&mut quota.endpoint.public(event.binding))?;
            event.client.register(registry, event.token)?;
        }
        Ok(())
    }
    pub(super) fn attach(
        &mut self,
        config: &config::Feed,
        topics: &mut [TopicOwner],
    ) -> Result<()> {
        self.feed.topics = topics
            .iter_mut()
            .enumerate()
            .filter_map(|(i, topic)| {
                config
                    .topics
                    .contains(&topic.split().0.header().topic_id)
                    .then(|| {
                        topic
                            .instrument_slot(config.instrument.id)
                            .map(|slot| (i, slot))
                    })
            })
            .collect::<tickwire_market_publisher::Result<_>>()?;
        for event in &mut self.events {
            event.topics.clone_from(&self.feed.topics);
            for &(i, _) in &event.topics {
                topics[i].configure_event(event.instrument, event.kind, event.source)?;
            }
        }
        Ok(())
    }
}
fn access(path: &std::path::Path) -> Result<public::SbeAccess> {
    let bytes = config::read(path, 512)?;
    let key = std::str::from_utf8(&bytes)
        .map_err(|_| invalid("SBE API key encoding"))?
        .trim();
    Ok(public::SbeAccess::ed25519_api_key(key.to_owned())?)
}
/// Cold construction only. This does not open connections or consume quota.
pub(super) fn build(
    f: &config::Feed,
    domain: tickwire_clock::ClockDomain,
    index: &config::FeedIndex,
    tokens: Reserved,
) -> Result<Built> {
    let (config, network) = f.build(domain, index)?;
    let binding = tickwire_binance_quota::PublicBinding {
        owner_id: f.owner_id,
        source: f.source.build(),
        domain,
    };
    let client = match config.source {
        public::Source::SpotProduction | public::Source::SpotTest => {
            let clock = SystemClock::new(domain)?;
            let client = if let Some(path) = &f.sbe_api_key_file {
                Spot::new_sbe(config, network, clock, access(path)?)?
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
    let mut events = Vec::with_capacity(f.events.len());
    for (event, token) in f.events.iter().zip(tokens.events) {
        let (config, network) = event.build(f, domain, index)?;
        let clock = SystemClock::new(domain)?;
        let client = if let Some(path) = &event.sbe_api_key_file {
            public::PublicEventClient::new_sbe(config, network, clock, access(path)?)?
        } else {
            public::PublicEventClient::new(config, network, clock)?
        };
        events.push(EventFeed {
            client,
            token,
            kind: event.kind.kind(),
            source: event.stream_id,
            binding: tickwire_binance_quota::PublicBinding {
                owner_id: event.owner_id,
                source: f.source.build(),
                domain,
            },
            instrument: f.instrument.id,
            topics: Vec::new(),
        });
    }
    Ok(Built {
        feed: Feed {
            client,
            tokens: tokens.depth,
            binding,
            instrument: f.instrument.id,
            topics: Vec::new(),
        },
        events,
    })
}
