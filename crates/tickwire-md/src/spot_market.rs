//! One public spot WebSocket and one authenticated shared-memory BBO topic.
use crate::{
    Result, config, invalid,
    server::{Server, ServerConfig, TopicOwner},
    spot_quotes::{self, Publication, QuoteConfig},
};
use serde::Deserialize;
use std::{io::ErrorKind, path::PathBuf, sync::mpsc, time::Duration};
use tickwire_book::BookKey;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
use tickwire_control_sys::{
    CpuSet, pin_current_thread, spawn_pinned_thread, validate_cpu_isolation,
};
use tickwire_instrument_catalog::{Catalog, Market};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_join::{JoinOwner, TopicRegistry};
use tickwire_market_publisher::Limits;
use tickwire_market_source::protocol::wire_bytes;
use tickwire_shm::Publisher;
use tickwire_snapshot::{InstrumentSpec, QUOTE};
use tickwire_venue::{Decimal, Environment, Product};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: QuoteConfig,
    pub resource: PathBuf,
    pub catalog: config::CatalogFile,
    pub clock_generation: u64,
    pub source_id: u32,
    pub instrument_set_version: u64,
    pub server: ServerConfig,
    #[serde(default)]
    pub reactor_cpu: Option<u32>,
    #[serde(default, with = "config::cpu_set_serde")]
    pub housekeeping_cpus: Option<CpuSet>,
}

fn now(clock: &mut SystemClock) -> Result<u64> {
    clock
        .sample()
        .map(|s| s.monotonic)
        .map(MonoTime::nanos)
        .map_err(|err| invalid(&format!("spot market clock: {err:?}")))
}

fn decimal(value: tickwire_binance::decimal::Decimal) -> Result<Decimal> {
    Decimal::new(value.mantissa(), -i8::try_from(value.scale())?)
        .map_err(|e| invalid(&format!("spot market increment: {e:?}")))
}

impl Config {
    fn topic(&self, resource: &config::ResourceSpec, key: BookKey) -> Result<TopicOwner> {
        let hash = tickwire_config::artifacts::reference_hash(&self.catalog.reference)
            .map_err(|_| invalid("invalid approved catalog reference"))?;
        let raw = config::read(&self.catalog.path, tickwire_instrument_catalog::MAX_BYTES)?;
        let catalog =
            Catalog::decode(&raw, hash).map_err(|e| invalid(&format!("spot catalog: {e:?}")))?;
        let entry = catalog
            .instrument(self.source.instrument_id)
            .ok_or_else(|| invalid("unknown spot instrument"))?;
        let binding = entry.binding();
        if catalog.venue() != key.venue
            || catalog.environment() != Environment::Production
            || catalog.market() != Market::Spot
            || entry.product() != Product::Spot
            || binding.instrument_version != self.source.instrument_version
            || catalog.catalog_version() != resource.catalog_version
            || binding.symbol.as_str() != self.source.symbol
            || binding.price_tick != decimal(self.source.price_tick)?
            || binding.quantity_step != decimal(self.source.quantity_step)?
            || self.instrument_set_version == 0
        {
            return Err(invalid(
                "public venue, symbol, version or increments differ from approved spot catalog",
            ));
        }
        let registry = TopicRegistry {
            catalog_hash: catalog.catalog_hash(),
            instrument_set_version: self.instrument_set_version,
            instruments: vec![InstrumentSpec {
                instrument_id: binding.instrument_id,
                instrument_version: binding.instrument_version,
                required_states: QUOTE,
            }],
        };
        let owner = JoinOwner::new(
            Publisher::<32>::create(resource.header_with_exact_events()?)?,
            registry,
        )?;
        Ok(TopicOwner::new_spot_quotes(
            owner,
            vec![(binding.clone(), key)],
            Limits {
                levels_per_side: 1,
                cache_bytes: 1024 * 1024,
                snapshot_bytes: 1024 * 1024,
                max_delta_bytes: 4096,
            },
        )?)
    }
}

// The active header names the bank clients must JOIN; a restarted process cannot reuse its generation.
fn publish_header(file: &std::path::Path, header: &ResourceHeader) -> Result<()> {
    match std::fs::symlink_metadata(file) {
        Ok(_) => {
            let old = ResourceHeader::decode(&config::read(file, 4096)?)?;
            if old.topic_id != header.topic_id
                || old.namespace_id != header.namespace_id
                || header.ring_epoch <= old.ring_epoch
                || header.feed_generation <= old.feed_generation
            {
                return Err(invalid(
                    "restart requires a fresh ring epoch and feed generation for this topic",
                ));
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    config::write_bytes(file, &wire_bytes(header)?)
}

pub fn run(config: Config) -> Result<()> {
    validate_cpu_isolation(
        &[("reactor_cpu", config.reactor_cpu)],
        config.housekeeping_cpus.as_ref(),
    )?;
    let resource: config::ResourceSpec =
        serde_json::from_slice(&config::read(&config.resource, 1024 * 1024)?)?;
    let domain = ClockDomain::new(resource.identity.host_boot, config.clock_generation)
        .map_err(|e| invalid(&format!("spot clock domain: {e:?}")))?;
    let mut clock =
        SystemClock::new(domain).map_err(|e| invalid(&format!("spot market clock: {e:?}")))?;
    let key = BookKey {
        venue: config.source.venue.id(),
        source: config.source_id,
        instrument: 0,
    };
    let mut topic = config.topic(&resource, key)?;
    spot_quotes::Session::new(&config.source, 1)?;
    let mut server = Server::open(&resource.identity, &config.server)?;
    let active = config::active_resource_path(&config.resource)?;
    publish_header(&active, topic.split().0.header())?;
    let (tx, rx) = mpsc::sync_channel(1);
    let source = config.source;
    let housekeeping = config.housekeeping_cpus;
    let reactor_cpu = config.reactor_cpu;
    spawn_pinned_thread("md-cold", housekeeping.as_ref(), move || {
        let run = || -> Result<()> {
            let mut clock = SystemClock::new(domain)
                .map_err(|e| invalid(&format!("spot input clock: {e:?}")))?;
            spot_quotes::run_with(
                &source,
                || now(&mut clock),
                |notice| {
                    tx.send(Ok(notice.publication(key)?))
                        .map_err(|_| invalid("spot topic owner closed"))
                },
            )
        };
        if let Err(e) = run() {
            let _ = tx.send(Err(e.to_string()));
        }
    })?;
    if let Some(cpu) = reactor_cpu {
        pin_current_thread(&CpuSet::single(cpu)?)?;
    }
    eprintln!(
        "{}",
        serde_json::json!({"event":"spot_market_started","topic":resource.topic_id,"active_resource":active})
    );
    loop {
        server.poll(std::slice::from_mut(&mut topic), now(&mut clock)?)?;
        if topic.needs_bank() {
            return Err(invalid("spot topic needs a fresh bank"));
        }
        match rx.recv_timeout(Duration::from_millis(1)) {
            Ok(Ok(publication)) => Publication::publish(publication, &mut topic, now(&mut clock)?)?,
            Ok(Err(message)) => return Err(invalid(&message)),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(invalid("spot public source terminated"));
            }
        }
    }
}
