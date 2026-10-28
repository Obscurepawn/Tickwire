use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::ToSocketAddrs,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tickwire_binance_public as public;
use tickwire_market_source::protocol::IdentityConfig;
use tickwire_transport::rustls;
use tickwire_venue as v;
mod expansion;
mod feed_index;
pub use expansion::ExpansionControl;
pub use feed_index::FeedIndex;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    pub id: u64,
    pub version: u64,
    pub symbol: String,
    pub exponent: i8,
}
impl Asset {
    fn build(&self) -> Result<v::AssetRef> {
        Ok(v::AssetRef {
            id: self.id,
            version: self.version,
            symbol: v::AssetSymbol::new(&self.symbol)
                .map_err(|e| invalid(&format!("asset {e:?}")))?,
            exponent: self.exponent,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub id: u64,
    pub version: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
    pub symbol: String,
    pub base: Asset,
    pub quote: Asset,
    pub settlement: Asset,
    pub price_tick: String,
    pub quantity_step: String,
    /// None selects perpetual for USD-M, Cash for Spot; explicit expiry selects a USD-M delivery contract.
    pub expiry_utc_ns: Option<u64>,
}
impl Instrument {
    pub fn build(&self, source: public::Source) -> Result<v::InstrumentBinding> {
        let spot = matches!(
            source,
            public::Source::SpotProduction | public::Source::SpotTest
        );
        if self.id == 0
            || self.version == 0
            || self.catalog_version == 0
            || self.catalog_hash == [0; 32]
            || (spot && self.expiry_utc_ns.is_some())
        {
            return Err(invalid("instrument identity"));
        }
        Ok(v::InstrumentBinding {
            instrument_id: self.id,
            instrument_version: self.version,
            catalog_version: self.catalog_version,
            catalog_hash: self.catalog_hash,
            symbol: v::Symbol::new(&self.symbol).map_err(|e| invalid(&format!("symbol {e:?}")))?,
            base: self.base.build()?,
            quote: self.quote.build()?,
            settlement: self.settlement.build()?,
            price_tick: self
                .price_tick
                .parse()
                .map_err(|e| invalid(&format!("tick {e:?}")))?,
            quantity_step: self
                .quantity_step
                .parse()
                .map_err(|e| invalid(&format!("step {e:?}")))?,
            contract: if spot {
                v::ContractKind::Cash
            } else if let Some(expiry) = self.expiry_utc_ns {
                v::ContractKind::Delivery {
                    expiry_utc_ns: expiry,
                }
            } else {
                v::ContractKind::Perpetual
            },
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feed {
    pub owner_id: [u8; 16],
    pub source: tickwire_binance_quota::bootstrap::Source,
    pub instrument: Instrument,
    pub venue_id: u32,
    pub stream_id: u64,
    pub first_connection_epoch: u64,
    pub first_operation: u64,
    pub initial_book_generation: u64,
    pub snapshot_limit: usize,
    pub stream_address: String,
    pub snapshot_address: String,
    pub trust_anchors: Vec<PathBuf>,
    /// SBE access uses an Ed25519 API key in the Upgrade header; no private signing key.
    #[serde(default)]
    pub sbe_api_key_file: Option<PathBuf>,
    pub topics: Vec<u64>,
    #[serde(default)]
    pub events: Vec<EventFeed>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Trade,
    BestBidAsk,
    MarkFunding,
}
impl EventType {
    pub fn kind(self) -> public::EventKind {
        match self {
            Self::Trade => public::EventKind::Trade,
            Self::BestBidAsk => public::EventKind::BestBidAsk,
            Self::MarkFunding => public::EventKind::MarkFunding,
        }
    }
    pub fn state(self) -> u8 {
        match self {
            Self::Trade => 2,
            Self::BestBidAsk => 1,
            Self::MarkFunding => 4,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventFeed {
    pub owner_id: [u8; 16],
    pub stream_id: u64,
    pub kind: EventType,
    pub first_connection_epoch: u64,
    pub first_operation: u64,
    pub stream_address: String,
    #[serde(default)]
    pub sbe_api_key_file: Option<PathBuf>,
    #[serde(default)]
    pub fast_mark: bool,
}
impl EventFeed {
    pub fn context(&self, feed: &Feed) -> Result<public::EventContext> {
        let source = feed.source.build();
        let instrument = feed.instrument.build(source)?;
        if self.owner_id == [0; 16]
            || self.owner_id == feed.owner_id
            || self.stream_id == 0
            || self.stream_id == feed.stream_id
            || self.first_connection_epoch == 0
            || self.first_operation == 0
            || self.fast_mark && !matches!(self.kind, EventType::MarkFunding)
            || self.sbe_api_key_file.is_some() && source != public::Source::SpotProduction
        {
            return Err(invalid("event source/product/identity"));
        }
        let context = public::EventContext {
            market: if matches!(
                source,
                public::Source::SpotProduction | public::Source::SpotTest
            ) {
                tickwire_binance::request::Market::Spot
            } else {
                tickwire_binance::request::Market::Usdm
            },
            identity: public::StreamIdentity {
                source_id: self.stream_id,
                connection_epoch: self.first_connection_epoch,
                instrument_id: instrument.instrument_id,
                instrument_version: instrument.instrument_version,
                catalog_version: instrument.catalog_version,
                catalog_hash: instrument.catalog_hash,
            },
            instrument,
            kind: self.kind.kind(),
            time_unit: if self.sbe_api_key_file.is_some() {
                public::TimeUnit::Microseconds
            } else {
                public::TimeUnit::Milliseconds
            },
        };
        context
            .stream_name(
                if self.sbe_api_key_file.is_some() {
                    public::EventEncoding::SpotSbeV1
                } else {
                    public::EventEncoding::Json
                },
                self.fast_mark,
            )
            .map_err(|e| invalid(&format!("event stream {e:?}")))?;
        Ok(context)
    }
    pub fn build(
        &self,
        feed: &Feed,
        domain: public::ClockDomain,
        index: &FeedIndex,
    ) -> Result<(public::EventConfig, public::EventNetworkConfig)> {
        let (base, network) = feed.build(domain, index)?;
        let ctx = self.context(feed)?;
        let stream_address = self
            .stream_address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| invalid("event stream address"))?;
        Ok((
            public::EventConfig {
                owner_id: self.owner_id,
                source: base.source,
                domain,
                first_connection_epoch: self.first_connection_epoch,
                first_operation: self.first_operation,
                context: ctx,
                encoding: if self.sbe_api_key_file.is_some() {
                    public::EventEncoding::SpotSbeV1
                } else {
                    public::EventEncoding::Json
                },
                subscribe: public::SubscribeMode::Explicit,
                fast_mark: self.fast_mark,
                timing: base.timing,
                limits: base.limits,
                max_events: 4096,
            },
            public::EventNetworkConfig {
                stream_address,
                tls: network.tls,
            },
        ))
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topic {
    /// Control may atomically replace this file with a higher ring/feed generation.
    pub resource: PathBuf,
    pub instrument_set_version: u64,
    pub maximum_levels: usize,
    pub maximum_cache_bytes: usize,
    pub maximum_snapshot_bytes: usize,
    pub maximum_delta_bytes: usize,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub identity: IdentityConfig,
    pub clock_generation: u64,
    pub socket: PathBuf,
    pub admission_directory: PathBuf,
    pub quota_directory: PathBuf,
    pub quota: tickwire_binance_quota::bootstrap::Config,
    pub control_directory: PathBuf,
    pub control_stream: [u8; 16],
    pub control_domain: [u8; 16],
    pub control_writer_epoch: u64,
    pub feeds: Vec<Feed>,
    pub topics: Vec<Topic>,
    pub maximum_idle_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expansion: Option<ExpansionControl>,
}
pub fn read(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.mode() & 0o022 != 0 || meta.len() > max as u64 {
        return Err(invalid("trusted file type/permission/size"));
    }
    let mut out = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut out)?;
    if out.len() > max {
        return Err(invalid("file size"));
    }
    Ok(out)
}
pub fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(invalid("control file size"));
    }
    let temp = path.with_extension(format!("pending-{}", std::process::id()));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(path.parent().ok_or_else(|| invalid("parent"))?)?.sync_all()?;
    Ok(())
}
impl Feed {
    pub fn build(
        &self,
        domain: public::ClockDomain,
        index: &FeedIndex,
    ) -> Result<(public::Config, public::NetworkConfig)> {
        let key = index.book_key(self)?;
        let source = self.source.build();
        let instrument = self.instrument.build(source)?;
        if self.sbe_api_key_file.is_some() && source != public::Source::SpotProduction {
            return Err(invalid("SBE requires Spot production source"));
        }
        if self.trust_anchors.is_empty() || self.trust_anchors.len() > 128 {
            return Err(invalid("trust roots"));
        }
        let mut roots = rustls::RootCertStore::empty();
        for path in &self.trust_anchors {
            roots.add(rustls::pki_types::CertificateDer::from(read(
                path,
                1024 * 1024,
            )?))?;
        }
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let stream_address = self
            .stream_address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| invalid("stream address"))?;
        let snapshot_address = self
            .snapshot_address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| invalid("snapshot address"))?;
        let config = public::Config {
            owner_id: self.owner_id,
            source,
            domain,
            first_connection_epoch: self.first_connection_epoch,
            first_operation: self.first_operation,
            depth: public::DepthConfig {
                instrument,
                key,
                time_unit: if self.sbe_api_key_file.is_some() {
                    public::TimeUnit::Microseconds
                } else {
                    public::TimeUnit::Milliseconds
                },
                snapshot_limit: self.snapshot_limit,
                max_message_bytes: 2 * 1024 * 1024,
                initial_book_generation: self.initial_book_generation,
                previous_connection_epoch: self
                    .first_connection_epoch
                    .checked_sub(1)
                    .ok_or_else(|| invalid("connection epoch"))?,
            },
            subscribe: public::SubscribeMode::Explicit,
            speed: if self.sbe_api_key_file.is_some() {
                public::DepthSpeed::Default
            } else {
                public::DepthSpeed::Ms100
            },
            timing: public::Timing {
                connect_ns: 10_000_000_000,
                request_ns: 10_000_000_000,
                first_event_ns: 30_000_000_000,
                heartbeat_ns: 180_000_000_000,
                pong_ns: 10_000_000_000,
                rotate_ns: 23 * 60 * 60 * 1_000_000_000,
                backoff_ns: 1_000_000_000,
                max_backoff_ns: 30_000_000_000,
            },
            limits: public::Limits {
                io: public::IoLimits::default(),
                request_bytes: 16 * 1024,
                message_bytes: 2 * 1024 * 1024,
                http_bytes: 2 * 1024 * 1024,
                http_headers: 64,
                control_frames: 32,
                timestamp_segments: 1024,
                total_buffer_bytes: PUBLIC_BUFFER_BYTES,
            },
        };
        Ok((
            config,
            public::NetworkConfig {
                stream_address,
                snapshot_address,
                tls,
            },
        ))
    }
}
const PUBLIC_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Control-provided immutable identity; this helper derives only wire layout.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub identity: IdentityConfig,
    pub resource_id: [u8; 16],
    pub channel_id: [u8; 16],
    pub topic_id: u64,
    pub channel_epoch: u64,
    pub ring_epoch: u64,
    pub feed_generation: u64,
    pub catalog_version: u64,
    pub first_seq: u64,
    pub initial_membership_version: u64,
    pub capacity: u32,
    pub created_mono_ns: u64,
    pub arena_classes: Vec<(u32, u32)>,
}
impl ResourceSpec {
    /// Explicit feature negotiation is required before publishing types 264–266.
    pub fn header_with_exact_events(&self) -> Result<tickwire_ipc::ResourceHeader> {
        let mut h = self.header()?;
        h.required_features |= tickwire_ipc::EXACT_MARKET_EVENTS;
        tickwire_shm::validate_bank(&h)?;
        Ok(h)
    }
    pub fn header(&self) -> Result<tickwire_ipc::ResourceHeader> {
        use tickwire_ipc::ResourceHeader;
        if self.identity.role != 2
            || self.capacity == 0
            || self.capacity > 1 << 20
            || self.topic_id == 0
            || self.catalog_version == 0
            || self.arena_classes.last().map(|c| c.0) != Some(65536)
        {
            return Err(invalid("resource specification"));
        }
        let mut h = ResourceHeader {
            magic: *b"TWIPC001",
            abi_major: 1,
            header_bytes: 4096,
            resource_kind: 1,
            target_arch: self.identity.arch,
            endian: 1,
            atomic_width: 8,
            page_shift: 12,
            isolation_shift: 7,
            record_bytes: 512,
            capacity: self.capacity,
            resource_bytes: 8192 + u64::from(self.capacity) * 512,
            resource_id: self.resource_id,
            channel_id: self.channel_id,
            producer_instance: self.identity.instance,
            producer_generation: self.identity.generation,
            host_boot_id: self.identity.host_boot,
            namespace_id: self.identity.namespace,
            channel_epoch: self.channel_epoch,
            ring_epoch: self.ring_epoch,
            feed_generation: self.feed_generation,
            first_seq: self.first_seq,
            initial_membership_version: self.initial_membership_version,
            producer_page_offset: 4096,
            slot_offset: 8192,
            max_consumers: 32,
            schema_major: 1,
            clock_domain: 1,
            required_features: 0x5e,
            trust_profile: u16::from(self.identity.trust_profile),
            event_class: 7,
            topic_id: self.topic_id,
            catalog_version: self.catalog_version,
            qos: 1,
            created_mono_ns: self.created_mono_ns,
            ..Default::default()
        };
        tickwire_shm::configure_market_arena(&mut h, &self.arena_classes)?;
        Ok(h)
    }
}
pub fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension(format!("pending-{}", std::process::id()));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(path.parent().ok_or_else(|| invalid("parent"))?)?.sync_all()?;
    Ok(())
}
/// MD exports the exact constructed header here, including its actual cutover.
pub fn active_resource_path(proposal: &Path) -> Result<PathBuf> {
    let name = proposal
        .file_name()
        .ok_or_else(|| invalid("resource filename"))?;
    let mut active = name.to_os_string();
    active.push(".active");
    Ok(proposal.with_file_name(active))
}
/// Human-sized form; `--client` inserts the exact 4096-byte resource image.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTemplate {
    pub endpoint: tickwire_market_source::protocol::Endpoint,
    pub resource: PathBuf,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<tickwire_market_source::protocol::Instrument>,
    pub cursor_id: [u8; 16],
    pub subscription_generation: u64,
    pub request_id: [u8; 16],
    pub maximum_levels: usize,
    pub startup_timeout_ms: u64,
    pub clock_generation: u64,
}
impl ClientTemplate {
    pub fn build(self) -> Result<tickwire_market_source::ConnectConfig> {
        Ok(tickwire_market_source::ConnectConfig {
            endpoint: self.endpoint,
            bank_header: read(&self.resource, 4096)?,
            catalog_hash: self.catalog_hash,
            instrument_set_version: self.instrument_set_version,
            instruments: self.instruments,
            cursor_id: self.cursor_id,
            subscription_generation: self.subscription_generation,
            request_id: self.request_id,
            maximum_levels: self.maximum_levels,
            startup_timeout_ms: self.startup_timeout_ms,
            clock_generation: self.clock_generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_owner_budget_covers_full_depth_fifo_and_both_connections() {
        use crate::engine::{BUFFER, LEVELS, UPDATES};
        let io = public::IoLimits::default();
        let total = io.max_receive_bytes * 4
            + io.tls_buffer_bytes * 2
            + io.max_send_bytes * 2
            + 2 * 1024 * 1024 * 3
            + 2 * 1024 * 1024 * 2
            + 16 * 1024 * (32 + 2)
            + 1024 * std::mem::size_of::<(usize, u64)>()
            + std::mem::size_of::<tickwire_binance::feed::DepthUpdate<UPDATES>>() * BUFFER
            + std::mem::size_of::<
                tickwire_binance::depth_sync::DepthSync<
                    tickwire_book::BinanceSpot,
                    LEVELS,
                    UPDATES,
                    BUFFER,
                >,
            >();
        assert!(total > 8 * 1024 * 1024);
        assert!(total <= PUBLIC_BUFFER_BYTES);
    }
    #[test]
    fn resource_template_preserves_full_depth_and_unique_control_identity() {
        let mut spec: ResourceSpec =
            serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
        spec.identity.arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
        let h = spec.header().unwrap();
        assert_eq!(h.resource_id, spec.resource_id);
        assert_eq!(h.required_features & 66, 66);
        assert_eq!(h.class_descriptors[1].payload_limit, 65536);
        assert_eq!(h.max_consumers, 32);
        spec.arena_classes.pop();
        assert!(spec.header().is_err());
    }
    #[test]
    fn deployment_example_contains_two_independent_owners_and_shared_topic() {
        let c: Config = serde_json::from_str(include_str!("../examples/md.json")).unwrap();
        assert_eq!(c.feeds.len(), 2);
        assert_eq!(c.topics.len(), 2);
        assert_ne!(c.feeds[0].owner_id, c.feeds[1].owner_id);
        assert!(c.feeds.iter().all(|f| f.topics.contains(&101)));
        assert_eq!(
            c.feeds[0]
                .instrument
                .build(c.feeds[0].source.build())
                .unwrap()
                .symbol
                .as_str(),
            "BTCUSDT"
        );
    }
}

#[cfg(test)]
mod event_config_tests {
    use super::*;
    #[test]
    fn explicit_events_bind_distinct_source_and_exact_product() {
        let c: Config = serde_json::from_str(include_str!("../examples/md.json")).unwrap();
        let feed = &c.feeds[0];
        let mut e = EventFeed {
            owner_id: [90; 16],
            stream_id: 90,
            kind: EventType::Trade,
            first_connection_epoch: 1,
            first_operation: 1,
            stream_address: "127.0.0.1:443".into(),
            sbe_api_key_file: None,
            fast_mark: false,
        };
        assert_eq!(e.context(feed).unwrap().identity.source_id, 90);
        e.kind = EventType::MarkFunding;
        assert!(e.context(feed).is_err());
        e.kind = EventType::BestBidAsk;
        e.sbe_api_key_file = Some("key".into());
        assert_eq!(
            e.context(feed).unwrap().time_unit,
            public::TimeUnit::Microseconds
        );
    }
}
