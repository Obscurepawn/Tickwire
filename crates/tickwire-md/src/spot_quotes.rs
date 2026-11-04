//! Single-symbol, snapshot-only public WebSocket input for Bybit and OKX spot.
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_binance::decimal::Decimal;
use tickwire_venue::SpotVenue;

mod network;
mod protocol;
pub use network::{Notice, run, run_with};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteConfig {
    #[serde(deserialize_with = "venue")]
    pub venue: SpotVenue,
    pub symbol: String,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
    pub ca_pem: Option<PathBuf>,
    /// Optional explicit HTTP CONNECT proxy. TLS still verifies the venue hostname.
    pub proxy_addr: Option<std::net::SocketAddr>,
}

fn venue<'de, D: serde::Deserializer<'de>>(de: D) -> std::result::Result<SpotVenue, D::Error> {
    match String::deserialize(de)?.as_str() {
        "bybit" => Ok(SpotVenue::Bybit),
        "okx" => Ok(SpotVenue::Okx),
        _ => Err(serde::de::Error::custom("expected bybit or okx")),
    }
}

impl QuoteConfig {
    pub fn validate(&self) -> Result<()> {
        if self.instrument_id == 0
            || self.instrument_version == 0
            || self.symbol.is_empty()
            || self.symbol.len() > 64
            || !self
                .symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            || self.price_tick.mantissa() <= 0
            || self.quantity_step.mantissa() <= 0
        {
            return Err(invalid("invalid spot quote instrument or increments"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct QuoteLevel {
    pub price_ticks: i64,
    pub quantity_lots: i64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Quote<'a> {
    pub venue_id: u32,
    pub symbol: &'a str,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub connection_epoch: u64,
    pub venue_sequence: u64,
    pub update_id: Option<u64>,
    /// Source's frame timestamp (Bybit ts or OKX ts), in Unix nanoseconds.
    pub exchange_event_ns: u64,
    pub received_mono_ns: u64,
    pub bid: Option<QuoteLevel>,
    pub ask: Option<QuoteLevel>,
}

pub struct Session<'a> {
    config: &'a QuoteConfig,
    epoch: u64,
    channel: String,
    acknowledged: bool,
    failed: bool,
}

impl<'a> Session<'a> {
    pub fn new(config: &'a QuoteConfig, epoch: u64) -> Result<Self> {
        config.validate()?;
        if epoch == 0 {
            return Err(invalid("zero quote connection epoch"));
        }
        let channel = match config.venue {
            SpotVenue::Bybit => format!("orderbook.1.{}", config.symbol),
            SpotVenue::Okx => "bbo-tbt".to_owned(),
        };
        Ok(Self {
            config,
            epoch,
            channel,
            acknowledged: false,
            failed: false,
        })
    }

    pub fn subscription(&self) -> Vec<u8> {
        let request = match self.config.venue {
            SpotVenue::Bybit => {
                serde_json::json!({"req_id":"twbbo","op":"subscribe","args":[self.channel]})
            }
            SpotVenue::Okx => {
                serde_json::json!({"id":"twbbo","op":"subscribe","args":[{"channel":self.channel,"instId":self.config.symbol}]})
            }
        };
        request.to_string().into_bytes()
    }

    /// On any malformed, rejected or misrouted frame the session must reconnect.
    pub fn on_text(&mut self, bytes: &[u8], received_mono_ns: u64) -> Result<Option<Quote<'a>>> {
        if self.failed || bytes.len() > network::MAX_MESSAGE {
            self.failed = true;
            return Err(invalid("spot quote session failed or oversized frame"));
        }
        let result = match self.config.venue {
            SpotVenue::Bybit => self.bybit(bytes, received_mono_ns),
            SpotVenue::Okx => self.okx(bytes, received_mono_ns),
        };
        self.failed = result.is_err();
        result
    }
}

/// Owned update for a single source-to-publisher handoff.
#[derive(Clone, Copy, Debug)]
pub enum Publication {
    Quote(tickwire_market_publisher::SpotQuote),
    Unavailable {
        instrument: u64,
        epoch: u64,
        received_ns: u64,
    },
}

impl Notice<'_> {
    pub fn publication(&self, key: tickwire_book::BookKey) -> Result<Publication> {
        match self {
            Notice::Quote(q) => q.publication(key),
            Notice::Unavailable {
                venue_id,
                instrument_id,
                connection_epoch,
                received_mono_ns,
                ..
            } if *venue_id == key.venue => Ok(Publication::Unavailable {
                instrument: *instrument_id,
                epoch: *connection_epoch,
                received_ns: *received_mono_ns,
            }),
            _ => Err(invalid("spot venue differs from publisher binding")),
        }
    }
}
impl Quote<'_> {
    pub fn publication(&self, key: tickwire_book::BookKey) -> Result<Publication> {
        if key.venue != self.venue_id || key.source == 0 {
            return Err(invalid(
                "spot venue or source differs from publisher binding",
            ));
        }
        if let (Some(bid), Some(ask)) = (self.bid, self.ask)
            && ask.price_ticks > bid.price_ticks
        {
            return Ok(Publication::Quote(tickwire_market_publisher::SpotQuote {
                key,
                instrument: self.instrument_id,
                version: self.instrument_version,
                epoch: self.connection_epoch,
                sequence: self.venue_sequence,
                exchange_ns: self.exchange_event_ns,
                received_ns: self.received_mono_ns,
                bid_ticks: bid.price_ticks,
                bid_lots: bid.quantity_lots,
                ask_ticks: ask.price_ticks,
                ask_lots: ask.quantity_lots,
            }));
        }
        // IPC exact BBO is a two-sided uncrossed price pair. A missing or crossed
        // exchange snapshot invalidates the last usable observation immediately.
        Ok(Publication::Unavailable {
            instrument: self.instrument_id,
            epoch: self.connection_epoch,
            received_ns: self.received_mono_ns,
        })
    }
}
impl Publication {
    pub fn publish<const M: usize>(
        self,
        owner: &mut tickwire_market_publisher::TopicPublisher<M>,
        now: u64,
    ) -> Result<()> {
        match self {
            Self::Quote(q) => owner.on_spot_quote(&q, now)?,
            Self::Unavailable {
                instrument,
                epoch,
                received_ns,
            } => {
                owner.spot_quote_unavailable_at(instrument, epoch, received_ns, now)?;
            }
        }
        Ok(())
    }
}
