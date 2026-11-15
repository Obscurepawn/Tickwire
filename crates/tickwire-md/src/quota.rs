//! Pure in-memory public market-data rate-limit permit and HTTP cooldown accounting.
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};
use tickwire_binance_public as public;
use tickwire_clock::{ClockDomain, MonoTime};

const MINUTE_NS: u64 = 60_000_000_000;
const FIVE_MINUTES_NS: u64 = 5 * MINUTE_NS;
const DEFAULT_COOLDOWN_NS: u64 = 5_000_000_000;

fn default_weight_per_minute() -> u64 {
    6_000
}
fn default_connections_per_5m() -> u64 {
    300
}
fn default_max_idempotency_entries() -> usize {
    4_096
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedSource {
    SpotProduction,
    SpotTest,
    UsdmProduction,
    UsdmTest,
}
impl FeedSource {
    pub fn build(self) -> public::Source {
        match self {
            Self::SpotProduction => public::Source::SpotProduction,
            Self::SpotTest => public::Source::SpotTest,
            Self::UsdmProduction => public::Source::UsdmProduction,
            Self::UsdmTest => public::Source::UsdmTest,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PublicBinding {
    pub owner_id: [u8; 16],
    pub source: public::Source,
    pub domain: ClockDomain,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicQuotaConfig {
    #[serde(default = "default_weight_per_minute")]
    pub max_api_weight_per_minute: u64,
    #[serde(default = "default_connections_per_5m")]
    pub max_connections_per_5m: u64,
    #[serde(default = "default_max_idempotency_entries")]
    pub max_idempotency_entries: usize,
}
impl Default for PublicQuotaConfig {
    fn default() -> Self {
        Self {
            max_api_weight_per_minute: default_weight_per_minute(),
            max_connections_per_5m: default_connections_per_5m(),
            max_idempotency_entries: default_max_idempotency_entries(),
        }
    }
}
impl PublicQuotaConfig {
    pub fn build(&self, domain: ClockDomain, now: MonoTime) -> Result<PublicQuota> {
        if self.max_api_weight_per_minute == 0
            || self.max_connections_per_5m == 0
            || self.max_idempotency_entries == 0
            || self.max_idempotency_entries > 65_536
            || now.domain() != domain
        {
            return Err(invalid("MD public quota configuration"));
        }
        Ok(PublicQuota {
            config: self.clone(),
            domain,
            last_ns: now.nanos(),
            cooldown_until_ns: 0,
            weight_events: VecDeque::new(),
            weight_used: 0,
            connect_events: VecDeque::new(),
            seen_set: BTreeSet::new(),
            seen_fifo: VecDeque::with_capacity(self.max_idempotency_entries.min(1024)),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ChargeKey {
    owner_id: [u8; 16],
    source: u8,
    lane: u8,
    connection_epoch: u64,
    operation: u64,
    kind: u8,
}
impl ChargeKey {
    fn from_charge(charge: &public::PublicCharge) -> Self {
        let source = match charge.source {
            public::Source::SpotProduction => 1,
            public::Source::SpotTest => 2,
            public::Source::UsdmProduction => 3,
            public::Source::UsdmTest => 4,
        };
        let lane = match charge.lane {
            public::Lane::Stream => 1,
            public::Lane::Snapshot => 2,
        };
        let kind = match charge.kind {
            public::ChargeKind::Connect => 1,
            public::ChargeKind::Upgrade => 2,
            public::ChargeKind::Subscribe => 3,
            public::ChargeKind::Pong => 4,
            public::ChargeKind::Close => 5,
            public::ChargeKind::Snapshot => 6,
        };
        Self {
            owner_id: charge.owner_id,
            source,
            lane,
            connection_epoch: charge.connection_epoch,
            operation: charge.operation,
            kind,
        }
    }
}

/// Pure in-memory public weight and connection limiter with bounded idempotency tracking.
pub struct PublicQuota {
    config: PublicQuotaConfig,
    domain: ClockDomain,
    last_ns: u64,
    cooldown_until_ns: u64,
    weight_events: VecDeque<(u64, u64)>,
    weight_used: u64,
    connect_events: VecDeque<u64>,
    seen_set: BTreeSet<ChargeKey>,
    seen_fifo: VecDeque<ChargeKey>,
}
impl PublicQuota {
    pub fn permit(&mut self, binding: PublicBinding) -> PermitView<'_> {
        PermitView {
            quota: self,
            binding,
        }
    }
    fn prune(&mut self, now_ns: u64) {
        while let Some(&(ts, weight)) = self.weight_events.front() {
            if now_ns.saturating_sub(ts) < MINUTE_NS {
                break;
            }
            self.weight_events.pop_front();
            self.weight_used = self.weight_used.saturating_sub(weight);
        }
        while let Some(&ts) = self.connect_events.front() {
            if now_ns.saturating_sub(ts) < FIVE_MINUTES_NS {
                break;
            }
            self.connect_events.pop_front();
        }
    }
    fn record_seen(&mut self, key: ChargeKey) {
        if !self.seen_set.insert(key) {
            return;
        }
        while self.seen_fifo.len() >= self.config.max_idempotency_entries {
            if let Some(evicted) = self.seen_fifo.pop_front() {
                self.seen_set.remove(&evicted);
            }
        }
        self.seen_fifo.push_back(key);
    }
}

pub struct PermitView<'a> {
    quota: &'a mut PublicQuota,
    binding: PublicBinding,
}
impl public::PublicPermit for PermitView<'_> {
    fn allow(&mut self, charge: &public::PublicCharge, now: MonoTime) -> bool {
        if charge.owner_id != self.binding.owner_id
            || charge.source != self.binding.source
            || charge.domain != self.binding.domain
            || now.domain() != self.quota.domain
            || now.nanos() < self.quota.last_ns
        {
            return false;
        }
        let now_ns = now.nanos();
        self.quota.last_ns = now_ns;
        let key = ChargeKey::from_charge(charge);
        if self.quota.seen_set.contains(&key) {
            return true;
        }
        if now_ns < self.quota.cooldown_until_ns {
            return false;
        }
        self.quota.prune(now_ns);
        let weight = u64::from(charge.api_weight);
        if weight > 0 {
            let Some(next_weight) = self.quota.weight_used.checked_add(weight) else {
                return false;
            };
            if next_weight > self.quota.config.max_api_weight_per_minute {
                return false;
            }
        }
        let is_connect = matches!(charge.kind, public::ChargeKind::Connect);
        if is_connect
            && (self.quota.connect_events.len() as u64) >= self.quota.config.max_connections_per_5m
        {
            return false;
        }
        if weight > 0 {
            self.quota.weight_events.push_back((now_ns, weight));
            self.quota.weight_used += weight;
        }
        if is_connect {
            self.quota.connect_events.push_back(now_ns);
        }
        self.quota.record_seen(key);
        true
    }

    fn observe_http(&mut self, feedback: &public::HttpFeedback) {
        if !matches!(feedback.status, 418 | 429) {
            return;
        }
        let mut retry_ns = DEFAULT_COOLDOWN_NS;
        for (name, value) in &feedback.headers {
            if name.eq_ignore_ascii_case("retry-after")
                && let Ok(text) = std::str::from_utf8(value)
                && let Ok(seconds) = text.trim().parse::<u64>()
            {
                retry_ns = seconds.saturating_mul(1_000_000_000).max(1_000_000_000);
                break;
            }
        }
        let until = self.quota.last_ns.saturating_add(retry_ns);
        self.quota.cooldown_until_ns = self.quota.cooldown_until_ns.max(until);
    }
}
