//! Named deployment rules converted to exact quota identities. The caller supplies
//! the authoritative store explicitly; repeated startup never resets its history.
use super::{Endpoint, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tickwire_quota as q;
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    SpotProduction,
    SpotTest,
    UsdmProduction,
    UsdmTest,
}
impl Source {
    pub fn build(self) -> tickwire_clock::Source {
        match self {
            Self::SpotProduction => tickwire_clock::Source::SpotProduction,
            Self::SpotTest => tickwire_clock::Source::SpotTest,
            Self::UsdmProduction => tickwire_clock::Source::UsdmProduction,
            Self::UsdmTest => tickwire_clock::Source::UsdmTest,
        }
    }
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Ip,
    Uid,
    Account,
    Connection,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Normal,
    Recovery,
}
impl Class {
    pub fn build(self) -> q::Class {
        match self {
            Self::Normal => q::Class::Normal,
            Self::Recovery => q::Class::Recovery,
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub name: String,
    pub namespace: [u8; 16],
    pub scope: Scope,
    pub subject: [u8; 16],
    pub pool: u64,
    pub window: u64,
    pub revision: u64,
    pub starts_ns: u64,
    pub ends_ns: u64,
    pub limit: u64,
    pub recovery_reserve: u64,
}
impl Window {
    pub fn key(&self) -> q::WindowKey {
        q::WindowKey {
            bucket: q::BucketId {
                namespace: self.namespace,
                scope: match self.scope {
                    Scope::Ip => q::ScopeKind::Ip,
                    Scope::Uid => q::ScopeKind::Uid,
                    Scope::Account => q::ScopeKind::Account,
                    Scope::Connection => q::ScopeKind::Connection,
                },
                subject: self.subject,
                pool: self.pool,
            },
            window: self.window,
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cost {
    pub window: String,
    pub units: u64,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub request_id: u64,
    pub class: Class,
    pub expires_ns: u64,
    pub costs: Vec<Cost>,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeKind {
    Connect,
    Query,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateKind {
    StreamConnect,
    ManagementConnect,
    Upgrade,
    Pong,
    Close,
    SpotLogon,
    SpotSubscribe,
    SpotSubscribeSignature,
    SpotStatus,
    SpotSubscriptions,
    SpotUnsubscribe,
    SpotLogout,
    UsdmCreate,
    UsdmKeepalive,
    UsdmClose,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicKind {
    Connect,
    Upgrade,
    Subscribe,
    Pong,
    Close,
    Snapshot,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadKind {
    Connect,
    TradePage,
    AccountSnapshot,
    OpenOrders,
    ConditionalOrders,
    OrderQuery,
    Income,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    Stream,
    Snapshot,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Rest,
    WebSocket,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListKind {
    Oco,
    Oto,
    Otoco,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    New,
    Cancel,
    CancelReplace,
    Amend,
    Modify,
    Query,
    CancelList,
    Algo,
    CancelAlgo,
    SpotList {
        kind_of_list: ListKind,
        legs: u8,
        advanced: bool,
    },
    PeggedNew,
    AdvancedCancelReplace,
    Sor,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    GuardianConnect,
    Read {
        kind: ReadKind,
    },
    Time {
        kind: TimeKind,
    },
    Private {
        kind: PrivateKind,
    },
    Public {
        lane: Lane,
        kind: PublicKind,
    },
    Command {
        transport: Transport,
        action: Command,
    },
}
impl Action {
    pub fn build(self) -> super::Action {
        use super::{Action as A, CommandAction as C};
        match self {
            Self::GuardianConnect => A::GuardianConnect,
            Self::Read { kind } => A::Read(match kind {
                ReadKind::Connect => super::ReadAction::Connect,
                ReadKind::TradePage => super::ReadAction::TradePage,
                ReadKind::AccountSnapshot => super::ReadAction::AccountSnapshot,
                ReadKind::OpenOrders => super::ReadAction::OpenOrders,
                ReadKind::ConditionalOrders => super::ReadAction::ConditionalOrders,
                ReadKind::OrderQuery => super::ReadAction::OrderQuery,
                ReadKind::Income => super::ReadAction::Income,
            }),
            Self::Time { kind } => match kind {
                TimeKind::Connect => A::TimeConnect,
                TimeKind::Query => A::TimeQuery,
            },
            Self::Private { kind } => match kind {
                PrivateKind::StreamConnect => A::PrivateStreamConnect,
                PrivateKind::ManagementConnect => A::PrivateManagementConnect,
                PrivateKind::Upgrade => A::PrivateUpgrade,
                PrivateKind::Pong => A::PrivatePong,
                PrivateKind::Close => A::PrivateClose,
                PrivateKind::SpotLogon => A::SpotLogon,
                PrivateKind::SpotSubscribe => A::SpotSubscribe,
                PrivateKind::SpotSubscribeSignature => A::SpotSubscribeSignature,
                PrivateKind::SpotStatus => A::SpotStatus,
                PrivateKind::SpotSubscriptions => A::SpotSubscriptions,
                PrivateKind::SpotUnsubscribe => A::SpotUnsubscribe,
                PrivateKind::SpotLogout => A::SpotLogout,
                PrivateKind::UsdmCreate => A::UsdmCreate,
                PrivateKind::UsdmKeepalive => A::UsdmKeepalive,
                PrivateKind::UsdmClose => A::UsdmClose,
            },
            Self::Public { lane, kind } => A::Public {
                lane: match lane {
                    Lane::Stream => tickwire_binance_public::Lane::Stream,
                    Lane::Snapshot => tickwire_binance_public::Lane::Snapshot,
                },
                kind: match kind {
                    PublicKind::Connect => tickwire_binance_public::ChargeKind::Connect,
                    PublicKind::Upgrade => tickwire_binance_public::ChargeKind::Upgrade,
                    PublicKind::Subscribe => tickwire_binance_public::ChargeKind::Subscribe,
                    PublicKind::Pong => tickwire_binance_public::ChargeKind::Pong,
                    PublicKind::Close => tickwire_binance_public::ChargeKind::Close,
                    PublicKind::Snapshot => tickwire_binance_public::ChargeKind::Snapshot,
                },
            },
            Self::Command { transport, action } => A::Command {
                transport: match transport {
                    Transport::Rest => super::Transport::Rest,
                    Transport::WebSocket => super::Transport::WebSocket,
                },
                action: match action {
                    Command::New => C::New,
                    Command::Cancel => C::Cancel,
                    Command::CancelReplace => C::CancelReplace,
                    Command::Amend => C::Amend,
                    Command::Modify => C::Modify,
                    Command::Query => C::Query,
                    Command::CancelList => C::CancelList,
                    Command::Algo => C::Algo,
                    Command::CancelAlgo => C::CancelAlgo,
                    Command::PeggedNew => C::PeggedNew,
                    Command::AdvancedCancelReplace => C::AdvancedCancelReplace,
                    Command::Sor => C::Sor,
                    Command::SpotList {
                        kind_of_list,
                        legs,
                        advanced,
                    } => C::SpotList {
                        kind: match kind_of_list {
                            ListKind::Oco => tickwire_venue::ListKind::Oco,
                            ListKind::Oto => tickwire_venue::ListKind::Oto,
                            ListKind::Otoco => tickwire_venue::ListKind::Otoco,
                        },
                        legs,
                        advanced,
                    },
                },
            },
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub source: Source,
    pub action: Action,
    pub class: Class,
    pub api_weight: u32,
    pub websocket_messages: u32,
    pub costs: Vec<Cost>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub supervisor_generation: u64,
    pub consumer_instance: [u8; 16],
    pub consumer_generation: u64,
    pub consumer_lease_until_ns: u64,
    pub producer_id: [u8; 16],
    pub policy_generation: u64,
    pub policy_starts_ns: u64,
    pub policy_ends_ns: u64,
    pub windows: Vec<Window>,
    pub grants: Vec<Grant>,
    pub rules: Vec<Rule>,
}
pub struct Built<S: q::QuotaStore> {
    pub coordinator: q::Coordinator<S>,
    pub endpoint: Endpoint,
}
impl Config {
    pub fn resolve_costs(&self, items: &[Cost]) -> Result<Vec<q::Cost>> {
        if items.is_empty() || items.len() > q::MAX_COSTS {
            return Err(Error::Invalid);
        }
        let mut costs = items
            .iter()
            .map(|item| {
                let window = self
                    .windows
                    .iter()
                    .find(|w| w.name == item.window)
                    .ok_or(Error::Policy)?;
                Ok(q::Cost {
                    window: window.key(),
                    units: item.units,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        costs.sort_by_key(|c| c.window);
        if costs.iter().any(|c| c.units == 0)
            || costs.windows(2).any(|w| w[0].window == w[1].window)
        {
            return Err(Error::Invalid);
        }
        Ok(costs)
    }
    pub fn policy(&self) -> Result<super::Policy> {
        Ok(super::Policy {
            generation: self.policy_generation,
            starts_ns: self.policy_starts_ns,
            ends_ns: self.policy_ends_ns,
            rules: self
                .rules
                .iter()
                .map(|r| {
                    Ok(super::Rule {
                        source: r.source.build(),
                        action: r.action.build(),
                        class: r.class.build(),
                        api_weight: r.api_weight,
                        websocket_messages: r.websocket_messages,
                        costs: self.resolve_costs(&r.costs)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
    pub fn build<S: q::QuotaStore>(&self, store: S, now: q::MonoTime) -> Result<Built<S>> {
        if self.windows.is_empty()
            || self.windows.len() > q::MAX_WINDOWS
            || self.grants.len() > q::MAX_GRANTS
            || self.rules.is_empty()
            || self.rules.len() > super::MAX_RULES
        {
            return Err(Error::Capacity);
        }
        let mut names = BTreeMap::new();
        for window in &self.windows {
            if window.name.is_empty()
                || window.name.len() > 64
                || !window.name.is_ascii()
                || names.insert(window.name.as_str(), window.key()).is_some()
            {
                return Err(Error::Invalid);
            }
        }
        let resolve = |items: &[Cost]| -> Result<Vec<q::Cost>> {
            if items.is_empty() || items.len() > q::MAX_COSTS {
                return Err(Error::Invalid);
            }
            let mut costs = items
                .iter()
                .map(|cost| {
                    Ok(q::Cost {
                        window: *names.get(cost.window.as_str()).ok_or(Error::Policy)?,
                        units: cost.units,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            costs.sort_by_key(|cost| cost.window);
            if costs.iter().any(|c| c.units == 0)
                || costs.windows(2).any(|w| w[0].window == w[1].window)
            {
                return Err(Error::Invalid);
            }
            Ok(costs)
        };
        let rules = self
            .rules
            .iter()
            .map(|rule| {
                Ok(super::Rule {
                    source: rule.source.build(),
                    action: rule.action.build(),
                    class: rule.class.build(),
                    api_weight: rule.api_weight,
                    websocket_messages: rule.websocket_messages,
                    costs: resolve(&rule.costs)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let grants = self
            .grants
            .iter()
            .map(|grant| Ok((grant, resolve(&grant.costs)?)))
            .collect::<Result<Vec<_>>>()?;
        let mut coordinator = q::Coordinator::open(store, self.supervisor_generation, now)?;
        // Sort chronology, allowing config authors to group windows by a descriptive name.
        let mut windows = self.windows.iter().collect::<Vec<_>>();
        windows.sort_by_key(|w| w.key());
        for w in windows {
            coordinator.add_window(
                q::WindowConfig {
                    key: w.key(),
                    revision: w.revision,
                    starts_ns: w.starts_ns,
                    ends_ns: w.ends_ns,
                    limit: w.limit,
                    recovery_reserve: w.recovery_reserve,
                },
                now,
            )?;
        }
        let mut local = coordinator.register_consumer(
            q::ConsumerId {
                instance: self.consumer_instance,
                generation: self.consumer_generation,
            },
            self.consumer_lease_until_ns,
            now,
        )?;
        for (g, costs) in grants {
            let grant = coordinator.issue(
                g.request_id,
                &local,
                g.class.build(),
                &costs,
                g.expires_ns,
                now,
            )?;
            local.install(grant, now)?;
        }
        let endpoint = Endpoint::new(
            local,
            self.producer_id,
            now.domain(),
            super::Policy {
                generation: self.policy_generation,
                starts_ns: self.policy_starts_ns,
                ends_ns: self.policy_ends_ns,
                rules,
            },
        )?;
        Ok(Built {
            coordinator,
            endpoint,
        })
    }
}
