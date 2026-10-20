//! Bounded private JSON messages and explicit local order identity binding.
use crate::{Error, Result, decimal::Decimal, feed::TimeUnit, request::Market};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
use tickwire_venue as v;

mod binding;
mod decode;
pub use binding::bind_order;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionIdentity {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: v::Environment,
    pub account: u64,
    pub connection_epoch: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateStream {
    SpotSubscription { subscription_id: u64 },
    UsdmListenKey,
}
#[derive(Clone, Debug)]
pub struct SessionContext {
    identity: SessionIdentity,
    market: Market,
    stream: PrivateStream,
    unit: TimeUnit,
}
impl SessionContext {
    pub fn new(
        identity: SessionIdentity,
        market: Market,
        stream: PrivateStream,
        unit: TimeUnit,
    ) -> Result<Self> {
        if identity.risk_domain_id == [0; 16]
            || identity.venue == 0
            || identity.account == 0
            || identity.connection_epoch == 0
            || !matches!(
                (market, stream),
                (Market::Spot, PrivateStream::SpotSubscription { .. })
                    | (Market::Usdm, PrivateStream::UsdmListenKey)
            )
            || (market == Market::Usdm && unit != TimeUnit::Milliseconds)
        {
            return Err(Error::IdentityMismatch);
        }
        Ok(Self {
            identity,
            market,
            stream,
            unit,
        })
    }
    pub fn identity(&self) -> &SessionIdentity {
        &self.identity
    }
    pub fn market(&self) -> Market {
        self.market
    }
}

#[derive(Debug)]
pub struct PrivateEvent<'a> {
    session: SessionContext,
    received_mono_ns: u64,
    event_time_ns: u64,
    transaction_time_ns: Option<u64>,
    body: PrivateBody<'a>,
    raw: &'a RawValue,
}
impl<'a> PrivateEvent<'a> {
    pub fn session(&self) -> &SessionContext {
        &self.session
    }
    pub fn body(&self) -> &PrivateBody<'a> {
        &self.body
    }
    pub fn raw(&self) -> &'a RawValue {
        self.raw
    }
    pub fn event_time_ns(&self) -> u64 {
        self.event_time_ns
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.transaction_time_ns
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.received_mono_ns
    }
}

#[derive(Debug)]
pub enum PrivateBody<'a> {
    Order(Box<OrderUpdate<'a>>),
    SpotBalances {
        last_update_ns: u64,
        balances: Vec<SpotBalance<'a>>,
    },
    BalanceDelta {
        asset: Cow<'a, str>,
        delta: Decimal,
        clear_time_ns: u64,
        external_lock: bool,
    },
    UsdmAccount {
        reason: Cow<'a, str>,
        funding_symbol: Option<Cow<'a, str>>,
        balances: Vec<FuturesBalance<'a>>,
        positions: Vec<FuturesPosition<'a>>,
    },
    AccountConfig {
        leverage: Option<Leverage<'a>>,
        multi_assets: Option<bool>,
    },
    List(ListStatus<'a>),
    Algo(Box<AlgoUpdate<'a>>),
    StreamTerminated,
    ListenKeyExpired,
    /// No default order status or account completeness is inferred.
    Other {
        event_type: Cow<'a, str>,
    },
}

#[derive(Debug)]
pub struct OrderUpdate<'a> {
    pub symbol: Cow<'a, str>,
    pub client_id: Cow<'a, str>,
    pub original_client_id: Option<Cow<'a, str>>,
    pub order_id: u64,
    pub list_id: Option<u64>,
    pub execution_id: Option<u64>,
    pub execution_type: Cow<'a, str>,
    pub status: Cow<'a, str>,
    pub side: Cow<'a, str>,
    pub position_side: Option<Cow<'a, str>>,
    pub order_type: Cow<'a, str>,
    pub time_in_force: Cow<'a, str>,
    pub original_quantity: Decimal,
    pub price: Decimal,
    pub cumulative_quantity: Decimal,
    pub cumulative_quote: Option<Decimal>,
    pub last_quantity: Decimal,
    pub last_price: Decimal,
    pub last_quote: Option<Decimal>,
    pub commission: Option<Decimal>,
    pub commission_asset: Option<Cow<'a, str>>,
    pub realized_pnl: Option<Decimal>,
    pub trade_id: Option<u64>,
    pub maker: bool,
    pub order_time_ns: u64,
    pub reject_reason: Option<Cow<'a, str>>,
    pub raw: &'a RawValue,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalance<'a> {
    #[serde(rename = "a", borrow)]
    pub asset: Cow<'a, str>,
    #[serde(rename = "f")]
    pub free: Decimal,
    #[serde(rename = "l")]
    pub locked: Decimal,
}
#[derive(Debug, Deserialize)]
pub struct FuturesBalance<'a> {
    #[serde(rename = "a", borrow)]
    pub asset: Cow<'a, str>,
    #[serde(rename = "wb")]
    pub wallet_balance: Decimal,
    #[serde(rename = "cw")]
    pub cross_wallet_balance: Decimal,
    #[serde(rename = "bc")]
    pub balance_change_excluding_pnl_and_commission: Decimal,
}
#[derive(Debug, Deserialize)]
pub struct FuturesPosition<'a> {
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "pa")]
    pub position_amount: Decimal,
    #[serde(rename = "ep")]
    pub entry_price: Decimal,
    #[serde(rename = "bep")]
    pub break_even_price: Option<Decimal>,
    #[serde(rename = "cr")]
    pub accumulated_realized: Decimal,
    #[serde(rename = "up")]
    pub unrealized_pnl: Decimal,
    #[serde(rename = "mt", borrow)]
    pub margin_type: Cow<'a, str>,
    #[serde(rename = "iw")]
    pub isolated_wallet: Decimal,
    #[serde(rename = "ps", borrow)]
    pub position_side: Cow<'a, str>,
}
#[derive(Debug, Deserialize)]
pub struct Leverage<'a> {
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "l")]
    pub leverage: u32,
}
#[derive(Debug)]
pub struct ListStatus<'a> {
    pub symbol: Cow<'a, str>,
    pub list_id: u64,
    pub list_client_id: Cow<'a, str>,
    pub contingency_type: Cow<'a, str>,
    pub status_type: Cow<'a, str>,
    pub order_status: Cow<'a, str>,
    pub reject_reason: Cow<'a, str>,
    pub transaction_time_ns: u64,
    pub orders: Vec<ListMember<'a>>,
}
#[derive(Debug, Deserialize)]
pub struct ListMember<'a> {
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "c", borrow)]
    pub client_id: Cow<'a, str>,
}
#[derive(Debug, Deserialize)]
pub struct AlgoUpdate<'a> {
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "caid", borrow)]
    pub client_algo_id: Cow<'a, str>,
    #[serde(rename = "aid")]
    pub algo_id: u64,
    #[serde(rename = "at", borrow)]
    pub algo_type: Cow<'a, str>,
    #[serde(rename = "o", borrow)]
    pub order_type: Cow<'a, str>,
    #[serde(rename = "S", borrow)]
    pub side: Cow<'a, str>,
    #[serde(rename = "ps", borrow)]
    pub position_side: Cow<'a, str>,
    #[serde(rename = "f", borrow)]
    pub time_in_force: Cow<'a, str>,
    #[serde(rename = "q")]
    pub quantity: Decimal,
    #[serde(rename = "X", borrow)]
    pub status: Cow<'a, str>,
    #[serde(rename = "ai", borrow)]
    pub actual_order_id: Option<Cow<'a, str>>,
    #[serde(rename = "ap")]
    pub average_fill_price: Option<Decimal>,
    #[serde(rename = "aq")]
    pub executed_quantity: Option<Decimal>,
    #[serde(rename = "act", borrow)]
    pub actual_order_type: Option<Cow<'a, str>>,
    #[serde(rename = "tp")]
    pub trigger_price: Option<Decimal>,
    #[serde(rename = "p")]
    pub price: Option<Decimal>,
    #[serde(rename = "tt")]
    pub trigger_time: Option<u64>,
    #[serde(rename = "gtd")]
    pub good_till_date: Option<u64>,
    #[serde(rename = "rm", borrow)]
    pub reject_message: Option<Cow<'a, str>>,
    #[serde(rename = "ia")]
    pub activated: Option<bool>,
}

/// Each variable-length array is checked before materializing its next entry.
/// Account messages are sparse changes; they never establish a complete baseline.
pub fn decode_private<'a, const N: usize>(
    session: &SessionContext,
    bytes: &'a [u8],
    max_bytes: usize,
    received_mono_ns: u64,
) -> Result<PrivateEvent<'a>> {
    decode::decode::<N>(session, bytes, max_bytes, received_mono_ns)
}

impl SessionContext {
    pub fn stream(&self) -> PrivateStream {
        self.stream
    }
    pub fn time_unit(&self) -> TimeUnit {
        self.unit
    }
}
