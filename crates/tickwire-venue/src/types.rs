use crate::{Error, Hash, TerminalReason};

/// Printable ASCII only; no truncation or case folding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundedText<const N: usize>(Box<str>);
impl<const N: usize> BoundedText<N> {
    pub fn new(value: impl AsRef<str>) -> Result<Self, Error> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > N {
            return Err(Error::Bounds);
        }
        if !value.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(Error::Text);
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
/// Catalog symbols preserve UTF-8 spelling without Unicode normalization.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundedSymbol<const N: usize>(Box<str>);
impl<const N: usize> BoundedSymbol<N> {
    pub fn new(value: impl AsRef<str>) -> Result<Self, Error> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > N {
            return Err(Error::Bounds);
        }
        if value.chars().any(|c| c.is_control() || c.is_whitespace()) {
            return Err(Error::Text);
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
/// Bounded source text; unlike identifiers, preserves spaces and UTF-8 error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedMessage<const N: usize>(Box<str>);
impl<const N: usize> BoundedMessage<N> {
    pub fn new(value: impl AsRef<str>) -> Result<Self, Error> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > N {
            return Err(Error::Bounds);
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedList<T, const N: usize>(Vec<T>);
impl<T, const N: usize> BoundedList<T, N> {
    pub fn new(value: Vec<T>) -> Result<Self, Error> {
        if value.len() > N {
            return Err(Error::Bounds);
        }
        Ok(Self(value))
    }
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

/// coefficient * 10^exponent; normalized, with exponent in -38..=38.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    coefficient: i128,
    exponent: i8,
}
impl Decimal {
    pub fn new(mut coefficient: i128, mut exponent: i8) -> Result<Self, Error> {
        if !(-38..=38).contains(&exponent) {
            return Err(Error::Decimal);
        }
        if coefficient == 0 {
            exponent = 0;
        }
        while coefficient != 0 && coefficient % 10 == 0 && exponent < 38 {
            coefficient /= 10;
            exponent += 1;
        }
        Ok(Self {
            coefficient,
            exponent,
        })
    }
    pub fn coefficient(self) -> i128 {
        self.coefficient
    }
    pub fn exponent(self) -> i8 {
        self.exponent
    }
    pub fn is_positive(self) -> bool {
        self.coefficient > 0
    }
    pub fn as_plain_string(self) -> String {
        let negative = self.coefficient < 0;
        let digits = self.coefficient.unsigned_abs().to_string();
        let mut out = if negative {
            String::from("-")
        } else {
            String::new()
        };
        if self.exponent >= 0 {
            out.push_str(&digits);
            out.extend(std::iter::repeat_n('0', self.exponent as usize));
        } else {
            let scale = (-self.exponent) as usize;
            if scale >= digits.len() {
                out.push_str("0.");
                out.extend(std::iter::repeat_n('0', scale - digits.len()));
                out.push_str(&digits);
            } else {
                let split = digits.len() - scale;
                out.push_str(&digits[..split]);
                out.push('.');
                out.push_str(&digits[split..]);
            }
        }
        out
    }
}

pub type ClientId = BoundedText<64>;
pub type ClientOrderId = ClientId;
pub type VenueOrderId = BoundedText<64>;
pub type VenueToken = BoundedText<64>;
pub type VenueTradeId = BoundedText<32>;
pub type Symbol = BoundedSymbol<64>;
pub type AssetSymbol = BoundedSymbol<32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntentKey {
    pub epoch: u64,
    pub intent_id: u64,
}

/// Stable numeric identities used by catalog, market data, commands, and stateless ORS gateways.
pub mod venue_id {
    pub const BINANCE: u32 = 1;
    pub const BYBIT: u32 = 2;
    pub const OKX: u32 = 3;
}

/// Venues using the snapshot-only spot feed and signed spot REST adapter.
/// Binance has its own spot and futures session owners.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpotVenue {
    Bybit,
    Okx,
}

impl SpotVenue {
    pub const fn id(self) -> u32 {
        match self {
            Self::Bybit => venue_id::BYBIT,
            Self::Okx => venue_id::OKX,
        }
    }

    pub const fn from_id(id: u32) -> Option<Self> {
        match id {
            venue_id::BYBIT => Some(Self::Bybit),
            venue_id::OKX => Some(Self::Okx),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Product {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    Option,
    LinearFuture,
    InverseFuture,
    UsdM,
    CoinM,
}

impl Product {
    #[allow(non_upper_case_globals)]
    pub const Options: Self = Self::Option;
    #[allow(non_upper_case_globals)]
    pub const LinearPerp: Self = Self::LinearPerpetual;
    #[allow(non_upper_case_globals)]
    pub const InversePerp: Self = Self::InversePerpetual;
    #[allow(non_upper_case_globals)]
    pub const LinearFutures: Self = Self::LinearFuture;
    #[allow(non_upper_case_globals)]
    pub const InverseFutures: Self = Self::InverseFuture;

    pub const ALL: [Self; 8] = [
        Self::Spot,
        Self::LinearPerpetual,
        Self::InversePerpetual,
        Self::Option,
        Self::LinearFuture,
        Self::InverseFuture,
        Self::UsdM,
        Self::CoinM,
    ];

    #[inline]
    pub const fn code(self) -> u8 {
        match self {
            Self::Spot => 1,
            Self::LinearPerpetual => 2,
            Self::InversePerpetual => 3,
            Self::Option => 4,
            Self::LinearFuture => 5,
            Self::InverseFuture => 6,
            Self::UsdM => 7,
            Self::CoinM => 8,
        }
    }

    #[inline]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Spot),
            2 => Some(Self::LinearPerpetual),
            3 => Some(Self::InversePerpetual),
            4 => Some(Self::Option),
            5 => Some(Self::LinearFuture),
            6 => Some(Self::InverseFuture),
            7 => Some(Self::UsdM),
            8 => Some(Self::CoinM),
            _ => None,
        }
    }

    #[inline]
    pub const fn mask_bit(self) -> u16 {
        1u16 << (self.code() - 1)
    }

    #[inline]
    pub const fn is_derivative(self) -> bool {
        matches!(
            self,
            Self::LinearPerpetual
                | Self::InversePerpetual
                | Self::LinearFuture
                | Self::InverseFuture
                | Self::UsdM
                | Self::CoinM
        )
    }
}

/// Wallet-partitioned asset identifier (`(wallet_id, asset_id)`) separating Spot, USD-M,
/// COIN-M, and Options collateral pools inside a unified master account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AssetKey {
    pub wallet_id: u16,
    pub asset_id: u64,
}

impl AssetKey {
    #[inline]
    pub const fn new(wallet_id: u16, asset_id: u64) -> Self {
        Self {
            wallet_id,
            asset_id,
        }
    }
}

/// Candidate venue order-entry session descriptor used by stateless ORS multi-product routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderRoute {
    pub connection_id: u16,
    pub account_ref: u8,
    pub ready: bool,
    pub colo: bool,
    pub product_mask: u16,
    pub local_ip: u32,
    pub ema_score_us: u32,
}

impl Default for OrderRoute {
    fn default() -> Self {
        Self {
            connection_id: Self::NO_CONNECTION,
            account_ref: 0,
            ready: false,
            colo: false,
            product_mask: 0,
            local_ip: 0,
            ema_score_us: Self::UNMEASURED_SCORE,
        }
    }
}

impl OrderRoute {
    pub const NO_CONNECTION: u16 = u16::MAX;
    pub const UNMEASURED_SCORE: u32 = u32::MAX;

    #[inline]
    pub const fn new(product_mask: u16) -> Self {
        Self {
            connection_id: 0,
            account_ref: 0,
            ready: true,
            colo: false,
            product_mask,
            local_ip: 0,
            ema_score_us: Self::UNMEASURED_SCORE,
        }
    }

    /// Returns `true` when `product_mask == 0` (universal session) or contains `product`.
    #[inline]
    pub const fn matches_product(&self, product: Option<Product>) -> bool {
        match product {
            None => true,
            Some(_) if self.product_mask == 0 => true,
            Some(p) => (self.product_mask & p.mask_bit()) != 0,
        }
    }
}

/// Selects a ready `OrderRoute` matching `product` using integer `9/8` EWMA hysteresis
/// (`candidate.ema_score_us * 9 < current.ema_score_us * 8`) to prevent route oscillation.
pub fn pick_order_route(
    routes: &[OrderRoute],
    product: Option<Product>,
    last_connection_id: u16,
    force_last: bool,
) -> Option<u16> {
    let last_route = if last_connection_id != OrderRoute::NO_CONNECTION {
        routes.iter().find(|r| {
            r.connection_id == last_connection_id && r.ready && r.matches_product(product)
        })
    } else {
        None
    };
    if force_last && let Some(r) = last_route {
        return Some(r.connection_id);
    }

    let mut best: Option<&OrderRoute> = last_route;
    for candidate in routes {
        if !candidate.ready
            || candidate.connection_id == OrderRoute::NO_CONNECTION
            || !candidate.matches_product(product)
        {
            continue;
        }
        let Some(curr) = best else {
            best = Some(candidate);
            continue;
        };
        if candidate.connection_id == curr.connection_id {
            continue;
        }
        let cand_measured = candidate.ema_score_us != OrderRoute::UNMEASURED_SCORE;
        let curr_measured = curr.ema_score_us != OrderRoute::UNMEASURED_SCORE;
        let better = match (cand_measured, curr_measured) {
            (true, false) => true,
            (false, true) => false,
            (false, false) => candidate.colo && !curr.colo,
            (true, true) => {
                (u64::from(candidate.ema_score_us) * 9) < (u64::from(curr.ema_score_us) * 8)
            }
        };
        if better {
            best = Some(candidate);
        }
    }
    best.map(|r| r.connection_id)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionRight {
    Call,
    Put,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractKind {
    Cash,
    Perpetual,
    Delivery {
        expiry_utc_ns: u64,
    },
    Option {
        expiry_utc_ns: u64,
        strike: Decimal,
        right: OptionRight,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Production,
    Test,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionMode {
    OneWay,
    Hedge,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarginMode {
    Cross,
    Isolated,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfTradePrevention {
    Default,
    None,
    ExpireTaker,
    ExpireMaker,
    ExpireBoth,
    Decrement,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkingPrice {
    Contract,
    Mark,
    Index,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    StopLimit,
    StopMarket,
    TakeProfitLimit,
    TakeProfitMarket,
    TrailingStop,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
    PostOnly,
    Gtd { expire_utc_ns: u64 },
    Rpi,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantity {
    Base(Decimal),
    Quote(Decimal),
    ClosePosition,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRef {
    pub id: u64,
    pub version: u64,
    pub symbol: AssetSymbol,
    pub exponent: i8,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentBinding {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub catalog_version: u64,
    pub catalog_hash: Hash,
    pub symbol: Symbol,
    pub base: AssetRef,
    pub quote: AssetRef,
    pub settlement: AssetRef,
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
    pub contract: ContractKind,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIdentity {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub product: Product,
    pub intent_id: u64,
    pub intent_exec_epoch: u64,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub deadline: u64,
    pub recovery_object: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyContext {
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    /// Strategy IPC sequence; not an executor-assigned reservation sequence.
    pub strategy_intent_seq: u64,
    pub authorization_epoch: u64,
    pub risk_policy_version: u64,
    pub order_config_version: u64,
    pub created_mono_ns: u64,
    pub economic_intent_id: [u8; 16],
    pub hedge_group_id: [u8; 16],
    pub leg_id: u32,
    /// An already-existing strategy dependency, not the closure root containing this command.
    pub recovery_manifest_hash: Hash,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerRef {
    pub topic_id: u64,
    pub feed_generation: u64,
    pub ring_epoch: u64,
    pub sequence: u64,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub observed_mono_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandContext {
    pub request: RequestIdentity,
    pub strategy: StrategyContext,
    pub instrument: InstrumentBinding,
    pub triggers: BoundedList<TriggerRef, 8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trigger {
    pub price: Decimal,
    pub working_price: WorkingPrice,
    pub price_protect: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrailingDistance {
    Percent(Decimal),
    BasisPoints(u32),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailing {
    pub activation_price: Option<Decimal>,
    pub working_price: WorkingPrice,
    pub distance: TrailingDistance,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductMode {
    Spot,
    Margin {
        margin_mode: MarginMode,
        auto_borrow: bool,
        auto_repay: bool,
    },
    Derivative {
        position_mode: PositionMode,
        margin_mode: MarginMode,
    },
    Option,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSpec {
    pub client_id: ClientId,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Quantity,
    pub price: Option<Decimal>,
    pub time_in_force: Option<TimeInForce>,
    pub position_side: PositionSide,
    pub mode: ProductMode,
    pub reduce_only: bool,
    pub self_trade_prevention: SelfTradePrevention,
    pub trigger: Option<Trigger>,
    pub trailing: Option<Trailing>,
    pub iceberg_quantity: Option<Decimal>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTarget {
    pub order_id: [u8; 16],
    pub expected_owner_revision: u64,
    pub original_intent_exec_epoch: u64,
    pub client_id: Option<ClientId>,
    pub venue_order_id: Option<VenueOrderId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReplaceMode {
    StopOnFailure,
    AllowFailure,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListKind {
    Oco,
    Oto,
    Otoco,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOrder {
    pub list_id: [u8; 16],
    pub client_id: ClientId,
    pub kind: ListKind,
    pub legs: BoundedList<OrderSpec, 3>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PegPriceType {
    Primary,
    Market,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peg {
    pub price_type: PegPriceType,
    /// Existing book price levels, 0..=100; None omits both offset fields.
    pub offset_levels: Option<u16>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotOrder {
    pub order: OrderSpec,
    pub peg: Option<Peg>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotAdvancedList {
    pub list_id: [u8; 16],
    pub client_id: ClientId,
    pub kind: ListKind,
    pub legs: BoundedList<SpotOrder, 3>,
}
/// Schema 1 command tag 11; old OrderSpec and command encodings remain unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpotAdvancedCommand {
    New(SpotOrder),
    CancelReplace {
        target: OrderTarget,
        replacement: SpotOrder,
        mode: CancelReplaceMode,
    },
    List(SpotAdvancedList),
    SorNew(OrderSpec),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTarget {
    pub list_id: [u8; 16],
    pub expected_owner_revision: u64,
    pub original_intent_exec_epoch: u64,
    pub client_id: Option<ClientId>,
    pub venue_list_id: Option<VenueOrderId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlgoOrder {
    pub parent_order_id: [u8; 16],
    pub order: OrderSpec,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlgoTarget {
    pub parent_order_id: [u8; 16],
    pub expected_owner_revision: u64,
    pub original_intent_exec_epoch: u64,
    pub client_id: Option<ClientId>,
    pub venue_algo_id: Option<VenueOrderId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrder {
    pub target: OrderTarget,
    pub new_client_id: Option<ClientId>,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub position_side: PositionSide,
    pub reduce_only: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandBody {
    New(OrderSpec),
    Cancel(OrderTarget),
    CancelReplace {
        target: OrderTarget,
        replacement: OrderSpec,
        mode: CancelReplaceMode,
    },
    AmendKeepPriority {
        target: OrderTarget,
        new_quantity: Decimal,
        new_client_id: Option<ClientId>,
    },
    Modify(ModifyOrder),
    Query(OrderTarget),
    SpotList(ListOrder),
    CancelList(ListTarget),
    Algo(AlgoOrder),
    CancelAlgo(AlgoTarget),
    SpotAdvanced(SpotAdvancedCommand),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub context: CommandContext,
    pub body: CommandBody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
    Rejected,
    Unknown,
    /// This trade row contains no order lifecycle report.
    Unreported,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liquidity {
    Unknown,
    Maker,
    Taker,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdentity {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub product: Product,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub source_connection_epoch: u64,
    pub source_sequence: Option<u64>,
    pub event_id: BoundedText<64>,
    pub venue_time_ns: Option<u64>,
    pub received_mono_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIdentity {
    pub order_id: [u8; 16],
    pub original_intent_id: u64,
    pub original_intent_exec_epoch: u64,
    pub client_id: Option<ClientId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub parent_order_id: Option<[u8; 16]>,
    pub list_id: Option<[u8; 16]>,
    pub venue_algo_id: Option<VenueOrderId>,
    pub venue_list_id: Option<VenueOrderId>,
    pub list_client_id: Option<ClientId>,
    pub algo_client_id: Option<ClientId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetDelta {
    pub asset: AssetRef,
    pub amount: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderObservation {
    pub order: OrderIdentity,
    pub status: OrderStatus,
    pub side: Side,
    pub position_side: PositionSide,
    pub original_quantity: Option<Decimal>,
    pub cumulative_quantity: Option<Decimal>,
    pub cumulative_quote: Option<Decimal>,
    pub leaves_quantity: Option<Decimal>,
    pub order_price: Option<Decimal>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillEvent {
    pub observation: OrderObservation,
    pub trade_id: VenueTradeId,
    pub last_quantity: Decimal,
    pub last_price: Decimal,
    pub last_quote: Option<Decimal>,
    /// Reported realized PnL, separate from commission; None means absent.
    pub realized_pnl: Option<AssetDelta>,
    pub liquidity: Liquidity,
    /// Each amount is signed asset cash flow: debit negative, rebate positive.
    pub fees: BoundedList<AssetDelta, 8>,
}

/// Stateless ORS exchange gateway front-acknowledgement (`FrontAck`), distinct from book acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontAck {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub order_seqnum: u64,
    pub orig_seqnum: u64,
    pub client_id: Option<ClientId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub received_mono_ns: u64,
}

impl FrontAck {
    pub fn validate(&self) -> Result<(), Error> {
        if self.ors_order_id == 0 || self.order_seqnum == 0 || self.strat_id == u8::MAX {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

/// Stateless ORS execution fill report (`Fill`), supporting both real/projected executions and
/// post-trade fee/rebate updates (`amendment == true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub order_seqnum: u64,
    pub exec_ref: u64,
    pub trade_id: Option<VenueTradeId>,
    pub side: Side,
    pub liquidity: Liquidity,
    pub last_quantity: Decimal,
    pub last_price: Decimal,
    pub cumulative_filled: Decimal,
    pub cumulative_projected: Decimal,
    pub fee: Option<AssetDelta>,
    pub amendment: bool,
    pub projected: bool,
    pub received_mono_ns: u64,
}

impl Fill {
    pub fn validate(&self) -> Result<(), Error> {
        if self.ors_order_id == 0 || self.order_seqnum == 0 || self.strat_id == u8::MAX {
            return Err(Error::Identity);
        }
        if self.amendment && self.projected {
            return Err(Error::OrderCombination);
        }
        if self.amendment {
            if self.fee.is_none() {
                return Err(Error::Field);
            }
            if self.last_quantity.coefficient() < 0 {
                return Err(Error::Quantity);
            }
            if self.last_price.coefficient() < 0 {
                return Err(Error::Price);
            }
        } else {
            if !self.last_quantity.is_positive() {
                return Err(Error::Quantity);
            }
            if !self.last_price.is_positive() {
                return Err(Error::Price);
            }
        }
        if self.cumulative_filled.coefficient() < 0 || self.cumulative_projected.coefficient() < 0 {
            return Err(Error::Quantity);
        }
        if let Some(fee) = &self.fee {
            crate::validate::cashflow(fee)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructureKind {
    Oco,
    Oto,
    Otoco,
    Algo,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructureStatus {
    Pending,
    Live,
    Triggered,
    Terminal,
    Rejected,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureIdentity {
    pub structure_id: [u8; 16],
    pub original_intent_id: u64,
    pub original_intent_exec_epoch: u64,
    pub kind: StructureKind,
    pub client_id: ClientId,
    pub venue_parent_id: Option<VenueOrderId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureChild {
    pub order: OrderIdentity,
    /// Actual exchange client ID when it differs from the original reserved leg.
    pub reported_client_id: Option<ClientId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureObservation {
    pub parent: StructureIdentity,
    pub status: StructureStatus,
    pub venue_status: BoundedText<32>,
    pub children: BoundedList<StructureChild, 3>,
}
/// Exchange execution IDs are not assumed equal across protocol sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AmendmentSource {
    RestResponse,
    RestHistory,
    PrivateStream,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmendmentObservation {
    pub order: OrderIdentity,
    pub request_intent_id: u64,
    pub request_intent_exec_epoch: u64,
    pub request_digest: Hash,
    pub execution_source: AmendmentSource,
    pub execution_id: u64,
    pub previous_quantity: Decimal,
    pub new_quantity: Decimal,
    /// History rows have no order status/cumulative quantity; absence stays absent.
    pub observation: Option<OrderObservation>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmendmentRejection {
    pub order: OrderIdentity,
    pub request_intent_id: u64,
    pub request_intent_exec_epoch: u64,
    pub request_digest: Hash,
    pub venue_code: BoundedText<32>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyObservation {
    pub target: OrderTarget,
    pub amendment_id: Option<VenueToken>,
    pub intent: Option<IntentKey>,
    pub prior_client_id: Option<ClientOrderId>,
    pub new_client_id: Option<ClientOrderId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub new_quantity: Decimal,
    pub new_price: Decimal,
    pub cumulative_filled: Option<Decimal>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyRejection {
    pub target: OrderTarget,
    pub amendment_id: Option<VenueToken>,
    pub intent: Option<IntentKey>,
    pub new_client_id: Option<ClientOrderId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub code: Option<i64>,
    pub reason: TerminalReason,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueEventBody {
    /// Schema 5/tag 8. Native or venue-initiated order price/quantity modification.
    Modified(Box<ModifyObservation>),
    /// Schema 5/tag 9. Rejected in-place order modification; resting order remains open unless closed separately.
    ModifyRejected(Box<ModifyRejection>),
    /// Schema 3/tag 6. Only a matched amendment result changes effective quantity.
    Amendment(Box<AmendmentObservation>),
    /// Schema 3/tag 7. A rejected management request, not a rejected order.
    AmendmentRejected(Box<AmendmentRejection>),
    /// Schema 2/tag 5. Parent state never substitutes for individual leg results.
    Structure(Box<StructureObservation>),
    Order(Box<OrderObservation>),
    Fill(Box<FillEvent>),
    Fee {
        order: OrderIdentity,
        fee_id: BoundedText<64>,
        deltas: BoundedList<AssetDelta, 8>,
    },
    Rejected {
        client_id: Option<ClientId>,
        venue_code: BoundedText<32>,
        message: BoundedMessage<256>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueEvent {
    pub identity: EventIdentity,
    pub body: VenueEventBody,
}

impl std::str::FromStr for Decimal {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.len() > 79 {
            return Err(Error::Decimal);
        }
        let (negative, s) = if let Some(s) = value.strip_prefix('-') {
            (true, s)
        } else {
            (false, value)
        };
        let mut magnitude = 0u128;
        let mut decimal = false;
        let mut scale = 0u8;
        let mut digits = 0;
        for b in s.bytes() {
            if b == b'.' && !decimal {
                decimal = true;
                continue;
            }
            if !b.is_ascii_digit() {
                return Err(Error::Decimal);
            }
            digits += 1;
            magnitude = magnitude
                .checked_mul(10)
                .and_then(|n| n.checked_add(u128::from(b - b'0')))
                .ok_or(Error::IntegerOverflow)?;
            if decimal {
                scale = scale.checked_add(1).ok_or(Error::Decimal)?;
                if scale > 38 {
                    return Err(Error::Decimal);
                }
            }
        }
        if digits == 0 || s.starts_with('.') || s.ends_with('.') {
            return Err(Error::Decimal);
        }
        let coefficient = if negative {
            if magnitude == (1u128 << 127) {
                i128::MIN
            } else {
                -i128::try_from(magnitude).map_err(|_| Error::IntegerOverflow)?
            }
        } else {
            i128::try_from(magnitude).map_err(|_| Error::IntegerOverflow)?
        };
        Self::new(coefficient, -(scale as i8))
    }
}
