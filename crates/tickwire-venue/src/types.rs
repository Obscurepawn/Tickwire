use crate::{Error, Hash};

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
pub type VenueOrderId = BoundedText<64>;
pub type VenueTradeId = BoundedText<32>;
pub type Symbol = BoundedSymbol<64>;
pub type AssetSymbol = BoundedSymbol<32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    Option,
    LinearFuture,
    InverseFuture,
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
    /// Strategy IPC sequence; not an executor-assigned WAL or reservation sequence.
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
pub enum VenueEventBody {
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
