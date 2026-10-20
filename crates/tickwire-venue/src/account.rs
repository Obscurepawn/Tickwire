//! Account-wide facts keep exchange identities independent of local orders.
use crate::{
    cbor::{Reader, Writer},
    codec::Codec,
    *,
};

pub const MAX_ACCOUNT_INPUT_BYTES: usize = 60 * 1024;
pub const MAX_ACCOUNT_OBSERVATION_ASSETS: usize = 64;
pub const MAX_ACCOUNT_OBSERVATION_POSITIONS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountMarket {
    Usdm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIdentity {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub market: AccountMarket,
    /// Local pinned catalog binding; not part of the exchange transaction key.
    pub wallet_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomeRecord {
    pub income_type: BoundedText<64>,
    pub transaction_id: BoundedText<32>,
    pub delta: AssetDelta,
    pub symbol: Option<Symbol>,
    pub trade_id: Option<VenueTradeId>,
    pub venue_time_ms: u64,
    pub info: Option<BoundedMessage<256>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletObservation {
    pub asset: AssetRef,
    pub wallet_balance: Decimal,
    pub cross_wallet_balance: Decimal,
    /// Raw reported bc. This is not a unique cashflow identity.
    pub balance_change: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionObservation {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub symbol: Symbol,
    pub side: PositionSide,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub break_even_price: Option<Decimal>,
    pub accumulated_realized: Decimal,
    pub unrealized_pnl: Decimal,
    pub margin_mode: MarginMode,
    pub isolated_wallet: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountObservation {
    pub reason: BoundedText<64>,
    pub funding_symbol: Option<Symbol>,
    pub event_time_ms: u64,
    pub transaction_time_ms: Option<u64>,
    pub balances: BoundedList<WalletObservation, MAX_ACCOUNT_OBSERVATION_ASSETS>,
    pub positions: BoundedList<PositionObservation, MAX_ACCOUNT_OBSERVATION_POSITIONS>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalOrigin {
    Unregistered,
    Liquidation,
    Adl,
    Settlement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTrade {
    pub instrument: InstrumentBinding,
    pub product: Product,
    pub order_id: VenueOrderId,
    pub trade_id: VenueTradeId,
    pub side: Side,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub quote: Option<Decimal>,
    pub realized_pnl: AssetDelta,
    pub fees: BoundedList<AssetDelta, 8>,
    pub origin: ExternalOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountInputBody {
    Income(IncomeRecord),
    Observation(AccountObservation),
    ExternalTrade(Box<ExternalTrade>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInput {
    pub identity: AccountIdentity,
    pub body: AccountInputBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAccountInput {
    input: Box<AccountInput>,
    bytes: Box<[u8]>,
    digest: Hash,
}
impl CanonicalAccountInput {
    pub fn new(input: AccountInput) -> Result<Self, Error> {
        validate_input(&input)?;
        let bytes = crate::encode("tickwire.venue-account", &input.identity, &input.body);
        if bytes.len() > MAX_ACCOUNT_INPUT_BYTES {
            return Err(Error::Bounds);
        }
        let digest = Sha256::digest(&bytes).into();
        Ok(Self {
            input: Box::new(input),
            bytes: bytes.into_boxed_slice(),
            digest,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_ACCOUNT_INPUT_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = Reader::new(bytes);
        r.array(4)?;
        if r.text(32)? != "tickwire.venue-account" || r.uint()? != SCHEMA_VERSION {
            return Err(Error::Schema);
        }
        let input = AccountInput {
            identity: Codec::read(&mut r)?,
            body: Codec::read(&mut r)?,
        };
        r.end()?;
        let canonical = Self::new(input)?;
        if canonical.bytes() != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(canonical)
    }
    pub fn input(&self) -> &AccountInput {
        &self.input
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}

fn validate_input(i: &AccountInput) -> Result<(), Error> {
    let id = &i.identity;
    if id.risk_domain_id == [0; 16] || id.venue == 0 || id.account == 0 || id.wallet_id == 0 {
        return Err(Error::Identity);
    }
    match &i.body {
        AccountInputBody::Income(row) => {
            let value = row
                .transaction_id
                .as_str()
                .parse::<i64>()
                .map_err(|_| Error::Identity)?;
            if value.to_string() != row.transaction_id.as_str() {
                return Err(Error::Identity);
            }
            crate::validate::cashflow(&row.delta)?;
        }
        AccountInputBody::Observation(o) => {
            if o.balances.as_slice().is_empty() && o.positions.as_slice().is_empty() {
                return Err(Error::Bounds);
            }
            for (n, b) in o.balances.as_slice().iter().enumerate() {
                for amount in [b.wallet_balance, b.cross_wallet_balance, b.balance_change] {
                    crate::validate::cashflow(&AssetDelta {
                        asset: b.asset.clone(),
                        amount,
                    })?;
                }
                if o.balances.as_slice()[..n]
                    .iter()
                    .any(|old| old.asset.id == b.asset.id || old.asset.symbol == b.asset.symbol)
                {
                    return Err(Error::Duplicate);
                }
            }
            for (n, p) in o.positions.as_slice().iter().enumerate() {
                if p.instrument_id == 0 || p.instrument_version == 0 {
                    return Err(Error::Instrument);
                }
                if p.entry_price.coefficient() < 0
                    || p.break_even_price.is_some_and(|x| x.coefficient() < 0)
                {
                    return Err(Error::Price);
                }
                if o.positions.as_slice()[..n].iter().any(|old| {
                    (old.instrument_id == p.instrument_id || old.symbol == p.symbol)
                        && old.side == p.side
                }) {
                    return Err(Error::Duplicate);
                }
            }
        }
        AccountInputBody::ExternalTrade(t) => {
            if !matches!(t.product, Product::LinearPerpetual | Product::LinearFuture) {
                return Err(Error::ProductCombination);
            }
            crate::validate::instrument(&t.instrument, t.product)?;
            if !t.quantity.is_positive() {
                return Err(Error::Quantity);
            }
            if !t.price.is_positive() {
                return Err(Error::Price);
            }
            if t.quote.is_some_and(|q| q.coefficient() < 0) {
                return Err(Error::Quantity);
            }
            if t.realized_pnl.asset != t.instrument.settlement {
                return Err(Error::Instrument);
            }
            crate::validate::cashflow(&t.realized_pnl)?;
            crate::validate::fees(&t.fees)?;
        }
    }
    Ok(())
}
macro_rules! structure {
    ($t:ident; $($f:ident),+)=>{impl Codec for $t {
        fn write(&self,w:&mut Writer){w.array([$(stringify!($f)),+].len() as u64);$(self.$f.write(w);)+}
        fn read(r:&mut Reader<'_>)->Result<Self,Error>{r.array([$(stringify!($f)),+].len())?;Ok(Self{$($f:Codec::read(r)?,)+})}
    }};
}
impl Codec for AccountMarket {
    fn write(&self, w: &mut Writer) {
        w.uint(1)
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.uint()? {
            1 => Ok(Self::Usdm),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for ExternalOrigin {
    fn write(&self, w: &mut Writer) {
        w.uint(match self {
            Self::Unregistered => 1,
            Self::Liquidation => 2,
            Self::Adl => 3,
            Self::Settlement => 4,
        })
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.uint()? {
            1 => Ok(Self::Unregistered),
            2 => Ok(Self::Liquidation),
            3 => Ok(Self::Adl),
            4 => Ok(Self::Settlement),
            _ => Err(Error::Enum),
        }
    }
}
structure!(AccountIdentity;risk_domain_id,venue,environment,account,market,wallet_id);
structure!(IncomeRecord;income_type,transaction_id,delta,symbol,trade_id,venue_time_ms,info);
structure!(WalletObservation;asset,wallet_balance,cross_wallet_balance,balance_change);
structure!(PositionObservation;instrument_id,instrument_version,symbol,side,quantity,entry_price,break_even_price,accumulated_realized,unrealized_pnl,margin_mode,isolated_wallet);
structure!(AccountObservation;reason,funding_symbol,event_time_ms,transaction_time_ms,balances,positions);
structure!(ExternalTrade;instrument,product,order_id,trade_id,side,position_side,quantity,price,quote,realized_pnl,fees,origin);
impl Codec for AccountInputBody {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        match self {
            Self::Income(x) => {
                w.uint(1);
                x.write(w)
            }
            Self::Observation(x) => {
                w.uint(2);
                x.write(w)
            }
            Self::ExternalTrade(x) => {
                w.uint(3);
                x.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        match r.uint()? {
            1 => Ok(Self::Income(Codec::read(r)?)),
            2 => Ok(Self::Observation(Codec::read(r)?)),
            3 => Ok(Self::ExternalTrade(Codec::read(r)?)),
            _ => Err(Error::Enum),
        }
    }
}
