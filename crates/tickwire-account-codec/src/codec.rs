use crate::{
    Error, MAX_ADMISSION_ORDERS, MAX_BRACKETS, MAX_OWNERS,
    cbor::{Reader, Writer},
};
use std::result::Result;
use tickwire_account::*;
use tickwire_risk::futures::Bracket;
use tickwire_venue as v;

pub(crate) trait Codec: Sized {
    fn write(&self, w: &mut Writer);
    fn read(r: &mut Reader<'_>) -> Result<Self, Error>;
}
macro_rules! unsigned {
    ($($t:ty),+) => {$(
        impl Codec for $t {
            fn write(&self, w: &mut Writer) { w.uint(*self as u64); }
            fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
                Self::try_from(r.uint()?).map_err(|_| Error::IntegerOverflow)
            }
        }
    )+};
}
unsigned!(u8, u32, u64);
impl Codec for bool {
    fn write(&self, w: &mut Writer) {
        w.uint(u64::from(*self))
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.uint()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Field),
        }
    }
}
impl Codec for i8 {
    fn write(&self, w: &mut Writer) {
        w.int(i128::from(*self))
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::try_from(r.int()?).map_err(|_| Error::IntegerOverflow)
    }
}
impl<const N: usize> Codec for [u8; N] {
    fn write(&self, w: &mut Writer) {
        w.bytes(self)
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.bytes()
    }
}
impl Codec for Fixed {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        w.int(self.atoms());
        w.uint(self.scale().into())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        Fixed::from_atoms(r.int()?, u8::read(r)?).map_err(|_| Error::Field)
    }
}
impl Codec for v::Decimal {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        w.int(self.coefficient());
        self.exponent().write(w)
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        let n = r.int()?;
        let exp = i8::read(r)?;
        let v = v::Decimal::new(n, exp).map_err(|_| Error::Field)?;
        if v.coefficient() != n || v.exponent() != exp {
            return Err(Error::NonCanonical);
        }
        Ok(v)
    }
}
impl<const N: usize> Codec for v::BoundedText<N> {
    fn write(&self, w: &mut Writer) {
        w.text(self.as_str())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::new(r.text(N)?).map_err(|_| Error::Text)
    }
}
impl<const N: usize> Codec for v::BoundedSymbol<N> {
    fn write(&self, w: &mut Writer) {
        w.text(self.as_str())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::new(r.text(N)?).map_err(|_| Error::Text)
    }
}
impl<T: Codec> Codec for Option<T> {
    fn write(&self, w: &mut Writer) {
        w.array(u64::from(self.is_some()));
        if let Some(v) = self {
            v.write(w)
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.len(4, 1)? {
            0 => Ok(None),
            1 => Ok(Some(T::read(r)?)),
            _ => unreachable!(),
        }
    }
}
impl<A: Codec, B: Codec> Codec for (A, B) {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        self.0.write(w);
        self.1.write(w)
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        Ok((A::read(r)?, B::read(r)?))
    }
}
impl<T: Codec> Codec for Vec<T> {
    fn write(&self, w: &mut Writer) {
        w.array(self.len() as u64);
        for x in self {
            x.write(w)
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        read_vec(r, MAX_OWNERS)
    }
}
fn read_vec<T: Codec>(r: &mut Reader<'_>, max: usize) -> Result<Vec<T>, Error> {
    let n = r.len(4, max)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(T::read(r)?);
    }
    Ok(out)
}
macro_rules! read_field {
    ($r:ident) => {
        Codec::read($r)?
    };
    ($r:ident, $max:expr) => {
        read_vec($r, $max)?
    };
}
macro_rules! structure {
    ($t:ty; $($f:ident $(=> $max:expr)?),+ $(,)?)=>{
        impl Codec for $t {
            fn write(&self,w:&mut Writer){
                w.array([$(stringify!($f)),+].len() as u64);$(self.$f.write(w);)+
            }
            fn read(r:&mut Reader<'_>)->Result<Self,Error>{
                r.array([$(stringify!($f)),+].len())?;
                Ok(Self{$($f:read_field!(r $(,$max)?)),+})
            }
        }
    };
}
macro_rules! enumeration {
    ($t:ty; $($v:ident=$tag:literal),+)=>{
        impl Codec for $t{
            fn write(&self,w:&mut Writer){w.uint(match self{$(Self::$v=>$tag),+})}
            fn read(r:&mut Reader<'_>)->Result<Self,Error>{
                match r.uint()?{$($tag=>Ok(Self::$v),)+_=>Err(Error::Field)}
            }
        }
    };
}
enumeration!(v::Environment; Production=1,Test=2);
enumeration!(v::Product; Spot=1,LinearPerpetual=2,InversePerpetual=3,Option=4,LinearFuture=5,InverseFuture=6);
enumeration!(v::PositionSide; Both=1,Long=2,Short=3);
enumeration!(v::PositionMode; OneWay=1,Hedge=2);
enumeration!(v::MarginMode; Cross=1,Isolated=2);
enumeration!(v::OptionRight; Call=1,Put=2);
impl Codec for v::ContractKind {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::Cash => {
                w.array(1);
                w.uint(1)
            }
            Self::Perpetual => {
                w.array(1);
                w.uint(2)
            }
            Self::Delivery { expiry_utc_ns } => {
                w.array(2);
                w.uint(3);
                expiry_utc_ns.write(w)
            }
            Self::Option {
                expiry_utc_ns,
                strike,
                right,
            } => {
                w.array(4);
                w.uint(4);
                expiry_utc_ns.write(w);
                strike.write(w);
                right.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match (r.len(4, 4)?, r.uint()?) {
            (1, 1) => Ok(Self::Cash),
            (1, 2) => Ok(Self::Perpetual),
            (2, 3) => Ok(Self::Delivery {
                expiry_utc_ns: Codec::read(r)?,
            }),
            (4, 4) => Ok(Self::Option {
                expiry_utc_ns: Codec::read(r)?,
                strike: Codec::read(r)?,
                right: Codec::read(r)?,
            }),
            _ => Err(Error::Field),
        }
    }
}
structure!(v::AssetRef; id,version,symbol,exponent);
structure!(v::InstrumentBinding; instrument_id,instrument_version,catalog_version,catalog_hash,symbol,
    base,quote,settlement,price_tick,quantity_step,contract);
structure!(AssetKey; wallet_id,asset_id);
structure!(AssetAmount; key,amount);
structure!(IntentKey; epoch,intent_id);
structure!(OrderKey; intent,client_id);
structure!(Scope; risk_domain_id,venue,environment,account);
structure!(Revisions; risk,fee,bracket,collateral,valuation);
structure!(AssetSnapshot; key,asset,free,locked,nontrade_locked,exclusive_fee_pool,ownership=>MAX_OWNERS);
structure!(PositionSnapshot; instrument_id,side,quantity,entry_bound,ownership=>MAX_OWNERS);
structure!(Snapshot; scope,account_epoch,ledger_sequence,revisions,valid_until,assets=>MAX_ASSETS,
    positions=>2*MAX_INSTRUMENTS,venue_available=>MAX_ASSETS);
impl Codec for [Fixed; 3] {
    fn write(&self, w: &mut Writer) {
        w.array(3);
        for v in self {
            v.write(w)
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(3)?;
        Ok([Codec::read(r)?, Codec::read(r)?, Codec::read(r)?])
    }
}
structure!(FeePolicy; rates,maximum_fills,exclusive_pool);
structure!(Bracket; floor,cap,rate,cumulative,max_initial_leverage);
structure!(FuturesPolicy; position_mode,margin_mode,stress_low,stress_high,configured_leverage,venue_leverage,
    brackets=>MAX_BRACKETS,maintenance_addon,exit_fee_rate,funding_rate_abs_upper,funding_settlements,
    pending_outbound,capital_buffer);
structure!(InstrumentPolicy; instrument,product,wallet_id,fees,futures,max_absolute_position,max_gross_quantity,
    strategy_limits=>MAX_OWNERS);
structure!(Catalog; entries=>MAX_INSTRUMENTS);
structure!(CapitalHold; wallet_id,settlement_asset,amount);
structure!(AdmissionImage; command_digest,intent,account_epoch,ledger_sequence,revisions,deadline,
    resources=>MAX_ASSETS,capital_holds=>MAX_ASSETS,orders=>MAX_ADMISSION_ORDERS,increases);

structure!(Reactivation; scope,product,account_epoch,ledger_sequence,baseline_digest,history_digest,input_stream,private_connection_epoch,gap_generation,through_sequence,query_coverage_digest,clock_boot_id,clock_generation,evaluated_at,valid_until,writer_epoch,execution_connection_epoch,venue_available);

impl Codec for RecoveryRecord {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::Reactivated(binding)=>{w.array(2);w.uint(10);binding.as_ref().write(w);}
            Self::Admit {
                command,
                evaluated_at,
                expected,
            } => {
                w.array(4);
                w.uint(1);
                w.bytes(command.bytes());
                evaluated_at.write(w);
                expected.write(w)
            }
            Self::IngressEvent {
                input_stream,
                input_sequence,
                event,
            } => {
                w.array(4);
                w.uint(9);
                input_stream.write(w);
                input_sequence.write(w);
                w.bytes(event.bytes());
            }
            Self::VenueEvent(e) => {
                w.array(2);
                w.uint(2);
                w.bytes(e.bytes())
            }
            Self::Aborted {
                intent,
                command_digest,
            } => {
                w.array(3);
                w.uint(3);
                intent.write(w);
                command_digest.write(w)
            }
            Self::FeesReconciled { order, cumulative } => {
                w.array(3);
                w.uint(4);
                order.write(w);
                cumulative.write(w)
            }
            Self::UnattributedCashFlow { id, delta } => {
                w.array(3);
                w.uint(5);
                id.write(w);
                delta.write(w)
            }
            Self::ExternalPosition {
                id,
                instrument,
                side,
                quantity,
                entry_bound,
            } => {
                w.array(6);
                w.uint(6);
                id.write(w);
                instrument.write(w);
                side.write(w);
                quantity.write(w);
                entry_bound.write(w)
            }
            Self::TransferAsset {
                key,
                from,
                to,
                amount,
            } => {
                w.array(5);
                w.uint(7);
                key.write(w);
                from.write(w);
                to.write(w);
                amount.write(w)
            }
            Self::TransferPosition {
                instrument,
                side,
                from,
                to,
                quantity,
            } => {
                w.array(6);
                w.uint(8);
                instrument.write(w);
                side.write(w);
                from.write(w);
                to.write(w);
                quantity.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match (r.len(4, 6)?, r.uint()?) {
            (2, 10)=>Self::Reactivated(Box::new(Codec::read(r)?)),
            (4, 1) => Self::Admit {
                command: Box::new(
                    v::CanonicalCommand::decode(r.blob(v::MAX_CANONICAL_BYTES)?)
                        .map_err(Error::Venue)?,
                ),
                evaluated_at: Codec::read(r)?,
                expected: Codec::read(r)?,
            },
            (2, 2) => Self::VenueEvent(Box::new(
                v::CanonicalEvent::decode(r.blob(v::MAX_CANONICAL_BYTES)?).map_err(Error::Venue)?,
            )),
            (4, 9) => Self::IngressEvent {
                input_stream: Codec::read(r)?,
                input_sequence: Codec::read(r)?,
                event: Box::new(
                    v::CanonicalEvent::decode(r.blob(v::MAX_CANONICAL_BYTES)?)
                        .map_err(Error::Venue)?,
                ),
            },
            (3, 3) => Self::Aborted {
                intent: Codec::read(r)?,
                command_digest: Codec::read(r)?,
            },
            (3, 4) => Self::FeesReconciled {
                order: Codec::read(r)?,
                cumulative: Codec::read(r)?,
            },
            (3, 5) => Self::UnattributedCashFlow {
                id: Codec::read(r)?,
                delta: Codec::read(r)?,
            },
            (6, 6) => Self::ExternalPosition {
                id: Codec::read(r)?,
                instrument: Codec::read(r)?,
                side: Codec::read(r)?,
                quantity: Codec::read(r)?,
                entry_bound: Codec::read(r)?,
            },
            (5, 7) => Self::TransferAsset {
                key: Codec::read(r)?,
                from: Codec::read(r)?,
                to: Codec::read(r)?,
                amount: Codec::read(r)?,
            },
            (6, 8) => Self::TransferPosition {
                instrument: Codec::read(r)?,
                side: Codec::read(r)?,
                from: Codec::read(r)?,
                to: Codec::read(r)?,
                quantity: Codec::read(r)?,
            },
            _ => return Err(Error::Field),
        })
    }
}
