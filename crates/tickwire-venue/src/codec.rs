use crate::{
    cbor::{Reader, Writer},
    *,
};

pub(crate) trait Codec: Sized {
    fn write(&self, w: &mut Writer);
    fn read(r: &mut Reader<'_>) -> Result<Self, Error>;
}
macro_rules! unsigned {
    ($($t:ty),+) => {$ (impl Codec for $t {
        fn write(&self,w:&mut Writer){w.uint(*self as u64)}
        fn read(r:&mut Reader<'_>)->Result<Self,Error>{Self::try_from(r.uint()?).map_err(|_|Error::IntegerOverflow)}
    })+};
}
unsigned!(u16, u32, u64);
impl Codec for i8 {
    fn write(&self, w: &mut Writer) {
        w.int(i128::from(*self))
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::try_from(r.int()?).map_err(|_| Error::IntegerOverflow)
    }
}
impl Codec for bool {
    fn write(&self, w: &mut Writer) {
        w.uint(u64::from(*self))
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.uint()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Enum),
        }
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
impl<const N: usize> Codec for BoundedText<N> {
    fn write(&self, w: &mut Writer) {
        w.text(self.as_str())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::new(r.text(N)?)
    }
}
impl<const N: usize> Codec for BoundedSymbol<N> {
    fn write(&self, w: &mut Writer) {
        w.text(self.as_str())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::new(r.text(N)?)
    }
}
impl<const N: usize> Codec for BoundedMessage<N> {
    fn write(&self, w: &mut Writer) {
        w.text(self.as_str())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Self::new(r.text(N)?)
    }
}
impl Codec for TrailingDistance {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        match self {
            Self::Percent(x) => {
                w.uint(1);
                x.write(w)
            }
            Self::BasisPoints(x) => {
                w.uint(2);
                x.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        match r.uint()? {
            1 => Ok(Self::Percent(Codec::read(r)?)),
            2 => Ok(Self::BasisPoints(Codec::read(r)?)),
            _ => Err(Error::Enum),
        }
    }
}
impl<T: Codec, const N: usize> Codec for BoundedList<T, N> {
    fn write(&self, w: &mut Writer) {
        w.array(self.as_slice().len() as u64);
        for x in self.as_slice() {
            x.write(w)
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, N)?;
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            values.push(T::read(r)?);
        }
        Self::new(values)
    }
}
impl<T: Codec> Codec for Box<T> {
    fn write(&self, w: &mut Writer) {
        self.as_ref().write(w)
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Box::new(T::read(r)?))
    }
}
impl<T: Codec> Codec for Option<T> {
    fn write(&self, w: &mut Writer) {
        w.array(u64::from(self.is_some()));
        if let Some(x) = self {
            x.write(w)
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
impl Codec for Decimal {
    fn write(&self, w: &mut Writer) {
        w.array(2);
        w.int(self.coefficient());
        w.int(i128::from(self.exponent()));
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.array(2)?;
        let coefficient = r.int()?;
        let exponent = i8::try_from(r.int()?).map_err(|_| Error::IntegerOverflow)?;
        let value = Self::new(coefficient, exponent)?;
        if value.coefficient() != coefficient || value.exponent() != exponent {
            return Err(Error::NonCanonical);
        }
        Ok(value)
    }
}
macro_rules! structure {
    ($t:ident; $($f:ident),+ $(,)?)=>{impl Codec for $t {
        fn write(&self,w:&mut Writer){w.array([$(stringify!($f)),+].len() as u64);$(self.$f.write(w);)+}
        fn read(r:&mut Reader<'_>)->Result<Self,Error>{r.array([$(stringify!($f)),+].len())?;Ok(Self{$($f:Codec::read(r)?,)+})}
    }};
}
macro_rules! enumeration {
    ($t:ident; $($v:ident=$tag:literal),+ $(,)?)=>{impl Codec for $t {
        fn write(&self,w:&mut Writer){w.uint(match self{$(Self::$v=>$tag,)+})}
        fn read(r:&mut Reader<'_>)->Result<Self,Error>{match r.uint()?{$($tag=>Ok(Self::$v),)+_=>Err(Error::Enum)}}
    }};
}
enumeration!(Product;Spot=1,LinearPerpetual=2,InversePerpetual=3,Option=4,LinearFuture=5,InverseFuture=6);
enumeration!(Environment;Production=1,Test=2);
enumeration!(Side;Buy=1,Sell=2);
enumeration!(PositionSide;Both=1,Long=2,Short=3);
enumeration!(PositionMode;OneWay=1,Hedge=2);
enumeration!(MarginMode;Cross=1,Isolated=2);
enumeration!(SelfTradePrevention;Default=0,None=1,ExpireTaker=2,ExpireMaker=3,ExpireBoth=4,Decrement=5);
enumeration!(WorkingPrice;Contract=1,Mark=2,Index=3);
enumeration!(OrderType;Limit=1,Market=2,StopLimit=3,StopMarket=4,TakeProfitLimit=5,TakeProfitMarket=6,TrailingStop=7);
enumeration!(CancelReplaceMode;StopOnFailure=1,AllowFailure=2);
enumeration!(ListKind;Oco=1,Oto=2,Otoco=3);
enumeration!(PegPriceType;Primary=1,Market=2);
enumeration!(OptionRight;Call=1,Put=2);
enumeration!(OrderStatus;Pending=1,New=2,PartiallyFilled=3,Filled=4,Canceled=5,Expired=6,Rejected=7,Unknown=8);
enumeration!(Liquidity;Unknown=0,Maker=1,Taker=2);
enumeration!(StructureKind;Oco=1,Oto=2,Otoco=3,Algo=4);
enumeration!(StructureStatus;Pending=1,Live=2,Triggered=3,Terminal=4,Rejected=5,Unknown=6);
enumeration!(AmendmentSource;RestResponse=1,RestHistory=2,PrivateStream=3);

impl Codec for TimeInForce {
    fn write(&self, w: &mut Writer) {
        w.array(if matches!(self, Self::Gtd { .. }) {
            2
        } else {
            1
        });
        w.uint(match self {
            Self::Gtc => 1,
            Self::Ioc => 2,
            Self::Fok => 3,
            Self::PostOnly => 4,
            Self::Gtd { .. } => 5,
        });
        if let Self::Gtd { expire_utc_ns } = self {
            expire_utc_ns.write(w)
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, 2)?;
        match (n, r.uint()?) {
            (1, 1) => Ok(Self::Gtc),
            (1, 2) => Ok(Self::Ioc),
            (1, 3) => Ok(Self::Fok),
            (1, 4) => Ok(Self::PostOnly),
            (2, 5) => Ok(Self::Gtd {
                expire_utc_ns: Codec::read(r)?,
            }),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for Quantity {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::Base(x) => {
                w.array(2);
                w.uint(1);
                x.write(w)
            }
            Self::Quote(x) => {
                w.array(2);
                w.uint(2);
                x.write(w)
            }
            Self::ClosePosition => {
                w.array(1);
                w.uint(3)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, 2)?;
        match (n, r.uint()?) {
            (2, 1) => Ok(Self::Base(Codec::read(r)?)),
            (2, 2) => Ok(Self::Quote(Codec::read(r)?)),
            (1, 3) => Ok(Self::ClosePosition),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for ProductMode {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::Spot => {
                w.array(1);
                w.uint(1)
            }
            Self::Margin {
                margin_mode,
                auto_borrow,
                auto_repay,
            } => {
                w.array(4);
                w.uint(2);
                margin_mode.write(w);
                auto_borrow.write(w);
                auto_repay.write(w)
            }
            Self::Derivative {
                position_mode,
                margin_mode,
            } => {
                w.array(3);
                w.uint(3);
                position_mode.write(w);
                margin_mode.write(w)
            }
            Self::Option => {
                w.array(1);
                w.uint(4)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, 4)?;
        match (n, r.uint()?) {
            (1, 1) => Ok(Self::Spot),
            (4, 2) => Ok(Self::Margin {
                margin_mode: Codec::read(r)?,
                auto_borrow: Codec::read(r)?,
                auto_repay: Codec::read(r)?,
            }),
            (3, 3) => Ok(Self::Derivative {
                position_mode: Codec::read(r)?,
                margin_mode: Codec::read(r)?,
            }),
            (1, 4) => Ok(Self::Option),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for ContractKind {
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
        let n = r.len(4, 4)?;
        match (n, r.uint()?) {
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
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for CommandBody {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::New(x) => {
                w.array(2);
                w.uint(1);
                x.write(w)
            }
            Self::Cancel(x) => {
                w.array(2);
                w.uint(2);
                x.write(w)
            }
            Self::CancelReplace {
                target,
                replacement,
                mode,
            } => {
                w.array(4);
                w.uint(3);
                target.write(w);
                replacement.write(w);
                mode.write(w)
            }
            Self::AmendKeepPriority {
                target,
                new_quantity,
                new_client_id,
            } => {
                w.array(4);
                w.uint(4);
                target.write(w);
                new_quantity.write(w);
                new_client_id.write(w)
            }
            Self::Modify(x) => {
                w.array(2);
                w.uint(5);
                x.write(w)
            }
            Self::Query(x) => {
                w.array(2);
                w.uint(6);
                x.write(w)
            }
            Self::SpotList(x) => {
                w.array(2);
                w.uint(7);
                x.write(w)
            }
            Self::CancelList(x) => {
                w.array(2);
                w.uint(8);
                x.write(w)
            }
            Self::Algo(x) => {
                w.array(2);
                w.uint(9);
                x.write(w)
            }
            Self::CancelAlgo(x) => {
                w.array(2);
                w.uint(10);
                x.write(w)
            }
            Self::SpotAdvanced(x) => {
                w.array(2);
                w.uint(11);
                x.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, 4)?;
        match (n, r.uint()?) {
            (2, 1) => Ok(Self::New(Codec::read(r)?)),
            (2, 2) => Ok(Self::Cancel(Codec::read(r)?)),
            (4, 3) => Ok(Self::CancelReplace {
                target: Codec::read(r)?,
                replacement: Codec::read(r)?,
                mode: Codec::read(r)?,
            }),
            (4, 4) => Ok(Self::AmendKeepPriority {
                target: Codec::read(r)?,
                new_quantity: Codec::read(r)?,
                new_client_id: Codec::read(r)?,
            }),
            (2, 5) => Ok(Self::Modify(Codec::read(r)?)),
            (2, 6) => Ok(Self::Query(Codec::read(r)?)),
            (2, 7) => Ok(Self::SpotList(Codec::read(r)?)),
            (2, 8) => Ok(Self::CancelList(Codec::read(r)?)),
            (2, 9) => Ok(Self::Algo(Codec::read(r)?)),
            (2, 10) => Ok(Self::CancelAlgo(Codec::read(r)?)),
            (2, 11) => Ok(Self::SpotAdvanced(Codec::read(r)?)),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for SpotAdvancedCommand {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::New(o) => {
                w.array(2);
                w.uint(1);
                o.write(w);
            }
            Self::CancelReplace {
                target,
                replacement,
                mode,
            } => {
                w.array(4);
                w.uint(2);
                target.write(w);
                replacement.write(w);
                mode.write(w);
            }
            Self::List(l) => {
                w.array(2);
                w.uint(3);
                l.write(w);
            }
            Self::SorNew(o) => {
                w.array(2);
                w.uint(4);
                o.write(w);
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        match (r.len(4, 4)?, r.uint()?) {
            (2, 1) => Ok(Self::New(Codec::read(r)?)),
            (4, 2) => Ok(Self::CancelReplace {
                target: Codec::read(r)?,
                replacement: Codec::read(r)?,
                mode: Codec::read(r)?,
            }),
            (2, 3) => Ok(Self::List(Codec::read(r)?)),
            (2, 4) => Ok(Self::SorNew(Codec::read(r)?)),
            _ => Err(Error::Enum),
        }
    }
}
impl Codec for VenueEventBody {
    fn write(&self, w: &mut Writer) {
        match self {
            Self::AmendmentRejected(x) => {
                w.array(2);
                w.uint(7);
                x.write(w)
            }
            Self::Amendment(x) => {
                w.array(2);
                w.uint(6);
                x.write(w)
            }
            Self::Structure(x) => {
                w.array(2);
                w.uint(5);
                x.write(w)
            }
            Self::Order(x) => {
                w.array(2);
                w.uint(1);
                x.write(w)
            }
            Self::Fill(x) => {
                w.array(2);
                w.uint(2);
                x.write(w)
            }
            Self::Fee {
                order,
                fee_id,
                deltas,
            } => {
                w.array(4);
                w.uint(3);
                order.write(w);
                fee_id.write(w);
                deltas.write(w)
            }
            Self::Rejected {
                client_id,
                venue_code,
                message,
            } => {
                w.array(4);
                w.uint(4);
                client_id.write(w);
                venue_code.write(w);
                message.write(w)
            }
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.len(4, 4)?;
        match (n, r.uint()?) {
            (2, 7) => Ok(Self::AmendmentRejected(Codec::read(r)?)),
            (2, 6) => Ok(Self::Amendment(Codec::read(r)?)),
            (2, 5) => Ok(Self::Structure(Codec::read(r)?)),
            (2, 1) => Ok(Self::Order(Codec::read(r)?)),
            (2, 2) => Ok(Self::Fill(Codec::read(r)?)),
            (4, 3) => Ok(Self::Fee {
                order: Codec::read(r)?,
                fee_id: Codec::read(r)?,
                deltas: Codec::read(r)?,
            }),
            (4, 4) => Ok(Self::Rejected {
                client_id: Codec::read(r)?,
                venue_code: Codec::read(r)?,
                message: Codec::read(r)?,
            }),
            _ => Err(Error::Enum),
        }
    }
}
structure!(AssetRef;id,version,symbol,exponent);
structure!(InstrumentBinding;instrument_id,instrument_version,catalog_version,catalog_hash,symbol,base,quote,settlement,price_tick,quantity_step,contract);
structure!(RequestIdentity;risk_domain_id,venue,environment,account,product,intent_id,intent_exec_epoch,strategy_generation,config_revision,deadline,recovery_object);
structure!(StrategyContext;logical_strategy_id,instance_id,strategy_intent_seq,authorization_epoch,risk_policy_version,order_config_version,created_mono_ns,economic_intent_id,hedge_group_id,leg_id,recovery_manifest_hash);
structure!(TriggerRef;topic_id,feed_generation,ring_epoch,sequence,instrument_id,instrument_version,observed_mono_ns);
structure!(CommandContext;request,strategy,instrument,triggers);
structure!(Trigger;price,working_price,price_protect);
structure!(Trailing;activation_price,working_price,distance);
structure!(OrderSpec;client_id,side,order_type,quantity,price,time_in_force,position_side,mode,reduce_only,self_trade_prevention,trigger,trailing,iceberg_quantity);
structure!(OrderTarget;order_id,expected_owner_revision,original_intent_exec_epoch,client_id,venue_order_id);
structure!(ListOrder;list_id,client_id,kind,legs);
structure!(Peg;price_type,offset_levels);
structure!(SpotOrder;order,peg);
structure!(SpotAdvancedList;list_id,client_id,kind,legs);
structure!(ListTarget;list_id,expected_owner_revision,original_intent_exec_epoch,client_id,venue_list_id);
structure!(AlgoOrder;parent_order_id,order);
structure!(AlgoTarget;parent_order_id,expected_owner_revision,original_intent_exec_epoch,client_id,venue_algo_id);
structure!(ModifyOrder;target,new_client_id,side,quantity,price,position_side,reduce_only);
structure!(Command;context,body);
structure!(EventIdentity;risk_domain_id,venue,environment,account,product,instrument_id,instrument_version,source_connection_epoch,source_sequence,event_id,venue_time_ns,received_mono_ns);
structure!(OrderIdentity;order_id,original_intent_id,original_intent_exec_epoch,client_id,venue_order_id,parent_order_id,list_id,venue_algo_id,venue_list_id,list_client_id,algo_client_id);
structure!(AssetDelta;asset,amount);
structure!(OrderObservation;order,status,side,position_side,original_quantity,cumulative_quantity,cumulative_quote,leaves_quantity,order_price);
structure!(FillEvent;observation,trade_id,last_quantity,last_price,last_quote,realized_pnl,liquidity,fees);
structure!(VenueEvent;identity,body);
structure!(StructureIdentity;structure_id,original_intent_id,original_intent_exec_epoch,kind,client_id,venue_parent_id);
structure!(StructureChild;order,reported_client_id);
structure!(StructureObservation;parent,status,venue_status,children);
structure!(AmendmentObservation;order,request_intent_id,request_intent_exec_epoch,request_digest,execution_source,execution_id,previous_quantity,new_quantity,observation);
structure!(AmendmentRejection;order,request_intent_id,request_intent_exec_epoch,request_digest,venue_code);
