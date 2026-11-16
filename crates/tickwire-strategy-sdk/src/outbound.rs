//! Inline 1,024-byte `IntentRecord` shared-memory encoding, `SelfDescribingClientId` minting,
//! `ModifyRequest` validation, and zero-copy SPSC `IntentWriter` / `IntentReader` transport.
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::os::fd::OwnedFd;

use tickwire_ipc::{
    ALL_STRATS, IntentBody, IntentRecord, RecordHeader, ResourceHeader, TriggerRef, Wire,
    intent_flags, intent_kind, tif_code,
};
use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};
use tickwire_venue::{
    CancelReplaceMode, ClientId, ClientIdDomain, Command, CommandBody, CommandContext, Decimal,
    ModifyOrder, OrderSpec, OrderTarget, OrderType, PositionSide, Product, Quantity,
    SelfDescribingClientId, SelfTradePrevention, Side, SpotAdvancedCommand, TimeInForce,
    VenueOrderId, WorkingPrice,
};

use crate::{
    Error, Result,
    arena::{ArenaSliceMeta, ShmArenaReader, ShmArenaWriter, in_process_arena_pair},
    dop::DopSession,
};

const SEQUENCE_LIMIT: u64 = 1 << 63;

/// Required `ResourceHeader::required_features` bit for the 1,024-byte SPSC intent ring (`resource_kind = 2`).
pub const FEATURE: u64 = 1 << 7;

/// Ergonomic alias for [`IntentWriter`] on the strategy outbound SPSC command lane.
pub type OutboundWriter = IntentWriter;

/// Ergonomic alias for [`IntentReader`] on the ORS inbound SPSC command lane.
pub type OutboundReader = IntentReader;

/// Outbound command opcode (`command_kind` `1..=13`, `message_type = 512 + command_kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum IntentOpcode {
    NewOrder = 1,
    Cancel = 2,
    CancelReplace = 3,
    AmendKeepPriority = 4,
    Modify = 5,
    QueryOrder = 6,
    CancelList = 7,
    CancelAlgo = 8,
    CancelAll = 9,
    NewRfqQuote = 10,
    ModifyRfqQuote = 11,
    CancelRfqQuote = 12,
    CancelAllRfqQuotes = 13,
}

impl IntentOpcode {
    #[inline]
    #[forbid(unsafe_code)]
    pub const fn code(self) -> u16 {
        self as u16
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub const fn message_type(self) -> u16 {
        512 + (self as u16)
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(Self::NewOrder),
            2 => Some(Self::Cancel),
            3 => Some(Self::CancelReplace),
            4 => Some(Self::AmendKeepPriority),
            5 => Some(Self::Modify),
            6 => Some(Self::QueryOrder),
            7 => Some(Self::CancelList),
            8 => Some(Self::CancelAlgo),
            9 => Some(Self::CancelAll),
            10 => Some(Self::NewRfqQuote),
            11 => Some(Self::ModifyRfqQuote),
            12 => Some(Self::CancelRfqQuote),
            13 => Some(Self::CancelAllRfqQuotes),
            _ => None,
        }
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub const fn from_message_type(message_type: u16) -> Option<Self> {
        if message_type >= intent_kind::MSG_NEW_ORDER
            && message_type <= intent_kind::MSG_CANCEL_ALL_RFQ_QUOTES
        {
            Self::from_code(message_type - 512)
        } else {
            None
        }
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub const fn claims_order_seqnum(self) -> bool {
        !matches!(self, Self::CancelAll | Self::CancelAllRfqQuotes)
    }
}

/// Exact integer conversion `value / step` into ticks or lots (`0` floating-point arithmetic).
#[forbid(unsafe_code)]
pub fn ticks_from_decimal(value: Decimal, step: Decimal) -> Result<i64> {
    if !step.is_positive() {
        return Err(Error::Command(tickwire_venue::Error::Decimal));
    }
    if value.coefficient() == 0 {
        return Ok(0);
    }
    let shift = i32::from(value.exponent()) - i32::from(step.exponent());
    let quotient = if shift >= 0 {
        let factor = 10i128
            .checked_pow(shift as u32)
            .ok_or(Error::Command(tickwire_venue::Error::IntegerOverflow))?;
        let scaled = value
            .coefficient()
            .checked_mul(factor)
            .ok_or(Error::Command(tickwire_venue::Error::IntegerOverflow))?;
        if scaled % step.coefficient() != 0 {
            return Err(Error::Command(tickwire_venue::Error::Decimal));
        }
        scaled / step.coefficient()
    } else {
        let factor = 10i128
            .checked_pow((-shift) as u32)
            .ok_or(Error::Command(tickwire_venue::Error::IntegerOverflow))?;
        let denom = step
            .coefficient()
            .checked_mul(factor)
            .ok_or(Error::Command(tickwire_venue::Error::IntegerOverflow))?;
        if value.coefficient() % denom != 0 {
            return Err(Error::Command(tickwire_venue::Error::Decimal));
        }
        value.coefficient() / denom
    };
    i64::try_from(quotient).map_err(|_| Error::Command(tickwire_venue::Error::IntegerOverflow))
}

/// Builder for [`CommandBody::Modify`] (`command_kind = 5`, `message_type = 517`) that validates
/// positive `price` and `quantity` plus non-zero `OrderTarget` identity before queuing to SPSC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyRequest {
    pub target: OrderTarget,
    pub new_client_id: Option<ClientId>,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub position_side: PositionSide,
    pub reduce_only: bool,
}

#[forbid(unsafe_code)]
impl ModifyRequest {
    #[inline]
    pub fn new(target: OrderTarget, side: Side, quantity: Decimal, price: Decimal) -> Self {
        Self {
            target,
            new_client_id: None,
            side,
            quantity,
            price,
            position_side: PositionSide::Both,
            reduce_only: false,
        }
    }

    #[inline]
    pub fn with_new_client_id(mut self, client_id: ClientId) -> Self {
        self.new_client_id = Some(client_id);
        self
    }

    /// Mints a deterministic `SelfDescribingClientId` for venues whose in-place modification
    /// wire assigns a fresh `newClientOrderId` (for example, `REPLACEMENT_ORDER_LEG`).
    pub fn with_minted_client_id(mut self, ctx: &CommandContext, leg: u8) -> Result<Self> {
        let (client_id, _) = mint_self_describing_client_id(ctx, leg)?;
        self.new_client_id = Some(client_id);
        Ok(self)
    }

    #[inline]
    pub fn with_position_side(mut self, position_side: PositionSide) -> Self {
        self.position_side = position_side;
        self
    }

    #[inline]
    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    /// Validates strictly positive `quantity` and `price` and non-empty `OrderTarget` fields.
    pub fn validate(&self) -> Result<()> {
        if !self.quantity.is_positive() {
            return Err(Error::Command(tickwire_venue::Error::Quantity));
        }
        if !self.price.is_positive() {
            return Err(Error::Command(tickwire_venue::Error::Price));
        }
        if self.target.order_id == [0; 16]
            || self.target.expected_owner_revision == 0
            || self.target.original_intent_exec_epoch == 0
            || (self.target.client_id.is_none() && self.target.venue_order_id.is_none())
        {
            return Err(Error::Command(tickwire_venue::Error::Identity));
        }
        Ok(())
    }

    pub fn into_modify_order(self) -> Result<ModifyOrder> {
        self.validate()?;
        Ok(ModifyOrder {
            target: self.target,
            new_client_id: self.new_client_id,
            side: self.side,
            quantity: self.quantity,
            price: self.price,
            position_side: self.position_side,
            reduce_only: self.reduce_only,
        })
    }

    /// Validates `self` and constructs a canonical CBOR [`tickwire_venue::CanonicalCommand`] checked against
    /// `context` instrument tick/step bounds.
    pub fn build_command(
        self,
        context: CommandContext,
    ) -> Result<tickwire_venue::CanonicalCommand> {
        let modify = self.into_modify_order()?;
        build_modify_command(context, modify)
    }

    /// Encodes `self` directly into an inline [`OutboundIntent`] (`IntentOpcode::Modify`).
    #[allow(clippy::too_many_arguments)]
    pub fn to_outbound_intent(
        &self,
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        price_tick: Decimal,
        quantity_step: Decimal,
        orig_order_seqnum: u64,
        tick_mono_ns: u64,
    ) -> Result<OutboundIntent> {
        self.validate()?;
        let price_ticks = ticks_from_decimal(self.price, price_tick)?;
        let qty_lots = ticks_from_decimal(self.quantity, quantity_step)?;
        let mut out = OutboundIntent::modify(
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            product,
            self.side,
            orig_order_seqnum,
            price_ticks,
            qty_lots,
            self.new_client_id.as_ref().map(ClientId::as_str),
            self.target.client_id.as_ref().map(ClientId::as_str),
            self.target
                .venue_order_id
                .as_ref()
                .map(VenueOrderId::as_str),
            tick_mono_ns,
        )?;
        out.order_id = self.target.order_id;
        out.position_side = encode_position_side(self.position_side);
        if self.reduce_only {
            out.intent_flags |= intent_flags::REDUCE_ONLY;
        }
        Ok(out)
    }
}

/// Validates a [`ModifyOrder`] (`price > 0`, `quantity > 0`, valid `target`, and catalog
/// tick/step alignment) and returns its [`tickwire_venue::CanonicalCommand`].
#[forbid(unsafe_code)]
pub fn build_modify_command(
    context: CommandContext,
    modify: ModifyOrder,
) -> Result<tickwire_venue::CanonicalCommand> {
    if !modify.quantity.is_positive() {
        return Err(Error::Command(tickwire_venue::Error::Quantity));
    }
    if !modify.price.is_positive() {
        return Err(Error::Command(tickwire_venue::Error::Price));
    }
    if modify.target.order_id == [0; 16]
        || modify.target.expected_owner_revision == 0
        || modify.target.original_intent_exec_epoch == 0
        || (modify.target.client_id.is_none() && modify.target.venue_order_id.is_none())
    {
        return Err(Error::Command(tickwire_venue::Error::Identity));
    }
    Ok(tickwire_venue::CanonicalCommand::new(Command {
        context,
        body: CommandBody::Modify(modify),
    })?)
}

/// Mint a canonical 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) and its
/// deterministic 16-byte internal order UUID from `CommandContext`.
#[forbid(unsafe_code)]
pub fn mint_self_describing_client_id(
    ctx: &CommandContext,
    leg: u8,
) -> Result<(ClientId, [u8; 16])> {
    let domain = ClientIdDomain::from_command_context(ctx);
    let sd = SelfDescribingClientId::from_command_context(ctx, leg)?;
    let client_id = sd.encode(&domain)?;
    Ok((client_id, sd.deterministic_order_id(&domain)))
}

/// Pure in-memory representation of a strategy outbound order/cancel/modify/cancel-all/RFQ intent
/// before encoding into a 1,024-byte `IntentRecord` SHM slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundIntent {
    pub opcode: IntentOpcode,
    pub strat_id: u8,
    pub account_ref: u8,
    pub order_seqnum: u64,
    pub orig_order_seqnum: u64,
    pub tick_mono_ns: u64,
    pub created_mono_ns: u64,
    pub valid_until_mono_ns: u64,
    pub expire_utc_ns: u64,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub config_version: u64,
    pub exec_generation: u64,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub stop_price_ticks: i64,
    pub quote_amount_units: i64,
    pub iceberg_qty_lots: i64,
    pub rfq_ask_price_ticks: i64,
    pub rfq_ask_qty_lots: i64,
    pub rfq_id: u64,
    pub leg_id: u16,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub position_side: u8,
    pub stp_mode: u8,
    pub working_price: u8,
    pub intent_product: u8,
    pub intent_flags: u8,
    pub cancel_replace_mode: u8,
    pub client_id_len: u8,
    pub orig_client_id_len: u8,
    pub venue_order_id_len: u8,
    pub client_id: [u8; 40],
    pub orig_client_id: [u8; 40],
    pub venue_order_id: [u8; 64],
    pub order_id: [u8; 16],
    pub triggers: Vec<TriggerRef>,
    pub arena_payload: Option<Vec<u8>>,
}

#[forbid(unsafe_code)]
impl OutboundIntent {
    fn blank(
        opcode: IntentOpcode,
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Option<Product>,
        tick_mono_ns: u64,
    ) -> Self {
        Self {
            opcode,
            strat_id,
            account_ref,
            order_seqnum: 0,
            orig_order_seqnum: 0,
            tick_mono_ns,
            created_mono_ns: 0,
            valid_until_mono_ns: 0,
            expire_utc_ns: 0,
            instrument_id,
            instrument_version,
            config_version: 1,
            exec_generation: 0,
            price_ticks: 0,
            qty_lots: 0,
            stop_price_ticks: 0,
            quote_amount_units: 0,
            iceberg_qty_lots: 0,
            rfq_ask_price_ticks: 0,
            rfq_ask_qty_lots: 0,
            rfq_id: 0,
            leg_id: 0,
            side: 0,
            order_type: 0,
            time_in_force: 0,
            position_side: 0,
            stp_mode: 0,
            working_price: 0,
            intent_product: product.map_or(0, Product::code),
            intent_flags: 0,
            cancel_replace_mode: 0,
            client_id_len: 0,
            orig_client_id_len: 0,
            venue_order_id_len: 0,
            client_id: [0; 40],
            orig_client_id: [0; 40],
            venue_order_id: [0; 64],
            order_id: [0; 16],
            triggers: Vec::new(),
            arena_payload: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_order(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        side: Side,
        order_type: OrderType,
        time_in_force: Option<TimeInForce>,
        price_ticks: i64,
        qty_lots: i64,
        client_id: &str,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::NewOrder,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.side = encode_side(side);
        out.order_type = encode_order_type(order_type);
        apply_tif(&mut out, time_in_force);
        out.price_ticks = price_ticks;
        out.qty_lots = qty_lots;
        write_padded_ascii(&mut out.client_id, &mut out.client_id_len, client_id)?;
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn cancel(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        side: Option<Side>,
        orig_order_seqnum: u64,
        orig_client_id: Option<&str>,
        venue_order_id: Option<&str>,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::Cancel,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.side = side.map_or(0, encode_side);
        out.orig_order_seqnum = orig_order_seqnum;
        if let Some(cid) = orig_client_id {
            write_padded_ascii(&mut out.orig_client_id, &mut out.orig_client_id_len, cid)?;
        }
        if let Some(vid) = venue_order_id {
            write_padded_ascii(&mut out.venue_order_id, &mut out.venue_order_id_len, vid)?;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn cancel_replace(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        side: Side,
        order_type: OrderType,
        time_in_force: Option<TimeInForce>,
        mode: CancelReplaceMode,
        orig_order_seqnum: u64,
        price_ticks: i64,
        qty_lots: i64,
        new_client_id: &str,
        orig_client_id: Option<&str>,
        venue_order_id: Option<&str>,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::CancelReplace,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.side = encode_side(side);
        out.order_type = encode_order_type(order_type);
        apply_tif(&mut out, time_in_force);
        out.cancel_replace_mode = encode_cancel_replace_mode(mode);
        out.orig_order_seqnum = orig_order_seqnum;
        out.price_ticks = price_ticks;
        out.qty_lots = qty_lots;
        write_padded_ascii(&mut out.client_id, &mut out.client_id_len, new_client_id)?;
        if let Some(cid) = orig_client_id {
            write_padded_ascii(&mut out.orig_client_id, &mut out.orig_client_id_len, cid)?;
        }
        if let Some(vid) = venue_order_id {
            write_padded_ascii(&mut out.venue_order_id, &mut out.venue_order_id_len, vid)?;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn amend_keep_priority(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        orig_order_seqnum: u64,
        new_qty_lots: i64,
        new_client_id: Option<&str>,
        orig_client_id: Option<&str>,
        venue_order_id: Option<&str>,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::AmendKeepPriority,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.orig_order_seqnum = orig_order_seqnum;
        out.qty_lots = new_qty_lots;
        if let Some(cid) = new_client_id {
            write_padded_ascii(&mut out.client_id, &mut out.client_id_len, cid)?;
        }
        if let Some(cid) = orig_client_id {
            write_padded_ascii(&mut out.orig_client_id, &mut out.orig_client_id_len, cid)?;
        }
        if let Some(vid) = venue_order_id {
            write_padded_ascii(&mut out.venue_order_id, &mut out.venue_order_id_len, vid)?;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn modify(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        side: Side,
        orig_order_seqnum: u64,
        price_ticks: i64,
        qty_lots: i64,
        new_client_id: Option<&str>,
        orig_client_id: Option<&str>,
        venue_order_id: Option<&str>,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::Modify,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.side = encode_side(side);
        out.orig_order_seqnum = orig_order_seqnum;
        out.price_ticks = price_ticks;
        out.qty_lots = qty_lots;
        if let Some(cid) = new_client_id {
            write_padded_ascii(&mut out.client_id, &mut out.client_id_len, cid)?;
        }
        if let Some(cid) = orig_client_id {
            write_padded_ascii(&mut out.orig_client_id, &mut out.orig_client_id_len, cid)?;
        }
        if let Some(vid) = venue_order_id {
            write_padded_ascii(&mut out.venue_order_id, &mut out.venue_order_id_len, vid)?;
        }
        Ok(out)
    }

    pub fn cancel_all(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Option<Product>,
        side: Option<Side>,
        tick_mono_ns: u64,
    ) -> Self {
        let mut out = Self::blank(
            IntentOpcode::CancelAll,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            product,
            tick_mono_ns,
        );
        out.side = side.map_or(0, encode_side);
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_rfq_quote(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        rfq_id: u64,
        bid_price_ticks: i64,
        bid_qty_lots: i64,
        ask_price_ticks: i64,
        ask_qty_lots: i64,
        client_id: &str,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::NewRfqQuote,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.rfq_id = rfq_id;
        out.price_ticks = bid_price_ticks;
        out.qty_lots = bid_qty_lots;
        out.rfq_ask_price_ticks = ask_price_ticks;
        out.rfq_ask_qty_lots = ask_qty_lots;
        out.side = if bid_qty_lots > 0 && ask_qty_lots == 0 {
            1
        } else if ask_qty_lots > 0 && bid_qty_lots == 0 {
            2
        } else {
            0
        };
        write_padded_ascii(&mut out.client_id, &mut out.client_id_len, client_id)?;
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn cancel_rfq_quote(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Product,
        rfq_id: u64,
        orig_order_seqnum: u64,
        orig_client_id: Option<&str>,
        venue_order_id: Option<&str>,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        let mut out = Self::blank(
            IntentOpcode::CancelRfqQuote,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.rfq_id = rfq_id;
        out.orig_order_seqnum = orig_order_seqnum;
        if let Some(cid) = orig_client_id {
            write_padded_ascii(&mut out.orig_client_id, &mut out.orig_client_id_len, cid)?;
        }
        if let Some(vid) = venue_order_id {
            write_padded_ascii(&mut out.venue_order_id, &mut out.venue_order_id_len, vid)?;
        }
        Ok(out)
    }

    pub fn cancel_all_rfq_quotes(
        strat_id: u8,
        account_ref: u8,
        instrument_id: u64,
        instrument_version: u64,
        product: Option<Product>,
        rfq_id: u64,
        tick_mono_ns: u64,
    ) -> Self {
        let mut out = Self::blank(
            IntentOpcode::CancelAllRfqQuotes,
            strat_id,
            account_ref,
            instrument_id,
            instrument_version,
            product,
            tick_mono_ns,
        );
        out.rfq_id = rfq_id;
        out
    }

    /// Translate a validated [`tickwire_venue::CanonicalCommand`] directly into an inline
    /// [`OutboundIntent`] (attaching `command.bytes()` as `arena_payload` only for multi-leg list,
    /// algo, or pegged spot-advanced commands whose extra parameters exceed fixed inline slots).
    pub fn from_canonical_command(
        command: &tickwire_venue::CanonicalCommand,
        strat_id: u8,
        account_ref: u8,
        tick_mono_ns: u64,
    ) -> Result<Self> {
        if strat_id == ALL_STRATS {
            return Err(Error::Identity);
        }
        let ctx = &command.command().context;
        let inst = &ctx.instrument;
        let product = ctx.request.product;
        let opcode = match &command.command().body {
            CommandBody::New(_) | CommandBody::SpotList(_) | CommandBody::Algo(_) => {
                IntentOpcode::NewOrder
            }
            CommandBody::Cancel(_) => IntentOpcode::Cancel,
            CommandBody::CancelReplace { .. }
            | CommandBody::SpotAdvanced(SpotAdvancedCommand::CancelReplace { .. }) => {
                IntentOpcode::CancelReplace
            }
            CommandBody::AmendKeepPriority { .. } => IntentOpcode::AmendKeepPriority,
            CommandBody::Modify(_) => IntentOpcode::Modify,
            CommandBody::Query(_) => IntentOpcode::QueryOrder,
            CommandBody::CancelList(_) => IntentOpcode::CancelList,
            CommandBody::CancelAlgo(_) => IntentOpcode::CancelAlgo,
            CommandBody::SpotAdvanced(_) => IntentOpcode::NewOrder,
        };

        let mut out = Self::blank(
            opcode,
            strat_id,
            account_ref,
            inst.instrument_id,
            inst.instrument_version,
            Some(product),
            tick_mono_ns,
        );
        out.order_seqnum = ctx.request.intent_id;
        out.created_mono_ns = ctx.strategy.created_mono_ns;
        out.valid_until_mono_ns = ctx.request.deadline;
        out.config_version = ctx.request.config_revision;
        out.exec_generation = ctx.request.intent_exec_epoch;
        out.leg_id = u16::try_from(ctx.strategy.leg_id).map_err(|_| Error::Bounds)?;

        for t in ctx.triggers.as_slice() {
            out.triggers.push(TriggerRef {
                topic_id: t.topic_id,
                feed_generation: t.feed_generation,
                ring_epoch: t.ring_epoch,
                sequence: t.sequence,
                instrument_id: t.instrument_id,
                instrument_version: t.instrument_version,
                observed_mono_ns: t.observed_mono_ns,
                required_feed_state: 2,
                product: u16::from(product.code()),
                _pad_60: [0; 4],
            });
        }

        match &command.command().body {
            CommandBody::New(spec) => {
                populate_from_order_spec(&mut out, spec, inst)?;
            }
            CommandBody::Cancel(target) => {
                populate_from_target(&mut out, target)?;
            }
            CommandBody::CancelReplace {
                target,
                replacement,
                mode,
            } => {
                populate_from_target(&mut out, target)?;
                populate_from_order_spec(&mut out, replacement, inst)?;
                out.cancel_replace_mode = encode_cancel_replace_mode(*mode);
            }
            CommandBody::AmendKeepPriority {
                target,
                new_quantity,
                new_client_id,
            } => {
                populate_from_target(&mut out, target)?;
                out.qty_lots = ticks_from_decimal(*new_quantity, inst.quantity_step)?;
                if let Some(cid) = new_client_id {
                    write_padded_ascii(&mut out.client_id, &mut out.client_id_len, cid.as_str())?;
                }
            }
            CommandBody::Modify(m) => {
                populate_from_target(&mut out, &m.target)?;
                out.side = encode_side(m.side);
                out.position_side = encode_position_side(m.position_side);
                out.price_ticks = ticks_from_decimal(m.price, inst.price_tick)?;
                out.qty_lots = ticks_from_decimal(m.quantity, inst.quantity_step)?;
                if m.reduce_only {
                    out.intent_flags |= intent_flags::REDUCE_ONLY;
                }
                if let Some(cid) = &m.new_client_id {
                    write_padded_ascii(&mut out.client_id, &mut out.client_id_len, cid.as_str())?;
                }
            }
            CommandBody::Query(target) => {
                populate_from_target(&mut out, target)?;
            }
            CommandBody::SpotList(list) => {
                out.order_id = list.list_id;
                write_padded_ascii(
                    &mut out.client_id,
                    &mut out.client_id_len,
                    list.client_id.as_str(),
                )?;
                if let Some(first_leg) = list.legs.as_slice().first() {
                    populate_from_order_spec(&mut out, first_leg, inst)?;
                    write_padded_ascii(
                        &mut out.client_id,
                        &mut out.client_id_len,
                        list.client_id.as_str(),
                    )?;
                }
                out.arena_payload = Some(command.bytes().to_vec());
            }
            CommandBody::CancelList(target) => {
                out.order_id = target.list_id;
                if let Some(cid) = &target.client_id {
                    write_padded_ascii(
                        &mut out.orig_client_id,
                        &mut out.orig_client_id_len,
                        cid.as_str(),
                    )?;
                }
                if let Some(vid) = &target.venue_list_id {
                    write_padded_ascii(
                        &mut out.venue_order_id,
                        &mut out.venue_order_id_len,
                        vid.as_str(),
                    )?;
                }
            }
            CommandBody::Algo(algo) => {
                out.order_id = algo.parent_order_id;
                populate_from_order_spec(&mut out, &algo.order, inst)?;
                out.arena_payload = Some(command.bytes().to_vec());
            }
            CommandBody::CancelAlgo(target) => {
                out.order_id = target.parent_order_id;
                if let Some(cid) = &target.client_id {
                    write_padded_ascii(
                        &mut out.orig_client_id,
                        &mut out.orig_client_id_len,
                        cid.as_str(),
                    )?;
                }
                if let Some(vid) = &target.venue_algo_id {
                    write_padded_ascii(
                        &mut out.venue_order_id,
                        &mut out.venue_order_id_len,
                        vid.as_str(),
                    )?;
                }
            }
            CommandBody::SpotAdvanced(adv) => match adv {
                SpotAdvancedCommand::New(spot) => {
                    populate_from_order_spec(&mut out, &spot.order, inst)?;
                    if spot.peg.is_some() {
                        out.arena_payload = Some(command.bytes().to_vec());
                    }
                }
                SpotAdvancedCommand::CancelReplace {
                    target,
                    replacement,
                    mode,
                } => {
                    populate_from_target(&mut out, target)?;
                    populate_from_order_spec(&mut out, &replacement.order, inst)?;
                    out.cancel_replace_mode = encode_cancel_replace_mode(*mode);
                    if replacement.peg.is_some() {
                        out.arena_payload = Some(command.bytes().to_vec());
                    }
                }
                SpotAdvancedCommand::List(list) => {
                    out.order_id = list.list_id;
                    write_padded_ascii(
                        &mut out.client_id,
                        &mut out.client_id_len,
                        list.client_id.as_str(),
                    )?;
                    out.arena_payload = Some(command.bytes().to_vec());
                }
                SpotAdvancedCommand::SorNew(spec) => {
                    populate_from_order_spec(&mut out, spec, inst)?;
                    out.arena_payload = Some(command.bytes().to_vec());
                }
            },
        }
        Ok(out)
    }

    #[inline]
    pub fn with_order_seqnum(mut self, order_seqnum: u64) -> Self {
        self.order_seqnum = order_seqnum;
        self
    }

    #[inline]
    pub fn with_orig_order_seqnum(mut self, orig_order_seqnum: u64) -> Self {
        self.orig_order_seqnum = orig_order_seqnum;
        self
    }

    #[inline]
    pub fn with_tick_mono_ns(mut self, tick_mono_ns: u64) -> Self {
        self.tick_mono_ns = tick_mono_ns;
        self
    }

    #[inline]
    pub fn with_account_ref(mut self, account_ref: u8) -> Self {
        self.account_ref = account_ref;
        self
    }

    #[inline]
    pub fn with_strat_id(mut self, strat_id: u8) -> Self {
        self.strat_id = strat_id;
        self
    }

    #[inline]
    pub fn with_intent_flags(mut self, flags: u8) -> Self {
        self.intent_flags = flags;
        self
    }

    #[inline]
    pub fn with_time_in_force(mut self, tif: TimeInForce) -> Self {
        apply_tif(&mut self, Some(tif));
        self
    }

    #[inline]
    pub fn with_position_side(mut self, position_side: PositionSide) -> Self {
        self.position_side = encode_position_side(position_side);
        self
    }

    #[inline]
    pub fn with_stp_mode(mut self, stp: SelfTradePrevention) -> Self {
        self.stp_mode = encode_stp(stp);
        if !matches!(
            stp,
            SelfTradePrevention::Default | SelfTradePrevention::None
        ) {
            self.intent_flags |= intent_flags::CHECK_STP;
        }
        self
    }

    #[inline]
    pub fn with_order_id(mut self, order_id: [u8; 16]) -> Self {
        self.order_id = order_id;
        self
    }

    #[inline]
    pub fn with_valid_until_mono_ns(mut self, valid_until_mono_ns: u64) -> Self {
        self.valid_until_mono_ns = valid_until_mono_ns;
        self
    }

    pub fn with_triggers(mut self, triggers: Vec<TriggerRef>) -> Result<Self> {
        if triggers.len() > 8 {
            return Err(Error::Bounds);
        }
        self.triggers = triggers;
        Ok(self)
    }

    pub fn with_arena_payload(mut self, payload: Vec<u8>) -> Result<Self> {
        if payload.is_empty() || payload.len() > 65_536 {
            return Err(Error::Bounds);
        }
        self.arena_payload = Some(payload);
        Ok(self)
    }
}

#[forbid(unsafe_code)]
fn populate_from_order_spec(
    out: &mut OutboundIntent,
    spec: &OrderSpec,
    inst: &tickwire_venue::InstrumentBinding,
) -> Result<()> {
    out.side = encode_side(spec.side);
    out.order_type = encode_order_type(spec.order_type);
    apply_tif(out, spec.time_in_force);
    out.position_side = encode_position_side(spec.position_side);
    out.stp_mode = encode_stp(spec.self_trade_prevention);
    if !matches!(
        spec.self_trade_prevention,
        SelfTradePrevention::Default | SelfTradePrevention::None
    ) {
        out.intent_flags |= intent_flags::CHECK_STP;
    }
    if spec.reduce_only {
        out.intent_flags |= intent_flags::REDUCE_ONLY;
    }
    match spec.quantity {
        Quantity::Base(q) => {
            out.qty_lots = ticks_from_decimal(q, inst.quantity_step)?;
        }
        Quantity::Quote(q) => {
            let quote_unit = Decimal::new(1, inst.quote.exponent)?;
            out.quote_amount_units = ticks_from_decimal(q, quote_unit)?;
            out.intent_flags |= intent_flags::QUOTE_AMOUNT;
        }
        Quantity::ClosePosition => {
            out.intent_flags |= intent_flags::CLOSE_POSITION;
        }
    }
    if let Some(px) = spec.price {
        out.price_ticks = ticks_from_decimal(px, inst.price_tick)?;
    }
    if let Some(iceberg) = spec.iceberg_quantity {
        out.iceberg_qty_lots = ticks_from_decimal(iceberg, inst.quantity_step)?;
    }
    if let Some(trig) = spec.trigger {
        out.stop_price_ticks = ticks_from_decimal(trig.price, inst.price_tick)?;
        out.working_price = encode_working_price(trig.working_price);
        if trig.price_protect {
            out.intent_flags |= intent_flags::PRICE_PROTECT;
        }
    } else if let Some(trail) = spec.trailing {
        if let Some(act) = trail.activation_price {
            out.stop_price_ticks = ticks_from_decimal(act, inst.price_tick)?;
        }
        out.working_price = encode_working_price(trail.working_price);
    }
    write_padded_ascii(
        &mut out.client_id,
        &mut out.client_id_len,
        spec.client_id.as_str(),
    )?;
    Ok(())
}

#[forbid(unsafe_code)]
fn populate_from_target(out: &mut OutboundIntent, target: &OrderTarget) -> Result<()> {
    out.order_id = target.order_id;
    if let Some(cid) = &target.client_id {
        write_padded_ascii(
            &mut out.orig_client_id,
            &mut out.orig_client_id_len,
            cid.as_str(),
        )?;
    }
    if let Some(vid) = &target.venue_order_id {
        write_padded_ascii(
            &mut out.venue_order_id,
            &mut out.venue_order_id_len,
            vid.as_str(),
        )?;
    }
    Ok(())
}

#[forbid(unsafe_code)]
fn write_padded_ascii<const N: usize>(
    dst: &mut [u8; N],
    len_out: &mut u8,
    text: &str,
) -> Result<()> {
    if text.is_empty() || text.len() > N || !text.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(Error::Bounds);
    }
    dst.fill(0);
    dst[..text.len()].copy_from_slice(text.as_bytes());
    *len_out = text.len() as u8;
    Ok(())
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_side(side: Side) -> u8 {
    match side {
        Side::Buy => 1,
        Side::Sell => 2,
    }
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_position_side(ps: PositionSide) -> u8 {
    match ps {
        PositionSide::Both => 1,
        PositionSide::Long => 2,
        PositionSide::Short => 3,
    }
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_order_type(ot: OrderType) -> u8 {
    match ot {
        OrderType::Limit => 1,
        OrderType::Market => 2,
        OrderType::StopLimit => 3,
        OrderType::StopMarket => 4,
        OrderType::TakeProfitLimit => 5,
        OrderType::TakeProfitMarket => 6,
        OrderType::TrailingStop => 7,
    }
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_stp(stp: SelfTradePrevention) -> u8 {
    match stp {
        SelfTradePrevention::Default => 0,
        SelfTradePrevention::None => 1,
        SelfTradePrevention::ExpireTaker => 2,
        SelfTradePrevention::ExpireMaker => 3,
        SelfTradePrevention::ExpireBoth => 4,
        SelfTradePrevention::Decrement => 5,
    }
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_working_price(wp: WorkingPrice) -> u8 {
    match wp {
        WorkingPrice::Contract => 1,
        WorkingPrice::Mark => 2,
        WorkingPrice::Index => 3,
    }
}

#[inline]
#[forbid(unsafe_code)]
const fn encode_cancel_replace_mode(mode: CancelReplaceMode) -> u8 {
    match mode {
        CancelReplaceMode::StopOnFailure => 1,
        CancelReplaceMode::AllowFailure => 2,
    }
}

#[forbid(unsafe_code)]
fn apply_tif(out: &mut OutboundIntent, tif: Option<TimeInForce>) {
    out.expire_utc_ns = 0;
    match tif {
        None => {
            out.time_in_force = tif_code::NONE;
        }
        Some(TimeInForce::Gtc) => {
            out.time_in_force = tif_code::GTC;
        }
        Some(TimeInForce::Ioc) => {
            out.time_in_force = tif_code::IOC;
        }
        Some(TimeInForce::Fok) => {
            out.time_in_force = tif_code::FOK;
        }
        Some(TimeInForce::PostOnly) => {
            out.time_in_force = tif_code::POST_ONLY;
            out.intent_flags |= intent_flags::POST_ONLY;
        }
        Some(TimeInForce::Gtd { expire_utc_ns }) => {
            out.time_in_force = tif_code::GTD;
            out.expire_utc_ns = expire_utc_ns;
        }
        Some(TimeInForce::Rpi) => {
            out.time_in_force = tif_code::RPI;
        }
    }
}

/// Encode an [`OutboundIntent`] into a wire-validated 1,024-byte [`IntentRecord`].
#[allow(clippy::too_many_arguments)]
#[forbid(unsafe_code)]
pub fn encode_intent_record(
    intent: &OutboundIntent,
    sequence: u64,
    msg_seqnum: u64,
    order_seqnum: u64,
    instance_id: [u8; 16],
    strategy_generation: u64,
    risk_scope: u64,
    now_mono_ns: u64,
    arena_meta: Option<ArenaSliceMeta>,
) -> Result<IntentRecord> {
    if sequence == 0
        || sequence >= SEQUENCE_LIMIT
        || msg_seqnum == 0
        || msg_seqnum >= SEQUENCE_LIMIT
        || order_seqnum >= SEQUENCE_LIMIT
        || intent.orig_order_seqnum >= SEQUENCE_LIMIT
        || risk_scope == 0
        || strategy_generation == 0
        || strategy_generation >= SEQUENCE_LIMIT
        || instance_id == [0; 16]
        || intent.triggers.len() > 8
    {
        return Err(Error::Identity);
    }
    if intent.strat_id == ALL_STRATS
        && !matches!(
            intent.opcode,
            IntentOpcode::CancelAll | IntentOpcode::CancelAllRfqQuotes
        )
    {
        return Err(Error::Identity);
    }
    if intent.arena_payload.is_some() != arena_meta.is_some() {
        return Err(Error::Bounds);
    }

    let mut triggers: [TriggerRef; 8] = core::array::from_fn(|_| TriggerRef::default());
    for (dst, src) in triggers.iter_mut().zip(intent.triggers.iter()) {
        if src.topic_id == 0 || src.sequence == 0 || src.feed_generation == 0 {
            return Err(Error::Identity);
        }
        *dst = TriggerRef {
            _pad_60: [0; 4],
            ..src.clone()
        };
    }

    let created_mono_ns = if intent.created_mono_ns != 0 {
        intent.created_mono_ns
    } else {
        now_mono_ns
    };

    let (payload_kind, payload_offset, payload_length, arena_class, allocation_seq, payload_crc32c) =
        match arena_meta {
            None => (0u16, 0u64, 0u32, 0u16, 0u64, 0u32),
            Some(meta) => {
                if meta.allocation_seq != sequence {
                    return Err(Error::Identity);
                }
                (
                    2u16,
                    meta.payload_offset,
                    meta.payload_length,
                    meta.class_id,
                    meta.allocation_seq,
                    meta.payload_crc32c,
                )
            }
        };

    let mut body = IntentBody {
        msg_seqnum,
        order_seqnum,
        orig_order_seqnum: intent.orig_order_seqnum,
        tick_mono_ns: intent.tick_mono_ns,
        created_mono_ns,
        valid_until_mono_ns: intent.valid_until_mono_ns,
        expire_utc_ns: intent.expire_utc_ns,
        instrument_id: intent.instrument_id,
        strategy_generation,
        exec_generation: intent.exec_generation,
        config_version: intent.config_version,
        price_ticks: intent.price_ticks,
        qty_lots: intent.qty_lots,
        stop_price_ticks: intent.stop_price_ticks,
        quote_amount_units: intent.quote_amount_units,
        iceberg_qty_lots: intent.iceberg_qty_lots,
        rfq_ask_price_ticks: intent.rfq_ask_price_ticks,
        rfq_ask_qty_lots: intent.rfq_ask_qty_lots,
        rfq_id: intent.rfq_id,
        arena_offset: payload_offset,
        arena_len: payload_length,
        command_kind: intent.opcode.code(),
        trigger_count: intent.triggers.len() as u16,
        leg_id: intent.leg_id,
        strat_id: intent.strat_id,
        account_ref: intent.account_ref,
        side: intent.side,
        order_type: intent.order_type,
        time_in_force: intent.time_in_force,
        position_side: intent.position_side,
        stp_mode: intent.stp_mode,
        working_price: intent.working_price,
        intent_product: intent.intent_product,
        intent_flags: intent.intent_flags,
        cancel_replace_mode: intent.cancel_replace_mode,
        client_id_len: intent.client_id_len,
        orig_client_id_len: intent.orig_client_id_len,
        venue_order_id_len: intent.venue_order_id_len,
        client_id: intent.client_id,
        orig_client_id: intent.orig_client_id,
        venue_order_id: intent.venue_order_id,
        instance_id,
        order_id: intent.order_id,
        _pad_360: [0; 24],
        triggers,
    };
    sanitize_intent_body_for_opcode(&mut body, intent.opcode);

    let flags = {
        let mut f = if now_mono_ns != 0 { 4u32 } else { 0u32 };
        if cfg!(feature = "crc32") {
            f |= 8;
        }
        f
    };

    #[cfg_attr(not(feature = "crc32"), allow(unused_mut))]
    let mut record = IntentRecord {
        header: RecordHeader {
            sequence,
            message_type: intent.opcode.message_type(),
            schema_major: 1,
            schema_minor: 0,
            header_bytes: 128,
            flags,
            payload_kind,
            body_bytes: 896,
            payload_offset,
            payload_length,
            arena_class,
            _pad_38: [0; 2],
            allocation_seq,
            topic_or_scope_id: risk_scope,
            feed_generation: 0,
            producer_generation: strategy_generation,
            instrument_id: intent.instrument_id,
            instrument_version: intent.instrument_version,
            source_timestamp_ns: 0,
            ingress_mono_ns: 0,
            publish_mono_ns: now_mono_ns,
            record_crc32c: 0,
            payload_crc32c: if cfg!(feature = "crc32") {
                payload_crc32c
            } else {
                0
            },
            _pad_120: [0; 8],
        },
        body,
    };

    let mut raw = [0u8; IntentRecord::SIZE];
    record.encode(&mut raw)?;
    #[cfg(feature = "crc32")]
    {
        record.header.record_crc32c = tickwire_ipc::crc32c_masked(&raw, 112..120);
        raw[112..116].copy_from_slice(&record.header.record_crc32c.to_le_bytes());
    }
    tickwire_ipc::decode_record(&raw)?;
    Ok(record)
}

#[forbid(unsafe_code)]
fn sanitize_intent_body_for_opcode(b: &mut IntentBody, op: IntentOpcode) {
    match op {
        IntentOpcode::NewOrder => {
            b.orig_order_seqnum = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.cancel_replace_mode = 0;
            b.orig_client_id_len = 0;
            b.venue_order_id_len = 0;
            b.orig_client_id = [0; 40];
            b.venue_order_id = [0; 64];
        }
        IntentOpcode::Cancel => {
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.qty_lots = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::CancelReplace => {
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
        }
        IntentOpcode::AmendKeepPriority => {
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.side = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::Modify => {
            b.expire_utc_ns = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.order_type = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::QueryOrder | IntentOpcode::CancelList | IntentOpcode::CancelAlgo => {
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.qty_lots = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.side = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::CancelAll => {
            b.orig_order_seqnum = 0;
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.qty_lots = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.rfq_id = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
            b.client_id_len = 0;
            b.orig_client_id_len = 0;
            b.venue_order_id_len = 0;
            b.client_id = [0; 40];
            b.orig_client_id = [0; 40];
            b.venue_order_id = [0; 64];
            b.order_id = [0; 16];
        }
        IntentOpcode::NewRfqQuote => {
            b.orig_order_seqnum = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.order_type = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
            b.orig_client_id_len = 0;
            b.venue_order_id_len = 0;
            b.orig_client_id = [0; 40];
            b.venue_order_id = [0; 64];
        }
        IntentOpcode::ModifyRfqQuote => {
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.order_type = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::CancelRfqQuote => {
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.qty_lots = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.side = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
        }
        IntentOpcode::CancelAllRfqQuotes => {
            b.orig_order_seqnum = 0;
            b.expire_utc_ns = 0;
            b.price_ticks = 0;
            b.qty_lots = 0;
            b.stop_price_ticks = 0;
            b.quote_amount_units = 0;
            b.iceberg_qty_lots = 0;
            b.rfq_ask_price_ticks = 0;
            b.rfq_ask_qty_lots = 0;
            b.side = 0;
            b.order_type = 0;
            b.time_in_force = 0;
            b.position_side = 0;
            b.stp_mode = 0;
            b.working_price = 0;
            b.cancel_replace_mode = 0;
            b.client_id_len = 0;
            b.orig_client_id_len = 0;
            b.venue_order_id_len = 0;
            b.client_id = [0; 40];
            b.orig_client_id = [0; 40];
            b.venue_order_id = [0; 64];
            b.order_id = [0; 16];
        }
    }
}

/// Receipt returned by [`IntentWriter::publish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedIntent {
    pub ring_sequence: u64,
    pub msg_seqnum: u64,
    pub order_seqnum: u64,
    pub account_ref: u8,
}

/// Strategy-side SPSC writer encoding outbound intents directly into 1,024-byte `IntentRecord`
/// SHM ring slots (with optional in-memory `ShmArenaWriter` strictly for oversize payloads).
pub struct IntentWriter {
    publisher: Publisher<1>,
    subscription: Option<SubscriptionKey>,
    arena: Option<ShmArenaWriter>,
    strat_id: u8,
    instance_id: [u8; 16],
    strategy_generation: u64,
    next_msg_seqnum: u64,
    next_order_seqnum: u64,
}

impl IntentWriter {
    #[forbid(unsafe_code)]
    pub fn new(
        publisher: Publisher<1>,
        strat_id: u8,
        instance_id: [u8; 16],
        strategy_generation: u64,
        next_order_seqnum: u64,
    ) -> Result<Self> {
        Self::with_arena(
            publisher,
            None,
            None,
            strat_id,
            instance_id,
            strategy_generation,
            next_order_seqnum,
        )
    }

    #[forbid(unsafe_code)]
    pub fn with_arena(
        publisher: Publisher<1>,
        subscription: Option<SubscriptionKey>,
        arena: Option<ShmArenaWriter>,
        strat_id: u8,
        instance_id: [u8; 16],
        strategy_generation: u64,
        next_order_seqnum: u64,
    ) -> Result<Self> {
        let h = publisher.header();
        if strat_id == ALL_STRATS
            || instance_id == [0; 16]
            || strategy_generation == 0
            || strategy_generation >= SEQUENCE_LIMIT
            || next_order_seqnum == 0
            || next_order_seqnum >= SEQUENCE_LIMIT
            || h.resource_kind != 2
            || h.record_bytes != IntentRecord::SIZE as u32
            || h.required_features & FEATURE == 0
            || h.risk_scope == 0
            || h.producer_instance != instance_id
            || h.producer_generation != strategy_generation
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            publisher,
            subscription,
            arena,
            strat_id,
            instance_id,
            strategy_generation,
            next_msg_seqnum: 1,
            next_order_seqnum,
        })
    }

    /// Adopt `strat_id`, `next_order_seqnum = echo.next_source_order_seqnum.max(1)`, and reset
    /// `next_msg_seqnum = 1` from a freshly logged-in [`DopSession`].
    #[forbid(unsafe_code)]
    pub fn sync_from_dop(&mut self, dop: &DopSession) -> Result<()> {
        if dop.strat_id() == ALL_STRATS {
            return Err(Error::Identity);
        }
        self.strat_id = dop.strat_id();
        self.next_order_seqnum = dop.next_order_seqnum().max(1);
        self.next_msg_seqnum = dop.next_msg_seqnum().max(1);
        Ok(())
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn publisher(&mut self) -> &mut Publisher<1> {
        &mut self.publisher
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        self.publisher.header()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn strat_id(&self) -> u8 {
        self.strat_id
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn next_msg_seqnum(&self) -> u64 {
        self.next_msg_seqnum
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn next_order_seqnum(&self) -> u64 {
        self.next_order_seqnum
    }

    /// Validate `account_ref` against `dop`, check local quote-age staleness against
    /// `dop.max_order_age_ns()` when `tick_mono_ns > 0`, publish onto the SHM ring, and keep
    /// `dop` sequence watermarks synchronized.
    #[forbid(unsafe_code)]
    pub fn publish_with_dop(
        &mut self,
        dop: &mut DopSession,
        intent: &OutboundIntent,
        now_mono_ns: u64,
    ) -> Result<PublishedIntent> {
        if !dop.ready_to_trade() {
            return Err(Error::NotReady);
        }
        let resolved_account = dop.resolve_account_ref(intent.account_ref)?;
        if intent.tick_mono_ns != 0
            && now_mono_ns.saturating_sub(intent.tick_mono_ns) > dop.max_order_age_ns()
        {
            return Err(Error::Stale);
        }
        let mut routed = intent.clone();
        routed.account_ref = resolved_account;
        let receipt = self.publish(&routed, now_mono_ns)?;
        dop.observe_published_sequences(self.next_msg_seqnum, self.next_order_seqnum);
        Ok(receipt)
    }

    /// Encode `intent` into a 1,024-byte `IntentRecord` and publish it onto the SHM ring with
    /// zero disk writes.
    #[forbid(unsafe_code)]
    pub fn publish(
        &mut self,
        intent: &OutboundIntent,
        now_mono_ns: u64,
    ) -> Result<PublishedIntent> {
        if intent.strat_id != self.strat_id
            && !(intent.strat_id == ALL_STRATS
                && matches!(
                    intent.opcode,
                    IntentOpcode::CancelAll | IntentOpcode::CancelAllRfqQuotes
                ))
        {
            return Err(Error::Identity);
        }
        if intent.valid_until_mono_ns != 0 && now_mono_ns >= intent.valid_until_mono_ns {
            return Err(Error::Stale);
        }

        let sequence = self
            .publisher
            .published()
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;
        let msg_seqnum = self.next_msg_seqnum;
        let next_msg = msg_seqnum
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;

        let (order_seqnum, next_order) = if intent.order_seqnum != 0 {
            if intent.order_seqnum < self.next_order_seqnum {
                return Err(Error::Sequence);
            }
            let next = intent
                .order_seqnum
                .checked_add(1)
                .filter(|&v| v < SEQUENCE_LIMIT)
                .ok_or(Error::Sequence)?;
            (intent.order_seqnum, next)
        } else if intent.opcode.claims_order_seqnum() {
            let claimed = self.next_order_seqnum;
            let next = claimed
                .checked_add(1)
                .filter(|&v| v < SEQUENCE_LIMIT)
                .ok_or(Error::Sequence)?;
            (claimed, next)
        } else {
            (0, self.next_order_seqnum)
        };

        let arena_meta = match &intent.arena_payload {
            None => None,
            Some(payload) => {
                let released = match self.subscription {
                    Some(sub) => self.publisher.cursor_watermarks(sub)?.released,
                    None => 0,
                };
                let arena = self.arena.as_mut().ok_or(Error::NotReady)?;
                Some(arena.write_payload(sequence, released, payload)?)
            }
        };

        let record = encode_intent_record(
            intent,
            sequence,
            msg_seqnum,
            order_seqnum,
            self.instance_id,
            self.strategy_generation,
            self.publisher.header().risk_scope,
            now_mono_ns,
            arena_meta,
        )?;
        let mut image = [0u8; IntentRecord::SIZE];
        record.encode(&mut image)?;
        let published_seq = self.publisher.publish(&[&image])?;
        self.next_msg_seqnum = next_msg;
        self.next_order_seqnum = next_order;
        Ok(PublishedIntent {
            ring_sequence: published_seq,
            msg_seqnum,
            order_seqnum,
            account_ref: intent.account_ref,
        })
    }
}

/// Zero-copy SPSC reader for 1,024-byte `IntentRecord` SHM slots (used by ORS `REQ` and SDK tests).
pub struct IntentReader {
    reader: Subscriber<4>,
    bank: ResourceHeader,
    arena: Option<ShmArenaReader>,
    last_msg_seqnum: u64,
}

impl IntentReader {
    /// Attach to an authenticated `F_SEAL_FUTURE_WRITE` intent ring `memfd`.
    ///
    /// # Safety
    /// Peer writer must be the sole `IntentWriter` obeying the `Publisher<1>` release protocol.
    #[allow(unsafe_code)]
    pub unsafe fn attach(
        fd: OwnedFd,
        bank: ResourceHeader,
        subscription: SubscriptionKey,
        arena: Option<ShmArenaReader>,
    ) -> Result<Self> {
        if bank.resource_kind != 2
            || bank.record_bytes != IntentRecord::SIZE as u32
            || bank.required_features & FEATURE == 0
            || bank.risk_scope == 0
        {
            return Err(Error::Identity);
        }
        let first = bank.first_seq.checked_sub(1).ok_or(Error::Bounds)?;
        // SAFETY: caller guarantees the sole writer and exact registered subscription cursor.
        let reader = unsafe { Subscriber::attach(fd, bank.clone(), subscription, first)? };
        Ok(Self {
            reader,
            bank,
            arena,
            last_msg_seqnum: 0,
        })
    }

    #[forbid(unsafe_code)]
    pub fn cursor_fd(&self) -> Result<OwnedFd> {
        Ok(self.reader.cursor_fd()?)
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        &self.bank
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn last_msg_seqnum(&self) -> u64 {
        self.last_msg_seqnum
    }

    #[forbid(unsafe_code)]
    pub fn reset_msg_seqnum(&mut self) {
        self.last_msg_seqnum = 0;
    }

    /// Borrow and validate the next 1,024-byte `IntentRecord` in place (`IntentRecord::from_aligned_slice`)
    /// without heap allocation, passing an optional borrowed arena payload slice while the slot guard is held.
    #[allow(unsafe_code)]
    pub fn poll_zero_copy<F>(&mut self, mut f: F) -> Result<bool>
    where
        F: FnMut(&IntentRecord, Option<&[u8]>) -> Result<()>,
    {
        let Some(guard) = self.reader.borrow()? else {
            return Ok(false);
        };
        let record = IntentRecord::from_aligned_slice(guard.bytes())?;
        if record.header.topic_or_scope_id != self.bank.risk_scope
            || record.header.producer_generation != self.bank.producer_generation
            || record.body.instance_id != self.bank.producer_instance
            || record.body.strategy_generation != self.bank.producer_generation
        {
            return Err(Error::Identity);
        }
        if record.body.msg_seqnum != self.last_msg_seqnum.checked_add(1).ok_or(Error::Sequence)? {
            return Err(Error::Sequence);
        }
        let payload_slice = if record.body.arena_len > 0 {
            let arena = self.arena.as_ref().ok_or(Error::NotReady)?;
            // SAFETY: `guard` holds the live ring borrow for `record.header.sequence`, preventing
            // `ShmArenaWriter` from reclaiming or mutating this arena block until `guard` drops.
            Some(unsafe { arena.read_retained_payload(&record.header)? })
        } else {
            None
        };
        f(record, payload_slice)?;
        let seq = guard.sequence();
        let msg_seq = record.body.msg_seqnum;
        drop(guard);
        self.reader.apply(seq)?;
        self.last_msg_seqnum = msg_seq;
        Ok(true)
    }

    /// Poll one `IntentRecord` (copying an optional oversize arena payload into a `Vec<u8>`).
    #[forbid(unsafe_code)]
    pub fn poll(&mut self) -> Result<Option<(IntentRecord, Option<Vec<u8>>)>> {
        let mut out = None;
        self.poll_zero_copy(|record, payload| {
            out = Some((record.clone(), payload.map(<[u8]>::to_vec)));
            Ok(())
        })?;
        Ok(out)
    }
}

/// Create an in-process `(IntentWriter, IntentReader)` pair over a sealed `memfd` ring.
#[allow(unsafe_code)]
pub fn in_process_intent_pair(
    bank_header: ResourceHeader,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    strat_id: u8,
    next_order_seqnum: u64,
) -> Result<(IntentWriter, IntentReader)> {
    in_process_intent_pair_with_arena(
        bank_header,
        None,
        subscription_generation,
        cursor_id,
        strat_id,
        next_order_seqnum,
    )
}

/// Create an in-process `(IntentWriter, IntentReader)` pair with an optional sealed `ShmArena`.
#[allow(unsafe_code)]
pub fn in_process_intent_pair_with_arena(
    bank_header: ResourceHeader,
    arena_header: Option<ResourceHeader>,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    strat_id: u8,
    next_order_seqnum: u64,
) -> Result<(IntentWriter, IntentReader)> {
    if cursor_id == [0; 16] || subscription_generation == 0 {
        return Err(Error::Identity);
    }
    let producer_instance = bank_header.producer_instance;
    let producer_generation = bank_header.producer_generation;
    let consumer_instance = bank_header.consumer_instance;
    let mut publisher = Publisher::<1>::create(bank_header)?;
    let sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(consumer_instance),
        generation: subscription_generation,
        cursor: u128::from_le_bytes(cursor_id),
    };
    publisher.join(sub)?;
    let (arena_writer, arena_reader) = match arena_header {
        None => (None, None),
        Some(ah) => {
            let (w, r) = in_process_arena_pair(ah)?;
            (Some(w), Some(r))
        }
    };
    let bank = publisher.header().clone();
    let bank_fd = publisher.reader_fd()?;
    // SAFETY: `publisher` is freshly created and exclusively owned by `IntentWriter`.
    let reader = unsafe { IntentReader::attach(bank_fd, bank, sub, arena_reader)? };
    let cursor_fd = reader.cursor_fd()?;
    // SAFETY: `cursor_fd` is the exact `Subscriber` cursor descriptor for `sub`.
    unsafe {
        publisher.register_cursor(sub, cursor_fd)?;
    }
    let writer = IntentWriter::with_arena(
        publisher,
        Some(sub),
        arena_writer,
        strat_id,
        producer_instance,
        producer_generation,
        next_order_seqnum,
    )?;
    Ok((writer, reader))
}
