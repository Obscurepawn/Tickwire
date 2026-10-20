use crate::{
    Error, Result,
    schema::{
        self, best_bid_ask_stream_event as quote, depth_diff_stream_event as diff,
        depth_snapshot_stream_event as partial, trades_stream_event as trades,
    },
};

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DEPTH_LEVELS: usize = 5000;
pub const MAX_TRADES: usize = 65_536;
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_depth_levels: usize,
    pub max_trades: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_depth_levels: 5000,
            max_trades: 4096,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        if self.max_frame_bytes == 0
            || self.max_frame_bytes > MAX_FRAME_BYTES
            || self.max_depth_levels == 0
            || self.max_depth_levels > MAX_DEPTH_LEVELS
            || self.max_trades == 0
            || self.max_trades > MAX_TRADES
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Trades,
    BestBidAsk,
    PartialDepth,
    DepthDiff,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelView {
    pub price_mantissa: i64,
    pub quantity_mantissa: i64,
}
#[derive(Clone, Copy, Debug)]
pub struct Levels<'a> {
    raw: &'a [u8],
}
impl<'a> Levels<'a> {
    pub fn len(self) -> usize {
        self.raw.len() / 16
    }
    pub fn is_empty(self) -> bool {
        self.raw.is_empty()
    }
    pub fn iter(self) -> impl ExactSizeIterator<Item = LevelView> + 'a {
        self.raw.as_chunks::<16>().0.iter().map(|row| LevelView {
            price_mantissa: i64::from_le_bytes(core::array::from_fn(|i| row[i])),
            quantity_mantissa: i64::from_le_bytes(core::array::from_fn(|i| row[8 + i])),
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub id: u64,
    pub price_mantissa: i64,
    pub quantity_mantissa: i64,
    pub is_buyer_maker: bool,
}
impl Trade {
    pub fn is_best_match(self) -> bool {
        true
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Trades<'a> {
    raw: &'a [u8],
}
impl<'a> Trades<'a> {
    pub fn len(self) -> usize {
        self.raw.len() / 25
    }
    pub fn is_empty(self) -> bool {
        self.raw.is_empty()
    }
    pub fn iter(self) -> impl ExactSizeIterator<Item = Trade> + 'a {
        self.raw.as_chunks::<25>().0.iter().map(|row| Trade {
            id: u64::from_le_bytes(core::array::from_fn(|i| row[i])),
            price_mantissa: i64::from_le_bytes(core::array::from_fn(|i| row[8 + i])),
            quantity_mantissa: i64::from_le_bytes(core::array::from_fn(|i| row[16 + i])),
            is_buyer_maker: row[24] == 1,
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub bid: LevelView,
    pub ask: LevelView,
}
/// Returned only after the entire frame, every group row and the trailing symbol validate.
#[derive(Clone, Copy, Debug)]
pub struct Frame<'a> {
    kind: Kind,
    symbol: &'a str,
    event_ns: u64,
    transaction_ns: Option<u64>,
    price_exponent: i8,
    quantity_exponent: i8,
    first: Option<u64>,
    last: Option<u64>,
    bids: Option<Levels<'a>>,
    asks: Option<Levels<'a>>,
    trades: Option<Trades<'a>>,
    quote: Option<Quote>,
}
impl<'a> Frame<'a> {
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn symbol(&self) -> &'a str {
        self.symbol
    }
    pub fn event_time_ns(&self) -> u64 {
        self.event_ns
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.transaction_ns
    }
    pub fn price_exponent(&self) -> i8 {
        self.price_exponent
    }
    pub fn quantity_exponent(&self) -> i8 {
        self.quantity_exponent
    }
    pub fn first_update_id(&self) -> Option<u64> {
        self.first
    }
    pub fn last_update_id(&self) -> Option<u64> {
        self.last
    }
    pub fn bids(&self) -> Option<Levels<'a>> {
        self.bids
    }
    pub fn asks(&self) -> Option<Levels<'a>> {
        self.asks
    }
    pub fn trades(&self) -> Option<Trades<'a>> {
        self.trades
    }
    pub fn quote(&self) -> Option<Quote> {
        self.quote
    }
}
fn array<const N: usize>(raw: &[u8], offset: usize) -> Result<[u8; N]> {
    raw.get(offset..offset.checked_add(N).ok_or(Error::Bounds)?)
        .ok_or(Error::Bounds)?
        .try_into()
        .map_err(|_| Error::Bounds)
}
fn i64_at(raw: &[u8], offset: usize) -> Result<i64> {
    Ok(i64::from_le_bytes(array(raw, offset)?))
}
fn id(raw: &[u8], offset: usize) -> Result<u64> {
    u64::try_from(i64_at(raw, offset)?).map_err(|_| Error::Value)
}
fn time(raw: &[u8], offset: usize) -> Result<u64> {
    id(raw, offset)?.checked_mul(1000).ok_or(Error::Overflow)
}
fn exponent(raw: &[u8], offset: usize) -> Result<i8> {
    let n = *raw.get(offset).ok_or(Error::Bounds)? as i8;
    if n == i8::MIN {
        return Err(Error::Value);
    }
    Ok(n)
}
struct Cursor<'a> {
    raw: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(n).ok_or(Error::Bounds)?;
        let value = self.raw.get(self.offset..end).ok_or(Error::Bounds)?;
        self.offset = end;
        Ok(value)
    }
    fn group(&mut self, block: usize, header: usize, maximum: usize) -> Result<&'a [u8]> {
        let dimensions = self.take(header)?;
        if usize::from(u16::from_le_bytes(array(dimensions, 0)?)) != block {
            return Err(Error::BlockLength);
        }
        let count = if header == 6 {
            u32::from_le_bytes(array(dimensions, 2)?) as usize
        } else {
            usize::from(u16::from_le_bytes(array(dimensions, 2)?))
        };
        if count > maximum {
            return Err(Error::Capacity);
        }
        self.take(count.checked_mul(block).ok_or(Error::Capacity)?)
    }
    fn symbol(&mut self) -> Result<&'a str> {
        let n = usize::from(self.take(1)?[0]);
        let symbol = core::str::from_utf8(self.take(n)?).map_err(|_| Error::Utf8)?;
        if symbol.is_empty() || symbol.chars().any(char::is_control) {
            return Err(Error::Value);
        }
        if self.offset != self.raw.len() {
            return Err(Error::Bounds);
        }
        Ok(symbol)
    }
}
fn levels(raw: &[u8], snapshot: bool) -> Result<Levels<'_>> {
    let levels = Levels { raw };
    for row in levels.iter() {
        if row.price_mantissa <= 0
            || row.quantity_mantissa < 0
            || (snapshot && row.quantity_mantissa == 0)
        {
            return Err(Error::Value);
        }
    }
    Ok(levels)
}
pub fn decode_frame(bytes: &[u8], limits: Limits) -> Result<Frame<'_>> {
    limits.validate()?;
    if bytes.len() > limits.max_frame_bytes {
        return Err(Error::Capacity);
    }
    let mut cursor = Cursor {
        raw: bytes,
        offset: 0,
    };
    let h = cursor.take(schema::MESSAGE_HEADER_BYTES)?;
    if u16::from_le_bytes(array(h, 4)?) != schema::SCHEMA_ID {
        return Err(Error::Schema);
    }
    if u16::from_le_bytes(array(h, 6)?) != schema::VERSION {
        return Err(Error::Version);
    }
    let template = u16::from_le_bytes(array(h, 2)?);
    let (kind, block) = match template {
        trades::TEMPLATE_ID => (Kind::Trades, trades::BLOCK_LENGTH),
        quote::TEMPLATE_ID => (Kind::BestBidAsk, quote::BLOCK_LENGTH),
        partial::TEMPLATE_ID => (Kind::PartialDepth, partial::BLOCK_LENGTH),
        diff::TEMPLATE_ID => (Kind::DepthDiff, diff::BLOCK_LENGTH),
        _ => return Err(Error::Template),
    };
    if usize::from(u16::from_le_bytes(array(h, 0)?)) != block {
        return Err(Error::BlockLength);
    }
    let root = cursor.take(block)?;
    let (p, q) = match kind {
        Kind::DepthDiff => (diff::PRICE_EXPONENT, diff::QTY_EXPONENT),
        _ => (16, 17),
    };
    let mut frame = Frame {
        kind,
        symbol: "",
        event_ns: time(root, 0)?,
        transaction_ns: None,
        price_exponent: exponent(root, p)?,
        quantity_exponent: exponent(root, q)?,
        first: None,
        last: None,
        bids: None,
        asks: None,
        trades: None,
        quote: None,
    };
    match kind {
        Kind::Trades => {
            frame.transaction_ns = Some(time(root, trades::TRANSACT_TIME)?);
            let raw = cursor.group(
                trades::trades::BLOCK_LENGTH,
                trades::trades::HEADER_BYTES,
                limits.max_trades,
            )?;
            for row in raw.as_chunks::<25>().0 {
                id(row, trades::trades::ID)?;
                if i64_at(row, trades::trades::PRICE)? <= 0
                    || i64_at(row, trades::trades::QTY)? <= 0
                    || row[trades::trades::IS_BUYER_MAKER] > 1
                {
                    return Err(Error::Value);
                }
            }
            frame.trades = Some(Trades { raw });
        }
        Kind::BestBidAsk => {
            frame.last = Some(id(root, quote::BOOK_UPDATE_ID)?);
            let value = Quote {
                bid: LevelView {
                    price_mantissa: i64_at(root, quote::BID_PRICE)?,
                    quantity_mantissa: i64_at(root, quote::BID_QTY)?,
                },
                ask: LevelView {
                    price_mantissa: i64_at(root, quote::ASK_PRICE)?,
                    quantity_mantissa: i64_at(root, quote::ASK_QTY)?,
                },
            };
            if value.bid.price_mantissa <= 0
                || value.ask.price_mantissa <= 0
                || value.bid.quantity_mantissa < 0
                || value.ask.quantity_mantissa < 0
            {
                return Err(Error::Value);
            }
            frame.quote = Some(value);
        }
        Kind::PartialDepth | Kind::DepthDiff => {
            let snapshot = kind == Kind::PartialDepth;
            let maximum = if snapshot {
                limits.max_depth_levels.min(20)
            } else {
                limits.max_depth_levels
            };
            if snapshot {
                frame.last = Some(id(root, partial::BOOK_UPDATE_ID)?);
            } else {
                let first = id(root, diff::FIRST_BOOK_UPDATE_ID)?;
                let last = id(root, diff::LAST_BOOK_UPDATE_ID)?;
                if first > last {
                    return Err(Error::Value);
                }
                frame.first = Some(first);
                frame.last = Some(last);
            }
            frame.bids = Some(levels(cursor.group(16, 4, maximum)?, snapshot)?);
            frame.asks = Some(levels(cursor.group(16, 4, maximum)?, snapshot)?);
        }
    }
    frame.symbol = cursor.symbol()?;
    Ok(frame)
}
