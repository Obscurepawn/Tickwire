use super::*;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};

pub(super) fn decode<'a, const N: usize>(
    session: &SessionContext,
    bytes: &'a [u8],
    max_bytes: usize,
    received: u64,
) -> Result<PrivateEvent<'a>> {
    if N == 0 || N > 4096 || max_bytes == 0 || max_bytes > 1024 * 1024 || bytes.len() > max_bytes {
        return Err(Error::Capacity);
    }
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(rename = "subscriptionId")]
        subscription: Option<u64>,
        #[serde(borrow)]
        event: Option<&'a RawValue>,
    }
    let envelope: Envelope<'a> = json(bytes)?;
    let raw: &'a RawValue = match session.stream {
        PrivateStream::SpotSubscription { subscription_id } => {
            if envelope.subscription != Some(subscription_id) {
                return Err(Error::IdentityMismatch);
            }
            envelope.event.ok_or(Error::InvalidResponse)?
        }
        PrivateStream::UsdmListenKey => {
            if envelope.subscription.is_some() || envelope.event.is_some() {
                return Err(Error::IdentityMismatch);
            }
            json(bytes)?
        }
    };
    #[derive(Deserialize)]
    struct Header<'a> {
        #[serde(borrow)]
        e: Cow<'a, str>,
        #[serde(rename = "E")]
        event_time: u64,
        #[serde(rename = "T")]
        transaction_time: Option<u64>,
        st: Option<u8>,
    }
    let h: Header<'a> = json(raw.get().as_bytes())?;
    text(&h.e, 64, false)?;
    if h.st.is_some_and(|n| n != 1) {
        return Err(Error::IdentityMismatch);
    }
    let time = |t| session.unit.nanoseconds(t);
    let mut transaction = h.transaction_time.map(time).transpose()?;
    let body = match (session.market, h.e.as_ref()) {
        (Market::Spot, "executionReport") | (Market::Usdm, "ORDER_TRADE_UPDATE") => {
            let order_raw = if session.market == Market::Usdm {
                #[derive(Deserialize)]
                struct Nested<'a> {
                    #[serde(borrow)]
                    o: &'a RawValue,
                }
                if transaction.is_none() {
                    return Err(Error::InvalidResponse);
                }
                json::<Nested<'a>>(raw.get().as_bytes())?.o
            } else {
                raw
            };
            let w: WireOrder<'a> = json(order_raw.get().as_bytes())?;
            if w.symbol_type.is_some_and(|n| n != 1) {
                return Err(Error::IdentityMismatch);
            }
            for s in [
                &w.symbol,
                &w.client_id,
                &w.side,
                &w.order_type,
                &w.tif,
                &w.execution,
                &w.status,
            ] {
                text(s, 64, false)?;
            }
            if let Some(s) = &w.original_client {
                text(s, 64, true)?;
            }
            if let Some(s) = &w.commission_asset {
                text(s, 32, false)?;
            }
            if let Some(s) = &w.position_side {
                text(s, 32, false)?;
            }
            if let Some(s) = &w.reject {
                text(s, 256, true)?;
            }
            let order_time_ns = time(w.order_time)?;
            if session.market == Market::Spot {
                transaction = Some(order_time_ns);
            }
            if session.market == Market::Usdm
                && (w.position_side.is_none() || w.realized_pnl.is_none())
            {
                return Err(Error::InvalidResponse);
            }
            for d in [
                w.quantity,
                w.price,
                w.cumulative,
                w.last_quantity,
                w.last_price,
            ] {
                nonnegative(d)?;
            }
            for d in [w.cumulative_quote, w.last_quote].into_iter().flatten() {
                nonnegative(d)?;
            }
            if w.commission.is_some_and(|n| n.mantissa() != 0) && w.commission_asset.is_none() {
                return Err(Error::InvalidResponse);
            }
            PrivateBody::Order(Box::new(OrderUpdate {
                symbol: w.symbol,
                client_id: w.client_id,
                original_client_id: w.original_client.filter(|s| !s.is_empty()),
                order_id: w.id,
                list_id: w.list.map(native_id).transpose()?.flatten(),
                execution_id: w.execution_id,
                execution_type: w.execution,
                status: w.status,
                side: w.side,
                position_side: w.position_side,
                order_type: w.order_type,
                time_in_force: w.tif,
                original_quantity: w.quantity,
                price: w.price,
                cumulative_quantity: w.cumulative,
                cumulative_quote: w.cumulative_quote,
                last_quantity: w.last_quantity,
                last_price: w.last_price,
                last_quote: w.last_quote,
                commission: w.commission,
                commission_asset: w.commission_asset,
                realized_pnl: w.realized_pnl,
                trade_id: native_id(w.trade_id)?,
                maker: w.maker,
                order_time_ns,
                reject_reason: w.reject,
                raw: order_raw,
            }))
        }
        (Market::Spot, "outboundAccountPosition") => {
            #[derive(Deserialize)]
            struct Balances<'a> {
                u: u64,
                #[serde(rename = "B", borrow)]
                balances: &'a RawValue,
            }
            let w: Balances<'a> = json(raw.get().as_bytes())?;
            let balances: Vec<SpotBalance<'a>> = rows(w.balances, N)?;
            for b in &balances {
                text(&b.asset, 32, false)?;
                nonnegative(b.free)?;
                nonnegative(b.locked)?;
            }
            unique(balances.iter().map(|b| b.asset.as_ref()))?;
            PrivateBody::SpotBalances {
                last_update_ns: time(w.u)?,
                balances,
            }
        }
        (Market::Spot, "balanceUpdate" | "externalLockUpdate") => {
            #[derive(Deserialize)]
            struct Delta<'a> {
                #[serde(borrow)]
                a: Cow<'a, str>,
                d: Decimal,
                #[serde(rename = "T")]
                t: u64,
            }
            let w: Delta<'a> = json(raw.get().as_bytes())?;
            text(&w.a, 32, false)?;
            PrivateBody::BalanceDelta {
                asset: w.a,
                delta: w.d,
                clear_time_ns: time(w.t)?,
                external_lock: h.e == "externalLockUpdate",
            }
        }
        (Market::Usdm, "ACCOUNT_UPDATE") => {
            #[derive(Deserialize)]
            struct Account<'a> {
                #[serde(borrow)]
                a: Update<'a>,
            }
            #[derive(Deserialize)]
            struct Update<'a> {
                #[serde(borrow)]
                m: Cow<'a, str>,
                #[serde(rename = "B", borrow)]
                balances: Option<&'a RawValue>,
                #[serde(rename = "P", borrow)]
                positions: Option<&'a RawValue>,
                #[serde(rename = "S", borrow)]
                symbol: Option<Cow<'a, str>>,
            }
            let w = json::<Account<'a>>(raw.get().as_bytes())?.a;
            if transaction.is_none() {
                return Err(Error::InvalidResponse);
            }
            text(&w.m, 64, false)?;
            if let Some(s) = &w.symbol {
                text(s, 64, false)?;
            }
            if w.balances.is_none() && w.positions.is_none() {
                return Err(Error::InvalidResponse);
            }
            let balances: Vec<FuturesBalance<'a>> = w
                .balances
                .map(|raw| rows(raw, N))
                .transpose()?
                .unwrap_or_default();
            let positions: Vec<FuturesPosition<'a>> = w
                .positions
                .map(|raw| rows(raw, N - balances.len()))
                .transpose()?
                .unwrap_or_default();
            for b in &balances {
                text(&b.asset, 32, false)?;
            }
            unique(balances.iter().map(|b| b.asset.as_ref()))?;
            for (i, p) in positions.iter().enumerate() {
                text(&p.symbol, 64, false)?;
                text(&p.position_side, 32, false)?;
                text(&p.margin_type, 32, false)?;
                nonnegative(p.entry_price)?;
                if let Some(b) = p.break_even_price {
                    nonnegative(b)?;
                }
                if positions[..i]
                    .iter()
                    .any(|q| q.symbol == p.symbol && q.position_side == p.position_side)
                {
                    return Err(Error::InvalidResponse);
                }
            }
            PrivateBody::UsdmAccount {
                reason: w.m,
                funding_symbol: w.symbol,
                balances,
                positions,
            }
        }
        (Market::Usdm, "ACCOUNT_CONFIG_UPDATE") => {
            #[derive(Deserialize)]
            struct Config<'a> {
                #[serde(borrow)]
                ac: Option<Leverage<'a>>,
                ai: Option<Multi>,
            }
            #[derive(Deserialize)]
            struct Multi {
                j: bool,
            }
            let w: Config<'a> = json(raw.get().as_bytes())?;
            if transaction.is_none() || (w.ac.is_none() && w.ai.is_none()) {
                return Err(Error::InvalidResponse);
            }
            if let Some(l) = &w.ac {
                text(&l.symbol, 64, false)?;
                if l.leverage == 0 {
                    return Err(Error::InvalidResponse);
                }
            }
            PrivateBody::AccountConfig {
                leverage: w.ac,
                multi_assets: w.ai.map(|m| m.j),
            }
        }
        (Market::Spot, "listStatus") => {
            #[derive(Deserialize)]
            struct List<'a> {
                #[serde(borrow)]
                s: Cow<'a, str>,
                g: u64,
                #[serde(borrow)]
                c: Cow<'a, str>,
                #[serde(borrow)]
                l: Cow<'a, str>,
                #[serde(rename = "L", borrow)]
                status: Cow<'a, str>,
                #[serde(borrow)]
                r: Cow<'a, str>,
                #[serde(rename = "C", borrow)]
                client: Cow<'a, str>,
                #[serde(rename = "T")]
                t: u64,
                #[serde(rename = "O", borrow)]
                orders: &'a RawValue,
            }
            let w: List<'a> = json(raw.get().as_bytes())?;
            for s in [&w.s, &w.c, &w.l, &w.status, &w.r, &w.client] {
                text(s, 64, false)?;
            }
            let orders: Vec<ListMember<'a>> = rows(w.orders, N)?;
            for (i, o) in orders.iter().enumerate() {
                text(&o.symbol, 64, false)?;
                text(&o.client_id, 64, false)?;
                if orders[..i]
                    .iter()
                    .any(|p| p.order_id == o.order_id || p.client_id == o.client_id)
                {
                    return Err(Error::InvalidResponse);
                }
            }
            PrivateBody::List(ListStatus {
                symbol: w.s,
                list_id: w.g,
                list_client_id: w.client,
                contingency_type: w.c,
                status_type: w.l,
                order_status: w.status,
                reject_reason: w.r,
                transaction_time_ns: time(w.t)?,
                orders,
            })
        }
        (Market::Usdm, "ALGO_UPDATE") => {
            #[derive(Deserialize)]
            struct Algo<'a> {
                #[serde(borrow)]
                o: AlgoUpdate<'a>,
            }
            let w = json::<Algo<'a>>(raw.get().as_bytes())?.o;
            if transaction.is_none() {
                return Err(Error::InvalidResponse);
            }
            for s in [
                &w.symbol,
                &w.client_algo_id,
                &w.algo_type,
                &w.order_type,
                &w.side,
                &w.position_side,
                &w.time_in_force,
                &w.status,
            ] {
                text(s, 64, false)?;
            }
            if let Some(s) = &w.actual_order_id {
                text(s, 64, true)?;
            }
            if let Some(s) = &w.actual_order_type {
                text(s, 64, true)?;
            }
            if let Some(s) = &w.reject_message
                && s.len() > 256
            {
                return Err(Error::Capacity);
            }
            nonnegative(w.quantity)?;
            for d in [
                w.average_fill_price,
                w.executed_quantity,
                w.trigger_price,
                w.price,
            ]
            .into_iter()
            .flatten()
            {
                nonnegative(d)?;
            }
            PrivateBody::Algo(Box::new(w))
        }
        (Market::Spot, "eventStreamTerminated") => PrivateBody::StreamTerminated,
        (Market::Usdm, "listenKeyExpired") => PrivateBody::ListenKeyExpired,
        _ => PrivateBody::Other { event_type: h.e },
    };
    Ok(PrivateEvent {
        session: session.clone(),
        received_mono_ns: received,
        event_time_ns: time(h.event_time)?,
        transaction_time_ns: transaction,
        body,
        raw,
    })
}

#[derive(Deserialize)]
struct WireOrder<'a> {
    #[serde(rename = "s", borrow)]
    symbol: Cow<'a, str>,
    #[serde(rename = "c", borrow)]
    client_id: Cow<'a, str>,
    #[serde(rename = "C", borrow)]
    original_client: Option<Cow<'a, str>>,
    #[serde(rename = "S", borrow)]
    side: Cow<'a, str>,
    #[serde(rename = "o", borrow)]
    order_type: Cow<'a, str>,
    #[serde(rename = "f", borrow)]
    tif: Cow<'a, str>,
    #[serde(rename = "q")]
    quantity: Decimal,
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "x", borrow)]
    execution: Cow<'a, str>,
    #[serde(rename = "X", borrow)]
    status: Cow<'a, str>,
    #[serde(rename = "i")]
    id: u64,
    #[serde(rename = "g")]
    list: Option<i128>,
    #[serde(rename = "I")]
    execution_id: Option<u64>,
    #[serde(rename = "l")]
    last_quantity: Decimal,
    #[serde(rename = "z")]
    cumulative: Decimal,
    #[serde(rename = "L")]
    last_price: Decimal,
    #[serde(rename = "Z")]
    cumulative_quote: Option<Decimal>,
    #[serde(rename = "Y")]
    last_quote: Option<Decimal>,
    #[serde(rename = "n")]
    commission: Option<Decimal>,
    #[serde(rename = "N", borrow)]
    commission_asset: Option<Cow<'a, str>>,
    #[serde(rename = "T")]
    order_time: u64,
    #[serde(rename = "t")]
    trade_id: i128,
    #[serde(rename = "m")]
    maker: bool,
    #[serde(rename = "ps", borrow)]
    position_side: Option<Cow<'a, str>>,
    #[serde(rename = "rp")]
    realized_pnl: Option<Decimal>,
    #[serde(rename = "r", borrow)]
    reject: Option<Cow<'a, str>>,
    #[serde(rename = "st")]
    symbol_type: Option<u8>,
}
pub(super) fn json<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)
}
fn native_id(n: i128) -> Result<Option<u64>> {
    if n == -1 {
        Ok(None)
    } else {
        Ok(Some(u64::try_from(n).map_err(|_| Error::InvalidResponse)?))
    }
}
fn nonnegative(d: Decimal) -> Result<()> {
    if d.mantissa() < 0 {
        Err(Error::InvalidResponse)
    } else {
        Ok(())
    }
}
fn text(s: &str, max: usize, empty: bool) -> Result<()> {
    if s.len() > max {
        return Err(Error::Capacity);
    }
    if (!empty && s.is_empty()) || s.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(Error::InvalidResponse);
    }
    Ok(())
}
fn unique<'a>(items: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = Vec::new();
    for s in items {
        if seen.contains(&s) {
            return Err(Error::InvalidResponse);
        }
        seen.push(s);
    }
    Ok(())
}
fn rows<'a, T: Deserialize<'a>>(raw: &'a RawValue, max: usize) -> Result<Vec<T>> {
    struct Seed {
        max: usize,
    }
    impl<'de> DeserializeSeed<'de> for Seed {
        type Value = Vec<&'de RawValue>;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> core::result::Result<Self::Value, D::Error> {
            struct Rows {
                max: usize,
            }
            impl<'de> Visitor<'de> for Rows {
                type Value = Vec<&'de RawValue>;
                fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                    f.write_str("bounded array")
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut a: A,
                ) -> core::result::Result<Self::Value, A::Error> {
                    let mut rows = Vec::with_capacity(self.max.min(64));
                    while let Some(row) = a.next_element::<&RawValue>()? {
                        if rows.len() == self.max {
                            return Err(serde::de::Error::custom("capacity"));
                        }
                        rows.push(row);
                    }
                    Ok(rows)
                }
            }
            d.deserialize_seq(Rows { max: self.max })
        }
    }
    let mut d = serde_json::Deserializer::from_str(raw.get());
    let rows = Seed { max }.deserialize(&mut d).map_err(|e| {
        if e.to_string().starts_with("capacity") {
            Error::Capacity
        } else {
            Error::InvalidResponse
        }
    })?;
    rows.into_iter().map(|r| json(r.get().as_bytes())).collect()
}
