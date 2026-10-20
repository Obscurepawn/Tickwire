use super::*;
#[derive(Debug)]
pub struct ExchangeInfo<'a> {
    pub market: Market,
    pub symbols: Vec<SymbolMetadata<'a>>,
    pub server_time: Option<u64>,
    pub rate_limits: &'a RawValue,
    pub exchange_filters: &'a RawValue,
    pub assets: Option<&'a RawValue>,
}
#[derive(Debug)]
pub struct SymbolMetadata<'a> {
    market: Market,
    pub symbol: Cow<'a, str>,
    pub status: Cow<'a, str>,
    pub base_asset: Cow<'a, str>,
    pub quote_asset: Cow<'a, str>,
    pub margin_asset: Option<Cow<'a, str>>,
    pub contract_type: Option<Cow<'a, str>>,
    pub symbol_type: Option<u64>,
    pub delivery_date_ms: Option<u64>,
    pub price_precision: Option<u32>,
    pub quantity_precision: Option<u32>,
    pub filters: Vec<FilterMetadata<'a>>,
    pub raw: &'a RawValue,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridFilter {
    pub minimum: Decimal,
    pub maximum: Decimal,
    pub increment: Decimal,
}
#[derive(Debug)]
pub struct FilterMetadata<'a> {
    pub filter_type: Cow<'a, str>,
    pub grid: Option<GridFilter>,
    pub raw: &'a RawValue,
}
impl SymbolMetadata<'_> {
    /// Product recognition only; status, permissions and every filter still need qualification.
    pub fn product(&self) -> Option<tickwire_venue::Product> {
        use tickwire_venue::Product;
        match self.market {
            Market::Spot if self.symbol_type.is_none() && self.contract_type.is_none() => {
                Some(Product::Spot)
            }
            Market::Usdm
                if self.symbol_type.is_none_or(|s| s == 1)
                    && self.margin_asset.as_deref() == Some(self.quote_asset.as_ref()) =>
            {
                match self.contract_type.as_deref() {
                    Some("PERPETUAL") => Some(Product::LinearPerpetual),
                    Some("CURRENT_QUARTER" | "NEXT_QUARTER" | "CURRENT_MONTH" | "NEXT_MONTH")
                        if self.delivery_date_ms.is_some_and(|t| t > 0) =>
                    {
                        Some(Product::LinearFuture)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
    pub fn price_filter(&self) -> Option<GridFilter> {
        self.filters
            .iter()
            .find(|f| f.filter_type == "PRICE_FILTER")
            .and_then(|f| f.grid)
    }
    pub fn quantity_filter(&self) -> Option<GridFilter> {
        self.filters
            .iter()
            .find(|f| f.filter_type == "LOT_SIZE")
            .and_then(|f| f.grid)
    }
}
pub fn decode_exchange_info<const N: usize>(
    market: Market,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<ExchangeInfo<'_>> {
    if N == 0
        || N > 16_384
        || max_bytes == 0
        || max_bytes > MAX_MARKET_MESSAGE_BYTES
        || bytes.len() > max_bytes
    {
        return Err(Error::Capacity);
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Wire<'a> {
        server_time: Option<u64>,
        #[serde(borrow)]
        rate_limits: &'a RawValue,
        #[serde(borrow)]
        exchange_filters: &'a RawValue,
        #[serde(borrow)]
        assets: Option<&'a RawValue>,
        #[serde(borrow)]
        symbols: &'a RawValue,
    }
    let wire: Wire<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    let rows = raw_rows::<N>(wire.symbols)?;
    let mut symbols = Vec::with_capacity(rows.len());
    for raw in rows {
        let symbol = symbol(market, raw)?;
        if symbols
            .iter()
            .any(|s: &SymbolMetadata<'_>| s.symbol == symbol.symbol)
        {
            return Err(Error::DuplicateParameter);
        }
        symbols.push(symbol);
    }
    // These collections stay raw so future limit/filter fields are not silently lost.
    raw_rows::<128>(wire.rate_limits)?;
    raw_rows::<64>(wire.exchange_filters)?;
    if let Some(assets) = wire.assets {
        raw_rows::<1024>(assets)?;
    }
    Ok(ExchangeInfo {
        market,
        symbols,
        server_time: wire.server_time,
        rate_limits: wire.rate_limits,
        exchange_filters: wire.exchange_filters,
        assets: wire.assets,
    })
}
fn symbol<'a>(market: Market, raw: &'a RawValue) -> Result<SymbolMetadata<'a>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Wire<'a> {
        #[serde(borrow)]
        symbol: Cow<'a, str>,
        #[serde(borrow)]
        status: Cow<'a, str>,
        #[serde(borrow)]
        base_asset: Cow<'a, str>,
        #[serde(borrow)]
        quote_asset: Cow<'a, str>,
        #[serde(borrow)]
        margin_asset: Option<Cow<'a, str>>,
        #[serde(borrow)]
        contract_type: Option<Cow<'a, str>>,
        st: Option<u64>,
        delivery_date: Option<u64>,
        price_precision: Option<u32>,
        quantity_precision: Option<u32>,
        #[serde(borrow)]
        filters: &'a RawValue,
    }
    let wire: Wire<'_> = serde_json::from_str(raw.get()).map_err(|_| Error::InvalidResponse)?;
    for (value, max) in [
        (&wire.symbol, 64),
        (&wire.base_asset, 32),
        (&wire.quote_asset, 32),
        (&wire.status, 64),
    ] {
        text(value, max)?;
    }
    if let Some(value) = &wire.margin_asset {
        text(value, 32)?;
    }
    if let Some(value) = &wire.contract_type {
        text(value, 64)?;
    }
    if market == Market::Usdm && (wire.margin_asset.is_none() || wire.contract_type.is_none()) {
        return Err(Error::InvalidResponse);
    }
    let rows = raw_rows::<64>(wire.filters)?;
    let mut filters = Vec::with_capacity(rows.len());
    for raw in rows {
        let filter = filter(raw)?;
        if filters
            .iter()
            .any(|f: &FilterMetadata<'_>| f.filter_type == filter.filter_type)
        {
            return Err(Error::DuplicateParameter);
        }
        filters.push(filter);
    }
    Ok(SymbolMetadata {
        market,
        symbol: wire.symbol,
        status: wire.status,
        base_asset: wire.base_asset,
        quote_asset: wire.quote_asset,
        margin_asset: wire.margin_asset,
        contract_type: wire.contract_type,
        symbol_type: wire.st,
        delivery_date_ms: wire.delivery_date,
        price_precision: wire.price_precision,
        quantity_precision: wire.quantity_precision,
        filters,
        raw,
    })
}
fn filter<'a>(raw: &'a RawValue) -> Result<FilterMetadata<'a>> {
    #[derive(Deserialize)]
    struct Kind<'a> {
        #[serde(rename = "filterType", borrow)]
        kind: Cow<'a, str>,
    }
    let kind: Kind<'_> = serde_json::from_str(raw.get()).map_err(|_| Error::InvalidResponse)?;
    text(&kind.kind, 64)?;
    let grid = match kind.kind.as_ref() {
        "PRICE_FILTER" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Price {
                min_price: Decimal,
                max_price: Decimal,
                tick_size: Decimal,
            }
            let p: Price = serde_json::from_str(raw.get()).map_err(|_| Error::InvalidResponse)?;
            Some(GridFilter {
                minimum: p.min_price,
                maximum: p.max_price,
                increment: p.tick_size,
            })
        }
        "LOT_SIZE" | "MARKET_LOT_SIZE" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Lot {
                min_qty: Decimal,
                max_qty: Decimal,
                step_size: Decimal,
            }
            let q: Lot = serde_json::from_str(raw.get()).map_err(|_| Error::InvalidResponse)?;
            Some(GridFilter {
                minimum: q.min_qty,
                maximum: q.max_qty,
                increment: q.step_size,
            })
        }
        _ => None,
    };
    if grid.is_some_and(|g| {
        [g.minimum, g.maximum, g.increment]
            .iter()
            .any(|d| d.mantissa() < 0)
    }) {
        return Err(Error::InvalidDecimal);
    }
    Ok(FilterMetadata {
        filter_type: kind.kind,
        grid,
        raw,
    })
}
fn text(value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(Error::InvalidResponse);
    }
    Ok(())
}
fn raw_rows<const N: usize>(raw: &RawValue) -> Result<Vec<&RawValue>> {
    struct Rows<const N: usize>;
    impl<'de, const N: usize> Visitor<'de> for Rows<N> {
        type Value = Vec<&'de RawValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded object array")
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(N.min(64));
            while let Some(row) = seq.next_element::<&'de RawValue>()? {
                if out.len() == N {
                    return Err(serde::de::Error::custom("capacity"));
                }
                out.push(row);
            }
            Ok(out)
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    serde::Deserializer::deserialize_seq(&mut decoder, Rows::<N>).map_err(|e| {
        if e.to_string().starts_with("capacity") {
            Error::Capacity
        } else {
            Error::InvalidResponse
        }
    })
}
