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
    pub symbol: Cow<'a,str>,
    pub status: Cow<'a,str>,
    pub base_asset: Cow<'a,str>,
    pub quote_asset: Cow<'a,str>,
    pub margin_asset: Option<Cow<'a,str>>,
    pub contract_type: Option<Cow<'a,str>>,
    pub symbol_type: Option<u64>,
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
    pub filter_type: Cow<'a,str>,
    pub grid: Option<GridFilter>,
    pub raw: &'a RawValue,
}
impl SymbolMetadata<'_> {
    pub fn price_filter(&self) -> Option<GridFilter> {
        self.filters.iter().find(|f| f.filter_type == "PRICE_FILTER").and_then(|f| f.grid)
    }
    pub fn quantity_filter(&self) -> Option<GridFilter> {
        self.filters.iter().find(|f| f.filter_type == "LOT_SIZE").and_then(|f| f.grid)
    }
}
pub fn decode_exchange_info<const N: usize>(
    _market: Market, _bytes: &[u8], _max_bytes: usize,
) -> Result<ExchangeInfo<'_>> {
    Err(Error::InvalidResponse)
}
