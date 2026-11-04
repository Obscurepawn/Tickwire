use crate::{
    Catalog, Error, Hash, Instrument, MAX_ASSETS, MAX_BYTES, MAX_DEPTH, MAX_INSTRUMENTS, Market,
    Result,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess, Visitor},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt, marker::PhantomData};
use tickwire_venue as v;

const DOMAIN: &[u8] = b"tickwire.instrument-catalog.v1\0";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Body {
    schema: u16,
    venue: u32,
    environment: Environment,
    market: Market,
    catalog_version: String,
    #[serde(deserialize_with = "assets")]
    assets: Vec<Asset>,
    #[serde(deserialize_with = "instruments")]
    instruments: Vec<Entry>,
}
#[derive(Deserialize, Serialize)]
#[serde(try_from = "String")]
enum Environment {
    Production,
    Test,
}
impl TryFrom<String> for Environment {
    type Error = &'static str;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            "Production" => Ok(Self::Production),
            "Test" => Ok(Self::Test),
            _ => Err("unknown environment"),
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Asset {
    id: String,
    version: String,
    symbol: String,
    exponent: i8,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: String,
    version: String,
    symbol: String,
    product: Product,
    base: String,
    quote: String,
    settlement: String,
    price_tick: String,
    quantity_step: String,
    contract: Contract,
}
#[derive(Deserialize, Serialize)]
#[serde(try_from = "String")]
enum Product {
    Spot,
    LinearPerpetual,
    LinearFuture,
}
impl TryFrom<String> for Product {
    type Error = &'static str;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            "Spot" => Ok(Self::Spot),
            "LinearPerpetual" => Ok(Self::LinearPerpetual),
            "LinearFuture" => Ok(Self::LinearFuture),
            _ => Err("unknown product"),
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Contract {
    Cash {},
    Perpetual {},
    Delivery { expiry_utc_ns: String },
}

pub(super) fn decode(raw: &[u8], expected: Hash) -> Result<Catalog> {
    if raw.is_empty() || raw.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    if <Hash>::from(Sha256::digest(raw)) != expected {
        return Err(Error::Digest);
    }
    depth(raw)?;
    let mut body: Body = serde_json::from_slice(raw).map_err(|_| Error::Encoding)?;
    if body.schema != 1 || body.venue == 0 {
        return Err(Error::Identity);
    }
    let version = id(&body.catalog_version)?;
    let mut assets = Vec::with_capacity(body.assets.len());
    let mut symbols = BTreeSet::new();
    for a in &body.assets {
        let asset = v::AssetRef {
            id: id(&a.id)?,
            version: id(&a.version)?,
            symbol: v::AssetSymbol::new(&a.symbol).map_err(|_| Error::Economics)?,
            exponent: a.exponent,
        };
        if !(-38..=38).contains(&asset.exponent) {
            return Err(Error::Economics);
        }
        if !symbols.insert(a.symbol.as_str()) {
            return Err(Error::Duplicate);
        }
        assets.push(asset);
    }
    assets.sort_unstable_by_key(|a| a.id);
    if assets.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(Error::Duplicate);
    }
    let asset = |text: &str| -> Result<v::AssetRef> {
        let id = id(text)?;
        assets
            .binary_search_by_key(&id, |a| a.id)
            .map(|i| assets[i].clone())
            .map_err(|_| Error::Economics)
    };
    let mut instruments = Vec::with_capacity(body.instruments.len());
    let mut symbols = BTreeSet::new();
    for i in &body.instruments {
        let (product, contract) = match (&body.market, &i.product, &i.contract) {
            (Market::Spot, Product::Spot, Contract::Cash {}) => {
                (v::Product::Spot, v::ContractKind::Cash)
            }
            (Market::Usdm, Product::LinearPerpetual, Contract::Perpetual {}) => {
                (v::Product::LinearPerpetual, v::ContractKind::Perpetual)
            }
            (Market::Usdm, Product::LinearFuture, Contract::Delivery { expiry_utc_ns }) => (
                v::Product::LinearFuture,
                v::ContractKind::Delivery {
                    expiry_utc_ns: id(expiry_utc_ns)?,
                },
            ),
            _ => return Err(Error::Economics),
        };
        let binding = v::InstrumentBinding {
            instrument_id: id(&i.id)?,
            instrument_version: id(&i.version)?,
            catalog_version: version,
            catalog_hash: [0; 32],
            symbol: v::Symbol::new(&i.symbol).map_err(|_| Error::Economics)?,
            base: asset(&i.base)?,
            quote: asset(&i.quote)?,
            settlement: asset(&i.settlement)?,
            price_tick: decimal(&i.price_tick)?,
            quantity_step: decimal(&i.quantity_step)?,
            contract,
        };
        if binding.base.id == binding.quote.id {
            return Err(Error::Economics);
        }
        if !symbols.insert(i.symbol.as_str()) {
            return Err(Error::Duplicate);
        }
        instruments.push(Instrument { product, binding });
    }
    instruments.sort_unstable_by_key(|i| i.binding.instrument_id);
    if instruments
        .windows(2)
        .any(|pair| pair[0].binding.instrument_id == pair[1].binding.instrument_id)
    {
        return Err(Error::Duplicate);
    }
    // IDs have already been validated; lexical length followed by bytes is numeric order.
    body.assets
        .sort_unstable_by(|a, b| a.id.len().cmp(&b.id.len()).then_with(|| a.id.cmp(&b.id)));
    body.instruments
        .sort_unstable_by(|a, b| a.id.len().cmp(&b.id.len()).then_with(|| a.id.cmp(&b.id)));
    let canonical = serde_json::to_vec(&body).map_err(|_| Error::Encoding)?;
    if canonical.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    let mut digest = Sha256::new();
    digest.update(DOMAIN);
    digest.update(&canonical);
    let catalog_hash = digest.finalize().into();
    for i in &mut instruments {
        i.binding.catalog_hash = catalog_hash;
    }
    Ok(Catalog {
        raw: raw.to_vec(),
        artifact_hash: expected,
        canonical,
        catalog_hash,
        version,
        venue: body.venue,
        environment: match body.environment {
            Environment::Production => v::Environment::Production,
            Environment::Test => v::Environment::Test,
        },
        market: body.market,
        assets,
        instruments,
    })
}

fn id(text: &str) -> Result<u64> {
    if text.is_empty()
        || text.len() > 20
        || text.starts_with('0')
        || !text.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::Identity);
    }
    text.parse().map_err(|_| Error::Identity)
}
fn decimal(text: &str) -> Result<v::Decimal> {
    let value: v::Decimal = text.parse().map_err(|_| Error::Economics)?;
    if !value.is_positive() || value.as_plain_string() != text {
        return Err(Error::Economics);
    }
    Ok(value)
}

// Check container nesting before serde allocates typed collections. Syntax and UTF-8
// validation remain serde's responsibility, including all escaped string content.
fn depth(raw: &[u8]) -> Result<()> {
    let mut level = 0usize;
    let mut string = false;
    let mut escaped = false;
    for &byte in raw {
        if string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                string = false;
            }
        } else {
            match byte {
                b'"' => string = true,
                b'[' | b'{' => {
                    level += 1;
                    if level > MAX_DEPTH {
                        return Err(Error::Bounds);
                    }
                }
                b']' | b'}' => {
                    level = level.checked_sub(1).ok_or(Error::Encoding)?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn assets<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Vec<Asset>, D::Error> {
    bounded::<Asset, D, MAX_ASSETS>(deserializer)
}
fn instruments<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Entry>, D::Error> {
    bounded::<Entry, D, MAX_INSTRUMENTS>(deserializer)
}
fn bounded<'de, T: Deserialize<'de>, D: Deserializer<'de>, const MAX: usize>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error> {
    struct Bounded<T, const MAX: usize>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const MAX: usize> Visitor<'de> for Bounded<T, MAX> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a nonempty array of at most {MAX} entries")
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut result = Vec::new();
            while let Some(entry) = seq.next_element()? {
                if result.len() == MAX {
                    return Err(de::Error::custom("catalog collection capacity"));
                }
                result.push(entry);
            }
            if result.is_empty() {
                return Err(de::Error::custom("empty catalog collection"));
            }
            Ok(result)
        }
    }
    deserializer.deserialize_seq(Bounded::<T, MAX>(PhantomData))
}
