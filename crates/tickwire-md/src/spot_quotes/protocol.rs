use super::*;
use serde::de::{SeqAccess, Visitor};
use std::{fmt, marker::PhantomData};

/// Level-one snapshots carry zero or one row per side. Decode without growing a Vec.
#[derive(Debug)]
struct Side<T>(Option<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Side<T> {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        struct One<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for One<T> {
            type Value = Side<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("zero or one price level")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let first = seq.next_element()?;
                if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom("extra price level"));
                }
                Ok(Side(first))
            }
        }
        de.deserialize_seq(One(PhantomData))
    }
}

#[derive(Deserialize)]
struct Bybit<'a> {
    topic: Option<&'a str>,
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    ts: Option<u64>,
    op: Option<&'a str>,
    req_id: Option<&'a str>,
    success: Option<bool>,
    ret_msg: Option<&'a str>,
    #[serde(borrow)]
    data: Option<BybitBook<'a>>,
}
#[derive(Deserialize)]
struct BybitBook<'a> {
    s: &'a str,
    u: u64,
    seq: u64,
    #[serde(borrow)]
    b: Side<[&'a str; 2]>,
    #[serde(borrow)]
    a: Side<[&'a str; 2]>,
}
#[derive(Deserialize)]
struct OkxArg<'a> {
    channel: &'a str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
}
#[derive(Deserialize)]
struct Okx<'a> {
    event: Option<&'a str>,
    code: Option<&'a str>,
    msg: Option<&'a str>,
    id: Option<&'a str>,
    action: Option<&'a str>,
    #[serde(borrow)]
    arg: Option<OkxArg<'a>>,
    #[serde(borrow)]
    data: Option<[OkxBook<'a>; 1]>,
}
#[derive(Deserialize)]
struct OkxBook<'a> {
    ts: &'a str,
    #[serde(rename = "seqId")]
    seq_id: u64,
    #[serde(borrow)]
    bids: Side<[&'a str; 4]>,
    #[serde(borrow)]
    asks: Side<[&'a str; 4]>,
}

impl<'a> Session<'a> {
    pub(super) fn bybit(&mut self, bytes: &[u8], received: u64) -> Result<Option<Quote<'a>>> {
        let m: Bybit<'_> = serde_json::from_slice(bytes)?;
        if let Some(op) = m.op {
            if op == "subscribe" && m.req_id == Some("twbbo") && m.success == Some(true) {
                self.acknowledged = true;
            } else if !(op == "pong"
                || op == "ping" && m.success == Some(true) && m.ret_msg == Some("pong"))
            {
                return Err(invalid(
                    "Bybit public subscription rejected or unexpected control message",
                ));
            }
            return Ok(None);
        }
        let d = m.data.ok_or_else(|| invalid("Bybit snapshot data"))?;
        if !self.acknowledged
            || m.topic != Some(&self.channel)
            || m.kind != Some("snapshot")
            || d.s != self.config.symbol
        {
            return Err(invalid("Bybit unacknowledged or mismatched spot snapshot"));
        }
        self.quote(
            m.ts.ok_or_else(|| invalid("Bybit timestamp"))?,
            received,
            d.seq,
            Some(d.u),
            d.b,
            d.a,
        )
        .map(Some)
    }

    pub(super) fn okx(&mut self, bytes: &[u8], received: u64) -> Result<Option<Quote<'a>>> {
        if bytes == b"pong" {
            return Ok(None);
        }
        let m: Okx<'_> = serde_json::from_slice(bytes)?;
        // OKX can announce infrastructure changes before the subscription acknowledgement.
        if m.event == Some("notice") {
            return Ok(None);
        }
        if m.event == Some("error") {
            return Err(invalid(&format!(
                "OKX rejected subscription {}: {}",
                m.code.unwrap_or("unknown"),
                m.msg.unwrap_or("unknown")
            )));
        }
        let arg = m
            .arg
            .ok_or_else(|| invalid("OKX missing subscription identity"))?;
        if arg.channel != self.channel || arg.inst_id != self.config.symbol {
            return Err(invalid("OKX quote identity"));
        }
        if let Some(event) = m.event {
            if event != "subscribe" || m.id != Some("twbbo") {
                return Err(invalid(
                    "OKX subscription rejected or unexpected control message",
                ));
            }
            self.acknowledged = true;
            return Ok(None);
        }
        if !self.acknowledged || m.action.is_some_and(|s| s != "snapshot") {
            return Err(invalid("OKX unacknowledged or incremental snapshot"));
        }
        let [d] = m.data.ok_or_else(|| invalid("OKX snapshot data"))?;
        self.quote(d.ts.parse()?, received, d.seq_id, None, d.bids, d.asks)
            .map(Some)
    }

    fn quote<const N: usize>(
        &self,
        millis: u64,
        received: u64,
        sequence: u64,
        update: Option<u64>,
        bid: Side<[&str; N]>,
        ask: Side<[&str; N]>,
    ) -> Result<Quote<'a>> {
        let units = |s: &str, step| {
            let n = Decimal::parse(s)
                .and_then(|d| tickwire_binance::feed::units(d, step))
                .map_err(|_| invalid("off-grid or overflowing spot price/size"))?;
            if n <= 0 {
                return Err(invalid("nonpositive spot quote level"));
            }
            Ok(n)
        };
        let level = |row: [&str; N]| -> Result<_> {
            Ok(QuoteLevel {
                price_ticks: units(row[0], self.config.price_tick)?,
                quantity_lots: units(row[1], self.config.quantity_step)?,
            })
        };
        Ok(Quote {
            venue_id: self.config.venue.id(),
            symbol: &self.config.symbol,
            instrument_id: self.config.instrument_id,
            instrument_version: self.config.instrument_version,
            connection_epoch: self.epoch,
            venue_sequence: sequence,
            update_id: update,
            exchange_event_ns: millis
                .checked_mul(1_000_000)
                .ok_or_else(|| invalid("exchange timestamp overflow"))?,
            received_mono_ns: received,
            bid: bid.0.map(level).transpose()?,
            ask: ask.0.map(level).transpose()?,
        })
    }
}
