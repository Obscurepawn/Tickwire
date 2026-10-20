use crate::*;
use serde::Deserialize;
use serde_json::value::RawValue;
use tickwire_binance::request::Market;

pub(crate) fn endpoints(source: Source) -> (&'static str, &'static str, &'static str) {
    match source {
        Source::SpotProduction => ("stream.binance.com", "api.binance.com", ""),
        Source::SpotTest => (
            "stream.testnet.binance.vision",
            "testnet.binance.vision",
            "",
        ),
        Source::UsdmProduction => ("fstream.binance.com", "fapi.binance.com", "/public"),
        Source::UsdmTest => (
            "demo-fstream.binance.com",
            "demo-fapi.binance.com",
            "/public",
        ),
    }
}
pub(crate) fn validate<P: DepthPolicy, const L: usize, const U: usize, const B: usize>(
    c: &Config,
    n: &NetworkConfig,
) -> Result<()> {
    let spot = matches!(c.source, Source::SpotProduction | Source::SpotTest);
    let t = c.timing;
    let l = c.limits;
    if spot != (P::MARKET == Market::Spot)
        || c.owner_id == [0; 16]
        || c.first_connection_epoch == 0
        || c.first_connection_epoch <= c.depth.previous_connection_epoch
        || c.first_operation == 0
        || c.first_operation > u32::MAX as u64
        || n.tls.enable_early_data
        || spot && c.speed == DepthSpeed::Ms500
        || !spot && c.depth.time_unit == TimeUnit::Microseconds
    {
        return Err(Error::Invalid);
    }
    if [
        t.connect_ns,
        t.request_ns,
        t.first_event_ns,
        t.heartbeat_ns,
        t.pong_ns,
        t.rotate_ns,
        t.backoff_ns,
    ]
    .contains(&0)
        || t.max_backoff_ns < t.backoff_ns
        || t.rotate_ns >= 86_400_000_000_000
        || t.pong_ns
            >= if spot {
                60_000_000_000
            } else {
                600_000_000_000
            }
        || l.request_bytes == 0
        || l.request_bytes > 1 << 20
        || l.message_bytes == 0
        || l.message_bytes > 16 << 20
        || l.message_bytes < c.depth.max_message_bytes
        || l.http_bytes < c.depth.max_message_bytes
        || l.http_bytes > 16 << 20
        || l.http_headers == 0
        || l.http_headers > 256
        || l.control_frames == 0
        || l.control_frames > 1024
        || l.timestamp_segments == 0
        || l.timestamp_segments > 65536
        || l.total_buffer_bytes == 0
        || l.total_buffer_bytes > 256 << 20
        || [
            l.io.max_send_bytes,
            l.io.max_receive_bytes,
            l.io.tls_buffer_bytes,
            l.io.max_accept_bytes,
        ]
        .iter()
        .any(|&x| x == 0 || x > 16 << 20)
        || l.request_bytes > l.io.max_send_bytes
    {
        return Err(Error::Bounds);
    }
    // Both connections, decoders, immutable send jobs, timestamp spans, and FIFO.
    let terms = [
        l.io.max_receive_bytes.checked_mul(4),
        l.io.tls_buffer_bytes.checked_mul(2),
        l.io.max_send_bytes.checked_mul(2),
        l.message_bytes.checked_mul(3),
        l.http_bytes.checked_mul(2),
        l.request_bytes.checked_mul(l.control_frames + 2),
        l.timestamp_segments
            .checked_mul(std::mem::size_of::<(usize, u64)>()),
        std::mem::size_of::<tickwire_binance::feed::DepthUpdate<U>>().checked_mul(B),
        Some(std::mem::size_of::<
            tickwire_binance::depth_sync::DepthSync<P, L, U, B>,
        >()),
    ];
    let mut total = 0usize;
    for term in terms {
        total = total
            .checked_add(term.ok_or(Error::Bounds)?)
            .ok_or(Error::Bounds)?;
    }
    if total > l.total_buffer_bytes {
        return Err(Error::Bounds);
    }
    Ok(())
}
fn encoded(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 15) as usize] as char);
        }
    }
    out
}
pub(crate) fn stream(c: &Config) -> String {
    let speed = match c.speed {
        DepthSpeed::Default => "",
        DepthSpeed::Ms100 => "@100ms",
        DepthSpeed::Ms500 => "@500ms",
    };
    format!(
        "{}@depth{speed}",
        c.depth.instrument.symbol.as_str().to_lowercase()
    )
}
pub(crate) fn path(c: &Config) -> String {
    let (_, _, prefix) = endpoints(c.source);
    let mut path = match c.subscribe {
        SubscribeMode::RawUrl => {
            let symbol = encoded(&c.depth.instrument.symbol.as_str().to_lowercase());
            let speed = match c.speed {
                DepthSpeed::Default => "",
                DepthSpeed::Ms100 => "@100ms",
                DepthSpeed::Ms500 => "@500ms",
            };
            format!("{prefix}/ws/{symbol}@depth{speed}")
        }
        SubscribeMode::Explicit if prefix.is_empty() => "/ws".into(),
        SubscribeMode::Explicit => format!("{prefix}/stream"),
    };
    if c.depth.time_unit == TimeUnit::Microseconds {
        path.push_str("?timeUnit=MICROSECOND");
    }
    path
}
pub(crate) fn sbe_path(c: &Config) -> String {
    match c.subscribe {
        SubscribeMode::RawUrl => format!(
            "/ws/{}@depth",
            encoded(&c.depth.instrument.symbol.as_str().to_lowercase())
        ),
        SubscribeMode::Explicit => "/ws".into(),
    }
}
pub(crate) fn weight(c: &Config) -> u32 {
    let n = c.depth.snapshot_limit;
    match c.source {
        Source::SpotProduction | Source::SpotTest => match n {
            0..=100 => 5,
            101..=500 => 25,
            501..=1000 => 50,
            _ => 250,
        },
        _ => match n {
            0..=50 => 2,
            51..=100 => 5,
            101..=500 => 10,
            _ => 20,
        },
    }
}
pub(crate) enum Incoming<'a> {
    Depth(&'a [u8]),
    Ack(u32),
    Shutdown,
}
#[derive(Deserialize)]
struct Outer<'a> {
    #[serde(borrow)]
    stream: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    #[serde(borrow)]
    e: Option<&'a str>,
    #[serde(borrow)]
    id: Option<&'a RawValue>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack<'a> {
    id: u32,
    #[serde(borrow)]
    result: &'a RawValue,
}
pub(crate) fn incoming<'a>(bytes: &'a [u8], stream: &str) -> Result<Incoming<'a>> {
    let outer: Outer<'_> = serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
    if outer.stream.is_some() || outer.data.is_some() {
        if outer.stream != Some(stream) || outer.id.is_some() || outer.e.is_some() {
            return Err(Error::Protocol);
        }
        let raw = outer.data.ok_or(Error::Protocol)?.get().as_bytes();
        let inner: Outer<'_> = serde_json::from_slice(raw).map_err(|_| Error::Protocol)?;
        if inner.e != Some("depthUpdate")
            || inner.id.is_some()
            || inner.stream.is_some()
            || inner.data.is_some()
        {
            return Err(Error::Protocol);
        }
        return Ok(Incoming::Depth(raw));
    }
    match outer.e {
        Some("depthUpdate") if outer.id.is_none() => Ok(Incoming::Depth(bytes)),
        Some("serverShutdown") if outer.id.is_none() => Ok(Incoming::Shutdown),
        None if outer.id.is_some() => {
            let ack: Ack<'_> = serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
            if ack.result.get() != "null" {
                return Err(Error::Protocol);
            }
            Ok(Incoming::Ack(ack.id))
        }
        _ => Err(Error::Protocol),
    }
}
