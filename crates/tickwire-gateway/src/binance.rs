//! Binance REST encoding for the durable gateway.
//!
//! The caller supplies a registered account key and a trusted service clock.
//! Encoding has no network side effects. The gateway persists authorization and
//! the canonical command before it queues these bytes, and rechecks its gate at
//! the transport's first application-byte acceptance boundary.
use crate::{EncodedRequest, Encoder, Error, Result};
use tickwire_binance::request::{
    self, Credentials, MAX_REQUEST_BYTES, RequestOptions, Timestamp, Transport,
};
use tickwire_transport::http;
use tickwire_venue::CanonicalCommand;

/// Supplies exchange-compatible wall time after checking calibration and freshness.
/// An unavailable or untrusted time reading must return an error. This contract
/// deliberately provides no unchecked local-wall-clock default.
pub trait TimeProvider {
    fn timestamp(&mut self) -> Result<Timestamp>;
}
impl<F: FnMut() -> Result<Timestamp>> TimeProvider for F {
    fn timestamp(&mut self) -> Result<Timestamp> {
        self()
    }
}
/// Encodes REST requests only. Credentials cannot change after construction.
/// `max_http_bytes` includes the request line, headers, and body.
pub struct RestEncoder<'a, T: TimeProvider> {
    credentials: Credentials<'a>,
    time: T,
    recv_window_micros: u64,
    max_http_bytes: usize,
}
impl<'a, T: TimeProvider> RestEncoder<'a, T> {
    pub fn new(
        credentials: Credentials<'a>,
        time: T,
        recv_window_micros: u64,
        max_http_bytes: usize,
    ) -> Result<Self> {
        if max_http_bytes == 0 || max_http_bytes > 16 * 1024 * 1024 {
            return Err(Error::Capacity);
        }
        if recv_window_micros == 0 || recv_window_micros > 60_000_000 {
            return Err(Error::Encoding);
        }
        Ok(Self {
            credentials,
            time,
            recv_window_micros,
            max_http_bytes,
        })
    }
}
impl<T: TimeProvider> Encoder for RestEncoder<'_, T> {
    fn encode(&mut self, command: &CanonicalCommand) -> Result<EncodedRequest> {
        let timestamp = self.time.timestamp()?;
        let prepared = request::prepare(
            command,
            &self.credentials,
            RequestOptions {
                transport: Transport::Rest,
                timestamp,
                recv_window_micros: self.recv_window_micros,
                // This validated field is unused on the REST wire. Correlation
                // uses the immutable command/intent and connection owner.
                request_id: "rest",
            },
            MAX_REQUEST_BYTES,
        )
        .map_err(|error| match error {
            tickwire_binance::Error::IdentityMismatch => Error::Identity,
            tickwire_binance::Error::Capacity => Error::Capacity,
            _ => Error::Encoding,
        })?;
        let api_key = prepared.api_key_header().ok_or(Error::Encoding)?;
        let bytes = http::encode_request(
            prepared.method().as_str(),
            prepared.path(),
            &[("Host", prepared.authority()), ("X-MBX-APIKEY", api_key)],
            prepared.payload(),
            self.max_http_bytes,
        )
        .map_err(|error| match error {
            tickwire_transport::Error::Bounds => Error::Capacity,
            _ => Error::Encoding,
        })?;
        EncodedRequest::new(
            prepared.command_digest(),
            prepared.authority().to_owned(),
            bytes,
        )
    }
}
