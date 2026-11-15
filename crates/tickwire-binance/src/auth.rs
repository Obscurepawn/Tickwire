use crate::{Error, Result, buffer::atomic};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{hmac, rand::SystemRandom, signature};

pub const MAX_SIGNING_INPUT: usize = 64 * 1024;
pub const MAX_SIGNATURE: usize = 1368;
const MAX_PEM_DER_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    HmacSha256,
    RsaSha256,
    Ed25519,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    SpotRest,
    SpotWebSocket,
    UsdmRest,
    UsdmRestQuery,
    UsdmWebSocket,
    CoinmRest,
    CoinmWebSocket,
    OptionsRest,
    SpotSessionLogon,
    UsdmSessionLogon,
    CoinmSessionLogon,
    SpotFixLogon,
}

impl Endpoint {
    pub fn permits(self, algorithm: Algorithm) -> bool {
        match self {
            Self::SpotRest | Self::SpotWebSocket | Self::UsdmRestQuery | Self::CoinmRest => true,
            Self::UsdmRest => {
                matches!(algorithm, Algorithm::HmacSha256 | Algorithm::RsaSha256)
            }
            Self::UsdmWebSocket | Self::CoinmWebSocket => {
                matches!(algorithm, Algorithm::HmacSha256 | Algorithm::Ed25519)
            }
            Self::OptionsRest => algorithm == Algorithm::HmacSha256,
            Self::SpotSessionLogon
            | Self::UsdmSessionLogon
            | Self::CoinmSessionLogon
            | Self::SpotFixLogon => algorithm == Algorithm::Ed25519,
        }
    }
}

enum Key {
    Hmac(hmac::Key),
    Ed25519(signature::Ed25519KeyPair),
    Rsa(Box<signature::RsaKeyPair>),
}

/// Key material deliberately has no `Debug`, `Clone`, or export implementation.
///
/// Separate `SigningKey` instances are constructed at cold startup for `REQ`, `Dialer`, and
/// `AsyncRestQuerier` threads (`SigningKey: Send + Sync`).
pub struct SigningKey(Key);

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SigningKey>();
};

impl SigningKey {
    pub fn hmac(secret: &[u8]) -> Result<Self> {
        if secret.is_empty() || secret.len() > 4096 {
            return Err(Error::InvalidKey);
        }
        Ok(Self(Key::Hmac(hmac::Key::new(hmac::HMAC_SHA256, secret))))
    }

    pub fn ed25519_pkcs8(der: &[u8]) -> Result<Self> {
        signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(der)
            .map(|k| Self(Key::Ed25519(k)))
            .map_err(|_| Error::InvalidKey)
    }

    /// Parses an Ed25519 PKCS#8 PEM private key (`-----BEGIN PRIVATE KEY-----`) without heap
    /// allocation. Non-PEM inputs fail with `Error::InvalidKey`.
    pub fn ed25519_pem(pem: &str) -> Result<Self> {
        let mut der = [0u8; MAX_PEM_DER_BYTES];
        let len = decode_pkcs8_pem(pem, &mut der)?;
        Self::ed25519_pkcs8(&der[..len])
    }

    pub fn rsa_pkcs8(der: &[u8]) -> Result<Self> {
        signature::RsaKeyPair::from_pkcs8(der)
            .map(|k| Self(Key::Rsa(Box::new(k))))
            .map_err(|_| Error::InvalidKey)
    }

    /// Parses an RSA PKCS#8 PEM private key (`-----BEGIN PRIVATE KEY-----`) without heap
    /// allocation. Non-PEM inputs fail with `Error::InvalidKey`.
    pub fn rsa_pem(pem: &str) -> Result<Self> {
        let mut der = [0u8; MAX_PEM_DER_BYTES];
        let len = decode_pkcs8_pem(pem, &mut der)?;
        Self::rsa_pkcs8(&der[..len])
    }

    pub fn algorithm(&self) -> Algorithm {
        match &self.0 {
            Key::Hmac(_) => Algorithm::HmacSha256,
            Key::Ed25519(_) => Algorithm::Ed25519,
            Key::Rsa(_) => Algorithm::RsaSha256,
        }
    }

    pub fn signature_len(&self) -> usize {
        match &self.0 {
            Key::Hmac(_) => 64,
            Key::Ed25519(_) => 88,
            Key::Rsa(k) => k.public().modulus_len().div_ceil(3) * 4,
        }
    }

    /// Produces raw hex/base64 text. REST percent-encodes the result once.
    pub fn sign(&self, endpoint: Endpoint, preimage: &[u8], out: &mut [u8]) -> Result<usize> {
        if !endpoint.permits(self.algorithm()) {
            return Err(Error::UnsupportedAlgorithm);
        }
        let len = self.signature_len();
        if preimage.len() > MAX_SIGNING_INPUT || out.len() < len {
            return Err(Error::Capacity);
        }
        let mut raw = [0; 1024];
        let mut encoded = [0; MAX_SIGNATURE];
        match &self.0 {
            Key::Hmac(key) => {
                const HEX: &[u8] = b"0123456789abcdef";
                for (i, b) in hmac::sign(key, preimage).as_ref().iter().enumerate() {
                    encoded[i * 2] = HEX[(b >> 4) as usize];
                    encoded[i * 2 + 1] = HEX[(b & 15) as usize];
                }
            }
            Key::Ed25519(key) => {
                STANDARD
                    .encode_slice(key.sign(preimage), &mut encoded)
                    .map_err(|_| Error::SigningFailed)?;
            }
            Key::Rsa(key) => {
                let signature = raw
                    .get_mut(..key.public().modulus_len())
                    .ok_or(Error::InvalidKey)?;
                key.sign(
                    &signature::RSA_PKCS1_SHA256,
                    &SystemRandom::new(),
                    preimage,
                    signature,
                )
                .map_err(|_| Error::SigningFailed)?;
                STANDARD
                    .encode_slice(signature, &mut encoded)
                    .map_err(|_| Error::SigningFailed)?;
            }
        }
        out[..len].copy_from_slice(&encoded[..len]);
        Ok(len)
    }
}

fn decode_pkcs8_pem(pem: &str, out_der: &mut [u8; MAX_PEM_DER_BYTES]) -> Result<usize> {
    const HEADER: &str = "-----BEGIN PRIVATE KEY-----";
    const FOOTER: &str = "-----END PRIVATE KEY-----";

    let trimmed = pem.trim();
    let rest = trimmed.strip_prefix(HEADER).ok_or(Error::InvalidKey)?;
    let body = rest.strip_suffix(FOOTER).ok_or(Error::InvalidKey)?;

    let mut b64 = [0u8; MAX_PEM_DER_BYTES * 2];
    let mut b64_len = 0usize;
    for &byte in body.as_bytes() {
        if byte.is_ascii_whitespace() {
            continue;
        }
        if b64_len >= b64.len() {
            return Err(Error::InvalidKey);
        }
        b64[b64_len] = byte;
        b64_len += 1;
    }
    if b64_len == 0 {
        return Err(Error::InvalidKey);
    }
    STANDARD
        .decode_slice(&b64[..b64_len], out_der)
        .map_err(|_| Error::InvalidKey)
}

pub fn fix_logon_preimage(
    sender: &str,
    target: &str,
    seq: u32,
    sending_time: &str,
    out: &mut [u8],
) -> Result<usize> {
    if sender.is_empty()
        || sender.len() > 8
        || !sender
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        || target != "SPOT"
        || !fix_timestamp(sending_time)
    {
        return Err(Error::InvalidParameter);
    }
    atomic(out, |s| {
        s.bytes(b"A\x01")?;
        s.bytes(sender.as_bytes())?;
        s.byte(1)?;
        s.bytes(target.as_bytes())?;
        s.byte(1)?;
        s.integer(seq as u128)?;
        s.byte(1)?;
        s.bytes(sending_time.as_bytes())
    })
}

fn fix_timestamp(value: &str) -> bool {
    let b = value.as_bytes();
    if !matches!(b.len(), 17 | 21 | 24) {
        return false;
    }
    b.iter().enumerate().all(|(i, v)| match i {
        8 => *v == b'-',
        11 | 14 => *v == b':',
        17 => *v == b'.',
        _ => v.is_ascii_digit(),
    })
}
