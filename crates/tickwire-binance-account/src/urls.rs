//! Colocation vs. public URL catalog, `UrlRole` bitset, and per-market URL resolution for
//! unified `BinanceSession` (`Spot`, `UsdM`, `CoinM`, `Options`).

use tickwire_binance::full_market::BinanceVenueMarket;
use tickwire_transport::LinkKind;
use tickwire_venue::Environment;

/// Connection role bitset (`human-docs/8.4`, `8.5`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct UrlRole(pub u8);

impl UrlRole {
    pub const NONE: Self = Self(0);
    pub const ORDER: Self = Self(1 << 0);
    pub const STREAM: Self = Self(1 << 1);
    pub const ORDER_AND_STREAM: Self = Self(Self::ORDER.0 | Self::STREAM.0);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn has_order(self) -> bool {
        (self.0 & Self::ORDER.0) != 0
    }

    #[inline]
    pub const fn has_stream(self) -> bool {
        (self.0 & Self::STREAM.0) != 0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Canonical URL catalog across all four Binance product lines (`Spot`, `UsdM`, `CoinM`, `Options`),
/// including colocation (`*-mm`) and public endpoints (`human-docs/8.5`, `13.9`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinanceUrlId {
    // Spot (`Order | Stream` single WebSocket pool + REST query host)
    SpotWssColo,
    SpotWssPublic,
    SpotWssTest,
    SpotHttpsPublic,
    SpotHttpsTest,

    // USD-M Futures (`ws-fapi` order + `fstream` listenKey stream + `fapi` REST query host)
    UmOrderWssColo,
    UmOrderWssPublic,
    UmOrderWssTest,
    UmStreamWssColo,
    UmStreamWssPublic,
    UmStreamWssTest,
    UmHttpsPublic,
    UmHttpsTest,

    // Coin-M Delivery (`ws-dapi` order + `dstream` listenKey stream + `dapi` REST query host)
    CmOrderWssColo,
    CmOrderWssPublic,
    CmStreamWssColo,
    CmStreamWssPublic,
    CmHttpsPublic,

    // European Options (`eapi` HTTPS `Exclusive` order pool + `fstream` listenKey stream + `eapi` REST query host)
    OptionHttpsOrderColo,
    OptionHttpsOrderPublic,
    OptionStreamWssColo,
    OptionStreamWssPublic,
    OptionHttpsPublic,
}

impl BinanceUrlId {
    #[inline]
    pub const fn as_id_str(self) -> &'static str {
        match self {
            Self::SpotWssColo => "binance_spot_wss_colo",
            Self::SpotWssPublic => "binance_spot_wss_public",
            Self::SpotWssTest => "binance_spot_wss_test",
            Self::SpotHttpsPublic => "binance_spot_https_public",
            Self::SpotHttpsTest => "binance_spot_https_test",
            Self::UmOrderWssColo => "binance_um_order_wss_colo",
            Self::UmOrderWssPublic => "binance_um_order_wss_public",
            Self::UmOrderWssTest => "binance_um_order_wss_test",
            Self::UmStreamWssColo => "binance_um_stream_wss_colo",
            Self::UmStreamWssPublic => "binance_um_stream_wss_public",
            Self::UmStreamWssTest => "binance_um_stream_wss_test",
            Self::UmHttpsPublic => "binance_um_https_public",
            Self::UmHttpsTest => "binance_um_https_test",
            Self::CmOrderWssColo => "binance_cm_order_wss_colo",
            Self::CmOrderWssPublic => "binance_cm_order_wss_public",
            Self::CmStreamWssColo => "binance_cm_stream_wss_colo",
            Self::CmStreamWssPublic => "binance_cm_stream_wss_public",
            Self::CmHttpsPublic => "binance_cm_https_public",
            Self::OptionHttpsOrderColo => "binance_option_https_order_colo",
            Self::OptionHttpsOrderPublic => "binance_option_https_order",
            Self::OptionStreamWssColo => "binance_option_stream_wss_colo",
            Self::OptionStreamWssPublic => "binance_option_stream_wss_public",
            Self::OptionHttpsPublic => "binance_option_https_public",
        }
    }

    #[inline]
    pub const fn base_url(self) -> &'static str {
        match self {
            Self::SpotWssColo => "wss://ws-api-mm.binance.com/ws-api/v3",
            Self::SpotWssPublic => "wss://ws-api.binance.com:443/ws-api/v3",
            Self::SpotWssTest => "wss://ws-api.testnet.binance.vision/ws-api/v3",
            Self::SpotHttpsPublic => "https://api.binance.com",
            Self::SpotHttpsTest => "https://testnet.binance.vision",
            Self::UmOrderWssColo => "wss://ws-fapi-mm.binance.com/ws-fapi/v1",
            Self::UmOrderWssPublic => "wss://ws-fapi.binance.com/ws-fapi/v1",
            Self::UmOrderWssTest => "wss://testnet.binancefuture.com/ws-fapi/v1",
            Self::UmStreamWssColo => "wss://fstream-mm.binance.com/ws",
            Self::UmStreamWssPublic => "wss://fstream.binance.com/private/ws",
            Self::UmStreamWssTest => "wss://fstream.binancefuture.com/private/ws",
            Self::UmHttpsPublic => "https://fapi.binance.com",
            Self::UmHttpsTest => "https://testnet.binancefuture.com",
            Self::CmOrderWssColo => "wss://ws-dapi-mm.binance.com/ws-dapi/v1",
            Self::CmOrderWssPublic => "wss://ws-dapi.binance.com/ws-dapi/v1",
            Self::CmStreamWssColo => "wss://dstream-mm.binance.com/ws",
            Self::CmStreamWssPublic => "wss://dstream.binance.com/private/ws",
            Self::CmHttpsPublic => "https://dapi.binance.com",
            Self::OptionHttpsOrderColo => "https://eapi-mm.binance.com",
            Self::OptionHttpsOrderPublic => "https://eapi.binance.com",
            Self::OptionStreamWssColo => "wss://fstream-mm.binance.com/ws",
            Self::OptionStreamWssPublic => "wss://fstream.binance.com:443/private/ws",
            Self::OptionHttpsPublic => "https://eapi.binance.com:443",
        }
    }

    #[inline]
    pub const fn host(self) -> &'static str {
        match self {
            Self::SpotWssColo => "ws-api-mm.binance.com",
            Self::SpotWssPublic => "ws-api.binance.com",
            Self::SpotWssTest => "ws-api.testnet.binance.vision",
            Self::SpotHttpsPublic => "api.binance.com",
            Self::SpotHttpsTest => "testnet.binance.vision",
            Self::UmOrderWssColo => "ws-fapi-mm.binance.com",
            Self::UmOrderWssPublic => "ws-fapi.binance.com",
            Self::UmOrderWssTest | Self::UmHttpsTest => "testnet.binancefuture.com",
            Self::UmStreamWssColo | Self::OptionStreamWssColo => "fstream-mm.binance.com",
            Self::UmStreamWssPublic | Self::OptionStreamWssPublic => "fstream.binance.com",
            Self::UmStreamWssTest => "fstream.binancefuture.com",
            Self::UmHttpsPublic => "fapi.binance.com",
            Self::CmOrderWssColo => "ws-dapi-mm.binance.com",
            Self::CmOrderWssPublic => "ws-dapi.binance.com",
            Self::CmStreamWssColo => "dstream-mm.binance.com",
            Self::CmStreamWssPublic => "dstream.binance.com",
            Self::CmHttpsPublic => "dapi.binance.com",
            Self::OptionHttpsOrderColo => "eapi-mm.binance.com",
            Self::OptionHttpsOrderPublic | Self::OptionHttpsPublic => "eapi.binance.com",
        }
    }

    #[inline]
    pub const fn port(self) -> u16 {
        443
    }

    #[inline]
    pub const fn ws_path_prefix(self) -> Option<&'static str> {
        match self {
            Self::SpotWssColo | Self::SpotWssPublic | Self::SpotWssTest => Some("/ws-api/v3"),
            Self::UmOrderWssColo | Self::UmOrderWssPublic | Self::UmOrderWssTest => {
                Some("/ws-fapi/v1")
            }
            Self::CmOrderWssColo | Self::CmOrderWssPublic => Some("/ws-dapi/v1"),
            Self::UmStreamWssColo | Self::CmStreamWssColo | Self::OptionStreamWssColo => {
                Some("/ws")
            }
            Self::UmStreamWssPublic
            | Self::UmStreamWssTest
            | Self::CmStreamWssPublic
            | Self::OptionStreamWssPublic => Some("/private/ws"),
            Self::SpotHttpsPublic
            | Self::SpotHttpsTest
            | Self::UmHttpsPublic
            | Self::UmHttpsTest
            | Self::CmHttpsPublic
            | Self::OptionHttpsOrderColo
            | Self::OptionHttpsOrderPublic
            | Self::OptionHttpsPublic => None,
        }
    }

    #[inline]
    pub const fn market(self) -> BinanceVenueMarket {
        match self {
            Self::SpotWssColo
            | Self::SpotWssPublic
            | Self::SpotWssTest
            | Self::SpotHttpsPublic
            | Self::SpotHttpsTest => BinanceVenueMarket::Spot,
            Self::UmOrderWssColo
            | Self::UmOrderWssPublic
            | Self::UmOrderWssTest
            | Self::UmStreamWssColo
            | Self::UmStreamWssPublic
            | Self::UmStreamWssTest
            | Self::UmHttpsPublic
            | Self::UmHttpsTest => BinanceVenueMarket::Usdm,
            Self::CmOrderWssColo
            | Self::CmOrderWssPublic
            | Self::CmStreamWssColo
            | Self::CmStreamWssPublic
            | Self::CmHttpsPublic => BinanceVenueMarket::Coinm,
            Self::OptionHttpsOrderColo
            | Self::OptionHttpsOrderPublic
            | Self::OptionStreamWssColo
            | Self::OptionStreamWssPublic
            | Self::OptionHttpsPublic => BinanceVenueMarket::Options,
        }
    }

    #[inline]
    pub const fn roles(self) -> UrlRole {
        match self {
            Self::SpotWssColo | Self::SpotWssPublic | Self::SpotWssTest => {
                UrlRole::ORDER_AND_STREAM
            }
            Self::UmOrderWssColo
            | Self::UmOrderWssPublic
            | Self::UmOrderWssTest
            | Self::CmOrderWssColo
            | Self::CmOrderWssPublic
            | Self::OptionHttpsOrderColo
            | Self::OptionHttpsOrderPublic => UrlRole::ORDER,
            Self::UmStreamWssColo
            | Self::UmStreamWssPublic
            | Self::UmStreamWssTest
            | Self::CmStreamWssColo
            | Self::CmStreamWssPublic
            | Self::OptionStreamWssColo
            | Self::OptionStreamWssPublic => UrlRole::STREAM,
            Self::SpotHttpsPublic
            | Self::SpotHttpsTest
            | Self::UmHttpsPublic
            | Self::UmHttpsTest
            | Self::CmHttpsPublic
            | Self::OptionHttpsPublic => UrlRole::NONE,
        }
    }

    #[inline]
    pub const fn link_kind(self) -> LinkKind {
        match self {
            Self::OptionHttpsOrderColo
            | Self::OptionHttpsOrderPublic
            | Self::SpotHttpsPublic
            | Self::SpotHttpsTest
            | Self::UmHttpsPublic
            | Self::UmHttpsTest
            | Self::CmHttpsPublic
            | Self::OptionHttpsPublic => LinkKind::Https,
            _ => LinkKind::Ws,
        }
    }

    #[inline]
    pub const fn is_colo(self) -> bool {
        matches!(
            self,
            Self::SpotWssColo
                | Self::UmOrderWssColo
                | Self::UmStreamWssColo
                | Self::CmOrderWssColo
                | Self::CmStreamWssColo
                | Self::OptionHttpsOrderColo
                | Self::OptionStreamWssColo
        )
    }

    #[inline]
    pub const fn is_query_only(self) -> bool {
        self.roles().0 == UrlRole::NONE.0
    }

    #[inline]
    pub const fn needs_listen_key(self) -> bool {
        self.market().needs_listen_key() && self.roles().has_stream()
    }
}

/// Configuration validation errors raised during `BinanceSession::configure` (`human-docs/8.4`, `8.5`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceConfigError {
    InvalidAccountRef(u8),
    EmptyMarkets,
    DuplicateMarket(BinanceVenueMarket),
    ConflictingColoAndPublicUrl {
        market: BinanceVenueMarket,
        role: UrlRole,
    },
    QueryUrlNotInstallable(BinanceUrlId),
    WrongMarketUrl {
        expected: BinanceVenueMarket,
        found: BinanceUrlId,
    },
    UnboundOrderSocket {
        market: BinanceVenueMarket,
    },
    EmptySocketBinds {
        market: BinanceVenueMarket,
        role: UrlRole,
    },
    MissingApiKey,
    MissingEd25519Pem {
        market: BinanceVenueMarket,
    },
    InvalidEd25519Pem {
        market: BinanceVenueMarket,
    },
    MissingHmacSecret {
        market: BinanceVenueMarket,
    },
    InvalidHmacSecret,
    QuotaError(String),
    TransportError(String),
}

impl core::fmt::Display for BinanceConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BinanceConfigError {}

/// Resolved order, optional separate stream, and REST query URLs for one Binance market line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceResolvedMarketUrls {
    pub market: BinanceVenueMarket,
    pub order_url: BinanceUrlId,
    /// `None` for Spot (which multiplexes `Order | Stream` on `order_url`); `Some` for USD-M,
    /// Coin-M, and Options.
    pub stream_url: Option<BinanceUrlId>,
    /// `AsyncRestQuerier` host (never installed as a `UrlConnections` socket pool).
    pub query_url: BinanceUrlId,
}

/// Resolves the active order, stream, and REST query URLs for `market` from account `requested_urls`:
/// - Rejects REST query-only `UrlId`s inside `requested_urls`.
/// - Rejects simultaneous colocation and public `UrlId`s for the same `(market, role)` (`8.5`).
/// - Applies `human-docs/8.5` defaults (`UmOrderWssColo` + `UmStreamWssColo` on USD-M Production;
///   public endpoints on Spot, Coin-M, and Options unless colocation `UrlId`s are selected).
pub fn resolve_market_urls(
    market: BinanceVenueMarket,
    environment: Environment,
    requested_urls: &[BinanceUrlId],
) -> Result<BinanceResolvedMarketUrls, BinanceConfigError> {
    let mut chosen_order: Option<BinanceUrlId> = None;
    let mut chosen_stream: Option<BinanceUrlId> = None;

    for &url_id in requested_urls {
        if url_id.is_query_only() {
            return Err(BinanceConfigError::QueryUrlNotInstallable(url_id));
        }
        if url_id.market() != market {
            return Err(BinanceConfigError::WrongMarketUrl {
                expected: market,
                found: url_id,
            });
        }
        if url_id.roles().has_order() {
            if let Some(prev) = chosen_order
                && prev != url_id
            {
                return Err(BinanceConfigError::ConflictingColoAndPublicUrl {
                    market,
                    role: UrlRole::ORDER,
                });
            }
            chosen_order = Some(url_id);
        }
        if url_id.roles().has_stream() && market != BinanceVenueMarket::Spot {
            if let Some(prev) = chosen_stream
                && prev != url_id
            {
                return Err(BinanceConfigError::ConflictingColoAndPublicUrl {
                    market,
                    role: UrlRole::STREAM,
                });
            }
            chosen_stream = Some(url_id);
        }
    }

    let (default_order, default_stream, query_url) = match (market, environment) {
        (BinanceVenueMarket::Spot, Environment::Production) => (
            BinanceUrlId::SpotWssPublic,
            None,
            BinanceUrlId::SpotHttpsPublic,
        ),
        (BinanceVenueMarket::Spot, Environment::Test) => {
            (BinanceUrlId::SpotWssTest, None, BinanceUrlId::SpotHttpsTest)
        }
        (BinanceVenueMarket::Usdm, Environment::Production) => (
            BinanceUrlId::UmOrderWssColo,
            Some(BinanceUrlId::UmStreamWssColo),
            BinanceUrlId::UmHttpsPublic,
        ),
        (BinanceVenueMarket::Usdm, Environment::Test) => (
            BinanceUrlId::UmOrderWssTest,
            Some(BinanceUrlId::UmStreamWssTest),
            BinanceUrlId::UmHttpsTest,
        ),
        (BinanceVenueMarket::Coinm, _) => (
            BinanceUrlId::CmOrderWssPublic,
            Some(BinanceUrlId::CmStreamWssPublic),
            BinanceUrlId::CmHttpsPublic,
        ),
        (BinanceVenueMarket::Options, _) => (
            BinanceUrlId::OptionHttpsOrderPublic,
            Some(BinanceUrlId::OptionStreamWssPublic),
            BinanceUrlId::OptionHttpsPublic,
        ),
    };

    Ok(BinanceResolvedMarketUrls {
        market,
        order_url: chosen_order.unwrap_or(default_order),
        stream_url: if market == BinanceVenueMarket::Spot {
            None
        } else {
            Some(chosen_stream.unwrap_or_else(|| default_stream.expect("non-spot stream default")))
        },
        query_url,
    })
}
