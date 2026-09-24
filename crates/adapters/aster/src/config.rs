// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Aster adapter configuration structures.

use std::any::Any;

use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{BinanceDataClientConfig, BinanceInstrumentProviderConfig},
};
use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::{AccountId, Venue};
use serde::{Deserialize, Serialize};

use crate::common::{
    consts::{ASTER_VENUE, aster_http_base_url, aster_ws_base_url},
    enums::AsterEnvironment,
};

/// Configuration for the Aster live market-data client.
///
/// Aster's Futures API is Binance-USD-M compatible, so this configuration is translated
/// into a [`BinanceDataClientConfig`] pinned to `UsdM`, Aster's endpoints, and the `ASTER`
/// venue. Execution is not supported.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.aster", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.aster")
)]
pub struct AsterDataClientConfig {
    /// Environment (mainnet or testnet).
    pub environment: AsterEnvironment,
    /// Optional base URL override for the HTTP API.
    pub base_url_http: Option<String>,
    /// Optional base URL override for the WebSocket API.
    ///
    /// Must end in `/ws`; Aster's `/stream` route connects but never delivers payloads.
    pub base_url_ws: Option<String>,
    /// Instrument loading configuration (reused from the Binance adapter).
    ///
    /// Aster rate-limits `exchangeInfo` aggressively, so prefer `load_ids` over `load_all`.
    pub instrument_provider: BinanceInstrumentProviderConfig,
    /// Interval in seconds for a full instrument catalogue refresh.
    ///
    /// Set to 0 to disable. Defaults to 3600 (60 minutes).
    pub instrument_refresh_interval_secs: u64,
    /// Interval in seconds for polling exchange info to detect instrument status changes.
    ///
    /// Set to 0 to disable. Defaults to 3600 (60 minutes).
    pub instrument_status_poll_secs: u64,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// Optional Nautilus venue identifier override (defaults to `ASTER`).
    pub venue: Option<Venue>,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(AsterDataClientConfig {
    environment: AsterEnvironment,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    instrument_provider: BinanceInstrumentProviderConfig,
    instrument_refresh_interval_secs: u64,
    instrument_status_poll_secs: u64,
    proxy_url: Option<String>,
    venue: Option<Venue>,
});

impl Default for AsterDataClientConfig {
    fn default() -> Self {
        Self {
            environment: AsterEnvironment::default(),
            base_url_http: None,
            base_url_ws: None,
            instrument_provider: BinanceInstrumentProviderConfig::default(),
            instrument_refresh_interval_secs: 3600,
            instrument_status_poll_secs: 3600,
            proxy_url: None,
            venue: None,
        }
    }
}

impl AsterDataClientConfig {
    /// Returns the configured venue, defaulting to `ASTER`.
    #[must_use]
    pub fn resolved_venue(&self) -> Venue {
        self.venue.unwrap_or(*ASTER_VENUE)
    }

    /// Returns the resolved Futures HTTP base URL.
    #[must_use]
    pub fn resolved_http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| aster_http_base_url(self.environment).to_string())
    }

    /// Returns the resolved Futures WebSocket base URL.
    #[must_use]
    pub fn resolved_ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| aster_ws_base_url(self.environment).to_string())
    }

    /// Translates this configuration into the equivalent Binance USD-M data configuration.
    #[must_use]
    pub fn to_binance(&self) -> BinanceDataClientConfig {
        BinanceDataClientConfig {
            product_type: BinanceProductType::UsdM,
            // Aster has its own testnet endpoints, so the Binance environment is always
            // `Live` and the resolved URLs below carry the environment selection.
            environment: BinanceEnvironment::Live,
            base_url_http: Some(self.resolved_http_url()),
            base_url_ws: Some(self.resolved_ws_url()),
            instrument_provider: self.instrument_provider.clone(),
            instrument_refresh_interval_secs: self.instrument_refresh_interval_secs,
            instrument_status_poll_secs: self.instrument_status_poll_secs,
            proxy_url: self.proxy_url.clone(),
            venue: Some(self.resolved_venue()),
            ..Default::default()
        }
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the translated Binance configuration is invalid (for example a
    /// `load_ids` entry that does not use the resolved venue).
    pub fn validate(&self) -> anyhow::Result<()> {
        self.to_binance().validate()
    }
}

impl ClientConfig for AsterDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Default per-attempt timeout for opening the private user data stream.
///
/// The transport's own connect timeout is shorter than a slow egress path needs; a stalled
/// handshake there fails the whole `connect`, so the adapter allows a longer window and retries.
pub const DEFAULT_WS_CONNECT_TIMEOUT_SECS: u64 = 20;

/// Configuration for the Aster live execution client.
///
/// Aster's Futures V3 trading endpoints are EIP-712 signed, so this client carries its own
/// signing credentials rather than an API key/secret pair. Instrument metadata is still loaded
/// through the Binance USD-M instrument provider because Aster serves a Binance-compatible
/// `exchangeInfo`.
///
/// The signer private key is never rendered by `Debug`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.aster", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.aster")
)]
pub struct AsterExecutionClientConfig {
    /// Account identifier for the execution client.
    pub account_id: AccountId,
    /// Environment (mainnet or testnet).
    ///
    /// Selects both the endpoints and the EIP-712 chain id used when signing.
    pub environment: AsterEnvironment,
    /// Master account wallet address (`user` in every signed request).
    ///
    /// Falls back to `ASTER_USER_ADDRESS`, then to the signer address.
    pub user_address: Option<String>,
    /// API wallet address (`signer` in every signed request).
    ///
    /// Falls back to `ASTER_SIGNER_ADDRESS`, then to the address derived from the private key.
    /// When set it must match the key, otherwise construction fails.
    pub signer_address: Option<String>,
    /// API wallet private key used for EIP-712 signing.
    ///
    /// Falls back to `ASTER_SIGNER_PRIVATE_KEY`. Never logged or included in `repr`.
    pub signer_private_key: Option<String>,
    /// Optional base URL override for the HTTP API.
    pub base_url_http: Option<String>,
    /// Optional base URL override for the WebSocket API.
    ///
    /// Must end in `/ws`; the listen key is appended as a further path segment.
    pub base_url_ws: Option<String>,
    /// Instrument loading configuration (reused from the Binance adapter).
    ///
    /// Aster rate-limits `exchangeInfo` aggressively, so prefer `load_ids` over `load_all`.
    pub instrument_provider: BinanceInstrumentProviderConfig,
    /// HTTP request timeout in seconds.
    pub http_timeout_secs: Option<u64>,
    /// WebSocket heartbeat interval in seconds.
    pub ws_heartbeat_secs: Option<u64>,
    /// Per-attempt timeout in seconds for opening the private user data stream.
    ///
    /// `connect` waits for the first listen key and socket before reporting the client as
    /// connected, so this bounds how long a stalled handshake can hold up the whole session.
    /// Hosts whose egress path is slow (a proxy, a long TLS negotiation) need more than the
    /// transport's own default; the attempt is retried on transport faults regardless.
    pub ws_connect_timeout_secs: Option<u64>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// Whether to report `EXPIRED` orders as canceled.
    ///
    /// Aster reports the unfilled remainder of `IOC`/`FOK` orders as `EXPIRED`, which maps
    /// more naturally onto `CANCELED` for the execution engine.
    pub treat_expired_as_canceled: bool,
    /// Whether to trade an account whose position mode the venue did not confirm.
    ///
    /// The adapter assumes one-way mode everywhere. A venue answer that the position-mode
    /// endpoint is unavailable leaves the mode unproven, and by default the session comes up
    /// with new risk denied while cancellations, queries, and provably reduce-only orders stay
    /// available. Setting this to `true` accepts the one-way assumption for a known
    /// environment; it is an explicit exemption and is logged on every connect.
    pub assume_one_way_mode_when_unconfirmed: bool,
    /// Optional Nautilus venue identifier override (defaults to `ASTER`).
    pub venue: Option<Venue>,
}

impl std::fmt::Debug for AsterExecutionClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AsterExecutionClientConfig))
            .field("account_id", &self.account_id)
            .field("environment", &self.environment)
            .field("user_address", &self.user_address)
            .field("signer_address", &self.signer_address)
            .field(
                "signer_private_key",
                &self.signer_private_key.as_ref().map(|_| REDACTED),
            )
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("instrument_provider", &self.instrument_provider)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_heartbeat_secs", &self.ws_heartbeat_secs)
            .field("ws_connect_timeout_secs", &self.ws_connect_timeout_secs)
            .field("proxy_url", &self.proxy_url)
            .field("treat_expired_as_canceled", &self.treat_expired_as_canceled)
            .field(
                "assume_one_way_mode_when_unconfirmed",
                &self.assume_one_way_mode_when_unconfirmed,
            )
            .field("venue", &self.venue)
            .finish()
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(AsterExecutionClientConfig {
    account_id: AccountId,
    environment: AsterEnvironment,
    user_address: Option<String>,
    signer_address: Option<String>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    instrument_provider: BinanceInstrumentProviderConfig,
    http_timeout_secs: Option<u64>,
    ws_heartbeat_secs: Option<u64>,
    ws_connect_timeout_secs: Option<u64>,
    proxy_url: Option<String>,
    treat_expired_as_canceled: bool,
    assume_one_way_mode_when_unconfirmed: bool,
    venue: Option<Venue>,
});

impl Default for AsterExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("ASTER-001"),
            environment: AsterEnvironment::default(),
            user_address: None,
            signer_address: None,
            signer_private_key: None,
            base_url_http: None,
            base_url_ws: None,
            instrument_provider: BinanceInstrumentProviderConfig::default(),
            http_timeout_secs: Some(60),
            ws_heartbeat_secs: Some(30),
            ws_connect_timeout_secs: Some(DEFAULT_WS_CONNECT_TIMEOUT_SECS),
            proxy_url: None,
            treat_expired_as_canceled: true,
            assume_one_way_mode_when_unconfirmed: false,
            venue: None,
        }
    }
}

impl AsterExecutionClientConfig {
    /// Returns the configured venue, defaulting to `ASTER`.
    #[must_use]
    pub fn resolved_venue(&self) -> Venue {
        self.venue.unwrap_or(*ASTER_VENUE)
    }

    /// Returns the resolved Futures HTTP base URL.
    #[must_use]
    pub fn resolved_http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| aster_http_base_url(self.environment).to_string())
    }

    /// Returns the resolved Futures WebSocket base URL.
    #[must_use]
    pub fn resolved_ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| aster_ws_base_url(self.environment).to_string())
    }

    /// Returns whether a signing key was configured explicitly.
    ///
    /// A `false` result does not mean the client cannot sign: the key may still come from
    /// `ASTER_SIGNER_PRIVATE_KEY`.
    #[must_use]
    pub fn has_explicit_credentials(&self) -> bool {
        self.signer_private_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument provider selection does not use the resolved venue.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.instrument_provider
            .validate_with_venue(BinanceProductType::UsdM, self.resolved_venue())
    }
}

impl ClientConfig for AsterExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::consts::{
        ASTER_HTTP_URL, ASTER_TESTNET_HTTP_URL, ASTER_TESTNET_WS_URL, ASTER_WS_URL,
    };

    #[rstest]
    fn test_to_binance_defaults_to_mainnet_usdm_on_aster_venue() {
        let config = AsterDataClientConfig::default();
        let binance = config.to_binance();

        assert_eq!(binance.product_type, BinanceProductType::UsdM);
        assert_eq!(binance.environment, BinanceEnvironment::Live);
        assert_eq!(binance.base_url_http.as_deref(), Some(ASTER_HTTP_URL));
        assert_eq!(binance.base_url_ws.as_deref(), Some(ASTER_WS_URL));
        assert_eq!(binance.venue, Some(*ASTER_VENUE));
        assert_eq!(binance.resolved_venue().as_str(), "ASTER");
    }

    #[rstest]
    fn test_to_binance_testnet_urls() {
        let config = AsterDataClientConfig {
            environment: AsterEnvironment::Testnet,
            ..Default::default()
        };
        let binance = config.to_binance();

        assert_eq!(
            binance.base_url_http.as_deref(),
            Some(ASTER_TESTNET_HTTP_URL)
        );
        assert_eq!(binance.base_url_ws.as_deref(), Some(ASTER_TESTNET_WS_URL));
    }

    #[rstest]
    fn test_to_binance_honours_url_and_venue_overrides() {
        let venue = Venue::from("ASTER_CUSTOM");
        let config = AsterDataClientConfig {
            base_url_http: Some("https://example.invalid".to_string()),
            base_url_ws: Some("wss://example.invalid/ws".to_string()),
            proxy_url: Some("http://127.0.0.1:8080".to_string()),
            venue: Some(venue),
            instrument_refresh_interval_secs: 0,
            instrument_status_poll_secs: 0,
            ..Default::default()
        };
        let binance = config.to_binance();

        assert_eq!(
            binance.base_url_http.as_deref(),
            Some("https://example.invalid")
        );
        assert_eq!(
            binance.base_url_ws.as_deref(),
            Some("wss://example.invalid/ws")
        );
        assert_eq!(binance.proxy_url.as_deref(), Some("http://127.0.0.1:8080"));
        assert_eq!(binance.venue, Some(venue));
        assert_eq!(binance.instrument_refresh_interval_secs, 0);
        assert_eq!(binance.instrument_status_poll_secs, 0);
        assert_eq!(config.resolved_venue(), venue);
    }

    #[rstest]
    fn test_validate_accepts_aster_load_ids() {
        let config = AsterDataClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec![
                    "BTCUSDT-PERP.ASTER".to_string(),
                    "NVDAUSDT-PERP.ASTER".to_string(),
                ]),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[rstest]
    fn test_validate_rejects_binance_load_ids() {
        let config = AsterDataClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec!["BTCUSDT-PERP.BINANCE".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("must use venue ASTER"), "{error}");
    }
    #[rstest]
    fn test_exec_config_defaults() {
        let config = AsterExecutionClientConfig::default();

        assert_eq!(config.account_id, AccountId::from("ASTER-001"));
        assert_eq!(config.environment, AsterEnvironment::Mainnet);
        assert_eq!(config.resolved_venue(), *ASTER_VENUE);
        assert_eq!(config.resolved_http_url(), ASTER_HTTP_URL);
        assert_eq!(config.resolved_ws_url(), ASTER_WS_URL);
        assert!(config.treat_expired_as_canceled);
        assert!(!config.has_explicit_credentials());
        assert!(config.validate().is_ok());
    }

    #[rstest]
    fn test_exec_config_testnet_urls() {
        let config = AsterExecutionClientConfig {
            environment: AsterEnvironment::Testnet,
            ..Default::default()
        };

        assert_eq!(config.resolved_http_url(), ASTER_TESTNET_HTTP_URL);
        assert_eq!(config.resolved_ws_url(), ASTER_TESTNET_WS_URL);
        assert_eq!(config.environment.chain_id(), 714);
    }

    #[rstest]
    fn test_exec_config_url_overrides() {
        let config = AsterExecutionClientConfig {
            base_url_http: Some("https://example.invalid".to_string()),
            base_url_ws: Some("wss://example.invalid/ws".to_string()),
            ..Default::default()
        };

        assert_eq!(config.resolved_http_url(), "https://example.invalid");
        assert_eq!(config.resolved_ws_url(), "wss://example.invalid/ws");
    }

    #[rstest]
    fn test_exec_config_debug_redacts_the_signer_key() {
        let config = AsterExecutionClientConfig {
            signer_private_key: Some("0x".to_string() + &"11".repeat(32)),
            ..Default::default()
        };

        let rendered = format!("{config:?}");

        assert!(config.has_explicit_credentials());
        assert!(rendered.contains(REDACTED), "{rendered}");
        assert!(!rendered.contains("1111"), "{rendered}");
    }

    #[rstest]
    fn test_exec_config_debug_shows_absent_key_as_none() {
        let rendered = format!("{:?}", AsterExecutionClientConfig::default());

        assert!(rendered.contains("signer_private_key: None"), "{rendered}");
    }

    #[rstest]
    fn test_exec_config_blank_key_is_not_explicit() {
        let config = AsterExecutionClientConfig {
            signer_private_key: Some("   ".to_string()),
            ..Default::default()
        };

        assert!(!config.has_explicit_credentials());
    }

    #[rstest]
    fn test_exec_config_validate_rejects_foreign_load_ids() {
        let config = AsterExecutionClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec!["BTCUSDT-PERP.BINANCE".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("must use venue ASTER"), "{error}");
    }

    #[rstest]
    fn test_exec_config_validate_accepts_aster_load_ids() {
        let config = AsterExecutionClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec!["NVDAUSDT-PERP.ASTER".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[rstest]
    fn test_exec_config_serde_round_trip_keeps_the_key() {
        let config = AsterExecutionClientConfig {
            signer_private_key: Some("0xabc".to_string()),
            environment: AsterEnvironment::Testnet,
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let restored: AsterExecutionClientConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.signer_private_key.as_deref(), Some("0xabc"));
        assert_eq!(restored.environment, AsterEnvironment::Testnet);
    }
}
