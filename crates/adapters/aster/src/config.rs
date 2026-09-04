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
use nautilus_model::identifiers::Venue;
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
}
