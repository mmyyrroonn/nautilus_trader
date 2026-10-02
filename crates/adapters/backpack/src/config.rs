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

//! Configuration and market eligibility for the USDC linear perpetual scope.

use std::collections::BTreeSet;

use thiserror::Error;

use crate::common::endpoints::BackpackEndpoints;

/// Immutable configuration for an explicit USDC perpetual allowlist.
///
/// The symbol namespace alone does not establish product eligibility. Call
/// [`Self::validate_market`] with venue metadata before admitting a market.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpackConfig {
    symbols: BTreeSet<String>,
    endpoints: BackpackEndpoints,
}

impl BackpackConfig {
    /// Validates the symbol allowlist and selects official production endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, duplicate, or malformed allowlist entries.
    pub fn new_checked(symbols: Vec<String>) -> Result<Self, BackpackConfigError> {
        Self::with_endpoints_checked(symbols, BackpackEndpoints::production())
    }

    /// Validates the symbol allowlist with an already validated endpoint selection.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, duplicate, or malformed allowlist entries.
    pub fn with_endpoints_checked(
        symbols: Vec<String>,
        endpoints: BackpackEndpoints,
    ) -> Result<Self, BackpackConfigError> {
        if symbols.is_empty() {
            return Err(BackpackConfigError::EmptyAllowlist);
        }

        let mut allowlist = BTreeSet::new();

        for symbol in symbols {
            let valid = symbol.strip_suffix("_USDC_PERP").is_some_and(|base| {
                !base.is_empty()
                    && base
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            });

            if !valid {
                return Err(BackpackConfigError::InvalidSymbol(symbol));
            }

            if !allowlist.insert(symbol.clone()) {
                return Err(BackpackConfigError::DuplicateSymbol(symbol));
            }
        }

        Ok(Self {
            symbols: allowlist,
            endpoints,
        })
    }

    /// Returns the explicit allowlist in lexical order.
    #[must_use]
    pub fn symbols(&self) -> &BTreeSet<String> {
        &self.symbols
    }

    /// Returns the validated endpoint selection.
    #[must_use]
    pub const fn endpoints(&self) -> &BackpackEndpoints {
        &self.endpoints
    }

    /// Checks official market metadata against the allowlist and product boundary.
    ///
    /// `market_type` and `quote_symbol` must come from the venue's `marketType` and
    /// `quoteSymbol` fields. A matching symbol suffix is insufficient.
    ///
    /// # Errors
    ///
    /// Returns an error for markets outside the allowlist or metadata other than
    /// `marketType=PERP` and `quoteSymbol=USDC`.
    pub fn validate_market(
        &self,
        symbol: &str,
        market_type: &str,
        quote_symbol: &str,
    ) -> Result<(), BackpackConfigError> {
        if !self.symbols.contains(symbol) {
            return Err(BackpackConfigError::SymbolNotAllowed(symbol.to_string()));
        }

        if market_type != "PERP" || quote_symbol != "USDC" {
            return Err(BackpackConfigError::UnsupportedMarket {
                market_type: market_type.to_string(),
                quote_symbol: quote_symbol.to_string(),
            });
        }

        Ok(())
    }
}

/// A configuration or market outside the Backpack adapter's explicit scope.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BackpackConfigError {
    /// No instruments were explicitly selected.
    #[error("Backpack requires a non-empty instrument allowlist")]
    EmptyAllowlist,
    /// A native symbol is outside the USDC perpetual namespace.
    #[error("invalid Backpack USDC perpetual symbol: {0}")]
    InvalidSymbol(String),
    /// The allowlist contains the same symbol more than once.
    #[error("duplicate Backpack allowlist symbol: {0}")]
    DuplicateSymbol(String),
    /// The venue returned a symbol outside the allowlist.
    #[error("Backpack symbol is outside the allowlist: {0}")]
    SymbolNotAllowed(String),
    /// Venue metadata does not identify a supported USDC linear perpetual.
    #[error("unsupported Backpack market type {market_type} with quote {quote_symbol}")]
    UnsupportedMarket {
        /// The venue's market type.
        market_type: String,
        /// The venue's quote currency.
        quote_symbol: String,
    },
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_non_empty_allowlist_and_production_defaults() {
        let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()]).unwrap();
        assert_eq!(
            config.symbols(),
            &BTreeSet::from(["BTC_USDC_PERP".to_string()])
        );
        assert_eq!(config.endpoints(), &BackpackEndpoints::production());
        assert_eq!(
            config.validate_market("BTC_USDC_PERP", "PERP", "USDC"),
            Ok(())
        );
    }

    #[rstest]
    fn test_empty_allowlist() {
        assert_eq!(
            BackpackConfig::new_checked(vec![]),
            Err(BackpackConfigError::EmptyAllowlist)
        );
    }

    #[rstest]
    fn test_duplicate_allowlist() {
        assert_eq!(
            BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string(); 2]),
            Err(BackpackConfigError::DuplicateSymbol(
                "BTC_USDC_PERP".to_string()
            )),
        );
    }

    #[rstest]
    #[case("BTC_USDC")]
    #[case("BTC_USDT_PERP")]
    #[case("BTC_USDC_IPERP")]
    #[case("BTC_USDC_PERP.BACKPACK")]
    #[case("_USDC_PERP")]
    #[case("btc_USDC_PERP")]
    #[case("BTC_USDC_PERP ")]
    #[case(" BTC_USDC_PERP")]
    #[case("BTC/USDC_PERP")]
    #[case("B\u{0422}C_USDC_PERP")]
    fn test_invalid_allowlist_symbol(#[case] symbol: &str) {
        assert_eq!(
            BackpackConfig::new_checked(vec![symbol.to_string()]),
            Err(BackpackConfigError::InvalidSymbol(symbol.to_string())),
        );
    }

    #[rstest]
    #[case("SPOT", "USDC")]
    #[case("IPERP", "USDC")]
    #[case("DATED", "USDC")]
    #[case("PREDICTION", "USDC")]
    #[case("RFQ", "USDC")]
    #[case("PERP", "USDT")]
    #[case("PERP", "USD")]
    #[case("PERP", "usdc")]
    #[case("UNKNOWN", "USDC")]
    fn test_suffix_does_not_establish_product_eligibility(
        #[case] market_type: &str,
        #[case] quote_symbol: &str,
    ) {
        let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()]).unwrap();
        assert_eq!(
            config.validate_market("BTC_USDC_PERP", market_type, quote_symbol),
            Err(BackpackConfigError::UnsupportedMarket {
                market_type: market_type.to_string(),
                quote_symbol: quote_symbol.to_string(),
            }),
        );
    }

    #[rstest]
    fn test_market_outside_allowlist() {
        let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()]).unwrap();
        assert_eq!(
            config.validate_market("ETH_USDC_PERP", "PERP", "USDC"),
            Err(BackpackConfigError::SymbolNotAllowed(
                "ETH_USDC_PERP".to_string()
            )),
        );
    }

    #[rstest]
    fn test_config_preserves_explicit_loopback_selection() {
        let endpoints =
            BackpackEndpoints::loopback_override("http://127.0.0.1:8080", "ws://127.0.0.1:8081")
                .unwrap();
        let config = BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".to_string()],
            endpoints.clone(),
        )
        .unwrap();
        assert_eq!(config.endpoints(), &endpoints);
    }
}

/// Bounded transport and recovery policy for a public session.
///
/// These are operational adapter limits, not claims about venue quotas or feed cadence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpackPublicLifecycleConfig {
    /// Overall HTTP budget including quota and read retries.
    pub http_timeout_secs: u64,
    /// Initial and replacement WebSocket handshake bound.
    pub ws_connect_timeout_secs: u64,
    /// Protocol Ping cadence; no account authentication is attached.
    pub ws_heartbeat_secs: u64,
    /// Maximum application frame silence before transport recovery.
    pub ws_idle_timeout_secs: u64,
    /// Maximum age of the latest valid BBO event and its original receipt.
    pub quote_stale_after_ms: u64,
    /// Maximum continuous recovery interval before closing this session.
    pub reconnect_timeout_secs: u64,
    /// Graceful teardown bound, followed by an equal forced drain bound.
    pub shutdown_timeout_secs: u64,
    /// Explicit per-side REST snapshot depth.
    pub depth_snapshot_limit: usize,
    /// Shared raw message queue and per-book bootstrap frame bound.
    pub max_buffer_frames: usize,
    /// Maximum levels stored per book side.
    pub max_levels_per_side: usize,
    /// Maximum incoming application frame bytes.
    pub max_ws_message_bytes: usize,
}
impl Default for BackpackPublicLifecycleConfig {
    fn default() -> Self {
        Self {
            http_timeout_secs: 15,
            ws_connect_timeout_secs: 10,
            ws_heartbeat_secs: 10,
            ws_idle_timeout_secs: 30,
            quote_stale_after_ms: 3000,
            reconnect_timeout_secs: 60,
            shutdown_timeout_secs: 5,
            depth_snapshot_limit: 1000,
            max_buffer_frames: 2048,
            max_levels_per_side: 5000,
            max_ws_message_bytes: 1_048_576,
        }
    }
}
impl BackpackPublicLifecycleConfig {
    /// Checks operational bounds before transport creation.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/unbounded timings, unsupported depth, or excessive buffers.
    pub fn validate(&self) -> Result<(), crate::data_error::BackpackDataError> {
        use crate::data_error::BackpackDataError;
        let timings = [
            self.http_timeout_secs,
            self.ws_connect_timeout_secs,
            self.ws_heartbeat_secs,
            self.ws_idle_timeout_secs,
            self.reconnect_timeout_secs,
            self.shutdown_timeout_secs,
        ];

        if timings.iter().any(|v| !(1..=60).contains(v))
            || !(1..=30_000).contains(&self.quote_stale_after_ms)
            || self.ws_idle_timeout_secs <= self.ws_heartbeat_secs
            || self.reconnect_timeout_secs < self.ws_connect_timeout_secs
            || ![5, 10, 20, 50, 100, 500, 1000].contains(&self.depth_snapshot_limit)
            || !(1..=2048).contains(&self.max_buffer_frames)
            || self.max_levels_per_side < self.depth_snapshot_limit
            || self.max_levels_per_side > 10_000
            || !(1024..=1_048_576).contains(&self.max_ws_message_bytes)
        {
            return Err(BackpackDataError::Configuration("lifecycle bounds"));
        }
        Ok(())
    }
}

/// Credential-free native public data configuration with explicit per-symbol economics.
#[derive(Clone, Debug)]
pub struct BackpackDataClientConfig {
    scope: BackpackConfig,
    economics: std::collections::BTreeMap<String, crate::instruments::BackpackInstrumentEconomics>,
    lifecycle: BackpackPublicLifecycleConfig,
    telemetry: crate::telemetry::BackpackPublicTelemetry,
}
impl BackpackDataClientConfig {
    /// Requires economics for exactly the configured native symbol allowlist.
    ///
    /// # Errors
    ///
    /// Returns an error for missing/additional economic inputs or an excessive instrument set.
    pub fn new_checked(
        scope: BackpackConfig,
        economics: std::collections::BTreeMap<
            String,
            crate::instruments::BackpackInstrumentEconomics,
        >,
    ) -> Result<Self, crate::data_error::BackpackDataError> {
        if scope.symbols().len() > 100
            || economics.keys().collect::<std::collections::BTreeSet<_>>()
                != scope
                    .symbols()
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
        {
            return Err(crate::data_error::BackpackDataError::Configuration(
                "economics must exactly cover allowlist",
            ));
        }
        Ok(Self {
            scope,
            economics,
            lifecycle: BackpackPublicLifecycleConfig::default(),
            telemetry: crate::telemetry::BackpackPublicTelemetry::default(),
        })
    }
    /// Selects a checked operational lifecycle policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied policy exceeds implementation bounds.
    pub fn with_lifecycle_checked(
        mut self,
        lifecycle: BackpackPublicLifecycleConfig,
    ) -> Result<Self, crate::data_error::BackpackDataError> {
        lifecycle.validate()?;
        self.lifecycle = lifecycle;
        Ok(self)
    }
    /// Returns the observation handle claimed by the actual native client.
    #[must_use]
    pub const fn telemetry(&self) -> &crate::telemetry::BackpackPublicTelemetry {
        &self.telemetry
    }
    /// Returns the exact product and endpoint scope.
    #[must_use]
    pub const fn scope(&self) -> &BackpackConfig {
        &self.scope
    }
    /// Returns caller-supplied economics; these do not establish execution readiness.
    #[must_use]
    pub fn economics(
        &self,
    ) -> &std::collections::BTreeMap<String, crate::instruments::BackpackInstrumentEconomics> {
        &self.economics
    }
    /// Returns the checked public operational policy.
    #[must_use]
    pub const fn lifecycle(&self) -> &BackpackPublicLifecycleConfig {
        &self.lifecycle
    }
}
impl nautilus_common::factories::ClientConfig for BackpackDataClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
