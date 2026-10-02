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
    #[case("BТC_USDC_PERP")]
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
