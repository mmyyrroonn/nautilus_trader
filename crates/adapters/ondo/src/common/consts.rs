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

//! Venue identifiers, endpoint resolution and tuning constants for the Ondo Perps adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

use crate::common::enums::OndoEnvironment;

/// Venue name string for Ondo Perps.
///
/// This is the venue of the Ondo Perps exchange, not the `ONDO` crypto asset ticker.
pub const ONDO: &str = "ONDO";

/// Ondo Perps venue identifier.
pub static ONDO_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(ONDO)));

/// Static client ID instance for Ondo Perps.
pub static ONDO_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(Ustr::from(ONDO)));

/// Product marker of a perps market in the venue's symbol scheme, as in `NVDA-USD.P`.
pub const ONDO_PERP_MARKET_SUFFIX: &str = ".P";

/// Quote token of every Ondo Perps market supported in this phase.
pub const ONDO_QUOTE_SYMBOL: &str = "USD";

/// Product marker inserted into the Nautilus symbol, as in `NVDA-USD-PERP`.
pub const ONDO_PERP_SYMBOL_MARKER: &str = "PERP";

/// Settlement currency code of Ondo Perps contracts.
pub const ONDO_SETTLEMENT_CURRENCY: &str = "USDC";

/// Contract multiplier of Ondo Perps contracts.
///
/// Source: `crates/adapters/ondo/test_data/README.md`, `tradesPerps` channel. The venue defines
/// `cost` = `price` x `size` with `price` in quote USD per **base unit** and `size` in base
/// units, so one contract is one base unit and the products are linear with quantity denominated
/// in the base token. Ondo's market metadata declares no multiplier field; if the venue starts
/// publishing one, it must replace this constant rather than be reconciled with it.
pub const ONDO_CONTRACT_MULTIPLIER: i32 = 1;

/// Production REST base URL.
pub const ONDO_HTTP_BASE_URL_PRODUCTION: &str = "https://api.ondoperps.xyz";

/// Sandbox REST base URL.
///
/// Sandbox has no observed traffic in this phase; the host is documented only and must be
/// re-checked before execution work.
pub const ONDO_HTTP_BASE_URL_SANDBOX: &str = "https://api.ondoperps-sandbox.xyz";

/// Production WebSocket URL.
pub const ONDO_WS_URL_PRODUCTION: &str = "wss://api.ondoperps.xyz/ws";

/// Sandbox WebSocket URL.
///
/// Sandbox has no observed traffic in this phase; the host is documented only and must be
/// re-checked before execution work.
pub const ONDO_WS_URL_SANDBOX: &str = "wss://api.ondoperps-sandbox.xyz/ws";

/// Returns the REST base URL for an environment.
#[must_use]
pub const fn http_base_url(environment: OndoEnvironment) -> &'static str {
    match environment {
        OndoEnvironment::Production => ONDO_HTTP_BASE_URL_PRODUCTION,
        OndoEnvironment::Sandbox => ONDO_HTTP_BASE_URL_SANDBOX,
    }
}

/// Returns the WebSocket URL for an environment.
#[must_use]
pub const fn ws_url(environment: OndoEnvironment) -> &'static str {
    match environment {
        OndoEnvironment::Production => ONDO_WS_URL_PRODUCTION,
        OndoEnvironment::Sandbox => ONDO_WS_URL_SANDBOX,
    }
}

/// Default REST request timeout in seconds.
pub const ONDO_HTTP_TIMEOUT_SECS: u64 = 15;

/// Default application-level WebSocket heartbeat interval in seconds.
///
/// The venue disconnects an idle connection after 180 s and requires a `{"op":"ping"}` frame;
/// the protocol-level ping alone is not sufficient.
pub const ONDO_WS_HEARTBEAT_SECS: u64 = 20;

/// Default maximum number of order book levels requested per market.
pub const ONDO_BOOK_LIMIT: u32 = 100;

/// Interval between low-priority market metadata refreshes in seconds.
pub const ONDO_METADATA_REFRESH_INTERVAL_SECS: u64 = 60;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_venue_identity() {
        assert_eq!(ONDO_VENUE.as_str(), "ONDO");
        assert_eq!(ONDO_CLIENT_ID.as_str(), "ONDO");
    }

    #[rstest]
    fn test_endpoint_resolution_is_per_environment() {
        assert_eq!(
            http_base_url(OndoEnvironment::Production),
            "https://api.ondoperps.xyz",
        );
        assert_eq!(
            ws_url(OndoEnvironment::Production),
            "wss://api.ondoperps.xyz/ws",
        );
        assert_eq!(
            http_base_url(OndoEnvironment::Sandbox),
            "https://api.ondoperps-sandbox.xyz",
        );
        assert_eq!(
            ws_url(OndoEnvironment::Sandbox),
            "wss://api.ondoperps-sandbox.xyz/ws",
        );
    }
}
