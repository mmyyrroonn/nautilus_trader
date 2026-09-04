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

//! Aster venue constants and API endpoints.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

use super::enums::AsterEnvironment;

/// The Aster venue identifier string.
pub const ASTER: &str = "ASTER";

/// Static venue instance for Aster.
pub static ASTER_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(ASTER)));

/// Static client ID instance for Aster.
pub static ASTER_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(Ustr::from(ASTER)));

/// Aster Futures HTTP base URL (Mainnet).
pub const ASTER_HTTP_URL: &str = "https://fapi.asterdex.com";

/// Aster Futures WebSocket base URL (Mainnet).
///
/// The `/ws` suffix is required: `/stream` connects but never delivers payloads, and the
/// bare host is rejected outright.
pub const ASTER_WS_URL: &str = "wss://fstream.asterdex.com/ws";

/// Aster Futures HTTP base URL (Testnet).
pub const ASTER_TESTNET_HTTP_URL: &str = "https://fapi.asterdex-testnet.com";

/// Aster Futures WebSocket base URL (Testnet).
pub const ASTER_TESTNET_WS_URL: &str = "wss://fstream5.asterdex-testnet.com/ws";

/// Returns the Aster Futures HTTP base URL for the environment.
#[must_use]
pub const fn aster_http_base_url(environment: AsterEnvironment) -> &'static str {
    match environment {
        AsterEnvironment::Mainnet => ASTER_HTTP_URL,
        AsterEnvironment::Testnet => ASTER_TESTNET_HTTP_URL,
    }
}

/// Returns the Aster Futures WebSocket base URL for the environment.
#[must_use]
pub const fn aster_ws_base_url(environment: AsterEnvironment) -> &'static str {
    match environment {
        AsterEnvironment::Mainnet => ASTER_WS_URL,
        AsterEnvironment::Testnet => ASTER_TESTNET_WS_URL,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_venue_and_client_id() {
        assert_eq!(ASTER_VENUE.as_str(), "ASTER");
        assert_eq!(ASTER_CLIENT_ID.as_str(), "ASTER");
    }

    #[rstest]
    #[case(AsterEnvironment::Mainnet, ASTER_HTTP_URL, ASTER_WS_URL)]
    #[case(
        AsterEnvironment::Testnet,
        ASTER_TESTNET_HTTP_URL,
        ASTER_TESTNET_WS_URL
    )]
    fn test_base_urls(#[case] environment: AsterEnvironment, #[case] http: &str, #[case] ws: &str) {
        assert_eq!(aster_http_base_url(environment), http);
        assert_eq!(aster_ws_base_url(environment), ws);
    }
}
