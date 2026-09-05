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

/// Signed order endpoint (`POST` submit, `GET` query, `DELETE` cancel).
pub const ASTER_ORDER_PATH: &str = "/fapi/v3/order";

/// Signed endpoint cancelling every open order for a symbol.
pub const ASTER_ALL_OPEN_ORDERS_PATH: &str = "/fapi/v3/allOpenOrders";

/// Signed endpoint listing open orders.
pub const ASTER_OPEN_ORDERS_PATH: &str = "/fapi/v3/openOrders";

/// Signed endpoint listing historical orders.
pub const ASTER_ALL_ORDERS_PATH: &str = "/fapi/v3/allOrders";

/// Signed endpoint returning per-asset futures balances.
pub const ASTER_BALANCE_PATH: &str = "/fapi/v3/balance";

/// Signed endpoint returning position risk (open positions).
pub const ASTER_POSITION_RISK_PATH: &str = "/fapi/v3/positionRisk";

/// Signed endpoint returning the account's commission rate for a symbol.
pub const ASTER_COMMISSION_RATE_PATH: &str = "/fapi/v3/commissionRate";

/// Signed endpoint returning the account's own trades (fills).
pub const ASTER_USER_TRADES_PATH: &str = "/fapi/v3/userTrades";

/// Signed endpoint reporting whether the account runs in hedge (dual-side) mode.
pub const ASTER_POSITION_SIDE_DUAL_PATH: &str = "/fapi/v3/positionSide/dual";

/// Signed endpoint managing the user data stream listen key.
pub const ASTER_LISTEN_KEY_PATH: &str = "/fapi/v3/listenKey";

/// Rate-limit key covering every request against the shared request-weight budget.
pub const ASTER_GLOBAL_RATE_KEY: &str = "aster:global";

/// Rate-limit key covering order-placement and cancellation requests.
pub const ASTER_ORDER_RATE_KEY: &str = "aster:orders";

/// Request weight budget per minute (`REQUEST_WEIGHT`, 2400/min).
pub const ASTER_REQUEST_WEIGHT_PER_MINUTE: u32 = 2400;

/// Order rate budget per minute (`ORDER`, 1200/min).
pub const ASTER_ORDERS_PER_MINUTE: u32 = 1200;

// -------------------------------------------------------------------------------------------
// Documented request weights
// -------------------------------------------------------------------------------------------
//
// Every signed endpoint spends its documented weight from the shared 2400/minute budget, so a
// handful of calls to a heavy endpoint costs as much as hundreds of order operations. Aster
// answers an exceeded budget with HTTP 429 and escalates repeated violations to an HTTP 418 IP
// ban lasting from two minutes to three days, which is why the weights are modelled exactly
// rather than approximated at one unit per request.
//
// # References
//
// - <https://github.com/asterdex/api-docs/tree/master/V3(Recommended)/EN>

/// Weight of `POST`/`GET`/`DELETE /fapi/v3/order`.
pub const ASTER_WEIGHT_ORDER: u32 = 1;

/// Weight of `DELETE /fapi/v3/allOpenOrders`.
pub const ASTER_WEIGHT_ALL_OPEN_ORDERS: u32 = 1;

/// Weight of `GET /fapi/v3/openOrders` for a single symbol.
pub const ASTER_WEIGHT_OPEN_ORDERS_SYMBOL: u32 = 1;

/// Weight of `GET /fapi/v3/openOrders` without a symbol (every symbol on the account).
pub const ASTER_WEIGHT_OPEN_ORDERS_ALL: u32 = 40;

/// Weight of `GET /fapi/v3/allOrders`.
pub const ASTER_WEIGHT_ALL_ORDERS: u32 = 5;

/// Weight of `GET /fapi/v3/userTrades`.
pub const ASTER_WEIGHT_USER_TRADES: u32 = 5;

/// Weight of `GET /fapi/v3/balance`.
pub const ASTER_WEIGHT_BALANCE: u32 = 5;

/// Weight of `GET /fapi/v3/positionRisk`.
pub const ASTER_WEIGHT_POSITION_RISK: u32 = 5;

/// Weight of `GET /fapi/v3/commissionRate`.
pub const ASTER_WEIGHT_COMMISSION_RATE: u32 = 20;

/// Weight of `GET /fapi/v3/positionSide/dual`.
pub const ASTER_WEIGHT_POSITION_SIDE_DUAL: u32 = 30;

/// Weight of the `POST`/`PUT`/`DELETE /fapi/v3/listenKey` lifecycle.
pub const ASTER_WEIGHT_LISTEN_KEY: u32 = 1;

/// Weight charged for a path with no documented entry in this table.
///
/// Matches the weight of the account endpoints rather than the cheapest endpoint, so a path
/// added without a table entry under-spends the budget instead of inviting a ban.
pub const ASTER_WEIGHT_DEFAULT: u32 = 5;

/// Interval at which the user data stream listen key must be renewed.
///
/// Aster expires an idle listen key after 60 minutes; renewing every 30 minutes leaves a
/// full renewal cycle of headroom.
pub const ASTER_LISTEN_KEY_RENEWAL_SECS: u64 = 30 * 60;

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
    fn test_signed_endpoint_paths() {
        assert_eq!(ASTER_ORDER_PATH, "/fapi/v3/order");
        assert_eq!(ASTER_ALL_OPEN_ORDERS_PATH, "/fapi/v3/allOpenOrders");
        assert_eq!(ASTER_LISTEN_KEY_PATH, "/fapi/v3/listenKey");
        assert_eq!(ASTER_USER_TRADES_PATH, "/fapi/v3/userTrades");
        assert!(ASTER_POSITION_SIDE_DUAL_PATH.starts_with("/fapi/v3/"));
    }

    #[rstest]
    fn test_rate_limit_budgets() {
        assert_eq!(ASTER_REQUEST_WEIGHT_PER_MINUTE, 2400);
        assert_eq!(ASTER_ORDERS_PER_MINUTE, 1200);
        assert_eq!(ASTER_LISTEN_KEY_RENEWAL_SECS, 1800);
    }

    #[rstest]
    fn test_documented_request_weights() {
        assert_eq!(ASTER_WEIGHT_ORDER, 1);
        assert_eq!(ASTER_WEIGHT_ALL_OPEN_ORDERS, 1);
        assert_eq!(ASTER_WEIGHT_LISTEN_KEY, 1);
        assert_eq!(ASTER_WEIGHT_OPEN_ORDERS_SYMBOL, 1);
        assert_eq!(ASTER_WEIGHT_OPEN_ORDERS_ALL, 40);
        assert_eq!(ASTER_WEIGHT_ALL_ORDERS, 5);
        assert_eq!(ASTER_WEIGHT_USER_TRADES, 5);
        assert_eq!(ASTER_WEIGHT_BALANCE, 5);
        assert_eq!(ASTER_WEIGHT_POSITION_RISK, 5);
        assert_eq!(ASTER_WEIGHT_COMMISSION_RATE, 20);
        assert_eq!(ASTER_WEIGHT_POSITION_SIDE_DUAL, 30);
        // An unmapped path is charged more than the cheapest endpoint, never less.
        assert_eq!(ASTER_WEIGHT_DEFAULT, 5);
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
