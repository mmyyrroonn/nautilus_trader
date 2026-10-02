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

//! Official market response fields, with unknown fields retained for later interpretation.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The official `GET /api/v1/market` response.
///
/// Decimal wire values remain strings until strict exact parsing. Unknown enum values
/// remain observable strings and are refused by the eligibility parser.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackMarket {
    /// Native venue symbol.
    pub symbol: String,
    /// Base currency code.
    pub base_symbol: String,
    /// Quote currency code.
    pub quote_symbol: String,
    /// Venue market family, requiring `PERP` for this adapter.
    pub market_type: String,
    /// Price and quantity constraints.
    pub filters: BackpackMarketFilters,
    /// Venue trading state, requiring `Open` for this phase.
    pub order_book_state: String,
    /// Naive creation datetime; no timezone or event timestamp is inferred.
    pub created_at: String,
    /// Venue visibility flag.
    pub visible: bool,
    /// Funding interval in milliseconds, when the venue supplies it.
    pub funding_interval: Option<u64>,
    /// Funding rate lower bound in basis points, when supplied.
    pub funding_rate_lower_bound: Option<String>,
    /// Funding rate upper bound in basis points, when supplied.
    pub funding_rate_upper_bound: Option<String>,
    /// Other venue fields, including margin functions, without invented semantics.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Official price and quantity filter groups.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackpackMarketFilters {
    /// Price constraints.
    pub price: BackpackPriceFilter,
    /// Order and position quantity constraints.
    pub quantity: BackpackQuantityFilter,
    /// Future filter groups retained without reinterpretation.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Official decimal-string price constraints.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackPriceFilter {
    /// Required minimum allowed price.
    pub min_price: String,
    /// Maximum price; absence or null means no supplied maximum.
    pub max_price: Option<String>,
    /// Required positive price increment.
    pub tick_size: String,
    /// Dynamic bands and future fields retained without claiming enforcement.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Official decimal-string quantity constraints.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackQuantityFilter {
    /// Required minimum allowed quantity.
    pub min_quantity: String,
    /// Maximum quantity; absence or null means no supplied maximum.
    pub max_quantity: Option<String>,
    /// Required positive quantity increment.
    pub step_size: String,
    /// Future quantity fields retained without reinterpretation.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
