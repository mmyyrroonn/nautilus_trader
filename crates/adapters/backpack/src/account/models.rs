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

//! Current official account schemas, preserving exact economics and unknown fields.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use serde_json::Value;

/// An exact decimal accepting documented strings and numeric position compatibility.
/// Numeric JSON is decoded with `serde_json/arbitrary_precision`, never through f64.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackDecimal(pub Decimal);
impl<'de> Deserialize<'de> for BackpackDecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let text = match value {
            Value::String(value) => value,
            Value::Number(value) => value.to_string(),
            _ => return Err(D::Error::custom("expected exact decimal")),
        };
        let digits = text.strip_prefix('-').unwrap_or(&text);
        if digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit() || b == b'.')
            || digits.bytes().filter(|b| *b == b'.').count() > 1
            || digits.starts_with('.')
            || digits.ends_with('.')
        {
            return Err(D::Error::custom("invalid exact decimal"));
        }
        Decimal::from_str_exact(&text)
            .map(Self)
            .map_err(|_| D::Error::custom("decimal cannot be represented exactly"))
    }
}
impl Serialize for BackpackDecimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

/// GET /api/v1/account: settings do not contain an authenticated account identifier.
/// Fee fields are basis points, not fractions. Unknown settings are retained.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackAccountPolicy {
    pub auto_borrow_settlements: bool,
    pub auto_lend: bool,
    pub auto_realize_pnl: bool,
    pub auto_repay_borrows: bool,
    pub borrow_limit: BackpackDecimal,
    pub futures_maker_fee: BackpackDecimal,
    pub futures_taker_fee: BackpackDecimal,
    pub leverage_limit: BackpackDecimal,
    pub limit_orders: u64,
    pub liquidating: bool,
    pub position_limit: BackpackDecimal,
    pub spot_maker_fee: BackpackDecimal,
    pub spot_taker_fee: BackpackDecimal,
    pub trigger_orders: u64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GET /api/v1/capital asset entry; staked is separate from available funds.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackpackWalletBalance {
    pub available: BackpackDecimal,
    pub locked: BackpackDecimal,
    pub staked: BackpackDecimal,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Asset collateral observations, with no inferred fungibility or margin readiness.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackCollateralAsset {
    pub symbol: String,
    pub asset_mark_price: BackpackDecimal,
    pub total_quantity: BackpackDecimal,
    pub balance_notional: BackpackDecimal,
    pub collateral_weight: BackpackDecimal,
    pub collateral_value: BackpackDecimal,
    pub open_order_quantity: BackpackDecimal,
    pub lend_quantity: BackpackDecimal,
    pub available_quantity: BackpackDecimal,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GET /api/v1/capital/collateral, distinct from wallet balances.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackCollateral {
    pub assets_value: BackpackDecimal,
    pub borrow_liability: BackpackDecimal,
    pub collateral: Vec<BackpackCollateralAsset>,
    pub imf: BackpackDecimal,
    pub unsettled_equity: BackpackDecimal,
    pub liabilities_value: BackpackDecimal,
    pub margin_fraction: Option<BackpackDecimal>,
    pub mmf: BackpackDecimal,
    pub net_equity: BackpackDecimal,
    pub net_equity_available: BackpackDecimal,
    pub net_equity_locked: BackpackDecimal,
    pub net_exposure_futures: BackpackDecimal,
    pub pnl_unrealized: BackpackDecimal,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GET /api/v1/position; no engine timestamp or empty-snapshot watermark is supplied.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackPosition {
    pub break_even_price: BackpackDecimal,
    pub entry_price: BackpackDecimal,
    pub est_liquidation_price: BackpackDecimal,
    pub imf: BackpackDecimal,
    pub imf_function: Value,
    pub mark_price: BackpackDecimal,
    pub mmf: BackpackDecimal,
    pub mmf_function: Value,
    pub net_cost: BackpackDecimal,
    pub net_quantity: BackpackDecimal,
    pub net_exposure_quantity: BackpackDecimal,
    pub net_exposure_notional: BackpackDecimal,
    pub pnl_realized: BackpackDecimal,
    pub pnl_unrealized: BackpackDecimal,
    pub cumulative_funding_payment: BackpackDecimal,
    pub subaccount_id: Option<u16>,
    pub symbol: String,
    pub user_id: i32,
    pub position_id: String,
    /// Deprecated compatibility field; never interpreted as trading capacity.
    pub cumulative_interest: Option<BackpackDecimal>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Resting endpoint integer creation time versus naive history time.
/// The REST integer's units and history timezone are not inferred from magnitude.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BackpackOrderCreatedAt {
    Resting(i64),
    History(String),
}

/// Common resting/history order fields. Optional economics remain missing, not zero.
/// Unknown enum strings and conditional/system fields remain observable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackOrder {
    pub id: String,
    pub client_id: Option<u32>,
    pub created_at: BackpackOrderCreatedAt,
    pub executed_quantity: Option<BackpackDecimal>,
    pub executed_quote_quantity: Option<BackpackDecimal>,
    pub expiry_reason: Option<String>,
    pub order_type: String,
    pub post_only: Option<bool>,
    pub price: Option<BackpackDecimal>,
    pub quantity: Option<BackpackDecimal>,
    pub quote_quantity: Option<BackpackDecimal>,
    pub reduce_only: Option<bool>,
    pub self_trade_prevention: String,
    pub status: String,
    pub side: String,
    pub symbol: String,
    pub time_in_force: String,
    pub system_order_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GET /wapi/v1/history/fills. Only true venue tradeId can identify a native fill.
/// Official clientId is a STRING, unlike integer clientId on order responses.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackFill {
    pub client_id: Option<String>,
    pub fee: BackpackDecimal,
    pub fee_symbol: String,
    pub is_maker: bool,
    pub order_id: String,
    pub price: BackpackDecimal,
    pub quantity: BackpackDecimal,
    pub side: String,
    pub symbol: String,
    pub system_order_type: Option<String>,
    /// Official schema explicitly establishes UTC for this naive datetime.
    pub timestamp: String,
    pub trade_id: Option<i64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GET /wapi/v1/history/funding: positive received, negative paid.
/// Currency and naive interval timezone are not supplied by the documented schema.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackFundingPayment {
    pub user_id: i32,
    pub subaccount_id: Option<u16>,
    pub symbol: String,
    pub quantity: BackpackDecimal,
    pub interval_end_timestamp: String,
    pub funding_rate: BackpackDecimal,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
