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

//! Typed Arcus perpetuals wire models. Monetary values retain exact decimal arithmetic.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusTickTier {
    pub up_to_price: Option<Decimal>,
    pub tick: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusMarket {
    pub market_display_name: String,
    pub market_id: u16,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    #[serde(rename = "type")]
    pub market_type: String,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    #[serde(default)]
    pub tick_tiers: Vec<ArcusTickTier>,
    pub min_order_size: Decimal,
    pub max_order_size: Decimal,
    pub min_order_notional: Decimal,
    pub oracle_price: Decimal,
    pub mark_price: Decimal,
    pub initial_margin_fraction: Decimal,
    pub maintenance_margin_fraction: Decimal,
    #[serde(default)]
    pub effective_maker_fee_ppm: i64,
    #[serde(default)]
    pub effective_taker_fee_ppm: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ArcusOrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ArcusOrderType {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ArcusTimeInForce {
    Gtt,
    Gtc,
    Ioc,
    Fok,
    Alo,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusOrderRequest {
    pub address: String,
    pub account_index: u8,
    pub market_id: u16,
    pub order_side: ArcusOrderSide,
    pub order_type: ArcusOrderType,
    pub time_in_force: ArcusTimeInForce,
    pub quantity: Decimal,
    pub price: Decimal,
    /// Epoch microseconds, required even for IOC/FOK.
    pub good_til_time: String,
    pub reduce_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusCancelRequest {
    pub address: String,
    pub account_index: u8,
    pub market_id: u16,
    /// Exactly `orderId` or `clientId`.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusOrderResponse {
    pub status: String,
    pub order_id: Option<String>,
    pub client_id: Option<String>,
    pub rejection_reason: Option<String>,
    pub update_time: Option<u64>,
    pub remaining_size: Option<Decimal>,
    pub filled_size: Option<Decimal>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusOrder {
    pub order_id: String,
    pub client_id: Option<String>,
    #[serde(default)]
    pub account_index: u8,
    pub market_id: u16,
    pub market_display_name: String,
    pub side: ArcusOrderSide,
    #[serde(rename = "type")]
    pub order_type: Option<ArcusOrderType>,
    pub status: String,
    pub state: Option<String>,
    pub price: Decimal,
    pub original_size: Decimal,
    pub remaining_size: Decimal,
    pub filled_size: Option<Decimal>,
    pub avg_fill_price: Option<Decimal>,
    pub time_in_force: Option<ArcusTimeInForce>,
    pub good_til_time: Option<String>,
    #[serde(default)]
    pub reduce_only: bool,
    pub rejection_reason: Option<String>,
    pub cancel_reason: Option<String>,
    pub created_at: Option<u64>,
    pub updated_at: u64,
    pub sequence_number: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusFill {
    pub trade_id: String,
    pub order_id: String,
    pub client_id: Option<String>,
    pub market_id: u16,
    pub market_display_name: String,
    pub side: ArcusOrderSide,
    pub original_size: Decimal,
    pub size: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub role: String,
    pub remaining_size: Option<Decimal>,
    pub created_at: u64,
    pub sequence_number: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusPosition {
    pub market_id: u16,
    pub market_display_name: String,
    pub side: String,
    pub size: Decimal,
    pub average_entry_price: Decimal,
    pub margin_mode: String,
    pub leverage: Decimal,
    pub margin_used: Decimal,
    pub unrealized_pnl: Decimal,
    pub mark_px: Decimal,
    pub sequence_number: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotPosition {
    pub spot_asset_id: u32,
    pub spot_asset_display_name: String,
    pub size: Decimal,
    pub value: Decimal,
    pub oracle_price: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusAccount {
    pub address: String,
    pub account_index: u8,
    pub net_quote_balance: Decimal,
    pub equity: Decimal,
    pub free_collateral: Decimal,
    pub net_deposits: Decimal,
    pub positions: std::collections::HashMap<String, ArcusPosition>,
    pub sequence_number: u64,
    pub loan_balance: Option<Decimal>,
    pub margin_state: Option<String>,
    #[serde(default)]
    pub spot_positions: std::collections::HashMap<String, ArcusSpotPosition>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusApiError {
    pub error: String,
    pub error_type: Option<String>,
    pub error_source: Option<String>,
    pub reason: Option<String>,
    pub retry_after_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ArcusMarketsResponse {
    pub markets: Vec<ArcusMarket>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ArcusOrdersResponse {
    pub orders: Vec<ArcusOrder>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ArcusFillsResponse {
    pub fills: Vec<ArcusFill>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ArcusPositionsResponse {
    pub positions: std::collections::HashMap<String, ArcusPosition>,
}
