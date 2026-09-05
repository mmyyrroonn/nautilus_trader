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

//! Response models for the Aster Futures V3 REST API.
//!
//! Aster's Futures payloads follow the Binance USD-M schema, so the venue *enumerations* are
//! reused from [`nautilus_binance`] rather than duplicated. The structs, however, are defined
//! here because Aster is not byte-compatible with Binance: several numeric fields come back
//! JSON-encoded as strings (`"orderId": "417663664"`, `"updateTime": "1776802344230"`,
//! `"code": "200"`) where Binance returns numbers. Every scalar that Aster has been observed
//! to render either way is parsed leniently through [`flexible_i64`].

use std::str::FromStr;

use nautilus_binance::common::enums::{
    BinanceFuturesOrderType, BinanceMarginType, BinanceOrderStatus, BinancePositionSide,
    BinanceSide, BinanceTimeInForce,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use ustr::Ustr;

use crate::common::currency::resolve_currency;

/// Error payload returned by any Aster endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsterErrorResponse {
    /// Aster error code (negative for errors).
    #[serde(deserialize_with = "flexible_i64")]
    pub code: i64,
    /// Human-readable error message.
    pub msg: String,
}

/// Response from `POST /fapi/v3/listenKey`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterListenKeyResponse {
    /// The listen key identifying the user data stream.
    pub listen_key: String,
}

/// Response from `DELETE /fapi/v3/allOpenOrders`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsterCancelAllOrdersResponse {
    /// Response code; `200` on success.
    #[serde(deserialize_with = "flexible_i64")]
    pub code: i64,
    /// Response message.
    pub msg: String,
}

impl AsterCancelAllOrdersResponse {
    /// Returns whether the venue reported success.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.code == 200
    }
}

/// Response from whether the account runs in hedge (dual-side) mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterPositionModeResponse {
    /// Whether dual-side (hedge) position mode is enabled.
    pub dual_side_position: bool,
}

/// A futures order as returned by `POST`/`GET`/`DELETE /fapi/v3/order`, `/openOrders`,
/// and `/allOrders`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterOrder {
    /// Venue symbol.
    pub symbol: Ustr,
    /// Venue order ID.
    #[serde(deserialize_with = "flexible_i64")]
    pub order_id: i64,
    /// Client order ID, echoed back unchanged.
    #[serde(default)]
    pub client_order_id: String,
    /// Original order quantity.
    pub orig_qty: String,
    /// Cumulative filled quantity.
    pub executed_qty: String,
    /// Original limit price (`"0"` for market orders).
    pub price: String,
    /// Average fill price.
    #[serde(default)]
    pub avg_price: Option<String>,
    /// Stop/trigger price.
    #[serde(default)]
    pub stop_price: Option<String>,
    /// Cumulative quote quantity transacted.
    #[serde(default)]
    pub cum_quote: Option<String>,
    /// Order status.
    pub status: BinanceOrderStatus,
    /// Time in force.
    pub time_in_force: BinanceTimeInForce,
    /// Order type.
    #[serde(rename = "type")]
    pub order_type: BinanceFuturesOrderType,
    /// Original order type before any venue-side conversion.
    #[serde(default)]
    pub orig_type: Option<BinanceFuturesOrderType>,
    /// Order side.
    pub side: BinanceSide,
    /// Position side (`BOTH` in one-way mode).
    #[serde(default)]
    pub position_side: Option<BinancePositionSide>,
    /// Whether the order is reduce-only.
    #[serde(default)]
    pub reduce_only: Option<bool>,
    /// Whether the order closes the whole position.
    #[serde(default)]
    pub close_position: Option<bool>,
    /// Order creation time in milliseconds.
    #[serde(default, deserialize_with = "flexible_opt_i64")]
    pub time: Option<i64>,
    /// Last update time in milliseconds.
    #[serde(default, deserialize_with = "flexible_opt_i64")]
    pub update_time: Option<i64>,
}

impl AsterOrder {
    /// Returns whether the order used the post-only (`GTX`) time in force.
    #[must_use]
    pub fn is_post_only(&self) -> bool {
        self.time_in_force == BinanceTimeInForce::Gtx
    }

    /// Converts this order into a Nautilus [`OrderStatusReport`].
    ///
    /// `treat_expired_as_canceled` maps Aster's `EXPIRED` status onto
    /// [`OrderStatus::Canceled`]; Aster emits `EXPIRED` both for genuinely expired orders and
    /// for `IOC`/`FOK` remainders.
    ///
    /// # Errors
    ///
    /// Returns an error if the client order ID, quantities, or prices cannot be parsed.
    pub fn to_order_status_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        treat_expired_as_canceled: bool,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let ts_last = self.update_time.map_or(ts_init, millis_to_nanos);
        let ts_accepted = self.time.map_or(ts_last, millis_to_nanos);

        let client_order_id = if self.client_order_id.is_empty() {
            None
        } else {
            Some(ClientOrderId::new_checked(&self.client_order_id)?)
        };

        let mut report = OrderStatusReport::new(
            account_id,
            instrument_id,
            client_order_id,
            VenueOrderId::new(self.order_id.to_string()),
            Some(parse_order_side(self.side)),
            parse_order_type(self.order_type),
            parse_time_in_force(self.time_in_force),
            parse_order_status(self.status, treat_expired_as_canceled),
            parse_quantity(&self.orig_qty, size_precision, "origQty")?,
            parse_quantity(&self.executed_qty, size_precision, "executedQty")?,
            ts_accepted,
            ts_last,
            ts_init,
            None, // report_id
        );

        report.price = Some(parse_price(&self.price, price_precision, "price")?);
        report.post_only = self.is_post_only();
        report.reduce_only = self.reduce_only.unwrap_or(false);

        if let Some(raw) = self.stop_price.as_deref()
            && let Ok(trigger) = parse_price(raw, price_precision, "stopPrice")
            && !trigger.is_zero()
        {
            report.trigger_price = Some(trigger);
        }

        if report.filled_qty.as_decimal() > Decimal::ZERO
            && let Some(raw) = self.avg_price.as_deref()
            && let Ok(avg) = parse_decimal(raw, "avgPrice")
            && avg > Decimal::ZERO
        {
            report.avg_px = Some(avg);
        }

        Ok(report)
    }
}

/// A per-asset futures balance from `GET /fapi/v3/balance`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterBalance {
    /// Asset code (for example `USDT`).
    pub asset: Ustr,
    /// Wallet balance.
    #[serde(alias = "walletBalance")]
    pub balance: String,
    /// Cross-margin wallet balance.
    #[serde(default)]
    pub cross_wallet_balance: Option<String>,
    /// Unrealized PnL of cross-margin positions.
    #[serde(default)]
    pub cross_un_pnl: Option<String>,
    /// Balance available to open new positions.
    #[serde(default)]
    pub available_balance: Option<String>,
    /// Maximum withdrawable amount.
    #[serde(default)]
    pub max_withdraw_amount: Option<String>,
    /// Whether the asset can be used as margin.
    #[serde(default)]
    pub margin_available: Option<bool>,
    /// Last update time in milliseconds; Aster encodes this as a string.
    #[serde(default, deserialize_with = "flexible_opt_i64")]
    pub update_time: Option<i64>,
}

impl AsterBalance {
    /// Returns the wallet balance as a decimal.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not a decimal number.
    pub fn total(&self) -> anyhow::Result<Decimal> {
        parse_decimal(&self.balance, "balance")
    }

    /// Returns the free (available) balance, falling back to the wallet balance.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not a decimal number.
    pub fn free(&self) -> anyhow::Result<Decimal> {
        match self.available_balance.as_deref() {
            Some(raw) => parse_decimal(raw, "availableBalance"),
            None => self.total(),
        }
    }

    /// Returns whether the venue reports this asset as fully empty.
    ///
    /// True only when the wallet balance *and* the available balance parse as zero. Aster's
    /// testnet reports assets whose wallet balance is zero while `availableBalance` is not (fee
    /// credits and airdropped assets usable as cross margin), so both fields are consulted.
    ///
    /// An explicit zero row is *not* a reason to drop the asset from the account state: account
    /// updates overwrite balances per currency, so dropping a zero row would leave the previous
    /// non-zero amount cached forever after a withdrawal. This predicate reports the fact; it
    /// does not decide what the account state carries.
    ///
    /// An unparsable balance is unknown rather than zero, so it is reported as non-zero and the
    /// caller decides.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        matches!(
            (self.total(), self.free()),
            (Ok(total), Ok(free)) if total.is_zero() && free.is_zero()
        )
    }
}

/// An open position from `GET /fapi/v3/positionRisk`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterPositionRisk {
    /// Venue symbol.
    pub symbol: Ustr,
    /// Signed position quantity; negative for short.
    pub position_amt: String,
    /// Average entry price.
    pub entry_price: String,
    /// Current mark price.
    #[serde(default)]
    pub mark_price: Option<String>,
    /// Unrealized PnL.
    #[serde(default)]
    pub un_realized_profit: Option<String>,
    /// Liquidation price.
    #[serde(default)]
    pub liquidation_price: Option<String>,
    /// Applied leverage.
    #[serde(default)]
    pub leverage: Option<String>,
    /// Margin type.
    #[serde(default)]
    pub margin_type: Option<BinanceMarginType>,
    /// Position side (`BOTH` in one-way mode).
    #[serde(default)]
    pub position_side: Option<BinancePositionSide>,
    /// Notional position value.
    #[serde(default)]
    pub notional: Option<String>,
    /// Last update time in milliseconds.
    #[serde(default, deserialize_with = "flexible_opt_i64")]
    pub update_time: Option<i64>,
}

impl AsterPositionRisk {
    /// Returns the signed position quantity.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not a decimal number.
    pub fn signed_quantity(&self) -> anyhow::Result<Decimal> {
        parse_decimal(&self.position_amt, "positionAmt")
    }

    /// Returns whether the position is flat.
    #[must_use]
    pub fn is_flat(&self) -> bool {
        self.signed_quantity().map_or(true, |value| value.is_zero())
    }
}

/// An account trade (fill) from `GET /fapi/v3/userTrades`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterUserTrade {
    /// Venue symbol.
    pub symbol: Ustr,
    /// Trade ID.
    #[serde(deserialize_with = "flexible_i64")]
    pub id: i64,
    /// Venue order ID this trade belongs to.
    #[serde(deserialize_with = "flexible_i64")]
    pub order_id: i64,
    /// Fill price.
    pub price: String,
    /// Fill quantity.
    pub qty: String,
    /// Quote quantity transacted.
    #[serde(default)]
    pub quote_qty: Option<String>,
    /// Realized PnL.
    #[serde(default)]
    pub realized_pnl: Option<String>,
    /// Trade side.
    pub side: BinanceSide,
    /// Position side.
    #[serde(default)]
    pub position_side: Option<BinancePositionSide>,
    /// Whether this account provided liquidity.
    #[serde(default)]
    pub maker: bool,
    /// Whether this account was the buyer.
    #[serde(default)]
    pub buyer: bool,
    /// Commission paid.
    #[serde(default)]
    pub commission: Option<String>,
    /// Commission asset.
    #[serde(default)]
    pub commission_asset: Option<Ustr>,
    /// Trade time in milliseconds.
    #[serde(deserialize_with = "flexible_i64")]
    pub time: i64,
}

impl AsterUserTrade {
    /// Converts this trade into a Nautilus [`FillReport`].
    ///
    /// `settlement_currency` is used when the venue omits `commissionAsset`.
    ///
    /// # Errors
    ///
    /// Returns an error if the price, quantity, or commission cannot be parsed.
    pub fn to_fill_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        settlement_currency: Currency,
        ts_init: UnixNanos,
    ) -> anyhow::Result<FillReport> {
        // The commission asset is a venue string; `resolve_currency` registers codes the
        // Nautilus currency map does not know rather than panicking or silently mislabelling
        // the fee as the settlement currency.
        let commission_currency = match self.commission_asset {
            Some(asset) => resolve_currency(asset.as_str()),
            None => settlement_currency,
        };
        let commission_amount = match self.commission.as_deref() {
            Some(raw) => parse_decimal(raw, "commission")?,
            None => Decimal::ZERO,
        };

        Ok(FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::new(self.order_id.to_string()),
            TradeId::new(self.id.to_string()),
            parse_order_side(self.side),
            parse_quantity(&self.qty, size_precision, "qty")?,
            parse_price(&self.price, price_precision, "price")?,
            Money::from_decimal(commission_amount, commission_currency)?,
            if self.maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            None, // client_order_id: not reported by Aster on userTrades
            None, // venue_position_id: one-way mode only
            millis_to_nanos(self.time),
            ts_init,
            None, // report_id
        ))
    }
}

/// The account's commission rate for one symbol, from `GET /fapi/v3/commissionRate`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsterCommissionRate {
    /// Venue symbol.
    pub symbol: Ustr,
    /// Maker commission rate as a decimal fraction.
    pub maker_commission_rate: String,
    /// Taker commission rate as a decimal fraction.
    pub taker_commission_rate: String,
}

impl AsterCommissionRate {
    /// Returns the maker rate as a decimal.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not a decimal number.
    pub fn maker_rate(&self) -> anyhow::Result<Decimal> {
        parse_decimal(&self.maker_commission_rate, "makerCommissionRate")
    }

    /// Returns the taker rate as a decimal.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not a decimal number.
    pub fn taker_rate(&self) -> anyhow::Result<Decimal> {
        parse_decimal(&self.taker_commission_rate, "takerCommissionRate")
    }
}

// ------------------------------------------------------------------------------------------------
// Parsing helpers
// ------------------------------------------------------------------------------------------------

/// Converts a millisecond epoch timestamp into [`UnixNanos`], saturating on overflow.
#[must_use]
pub fn millis_to_nanos(millis: i64) -> UnixNanos {
    UnixNanos::from(millis.max(0) as u64 * 1_000_000)
}

/// Parses a decimal string, naming the field in the error.
///
/// # Errors
///
/// Returns an error if `raw` is not a decimal number.
pub fn parse_decimal(raw: &str, field: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(raw.trim())
        .map_err(|e| anyhow::anyhow!("Invalid Aster decimal for `{field}`: '{raw}': {e}"))
}

/// Parses a decimal string into a [`Price`] at the instrument's precision.
///
/// # Errors
///
/// Returns an error if `raw` is not a decimal number or does not fit the precision.
pub fn parse_price(raw: &str, precision: u8, field: &str) -> anyhow::Result<Price> {
    Price::from_decimal_dp(parse_decimal(raw, field)?, precision)
        .map_err(|e| anyhow::anyhow!("Invalid Aster price for `{field}`: '{raw}': {e}"))
}

/// Parses a decimal string into a [`Quantity`] at the instrument's precision.
///
/// # Errors
///
/// Returns an error if `raw` is not a decimal number or does not fit the precision.
pub fn parse_quantity(raw: &str, precision: u8, field: &str) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(parse_decimal(raw, field)?.abs(), precision)
        .map_err(|e| anyhow::anyhow!("Invalid Aster quantity for `{field}`: '{raw}': {e}"))
}

/// Maps an Aster order side onto the Nautilus enumeration.
#[must_use]
pub const fn parse_order_side(side: BinanceSide) -> OrderSide {
    match side {
        BinanceSide::Buy => OrderSide::Buy,
        BinanceSide::Sell => OrderSide::Sell,
    }
}

/// Maps an Aster order type onto the Nautilus enumeration.
///
/// Only `LIMIT` and `MARKET` are submitted by this adapter; the trigger types are mapped so
/// externally placed orders still reconcile.
#[must_use]
pub const fn parse_order_type(order_type: BinanceFuturesOrderType) -> OrderType {
    match order_type {
        BinanceFuturesOrderType::Market => OrderType::Market,
        BinanceFuturesOrderType::Stop => OrderType::StopLimit,
        BinanceFuturesOrderType::StopMarket => OrderType::StopMarket,
        BinanceFuturesOrderType::TakeProfit => OrderType::LimitIfTouched,
        BinanceFuturesOrderType::TakeProfitMarket | BinanceFuturesOrderType::TrailingStopMarket => {
            OrderType::MarketIfTouched
        }
        _ => OrderType::Limit,
    }
}

/// Maps an Aster time in force onto the Nautilus enumeration.
///
/// `GTX` is Aster's post-only flavour of `GTC`; the post-only intent is carried separately on
/// the report rather than in the time in force.
#[must_use]
pub const fn parse_time_in_force(tif: BinanceTimeInForce) -> TimeInForce {
    match tif {
        BinanceTimeInForce::Ioc => TimeInForce::Ioc,
        BinanceTimeInForce::Fok => TimeInForce::Fok,
        BinanceTimeInForce::Gtd => TimeInForce::Gtd,
        _ => TimeInForce::Gtc,
    }
}

/// Maps an Aster order status onto the Nautilus enumeration.
#[must_use]
pub const fn parse_order_status(
    status: BinanceOrderStatus,
    treat_expired_as_canceled: bool,
) -> OrderStatus {
    match status {
        BinanceOrderStatus::New => OrderStatus::Accepted,
        BinanceOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        BinanceOrderStatus::Filled => OrderStatus::Filled,
        BinanceOrderStatus::Canceled => OrderStatus::Canceled,
        BinanceOrderStatus::Rejected => OrderStatus::Rejected,
        BinanceOrderStatus::Expired => {
            if treat_expired_as_canceled {
                OrderStatus::Canceled
            } else {
                OrderStatus::Expired
            }
        }
        _ => OrderStatus::Accepted,
    }
}

/// Deserializes an integer that Aster may encode as a JSON number or a JSON string.
fn flexible_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Int(i64),
        Str(String),
    }

    match Repr::deserialize(deserializer)? {
        Repr::Int(value) => Ok(value),
        Repr::Str(value) => value
            .trim()
            .parse::<i64>()
            .map_err(serde::de::Error::custom),
    }
}

/// Deserializes an optional integer that Aster may encode as a number or a string.
fn flexible_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Int(i64),
        Str(String),
    }

    match Option::<Repr>::deserialize(deserializer)? {
        None => Ok(None),
        Some(Repr::Int(value)) => Ok(Some(value)),
        Some(Repr::Str(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<i64>()
                .map(Some)
                .map_err(serde::de::Error::custom)
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    /// Fixtures carry a `_source` header naming the CCXT file they were copied from; the
    /// payload under test is the `response` member.
    fn fixture(raw: &str) -> Value {
        let doc: Value = serde_json::from_str(raw).expect("fixture must be valid JSON");
        assert!(
            doc.get("_source").is_some(),
            "fixture must document its source"
        );
        doc.get("response").expect("fixture needs response").clone()
    }

    const ORDER: &str = include_str!("../../test_data/http_order.json");
    const OPEN_ORDERS: &str = include_str!("../../test_data/http_open_orders.json");
    const CANCEL_ORDER: &str = include_str!("../../test_data/http_cancel_order.json");
    const BALANCE: &str = include_str!("../../test_data/http_balance.json");
    const BALANCE_TESTNET: &str =
        include_str!("../../test_data/http_balance_testnet_unknown_assets.json");
    const POSITION_RISK: &str = include_str!("../../test_data/http_position_risk.json");
    const USER_TRADES: &str = include_str!("../../test_data/http_user_trades.json");
    const COMMISSION_RATE: &str = include_str!("../../test_data/http_commission_rate.json");
    const CANCEL_ALL: &str = include_str!("../../test_data/http_cancel_all_orders.json");

    fn account_id() -> AccountId {
        AccountId::from("ASTER-001")
    }

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.ASTER")
    }

    #[rstest]
    fn test_deserialize_order() {
        let order: AsterOrder = serde_json::from_value(fixture(ORDER)).unwrap();

        assert_eq!(order.symbol.as_str(), "BTCUSDT");
        assert_eq!(order.order_id, 1_917_641);
        assert_eq!(order.client_order_id, "abc");
        assert_eq!(order.status, BinanceOrderStatus::New);
        assert_eq!(order.side, BinanceSide::Buy);
        assert_eq!(order.time_in_force, BinanceTimeInForce::Gtc);
        assert_eq!(order.orig_qty, "0.40");
        assert_eq!(order.executed_qty, "0");
        assert_eq!(order.update_time, Some(1_579_276_756_075));
        assert!(!order.is_post_only());
    }

    #[rstest]
    fn test_deserialize_open_orders() {
        let orders: Vec<AsterOrder> = serde_json::from_value(fixture(OPEN_ORDERS)).unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, 1_917_641);
        assert_eq!(orders[0].status, BinanceOrderStatus::New);
    }

    #[rstest]
    fn test_deserialize_cancel_order_response_with_string_scalars() {
        // Aster returns `orderId` and `updateTime` as JSON strings here; Binance returns
        // numbers. The lenient scalar parsing is what makes this fixture load.
        let order: AsterOrder = serde_json::from_value(fixture(CANCEL_ORDER)).unwrap();

        assert_eq!(order.order_id, 417_663_664);
        assert_eq!(order.update_time, Some(1_776_318_949_284));
        assert_eq!(order.status, BinanceOrderStatus::Canceled);
        assert_eq!(order.client_order_id, "web_YrnsFvHisQA0cmCVr1Qr");
    }

    #[rstest]
    fn test_order_to_order_status_report() {
        let order: AsterOrder = serde_json::from_value(fixture(ORDER)).unwrap();

        let report = order
            .to_order_status_report(
                account_id(),
                instrument_id(),
                2,
                3,
                true,
                UnixNanos::from(42),
            )
            .unwrap();

        assert_eq!(report.account_id, account_id());
        assert_eq!(report.instrument_id, instrument_id());
        assert_eq!(report.venue_order_id, VenueOrderId::from("1917641"));
        assert_eq!(report.client_order_id, Some(ClientOrderId::from("abc")));
        assert_eq!(report.order_side, Some(OrderSide::Buy));
        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
        assert_eq!(report.quantity, Quantity::from("0.400"));
        assert_eq!(report.filled_qty, Quantity::from("0.000"));
        assert_eq!(report.ts_last, millis_to_nanos(1_579_276_756_075));
        assert!(!report.post_only);
        assert!(!report.reduce_only);
        // avg_price is "0.00000" with no fills, so no average is reported.
        assert_eq!(report.avg_px, None);
    }

    #[rstest]
    fn test_order_to_order_status_report_marks_post_only_for_gtx() {
        let mut order: AsterOrder = serde_json::from_value(fixture(ORDER)).unwrap();
        order.time_in_force = BinanceTimeInForce::Gtx;

        let report = order
            .to_order_status_report(
                account_id(),
                instrument_id(),
                2,
                3,
                true,
                UnixNanos::default(),
            )
            .unwrap();

        assert!(report.post_only);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
    }

    #[rstest]
    #[case(true, OrderStatus::Canceled)]
    #[case(false, OrderStatus::Expired)]
    fn test_expired_status_mapping(
        #[case] treat_expired_as_canceled: bool,
        #[case] expected: OrderStatus,
    ) {
        assert_eq!(
            parse_order_status(BinanceOrderStatus::Expired, treat_expired_as_canceled),
            expected
        );
    }

    #[rstest]
    fn test_deserialize_balances_with_string_update_time() {
        // Aster serializes `updateTime` as a string on this endpoint.
        let balances: Vec<AsterBalance> = serde_json::from_value(fixture(BALANCE)).unwrap();

        let usdt = balances
            .iter()
            .find(|b| b.asset.as_str() == "USDT")
            .expect("USDT balance in fixture");

        assert_eq!(usdt.total().unwrap().to_string(), "1.11341089");
        assert_eq!(usdt.free().unwrap().to_string(), "0.81341089");
        assert_eq!(usdt.update_time, Some(1_776_802_344_230));
        assert!(!usdt.is_zero());
        assert!(balances.iter().any(AsterBalance::is_zero));
    }

    #[rstest]
    fn test_deserialize_testnet_balance_with_unknown_assets() {
        let balances: Vec<AsterBalance> = serde_json::from_value(fixture(BALANCE_TESTNET)).unwrap();

        let codes: Vec<&str> = balances.iter().map(|b| b.asset.as_str()).collect();
        assert_eq!(codes, vec!["USDT", "BTC", "ASTER", "AFEE"]);

        let usdt = &balances[0];
        assert_eq!(usdt.total().unwrap().to_string(), "1000.00000000");
        assert_eq!(usdt.free().unwrap().to_string(), "1590.98089862");
        // Aster reports availableBalance above walletBalance on cross-margin accounts.
        assert!(usdt.free().unwrap() > usdt.total().unwrap());
        assert_eq!(usdt.update_time, Some(1_788_571_663_397));
    }

    #[rstest]
    fn test_zero_wallet_balance_with_available_funds_is_not_zero() {
        let balances: Vec<AsterBalance> = serde_json::from_value(fixture(BALANCE_TESTNET)).unwrap();

        // BTC and AFEE hold no wallet balance but carry available cross-margin funds; dropping
        // them would hide the assets from the account state entirely.
        for balance in &balances {
            assert!(!balance.is_zero(), "{} must be reported", balance.asset);
        }

        let fully_empty: AsterBalance = serde_json::from_str(
            r#"{"asset":"CDL","balance":"0.00000000","availableBalance":"0.00000000"}"#,
        )
        .unwrap();
        assert!(fully_empty.is_zero());
    }

    #[rstest]
    fn test_unparsable_balance_is_unknown_rather_than_zero() {
        // An unparsable amount says nothing about the asset holding nothing, so it must not
        // masquerade as an explicit zero row.
        let broken: AsterBalance =
            serde_json::from_str(r#"{"asset":"USDT","balance":"not-a-number"}"#).unwrap();

        assert!(!broken.is_zero());
        assert!(broken.total().is_err());
    }

    #[rstest]
    fn test_commission_asset_unknown_to_nautilus_does_not_panic() {
        let trade: AsterUserTrade = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","id":1,"orderId":2,"price":"100.0","qty":"1.0",
                "side":"BUY","maker":false,"buyer":true,"commission":"0.5",
                "commissionAsset":"AFEE","time":1788571663397}"#,
        )
        .unwrap();

        let report = trade
            .to_fill_report(
                account_id(),
                instrument_id(),
                2,
                3,
                Currency::USDT(),
                UnixNanos::default(),
            )
            .unwrap();

        assert_eq!(report.commission.currency.code.as_str(), "AFEE");
        assert_eq!(report.commission.as_decimal().to_string(), "0.50000000");
    }

    #[rstest]
    fn test_balance_free_falls_back_to_total() {
        let balance: AsterBalance =
            serde_json::from_str(r#"{"asset":"USDT","balance":"5.5"}"#).unwrap();

        assert_eq!(balance.free().unwrap(), balance.total().unwrap());
        assert_eq!(balance.update_time, None);
    }

    #[rstest]
    fn test_deserialize_position_risk() {
        let positions: Vec<AsterPositionRisk> =
            serde_json::from_value(fixture(POSITION_RISK)).unwrap();

        assert_eq!(positions.len(), 1);
        let position = &positions[0];
        assert_eq!(position.symbol.as_str(), "BTCUSDT");
        assert_eq!(position.signed_quantity().unwrap().to_string(), "20.000");
        assert_eq!(position.entry_price, "6563.66500");
        assert_eq!(position.leverage.as_deref(), Some("10"));
        assert_eq!(position.margin_type, Some(BinanceMarginType::Isolated));
        assert!(!position.is_flat());
    }

    #[rstest]
    fn test_position_risk_flat_detection() {
        let position: AsterPositionRisk = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","positionAmt":"0.000","entryPrice":"0.0"}"#,
        )
        .unwrap();

        assert!(position.is_flat());
    }

    #[rstest]
    fn test_deserialize_user_trades_and_fill_report() {
        let trades: Vec<AsterUserTrade> = serde_json::from_value(fixture(USER_TRADES)).unwrap();

        assert_eq!(trades.len(), 1);
        let trade = &trades[0];
        assert_eq!(trade.id, 698_759);
        assert_eq!(trade.order_id, 25_851_813);
        assert!(!trade.maker);

        let report = trade
            .to_fill_report(
                account_id(),
                instrument_id(),
                2,
                3,
                Currency::USDT(),
                UnixNanos::default(),
            )
            .unwrap();

        assert_eq!(report.venue_order_id, VenueOrderId::from("25851813"));
        assert_eq!(report.trade_id, TradeId::from("698759"));
        assert_eq!(report.order_side, OrderSide::Sell);
        assert_eq!(report.last_px, Price::from("7819.01"));
        assert_eq!(report.last_qty, Quantity::from("0.002"));
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
        assert_eq!(
            report.commission,
            Money::new(-0.078_190_10, Currency::USDT())
        );
        assert_eq!(report.ts_event, millis_to_nanos(1_569_514_978_020));
    }

    #[rstest]
    fn test_fill_report_falls_back_to_settlement_currency() {
        let trade: AsterUserTrade = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","id":"1","orderId":"2","price":"100.0","qty":"1.0",
                "side":"BUY","maker":true,"time":"1569514978020"}"#,
        )
        .unwrap();

        let report = trade
            .to_fill_report(
                account_id(),
                instrument_id(),
                2,
                3,
                Currency::USDT(),
                UnixNanos::default(),
            )
            .unwrap();

        assert_eq!(report.commission, Money::new(0.0, Currency::USDT()));
        assert_eq!(report.liquidity_side, LiquiditySide::Maker);
    }

    #[rstest]
    fn test_deserialize_commission_rate() {
        let rate: AsterCommissionRate = serde_json::from_value(fixture(COMMISSION_RATE)).unwrap();

        assert_eq!(rate.symbol.as_str(), "BTCUSDT");
        assert_eq!(rate.maker_rate().unwrap().to_string(), "0.000100");
        assert_eq!(rate.taker_rate().unwrap().to_string(), "0.000350");
    }

    #[rstest]
    fn test_deserialize_cancel_all_orders_with_string_code() {
        // Aster returns `"code": "200"`; Binance returns `"code": 200`.
        let response: AsterCancelAllOrdersResponse =
            serde_json::from_value(fixture(CANCEL_ALL)).unwrap();

        assert_eq!(response.code, 200);
        assert!(response.is_success());
        assert!(response.msg.contains("cancel all open order"));
    }

    #[rstest]
    fn test_deserialize_cancel_all_orders_with_numeric_code() {
        let response: AsterCancelAllOrdersResponse =
            serde_json::from_str(r#"{"code": 200, "msg": "ok"}"#).unwrap();

        assert!(response.is_success());
    }

    #[rstest]
    fn test_deserialize_cancel_all_orders_rejects_non_numeric_code() {
        assert!(
            serde_json::from_str::<AsterCancelAllOrdersResponse>(r#"{"code":"ok","msg":"x"}"#)
                .is_err()
        );
    }

    #[rstest]
    fn test_deserialize_error_response() {
        let error: AsterErrorResponse = serde_json::from_str(
            r#"{"code":-5050,"msg":"This function can only be used after deposit"}"#,
        )
        .unwrap();

        assert_eq!(error.code, -5050);
        assert!(error.msg.contains("after deposit"));
    }

    #[rstest]
    fn test_deserialize_listen_key_and_position_mode() {
        let key: AsterListenKeyResponse =
            serde_json::from_str(r#"{"listenKey":"pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv"}"#)
                .unwrap();
        let mode: AsterPositionModeResponse =
            serde_json::from_str(r#"{"dualSidePosition":false}"#).unwrap();

        assert_eq!(key.listen_key.len(), 44);
        assert!(!mode.dual_side_position);
    }

    #[rstest]
    #[case(BinanceFuturesOrderType::Limit, OrderType::Limit)]
    #[case(BinanceFuturesOrderType::Market, OrderType::Market)]
    #[case(BinanceFuturesOrderType::StopMarket, OrderType::StopMarket)]
    fn test_order_type_mapping(
        #[case] venue: BinanceFuturesOrderType,
        #[case] expected: OrderType,
    ) {
        assert_eq!(parse_order_type(venue), expected);
    }

    #[rstest]
    #[case(BinanceTimeInForce::Gtc, TimeInForce::Gtc)]
    #[case(BinanceTimeInForce::Gtx, TimeInForce::Gtc)]
    #[case(BinanceTimeInForce::Ioc, TimeInForce::Ioc)]
    #[case(BinanceTimeInForce::Fok, TimeInForce::Fok)]
    fn test_time_in_force_mapping(
        #[case] venue: BinanceTimeInForce,
        #[case] expected: TimeInForce,
    ) {
        assert_eq!(parse_time_in_force(venue), expected);
    }

    #[rstest]
    fn test_parse_helpers_report_the_offending_field() {
        let error = parse_decimal("abc", "price").unwrap_err().to_string();

        assert!(error.contains("price"), "{error}");
        assert!(error.contains("abc"), "{error}");
    }
}
