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

//! Closed read operations and validated scalar parameters.

use crate::{
    http::error::{BackpackHttpError, BackpackHttpErrorKind},
    signing::{BackpackParameters, BackpackScalar},
};

/// Supported GET operations; no mutation method or arbitrary URL is exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackReadOperation {
    Markets,
    Market,
    Depth,
    MarkPrices,
    FundingRates,
    RecentTrades,
    HistoricalTrades,
    Account,
    Balances,
    Collateral,
    Positions,
    OpenOrders,
    OpenOrder,
    OrderHistory,
    FillHistory,
    FundingHistory,
}
impl BackpackReadOperation {
    pub(crate) const fn path(self) -> &'static str {
        match self {
            Self::Markets => "/api/v1/markets",
            Self::Market => "/api/v1/market",
            Self::Depth => "/api/v1/depth",
            Self::MarkPrices => "/api/v1/markPrices",
            Self::FundingRates => "/api/v1/fundingRates",
            Self::RecentTrades => "/api/v1/trades",
            Self::HistoricalTrades => "/api/v1/trades/history",
            Self::Account => "/api/v1/account",
            Self::Balances => "/api/v1/capital",
            Self::Collateral => "/api/v1/capital/collateral",
            Self::Positions => "/api/v1/position",
            Self::OpenOrders => "/api/v1/orders",
            Self::OpenOrder => "/api/v1/order",
            Self::OrderHistory => "/wapi/v1/history/orders",
            Self::FillHistory => "/wapi/v1/history/fills",
            Self::FundingHistory => "/wapi/v1/history/funding",
        }
    }
    pub(crate) const fn instruction(self) -> Option<&'static str> {
        match self {
            Self::Account => Some("accountQuery"),
            Self::Balances => Some("balanceQuery"),
            Self::Collateral => Some("collateralQuery"),
            Self::Positions => Some("positionQuery"),
            Self::OpenOrders => Some("orderQueryAll"),
            Self::OpenOrder => Some("orderQuery"),
            Self::OrderHistory => Some("orderHistoryQueryAll"),
            Self::FillHistory => Some("fillHistoryQueryAll"),
            Self::FundingHistory => Some("fundingHistoryQueryAll"),
            _ => None,
        }
    }
    pub(crate) const fn historical_market(self) -> bool {
        matches!(self, Self::FundingRates | Self::HistoricalTrades)
    }
    const fn allowed_keys(self) -> &'static [&'static str] {
        match self {
            Self::Account | Self::Balances | Self::Collateral | Self::Markets => &[],
            Self::Market => &["symbol"],
            Self::MarkPrices => &["symbol", "marketType"],
            Self::Depth | Self::RecentTrades => &["symbol", "limit"],
            Self::FundingRates | Self::HistoricalTrades => &["symbol", "limit", "offset"],
            Self::Positions | Self::OpenOrders => &["symbol", "marketType"],
            Self::OpenOrder => &["symbol", "orderId", "clientId"],
            Self::OrderHistory => &["symbol", "orderId", "limit", "offset", "sortDirection"],
            Self::FillHistory => &[
                "symbol",
                "orderId",
                "limit",
                "offset",
                "sortDirection",
                "from",
                "to",
            ],
            Self::FundingHistory => &["symbol", "limit", "offset", "sortDirection"],
        }
    }
}

/// An immutable read request validated before any network operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpackReadRequest {
    pub(crate) operation: BackpackReadOperation,
    pub(crate) parameters: BackpackParameters,
}
impl BackpackReadRequest {
    /// Validates operation-specific keys, scalar types and documented limits.
    ///
    /// Array filters and advanced products are outside this first protocol slice.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported keys, missing required fields or invalid values.
    pub fn new(
        operation: BackpackReadOperation,
        parameters: BackpackParameters,
    ) -> Result<Self, BackpackHttpError> {
        let invalid = || BackpackHttpError::local(BackpackHttpErrorKind::Validation);
        for key in parameters.keys() {
            if !operation.allowed_keys().contains(&key) {
                return Err(invalid());
            }
            let value = parameters.get(key).ok_or_else(invalid)?;
            let valid = match (key, value) {
                ("symbol", BackpackScalar::Token(symbol)) => {
                    symbol.strip_suffix("_USDC_PERP").is_some_and(|base| {
                        !base.is_empty()
                            && base
                                .bytes()
                                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                    })
                }
                ("orderId", BackpackScalar::Token(_)) => true,
                ("marketType", BackpackScalar::Token(kind)) => kind == "PERP",
                ("sortDirection", BackpackScalar::Token(direction)) => {
                    ["Asc", "Desc"].contains(&direction.as_str())
                }
                ("clientId", BackpackScalar::Unsigned(value)) => u32::try_from(*value).is_ok(),
                ("offset", BackpackScalar::Unsigned(_)) => true,
                ("from" | "to", BackpackScalar::Signed(value)) => *value >= 0,
                ("limit", BackpackScalar::Unsigned(value)) => {
                    if operation == BackpackReadOperation::Depth {
                        [5, 10, 20, 50, 100, 500, 1000].contains(value)
                    } else {
                        *value > 0
                            && *value
                                <= if operation == BackpackReadOperation::FundingRates {
                                    10_000
                                } else {
                                    1_000
                                }
                    }
                }
                _ => false,
            };
            if !valid {
                return Err(invalid());
            }
        }
        if matches!(
            operation,
            BackpackReadOperation::Market
                | BackpackReadOperation::Depth
                | BackpackReadOperation::FundingRates
                | BackpackReadOperation::RecentTrades
                | BackpackReadOperation::HistoricalTrades
                | BackpackReadOperation::OpenOrder
        ) && parameters.get("symbol").is_none()
        {
            return Err(invalid());
        }
        if operation == BackpackReadOperation::OpenOrder
            && parameters.get("orderId").is_some() == parameters.get("clientId").is_some()
        {
            return Err(invalid());
        }
        if let (Some(BackpackScalar::Signed(from)), Some(BackpackScalar::Signed(to))) =
            (parameters.get("from"), parameters.get("to"))
            && from >= to
        {
            return Err(invalid());
        }
        if operation == BackpackReadOperation::HistoricalTrades {
            let offset = match parameters.get("offset") {
                Some(BackpackScalar::Unsigned(v)) => *v,
                _ => 0,
            };
            let limit = match parameters.get("limit") {
                Some(BackpackScalar::Unsigned(v)) => *v,
                _ => 100,
            };
            if offset.checked_add(limit).is_none_or(|value| value > 10_000) {
                return Err(invalid());
            }
        }
        Ok(Self {
            operation,
            parameters,
        })
    }
    /// Returns whether this request requires authentication.
    #[must_use]
    pub const fn is_authenticated(&self) -> bool {
        self.operation.instruction().is_some()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    #[rstest]
    fn test_exclusive_identifier_and_u32_bound() {
        let mut p = BackpackParameters::default();
        p.insert(
            "symbol",
            Some(BackpackScalar::Token("BTC_USDC_PERP".into())),
        )
        .unwrap();
        assert!(BackpackReadRequest::new(BackpackReadOperation::OpenOrder, p.clone()).is_err());
        p.insert("clientId", Some(BackpackScalar::Unsigned(u32::MAX.into())))
            .unwrap();
        assert!(BackpackReadRequest::new(BackpackReadOperation::OpenOrder, p.clone()).is_ok());
        p.insert("orderId", Some(BackpackScalar::Token("1".into())))
            .unwrap();
        assert!(BackpackReadRequest::new(BackpackReadOperation::OpenOrder, p).is_err());
    }
    #[rstest]
    #[case(BackpackReadOperation::Markets)]
    #[case(BackpackReadOperation::OrderHistory)]
    #[case(BackpackReadOperation::FillHistory)]
    fn test_array_market_filter_not_accepted_as_scalar(#[case] operation: BackpackReadOperation) {
        let mut p = BackpackParameters::default();
        p.insert("marketType", Some(BackpackScalar::Token("PERP".into())))
            .unwrap();
        assert!(BackpackReadRequest::new(operation, p).is_err());
    }
    #[rstest]
    fn test_mutation_key_and_ambiguous_scope_refused() {
        let mut p = BackpackParameters::default();
        p.insert("autoBorrow", Some(BackpackScalar::Boolean(true)))
            .unwrap();
        assert!(BackpackReadRequest::new(BackpackReadOperation::Balances, p).is_err());
        let mut p = BackpackParameters::default();
        p.insert(
            "clientId",
            Some(BackpackScalar::Unsigned(u64::from(u32::MAX) + 1)),
        )
        .unwrap();
        p.insert(
            "symbol",
            Some(BackpackScalar::Token("BTC_USDC_PERP".into())),
        )
        .unwrap();
        assert!(BackpackReadRequest::new(BackpackReadOperation::OpenOrder, p).is_err());
    }
}
