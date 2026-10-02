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

//! Native command validation and a closed scalar wire encoding.

use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{BackpackExecutionError, BackpackExecutionErrorKind};
use crate::{
    config::BackpackConfig,
    instruments::BackpackInstrumentMetadata,
    models::BackpackMarket,
    parsing::parse_market,
    signing::{BackpackParameters, BackpackScalar},
};

/// Immutable single-order input. Unrepresented advanced flags have no wire path.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackpackOrderSpec {
    pub client_order_id: ClientOrderId,
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub post_only: bool,
    pub reduce_only: bool,
}

impl BackpackOrderSpec {
    pub(crate) fn validate(
        &self,
        metadata: &BackpackInstrumentMetadata,
        config: &BackpackConfig,
    ) -> Result<(), BackpackExecutionError> {
        use BackpackExecutionErrorKind::{Ownership, UnsupportedCommand, Validation};
        if self.client_order_id.is_external() {
            return Err(Ownership.into());
        }
        // Metadata is a public data container: reparse the retained venue facts rather than trust mutable fields.
        let market: BackpackMarket = serde_json::from_value(metadata.raw_metadata.clone())
            .map_err(|_| BackpackExecutionError::from(Validation))?;
        let checked = parse_market(&market, config, metadata.received_at)
            .map_err(|_| BackpackExecutionError::from(Validation))?;
        if self.instrument_id != checked.instrument_id
            || metadata.instrument_id != checked.instrument_id
            || metadata.raw_symbol != checked.raw_symbol
            || metadata.base_currency != checked.base_currency
            || metadata.quote_currency != checked.quote_currency
            || metadata.price_increment != checked.price_increment
            || metadata.size_increment != checked.size_increment
            || metadata.min_quantity != checked.min_quantity
            || metadata.max_quantity != checked.max_quantity
            || metadata.min_price != checked.min_price
            || metadata.max_price != checked.max_price
        {
            return Err(Validation.into());
        }
        if !matches!(self.side, OrderSide::Buy | OrderSide::Sell)
            || !matches!(self.order_type, OrderType::Market | OrderType::Limit)
            || !matches!(
                self.time_in_force,
                TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Fok
            )
            || (self.post_only
                && (self.order_type != OrderType::Limit || self.time_in_force != TimeInForce::Gtc))
            || (self.order_type == OrderType::Market
                && (!self.reduce_only
                    || self.price.is_some()
                    || self.time_in_force == TimeInForce::Gtc))
        {
            return Err(UnsupportedCommand.into());
        }
        if self.quantity < checked.min_quantity.as_decimal()
            || checked
                .max_quantity
                .is_some_and(|max| self.quantity > max.as_decimal())
            || self
                .quantity
                .checked_rem(checked.size_increment.as_decimal())
                != Some(Decimal::ZERO)
        {
            return Err(Validation.into());
        }
        if self.order_type == OrderType::Limit {
            let price = self
                .price
                .ok_or_else(|| BackpackExecutionError::from(Validation))?;
            if price < checked.min_price.as_decimal()
                || checked
                    .max_price
                    .is_some_and(|max| price > max.as_decimal())
                || price.checked_rem(checked.price_increment.as_decimal()) != Some(Decimal::ZERO)
            {
                return Err(Validation.into());
            }
        }
        Ok(())
    }

    pub(crate) fn parameters(
        &self,
        symbol: &str,
        client_id: Option<u32>,
    ) -> Result<BackpackParameters, BackpackExecutionError> {
        let mut values = BackpackParameters::default();
        for (key, value) in [
            ("symbol", Some(BackpackScalar::Token(symbol.to_owned()))),
            (
                "side",
                Some(BackpackScalar::Token(
                    if self.side == OrderSide::Buy {
                        "Bid"
                    } else {
                        "Ask"
                    }
                    .into(),
                )),
            ),
            (
                "orderType",
                Some(BackpackScalar::Token(
                    if self.order_type == OrderType::Limit {
                        "Limit"
                    } else {
                        "Market"
                    }
                    .into(),
                )),
            ),
            (
                "timeInForce",
                Some(BackpackScalar::Token(
                    match self.time_in_force {
                        TimeInForce::Gtc => "GTC",
                        TimeInForce::Ioc => "IOC",
                        _ => "FOK",
                    }
                    .into(),
                )),
            ),
            ("quantity", Some(BackpackScalar::Decimal(self.quantity))),
            ("price", self.price.map(BackpackScalar::Decimal)),
            (
                "clientId",
                client_id.map(|id| BackpackScalar::Unsigned(u64::from(id))),
            ),
            ("postOnly", Some(BackpackScalar::Boolean(self.post_only))),
            (
                "reduceOnly",
                Some(BackpackScalar::Boolean(self.reduce_only)),
            ),
        ] {
            values.insert(key, value)?;
        }
        Ok(values)
    }
}
