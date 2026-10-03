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

//! Validated restoration of an execution client's durable native consumer state.

use std::collections::{HashMap, HashSet};

use nautilus_common::{cache::Cache, clients::ExecutionCacheRecovery};
use nautilus_model::{
    enums::{OmsType, OrderStatus},
    identifiers::{AccountId, ClientId, TraderId, Venue},
    instruments::Instrument,
    orders::Order,
};

#[derive(Clone, Copy)]
pub(super) struct RecoveryScope {
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub trader_id: TraderId,
    pub venue: Venue,
    pub oms_type: OmsType,
}

pub(super) fn restore_execution_cache(
    cache: &mut Cache,
    recovery: ExecutionCacheRecovery,
    scope: RecoveryScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        recovery.account.id() == scope.account_id,
        "durable execution account differs from client account"
    );
    anyhow::ensure!(
        cache.account(&scope.account_id).is_none()
            && cache
                .orders(None, None, None, Some(&scope.account_id), None)
                .is_empty()
            && cache
                .positions(None, None, None, Some(&scope.account_id), None)
                .is_empty(),
        "durable execution recovery requires an empty account scope"
    );
    let mut instruments = HashSet::new();
    for instrument in &recovery.instruments {
        anyhow::ensure!(
            instrument.id().venue == scope.venue && instruments.insert(instrument.id()),
            "durable execution instrument is duplicated or outside client venue"
        );
        if let Some(existing) = cache.instrument(&instrument.id()) {
            anyhow::ensure!(
                serde_json::to_value(existing)? == serde_json::to_value(instrument)?,
                "durable execution instrument conflicts with existing metadata"
            );
        }
    }
    let mut orders = HashSet::new();
    let mut venue_orders = HashMap::new();
    for order in &recovery.orders {
        anyhow::ensure!(
            (order.account_id() == Some(scope.account_id)
                || (order.account_id().is_none()
                    && order.status() == OrderStatus::Initialized
                    && order.filled_qty().is_zero()
                    && order.venue_order_id().is_none()))
                && order.trader_id() == scope.trader_id
                && instruments.contains(&order.instrument_id()),
            "durable execution order is outside recovered account, trader or instrument scope"
        );
        anyhow::ensure!(
            orders.insert(order.client_order_id()) && !cache.order_exists(&order.client_order_id()),
            "durable execution order conflicts with an existing identity"
        );
        for id in order
            .venue_order_ids()
            .into_iter()
            .copied()
            .chain(order.venue_order_id())
        {
            let previous = venue_orders.insert(id, order.client_order_id());
            anyhow::ensure!(
                previous.is_none_or(|owner| owner == order.client_order_id()),
                "duplicate durable venue order identity"
            );
            anyhow::ensure!(
                cache.client_order_id(&id).is_none(),
                "durable venue order identity already belongs to a cached order"
            );
        }
    }
    let mut positions = HashSet::new();
    for position in &recovery.positions {
        anyhow::ensure!(
            position.account_id == scope.account_id
                && position.trader_id == scope.trader_id
                && instruments.contains(&position.instrument_id)
                && orders.contains(&position.opening_order_id)
                && position
                    .closing_order_id
                    .is_none_or(|id| orders.contains(&id)),
            "durable execution position has an invalid owner or missing order"
        );
        anyhow::ensure!(
            positions.insert(position.id) && !cache.position_exists(&position.id),
            "durable execution position conflicts with an existing identity"
        );
        for fill in &position.events {
            anyhow::ensure!(
                fill.account_id == scope.account_id
                    && fill.trader_id == scope.trader_id
                    && fill.instrument_id == position.instrument_id
                    && orders.contains(&fill.client_order_id),
                "durable position fill is outside recovered execution scope"
            );
        }
    }
    for order in &recovery.orders {
        if let Some(id) = order.position_id() {
            anyhow::ensure!(
                positions.contains(&id),
                "durable order references a missing position"
            );
        }
    }

    // Validate the entire scope before applying any object. A backing-store failure
    // aborts node construction rather than admitting a partially recovered client.
    for instrument in recovery.instruments {
        if cache.instrument(&instrument.id()).is_none() {
            cache.add_instrument(instrument)?;
        }
    }
    cache.add_account(recovery.account)?;
    for order in recovery.orders {
        let position_id = order.position_id();
        let client_order_id = order.client_order_id();
        let current_venue_order_id = order.venue_order_id();
        let aliases: Vec<_> = order.venue_order_ids().into_iter().copied().collect();
        cache.add_order(order, position_id, Some(scope.client_id), false)?;
        if let Some(id) = current_venue_order_id {
            cache.add_venue_order_id(&client_order_id, &id, false)?;
        }
        for id in aliases {
            cache.index_venue_order_id(&client_order_id, &id)?;
        }
    }
    for position in recovery.positions {
        cache.add_position(&position, scope.oms_type)?;
        if position.is_closed() {
            cache.update_position(&position)?;
        }
    }
    cache.build_index();
    anyhow::ensure!(
        cache.check_integrity(),
        "durable execution cache indices are inconsistent"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        accounts::AccountAny,
        enums::{OrderSide, OrderType},
        identifiers::{PositionId, TradeId, VenueOrderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
        orders::{builder::OrderTestBuilder, stubs::TestOrderEventStubs},
        position::Position,
        types::{Money, Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn fixture() -> (ExecutionCacheRecovery, RecoveryScope) {
        let account = AccountAny::default();
        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let mut order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1_000))
            .build();
        order
            .apply(TestOrderEventStubs::submitted(&order, account.id()))
            .unwrap();
        order
            .apply(TestOrderEventStubs::accepted(
                &order,
                account.id(),
                VenueOrderId::from("RECOVERED-1"),
            ))
            .unwrap();
        let fill = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("TRUE-1")),
            Some(PositionId::from("RECOVERY-POSITION")),
            Some(Price::from("1.00000")),
            Some(Quantity::from(500)),
            None,
            Some(Money::from("-0.01 USD")),
            None,
            Some(account.id()),
        );
        order.apply(fill.clone()).unwrap();
        let position = Position::new(&instrument, fill.into());
        let scope = RecoveryScope {
            account_id: account.id(),
            client_id: ClientId::from("SIM"),
            trader_id: order.trader_id(),
            venue: instrument.id().venue,
            oms_type: OmsType::Netting,
        };
        (
            ExecutionCacheRecovery {
                account,
                instruments: vec![instrument],
                orders: vec![order],
                positions: vec![position],
            },
            scope,
        )
    }

    #[rstest]
    fn restores_exact_native_economics_and_indices() {
        let (state, scope) = fixture();
        let order_id = state.orders[0].client_order_id();
        let position_id = state.positions[0].id;
        let expected = serde_json::to_value(&state.positions[0]).unwrap();
        let mut cache = Cache::default();
        restore_execution_cache(&mut cache, state, scope).unwrap();
        assert_eq!(
            cache.client_order_id(&VenueOrderId::from("RECOVERED-1")),
            Some(&order_id)
        );
        assert_eq!(cache.position_id(&order_id), Some(&position_id));
        assert_eq!(
            serde_json::to_value(cache.position_owned(&position_id).unwrap()).unwrap(),
            expected
        );
        assert!(cache.check_integrity());
    }

    #[rstest]
    #[case("account")]
    #[case("trader")]
    #[case("venue")]
    #[case("missing_order")]
    #[case("missing_position")]
    #[case("duplicate_order")]
    #[case("duplicate_position")]
    fn invalid_recovery_is_refused_before_any_cache_write(#[case] fault: &str) {
        let (mut state, mut scope) = fixture();
        match fault {
            "account" => scope.account_id = AccountId::from("SIM-OTHER"),
            "trader" => scope.trader_id = TraderId::from("OTHER-001"),
            "venue" => scope.venue = Venue::from("OTHER"),
            "missing_order" => state.orders.clear(),
            "missing_position" => state.positions.clear(),
            "duplicate_order" => state.orders.push(state.orders[0].clone()),
            "duplicate_position" => state.positions.push(state.positions[0].clone()),
            _ => unreachable!(),
        }
        let account_id = state.account.id();
        let mut cache = Cache::default();
        assert!(restore_execution_cache(&mut cache, state, scope).is_err());
        assert!(cache.account(&account_id).is_none());
        assert!(cache.orders(None, None, None, None, None).is_empty());
        assert!(cache.positions(None, None, None, None, None).is_empty());
    }

    #[rstest]
    fn recovery_never_overwrites_existing_account_state() {
        let (state, scope) = fixture();
        let account_id = state.account.id();
        let mut cache = Cache::default();
        cache.add_account(state.account.clone()).unwrap();
        assert!(restore_execution_cache(&mut cache, state, scope).is_err());
        assert!(cache.account(&account_id).is_some());
        assert!(cache.orders(None, None, None, None, None).is_empty());
    }
}
