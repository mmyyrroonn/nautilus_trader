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

//! Exact translation of the restricted native single-order command surface.

use nautilus_common::messages::execution::{CancelOrder, SubmitOrder};
use nautilus_model::{
    enums::OrderStatus,
    identifiers::{ClientId, TraderId, VenueOrderId},
    orders::{Order, OrderAny},
};

use crate::execution::command::BackpackOrderSpec;

pub(crate) fn submit_spec(
    command: &SubmitOrder,
    cached: &OrderAny,
    trader: TraderId,
    client: ClientId,
) -> anyhow::Result<BackpackOrderSpec> {
    let init = &command.order_init;
    anyhow::ensure!(
        command.trader_id == trader
            && command.client_id.is_none_or(|id| id == client)
            && command.strategy_id == cached.strategy_id()
            && command.instrument_id == cached.instrument_id()
            && command.client_order_id == cached.client_order_id()
            && init.trader_id == trader
            && init.strategy_id == command.strategy_id
            && init.instrument_id == command.instrument_id
            && init.client_order_id == command.client_order_id,
        "native submit identity mismatch"
    );
    anyhow::ensure!(
        cached.init_event() == init
            && matches!(
                cached.status(),
                OrderStatus::Initialized | OrderStatus::Released
            ),
        "native submit differs from original cached order"
    );
    anyhow::ensure!(
        command.params.is_none()
            && command.exec_algorithm_id.is_none()
            && command.position_id.is_none(),
        "unsupported native command parameters"
    );
    cached_spec(cached)
}

pub(crate) fn cached_spec(cached: &OrderAny) -> anyhow::Result<BackpackOrderSpec> {
    let init = cached.init_event();
    anyhow::ensure!(
        !init.quote_quantity
            && !init.reconciliation
            && init.activation_price.is_none()
            && init.trigger_price.is_none()
            && init.trigger_type.is_none()
            && init.limit_offset.is_none()
            && init.trailing_offset.is_none()
            && init.trailing_offset_type.is_none()
            && init.expire_time.is_none()
            && init.display_qty.is_none()
            && init.emulation_trigger.is_none()
            && init.trigger_instrument_id.is_none()
            && init.contingency_type.is_none()
            && init.order_list_id.is_none()
            && init.linked_order_ids.is_none()
            && init.parent_order_id.is_none()
            && init.exec_algorithm_id.is_none()
            && init.exec_algorithm_params.is_none()
            && init.exec_spawn_id.is_none(),
        "unsupported native submit semantics"
    );
    Ok(BackpackOrderSpec {
        client_order_id: init.client_order_id,
        instrument_id: init.instrument_id,
        side: init.order_side,
        order_type: init.order_type,
        time_in_force: init.time_in_force,
        quantity: init.quantity.as_decimal(),
        price: init.price.map(|value| value.as_decimal()),
        post_only: init.post_only,
        reduce_only: init.reduce_only,
    })
}

pub(crate) fn validate_cancel(
    command: &CancelOrder,
    cached: &OrderAny,
    bound_venue: VenueOrderId,
    trader: TraderId,
    client: ClientId,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        command.trader_id == trader
            && command.client_id.is_none_or(|id| id == client)
            && cached.trader_id() == trader
            && command.strategy_id == cached.strategy_id()
            && command.instrument_id == cached.instrument_id()
            && command.client_order_id == cached.client_order_id()
            && command.venue_order_id.is_none_or(|id| id == bound_venue)
            && cached.venue_order_id().is_none_or(|id| id == bound_venue)
            && command.params.is_none(),
        "native cancellation does not match the independently bound owned order"
    );
    Ok(())
}
