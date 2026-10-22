#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Exercise Arcus execution with the built-in ExecTester strategy.

Defaults to testnet and dry-run. Register and fund a testnet API key, export
ARCUS_TESTNET_ADDRESS and ARCUS_TESTNET_API_SECRET, then set DRY_RUN to False when order
submission is intended. Switching both configs to MAINNET selects real funds and the
separate ARCUS_ADDRESS/ARCUS_API_SECRET credentials.

"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.adapters.arcus import ArcusDataClientConfig
from nautilus_trader.adapters.arcus import ArcusDataClientFactory
from nautilus_trader.adapters.arcus import ArcusEnvironment
from nautilus_trader.adapters.arcus import ArcusExecutionClientConfig
from nautilus_trader.adapters.arcus import ArcusExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import ClientId
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Quantity
from nautilus_trader.model import StrategyId
from nautilus_trader.model import TimeInForce
from nautilus_trader.model import TraderId
from nautilus_trader.testkit import ExecTesterConfig


# WARNING: With DRY_RUN = True, this tester submits orders to the configured
# environment and may use real funds. Set DRY_RUN = True to connect without
# submitting orders or sending shutdown cancel/close commands.
DRY_RUN = True
ARCUS = "ARCUS"
TRADER_ID = TraderId.from_str("TESTER-001")
ACCOUNT_ID = AccountId.from_str("ARCUS-001")
STRATEGY_ID = StrategyId.from_str("EXEC_TESTER-001")
INSTRUMENT_ID = InstrumentId.from_str(f"BTC-USD-PERP.{ARCUS}")
ORDER_QTY = "0.0001"
TOB_OFFSET_TICKS = 500


def main() -> None:
    """
    Run the example.
    """
    node = (
        LiveNode.builder("ARCUS-EXEC-TESTER-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(reconciliation=True)
        .add_data_client(
            None,
            ArcusDataClientFactory(),
            ArcusDataClientConfig(environment=ArcusEnvironment.TESTNET),
        )
        .add_exec_client(
            None,
            ArcusExecutionClientFactory(),
            ArcusExecutionClientConfig(
                account_id=ACCOUNT_ID,
                environment=ArcusEnvironment.TESTNET,
            ),
        )
        .build()
    )
    node.add_builtin_strategy(
        "ExecTester",
        ExecTesterConfig(
            strategy_id=STRATEGY_ID,
            instrument_id=INSTRUMENT_ID,
            client_id=ClientId.from_str(ARCUS),
            external_order_claims=[INSTRUMENT_ID],
            order_qty=Quantity.from_str(ORDER_QTY),
            subscribe_quotes=True,
            subscribe_trades=True,
            open_position_on_start_qty=Decimal(ORDER_QTY),
            open_position_on_first_quote=True,
            open_position_time_in_force=TimeInForce.IOC,
            enable_limit_buys=True,
            enable_limit_sells=True,
            tob_offset_ticks=TOB_OFFSET_TICKS,
            use_post_only=True,
            cancel_orders_on_stop=True,
            close_positions_on_stop=True,
            reduce_only_on_stop=True,
            dry_run=DRY_RUN,
            log_data=False,
        ),
    )

    node.run()


if __name__ == "__main__":
    main()
