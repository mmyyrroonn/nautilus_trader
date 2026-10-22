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
Verify the native Arcus facade and LiveNode factory registration.
"""

import pytest

from nautilus_trader.adapters.arcus import ARCUS
from nautilus_trader.adapters.arcus import ArcusDataClientConfig
from nautilus_trader.adapters.arcus import ArcusDataClientFactory
from nautilus_trader.adapters.arcus import ArcusEnvironment
from nautilus_trader.adapters.arcus import ArcusExecutionClientConfig
from nautilus_trader.adapters.arcus import ArcusExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import Currency
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import TraderId


def test_factories_register_with_live_node() -> None:
    """
    Build both native clients without connecting to an exchange.
    """
    instrument = InstrumentId.from_str("BTC-USD-PERP.ARCUS")
    node = (
        LiveNode.builder("ARCUS-PYTEST", TraderId.from_str("TESTER-001"), Environment.LIVE)
        .add_data_client(
            None,
            ArcusDataClientFactory(),
            ArcusDataClientConfig(load_ids=[instrument]),
        )
        .add_exec_client(
            None,
            ArcusExecutionClientFactory(),
            ArcusExecutionClientConfig(
                account_id=AccountId.from_str("ARCUS-001"),
                environment=ArcusEnvironment.TESTNET,
                address="0x" + "01" * 20,
                api_secret="00" * 32,
                load_ids=[instrument],
            ),
        )
        .build()
    )
    assert node.trader_id == TraderId.from_str("TESTER-001")
    assert ArcusDataClientFactory().name() == ARCUS
    assert ArcusExecutionClientFactory().name() == ARCUS
    node.dispose()


def test_secret_is_not_exposed_to_python() -> None:
    """
    Keep signer material out of Python getters and representations.
    """
    seed = "11" * 32
    config = ArcusExecutionClientConfig(api_secret=seed)
    assert seed not in repr(config)
    assert not hasattr(config, "api_secret")
    assert not hasattr(config, "api_key")


@pytest.mark.parametrize("capacity", [0, 65537])
def test_configuration_rejects_invalid_queue_bounds(capacity: int) -> None:
    """
    Reject unbounded or excessive queues before client construction.
    """
    with pytest.raises(ValueError, match="queue_capacity"):
        ArcusDataClientConfig(queue_capacity=capacity)


def test_environment_preserves_integer_hash_contract() -> None:
    """
    Preserve dictionary lookup semantics for integer-comparable environments.
    """
    assert ArcusEnvironment.MAINNET == 0
    assert hash(ArcusEnvironment.MAINNET) == hash(0)
    assert {ArcusEnvironment.MAINNET: "mainnet"}[0] == "mainnet"


def test_import_registers_exact_settlement_currency() -> None:
    """
    Register the venue ledger for replay without changing the shared token.
    """
    assert Currency.from_str("USDG.ARCUS", strict=True).precision == 9
    assert Currency.from_str("USDG", strict=True).precision == 8
