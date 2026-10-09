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
"""Verify the installed native io configuration and its detached diagnostic API."""

import inspect

import pytest

from nautilus_trader.adapters.hyperliquid import HyperliquidEnvironment
from nautilus_trader.adapters.hyperliquid import HyperliquidExecutionClientConfig
from nautilus_trader.adapters.hyperliquid import HyperliquidExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import TraderId


@pytest.mark.parametrize("maximum_age_ms", [1, 300, 30_000])
def test_io_config_readback_is_frozen_and_credential_opaque(maximum_age_ms: int) -> None:
    """Scope settings survive native construction without exposing synthetic signing inputs."""
    config = HyperliquidExecutionClientConfig(
        account_id=AccountId("HYPERLIQUID-ENTROPY"),
        private_key="synthetic-opaque-config-key",
        account_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        environment=HyperliquidEnvironment.TESTNET,
        account_dex="io",
        account_snapshot_max_age_ms=maximum_age_ms,
    )
    assert str(config.account_id) == "HYPERLIQUID-ENTROPY"
    assert config.account_address == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert config.account_dex == "io"
    assert config.account_snapshot_max_age_ms == maximum_age_ms
    assert config.environment == HyperliquidEnvironment.TESTNET
    for field in ("account_dex", "account_snapshot_max_age_ms"):
        with pytest.raises(AttributeError):
            setattr(config, field, None)
    assert not hasattr(config, "private_key")
    assert "synthetic-opaque-config-key" not in repr(config)


def test_new_scope_options_preserve_legacy_positional_constructor_order() -> None:
    """Adding io options must preserve the existing configuration positional API."""
    legacy = [
        "account_id",
        "private_key",
        "vault_address",
        "account_address",
        "environment",
        "base_url_ws",
        "base_url_http",
        "base_url_exchange",
        "proxy_url",
        "http_timeout_secs",
        "max_retries",
        "retry_delay_initial_ms",
        "retry_delay_max_ms",
        "normalize_prices",
        "market_order_slippage_bps",
        "include_builder_attribution",
        "ws_post_timeout_secs",
        "transport_backend",
    ]
    parameters = list(inspect.signature(HyperliquidExecutionClientConfig).parameters)
    assert parameters[: len(legacy)] == legacy
    assert parameters[len(legacy) :] == [
        "account_dex",
        "account_snapshot_max_age_ms",
        "io_execution_policy_json",
        "io_economics_policy_json",
    ]
    config = HyperliquidExecutionClientConfig(
        AccountId("HYPERLIQUID-LEGACY"),
        None,
        None,
        None,
        HyperliquidEnvironment.TESTNET,
    )
    assert config.environment == HyperliquidEnvironment.TESTNET
    assert config.account_dex is None
    assert config.account_snapshot_max_age_ms == 30_000
    assert config.io_execution_policy_json is None
    assert config.io_economics_policy_json is None


def test_unbound_native_factory_returns_no_account_proof() -> None:
    """No created client or empty cache is evidence of a funded or flat io account."""
    factory = HyperliquidExecutionClientFactory()
    assert factory.name() == "HYPERLIQUID"
    assert factory.account_scope_snapshot_json() is None


@pytest.fixture
def io_config_values() -> dict:
    """Provide synthetic supported settings without a reachable venue endpoint."""
    return {
        "account_id": AccountId("HYPERLIQUID-ENTROPY"),
        "private_key": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "account_address": "0xc96aaa54e2d44c299564da76e1cd3184a2386b8d",
        "account_dex": "io",
        "account_snapshot_max_age_ms": 30_000,
        "environment": HyperliquidEnvironment.TESTNET,
        "base_url_http": "http://127.0.0.1:0/info",
        "base_url_ws": "ws://127.0.0.1:0/ws",
        "base_url_exchange": "http://127.0.0.1:0/exchange",
    }


def test_normal_node_factory_builds_valid_io_configuration_without_account_proof(
    io_config_values,
) -> None:
    """The same factory can build supported settings without running or inventing a proof."""
    factory = HyperliquidExecutionClientFactory()
    builder = LiveNode.builder("IO-CONFIG-VALID", TraderId("TESTER-001"), Environment.SANDBOX)
    node = builder.add_exec_client(
        "HYPERLIQUID", factory, HyperliquidExecutionClientConfig(**io_config_values)
    ).build()
    assert isinstance(node, LiveNode)
    assert factory.account_scope_snapshot_json() is None


@pytest.mark.parametrize(
    ("invalid_scope", "reason"),
    [
        ({"account_snapshot_max_age_ms": 0}, "account_snapshot_max_age_ms must be in"),
        ({"account_snapshot_max_age_ms": 30_001}, "account_snapshot_max_age_ms must be in"),
        ({"account_dex": "xyz"}, "Only explicit io account_dex is supported"),
        ({"account_id": AccountId("HYPERLIQUID-001")}, "explicit dedicated account_id"),
    ],
)
def test_normal_node_factory_refuses_invalid_io_configuration_before_io(
    io_config_values, invalid_scope, reason
) -> None:
    """The ordinary factory rejects unsupported scopes and unsafe proof limits at build time."""
    config = HyperliquidExecutionClientConfig(**(io_config_values | invalid_scope))
    factory = HyperliquidExecutionClientFactory()
    builder = LiveNode.builder("IO-CONFIG-CHECK", TraderId("TESTER-001"), Environment.SANDBOX)
    with pytest.raises((RuntimeError, ValueError), match=reason):
        builder.add_exec_client("HYPERLIQUID", factory, config).build()
    assert factory.account_scope_snapshot_json() is None
