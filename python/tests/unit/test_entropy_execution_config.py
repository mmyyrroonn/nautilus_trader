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
"""Exercise the installed native bounded policy through the ordinary LiveNode factory."""

import json
from pathlib import Path

import pytest

from nautilus_trader.adapters.hyperliquid import HyperliquidEnvironment
from nautilus_trader.adapters.hyperliquid import HyperliquidExecutionClientConfig
from nautilus_trader.adapters.hyperliquid import HyperliquidExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import TraderId


def bounded_policy(tmp_path: Path) -> dict:
    """Provide finite synthetic limits and a separate owned journal."""
    return {
        "schema_version": 1,
        "strategy_id": "ENTROPY-001",
        "journal_path": str(tmp_path / "io-intents.jsonl"),
        "max_order_notional": "100",
        "max_gross_notional": "200",
        "max_open_orders": 2,
        "max_actions": 16,
        "max_leverage": 1,
        "margin_buffer": "5",
        "fee_buffer_bps": "10",
        "action_timeout_ms": 1000,
        "recovery_timeout_ms": 1500,
        "recovery_max_attempts": 2,
        "recovery_retry_delay_ms": 10,
        "metadata_max_age_ms": 1000,
        "symbols": [
            {
                "instrument_id": "io:SNDK-USD-PERP.HYPERLIQUID",
                "max_quantity": "1",
                "min_price": "95",
                "max_price": "105",
            }
        ],
    }


def config(policy: dict | None, **changes: object) -> HyperliquidExecutionClientConfig:
    """Use the normal native configuration with unreachable local endpoints."""
    values = {
        "account_id": AccountId("HYPERLIQUID-ENTROPY"),
        "private_key": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "account_address": "0xc96aaa54e2d44c299564da76e1cd3184a2386b8d",
        "account_dex": "io",
        "environment": HyperliquidEnvironment.TESTNET,
        "base_url_http": "http://127.0.0.1:0/info",
        "base_url_ws": "ws://127.0.0.1:0/ws",
        "base_url_exchange": "http://127.0.0.1:0/exchange",
        "include_builder_attribution": False,
        "io_execution_policy_json": None if policy is None else json.dumps(policy),
    }
    return HyperliquidExecutionClientConfig(**(values | changes))


def build(factory, settings) -> LiveNode:
    """Build the ordinary factory without starting any account or network requests."""
    builder = LiveNode.builder("IO-BOUNDED-CONFIG", TraderId("TESTER-001"), Environment.SANDBOX)
    return builder.add_exec_client("HYPERLIQUID", factory, settings).build()


def test_installed_policy_is_frozen_and_default_remains_readonly(tmp_path) -> None:
    """Read back explicit limits without treating configuration as execution proof."""
    readonly = config(None)
    assert readonly.io_execution_policy_json is None
    policy = bounded_policy(tmp_path)
    settings = config(policy)
    assert json.loads(settings.io_execution_policy_json) == policy
    with pytest.raises(AttributeError):
        settings.io_execution_policy_json = None
    factory = HyperliquidExecutionClientFactory()
    assert factory.execution_scope_snapshot_json() is None
    node = build(factory, settings)
    assert isinstance(node, LiveNode)
    snapshot = json.loads(factory.execution_scope_snapshot_json())
    assert snapshot["recovery_complete"] is False
    assert snapshot["policy"]["max_order_notional"] == "100"
    assert snapshot["user_asset_source_time_ms"] is None
    assert snapshot["actual_fills"] == {}
    assert "deadbeef" not in factory.execution_scope_snapshot_json()
    assert (tmp_path / "io-intents.jsonl").is_file()


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"schema_version": 2}, "Unsupported io policy schema"),
        ({"max_order_notional": 100.1}, "Invalid io execution policy"),
        ({"max_order_notional": "-1"}, "monetary bounds"),
        ({"max_gross_notional": "1"}, "monetary bounds"),
        ({"fee_buffer_bps": "10001"}, "monetary bounds"),
        ({"max_actions": 0}, "count/leverage bounds"),
        ({"max_open_orders": 65}, "count/leverage bounds"),
        ({"max_leverage": 0}, "count/leverage bounds"),
        ({"action_timeout_ms": 0}, "deadlines"),
        ({"recovery_timeout_ms": 30001}, "deadlines"),
        ({"recovery_max_attempts": 11}, "recovery policy"),
        ({"journal_path": "relative.jsonl"}, "absolute"),
        ({"symbols": []}, "explicit symbol set"),
        ({"unexpected_execution_permission": True}, "Invalid io execution policy"),
    ],
)
def test_native_factory_rejects_unbounded_or_inexact_policy_before_io(
    tmp_path, change, reason
) -> None:
    """Reject unsafe native policies through the same builder used by the application."""
    factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError), match=reason):
        build(factory, config(bounded_policy(tmp_path) | change))
    assert factory.account_scope_snapshot_json() is None
    assert factory.execution_scope_snapshot_json() is None


def test_policy_requires_the_explicit_io_account_scope(tmp_path) -> None:
    """Do not silently discard execution limits when the account scope is absent."""
    factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError), match=r"io|account_dex"):
        build(factory, config(bounded_policy(tmp_path), account_dex=None))


def test_two_clients_cannot_share_the_same_durable_intent_journal(tmp_path) -> None:
    """Prevent two clients from admitting separate budgets against one ownership journal."""
    policy = bounded_policy(tmp_path)
    first_factory = HyperliquidExecutionClientFactory()
    first = build(first_factory, config(policy))
    assert isinstance(first, LiveNode)
    second_factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError), match=r"journal|lock|lease"):
        build(second_factory, config(policy))
    assert second_factory.execution_scope_snapshot_json() is None
