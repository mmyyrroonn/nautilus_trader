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
"""Use the installed normal LiveNode factory without starting a network connection."""

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


def economic_policy(tmp_path: Path) -> dict:
    """Declare a bounded report policy for one synthetic exact native instrument."""
    return {
        "schema_version": 1,
        "checkpoint_path": str(tmp_path / "economic-report.json"),
        "instruments": ["io:SNDK-USD-PERP.HYPERLIQUID"],
        "history_start_ms": 1,
        "history_max_window_ms": 60_000,
        "history_timeout_ms": 1000,
        "history_max_pages": 4,
        "history_max_records": 1000,
        "max_observations": 1000,
        "max_receipts": 1000,
        "max_checkpoint_bytes": 1_048_576,
    }


def config(policy: dict | None, **changes: object) -> HyperliquidExecutionClientConfig:
    """Use explicit io identity and unreachable local endpoints with no real credentials."""
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
        "io_economics_policy_json": None if policy is None else json.dumps(policy),
    }
    return HyperliquidExecutionClientConfig(**(values | changes))


def build(factory, settings) -> LiveNode:
    """Create through the same native builder as the application; never call start."""
    return (
        LiveNode.builder("IO-ECON-CONFIG", TraderId("TESTER-001"), Environment.SANDBOX)
        .add_exec_client("HYPERLIQUID", factory, settings)
        .build()
    )


def test_installed_report_policy_is_optional_readonly_and_never_implies_cash(tmp_path) -> None:
    """Build a native report consumer without inventing account funds or venue history."""
    factory = HyperliquidExecutionClientFactory()
    for method in (
        factory.economics_scope_snapshot_json,
        factory.pending_economics_json,
        factory.persist_economics,
    ):
        assert method() is None
    assert config(None).io_economics_policy_json is None
    settings = config(economic_policy(tmp_path))
    assert settings.io_execution_policy_json is None
    with pytest.raises(AttributeError):
        settings.io_economics_policy_json = None
    node = build(factory, settings)
    assert isinstance(node, LiveNode)
    state = json.loads(factory.economics_scope_snapshot_json())
    assert state["policy"]["instruments"] == ["io:SNDK-USD-PERP.HYPERLIQUID"]
    assert state["pending"] == 0
    assert state["durable_receipts"] == 0
    assert state["retention"] == state["native_applied"] == "Unknown"
    assert state["balance_adjustment"] is False
    assert state["native_cache_recovery"] is False
    assert state["report"]["funding_usdc"] == "0"
    assert state["report"]["actual_fee_usdc"] == "0"
    assert state["coverage"] == []
    assert factory.account_scope_snapshot_json() is None
    assert factory.execution_scope_snapshot_json() is None
    report = json.loads(factory.persist_economics())
    assert report["report"] == state["report"]
    assert json.loads(factory.persist_economics())["report"] == report["report"]
    assert "deadbeef" not in factory.economics_scope_snapshot_json()
    assert (tmp_path / "economic-report.json").is_file()


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"checkpoint_path": "relative.json"},
        {"instruments": []},
        {"instruments": ["SNDK-USD-PERP.HYPERLIQUID"]},
        {"instruments": ["xyz:SNDK-USD-PERP.HYPERLIQUID"]},
        {"instruments": ["io:SNDK-USD-PERP.HYPERLIQUID"] * 2},
        {"history_timeout_ms": 0},
        {"history_timeout_ms": 30_001},
        {"history_max_window_ms": 86_400_001},
        {"history_max_pages": 0},
        {"history_max_pages": 33},
        {"history_max_records": 10_001},
        {"max_observations": 0},
        {"max_receipts": 0},
        {"max_checkpoint_bytes": 16_777_217},
        {"max_raw_frame_bytes": 1_048_577},
        {"max_history_body_bytes": 16_777_217},
        {"arbitrary_consumed_ack": True},
    ],
)
def test_native_factory_rejects_unbounded_economic_policy_before_io(tmp_path, change) -> None:
    """Reject invalid finite policies in the ordinary native factory before connections."""
    factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError)):
        build(factory, config(economic_policy(tmp_path) | change))
    assert factory.economics_scope_snapshot_json() is None


def test_economic_scope_requires_explicit_io_and_cannot_use_a_credentials_path(tmp_path) -> None:
    """Require explicit io ownership and keep checkpoints away from credentials paths."""
    factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError), match=r"io|account_dex"):
        build(factory, config(economic_policy(tmp_path), account_dex=None))
    with pytest.raises((ValueError, RuntimeError), match=r"credentials|checkpoint"):
        build(
            factory, config(economic_policy(tmp_path) | {"checkpoint_path": str(tmp_path / ".env")})
        )


def test_normal_clients_cannot_share_authoritative_economic_checkpoint(tmp_path) -> None:
    """Enforce one authoritative consumer for the same checkpoint."""
    first_factory = HyperliquidExecutionClientFactory()
    first = build(first_factory, config(economic_policy(tmp_path)))
    assert isinstance(first, LiveNode)
    second_factory = HyperliquidExecutionClientFactory()
    with pytest.raises((ValueError, RuntimeError), match=r"lease|lock|consumer"):
        build(second_factory, config(economic_policy(tmp_path)))
    assert second_factory.economics_scope_snapshot_json() is None


def test_one_factory_cannot_silently_replace_its_active_report_consumer(tmp_path) -> None:
    """Keep active client diagnostics bound to their original report consumer."""
    factory = HyperliquidExecutionClientFactory()
    first = build(factory, config(economic_policy(tmp_path)))
    assert isinstance(first, LiveNode)
    policy = economic_policy(tmp_path) | {"checkpoint_path": str(tmp_path / "other-report.json")}
    with pytest.raises((ValueError, RuntimeError), match=r"active|consumer|bound"):
        build(factory, config(policy))
