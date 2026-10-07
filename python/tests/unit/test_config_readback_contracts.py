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
"""Verify installed configuration readback and generated constructor contracts without client I/O."""

import ast
import inspect
import time
from decimal import Decimal
from pathlib import Path

import pytest

from nautilus_trader.adapters import aster
from nautilus_trader.adapters import blockchain
from nautilus_trader.adapters import coinbase
from nautilus_trader.adapters import interactive_brokers
from nautilus_trader.adapters import ondo
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.common import DataActorConfig
from nautilus_trader.model import AccountId
from nautilus_trader.model import InstrumentId
from nautilus_trader.trading import ExecutionAlgorithmConfig
from nautilus_trader.trading import StrategyConfig


@pytest.fixture
def envelope_values() -> dict[str, object]:
    """Use exact quantities and distinct limits within the immutable native ceilings."""
    now = time.time_ns()
    return {
        "instrument_id": InstrumentId.from_str("BTC-USD-PERP.ONDO"),
        "entry_side": "sell",
        "entry_max_quantity": "0.0002100000000000000000000001",
        "entry_worst_price": "51001.2500000000000000000000",
        "entry_max_notional_usd": "18.75000000000000000000000000",
        "close_side": "buy",
        "close_max_quantity": "0.0002100000000000000000000001",
        "close_worst_price": "50502.7500000000000000000000",
        "max_close_attempts": 2,
        "max_notional_per_order_usd": "37.12500000000000000000000000",
        "max_gross_exposure_usd": "73.87500000000000000000000000",
        "min_available_margin_usdc": "31.62500000000000000000000000",
        "max_orders": 3,
        "max_new_risk_requests": 1,
        "max_app_requests": 6,
        "entry_deadline_unix_nanos": now + 120_000_000_000,
        "cleanup_deadline_unix_nanos": now + 300_000_000_000,
        "require_flat_start": True,
    }


def test_ondo_envelope_preserves_all_fields_and_exact_decimals(envelope_values) -> None:
    """All 18 public values survive construction and nested readback without float conversion."""
    envelope = ondo.OndoExecutionEnvelopeConfig(**envelope_values)
    config = ondo.OndoExecutionClientConfig(
        environment=ondo.OndoEnvironment.SANDBOX,
        account_id=AccountId("ONDO-CONFIG-TEST"),
        api_key="synthetic-config-key",
        api_secret="synthetic-config-secret",
        http_timeout_secs=17,
        dms_timeout_secs=23,
        reconcile_interval_secs=29,
        execution_envelope=envelope,
    )
    readback = config.execution_envelope
    assert ondo.OndoExecutionClientConfig().execution_envelope is None
    assert config.http_timeout_secs == 17
    assert config.dms_timeout_secs == 23
    assert config.reconcile_interval_secs == 29
    decimal_fields = {
        "entry_max_quantity",
        "entry_worst_price",
        "entry_max_notional_usd",
        "close_max_quantity",
        "close_worst_price",
        "max_notional_per_order_usd",
        "max_gross_exposure_usd",
        "min_available_margin_usdc",
    }
    assert len(envelope_values) == 18
    for instance in (envelope, readback):
        for field, value in envelope_values.items():
            actual = getattr(instance, field)
            if field in decimal_fields:
                assert isinstance(actual, Decimal)
                assert actual.as_tuple() == Decimal(value).as_tuple()
            else:
                assert actual == value
        with pytest.raises(AttributeError):
            instance.max_orders = 2
    with pytest.raises(AttributeError):
        config.execution_envelope = None
    for name in ("api_key", "api_secret", "credential", "signer_private_key"):
        assert not hasattr(config, name)
    assert "synthetic-config-key" not in repr(config)
    assert "synthetic-config-secret" not in repr(config)
    assert ondo.OndoExecutionClientFactory().production_shutdown_diagnostics() is None


@pytest.mark.parametrize("invalid_field", ["entry_max_quantity", "entry_side", "max_orders"])
def test_ondo_invalid_envelope_errors_do_not_echo_inputs(envelope_values, invalid_field) -> None:
    """Constructor refusals expose a fixed error rather than untrusted input values."""
    marker = "synthetic-untrusted-envelope-input"
    envelope_values[invalid_field] = 99 if invalid_field == "max_orders" else marker
    with pytest.raises(ValueError, match=r"[Ii]nvalid") as error:
        ondo.OndoExecutionEnvelopeConfig(**envelope_values)
    assert marker not in str(error.value)


@pytest.mark.parametrize(
    "config_class", [aster.AsterDataClientConfig, aster.AsterExecutionClientConfig]
)
@pytest.mark.parametrize(
    "proxy_url", [None, "http://synthetic-user:synthetic-password@127.0.0.1:8888"]
)
def test_aster_proxy_readback_is_boolean_and_secrets_stay_opaque(config_class, proxy_url) -> None:
    """Proxy presence is observable; proxy authentication and signing inputs stay opaque."""
    values = {
        "environment": aster.AsterEnvironment.TESTNET,
        "base_url_http": "https://config.example.test",
        "base_url_ws": "wss://config.example.test/ws",
        "proxy_url": proxy_url,
    }
    if config_class is aster.AsterDataClientConfig:
        values.update(instrument_refresh_interval_secs=31, instrument_status_poll_secs=37)
    else:
        values.update(
            signer_private_key="synthetic-signing-input",
            http_timeout_secs=17,
            ws_heartbeat_secs=19,
            ws_connect_timeout_secs=23,
            treat_expired_as_canceled=False,
            assume_one_way_mode_when_unconfirmed=True,
        )
    config = config_class(**values)
    assert config.has_proxy_url is (proxy_url is not None)
    for name, expected in values.items():
        if name not in {"proxy_url", "signer_private_key"}:
            assert getattr(config, name) == expected
    for name in ("proxy_url", "signer_private_key", "api_key", "api_secret", "credential"):
        assert not hasattr(config, name)
    for marker in ("synthetic-user", "synthetic-password", "synthetic-signing-input"):
        assert marker not in repr(config)
        assert marker not in str(config)
    with pytest.raises(AttributeError):
        config.has_proxy_url = False


@pytest.mark.parametrize(
    "config_class", [aster.AsterDataClientConfig, aster.AsterExecutionClientConfig]
)
def test_aster_validation_errors_do_not_echo_proxy_or_key(config_class) -> None:
    """Non-secret instrument validation refuses the config without rendering its secrets."""
    values = {
        "proxy_url": "http://synthetic-user:synthetic-password@127.0.0.1:8888",
        "instrument_provider": BinanceInstrumentProviderConfig(
            load_ids=["BTCUSDT-PERP.BINANCE"],
        ),
    }
    if config_class is aster.AsterExecutionClientConfig:
        values["signer_private_key"] = "synthetic-signing-input"
    with pytest.raises(ValueError, match="must use venue ASTER") as error:
        config_class(**values)
    for marker in ("synthetic-user", "synthetic-password", "synthetic-signing-input"):
        assert marker not in str(error.value)


def test_qualified_domain_bindings_preserve_nondefault_readback() -> None:
    """Decimal and SymbologyMethod correspondence limits were scanner limits, not API defects."""
    leverage = Decimal("3.125000000000000000000000001")
    config = coinbase.CoinbaseExecutionClientConfig(default_leverage=leverage)
    assert config.default_leverage.as_tuple() == leverage.as_tuple()
    provider = interactive_brokers.InteractiveBrokersInstrumentProviderConfig(
        symbology_method=interactive_brokers.SymbologyMethod.RAW,
    )
    assert provider.symbology_method == interactive_brokers.SymbologyMethod.RAW


@pytest.mark.parametrize(
    "config_class", [DataActorConfig, ExecutionAlgorithmConfig, StrategyConfig]
)
def test_variadic_config_metadata_accepts_arbitrary_keyword_values(config_class) -> None:
    """The kwargs annotation describes each value, and the variadic parameter has no default."""
    config = config_class(log_events=False, log_commands=False, arbitrary_value=object())
    signature = inspect.signature(config_class)
    assert signature.parameters["_kwargs"].kind is inspect.Parameter.VAR_KEYWORD
    assert signature.parameters["_kwargs"].default is inspect.Parameter.empty
    assert signature.parameters["log_events"].default is True
    assert signature.parameters["log_commands"].default is True
    assert config.log_events is False
    assert config.log_commands is False


def test_blockchain_generated_keyword_only_order_matches_runtime() -> None:
    """Generated metadata retains the real keyword-only boundary and order."""
    signature = inspect.signature(blockchain.BlockchainExecutionClientConfig)
    stub = Path(blockchain.__file__).with_suffix(".pyi")
    declaration = next(
        node
        for node in ast.parse(stub.read_text()).body
        if isinstance(node, ast.ClassDef) and node.name == "BlockchainExecutionClientConfig"
    )
    constructor = next(
        node
        for node in declaration.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    keyword_names = [argument.arg for argument in constructor.args.kwonlyargs]
    assert keyword_names == [
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    ]
    assert keyword_names[0] == "allowed_token_pairs"
    assert keyword_names[-1] == "verification"
