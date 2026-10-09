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
"""Verify optional native Aster selected-scope configuration without network access."""

import inspect
import json

import pytest

from nautilus_trader.adapters.aster import AsterExecutionClientConfig
from nautilus_trader.adapters.aster import AsterExecutionClientFactory
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig


@pytest.fixture
def config_values() -> dict[str, object]:
    """Provide explicit synthetic account inputs without ambient credential lookup."""
    return {
        "user_address": "0xc96aaa54e2d44c299564da76e1cd3184a2386b8d",
        "signer_address": "0xc96aaa54e2d44c299564da76e1cd3184a2386b8d",
        "signer_private_key": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "instrument_provider": BinanceInstrumentProviderConfig(
            load_all=False,
            load_ids=["SNDKUSD1-PERP.ASTER"],
        ),
    }


def policy(**updates: object) -> str:
    """Serialize the bounded selection with individual validation mutations."""
    values = {
        "instrument_ids": ["SNDKUSD1-PERP.ASTER"],
        "balance_asset": "USD1",
        "max_age_ms": 2000,
        "max_refresh_ms": 15000,
    }
    values.update(updates)
    return json.dumps(values)


def test_legacy_constructor_keeps_optional_scope_as_last_suffix() -> None:
    """Preserve positional configuration compatibility and absent diagnostics."""
    names = list(inspect.signature(AsterExecutionClientConfig).parameters)
    assert names[-2:] == ["assume_one_way_mode_when_unconfirmed", "selected_scope_policy_json"]
    assert AsterExecutionClientConfig().selected_scope_policy_json is None
    assert AsterExecutionClientFactory().selected_scope_snapshot_json() is None


def test_optional_policy_is_readonly_and_does_not_expose_signer_key(config_values) -> None:
    """Keep policy immutable and signing material outside public diagnostics."""
    raw = policy()
    config = AsterExecutionClientConfig(**config_values, selected_scope_policy_json=raw)
    assert config.selected_scope_policy_json == raw
    with pytest.raises(AttributeError):
        config.selected_scope_policy_json = None
    assert not hasattr(config, "signer_private_key")
    assert config_values["signer_private_key"] not in repr(config)


@pytest.mark.parametrize("value", [True, False, 2.0, -1, 0, 60001, "2000"])
@pytest.mark.parametrize("field", ["max_age_ms", "max_refresh_ms"])
def test_optional_policy_rejects_non_integer_or_unbounded_limits(
    config_values, field, value
) -> None:
    """Reject serde type mismatches and values beyond the finite policy bounds."""
    bound = "age" if field == "max_age_ms" else "refresh"
    message = (
        f"Invalid Aster selected scope {bound} bound"
        if type(value) is int and value >= 0
        else "expected u64"
    )
    with pytest.raises(ValueError, match=message):
        AsterExecutionClientConfig(
            **config_values, selected_scope_policy_json=policy(**{field: value})
        )


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"ready": True}, "unknown field `ready`"),
        ({"instrument_ids": []}, "requires 1 to 8 instrument IDs"),
        (
            {"instrument_ids": ["SNDKUSD1-PERP.ASTER"] * 2},
            "Duplicate Aster selected scope instrument ID",
        ),
        ({"instrument_ids": ["BTCUSDT-PERP.ASTER"]}, "IDs must equal finite configured load IDs"),
        ({"instrument_ids": ["SNDKUSD1-PERP.BINANCE"]}, "ID must use the exact configured venue"),
        ({"balance_asset": "usd1"}, "Invalid Aster selected scope balance asset"),
    ],
)
def test_optional_policy_rejects_unknown_or_different_scope(
    config_values, updates, message
) -> None:
    """Bind every policy rejection to its intended scope validation failure."""
    with pytest.raises(ValueError, match=message):
        AsterExecutionClientConfig(**config_values, selected_scope_policy_json=policy(**updates))


def test_optional_policy_rejects_duplicate_keys(config_values) -> None:
    """Refuse duplicate JSON fields rather than selecting a later authority value."""
    raw = policy()[:-1] + ', "max_age_ms": 3000}'
    with pytest.raises(ValueError, match="duplicate field `max_age_ms`"):
        AsterExecutionClientConfig(**config_values, selected_scope_policy_json=raw)


@pytest.mark.parametrize("field", ["signer_private_key", "signer_address", "user_address"])
def test_optional_policy_requires_explicit_account_identity(config_values, field) -> None:
    """Require explicit signing credentials and canonical user/signer addresses."""
    config_values[field] = None
    message = (
        "requires explicit user, signer and signing credentials"
        if field == "signer_private_key"
        else "requires canonical explicit wallet addresses"
    )
    with pytest.raises(ValueError, match=message):
        AsterExecutionClientConfig(**config_values, selected_scope_policy_json=policy())


def test_optional_policy_refuses_mode_exemption_and_catalogue_loading(config_values) -> None:
    """Require confirmed mode and a finite instrument selection for opt-in proof."""
    with pytest.raises(ValueError, match="does not permit a position-mode exemption"):
        AsterExecutionClientConfig(
            **config_values,
            selected_scope_policy_json=policy(),
            assume_one_way_mode_when_unconfirmed=True,
        )
    config_values["instrument_provider"] = BinanceInstrumentProviderConfig(load_all=True)
    with pytest.raises(ValueError, match="requires finite instrument loading"):
        AsterExecutionClientConfig(**config_values, selected_scope_policy_json=policy())


@pytest.mark.parametrize("value", ["bad", "0x" + "g" * 40, " " + "0x" + "a" * 40])
def test_selected_scope_rejects_noncanonical_wallet_identity(config_values, value) -> None:
    """Refuse malformed identities without normalizing their source binding."""
    config_values["user_address"] = value
    with pytest.raises(ValueError, match="requires canonical explicit wallet addresses"):
        AsterExecutionClientConfig(**config_values, selected_scope_policy_json=policy())


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://synthetic:secret@127.0.0.1:0",
        "http://127.0.0.1:0?account=unknown",
        "http://127.0.0.1:0#unknown",
    ],
)
def test_selected_scope_rejects_credential_bearing_or_ambiguous_source_url(
    config_values, endpoint
) -> None:
    """Reject embedded credentials or source-changing URL query/fragment data."""
    with pytest.raises(
        ValueError, match="must not contain embedded credentials, query or fragment"
    ):
        AsterExecutionClientConfig(
            **config_values, selected_scope_policy_json=policy(), base_url_http=endpoint
        )
