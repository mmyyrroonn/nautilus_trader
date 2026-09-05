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
Aster DEX execution probe (testnet by default).

Exercises the ``nautilus-aster`` execution client end to end against a live Aster account:

1. Submits a GTC LIMIT BUY on ``BTCUSDT-PERP.ASTER`` far below the market so it rests, waits
   for ``OrderAccepted``, logs the resulting order status, cancels it, and waits for
   ``OrderCanceled``.
2. Submits an IOC LIMIT BUY at the current ask for the same size and logs whatever comes
   back (a fill, or an expiry when nothing crosses).
3. Logs the account's commission rates for ``BTCUSDT`` and ``NVDAUSDT``.
4. Stops the node.

Credentials come from the environment (loaded from a local ``.env`` when ``python-dotenv`` is
installed):

    ASTER_SIGNER_PRIVATE_KEY   API wallet private key            (required)
    ASTER_SIGNER_ADDRESS       API wallet address                (optional, derived otherwise)
    ASTER_USER_ADDRESS         Master account wallet address     (optional, defaults to signer)
    ASTER_TESTNET              "false" selects mainnet           (optional, defaults to testnet)

WARNING: mainnet places REAL orders with REAL funds and therefore additionally requires the
explicit ``--i-know-mainnet`` flag.

    python aster_exec_probe.py --help
    python aster_exec_probe.py --dry-run          # build the config only, no network
    python aster_exec_probe.py                    # testnet probe
"""

from __future__ import annotations

import argparse
import os
import sys
from decimal import ROUND_CEILING
from decimal import Decimal
from typing import Any
from typing import Self


try:  # Optional: load a local .env when python-dotenv is available.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - dotenv is a convenience, not a requirement
    pass

from nautilus_trader.adapters.aster import ASTER
from nautilus_trader.adapters.aster import AsterDataClientConfig
from nautilus_trader.adapters.aster import AsterDataClientFactory
from nautilus_trader.adapters.aster import AsterEnvironment
from nautilus_trader.adapters.aster import AsterExecutionClientConfig
from nautilus_trader.adapters.aster import AsterExecutionClientFactory
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.common import Environment
from nautilus_trader.common import LoggerConfig
from nautilus_trader.common import LogLevel
from nautilus_trader.config import LiveRiskEngineConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderSide
from nautilus_trader.model import Quantity
from nautilus_trader.model import TimeInForce
from nautilus_trader.model import TraderId
from nautilus_trader.trading import Strategy
from nautilus_trader.trading import StrategyConfig


BTC_INSTRUMENT_ID = InstrumentId.from_str("BTCUSDT-PERP.ASTER")
NVDA_INSTRUMENT_ID = InstrumentId.from_str("NVDAUSDT-PERP.ASTER")

# Aster enforces a minimum notional per order; 5 USDT is the documented floor for USD-M
# perpetuals. The probe sizes to the larger of the instrument minimum and this notional.
MIN_NOTIONAL_USDT = Decimal("5")

# How far below the best bid the resting order is placed, so it can never cross.
RESTING_PRICE_FACTOR = Decimal("0.5")


class AsterProbeConfig(StrategyConfig):
    """
    Configuration for :class:`AsterProbeStrategy`.
    """

    _CUSTOM_FIELDS = ("instrument_id", "commission_instrument_ids")

    def __new__(cls, *args: object, **kwargs: object) -> Self:
        """
        Create a new instance, hiding the custom fields from the base config.
        """
        for field in cls._CUSTOM_FIELDS:
            kwargs.pop(field, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        instrument_id: InstrumentId,
        commission_instrument_ids: tuple[InstrumentId, ...] = (),
        **_kwargs: object,
    ) -> None:
        """
        Initialize the configuration.
        """
        super().__init__()
        self.instrument_id = instrument_id
        self.commission_instrument_ids = commission_instrument_ids


class AsterProbeStrategy(Strategy):
    """
    Drives one pass of the Aster execution probe, logging every step with timestamps.
    """

    def __init__(self, config: AsterProbeConfig) -> None:
        """
        Initialize the strategy.
        """
        super().__init__(config)
        self._instrument_id = config.instrument_id
        self._commission_instrument_ids = config.commission_instrument_ids
        self._instrument: Any = None
        self._resting_order_id = None
        self._ioc_order_id = None
        self._phase = "init"

    # -- lifecycle -------------------------------------------------------------------------

    def on_start(self) -> None:
        """
        Subscribe to quotes; the probe starts on the first one.
        """
        self._instrument = self.cache.instrument(self._instrument_id)
        if self._instrument is None:
            self.log.error(f"[probe] instrument {self._instrument_id} not found; stopping")
            self.stop()
            return

        self.log.info(
            f"[probe] instrument loaded: {self._instrument.id} "
            f"price_increment={self._instrument.price_increment} "
            f"size_increment={self._instrument.size_increment} "
            f"min_quantity={self._instrument.min_quantity}",
        )
        self.subscribe_quotes(self._instrument_id)
        self._phase = "awaiting_quote"
        self.log.info("[probe] waiting for the first quote before submitting")

    def on_stop(self) -> None:
        """
        Log the phase the probe reached.
        """
        self.log.info(f"[probe] stopping in phase={self._phase}")

    # -- market data -----------------------------------------------------------------------

    def on_quote(self, quote) -> None:
        """
        Start step 1 on the first quote.
        """
        if self._phase != "awaiting_quote":
            return

        self._phase = "resting_submitted"
        self.log.info(
            f"[probe] first quote: bid={quote.bid_price} ask={quote.ask_price} "
            f"ts_event={quote.ts_event}",
        )
        self._submit_resting_order(quote)

    # -- probe steps -----------------------------------------------------------------------

    def _probe_quantity(self, reference_price: Decimal) -> Quantity:
        """
        Return the smallest quantity satisfying both the instrument minimum and the venue
        minimum notional at ``reference_price``.
        """
        increment = self._instrument.size_increment.as_decimal()
        minimum = (
            self._instrument.min_quantity.as_decimal()
            if self._instrument.min_quantity is not None
            else increment
        )
        needed = MIN_NOTIONAL_USDT / reference_price
        steps = max(
            (minimum / increment).to_integral_value(rounding=ROUND_CEILING),
            (needed / increment).to_integral_value(rounding=ROUND_CEILING),
        )
        return self._instrument.make_qty(steps * increment)

    def _submit_resting_order(self, quote) -> None:
        bid = quote.bid_price.as_decimal()
        price = self._instrument.make_price(bid * RESTING_PRICE_FACTOR)
        quantity = self._probe_quantity(bid)

        order = self.order_factory.limit(
            instrument_id=self._instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.GTC,
        )
        self._resting_order_id = order.client_order_id
        self.log.info(
            f"[probe] step 1: submitting GTC LIMIT BUY {quantity} @ {price} "
            f"(50% below bid {bid}) client_order_id={order.client_order_id}",
        )
        self.submit_order(order)

    def _submit_ioc_order(self) -> None:
        quote = self.cache.quote(self._instrument_id)
        if quote is None:
            self.log.error("[probe] no cached quote for the IOC leg; skipping to step 3")
            self._report_commission_rates()
            return

        ask = quote.ask_price
        quantity = self._probe_quantity(ask.as_decimal())
        order = self.order_factory.limit(
            instrument_id=self._instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            price=ask,
            time_in_force=TimeInForce.IOC,
        )
        self._ioc_order_id = order.client_order_id
        self._phase = "ioc_submitted"
        self.log.info(
            f"[probe] step 2: submitting IOC LIMIT BUY {quantity} @ {ask} "
            f"client_order_id={order.client_order_id}",
        )
        self.submit_order(order)

    def _report_commission_rates(self) -> None:
        self._phase = "commission"
        self.log.info("[probe] step 3: commission rates")

        for instrument_id in self._commission_instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                self.log.warning(f"[probe] {instrument_id} not loaded; no commission to report")
                continue
            self.log.info(
                f"[probe] {instrument_id} maker_fee={instrument.maker_fee} "
                f"taker_fee={instrument.taker_fee}",
            )

        self._phase = "done"
        self.log.info("[probe] complete; stopping")
        self.stop()

    # -- order events ----------------------------------------------------------------------

    def on_order_accepted(self, event) -> None:
        """
        Log the acceptance and, for the resting order, cancel it.
        """
        self.log.info(
            f"[probe] OrderAccepted client_order_id={event.client_order_id} "
            f"venue_order_id={event.venue_order_id} ts_event={event.ts_event}",
        )

        if event.client_order_id != self._resting_order_id:
            return

        order = self.cache.order(event.client_order_id)
        self.log.info(
            f"[probe] resting order status: status={order.status} quantity={order.quantity} "
            f"filled_qty={order.filled_qty} price={order.price} "
            f"venue_order_id={order.venue_order_id}",
        )

        self._phase = "cancel_requested"
        self.log.info(f"[probe] cancelling resting order {event.client_order_id}")
        self.cancel_order(order)

    def on_order_canceled(self, event) -> None:
        """
        Move on to the IOC leg once the resting order is gone.
        """
        self.log.info(
            f"[probe] OrderCanceled client_order_id={event.client_order_id} "
            f"ts_event={event.ts_event}",
        )
        if event.client_order_id == self._resting_order_id:
            self._submit_ioc_order()

    def on_order_filled(self, event) -> None:
        """
        Log the fill and finish once the IOC leg resolves.
        """
        self.log.info(
            f"[probe] OrderFilled client_order_id={event.client_order_id} "
            f"last_qty={event.last_qty} last_px={event.last_px} "
            f"commission={event.commission} liquidity_side={event.liquidity_side} "
            f"ts_event={event.ts_event}",
        )
        if event.client_order_id == self._ioc_order_id:
            self._report_commission_rates()

    def on_order_expired(self, event) -> None:
        """
        An unfilled IOC remainder expires; that also finishes step 2.
        """
        self.log.info(
            f"[probe] OrderExpired client_order_id={event.client_order_id} "
            f"ts_event={event.ts_event}",
        )
        if event.client_order_id == self._ioc_order_id:
            self._report_commission_rates()

    def on_order_rejected(self, event) -> None:
        """
        Stop on any rejection; the probe cannot continue meaningfully.
        """
        self.log.error(
            f"[probe] OrderRejected client_order_id={event.client_order_id} "
            f"reason={event.reason} ts_event={event.ts_event}",
        )
        self.stop()

    def on_order_denied(self, event) -> None:
        """
        Stop on any local denial.
        """
        self.log.error(
            f"[probe] OrderDenied client_order_id={event.client_order_id} reason={event.reason}",
        )
        self.stop()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"false", "0", "no", "off"}


def build_parser() -> argparse.ArgumentParser:
    """
    Return the command-line parser.
    """
    return _build_parser()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aster_exec_probe",
        description=(
            "Probe the Aster DEX execution client: rest and cancel a far-from-market GTC LIMIT "
            "order, then send a minimum-size IOC LIMIT order, then log commission rates. Reads "
            "ASTER_SIGNER_PRIVATE_KEY / ASTER_SIGNER_ADDRESS / ASTER_USER_ADDRESS / "
            "ASTER_TESTNET from the environment."
        ),
    )
    parser.add_argument(
        "--i-know-mainnet",
        action="store_true",
        help="required to run against mainnet, where the probe places REAL orders",
    )
    parser.add_argument(
        "--instrument-id",
        default=str(BTC_INSTRUMENT_ID),
        help=f"instrument to trade (default: {BTC_INSTRUMENT_ID})",
    )
    parser.add_argument(
        "--connection-timeout-secs",
        type=int,
        default=60,
        help="abort startup if the clients do not connect within this many seconds (default: 60)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="stdout log level for the node (default: INFO)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build the configuration, print it, and exit without touching the network",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    Run the probe; returns the process exit code.
    """
    args = _build_parser().parse_args(argv)

    testnet = _env_flag("ASTER_TESTNET", default=True)
    environment = AsterEnvironment.TESTNET if testnet else AsterEnvironment.MAINNET

    if not testnet and not args.i_know_mainnet:
        print(
            "Refusing to run on MAINNET: pass --i-know-mainnet to place real orders, or leave "
            "ASTER_TESTNET unset to stay on testnet.",
            file=sys.stderr,
        )
        return 2

    instrument_id = InstrumentId.from_str(args.instrument_id)
    load_ids = sorted({str(instrument_id), str(BTC_INSTRUMENT_ID), str(NVDA_INSTRUMENT_ID)})
    instrument_provider = BinanceInstrumentProviderConfig(load_all=False, load_ids=load_ids)

    data_config = AsterDataClientConfig(
        environment=environment,
        instrument_provider=instrument_provider,
    )
    exec_config = AsterExecutionClientConfig(
        environment=environment,
        user_address=os.environ.get("ASTER_USER_ADDRESS"),
        signer_address=os.environ.get("ASTER_SIGNER_ADDRESS"),
        signer_private_key=os.environ.get("ASTER_SIGNER_PRIVATE_KEY"),
        instrument_provider=instrument_provider,
    )

    print(f"venue             : {ASTER}")
    print(f"environment       : {environment}")
    print(f"instrument        : {instrument_id}")
    print(f"instruments loaded: {', '.join(load_ids)}")
    print(f"exec config       : {exec_config!r}")
    print(f"signer key set    : {bool(os.environ.get('ASTER_SIGNER_PRIVATE_KEY'))}")

    if args.dry_run:
        print("dry run: configuration built, exiting without connecting")
        return 0

    if not os.environ.get("ASTER_SIGNER_PRIVATE_KEY"):
        print(
            "ASTER_SIGNER_PRIVATE_KEY is not set; the execution client cannot sign requests.",
            file=sys.stderr,
        )
        return 2

    node = (
        LiveNode.builder(
            "ASTER-EXEC-PROBE-001",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .with_logging(LoggerConfig(stdout_level=LogLevel.from_str(args.log_level)))
        .with_risk_engine_config(LiveRiskEngineConfig(bypass=True))
        .with_timeout_connection(args.connection_timeout_secs)
        .add_data_client(None, AsterDataClientFactory(), data_config)
        .add_exec_client(None, AsterExecutionClientFactory(), exec_config)
        .build()
    )
    node.add_strategy(
        AsterProbeStrategy(
            AsterProbeConfig(
                instrument_id=instrument_id,
                commission_instrument_ids=(BTC_INSTRUMENT_ID, NVDA_INSTRUMENT_ID),
            ),
        ),
    )

    print(f"starting node against {ASTER} ({environment})")
    node.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
