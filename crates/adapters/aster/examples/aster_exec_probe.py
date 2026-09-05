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
Stage 2 - Aster DEX execution probe (TESTNET ONLY).

This probe refuses to start against mainnet. Mainnet execution belongs to a future
stage-3 script that carries the hard limits required by CLAUDE.md / PROMPT.md.

Exercises the ``nautilus-aster`` execution client end to end against a live Aster account:

1. Submits a GTC LIMIT BUY on the probe instrument far below the market so it rests, waits
   for ``OrderAccepted``, logs the resulting order status, cancels it, and waits for the
   order to reach a terminal state.
2. Submits an IOC LIMIT BUY at the current ask for the same notional and follows the order
   to its real terminal state (zero-fill cancel/expiry, partial fill then cancel, or full
   fill), accumulating every fill and commission.
3. Logs the fee values the adapter published for ``BTCUSDT`` and ``NVDAUSDT``, labelled with
   their provenance - they are NOT a verified account fee schedule.
4. Signals "done" so the watchdog in ``main()`` stops the node within bounded time.

Order sizing honours the instrument's ``min_notional`` / ``min_quantity`` / ``size_increment``
against the rounded limit price actually sent, and is capped at ``MAX_NOTIONAL_USDT`` per
order. If the venue minimum exceeds that cap the probe refuses instead of sizing up.

Credentials come from the environment (loaded from a local ``.env`` when ``python-dotenv`` is
installed):

    ASTER_SIGNER_PRIVATE_KEY   API wallet private key            (required)
    ASTER_SIGNER_ADDRESS       API wallet address                (optional, derived otherwise)
    ASTER_USER_ADDRESS         Master account wallet address     (optional, defaults to signer)
    ASTER_TESTNET              unset or "true" -> testnet; anything else -> the probe refuses

    python <this file> --help
    python <this file> --dry-run          # build the configuration only, no network
    python <this file>                    # testnet probe
"""

from __future__ import annotations

import argparse
import functools
import os
import sys
import threading
import time
import traceback
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
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
from nautilus_trader.model import TimeInForce
from nautilus_trader.model import TraderId
from nautilus_trader.trading import Strategy
from nautilus_trader.trading import StrategyConfig


BTC_INSTRUMENT_ID = InstrumentId.from_str("BTCUSDT-PERP.ASTER")
NVDA_INSTRUMENT_ID = InstrumentId.from_str("NVDAUSDT-PERP.ASTER")

# Used only when the instrument carries no ``min_notional``. 5 USDT is the documented floor
# for Aster USD-M perpetuals; the instrument value always wins when it is present.
MIN_NOTIONAL_FALLBACK_USDT = Decimal("5")

# Hard probe budget: no single probe order may exceed this notional. It is a module constant
# rather than a CLI/env knob so no configuration can widen it. When the venue minimum order
# is larger than this, the probe refuses instead of sizing up to meet the venue floor.
CONNECT_ATTEMPTS = 3  # whole-node retries while no order has been sent
CONNECT_RETRY_SECS = 15
MAX_NOTIONAL_USDT = Decimal("20")

# How far below the best bid the resting order is placed, so it can never cross.
RESTING_PRICE_FACTOR = Decimal("0.5")

# Watchdog default: stop the node this long after start no matter what happens.
DEFAULT_TIMEOUT_SECS = 180

# Bounded grace between "the probe finished" and "stop the node", so the cancel requests the
# cleanup issued for still-open probe orders have a chance to be confirmed by the venue.
CLEANUP_GRACE_SECS = 10.0

# Binance VIP0 defaults. The Aster instrument metadata is loaded through the Binance
# USD-M path, which falls back to these when no account commissionRate is available, so
# seeing exactly these values is a strong hint the numbers are a fallback, not our fees.
BINANCE_DEFAULT_MAKER_FEE = Decimal("0.0002")
BINANCE_DEFAULT_TAKER_FEE = Decimal("0.0005")

FEE_PROVENANCE_NOTE = (
    "fees as published by the adapter "
    "(source: commissionRate when available, otherwise venue default - unverified)"
)

_TRUE_VALUES = {"", "1", "true", "yes", "on"}

# Exit codes.
EXIT_OK = 0
EXIT_PROBE_FAILED = 1
EXIT_REFUSED = 2
EXIT_TIMEOUT = 3


class ProbeSizingError(RuntimeError):
    """
    Raised when no order size satisfies both the venue minimums and the probe budget.
    """


@dataclass(frozen=True)
class OrderPlan:
    """
    A concrete, checked order: the rounded price actually sent and its matching quantity.
    """

    price: Any  # nautilus_trader.model.Price
    quantity: Any  # nautilus_trader.model.Quantity
    notional: Decimal
    min_notional: Decimal
    max_notional: Decimal

    def describe(self) -> str:
        """
        Return a one-line human description of the plan.
        """
        return (
            f"qty={self.quantity} price={self.price} notional={self.notional} USDT "
            f"(min={self.min_notional} cap={self.max_notional})"
        )


def plan_order(
    instrument: Any,
    raw_price: Decimal,
    *,
    max_notional: Decimal = MAX_NOTIONAL_USDT,
) -> OrderPlan:
    """
    Size one probe order against the limit price that will actually be sent.

    The price is rounded to the instrument's ``price_increment`` first, then the quantity is
    derived from that rounded price so ``quantity * price`` really does clear the venue
    minimum notional. Raises :class:`ProbeSizingError` when the venue minimums and the probe
    budget cannot both be satisfied.
    """
    price = instrument.make_price(raw_price)
    limit = price.as_decimal()
    if limit <= 0:
        raise ProbeSizingError(
            f"limit price rounds to {limit} at price_increment={instrument.price_increment}",
        )

    min_price = instrument.min_price
    if min_price is not None and limit < min_price.as_decimal():
        raise ProbeSizingError(f"limit price {limit} is below the venue min_price {min_price}")

    increment = instrument.size_increment.as_decimal()
    if increment <= 0:
        raise ProbeSizingError(f"instrument size_increment is {increment}")

    min_quantity = (
        instrument.min_quantity.as_decimal() if instrument.min_quantity is not None else increment
    )
    min_notional = (
        instrument.min_notional.as_decimal()
        if instrument.min_notional is not None
        else MIN_NOTIONAL_FALLBACK_USDT
    )

    steps_for_quantity = (min_quantity / increment).to_integral_value(rounding=ROUND_CEILING)
    steps_for_notional = (min_notional / limit / increment).to_integral_value(
        rounding=ROUND_CEILING,
    )
    steps = max(Decimal(1), steps_for_quantity, steps_for_notional)
    quantity = instrument.make_qty(steps * increment)
    notional = quantity.as_decimal() * limit

    # Post-rounding assertions: the numbers actually sent, not the ones we asked for.
    if notional < min_notional:
        raise ProbeSizingError(
            f"rounded order {quantity} @ {price} is {notional} USDT, below the venue "
            f"min_notional {min_notional} USDT",
        )
    if notional > max_notional:
        raise ProbeSizingError(
            f"the smallest order this venue accepts is {quantity} @ {price} = {notional} USDT, "
            f"above the probe budget of {max_notional} USDT per order "
            f"(min_quantity={instrument.min_quantity} size_increment={instrument.size_increment} "
            f"min_notional={instrument.min_notional}); refusing to size up",
        )
    max_quantity = instrument.max_quantity
    if max_quantity is not None and quantity.as_decimal() > max_quantity.as_decimal():
        raise ProbeSizingError(f"quantity {quantity} exceeds the venue max_quantity {max_quantity}")

    return OrderPlan(
        price=price,
        quantity=quantity,
        notional=notional,
        min_notional=min_notional,
        max_notional=max_notional,
    )


def fee_line(instrument: Any) -> str:
    """
    Return one labelled fee line for ``instrument``.

    The values are whatever the adapter put on the instrument. They are never presented as a
    verified account fee schedule: the label names the provenance, and an exact match with
    the Binance VIP0 defaults is called out explicitly.
    """
    info = getattr(instrument, "info", None) or {}
    source = None
    for key in ("fee_source", "feeSource"):
        value = info.get(key)
        if value:
            source = f"source: {value}"
            break

    label = source or FEE_PROVENANCE_NOTE
    line = (
        f"{instrument.id} maker_fee={instrument.maker_fee} "
        f"taker_fee={instrument.taker_fee} [{label}]"
    )
    if source is None and (
        instrument.maker_fee == BINANCE_DEFAULT_MAKER_FEE
        and instrument.taker_fee == BINANCE_DEFAULT_TAKER_FEE
    ):
        line += (
            " WARNING: exactly the Binance VIP0 defaults - "
            "treat as the fallback, not Aster's account rate"
        )
    return line


def start_stop_watchdog(
    done_event: threading.Event,
    stop_callable: Callable[[], None],
    timeout_secs: float,
    cleanup_event: threading.Event | None = None,
    cleanup_grace_secs: float = 0.0,
) -> tuple[threading.Thread, dict[str, bool]]:
    """
    Stop the node when the probe signals done, or when ``timeout_secs`` elapses.

    ``Strategy.stop()`` only stops the strategy; the node keeps running and ``node.run()``
    keeps blocking. This watchdog owns the node-level stop so the process always terminates
    without a manual Ctrl+C. Returns the started thread and a state dict that reports whether
    the stop was triggered by the done event or by the timeout.

    When ``cleanup_event`` is given, a finished probe gets up to ``cleanup_grace_secs`` for
    its cancel requests to be confirmed before the node is stopped; the wait is bounded and
    the outcome is reported in ``state["cleanup_confirmed"]``. The timeout path never waits:
    ``on_stop`` does the last-chance cleanup there.
    """
    state = {"fired": False, "timed_out": False, "stopped": False, "cleanup_confirmed": False}

    def _run() -> None:
        fired = done_event.wait(timeout_secs)
        state["fired"] = fired
        state["timed_out"] = not fired
        if fired and cleanup_event is not None:
            state["cleanup_confirmed"] = cleanup_event.wait(cleanup_grace_secs)
        try:
            stop_callable()
        finally:
            state["stopped"] = True

    thread = threading.Thread(target=_run, name="aster-probe-watchdog", daemon=True)
    thread.start()
    return thread, state


def _guarded(method: Callable[..., None]) -> Callable[..., None]:
    """
    Log-and-fail wrapper for strategy callbacks.

    NautilusTrader swallows exceptions raised inside ``on_*`` handlers: the live run that
    found the ``cancel_order(order)`` type error showed the log line before the call and then
    nothing at all. Every handler is wrapped so an exception is logged at ERROR with its
    traceback and moves the probe into its failure state instead of vanishing.
    """

    @functools.wraps(method)
    def wrapper(self: AsterProbeStrategy, *args: object, **kwargs: object) -> None:
        try:
            return method(self, *args, **kwargs)
        except Exception:
            self.record_failure(
                f"unhandled exception in {method.__name__}:\n{traceback.format_exc()}",
            )
            return None

    return wrapper


class AsterProbeConfig(StrategyConfig):
    """
    Configuration for :class:`AsterProbeStrategy`.
    """

    _CUSTOM_FIELDS = ("instrument_id", "commission_instrument_ids", "max_notional")

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
        max_notional: Decimal = MAX_NOTIONAL_USDT,
        **_kwargs: object,
    ) -> None:
        """
        Initialize the configuration.
        """
        super().__init__()
        self.instrument_id = instrument_id
        self.commission_instrument_ids = commission_instrument_ids
        self.max_notional = min(Decimal(max_notional), MAX_NOTIONAL_USDT)


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
        self._max_notional = config.max_notional
        self._instrument: Any = None
        self._resting_order_id = None
        self._ioc_order_id = None
        self._phase = "init"

        self._orders_sent = 0
        self._event_counts: Counter[str] = Counter()
        self._fills: list[tuple[Any, Any]] = []
        self._commissions: dict[str, Decimal] = {}
        self._fee_lines: list[str] = []
        self._failures: list[str] = []
        self._resting_closed = False
        self._ioc_closed = False
        self._finished = False

        # Every order this probe created, in submit order. Cleanup only ever touches these:
        # it must never cancel account-wide.
        self._probe_order_ids: list[Any] = []
        # Once frozen, no step may send a new order; late events are bookkeeping only.
        self._frozen = False
        self._cleanup_started = False
        self._leftovers: list[str] = []

        # Set when the probe reaches a terminal state; the watchdog in main() waits on it.
        self.done_event = threading.Event()
        # Set when every probe order is confirmed closed; the watchdog waits on it, bounded.
        self.cleanup_done_event = threading.Event()
        self.summary_line = ""

    # -- results ---------------------------------------------------------------------------

    @property
    def failures(self) -> list[str]:
        """
        Return every failure reason recorded so far.
        """
        return list(self._failures)

    @property
    def orders_sent(self) -> int:
        return self._orders_sent

    @property
    def leftovers(self) -> list[str]:
        """
        Return ``id=status`` for every probe order still not confirmed closed.
        """
        return list(self._leftovers)

    @property
    def finished(self) -> bool:
        """
        Return whether the probe reached a terminal state.
        """
        return self._finished

    @property
    def exit_code(self) -> int:
        """
        Return the process exit code implied by the probe result.
        """
        return EXIT_OK if self._finished and not self._failures else EXIT_PROBE_FAILED

    def _log_safe(self, level: str, message: str) -> None:
        """
        Log ``message`` without letting a re-entrant borrow abort the caller.

        Inside a callback fired synchronously from a Strategy method (e.g. OrderPendingCancel
        from ``cancel_order``) the strategy is still mutably borrowed on the Rust side and
        ``self.log`` raises "Already mutably borrowed". The message then goes to stderr
        instead of being lost. The logger also drops multi-line messages, so those are
        flattened for the log and printed in full on stderr.
        """
        flat = " | ".join(message.splitlines())
        try:
            getattr(self.log, level)(flat)
        except RuntimeError:
            print(message, file=sys.stderr, flush=True)

    def record_failure(self, reason: str) -> None:
        """
        Record a failure, log it, and finish the probe.

        Public because :func:`_guarded` calls it from outside the class body.
        """
        self._failures.append(reason)
        # stderr first: the log call can be refused by a re-entrant borrow (see _log_safe).
        print(f"[probe] FAILED: {reason}", file=sys.stderr, flush=True)
        self._log_safe("error", f"[probe] FAILED: {reason}")
        self._finish("failure")

    # -- lifecycle -------------------------------------------------------------------------

    @_guarded
    def on_start(self) -> None:
        """
        Subscribe to quotes; the probe starts on the first one.
        """
        self._instrument = self.cache.instrument(self._instrument_id)
        if self._instrument is None:
            self.record_failure(f"instrument {self._instrument_id} not found in the cache")
            return

        self.log.info(
            f"[probe] instrument loaded: {self._instrument.id} "
            f"price_increment={self._instrument.price_increment} "
            f"size_increment={self._instrument.size_increment} "
            f"min_quantity={self._instrument.min_quantity} "
            f"min_notional={self._instrument.min_notional} "
            f"probe_budget={self._max_notional} USDT/order",
        )
        self.subscribe_quotes(self._instrument_id)
        self._phase = "awaiting_quote"
        self.log.info("[probe] waiting for the first quote before submitting")

    @_guarded
    def on_stop(self) -> None:
        """
        Freeze, run the last-chance cleanup, and report anything left open.

        The node is stopped by the watchdog, including on the hard timeout, when an accepted
        GTC may still be live and unconfirmed. ``manage_stop`` is False by default, so the
        framework cancels nothing on our behalf: this is the probe's only remaining chance to
        ask the venue to cancel the orders it created, and to name what it could not confirm.
        """
        self._frozen = True
        self._log_safe("info", f"[probe] stopping in phase={self._phase}")
        outstanding = self._run_cleanup("stop")
        if outstanding:
            detail = ", ".join(self._leftovers)
            message = (
                f"[probe] LEFTOVER: {len(outstanding)} probe order(s) not confirmed closed at "
                f"stop: {detail} - cancel requested, confirmation not received; check the "
                f"venue manually"
            )
            print(message, file=sys.stderr, flush=True)
            self._log_safe("error", message)

    # -- market data -----------------------------------------------------------------------

    @_guarded
    def on_quote(self, quote) -> None:
        """
        Start step 1 on the first quote.
        """
        if self._phase != "awaiting_quote":
            return

        self._phase = "resting_sizing"
        self.log.info(
            f"[probe] first quote: bid={quote.bid_price} ask={quote.ask_price} "
            f"ts_event={quote.ts_event}",
        )
        self._submit_resting_order(quote)

    # -- probe steps -----------------------------------------------------------------------

    def _submit_resting_order(self, quote) -> None:
        if self._frozen:
            self._log_safe("warning", "[probe] frozen; step 1 not sent")
            return

        bid = quote.bid_price.as_decimal()
        if bid <= 0:
            self.record_failure(f"first quote has a non-positive bid: {bid}")
            return

        try:
            plan = plan_order(
                self._instrument,
                bid * RESTING_PRICE_FACTOR,
                max_notional=self._max_notional,
            )
        except ProbeSizingError as exc:
            self.record_failure(f"step 1 sizing refused: {exc}")
            return

        order = self.order_factory.limit(
            instrument_id=self._instrument_id,
            order_side=OrderSide.BUY,
            quantity=plan.quantity,
            price=plan.price,
            time_in_force=TimeInForce.GTC,
        )
        self._resting_order_id = order.client_order_id
        self._probe_order_ids.append(order.client_order_id)
        self._orders_sent += 1
        self._phase = "resting_submitted"
        self.log.info(
            f"[probe] step 1: submitting GTC LIMIT BUY {plan.describe()} "
            f"(50% below bid {bid}) client_order_id={order.client_order_id}",
        )
        self.submit_order(order)

    def _submit_ioc_order(self) -> None:
        if self._frozen:
            self._log_safe("warning", "[probe] frozen; step 2 (IOC) not sent")
            return

        quote = self.cache.quote(self._instrument_id)
        if quote is None:
            self.record_failure("no cached quote for the IOC leg")
            return

        ask = quote.ask_price.as_decimal()
        if ask <= 0:
            self.record_failure(f"cached quote has a non-positive ask: {ask}")
            return

        try:
            plan = plan_order(self._instrument, ask, max_notional=self._max_notional)
        except ProbeSizingError as exc:
            self.record_failure(f"step 2 sizing refused: {exc}")
            return

        order = self.order_factory.limit(
            instrument_id=self._instrument_id,
            order_side=OrderSide.BUY,
            quantity=plan.quantity,
            price=plan.price,
            time_in_force=TimeInForce.IOC,
        )
        self._ioc_order_id = order.client_order_id
        self._probe_order_ids.append(order.client_order_id)
        self._orders_sent += 1
        self._phase = "ioc_submitted"
        self.log.info(
            f"[probe] step 2: submitting IOC LIMIT BUY {plan.describe()} "
            f"client_order_id={order.client_order_id}",
        )
        self.submit_order(order)

    def _report_fees(self) -> None:
        self._phase = "fees"
        self.log.info(f"[probe] step 3: {FEE_PROVENANCE_NOTE}")

        for instrument_id in self._commission_instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                line = f"{instrument_id} not loaded; no fee to report"
                self.log.warning(f"[probe] {line}")
            else:
                line = fee_line(instrument)
                self.log.info(f"[probe] {line}")
            self._fee_lines.append(line)

    # -- terminal handling -----------------------------------------------------------------

    def _order_or_fail(self, client_order_id) -> Any:
        order = self.cache.order(client_order_id)
        if order is None:
            self.record_failure(f"no cached order for {client_order_id}")
        return order

    def _outstanding_orders(self) -> list[tuple[Any, Any]]:
        """
        Return ``(client_order_id, order)`` for every probe order not confirmed closed.

        A cache read that fails leaves the order state unknown; that counts as outstanding
        (and is logged), because "we could not check" is not "it is closed".
        """
        outstanding: list[tuple[Any, Any]] = []
        for client_order_id in self._probe_order_ids:
            try:
                order = self.cache.order(client_order_id)
            except Exception:
                self._log_safe(
                    "error",
                    f"[probe] cannot read {client_order_id} from the cache:\n"
                    f"{traceback.format_exc()}",
                )
                order = None
            if order is None or not order.is_closed:
                outstanding.append((client_order_id, order))
        return outstanding

    @staticmethod
    def _leftover_label(client_order_id, order) -> str:
        status = order.status if order is not None else "unknown (not in the cache)"
        return f"{client_order_id}={status}"

    def _run_cleanup(self, trigger: str) -> list[tuple[Any, Any]]:
        """
        Freeze new orders and ask the venue to cancel this probe's own open orders.

        Only the orders this probe created are touched - never an account-wide cancel. The
        request is fire-and-forget here; confirmation is awaited elsewhere within a bound
        (the watchdog grace before the node stops, or reported as leftover at stop).
        """
        self._frozen = True
        self._cleanup_started = True
        outstanding = self._outstanding_orders()
        self._leftovers = [self._leftover_label(coid, order) for coid, order in outstanding]

        if not outstanding:
            self.cleanup_done_event.set()
            self._log_safe("info", f"[probe] cleanup ({trigger}): no probe order left open")
            return []

        for client_order_id, order in outstanding:
            self._log_safe(
                "warning",
                f"[probe] cleanup ({trigger}): {self._leftover_label(client_order_id, order)} "
                f"is not closed; sending cancel",
            )
            self.cancel_order(client_order_id)
        return outstanding

    def _recheck_cleanup(self) -> None:
        """
        Confirm the cleanup once every probe order is closed.
        """
        if not self._cleanup_started or self.cleanup_done_event.is_set():
            return
        outstanding = self._outstanding_orders()
        self._leftovers = [self._leftover_label(coid, order) for coid, order in outstanding]
        if outstanding:
            return
        self.cleanup_done_event.set()
        self._log_safe("info", "[probe] cleanup confirmed: every probe order is closed")

    def _check_terminal(self, client_order_id) -> None:
        """
        Advance the probe from the order's real terminal state, never from a single event.

        A zero-fill IOC arrives as OrderCanceled (``treat_expired_as_canceled=True``) or
        OrderExpired; a partial fill is followed by a cancel/expiry for the remainder; a full
        fill ends with ``filled_qty == quantity``. Only ``is_closed`` decides.
        """
        order = self._order_or_fail(client_order_id)
        if order is None:
            return

        if not order.is_closed:
            self.log.info(
                f"[probe] {client_order_id} still open: status={order.status} "
                f"filled_qty={order.filled_qty}/{order.quantity}",
            )
            return

        if self._finished:
            # Late terminal report. The probe already ended (success or failure), so this can
            # only update bookkeeping and close out the cleanup - never start a new step.
            if client_order_id == self._resting_order_id:
                self._resting_closed = True
            elif client_order_id == self._ioc_order_id:
                self._ioc_closed = True
            self._log_safe(
                "info",
                f"[probe] late terminal report for {client_order_id}: status={order.status} "
                f"filled_qty={order.filled_qty}/{order.quantity} "
                f"(probe already finished; no new step)",
            )
            self._recheck_cleanup()
            return

        if client_order_id == self._resting_order_id and not self._resting_closed:
            self._resting_closed = True
            self.log.info(
                f"[probe] resting order terminal: status={order.status} "
                f"filled_qty={order.filled_qty}/{order.quantity}",
            )
            self._recheck_cleanup()
            if order.status.name != "CANCELED":
                # The cancel step is what step 1 exists to verify. A resting leg that ends
                # FILLED or EXPIRED never proved it, so the run has failed - and the probe
                # must not add another position on top of the one it just took.
                self.record_failure(
                    f"resting order ended as {order.status.name} instead of CANCELED: the "
                    f"cancel was never verified (filled_qty={order.filled_qty}/"
                    f"{order.quantity}); not sending the IOC leg",
                )
                return
            self._submit_ioc_order()
            return

        if client_order_id == self._ioc_order_id and not self._ioc_closed:
            self._ioc_closed = True
            self.log.info(
                f"[probe] IOC order terminal: status={order.status} "
                f"filled_qty={order.filled_qty}/{order.quantity}",
            )
            self._recheck_cleanup()
            self._report_fees()
            self._finish("complete")

    def _finish(self, reason: str) -> None:
        if self._finished:
            return
        self._finished = True
        self._frozen = True
        self._phase = "done"

        # Ask the venue to cancel anything this probe left open before the node is stopped.
        self._run_cleanup(f"finish:{reason}")

        events = " ".join(f"{name}={count}" for name, count in sorted(self._event_counts.items()))
        commissions = (
            " ".join(
                f"{currency}={amount}" for currency, amount in sorted(self._commissions.items())
            )
            or "none"
        )
        filled_qty = sum((qty for qty, _ in self._fills), Decimal(0))
        self.summary_line = (
            f"[probe] SUMMARY reason={reason} "
            f"result={'ok' if not self._failures else 'failed'} "
            f"orders_sent={self._orders_sent} events=[{events}] "
            f"fills={len(self._fills)} filled_qty={filled_qty} "
            f"commissions=[{commissions}] fee_lines={len(self._fee_lines)} "
            f"outstanding={len(self._leftovers)} "
            f"failures={len(self._failures)} exit_code={self.exit_code}"
        )
        self._log_safe("error" if self._failures else "info", self.summary_line)
        self.done_event.set()

    def _record_event(self, name: str, event) -> None:
        self._event_counts[name] += 1
        self.log.info(
            f"[probe] {name} client_order_id={event.client_order_id} ts_event={event.ts_event}",
        )

    # -- order events ----------------------------------------------------------------------

    @_guarded
    def on_order_submitted(self, event) -> None:
        """
        Count the submission.
        """
        self._record_event("OrderSubmitted", event)

    @_guarded
    def on_order_accepted(self, event) -> None:
        """
        Log the acceptance and, for the resting order, cancel it.
        """
        self._record_event("OrderAccepted", event)
        if event.client_order_id != self._resting_order_id:
            return

        order = self._order_or_fail(event.client_order_id)
        if order is None:
            return

        self.log.info(
            f"[probe] resting order status: status={order.status} quantity={order.quantity} "
            f"filled_qty={order.filled_qty} price={order.price} "
            f"venue_order_id={order.venue_order_id}",
        )

        self._phase = "cancel_requested"
        self.log.info(f"[probe] cancelling resting order {event.client_order_id}")
        # Strategy.cancel_order takes a ClientOrderId. The order-object form belongs to
        # ExecutionAlgorithm; passing an order here raises a TypeError that the framework
        # swallows inside the callback, and the order silently stays live on the venue.
        self.cancel_order(order.client_order_id)

    @_guarded
    def on_order_pending_cancel(self, event) -> None:
        """
        Count the event without touching ``self.log`` / ``self.cache``.

        OrderPendingCancel is published synchronously from inside ``Strategy.cancel_order``
        while the strategy is still mutably borrowed on the Rust side; any re-entrant call
        into the strategy (logging included) raises "Already mutably borrowed".
        """
        self._event_counts["OrderPendingCancel"] += 1

    @_guarded
    def on_order_cancel_rejected(self, event) -> None:
        """
        A rejected cancel leaves the resting order live; the probe cannot finish cleanly.
        """
        self._event_counts["OrderCancelRejected"] += 1
        self.record_failure(
            f"OrderCancelRejected client_order_id={event.client_order_id} reason={event.reason}",
        )

    @_guarded
    def on_order_canceled(self, event) -> None:
        """
        Re-check the order's terminal state (a zero-fill IOC also arrives here).
        """
        self._record_event("OrderCanceled", event)
        self._check_terminal(event.client_order_id)

    @_guarded
    def on_order_expired(self, event) -> None:
        """
        Re-check the order's terminal state.
        """
        self._record_event("OrderExpired", event)
        self._check_terminal(event.client_order_id)

    @_guarded
    def on_order_filled(self, event) -> None:
        """
        Accumulate the fill, then re-check the order's terminal state.

        A partial fill must not finish the probe: the order stays open until the remainder is
        cancelled or expires.
        """
        self._event_counts["OrderFilled"] += 1
        self._fills.append((event.last_qty.as_decimal(), event.last_px.as_decimal()))
        commission = event.commission
        if commission is not None:
            code = commission.currency.code
            self._commissions[code] = (
                self._commissions.get(code, Decimal(0)) + commission.as_decimal()
            )
        self.log.info(
            f"[probe] OrderFilled client_order_id={event.client_order_id} "
            f"last_qty={event.last_qty} last_px={event.last_px} "
            f"commission={commission} liquidity_side={event.liquidity_side} "
            f"ts_event={event.ts_event}",
        )
        self._check_terminal(event.client_order_id)

    @_guarded
    def on_order_rejected(self, event) -> None:
        """
        A rejection on either leg is a probe failure.
        """
        self._event_counts["OrderRejected"] += 1
        self.record_failure(
            f"OrderRejected client_order_id={event.client_order_id} reason={event.reason}",
        )

    @_guarded
    def on_order_denied(self, event) -> None:
        """
        A local denial (risk engine, validation) on either leg is a probe failure.
        """
        self._event_counts["OrderDenied"] += 1
        self.record_failure(
            f"OrderDenied client_order_id={event.client_order_id} reason={event.reason}",
        )


def resolve_environment() -> AsterEnvironment | None:
    """
    Return the Aster environment to run against, or ``None`` when the probe must refuse.

    Unset or a true-ish ``ASTER_TESTNET`` selects testnet. Anything else - including
    ``false``, ``0`` and unrecognised values - returns ``None``: this probe has no mainnet
    path.
    """
    raw = os.environ.get("ASTER_TESTNET")
    if raw is None or raw.strip().lower() in _TRUE_VALUES:
        return AsterEnvironment.TESTNET
    return None


def build_parser() -> argparse.ArgumentParser:
    """
    Return the command-line parser.
    """
    return _build_parser()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aster_exec_probe",
        description=(
            "Probe the Aster DEX execution client on TESTNET ONLY: rest and cancel a "
            "far-from-market GTC LIMIT order, then send a minimum-size IOC LIMIT order, then "
            f"log the adapter's fee values. Every order is capped at {MAX_NOTIONAL_USDT} USDT "
            "notional. Reads ASTER_SIGNER_PRIVATE_KEY / ASTER_SIGNER_ADDRESS / "
            "ASTER_USER_ADDRESS / ASTER_TESTNET from the environment; a non-testnet "
            "ASTER_TESTNET makes the probe refuse to start."
        ),
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
        "--timeout-secs",
        type=float,
        default=DEFAULT_TIMEOUT_SECS,
        help=(
            "watchdog: stop the node this long after start even if the probe never finishes "
            f"(default: {DEFAULT_TIMEOUT_SECS})"
        ),
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

    environment = resolve_environment()
    if environment is None:
        print(
            "Refusing to start: this probe runs on Aster TESTNET only, but ASTER_TESTNET does "
            "not resolve to testnet. Unset it (or set it to 'true') to run on testnet. Mainnet "
            "execution belongs to a separate script that enforces hard per-order and total "
            "exposure limits, not to this probe.",
            file=sys.stderr,
        )
        return EXIT_REFUSED

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
    # The risk engine is NOT bypassed: it is the second line of defence behind plan_order,
    # and it enforces the same per-order notional cap the probe sizes against.
    risk_config = LiveRiskEngineConfig(
        bypass=False,
        max_notional_per_order={symbol: str(MAX_NOTIONAL_USDT) for symbol in load_ids},
    )

    print(f"venue             : {ASTER}")
    print(f"environment       : {environment}")
    print(f"instrument        : {instrument_id}")
    print(f"instruments loaded: {', '.join(load_ids)}")
    print(f"max notional/order: {MAX_NOTIONAL_USDT} USDT (risk engine bypass=False)")
    print(f"watchdog timeout  : {args.timeout_secs}s")
    print(f"signer key set    : {bool(os.environ.get('ASTER_SIGNER_PRIVATE_KEY'))}")
    print(f"signer address set: {bool(os.environ.get('ASTER_SIGNER_ADDRESS'))}")
    print(f"user address set  : {bool(os.environ.get('ASTER_USER_ADDRESS'))}")

    if args.dry_run:
        print("dry run: configuration built, exiting without connecting")
        return EXIT_OK

    if not os.environ.get("ASTER_SIGNER_PRIVATE_KEY"):
        print(
            "ASTER_SIGNER_PRIVATE_KEY is not set; the execution client cannot sign requests.",
            file=sys.stderr,
        )
        return EXIT_REFUSED

    # Venue connects are flaky through this host's proxy (exchangeInfo / user-stream
    # timeouts). A failed connect never sends an order, so retrying the whole node is safe;
    # once any order has been sent the run is never repeated.
    exit_code = EXIT_PROBE_FAILED
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        node = (
            LiveNode.builder(
                "ASTER-EXEC-PROBE-001",
                TraderId.from_str("TESTER-001"),
                Environment.LIVE,
            )
            .with_logging(LoggerConfig(stdout_level=LogLevel.from_str(args.log_level)))
            .with_risk_engine_config(risk_config)
            .with_timeout_connection(args.connection_timeout_secs)
            .add_data_client(None, AsterDataClientFactory(), data_config)
            .add_exec_client(None, AsterExecutionClientFactory(), exec_config)
            .build()
        )
        strategy = AsterProbeStrategy(
            AsterProbeConfig(
                instrument_id=instrument_id,
                commission_instrument_ids=(BTC_INSTRUMENT_ID, NVDA_INSTRUMENT_ID),
            ),
        )
        node.add_strategy(strategy)

        handle = node.handle()
        watchdog, watchdog_state = start_stop_watchdog(
            strategy.done_event,
            handle.stop,
            args.timeout_secs,
            strategy.cleanup_done_event,
            CLEANUP_GRACE_SECS,
        )

        print(f"starting node against {ASTER} ({environment}) attempt {attempt}/{CONNECT_ATTEMPTS}")
        run_error: BaseException | None = None
        try:
            node.run()
        except KeyboardInterrupt as exc:
            run_error = exc
            print("[probe] interrupted, stopping", file=sys.stderr)
            handle.stop()
        except Exception as exc:  # noqa: BLE001 - reported, never swallowed
            run_error = exc
            traceback.print_exc()
        finally:
            strategy.done_event.set()  # release the watchdog if the node returned on its own
            watchdog.join(timeout=5.0)

        connect_failed = (
            isinstance(run_error, RuntimeError)
            and strategy.orders_sent == 0
            and not strategy.finished
        )
        if connect_failed and attempt < CONNECT_ATTEMPTS:
            print(
                f"[probe] attempt {attempt}/{CONNECT_ATTEMPTS} did not connect: {run_error}; "
                f"retrying in {CONNECT_RETRY_SECS}s",
                file=sys.stderr,
            )
            time.sleep(CONNECT_RETRY_SECS)
            continue

        print(strategy.summary_line or "[probe] SUMMARY unavailable: the probe never finished")
        for reason in strategy.failures:
            print(f"[probe] failure detail: {ascii(reason)}", file=sys.stderr, flush=True)
        if run_error is not None:
            exit_code = EXIT_PROBE_FAILED
        elif watchdog_state["timed_out"]:
            print(
                f"[probe] watchdog fired after {args.timeout_secs}s without a result",
                file=sys.stderr,
            )
            exit_code = EXIT_TIMEOUT
        else:
            exit_code = strategy.exit_code

        if strategy.leftovers:
            # The probe asked for these to be cancelled but never saw the confirmation; a run
            # that may have left a live order on the venue is not a clean run.
            print(
                f"[probe] LEFTOVER unconfirmed order(s): {', '.join(strategy.leftovers)}",
                file=sys.stderr,
                flush=True,
            )
            if exit_code == EXIT_OK:
                exit_code = EXIT_PROBE_FAILED
        elif watchdog_state["fired"]:
            print(f"[probe] cleanup confirmed: {watchdog_state['cleanup_confirmed']}")
        break
    print(f"[probe] exit code: {exit_code}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
