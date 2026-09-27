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
Checks an installed wheel through LiveNode and the application's account reporter.

All endpoints bind to loopback; the signer is derived from a public test label. No .env
is read and no real venue is contacted. Requires websockets and the wheel under test.

"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import threading
import time
import traceback
from datetime import timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs
from urllib.parse import urlsplit

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from nautilus_trader.adapters.aster import AsterEnvironment
from nautilus_trader.adapters.aster import AsterExecutionClientConfig
from nautilus_trader.adapters.aster import AsterExecutionClientFactory
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.common import Environment
from nautilus_trader.common import LoggerConfig
from nautilus_trader.common import LogLevel
from nautilus_trader.live import LiveExecutionEngineConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import Currency
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderSide
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import TraderId
from nautilus_trader.trading import Strategy


BTC = InstrumentId.from_str("BTCUSDT-PERP.ASTER")


def amount(money) -> Decimal:
    """
    Return the exact numeric part of native Money.
    """
    return Decimal(str(money).split()[0])


def order_row(order_id, side, timestamp) -> dict:
    """
    Build a fully filled one-way order response.
    """
    return {
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "clientOrderId": f"O-OFFLINE-{order_id}",
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": "0.010",
        "executedQty": "0.010",
        "cumQuote": "500.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": side == "SELL",
        "closePosition": False,
        "time": timestamp,
        "updateTime": timestamp,
    }


def trade_row(trade_id, order_id, side, timestamp) -> dict:
    """
    Build a real trade response with an explicit commission.
    """
    return {
        "symbol": "BTCUSDT",
        "id": trade_id,
        "orderId": order_id,
        "price": "50000.00",
        "qty": "0.010",
        "quoteQty": "500.0",
        "realizedPnl": "0",
        "side": side,
        "positionSide": "BOTH",
        "maker": True,
        "buyer": side == "BUY",
        "commission": "0.02",
        "commissionAsset": "USDT",
        "time": timestamp,
    }


class MockVenue:
    """
    Serves the scripted account exclusively over loopback HTTP and WebSocket.
    """

    def __init__(self, fixtures) -> None:  # noqa: C901
        """
        Start the mock transports and seeds a real opening trade.
        """
        self.loop = asyncio.new_event_loop()
        self.sockets = set()
        self.connections = 0
        self.requests = []
        self.lock = threading.Lock()
        self.open_time = int(time.time() * 1000) - 60_000
        self.orders = [order_row(990001, "BUY", self.open_time)]
        self.trades = [trade_row(990101, 990001, "BUY", self.open_time)]
        self.positions = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.010",
                "entryPrice": "50000.00",
                "positionSide": "BOTH",
                "updateTime": self.open_time,
            },
        ]
        self.balances = [{"asset": "USDT", "balance": "100", "availableBalance": "20"}]
        exchange_info = json.loads((fixtures / "http_exchange_info.json").read_text())["response"]
        venue = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def do_GET(self):
                self.answer()

            def do_POST(self):
                self.answer()

            def do_PUT(self):
                self.answer()

            def do_DELETE(self):
                self.answer()

            def answer(self):
                parsed = urlsplit(self.path)
                params = parse_qs(parsed.query)
                endpoint = parsed.path.rsplit("/", 1)[-1]
                with venue.lock:
                    venue.requests.append((self.command, endpoint))
                    if endpoint == "exchangeInfo":
                        body = exchange_info
                    elif endpoint == "dual":
                        body = {"dualSidePosition": False}
                    elif endpoint == "commissionRate":
                        body = {
                            "symbol": params.get("symbol", ["BTCUSDT"])[0],
                            "makerCommissionRate": "0.00005",
                            "takerCommissionRate": "0.0004",
                        }
                    elif endpoint == "balance":
                        body = venue.balances
                    elif endpoint == "positionRisk":
                        body = venue.positions
                    elif endpoint == "openOrders":
                        body = []
                    elif endpoint in {"allOrders", "userTrades"}:
                        rows = venue.orders if endpoint == "allOrders" else venue.trades
                        body = [
                            r
                            for r in rows
                            if r["symbol"] == params.get("symbol", [""])[0]
                            and r["time"] >= int(params.get("startTime", [0])[0])
                            and r["time"] <= int(params.get("endTime", [2**63 - 1])[0])
                            and r.get("id", 0) >= int(params.get("fromId", [0])[0])
                        ]
                    elif endpoint == "order" and self.command == "GET":
                        body = next(
                            r
                            for r in venue.orders
                            if str(r["orderId"]) == params.get("orderId", [""])[0]
                            or r["clientOrderId"] == params.get("origClientOrderId", [""])[0]
                        )
                    elif endpoint == "listenKey":
                        body = {"listenKey": "offline-account-position"}
                    else:
                        body = {"code": -1000, "msg": "unexpected offline endpoint"}
                payload = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        ready = threading.Event()

        async def websocket(socket):
            self.sockets.add(socket)
            self.connections += 1
            try:
                async for _ in socket:
                    pass
            except ConnectionClosed:
                pass
            finally:
                self.sockets.discard(socket)

        async def start():
            self.ws = await serve(websocket, "127.0.0.1", 0)
            self.ws_port = self.ws.sockets[0].getsockname()[1]
            ready.set()

        def run():
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(start())
            self.loop.run_forever()

        threading.Thread(target=run, daemon=True).start()
        assert ready.wait(10), "mock websocket startup timed out"

    def send(self, payload) -> None:
        """
        Send a frame on the currently connected private stream.
        """

        async def broadcast():
            for socket in tuple(self.sockets):
                await socket.send(json.dumps(payload))

        asyncio.run_coroutine_threadsafe(broadcast(), self.loop).result(5)

    def disconnect(self) -> None:
        """
        Close every private socket to trigger adapter recovery.
        """

        async def close():
            for socket in tuple(self.sockets):
                await socket.close()

        asyncio.run_coroutine_threadsafe(close(), self.loop).result(5)


class Acceptance(Strategy):
    """
    Observes native accounts, risk admission, and recovered position economics.
    """

    def __init__(self) -> None:
        """
        Initialize the acceptance state machine.
        """
        super().__init__()
        self.phase = 0
        self.results = {}
        self.failure = None
        self.display = []

    def report_application(self) -> None:
        """
        Run the existing application reporter against the native portfolio.
        """
        proxy = SimpleNamespace(
            _maker_id=BTC,
            _hedge_inst=None,
            portfolio=self.portfolio,
            cache=self.cache,
            _marks=lambda: (50000.0, 50000.0),
            _log_safe=lambda level, message: self.display.append(message),
        )
        self.reporter(proxy)

    def on_start(self) -> None:
        """
        Start polling after startup reconciliation has completed.
        """
        self.clock.set_timer("acceptance", timedelta(milliseconds=100), callback=self.tick)
        self.deadline = time.monotonic() + 60

    def on_order_denied(self, event) -> None:
        """
        Record the native risk engine's reason for denying the test order.
        """
        self.results["risk_denial"] = event.reason

    def tick(self, _event) -> None:
        """
        Check the next observable transition and stops on any failed assertion.
        """
        try:
            self.check()
        except Exception as e:
            self.failure = f"phase {self.phase}: {e!r}\n{traceback.format_exc()}"
            self.handle.stop()

    def check(self) -> None:
        """
        Advances only after the account or position confirms the required state.
        """
        assert time.monotonic() < self.deadline, f"acceptance timed out in phase {self.phase}"
        account = self.portfolio.account(BTC.venue)
        assert account is not None, "native account missing"
        free = amount(account.balance_free(Currency.from_str("USDT")))
        if self.phase == 0:
            assert free == 20, f"initial free={free}, balance={account.balances()}"
            assert self.cache.positions_open(instrument_id=BTC), (
                "startup did not restore the open position"
            )
            self.report_application()
            self.ws_time = int(time.time() * 1000)
            self.venue.send(
                {
                    "e": "ACCOUNT_UPDATE",
                    "E": self.ws_time,
                    "T": self.ws_time,
                    "a": {
                        "m": "ORDER",
                        "B": [{"a": "USDT", "wb": "100", "cw": "100", "bc": "0"}],
                        "P": [],
                    },
                },
            )
            self.phase = 1
        elif self.phase == 1 and account.last_event.ts_event == self.ws_time * 1_000_000:
            assert free == 20, "WS cross wallet balance raised native availability"
            self.report_application()
            native_text = str(account.balance_free(Currency.from_str("USDT")))
            assert any(f"free {native_text}" in text for text in self.display)
            self.results["ws_free"] = str(free)
            order = self.order_factory.limit(
                BTC,
                OrderSide.BUY,
                Quantity.from_str("0.005"),
                Price.from_str("50000.00"),
            )
            self.submit_order(order)
            self.phase = 2
        elif self.phase == 2 and "risk_denial" in self.results:
            assert (
                "margin" in self.results["risk_denial"].lower()
                or "balance" in self.results["risk_denial"].lower()
            ), self.results
            timestamp = int(time.time() * 1000)
            with self.venue.lock:
                self.venue.orders.append(order_row(990002, "SELL", timestamp))
                self.venue.trades.append(trade_row(990102, 990002, "SELL", timestamp))
                self.venue.positions = []
                self.venue.balances = [
                    {"asset": "USDT", "balance": "100", "availableBalance": "30"},
                ]
            self.venue.disconnect()
            self.phase = 3
        elif self.phase == 3 and free == 30 and not self.cache.positions_open(instrument_id=BTC):
            self.check_economics()
            self.before_repeat = account.event_count
            self.venue.disconnect()
            self.phase = 4
        elif (
            self.phase == 4
            and self.venue.connections >= 3
            and account.event_count > self.before_repeat
        ):
            self.check_economics()
            assert not self.cache.positions_open(instrument_id=BTC)
            assert not any(
                method == "POST" and endpoint == "order" for method, endpoint in self.venue.requests
            )
            self.report_application()
            self.results.update(
                flat=True,
                closing_trade="990102",
                commission_each="0.02 USDT",
                repeated_reconnect=True,
                live_orders=0,
                application_display=self.display,
            )
            self.phase = 5
            self.handle.stop()

    def check_economics(self) -> None:
        """
        Check exact real trade IDs and fees with no synthetic or duplicate effects.
        """
        orders = self.cache.orders(instrument_id=BTC)
        filled = [o for o in orders if o.trade_ids]
        assert len(filled) == 2, f"unexpected filled orders: {filled}"
        ids = sorted(str(t) for o in filled for t in o.trade_ids)
        assert ids == ["990101", "990102"], ids
        assert all(
            amount(o.commissions()[Currency.from_str("USDT")]) == Decimal("0.02") for o in filled
        )


def main() -> None:
    """
    Run acceptance using the installed wheel and writes the checked observations.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(args.app_root / "src"))
    from maker_live import LighterMaker

    fixtures = Path(__file__).resolve().parents[4] / "crates/adapters/aster/test_data"
    venue = MockVenue(fixtures)
    config = AsterExecutionClientConfig(
        environment=AsterEnvironment.TESTNET,
        signer_private_key=hashlib.sha256(b"public offline acceptance signer").hexdigest(),
        base_url_http=f"http://127.0.0.1:{venue.http.server_port}",
        base_url_ws=f"ws://127.0.0.1:{venue.ws_port}",
        instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(BTC)]),
    )
    node = (
        LiveNode.builder(
            "ASTER-ACCOUNT-POSITION",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        .with_exec_engine_config(LiveExecutionEngineConfig(filter_unclaimed_external_orders=False))
        .add_exec_client(None, AsterExecutionClientFactory(), config)
        .build()
    )
    strategy = Acceptance()
    strategy.venue = venue
    strategy.reporter = LighterMaker._report_accounts
    strategy.handle = node.handle()
    node.add_strategy(strategy)
    watchdog = threading.Timer(80, strategy.handle.stop)
    watchdog.start()
    try:
        node.run()
    finally:
        watchdog.cancel()
        node.dispose()
        venue.http.shutdown()
    assert strategy.failure is None, strategy.failure
    assert strategy.results.get("flat"), strategy.results
    args.output.write_text(json.dumps(strategy.results, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write("installed wheel account/position acceptance passed; no live orders\n")


if __name__ == "__main__":
    main()
