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
Probe an installed wheel through Python DataActor -> LiveNode -> native Hyperliquid.

The default loopback mode asserts the actual subscription JSON and moving snapshots.
Pass --public for a bounded, credential-free BTC public feed sample; run each fast mode
in a separate process. No execution client is constructed. Raw timestamps and depth
counts are retained so measured cadence is evidence, not a promised SLA.

"""

from __future__ import annotations

# This standalone evidence script is not an importable package.
# ruff: noqa: INP001
import argparse
import json
import os
import statistics
import threading
import time
from contextlib import ExitStack
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from itertools import pairwise
from pathlib import Path

from websockets.sync.server import ServerConnection
from websockets.sync.server import serve

from nautilus_trader.adapters.hyperliquid import HyperliquidDataClientConfig
from nautilus_trader.adapters.hyperliquid import HyperliquidDataClientFactory
from nautilus_trader.common import DataActor
from nautilus_trader.common import Environment
from nautilus_trader.common import LoggerConfig
from nautilus_trader.common import LogLevel
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BookType
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderBook
from nautilus_trader.model import OrderBookDeltas
from nautilus_trader.model import TraderId


MAX_DURATION_SECS = 120
FRESHNESS_MS = 2000
INSTRUMENT = InstrumentId.from_str("BTC-USD-PERP.HYPERLIQUID")


class Loopback:
    """
    Serve public metadata and record native subscribe/unsubscribe payloads.
    """

    def __init__(self, stack: ExitStack, *, fast: bool | None) -> None:  # noqa: C901
        """
        Start the finite loopback fixture and register cleanup.
        """
        self.messages: list[dict] = []
        self.frames: list[dict] = []
        self.errors: list[str] = []
        meta = {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]}
        spot = {
            "universe": [],
            "tokens": [
                {
                    "name": "USDC",
                    "szDecimals": 6,
                    "weiDecimals": 6,
                    "index": 0,
                    "tokenId": "0x1",
                    "isCanonical": True,
                },
            ],
        }
        responses = {
            "meta": meta,
            "allPerpMetas": [meta],
            "perpDexs": [None],
            "spotMeta": spot,
            "outcomeMeta": {"outcomes": [], "questions": []},
        }
        errors = self.errors

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path != "/info" or request["type"] not in responses:
                    errors.append(f"unexpected HTTP request: {self.path} {request['type']}")
                    self.send_error(400)
                    return
                body = json.dumps(responses[request["type"]]).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        stack.callback(http.server_close)
        stack.callback(http.shutdown)
        threading.Thread(target=http.serve_forever, daemon=True).start()
        self.http_url = f"http://127.0.0.1:{http.server_port}/info"
        levels = 5 if fast is True else 20
        for shift, count in [(0, levels), (100, levels), (200, 3)]:
            self.frames.append(
                {
                    "channel": "l2Book",
                    "data": {
                        "coin": "BTC",
                        "time": 1703875200001 + shift,
                        "levels": [
                            [
                                {"px": str(98000 + shift - i), "sz": "1", "n": 1}
                                for i in range(count)
                            ],
                            [
                                {"px": str(98001 + shift + i), "sz": "2", "n": 2}
                                for i in range(count)
                            ],
                        ],
                    },
                },
            )

        def handle(socket: ServerConnection) -> None:
            for text in socket:
                message = json.loads(text)
                self.messages.append(message)
                if message["method"] == "ping":
                    socket.send(json.dumps({"channel": "pong"}))
                elif message["method"] == "subscribe":
                    expected = {"type": "l2Book", "coin": "BTC", "nSigFigs": 5, "mantissa": 2}
                    if fast is not None:
                        expected["fast"] = fast
                    if message["subscription"] != expected:
                        self.errors.append(f"unexpected subscription: {message}")
                        continue
                    socket.send(json.dumps({"channel": "subscriptionResponse", "data": message}))
                    for frame in self.frames:
                        socket.send(json.dumps(frame))

        ws = serve(handle, "127.0.0.1", 0)
        stack.callback(ws.shutdown)
        threading.Thread(target=ws.serve_forever, daemon=True).start()
        self.ws_url = f"ws://127.0.0.1:{ws.socket.getsockname()[1]}"


class BookProbe(DataActor):
    """
    Maintain the real native book from Python subscription callbacks.
    """

    def initialize_probe(self, params: dict, seconds: float) -> BookProbe:
        """
        Set the subscription parameters and observation window.
        """
        self.params = params
        self.seconds = seconds
        self.samples: list[dict] = []
        self.book = OrderBook(INSTRUMENT, BookType.L2_MBP)
        self.handle = None
        self.timer = None
        self.started_ns = None
        return self

    def on_start(self) -> None:
        """
        Subscribe through the Python actor entry point.
        """
        self.started_ns = time.time_ns()
        self.subscribe_book_deltas(INSTRUMENT, BookType.L2_MBP, params=self.params)
        self.timer = threading.Timer(self.seconds, self.handle.stop)
        self.timer.daemon = True
        self.timer.start()

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Apply native snapshots and record their actual depth and timestamps.
        """
        self.book.apply_deltas(deltas)
        self.samples.append(
            {
                "source_ns": deltas.ts_event,
                "native_receive_ns": deltas.ts_init,
                "callback_ns": time.time_ns(),
                "bids": len(self.book.bids()),
                "asks": len(self.book.asks()),
                "best_bid": str(self.book.best_bid_price()),
                "best_ask": str(self.book.best_ask_price()),
            },
        )

    def on_stop(self) -> None:
        """
        Release the subscription and timer.
        """
        self.unsubscribe_book_deltas(INSTRUMENT)
        if self.timer:
            self.timer.cancel()


def main() -> None:
    """
    Run one bounded native observation and write its evidence.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public", action="store_true")
    parser.add_argument("--fast", choices=("omitted", "true", "false"), default="true")
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= MAX_DURATION_SECS:
        parser.error("--seconds must be between 1 and 120")
    for key in ("HYPERLIQUID_PK", "HYPERLIQUID_TESTNET_PK"):
        os.environ.pop(key, None)
    fast = {"omitted": None, "true": True, "false": False}[args.fast]
    params = {} if args.public else {"n_sig_figs": 5, "mantissa": 2}
    if fast is not None:
        params["fast"] = fast

    with ExitStack() as stack:
        venue = None if args.public else Loopback(stack, fast=fast)
        config = HyperliquidDataClientConfig(
            base_url_http=venue.http_url if venue else None,
            base_url_ws=venue.ws_url if venue else None,
            update_instruments_interval_mins=0,
            stale_stream_recovery_enabled=False,
        )
        node = (
            LiveNode.builder(
                "HYPERLIQUID-FAST-BOOK",
                TraderId.from_str("PROBE-001"),
                Environment.LIVE,
            )
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .add_data_client(None, HyperliquidDataClientFactory(), config)
            .build()
        )
        stack.callback(node.dispose)
        actor = BookProbe().initialize_probe(params, args.seconds)
        node.add_actor(actor)
        actor.handle = node.handle()
        watchdog = threading.Timer(args.seconds + 45, actor.handle.stop)
        stack.callback(watchdog.cancel)
        watchdog.start()
        node.run()
        samples = actor.samples
        intervals = [
            (b["native_receive_ns"] - a["native_receive_ns"]) / 1e6 for a, b in pairwise(samples)
        ]
        ages = [(item["native_receive_ns"] - item["source_ns"]) / 1e6 for item in samples]
        errors = list(venue.errors) if venue else []
        if not samples:
            errors.append("no native book callbacks received")
        if venue:
            expected = [
                (
                    frame["data"]["time"] * 1_000_000,
                    len(frame["data"]["levels"][0]),
                    len(frame["data"]["levels"][1]),
                    Decimal(frame["data"]["levels"][0][0]["px"]),
                    Decimal(frame["data"]["levels"][1][0]["px"]),
                )
                for frame in venue.frames
            ]
            actual = [
                (
                    sample["source_ns"],
                    sample["bids"],
                    sample["asks"],
                    Decimal(sample["best_bid"]),
                    Decimal(sample["best_ask"]),
                )
                for sample in samples
            ]
            if actual != expected:
                errors.append(f"snapshot/timestamp mismatch: {actual} != {expected}")
            subs = [msg["subscription"] for msg in venue.messages if msg["method"] == "subscribe"]
            unsubs = [
                msg["subscription"] for msg in venue.messages if msg["method"] == "unsubscribe"
            ]
            if not subs or unsubs != subs:
                errors.append("final unsubscribe differs from original subscription")
        max_depth = 5 if fast is True else 20
        if any(sample[side] > max_depth for sample in samples for side in ("bids", "asks")):
            errors.append("native book contains more than the requested feed's depth")
        result = {
            "public_only": True,
            "endpoint": "public" if args.public else "loopback",
            "params": params,
            "duration_secs": args.seconds,
            "started_ns": actor.started_ns,
            "count": len(samples),
            "samples": samples,
            "interval_ms_median": statistics.median(intervals) if intervals else None,
            "interval_ms_max": max(intervals) if intervals else None,
            "source_age_ms_median": statistics.median(ages) if ages else None,
            "receive_gaps_over_2s": sum(value > FRESHNESS_MS for value in intervals),
            "missing_sides": sum(not row["bids"] or not row["asks"] for row in samples),
            "missing_message_count": None,
            "missing_message_note": "l2Book has no sequence number; loss cannot be quantified",
            "wire_messages": venue.messages if venue else None,
            "errors": errors,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if errors:
            raise RuntimeError("; ".join(errors))
        print(f"Verified {len(samples)} native book callbacks: {args.output}")  # noqa: T201


if __name__ == "__main__":
    main()
