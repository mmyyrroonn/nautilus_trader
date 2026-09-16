// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! WebSocket transport surface for the Ondo Perps public market data feed.
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`messages`] | The wire schema: channels, requests, and the per-channel item shapes. |
//! | [`parse`] | Wire bytes to Nautilus domain values, including the one place a wire number is converted. |
//! | [`book`] | The book state machine: full replacement, event-time ordering, session ownership. |
//! | [`client`] | The session state machine and the transport: one connection, heartbeat, idle bound, reconnect. |
//! | [`private`] | The **private** surface: the login handshake, the account's channels, and the transport that owns them. |
//!
//! The two surfaces are separate connections on purpose: the public data client reads no key, and
//! the private channels require a login. See [`private`] for why that split is required rather than
//! convenient.

pub mod book;
pub mod client;
pub mod messages;
pub mod parse;
pub mod private;

pub use book::{BookCounters, OndoBookState, OrderBookLevels, SnapshotOutcome};
pub use client::{
    ONDO_WS_IDLE_TIMEOUT_SECS, ONDO_WS_MAX_CLIENT_MESSAGE_BYTES, ONDO_WS_MAX_SERVER_MESSAGE_BYTES,
    ONDO_WS_REQUEST_BURST, ONDO_WS_REQUEST_RATE_PER_SECOND, OndoWebSocketClient, OndoWsSession,
    REASON_DISCONNECTED, REASON_SNAPSHOT_READY, SessionCounters, WsCommand, WsOutcome,
    WsSessionCommand, reconnect_backoff, request_quota,
};
pub use messages::{
    BookSnapshotItem, FundingRateItem, MarkPriceItem, PremiumSample, SubscriptionRequest,
    TradeItem, WsChannel, WsMessageType, WsOp,
};
pub use parse::{
    Converted, EventTimeSource, NO_EXCHANGE_SEQUENCE, ParsedBookSnapshot, ParsedFundingRate,
    ParsedMarkPrice, ServerMessage, WireBookSnapshot, WireLevel, WsUpdate, convert_price,
    convert_quantity, decode_updates, parse_book_snapshot, parse_depth10, parse_funding_rate,
    parse_mark_price, parse_quote_tick, parse_server_message, parse_trade_tick,
};
