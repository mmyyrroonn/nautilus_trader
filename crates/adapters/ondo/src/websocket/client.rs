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

//! WebSocket lifecycle for the Ondo Perps public market data feed.
//!
//! The public feed is one multiplexed endpoint carrying five channels, so this file has two
//! layers:
//!
//! - [`OndoWsSession`] is the protocol state machine: subscription intent and confirmation, the
//!   per-instrument books ([`crate::websocket::book`]), and the routing of every frame. It performs
//!   no I/O, which is why it is fully covered by offline tests.
//! - [`OndoWebSocketClient`] is the transport: one connection, the application-level heartbeat,
//!   the idle bound, and reconnection on the plan's 1/2/4/8/16/30 second backoff with jitter.
//!
//! **Inbound path.** Every frame the venue sends enters the adapter at exactly one place,
//! [`OndoWsSession::handle_raw_frame`]. It takes the raw frame text and the local `ts_init` and
//! returns the outcomes in receive order. A raw market data recorder hooks in there: it sees the
//! frame before any decoding, so it records exactly what the venue sent without depending on the
//! parsed result.
//!
//! **Sends.** Requests are built from the shared [`SubscriptionState`], so a subscription taken
//! before the socket exists is replayed when it is established, and a topic that was removed is not
//! replayed. The transport only sends what the session asks for.
//!
//! **Recording.** A recorder ([`crate::recording`]), when one is configured, is hooked at those two
//! places and nowhere else: the inbound frame is offered to it before any decoding, and an outbound
//! request is offered to it after the venue accepted it. Both offers go through the recorder's
//! whitelist, which is the only thing that can produce a record, so a login message, a private
//! payload or an HTTP header cannot reach a file from here - and a transport with no recorder
//! configured touches no file at all.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::{Context, ensure};
use futures_util::StreamExt;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Data, InstrumentStatus},
    enums::MarketStatusAction,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    backoff::ExponentialBackoff,
    ratelimiter::quota::Quota,
    transport::{Message, TransportError},
    websocket::{
        MessageReader, SubscriptionState, TransportBackend, WebSocketClient, WebSocketConfig,
        subscription::split_topic,
    },
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    recording::RawMdSink,
    websocket::{
        book::{BookCounters, OndoBookState, SnapshotOutcome},
        messages::{PingRequest, SubscriptionRequest, WsChannel, WsMessageType, WsOp},
        parse::{
            ParsedBookSnapshot, ServerMessage, WsUpdate, decode_updates, parse_book_snapshot,
            parse_depth10, parse_funding_rate, parse_mark_price, parse_quote_tick,
            parse_server_message, parse_trade_tick,
        },
    },
};

/// The venue's documented cap on one client message (32 KB).
///
/// This bounds what the adapter *sends*; it is never used to truncate what the venue sends back.
pub const ONDO_WS_MAX_CLIENT_MESSAGE_BYTES: usize = 32 * 1024;

/// The bound on one inbound server frame this adapter will parse.
///
/// This bounds *parsing*, not the receive buffer: the transport reads a frame in full and hands it to
/// [`OndoWsSession::handle_raw_frame`], and this constant decides there whether the frame is parsed
/// at all. The venue does not shrink its frames to the client send cap, so the inbound bound is
/// different from [`ONDO_WS_MAX_CLIENT_MESSAGE_BYTES`] and much larger: a funding frame measured in
/// this phase is already ~2 KB and carries ten premium samples, and a book frame scales with the
/// requested level limit. A frame larger than this bound is reported as a protocol error with its
/// byte length and is never parsed partially, so server data is never silently truncated: what the
/// transport read is preserved verbatim in the outcome's raw frame.
pub const ONDO_WS_MAX_SERVER_MESSAGE_BYTES: usize = 1024 * 1024;

/// The documented client request rate: 25 requests per second.
pub const ONDO_WS_REQUEST_RATE_PER_SECOND: u32 = 25;

/// The documented client request burst: 50 requests.
pub const ONDO_WS_REQUEST_BURST: u32 = 50;

/// The venue's idle disconnect window: 180 seconds without a client request.
pub const ONDO_WS_IDLE_TIMEOUT_SECS: u64 = 180;

/// The first reconnect delay, in seconds.
pub const ONDO_WS_RECONNECT_INITIAL_SECS: u64 = 1;

/// The maximum reconnect delay, in seconds.
pub const ONDO_WS_RECONNECT_MAX_SECS: u64 = 30;

/// The reconnect backoff factor: 1, 2, 4, 8, 16, 30 seconds.
pub const ONDO_WS_RECONNECT_FACTOR: f64 = 2.0;

/// The bounded jitter added to each reconnect delay.
pub const ONDO_WS_RECONNECT_JITTER_MS: u64 = 250;

/// The local feed state reason published when the adapter loses its market data connection.
///
/// This is a local feed state: it says the adapter has no usable data, never that the venue halted
/// a market.
pub const REASON_DISCONNECTED: &str = "adapter:disconnected";

/// The local feed state reason published when a new snapshot has made the book usable again.
pub const REASON_SNAPSHOT_READY: &str = "adapter:snapshot_ready";

/// Returns the client request quota this adapter configures: 25 per second with a burst of 50.
#[must_use]
pub fn request_quota() -> Quota {
    let per_second = NonZeroU32::new(ONDO_WS_REQUEST_RATE_PER_SECOND).expect("non-zero rate");
    let burst = NonZeroU32::new(ONDO_WS_REQUEST_BURST).expect("non-zero burst");

    Quota::per_second(per_second)
        .expect("a 25/s quota has a representable replenish interval")
        .allow_burst(burst)
}

/// Returns the reconnect backoff used for the public feed.
///
/// The sequence is 1, 2, 4, 8, 16 and 30 seconds; the delay is capped at 30 s and each delay carries
/// bounded jitter so a venue-wide reconnect does not synchronise every client.
///
/// # Errors
///
/// Returns an error if the backoff parameters are rejected, which cannot happen for the constants
/// in this module.
pub fn reconnect_backoff() -> anyhow::Result<ExponentialBackoff> {
    ExponentialBackoff::new(
        Duration::from_secs(ONDO_WS_RECONNECT_INITIAL_SECS),
        Duration::from_secs(ONDO_WS_RECONNECT_MAX_SECS),
        ONDO_WS_RECONNECT_FACTOR,
        ONDO_WS_RECONNECT_JITTER_MS,
        false,
    )
}

/// A command the transport must deliver to the venue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsCommand {
    /// Send one request body (a subscription request or the application-level heartbeat).
    Send(String),
    /// Close the connection and end the transport task.
    Shutdown,
}

/// A change the data client asks the transport's protocol session to apply.
///
/// This is the **registration seam**. The session is owned by the transport task, so without it the
/// data client could not register the instruments it loaded: every frame would be routed to
/// `unknown_markets` and [`OndoWsSession::invalidate_all`] would publish nothing on a real
/// disconnect. A command is applied to the session in the order it was queued, on the same channel
/// for every connection, so a registration always reaches the session before the subscription that
/// needs it.
///
/// Applying a command is not the same as sending a request. A subscribe applied while no socket
/// exists only records intent; the request body is built and sent by
/// [`OndoWsSession::replay_requests`] when the next connection is established, while a subscribe
/// applied on a live connection is sent immediately. Either way the venue is asked once.
#[derive(Debug)]
pub enum WsSessionCommand {
    /// Registers a loaded instrument so frames for its market can be routed.
    RegisterInstrument(Box<InstrumentAny>),
    /// Subscribes to a channel for a set of instruments.
    Subscribe {
        /// The channel to subscribe to.
        channel: WsChannel,
        /// The instruments to subscribe for.
        instruments: Vec<InstrumentId>,
    },
    /// Subscribes to an on-demand depth10 projected from the same book subscription.
    SubscribeDepth10(InstrumentId),
    /// Unsubscribes from a channel for a set of instruments.
    Unsubscribe {
        /// The channel to unsubscribe from.
        channel: WsChannel,
        /// The instruments to unsubscribe for.
        instruments: Vec<InstrumentId>,
    },
    /// Unsubscribes from an on-demand depth10.
    UnsubscribeDepth10(InstrumentId),
}

/// What one inbound frame produced, in receive order.
#[derive(Clone, Debug)]
pub enum WsOutcome {
    /// A Nautilus data value to publish to the data engine.
    Data(Data),
    /// A frame the adapter recognises but does not act on, named so it is not silently ignored.
    Ignored {
        /// What was recognised.
        detail: String,
    },
    /// A frame the adapter does not handle: an unknown message type, a channel it does not
    /// implement, or an instrument it does not track. The raw frame is kept as evidence.
    Unsupported {
        /// Why the frame was not handled.
        reason: String,
        /// The raw frame, verbatim.
        raw_frame: String,
    },
    /// A permanent protocol problem: a venue error frame, or a frame that violates the documented
    /// schema. The raw frame is kept as evidence.
    ProtocolError {
        /// Why the frame was rejected.
        reason: String,
        /// The raw frame, verbatim.
        raw_frame: String,
    },
}

/// Per-session counters, for diagnostics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionCounters {
    /// Frames that reached [`OndoWsSession::handle_raw_frame`].
    pub frames_received: u64,
    /// Frames larger than the inbound bound, rejected without being parsed.
    pub oversized_frames: u64,
    /// Heartbeat responses.
    pub pongs: u64,
    /// Subscription acknowledgements.
    pub subscribed_acks: u64,
    /// Unsubscription acknowledgements.
    pub unsubscribed_acks: u64,
    /// Frames whose message type is not one this adapter knows.
    pub unknown_message_types: u64,
    /// Permanent protocol problems: venue errors and schema violations.
    pub protocol_errors: u64,
    /// Items routed to a market this adapter does not track.
    pub unknown_markets: u64,
    /// Decoded items that produced no data value (a one-sided top of book).
    pub skipped_items: u64,
    /// `adapter:snapshot_ready` feed states published.
    pub snapshot_ready_published: u64,
    /// `adapter:disconnected` feed states published.
    pub disconnected_published: u64,
    /// Funding frames mapped to an update.
    pub funding_frames: u64,
}

/// The protocol state machine for one connection to the public feed.
///
/// One session serves one connection: [`Self::invalidate_all`] ends it and moves every book to the
/// next session id, so a frame that arrives late from the previous connection cannot mutate the new
/// connection's books.
#[derive(Debug)]
pub struct OndoWsSession {
    book_limit: u32,
    depth_levels: Option<String>,
    session_id: u64,
    instruments: AHashMap<InstrumentId, InstrumentAny>,
    markets: AHashMap<String, InstrumentId>,
    books: AHashMap<InstrumentId, OndoBookState>,
    depth10: AHashSet<InstrumentId>,
    subscriptions: SubscriptionState,
    counters: SessionCounters,
}

impl OndoWsSession {
    /// Creates a session.
    ///
    /// `depth_levels` is the confirmed price grouping for the book channel, when a caller has one.
    /// It is [`None`] by default: the parameter is a price grouping, not a level count, and this
    /// adapter never invents a value for markets it has not confirmed one for.
    #[must_use]
    pub fn new(book_limit: u32, depth_levels: Option<String>) -> Self {
        Self {
            book_limit,
            depth_levels,
            session_id: 1,
            instruments: AHashMap::new(),
            markets: AHashMap::new(),
            books: AHashMap::new(),
            depth10: AHashSet::new(),
            subscriptions: SubscriptionState::new(':'),
            counters: SessionCounters::default(),
        }
    }

    /// Registers an instrument so frames for its market can be routed.
    ///
    /// The routing key is the instrument's `raw_symbol`, which is the venue's own market string: a
    /// frame never routes through the Nautilus product marker.
    pub fn register_instrument(&mut self, instrument: &InstrumentAny) {
        let market = instrument.raw_symbol().to_string();

        self.markets.insert(market, instrument.id());
        self.instruments.insert(instrument.id(), instrument.clone());
        self.books
            .entry(instrument.id())
            .or_insert_with(|| OndoBookState::new(self.session_id));
    }

    /// Returns the number of registered instruments.
    #[must_use]
    pub fn instrument_count(&self) -> usize {
        self.instruments.len()
    }

    /// Returns the local session id.
    ///
    /// This identifies a connection, is never sent to the venue, and is never presented as an
    /// exchange sequence.
    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Returns the session counters.
    #[must_use]
    pub const fn counters(&self) -> SessionCounters {
        self.counters
    }

    /// Returns the shared subscription intent and confirmation state.
    #[must_use]
    pub const fn subscriptions(&self) -> &SubscriptionState {
        &self.subscriptions
    }

    /// Returns the book state of an instrument, when one is registered.
    #[must_use]
    pub fn book_state(&self, instrument_id: &InstrumentId) -> Option<&OndoBookState> {
        self.books.get(instrument_id)
    }

    /// Returns the book counters of an instrument, when one is registered.
    #[must_use]
    pub fn book_counters(&self, instrument_id: &InstrumentId) -> Option<BookCounters> {
        self.books.get(instrument_id).map(OndoBookState::counters)
    }

    /// Returns the topics the adapter expects to be active, whether or not they are confirmed.
    #[must_use]
    pub fn expected_topics(&self) -> Vec<String> {
        self.subscriptions.all_topics()
    }

    /// Returns the topics the venue has acknowledged.
    ///
    /// A confirmation is proof that the venue accepted the subscription, never proof that market
    /// data is flowing; a caller gates on the book's own validity, not on this.
    #[must_use]
    pub fn confirmed_topics(&self) -> Vec<String> {
        topic_strings(&self.subscriptions.confirmed())
    }

    /// Returns the topics awaiting confirmation.
    #[must_use]
    pub fn pending_topics(&self) -> Vec<String> {
        self.subscriptions.pending_subscribe_topics()
    }

    /// Returns the local subscriber count for one channel and instrument.
    #[must_use]
    pub fn reference_count(&self, channel: WsChannel, instrument_id: &InstrumentId) -> usize {
        self.topic_of(channel, instrument_id)
            .map_or(0, |topic| self.subscriptions.get_reference_count(&topic))
    }

    /// Subscribes to a channel for a set of instruments.
    ///
    /// Returns the request body to send when the venue has not already been asked for every one of
    /// those topics, and [`None`] when nothing new has to be sent. Only the first local subscriber
    /// of a topic causes a request, so several local subscribers of the book never produce several
    /// subscriptions. A market the adapter did not load is an error rather than a guessed market
    /// string.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument is not registered, or if the request cannot be serialized.
    pub fn subscribe(
        &mut self,
        channel: WsChannel,
        instruments: &[InstrumentId],
    ) -> anyhow::Result<Option<String>> {
        let mut fresh = Vec::new();

        for instrument_id in instruments {
            let topic = self.topic_of(channel, instrument_id).with_context(|| {
                format!("cannot subscribe to `{instrument_id}`: it is not a loaded Ondo market")
            })?;

            self.subscriptions.add_reference(&topic);
            if self.subscriptions.try_mark_subscribe(&topic) {
                fresh.push(self.market_of(instrument_id)?);
            }
        }

        if fresh.is_empty() {
            return Ok(None);
        }

        self.subscribe_request(channel, fresh).map(Some)
    }

    /// Subscribes to an on-demand depth10 for an instrument.
    ///
    /// The depth10 is projected from the *same* book subscription: it takes a second reference on
    /// the book topic and does not open a connection of its own, so it can never unsubscribe a
    /// subscription another consumer is using.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not registered, or if the request cannot be serialized.
    pub fn subscribe_depth10(
        &mut self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<String>> {
        self.depth10.insert(*instrument_id);

        self.subscribe(WsChannel::DepthBooksPerps, &[*instrument_id])
    }

    /// Unsubscribes from a channel for a set of instruments.
    ///
    /// Returns the request body to send only when the last local subscriber of a topic goes away.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument is not registered, or if the request cannot be serialized.
    pub fn unsubscribe(
        &mut self,
        channel: WsChannel,
        instruments: &[InstrumentId],
    ) -> anyhow::Result<Option<String>> {
        let mut removed = Vec::new();

        for instrument_id in instruments {
            let topic = self.topic_of(channel, instrument_id).with_context(|| {
                format!("cannot unsubscribe from `{instrument_id}`: it is not a loaded Ondo market")
            })?;

            if self.subscriptions.remove_reference(&topic) {
                self.subscriptions.mark_unsubscribe(&topic);
                removed.push(self.market_of(instrument_id)?);
            }
        }

        if removed.is_empty() {
            return Ok(None);
        }

        SubscriptionRequest::new(WsOp::Unsubscribe, channel, removed, None, None, None)
            .to_json_text()
            .map(Some)
    }

    /// Unsubscribes from an on-demand depth10 for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not registered, or if the request cannot be serialized.
    pub fn unsubscribe_depth10(
        &mut self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<String>> {
        self.depth10.remove(instrument_id);

        self.unsubscribe(WsChannel::DepthBooksPerps, &[*instrument_id])
    }

    /// Returns the request bodies that replay the adapter's intent on a new connection.
    ///
    /// A topic that was unsubscribed is not replayed: the venue is never asked for something the
    /// adapter no longer wants.
    pub fn replay_requests(&mut self) -> Vec<String> {
        let mut by_channel: AHashMap<WsChannel, Vec<String>> = AHashMap::new();

        for topic in self.subscriptions.reset_after_reconnect() {
            let (channel_name, Some(market)) = split_topic(&topic, self.subscriptions.delimiter())
            else {
                continue;
            };
            let Some(channel) = WsChannel::from_wire(channel_name) else {
                continue;
            };
            if !self.markets.contains_key(market) {
                continue;
            }

            by_channel
                .entry(channel)
                .or_default()
                .push(market.to_string());
        }

        let mut requests = Vec::with_capacity(by_channel.len());
        for channel in WsChannel::ALL {
            let Some(markets) = by_channel.remove(&channel) else {
                continue;
            };

            if let Ok(text) = self.subscribe_request(channel, markets) {
                requests.push(text);
            }
        }

        requests
    }

    /// Returns the application-level heartbeat body.
    #[must_use]
    pub fn heartbeat_request(&self) -> String {
        PingRequest { op: WsOp::Ping }
            .to_json_text()
            .unwrap_or_else(|_| "{\"op\":\"ping\"}".to_string())
    }

    /// Handles one raw inbound frame and returns its outcomes in receive order.
    ///
    /// This is the single inbound path of the adapter: every frame enters here, before any decoding,
    /// so a raw recorder can be attached at exactly one place.
    pub fn handle_raw_frame(&mut self, raw: &str, ts_init: UnixNanos) -> Vec<WsOutcome> {
        self.counters.frames_received += 1;

        if raw.len() > ONDO_WS_MAX_SERVER_MESSAGE_BYTES {
            self.counters.oversized_frames += 1;

            return vec![WsOutcome::ProtocolError {
                reason: format!(
                    "frame of {} bytes exceeds the {} byte inbound bound",
                    raw.len(),
                    ONDO_WS_MAX_SERVER_MESSAGE_BYTES
                ),
                raw_frame: raw.to_string(),
            }];
        }

        let message = match parse_server_message(raw) {
            Ok(message) => message,
            Err(e) => {
                self.counters.protocol_errors += 1;

                return vec![WsOutcome::ProtocolError {
                    reason: e.to_string(),
                    raw_frame: raw.to_string(),
                }];
            }
        };

        match message.kind {
            WsMessageType::Pong => {
                self.counters.pongs += 1;

                Vec::new()
            }
            WsMessageType::Subscribed => {
                self.counters.subscribed_acks += 1;

                self.confirm_acknowledgement(SubscriptionAck::Subscribed, &message, raw)
            }
            WsMessageType::Unsubscribed => {
                self.counters.unsubscribed_acks += 1;

                self.confirm_acknowledgement(SubscriptionAck::Unsubscribed, &message, raw)
            }
            WsMessageType::Update => self.handle_update(&message, raw, ts_init),
            WsMessageType::Error => {
                self.counters.protocol_errors += 1;
                let reason = message
                    .message
                    .clone()
                    .unwrap_or_else(|| "the venue reported an error without a message".to_string());
                let reason = match &message.code {
                    Some(code) => format!("{reason} (code {code})"),
                    None => reason,
                };

                vec![WsOutcome::ProtocolError {
                    reason,
                    raw_frame: raw.to_string(),
                }]
            }
            WsMessageType::LoggedIn => vec![WsOutcome::Unsupported {
                reason: "the public data client never logs in".to_string(),
                raw_frame: raw.to_string(),
            }],
            WsMessageType::Unknown => {
                self.counters.unknown_message_types += 1;

                vec![WsOutcome::Unsupported {
                    reason: format!("unknown message type `{}`", message.kind_raw),
                    raw_frame: raw.to_string(),
                }]
            }
        }
    }

    /// Ends the session and invalidates every book.
    ///
    /// Returns one local feed state per instrument the adapter has a subscription for, so a
    /// disconnect reaches the application as an event rather than as a flag it has to poll. A book
    /// becomes usable again only when a snapshot is accepted on the new session.
    pub fn invalidate_all(&mut self, reason: &str, ts_init: UnixNanos) -> Vec<WsOutcome> {
        self.session_id += 1;
        let session_id = self.session_id;

        let mut instruments: Vec<InstrumentId> = Vec::new();
        for topic in self.subscriptions.all_topics() {
            let (_channel, Some(market)) = split_topic(&topic, self.subscriptions.delimiter())
            else {
                continue;
            };
            if let Some(instrument_id) = self.markets.get(market) {
                instruments.push(*instrument_id);
            }
        }
        instruments.sort();
        instruments.dedup();

        for instrument_id in &instruments {
            if let Some(state) = self.books.get_mut(instrument_id) {
                state.invalidate_to(session_id, reason);
            }
        }

        self.counters.disconnected_published += instruments.len() as u64;

        instruments
            .into_iter()
            .map(|instrument_id| {
                WsOutcome::Data(Data::InstrumentStatus(feed_status(
                    instrument_id,
                    reason,
                    false,
                    ts_init,
                )))
            })
            .collect()
    }

    fn subscribe_request(
        &self,
        channel: WsChannel,
        markets: Vec<String>,
    ) -> anyhow::Result<String> {
        SubscriptionRequest::new(
            WsOp::Subscribe,
            channel,
            markets,
            Some(self.book_limit),
            Some(0),
            self.depth_levels.clone(),
        )
        .to_json_text()
    }

    fn handle_update(
        &mut self,
        message: &ServerMessage,
        raw: &str,
        ts_init: UnixNanos,
    ) -> Vec<WsOutcome> {
        let Some(channel_name) = message.channel.as_deref() else {
            self.counters.protocol_errors += 1;

            return vec![WsOutcome::ProtocolError {
                reason: "an update frame carries no `channel`".to_string(),
                raw_frame: raw.to_string(),
            }];
        };

        let Some(channel) = WsChannel::from_wire(channel_name) else {
            return vec![WsOutcome::Unsupported {
                reason: format!("channel `{channel_name}` is not implemented by this adapter"),
                raw_frame: raw.to_string(),
            }];
        };

        let Some(data) = message.data.as_ref() else {
            self.counters.protocol_errors += 1;

            return vec![WsOutcome::ProtocolError {
                reason: format!("an update frame for `{channel_name}` carries no `data`"),
                raw_frame: raw.to_string(),
            }];
        };

        let updates = match decode_updates(channel, data) {
            Ok(updates) => updates,
            Err(e) => {
                self.counters.protocol_errors += 1;

                return vec![WsOutcome::ProtocolError {
                    reason: e.to_string(),
                    raw_frame: raw.to_string(),
                }];
            }
        };

        let mut outcomes = Vec::new();
        for update in updates {
            let market = update.market().to_string();
            let Some(instrument_id) = self.markets.get(&market).copied() else {
                self.counters.unknown_markets += 1;
                // `mark_failure` returns this topic to pending only when it is tracked, and an
                // unloaded market's topic never is: a topic enters the subscription state through
                // `subscribe`, which refuses an instrument this session has not registered. So the
                // call below is a no-op for an unknown market and nothing about a loaded market's
                // subscription is degraded; the raw frame is preserved in the outcome instead of
                // being dropped, which is the only evidence of what the venue sent.
                self.subscriptions
                    .mark_failure(&format!("{}:{market}", channel.as_str()));

                outcomes.push(WsOutcome::Unsupported {
                    reason: format!("market `{market}` is not loaded by this adapter"),
                    raw_frame: raw.to_string(),
                });
                continue;
            };

            let Some(instrument) = self.instruments.get(&instrument_id).cloned() else {
                continue;
            };

            let routed = self.route_update(
                channel,
                &instrument,
                &update,
                message.timestamp,
                ts_init,
                raw,
            );
            outcomes.extend(routed);
        }

        outcomes
    }

    fn route_update(
        &mut self,
        channel: WsChannel,
        instrument: &InstrumentAny,
        update: &WsUpdate,
        envelope_ts: Option<UnixNanos>,
        ts_init: UnixNanos,
        raw: &str,
    ) -> Vec<WsOutcome> {
        match update {
            WsUpdate::Book(item) => {
                let Some(time) = item.time.as_deref() else {
                    self.counters.protocol_errors += 1;

                    return vec![WsOutcome::ProtocolError {
                        reason: format!(
                            "book snapshot for market `{}` carries no `time`, which is the price event time",
                            item.market
                        ),
                        raw_frame: raw.to_string(),
                    }];
                };
                let ts_event = match crate::common::parse::parse_timestamp(time) {
                    Ok(ts_event) => ts_event,
                    Err(e) => {
                        self.counters.protocol_errors += 1;

                        return vec![WsOutcome::ProtocolError {
                            reason: e.to_string(),
                            raw_frame: raw.to_string(),
                        }];
                    }
                };

                let parsed = match parse_book_snapshot(item, instrument, ts_event, ts_init) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        self.counters.protocol_errors += 1;

                        return vec![WsOutcome::ProtocolError {
                            reason: e.to_string(),
                            raw_frame: raw.to_string(),
                        }];
                    }
                };

                if channel == WsChannel::TopOfBooksPerps {
                    return self.publish_top_of_book(instrument, &parsed, ts_event, ts_init, raw);
                }

                self.apply_book_snapshot(instrument, parsed, ts_event, ts_init, raw)
            }
            WsUpdate::Trade(item) => match parse_trade_tick(item, instrument, ts_init) {
                Ok(tick) => vec![WsOutcome::Data(Data::Trade(tick))],
                Err(e) => self.protocol_error(e, raw),
            },
            WsUpdate::FundingRate(item) => {
                match parse_funding_rate(item, instrument, envelope_ts, ts_init) {
                    Ok(parsed) => {
                        self.counters.funding_frames += 1;

                        vec![WsOutcome::Data(Data::FundingRate(parsed.update))]
                    }
                    Err(e) => self.protocol_error(e, raw),
                }
            }
            WsUpdate::MarkPrice(item) => {
                match parse_mark_price(item, instrument, envelope_ts, ts_init) {
                    Ok(parsed) => vec![WsOutcome::Data(Data::MarkPrice(parsed.update))],
                    Err(e) => self.protocol_error(e, raw),
                }
            }
        }
    }

    fn protocol_error(&mut self, error: anyhow::Error, raw: &str) -> Vec<WsOutcome> {
        self.counters.protocol_errors += 1;

        vec![WsOutcome::ProtocolError {
            reason: error.to_string(),
            raw_frame: raw.to_string(),
        }]
    }

    fn publish_top_of_book(
        &mut self,
        instrument: &InstrumentAny,
        parsed: &ParsedBookSnapshot,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        raw: &str,
    ) -> Vec<WsOutcome> {
        match parse_quote_tick(&parsed.wire, instrument, ts_event, ts_init) {
            Ok(Some(quote)) => vec![WsOutcome::Data(Data::Quote(quote))],
            Ok(None) => {
                self.counters.skipped_items += 1;

                vec![WsOutcome::Ignored {
                    detail: format!(
                        "one-sided top of book for `{}` cannot be published as a quote",
                        instrument.id()
                    ),
                }]
            }
            Err(e) => self.protocol_error(e, raw),
        }
    }

    fn apply_book_snapshot(
        &mut self,
        instrument: &InstrumentAny,
        parsed: ParsedBookSnapshot,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        raw: &str,
    ) -> Vec<WsOutcome> {
        let instrument_id = instrument.id();
        let session_id = self.session_id;

        if !self.books.contains_key(&instrument_id) {
            self.books
                .insert(instrument_id, OndoBookState::new(session_id));
        }

        let Some(state) = self.books.get_mut(&instrument_id) else {
            return Vec::new();
        };
        state.set_coverage_limit(Some(self.book_limit));

        let was_valid = state.is_valid();
        let outcome = match state.apply_snapshot(session_id, ts_event, &parsed) {
            Ok(outcome) => outcome,
            Err(e) => return self.protocol_error(e, raw),
        };

        match outcome {
            SnapshotOutcome::Accepted { .. } => {
                let is_valid = state.is_valid();
                let wire = state.wire().cloned();

                let mut outcomes = vec![WsOutcome::Data(Data::Deltas(Box::new(parsed.deltas)))];

                // An on-demand depth10 is a projection of this same book subscription and cache,
                // never a second connection.
                if is_valid && self.depth10.contains(&instrument_id) {
                    if let Some(wire) = wire {
                        match parse_depth10(&wire, instrument, ts_event, ts_init) {
                            Ok(depth) => {
                                outcomes.push(WsOutcome::Data(Data::Depth10(Box::new(depth))));
                            }
                            Err(e) => outcomes.extend(self.protocol_error(e, raw)),
                        }
                    }
                }

                if is_valid && !was_valid {
                    self.counters.snapshot_ready_published += 1;
                    outcomes.push(WsOutcome::Data(Data::InstrumentStatus(feed_status(
                        instrument_id,
                        REASON_SNAPSHOT_READY,
                        true,
                        ts_init,
                    ))));
                }

                outcomes
            }
            SnapshotOutcome::DuplicateSuppressed | SnapshotOutcome::StaleRejected { .. } => {
                Vec::new()
            }
            SnapshotOutcome::ForeignSession { expected, received } => {
                vec![WsOutcome::ProtocolError {
                    reason: format!(
                        "book frame for `{instrument_id}` belongs to session {received}, not {expected}"
                    ),
                    raw_frame: raw.to_string(),
                }]
            }
        }
    }

    fn confirm_acknowledgement(
        &mut self,
        ack: SubscriptionAck,
        message: &ServerMessage,
        raw: &str,
    ) -> Vec<WsOutcome> {
        let Some(data) = message.data.as_ref() else {
            // The observed acknowledgement echoes the request inside `data` (conflicts.md conflict
            // 8). Without it the acknowledgement cannot be attributed to a topic, which is reported
            // rather than guessed.
            return vec![WsOutcome::Unsupported {
                reason: format!(
                    "`{}` acknowledgement carries no `data` to attribute it to topics",
                    ack.as_str()
                ),
                raw_frame: raw.to_string(),
            }];
        };

        let mut topics = Vec::new();
        collect_ack_topics(data, &mut topics);

        if topics.is_empty() {
            return vec![WsOutcome::Unsupported {
                reason: format!(
                    "`{}` acknowledgement names no channel and markets",
                    ack.as_str()
                ),
                raw_frame: raw.to_string(),
            }];
        }

        for topic in &topics {
            match ack {
                SubscriptionAck::Subscribed => self.subscriptions.confirm_subscribe(topic),
                SubscriptionAck::Unsubscribed => self.subscriptions.confirm_unsubscribe(topic),
            }
        }

        Vec::new()
    }

    fn topic_of(&self, channel: WsChannel, instrument_id: &InstrumentId) -> Option<String> {
        self.market_of(instrument_id)
            .ok()
            .map(|market| format!("{}:{market}", channel.as_str()))
    }

    fn market_of(&self, instrument_id: &InstrumentId) -> anyhow::Result<String> {
        let instrument = self.instruments.get(instrument_id).with_context(|| {
            format!("instrument `{instrument_id}` is not registered with the Ondo session")
        })?;

        let market = instrument.raw_symbol().to_string();
        ensure!(
            self.markets.contains_key(&market),
            "instrument `{instrument_id}` has no venue market mapping"
        );

        Ok(market)
    }
}

/// Flattens a confirmed-subscription snapshot into sorted `channel:symbol` topics.
fn topic_strings(snapshot: &nautilus_network::websocket::SubscriptionSnapshot) -> Vec<String> {
    let mut topics: Vec<String> = snapshot
        .iter()
        .flat_map(|(channel, symbols)| {
            symbols
                .iter()
                .map(move |symbol| format!("{channel}:{symbol}"))
        })
        .collect();
    topics.sort();

    topics
}

/// A local feed state for an instrument.
///
/// `action` is [`MarketStatusAction::None`] because this is not an exchange status change: the venue
/// has not said anything about the market. `reason` names the adapter condition, and the two reasons
/// this adapter publishes are [`REASON_DISCONNECTED`] and [`REASON_SNAPSHOT_READY`]. `is_trading` is
/// left unset because the adapter is in no position to describe venue trading, and `is_quoting`
/// states only whether this adapter's own feed can quote.
fn feed_status(
    instrument_id: InstrumentId,
    reason: &str,
    is_quoting: bool,
    ts_init: UnixNanos,
) -> InstrumentStatus {
    InstrumentStatus::new(
        instrument_id,
        MarketStatusAction::None,
        ts_init,
        ts_init,
        Some(Ustr::from(reason)),
        None,
        None,
        Some(is_quoting),
        None,
    )
}

/// Which acknowledgement an acknowledgement frame is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionAck {
    Subscribed,
    Unsubscribed,
}

impl SubscriptionAck {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Subscribed => "subscribed",
            Self::Unsubscribed => "unsubscribed",
        }
    }
}

/// Collects the `channel:symbol` topics an acknowledgement frame names.
///
/// The payload may be the echoed request object or an array of them; only `channel` and `markets`
/// are read, so an acknowledgement can never be mistaken for a book or market update.
fn collect_ack_topics(data: &serde_json::Value, topics: &mut Vec<String>) {
    match data {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_ack_topics(item, topics);
            }
        }
        serde_json::Value::Object(map) => {
            let Some(channel) = map.get("channel").and_then(serde_json::Value::as_str) else {
                return;
            };

            if let Some(markets) = map.get("markets").and_then(serde_json::Value::as_array) {
                for market in markets.iter().filter_map(serde_json::Value::as_str) {
                    topics.push(format!("{channel}:{market}"));
                }
            }
        }
        _ => {}
    }
}

/// The transport for the Ondo Perps public market data feed.
///
/// The client owns one connection at a time and reconnects on its own. Subscription intent lives in
/// the shared [`SubscriptionState`], so a subscribe taken before the socket exists is replayed when
/// it is established, and a topic that was removed is not replayed.
#[derive(Debug)]
pub struct OndoWebSocketClient {
    url: String,
    subscriptions: SubscriptionState,
    cmd_tx: mpsc::UnboundedSender<WsCommand>,
    session_tx: mpsc::UnboundedSender<WsSessionCommand>,
    is_connected: Arc<AtomicBool>,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl OndoWebSocketClient {
    /// Creates a transport and the receiver its outcomes are published on.
    ///
    /// The transport task is spawned immediately; it attempts its first connection without the
    /// caller having to do anything, and it keeps the socket's lifecycle out of the data client.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no Tokio runtime to host the transport task, or if the
    /// reconnect backoff cannot be constructed.
    pub fn new(
        url: String,
        heartbeat_secs: u64,
        book_limit: u32,
        depth_levels: Option<String>,
    ) -> anyhow::Result<(Self, mpsc::UnboundedReceiver<WsOutcome>)> {
        Self::new_with_recorder(url, heartbeat_secs, book_limit, depth_levels, None)
    }

    /// Creates a transport that also offers its public frames to `recorder`.
    ///
    /// The recorder sees the frames the venue sends, exactly as they arrived and before any decoding,
    /// and the public request bodies this transport manages to send. [`Self::new`] is this
    /// constructor without a recorder: a transport with no recorder configured never touches a file.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no Tokio runtime to host the transport task, or if the
    /// reconnect backoff cannot be constructed.
    pub fn new_with_recorder(
        url: String,
        heartbeat_secs: u64,
        book_limit: u32,
        depth_levels: Option<String>,
        recorder: Option<RawMdSink>,
    ) -> anyhow::Result<(Self, mpsc::UnboundedReceiver<WsOutcome>)> {
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (session_tx, session_rx) = mpsc::unbounded_channel();
        let subscriptions = SubscriptionState::new(':');
        let is_connected = Arc::new(AtomicBool::new(false));
        let cancellation = CancellationToken::new();

        let state = WsTransportState {
            url: url.clone(),
            heartbeat_secs,
            book_limit,
            depth_levels: depth_levels.clone(),
            subscriptions: subscriptions.clone(),
            out_tx,
            is_connected: Arc::clone(&is_connected),
            cancellation: cancellation.clone(),
            recorder,
        };

        let runtime = tokio::runtime::Handle::try_current().context(
            "the Ondo WebSocket transport needs a Tokio runtime to host its connection task",
        )?;
        let task = runtime.spawn(state.run(cmd_rx, session_rx));

        let client = Self {
            url,
            subscriptions,
            cmd_tx,
            session_tx,
            is_connected,
            cancellation,
            task: Some(task),
        };

        Ok((client, out_rx))
    }

    /// Returns `true` while the socket is established.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    /// Returns the shared subscription intent and confirmation state.
    #[must_use]
    pub const fn subscriptions(&self) -> &SubscriptionState {
        &self.subscriptions
    }

    /// Returns the WebSocket URL this client connects to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Sends one request body on the active connection.
    ///
    /// The intent is recorded by the session first, so a request sent while no socket exists is not
    /// lost: it is replayed from the recorded intent on the next connection.
    pub fn send(&self, body: String) {
        if self.cmd_tx.send(WsCommand::Send(body)).is_err() {
            log::debug!("Ondo WebSocket transport is no longer accepting commands");
        }
    }

    /// Registers a loaded instrument with the transport's session.
    ///
    /// Until this is called, every frame for the instrument's market is routed to `unknown_markets`
    /// and the disconnect feed state names no instrument. Registration is recorded in the order it
    /// was requested, before any subscription queued after it.
    pub fn register_instrument(&self, instrument: &InstrumentAny) {
        self.apply_session_command(WsSessionCommand::RegisterInstrument(Box::new(
            instrument.clone(),
        )));
    }

    /// Subscribes to a channel for a set of instruments through the transport's session.
    ///
    /// Returns nothing: building the request and sending it belongs to the session, which knows
    /// whether the venue has already been asked for a topic and whether a socket exists. The caller
    /// checks that the instrument is loaded; an instrument the session does not know is reported in
    /// its own log rather than invented as a market string.
    pub fn subscribe(&self, channel: WsChannel, instruments: &[InstrumentId]) {
        self.apply_session_command(WsSessionCommand::Subscribe {
            channel,
            instruments: instruments.to_vec(),
        });
    }

    /// Subscribes to an on-demand depth10 through the transport's session.
    ///
    /// The projection rides the same book subscription and never opens a connection of its own.
    pub fn subscribe_depth10(&self, instrument_id: &InstrumentId) {
        self.apply_session_command(WsSessionCommand::SubscribeDepth10(*instrument_id));
    }

    /// Unsubscribes from a channel for a set of instruments through the transport's session.
    ///
    /// The venue is asked only when the last local subscriber of a topic goes away.
    pub fn unsubscribe(&self, channel: WsChannel, instruments: &[InstrumentId]) {
        self.apply_session_command(WsSessionCommand::Unsubscribe {
            channel,
            instruments: instruments.to_vec(),
        });
    }

    /// Unsubscribes from an on-demand depth10 through the transport's session.
    pub fn unsubscribe_depth10(&self, instrument_id: &InstrumentId) {
        self.apply_session_command(WsSessionCommand::UnsubscribeDepth10(*instrument_id));
    }

    fn apply_session_command(&self, command: WsSessionCommand) {
        if self.session_tx.send(command).is_err() {
            log::debug!("Ondo WebSocket transport is no longer accepting session commands");
        }
    }

    /// Stops the transport: cancels the heartbeat, the idle timer and the connection task, and lets
    /// the task close its socket.
    pub async fn stop(&mut self) {
        self.cancellation.cancel();
        let _ = self.cmd_tx.send(WsCommand::Shutdown);

        if let Some(task) = self.task.take() {
            // The task observes the cancellation and returns; the wait is bounded so a shutdown can
            // never hang on a wedged socket.
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }
}

impl Drop for OndoWebSocketClient {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _ = self.cmd_tx.send(WsCommand::Shutdown);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// The transport task state: one connection at a time, its session, and the reconnect loop.
#[derive(Debug)]
struct WsTransportState {
    url: String,
    heartbeat_secs: u64,
    book_limit: u32,
    depth_levels: Option<String>,
    subscriptions: SubscriptionState,
    out_tx: mpsc::UnboundedSender<WsOutcome>,
    is_connected: Arc<AtomicBool>,
    cancellation: CancellationToken,
    recorder: Option<RawMdSink>,
}

impl WsTransportState {
    async fn run(
        self,
        mut cmd_rx: mpsc::UnboundedReceiver<WsCommand>,
        mut session_rx: mpsc::UnboundedReceiver<WsSessionCommand>,
    ) {
        let mut backoff = match reconnect_backoff() {
            Ok(backoff) => backoff,
            Err(e) => {
                log::error!("Ondo WebSocket reconnect backoff is misconfigured: {e}");

                return;
            }
        };
        let mut session = OndoWsSession::new(self.book_limit, self.depth_levels.clone());
        // The session mutates the *shared* subscription state, so an intent recorded through
        // `OndoWebSocketClient::subscriptions()` before the socket existed is the same intent this
        // connection replays, and a confirmation seen here is visible to that caller.
        session.subscriptions = self.subscriptions.clone();

        loop {
            if self.cancellation.is_cancelled() {
                return;
            }

            // Apply whatever the data client queued while there was no connection, so a replay is
            // built from instruments that are already registered. Nothing is sent here: with no
            // socket, recording the intent is the whole effect, and the request body is built and
            // sent by the replay that follows a successful connection.
            drain_session_commands(&mut session, &mut session_rx);

            match self.connect_once().await {
                Ok((reader, client)) => {
                    backoff.reset();
                    self.is_connected.store(true, Ordering::Release);
                    log::debug!("Ondo WebSocket connected");

                    // Registration must land before the replay is built, because a replay drops a
                    // topic whose market is not registered yet. Still no send here: the replay below
                    // is what sends every desired topic exactly once.
                    drain_session_commands(&mut session, &mut session_rx);
                    for request in session.replay_requests() {
                        if let Err(e) = send_body(&client, &request, self.recorder.as_ref()).await {
                            log::warn!("Failed to replay an Ondo subscription: {e}");
                        }
                    }

                    self.serve_connection(
                        &mut session,
                        reader,
                        &client,
                        &mut cmd_rx,
                        &mut session_rx,
                    )
                    .await;

                    self.is_connected.store(false, Ordering::Release);
                    let now = get_atomic_clock_realtime().get_time_ns();
                    for outcome in session.invalidate_all(REASON_DISCONNECTED, now) {
                        if self.out_tx.send(outcome).is_err() {
                            return;
                        }
                    }
                    client.disconnect().await;

                    if self.cancellation.is_cancelled() {
                        return;
                    }
                }
                Err(e) => {
                    log::warn!("Ondo WebSocket connection attempt failed: {e}");
                }
            }

            let delay = backoff.next_duration();
            log::debug!("Ondo WebSocket reconnecting in {delay:?}");
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn serve_connection(
        &self,
        session: &mut OndoWsSession,
        mut reader: MessageReader,
        client: &WebSocketClient,
        cmd_rx: &mut mpsc::UnboundedReceiver<WsCommand>,
        session_rx: &mut mpsc::UnboundedReceiver<WsSessionCommand>,
    ) {
        let heartbeat = Duration::from_secs(self.heartbeat_secs.max(1));
        let idle = Duration::from_secs(ONDO_WS_IDLE_TIMEOUT_SECS);
        let mut heartbeat_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
        let mut last_frame = tokio::time::Instant::now();

        loop {
            let idle_deadline = last_frame + idle;

            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep_until(idle_deadline) => {
                    log::warn!(
                        "Ondo WebSocket received no frame for {ONDO_WS_IDLE_TIMEOUT_SECS}s; reconnecting",
                    );
                    return;
                }
                _ = heartbeat_tick.tick() => {
                    // The application-level heartbeat is required: the venue idles a connection out
                    // after 180 s without a client request, and a protocol-level ping is not one.
                    if let Err(e) = send_body(client, &session.heartbeat_request(), self.recorder.as_ref()).await {
                        log::warn!("Failed to send the Ondo WebSocket heartbeat: {e}");
                        return;
                    }
                }
                command = cmd_rx.recv() => {
                    match command {
                        Some(WsCommand::Send(body)) => {
                            if let Err(e) = send_body(client, &body, self.recorder.as_ref()).await {
                                log::warn!("Failed to send an Ondo WebSocket request: {e}");
                                return;
                            }
                        }
                        Some(WsCommand::Shutdown) | None => return,
                    }
                }
                session_command = session_rx.recv() => {
                    match session_command {
                        Some(session_command) => {
                            apply_session_command(
                                session,
                                session_command,
                                Some(client),
                                self.recorder.as_ref(),
                            )
                            .await;
                        }
                        None => return,
                    }
                }
                message = reader.next() => {
                    match message {
                        Some(Ok(Message::Text(bytes))) => {
                            last_frame = tokio::time::Instant::now();
                            let Ok(text) = std::str::from_utf8(&bytes) else {
                                // The transport contract says text is UTF-8; a violation is reported
                                // with its byte length rather than lossily converted.
                                log::error!(
                                    "Ondo WebSocket sent a text frame with {} bytes of invalid UTF-8",
                                    bytes.len(),
                                );
                                continue;
                            };
                            let ts_init = get_atomic_clock_realtime().get_time_ns();
                            // The recorder is offered the frame here, before any decoding: it sees
                            // exactly what the venue sent, and its whitelist decides whether that is
                            // part of the public stream. The outcome belongs to the recorder - its
                            // counters carry it and it logs its own failure once - so the receive
                            // path never acts on a recording problem.
                            if let Some(recorder) = &self.recorder {
                                let _outcome = recorder.record_inbound(text, ts_init);
                            }
                            for outcome in session.handle_raw_frame(text, ts_init) {
                                if self.out_tx.send(outcome).is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            log::debug!("Ondo WebSocket closed by the venue: {frame:?}");
                            return;
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                            // Protocol-level control frames are the transport's own business.
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            last_frame = tokio::time::Instant::now();
                            log::warn!(
                                "Ondo WebSocket sent a binary frame of {} bytes, which this protocol does not use",
                                bytes.len(),
                            );
                        }
                        Some(Err(e)) => {
                            log::warn!("Ondo WebSocket read failed: {e}");
                            return;
                        }
                        None => {
                            log::debug!("Ondo WebSocket stream ended");
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn connect_once(&self) -> Result<(MessageReader, WebSocketClient), TransportError> {
        WebSocketClient::stream_builder()
            .config(WebSocketConfig {
                url: self.url.clone(),
                headers: Vec::new(),
                heartbeat_interval_secs: None,
                heartbeat_payload: None,
                connect_timeout_ms: None,
                reconnect_delay_initial_ms: None,
                reconnect_delay_max_ms: None,
                reconnect_backoff_factor: None,
                reconnect_jitter_ms: None,
                reconnect_max_attempts: None,
                heartbeat_timeout_secs: None,
                idle_timeout_ms: None,
                backend: TransportBackend::default(),
                proxy_url: None,
            })
            .default_quota(request_quota())
            .connect()
            .await
    }
}

/// Applies one session command to the protocol session.
///
/// When `client` is `Some`, the session is live and a request it produces is sent immediately. When
/// it is [`None`] the command only records intent, which the replay after the next successful
/// connection sends; that is what keeps a subscribe taken while disconnected from being lost and a
/// subscribe taken on a live connection from being sent twice.
async fn apply_session_command(
    session: &mut OndoWsSession,
    command: WsSessionCommand,
    client: Option<&WebSocketClient>,
    recorder: Option<&RawMdSink>,
) {
    let request = match command {
        WsSessionCommand::RegisterInstrument(instrument) => {
            session.register_instrument(&instrument);

            return;
        }
        WsSessionCommand::Subscribe {
            channel,
            instruments,
        } => session.subscribe(channel, &instruments),
        WsSessionCommand::SubscribeDepth10(instrument_id) => {
            session.subscribe_depth10(&instrument_id)
        }
        WsSessionCommand::Unsubscribe {
            channel,
            instruments,
        } => session.unsubscribe(channel, &instruments),
        WsSessionCommand::UnsubscribeDepth10(instrument_id) => {
            session.unsubscribe_depth10(&instrument_id)
        }
    };

    let body = match request {
        Ok(Some(body)) => body,
        Ok(None) => return,
        Err(e) => {
            log::warn!("Ondo session command was not applied: {e}");

            return;
        }
    };

    if let Some(client) = client
        && let Err(e) = send_body(client, &body, recorder).await
    {
        log::warn!("Failed to send an Ondo WebSocket request: {e}");
    }
}

/// Applies every session command that is already queued, in order, without blocking.
///
/// This is the no-socket form of [`apply_session_command`]: it only records intent, because the
/// request body of a subscribe is built and sent by the replay that follows a successful connection.
fn drain_session_commands(
    session: &mut OndoWsSession,
    session_rx: &mut mpsc::UnboundedReceiver<WsSessionCommand>,
) {
    while let Ok(command) = session_rx.try_recv() {
        let request = match command {
            WsSessionCommand::RegisterInstrument(instrument) => {
                session.register_instrument(&instrument);

                continue;
            }
            WsSessionCommand::Subscribe {
                channel,
                instruments,
            } => session.subscribe(channel, &instruments),
            WsSessionCommand::SubscribeDepth10(instrument_id) => {
                session.subscribe_depth10(&instrument_id)
            }
            WsSessionCommand::Unsubscribe {
                channel,
                instruments,
            } => session.unsubscribe(channel, &instruments),
            WsSessionCommand::UnsubscribeDepth10(instrument_id) => {
                session.unsubscribe_depth10(&instrument_id)
            }
        };

        if let Err(e) = request {
            log::warn!("Ondo session command was not applied: {e}");
        }
    }
}

/// Sends one request body, refusing to exceed the venue's client message cap.
///
/// A body the venue accepted is offered to `recorder`, which records it only if its whitelist
/// classifies it as one of this adapter's public operations. A body that was refused here, or whose
/// send failed, never went out, so it is not recorded: the file holds what the connection carried.
async fn send_body(
    client: &WebSocketClient,
    body: &str,
    recorder: Option<&RawMdSink>,
) -> Result<(), TransportError> {
    if body.len() > ONDO_WS_MAX_CLIENT_MESSAGE_BYTES {
        // A request larger than the cap would be rejected or truncated by the venue, so it is
        // refused locally with the size named instead.
        return Err(TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "request of {} bytes exceeds the {} byte client message cap",
                body.len(),
                ONDO_WS_MAX_CLIENT_MESSAGE_BYTES
            ),
        )));
    }

    client
        .send_text(body.to_string(), None)
        .await
        .map_err(|e| {
            log::debug!("Ondo WebSocket send failed: {e}");
            TransportError::Io(std::io::Error::other(e.to_string()))
        })?;

    if let Some(recorder) = recorder {
        let _outcome = recorder.record_outbound(body, get_atomic_clock_realtime().get_time_ns());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::AggressorSide, instruments::Instrument};
    use rstest::rstest;

    use super::*;
    use crate::{
        common::parse::{market_to_instrument_id, parse_timestamp},
        http::models::parse_instruments,
    };

    const DEPTH_FIXTURE: &str = include_str!("../../test_data/ws/depth_observed.json");
    const TOP_FIXTURE: &str = include_str!("../../test_data/ws/topofbook_observed.json");
    const TRADES_FIXTURE: &str = include_str!("../../test_data/ws/trades_observed.json");
    const FUNDING_FIXTURE: &str = include_str!("../../test_data/ws/funding_observed.json");
    const MARKPRICES_FIXTURE: &str = include_str!("../../test_data/ws/markprices_observed.json");
    const SUBSCRIBE_FIXTURE: &str = include_str!("../../test_data/ws/subscribe_observed.json");

    fn ts(value: &str) -> UnixNanos {
        parse_timestamp(value).unwrap()
    }

    fn instrument(market: &str) -> InstrumentAny {
        let body = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/rest/markets_synthetic.json"
        ))
        .unwrap();

        parse_instruments(
            &body,
            &[market_to_instrument_id(market).unwrap()],
            UnixNanos::from(1),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    }

    fn session_with_nvda() -> (OndoWsSession, InstrumentId) {
        let mut session = OndoWsSession::new(100, None);
        let nvda = instrument("NVDA-USD.P");
        let instrument_id = nvda.id();
        session.register_instrument(&nvda);

        (session, instrument_id)
    }

    fn subscribed_ack(request: &str) -> String {
        ack("subscribed", request)
    }

    fn unsubscribed_ack(request: &str) -> String {
        ack("unsubscribed", request)
    }

    fn ack(kind: &str, request: &str) -> String {
        let value: serde_json::Value = serde_json::from_str(request).unwrap();

        serde_json::json!({
            "type": kind,
            "channel": value["channel"],
            "timestamp": "2026-09-14T11:09:59.670112401Z",
            "data": value,
        })
        .to_string()
    }

    #[rstest]
    fn test_request_quota_is_25_per_second_with_a_burst_of_50() {
        let quota = request_quota();

        assert_eq!(quota.burst_size().get(), 50);
        assert_eq!(quota.replenish_interval(), Duration::from_millis(40));
    }

    #[rstest]
    fn test_reconnect_backoff_follows_one_two_four_eight_sixteen_thirty() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_secs(ONDO_WS_RECONNECT_INITIAL_SECS),
            Duration::from_secs(ONDO_WS_RECONNECT_MAX_SECS),
            ONDO_WS_RECONNECT_FACTOR,
            0,
            false,
        )
        .unwrap();

        let delays: Vec<u64> = (0..7).map(|_| backoff.next_duration().as_secs()).collect();

        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30]);
    }

    #[rstest]
    fn test_the_observed_subscribe_set_round_trips_through_the_session() {
        // The capture subscribed with limit=10, so the session is configured with that limit and
        // every generated request must be byte-identical to the archived one. The archived
        // requests name both captured markets, so both are registered.
        let mut session = OndoWsSession::new(10, None);
        let nvda = instrument("NVDA-USD.P");
        let tsla = instrument("TSLA-USD.P");
        let instruments = [nvda.id(), tsla.id()];
        session.register_instrument(&nvda);
        session.register_instrument(&tsla);

        let observed: Vec<SubscriptionRequest> = serde_json::from_str(SUBSCRIBE_FIXTURE).unwrap();
        for request in &observed {
            let sent = session
                .subscribe(request.channel, &instruments)
                .unwrap()
                .expect("every observed channel is newly subscribed");

            assert_eq!(sent, request.to_json_text().unwrap());
        }

        // The subscription snapshot is an unordered set of `channel:symbol` topics, so it is
        // compared as one rather than by the iteration order of the underlying maps.
        let mut topics = session.expected_topics();
        topics.sort();
        assert_eq!(
            topics,
            vec![
                "depthBooksPerps:NVDA-USD.P",
                "depthBooksPerps:TSLA-USD.P",
                "fundingRatesPerps:NVDA-USD.P",
                "fundingRatesPerps:TSLA-USD.P",
                "topOfBooksPerps:NVDA-USD.P",
                "topOfBooksPerps:TSLA-USD.P",
                "tradesPerps:NVDA-USD.P",
                "tradesPerps:TSLA-USD.P",
            ]
        );
    }

    #[rstest]
    fn test_a_second_local_subscriber_does_not_resubscribe() {
        let (mut session, instrument_id) = session_with_nvda();

        assert!(
            session
                .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
                .unwrap()
                .is_some()
        );
        assert!(
            session
                .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
                .unwrap()
                .is_none(),
            "the venue is not asked twice for the same topic"
        );
        assert_eq!(
            session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
            2
        );
    }

    #[rstest]
    fn test_book_reference_counting_is_shared_by_deltas_and_depth10() {
        let (mut session, instrument_id) = session_with_nvda();

        assert!(
            session
                .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
                .unwrap()
                .is_some(),
            "the first book subscriber subscribes"
        );
        assert!(
            session.subscribe_depth10(&instrument_id).unwrap().is_none(),
            "depth10 rides the same subscription instead of issuing a second one"
        );
        assert_eq!(
            session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
            2
        );

        assert!(
            session
                .unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id])
                .unwrap()
                .is_none(),
            "the book stays subscribed while depth10 still needs it"
        );
        assert_eq!(
            session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
            1
        );
        assert_eq!(
            session
                .unsubscribe_depth10(&instrument_id)
                .unwrap()
                .as_deref(),
            Some(r#"{"op":"unsubscribe","channel":"depthBooksPerps","markets":["NVDA-USD.P"]}"#)
        );
    }

    #[rstest]
    fn test_an_unsubscribed_topic_is_not_replayed_on_reconnect() {
        let (mut session, instrument_id) = session_with_nvda();
        let book = session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();
        let top = session
            .subscribe(WsChannel::TopOfBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();
        session.handle_raw_frame(&subscribed_ack(&book), UnixNanos::from(1));
        session.handle_raw_frame(&subscribed_ack(&top), UnixNanos::from(1));
        assert_eq!(session.confirmed_topics().len(), 2);

        let unsubscribe = session
            .unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();
        session.handle_raw_frame(&unsubscribed_ack(&unsubscribe), UnixNanos::from(1));

        let replayed = session.replay_requests();

        assert_eq!(
            replayed.len(),
            1,
            "only the still-desired channel is replayed"
        );
        assert!(replayed[0].contains("topOfBooksPerps"));
        assert!(
            !replayed[0].contains("depthBooksPerps"),
            "the venue is never asked again for a removed subscription"
        );
    }

    #[rstest]
    fn test_an_array_frame_routes_every_item_by_its_full_market() {
        let mut session = OndoWsSession::new(100, None);
        for market in ["NVDA-USD.P", "TSLA-USD.P"] {
            session.register_instrument(&instrument(market));
        }
        session
            .subscribe(
                WsChannel::DepthBooksPerps,
                &[
                    market_to_instrument_id("NVDA-USD.P").unwrap(),
                    market_to_instrument_id("TSLA-USD.P").unwrap(),
                ],
            )
            .unwrap();

        let frame = r#"{"type":"update","channel":"depthBooksPerps","timestamp":"2026-09-14T11:09:59.670112401Z","data":[
            {"market":"NVDA-USD.P","time":"2026-09-14T11:09:59.000000000Z","bids":[["100.00","1.00"]],"asks":[["101.00","1.00"]]},
            {"market":"TSLA-USD.P","time":"2026-09-14T11:09:59.500000000Z","bids":[["200.00","2.00"]],"asks":[["201.00","2.00"]]}
        ]}"#;
        let outcomes = session.handle_raw_frame(frame, UnixNanos::from(7));

        let mut markets = Vec::new();
        for outcome in outcomes {
            match outcome {
                WsOutcome::Data(Data::Deltas(deltas)) => {
                    markets.push(deltas.instrument_id.to_string());
                }
                // The first accepted snapshot of each book also publishes the local feed state.
                WsOutcome::Data(Data::InstrumentStatus(_)) => {}
                other => panic!("each item publishes a delta batch: {other:?}"),
            }
        }
        markets.sort();

        assert_eq!(markets, vec!["NVDA-USD-PERP.ONDO", "TSLA-USD-PERP.ONDO"]);
    }

    #[rstest]
    fn test_an_unknown_market_keeps_its_evidence_and_stays_pending() {
        let (mut session, instrument_id) = session_with_nvda();
        let request = session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();
        session.handle_raw_frame(&subscribed_ack(&request), UnixNanos::from(1));

        let frame = r#"{"type":"update","channel":"depthBooksPerps","data":[
            {"market":"AAPL-USD.P","time":"2026-09-14T11:09:59Z","bids":[["1.00","1.00"]],"asks":[]}
        ]}"#;
        let outcomes = session.handle_raw_frame(frame, UnixNanos::from(7));

        assert_eq!(outcomes.len(), 1);
        let WsOutcome::Unsupported { reason, raw_frame } = &outcomes[0] else {
            panic!("an unloaded market is an unsupported item: {outcomes:?}");
        };
        assert!(reason.contains("AAPL-USD.P"));
        assert!(raw_frame.contains("AAPL-USD.P"));
        assert_eq!(session.counters().unknown_markets, 1);
        assert!(
            session.pending_topics().is_empty(),
            "a different market's frames do not degrade the NVDA subscription"
        );
    }

    #[rstest]
    fn test_a_subscribed_acknowledgement_is_not_a_market_update() {
        let (mut session, instrument_id) = session_with_nvda();
        let request = session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();

        let outcomes = session.handle_raw_frame(&subscribed_ack(&request), UnixNanos::from(7));

        assert!(outcomes.is_empty(), "an acknowledgement publishes no data");
        assert_eq!(session.counters().subscribed_acks, 1);
        assert_eq!(
            session.confirmed_topics(),
            vec!["depthBooksPerps:NVDA-USD.P".to_string()]
        );
        assert!(session.pending_topics().is_empty());
    }

    #[rstest]
    fn test_a_subscription_confirmation_is_not_proof_that_data_is_flowing() {
        let (mut session, instrument_id) = session_with_nvda();
        let request = session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap()
            .unwrap();
        session.handle_raw_frame(&subscribed_ack(&request), UnixNanos::from(7));

        assert_eq!(session.confirmed_topics().len(), 1);
        assert!(
            !session.book_state(&instrument_id).unwrap().is_valid(),
            "the book is unusable until a snapshot arrives, however confirmed the subscription is"
        );
    }

    #[rstest]
    fn test_pong_is_handled_without_publishing_data() {
        let (mut session, _) = session_with_nvda();

        let outcomes = session.handle_raw_frame(r#"{"type":"pong"}"#, UnixNanos::from(7));

        assert!(outcomes.is_empty());
        assert_eq!(session.counters().pongs, 1);
    }

    #[rstest]
    fn test_an_error_frame_is_a_protocol_error_that_keeps_its_evidence() {
        let (mut session, _) = session_with_nvda();
        let raw = r#"{"type":"error","message":"unknown channel","code":400}"#;
        let outcomes = session.handle_raw_frame(raw, UnixNanos::from(7));

        let WsOutcome::ProtocolError { reason, raw_frame } = &outcomes[0] else {
            panic!("a venue error is a protocol error: {outcomes:?}");
        };
        assert!(reason.contains("unknown channel"));
        assert!(reason.contains("400"));
        assert_eq!(raw_frame, raw);
        assert_eq!(session.counters().protocol_errors, 1);
    }

    #[rstest]
    fn test_an_unknown_message_type_is_unsupported_and_keeps_the_frame() {
        let (mut session, _) = session_with_nvda();
        let raw = r#"{"type":"snapshot","channel":"depthBooksPerps"}"#;
        let outcomes = session.handle_raw_frame(raw, UnixNanos::from(7));

        let WsOutcome::Unsupported { reason, raw_frame } = &outcomes[0] else {
            panic!("an unknown message type is unsupported: {outcomes:?}");
        };
        assert!(reason.contains("snapshot"));
        assert_eq!(raw_frame, raw);
        assert_eq!(session.counters().unknown_message_types, 1);
    }

    #[rstest]
    fn test_a_malformed_frame_is_a_protocol_error_that_keeps_the_frame() {
        let (mut session, _) = session_with_nvda();
        let outcomes = session.handle_raw_frame("not json", UnixNanos::from(7));

        let WsOutcome::ProtocolError { raw_frame, .. } = &outcomes[0] else {
            panic!("a malformed frame is a protocol error: {outcomes:?}");
        };
        assert_eq!(raw_frame, "not json");
    }

    #[rstest]
    fn test_the_depth_fixture_produces_one_clear_plus_add_batch_and_a_ready_status() {
        let (mut session, instrument_id) = session_with_nvda();
        let outcomes = session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(7));

        assert_eq!(outcomes.len(), 2, "a delta batch and a ready status");
        let WsOutcome::Data(Data::Deltas(deltas)) = &outcomes[0] else {
            panic!("the first outcome is the batch: {outcomes:?}");
        };
        assert_eq!(
            deltas.deltas.len(),
            21,
            "one CLEAR plus ten levels per side"
        );
        assert_eq!(deltas.sequence, 0, "no exchange sequence is invented");
        assert_eq!(session.counters().snapshot_ready_published, 1);

        let state = session.book_state(&instrument_id).unwrap();
        assert!(state.is_valid());
        assert_eq!(state.book().level_counts(), (10, 10));
    }

    #[rstest]
    fn test_the_top_of_book_fixture_publishes_a_quote() {
        let (mut session, _) = session_with_nvda();
        let outcomes = session.handle_raw_frame(TOP_FIXTURE, UnixNanos::from(7));

        let WsOutcome::Data(Data::Quote(quote)) = &outcomes[0] else {
            panic!("the top of book channel publishes a quote: {outcomes:?}");
        };
        assert_eq!(quote.bid_price.to_string(), "212.22");
        assert_eq!(quote.ask_price.to_string(), "212.25");
        assert_eq!(quote.ts_event, ts("2026-09-14T11:09:58.8461101Z"));
        assert_ne!(
            quote.ts_event,
            ts("2026-09-14T11:09:59.670112401Z"),
            "the envelope send time is not the item event time"
        );
    }

    #[rstest]
    fn test_a_one_sided_top_of_book_is_skipped_rather_than_zero_filled() {
        let (mut session, _) = session_with_nvda();
        let frame = r#"{"type":"update","channel":"topOfBooksPerps","data":[
            {"market":"NVDA-USD.P","time":"2026-09-14T11:09:00Z","bids":[["100.00","1.00"]],"asks":[]}
        ]}"#;
        let outcomes = session.handle_raw_frame(frame, UnixNanos::from(7));

        let WsOutcome::Ignored { detail } = &outcomes[0] else {
            panic!("a one-sided top of book is skipped: {outcomes:?}");
        };
        assert!(detail.contains("one-sided"));
        assert_eq!(session.counters().skipped_items, 1);
    }

    #[rstest]
    fn test_the_trades_fixture_maps_its_item_event_time() {
        let (mut session, _) = session_with_nvda();
        let outcomes = session.handle_raw_frame(TRADES_FIXTURE, UnixNanos::from(7));

        let WsOutcome::Data(Data::Trade(tick)) = &outcomes[0] else {
            panic!("the trades channel publishes a trade: {outcomes:?}");
        };
        assert_eq!(tick.price.to_string(), "212.22");
        assert_eq!(tick.size.to_string(), "0.19");
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.ts_event, ts("2026-09-14T11:10:04.382125561Z"));
        assert_ne!(tick.ts_event, ts("2026-09-14T11:10:04.386125572Z"));
    }

    #[rstest]
    fn test_the_funding_fixture_keeps_the_raw_rate_and_separates_interval_ends() {
        let (mut session, _) = session_with_nvda();
        let outcomes = session.handle_raw_frame(FUNDING_FIXTURE, UnixNanos::from(7));

        let WsOutcome::Data(Data::FundingRate(update)) = &outcomes[0] else {
            panic!("the funding channel publishes a funding rate: {outcomes:?}");
        };

        assert_eq!(update.rate.to_string(), "0.0000063");
        assert_eq!(
            update.next_funding_ns,
            Some(ts("2026-09-14T12:00:00Z")),
            "intervalEnds is a settlement time and maps to next_funding_ns"
        );
        assert_eq!(
            update.ts_event,
            ts("2026-09-14T11:09:59.670112401Z"),
            "the event time is the envelope send time, never the future settlement time"
        );
        assert_ne!(update.ts_event, ts("2026-09-14T12:00:00Z"));
        assert_eq!(session.counters().funding_frames, 1);
    }

    #[rstest]
    fn test_the_mark_price_example_routes_by_market_rather_than_by_position() {
        let (mut session, _) = session_with_nvda();
        // The official example names AAPL-USD.P, which this session does not track, so the frame is
        // reported as an unloaded market instead of being attributed to NVDA.
        let outcomes = session.handle_raw_frame(MARKPRICES_FIXTURE, UnixNanos::from(7));

        assert!(matches!(outcomes[0], WsOutcome::Unsupported { .. }));

        let frame = r#"{"type":"update","channel":"markPricesPerps","timestamp":"2026-09-14T11:09:59.670112401Z","data":[
            {"market":"NVDA-USD.P","markPrice":"227.50"}
        ]}"#;
        let outcomes = session.handle_raw_frame(frame, UnixNanos::from(7));

        let WsOutcome::Data(Data::MarkPrice(update)) = &outcomes[0] else {
            panic!("the mark price channel publishes a mark price: {outcomes:?}");
        };
        assert_eq!(update.value.to_string(), "227.50");
        assert_eq!(update.ts_event, ts("2026-09-14T11:09:59.670112401Z"));
    }

    #[rstest]
    fn test_a_disconnect_publishes_a_local_feed_state_for_every_subscribed_instrument() {
        let (mut session, instrument_id) = session_with_nvda();
        session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap();
        session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(7));
        assert!(session.book_state(&instrument_id).unwrap().is_valid());

        let outcomes = session.invalidate_all(REASON_DISCONNECTED, UnixNanos::from(9));

        assert_eq!(outcomes.len(), 1);
        let WsOutcome::Data(Data::InstrumentStatus(status)) = &outcomes[0] else {
            panic!("a disconnect publishes a feed state: {outcomes:?}");
        };
        assert_eq!(status.instrument_id, instrument_id);
        assert_eq!(status.action, MarketStatusAction::None);
        assert_eq!(status.reason, Some(Ustr::from(REASON_DISCONNECTED)));
        assert_eq!(status.is_quoting, Some(false));
        assert_eq!(status.is_trading, None);
        assert_eq!(status.ts_event, UnixNanos::from(9));
        assert!(!session.book_state(&instrument_id).unwrap().is_valid());
        assert_eq!(session.counters().disconnected_published, 1);
    }

    #[rstest]
    fn test_the_ready_status_is_published_only_on_the_transition_to_ready() {
        let (mut session, _) = session_with_nvda();
        let first = session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(7));
        assert!(matches!(
            first[1],
            WsOutcome::Data(Data::InstrumentStatus(_))
        ));

        // A later snapshot is published as data. The book is already valid, so this is not a
        // transition and no second ready status may be published. (An identical repeat at the same
        // item time is suppressed as a duplicate instead; that path is covered separately.)
        let later = r#"{"type":"update","channel":"depthBooksPerps","data":[
            {"market":"NVDA-USD.P","time":"2026-09-14T11:10:00.000000000Z","bids":[["212.23","1.00"]],"asks":[["212.24","1.00"]]}]}"#;
        let second = session.handle_raw_frame(later, UnixNanos::from(8));

        assert_eq!(second.len(), 1, "data only, never another ready status");
        assert!(matches!(second[0], WsOutcome::Data(Data::Deltas(_))));
        assert_eq!(session.counters().snapshot_ready_published, 1);
    }

    #[rstest]
    fn test_an_oversized_frame_is_reported_and_never_parsed() {
        let (mut session, _) = session_with_nvda();
        let huge = format!(
            r#"{{"type":"pong","padding":"{}"}}"#,
            "x".repeat(ONDO_WS_MAX_SERVER_MESSAGE_BYTES)
        );

        let outcomes = session.handle_raw_frame(&huge, UnixNanos::from(7));

        let WsOutcome::ProtocolError { reason, .. } = &outcomes[0] else {
            panic!("an oversized frame is a protocol error: {outcomes:?}");
        };
        assert!(reason.contains("inbound bound"));
        assert_eq!(session.counters().oversized_frames, 1);
        assert_eq!(
            session.counters().pongs,
            0,
            "the frame was never parsed as a heartbeat response"
        );
    }

    #[rstest]
    fn test_a_depth10_subscriber_gets_a_projection_of_the_same_book() {
        let (mut session, instrument_id) = session_with_nvda();
        session.subscribe_depth10(&instrument_id).unwrap();

        let outcomes = session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(7));

        assert_eq!(
            outcomes.len(),
            3,
            "deltas, the depth10 projection, and the ready status"
        );
        let WsOutcome::Data(Data::Depth10(depth)) = &outcomes[1] else {
            panic!("the depth10 is projected from the same snapshot: {outcomes:?}");
        };
        assert_eq!(depth.bids[0].price.to_string(), "212.22");
        assert_eq!(depth.asks[0].price.to_string(), "212.25");
    }

    #[rstest]
    fn test_registering_an_instrument_keeps_the_venue_market_string() {
        let (mut session, instrument_id) = session_with_nvda();

        assert_eq!(session.instrument_count(), 1);
        assert_eq!(session.book_state(&instrument_id).unwrap().session_id(), 1);
        assert!(
            session
                .subscribe(WsChannel::DepthBooksPerps, &[])
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    fn test_subscribing_to_an_unloaded_instrument_is_refused() {
        let (mut session, _) = session_with_nvda();
        let unknown = market_to_instrument_id("TSLA-USD.P").unwrap();
        let error = session
            .subscribe(WsChannel::DepthBooksPerps, &[unknown])
            .unwrap_err();

        assert!(error.to_string().contains("TSLA-USD-PERP.ONDO"));
    }

    #[rstest]
    fn test_the_confirmed_and_expected_sets_are_kept_apart() {
        let (mut session, instrument_id) = session_with_nvda();
        session
            .subscribe(WsChannel::FundingRatesPerps, &[instrument_id])
            .unwrap();

        assert_eq!(session.confirmed_topics().len(), 0);
        assert_eq!(
            session.pending_topics(),
            vec!["fundingRatesPerps:NVDA-USD.P".to_string()]
        );
        assert_eq!(session.expected_topics().len(), 1);
    }

    #[rstest]
    fn test_the_heartbeat_is_the_application_level_ping() {
        let (session, _) = session_with_nvda();

        assert_eq!(session.heartbeat_request(), r#"{"op":"ping"}"#);
    }

    #[rstest]
    fn test_duplicate_snapshots_are_suppressed_and_conflicts_are_counted() {
        let (mut session, instrument_id) = session_with_nvda();
        let time = "2026-09-14T11:09:59.000000000Z";
        let first = format!(
            r#"{{"type":"update","channel":"depthBooksPerps","data":[{{"market":"NVDA-USD.P","time":"{time}","bids":[["100.00","1.00"]],"asks":[["101.00","1.00"]]}}]}}"#
        );
        let conflicting = format!(
            r#"{{"type":"update","channel":"depthBooksPerps","data":[{{"market":"NVDA-USD.P","time":"{time}","bids":[["99.00","1.00"]],"asks":[["102.00","1.00"]]}}]}}"#
        );

        assert_eq!(
            session.handle_raw_frame(&first, UnixNanos::from(1)).len(),
            2
        );
        assert!(
            session
                .handle_raw_frame(&first, UnixNanos::from(2))
                .is_empty(),
            "an identical repeat at the same time publishes nothing"
        );
        assert_eq!(
            session
                .handle_raw_frame(&conflicting, UnixNanos::from(3))
                .len(),
            1
        );

        let counters = session.book_counters(&instrument_id).unwrap();
        assert_eq!(counters.accepted, 2);
        assert_eq!(counters.duplicates, 1);
        assert_eq!(counters.conflicts, 1);
        assert_eq!(
            session
                .book_state(&instrument_id)
                .unwrap()
                .book()
                .best_bid()
                .unwrap()
                .0
                .to_string(),
            "99.00"
        );
    }

    #[rstest]
    fn test_an_older_snapshot_cannot_restore_an_old_book() {
        let (mut session, instrument_id) = session_with_nvda();
        let newer = r#"{"type":"update","channel":"depthBooksPerps","data":[
            {"market":"NVDA-USD.P","time":"2026-09-14T11:09:59.900000000Z","bids":[["100.00","1.00"]],"asks":[["101.00","1.00"]]}]}"#;
        let older = r#"{"type":"update","channel":"depthBooksPerps","data":[
            {"market":"NVDA-USD.P","time":"2026-09-14T11:09:58.000000000Z","bids":[["50.00","1.00"]],"asks":[["51.00","1.00"]]}]}"#;

        session.handle_raw_frame(newer, UnixNanos::from(1));
        let outcomes = session.handle_raw_frame(older, UnixNanos::from(2));

        assert!(outcomes.is_empty(), "a stale snapshot publishes nothing");
        let state = session.book_state(&instrument_id).unwrap();
        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "100.00");
        assert_eq!(state.counters().stale_rejected, 1);
    }

    #[rstest]
    fn test_a_disconnect_makes_the_old_book_unusable_until_a_new_snapshot() {
        let (mut session, instrument_id) = session_with_nvda();
        session
            .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .unwrap();
        session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(1));
        assert!(session.book_state(&instrument_id).unwrap().is_valid());

        session.invalidate_all(REASON_DISCONNECTED, UnixNanos::from(2));

        assert_eq!(session.session_id(), 2);
        assert!(!session.book_state(&instrument_id).unwrap().is_valid());
        assert!(
            session
                .book_state(&instrument_id)
                .unwrap()
                .book()
                .is_empty()
        );

        let outcomes = session.handle_raw_frame(DEPTH_FIXTURE, UnixNanos::from(3));

        assert_eq!(outcomes.len(), 2, "the new session accepts the snapshot");
        assert!(session.book_state(&instrument_id).unwrap().is_valid());
    }

    #[rstest]
    fn test_feed_status_is_a_local_state_not_an_exchange_halt() {
        let status = feed_status(
            market_to_instrument_id("NVDA-USD.P").unwrap(),
            REASON_DISCONNECTED,
            false,
            UnixNanos::from(5),
        );

        assert_eq!(status.action, MarketStatusAction::None);
        assert_eq!(
            status.trading_event, None,
            "a local feed state never claims an exchange trading event"
        );
        assert_ne!(status.action, MarketStatusAction::Halt);
    }
}
