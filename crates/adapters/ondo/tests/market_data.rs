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

//! Offline WebSocket tests for the Ondo Perps data feed.
//!
//! Nothing here reaches the venue. Every market-data frame is a fixture from `test_data/ws/`
//! (whose `kind` is read from `test_data/manifest.json`, never from its file name) or an inline
//! body built in this file. The two connection-lifecycle tests run against a mock WebSocket server
//! bound to `127.0.0.1:0` inside the test process, so the public transport is exercised without a
//! network dependency and without credentials.
//!
//! The last section drives the whole chain in one process against a mock venue: `GET /v1/markets`
//! over a mock REST endpoint, frames over the mock socket, the [`OndoDataClient`], the platform's own
//! data engine, and a message-bus subscriber on the instrument status topic. No live host and no
//! credential is involved anywhere in this file.

use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    clock::{Clock, TestClock},
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeBookDepth10, SubscribeInstrument,
            SubscribeInstrumentStatus, UnsubscribeBookDeltas, UnsubscribeBookDepth10,
        },
    },
    msgbus::{self, MessageBus, stubs::get_any_saving_handler, switchboard},
    providers::InstrumentProvider,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_data::engine::DataEngine;
use nautilus_model::{
    data::{
        Data, FundingRateUpdate, InstrumentStatus, MarkPriceUpdate, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, BookType, MarketStatusAction, RecordFlag},
    identifiers::{ClientId, InstrumentId, TraderId},
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
use nautilus_ondo::{
    common::parse::{market_to_instrument_id, parse_decimal, parse_timestamp},
    config::OndoDataClientConfig,
    data::{OndoDataClient, REASON_METADATA_READY, REASON_METADATA_STALE},
    http::{models::parse_instruments, rate_limit::OndoRateBudget},
    websocket::{
        EventTimeSource, NO_EXCHANGE_SEQUENCE, OndoBookState, OndoWebSocketClient, OndoWsSession,
        ParsedBookSnapshot, REASON_DISCONNECTED, REASON_SNAPSHOT_READY, SnapshotOutcome,
        SubscriptionRequest, WsChannel, WsMessageType, WsOp, WsOutcome, WsUpdate, convert_price,
        decode_updates, parse_book_snapshot, parse_funding_rate, parse_mark_price,
        parse_server_message,
    },
};
use rstest::rstest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

const NVDA_MARKET: &str = "NVDA-USD.P";
const TSLA_MARKET: &str = "TSLA-USD.P";
const NVDA_INSTRUMENT: &str = "NVDA-USD-PERP.ONDO";
const TSLA_INSTRUMENT: &str = "TSLA-USD-PERP.ONDO";
/// The market the venue's 2026-09-11 observation listed as `disabled`: the fixture's third trading
/// pair, used to prove a non-empty `load_ids` set is a boundary rather than a hint.
const ENA_INSTRUMENT: &str = "ENA-USD-PERP.ONDO";

/// The envelope `timestamp` of the archived update frames: the server send/batch time, which is
/// never an item's price event time.
const ENVELOPE_TIMESTAMP: &str = "2026-09-14T11:09:59.670112401Z";

/// The archived frames. `ws/markprices_observed.json` is named `_observed` but its manifest `kind`
/// is `official-example`; see `test_the_mark_price_fixture_is_an_official_example_per_the_manifest`.
const DEPTH_FIXTURE: &str = include_str!("../test_data/ws/depth_observed.json");
const TOP_OF_BOOK_FIXTURE: &str = include_str!("../test_data/ws/topofbook_observed.json");
const TRADES_FIXTURE: &str = include_str!("../test_data/ws/trades_observed.json");
const FUNDING_FIXTURE: &str = include_str!("../test_data/ws/funding_observed.json");
const MARK_PRICES_FIXTURE: &str = include_str!("../test_data/ws/markprices_observed.json");
const MANIFEST: &str = include_str!("../test_data/manifest.json");
const MARKETS_FIXTURE: &str = include_str!("../test_data/rest/markets_synthetic.json");

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// The single local receive time every test passes as `ts_init`.
fn ts_init() -> UnixNanos {
    UnixNanos::from(7)
}

fn ts(value: &str) -> UnixNanos {
    parse_timestamp(value).expect("a fixture timestamp is RFC 3339")
}

fn instrument_id(market: &str) -> InstrumentId {
    market_to_instrument_id(market).expect("the market maps to an instrument id")
}

/// Builds a test instrument through the shared metadata boundary, so a test can never disagree with
/// the production precision path.
fn instrument(market: &str) -> InstrumentAny {
    parse_instruments(MARKETS_FIXTURE, &[instrument_id(market)], ts_init())
        .expect("the market metadata fixture builds the instrument")
        .into_iter()
        .next()
        .expect("one instrument per requested id")
}

fn session_for(markets: &[&str]) -> OndoWsSession {
    let mut session = OndoWsSession::new(100, None);

    for market in markets {
        session.register_instrument(&instrument(market));
    }

    session
}

/// A `depthBooksPerps` update frame for one market, carrying exactly the levels given.
fn book_frame(market: &str, time: &str, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
    let levels = |side: &[(&str, &str)]| {
        side.iter()
            .map(|(price, size)| serde_json::json!([price, size]))
            .collect::<Vec<_>>()
    };

    serde_json::json!({
        "type": "update",
        "channel": "depthBooksPerps",
        "timestamp": ENVELOPE_TIMESTAMP,
        "data": [{
            "market": market,
            "time": time,
            "bids": levels(bids),
            "asks": levels(asks),
        }],
    })
    .to_string()
}

/// A `depthBooksPerps` update frame carrying one item per `(market, time, bid, ask)` entry.
fn multi_market_book_frame(items: &[(&str, &str, &str, &str)]) -> String {
    let data = items
        .iter()
        .map(|(market, time, bid, ask)| {
            serde_json::json!({
                "market": market,
                "time": time,
                "bids": [[bid, "1.00"]],
                "asks": [[ask, "1.00"]],
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "type": "update",
        "channel": "depthBooksPerps",
        "timestamp": ENVELOPE_TIMESTAMP,
        "data": data,
    })
    .to_string()
}

/// An acknowledgement frame that echoes the request it answers inside `data`, which is the observed
/// shape (`test_data/conflicts.md` conflict 8).
fn ack(kind: &str, request: &str) -> String {
    let echo: serde_json::Value = serde_json::from_str(request).expect("a request body is JSON");

    serde_json::json!({
        "type": kind,
        "channel": echo["channel"],
        "timestamp": ENVELOPE_TIMESTAMP,
        "data": echo,
    })
    .to_string()
}

/// Parses one book frame into the batch a caller hands to [`OndoBookState::apply_snapshot`], through
/// the same wire path the adapter uses.
fn parsed_snapshot_of(frame: &str, instrument: &InstrumentAny) -> ParsedBookSnapshot {
    let message = parse_server_message(frame).expect("a book frame decodes");
    let channel = WsChannel::from_wire(
        message
            .channel
            .as_deref()
            .expect("the frame names a channel"),
    )
    .expect("the frame names a known channel");
    let updates = decode_updates(
        channel,
        message.data.as_ref().expect("the frame carries data"),
    )
    .expect("the frame decodes as its channel schema");
    let WsUpdate::Book(item) = &updates[0] else {
        panic!("the depth channel decodes to book items");
    };
    let ts_event = ts(item
        .time
        .as_deref()
        .expect("a book item carries an event time"));

    parse_book_snapshot(item, instrument, ts_event, ts_init())
        .expect("the snapshot is representable at the declared precision")
}

/// What a batch of outcomes published, split by kind.
#[derive(Default)]
struct Published {
    deltas: Vec<OrderBookDeltas>,
    depth10: Vec<OrderBookDepth10>,
    quotes: Vec<QuoteTick>,
    trades: Vec<TradeTick>,
    funding: Vec<FundingRateUpdate>,
    marks: Vec<MarkPriceUpdate>,
    statuses: Vec<InstrumentStatus>,
    ignored: Vec<String>,
    unsupported: Vec<String>,
    protocol_errors: Vec<String>,
}

impl Published {
    fn collect(outcomes: Vec<WsOutcome>) -> Self {
        let mut published = Self::default();

        for outcome in outcomes {
            published.push(outcome);
        }

        published
    }

    fn push(&mut self, outcome: WsOutcome) {
        match outcome {
            WsOutcome::Data(data) => match data {
                Data::Deltas(deltas) => self.deltas.push(*deltas),
                Data::Depth10(depth) => self.depth10.push(*depth),
                Data::Quote(quote) => self.quotes.push(quote),
                Data::Trade(trade) => self.trades.push(trade),
                Data::FundingRate(update) => self.funding.push(update),
                Data::MarkPrice(update) => self.marks.push(update),
                Data::InstrumentStatus(status) => self.statuses.push(status),
                other => panic!("unexpected data published by the adapter: {other:?}"),
            },
            WsOutcome::Ignored { detail } => self.ignored.push(detail),
            WsOutcome::Unsupported { reason, .. } => self.unsupported.push(reason),
            WsOutcome::ProtocolError { reason, .. } => self.protocol_errors.push(reason),
        }
    }

    /// The number of market data values, which is never a local feed state.
    fn market_data(&self) -> usize {
        self.deltas.len()
            + self.depth10.len()
            + self.quotes.len()
            + self.trades.len()
            + self.funding.len()
            + self.marks.len()
    }

    /// The local feed state published with `reason`, when one was published.
    fn status_with_reason(&self, reason: &str) -> Option<&InstrumentStatus> {
        self.statuses
            .iter()
            .find(|status| status.reason == Some(Ustr::from(reason)))
    }
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();

    values
}

fn subscribe_body(channel: WsChannel, markets: &[&str]) -> String {
    SubscriptionRequest::new(
        WsOp::Subscribe,
        channel,
        markets.iter().map(|market| (*market).to_string()).collect(),
        Some(10),
        Some(0),
        None,
    )
    .to_json_text()
    .expect("a subscription request serializes")
}

/// A mock WebSocket endpoint inside the test process. It accepts every connection, counts them, and
/// records the text of every frame a client sends.
struct MockServer {
    url: String,
    connections: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a mock endpoint binds on the loopback interface");
        let url = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("the mock endpoint has an address")
        );
        let connections = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::clone(&connections);
        let recorded = Arc::clone(&bodies);

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);

                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };

                    while let Some(Ok(message)) = socket.next().await {
                        if let Message::Text(text) = message {
                            recorded
                                .lock()
                                .expect("the mock body log is not poisoned")
                                .push(text.to_string());
                        }
                    }
                });
            }
        });

        Self {
            url,
            connections,
            bodies,
        }
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies
            .lock()
            .expect("the mock body log is not poisoned")
            .clone()
    }
}

async fn wait_until<F: Fn() -> bool>(predicate: F, what: &str) {
    for _ in 0..500 {
        if predicate() {
            return;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("timed out after 5s waiting for {what}");
}

// ------------------------------------------------------------------------------------------------
// Routing and subscription bookkeeping
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_data_array_routes_every_item_by_its_full_market() {
    let mut session = session_for(&[NVDA_MARKET, TSLA_MARKET]);
    session
        .subscribe(
            WsChannel::DepthBooksPerps,
            &[instrument_id(NVDA_MARKET), instrument_id(TSLA_MARKET)],
        )
        .expect("both markets are loaded");

    let frame = multi_market_book_frame(&[
        (
            NVDA_MARKET,
            "2026-09-14T11:09:59.000000000Z",
            "100.00",
            "101.00",
        ),
        (
            TSLA_MARKET,
            "2026-09-14T11:09:59.500000000Z",
            "200.00",
            "201.00",
        ),
    ]);
    let published = Published::collect(session.handle_raw_frame(&frame, ts_init()));

    // One batch per market, each carrying its own item's instrument - never the neighbouring item's.
    assert_eq!(published.deltas.len(), 2);
    let instruments = sorted(
        published
            .deltas
            .iter()
            .map(|batch| batch.instrument_id.to_string())
            .collect(),
    );
    assert_eq!(instruments, vec![NVDA_INSTRUMENT, TSLA_INSTRUMENT]);
    // The first accepted snapshot of each book also publishes the local feed state.
    assert_eq!(published.statuses.len(), 2);
}

#[rstest]
fn test_an_item_for_an_unloaded_market_is_reported_and_does_not_affect_the_loaded_one() {
    let mut session = session_for(&[NVDA_MARKET]);
    session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id(NVDA_MARKET)])
        .expect("NVDA is loaded");

    let frame = multi_market_book_frame(&[
        (
            NVDA_MARKET,
            "2026-09-14T11:09:59.000000000Z",
            "100.00",
            "101.00",
        ),
        (
            "AAPL-USD.P",
            "2026-09-14T11:09:59.000000000Z",
            "227.00",
            "228.00",
        ),
    ]);
    let published = Published::collect(session.handle_raw_frame(&frame, ts_init()));

    assert_eq!(published.deltas.len(), 1);
    assert_eq!(
        published.deltas[0].instrument_id.to_string(),
        NVDA_INSTRUMENT
    );
    assert_eq!(published.unsupported.len(), 1);
    assert!(published.unsupported[0].contains("AAPL-USD.P"));
    assert_eq!(session.counters().unknown_markets, 1);
}

#[rstest]
fn test_control_messages_are_never_published_as_market_data() {
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument_id(NVDA_MARKET);
    let request = session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded")
        .expect("the first subscriber subscribes");

    let subscribed =
        Published::collect(session.handle_raw_frame(&ack("subscribed", &request), ts_init()));
    assert_eq!(subscribed.market_data(), 0, "an ack is not data");
    assert!(subscribed.protocol_errors.is_empty());

    let pong = Published::collect(session.handle_raw_frame(r#"{"type":"pong"}"#, ts_init()));
    assert_eq!(pong.market_data(), 0);

    // A venue error that even carries a `data` array of book-shaped items is still an error: it is
    // never decoded as market data.
    let sneaky = r#"{"type":"error","channel":"depthBooksPerps","message":"bad depthLevels","code":400,"data":[{"market":"NVDA-USD.P","time":"2026-09-14T11:09:59Z","bids":[["100.00","1.00"]],"asks":[]}]}"#;
    let error = Published::collect(session.handle_raw_frame(sneaky, ts_init()));
    assert_eq!(error.market_data(), 0);
    assert_eq!(error.protocol_errors.len(), 1);
    assert!(error.protocol_errors[0].contains("bad depthLevels"));

    let unsubscribe = session
        .unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded")
        .expect("the last subscriber unsubscribes");
    let unsubscribed =
        Published::collect(session.handle_raw_frame(&ack("unsubscribed", &unsubscribe), ts_init()));
    assert_eq!(unsubscribed.market_data(), 0);

    // Every control frame moved its own counter, and none of them touched the book.
    let counters = session.counters();
    assert_eq!(counters.subscribed_acks, 1);
    assert_eq!(counters.unsubscribed_acks, 1);
    assert_eq!(counters.pongs, 1);
    assert!(counters.protocol_errors >= 1);
    assert!(
        !session
            .book_state(&instrument_id)
            .expect("the book exists")
            .is_valid(),
        "no control frame is a snapshot"
    );
}

#[rstest]
fn test_the_expected_and_confirmed_subscription_sets_are_distinct() {
    let mut session = session_for(&[NVDA_MARKET, TSLA_MARKET]);
    let markets = [instrument_id(NVDA_MARKET), instrument_id(TSLA_MARKET)];
    let request = session
        .subscribe(WsChannel::DepthBooksPerps, &markets)
        .expect("both markets are loaded")
        .expect("a new topic is subscribed");

    let expected = vec![
        "depthBooksPerps:NVDA-USD.P".to_string(),
        "depthBooksPerps:TSLA-USD.P".to_string(),
    ];
    assert_eq!(sorted(session.expected_topics()), expected);
    assert_eq!(sorted(session.pending_topics()), expected);
    assert!(
        session.confirmed_topics().is_empty(),
        "a request is not a confirmation"
    );

    Published::collect(session.handle_raw_frame(&ack("subscribed", &request), ts_init()));

    assert_eq!(sorted(session.confirmed_topics()), expected);
    assert_eq!(sorted(session.expected_topics()), expected);
    assert!(session.pending_topics().is_empty());
}

// ------------------------------------------------------------------------------------------------
// Book replacement
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_snapshot_replaces_the_whole_covered_range() {
    let instrument = instrument(NVDA_MARKET);
    let mut state = OndoBookState::new(1);

    // Snapshot A: bids 100 x 1, 99 x 2; ask 101 x 1.
    let a = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:00Z",
            &[("100.00", "1.00"), ("99.00", "2.00")],
            &[("101.00", "1.00")],
        ),
        &instrument,
    );
    assert_eq!(
        state
            .apply_snapshot(1, ts("2026-09-14T11:09:00Z"), &a)
            .expect("the snapshot applies"),
        SnapshotOutcome::Accepted { conflict: false }
    );
    assert_eq!(state.book().level_counts(), (2, 1));
    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "100.00"
    );
    assert_eq!(
        state.book().best_ask().expect("an ask").0.to_string(),
        "101.00"
    );

    // Snapshot B: bid 98 x 3; ask 102 x 4. Nothing from A survives.
    let b = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:01Z",
            &[("98.00", "3.00")],
            &[("102.00", "4.00")],
        ),
        &instrument,
    );
    assert_eq!(
        state
            .apply_snapshot(1, ts("2026-09-14T11:09:01Z"), &b)
            .expect("the snapshot applies"),
        SnapshotOutcome::Accepted { conflict: false }
    );

    // 100, 99 and 101 are absent: the covered range now holds one level per side and the best levels
    // are B's.
    assert_eq!(state.book().level_counts(), (1, 1));
    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "98.00"
    );
    assert_eq!(
        state.book().best_ask().expect("an ask").0.to_string(),
        "102.00"
    );
    assert!(state.is_valid());
    assert_eq!(state.counters().accepted, 2);
}

#[rstest]
fn test_an_empty_snapshot_clears_both_sides() {
    let instrument = instrument(NVDA_MARKET);
    let mut state = OndoBookState::new(1);

    let two_sided = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:00Z",
            &[("100.00", "1.00")],
            &[("101.00", "1.00")],
        ),
        &instrument,
    );
    state
        .apply_snapshot(1, ts("2026-09-14T11:09:00Z"), &two_sided)
        .expect("the snapshot applies");
    assert!(state.is_valid());

    let empty = parsed_snapshot_of(
        &book_frame(NVDA_MARKET, "2026-09-14T11:09:02Z", &[], &[]),
        &instrument,
    );
    let outcome = state
        .apply_snapshot(1, ts("2026-09-14T11:09:02Z"), &empty)
        .expect("the empty snapshot applies");

    assert_eq!(outcome, SnapshotOutcome::Accepted { conflict: false });
    assert!(state.book().is_empty());
    assert!(state.book().best_bid().is_none());
    assert!(state.book().best_ask().is_none());
    assert!(
        !state.is_valid(),
        "an empty book cannot be quoted against, so it stays unusable"
    );
    assert_eq!(state.invalid_reason(), Some("snapshot carried no levels"));
}

#[rstest]
fn test_a_one_sided_snapshot_replaces_the_other_side_too() {
    let instrument = instrument(NVDA_MARKET);
    let mut state = OndoBookState::new(1);

    let both = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:00Z",
            &[("100.00", "1.00")],
            &[("101.00", "1.00")],
        ),
        &instrument,
    );
    state
        .apply_snapshot(1, ts("2026-09-14T11:09:00Z"), &both)
        .expect("the snapshot applies");

    let bids_only = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:01Z",
            &[("99.00", "2.00")],
            &[],
        ),
        &instrument,
    );
    state
        .apply_snapshot(1, ts("2026-09-14T11:09:01Z"), &bids_only)
        .expect("the one-sided snapshot applies");

    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "99.00"
    );
    assert!(
        state.book().best_ask().is_none(),
        "the absent side is replaced, not kept"
    );
    assert_eq!(state.book().level_counts(), (1, 0));
}

#[rstest]
fn test_levels_absent_from_a_limited_snapshot_do_not_linger() {
    let instrument = instrument(NVDA_MARKET);
    let mut session = OndoWsSession::new(10, None);
    let instrument_id = instrument.id();
    session.register_instrument(&instrument);
    session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded");

    // A full 10-level frame, then a frame whose covered range carries one level per side.
    Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, ts_init()));
    {
        let state = session.book_state(&instrument_id).expect("the book exists");
        assert_eq!(state.book().level_counts(), (10, 10));
        assert_eq!(state.coverage_limit(), Some(10));
    }

    let narrow = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00.000000000Z",
        &[("212.20", "1.00")],
        &[("212.24", "1.00")],
    );
    Published::collect(session.handle_raw_frame(&narrow, ts_init()));

    let state = session.book_state(&instrument_id).expect("the book exists");
    assert_eq!(
        state.book().level_counts(),
        (1, 1),
        "levels the frame did not carry do not linger"
    );
    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "212.20"
    );
    assert_eq!(
        state.book().best_ask().expect("an ask").0.to_string(),
        "212.24"
    );
    assert_eq!(
        state.coverage_limit(),
        Some(10),
        "`limit=10` is a maximum, never a guarantee of ten levels"
    );
}

// ------------------------------------------------------------------------------------------------
// Batch marking
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_each_market_frame_is_one_clear_plus_adds_batch_with_the_snapshot_flag() {
    let instrument = instrument(NVDA_MARKET);
    let frame = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("100.00", "1.00"), ("99.00", "2.00")],
        &[("101.00", "1.00")],
    );
    let parsed = parsed_snapshot_of(&frame, &instrument);
    let batch = &parsed.deltas;

    assert_eq!(batch.deltas.len(), 4, "one CLEAR plus three levels");
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    for delta in &batch.deltas {
        assert_eq!(
            delta.flags & RecordFlag::F_SNAPSHOT as u8,
            RecordFlag::F_SNAPSHOT as u8,
            "every delta of a replacement carries F_SNAPSHOT"
        );
    }
    let (last, leading) = batch.deltas.split_last().expect("a non-empty batch");
    assert_eq!(
        last.flags & RecordFlag::F_LAST as u8,
        RecordFlag::F_LAST as u8
    );
    for delta in leading {
        assert_eq!(
            delta.flags & RecordFlag::F_LAST as u8,
            0,
            "F_LAST marks the end of the batch and nothing else"
        );
    }
    assert_eq!(batch.deltas[1].action, BookAction::Add);
    assert_eq!(batch.deltas[3].action, BookAction::Add);
}

#[rstest]
fn test_an_empty_snapshot_publishes_one_trailing_clear() {
    let instrument = instrument(NVDA_MARKET);
    let parsed = parsed_snapshot_of(
        &book_frame(NVDA_MARKET, "2026-09-14T11:09:00Z", &[], &[]),
        &instrument,
    );

    assert_eq!(parsed.deltas.deltas.len(), 1, "a CLEAR and nothing else");
    assert_eq!(parsed.deltas.deltas[0].action, BookAction::Clear);
    assert_eq!(
        parsed.deltas.deltas[0].flags,
        (RecordFlag::F_SNAPSHOT as u8) | (RecordFlag::F_LAST as u8)
    );
    assert!(parsed.wire.bids.is_empty());
    assert!(parsed.wire.asks.is_empty());
}

#[rstest]
fn test_no_exchange_sequence_is_fabricated() {
    assert_eq!(
        NO_EXCHANGE_SEQUENCE, 0,
        "the no-sequence convention is zero"
    );

    let instrument = instrument(NVDA_MARKET);
    let mut session = session_for(&[NVDA_MARKET]);

    let first = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    );
    let second = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:01Z",
        &[("99.00", "1.00")],
        &[("102.00", "1.00")],
    );

    for frame in [&first, &second] {
        let published = Published::collect(session.handle_raw_frame(frame, ts_init()));
        assert_eq!(published.deltas.len(), 1);
        let batch = &published.deltas[0];
        assert_eq!(
            batch.sequence, NO_EXCHANGE_SEQUENCE,
            "no local counter is presented as an exchange sequence"
        );
        for delta in &batch.deltas {
            assert_eq!(delta.sequence, NO_EXCHANGE_SEQUENCE);
        }
    }

    let state = session
        .book_state(&instrument.id())
        .expect("the book exists");
    assert_eq!(
        state.recv_seq(),
        2,
        "the local receive counter is local bookkeeping"
    );
}

// ------------------------------------------------------------------------------------------------
// Ordering and duplication
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_an_older_item_time_cannot_overwrite_newer_state() {
    let instrument = instrument(NVDA_MARKET);
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument.id();

    let newer = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:05Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    );
    Published::collect(session.handle_raw_frame(&newer, ts_init()));

    let older = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:04Z",
        &[("50.00", "1.00")],
        &[("51.00", "1.00")],
    );
    let published = Published::collect(session.handle_raw_frame(&older, ts_init()));

    assert_eq!(
        published.market_data(),
        0,
        "an older frame publishes nothing"
    );
    let state = session.book_state(&instrument_id).expect("the book exists");
    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "100.00"
    );
    assert_eq!(state.counters().stale_rejected, 1);
}

#[rstest]
fn test_an_identical_snapshot_at_the_same_time_is_suppressed() {
    let instrument = instrument(NVDA_MARKET);
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument.id();
    let frame = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    );

    Published::collect(session.handle_raw_frame(&frame, ts_init()));
    let repeat = Published::collect(session.handle_raw_frame(&frame, ts_init()));

    assert_eq!(
        repeat.market_data(),
        0,
        "an identical repeat publishes nothing"
    );
    let counters = session
        .book_counters(&instrument_id)
        .expect("the book exists");
    assert_eq!(counters.accepted, 1);
    assert_eq!(counters.duplicates, 1);
    assert_eq!(
        session
            .book_state(&instrument_id)
            .expect("the book exists")
            .recv_seq(),
        2,
        "the repeated frame was still received in order"
    );
}

#[rstest]
fn test_different_content_at_the_same_time_is_processed_in_receive_order() {
    let instrument = instrument(NVDA_MARKET);
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument.id();

    let first = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    );
    let conflicting = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("99.00", "1.00")],
        &[("102.00", "1.00")],
    );

    Published::collect(session.handle_raw_frame(&first, ts_init()));
    let published = Published::collect(session.handle_raw_frame(&conflicting, ts_init()));

    assert_eq!(
        published.market_data(),
        1,
        "equal times with different content do not drop the later frame"
    );
    let counters = session
        .book_counters(&instrument_id)
        .expect("the book exists");
    assert_eq!(counters.accepted, 2);
    assert_eq!(counters.duplicates, 0);
    assert_eq!(counters.conflicts, 1);
    assert_eq!(
        session
            .book_state(&instrument_id)
            .expect("the book exists")
            .book()
            .best_bid()
            .expect("a bid")
            .0
            .to_string(),
        "99.00",
        "the later frame is applied in receive order"
    );
}

// ------------------------------------------------------------------------------------------------
// Session guard and reconnect readiness
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_late_callback_from_the_previous_session_cannot_mutate_the_new_book() {
    let instrument = instrument(NVDA_MARKET);
    let mut state = OndoBookState::new(1);

    let first = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:00Z",
            &[("100.00", "1.00")],
            &[("101.00", "1.00")],
        ),
        &instrument,
    );
    state
        .apply_snapshot(1, ts("2026-09-14T11:09:00Z"), &first)
        .expect("the snapshot applies");
    assert!(state.is_valid());

    assert!(state.invalidate_to(2, REASON_DISCONNECTED));
    assert_eq!(state.session_id(), 2);
    assert!(!state.is_valid());
    assert_eq!(state.invalid_reason(), Some(REASON_DISCONNECTED));

    // A callback that arrives late still carries the session it was received on.
    let late = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:09:30Z",
            &[("500.00", "1.00")],
            &[("501.00", "1.00")],
        ),
        &instrument,
    );
    let outcome = state
        .apply_snapshot(1, ts("2026-09-14T11:09:30Z"), &late)
        .expect("a foreign frame is reported, not an error");

    assert_eq!(
        outcome,
        SnapshotOutcome::ForeignSession {
            expected: 2,
            received: 1,
        }
    );
    assert!(!state.is_valid());
    assert!(state.book().is_empty());
    assert_eq!(state.counters().foreign_session, 1);

    // Only a snapshot accepted on the current session restores a book.
    let recovered = parsed_snapshot_of(
        &book_frame(
            NVDA_MARKET,
            "2026-09-14T11:10:00Z",
            &[("98.00", "3.00")],
            &[("102.00", "4.00")],
        ),
        &instrument,
    );
    assert_eq!(
        state
            .apply_snapshot(2, ts("2026-09-14T11:10:00Z"), &recovered)
            .expect("the current session's snapshot applies"),
        SnapshotOutcome::Accepted { conflict: false }
    );
    assert!(state.is_valid());
}

#[rstest]
fn test_a_disconnect_makes_the_book_unusable_and_only_a_new_snapshot_restores_it() {
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument_id(NVDA_MARKET);
    session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded");
    Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, ts_init()));
    assert!(
        session
            .book_state(&instrument_id)
            .expect("a book")
            .is_valid()
    );

    let disconnected = Published::collect(session.invalidate_all(REASON_DISCONNECTED, ts_init()));

    assert_eq!(session.session_id(), 2);
    assert_eq!(disconnected.statuses.len(), 1);
    {
        let state = session.book_state(&instrument_id).expect("a book");
        assert!(!state.is_valid());
        assert!(state.book().is_empty());
        assert_eq!(state.invalid_reason(), Some(REASON_DISCONNECTED));
    }

    // A new socket replays the subscription intent. That is not a snapshot, and it does not make the
    // book usable again on its own.
    let replayed = session.replay_requests();
    assert_eq!(replayed.len(), 1);
    assert!(replayed[0].contains("depthBooksPerps"));
    assert!(
        !session
            .book_state(&instrument_id)
            .expect("a book")
            .is_valid(),
        "reconnecting is not readiness"
    );

    let recovered = Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, ts_init()));

    assert_eq!(recovered.deltas.len(), 1);
    assert_eq!(recovered.statuses.len(), 1);
    assert!(
        session
            .book_state(&instrument_id)
            .expect("a book")
            .is_valid()
    );
    assert_eq!(session.counters().snapshot_ready_published, 2);
}

#[rstest]
fn test_the_disconnect_and_ready_signals_are_documented_local_feed_states() {
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument_id(NVDA_MARKET);
    session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded");

    let ready = Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, ts_init()));
    let status = &ready.statuses[0];
    assert_eq!(status.instrument_id, instrument_id);
    assert_eq!(status.reason, Some(Ustr::from(REASON_SNAPSHOT_READY)));
    assert_eq!(
        status.action,
        MarketStatusAction::None,
        "a local feed state never claims an exchange status change"
    );
    assert_eq!(status.trading_event, None);
    assert_eq!(status.is_quoting, Some(true));
    assert_eq!(
        status.is_trading, None,
        "the adapter does not describe venue trading"
    );
    assert_eq!(status.ts_event, ts_init());

    let disconnected = Published::collect(session.invalidate_all(REASON_DISCONNECTED, ts_init()));
    let status = &disconnected.statuses[0];
    assert_eq!(status.reason, Some(Ustr::from(REASON_DISCONNECTED)));
    assert_eq!(status.action, MarketStatusAction::None);
    assert_ne!(status.action, MarketStatusAction::Halt);
    assert_eq!(status.trading_event, None);
    assert_eq!(status.is_quoting, Some(false));
}

// ------------------------------------------------------------------------------------------------
// Mapping to Nautilus events
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_quote_and_trade_messages_map_to_their_nautilus_events() {
    let mut session = session_for(&[NVDA_MARKET]);

    let quotes = Published::collect(session.handle_raw_frame(TOP_OF_BOOK_FIXTURE, ts_init()));
    assert_eq!(quotes.quotes.len(), 1);
    let quote = &quotes.quotes[0];
    assert_eq!(quote.instrument_id.to_string(), NVDA_INSTRUMENT);
    assert_eq!(quote.bid_price.to_string(), "212.22");
    assert_eq!(quote.ask_price.to_string(), "212.25");
    assert_eq!(quote.bid_size.to_string(), "4.70");
    assert_eq!(quote.ask_size.to_string(), "4.70");

    let trades = Published::collect(session.handle_raw_frame(TRADES_FIXTURE, ts_init()));
    assert_eq!(trades.trades.len(), 1);
    let trade = &trades.trades[0];
    assert_eq!(trade.instrument_id.to_string(), NVDA_INSTRUMENT);
    assert_eq!(trade.price.to_string(), "212.22");
    assert_eq!(trade.size.to_string(), "0.19");
    assert_eq!(trade.aggressor_side, AggressorSide::Sell);
}

#[rstest]
fn test_item_time_becomes_ts_event_while_the_envelope_and_local_times_stay_separate() {
    let mut session = session_for(&[NVDA_MARKET]);
    let local = ts_init();

    let quotes = Published::collect(session.handle_raw_frame(TOP_OF_BOOK_FIXTURE, local));
    let quote = &quotes.quotes[0];
    assert_eq!(
        quote.ts_event,
        ts("2026-09-14T11:09:58.8461101Z"),
        "the item `time` is the price event time"
    );
    assert_ne!(quote.ts_event, ts(ENVELOPE_TIMESTAMP));
    assert_eq!(
        quote.ts_init, local,
        "the local receive time is supplied by the transport"
    );
    assert_ne!(quote.ts_event, quote.ts_init);

    let trades = Published::collect(session.handle_raw_frame(TRADES_FIXTURE, local));
    let trade = &trades.trades[0];
    assert_eq!(trade.ts_event, ts("2026-09-14T11:10:04.382125561Z"));
    assert_ne!(
        trade.ts_event,
        ts("2026-09-14T11:10:04.386125572Z"),
        "the envelope send time of the trade frame is not its event time"
    );
    assert_eq!(trade.ts_init, local);

    let books = Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, local));
    let batch = &books.deltas[0];
    assert_eq!(batch.ts_event, ts("2026-09-14T11:09:59.570112122Z"));
    assert_eq!(batch.ts_init, local);
    assert_ne!(
        batch.ts_event,
        ts(ENVELOPE_TIMESTAMP),
        "a book item's event time never comes from the envelope"
    );
    assert!(batch.deltas.iter().all(|delta| delta.ts_init == local));
}

#[rstest]
fn test_funding_keeps_the_hourly_rate_and_maps_interval_ends_to_next_funding_ns() {
    let instrument = instrument(NVDA_MARKET);
    let mut session = session_for(&[NVDA_MARKET]);

    let published = Published::collect(session.handle_raw_frame(FUNDING_FIXTURE, ts_init()));
    assert_eq!(published.funding.len(), 1);
    let update = &published.funding[0];

    // `rate` is an hourly decimal fraction, passed through unchanged: never /100, never x8.
    assert_eq!(update.rate.to_string(), "0.0000063");
    assert_eq!(
        update.rate,
        parse_decimal("0.0000063", "rate").expect("exact")
    );
    assert_ne!(
        update.rate,
        parse_decimal("0.000000063", "rate").expect("exact")
    );
    assert_ne!(
        update.rate,
        parse_decimal("0.0000504", "rate").expect("exact")
    );
    assert_eq!(update.interval, None);
    assert_eq!(
        update.next_funding_ns,
        Some(ts("2026-09-14T12:00:00Z")),
        "`intervalEnds` is a settlement time"
    );
    assert_eq!(
        update.ts_event,
        ts(ENVELOPE_TIMESTAMP),
        "the event time is the envelope send time"
    );
    assert_ne!(
        update.ts_event,
        ts("2026-09-14T12:00:00Z"),
        "`intervalEnds` is never the funding event time"
    );
    assert_eq!(update.ts_init, ts_init());

    // The premium samples stay raw: their lexemes are wider than `Decimal` holds, so they are
    // counted and preserved rather than converted.
    let message = parse_server_message(FUNDING_FIXTURE).expect("the funding frame decodes");
    assert_eq!(message.kind, WsMessageType::Update);
    assert_eq!(message.timestamp, Some(ts(ENVELOPE_TIMESTAMP)));
    let updates = decode_updates(
        WsChannel::FundingRatesPerps,
        message.data.as_ref().expect("the frame carries data"),
    )
    .expect("the funding channel decodes");
    let WsUpdate::FundingRate(item) = updates.into_iter().next().expect("one item") else {
        panic!("the funding channel decodes to funding items");
    };
    let parsed = parse_funding_rate(&item, &instrument, message.timestamp, ts_init())
        .expect("the funding rate maps");
    assert_eq!(parsed.premium_samples, 10);
    assert_eq!(parsed.event_time_source, EventTimeSource::EnvelopeTimestamp);
    assert_eq!(parsed.update.rate.to_string(), "0.0000063");
}

#[rstest]
fn test_the_mark_price_example_maps_by_market_and_falls_back_to_a_later_time() {
    let instrument = instrument(NVDA_MARKET);

    // The official example names AAPL-USD.P, which this instrument set does not load; the item is
    // still decoded by market rather than by position.
    let message = parse_server_message(MARK_PRICES_FIXTURE).expect("the example decodes");
    let updates = decode_updates(
        WsChannel::MarkPricesPerps,
        message.data.as_ref().expect("the example carries data"),
    )
    .expect("the mark price channel decodes");
    let WsUpdate::MarkPrice(example) = updates.into_iter().next().expect("one item") else {
        panic!("the mark price channel decodes to mark price items");
    };
    assert_eq!(example.market, "AAPL-USD.P");
    assert_eq!(example.mark_price, "227.50");

    // The example carries neither an item `time` nor an envelope `timestamp`, so the event time is
    // the local receive time and the source says so.
    assert_eq!(message.timestamp, None);
    let parsed =
        parse_mark_price(&example, &instrument, message.timestamp, ts_init()).expect("it maps");
    assert_eq!(parsed.event_time_source, EventTimeSource::LocalReceive);
    assert_eq!(parsed.update.value.to_string(), "227.50");
    assert_eq!(parsed.update.ts_event, ts_init());
    assert_eq!(parsed.update.ts_init, ts_init());

    // With an item `time`, that is the event time.
    let mut with_time = example.clone();
    with_time.time = Some("2026-09-14T11:10:00.500000000Z".to_string());
    let parsed = parse_mark_price(
        &with_time,
        &instrument,
        Some(ts(ENVELOPE_TIMESTAMP)),
        ts_init(),
    )
    .expect("it maps");
    assert_eq!(parsed.event_time_source, EventTimeSource::ItemTime);
    assert_eq!(parsed.update.ts_event, ts("2026-09-14T11:10:00.500000000Z"));

    // Without an item time the envelope send time is used, and is reported as such.
    let mut without_time = example;
    without_time.time = None;
    let parsed = parse_mark_price(
        &without_time,
        &instrument,
        Some(ts(ENVELOPE_TIMESTAMP)),
        ts_init(),
    )
    .expect("it maps");
    assert_eq!(parsed.event_time_source, EventTimeSource::EnvelopeTimestamp);
    assert_eq!(parsed.update.ts_event, ts(ENVELOPE_TIMESTAMP));

    // Through the session, the channel publishes a Nautilus mark price for a loaded market.
    let mut session = session_for(&[NVDA_MARKET]);
    let frame = format!(
        r#"{{"type":"update","channel":"markPricesPerps","timestamp":"{ENVELOPE_TIMESTAMP}","data":[{{"market":"NVDA-USD.P","markPrice":"227.50"}}]}}"#
    );
    let published = Published::collect(session.handle_raw_frame(&frame, ts_init()));
    assert_eq!(published.marks.len(), 1);
    assert_eq!(published.marks[0].value.to_string(), "227.50");
    assert_eq!(published.marks[0].ts_event, ts(ENVELOPE_TIMESTAMP));
}

#[rstest]
fn test_a_one_sided_top_of_book_is_ignored_rather_than_zero_filled() {
    let mut session = session_for(&[NVDA_MARKET]);
    let frame = format!(
        r#"{{"type":"update","channel":"topOfBooksPerps","timestamp":"{ENVELOPE_TIMESTAMP}","data":[{{"market":"NVDA-USD.P","time":"2026-09-14T11:09:00Z","bids":[["100.00","1.00"]],"asks":[]}}]}}"#
    );
    let published = Published::collect(session.handle_raw_frame(&frame, ts_init()));

    assert_eq!(published.market_data(), 0, "a quote needs both sides");
    assert_eq!(published.ignored.len(), 1);
    assert!(published.ignored[0].contains("one-sided"));
    assert_eq!(session.counters().skipped_items, 1);
}

#[rstest]
fn test_the_mark_price_fixture_is_an_official_example_per_the_manifest() {
    let manifest: serde_json::Value = serde_json::from_str(MANIFEST).expect("the manifest is JSON");
    let fixtures = manifest["fixtures"].as_array().expect("a fixtures array");

    let kind_of = |path: &str| {
        fixtures
            .iter()
            .find(|fixture| fixture["path"] == path)
            .unwrap_or_else(|| panic!("`{path}` is indexed in the manifest"))["kind"]
            .as_str()
            .expect("a kind string")
            .to_string()
    };

    // The file name says `observed`; the manifest says otherwise, and the manifest is the authority.
    assert_eq!(kind_of("ws/markprices_observed.json"), "official-example");
    assert_eq!(kind_of("ws/depth_observed.json"), "observed");
    assert_eq!(kind_of("ws/topofbook_observed.json"), "observed");
    assert_eq!(kind_of("ws/trades_observed.json"), "observed");
    assert_eq!(kind_of("ws/funding_observed.json"), "observed");
    assert_eq!(kind_of("ws/subscribe_observed.json"), "observed");
}

// ------------------------------------------------------------------------------------------------
// Decimal width: exact, or explicitly rounded, never silently rounded
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_a_lexeme_wider_than_decimal_is_rejected_and_never_rounded() {
    let message = parse_server_message(FUNDING_FIXTURE).expect("the funding frame decodes");
    let updates = decode_updates(
        WsChannel::FundingRatesPerps,
        message.data.as_ref().expect("the frame carries data"),
    )
    .expect("the funding channel decodes");
    let WsUpdate::FundingRate(item) = &updates[0] else {
        panic!("the funding channel decodes to funding items");
    };

    let wide_bid = item.premiums[1]
        .bid
        .clone()
        .expect("the second premium sample carries a bid");
    let wide_premium = item.premiums[0]
        .premium_index
        .clone()
        .expect("the first premium sample carries a premium index");

    // The lexemes are preserved verbatim in the item...
    assert_eq!(wide_bid, "212.22994961526383480047124429696391461");
    assert_eq!(
        wide_premium,
        "-0.000023702373955322580120738856244065276621"
    );

    // ...and a conversion refuses them instead of rounding them away.
    assert!(parse_decimal(&wide_bid, "bid").is_err());
    assert!(parse_decimal(&wide_premium, "premiumIndex").is_err());
    assert!(convert_price(&wide_bid, "book price", 2).is_err());
    assert!(
        convert_price("1e-3", "book price", 2).is_err(),
        "exponent notation is not an exact lexeme"
    );
}

#[rstest]
fn test_a_rounded_conversion_is_explicit_and_counted() {
    let exact = convert_price("212.25", "book price", 2).expect("exact");
    assert!(!exact.rounded);
    assert_eq!(exact.wire, "212.25");

    let rounded = convert_price("212.255", "book price", 2).expect("representable at precision 2");
    assert!(rounded.rounded, "a reduced-scale conversion is reported");
    assert_eq!(rounded.value.to_string(), "212.26");
    assert_eq!(rounded.wire, "212.255", "the wire lexeme is kept verbatim");

    // The same rule inside the book state machine, where the rounding is counted per book.
    let instrument = instrument(NVDA_MARKET);
    let fine = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:09:00Z",
        &[("100.005", "1.00")],
        &[("101.00", "1.00")],
    );
    let parsed = parsed_snapshot_of(&fine, &instrument);
    assert_eq!(parsed.rounded_levels, 1);

    let mut state = OndoBookState::new(1);
    state
        .apply_snapshot(1, ts("2026-09-14T11:09:00Z"), &parsed)
        .expect("the snapshot applies");

    assert_eq!(
        state.book().best_bid().expect("a bid").0.to_string(),
        "100.01",
        "the level is rounded to the declared precision, never truncated"
    );
    assert_eq!(state.counters().rounded_levels, 1);
}

// ------------------------------------------------------------------------------------------------
// One connection and its reference counts
// ------------------------------------------------------------------------------------------------

#[rstest]
fn test_the_book_channel_reference_counting_covers_quote_depth10_and_deltas() {
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument_id(NVDA_MARKET);

    // The quote channel is a subscription of its own and shares nothing with the book channel.
    assert!(
        session
            .subscribe(WsChannel::TopOfBooksPerps, &[instrument_id])
            .expect("NVDA is loaded")
            .is_some()
    );
    assert_eq!(
        session.reference_count(WsChannel::TopOfBooksPerps, &instrument_id),
        1
    );
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
        0
    );

    // Two local subscribers of the book deltas produce one request and a count of two.
    let first = session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded");
    assert!(first.is_some());
    let second = session
        .subscribe(WsChannel::DepthBooksPerps, &[instrument_id])
        .expect("NVDA is loaded");
    assert!(second.is_none(), "the venue is not asked twice");
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
        2
    );

    // An on-demand depth10 rides the same topic instead of opening its own subscription.
    assert!(
        session
            .subscribe_depth10(&instrument_id)
            .expect("NVDA is loaded")
            .is_none()
    );
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
        3
    );

    // Dropping one subscriber does not unsubscribe the others.
    assert!(
        session
            .unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .expect("NVDA is loaded")
            .is_none()
    );
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
        2
    );
    assert!(
        session
            .unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id])
            .expect("NVDA is loaded")
            .is_none(),
        "the depth10 projection still holds a reference"
    );

    // The last reference going away is what sends the unsubscribe, and the quote subscription is
    // untouched by the book channel's teardown.
    let last = session
        .unsubscribe_depth10(&instrument_id)
        .expect("NVDA is loaded");
    assert_eq!(
        last.as_deref(),
        Some(r#"{"op":"unsubscribe","channel":"depthBooksPerps","markets":["NVDA-USD.P"]}"#)
    );
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &instrument_id),
        0
    );
    assert_eq!(
        session.reference_count(WsChannel::TopOfBooksPerps, &instrument_id),
        1
    );
}

#[rstest]
fn test_an_unsubscribed_market_is_never_resubscribed() {
    let mut session = session_for(&[NVDA_MARKET, TSLA_MARKET]);
    let nvda = instrument_id(NVDA_MARKET);
    let tsla = instrument_id(TSLA_MARKET);

    let request = session
        .subscribe(WsChannel::DepthBooksPerps, &[nvda, tsla])
        .expect("both markets are loaded")
        .expect("a new topic is subscribed");
    Published::collect(session.handle_raw_frame(&ack("subscribed", &request), ts_init()));
    assert_eq!(session.confirmed_topics().len(), 2);

    let unsubscribe = session
        .unsubscribe(WsChannel::DepthBooksPerps, &[nvda])
        .expect("NVDA is loaded")
        .expect("the last reference to NVDA went away");
    assert!(unsubscribe.contains(NVDA_MARKET));
    assert!(!unsubscribe.contains(TSLA_MARKET));
    Published::collect(session.handle_raw_frame(&ack("unsubscribed", &unsubscribe), ts_init()));

    let replayed = session.replay_requests();

    assert_eq!(replayed.len(), 1);
    assert!(replayed[0].contains(TSLA_MARKET));
    assert!(
        !replayed[0].contains(NVDA_MARKET),
        "a removed subscription is never replayed"
    );

    // Replaying again does not resurrect it either.
    let replayed = session.replay_requests();
    assert_eq!(replayed.len(), 1);
    assert!(!replayed[0].contains(NVDA_MARKET));
    assert_eq!(
        session.reference_count(WsChannel::DepthBooksPerps, &nvda),
        0
    );
}

#[rstest]
fn test_the_depth10_projection_is_taken_from_the_same_book_subscription() {
    let mut session = session_for(&[NVDA_MARKET]);
    let instrument_id = instrument_id(NVDA_MARKET);
    assert!(
        session
            .subscribe_depth10(&instrument_id)
            .expect("NVDA is loaded")
            .is_some(),
        "the depth10 opens the book subscription it projects from"
    );

    let published = Published::collect(session.handle_raw_frame(DEPTH_FIXTURE, ts_init()));

    assert_eq!(published.deltas.len(), 1);
    assert_eq!(published.depth10.len(), 1);
    let depth = &published.depth10[0];
    assert_eq!(depth.instrument_id, instrument_id);
    assert_eq!(depth.bids[0].price.to_string(), "212.22");
    assert_eq!(depth.asks[0].price.to_string(), "212.25");
    assert_eq!(depth.bids[9].price.to_string(), "212.09");
    assert_eq!(depth.bid_counts[9], 1);
    assert_eq!(depth.ask_counts[9], 1);
    assert_eq!(depth.sequence, NO_EXCHANGE_SEQUENCE);
    // The projection is the same covered range the delta batch carries.
    assert_eq!(published.deltas[0].deltas.len(), 21);
}

// ------------------------------------------------------------------------------------------------
// The public transport: one connection, no credentials
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_public_connection_needs_no_credentials() {
    let server = MockServer::start().await;
    let (mut client, _outcomes) = OndoWebSocketClient::new(server.url.clone(), 20, 10, None)
        .expect("the public client is created without any credential");

    assert_eq!(client.url(), server.url.as_str());
    wait_until(|| client.is_connected(), "the public socket to connect").await;

    let bodies: Vec<String> = WsChannel::ALL
        .iter()
        .map(|channel| subscribe_body(*channel, &[NVDA_MARKET, TSLA_MARKET]))
        .collect();

    for body in &bodies {
        client.send(body.clone());
    }

    let expected = bodies.len();
    wait_until(
        || server.bodies().len() == expected,
        "every subscription request to reach the mock endpoint",
    )
    .await;

    let sent = server.bodies();
    for body in &bodies {
        assert!(sent.contains(body), "the venue received `{body}`");
    }

    // The public data client has no login path and no key material, so nothing credential-shaped is
    // ever put on the wire.
    for body in &sent {
        let lowered = body.to_lowercase();
        for needle in ["login", "key", "sign", "secret", "token", "auth"] {
            assert!(!lowered.contains(needle), "`{body}` carries `{needle}`");
        }
    }

    assert_eq!(server.connection_count(), 1);
    client.stop().await;
}

#[tokio::test]
async fn test_several_subscriptions_share_one_connection() {
    let server = MockServer::start().await;
    let (mut client, _outcomes) = OndoWebSocketClient::new(server.url.clone(), 20, 10, None)
        .expect("the public client is created without any credential");
    wait_until(|| client.is_connected(), "the socket to connect").await;

    let book = subscribe_body(WsChannel::DepthBooksPerps, &[NVDA_MARKET, TSLA_MARKET]);
    let quote = subscribe_body(WsChannel::TopOfBooksPerps, &[NVDA_MARKET, TSLA_MARKET]);
    let trades = subscribe_body(WsChannel::TradesPerps, &[NVDA_MARKET, TSLA_MARKET]);
    let funding = subscribe_body(WsChannel::FundingRatesPerps, &[NVDA_MARKET, TSLA_MARKET]);
    let marks = subscribe_body(WsChannel::MarkPricesPerps, &[NVDA_MARKET, TSLA_MARKET]);

    // Five channels plus a second local subscriber of the book channel, all on one client.
    let bodies = vec![
        book.clone(),
        quote.clone(),
        book.clone(),
        trades.clone(),
        funding.clone(),
        marks.clone(),
    ];
    for body in &bodies {
        client.send(body.clone());
    }

    let expected = bodies.len();
    wait_until(
        || server.bodies().len() == expected,
        "every subscription request to reach the mock endpoint",
    )
    .await;

    let sent = server.bodies();
    assert_eq!(
        server.connection_count(),
        1,
        "several subscriptions are served by one connection"
    );
    assert_eq!(
        sent.iter().filter(|body| **body == book).count(),
        2,
        "the second book subscriber reuses the first subscription"
    );
    for body in [&quote, &trades, &funding, &marks] {
        assert!(sent.contains(body), "the venue received `{body}`");
    }

    client.stop().await;
}

// ------------------------------------------------------------------------------------------------
// The registration seam: what the data client asks the transport's session to do
// ------------------------------------------------------------------------------------------------

/// A mock endpoint the test drives: it records every frame the client sends, can push a frame back,
/// and can close the connection on demand.
///
/// The `OndoWsSession` lives inside the transport task, so this is the only way a test can make a
/// real socket disconnect happen and observe what the client publishes because of it.
struct MockFeed {
    url: String,
    connections: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<String>>>,
    outgoing: Arc<Mutex<Vec<tokio::sync::mpsc::UnboundedSender<Message>>>>,
}

impl MockFeed {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a mock endpoint binds on the loopback interface");
        let url = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("the mock endpoint has an address")
        );
        let connections = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let outgoing: Arc<Mutex<Vec<tokio::sync::mpsc::UnboundedSender<Message>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let accepted = Arc::clone(&connections);
        let recorded = Arc::clone(&bodies);
        let sinks = Arc::clone(&outgoing);

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);

                let recorded = Arc::clone(&recorded);
                let sinks = Arc::clone(&sinks);
                tokio::spawn(async move {
                    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let (mut sink, mut source) = socket.split();
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
                    sinks
                        .lock()
                        .expect("the mock sink log is not poisoned")
                        .push(tx);

                    loop {
                        tokio::select! {
                            outbound = rx.recv() => {
                                match outbound {
                                    // Dropping the test's sender is this task's close signal: the
                                    // socket is dropped and the client sees the stream end.
                                    Some(message) => {
                                        if sink.send(message).await.is_err() {
                                            return;
                                        }
                                    }
                                    None => return,
                                }
                            }
                            inbound = source.next() => {
                                match inbound {
                                    Some(Ok(Message::Text(text))) => recorded
                                        .lock()
                                        .expect("the mock body log is not poisoned")
                                        .push(text.to_string()),
                                    Some(Ok(_)) => {}
                                    Some(Err(_)) | None => return,
                                }
                            }
                        }
                    }
                });
            }
        });

        Self {
            url,
            connections,
            bodies,
            outgoing,
        }
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies
            .lock()
            .expect("the mock body log is not poisoned")
            .clone()
    }

    /// Pushes one frame to the client on the most recent connection.
    fn push(&self, frame: &str) {
        let sinks = self
            .outgoing
            .lock()
            .expect("the mock sink log is not poisoned");
        let sender = sinks.last().expect("a connection is open");

        sender
            .send(Message::Text(frame.into()))
            .expect("the client is reading");
    }

    /// Closes the most recent connection, which is what a venue-side disconnect looks like.
    fn disconnect(&self) {
        let dropped = self
            .outgoing
            .lock()
            .expect("the mock sink log is not poisoned")
            .pop();

        assert!(dropped.is_some(), "a connection was open to close");
    }
}

/// Collects outcomes until `predicate` holds or five seconds pass.
///
/// The collector is what makes these tests able to fail: a missing publication leaves the predicate
/// false and the deadline ends the wait, so the assertion that follows sees the absence.
async fn outcomes_until(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsOutcome>,
    predicate: impl Fn(&Published) -> bool,
) -> Published {
    let mut published = Published::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    while !predicate(&published) {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(outcome)) => published.push(outcome),
            Ok(None) | Err(_) => break,
        }
    }

    published
}

async fn feed_connection(feed: &MockFeed, expected: usize) {
    wait_until(
        || feed.connection_count() >= expected,
        "the transport to (re)connect to the mock endpoint",
    )
    .await;
}

async fn feed_request(feed: &MockFeed, expected: usize) {
    wait_until(
        || feed.bodies().len() >= expected,
        "the subscription request to reach the mock endpoint",
    )
    .await;
}

#[tokio::test]
async fn test_the_session_command_seam_registers_routes_and_publishes_the_disconnect() {
    let feed = MockFeed::start().await;
    let (mut client, mut outcomes) = OndoWebSocketClient::new(feed.url.clone(), 20, 100, None)
        .expect("a runtime hosts the transport");

    // The seam: the data client registers the instruments it loaded and subscribes for them. Both
    // are queued before the socket exists, which is the case that used to leave the session without
    // any instrument and the disconnect without any instrument to name.
    let instrument = instrument(NVDA_MARKET);
    let instrument_id = instrument.id();
    client.register_instrument(&instrument);
    client.subscribe(WsChannel::DepthBooksPerps, &[instrument_id]);

    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    let bodies = feed.bodies();
    assert_eq!(bodies.len(), 1, "one subscription, sent once");
    assert!(bodies[0].contains("\"channel\":\"depthBooksPerps\""));
    assert!(bodies[0].contains(NVDA_MARKET));
    assert!(bodies[0].contains("\"limit\":100"));

    // A frame for the registered market routes to the instrument: with no registration it would be
    // reported as an unloaded market and no delta would ever be published.
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    ));
    let published = outcomes_until(&mut outcomes, |published| {
        !published.deltas.is_empty()
            && published
                .status_with_reason(REASON_SNAPSHOT_READY)
                .is_some()
    })
    .await;
    assert_eq!(published.deltas.len(), 1);
    assert_eq!(published.deltas[0].instrument_id, instrument_id);
    assert!(published.unsupported.is_empty(), "the market is loaded");
    assert_eq!(
        published
            .status_with_reason(REASON_SNAPSHOT_READY)
            .expect("a first snapshot makes the book usable")
            .is_quoting,
        Some(true)
    );

    // The venue closes the socket: the session ends it and the client publishes the local feed
    // state for the instrument it has a subscription for.
    feed.disconnect();
    let published = outcomes_until(&mut outcomes, |published| !published.statuses.is_empty()).await;
    let disconnected = published
        .status_with_reason(REASON_DISCONNECTED)
        .expect("the disconnect reaches the client as an event, not as a flag");
    assert_eq!(disconnected.instrument_id, instrument_id);
    assert_eq!(disconnected.is_quoting, Some(false));
    assert_eq!(disconnected.is_trading, None);

    client.stop().await;
}

#[tokio::test]
async fn test_the_seam_subscribes_once_per_topic_and_unsubscribes_on_the_last_reference() {
    let feed = MockFeed::start().await;
    let (mut client, _outcomes) =
        OndoWebSocketClient::new(feed.url.clone(), 20, 100, None).expect("a runtime");

    let instrument = instrument(NVDA_MARKET);
    let instrument_id = instrument.id();
    client.register_instrument(&instrument);

    // The quote channel is a subscription of its own; the book channel is shared by the deltas and
    // the status feed state, both of which need the book to be usable.
    client.subscribe(WsChannel::DepthBooksPerps, &[instrument_id]);
    client.subscribe(WsChannel::DepthBooksPerps, &[instrument_id]);
    client.subscribe(WsChannel::TopOfBooksPerps, &[instrument_id]);

    feed_connection(&feed, 1).await;
    feed_request(&feed, 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let bodies = feed.bodies();
    assert_eq!(
        bodies
            .iter()
            .filter(|body| body.contains("\"channel\":\"depthBooksPerps\""))
            .count(),
        1,
        "the book topic is asked for once, however many local subscribers use it"
    );
    assert_eq!(
        bodies
            .iter()
            .filter(|body| body.contains("\"channel\":\"topOfBooksPerps\""))
            .count(),
        1
    );

    // One reference going away is not an unsubscribe; the last one is.
    client.unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id]);
    client.unsubscribe(WsChannel::DepthBooksPerps, &[instrument_id]);
    feed_request(&feed, 3).await;

    let bodies = feed.bodies();
    let unsubscribe = bodies
        .last()
        .expect("the unsubscribe request was sent")
        .clone();
    assert!(unsubscribe.contains("\"op\":\"unsubscribe\""));
    assert!(unsubscribe.contains("\"channel\":\"depthBooksPerps\""));
    assert!(unsubscribe.contains(NVDA_MARKET));

    client.stop().await;
}

// ------------------------------------------------------------------------------------------------
// The data client over a mock venue, and the invalidation chain in one process
// ------------------------------------------------------------------------------------------------

/// A mock REST endpoint inside the test process: every request is answered with one body.
///
/// The data client's metadata read is the only REST request it makes, so this is the entire venue
/// side of `GET /v1/markets` for a test. The request line is recorded so a test can assert which
/// request target was actually dialled.
struct MockRest {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockRest {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockRest {
    async fn start(body: String) -> Self {
        Self::start_with_bodies(vec![body]).await
    }

    /// Answers one body per request, in order, repeating the last one once the list is exhausted.
    ///
    /// A venue that changes its mind between two reads is a venue that answers differently, and the
    /// metadata refresh is a read like any other - so this is how a status change reaches the
    /// client without waiting a refresh interval for it.
    async fn start_with_bodies(bodies: Vec<String>) -> Self {
        assert!(!bodies.is_empty(), "a mock endpoint answers something");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a mock endpoint binds on the loopback interface");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("the mock endpoint has an address")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let bodies = Arc::new(bodies);
        let served = Arc::new(AtomicUsize::new(0));

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body =
                    bodies[served.fetch_add(1, Ordering::SeqCst).min(bodies.len() - 1)].clone();
                let recorded = Arc::clone(&recorded);

                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut chunk = [0_u8; 1024];

                    // Read until the request head is complete. The body of a GET is never needed,
                    // and `Connection: close` ends the exchange after the one reply.
                    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => head.extend_from_slice(&chunk[..read]),
                        }
                    }

                    recorded
                        .lock()
                        .expect("the mock request log is not poisoned")
                        .push(String::from_utf8_lossy(&head).to_string());

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        Self {
            url,
            requests,
            handle,
        }
    }

    /// The request line of every request the endpoint answered.
    fn targets(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("the mock request log is not poisoned")
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_string())
            .collect()
    }
}

/// The market metadata fixture carrying only its first trading pair, which is a venue that stopped
/// publishing the markets this adapter loads.
fn fixture_with_first_pair_only() -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
    let pairs = value["result"]["perps"]["tradingPairs"]
        .as_array()
        .expect("the fixture carries trading pairs");
    let first = pairs.first().expect("at least one pair").clone();
    value["result"]["perps"]["tradingPairs"] = serde_json::Value::Array(vec![first]);

    value.to_string()
}

/// The market metadata fixture with the first pair's maker fee changed, which is a venue whose view
/// of the tradable set is unchanged: a read of it is accepted as a version that replaced the last.
fn fixture_with_a_changed_maker_fee() -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
    value["result"]["perps"]["tradingPairs"][0]["makerFee"] =
        serde_json::Value::String("0.0002".to_string());

    value.to_string()
}

/// The market metadata fixture carrying no market at all.
fn fixture_with_no_pairs() -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
    value["result"]["perps"]["tradingPairs"] = serde_json::Value::Array(Vec::new());

    value.to_string()
}

/// The configuration of a data client pointed at the mock venue.
fn config_for(rest: &MockRest, feed: &MockFeed, load_ids: &[&str]) -> OndoDataClientConfig {
    OndoDataClientConfig::builder()
        .base_url_http(rest.url.clone())
        .base_url_ws(feed.url.clone())
        .load_ids(
            load_ids
                .iter()
                .map(|id| InstrumentId::from(*id))
                .collect::<Vec<_>>(),
        )
        .build()
}

/// Builds the client the way the process does: the data event channel first, the client second.
///
/// `OndoDataClient::new` reads the process's data event sender at construction, so the channel the
/// test holds the receiving end of is the one a running node's engine would own.
fn data_client(
    config: OndoDataClientConfig,
) -> (
    OndoDataClient,
    tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = OndoDataClient::new(ClientId::from("ONDO-TEST"), config, OndoRateBudget::new())
        .expect("the data client is built from the mock venue's configuration");

    (client, rx)
}

/// The instruments the client published, in the order it published them.
fn published_copies(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> Vec<InstrumentAny> {
    let mut published = Vec::new();

    while let Ok(event) = events.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            published.push(instrument);
        }
    }

    published
}

/// The instrument ids the client published, in the order it published them.
fn published_instruments(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> Vec<String> {
    published_copies(events)
        .iter()
        .map(|instrument| instrument.id().to_string())
        .collect()
}

/// The metadata version a consumer reads off an instrument it was handed.
///
/// The key is spelled out rather than taken from the adapter's own constant: this is the name the
/// application reads, and a test that imported it would follow a rename instead of failing on one.
fn metadata_version_of(instrument: &InstrumentAny) -> Option<u64> {
    match instrument {
        InstrumentAny::CryptoPerpetual(perp) => perp
            .info
            .as_ref()
            .and_then(|info| info.get_u64("metadata_version")),
        _ => None,
    }
}

/// Forwards the client's events to the data engine until `predicate` holds or the deadline passes.
///
/// This is the live runner's job in a running node, and it is what makes the message-bus subscriber
/// below a real one: nothing here asks the client what it published. Only the local feed states are
/// replayed into the engine - the path this test is about - while every published market data value
/// is collected as it arrived so its shape can be asserted. Returns whether `predicate` held.
async fn drive_statuses<F: Fn() -> bool>(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    engine: &mut DataEngine,
    published: &mut Vec<Data>,
    deadline: tokio::time::Instant,
    predicate: F,
) -> bool {
    while !predicate() {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(DataEvent::Data(data))) => {
                if matches!(data, Data::InstrumentStatus(_)) {
                    engine.process_data(data);
                } else {
                    published.push(data);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    predicate()
}

#[tokio::test]
async fn test_the_instrument_provider_publishes_exactly_the_requested_instruments() {
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));

    client
        .load_all(None)
        .await
        .expect("the requested market loads");

    assert_eq!(
        rest.targets(),
        vec!["GET /v1/markets HTTP/1.1".to_string()],
        "one metadata read, through the Stage B request target"
    );
    assert_eq!(
        client.store().count(),
        1,
        "the store holds exactly the market that was requested"
    );
    assert!(client.store().contains(&instrument_id(NVDA_MARKET)));
    assert_eq!(
        published_instruments(&mut events),
        vec![NVDA_INSTRUMENT.to_string()],
        "a non-empty load_ids set is a boundary, not a hint: the rest of the venue is never published"
    );

    // The venue's view of a market this client did not load is not this client's: `is_market_tradable`
    // may only answer for the set that was loaded and published, so it can never vouch for an
    // instrument no consumer of this client ever received.
    assert!(client.is_market_tradable(&instrument_id(NVDA_MARKET)));
    assert!(
        !client.is_market_tradable(&instrument_id(TSLA_MARKET)),
        "TSLA is `active` at the venue but this client never loaded or published it"
    );

    client.disconnect().await.expect("the client stops");
}

/// The metadata version an application records is the version of the instrument it was handed.
///
/// The version is not published as an event of its own: a consumer reads it off the instrument, so
/// both carriers have to carry it - the copy the provider's store answers a subscription with, and
/// the copy a read that replaced the accepted version republishes.
#[tokio::test]
async fn test_the_instrument_a_consumer_is_handed_names_the_metadata_version_it_belongs_to() {
    // The venue answers the first read with its opening metadata and the second with the same
    // markets at a different maker fee: a view of the tradable set that did not change, so the
    // second read is accepted as a version that replaced the last one.
    let rest = MockRest::start_with_bodies(vec![
        MARKETS_FIXTURE.to_string(),
        fixture_with_a_changed_maker_fee(),
    ])
    .await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));

    let instrument_id = instrument_id(NVDA_MARKET);
    client.load_all(None).await.expect("the market loads");

    let published = published_copies(&mut events);
    assert_eq!(
        published.len(),
        1,
        "one market was loaded, so one was published"
    );
    assert_eq!(
        metadata_version_of(&published[0]),
        Some(1),
        "the first accepted version names itself on the instrument it publishes"
    );

    // The application's own call: it subscribes to the market it trades, and is answered from the
    // provider's store rather than by the read that filled it.
    client
        .subscribe_instrument(SubscribeInstrument::new(
            instrument_id,
            Some(ClientId::from("ONDO-TEST")),
            None,
            UUID4::new(),
            ts_init(),
            None,
            None,
        ))
        .expect("the market this client loaded is published to the subscription");

    let answered = published_copies(&mut events);
    assert_eq!(
        answered.len(),
        1,
        "a subscription is answered with exactly the instrument it asked for"
    );
    assert_eq!(answered[0].id(), instrument_id);
    assert_eq!(
        metadata_version_of(&answered[0]),
        Some(1),
        "the store hands out the version it holds"
    );

    // The venue's metadata changes underneath the run: the read is accepted as a new version, and
    // the instrument republished with it is the new copy, named with the version it became.
    client
        .load_all(None)
        .await
        .expect("the venue's second answer is still market metadata");

    let republished = published_copies(&mut events);
    assert_eq!(
        republished.len(),
        1,
        "the version that replaced the accepted one republishes its markets"
    );
    assert_eq!(
        metadata_version_of(&republished[0]),
        Some(2),
        "the republished instrument names the version that replaced the accepted one"
    );
    let InstrumentAny::CryptoPerpetual(perpetual) = &republished[0] else {
        panic!("this venue publishes crypto perpetuals");
    };
    assert_eq!(
        perpetual.maker_fee.to_string(),
        "0.0002",
        "the republished copy is the read that was accepted, not the copy it replaced"
    );

    client.disconnect().await.expect("the client stops");
}

// ------------------------------------------------------------------------------------------------
// The book reference the data client takes, and the venue being told it went away
// ------------------------------------------------------------------------------------------------

/// The exact body the venue must receive when the last reference to a book topic goes away.
const BOOK_UNSUBSCRIBE: &str =
    r#"{"op":"unsubscribe","channel":"depthBooksPerps","markets":["NVDA-USD.P"]}"#;

/// The book-deltas subscription the data engine sends for one market.
fn subscribe_book_deltas(instrument_id: InstrumentId) -> SubscribeBookDeltas {
    SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(ClientId::from("ONDO-TEST")),
        None,
        UUID4::new(),
        ts_init(),
        None,
        false,
        None,
        None,
    )
}

/// The on-demand depth10 subscription the data engine sends for one market.
fn subscribe_book_depth10(instrument_id: InstrumentId) -> SubscribeBookDepth10 {
    SubscribeBookDepth10::new(
        instrument_id,
        BookType::L2_MBP,
        Some(ClientId::from("ONDO-TEST")),
        None,
        UUID4::new(),
        ts_init(),
        None,
        false,
        None,
        None,
    )
}

/// The matching book-deltas unsubscribe.
fn unsubscribe_book_deltas(instrument_id: InstrumentId) -> UnsubscribeBookDeltas {
    UnsubscribeBookDeltas::new(
        instrument_id,
        Some(ClientId::from("ONDO-TEST")),
        None,
        UUID4::new(),
        ts_init(),
        None,
        None,
    )
}

/// The matching on-demand depth10 unsubscribe.
fn unsubscribe_book_depth10(instrument_id: InstrumentId) -> UnsubscribeBookDepth10 {
    UnsubscribeBookDepth10::new(
        instrument_id,
        Some(ClientId::from("ONDO-TEST")),
        None,
        UUID4::new(),
        ts_init(),
        None,
        None,
    )
}

/// Collects the market data values the client publishes until `predicate` holds or five seconds pass.
async fn data_until<F: Fn(&[Data]) -> bool>(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    predicate: F,
) -> Vec<Data> {
    let mut published = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    while !predicate(&published) {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(DataEvent::Data(data))) => published.push(data),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    published
}

fn published_depth10(published: &[Data]) -> Option<&OrderBookDepth10> {
    published.iter().find_map(|data| match data {
        Data::Depth10(depth) => Some(depth.as_ref()),
        _ => None,
    })
}

fn published_deltas(published: &[Data]) -> Option<&OrderBookDeltas> {
    published.iter().find_map(|data| match data {
        Data::Deltas(deltas) => Some(deltas.as_ref()),
        _ => None,
    })
}

#[tokio::test]
async fn test_the_data_client_unsubscribes_the_shared_book_topic_after_deltas_and_depth10() {
    // The data-engine-level proof of the reference balance: `subscribe_book_deltas` and
    // `subscribe_book_depth10` share the `depthBooksPerps` topic, so when both are unsubscribed the
    // topic must be back at zero and the venue must actually be told. A leak here would leave the
    // topic at one, `unsubscribe` would return `None`, no unsubscribe body would ever be sent and the
    // adapter would keep replaying the market on every reconnect.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));
    let instrument_id = instrument_id(NVDA_MARKET);

    client
        .connect()
        .await
        .expect("the metadata loads over the mock REST endpoint and the feed starts");
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is accepted");
    client
        .subscribe_book_depth10(subscribe_book_depth10(instrument_id))
        .expect("the depth10 subscription rides the same book topic");

    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;
    assert_eq!(
        feed.bodies().len(),
        1,
        "the two subscriptions share one request: the second never reaches the venue"
    );

    // The depth10 the data client serves is a projection of the same book state the deltas come from,
    // which is the property `supports_depth10` stands for.
    feed.push(DEPTH_FIXTURE);
    let published = data_until(&mut events, |published| {
        published_depth10(published).is_some()
    })
    .await;
    let depth10 = published_depth10(&published).expect("the projection is published");
    let deltas = published_deltas(&published).expect("the same frame published its book deltas");
    assert_eq!(depth10.instrument_id, instrument_id);
    assert_eq!(deltas.instrument_id, instrument_id);
    assert_eq!(depth10.bids[0].price.to_string(), "212.22");
    assert_eq!(
        depth10.bids[0].price, deltas.deltas[1].order.price,
        "the projection's best bid is the best bid of the batch it rides"
    );
    assert_eq!(
        deltas.deltas.len(),
        21,
        "one CLEAR plus ten levels per side"
    );

    // Dropping the deltas subscription leaves the depth10's reference in place: no unsubscribe yet.
    client
        .unsubscribe_book_deltas(&unsubscribe_book_deltas(instrument_id))
        .expect("the deltas subscription is released");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        feed.bodies().len(),
        1,
        "the book stays subscribed while the depth10 projection still needs it"
    );

    // The last reference going away is what unsubscribes the venue.
    client
        .unsubscribe_book_depth10(&unsubscribe_book_depth10(instrument_id))
        .expect("the depth10 subscription is released");
    feed_request(&feed, 2).await;

    let bodies = feed.bodies();
    assert_eq!(
        bodies.len(),
        2,
        "exactly one subscribe and one unsubscribe reached the venue, was {bodies:?}"
    );
    assert_eq!(
        bodies[1], BOOK_UNSUBSCRIBE,
        "the last reference going away sends the book unsubscribe"
    );

    client.disconnect().await.expect("the client stops");
}

#[tokio::test]
async fn test_the_data_client_balances_a_deltas_only_book_subscription() {
    // The counterpart of the shared-topic case: one subscriber of one topic takes one reference and
    // its unsubscribe releases exactly that one, so the pair is balanced and the venue is told.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let (mut client, _events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));
    let instrument_id = instrument_id(NVDA_MARKET);

    client
        .connect()
        .await
        .expect("the metadata loads over the mock REST endpoint and the feed starts");
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is accepted");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    client
        .unsubscribe_book_deltas(&unsubscribe_book_deltas(instrument_id))
        .expect("the only subscription of the topic is released");
    feed_request(&feed, 2).await;

    let bodies = feed.bodies();
    assert_eq!(
        bodies.len(),
        2,
        "the subscribe/unsubscribe pair is balanced, was {bodies:?}"
    );
    assert!(
        bodies[0].contains("\"op\":\"subscribe\""),
        "the first body is the subscription, was `{}`",
        bodies[0]
    );
    assert_eq!(bodies[1], BOOK_UNSUBSCRIBE);

    client.disconnect().await.expect("the client stops");
}

#[tokio::test]
async fn test_a_load_the_metadata_cannot_satisfy_fails_instead_of_loading_what_is_there() {
    let feed = MockFeed::start().await;

    // The venue answers with a market this client did not ask for. ENA is the market the venue's
    // 2026-09-11 observation listed as disabled, and the requested set here is ENA alone against a
    // payload that carries NVDA alone.
    let rest = MockRest::start(fixture_with_first_pair_only()).await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[ENA_INSTRUMENT]));

    let error = client
        .load_all(None)
        .await
        .expect_err("a requested market the venue does not publish is a start-up failure");

    assert!(
        error.to_string().contains(ENA_INSTRUMENT),
        "the failure names the market it could not load, was `{error}`"
    );
    assert_eq!(
        client.store().count(),
        0,
        "nothing is loaded from a payload that cannot satisfy the request"
    );
    assert!(published_instruments(&mut events).is_empty());

    // The venue answers with no perps market at all: an empty success here would be a lie.
    let empty = MockRest::start(fixture_with_no_pairs()).await;
    let (mut client, _events) = data_client(config_for(&empty, &feed, &[]));

    let error = client
        .load_all(None)
        .await
        .expect_err("a venue with no market is a start-up failure");

    assert!(
        error.to_string().contains("no perps market data"),
        "was `{error}`"
    );
    assert_eq!(client.store().count(), 0);

    // Asking for nothing explicitly never loads everything by accident.
    let error = client
        .load_ids(&[], None)
        .await
        .expect_err("an empty request loads nothing");

    assert!(
        error.to_string().contains("no instrument id was requested"),
        "was `{error}`"
    );
}

#[tokio::test]
async fn test_a_disconnect_reaches_an_instrument_status_subscriber_and_only_a_new_snapshot_restores_the_book()
 {
    // One process, one chain: the mock venue (metadata over REST, frames over the socket), the data
    // client, the platform's own data engine, and a subscriber on the message bus topic that an
    // actor's `subscribe_instrument_status` reads.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));

    let _msgbus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None)
        .register_message_bus();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut engine = DataEngine::new(clock, cache, None);

    let instrument_id = instrument_id(NVDA_MARKET);
    let topic = switchboard::get_instrument_status_topic(instrument_id);
    let (handler, subscriber) =
        get_any_saving_handler::<InstrumentStatus>(Some(Ustr::from("ondo-status-subscriber")));
    msgbus::subscribe_any(topic.into(), handler, None);

    let seen = |reason: &str| -> usize {
        subscriber
            .get_messages()
            .into_iter()
            .filter(|status| status.reason == Some(Ustr::from(reason)))
            .count()
    };

    client
        .connect()
        .await
        .expect("the metadata loads over the mock REST endpoint and the feed starts");
    client
        .subscribe_instrument_status(SubscribeInstrumentStatus::new(
            instrument_id,
            Some(ClientId::from("ONDO-TEST")),
            None,
            UUID4::new(),
            ts_init(),
            None,
            None,
        ))
        .expect("the status subscription is routed to the book channel it is derived from");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    // 1. The first accepted snapshot makes the book usable, and the subscriber sees it.
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    ));

    let mut published = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut published, deadline, || {
            seen(REASON_SNAPSHOT_READY) == 1
        })
        .await,
        "the first accepted snapshot reaches the subscriber as `{REASON_SNAPSHOT_READY}`"
    );

    let ready = subscriber
        .get_messages()
        .into_iter()
        .find(|status| status.reason == Some(Ustr::from(REASON_SNAPSHOT_READY)))
        .expect("the ready state was published");
    assert_eq!(ready.instrument_id, instrument_id);
    assert_eq!(ready.action, MarketStatusAction::None);
    assert_eq!(ready.is_quoting, Some(true));
    assert_eq!(ready.is_trading, None);

    assert_eq!(published.len(), 1, "one frame, one batch");
    let Data::Deltas(batch) = &published[0] else {
        panic!(
            "a book snapshot is published as deltas, was {:?}",
            published[0]
        );
    };
    assert_eq!(batch.instrument_id, instrument_id);
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    assert_eq!(batch.deltas.len(), 3, "one CLEAR plus the two levels");
    assert!(
        batch
            .deltas
            .iter()
            .all(|delta| delta.flags & RecordFlag::F_SNAPSHOT as u8 == RecordFlag::F_SNAPSHOT as u8),
        "every delta of a replacement carries F_SNAPSHOT"
    );
    assert_eq!(
        batch.deltas[2].flags & RecordFlag::F_LAST as u8,
        RecordFlag::F_LAST as u8
    );
    assert_eq!(batch.sequence, NO_EXCHANGE_SEQUENCE);

    // 2. The venue closes the socket: the subscriber sees the local feed state, and the fields of
    //    the value a subscriber receives are the documented ones.
    feed.disconnect();

    let mut disconnected = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(
            &mut events,
            &mut engine,
            &mut disconnected,
            deadline,
            || { seen(REASON_DISCONNECTED) == 1 }
        )
        .await,
        "the disconnect reaches the subscriber as `{REASON_DISCONNECTED}`"
    );

    let status = subscriber
        .get_messages()
        .into_iter()
        .find(|status| status.reason == Some(Ustr::from(REASON_DISCONNECTED)))
        .expect("the disconnected state was published");
    assert_eq!(status.instrument_id, instrument_id);
    assert_eq!(status.action, MarketStatusAction::None);
    assert_eq!(status.reason, Some(Ustr::from("adapter:disconnected")));
    assert_eq!(status.is_quoting, Some(false), "the adapter is not quoting");
    assert_eq!(
        status.is_trading, None,
        "the adapter does not describe the venue"
    );
    assert_eq!(status.trading_event, None);

    // 3. The transport reconnects on its own and replays its subscriptions, which is all a socket
    //    reconnect is: no snapshot of the new session has been accepted, so nothing is published
    //    because of the reconnect and the book stays unusable.
    feed_connection(&feed, 2).await;
    feed_request(&feed, 2).await;

    let mut reconnected = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
    assert!(
        !drive_statuses(&mut events, &mut engine, &mut reconnected, deadline, || {
            false
        })
        .await,
        "the predicate never holds: this window exists to give a wrongful publication a chance"
    );
    assert!(
        reconnected.is_empty(),
        "a reconnected socket is not market data, was {reconnected:?}"
    );
    assert_eq!(
        seen(REASON_SNAPSHOT_READY),
        1,
        "reconnecting alone never restores the ready state"
    );

    // 4. A snapshot on the new session publishes market data again and restores usability. The
    //    ready state is published on the invalid -> valid transition, so the ready status arriving
    //    a second time says the book had been invalid: the reconnect did not leave a usable book.
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:11:00Z",
        &[("98.00", "3.00"), ("99.00", "1.00")],
        &[("102.00", "4.00")],
    ));

    let mut restored = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut restored, deadline, || {
            seen(REASON_SNAPSHOT_READY) == 2
        })
        .await,
        "a snapshot of the new session restores the book and says so"
    );
    assert_eq!(
        seen(REASON_SNAPSHOT_READY),
        2,
        "the ready state is published on the invalid -> valid transition, once per restoration"
    );

    assert_eq!(restored.len(), 1, "one frame, one batch");
    let Data::Deltas(batch) = &restored[0] else {
        panic!(
            "the restoring snapshot is published as deltas, was {:?}",
            restored[0]
        );
    };
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    assert_eq!(
        batch.deltas.len(),
        4,
        "one CLEAR plus the three levels of the new snapshot"
    );
    let levels: Vec<Price> = batch.deltas[1..]
        .iter()
        .map(|delta| delta.order.price)
        .collect();
    for level in ["98.00", "99.00", "102.00"] {
        assert!(
            levels.contains(&Price::from(level)),
            "the new book carries {level}, was {levels:?}"
        );
    }
    for gone in ["100.00", "101.00"] {
        assert!(
            !levels.contains(&Price::from(gone)),
            "the pre-disconnect level {gone} is gone: the book was rebuilt, not amended"
        );
    }

    client.disconnect().await.expect("the client stops");
}

// ------------------------------------------------------------------------------------------------
// The metadata axis, the market axis, and one frame of recovery
// ------------------------------------------------------------------------------------------------

/// The market metadata fixture with NVDA's status string set to `disabled`.
///
/// The string field is the one that wins the precedence in `MarketStatusInfo::resolve`, so this is
/// how the venue re-grids... regrades a market: the same increments, a different tradability.
fn fixture_with_nvda_disabled() -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
    let pairs = value["result"]["perps"]["tradingPairs"]
        .as_array_mut()
        .expect("the fixture carries trading pairs");

    for pair in pairs {
        if pair["market"] == NVDA_MARKET {
            pair["status"] = serde_json::Value::String("disabled".to_string());
        }
    }

    value.to_string()
}

/// Every instrument status the client published into `events`, in order.
fn published_statuses(published: &[Data]) -> Vec<&InstrumentStatus> {
    published
        .iter()
        .filter_map(|data| match data {
            Data::InstrumentStatus(status) => Some(status),
            _ => None,
        })
        .collect()
}

/// One frame of recovery depth is all a book needs, and the events say which came first.
///
/// The venue's `depthBooksPerps` frame is a complete replacement, so the frame after a reconnect is
/// the whole book - and the session publishes it as deltas before it publishes the state that says
/// the book is usable, which is the order a consumer needs: the data before the permission to use
/// it. A test that only checked "a ready state arrived" would pass on the wrong order.
#[tokio::test]
async fn test_one_recovery_frame_makes_the_feed_usable_and_the_deltas_come_first() {
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));
    let instrument_id = instrument_id(NVDA_MARKET);

    client
        .connect()
        .await
        .expect("the metadata loads over the mock REST endpoint and the feed starts");
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is accepted");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    ));
    let opening = data_until(&mut events, |published| {
        published_statuses(published)
            .iter()
            .any(|status| status.reason == Some(Ustr::from(REASON_SNAPSHOT_READY)))
    })
    .await;

    let deltas_at = opening
        .iter()
        .position(|data| matches!(data, Data::Deltas(_)))
        .expect("the first snapshot published its deltas");
    let ready_at = opening
        .iter()
        .position(|data| {
            matches!(data, Data::InstrumentStatus(status) if status.reason == Some(Ustr::from(REASON_SNAPSHOT_READY)))
        })
        .expect("the first snapshot published the ready state");
    assert!(
        deltas_at < ready_at,
        "the book is published before the state that permits using it, was {opening:?}"
    );

    // The venue closes the socket. The book becomes unusable and the client says so; the transport
    // reconnects on its own and replays its subscription.
    feed.disconnect();
    let disconnected = data_until(&mut events, |published| {
        published_statuses(published)
            .iter()
            .any(|status| status.reason == Some(Ustr::from(REASON_DISCONNECTED)))
    })
    .await;
    assert!(
        published_deltas(&disconnected).is_none(),
        "a disconnect is not market data, was {disconnected:?}"
    );

    feed_connection(&feed, 2).await;
    feed_request(&feed, 2).await;

    // Exactly one frame of recovery depth.
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:11:00Z",
        &[("98.00", "3.00")],
        &[("102.00", "4.00")],
    ));
    let recovery = data_until(&mut events, |published| {
        published_statuses(published)
            .iter()
            .any(|status| status.reason == Some(Ustr::from(REASON_SNAPSHOT_READY)))
    })
    .await;

    let deltas_at = recovery
        .iter()
        .position(|data| matches!(data, Data::Deltas(_)))
        .expect("the one recovery frame published its book");
    let ready_at = recovery
        .iter()
        .position(|data| {
            matches!(data, Data::InstrumentStatus(status) if status.reason == Some(Ustr::from(REASON_SNAPSHOT_READY)))
        })
        .expect("the one recovery frame published the ready state");
    assert!(
        deltas_at < ready_at,
        "one frame is the whole book, and it arrives before the state that permits using it"
    );
    let deltas = published_deltas(&recovery).expect("the recovery frame published its deltas");
    assert_eq!(deltas.deltas[0].action, BookAction::Clear);
    assert_eq!(
        deltas.deltas.len(),
        3,
        "one CLEAR plus the two levels of the recovery frame"
    );

    client.disconnect().await.expect("the client stops");
}

/// The venue's own halt reaches a subscriber, and neither a metadata recovery nor a fresh snapshot
/// clears it.
///
/// This is the chain the application's replay reads: the client reads the venue's view over REST,
/// the session publishes the book, and the platform's data engine puts both on the message bus.
#[tokio::test]
async fn test_a_venue_halt_reaches_a_subscriber_and_only_the_venue_clears_it() {
    let rest = MockRest::start_with_bodies(vec![
        MARKETS_FIXTURE.to_string(),
        fixture_with_nvda_disabled(),
    ])
    .await;
    let feed = MockFeed::start().await;
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));

    let _msgbus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None)
        .register_message_bus();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut engine = DataEngine::new(clock, cache, None);

    let instrument_id = instrument_id(NVDA_MARKET);
    let topic = switchboard::get_instrument_status_topic(instrument_id);
    let (handler, subscriber) =
        get_any_saving_handler::<InstrumentStatus>(Some(Ustr::from("ondo-market-axis-subscriber")));
    msgbus::subscribe_any(topic.into(), handler, None);

    let seen = |reason: &str| -> usize {
        subscriber
            .get_messages()
            .into_iter()
            .filter(|status| status.reason == Some(Ustr::from(reason)))
            .count()
    };
    let venue_states = || -> Vec<InstrumentStatus> {
        subscriber
            .get_messages()
            .into_iter()
            .filter(|status| status.action != MarketStatusAction::None)
            .collect()
    };

    client
        .connect()
        .await
        .expect("the metadata loads over the mock REST endpoint and the feed starts");
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is accepted");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    let mut published = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut published, deadline, || {
            !venue_states().is_empty() && seen(REASON_METADATA_READY) == 1
        })
        .await,
        "the first version publishes the venue's own status and the metadata state"
    );

    let opening = venue_states();
    assert_eq!(opening.len(), 1);
    assert_eq!(opening[0].action, MarketStatusAction::Trading);
    assert_eq!(opening[0].is_trading, Some(true));
    assert_eq!(
        opening[0].is_quoting, None,
        "the venue's status is not this adapter's feed state"
    );

    // The venue disables NVDA. The refresh reads the venue's view again - the same read the
    // sixty-second task makes, driven here instead of waited for - and this client refuses the
    // version that says so while still reporting what the venue said.
    client
        .load_all(None)
        .await
        .expect("the venue's second answer is still market metadata");

    let mut halted = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut halted, deadline, || {
            seen(REASON_METADATA_STALE) == 1
        })
        .await,
        "the metadata gate closes on the version the venue changed"
    );

    let venue = venue_states();
    assert_eq!(
        venue.len(),
        2,
        "one opening state and one halt, was {venue:?}"
    );
    let halt = &venue[1];
    assert_eq!(halt.instrument_id, instrument_id);
    assert_eq!(halt.action, MarketStatusAction::Halt);
    assert_eq!(halt.is_trading, Some(false));
    assert_eq!(
        halt.reason, None,
        "the action is the venue's statement; a reason would read as an adapter condition"
    );

    let stale = subscriber
        .get_messages()
        .into_iter()
        .find(|status| status.reason == Some(Ustr::from(REASON_METADATA_STALE)))
        .expect("the stale state was published");
    assert_eq!(stale.action, MarketStatusAction::None);
    assert_eq!(stale.is_trading, None);
    assert_eq!(stale.is_quoting, Some(false));
    assert_eq!(
        seen(REASON_METADATA_READY),
        1,
        "a read that keeps the previous version is not a ready state"
    );

    // The feed loses its connection and comes back with one frame. That restores the feed axis and
    // nothing else: the halt stands until the venue says otherwise.
    feed.disconnect();
    let mut dropped = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut dropped, deadline, || {
            seen(REASON_DISCONNECTED) == 1
        })
        .await,
        "the disconnect reaches the subscriber"
    );

    feed_connection(&feed, 2).await;
    feed_request(&feed, 2).await;
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:11:00Z",
        &[("98.00", "3.00")],
        &[("102.00", "4.00")],
    ));

    let mut recovered = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        drive_statuses(&mut events, &mut engine, &mut recovered, deadline, || {
            seen(REASON_SNAPSHOT_READY) == 1
        })
        .await,
        "one frame of recovery depth makes the feed usable again"
    );

    assert_eq!(
        venue_states().len(),
        2,
        "a recovery snapshot is not the venue resuming trading, was {:?}",
        venue_states()
    );
    assert_eq!(
        venue_states().last().map(|status| status.action),
        Some(MarketStatusAction::Halt),
        "the venue's halt is still the last thing the venue said"
    );

    client.disconnect().await.expect("the client stops");
}

// ------------------------------------------------------------------------------------------------
// The raw public-frame recording, over the mock venue
// ------------------------------------------------------------------------------------------------

/// A fresh directory under the system temp root, unique to this test and to this process.
fn raw_temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("nautilus-ondo-live-{label}-{}", UUID4::new()));
    fs::create_dir_all(&path).expect("the test's root directory is created");

    path
}

/// Every raw segment of `directory`, in rotation order (plain name sort), as `(name, records)`.
fn raw_segments(directory: &Path) -> Vec<(String, Vec<serde_json::Value>)> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .expect("the raw directory is readable")
        .map(|entry| entry.expect("a directory entry").file_name())
        .filter_map(|name| name.to_str().map(str::to_string))
        .filter(|name| name.starts_with("raw_md") && name.ends_with(".jsonl"))
        .collect();
    names.sort();

    names
        .into_iter()
        .map(|name| {
            let text = fs::read_to_string(directory.join(&name)).expect("a segment is readable");
            let records = text
                .lines()
                .map(|line| serde_json::from_str(line).expect("every line is one JSON record"))
                .collect();

            (name, records)
        })
        .collect()
}

/// The concatenated JSON text of every raw segment, which is what a reader would read.
fn raw_text(directory: &Path) -> String {
    raw_segments(directory)
        .into_iter()
        .map(|(_, records)| {
            records
                .into_iter()
                .map(|record| record.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Drives the client's events until a delta batch arrives, so a frame is proven to have been carried.
async fn wait_for_deltas(events: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(DataEvent::Data(Data::Deltas(_)))) => return true,
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return false,
        }
    }
}

#[tokio::test]
async fn test_the_raw_recorder_writes_the_public_stream_and_nothing_else() {
    // The whole chain in one process: the mock REST endpoint, the mock socket, the data client with a
    // configured `raw_md_path`, and the recorder inside the transport.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let root = raw_temp_dir("live");
    let raw_dir = root.join("raw_ondo");
    let mut config = config_for(&rest, &feed, &[NVDA_INSTRUMENT]);
    config.raw_md_path = Some(raw_dir.display().to_string());
    let (mut client, mut events) = data_client(config);

    client
        .connect()
        .await
        .expect("the metadata loads and the transport starts with a recorder");
    let instrument_id = instrument_id(NVDA_MARKET);
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is routed");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;

    let frame = book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    );
    let login = r#"{"type":"loggedIn","data":{"token":"not-recorded"}}"#;
    let private = r#"{"type":"update","channel":"orderUpdatesPerps","data":[{"orderId":"42"}]}"#;
    feed.push(&frame);
    feed.push(login);
    feed.push(&private);
    assert!(
        wait_for_deltas(&mut events).await,
        "the book frame is published, so the connection really carried it"
    );

    client.disconnect().await.expect("the client stops");

    // The recording's own verdict: it ended, and it dropped nothing and failed at nothing.
    let stats = client
        .raw_recording_stats()
        .expect("a configured recording is reported, failed or not");
    assert!(stats.clean, "the run ends clean, was {stats:?}");
    assert_eq!(stats.dropped, 0);
    assert_eq!(stats.failed, None);
    assert!(stats.finished);
    assert_eq!(stats.rotations, 0, "a short run never rotates");
    assert_eq!(stats.segments, 1);
    assert!(
        stats.records >= 3,
        "the metadata snapshot, the inbound frame and the outbound request, was {stats:?}"
    );

    let segments = raw_segments(&raw_dir);
    assert_eq!(segments.len(), 1, "one segment for a run this short");
    let records = &segments[0].1;
    assert_eq!(
        records[0]["kind"], "run_start",
        "the file opens with its header"
    );
    assert_eq!(
        records.last().expect("an end record")["kind"],
        "run_end",
        "and closes with the run's statistics"
    );
    assert_eq!(records[0]["path"], raw_dir.display().to_string());
    assert_eq!(records[0]["environment"], "production");
    assert_eq!(records[0]["queue_capacity"], 4096);
    assert_eq!(records[0]["rotate_bytes"], 128 * 1024 * 1024);

    let frames: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| record["kind"] == "frame")
        .collect();
    assert!(
        frames
            .iter()
            .any(|record| record["direction"] == "inbound" && record["raw"] == frame.as_str()),
        "the inbound frame is recorded verbatim, was {frames:?}"
    );
    assert!(
        frames.iter().any(|record| record["direction"] == "outbound"
            && record["frame_class"] == "subscription_request"
            && record["raw"]
                .as_str()
                .is_some_and(|raw| raw.contains("\"op\":\"subscribe\""))),
        "the public subscription the transport sent is recorded, was {frames:?}"
    );
    let receive_numbers: Vec<u64> = frames
        .iter()
        .map(|record| {
            record["recv_seq"]
                .as_u64()
                .expect("a frame carries its receive number")
        })
        .collect();
    assert_eq!(
        receive_numbers,
        (1..=frames.len() as u64).collect::<Vec<u64>>(),
        "the receive order is continuous, whatever the direction"
    );

    // The whitelist, on the file itself: neither the login acknowledgement nor the private payload is
    // in it, and nothing about the login's data leaks into a record either.
    let text = raw_text(&raw_dir);
    assert!(
        !text.contains("loggedIn"),
        "no login acknowledgement is recorded"
    );
    assert!(
        !text.contains("orderUpdatesPerps"),
        "no private payload is recorded"
    );
    assert!(!text.contains("not-recorded"));

    // The REST metadata snapshot is in the same stream, with the whitelisted headers only.
    let metadata: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| record["kind"] == "rest_metadata")
        .collect();
    assert_eq!(metadata.len(), 1, "the run's one metadata read is recorded");
    assert_eq!(metadata[0]["target"], "/v1/markets");
    assert_eq!(metadata[0]["status"], 200);
    assert_eq!(
        metadata[0]["body_hash_blake3"]
            .as_str()
            .expect("a body hash")
            .len(),
        64
    );
    assert!(
        metadata[0]["body"]
            .as_str()
            .is_some_and(|body| body.contains("tradingPairs")),
        "the metadata body is kept verbatim"
    );
    for name in metadata[0]["headers"]
        .as_object()
        .expect("the headers are an object")
        .keys()
    {
        assert!(
            ["content-type", "content-length", "date", "retry-after"].contains(&name.as_str()),
            "a recorded header is whitelisted, was `{name}`"
        );
    }

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_the_configured_run_id_joins_the_raw_frames_with_the_tape() {
    // The application passes its process stamp as `raw_md_run_id` and a `--out` that is NOT the run
    // directory (its default `reports/stage1` is the case that matters): without the explicit id the
    // recorder would derive `stage1` from the path, and the raw frames would silently carry a
    // different run id than every tape record of the same run.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let root = raw_temp_dir("run-id");
    let raw_dir = root.join("stage1").join("raw_ondo");
    let mut config = config_for(&rest, &feed, &[NVDA_INSTRUMENT]);
    config.raw_md_path = Some(raw_dir.display().to_string());
    config.raw_md_run_id = Some("20260914T000000Z".to_string());
    let (mut client, mut events) = data_client(config);

    client
        .connect()
        .await
        .expect("the metadata loads and the transport starts with a recorder");
    let instrument_id = instrument_id(NVDA_MARKET);
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is routed");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    ));
    assert!(
        wait_for_deltas(&mut events).await,
        "the book frame is published, so the connection really carried it"
    );

    client.disconnect().await.expect("the client stops");

    let mut segments = raw_segments(&raw_dir);
    assert_eq!(segments.len(), 1, "one segment for a run this short");
    let records = segments.remove(0).1;
    assert_eq!(records[0]["kind"], "run_start");
    assert_eq!(
        records[0]["run_id"], "20260914T000000Z",
        "the raw frames carry the application's process stamp, never the directory name `stage1`"
    );
    assert_eq!(
        records[0]["run_id_source"], "configured",
        "the explicit id is recorded as such, so the tape join is never assumed"
    );
    assert!(
        records
            .iter()
            .filter(|record| record["kind"] == "frame")
            .all(|record| record["run_id"] == "20260914T000000Z"),
        "every frame of the run carries the configured run id"
    );

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_without_a_raw_md_path_nothing_is_recorded_at_all() {
    // The default configuration: `raw_md_path` is `None`, so there is no recorder, no writer thread,
    // no directory and no file - not even an empty one.
    let rest = MockRest::start(MARKETS_FIXTURE.to_string()).await;
    let feed = MockFeed::start().await;
    let root = raw_temp_dir("disabled");
    let (mut client, mut events) = data_client(config_for(&rest, &feed, &[NVDA_INSTRUMENT]));

    assert!(client.raw_recording_stats().is_none());
    client.connect().await.expect("the metadata loads");
    assert!(
        client.raw_recording_stats().is_none(),
        "no recorder is started without a path"
    );
    let instrument_id = instrument_id(NVDA_MARKET);
    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id))
        .expect("the book subscription is routed");
    feed_connection(&feed, 1).await;
    feed_request(&feed, 1).await;
    feed.push(&book_frame(
        NVDA_MARKET,
        "2026-09-14T11:10:00Z",
        &[("100.00", "1.00")],
        &[("101.00", "1.00")],
    ));
    assert!(
        wait_for_deltas(&mut events).await,
        "the feed works exactly as it does with a recording configured"
    );

    client.disconnect().await.expect("the client stops");

    assert!(
        client.raw_recording_stats().is_none(),
        "there is nothing to report because nothing was recorded"
    );
    let entries: Vec<String> = fs::read_dir(&root)
        .expect("the root directory is readable")
        .map(|entry| entry.expect("a directory entry").file_name())
        .filter_map(|name| name.to_str().map(str::to_string))
        .collect();
    assert!(
        entries.is_empty(),
        "a run with no configured recording writes nothing at all, was {entries:?}"
    );

    let _ = fs::remove_dir_all(&root);
}
