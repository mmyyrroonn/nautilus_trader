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

//! Owned public session, publication gates and bounded depth bootstrap.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use nautilus_common::messages::DataEvent;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::task::TaskSpawner;
use nautilus_model::{data::Data, instruments::InstrumentAny};
use nautilus_network::{
    SocketState, SocketStateSink,
    transport::Message,
    websocket::{EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig},
};
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    config::BackpackDataClientConfig,
    data_error::BackpackDataError,
    depth::{BackpackBookCoverage, BackpackDepthSynchronizer},
    http::{
        client::BackpackHttpClient,
        request::{BackpackReadOperation, BackpackReadRequest},
    },
    instruments::BackpackInstrumentMetadata,
    models::BackpackMarket,
    provider::BackpackInstrumentProvider,
    public::{BackpackPublicEvent, BackpackPublicStreamParser},
    signing::{BackpackParameters, BackpackScalar},
};

pub(crate) type EventSender = mpsc::UnboundedSender<DataEvent>;
pub(crate) type Metadata = BTreeMap<String, BackpackInstrumentMetadata>;

pub(crate) fn now() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}

/// An observation of current public session readiness; execution readiness is never implied.
#[derive(Clone, Debug, serde::Serialize)]
pub struct BackpackPublicHealth {
    /// Owning manual connection generation.
    pub generation: u64,
    /// Unique identity of the actual native client run using this telemetry handle.
    pub run_id: Option<u64>,
    /// Version of the sanitized telemetry schema.
    pub schema_version: u32,
    /// Current shared transport connection epoch.
    pub connection_epoch: u64,
    /// Complete validated instrument metadata has been published for this connection.
    pub metadata_ready: bool,
    /// Current session transport and publication gate are open.
    pub connected: bool,
    /// Last critical fault, without response bodies or raw frames.
    pub stale_reason: Option<&'static str>,
    /// Per-symbol BBO freshness, independent of book depth coverage.
    pub quotes_fresh: BTreeMap<String, bool>,
    /// Positive subscription acknowledgements have not been established by official evidence.
    pub subscription_acknowledgements_verified: bool,
    /// Public observations never establish execution readiness.
    pub execution_ready: bool,
    /// Sequence continuity of each currently subscribed bounded book.
    pub books_continuous: BTreeMap<String, bool>,
    /// Original receipt age of each bounded book is within the operational idle policy.
    pub books_fresh: BTreeMap<String, bool>,
    /// Exact Unix nanoseconds of the latest valid BBO engine event.
    pub quote_event_ns: BTreeMap<String, String>,
    /// Exact Unix nanoseconds when that event originally arrived.
    pub quote_received_ns: BTreeMap<String, String>,
    /// Per-symbol depth continuity and snapshot coverage.
    #[serde(serialize_with = "serialize_books")]
    pub books: BTreeMap<String, Option<BackpackBookCoverage>>,
}

#[derive(Default)]
pub(crate) struct Gate {
    pub owner: u64,
    pub fault_serial: u64,
    pub run_id: u64,
    pub idle_secs: u64,
    pub quote_stale_after_ms: u64,
    pub epoch: u64,
    pub running: bool,
    pub metadata_ready: bool,
    pub connected: bool,
    pub stale: Option<&'static str>,
    pub lost_at: Option<nautilus_network::dst::time::Instant>,
    pub revisions: BTreeMap<String, u64>,
    pub quote_receipts: BTreeMap<String, (UnixNanos, UnixNanos)>,
    pub books: BTreeMap<String, Option<BackpackBookCoverage>>,
    pub book_receipts: BTreeMap<String, UnixNanos>,
    pub book_tokens: BTreeMap<String, u64>,
    pub instruments: Vec<InstrumentAny>,
}
impl Gate {
    pub(crate) fn invalidate(&mut self, reason: &'static str) {
        match self.fault_serial.checked_add(1) {
            Some(v) => self.fault_serial = v,
            None => self.running = false,
        }
        self.connected = false;
        self.metadata_ready = false;
        self.stale = Some(reason);
        self.quote_receipts.clear();
        self.book_receipts.clear();
        self.books.values_mut().for_each(|v| *v = None);
        self.lost_at
            .get_or_insert_with(nautilus_network::dst::time::Instant::now);
    }
    pub(crate) fn current(&self, owner: u64, epoch: u64) -> bool {
        self.running && self.owner == owner && self.epoch == epoch
    }
    pub(crate) fn admits(&self, owner: u64, epoch: u64) -> bool {
        self.current(owner, epoch) && self.connected && self.metadata_ready
    }
    pub(crate) fn health(&self, _idle_secs: u64) -> BackpackPublicHealth {
        let receipt_now = now().as_u64();
        BackpackPublicHealth {
            schema_version: 1,
            run_id: (self.run_id != 0).then_some(self.run_id),
            generation: self.owner,
            connection_epoch: self.epoch,
            metadata_ready: self.metadata_ready,
            connected: self.running && self.connected,
            stale_reason: self.stale,
            subscription_acknowledgements_verified: false,
            execution_ready: false,
            books_continuous: self
                .books
                .iter()
                .map(|(s, c)| {
                    (
                        s.clone(),
                        self.running && self.connected && self.metadata_ready && c.is_some(),
                    )
                })
                .collect(),
            books_fresh: self
                .books
                .iter()
                .map(|(s, c)| {
                    (
                        s.clone(),
                        self.running
                            && self.connected
                            && self.metadata_ready
                            && c.is_some()
                            && self.book_receipts.get(s).is_some_and(|r| {
                                r.as_u64() <= receipt_now
                                    && receipt_now.saturating_sub(r.as_u64())
                                        <= self.idle_secs * 1_000_000_000
                            }),
                    )
                })
                .collect(),
            quotes_fresh: self
                .quote_receipts
                .iter()
                .map(|(s, (_, r))| {
                    (
                        s.clone(),
                        self.running
                            && self.connected
                            && self.metadata_ready
                            && r.as_u64() <= receipt_now
                            && self.quote_receipts[s].0.as_u64() <= receipt_now
                            && receipt_now
                                .saturating_sub(r.as_u64())
                                .max(receipt_now.saturating_sub(self.quote_receipts[s].0.as_u64()))
                                <= self.quote_stale_after_ms * 1_000_000,
                    )
                })
                .collect(),
            quote_event_ns: self
                .quote_receipts
                .iter()
                .map(|(s, (e, _))| (s.clone(), e.as_u64().to_string()))
                .collect(),
            quote_received_ns: self
                .quote_receipts
                .iter()
                .map(|(s, (_, r))| (s.clone(), r.as_u64().to_string()))
                .collect(),
            books: self
                .books
                .iter()
                .map(|(s, c)| {
                    (
                        s.clone(),
                        if self.running
                            && self.connected
                            && self.metadata_ready
                            && self.book_receipts.get(s).is_some_and(|r| {
                                r.as_u64() <= receipt_now
                                    && receipt_now.saturating_sub(r.as_u64())
                                        <= self.idle_secs * 1_000_000_000
                            })
                        {
                            *c
                        } else {
                            None
                        },
                    )
                })
                .collect(),
        }
    }
}

pub(crate) struct PublicSession {
    pub config: BackpackDataClientConfig,
    pub http: BackpackHttpClient,
    pub gate: Arc<Mutex<Gate>>,
    pub subscriptions: SubscriptionState,
    pub notify: Arc<Notify>,
    pub sender: EventSender,
}

pub(crate) async fn load_metadata(
    config: &BackpackDataClientConfig,
    http: &BackpackHttpClient,
    cancel: &CancellationToken,
    admission: Option<&(dyn Fn() -> bool + Send + Sync)>,
    deadline: Option<nautilus_network::dst::time::Instant>,
) -> Result<(Metadata, Vec<InstrumentAny>), BackpackDataError> {
    let request = BackpackReadRequest::new(
        BackpackReadOperation::Markets,
        BackpackParameters::default(),
    )
    .map_err(|_| BackpackDataError::Metadata)?;
    let response = http
        .read(&request, deadline, admission, cancel)
        .await
        .map_err(|_| BackpackDataError::Metadata)?;
    let received_at = now();

    if response.body().len() > 8 * 1024 * 1024 {
        return Err(BackpackDataError::Metadata);
    }
    let markets: Vec<BackpackMarket> =
        serde_json::from_slice(response.body()).map_err(|_| BackpackDataError::Metadata)?;
    let mut provider = BackpackInstrumentProvider::new(config.scope().clone());
    provider
        .replace_markets(&markets, received_at)
        .map_err(|_| BackpackDataError::Metadata)?;
    let mut metadata = Metadata::new();
    let mut instruments = Vec::new();

    for market in provider.all() {
        let economics = config
            .economics()
            .get(market.raw_symbol.as_str())
            .ok_or(BackpackDataError::Metadata)?;
        let instrument = market
            .to_instrument(Some(economics))
            .map_err(|_| BackpackDataError::Metadata)?;
        instruments.push(InstrumentAny::CryptoPerpetual(instrument));
        metadata.insert(market.raw_symbol.to_string(), market.clone());
    }
    Ok((metadata, instruments))
}

pub(crate) struct Frame {
    book_token: u64,
    epoch: u64,
    revision: u64,
    topic: String,
    bytes: Bytes,
    received: UnixNanos,
}
pub(crate) struct Snapshot {
    epoch: u64,
    revision: u64,
    token: u64,
    symbol: String,
    received: UnixNanos,
    result: Result<Bytes, BackpackDataError>,
}
pub(crate) enum Input {
    Frame(Frame),
    Reconnected(u64),
    Snapshot(Snapshot),
}
#[derive(Deserialize)]
struct FrameHeader {
    stream: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}
struct MarketState {
    metadata: BackpackInstrumentMetadata,
    stream_token: u64,
    book_parser: Option<BackpackPublicStreamParser>,
    parser: BackpackPublicStreamParser,
    book: Option<BackpackDepthSynchronizer>,
    token: u64,
    last_sequence: Option<u64>,
    snapshot_cancel: Option<CancellationToken>,
}
impl Drop for MarketState {
    fn drop(&mut self) {
        if let Some(c) = self.snapshot_cancel.take() {
            c.cancel();
        }
    }
}

struct ActorState {
    epoch: u64,
    generation: u64,
    markets: BTreeMap<String, MarketState>,
    sent: BTreeMap<String, u64>,
}

impl PublicSession {
    pub(crate) async fn open(
        &self,
        owner: u64,
        cancel: CancellationToken,
    ) -> Result<
        (
            Arc<WebSocketClient>,
            mpsc::Receiver<Input>,
            mpsc::Sender<Input>,
        ),
        BackpackDataError,
    > {
        let (tx, rx) = mpsc::channel(self.config.lifecycle().max_buffer_frames);
        let gate = self.gate.clone();
        let notify = self.notify.clone();
        let frame_tx = tx.clone();
        let cap = self.config.lifecycle().max_ws_message_bytes;
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            let received = now();
            let mut state = gate.lock();
            if !state.current(owner, epoch)
                && !matches!(&message,Message::Text(b) if b.as_ref()==nautilus_network::RECONNECTED.as_bytes() && epoch>state.epoch && state.owner==owner && state.running)
            {
                return;
            }
            let input = match message {
                Message::Text(bytes)
                    if bytes.as_ref() == nautilus_network::RECONNECTED.as_bytes() =>
                {
                    state.epoch = epoch;
                    state.invalidate("replacement bootstrap");
                    Some(Input::Reconnected(epoch))
                }
                Message::Text(bytes) if bytes.len() <= cap => {
                    match serde_json::from_slice::<FrameHeader>(&bytes) {
                        Ok(h) if h.error.is_some() => {
                            state.invalidate("venue stream error");
                            notify.notify_one();
                            None
                        }
                        Ok(h) if h.stream.is_some() => {
                            let topic = h.stream.unwrap();
                            let revision = *state.revisions.get(&topic).unwrap_or(&0);
                            Some(Input::Frame(Frame {
                                book_token: topic
                                    .strip_prefix("depth.")
                                    .and_then(|s| state.book_tokens.get(s))
                                    .copied()
                                    .unwrap_or(0),
                                epoch,
                                revision,
                                topic,
                                bytes,
                                received,
                            }))
                        }
                        _ => {
                            state.invalidate("malformed public frame");
                            notify.notify_one();
                            None
                        }
                    }
                }
                Message::Ping(_) | Message::Pong(_) | Message::Close(_) => None,
                _ => {
                    state.invalidate("oversized or non-text public frame");
                    notify.notify_one();
                    None
                }
            };

            if let Some(input) = input
                && frame_tx.try_send(input).is_err()
            {
                state.invalidate("public frame queue overflow");
                notify.notify_one();
            }
        });
        let gate = self.gate.clone();
        let notify = self.notify.clone();
        let sink = SocketStateSink::new(move |edge| {
            if edge == SocketState::Disconnected {
                let mut state = gate.lock();
                if state.owner == owner && state.running {
                    state.invalidate("transport disconnected");
                    notify.notify_one();
                }
            }
        });
        let policy = self.config.lifecycle();
        let ws_config = WebSocketConfig::builder()
            .url(self.config.scope().endpoints().websocket_url().to_owned())
            .heartbeat_interval_secs(policy.ws_heartbeat_secs)
            .heartbeat_timeout_secs(policy.ws_idle_timeout_secs)
            .idle_timeout_ms(policy.ws_idle_timeout_secs * 1000)
            .connect_timeout_ms(policy.ws_connect_timeout_secs * 1000)
            .reconnect_delay_initial_ms(100)
            .reconnect_delay_max_ms(2000)
            .reconnect_max_attempts(20)
            .build()
            .map_err(|_| BackpackDataError::Configuration("websocket policy"))?;
        let ws = WebSocketClient::epoch_builder()
            .config(ws_config)
            .epoch_handler(handler)
            .state_sink(sink)
            .cancellation_token(cancel)
            .connect()
            .await
            .map_err(|_| BackpackDataError::Transport)?;
        Ok((Arc::new(ws), rx, tx))
    }

    pub(crate) async fn run(
        self,
        owner: u64,
        ws: Arc<WebSocketClient>,
        mut rx: mpsc::Receiver<Input>,
        tx: mpsc::Sender<Input>,
        initial: Metadata,
        spawner: TaskSpawner,
    ) {
        let cancel = spawner.cancellation_token();
        let mut actor = ActorState {
            epoch: 0,
            generation: 0,
            markets: BTreeMap::new(),
            sent: BTreeMap::new(),
        };

        for (symbol, metadata) in initial {
            actor.generation += 1;
            actor.markets.insert(
                symbol,
                MarketState {
                    stream_token: actor.generation,
                    book_parser: None,
                    parser: BackpackPublicStreamParser::new(metadata.clone(), actor.generation),
                    metadata,
                    book: None,
                    token: actor.generation,
                    last_sequence: None,
                    snapshot_cancel: None,
                },
            );
        }

        loop {
            let (current, expired, ready) = {
                let state = self.gate.lock();
                (
                    state.running && state.owner == owner,
                    state.lost_at.is_some_and(|t| {
                        t.elapsed()
                            >= Duration::from_secs(self.config.lifecycle().reconnect_timeout_secs)
                    }),
                    state.admits(owner, actor.epoch),
                )
            };

            if cancel.is_cancelled() || !current {
                break;
            }

            if expired {
                self.gate.lock().invalidate("recovery deadline");
                let _ = nautilus_network::dst::time::timeout(
                    Duration::from_secs(self.config.lifecycle().shutdown_timeout_secs),
                    ws.disconnect(),
                )
                .await;
                break;
            }

            if ready
                && self
                    .reconcile(
                        owner,
                        actor.epoch,
                        &ws,
                        &mut actor.markets,
                        &mut actor.sent,
                        &mut actor.generation,
                        &spawner,
                        &tx,
                    )
                    .await
                    .is_err()
            {
                self.fault(owner, actor.epoch, "subscription send failed", &ws);
            }
            let input = tokio::select! {
                biased;
                ()=cancel.cancelled()=>break,
                ()=self.notify.notified()=> {
                    if !self.gate.lock().admits(owner,actor.epoch) {
                        let _=ws.reconnect_handle().request_reconnect();
                    }
                    None
                },
                ()=nautilus_network::dst::time::sleep(Duration::from_millis(100))=>None,
                input=rx.recv()=>match input {Some(input)=>Some(input),None=>break},
            };

            match input {
                Some(Input::Reconnected(epoch)) => {
                    self.rebootstrap(owner, &ws, &mut actor, epoch, &cancel)
                        .await;
                }
                Some(Input::Frame(frame)) => {
                    self.frame(owner, &ws, &mut actor, &frame, &spawner, &tx);
                }
                Some(Input::Snapshot(snapshot)) => self.install(owner, &ws, &mut actor, snapshot),
                None => {}
            }
        }
        {
            let mut state = self.gate.lock();
            if state.owner == owner {
                let reason = state.stale.unwrap_or("session stopped");
                state.invalidate(reason);
                state.running = false;
            }
        }
        drop(actor);
        let _ = nautilus_network::dst::time::timeout(
            Duration::from_secs(self.config.lifecycle().shutdown_timeout_secs),
            ws.disconnect(),
        )
        .await;
    }

    async fn rebootstrap(
        &self,
        owner: u64,
        ws: &WebSocketClient,
        actor: &mut ActorState,
        epoch: u64,
        cancel: &CancellationToken,
    ) {
        let (current, fault_serial, deadline, bootstrap) = {
            let state = self.gate.lock();
            (
                epoch > actor.epoch && state.current(owner, epoch),
                state.fault_serial,
                state.lost_at.and_then(|t| {
                    t.checked_add(Duration::from_secs(
                        self.config.lifecycle().reconnect_timeout_secs,
                    ))
                }),
                state.stale == Some("replacement bootstrap"),
            )
        };

        if !current {
            return;
        }
        actor.epoch = epoch;
        actor.markets.clear();
        actor.sent.clear();
        self.subscriptions.reset_after_reconnect();

        if !bootstrap {
            self.fault(owner, epoch, "bootstrap invalidated", ws);
            return;
        }
        let gate = self.gate.clone();
        let admission = move || {
            let state = gate.lock();
            state.current(owner, epoch) && state.fault_serial == fault_serial
        };
        let result =
            load_metadata(&self.config, &self.http, cancel, Some(&admission), deadline).await;
        let Ok((metadata, instruments)) = result else {
            self.fault(owner, epoch, "metadata refresh failed", ws);
            return;
        };
        let mut state = self.gate.lock();
        if !state.current(owner, epoch) || state.fault_serial != fault_serial {
            drop(state);
            self.fault(owner, epoch, "bootstrap invalidated", ws);
            return;
        }

        for instrument in &instruments {
            if self
                .sender
                .send(DataEvent::Instrument(instrument.clone()))
                .is_err()
            {
                state.invalidate("data engine unavailable");
                state.running = false;
                return;
            }
        }

        for (symbol, metadata) in metadata {
            actor.generation += 1;
            actor.markets.insert(
                symbol,
                MarketState {
                    stream_token: actor.generation,
                    book_parser: None,
                    parser: BackpackPublicStreamParser::new(metadata.clone(), actor.generation),
                    metadata,
                    book: None,
                    token: actor.generation,
                    last_sequence: None,
                    snapshot_cancel: None,
                },
            );
        }
        state.instruments = instruments;
        state.metadata_ready = true;
        state.connected = true;
        state.stale = None;
        state.lost_at = None;
    }

    fn frame(
        &self,
        owner: u64,
        ws: &WebSocketClient,
        actor: &mut ActorState,
        frame: &Frame,
        spawner: &TaskSpawner,
        tx: &mpsc::Sender<Input>,
    ) {
        let epoch = actor.epoch;
        if frame.epoch != epoch || !self.current_topic(owner, epoch, &frame.topic, frame.revision) {
            return;
        }

        if !self.fresh(frame.received) {
            self.fault(owner, epoch, "stale queued frame", ws);
            return;
        }
        let Some((_, symbol)) = frame.topic.split_once('.') else {
            self.fault(owner, epoch, "invalid stream topic", ws);
            return;
        };
        let Some(market) = actor.markets.get_mut(symbol) else {
            self.fault(owner, epoch, "unknown public instrument", ws);
            return;
        };
        let decoded = if frame.topic.starts_with("depth.") {
            if frame.book_token != market.token {
                return;
            }
            let Some(parser) = market.book_parser.as_mut() else {
                return;
            };
            parser.decode(market.token, &frame.bytes, frame.received)
        } else {
            market
                .parser
                .decode(market.stream_token, &frame.bytes, frame.received)
        };

        match decoded {
            Ok(BackpackPublicEvent::Quote(quote)) => {
                let mut state = self.gate.lock();
                if !self.admits_topic(&state, owner, epoch, &frame.topic, frame.revision) {
                    return;
                }
                let monotonic = state
                    .quote_receipts
                    .get(symbol)
                    .is_none_or(|(event, receipt)| {
                        quote.ts_event >= *event && frame.received >= *receipt
                    });
                let observed_now = now().as_u64();
                let current = quote.ts_event.as_u64() <= observed_now
                    && frame.received.as_u64() <= observed_now
                    && (observed_now - quote.ts_event.as_u64())
                        .max(observed_now - frame.received.as_u64())
                        <= self.config.lifecycle().quote_stale_after_ms * 1_000_000;

                if monotonic && current {
                    state
                        .quote_receipts
                        .insert(symbol.to_owned(), (quote.ts_event, frame.received));

                    if self
                        .sender
                        .send(DataEvent::Data(Data::Quote(quote)))
                        .is_err()
                    {
                        state.invalidate("data engine unavailable");
                        state.running = false;
                    }
                } else {
                    state.quote_receipts.remove(symbol);
                }
            }
            Ok(BackpackPublicEvent::QuoteUnavailable { .. }) => {
                let mut state = self.gate.lock();
                if self.admits_topic(&state, owner, epoch, &frame.topic, frame.revision) {
                    state.quote_receipts.remove(symbol);
                }
            }
            Ok(BackpackPublicEvent::Trade(trade)) => self.publish(
                owner,
                epoch,
                &frame.topic,
                frame.revision,
                Data::Trade(trade),
            ),
            Ok(BackpackPublicEvent::Mark { price, .. }) => self.publish(
                owner,
                epoch,
                &frame.topic,
                frame.revision,
                Data::MarkPrice(price),
            ),
            Ok(BackpackPublicEvent::Depth(update)) => {
                if update.ts_event.as_u64() > now().as_u64() {
                    self.fault(owner, epoch, "future depth event", ws);
                    return;
                }
                let last = update.last;
                let advances = market.last_sequence.is_none_or(|previous| last > previous);
                if let Some(book) = market.book.as_mut() {
                    match book.apply(update) {
                        Ok(Some(batch)) => {
                            market.last_sequence = Some(last);
                            self.book_ready(
                                owner,
                                epoch,
                                symbol,
                                frame.revision,
                                book.coverage(),
                                batch.ts_init,
                            );
                            self.publish(
                                owner,
                                epoch,
                                &frame.topic,
                                frame.revision,
                                Data::from(batch),
                            );
                        }
                        Ok(None) => {
                            if book.is_continuous() && advances {
                                market.last_sequence = Some(last);
                                self.book_ready(
                                    owner,
                                    epoch,
                                    symbol,
                                    frame.revision,
                                    book.coverage(),
                                    frame.received,
                                );
                            }
                        }
                        Err(_) => {
                            self.book_ready(
                                owner,
                                epoch,
                                symbol,
                                frame.revision,
                                None,
                                frame.received,
                            );
                            actor.generation += 1;

                            if self
                                .start_book(
                                    owner,
                                    epoch,
                                    symbol,
                                    frame.revision,
                                    actor.generation,
                                    market,
                                    spawner,
                                    tx,
                                )
                                .is_err()
                            {
                                self.fault(owner, epoch, "depth recovery failed", ws);
                            }
                        }
                    }
                }
            }
            Ok(BackpackPublicEvent::Duplicate) => {}
            Err(_) => self.fault(owner, epoch, "invalid public observation", ws),
        }
    }

    fn install(
        &self,
        owner: u64,
        ws: &WebSocketClient,
        actor: &mut ActorState,
        snapshot: Snapshot,
    ) {
        let epoch = actor.epoch;
        let topic = format!("depth.{}", snapshot.symbol);
        if snapshot.epoch != epoch || !self.current_topic(owner, epoch, &topic, snapshot.revision) {
            return;
        }
        let Some(market) = actor.markets.get_mut(&snapshot.symbol) else {
            return;
        };

        if market.token != snapshot.token {
            return;
        }

        if !self.fresh(snapshot.received) {
            self.fault(owner, epoch, "stale depth snapshot", ws);
            return;
        }
        let Ok(body) = snapshot.result else {
            self.fault(owner, epoch, "depth snapshot failed", ws);
            return;
        };

        if let Some(book) = market.book.as_mut() {
            match book.install_snapshot(snapshot.token, &body, snapshot.received) {
                Ok(batch) => {
                    if !self.fresh(batch.ts_init) || batch.ts_event.as_u64() > now().as_u64() {
                        book.invalidate();
                        self.fault(owner, epoch, "stale or future depth replay", ws);
                    } else {
                        market.last_sequence = Some(batch.sequence);
                        self.book_ready(
                            owner,
                            epoch,
                            &snapshot.symbol,
                            snapshot.revision,
                            book.coverage(),
                            batch.ts_init,
                        );
                        self.publish(owner, epoch, &topic, snapshot.revision, Data::from(batch));
                    }
                }
                Err(_) => self.fault(owner, epoch, "depth snapshot rejected", ws),
            }
        }
    }

    fn fresh(&self, receipt: UnixNanos) -> bool {
        let observed_now = now().as_u64();
        receipt.as_u64() <= observed_now
            && observed_now - receipt.as_u64()
                <= self.config.lifecycle().ws_idle_timeout_secs * 1_000_000_000
    }

    fn admits_topic(
        &self,
        state: &Gate,
        owner: u64,
        epoch: u64,
        topic: &str,
        revision: u64,
    ) -> bool {
        state.admits(owner, epoch)
            && state.revisions.get(topic) == Some(&revision)
            && self.subscriptions.all_topics().iter().any(|t| t == topic)
    }
    fn current_topic(&self, owner: u64, epoch: u64, topic: &str, revision: u64) -> bool {
        self.admits_topic(&self.gate.lock(), owner, epoch, topic, revision)
    }
    fn publish(&self, owner: u64, epoch: u64, topic: &str, revision: u64, data: Data) {
        use nautilus_model::data::HasTsInit;
        let event_time = match &data {
            Data::Trade(t) => Some(t.ts_event),
            Data::MarkPrice(p) => Some(p.ts_event),
            Data::Deltas(d) => Some(d.ts_event),
            _ => None,
        };
        let mut state = self.gate.lock();
        if self.admits_topic(&state, owner, epoch, topic, revision)
            && self.fresh(data.ts_init())
            && event_time.is_some_and(|t| t.as_u64() <= now().as_u64())
            && self.sender.send(DataEvent::Data(data)).is_err()
        {
            state.invalidate("data engine unavailable");
            state.running = false;
        }
    }
    fn book_ready(
        &self,
        owner: u64,
        epoch: u64,
        symbol: &str,
        revision: u64,
        coverage: Option<BackpackBookCoverage>,
        received: UnixNanos,
    ) {
        let mut state = self.gate.lock();
        if self.admits_topic(&state, owner, epoch, &format!("depth.{symbol}"), revision) {
            state.books.insert(symbol.to_owned(), coverage);
            state.book_receipts.insert(symbol.to_owned(), received);
        }
    }
    fn fault(&self, owner: u64, epoch: u64, reason: &'static str, ws: &WebSocketClient) {
        let mut state = self.gate.lock();
        if !state.current(owner, epoch) {
            return;
        }
        state.invalidate(reason);
        let expired = state.lost_at.is_some_and(|t| {
            t.elapsed() >= Duration::from_secs(self.config.lifecycle().reconnect_timeout_secs)
        });
        drop(state);

        if expired {
            return;
        }
        let _ = ws.reconnect_handle().request_reconnect();
    }

    #[expect(clippy::too_many_arguments)]
    async fn reconcile(
        &self,
        owner: u64,
        epoch: u64,
        ws: &WebSocketClient,
        markets: &mut BTreeMap<String, MarketState>,
        sent: &mut BTreeMap<String, u64>,
        generation: &mut u64,
        spawner: &TaskSpawner,
        tx: &mpsc::Sender<Input>,
    ) -> Result<(), BackpackDataError> {
        let desired = {
            let state = self.gate.lock();
            self.subscriptions
                .all_topics()
                .into_iter()
                .map(|t| {
                    let r = *state.revisions.get(&t).unwrap_or(&0);
                    (t, r)
                })
                .collect::<BTreeMap<_, _>>()
        };
        let removed = sent
            .iter()
            .filter(|(t, r)| desired.get(*t) != Some(*r))
            .map(|(t, _)| t.clone())
            .collect::<Vec<_>>();

        for topic in removed {
            ws.send_text_on_connection(
                serde_json::json!({"method":"UNSUBSCRIBE","params":[topic]}).to_string(),
                None,
                epoch,
            )
            .await
            .map_err(|_| BackpackDataError::Transport)?;
            sent.remove(&topic);
            if let Some(symbol) = topic.strip_prefix("depth.") {
                if let Some(m) = markets.get_mut(symbol) {
                    m.book = None;
                    if let Some(c) = m.snapshot_cancel.take() {
                        c.cancel();
                    }
                }
                self.book_ready(owner, epoch, symbol, 0, None, now());
            }
        }

        for (topic, revision) in desired {
            if sent.get(&topic) == Some(&revision) {
                continue;
            }

            if let Some(symbol) = topic.strip_prefix("depth.") {
                let market = markets.get_mut(symbol).ok_or(BackpackDataError::Metadata)?;
                *generation += 1;
                self.prepare_book(owner, epoch, revision, symbol, *generation, market)?;
            }
            ws.send_text_on_connection(
                serde_json::json!({"method":"SUBSCRIBE","params":[topic]}).to_string(),
                None,
                epoch,
            )
            .await
            .map_err(|_| BackpackDataError::Transport)?;
            sent.insert(topic.clone(), revision);
            if let Some(symbol) = topic.strip_prefix("depth.") {
                let market = markets.get_mut(symbol).ok_or(BackpackDataError::Metadata)?;
                self.snapshot(owner, epoch, symbol, revision, market, spawner, tx)?;
            }
        }
        Ok(())
    }
    fn prepare_book(
        &self,
        owner: u64,
        epoch: u64,
        revision: u64,
        symbol: &str,
        token: u64,
        market: &mut MarketState,
    ) -> Result<(), BackpackDataError> {
        let mut state = self.gate.lock();
        if !self.admits_topic(&state, owner, epoch, &format!("depth.{symbol}"), revision) {
            return Ok(());
        }

        if let Some(c) = market.snapshot_cancel.take() {
            c.cancel();
        }
        let metadata = market.metadata.clone();
        let p = self.config.lifecycle();
        market.book_parser = Some(BackpackPublicStreamParser::new(metadata.clone(), token));
        market.token = token;
        market.last_sequence = None;
        market.book = Some(
            BackpackDepthSynchronizer::new_checked(
                metadata,
                token,
                p.depth_snapshot_limit,
                p.max_buffer_frames,
                p.max_levels_per_side,
            )
            .map_err(|_| BackpackDataError::Configuration("depth policy"))?,
        );
        state.books.insert(symbol.to_owned(), None);
        state.book_tokens.insert(symbol.to_owned(), token);
        Ok(())
    }
    #[expect(clippy::too_many_arguments)]
    fn start_book(
        &self,
        owner: u64,
        epoch: u64,
        symbol: &str,
        revision: u64,
        token: u64,
        market: &mut MarketState,
        spawner: &TaskSpawner,
        tx: &mpsc::Sender<Input>,
    ) -> Result<(), BackpackDataError> {
        self.prepare_book(owner, epoch, revision, symbol, token, market)?;
        self.snapshot(owner, epoch, symbol, revision, market, spawner, tx)
    }
    #[expect(clippy::too_many_arguments)]
    fn snapshot(
        &self,
        owner: u64,
        epoch: u64,
        symbol: &str,
        revision: u64,
        market: &mut MarketState,
        spawner: &TaskSpawner,
        tx: &mpsc::Sender<Input>,
    ) -> Result<(), BackpackDataError> {
        let cancel = spawner.cancellation_token();
        market.snapshot_cancel = Some(cancel.clone());
        let token = market.token;
        let mut parameters = BackpackParameters::default();
        parameters
            .insert("symbol", Some(BackpackScalar::Token(symbol.to_owned())))
            .map_err(|_| BackpackDataError::Configuration("snapshot symbol"))?;
        parameters
            .insert(
                "limit",
                Some(BackpackScalar::Unsigned(
                    self.config.lifecycle().depth_snapshot_limit as u64,
                )),
            )
            .map_err(|_| BackpackDataError::Configuration("snapshot limit"))?;
        let request = BackpackReadRequest::new(BackpackReadOperation::Depth, parameters)
            .map_err(|_| BackpackDataError::Configuration("snapshot request"))?;
        let http = self.http.clone();
        let gate = self.gate.clone();
        let subscriptions = self.subscriptions.clone();
        let topic = format!("depth.{symbol}");
        let symbol = symbol.to_owned();
        let tx = tx.clone();
        let cap = self.config.lifecycle().max_ws_message_bytes;
        spawner
            .spawn(async move {
                let admission = || {
                    let state = gate.lock();
                    state.admits(owner, epoch)
                        && state.book_tokens.get(&symbol) == Some(&token)
                        && state.revisions.get(&topic) == Some(&revision)
                        && subscriptions.all_topics().contains(&topic)
                };
                let response = http.read(&request, None, Some(&admission), &cancel).await;
                let received = now();
                let result = response
                    .map_err(|_| BackpackDataError::Transport)
                    .and_then(|r| {
                        if r.body().len() <= cap {
                            Ok(Bytes::copy_from_slice(r.body()))
                        } else {
                            Err(BackpackDataError::Replay)
                        }
                    });
                let message = Input::Snapshot(Snapshot {
                    epoch,
                    revision,
                    token,
                    symbol,
                    received,
                    result,
                });
                tokio::select! {()=cancel.cancelled()=>{},_ = tx.send(message)=>{}}
            })
            .map_err(|_| BackpackDataError::Lifecycle("snapshot task admission"))
    }
}

fn serialize_books<S: serde::Serializer>(
    books: &BTreeMap<String, Option<BackpackBookCoverage>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::Serialize;
    let view=books.iter().map(|(symbol,coverage)|(symbol,coverage.as_ref().map(|c|serde_json::json!({"snapshot_limit":c.snapshot_limit,"initial_bid_floor":c.initial_bid_floor.map(|p|p.to_string()),"initial_ask_ceiling":c.initial_ask_ceiling.map(|p|p.to_string())})))).collect::<BTreeMap<_,_>>();
    view.serialize(serializer)
}
