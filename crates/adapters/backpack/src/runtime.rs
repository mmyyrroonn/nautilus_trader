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

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use nautilus_common::messages::DataEvent;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::task::TaskSpawner;
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, OrderBookDeltas, QuoteTick},
    enums::{BookAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
    types::{Price, Quantity},
};
use nautilus_network::{
    SocketState, SocketStateSink,
    transport::Message,
    websocket::{EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig},
};
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

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

/// A bounded, sanitized public fault observation retained after recovery.
#[derive(Clone, Debug, serde::Serialize)]
pub struct BackpackPublicFault {
    /// Wall-clock fault timestamp, serialized exactly for JSON consumers.
    pub unix_ns: String,
    /// Transport epoch in which this transition occurred.
    pub epoch: u64,
    /// Fixed diagnostic reason without payloads or credentials.
    pub reason: &'static str,
    /// Queue occupancy most recently observed by the producer or consumer.
    pub queued_frames: usize,
    /// Largest queue wait observed before this transition.
    pub max_queue_wait_ns: u64,
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
    /// Most recent public fault, retained after successful recovery.
    pub last_fault_reason: Option<&'static str>,
    /// Connection epoch on which the retained public fault was observed.
    pub last_fault_epoch: u64,
    /// Recovery budgets exhausted while the session remained closed to publication.
    pub recovery_timeouts: u64,
    /// Last 32 sanitized fault transitions, retained across connection recovery.
    pub fault_history: Vec<BackpackPublicFault>,
    /// Largest number of inputs waiting in the bounded public queue.
    pub input_queue_high_water: usize,
    /// Inputs refused by the bounded public queue.
    pub input_queue_overflows: u64,
    /// Largest monotonic receive-to-consume delay in this client run.
    pub max_queue_wait_ns: u64,
    /// Per-symbol BBO freshness, independent of book depth coverage.
    pub quotes_fresh: BTreeMap<String, bool>,
    /// Positive subscription acknowledgements have not been established by official evidence.
    pub subscription_acknowledgements_verified: bool,
    /// Public observations never establish execution readiness.
    pub execution_ready: bool,
    /// Sequence continuity of each currently subscribed bounded book.
    pub books_continuous: BTreeMap<String, bool>,
    /// Both engine event and original receipt ages are within the operational idle policy.
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
    pub last_fault_reason: Option<&'static str>,
    pub last_fault_epoch: u64,
    pub recovery_timeouts: u64,
    pub fault_history: VecDeque<BackpackPublicFault>,
    pub queued_frames: usize,
    pub input_queue_high_water: usize,
    pub input_queue_overflows: u64,
    pub max_queue_wait_ns: u64,
    pub lost_at: Option<nautilus_network::dst::time::Instant>,
    pub revisions: BTreeMap<String, u64>,
    pub quote_cache_only: bool,
    pub quote_receipts: BTreeMap<String, (UnixNanos, UnixNanos)>,
    pub latest_quotes: BTreeMap<String, QuoteTick>,
    pub books: BTreeMap<String, Option<BackpackBookCoverage>>,
    pub book_receipts: BTreeMap<String, UnixNanos>,
    pub book_tokens: BTreeMap<String, u64>,
    pub instruments: Vec<InstrumentAny>,
}
impl Gate {
    pub(crate) fn invalidate(&mut self, reason: &'static str) {
        if !matches!(
            reason,
            "initial bootstrap"
                | "replacement bootstrap"
                | "session stopped"
                | "stopped"
                | "reset"
                | "disposed"
                | "owner dropped"
                | "disconnected"
                | "recovery deadline"
        ) && (reason != "transport disconnected" || self.connected)
        {
            if self.stale != Some(reason) || self.last_fault_epoch != self.epoch {
                if self.fault_history.len() == 32 {
                    self.fault_history.pop_front();
                }
                self.fault_history.push_back(BackpackPublicFault {
                    unix_ns: now().as_u64().to_string(),
                    epoch: self.epoch,
                    reason,
                    queued_frames: self.queued_frames,
                    max_queue_wait_ns: self.max_queue_wait_ns,
                });
            }
            self.last_fault_reason = Some(reason);
            self.last_fault_epoch = self.epoch;
        }
        match self.fault_serial.checked_add(1) {
            Some(v) => self.fault_serial = v,
            None => self.running = false,
        }
        self.connected = false;
        self.metadata_ready = false;
        self.stale = Some(reason);
        self.quote_receipts.clear();
        self.latest_quotes.clear();
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
    fn needs_reconnect(&self, owner: u64, actor_epoch: u64) -> bool {
        self.running
            && self.owner == owner
            && !self.admits(owner, actor_epoch)
            // The network controller already owns recovery of transport loss
            && self.stale != Some("transport disconnected")
            // A queued replacement bootstrap owns recovery of this new epoch.
            // Other faults still require recovery if that control message was lost.
            && !(self.epoch > actor_epoch && self.stale == Some("replacement bootstrap"))
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
            last_fault_reason: self.last_fault_reason,
            last_fault_epoch: self.last_fault_epoch,
            recovery_timeouts: self.recovery_timeouts,
            fault_history: self.fault_history.iter().cloned().collect(),
            input_queue_high_water: self.input_queue_high_water,
            input_queue_overflows: self.input_queue_overflows,
            max_queue_wait_ns: self.max_queue_wait_ns,
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
    received_at: nautilus_network::dst::time::Instant,
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
struct PendingDepthBatch {
    revision: u64,
    instrument_id: InstrumentId,
    first_queued_at: nautilus_network::dst::time::Instant,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
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
    pending_depth: Option<PendingDepthBatch>,
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
    last_data: Option<nautilus_network::dst::time::Instant>,
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
                                received_at: nautilus_network::dst::time::Instant::now(),
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

            if let Some(input) = input {
                let waiting = frame_tx.max_capacity() - frame_tx.capacity();
                state.queued_frames = waiting;
                state.input_queue_high_water = state
                    .input_queue_high_water
                    .max((waiting + 1).min(frame_tx.max_capacity()));
                if frame_tx.try_send(input).is_err() {
                    state.input_queue_overflows = state.input_queue_overflows.saturating_add(1);
                    state.invalidate("public frame queue overflow");
                    notify.notify_one();
                }
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
            .connect_timeout_ms(policy.ws_connect_timeout_secs * 1000)
            .reconnect_delay_initial_ms(100)
            .reconnect_delay_max_ms(2000)
            // A long-running public feed must survive more than 20 short connections,
            // attempts retain the transport's capped backoff and handshake timeout.
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
            last_data: None,
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
                    pending_depth: None,
                },
            );
        }

        let mut reconcile_pending = true;
        let mut reconcile_after = None;

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
                // Bound each bootstrap budget without abandoning the owning task,
                // no observations become admissible until a fresh epoch is bootstrapped.
                let mut state = self.gate.lock();
                state.invalidate("recovery deadline");
                state.recovery_timeouts = state.recovery_timeouts.saturating_add(1);
                state.lost_at = Some(nautilus_network::dst::time::Instant::now());
                drop(state);
                let _ = ws.reconnect_handle().request_reconnect();
            }

            if ready
                && reconcile_pending
                && rx.len() <= self.config.lifecycle().max_buffer_frames / 8
                && reconcile_after
                    .is_none_or(|deadline| nautilus_network::dst::time::Instant::now() >= deadline)
            {
                match self
                    .reconcile(
                        owner,
                        actor.epoch,
                        &ws,
                        &mut actor.markets,
                        &mut actor.sent,
                        &mut actor.last_data,
                        &mut actor.generation,
                        &spawner,
                        &tx,
                    )
                    .await
                {
                    Ok(pending) => {
                        reconcile_pending = pending;
                        reconcile_after = Some(
                            nautilus_network::dst::time::Instant::now() + Duration::from_millis(20),
                        );
                    }
                    Err(_) => self.fault(owner, actor.epoch, "subscription send failed", &ws),
                }
            }
            let coalesce_ms = self.config.lifecycle().depth_coalesce_ms;
            let depth_sleep = if coalesce_ms > 0 {
                self.flush_due_depth(owner, &mut actor);
                let window = Duration::from_millis(coalesce_ms);
                actor
                    .markets
                    .values()
                    .filter_map(|m| {
                        m.pending_depth
                            .as_ref()
                            .map(|p| window.saturating_sub(p.first_queued_at.elapsed()))
                    })
                    .min()
                    .map_or(Duration::from_millis(100), |d| {
                        d.min(Duration::from_millis(100))
                    })
            } else {
                Duration::from_millis(100)
            };
            let input = tokio::select! {
                biased;
                ()=cancel.cancelled()=>break,
                ()=self.notify.notified()=> {
                    reconcile_pending = true;
                    let needs_reconnect = self.gate.lock().needs_reconnect(owner,actor.epoch);
                    if needs_reconnect {
                        let _=ws.reconnect_handle().request_reconnect();
                    }
                    None
                },
                ()=nautilus_network::dst::time::sleep(depth_sleep)=>None,
                input=rx.recv()=>match input {Some(input)=>Some(input),None=>break},
            };

            match input {
                Some(Input::Reconnected(epoch)) => {
                    self.rebootstrap(owner, &ws, &mut actor, epoch, &cancel)
                        .await;
                    reconcile_pending = true;
                    reconcile_after = None;
                }
                Some(Input::Frame(frame)) => {
                    self.frame(owner, &ws, &mut actor, &frame, &spawner, &tx, rx.len());
                }
                Some(Input::Snapshot(snapshot)) => self.install(owner, &ws, &mut actor, snapshot),
                None => {}
            }
            if coalesce_ms > 0 {
                self.flush_due_depth(owner, &mut actor);
            }
            // Heartbeats cover an unsubscribed connection. Business-data silence
            // becomes a fault only after a subscription was sent on this epoch.
            if self.gate.lock().admits(owner, actor.epoch)
                && actor.last_data.is_some_and(|last| {
                    last.elapsed()
                        >= Duration::from_secs(self.config.lifecycle().ws_idle_timeout_secs)
                })
            {
                self.fault(owner, actor.epoch, "subscribed data idle timeout", &ws);
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
        actor.last_data = None;
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
                    pending_depth: None,
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
        queued_frames: usize,
    ) {
        let epoch = actor.epoch;
        let queue_wait_ns = frame
            .received_at
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        let admitted = {
            // Reuse the required admission lock; diagnostics must not add a
            // second producer/consumer lock handoff for every incoming frame.
            let mut state = self.gate.lock();
            state.queued_frames = queued_frames;
            state.max_queue_wait_ns = state.max_queue_wait_ns.max(queue_wait_ns);
            frame.epoch == epoch
                && self.admits_topic(&state, owner, epoch, &frame.topic, frame.revision)
        };
        if !admitted {
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

        if decoded.is_ok() && actor.sent.get(&frame.topic) == Some(&frame.revision) {
            actor.last_data = Some(
                actor
                    .last_data
                    .map_or(frame.received_at, |last| last.max(frame.received_at)),
            );
        }
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
                    state.latest_quotes.insert(symbol.to_owned(), quote);

                    if !self.config.lifecycle().quote_cache_only
                        && self
                            .sender
                            .send(DataEvent::Data(Data::Quote(quote)))
                            .is_err()
                    {
                        state.invalidate("data engine unavailable");
                        state.running = false;
                    }
                } else {
                    state.quote_receipts.remove(symbol);
                    state.latest_quotes.remove(symbol);
                }
            }
            Ok(BackpackPublicEvent::QuoteUnavailable { .. }) => {
                let mut state = self.gate.lock();
                if self.admits_topic(&state, owner, epoch, &frame.topic, frame.revision) {
                    state.quote_receipts.remove(symbol);
                    state.latest_quotes.remove(symbol);
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
                if !self.fresh(update.ts_event) {
                    self.fault(owner, epoch, "stale or future depth event", ws);
                    return;
                }
                let last = update.last;
                let observed_at = update.ts_event.min(frame.received);
                let advances = market.last_sequence.is_none_or(|previous| last > previous);
                let instrument_id = market.metadata.instrument_id;
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
                                batch.ts_init.min(batch.ts_event),
                            );
                            if self.config.lifecycle().depth_coalesce_ms == 0 {
                                self.publish(
                                    owner,
                                    epoch,
                                    &frame.topic,
                                    frame.revision,
                                    Data::from(batch),
                                );
                            } else {
                                let entry =
                                    market
                                        .pending_depth
                                        .get_or_insert_with(|| PendingDepthBatch {
                                            revision: frame.revision,
                                            instrument_id,
                                            first_queued_at:
                                                nautilus_network::dst::time::Instant::now(),
                                            sequence: batch.sequence,
                                            ts_event: batch.ts_event,
                                            ts_init: batch.ts_init,
                                            bids: BTreeMap::new(),
                                            asks: BTreeMap::new(),
                                        });
                                if entry.revision != frame.revision {
                                    *entry = PendingDepthBatch {
                                        revision: frame.revision,
                                        instrument_id,
                                        first_queued_at: nautilus_network::dst::time::Instant::now(
                                        ),
                                        sequence: batch.sequence,
                                        ts_event: batch.ts_event,
                                        ts_init: batch.ts_init,
                                        bids: BTreeMap::new(),
                                        asks: BTreeMap::new(),
                                    };
                                }
                                entry.sequence = batch.sequence;
                                entry.ts_event = batch.ts_event;
                                entry.ts_init = batch.ts_init;
                                for delta in batch.deltas {
                                    let side_map = match delta.order.side {
                                        Some(OrderSide::Buy) => &mut entry.bids,
                                        Some(OrderSide::Sell) => &mut entry.asks,
                                        None => continue,
                                    };
                                    let size = if delta.action == BookAction::Delete
                                        || delta.order.size.is_zero()
                                    {
                                        Quantity::zero(delta.order.size.precision)
                                    } else {
                                        delta.order.size
                                    };
                                    side_map.insert(delta.order.price, size);
                                }
                            }
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
                                    observed_at,
                                );
                            }
                        }
                        Err(_) => {
                            market.pending_depth = None;
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

        market.pending_depth = None;
        if let Some(book) = market.book.as_mut() {
            match book.install_snapshot(snapshot.token, &body, snapshot.received) {
                Ok(batch) => {
                    if !self.fresh(batch.ts_init) || !self.fresh(batch.ts_event) {
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
                            batch.ts_init.min(batch.ts_event),
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
            && topic.split_once('.').is_some_and(|(channel, symbol)| {
                self.subscriptions
                    .is_subscribed(&Ustr::from(channel), &Ustr::from(symbol))
            })
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
    fn flush_due_depth(&self, owner: u64, actor: &mut ActorState) {
        let coalesce_ms = self.config.lifecycle().depth_coalesce_ms;
        if coalesce_ms == 0 {
            return;
        }
        let window = Duration::from_millis(coalesce_ms);
        let due_symbols: Vec<String> = actor
            .markets
            .iter()
            .filter_map(|(symbol, market)| {
                market
                    .pending_depth
                    .as_ref()
                    .filter(|pending| pending.first_queued_at.elapsed() >= window)
                    .map(|_| symbol.clone())
            })
            .collect();
        for symbol in due_symbols {
            let Some(market) = actor.markets.get_mut(&symbol) else {
                continue;
            };
            let Some(pending) = market.pending_depth.take() else {
                continue;
            };
            let count = pending.bids.len() + pending.asks.len();
            if count == 0 {
                continue;
            }
            let mut deltas = Vec::with_capacity(count);
            for (price, size) in pending.bids.into_iter().rev() {
                if let Ok(delta) = OrderBookDelta::new_checked(
                    pending.instrument_id,
                    if size.is_zero() {
                        BookAction::Delete
                    } else {
                        BookAction::Update
                    },
                    BookOrder::new(OrderSide::Buy, price, size, 0),
                    0,
                    pending.sequence,
                    pending.ts_event,
                    pending.ts_init,
                ) {
                    deltas.push(delta);
                }
            }
            for (price, size) in pending.asks {
                if let Ok(delta) = OrderBookDelta::new_checked(
                    pending.instrument_id,
                    if size.is_zero() {
                        BookAction::Delete
                    } else {
                        BookAction::Update
                    },
                    BookOrder::new(OrderSide::Sell, price, size, 0),
                    0,
                    pending.sequence,
                    pending.ts_event,
                    pending.ts_init,
                ) {
                    deltas.push(delta);
                }
            }
            if let Some(last) = deltas.last_mut() {
                last.flags |= RecordFlag::F_LAST as u8;
            }
            if let Ok(batch) = OrderBookDeltas::new_checked(pending.instrument_id, deltas) {
                self.publish(
                    owner,
                    actor.epoch,
                    &format!("depth.{symbol}"),
                    pending.revision,
                    Data::from(batch),
                );
            }
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
        last_data: &mut Option<nautilus_network::dst::time::Instant>,
        generation: &mut u64,
        spawner: &TaskSpawner,
        tx: &mpsc::Sender<Input>,
    ) -> Result<bool, BackpackDataError> {
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

        // Yield between bounded command batches so a subscription burst cannot
        // monopolize the actor while its bounded market-data queue fills up.
        let mut commands = 0;

        for topic in removed {
            if commands == 8 {
                return Ok(true);
            }
            ws.send_text_on_connection(
                serde_json::json!({"method":"UNSUBSCRIBE","params":[topic]}).to_string(),
                None,
                epoch,
            )
            .await
            .map_err(|_| BackpackDataError::Transport)?;
            sent.remove(&topic);
            commands += 1;
            if sent.is_empty() {
                *last_data = None;
            }
            if let Some(symbol) = topic.strip_prefix("depth.") {
                if let Some(m) = markets.get_mut(symbol) {
                    m.book = None;
                    m.pending_depth = None;
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
            if commands == 8 {
                return Ok(true);
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
            commands += 1;
            last_data.get_or_insert_with(nautilus_network::dst::time::Instant::now);
            if let Some(symbol) = topic.strip_prefix("depth.") {
                let market = markets.get_mut(symbol).ok_or(BackpackDataError::Metadata)?;
                self.snapshot(owner, epoch, symbol, revision, market, spawner, tx)?;
            }
        }
        Ok(false)
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
        market.pending_depth = None;
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
                        && subscriptions
                            .is_subscribed(&Ustr::from("depth"), &Ustr::from(symbol.as_str()))
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

#[cfg(test)]
mod reconnect_tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[tokio::test]
    async fn transport_recovery_notification_does_not_replace_an_already_active_socket(
        #[case] gate_epoch: u64,
    ) {
        let notify = Notify::new();
        let mut gate = Gate {
            owner: 1,
            epoch: gate_epoch,
            running: true,
            ..Gate::default()
        };
        gate.invalidate("transport disconnected");
        notify.notify_one();

        // The controller can activate its replacement before delivering RECONNECTED,
        // so the adapter gate may still name either the old or the replacement epoch.
        let transport_mode = nautilus_network::mode::ConnectionMode::Active;
        notify.notified().await;

        assert!(transport_mode.is_active());
        assert!(!gate.admits(1, 1));
        assert!(!gate.needs_reconnect(1, 1));

        gate.invalidate("malformed public frame");
        assert!(gate.needs_reconnect(1, 1));
    }

    #[tokio::test]
    async fn retained_disconnect_notification_does_not_replace_queued_reconnected_epoch() {
        let notify = Notify::new();
        let (tx, mut rx) = mpsc::channel(1);
        let mut gate = Gate {
            owner: 1,
            epoch: 1,
            running: true,
            connected: true,
            metadata_ready: true,
            ..Gate::default()
        };
        gate.invalidate("transport disconnected");
        notify.notify_one();
        gate.epoch = 2;
        gate.invalidate("replacement bootstrap");
        assert!(tx.try_send(Input::Reconnected(2)).is_ok());

        let input = tokio::select! {
            biased;
            () = notify.notified() => None,
            input = rx.recv() => input,
        };

        assert!(input.is_none());
        assert!(!gate.admits(1, 1));
        assert!(!gate.needs_reconnect(1, 1));
        assert!(matches!(rx.try_recv(), Ok(Input::Reconnected(2))));

        // A real fault during the pending bootstrap must still request recovery
        gate.invalidate("malformed public frame");
        assert!(gate.needs_reconnect(1, 1));
    }

    #[rstest]
    fn public_fault_history_is_bounded_and_survives_recovery() {
        let mut gate = Gate {
            running: true,
            queued_frames: 64,
            max_queue_wait_ns: 123,
            ..Gate::default()
        };
        for epoch in 0..40 {
            gate.epoch = epoch;
            gate.connected = true;
            gate.stale = None;
            gate.invalidate("public frame queue overflow");
            gate.invalidate("public frame queue overflow");
        }
        gate.invalidate("replacement bootstrap");
        gate.connected = true;
        gate.metadata_ready = true;
        gate.stale = None;
        let health = gate.health(60);
        assert_eq!(health.fault_history.len(), 32);
        assert_eq!(health.fault_history[0].epoch, 8);
        assert_eq!(health.fault_history[31].epoch, 39);
        assert_eq!(health.fault_history[31].queued_frames, 64);
        assert_eq!(health.fault_history[31].max_queue_wait_ns, 123);
        assert!(
            health
                .fault_history
                .iter()
                .all(|fault| fault.unix_ns.parse::<u64>().unwrap() > 0)
        );
        assert!(health.connected);
    }

    #[tokio::test]
    async fn lost_reconnected_control_message_still_requests_recovery() {
        let notify = Notify::new();
        let (tx, mut rx) = mpsc::channel(1);
        let mut gate = Gate {
            owner: 1,
            epoch: 2,
            running: true,
            ..Gate::default()
        };
        assert!(tx.try_send(Input::Reconnected(1)).is_ok());
        gate.invalidate("replacement bootstrap");
        assert!(tx.try_send(Input::Reconnected(2)).is_err());
        gate.invalidate("public frame queue overflow");
        notify.notify_one();

        let input = tokio::select! {
            biased;
            () = notify.notified() => None,
            input = rx.recv() => input,
        };

        assert!(input.is_none());
        assert!(!gate.current(1, 1));
        assert!(gate.needs_reconnect(1, 1));
        assert!(matches!(rx.try_recv(), Ok(Input::Reconnected(1))));
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    #[case(1, 1, true, true, true, None, false)]
    #[case(1, 1, true, false, true, Some("transport disconnected"), false)]
    #[case(1, 1, true, true, false, Some("metadata refresh failed"), true)]
    #[case(1, 2, true, false, false, Some("replacement bootstrap"), false)]
    #[case(1, 2, true, false, false, Some("transport disconnected"), false)]
    #[case(1, 2, true, false, false, Some("malformed public frame"), true)]
    #[case(1, 2, true, false, false, Some("public frame queue overflow"), true)]
    #[case(1, 2, false, false, false, Some("transport disconnected"), false)]
    #[case(2, 2, true, false, false, Some("transport disconnected"), false)]
    fn reconnect_notification_respects_owner_epoch_and_actual_fault(
        #[case] owner: u64,
        #[case] epoch: u64,
        #[case] running: bool,
        #[case] connected: bool,
        #[case] metadata_ready: bool,
        #[case] stale: Option<&'static str>,
        #[case] expected: bool,
    ) {
        let gate = Gate {
            owner,
            epoch,
            running,
            connected,
            metadata_ready,
            stale,
            ..Gate::default()
        };
        assert_eq!(gate.needs_reconnect(1, 1), expected);
    }
}
