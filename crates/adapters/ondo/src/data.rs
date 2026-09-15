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

//! The Nautilus [`DataClient`] for the Ondo Perps public feed.
//!
//! The client owns three things and delegates everything else:
//!
//! - **market metadata** ([`OndoMarketMetadata`]) read once through Task 1's single conversion
//!   boundary (`GET /v1/markets` -> [`parse_instruments`]) and refreshed on the plan's low-priority
//!   60 second interval. A refresh that fails, or that reports a different market status, keeps the
//!   previous version and marks it stale: the last known good metadata is never silently replaced;
//! - **the subscription surface**, which this client maps onto the five public channels and hands to
//!   the Stage A transport through its session commands;
//! - **publication**, which is one consumer task forwarding every [`WsOutcome`] the transport
//!   produces into the data engine as a [`DataEvent`].
//!
//! # What this client never does
//!
//! It reads no API key, loads no `.env`, logs no secret, and constructs no execution client: it is
//! the public market data client of a venue whose public feed needs no credentials
//! (plan §4.1, §4.4).
//!
//! # The invalidation chain (plan §4.2)
//!
//! A disconnect is not a flag this adapter keeps to itself. The session ends the connection, moves
//! every book to the next session and emits one local feed state per subscribed instrument
//! ([`crate::websocket::REASON_DISCONNECTED`], `is_quoting = false`); the publisher task here forwards
//! it into the data engine, which publishes it on the message bus, where a subscriber sees it. The
//! corresponding [`crate::websocket::REASON_SNAPSHOT_READY`] state follows only when a snapshot for the
//! new session has been accepted, so a reconnect on its own never restores readiness. Both are local
//! feed states: their action is `None` and they never claim the venue halted a market.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::runner::get_data_event_sender,
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeBookDepth10, SubscribeFundingRates, SubscribeInstrument,
            SubscribeInstrumentStatus, SubscribeInstruments, SubscribeMarkPrices, SubscribeQuotes,
            SubscribeTrades, UnsubscribeBookDeltas, UnsubscribeBookDepth10,
            UnsubscribeFundingRates, UnsubscribeInstrument, UnsubscribeInstrumentStatus,
            UnsubscribeMarkPrices, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
    providers::{InstrumentProvider, InstrumentStore},
};
use nautilus_core::{
    UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::{ONDO_METADATA_REFRESH_INTERVAL_SECS, ONDO_VENUE},
        enums::MarketStatus,
    },
    config::OndoDataClientConfig,
    http::{
        client::OndoHttpClient,
        error::OndoHttpError,
        models::{MarketInfo, MarketsResponse, parse_instruments, parse_markets},
        query::{MARKETS_PATH, OndoRequestTarget},
        rate_limit::OndoRateBudget,
    },
    recording::{MetadataSnapshot, RawMdRecorder, RawMdRecorderConfig, RawMdSink, RecordingStats},
    websocket::{
        client::{OndoWebSocketClient, WsOutcome},
        messages::WsChannel,
    },
};

/// The venue's market metadata as the last accepted read returned it.
///
/// A version is *accepted* only when the read succeeded and the venue's view of the tradable set is
/// unchanged. An accepted read replaces the instruments and advances [`Self::version`]; a failed read
/// or a changed status keeps the previous instruments and version and records why the version is
/// [`Self::is_stale`], so a consumer can gate new orders on the flag without ever losing the last
/// known good metadata.
#[derive(Debug, Default)]
pub struct OndoMarketMetadata {
    version: u64,
    instruments: Vec<InstrumentAny>,
    statuses: AHashMap<InstrumentId, MarketStatus>,
    stale_reason: Option<String>,
}

/// What a metadata read did to [`OndoMarketMetadata`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataRefresh {
    /// The first accepted version.
    Initial,
    /// The venue's view is unchanged: the version advanced.
    Refreshed,
    /// The read failed or the venue's view changed: the previous version is kept and marked stale.
    KeptPrevious {
        /// Why the previous version was kept.
        reason: String,
    },
}

impl OndoMarketMetadata {
    /// Creates empty metadata with no accepted version.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the accepted version, which is `0` before the first accepted read.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the instruments of the last accepted version.
    #[must_use]
    pub fn instruments(&self) -> &[InstrumentAny] {
        &self.instruments
    }

    /// Returns `true` when a read failed or changed the venue's view after the accepted version.
    #[must_use]
    pub const fn is_stale(&self) -> bool {
        self.stale_reason.is_some()
    }

    /// Returns why the accepted version is stale, when it is.
    #[must_use]
    pub fn stale_reason(&self) -> Option<&str> {
        self.stale_reason.as_deref()
    }

    /// Returns the accepted status of a market, when the accepted version has it.
    #[must_use]
    pub fn status(&self, instrument_id: &InstrumentId) -> Option<MarketStatus> {
        self.statuses.get(instrument_id).copied()
    }

    /// Returns whether new orders may be sent for every instrument of the accepted version.
    ///
    /// A stale version answers `false`: with the venue's view in doubt the adapter must not claim a
    /// market is tradable.
    #[must_use]
    pub fn is_tradable(&self, instrument_id: &InstrumentId) -> bool {
        !self.is_stale() && self.status(instrument_id) == Some(MarketStatus::Active)
    }

    /// Records a successful metadata read.
    ///
    /// `infos` is the venue's view of every market in the payload; `instruments` are the instruments
    /// this adapter selected from it. Only the selected instruments get a recorded status, so
    /// [`Self::status`] and [`Self::is_tradable`] can never answer for a market this adapter did not
    /// load and never published.
    pub fn apply(
        &mut self,
        infos: &[MarketInfo],
        instruments: Vec<InstrumentAny>,
    ) -> MetadataRefresh {
        // The venue's view of a market this adapter did not select is not this adapter's business:
        // gating the statuses on the loaded set is what keeps `is_tradable` from claiming an
        // instrument that no consumer of this client ever received.
        let loaded: AHashSet<InstrumentId> = instruments
            .iter()
            .map(|instrument| instrument.id())
            .collect();
        let statuses: AHashMap<InstrumentId, MarketStatus> = infos
            .iter()
            .filter(|info| loaded.contains(&info.instrument_id()))
            .map(|info| (info.instrument_id(), info.status()))
            .collect();

        if self.version == 0 {
            self.version = 1;
            self.instruments = instruments;
            self.statuses = statuses;
            self.stale_reason = None;

            return MetadataRefresh::Initial;
        }

        if let Some(reason) = self.change_against(&statuses) {
            self.stale_reason = Some(reason.clone());

            return MetadataRefresh::KeptPrevious { reason };
        }

        self.version += 1;
        self.instruments = instruments;
        self.statuses = statuses;
        self.stale_reason = None;

        MetadataRefresh::Refreshed
    }

    /// Records that a metadata read failed.
    pub fn mark_stale(&mut self, reason: impl Into<String>) -> MetadataRefresh {
        let reason = reason.into();
        self.stale_reason = Some(reason.clone());

        MetadataRefresh::KeptPrevious { reason }
    }

    /// Returns why `statuses` differs from the accepted version, when it does.
    fn change_against(&self, statuses: &AHashMap<InstrumentId, MarketStatus>) -> Option<String> {
        for (instrument_id, status) in statuses {
            match self.statuses.get(instrument_id) {
                Some(accepted) if accepted == status => {}
                Some(accepted) => {
                    return Some(format!(
                        "market status changed for `{instrument_id}`: {accepted:?} -> {status:?}",
                    ));
                }
                None => {
                    return Some(format!(
                        "the venue published a market this adapter did not have: `{instrument_id}`"
                    ));
                }
            }
        }

        for instrument_id in self.statuses.keys() {
            if !statuses.contains_key(instrument_id) {
                return Some(format!(
                    "the venue no longer publishes `{instrument_id}`, which this adapter holds"
                ));
            }
        }

        None
    }
}

/// The public market data client for Ondo Perps.
///
/// See the module documentation for what it owns, what it never does, and how the invalidation chain
/// reaches a subscriber.
#[derive(Debug)]
pub struct OndoDataClient {
    client_id: ClientId,
    config: OndoDataClientConfig,
    http_client: OndoHttpClient,
    store: InstrumentStore,
    metadata: Arc<Mutex<OndoMarketMetadata>>,
    ws: Option<OndoWebSocketClient>,
    publisher: Option<tokio::task::JoinHandle<()>>,
    refresh: Option<tokio::task::JoinHandle<()>>,
    cancellation: CancellationToken,
    depth10: AHashSet<InstrumentId>,
    statuses: AHashSet<InstrumentId>,
    raw_recording: Option<RawMdRecorder>,
    raw_recording_stats: Option<RecordingStats>,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl OndoDataClient {
    /// Creates a data client for `config`, drawing on `budget` for every REST read.
    ///
    /// `budget` is the shared per-environment REST budget (plan §4.4): one process has one budget, so
    /// the data client and any execution client built for the same environment pace against the same
    /// bucket instead of each holding their own.
    ///
    /// No connection is attempted here: [`DataClient::connect`] loads the metadata and starts the
    /// transport.
    ///
    /// # Errors
    ///
    /// Returns an error if the REST transport cannot be built.
    ///
    /// # Panics
    ///
    /// Panics if the process has no data event sender, which only happens when the client is built
    /// outside a running node (the same contract every live adapter's data client has).
    pub fn new(
        client_id: ClientId,
        config: OndoDataClientConfig,
        budget: OndoRateBudget,
    ) -> anyhow::Result<Self> {
        let http_client = OndoHttpClient::builder()
            .base_url(config.http_base_url().to_string())
            .timeout_secs(config.http_timeout_secs)
            .budget(budget)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to create the Ondo REST client: {e}"))?;

        Ok(Self {
            client_id,
            config,
            http_client,
            store: InstrumentStore::new(),
            metadata: Arc::new(Mutex::new(OndoMarketMetadata::new())),
            ws: None,
            publisher: None,
            refresh: None,
            cancellation: CancellationToken::new(),
            depth10: AHashSet::new(),
            statuses: AHashSet::new(),
            raw_recording: None,
            raw_recording_stats: None,
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
        })
    }

    /// Returns whether this adapter implements the on-demand `depth10` subscription.
    ///
    /// This is `true`, and the registry consumer must read it here rather than assume either answer:
    /// [`DataClient::subscribe_book_depth10`] is served by a projection taken from the **same** book
    /// subscription and cache the deltas come from, so it never opens a second connection and can
    /// never unsubscribe a subscription another consumer is using (plan §4.2).
    #[must_use]
    pub const fn supports_depth10() -> bool {
        true
    }

    /// Returns the REST budget every read of this client draws on.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        self.http_client.budget()
    }

    /// Returns the accepted metadata version.
    #[must_use]
    pub fn metadata_version(&self) -> u64 {
        self.lock_metadata().version()
    }

    /// Returns whether the accepted metadata version is stale.
    #[must_use]
    pub fn is_metadata_stale(&self) -> bool {
        self.lock_metadata().is_stale()
    }

    /// Returns why the accepted metadata version is stale, when it is.
    #[must_use]
    pub fn metadata_stale_reason(&self) -> Option<String> {
        self.lock_metadata().stale_reason().map(str::to_string)
    }

    /// Returns whether new orders may be sent for `instrument_id` under the accepted metadata.
    #[must_use]
    pub fn is_market_tradable(&self, instrument_id: &InstrumentId) -> bool {
        self.lock_metadata().is_tradable(instrument_id)
    }

    /// Returns the venue this client is associated with.
    #[must_use]
    pub fn venue(&self) -> Venue {
        *ONDO_VENUE
    }

    /// Returns the statistics of the raw public-frame recording, when one is configured.
    ///
    /// This is how a recording failure is read rather than only logged: `failed` names why an aborted
    /// recording stopped, `dropped` counts the frames a full queue shed, and `clean` is `true` only
    /// for a run that ended with its `run_end` record, dropped nothing and failed at nothing. It is
    /// [`None`] while the configuration carries no `raw_md_path`, which is the same condition under
    /// which no directory, file or thread exists.
    #[must_use]
    pub fn raw_recording_stats(&self) -> Option<RecordingStats> {
        self.raw_recording
            .as_ref()
            .map(RawMdRecorder::stats)
            .or_else(|| self.raw_recording_stats.clone())
    }

    /// Starts the raw public-frame recording when the configuration names a directory.
    ///
    /// With `raw_md_path == None` this does nothing at all: no directory is created, no file is
    /// opened and no writer thread is started. With a path, the directory is created if it is
    /// missing (the application normally creates it first, and an existing empty directory is what
    /// this tolerates), and the run identity comes from [`raw_recorder_config`]: the configured
    /// `raw_md_run_id` when the application set one, otherwise the directory-derived fallback, which
    /// warns once because it only joins the tape when the run directory is named after the run.
    ///
    /// # Errors
    ///
    /// Returns an error when the recording cannot start, which fails the connection: a recording the
    /// operator asked for and this adapter could not begin is a start-up failure, never a silent
    /// absence.
    fn start_raw_recording(&self) -> anyhow::Result<Option<RawMdRecorder>> {
        let Some(config) = raw_recorder_config(&self.config) else {
            return Ok(None);
        };

        RawMdRecorder::start(config).map(Some)
    }

    /// Ends the raw recording, if one is running, and keeps what it did.
    ///
    /// An unclean end is reported here as well as being visible in [`Self::raw_recording_stats`]: a
    /// recording that failed or dropped frames is never left as a quiet log line.
    fn stop_raw_recording(&mut self) {
        let Some(mut recorder) = self.raw_recording.take() else {
            return;
        };

        let stats = recorder.finish();
        match (&stats.failed, stats.clean) {
            (Some(reason), _) => log::error!("The Ondo raw recording failed: {reason}"),
            (None, false) => log::error!(
                "The Ondo raw recording ended unclean: {} frame(s) dropped across {} gap(s)",
                stats.dropped,
                stats.gaps
            ),
            (None, true) => log::debug!(
                "The Ondo raw recording ended clean with {} record(s) in {} byte(s)",
                stats.records,
                stats.bytes
            ),
        }

        self.raw_recording_stats = Some(stats);
    }

    fn lock_metadata(&self) -> std::sync::MutexGuard<'_, OndoMarketMetadata> {
        // A poisoned lock means a previous holder panicked while holding it; the metadata is a plain
        // value with no invariant to break, so it is recovered rather than propagated.
        self.metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Publishes every instrument of the store to the data engine.
    fn publish_instruments(&self) {
        for instrument in self.store.list_all() {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::warn!("Failed to publish Ondo instrument: {e}");
            }
        }
    }

    /// Reads the market metadata and publishes exactly the selected instruments.
    async fn load_selected(&mut self, load_ids: &[InstrumentId]) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let recorder = self.raw_recording.as_ref().map(RawMdRecorder::sink);
        let (infos, instruments) =
            read_markets(&self.http_client, load_ids, ts_init, recorder.as_ref()).await?;

        self.store.clear();
        for instrument in &instruments {
            self.store.add(instrument.clone());
        }
        self.metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .apply(&infos, instruments.clone());

        self.publish_instruments();

        Ok(())
    }

    /// Routes a public subscription to the transport's session.
    fn route_subscribe(
        &self,
        channel: WsChannel,
        instrument_id: &InstrumentId,
        context: &str,
    ) -> anyhow::Result<()> {
        let ws = self
            .ws
            .as_ref()
            .context("the Ondo data client is not connected, so it cannot subscribe")?;
        ensure!(
            self.store.contains(instrument_id),
            "cannot {context} for `{instrument_id}`: it is not a market this Ondo data client loaded"
        );

        ws.subscribe(channel, &[*instrument_id]);

        Ok(())
    }

    /// Routes a channel unsubscribe to the transport's session.
    ///
    /// Unsubscribing is not gated on the instrument being loaded: the session reports a topic it does
    /// not know rather than this client refusing to release a subscription.
    fn route_unsubscribe(
        &self,
        channel: WsChannel,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<()> {
        let ws = self
            .ws
            .as_ref()
            .context("the Ondo data client is not connected")?;
        ws.unsubscribe(channel, &[*instrument_id]);

        Ok(())
    }

    /// Starts the transport, its outcome publisher and the metadata refresh task.
    ///
    /// The raw public-frame recording, when the configuration names a directory, is taken over here
    /// and its handle is handed to the transport: the transport offers it the frame of every inbound
    /// message and the body of every request it manages to send, and offers nothing when no recorder
    /// is configured.
    fn start_transport(&mut self) -> anyhow::Result<()> {
        let recorder: Option<RawMdSink> = self.raw_recording.as_ref().map(RawMdRecorder::sink);
        let (ws, out_rx) = OndoWebSocketClient::new_with_recorder(
            self.config.ws_url().to_string(),
            self.config.ws_heartbeat_secs,
            self.config.book_limit,
            // `depthLevels` is a price grouping, and this adapter has never confirmed one for a
            // market, so it is omitted rather than invented (plan §4.2).
            None,
            recorder.clone(),
        )?;

        // Registration precedes every subscription, so a frame for a loaded market is routed to its
        // instrument instead of being reported as a market this adapter does not track.
        for instrument in self.store.list_all() {
            ws.register_instrument(instrument);
        }

        self.cancellation = CancellationToken::new();
        let data_sender = self.data_sender.clone();
        self.publisher = Some(tokio::spawn(publish_outcomes(out_rx, data_sender)));
        self.refresh = Some(tokio::spawn(refresh_metadata(
            self.http_client.clone(),
            Arc::clone(&self.metadata),
            self.config.load_ids.clone(),
            self.cancellation.child_token(),
            self.data_sender.clone(),
            self.clock,
            recorder,
        )));
        self.ws = Some(ws);

        Ok(())
    }

    /// Cancels the refresh task and drops the transport without waiting for it.
    ///
    /// This is the local teardown of the synchronous trait methods. A deliberate local stop is not
    /// the adapter losing the venue: the feed states of plan §4.2 are published by
    /// [`DataClient::disconnect`], which can await the transport's own shutdown. The raw recording,
    /// when one is running, is ended here too and its statistics are kept.
    fn abort_tasks(&mut self) {
        self.cancellation.cancel();
        self.ws = None;

        if let Some(task) = self.publisher.take() {
            task.abort();
        }
        if let Some(task) = self.refresh.take() {
            task.abort();
        }

        self.depth10.clear();
        self.statuses.clear();
        self.stop_raw_recording();
    }
}

/// The raw recorder's configuration for `config`, when it names a `raw_md_path`.
///
/// The run id is the configured `raw_md_run_id` when the application set one - its process stamp,
/// which is the same string every tape record of the run carries, so the raw public frames and the
/// tape join *by construction* whatever the run directory is called. Without it the id is
/// [`RawMdRecorderConfig::derived_from_path`]'s answer (the raw directory's parent name) and the run
/// header says `run_id_source: "derived_from_path"`; that fallback agrees with the tape only when the
/// run directory is itself named after the run, so the warning names the path when it is taken.
fn raw_recorder_config(config: &OndoDataClientConfig) -> Option<RawMdRecorderConfig> {
    let path = config.raw_md_path.as_deref()?;
    let endpoint = config.ws_url().to_string();

    Some(match config.raw_md_run_id.as_deref() {
        Some(run_id) => {
            RawMdRecorderConfig::new(PathBuf::from(path), run_id, endpoint, config.environment)
        }
        None => {
            log::warn!(
                "The Ondo raw recording has no configured run id: deriving it from `{path}`'s \
                 parent directory name, which joins the tape only when the run directory is itself \
                 named after the run"
            );

            RawMdRecorderConfig::derived_from_path(
                PathBuf::from(path),
                endpoint,
                config.environment,
            )
        }
    })
}

#[async_trait(?Send)]
impl DataClient for OndoDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*ONDO_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::debug!("Starting {}", self.client_id);

        Ok(())
    }

    /// Stops the client locally; the `adapter:disconnected` feed state reaches subscribers only
    /// through [`DataClient::disconnect`], which can await the transport's own shutdown.
    ///
    /// `abort_tasks` cancels the publisher immediately, so this synchronous path publishes nothing:
    /// a caller that needs its subscribers to see the local feed state must call
    /// [`DataClient::disconnect`], not `stop`.
    fn stop(&mut self) -> anyhow::Result<()> {
        log::debug!("Stopping {}", self.client_id);
        self.abort_tasks();

        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting {}", self.client_id);
        self.abort_tasks();
        self.store.clear();
        *self.lock_metadata() = OndoMarketMetadata::new();

        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing {}", self.client_id);
        self.abort_tasks();

        Ok(())
    }

    fn is_connected(&self) -> bool {
        // The socket, not an intention: the transport reconnects on its own, and this reports what
        // the venue side of the feed actually is at the moment it is asked.
        self.ws
            .as_ref()
            .is_some_and(OndoWebSocketClient::is_connected)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.ws.is_some() {
            log::debug!("Already connected {}", self.client_id);

            return Ok(());
        }

        log::info!("Connecting {}", self.client_id);

        // The recording starts before the metadata is read, so the run's first metadata snapshot is
        // recorded as part of the run rather than being the one read that is missing from it.
        if self.raw_recording.is_none() {
            self.raw_recording = self.start_raw_recording()?;
        }

        // A start-up failure, never an empty success: the configured markets must all load.
        self.load_all(None)
            .await
            .context("the Ondo market metadata did not load")?;
        self.start_transport()?;

        log::info!(
            "Connected {} with {} instruments",
            self.client_id,
            self.store.count()
        );

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        log::info!("Disconnecting {}", self.client_id);

        self.cancellation.cancel();

        if let Some(mut ws) = self.ws.take() {
            // The transport ends the session on its way out, so every subscribed instrument gets its
            // `adapter:disconnected` feed state published before this returns.
            ws.stop().await;
        }

        if let Some(task) = self.publisher.take() {
            // The transport dropped its sender, so the publisher drains the last feed states and
            // returns; the wait is bounded so a shutdown can never hang on it.
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
        if let Some(task) = self.refresh.take() {
            task.abort();
        }

        self.depth10.clear();
        self.statuses.clear();
        // The transport has stopped, so the recording drains its queue, writes its end record and
        // stops. Whatever it did - clean, dropped or failed - is kept for the caller.
        self.stop_raw_recording();

        log::info!("Disconnected {}", self.client_id);

        Ok(())
    }

    fn subscribe_instruments(&mut self, _cmd: SubscribeInstruments) -> anyhow::Result<()> {
        // The venue has no instruments channel: the loaded metadata is the answer, so the currently
        // published set is re-sent rather than a subscription that cannot exist being accepted.
        self.publish_instruments();

        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        match self.store.find(&cmd.instrument_id) {
            Some(instrument) => {
                let instrument = instrument.clone();
                if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                    log::warn!("Failed to publish Ondo instrument: {e}");
                }
            }
            None => log::warn!(
                "Ondo does not publish `{}`, which this adapter did not load",
                cmd.instrument_id
            ),
        }

        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.route_subscribe(
            WsChannel::TopOfBooksPerps,
            &cmd.instrument_id,
            "subscribe to quotes",
        )
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.route_subscribe(
            WsChannel::TradesPerps,
            &cmd.instrument_id,
            "subscribe to trades",
        )
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        self.route_subscribe(
            WsChannel::DepthBooksPerps,
            &cmd.instrument_id,
            "subscribe to book deltas",
        )
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        let ws = self
            .ws
            .as_ref()
            .context("the Ondo data client is not connected, so it cannot subscribe")?;
        ensure!(
            self.store.contains(&cmd.instrument_id),
            "cannot subscribe to book depth10 for `{}`: it is not a market this Ondo data client \
             loaded",
            cmd.instrument_id
        );

        // Reference invariant: a depth10 subscription holds exactly **one** reference on the shared
        // `depthBooksPerps` topic, taken by `OndoWsSession::subscribe_depth10`, which subscribes the
        // book topic itself. `unsubscribe_book_depth10` releases exactly that one, so
        // `subscribe_book_deltas` + `subscribe_book_depth10` followed by both unsubscribes leaves the
        // topic at zero and the venue is really unsubscribed. Routing the depth10 through
        // `route_subscribe` as well would take a second reference this client never releases, which
        // would keep the venue subscribed forever and replay the market on every reconnect.
        if self.depth10.insert(cmd.instrument_id) {
            ws.subscribe_depth10(&cmd.instrument_id);
        }

        Ok(())
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        self.route_subscribe(
            WsChannel::FundingRatesPerps,
            &cmd.instrument_id,
            "subscribe to funding rates",
        )
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.route_subscribe(
            WsChannel::MarkPricesPerps,
            &cmd.instrument_id,
            "subscribe to mark prices",
        )
    }

    fn subscribe_instrument_status(
        &mut self,
        cmd: SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        // The local feed state of an instrument is derived from its book: `adapter:disconnected` is
        // emitted for the instruments of an ended session, and `adapter:snapshot_ready` when a new
        // session's snapshot is accepted. So the status feed state needs the book subscription, and
        // this takes one reference on that same topic rather than opening a channel the venue does
        // not have.
        self.route_subscribe(
            WsChannel::DepthBooksPerps,
            &cmd.instrument_id,
            "subscribe to instrument status",
        )?;

        if self.statuses.insert(cmd.instrument_id) {
            log::debug!(
                "Ondo instrument status for `{}` is derived from its book feed state",
                cmd.instrument_id
            );
        }

        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.route_unsubscribe(WsChannel::TopOfBooksPerps, &cmd.instrument_id)
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.route_unsubscribe(WsChannel::TradesPerps, &cmd.instrument_id)
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.route_unsubscribe(WsChannel::DepthBooksPerps, &cmd.instrument_id)
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        if !self.depth10.remove(&cmd.instrument_id) {
            return Ok(());
        }

        let ws = self
            .ws
            .as_ref()
            .context("the Ondo data client is not connected")?;
        ws.unsubscribe_depth10(&cmd.instrument_id);

        Ok(())
    }

    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.route_unsubscribe(WsChannel::FundingRatesPerps, &cmd.instrument_id)
    }

    fn unsubscribe_mark_prices(&mut self, cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.route_unsubscribe(WsChannel::MarkPricesPerps, &cmd.instrument_id)
    }

    fn unsubscribe_instrument_status(
        &mut self,
        cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        self.statuses.remove(&cmd.instrument_id);

        self.route_unsubscribe(WsChannel::DepthBooksPerps, &cmd.instrument_id)
    }

    fn unsubscribe_instrument(&mut self, _cmd: &UnsubscribeInstrument) -> anyhow::Result<()> {
        // Nothing was subscribed for it: `subscribe_instrument` publishes the loaded instrument.
        Ok(())
    }
}

#[async_trait(?Send)]
impl InstrumentProvider for OndoDataClient {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }

    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }

    /// Loads the markets this configuration publishes.
    ///
    /// A non-empty `load_ids` narrows the loaded set; with an empty list every market the venue
    /// publishes is loaded. A non-empty list is never widened to the whole market, which is what
    /// makes `load_ids` a subscription boundary and not a hint (plan §4.1).
    async fn load_all(&mut self, _filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        let requested = self.config.load_ids.clone();

        self.load_selected(&requested).await
    }

    /// Loads exactly `instrument_ids`, which must all be present in the venue's metadata.
    async fn load_ids(
        &mut self,
        instrument_ids: &[InstrumentId],
        _filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        ensure!(
            !instrument_ids.is_empty(),
            "no instrument id was requested, so nothing can be loaded"
        );

        self.load_selected(instrument_ids).await
    }

    /// Loads exactly one instrument.
    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        _filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.load_selected(&[*instrument_id]).await
    }
}

/// Reads `GET /v1/markets` and returns the venue's view plus the selected instruments.
///
/// The read is one request through the Stage B client, so it draws on the shared REST budget and uses
/// the Stage B request target rather than a second URL builder. Both parses go through Task 1's single
/// conversion boundary: the venue's view for the status comparison, and
/// [`parse_instruments`] for the instruments, which fails closed when a requested market is absent,
/// when a selected market's increments are unusable, or when its status cannot be classified.
///
/// When a recorder is configured, the read is also recorded as a metadata snapshot: the request's
/// start and end time, the HTTP status, the body and its hash, and the response headers that the
/// transport retains (the whitelist in [`crate::recording::RAW_MD_HEADER_WHITELIST`]). The snapshot is
/// taken before the body is interpreted, and a rejection (a 429 or a 5xx) is recorded from the status
/// and body the classified error carried, so the metadata history of a run explains its own failures.
async fn read_markets(
    http: &OndoHttpClient,
    load_ids: &[InstrumentId],
    ts_init: UnixNanos,
    recorder: Option<&RawMdSink>,
) -> anyhow::Result<(Vec<MarketInfo>, Vec<InstrumentAny>)> {
    let target = OndoRequestTarget::new(MARKETS_PATH);
    let request_started_at_ns = get_atomic_clock_realtime().get_time_ns().as_u64();

    let response = match http.get_raw_response(&target).await {
        Ok(response) => response,
        Err(e) => {
            if let Some(recorder) = recorder
                && let Some((status, body)) = rejected_read(&e)
            {
                let snapshot = MetadataSnapshot::new(
                    MARKETS_PATH,
                    status,
                    std::iter::empty(),
                    body,
                    request_started_at_ns,
                    get_atomic_clock_realtime().get_time_ns().as_u64(),
                );
                let _outcome = recorder.record_metadata_snapshot(&snapshot);
            }

            return Err(anyhow::anyhow!(
                "{MARKETS_PATH} did not return market metadata: {e}"
            ));
        }
    };
    let request_ended_at_ns = get_atomic_clock_realtime().get_time_ns().as_u64();

    if let Some(recorder) = recorder {
        let snapshot = MetadataSnapshot::new(
            MARKETS_PATH,
            response.status,
            response.headers,
            String::from_utf8_lossy(&response.body).to_string(),
            request_started_at_ns,
            request_ended_at_ns,
        );
        let _outcome = recorder.record_metadata_snapshot(&snapshot);
    }

    let body = String::from_utf8(response.body)
        .map_err(|e| anyhow::anyhow!("{MARKETS_PATH} response is not UTF-8: {e}"))?;

    let response: MarketsResponse = parse_markets(&body)
        .map_err(|e| anyhow::anyhow!("{MARKETS_PATH} response is not market metadata: {e}"))?;
    let infos = response
        .market_infos(ts_init)
        .map_err(|e| anyhow::anyhow!("{MARKETS_PATH} could not be normalised: {e}"))?;

    let instruments = parse_instruments(&body, load_ids, ts_init)
        .map_err(|e| anyhow::anyhow!("the Ondo market metadata did not build instruments: {e}"))?;

    ensure!(
        !instruments.is_empty(),
        "the venue published no market for this Ondo data client, which is a start-up failure"
    );

    Ok((infos, instruments))
}

/// Returns the HTTP status and body a rejected read carried, when it reached the venue at all.
///
/// A read that never got an answer (a transport failure or a timeout) has no status, so nothing is
/// recorded for it: the recording holds what the venue answered, never a guess at it.
fn rejected_read(error: &OndoHttpError) -> Option<(u16, String)> {
    match error {
        OndoHttpError::RateLimited { body, .. } => Some((429, body.clone())),
        OndoHttpError::RequestRejected {
            status, message, ..
        } => Some((*status, message.clone())),
        OndoHttpError::Http { status, body } => Some((*status, body.clone())),
        _ => None,
    }
}

/// Forwards every outcome the transport produces into the data engine.
///
/// This is the publication boundary: a market data value becomes a [`DataEvent::Data`], and a local
/// feed state becomes one too, because a disconnect has to reach the application as an event rather
/// than as a flag it would have to poll (plan §4.2).
async fn publish_outcomes(
    mut rx: mpsc::UnboundedReceiver<WsOutcome>,
    data_sender: mpsc::UnboundedSender<DataEvent>,
) {
    while let Some(outcome) = rx.recv().await {
        match outcome {
            WsOutcome::Data(data) => {
                if let Err(e) = data_sender.send(DataEvent::Data(data)) {
                    log::warn!("Failed to publish Ondo market data: {e}");

                    return;
                }
            }
            WsOutcome::Ignored { detail } => log::debug!("Ondo frame ignored: {detail}"),
            WsOutcome::Unsupported { reason, raw_frame } => {
                log::warn!(
                    "Ondo frame was not handled: {reason} ({} bytes)",
                    raw_frame.len()
                );
                log::debug!("Unhandled Ondo frame: {raw_frame}");
            }
            WsOutcome::ProtocolError { reason, raw_frame } => {
                log::error!("Ondo protocol error: {reason} ({} bytes)", raw_frame.len());
                log::debug!("Ondo frame that failed to decode: {raw_frame}");
            }
        }
    }

    log::debug!("Ondo outcome publisher finished");
}

/// Applies the outcome of one metadata read to the accepted version.
///
/// This is the refresh task's decision, kept out of the loop so both of its answers are provable
/// without waiting a whole interval: a read that failed, and a read that returns a different venue
/// view, keep the previous version and mark it stale, while a read of an unchanged view advances it.
fn record_read(
    metadata: &Mutex<OndoMarketMetadata>,
    read: &anyhow::Result<(Vec<MarketInfo>, Vec<InstrumentAny>)>,
) -> MetadataRefresh {
    let mut accepted = metadata
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    match read {
        Ok((infos, instruments)) => accepted.apply(infos, instruments.clone()),
        Err(e) => accepted.mark_stale(e.to_string()),
    }
}

/// Refreshes the market metadata on the plan's low-priority interval.
///
/// The refresh is a metadata read, so it takes the ordinary (non-priority) path through the shared
/// budget: plan §4.4 reserves the priority class for cancel, unknown-order and reconciliation
/// traffic, which must never queue behind a metadata refresh.
async fn refresh_metadata(
    http: OndoHttpClient,
    metadata: Arc<Mutex<OndoMarketMetadata>>,
    load_ids: Vec<InstrumentId>,
    cancellation: CancellationToken,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    clock: &'static AtomicTime,
    recorder: Option<RawMdSink>,
) {
    let interval = Duration::from_secs(ONDO_METADATA_REFRESH_INTERVAL_SECS);

    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                log::debug!("Ondo metadata refresh stopped");

                return;
            }
            () = tokio::time::sleep(interval) => {}
        }

        let ts_init = clock.get_time_ns();
        let read = read_markets(&http, &load_ids, ts_init, recorder.as_ref()).await;
        let outcome = record_read(&metadata, &read);

        // Only a refresh that replaced the accepted version - never one that kept it - republishes
        // instruments.
        if let (MetadataRefresh::Refreshed, Ok((_, instruments))) = (&outcome, &read) {
            for instrument in instruments {
                if let Err(e) = data_sender.send(DataEvent::Instrument(instrument.clone())) {
                    log::warn!("Failed to publish a refreshed Ondo instrument: {e}");
                }
            }
        }

        match outcome {
            MetadataRefresh::Initial | MetadataRefresh::Refreshed => log::debug!(
                "Ondo market metadata refreshed to version {}",
                metadata
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .version()
            ),
            MetadataRefresh::KeptPrevious { reason } => log::warn!(
                "Ondo market metadata is stale, keeping version {}: {reason}",
                metadata
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .version()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::live::runner::replace_data_event_sender;
    use nautilus_model::{
        data::Data,
        identifiers::{ClientId, InstrumentId},
        instruments::Instrument,
    };
    use rstest::rstest;

    use super::*;
    use crate::websocket::OndoWsSession;

    const MARKETS_FIXTURE: &str = include_str!("../test_data/rest/markets_synthetic.json");
    const DEPTH_FIXTURE: &str = include_str!("../test_data/ws/depth_observed.json");
    const NVDA: &str = "NVDA-USD-PERP.ONDO";
    const TSLA: &str = "TSLA-USD-PERP.ONDO";
    const ENA: &str = "ENA-USD-PERP.ONDO";

    /// Installs the process data event channel the client publishes into, as a running node does.
    fn data_event_channel() -> tokio::sync::mpsc::UnboundedReceiver<DataEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);

        rx
    }

    fn ts_init() -> UnixNanos {
        UnixNanos::from(7)
    }

    fn infos(body: &str) -> Vec<MarketInfo> {
        parse_markets(body)
            .expect("the fixture is market metadata")
            .market_infos(ts_init())
            .expect("every market normalises")
    }

    /// The instruments of the two classifiable markets of the fixture.
    ///
    /// The fixture also carries a `disabled` market and a market whose status the venue's vocabulary
    /// does not classify; neither is requested here, because a requested market with an unclassifiable
    /// status is a start-up failure by design.
    fn instruments(body: &str) -> Vec<InstrumentAny> {
        instruments_for(body, &[NVDA, TSLA])
    }

    fn instruments_for(body: &str, ids: &[&str]) -> Vec<InstrumentAny> {
        let ids: Vec<InstrumentId> = ids.iter().map(|id| InstrumentId::from(*id)).collect();

        parse_instruments(body, &ids, ts_init()).expect("the requested markets build instruments")
    }

    /// The fixture with the first trading pair's status replaced, which is what a venue-side status
    /// change looks like on the wire.
    fn with_first_status(status: &str) -> String {
        let mut value: serde_json::Value =
            serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
        value["result"]["perps"]["tradingPairs"][0]["status"] =
            serde_json::Value::String(status.to_string());

        value.to_string()
    }

    /// The fixture carrying only its first trading pair.
    fn with_first_pair_only() -> String {
        let mut value: serde_json::Value =
            serde_json::from_str(MARKETS_FIXTURE).expect("the fixture is JSON");
        let pairs = value["result"]["perps"]["tradingPairs"]
            .as_array()
            .expect("the fixture carries trading pairs");
        let first = pairs.first().expect("at least one pair").clone();
        value["result"]["perps"]["tradingPairs"] = serde_json::Value::Array(vec![first]);

        value.to_string()
    }

    #[rstest]
    fn test_the_first_accepted_version_is_not_stale() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();

        assert_eq!(metadata.version(), 0);
        assert!(!metadata.is_stale());

        let outcome = metadata.apply(&infos(body), instruments(body));

        assert_eq!(outcome, MetadataRefresh::Initial);
        assert_eq!(metadata.version(), 1);
        assert!(!metadata.is_stale());
        assert_eq!(metadata.stale_reason(), None);
        assert_eq!(metadata.instruments().len(), 2);
        assert!(metadata.is_tradable(&InstrumentId::from(NVDA)));
    }

    #[rstest]
    fn test_an_unchanged_refresh_advances_the_version_and_clears_the_flag() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();
        metadata.apply(&infos(body), instruments(body));
        metadata.mark_stale("a read that failed");
        assert!(metadata.is_stale());

        let outcome = metadata.apply(&infos(body), instruments(body));

        assert_eq!(outcome, MetadataRefresh::Refreshed);
        assert_eq!(metadata.version(), 2);
        assert!(
            !metadata.is_stale(),
            "an unchanged venue view clears the flag"
        );
        assert_eq!(metadata.stale_reason(), None);
    }

    #[rstest]
    fn test_a_failed_refresh_keeps_the_previous_version_and_flags_it_stale() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();
        metadata.apply(&infos(body), instruments(body));
        let accepted: Vec<String> = metadata
            .instruments()
            .iter()
            .map(|instrument| instrument.id().to_string())
            .collect();

        let outcome = metadata.mark_stale("the venue stopped answering");

        assert_eq!(
            outcome,
            MetadataRefresh::KeptPrevious {
                reason: "the venue stopped answering".to_string()
            }
        );
        assert_eq!(metadata.version(), 1, "the previous version is kept");
        assert_eq!(
            metadata
                .instruments()
                .iter()
                .map(|instrument| instrument.id().to_string())
                .collect::<Vec<_>>(),
            accepted,
            "the last known good metadata is never dropped"
        );
        assert!(metadata.is_stale());
        assert_eq!(metadata.stale_reason(), Some("the venue stopped answering"));
        assert!(
            !metadata.is_tradable(&InstrumentId::from(NVDA)),
            "a stale version cannot claim a market is tradable"
        );
        assert_eq!(
            metadata.status(&InstrumentId::from(NVDA)),
            Some(MarketStatus::Active)
        );
    }

    #[rstest]
    fn test_a_market_status_change_keeps_the_previous_version_and_flags_it_stale() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();
        metadata.apply(&infos(body), instruments(body));

        let changed = with_first_status("disabled");
        let outcome = metadata.apply(&infos(&changed), instruments(&changed));

        let MetadataRefresh::KeptPrevious { reason } = outcome else {
            panic!("a status change keeps the previous version, was {outcome:?}");
        };
        assert!(reason.contains(&format!("`{NVDA}`")), "was `{reason}`");
        assert!(reason.contains("Active -> Disabled"), "was `{reason}`");
        assert_eq!(metadata.version(), 1);
        assert_eq!(
            metadata.status(&InstrumentId::from(NVDA)),
            Some(MarketStatus::Active),
            "the accepted status is the previous one, not the unaccepted reading"
        );
        assert!(metadata.is_stale());
        assert_eq!(
            metadata.instruments().len(),
            2,
            "the previous instrument set is kept whole"
        );
    }

    #[rstest]
    fn test_a_market_disappearing_from_the_venue_keeps_the_previous_version_and_flags_it_stale() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();
        metadata.apply(&infos(body), instruments(body));

        // The payload the venue now publishes carries the first pair only, so the instruments that
        // payload can build are the first pair's: the comparison is over the venue's *view*, and it
        // is that view - which no longer names TSLA - that is rejected.
        let reduced = with_first_pair_only();
        let outcome = metadata.apply(&infos(&reduced), instruments_for(&reduced, &[NVDA]));

        let MetadataRefresh::KeptPrevious { reason } = outcome else {
            panic!("a market vanishing keeps the previous version, was {outcome:?}");
        };
        assert!(reason.contains("no longer publishes"), "was `{reason}`");
        assert_eq!(metadata.version(), 1);
        assert_eq!(metadata.instruments().len(), 2);
        assert!(metadata.is_stale());
    }

    #[rstest]
    fn test_metadata_recovers_when_the_venue_view_returns_to_the_accepted_one() {
        let body = MARKETS_FIXTURE;
        let mut metadata = OndoMarketMetadata::new();
        metadata.apply(&infos(body), instruments(body));
        metadata.apply(
            &infos(&with_first_status("disabled")),
            instruments(&with_first_status("disabled")),
        );
        assert!(metadata.is_stale());

        let outcome = metadata.apply(&infos(body), instruments(body));

        assert_eq!(outcome, MetadataRefresh::Refreshed);
        assert_eq!(metadata.version(), 2);
        assert!(!metadata.is_stale());
        assert!(metadata.is_tradable(&InstrumentId::from(NVDA)));
        assert!(
            metadata.is_tradable(&InstrumentId::from(TSLA)),
            "the recovered view is `active` for both markets of the fixture, so both are tradable"
        );
    }

    #[rstest]
    fn test_an_unclassifiable_status_fails_the_load_instead_of_being_guessed() {
        let changed = with_first_status("halted");

        // `parse_instruments` refuses a selected market whose status cannot be classified, and an
        // unselected market contributes nothing, so the same payload loads only what it can vouch for.
        let error = parse_instruments(&changed, &[InstrumentId::from(NVDA)], ts_init())
            .expect_err("an unknown status for a requested market is a start-up failure");

        assert!(error.to_string().contains("halted"), "was `{error}`");
        assert_eq!(
            parse_instruments(&changed, &[InstrumentId::from(TSLA)], ts_init())
                .expect("the other market is unaffected")
                .len(),
            1
        );
    }

    #[rstest]
    fn test_the_data_client_shares_the_rest_budget_it_is_given() {
        let _events = data_event_channel();
        let budget = OndoRateBudget::new();

        let client = OndoDataClient::new(
            ClientId::from("ONDO-TEST"),
            OndoDataClientConfig::default(),
            budget.clone(),
        )
        .expect("the data client is constructed without reaching the venue");

        assert!(
            Arc::ptr_eq(client.budget().limiter(), budget.limiter()),
            "§4.4: the client spends from the budget it was handed, not one of its own"
        );
        assert_eq!(client.venue(), *ONDO_VENUE);

        // The client is the `DataClient` the engine holds as a trait object, and it reports the
        // socket it has not opened rather than an intention to open one.
        let mut client: Box<dyn DataClient> = Box::new(client);
        assert_eq!(client.client_id(), ClientId::from("ONDO-TEST"));
        assert!(!client.is_connected());
        assert!(client.is_disconnected());
        client
            .stop()
            .expect("stopping a client that never connected is not an error");
    }

    #[rstest]
    fn test_supports_depth10_reports_the_projection_this_client_serves() {
        // `supports_depth10` is the registry's answer, and it may only be `true` because the
        // on-demand depth10 is a projection of the *same* book state the delta batch comes from. The
        // assertions below drive that projection path - the subscription
        // `OndoDataClient::subscribe_book_depth10` routes to, `WsChannel::DepthBooksPerps` plus the
        // session command `OndoWsSession::subscribe_depth10` - and would fail if it produced no
        // depth10, or a depth10 of some other book. A later phase that stopped projecting, or that
        // opened a second connection for it, would break this assertion rather than leave the flag a
        // lie the registry cannot check.
        let instrument = instruments(MARKETS_FIXTURE)
            .into_iter()
            .find(|instrument| instrument.id() == InstrumentId::from(NVDA))
            .expect("the fixture publishes the NVDA market");

        let mut session = OndoWsSession::new(10, None);
        session.register_instrument(&instrument);
        assert!(
            session
                .subscribe(WsChannel::DepthBooksPerps, &[instrument.id()])
                .expect("the market is registered")
                .is_some(),
            "the deltas subscription is the book subscription the projection rides"
        );
        assert!(
            session
                .subscribe_depth10(&instrument.id())
                .expect("the market is registered")
                .is_none(),
            "the depth10 rides that same subscription instead of opening a second one"
        );

        let outcomes = session.handle_raw_frame(DEPTH_FIXTURE, ts_init());
        let mut deltas = None;
        let mut depth10 = None;
        for outcome in outcomes {
            match outcome {
                WsOutcome::Data(Data::Deltas(batch)) => deltas = Some(batch),
                WsOutcome::Data(Data::Depth10(depth)) => depth10 = Some(depth),
                _ => {}
            }
        }
        let deltas = deltas.expect("the same frame publishes the book deltas");
        let depth10 = depth10.expect("the projection is a depth10 of that same book state");
        let state = session
            .book_state(&instrument.id())
            .expect("the book exists");

        assert_eq!(depth10.instrument_id, instrument.id());
        assert_eq!(
            depth10.bids[0].price.to_string(),
            state.book().best_bid().expect("a best bid").0.to_string(),
            "the projection's best bid is the projected book's best bid"
        );
        assert_eq!(
            depth10.asks[0].price.to_string(),
            state.book().best_ask().expect("a best ask").0.to_string(),
            "the projection's best ask is the projected book's best ask"
        );
        assert_eq!(
            deltas.deltas.len(),
            21,
            "the delta batch of the same frame is what the projection rides"
        );

        assert!(OndoDataClient::supports_depth10());
    }

    #[rstest]
    fn test_a_failed_refresh_read_keeps_the_previous_version_and_flags_it_stale() {
        // Plan §4.4: the metadata read is low-priority traffic on a sixty second interval, so it
        // never takes a slot a cancel would need - and its failure is not allowed to throw the last
        // known good metadata away.
        assert_eq!(ONDO_METADATA_REFRESH_INTERVAL_SECS, 60);

        let metadata = Mutex::new(OndoMarketMetadata::new());
        let accepted = record_read(
            &metadata,
            &Ok((infos(MARKETS_FIXTURE), instruments(MARKETS_FIXTURE))),
        );
        assert_eq!(accepted, MetadataRefresh::Initial);

        let outcome = record_read(
            &metadata,
            &Err(anyhow::anyhow!("the venue stopped answering")),
        );
        assert_eq!(
            outcome,
            MetadataRefresh::KeptPrevious {
                reason: "the venue stopped answering".to_string()
            }
        );

        let accepted = metadata.lock().expect("the metadata lock is not poisoned");
        assert_eq!(accepted.version(), 1, "the previous version is kept");
        assert_eq!(
            accepted.instruments().len(),
            2,
            "the instruments are kept whole"
        );
        assert!(accepted.is_stale());
        assert_eq!(accepted.stale_reason(), Some("the venue stopped answering"));
        assert!(
            !accepted.is_tradable(&InstrumentId::from(NVDA)),
            "a stale version cannot claim a market is tradable"
        );
    }

    #[rstest]
    fn test_a_refresh_read_that_reports_a_status_change_keeps_the_previous_version_and_flags_it_stale()
     {
        let changed = with_first_status("disabled");
        let metadata = Mutex::new(OndoMarketMetadata::new());
        record_read(
            &metadata,
            &Ok((infos(MARKETS_FIXTURE), instruments(MARKETS_FIXTURE))),
        );

        let MetadataRefresh::KeptPrevious { reason } =
            record_read(&metadata, &Ok((infos(&changed), instruments(&changed))))
        else {
            panic!("a venue-side status change keeps the previous version");
        };
        assert!(reason.contains("Active -> Disabled"), "was `{reason}`");

        let accepted = metadata.lock().expect("the metadata lock is not poisoned");
        assert_eq!(accepted.version(), 1);
        assert_eq!(
            accepted.status(&InstrumentId::from(NVDA)),
            Some(MarketStatus::Active),
            "the accepted view is the previous one, not the unaccepted reading"
        );
        assert!(accepted.is_stale());
    }

    #[rstest]
    fn test_a_requested_market_is_never_widened_to_the_whole_market() {
        // `load_ids` is a subscription boundary and not a hint (plan §4.1): asking for the `disabled`
        // ENA publishes exactly ENA, even though the payload carries two `active` markets beside it.
        let selected = instruments_for(MARKETS_FIXTURE, &[ENA]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id(), InstrumentId::from(ENA));
        assert!(
            !selected
                .iter()
                .any(|instrument| instrument.id() == InstrumentId::from(NVDA)),
            "the requested set is not widened"
        );
        assert_eq!(
            instruments(MARKETS_FIXTURE).len(),
            2,
            "the same payload does carry the two active markets"
        );
    }

    #[rstest]
    fn test_a_requested_market_the_payload_does_not_carry_fails_the_whole_load() {
        // The load is all-or-nothing: a requested market the venue did not publish fails the read
        // instead of quietly loading the markets that happen to be in the payload.
        let reduced = with_first_pair_only();
        let requested = [InstrumentId::from(NVDA), InstrumentId::from(ENA)];

        let error = parse_instruments(&reduced, &requested, ts_init())
            .expect_err("a requested market the venue does not publish is a start-up failure");

        assert!(
            error.to_string().contains(ENA),
            "the error names the market it could not load, was `{error}`"
        );
        assert_eq!(
            parse_instruments(&reduced, &requested[..1], ts_init())
                .expect("the market the payload does carry loads")
                .len(),
            1
        );
    }

    #[rstest]
    fn test_a_payload_with_no_market_at_all_fails_the_load() {
        // A venue answering with no perps market is a start-up failure, never an empty success.
        let empty = r#"{"success":true,"result":{"perps":{"tradingPairs":[]}}}"#;

        let error = parse_instruments(empty, &[], ts_init())
            .expect_err("no market at all cannot build an instrument");

        assert!(
            error.to_string().contains("no perps market data"),
            "was `{error}`"
        );
    }

    #[rstest]
    fn test_the_configuration_surface_is_the_documented_one_and_carries_no_credential() {
        let value = serde_json::to_value(OndoDataClientConfig::default())
            .expect("the configuration serializes");
        let mut members: Vec<String> = value
            .as_object()
            .expect("the configuration is an object")
            .keys()
            .cloned()
            .collect();
        members.sort();

        // Plan §4.1: exactly these members, and nothing about the authenticated account beside them.
        assert_eq!(
            members,
            [
                "base_url_http",
                "base_url_ws",
                "book_limit",
                "environment",
                "http_timeout_secs",
                "load_ids",
                "raw_md_path",
                "raw_md_run_id",
                "ws_heartbeat_secs",
            ]
        );

        for credential in [
            "api_key",
            "apiKey",
            "secret",
            "private_key",
            "passphrase",
            "token",
        ] {
            let body = format!(r#"{{"{credential}":"x"}}"#);

            assert!(
                serde_json::from_str::<OndoDataClientConfig>(&body).is_err(),
                "`{credential}` is not a member of the public data client configuration"
            );
        }
    }

    #[rstest]
    fn test_the_raw_recording_takes_the_configured_run_id_not_the_directory_name() {
        use crate::recording::RunIdSource;

        // The application's default `--out reports/stage1`: the parent is not a run id, so the
        // derived value would be `stage1` while every tape record carries the process stamp.
        let unconfigured = OndoDataClientConfig {
            raw_md_path: Some("reports/stage1/raw_ondo".to_string()),
            ..OndoDataClientConfig::default()
        };
        let derived =
            raw_recorder_config(&unconfigured).expect("a named path configures a recording");
        assert_eq!(derived.run_id, "stage1");
        assert_eq!(
            derived.run_id_source,
            RunIdSource::DerivedFromPath,
            "the file says how the id was established, so a mismatch is never silent"
        );

        let configured = OndoDataClientConfig {
            raw_md_path: Some("reports/stage1/raw_ondo".to_string()),
            raw_md_run_id: Some("20260914T000000Z".to_string()),
            ..OndoDataClientConfig::default()
        };
        let recorder =
            raw_recorder_config(&configured).expect("a named path configures a recording");
        assert_eq!(
            recorder.run_id, "20260914T000000Z",
            "the explicit run id joins the tape even when --out is not the run directory"
        );
        assert_eq!(recorder.run_id_source, RunIdSource::Configured);
        assert!(
            recorder.session_id.starts_with("20260914T000000Z-"),
            "the session id is built from the run id the run actually carries, was `{}`",
            recorder.session_id
        );

        assert!(
            raw_recorder_config(&OndoDataClientConfig::default()).is_none(),
            "with no raw_md_path there is no recorder configuration at all"
        );
    }

    #[rstest]
    fn test_the_data_client_has_no_key_env_or_execution_path() {
        // The production sources of this adapter, read as text: no environment read, no credential,
        // and no execution client, in the public data client itself and in every module it delegates
        // to (the REST client, the WebSocket transport and the shared helpers), so a credential
        // cannot drift into any of them. The word "secret" may appear in prose that documents the
        // absence, so every occurrence is asserted to be on a comment line: nothing can log a secret
        // that no code path ever names. Each file is scanned only up to its test module, since that
        // is where those strings are named in order to forbid them. `src/recording.rs` is on the list
        // because a raw recording is the one place where the absence of a credential matters most: it
        // writes what a connection carried, and it must never be able to write or log anything else.
        //
        // `src/common/credential.rs` is deliberately **not** on this list. Task 6 turned it from the
        // prose that recorded the absence into the credential store itself - the one module that
        // reads the environment and holds the API secret, by design - so it is no longer on the data
        // path. `src/http/error.rs` *is* on the list and now names the venue's own documented auth
        // codes (`api_key_not_found`) and the API-key page it cites; those are the wire contract, not
        // a key, so the `api_key` check below subtracts exactly those two spellings. The data client
        // is separately asserted, after the loop, never to name the credential module at all.
        let sources: [(&str, &str); 17] = [
            ("src/data.rs", include_str!("data.rs")),
            ("src/recording.rs", include_str!("recording.rs")),
            ("src/http/client.rs", include_str!("http/client.rs")),
            ("src/http/error.rs", include_str!("http/error.rs")),
            ("src/http/models.rs", include_str!("http/models.rs")),
            ("src/http/mod.rs", include_str!("http/mod.rs")),
            ("src/http/query.rs", include_str!("http/query.rs")),
            ("src/http/rate_limit.rs", include_str!("http/rate_limit.rs")),
            ("src/websocket/book.rs", include_str!("websocket/book.rs")),
            (
                "src/websocket/client.rs",
                include_str!("websocket/client.rs"),
            ),
            (
                "src/websocket/messages.rs",
                include_str!("websocket/messages.rs"),
            ),
            ("src/websocket/mod.rs", include_str!("websocket/mod.rs")),
            ("src/websocket/parse.rs", include_str!("websocket/parse.rs")),
            ("src/common/consts.rs", include_str!("common/consts.rs")),
            ("src/common/enums.rs", include_str!("common/enums.rs")),
            ("src/common/mod.rs", include_str!("common/mod.rs")),
            ("src/common/parse.rs", include_str!("common/parse.rs")),
        ];

        for (path, source) in sources {
            let production = source
                .split("#[cfg(test)]")
                .next()
                .expect("every source has contents before its test module");

            for (number, line) in production.lines().enumerate() {
                for forbidden in [
                    "std::env",
                    "env::var",
                    "dotenv",
                    "ExecutionClient",
                    "private_key",
                    "Authorization",
                ] {
                    assert!(
                        !line.contains(forbidden),
                        "{path}: line {}: `{forbidden}` has no place in this public data path",
                        number + 1
                    );
                }
                // The venue's documented auth code and the API-key page's filename are the wire
                // contract, not a key: they are subtracted before the key-identifier check, so every
                // other spelling of `api_key` still fails here.
                let without_venue_names = line
                    .replace("api_key_not_found", "")
                    .replace("api_key_authentication", "");
                assert!(
                    !without_venue_names.contains("api_key"),
                    "{path}: line {}: `api_key` has no place in this public data path",
                    number + 1
                );
                assert!(
                    !line.contains("secret") || line.trim_start().starts_with("//"),
                    "{path}: line {}: a secret may be named in prose, never in code",
                    number + 1
                );
            }
        }

        // The compensation for dropping `src/common/credential.rs` from the list above: the data
        // client itself must not name the credential module or its type, so no credential is
        // reachable from the public data path even though the module now exists.
        let data_client = include_str!("data.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("data.rs has contents before its test module");
        assert!(
            !data_client.contains("OndoCredential") && !data_client.contains("common::credential"),
            "the public data client must not reach the credential module",
        );
    }
}
