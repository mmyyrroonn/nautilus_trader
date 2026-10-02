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

//! Native credential-free public data client and strict instrument provider.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::runner::try_get_data_event_sender,
    messages::{
        DataEvent,
        data::{DataResponse, InstrumentResponse, InstrumentsResponse},
    },
    providers::{InstrumentProvider, InstrumentStore},
};
use nautilus_live::task::{TaskGroup, TaskGroupGuard};
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};
use nautilus_network::websocket::{SubscriptionState, WebSocketClient};
use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::runtime::BackpackPublicHealth;
use crate::{
    config::BackpackDataClientConfig,
    data_error::BackpackDataError,
    http::{
        client::{BackpackHttpClient, BackpackHttpPolicy, BackpackSystemClock},
        quota::BackpackQuota,
    },
    runtime::{EventSender, Gate, PublicSession, load_metadata, now},
    signing::BackpackReceiveWindow,
};

/// Public-only native owner. Construction reads neither credentials nor environment variables.
pub struct BackpackDataClient {
    client_id: ClientId,
    config: BackpackDataClientConfig,
    http: BackpackHttpClient,
    sender: EventSender,
    gate: Arc<Mutex<Gate>>,
    subscriptions: SubscriptionState,
    notify: Arc<Notify>,
    tasks: TaskGroup,
    websocket: Option<Arc<WebSocketClient>>,
    store: InstrumentStore,
    disposed: bool,
}
impl std::fmt::Debug for BackpackDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpackDataClient")
            .field("client_id", &self.client_id)
            .field("health", &self.health())
            .finish_non_exhaustive()
    }
}
impl BackpackDataClient {
    /// Creates an unconnected client using the native runner's event channel.
    ///
    /// # Errors
    ///
    /// Returns an error when the event channel or bounded transport configuration is unavailable.
    pub fn new(
        client_id: ClientId,
        config: BackpackDataClientConfig,
    ) -> Result<Self, BackpackDataError> {
        Self::with_quota(client_id, config, BackpackQuota::default())
    }
    /// Creates an unconnected public client sharing an explicit quota scope with other owners.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing runner channel or invalid transport configuration.
    pub fn with_quota(
        client_id: ClientId,
        config: BackpackDataClientConfig,
        quota: BackpackQuota,
    ) -> Result<Self, BackpackDataError> {
        config.lifecycle().validate()?;
        let sender = try_get_data_event_sender().ok_or(BackpackDataError::Lifecycle(
            "native data event sender is not initialized",
        ))?;
        let policy = BackpackHttpPolicy::new(
            BackpackReceiveWindow::default(),
            Duration::from_secs(config.lifecycle().http_timeout_secs),
            2,
        )
        .map_err(|_| BackpackDataError::Configuration("HTTP policy"))?;
        let http = BackpackHttpClient::new(
            config.scope().endpoints().clone(),
            None,
            quota,
            policy,
            Arc::new(BackpackSystemClock),
        )
        .map_err(|_| BackpackDataError::Transport)?;
        let gate = config.telemetry().claim(
            config.lifecycle().ws_idle_timeout_secs,
            config.lifecycle().quote_stale_after_ms,
        )?;
        Ok(Self {
            client_id,
            config,
            http,
            sender,
            gate,
            subscriptions: SubscriptionState::new('.'),
            notify: Arc::new(Notify::new()),
            tasks: TaskGroup::new(),
            websocket: None,
            store: InstrumentStore::new(),
            disposed: false,
        })
    }
    /// Returns sanitized public session and freshness diagnostics.
    #[must_use]
    pub fn health(&self) -> BackpackPublicHealth {
        self.gate
            .lock()
            .health(self.config.lifecycle().ws_idle_timeout_secs)
    }
    fn check_alive(&self) -> Result<(), BackpackDataError> {
        if self.disposed {
            Err(BackpackDataError::Lifecycle("disposed"))
        } else {
            Ok(())
        }
    }
    fn symbol(&self, id: InstrumentId) -> Result<String, BackpackDataError> {
        let symbol = id.symbol.as_str();
        if id.venue != Venue::from("BACKPACK") || !self.config.scope().symbols().contains(symbol) {
            return Err(BackpackDataError::Configuration(
                "instrument outside allowlist",
            ));
        }
        Ok(symbol.to_owned())
    }
    fn topic(&self, id: InstrumentId, channel: &str, subscribe: bool) -> anyhow::Result<()> {
        self.check_alive()?;
        let symbol = self.symbol(id)?;
        let topic = format!("{channel}.{symbol}");
        let mut gate = self.gate.lock();
        let changed = if subscribe {
            self.subscriptions.try_mark_subscribe(&topic)
        } else {
            let present = self.subscriptions.all_topics().contains(&topic);
            self.subscriptions.mark_unsubscribe(&topic);
            present
        };
        if changed {
            let revision = gate.revisions.entry(topic).or_default();
            *revision = revision.checked_add(1).ok_or(BackpackDataError::Lifecycle(
                "subscription revision exhausted",
            ))?;
        }
        if !subscribe {
            if channel == "bookTicker" {
                gate.quote_receipts.remove(&symbol);
            }
            if channel == "depth" {
                gate.books.insert(symbol, None);
            }
        }
        drop(gate);
        self.notify.notify_one();
        Ok(())
    }
    fn close_gate(&mut self, reason: &'static str) {
        let mut state = self.gate.lock();
        state.running = false;
        state.invalidate(reason);
        state.instruments.clear();
        drop(state);
        self.tasks.abort();
        self.websocket.take();
        self.store.clear();
    }
    async fn drain(&self) -> Result<(), BackpackDataError> {
        let bound = Duration::from_secs(self.config.lifecycle().shutdown_timeout_secs);
        self.tasks
            .finish_shutdown(bound, bound)
            .await
            .map_err(|_| BackpackDataError::Lifecycle("owned task shutdown incomplete"))
    }
    async fn refresh_provider(
        &mut self,
        ids: Option<&[InstrumentId]>,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.check_alive()?;
        self.store.clear();
        if filters.is_some_and(|f| !f.is_empty()) {
            return Err(BackpackDataError::Unsupported("instrument filters").into());
        }
        if self.gate.lock().running {
            return Err(BackpackDataError::Lifecycle(
                "explicit provider refresh requires disconnected owner",
            )
            .into());
        }
        if let Some(ids) = ids {
            if ids.is_empty() {
                return Err(BackpackDataError::Configuration("empty instrument selection").into());
            }
            for id in ids {
                self.symbol(*id)?;
            }
        }
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_, instruments) = load_metadata(&self.config, &self.http, &cancel, None, None).await?;
        self.store.add_bulk(
            instruments
                .into_iter()
                .filter(|i| ids.is_none_or(|ids| ids.contains(&i.id())))
                .collect(),
        );
        self.store.set_initialized();
        Ok(())
    }
    fn publish_cached_instruments(&self, id: Option<InstrumentId>) -> anyhow::Result<()> {
        let state = self.gate.lock();
        if !state.running || !state.metadata_ready {
            return Err(BackpackDataError::Lifecycle("metadata unavailable").into());
        }
        for instrument in &state.instruments {
            if id.is_none_or(|id| id == instrument.id()) {
                self.sender
                    .send(DataEvent::Instrument(instrument.clone()))
                    .map_err(|_| BackpackDataError::Lifecycle("data engine unavailable"))?;
            }
        }
        Ok(())
    }
}
impl Drop for BackpackDataClient {
    fn drop(&mut self) {
        self.close_gate("owner dropped");
        self.config.telemetry().release();
    }
}

#[async_trait(?Send)]
impl InstrumentProvider for BackpackDataClient {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }
    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }
    async fn load_all(&mut self, filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        self.refresh_provider(None, filters).await
    }
    async fn load_ids(
        &mut self,
        ids: &[InstrumentId],
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.refresh_provider(Some(ids), filters).await
    }
    async fn load(
        &mut self,
        id: &InstrumentId,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.refresh_provider(Some(std::slice::from_ref(id)), filters)
            .await
    }
}

#[async_trait(?Send)]
impl DataClient for BackpackDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }
    fn venue(&self) -> Option<Venue> {
        Some(Venue::from("BACKPACK"))
    }
    fn start(&mut self) -> anyhow::Result<()> {
        self.check_alive()?;
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.close_gate("stopped");
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.check_alive()?;
        self.close_gate("reset");
        self.subscriptions.clear();
        self.gate.lock().revisions.clear();
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.close_gate("disposed");
        self.subscriptions.clear();
        self.disposed = true;
        Ok(())
    }
    fn is_connected(&self) -> bool {
        let state = self.gate.lock();
        state.running && state.connected && state.metadata_ready
    }
    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        self.check_alive()?;
        if self.is_connected() {
            return Ok(());
        }
        if self.gate.lock().running {
            return Err(BackpackDataError::Lifecycle("connection recovery in progress").into());
        }
        if !self.tasks.is_open() {
            self.drain().await?;
            self.tasks
                .start_generation()
                .map_err(|_| BackpackDataError::Lifecycle("prior generation not drained"))?;
        }
        self.store.clear();
        let owner = {
            let mut state = self.gate.lock();
            state.owner = state
                .owner
                .checked_add(1)
                .ok_or(BackpackDataError::Lifecycle("session generation exhausted"))?;
            state.epoch = 0;
            state.running = true;
            state.invalidate("initial bootstrap");
            state.owner
        };
        let gate = self.gate.clone();
        let guard = TaskGroupGuard::new(&[&self.tasks], move || {
            let mut state = gate.lock();
            if state.owner == owner {
                state.running = false;
                state.invalidate("partial connect rollback");
                state.instruments.clear();
            }
        });
        let cancel = self.tasks.cancellation_token();
        let gate = self.gate.clone();
        let admission = move || gate.lock().current(owner, 0);
        let result = async {
            let (metadata, instruments) =
                load_metadata(&self.config, &self.http, &cancel, Some(&admission), None).await?;
            {
                let mut state = self.gate.lock();
                if !state.current(owner, 0) {
                    return Err(BackpackDataError::Lifecycle("bootstrap admission lost"));
                }
                for instrument in &instruments {
                    self.sender
                        .send(DataEvent::Instrument(instrument.clone()))
                        .map_err(|_| BackpackDataError::Lifecycle("data engine unavailable"))?;
                }
                state.instruments = instruments.clone();
            }
            self.store.add_bulk(instruments);
            self.store.set_initialized();
            let session = PublicSession {
                config: self.config.clone(),
                http: self.http.clone(),
                gate: self.gate.clone(),
                subscriptions: self.subscriptions.clone(),
                notify: self.notify.clone(),
                sender: self.sender.clone(),
            };
            let fault_serial = self.gate.lock().fault_serial;
            let (ws, rx, tx) = session.open(owner, cancel).await?;
            {
                let mut state = self.gate.lock();
                if !state.current(owner, 0) || state.fault_serial != fault_serial {
                    return Err(BackpackDataError::Lifecycle("websocket admission lost"));
                }
                state.connected = true;
                state.metadata_ready = true;
                state.stale = None;
                state.lost_at = None;
            }
            let spawner = self
                .tasks
                .spawner()
                .map_err(|_| BackpackDataError::Lifecycle("runtime task admission"))?;
            self.websocket = Some(ws.clone());
            self.tasks
                .spawn(session.run(owner, ws, rx, tx, metadata, spawner))
                .map_err(|_| BackpackDataError::Lifecycle("runtime task admission"))?;
            Ok::<(), BackpackDataError>(())
        }
        .await;
        match result {
            Ok(()) => {
                guard.disarm();
                Ok(())
            }
            Err(e) => {
                drop(guard);
                self.websocket.take();
                self.store.clear();
                self.drain().await?;
                Err(e.into())
            }
        }
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        {
            let mut state = self.gate.lock();
            state.running = false;
            state.invalidate("disconnected");
            state.instruments.clear();
        }
        self.tasks.begin_shutdown();
        self.store.clear();
        if let Some(ws) = self.websocket.take() {
            let bound = Duration::from_secs(self.config.lifecycle().shutdown_timeout_secs);
            let _ = nautilus_network::dst::time::timeout(bound, ws.disconnect()).await;
        }
        self.drain().await?;
        Ok(())
    }
    fn subscribe_quotes(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeQuotes,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "bookTicker", true)
    }
    fn subscribe_trades(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeTrades,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "trade", true)
    }
    fn subscribe_mark_prices(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "markPrice", true)
    }
    fn subscribe_book_deltas(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP
            || cmd
                .depth
                .is_some_and(|d| d.get() != self.config.lifecycle().depth_snapshot_limit)
        {
            return Err(BackpackDataError::Unsupported("book type or requested depth").into());
        }
        self.topic(cmd.instrument_id, "depth", true)
    }
    fn unsubscribe_quotes(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeQuotes,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "bookTicker", false)
    }
    fn unsubscribe_trades(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeTrades,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "trade", false)
    }
    fn unsubscribe_mark_prices(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "markPrice", false)
    }
    fn unsubscribe_book_deltas(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        self.topic(cmd.instrument_id, "depth", false)
    }
    fn subscribe_instruments(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeInstruments,
    ) -> anyhow::Result<()> {
        if cmd.venue != Venue::from("BACKPACK") {
            return Err(BackpackDataError::Configuration("foreign venue").into());
        }
        self.publish_cached_instruments(None)
    }
    fn subscribe_instrument(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeInstrument,
    ) -> anyhow::Result<()> {
        self.symbol(cmd.instrument_id)?;
        self.publish_cached_instruments(Some(cmd.instrument_id))
    }
    fn unsubscribe_instruments(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeInstruments,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn unsubscribe_instrument(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        self.symbol(cmd.instrument_id)?;
        Ok(())
    }
    fn request_instrument(
        &self,
        request: nautilus_common::messages::data::RequestInstrument,
    ) -> anyhow::Result<()> {
        self.symbol(request.instrument_id)?;
        if request.start.is_some() || request.end.is_some() {
            return Err(BackpackDataError::Unsupported("historical metadata").into());
        }
        let state = self.gate.lock();
        if !state.running || !state.metadata_ready {
            return Err(BackpackDataError::Lifecycle("metadata unavailable").into());
        }
        let instrument = state
            .instruments
            .iter()
            .find(|i| i.id() == request.instrument_id)
            .ok_or(BackpackDataError::Metadata)?
            .clone();
        self.sender
            .send(DataEvent::Response(DataResponse::Instrument(Box::new(
                InstrumentResponse::new(
                    request.request_id,
                    self.client_id,
                    request.instrument_id,
                    instrument,
                    None,
                    None,
                    now(),
                    request.params,
                ),
            ))))
            .map_err(|_| BackpackDataError::Lifecycle("data engine unavailable"))?;
        Ok(())
    }
    fn request_instruments(
        &self,
        request: nautilus_common::messages::data::RequestInstruments,
    ) -> anyhow::Result<()> {
        if request.start.is_some()
            || request.end.is_some()
            || request.venue.is_some_and(|v| v != Venue::from("BACKPACK"))
        {
            return Err(
                BackpackDataError::Unsupported("historical metadata or foreign venue").into(),
            );
        }
        let state = self.gate.lock();
        if !state.running || !state.metadata_ready {
            return Err(BackpackDataError::Lifecycle("metadata unavailable").into());
        }
        self.sender
            .send(DataEvent::Response(DataResponse::Instruments(
                InstrumentsResponse::new(
                    request.request_id,
                    self.client_id,
                    Venue::from("BACKPACK"),
                    state.instruments.clone(),
                    None,
                    None,
                    now(),
                    request.params,
                ),
            )))
            .map_err(|_| BackpackDataError::Lifecycle("data engine unavailable"))?;
        Ok(())
    }
    fn subscribe(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeCustomData,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe").into())
    }
    fn subscribe_book_depth10(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeBookDepth10,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_book_depth10").into())
    }
    fn subscribe_index_prices(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_index_prices").into())
    }
    fn subscribe_funding_rates(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_funding_rates").into())
    }
    fn subscribe_bars(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeBars,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_bars").into())
    }
    fn subscribe_instrument_status(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_instrument_status").into())
    }
    fn subscribe_instrument_close(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_instrument_close").into())
    }
    fn subscribe_option_greeks(
        &mut self,
        _cmd: nautilus_common::messages::data::SubscribeOptionGreeks,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("subscribe_option_greeks").into())
    }
    fn unsubscribe(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeCustomData,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe").into())
    }
    fn unsubscribe_book_depth10(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeBookDepth10,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_book_depth10").into())
    }
    fn unsubscribe_index_prices(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_index_prices").into())
    }
    fn unsubscribe_funding_rates(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_funding_rates").into())
    }
    fn unsubscribe_bars(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeBars,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_bars").into())
    }
    fn unsubscribe_instrument_status(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_instrument_status").into())
    }
    fn unsubscribe_instrument_close(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_instrument_close").into())
    }
    fn unsubscribe_option_greeks(
        &mut self,
        _cmd: &nautilus_common::messages::data::UnsubscribeOptionGreeks,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("unsubscribe_option_greeks").into())
    }
    fn request_data(
        &self,
        _request: nautilus_common::messages::data::RequestCustomData,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_data").into())
    }
    fn request_book_snapshot(
        &self,
        _request: nautilus_common::messages::data::RequestBookSnapshot,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_book_snapshot").into())
    }
    fn request_quotes(
        &self,
        _request: nautilus_common::messages::data::RequestQuotes,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_quotes").into())
    }
    fn request_trades(
        &self,
        _request: nautilus_common::messages::data::RequestTrades,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_trades").into())
    }
    fn request_funding_rates(
        &self,
        _request: nautilus_common::messages::data::RequestFundingRates,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_funding_rates").into())
    }
    fn request_forward_prices(
        &self,
        _request: nautilus_common::messages::data::RequestForwardPrices,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_forward_prices").into())
    }
    fn request_bars(
        &self,
        _request: nautilus_common::messages::data::RequestBars,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_bars").into())
    }
    fn request_book_depth(
        &self,
        _request: nautilus_common::messages::data::RequestBookDepth,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_book_depth").into())
    }
    fn request_book_deltas(
        &self,
        _request: nautilus_common::messages::data::RequestBookDeltas,
    ) -> anyhow::Result<()> {
        Err(BackpackDataError::Unsupported("request_book_deltas").into())
    }
}
