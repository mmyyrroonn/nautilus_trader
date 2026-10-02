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

//! Native read-only lifecycle and account reports using owned bounded tasks.
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory},
    messages::{ExecutionEvent, ExecutionReport, execution::*},
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::{
    execution::emitter::ExecutionEventEmitter,
    task::{TaskGroup, TaskGroupGuard},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType},
    identifiers::{AccountId, ClientId, InstrumentId, TraderId, Venue},
    instruments::InstrumentAny,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};
use nautilus_network::{
    SocketState, SocketStateSink,
    transport::Message,
    websocket::{WebSocketClient, WebSocketConfig},
};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    config::BackpackExecutionClientConfig,
    private::{BackpackPrivateFact, MAX_PRIVATE_FRAME_BYTES, decode_private},
    telemetry::{BackpackAccountHealth, BackpackAccountState, BackpackAccountTelemetry},
};
use crate::{
    account::{
        BackpackAccountError,
        client::{BackpackAccountReader, BackpackAccountSnapshot, BackpackRestingOrder},
        pagination::BackpackHistoryWindow,
        reconciliation::{
            BackpackAppliedFill, BackpackFillKey, BackpackFillReconciler, BackpackFillStage,
        },
        reports::{
            BackpackReportContext, fill_report, order_report, position_report, wallet_report,
        },
    },
    http::{
        client::{BackpackClock, BackpackHttpClient, BackpackSystemClock},
        request::{BackpackReadOperation, BackpackReadRequest},
    },
    identity::BackpackClientIdStore,
    models::BackpackMarket,
    provider::BackpackInstrumentProvider,
    signing::{BackpackParameters, BackpackReceiveWindow},
};
fn now() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}
#[derive(Debug)]
struct Shared {
    config: BackpackExecutionClientConfig,
    run: u64,
    telemetry: BackpackAccountTelemetry,
    gate: Arc<Mutex<super::telemetry::Gate>>,
    identities: BackpackClientIdStore,
    provider: Mutex<BackpackInstrumentProvider>,
    fills: Mutex<BackpackFillReconciler>,
    emitter: ExecutionEventEmitter,
    http: BackpackHttpClient,
    reader: BackpackAccountReader,
}
impl Shared {
    fn token(&self) -> anyhow::Result<(u64, u64)> {
        let g = self.gate.lock();
        anyhow::ensure!(
            g.running && g.health.transport_connected && g.health.run_id == Some(self.run),
            "account client is stopped or disconnected"
        );
        Ok((
            g.health.generation,
            g.health
                .connection_epoch
                .ok_or_else(|| anyhow::anyhow!("account session has no epoch"))?,
        ))
    }
    fn valid(&self, generation: u64, epoch: u64) -> bool {
        let g = self.gate.lock();
        g.running
            && g.health.run_id == Some(self.run)
            && g.health.generation == generation
            && g.health.connection_epoch == Some(epoch)
    }
    fn fault(&self, generation: u64, reason: &str) {
        let mut g = self.gate.lock();
        if g.running && g.health.run_id == Some(self.run) && g.health.generation == generation {
            g.fault(reason);
        }
    }
    fn window(
        &self,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
    ) -> anyhow::Result<BackpackHistoryWindow> {
        let cutoff = end.unwrap_or_else(now).as_u64() / 1_000_000;
        let lookback = u64::try_from(self.config.policy.recovery_lookback.as_millis())?;
        let from = start.map_or(cutoff.saturating_sub(lookback), |t| t.as_u64() / 1_000_000);
        Ok(BackpackHistoryWindow::new(from, cutoff)?)
    }
    fn symbol(&self, id: InstrumentId) -> anyhow::Result<String> {
        Ok(self
            .provider
            .lock()
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("instrument outside validated allowlist"))?
            .raw_symbol
            .to_string())
    }
    async fn metadata(&self, generation: u64, cancel: &CancellationToken) -> anyhow::Result<()> {
        let revision = self.gate.lock().revision;
        let request = BackpackReadRequest::new(
            BackpackReadOperation::Markets,
            BackpackParameters::default(),
        )?;
        let response = self.http.read(&request, None, None, cancel).await?;
        anyhow::ensure!(
            response.body().len() <= 8 * 1024 * 1024,
            "metadata response exceeds bound"
        );
        let markets: Vec<BackpackMarket> = serde_json::from_slice(response.body())
            .map_err(|_| anyhow::anyhow!("invalid market metadata"))?;
        let gate = self.gate.lock();
        anyhow::ensure!(
            gate.running
                && gate.health.run_id == Some(self.run)
                && gate.health.generation == generation
                && gate.health.connection_epoch == Some(0)
                && gate.revision == revision,
            "stale market metadata"
        );
        self.provider
            .lock()
            .replace_markets(&markets, now())
            .map_err(|_| anyhow::anyhow!("invalid market metadata"))?;
        Ok(())
    }
    fn publish_snapshot(
        &self,
        snapshot: BackpackAccountSnapshot,
        generation: u64,
        epoch: u64,
        revision: u64,
    ) -> anyhow::Result<()> {
        let mut gate = self.gate.lock();
        anyhow::ensure!(
            gate.running
                && gate.health.run_id == Some(self.run)
                && gate.health.generation == generation
                && gate.health.connection_epoch == Some(epoch)
                && gate.revision == revision,
            "stale account snapshot"
        );
        let provider = self.provider.lock();
        let context = BackpackReportContext {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        let balances = snapshot
            .balances
            .iter()
            .map(|(symbol, b)| wallet_report(symbol, b).map(|b| b.trading_balance))
            .collect::<Result<Vec<_>, _>>()?;
        let mut reports = Vec::new();
        let mut gaps = snapshot.gaps;
        for raw in snapshot.open_orders {
            if !self.config.scope.symbols().contains(&raw.symbol) {
                continue;
            }
            let observation = order_report(raw, &context)?;
            gaps.extend(observation.gaps);
            if let Some(report) = observation.report {
                reports.push(ExecutionReport::Order(Box::new(report)));
            }
        }
        for position in snapshot.positions {
            if !self.config.scope.symbols().contains(&position.symbol) {
                continue;
            }
            reports.push(ExecutionReport::Position(Box::new(position_report(
                &position, &context,
            )?)));
        }
        gate.health
            .evidence_gaps
            .extend(gaps.iter().map(|gap| format!("{gap:?}")));
        let info = Some(Params::from_index_map(
            [
                ("backpack_read_only".into(), serde_json::json!(true)),
                ("backpack_execution_ready".into(), serde_json::json!(false)),
                (
                    "backpack_wallet_trading_balance_only".into(),
                    serde_json::json!(true),
                ),
                (
                    "backpack_margin_coverage_unknown".into(),
                    serde_json::json!(true),
                ),
            ]
            .into_iter()
            .collect(),
        ));
        // Empty balances carry no fabricated zero balance. Margins/equity are not inferred.
        if !balances.is_empty() {
            self.emitter.try_emit_account_state(
                balances,
                Vec::new(),
                true,
                UnixNanos::from(0),
                info,
            )?;
        }
        for report in reports {
            match report {
                ExecutionReport::Order(report) => {
                    self.emit_order_without_inference(report, &mut gate)?;
                }
                other => self.emitter.try_send_execution_report(other)?,
            }
        }
        gate.health.rest_snapshot_observed = true;
        gate.health.state = BackpackAccountState::Degraded;
        gate.health.recovery_count = gate
            .health
            .recovery_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("recovery counter exhausted"))?;
        gate.health.last_observed_ns = Some(now().as_u64());
        Ok(())
    }
    async fn recover(
        &self,
        generation: u64,
        epoch: u64,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let revision = {
            let g = self.gate.lock();
            anyhow::ensure!(g.pending_frames == 0, "private frames await delivery");
            g.revision
        };
        let snapshot = self.reader.snapshot(cancel).await?;
        self.publish_snapshot(snapshot, generation, epoch, revision)?;
        let window = self.window(None, None)?;
        let history = self.reader.fill_history(window, None, cancel).await?;
        let mut gate = self.gate.lock();
        anyhow::ensure!(
            gate.current(self.run, generation, epoch)
                || (gate.running
                    && !gate.health.transport_connected
                    && gate.health.run_id == Some(self.run)
                    && gate.health.generation == generation
                    && gate.health.connection_epoch == Some(epoch)
                    && gate.revision == revision),
            "stale fill recovery"
        );
        let provider = self.provider.lock();
        let context = BackpackReportContext {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        gate.health
            .evidence_gaps
            .extend(history.evidence.gaps.iter().map(|gap| format!("{gap:?}")));
        for fill in history.records {
            if !self.config.scope.symbols().contains(&fill.symbol) {
                continue;
            }
            let observation = fill_report(fill, &context)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report {
                self.stage_emit(report)?;
            }
        }
        gate.health.pending_fills = self.fills.lock().pending_in_event_order().len();
        Ok(())
    }
    // Native engine cumulative reports can synthesize unobserved fills/fees. Only
    // zero-execution lifecycle observations may be auto-published; query DTOs retain
    // real cumulative quantities. Economics use independently staged true FillReports.
    fn emit_order_without_inference(
        &self,
        report: Box<OrderStatusReport>,
        gate: &mut super::telemetry::Gate,
    ) -> anyhow::Result<()> {
        if !report.filled_qty.is_zero() {
            gate.health
                .evidence_gaps
                .insert("CumulativeOrderReportUnpublished".into());
            return Ok(());
        }
        self.emitter
            .try_send_execution_report(ExecutionReport::Order(report))
    }
    fn stage_emit(&self, report: FillReport) -> anyhow::Result<()> {
        if let BackpackFillStage::Pending(report) = self.fills.lock().stage(report)? {
            self.emitter
                .try_send_execution_report(ExecutionReport::Fill(report))?;
        }
        Ok(())
    }
    fn frame(&self, generation: u64, epoch: u64, bytes: &[u8]) -> anyhow::Result<()> {
        let mut gate = self.gate.lock();
        anyhow::ensure!(
            gate.current(self.run, generation, epoch),
            "stale private frame"
        );
        gate.pending_frames = gate.pending_frames.saturating_sub(1);
        let provider = self.provider.lock();
        let context = BackpackReportContext {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        let observation = decode_private(bytes, &context)?;
        gate.health
            .evidence_gaps
            .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
        if let Some(topic) = observation.topic {
            gate.health.observed_topics.insert(topic);
        }
        for fact in observation.facts {
            match fact {
                BackpackPrivateFact::Order(report) => {
                    self.emit_order_without_inference(report, &mut gate)?;
                }
                BackpackPrivateFact::Fill(report) => self.stage_emit(*report)?,
                BackpackPrivateFact::Position(report) => self
                    .emitter
                    .try_send_execution_report(ExecutionReport::Position(report))?,
                BackpackPrivateFact::Wallet {
                    asset,
                    balance,
                    ts_event,
                } => {
                    let info = Some(Params::from_index_map(
                        [
                            ("backpack_execution_ready".into(), serde_json::json!(false)),
                            (
                                "backpack_partial_wallet_update".into(),
                                serde_json::json!(true),
                            ),
                            (
                                "backpack_staked".into(),
                                serde_json::json!({asset:balance.staked.as_decimal().to_string()}),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ));
                    self.emitter.try_emit_account_state(
                        vec![balance.trading_balance],
                        Vec::new(),
                        true,
                        ts_event,
                        info,
                    )?;
                }
            }
        }
        gate.revision = gate
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("private revision exhausted"))?;
        gate.health.last_observed_ns = Some(now().as_u64());
        gate.health.pending_fills = self.fills.lock().pending_in_event_order().len();
        Ok(())
    }
    async fn subscribe(&self, ws: &WebSocketClient, epoch: u64) -> anyhow::Result<()> {
        let signed = self.config.credential.subscription(
            self.config.scope.endpoints(),
            vec![
                "account.balanceUpdate".into(),
                "account.orderUpdate".into(),
                "account.positionUpdate".into(),
            ],
            BackpackSystemClock.timestamp_ms()?,
            BackpackReceiveWindow::default(),
        )?;
        ws.send_text_on_connection(signed.to_json().to_string(), None, epoch)
            .await
            .map_err(|_| anyhow::anyhow!("private subscription transport failure"))?;
        Ok(())
    }
    async fn order(
        &self,
        instrument: InstrumentId,
        venue: Option<&str>,
        client: Option<nautilus_model::identifiers::ClientOrderId>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let (generation, epoch) = self.token()?;
        let symbol = self.symbol(instrument)?;
        let numeric = if venue.is_none() {
            Some(
                self.identities
                    .venue_id(
                        &client.ok_or_else(|| anyhow::anyhow!("an order selector is required"))?,
                    )
                    .ok_or_else(|| anyhow::anyhow!("unknown durable client ID"))?,
            )
        } else {
            None
        };
        let observation = self
            .reader
            .resting_order(&symbol, venue, numeric, cancel)
            .await?;
        anyhow::ensure!(
            self.gate.lock().current(self.run, generation, epoch),
            "stale order response"
        );
        match observation {
            BackpackRestingOrder::UnknownNotResting => {
                self.fault(generation, "RestingAbsenceIsUnknown");
                Ok(None)
            }
            BackpackRestingOrder::Observed(raw) => {
                let observation = {
                    let provider = self.provider.lock();
                    let context = BackpackReportContext {
                        account_id: self.config.account_id,
                        instruments: &provider,
                        identities: &self.identities,
                        confirmed_orders: None,
                        ts_init: now(),
                    };
                    order_report(*raw, &context)?
                };
                for gap in observation.gaps {
                    self.fault(generation, &format!("{gap:?}"));
                }
                Ok(observation.report)
            }
        }
    }
}
#[derive(Debug)]
enum Input {
    Frame(u64, Vec<u8>),
    Reconnected(u64),
}
/// Send/Sync economic delivery handle, independent of the native client's thread-local cache.
/// Consumer callbacks run outside every adapter lock. Consumers must be idempotent by
/// BackpackFillKey, including competing acknowledgements and recovery after a crash.
/// Keeping this handle alive retains the original identity lock and pending deliveries.
#[derive(Clone, Debug)]
pub struct BackpackFillDelivery {
    shared: Arc<Shared>,
}
impl BackpackFillDelivery {
    /// Returns pending economic reports in event order, without marking delivery applied.
    #[must_use]
    pub fn pending(&self) -> Vec<FillReport> {
        self.shared
            .fills
            .lock()
            .pending_in_event_order()
            .into_iter()
            .cloned()
            .collect()
    }
    /// Returns only consumer-acknowledged receipts, suitable for its durable checkpoint.
    #[must_use]
    pub fn applied(&self) -> Vec<BackpackAppliedFill> {
        self.shared
            .fills
            .lock()
            .applied_records()
            .cloned()
            .collect()
    }
    /// Commits dedup after actual durable consumer application, with no adapter lock in callback.
    /// Concurrent calls may submit the same immutable receipt to an idempotent consumer.
    ///
    /// # Errors
    /// Returns an error for unknown keys, conflicting economics or failed application.
    pub fn acknowledge_with(
        &self,
        key: BackpackFillKey,
        commit: impl FnOnce(&BackpackAppliedFill) -> Result<(), BackpackAccountError>,
    ) -> Result<(), BackpackAccountError> {
        let receipt = { self.shared.fills.lock().pending_acknowledgement(key)? };
        commit(&receipt)?;
        let mut gate = self.shared.gate.lock();
        let count = {
            let mut fills = self.shared.fills.lock();
            fills.acknowledge_committed(&receipt)?;
            fills.pending_in_event_order().len()
        };
        gate.health.pending_fills = count;
        Ok(())
    }
}
/// Real native account client. Connection is read-only; readiness remains explicitly degraded.
#[derive(Debug)]
pub struct BackpackExecutionClient {
    core: ExecutionClientCore,
    shared: Arc<Shared>,
    tasks: TaskGroup,
    websocket: Option<Arc<WebSocketClient>>,
}
impl BackpackExecutionClient {
    /// Opens the durable identity namespace and claims this config's telemetry, without network I/O.
    ///
    /// # Errors
    /// Returns an error for identity/lock corruption, duplicate config owner or invalid transport.
    pub fn new(
        trader: TraderId,
        name: &str,
        config: BackpackExecutionClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Self> {
        let telemetry = config.telemetry.clone();
        let (run, gate) = telemetry.claim()?;
        let result = (|| {
            let identities =
                BackpackClientIdStore::open(&config.identity_directory, &config.namespace)?;
            let http = BackpackHttpClient::new(
                config.scope.endpoints().clone(),
                Some(config.credential.clone()),
                config.quota.clone(),
                config.http_policy,
                Arc::new(BackpackSystemClock),
            )?;
            let reader = BackpackAccountReader::new(http.clone(), config.read_budget);
            let core = ExecutionClientCore::new(
                trader,
                ClientId::new_checked(name)?,
                Venue::from("BACKPACK"),
                OmsType::Netting,
                config.account_id,
                AccountType::Margin,
                None,
                cache,
            );
            let emitter = ExecutionEventEmitter::new(
                get_atomic_clock_realtime(),
                trader,
                config.account_id,
                AccountType::Margin,
                None,
            );
            let shared = Arc::new(Shared {
                provider: Mutex::new(BackpackInstrumentProvider::new(config.scope.clone())),
                fills: Mutex::new(BackpackFillReconciler::from_applied(
                    config.policy.fill_capacity,
                    [],
                )?),
                telemetry: config.telemetry.clone(),
                config,
                run,
                gate,
                identities,
                emitter,
                http,
                reader,
            });
            Ok(Self {
                core,
                shared,
                tasks: TaskGroup::new(),
                websocket: None,
            })
        })();
        if result.is_err() {
            telemetry.release(run);
        }
        result
    }
    #[must_use]
    pub fn health(&self) -> BackpackAccountHealth {
        self.shared.telemetry.snapshot()
    }
    #[must_use]
    pub fn telemetry(&self) -> BackpackAccountTelemetry {
        self.shared.telemetry.clone()
    }
    /// Installs an explicit framework event sender before start. Enqueue is not economic ACK.
    pub fn set_event_sender(&mut self, sender: mpsc::UnboundedSender<ExecutionEvent>) {
        let mut emitter = self.shared.emitter.clone();
        emitter.set_sender(sender);
    }
    #[must_use]
    pub fn fill_delivery(&self) -> BackpackFillDelivery {
        BackpackFillDelivery {
            shared: self.shared.clone(),
        }
    }
    /// Restores consumer-applied receipts before starting. No account observation creates receipts.
    ///
    /// # Errors
    /// Returns an error once started or for invalid/conflicting receipt records.
    pub fn restore_applied_fills(
        &mut self,
        records: Vec<BackpackAppliedFill>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.core.is_started() && self.health().generation == 0 && self.tasks.is_empty(),
            "restore receipts before first start"
        );
        *self.shared.fills.lock() =
            BackpackFillReconciler::from_applied(self.shared.config.policy.fill_capacity, records)?;
        Ok(())
    }
    /// Acknowledges actual consumer economic application atomically with its durable receipt.
    /// Channel delivery alone cannot call this method on the consumer's behalf.
    ///
    /// # Errors
    /// Returns an error for unknown keys or failed durable consumer application.
    pub fn acknowledge_fill_with(
        &self,
        key: BackpackFillKey,
        commit: impl FnOnce(&BackpackAppliedFill) -> Result<(), BackpackAccountError>,
    ) -> Result<(), BackpackAccountError> {
        self.fill_delivery().acknowledge_with(key, commit)
    }
    async fn connect_inner(
        &mut self,
        generation: u64,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        self.shared.metadata(generation, &cancel).await?;
        self.shared.recover(generation, 0, &cancel).await?;
        let (tx, rx) = mpsc::channel(self.shared.config.policy.input_capacity);
        let gate = self.shared.gate.clone();
        let run = self.shared.run;
        let frame_tx = tx.clone();
        let handler = Arc::new(move |epoch: u64, message: Message| {
            let input = match message {
                Message::Text(bytes)
                    if bytes.as_ref() == nautilus_network::RECONNECTED.as_bytes() =>
                {
                    Input::Reconnected(epoch)
                }
                Message::Text(bytes) => {
                    if bytes.len() > MAX_PRIVATE_FRAME_BYTES {
                        let mut g = gate.lock();
                        if g.running
                            && g.health.run_id == Some(run)
                            && g.health.generation == generation
                        {
                            g.fault("PrivateFrameBound");
                        }
                        return;
                    }
                    Input::Frame(epoch, bytes.to_vec())
                }
                Message::Binary(_) => {
                    let mut g = gate.lock();
                    if g.running
                        && g.health.run_id == Some(run)
                        && g.health.generation == generation
                    {
                        g.fault("UnexpectedPrivateBinary");
                    }
                    return;
                }
                _ => return,
            };
            let mut g = gate.lock();
            if !g.running || g.health.run_id != Some(run) || g.health.generation != generation {
                return;
            }
            if let Input::Reconnected(epoch) = input {
                if g.health
                    .connection_epoch
                    .is_some_and(|previous| epoch <= previous)
                {
                    return;
                }
                g.health.connection_epoch = Some(epoch);
                g.health.transport_connected = true;
                g.pending_frames = 0;
                g.health.rest_snapshot_observed = false;
                g.health.observed_topics.clear();
                g.fault("PrivateReconnectGap");
            } else if !g.current(run, generation, epoch) {
                return;
            }
            let is_frame = matches!(input, Input::Frame(..));
            if is_frame {
                g.revision = g.revision.saturating_add(1);
                g.pending_frames = g.pending_frames.saturating_add(1);
            }
            if frame_tx.try_send(input).is_err() {
                if is_frame {
                    g.pending_frames = g.pending_frames.saturating_sub(1);
                }
                g.fault("PrivateQueueOverflow");
            }
        });
        let gate = self.shared.gate.clone();
        let sink = SocketStateSink::new(move |state| {
            let mut g = gate.lock();
            if !g.running || g.health.run_id != Some(run) || g.health.generation != generation {
                return;
            }
            if state == SocketState::Disconnected {
                g.health.transport_connected = false;
                g.health.rest_snapshot_observed = false;
                g.health.observed_topics.clear();
                g.revision = g.revision.saturating_add(1);
                g.fault("PrivateConnectionLost");
            }
        });
        let ws_config = WebSocketConfig::builder()
            .url(
                self.shared
                    .config
                    .scope
                    .endpoints()
                    .websocket_url()
                    .to_owned(),
            )
            .heartbeat_interval_secs(10)
            .heartbeat_timeout_secs(30)
            .connect_timeout_ms(10_000)
            .reconnect_delay_initial_ms(100)
            .reconnect_delay_max_ms(2000)
            .reconnect_max_attempts(20)
            .build()?;
        let ws = Arc::new(
            WebSocketClient::epoch_builder()
                .config(ws_config)
                .epoch_handler(handler)
                .state_sink(sink)
                .cancellation_token(cancel.clone())
                .connect()
                .await
                .map_err(|_| anyhow::anyhow!("private connection failure"))?,
        );
        self.websocket = Some(ws.clone());
        {
            let mut g = self.shared.gate.lock();
            anyhow::ensure!(
                g.running
                    && g.health.run_id == Some(self.shared.run)
                    && g.health.generation == generation,
                "stale private connect"
            );
            g.health.transport_connected = true;
            g.health.state = BackpackAccountState::Degraded;
        }
        self.shared.subscribe(&ws, 0).await?;
        let shared = self.shared.clone();
        let interval = self.shared.config.policy.recovery_interval;
        self.tasks.spawn(async move {
            run_session(shared, generation, ws, rx, cancel, interval).await;
        })?;
        Ok(())
    }
}
async fn run_session(
    shared: Arc<Shared>,
    generation: u64,
    ws: Arc<WebSocketClient>,
    mut rx: mpsc::Receiver<Input>,
    cancel: CancellationToken,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            ()=cancel.cancelled()=>break,
            input=rx.recv()=>match input {
                Some(Input::Frame(epoch,bytes))=>{
                    if shared.frame(generation,epoch,&bytes).is_err(){
                        let mut g=shared.gate.lock();if g.current(shared.run,generation,epoch){g.health.parse_failures=g.health.parse_failures.saturating_add(1);g.fault("PrivateParseOrDeliveryFailure");}
                    }
                }
                Some(Input::Reconnected(epoch))=>{
                    if !shared.valid(generation,epoch){continue;}
                    if shared.subscribe(&ws,epoch).await.is_err(){shared.fault(generation,"PrivateSubscriptionTransportFailure");continue;}
                    if shared.recover(generation,epoch,&cancel).await.is_err(){shared.fault(generation,"RecoveryIncomplete");}
                }
                None=>break,
            },
            _=tick.tick()=>{
                if let Ok((current,epoch))=shared.token(){if current!=generation{break;}
                    if shared.recover(generation,epoch,&cancel).await.is_err(){shared.fault(generation,"RecoveryIncomplete");}
                } else {break;}
            }
        }
    }
}
impl Drop for BackpackExecutionClient {
    fn drop(&mut self) {
        self.tasks.abort();
        self.shared.telemetry.release(self.shared.run);
    }
}
/// Normal framework factory, sharing no state across independent configurations.
#[derive(Debug, Default)]
pub struct BackpackExecutionClientFactory;
impl ExecutionClientFactory for BackpackExecutionClientFactory {
    fn create(
        &self,
        trader: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<BackpackExecutionClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("invalid Backpack execution configuration"))?;
        Ok(Box::new(BackpackExecutionClient::new(
            trader,
            name,
            config.clone(),
            cache,
        )?))
    }
    fn name(&self) -> &'static str {
        "BACKPACK"
    }
    fn config_type(&self) -> &'static str {
        "BackpackExecutionClientConfig"
    }
}
#[async_trait(?Send)]
impl ExecutionClient for BackpackExecutionClient {
    fn is_connected(&self) -> bool {
        let h = self.health();
        h.transport_connected && h.state != BackpackAccountState::Stopped
    }
    fn client_id(&self) -> ClientId {
        self.core.client_id
    }
    fn account_id(&self) -> AccountId {
        self.core.account_id
    }
    fn venue(&self) -> Venue {
        self.core.venue
    }
    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }
    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }
    fn provides_bulk_position_coverage(&self, _: InstrumentId) -> bool {
        false
    }
    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.shared
            .emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }
    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        if !self.shared.emitter.is_initialized() {
            let sender = nautilus_common::live::runner::try_get_exec_event_sender()
                .ok_or_else(|| anyhow::anyhow!("execution event sender is not installed"))?;
            self.set_event_sender(sender);
        }
        self.core.set_started();
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        {
            let mut g = self.shared.gate.lock();
            if g.health.run_id == Some(self.shared.run) {
                g.running = false;
                g.health.transport_connected = false;
                g.health.state = BackpackAccountState::Stopped;
                g.health.observed_topics.clear();
            }
        }
        self.tasks.begin_shutdown();
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if self.websocket.is_some() {
            self.disconnect().await?;
        }
        if !self.tasks.is_open() {
            let budget = self.shared.config.policy.shutdown_timeout;
            self.tasks.finish_shutdown(budget, budget).await?;
            self.tasks.start_generation()?;
        }
        self.start()?;
        let generation = {
            let mut g = self.shared.gate.lock();
            g.health.generation = g
                .health
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("account generation exhausted"))?;
            g.running = true;
            g.pending_frames = 0;
            g.health.connection_epoch = Some(0);
            g.health.state = BackpackAccountState::Connecting;
            g.health.rest_snapshot_observed = false;
            g.health.observed_topics.clear();
            g.health.private_subscription_confirmed = false;
            g.health.generation
        };
        let gate = self.shared.gate.clone();
        let run = self.shared.run;
        let guard = TaskGroupGuard::new(&[&self.tasks], move || {
            let mut g = gate.lock();
            if g.health.run_id == Some(run) && g.health.generation == generation {
                g.running = false;
                g.health.transport_connected = false;
                g.health.state = BackpackAccountState::Stopped;
            }
        });
        let cancel = self.tasks.cancellation_token();
        let budget = self.shared.config.policy.connect_timeout;
        let result = tokio::time::timeout(budget, self.connect_inner(generation, cancel))
            .await
            .map_err(|_| anyhow::anyhow!("account connect budget exhausted"))?;
        if let Err(error) = result {
            drop(guard);
            self.disconnect().await?;
            return Err(error);
        }
        guard.disarm();
        self.core.set_connected();
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        let budget = self.shared.config.policy.shutdown_timeout;
        if let Some(ws) = &self.websocket {
            tokio::time::timeout(budget, ws.disconnect())
                .await
                .map_err(|_| anyhow::anyhow!("private shutdown budget exhausted"))?;
        }
        self.tasks.finish_shutdown(budget, budget).await?;
        self.websocket = None;
        Ok(())
    }
    fn submit_order(&self, _: SubmitOrder) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn submit_order_list(&self, _: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn modify_order(&self, _: ModifyOrder) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn batch_modify_orders(&self, _: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn cancel_order(&self, _: CancelOrder) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn cancel_all_orders(&self, _: CancelAllOrders) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn batch_cancel_orders(&self, _: BatchCancelOrders) -> anyhow::Result<()> {
        anyhow::bail!("Backpack native account client is read-only")
    }
    fn calculate_commission(
        &self,
        _: &InstrumentAny,
        _: Quantity,
        _: Price,
        _: LiquiditySide,
    ) -> anyhow::Result<Option<Money>> {
        anyhow::bail!(
            "Backpack fees require a true venue fill, never inferred cumulative execution"
        )
    }
    async fn generate_mass_status(
        &self,
        _: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        anyhow::bail!("Backpack account history does not establish complete mass-status coverage")
    }
    fn query_account(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.account_id == self.core.account_id,
            "account label mismatch"
        );
        let shared = self.shared.clone();
        let (generation, epoch) = shared.token()?;
        let cancel = self.tasks.cancellation_token();
        self.tasks.spawn(async move {
            if shared.recover(generation, epoch, &cancel).await.is_err() {
                shared.fault(generation, "AccountQueryIncomplete");
            }
        })?;
        Ok(())
    }
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let shared = self.shared.clone();
        let (generation, epoch) = shared.token()?;
        let cancel = self.tasks.cancellation_token();
        self.tasks.spawn(async move {
            match shared
                .order(
                    cmd.instrument_id,
                    cmd.venue_order_id.as_ref().map(|v| v.as_str()),
                    Some(cmd.client_order_id),
                    &cancel,
                )
                .await
            {
                Ok(Some(report)) => {
                    let mut g = shared.gate.lock();
                    if g.current(shared.run, generation, epoch)
                        && shared
                            .emit_order_without_inference(Box::new(report), &mut g)
                            .is_err()
                    {
                        drop(g);
                        shared.fault(generation, "OrderQueryDeliveryFailure");
                    }
                }
                Ok(None) => {}
                Err(_) => shared.fault(generation, "OrderQueryIncomplete"),
            }
        })?;
        Ok(())
    }
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument = cmd
            .instrument_id
            .ok_or_else(|| anyhow::anyhow!("an instrument is required for resting order query"))?;
        self.shared
            .order(
                instrument,
                cmd.venue_order_id.as_ref().map(|v| v.as_str()),
                cmd.client_order_id,
                &self.tasks.cancellation_token(),
            )
            .await
    }
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let (generation, epoch) = self.shared.token()?;
        let cancel = self.tasks.cancellation_token();
        let validated_symbol = cmd
            .instrument_id
            .map(|id| self.shared.symbol(id))
            .transpose()?;
        let (records, gaps) = if cmd.open_only {
            let snapshot = self.shared.reader.snapshot(&cancel).await?;
            (snapshot.open_orders, snapshot.gaps)
        } else {
            let symbol = validated_symbol.clone();
            let history = self
                .shared
                .reader
                .order_history(
                    self.shared.window(cmd.start, cmd.end)?,
                    symbol.as_deref(),
                    &cancel,
                )
                .await?;
            (history.records, history.evidence.gaps)
        };
        let mut gate = self.shared.gate.lock();
        anyhow::ensure!(
            gate.current(self.shared.run, generation, epoch),
            "stale order history"
        );
        gate.health
            .evidence_gaps
            .extend(gaps.iter().map(|gap| format!("{gap:?}")));
        let provider = self.shared.provider.lock();
        let context = BackpackReportContext {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        let mut result = Vec::new();
        for raw in records {
            if !self.shared.config.scope.symbols().contains(&raw.symbol) {
                continue;
            }
            if cmd
                .instrument_id
                .is_some_and(|id| id.symbol.as_str() != raw.symbol)
            {
                continue;
            }
            let observation = order_report(raw, &context)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report {
                result.push(report);
            }
        }
        Ok(result)
    }
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let (generation, epoch) = self.shared.token()?;
        let cancel = self.tasks.cancellation_token();
        let symbol = cmd
            .instrument_id
            .map(|id| self.shared.symbol(id))
            .transpose()?;
        let history = self
            .shared
            .reader
            .fill_history(
                self.shared.window(cmd.start, cmd.end)?,
                symbol.as_deref(),
                &cancel,
            )
            .await?;
        let mut gate = self.shared.gate.lock();
        anyhow::ensure!(
            gate.current(self.shared.run, generation, epoch),
            "stale fill history"
        );
        gate.health
            .evidence_gaps
            .extend(history.evidence.gaps.iter().map(|gap| format!("{gap:?}")));
        let provider = self.shared.provider.lock();
        let context = BackpackReportContext {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        let mut result = Vec::new();
        for raw in history.records {
            if !self.shared.config.scope.symbols().contains(&raw.symbol)
                || cmd
                    .venue_order_id
                    .is_some_and(|v| v.as_str() != raw.order_id)
            {
                continue;
            }
            let observation = fill_report(raw, &context)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report
                && let BackpackFillStage::Pending(report) =
                    self.shared.fills.lock().stage(report)?
            {
                result.push(*report);
            }
        }
        gate.health.pending_fills = self.shared.fills.lock().pending_in_event_order().len();
        Ok(result)
    }
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::ensure!(
            cmd.start.is_none() && cmd.end.is_none(),
            "historical positions are unsupported"
        );
        let (generation, epoch) = self.shared.token()?;
        if let Some(id) = cmd.instrument_id {
            self.shared.symbol(id)?;
        }
        let snapshot = self
            .shared
            .reader
            .snapshot(&self.tasks.cancellation_token())
            .await?;
        let mut gate = self.shared.gate.lock();
        anyhow::ensure!(
            gate.current(self.shared.run, generation, epoch),
            "stale position snapshot"
        );
        gate.health
            .evidence_gaps
            .extend(snapshot.gaps.iter().map(|gap| format!("{gap:?}")));
        let provider = self.shared.provider.lock();
        let context = BackpackReportContext {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
            confirmed_orders: None,
            ts_init: now(),
        };
        snapshot
            .positions
            .iter()
            .filter(|p| {
                self.shared.config.scope.symbols().contains(&p.symbol)
                    && cmd
                        .instrument_id
                        .is_none_or(|id| id.symbol.as_str() == p.symbol)
            })
            .map(|p| position_report(p, &context).map_err(Into::into))
            .collect()
    }
}
