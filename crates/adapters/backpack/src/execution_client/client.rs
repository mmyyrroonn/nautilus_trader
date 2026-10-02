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
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory, OrderEventFactory},
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
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TraderId, Venue},
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
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
    identity::{IdentityReader, ReportReader},
    private::{BackpackPrivateFact, MAX_PRIVATE_FRAME_BYTES},
    restricted::{BackpackLoopbackSession, LoopbackExecution},
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
        reports::wallet_report,
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
    identities: IdentityReader,
    restricted: Option<Arc<LoopbackExecution>>,
    shutdown: Mutex<Option<crate::execution::owner::BackpackShutdownReport>>,
    provider: Mutex<BackpackInstrumentProvider>,
    fills: Mutex<BackpackFillReconciler>,
    acknowledgements: Mutex<BTreeSet<BackpackFillKey>>,
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
        let context = ReportReader {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            ts_init: now(),
        };
        if let Some(control) = &self.restricted {
            control.owner.guard().invalidate_account();
        }
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
            let observation = context.order(raw)?;
            gaps.extend(observation.gaps);
            if let Some(report) = observation.report {
                if let Some(control) = &self.restricted {
                    control.retain_order(report.instrument_id, observation.raw)?;
                } else {
                    reports.push(ExecutionReport::Order(Box::new(report)));
                }
            }
        }
        for position in snapshot.positions {
            if !self.config.scope.symbols().contains(&position.symbol) {
                continue;
            }
            reports.push(ExecutionReport::Position(Box::new(
                context.position(&position)?,
            )));
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
        let context = ReportReader {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            ts_init: now(),
        };
        gate.health
            .evidence_gaps
            .extend(history.evidence.gaps.iter().map(|gap| format!("{gap:?}")));
        for fill in history.records {
            if !self.config.scope.symbols().contains(&fill.symbol) {
                continue;
            }
            let observation = context.fill(fill)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report {
                if let Some(control) = &self.restricted {
                    control.owner.guard().invalidate_account();
                    control.retain_fill(
                        BackpackFillKey {
                            instrument_id: report.instrument_id,
                            trade_id: report.trade_id,
                        },
                        observation.raw,
                    )?;
                }
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
        if self.restricted.as_ref().is_some_and(|control| {
            !report
                .client_order_id
                .is_some_and(|id| control.released(id))
        }) {
            gate.health
                .evidence_gaps
                .insert("UnboundOrderReportUnpublished".into());
            return Ok(());
        }
        if !report.filled_qty.is_zero() {
            gate.health
                .evidence_gaps
                .insert("CumulativeOrderReportUnpublished".into());
            return Ok(());
        }
        self.emitter
            .try_send_execution_report(ExecutionReport::Order(report))
    }
    fn submission_observed(
        &self,
        order: &OrderAny,
        receipt: &crate::execution::BackpackMutationReceipt,
        factory: &OrderEventFactory,
    ) -> anyhow::Result<()> {
        let control = self
            .restricted
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client is read-only"))?;
        let (venue, instrument) = control
            .owner
            .confirmed_binding(order.client_order_id())
            .ok_or_else(|| anyhow::anyhow!("POST binding is absent"))?;
        anyhow::ensure!(
            instrument == order.instrument_id(),
            "POST binding instrument mismatch"
        );
        let raw: serde_json::Value = serde_json::from_slice(receipt.body())?;
        self.emitter
            .try_send_order_event(factory.generate_order_submitted(order, now()))?;
        let status = raw.get("status").and_then(serde_json::Value::as_str);
        anyhow::ensure!(
            matches!(
                status,
                Some("New" | "PartiallyFilled" | "Filled" | "Cancelled" | "Expired")
            ),
            "unsupported POST lifecycle"
        );
        self.emitter
            .try_send_order_event(factory.generate_order_accepted(
                order,
                venue,
                UnixNanos::from(0),
                now(),
            ))?;
        control.release(order.client_order_id());
        self.flush_attributed()?;
        // A true terminal lifecycle may be published only after its cumulative amount
        // is covered by consumer-acknowledged true fills. No cumulative fill is synthesized.
        if matches!(status, Some("Cancelled" | "Expired")) {
            self.retain_mutation_terminal(order, &raw)?;
        }
        Ok(())
    }
    fn cancellation_observed(
        &self,
        order: &OrderAny,
        receipt: &crate::execution::BackpackMutationReceipt,
    ) -> anyhow::Result<()> {
        let raw: serde_json::Value = serde_json::from_slice(receipt.body())?;
        if matches!(
            raw.get("status").and_then(serde_json::Value::as_str),
            Some("Cancelled" | "Expired")
        ) {
            self.retain_mutation_terminal(order, &raw)?;
        }
        Ok(())
    }
    fn retain_mutation_terminal(
        &self,
        order: &OrderAny,
        value: &serde_json::Value,
    ) -> anyhow::Result<()> {
        // The original full response has already passed the guarded command matcher.
        // Fill missing read-only DTO fields only as Unknown, never as venue-derived time or economics.
        let mut value = value.clone();
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("terminal response is not an order"))?;
        object.entry("createdAt").or_insert(serde_json::json!(0));
        object
            .entry("selfTradePrevention")
            .or_insert(serde_json::json!("Unknown"));
        let raw: crate::account::models::BackpackOrder = serde_json::from_value(value)?;
        let control = self
            .restricted
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client is read-only"))?;
        control.retain_order(order.instrument_id(), raw)?;
        self.flush_terminals()
    }
    fn flush_attributed(&self) -> anyhow::Result<()> {
        let Some(control) = &self.restricted else {
            return Ok(());
        };
        let pending = control.pending.lock().clone();
        let provider = self.provider.lock();
        let context = ReportReader {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            ts_init: now(),
        };
        for raw in pending.into_values() {
            let observation = context.fill(raw)?;
            if let Some(report) = observation.report
                && report
                    .client_order_id
                    .is_some_and(|id| control.released(id))
            {
                self.stage_emit(report)?;
            }
        }
        drop(provider);
        self.flush_terminals()
    }
    fn flush_terminals(&self) -> anyhow::Result<()> {
        let Some(control) = &self.restricted else {
            return Ok(());
        };
        let orders = control.orders.lock().clone();
        let raw_fills = control.pending.lock().clone();
        let provider = self.provider.lock();
        let context = ReportReader {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            ts_init: now(),
        };
        for (_, raw) in orders {
            let observation = context.order(raw)?;
            let Some(report) = observation.report else {
                continue;
            };
            let Some(client) = report.client_order_id.filter(|id| control.released(*id)) else {
                continue;
            };
            control
                .owner
                .observe_owned_order(client, &observation.raw)?;
            if !matches!(
                report.order_status,
                nautilus_model::enums::OrderStatus::Canceled
                    | nautilus_model::enums::OrderStatus::Expired
            ) {
                continue;
            }
            let mut applied = rust_decimal::Decimal::ZERO;
            for fill in raw_fills
                .values()
                .filter(|fill| fill.order_id == report.venue_order_id.as_str())
            {
                let Some(fill) = context.fill(fill.clone())?.report else {
                    continue;
                };
                anyhow::ensure!(
                    fill.client_order_id == Some(client)
                        && fill.instrument_id == report.instrument_id,
                    "terminal fill attribution mismatch"
                );
                if matches!(
                    self.fills.lock().stage(fill.clone())?,
                    BackpackFillStage::AlreadyApplied
                ) {
                    applied = crate::execution::guard::add(applied, fill.last_qty.as_decimal())?;
                }
            }
            anyhow::ensure!(
                applied <= report.filled_qty.as_decimal(),
                "terminal cumulative quantity regressed"
            );
            if applied != report.filled_qty.as_decimal() {
                continue;
            }
            if control.terminal_published.lock().contains(&client) {
                continue;
            }
            let order = control.native_orders.lock().get(&client).cloned();
            let Some(order) = order else {
                continue;
            };
            let factory = OrderEventFactory::new(
                self.emitter.trader_id(),
                self.config.account_id,
                AccountType::Margin,
                None,
            );
            let event = if report.order_status == nautilus_model::enums::OrderStatus::Canceled {
                factory.generate_order_canceled(
                    &order,
                    Some(report.venue_order_id),
                    report.ts_last,
                    now(),
                )
            } else {
                factory.generate_order_expired(
                    &order,
                    Some(report.venue_order_id),
                    report.ts_last,
                    now(),
                )
            };
            self.emitter.try_send_order_event(event)?;
            control.terminal_published.lock().insert(client);
        }
        Ok(())
    }
    fn stage_emit(&self, report: FillReport) -> anyhow::Result<()> {
        if let BackpackFillStage::Pending(report) = self.fills.lock().stage(report)? {
            if self.restricted.as_ref().is_some_and(|control| {
                !report
                    .client_order_id
                    .is_some_and(|id| control.released(id))
            }) {
                return Ok(());
            }
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
        let context = ReportReader {
            account_id: self.config.account_id,
            instruments: &provider,
            identities: &self.identities,
            ts_init: now(),
        };
        let observation = context.private(bytes)?;
        if let Some(control) = &self.restricted
            && !observation.facts.is_empty()
        {
            control.owner.guard().invalidate_account();
        }
        gate.health
            .evidence_gaps
            .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
        if let Some(topic) = observation.topic {
            gate.health.observed_topics.insert(topic);
        }
        if let Some(control) = &self.restricted {
            for raw in observation.raw_orders {
                if let Some(report) = context.order(raw.clone())?.report {
                    if let Some(id) = report.client_order_id {
                        control.owner.observe_owned_order(id, &raw)?;
                    }
                    control.retain_order(report.instrument_id, raw)?;
                }
            }
            for raw in observation.raw_fills {
                let fill = context.fill(raw.clone())?;
                if let Some(report) = fill.report {
                    control.owner.guard().invalidate_account();
                    control.retain_fill(
                        BackpackFillKey {
                            instrument_id: report.instrument_id,
                            trade_id: report.trade_id,
                        },
                        raw,
                    )?;
                    self.stage_emit(report)?;
                }
            }
        }
        for fact in observation.facts {
            if self.restricted.is_some()
                && matches!(
                    &fact,
                    BackpackPrivateFact::Order(_) | BackpackPrivateFact::Fill(_)
                )
            {
                continue;
            }
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
        drop(provider);
        if self.restricted.is_some() {
            self.flush_terminals()?;
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
                    )?
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
                    let context = ReportReader {
                        account_id: self.config.account_id,
                        instruments: &provider,
                        identities: &self.identities,
                        ts_init: now(),
                    };
                    context.order(*raw)?
                };
                for gap in observation.gaps {
                    self.fault(generation, &format!("{gap:?}"));
                }
                if let Some(control) = &self.restricted {
                    control.owner.guard().invalidate_account();
                    if let Some(report) = &observation.report {
                        if let Some(id) = report.client_order_id {
                            control.owner.observe_owned_order(id, &observation.raw)?;
                        }
                        control.retain_order(report.instrument_id, observation.raw)?;
                        if !report.filled_qty.is_zero()
                            || !report
                                .client_order_id
                                .is_some_and(|id| control.released(id))
                        {
                            self.gate
                                .lock()
                                .health
                                .evidence_gaps
                                .insert("RestrictedCumulativeQueryUnpublished".into());
                            return Ok(None);
                        }
                    }
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
/// Economic receipt acknowledgement, distinct from channel delivery or order capacity release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackFillAcknowledgement {
    Applied,
    AlreadyApplied,
    InProgress,
}
struct FillAcknowledgementLease {
    shared: Arc<Shared>,
    key: BackpackFillKey,
}
impl Drop for FillAcknowledgementLease {
    fn drop(&mut self) {
        self.shared.acknowledgements.lock().remove(&self.key);
    }
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
    /// Commits one immutable receipt only after actual durable consumer application.
    /// No adapter lock is held during `commit`. Same-key concurrent/reentrant calls return
    /// InProgress without invoking the callback; applied duplicates return AlreadyApplied.
    /// A callback error retains pending work and releases its lease. The consumer must remain
    /// idempotent by key: an error or process crash may follow its actual durable commit.
    ///
    /// # Errors
    /// Returns an error for unknown keys, conflicting economics or failed consumer application.
    pub fn acknowledge_with(
        &self,
        key: BackpackFillKey,
        commit: impl FnOnce(&BackpackAppliedFill) -> Result<(), BackpackAccountError>,
    ) -> Result<BackpackFillAcknowledgement, BackpackAccountError> {
        let receipt = {
            let mut leases = self.shared.acknowledgements.lock();
            if leases.contains(&key) {
                return Ok(BackpackFillAcknowledgement::InProgress);
            }
            let fills = self.shared.fills.lock();
            if fills.applied_records().any(|record| record.key == key) {
                return Ok(BackpackFillAcknowledgement::AlreadyApplied);
            }
            let receipt = fills.pending_acknowledgement(key)?;
            leases.insert(key);
            receipt
        };
        let _lease = FillAcknowledgementLease {
            shared: self.shared.clone(),
            key,
        };
        commit(&receipt)?;
        let mut gate = self.shared.gate.lock();
        let count = {
            let mut fills = self.shared.fills.lock();
            fills.acknowledge_committed(&receipt)?;
            fills.pending_in_event_order().len()
        };
        gate.health.pending_fills = count;
        if self.shared.restricted.is_some() && self.shared.flush_terminals().is_err() {
            gate.fault("TerminalLifecycleUnpublished");
        }
        Ok(BackpackFillAcknowledgement::Applied)
    }
}
/// Real native account client, read-only by default; guarded local mutations require explicit opt-in.
/// Production execution readiness remains explicitly degraded.
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
            let identities = if let Some(mode) = &config.restricted {
                IdentityReader::Restricted(Arc::new(
                    crate::execution::owner::BackpackOrderOwner::new_checked(
                        crate::execution::owner::BackpackOrderOwnerConfig {
                            config: config.scope.clone(),
                            endpoints: config.scope.endpoints().clone(),
                            credential: config.credential.clone(),
                            quota: config.quota.clone(),
                            clock: Arc::new(BackpackSystemClock),
                            policy: mode.mutation_policy,
                            identities,
                            namespace: config.namespace.clone(),
                            authority: mode.authority.clone(),
                        },
                    )?,
                ))
            } else {
                IdentityReader::ReadOnly(identities)
            };
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
            let restricted = identities
                .owner()
                .map(|owner| {
                    LoopbackExecution::new(
                        owner.clone(),
                        config.policy.fill_capacity,
                        config.policy.input_capacity,
                    )
                    .map(Arc::new)
                })
                .transpose()?;
            gate.lock().restricted = restricted.as_ref().map(Arc::downgrade);
            let shared = Arc::new(Shared {
                provider: Mutex::new(BackpackInstrumentProvider::new(config.scope.clone())),
                fills: Mutex::new(BackpackFillReconciler::from_applied(
                    config.policy.fill_capacity,
                    [],
                )?),
                acknowledgements: Mutex::new(BTreeSet::new()),
                telemetry: config.telemetry.clone(),
                config,
                run,
                gate,
                identities,
                restricted,
                shutdown: Mutex::new(None),
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
    fn loopback(&self, token: BackpackLoopbackSession) -> anyhow::Result<Arc<LoopbackExecution>> {
        let gate = self.shared.gate.lock();
        anyhow::ensure!(
            gate.current(token.run, token.client_generation, token.private_epoch)
                && token.run == self.shared.run,
            "stale native loopback token"
        );
        let restricted = self
            .shared
            .restricted
            .clone()
            .ok_or_else(|| anyhow::anyhow!("native account client is read-only"))?;
        restricted.current(token)?;
        Ok(restricted)
    }
    /// Starts a new explicit local-peer evidence generation for the current private/public run.
    /// Caller-supplied local peer facts never turn account read uncertainty into production readiness.
    ///
    /// # Errors
    /// Returns an error in read-only mode or with a stopped/disconnected/stale public or private owner.
    pub fn begin_loopback_session(&self) -> anyhow::Result<BackpackLoopbackSession> {
        let gate = self.shared.gate.lock();
        let epoch = gate
            .health
            .connection_epoch
            .ok_or_else(|| anyhow::anyhow!("private epoch absent"))?;
        anyhow::ensure!(
            gate.current(self.shared.run, gate.health.generation, epoch),
            "private owner is not current"
        );
        let mode = self
            .shared
            .config
            .restricted
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client is read-only"))?;
        let control = self
            .shared
            .restricted
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client is read-only"))?;
        control.begin(
            self.shared.run,
            gate.health.generation,
            epoch,
            &self.shared.config.namespace,
            mode.public_telemetry.clone(),
        )
    }
    /// Accepts complete explicit local-peer account facts for the current opaque session.
    ///
    /// # Errors
    /// Returns an error for old tokens, absent fields, unsupported policy or stale evidence.
    pub fn accept_loopback_account(
        &self,
        token: BackpackLoopbackSession,
        facts: crate::execution::guard::BackpackLoopbackAccountFacts,
    ) -> anyhow::Result<()> {
        self.loopback(token)?
            .account(token, facts, now().as_u64() / 1_000_000)
    }
    /// Refreshes one admitted market from the actual native quote cache and validated REST metadata.
    /// Final admission additionally checks the current public token and exact original receipt times.
    ///
    /// # Errors
    /// Returns an error for old sessions, missing metadata/quote, invalid grids or stale observations.
    pub fn refresh_loopback_market(
        &self,
        token: BackpackLoopbackSession,
        id: InstrumentId,
    ) -> anyhow::Result<()> {
        let control = self.loopback(token)?;
        let quote = self
            .core
            .cache()
            .quote(&id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("native quote cache is empty"))?;
        let metadata = self
            .shared
            .provider
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("validated metadata is absent"))?;
        control.market(
            token,
            crate::execution::guard::BackpackLoopbackMarketFacts {
                generation: token.generation,
                quote,
                metadata,
            },
            now().as_u64() / 1_000_000,
        )
    }
    /// Invalidates only this currently admitted local-peer token; late invalidations cannot affect a replacement.
    ///
    /// # Errors
    /// Returns an error for a stopped/replaced session or read-only client.
    pub fn invalidate_loopback_session(
        &self,
        token: BackpackLoopbackSession,
    ) -> anyhow::Result<()> {
        self.loopback(token)?.invalidate();
        Ok(())
    }

    /// Reattaches a consumer-restored native order to its independently durable original POST binding.
    /// No numeric client ID observation creates ownership, resend permission or an economic ACK.
    ///
    /// # Errors
    /// Returns an error for a stale session, missing original cached order, advanced semantics,
    /// missing true POST binding or any immutable-intent/venue mismatch.
    pub fn restore_loopback_order(
        &self,
        token: BackpackLoopbackSession,
        id: ClientOrderId,
    ) -> anyhow::Result<()> {
        let control = self.loopback(token)?;
        let order = self
            .core
            .cache()
            .order(&id)
            .map(|order| (*order).clone())
            .ok_or_else(|| anyhow::anyhow!("original native order missing"))?;
        anyhow::ensure!(
            order.trader_id() == self.core.trader_id
                && order.status() != nautilus_model::enums::OrderStatus::Initialized,
            "native restoration has no accepted lifecycle"
        );
        let venue = order
            .venue_order_id()
            .ok_or_else(|| anyhow::anyhow!("native restored binding missing"))?;
        control
            .owner
            .restore_native_binding(&super::commands::cached_spec(&order)?, venue)?;
        control.retain_native(id, order)?;
        control.release(id);
        self.shared.flush_attributed()?;
        Ok(())
    }

    /// Accepts an explicit peer terminal attestation after all true fills have durable consumer ACKs.
    /// Cached order or portfolio values alone never release owner capacity.
    ///
    /// # Errors
    /// Returns an error for a stale session, nonterminal/missing observation, unbound identity,
    /// pending or mismatched true fills, or a missing independent durable economic reference.
    pub fn accept_loopback_terminal(
        &self,
        token: BackpackLoopbackSession,
        evidence: &crate::execution::owner::BackpackLoopbackTerminalEvidence,
    ) -> anyhow::Result<()> {
        let control = self.loopback(token)?;
        anyhow::ensure!(
            evidence.generation == token.generation,
            "stale terminal evidence"
        );
        let provider = self.shared.provider.lock();
        let context = ReportReader {
            account_id: self.shared.config.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
            ts_init: now(),
        };
        let raw = control
            .orders
            .lock()
            .get(&(evidence.instrument_id, evidence.venue_order_id))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("terminal observation missing"))?;
        let report = context
            .order(raw)?
            .report
            .ok_or_else(|| anyhow::anyhow!("terminal observation unusable"))?;
        anyhow::ensure!(
            report.client_order_id == Some(evidence.client_order_id)
                && matches!(
                    report.order_status,
                    nautilus_model::enums::OrderStatus::Filled
                        | nautilus_model::enums::OrderStatus::Canceled
                        | nautilus_model::enums::OrderStatus::Expired
                )
                && report.filled_qty.as_decimal() == evidence.cumulative_quantity,
            "terminal evidence mismatch"
        );
        let pending = control.pending.lock().clone();
        let mut applied = rust_decimal::Decimal::ZERO;
        for raw in pending
            .values()
            .filter(|fill| fill.order_id == evidence.venue_order_id.as_str())
        {
            let report = context
                .fill(raw.clone())?
                .report
                .ok_or_else(|| anyhow::anyhow!("true fill unusable"))?;
            anyhow::ensure!(
                report.client_order_id == Some(evidence.client_order_id)
                    && report.instrument_id == evidence.instrument_id,
                "terminal fill binding mismatch"
            );
            anyhow::ensure!(
                matches!(
                    self.shared.fills.lock().stage(report.clone())?,
                    BackpackFillStage::AlreadyApplied
                ),
                "true fill has no durable consumer ACK"
            );
            applied = crate::execution::guard::add(applied, report.last_qty.as_decimal())?;
        }
        anyhow::ensure!(
            applied == evidence.applied_fill_quantity && applied == evidence.cumulative_quantity,
            "terminal true fill quantity mismatch"
        );
        control.owner.acknowledge_reconciled_terminal(evidence)?;
        Ok(())
    }
    /// Returns persisted stop evidence; None means this client has not stopped its local-peer owner.
    #[must_use]
    pub fn loopback_shutdown_report(
        &self,
    ) -> Option<crate::execution::owner::BackpackShutdownReport> {
        *self.shared.shutdown.lock()
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
    ) -> Result<BackpackFillAcknowledgement, BackpackAccountError> {
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
        if let Some(control) = &self.shared.restricted {
            match control.stop() {
                Ok(report) => *self.shared.shutdown.lock() = Some(report),
                Err(_) => self
                    .shared
                    .gate
                    .lock()
                    .fault("DirtyShutdownCheckpointFailure"),
            }
        }
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
        if let Some(control) = &self.shared.restricted {
            *self.shared.shutdown.lock() = Some(control.stop()?);
        }
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
    fn submit_order(&self, command: SubmitOrder) -> anyhow::Result<()> {
        let control = self
            .shared
            .restricted
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Backpack native account client is read-only"))?;
        let token = control.token()?;
        self.loopback(token)?;
        let order = self
            .core
            .cache()
            .order(&command.client_order_id)
            .map(|order| (*order).clone())
            .ok_or_else(|| anyhow::anyhow!("native cached order is absent"))?;
        let spec = super::commands::submit_spec(
            &command,
            &order,
            self.core.trader_id,
            self.core.client_id,
        )?;
        control
            .owner
            .guard()
            .lock()?
            .reservation(&spec, now().as_u64() / 1_000_000, None)?;
        let permit = control
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("bounded mutation tasks exhausted"))?;
        control.retain_native(command.client_order_id, order.clone())?;
        let shared = self.shared.clone();
        let cancel = self.tasks.cancellation_token();
        self.tasks.spawn(async move {
            let _permit = permit;
            let result = control.owner.submit(spec, &cancel).await;
            let mut gate = shared.gate.lock();
            if !gate.current(token.run, token.client_generation, token.private_epoch)
                || control.current(token).is_err()
            {
                return;
            }
            let factory = OrderEventFactory::new(
                shared.emitter.trader_id(),
                shared.config.account_id,
                AccountType::Margin,
                None,
            );
            let result = match result {
                Ok(receipt) => shared.submission_observed(&order, &receipt, &factory),
                Err(error) => {
                    use crate::http::error::BackpackRequestOutcome;
                    match error.outcome() {
                        BackpackRequestOutcome::NotSent => {
                            shared
                                .emitter
                                .try_send_order_event(factory.generate_order_denied(
                                    &order,
                                    "GuardedLocalRefusal",
                                    now(),
                                ))
                        }
                        BackpackRequestOutcome::VenueRejected => shared
                            .emitter
                            .try_send_order_event(factory.generate_order_submitted(&order, now()))
                            .and_then(|()| {
                                shared.emitter.try_send_order_event(
                                    factory.generate_order_rejected(
                                        &order,
                                        "DefinitiveVenueRejection",
                                        UnixNanos::from(0),
                                        now(),
                                        false,
                                    ),
                                )
                            }),
                        BackpackRequestOutcome::Unknown => {
                            let result = shared.emitter.try_send_order_event(
                                factory.generate_order_submitted(&order, now()),
                            );
                            gate.fault("MutationOutcomeUnknown");
                            result
                        }
                    }
                }
            };
            if result.is_err() {
                gate.fault("MutationLifecycleUnpublished");
            }
        })?;
        Ok(())
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
    fn cancel_order(&self, command: CancelOrder) -> anyhow::Result<()> {
        let control = self
            .shared
            .restricted
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Backpack native account client is read-only"))?;
        let token = control.token()?;
        self.loopback(token)?;
        let order = self
            .core
            .cache()
            .order(&command.client_order_id)
            .map(|order| (*order).clone())
            .ok_or_else(|| anyhow::anyhow!("native cached order is absent"))?;
        let (venue, instrument) = control
            .owner
            .confirmed_binding(command.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("order is not independently owned"))?;
        anyhow::ensure!(
            instrument == command.instrument_id,
            "cancel instrument mismatch"
        );
        super::commands::validate_cancel(
            &command,
            &order,
            venue,
            self.core.trader_id,
            self.core.client_id,
        )?;
        let permit = control
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("bounded mutation tasks exhausted"))?;
        let shared = self.shared.clone();
        let cancel = self.tasks.cancellation_token();
        self.tasks.spawn(async move {
            let _permit = permit;
            let result = control
                .owner
                .cancel_owned(command.client_order_id, &cancel)
                .await;
            let mut gate = shared.gate.lock();
            if !gate.current(token.run, token.client_generation, token.private_epoch)
                || control.current(token).is_err()
            {
                return;
            }
            match result {
                Ok(receipt)
                    if receipt.status()
                        == crate::execution::BackpackMutationStatus::CancelPending =>
                {
                    gate.health.evidence_gaps.insert("CancelPending".into());
                }
                Ok(receipt) => {
                    if shared.cancellation_observed(&order, &receipt).is_err() {
                        gate.fault("CancelLifecycleUnpublished");
                    }
                }
                Err(error) => {
                    if error.outcome() == crate::http::error::BackpackRequestOutcome::Unknown {
                        gate.fault("CancelOutcomeUnknown");
                    } else {
                        gate.health.evidence_gaps.insert("CancelRefused".into());
                    }
                }
            }
        })?;
        Ok(())
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
        let context = ReportReader {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
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
            let observation = context.order(raw)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report {
                if let Some(control) = &self.shared.restricted {
                    control.owner.guard().invalidate_account();
                    if let Some(id) = report.client_order_id {
                        control.owner.observe_owned_order(id, &observation.raw)?;
                    }
                    control.retain_order(report.instrument_id, observation.raw)?;
                    if !report.filled_qty.is_zero()
                        || !report
                            .client_order_id
                            .is_some_and(|id| control.released(id))
                    {
                        gate.health
                            .evidence_gaps
                            .insert("RestrictedCumulativeQueryUnpublished".into());
                        continue;
                    }
                }
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
        let context = ReportReader {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
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
            let observation = context.fill(raw)?;
            gate.health
                .evidence_gaps
                .extend(observation.gaps.iter().map(|gap| format!("{gap:?}")));
            if let Some(report) = observation.report {
                if let Some(control) = &self.shared.restricted {
                    control.owner.guard().invalidate_account();
                    control.retain_fill(
                        BackpackFillKey {
                            instrument_id: report.instrument_id,
                            trade_id: report.trade_id,
                        },
                        observation.raw,
                    )?;
                }
                if let BackpackFillStage::Pending(report) =
                    self.shared.fills.lock().stage(report)?
                    && self.shared.restricted.as_ref().is_none_or(|control| {
                        report
                            .client_order_id
                            .is_some_and(|id| control.released(id))
                    })
                {
                    result.push(*report);
                }
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
        let context = ReportReader {
            account_id: self.core.account_id,
            instruments: &provider,
            identities: &self.shared.identities,
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
            .map(|p| context.position(p))
            .collect()
    }
}
