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

//! Bounded Arcus execution dispatch and REST reconciliation.
//!
//! A transport acknowledgement does not imply exchange acceptance. Every write is sent once;
//! uncertain outcomes remain pending until an authoritative order record resolves them.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{
        get_runtime,
        runner::{get_data_event_sender, get_exec_event_sender},
    },
    messages::{
        DataEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, ExecutionReport, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
        },
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
        TimeInForce,
    },
    events::{OrderEventAny, OrderPendingUpdate, OrderUpdated},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::{Mutex, RwLock};
use rust_decimal::Decimal;

use crate::{
    config::ArcusExecutionClientConfig,
    http::{ArcusHttpClient, ArcusHttpError},
    instruments::{parse_instrument, settlement_currency},
    models::{
        ArcusCancelRequest, ArcusFill, ArcusMarket, ArcusModifyRequest, ArcusOrder,
        ArcusOrderRequest, ArcusOrderSide, ArcusOrderType, ArcusPosition, ArcusTimeInForce,
    },
    websocket::{ArcusPrivateStream, ArcusWsEvent},
};

const MAX_DEDUPE_FILLS: usize = 100_000;
const FILL_OVERLAP_US: u64 = 60_000_000;

type FillIdentity = (InstrumentId, VenueOrderId, TradeId);

struct MessageRevision<'a>(&'a AtomicU64);

impl Drop for MessageRevision<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
struct DeliveredFills {
    identities: HashMap<FillIdentity, u64>,
    times: BTreeSet<(u64, FillIdentity)>,
    latest: u64,
}

impl DeliveredFills {
    fn key(fill: &FillReport) -> FillIdentity {
        (fill.instrument_id, fill.venue_order_id, fill.trade_id)
    }
    fn contains(&self, fill: &FillReport) -> bool {
        self.identities.contains_key(&Self::key(fill))
    }
    fn validate_capacity(&self, additional: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.identities.len().saturating_add(additional) <= MAX_DEDUPE_FILLS,
            "Arcus fill deduplication capacity reached"
        );
        Ok(())
    }
    fn prune(&mut self, timestamp: u64) {
        self.latest = self.latest.max(timestamp);
        let floor = self.latest.saturating_sub(FILL_OVERLAP_US * 1_000);

        while let Some((time, key)) = self.times.first().copied() {
            if time >= floor {
                break;
            }
            self.times.pop_first();
            self.identities.remove(&key);
        }
    }
    fn remember(&mut self, timestamp: u64, identity: FillIdentity) -> anyhow::Result<()> {
        self.prune(timestamp);
        anyhow::ensure!(
            self.identities.len() < MAX_DEDUPE_FILLS || self.identities.contains_key(&identity),
            "Arcus fill deduplication capacity reached"
        );
        self.identities.insert(identity, timestamp);
        self.times.insert((timestamp, identity));
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MarketContext {
    market: ArcusMarket,
    instrument: InstrumentAny,
}

#[derive(Clone, Debug)]
struct PendingModify {
    command: ModifyOrder,
    request: ArcusModifyRequest,
    prior: ArcusOrder,
    baseline_filled: Decimal,
    sent: bool,
    uncertain: bool,
    rejection_reported: bool,
    emit_pending: bool,
}

#[derive(Debug)]
struct Session {
    http: ArcusHttpClient,
    config: ArcusExecutionClientConfig,
    address: String,
    emitter: ExecutionEventEmitter,
    markets: RwLock<HashMap<InstrumentId, Arc<MarketContext>>>,
    ready: AtomicBool,
    stream_health: RwLock<Arc<AtomicBool>>,
    stream_epoch: RwLock<Arc<AtomicU64>>,
    ready_epoch: AtomicU64,
    pending: Mutex<HashSet<ClientOrderId>>,
    delivered: Mutex<DeliveredFills>,
    watermark_us: Mutex<u64>,
    order_cursor_us: AtomicU64,
    account_sequence: AtomicU64,
    reconcile_gate: tokio::sync::Mutex<()>,
    order_records: Mutex<HashMap<String, ArcusOrder>>,
    submitted: Mutex<HashMap<String, ArcusOrderRequest>>,
    order_types: Mutex<HashMap<String, OrderType>>,
    reconcile_notify: tokio::sync::Notify,
    state_revision: AtomicU64,
    commit: Mutex<()>,
    modifications: Mutex<HashMap<ClientOrderId, PendingModify>>,
    replacements: Mutex<HashMap<VenueOrderId, Decimal>>,
    modify_uncertain: AtomicBool,
}

impl Session {
    fn now() -> UnixNanos {
        get_atomic_clock_realtime().get_time_ns()
    }

    fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.modify_uncertain.load(Ordering::Acquire)
            && self.stream_health.read().load(Ordering::Acquire)
            && self.ready_epoch.load(Ordering::Acquire)
                == self.stream_epoch.read().load(Ordering::Acquire)
    }

    fn context(&self, id: InstrumentId) -> anyhow::Result<Arc<MarketContext>> {
        self.markets
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Arcus instrument {id} is not loaded"))
    }

    fn refresh_modify_uncertainty(&self) {
        let uncertain = self
            .modifications
            .lock()
            .values()
            .any(|pending| pending.uncertain);
        let replacing = !self.replacements.lock().is_empty();
        self.modify_uncertain
            .store(uncertain || replacing, Ordering::Release);
    }

    fn reject_modify(&self, pending: &PendingModify, reason: &str) {
        self.emitter.emit_order_modify_rejected_event(
            pending.command.strategy_id,
            pending.command.instrument_id,
            pending.command.client_order_id,
            Some(VenueOrderId::from(pending.request.order_id.as_str())),
            reason,
            Self::now(),
        );
    }

    fn mark_modifies_uncertain(&self) {
        for pending in self.modifications.lock().values_mut() {
            if pending.sent {
                pending.uncertain = true;
            }
        }
        self.refresh_modify_uncertainty();
    }

    fn reject_unsent_modifies(&self, reason: &str) {
        let mut modifications = self.modifications.lock();
        modifications.retain(|_, pending| {
            if pending.sent {
                return true;
            }
            self.reject_modify(pending, reason);
            false
        });
        drop(modifications);
        self.refresh_modify_uncertainty();
    }

    // Called with the short publication lock held; it never performs network I/O.
    fn resolve_modify(
        &self,
        record: &ArcusOrder,
        report: &mut OrderStatusReport,
    ) -> anyhow::Result<bool> {
        let venue_id = report.venue_order_id;
        let mut modifications = self.modifications.lock();
        let client_id = record
            .client_id
            .as_deref()
            .and_then(|id| ClientOrderId::new_checked(id).ok())
            .filter(|id| modifications.contains_key(id))
            .or_else(|| {
                modifications
                    .iter()
                    .find(|(_, pending)| pending.request.order_id == record.order_id)
                    .map(|(id, _)| *id)
            });
        let Some(client_id) = client_id else {
            if let Some(baseline) = self.replacements.lock().get(&venue_id).copied() {
                anyhow::ensure!(
                    report.filled_qty.as_decimal() >= baseline,
                    "Arcus external replacement decreased cumulative fills; refusing to infer a reset"
                );
            }

            if report.order_status == OrderStatus::Rejected
                && self.replacements.lock().contains_key(&venue_id)
            {
                report.order_status = OrderStatus::Canceled;
            }
            self.replacements.lock().remove(&venue_id);
            drop(modifications);
            self.refresh_modify_uncertainty();
            return Ok(true);
        };
        let pending = modifications
            .get_mut(&client_id)
            .context("Arcus pending modify disappeared")?;

        if !pending.sent {
            return Ok(true);
        }

        if record
            .sequence_number
            .zip(pending.prior.sequence_number)
            .is_none_or(|(new, old)| new <= old)
        {
            return Ok(false);
        }
        anyhow::ensure!(
            report.filled_qty.as_decimal() >= pending.baseline_filled,
            "Arcus replacement decreased cumulative fills; refusing to infer a fill reset"
        );
        anyhow::ensure!(
            record.original_size - record.remaining_size == report.filled_qty.as_decimal(),
            "Arcus modified order cumulative fills disagree with total minus remaining quantity"
        );
        anyhow::ensure!(
            record.side == pending.request.side
                && record.time_in_force == Some(pending.request.time_in_force)
                && record.reduce_only == pending.request.reduce_only
                && record
                    .good_til_time
                    .as_ref()
                    .is_none_or(|expiry| expiry == &pending.request.good_til_time),
            "Arcus modified order changed immutable attributes"
        );
        let replaced = self.replacements.lock().contains_key(&venue_id);
        let reason = record
            .rejection_reason
            .as_deref()
            .unwrap_or("Arcus modify was rejected");

        if report.order_status == OrderStatus::Rejected {
            if !pending.rejection_reported {
                self.reject_modify(pending, reason);
                pending.rejection_reported = true;
            }

            if !replaced && modify_rejection_preserves_order(reason) {
                let mut prior = pending.prior.clone();
                prior.sequence_number = record.sequence_number;
                prior.updated_at = record.updated_at;
                self.order_records
                    .lock()
                    .insert(record.order_id.clone(), prior);
                modifications.remove(&client_id);
                drop(modifications);
                self.refresh_modify_uncertainty();
                return Ok(false);
            }

            if replaced || modify_rejection_cancels_order(reason) {
                report.order_status = OrderStatus::Canceled;
            } else {
                pending.uncertain = true;
                let mut prior = pending.prior.clone();
                prior.sequence_number = record.sequence_number;
                prior.updated_at = record.updated_at;
                self.order_records
                    .lock()
                    .insert(record.order_id.clone(), prior);
                drop(modifications);
                self.refresh_modify_uncertainty();
                self.reconcile_notify.notify_one();
                return Ok(false);
            }
        } else if report.quantity.as_decimal() != pending.request.quantity
            || report
                .price
                .is_none_or(|price| price.as_decimal() != pending.request.price)
        {
            anyhow::ensure!(
                record.original_size == pending.prior.original_size
                    && record.price == pending.prior.price,
                "Arcus modify outcome matches neither original nor requested total quantity and price"
            );

            if !is_terminal(report.order_status) {
                return Ok(true);
            }

            if !pending.rejection_reported {
                self.reject_modify(
                    pending,
                    "Arcus target terminated before the requested modification was confirmed",
                );
            }
        }

        if report.quantity.as_decimal() == pending.request.quantity
            && report
                .price
                .is_some_and(|price| price.as_decimal() == pending.request.price)
            && record.state.as_deref().unwrap_or(&record.status) != "REJECTED"
            && !matches!(
                report.order_status,
                OrderStatus::Submitted | OrderStatus::PendingUpdate | OrderStatus::PendingCancel
            )
        {
            // Generic Accepted reconciliation clears PendingUpdate without applying amended fields.
            // Confirm the operation explicitly before publishing its authoritative state and fills.
            self.emitter
                .send_order_event(OrderEventAny::Updated(OrderUpdated::new(
                    pending.command.trader_id,
                    pending.command.strategy_id,
                    pending.command.instrument_id,
                    client_id,
                    report.quantity,
                    UUID4::new(),
                    report.ts_last,
                    Self::now(),
                    false,
                    Some(report.venue_order_id),
                    Some(self.config.account_id),
                    report.price,
                    None,
                    None,
                    false,
                )));
        }
        modifications.remove(&client_id);
        self.replacements.lock().remove(&venue_id);
        drop(modifications);
        self.refresh_modify_uncertainty();
        Ok(true)
    }

    fn context_for_market(&self, id: u16) -> anyhow::Result<Arc<MarketContext>> {
        self.markets
            .read()
            .values()
            .find(|c| c.market.market_id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown Arcus market ID {id}"))
    }

    fn validate_public_order_report(&self, report: &OrderStatusReport) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self
                .replacements
                .lock()
                .contains_key(&report.venue_order_id),
            "Arcus replacement outcome is pending; refusing a terminal public report"
        );

        if self.modifications.lock().values().any(|pending| {
            pending.sent && pending.request.order_id == report.venue_order_id.as_str()
        }) {
            anyhow::bail!(
                "Arcus pending modification requires operation reconciliation before public order reporting"
            );
        }
        Ok(())
    }

    async fn load_markets(&self) -> anyhow::Result<()> {
        let markets = self.http.markets().await?;
        let mut contexts = HashMap::new();

        for market in markets {
            let instrument = parse_instrument(&market, Self::now())?;
            // All market definitions are needed to preserve external account exposure.
            get_data_event_sender()
                .send(DataEvent::Instrument(instrument.clone()))
                .map_err(|_| anyhow::anyhow!("Arcus data event receiver is closed"))?;
            contexts.insert(
                instrument.id(),
                Arc::new(MarketContext { market, instrument }),
            );
        }

        for id in &self.config.load_ids {
            anyhow::ensure!(
                contexts.contains_key(id),
                "Configured Arcus instrument {id} is absent"
            );
        }
        *self.markets.write() = contexts;
        Ok(())
    }

    async fn account(&self) -> anyhow::Result<()> {
        let epoch = self.stream_epoch.read().load(Ordering::Acquire);
        let account = self
            .http
            .account(&self.address, self.config.subaccount_id)
            .await?;
        anyhow::ensure!(
            epoch == self.stream_epoch.read().load(Ordering::Acquire),
            "Arcus account response crossed a connection boundary"
        );
        anyhow::ensure!(
            account.address.eq_ignore_ascii_case(&self.address)
                && account.account_index == self.config.subaccount_id,
            "Arcus account identity mismatch"
        );
        anyhow::ensure!(
            account.loan_balance.is_none_or(|loan| loan.is_zero()),
            "Arcus lending and multi-asset collateral accounts are unsupported"
        );
        anyhow::ensure!(
            account.spot_positions.is_empty(),
            "Arcus spot collateral accounts are unsupported"
        );
        let currency = settlement_currency()?;
        anyhow::ensure!(
            account.free_collateral >= Decimal::ZERO && account.equity >= account.free_collateral,
            "Arcus account collateral is inconsistent"
        );
        let balance = AccountBalance::new(
            exact_money(account.equity, currency)?,
            exact_money(account.equity - account.free_collateral, currency)?,
            exact_money(account.free_collateral, currency)?,
        );
        let _commit = self.commit.lock();
        anyhow::ensure!(
            epoch == self.stream_epoch.read().load(Ordering::Acquire),
            "Arcus account publication crossed a connection boundary"
        );

        if account.sequence_number < self.account_sequence.load(Ordering::Acquire) {
            return Ok(());
        }
        self.emitter
            .try_emit_account_state(vec![balance], vec![], true, Self::now(), None)?;
        self.account_sequence
            .fetch_max(account.sequence_number, Ordering::AcqRel);
        Ok(())
    }

    async fn orders(
        &self,
        id: Option<InstrumentId>,
        start: Option<u64>,
        end: Option<u64>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let market = id.map(|id| self.context(id)).transpose()?;
        let records = self
            .http
            .orders(
                &self.address,
                self.config.subaccount_id,
                market
                    .as_ref()
                    .map(|c| c.market.market_display_name.as_str()),
                start,
                end,
            )
            .await?;
        self.cache_order_records(records)
    }

    fn cache_order_records(
        &self,
        mut records: Vec<ArcusOrder>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut cache = self.order_records.lock();

        for record in &mut records {
            if let Some(previous) = cache.get(&record.order_id)
                && previous
                    .sequence_number
                    .is_some_and(|old| record.sequence_number.is_none_or(|new| old >= new))
            {
                *record = previous.clone();
            }
        }
        let mut reports = Vec::with_capacity(records.len());

        for record in &records {
            if is_modify_cancel(record) {
                let baseline = cache
                    .get(&record.order_id)
                    .filter(|prior| !is_modify_cancel(prior))
                    .map_or(Decimal::ZERO, |prior| {
                        prior
                            .filled_size
                            .unwrap_or(prior.original_size - prior.remaining_size)
                    });
                self.replacements
                    .lock()
                    .entry(VenueOrderId::new_checked(&record.order_id)?)
                    .or_insert(baseline);
                self.modify_uncertain.store(true, Ordering::Release);
                continue;
            }
            reports.push(self.order_report(record)?);
        }

        for order in records {
            anyhow::ensure!(
                cache.len() < MAX_DEDUPE_FILLS || cache.contains_key(&order.order_id),
                "Arcus order identity capacity reached"
            );
            cache.insert(order.order_id.clone(), order);
        }
        Ok(reports)
    }

    async fn fills(
        &self,
        id: Option<InstrumentId>,
        start: Option<u64>,
        end: Option<u64>,
    ) -> anyhow::Result<Vec<FillReport>> {
        let market = id.map(|id| self.context(id)).transpose()?;
        self.http
            .fills(
                &self.address,
                self.config.subaccount_id,
                market
                    .as_ref()
                    .map(|c| c.market.market_display_name.as_str()),
                start,
                end,
            )
            .await?
            .iter()
            .map(|fill| self.fill_report(fill))
            .collect()
    }

    async fn positions(
        &self,
        id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let epoch = self.stream_epoch.read().load(Ordering::Acquire);
        let positions = self
            .http
            .positions(&self.address, self.config.subaccount_id)
            .await?;
        anyhow::ensure!(
            epoch == self.stream_epoch.read().load(Ordering::Acquire),
            "Arcus position response crossed a connection boundary"
        );
        let mut reports = Vec::new();
        let mut seen = HashSet::new();

        for position in positions {
            let context = self.context_for_market(position.market_id)?;
            let instrument_id = context.instrument.id();
            anyhow::ensure!(
                seen.insert(instrument_id),
                "Duplicate Arcus position for {instrument_id}"
            );

            if id.is_none_or(|id| id == instrument_id) {
                reports.push(self.position_report(&position, &context)?);
            }
        }
        // Only a successfully decoded complete HTTP snapshot proves omitted markets flat.
        for (instrument_id, context) in self.markets.read().iter() {
            if !seen.contains(instrument_id) && id.is_none_or(|id| id == *instrument_id) {
                reports.push(PositionStatusReport::new(
                    self.config.account_id,
                    *instrument_id,
                    PositionSide::Flat,
                    Quantity::from_decimal_dp(Decimal::ZERO, context.instrument.size_precision())?,
                    Self::now(),
                    Self::now(),
                    None,
                    None,
                    None,
                ));
            }
        }
        Ok(reports)
    }

    async fn reconcile(&self) -> anyhow::Result<()> {
        let _gate = self.reconcile_gate.lock().await;
        let epoch = self.stream_epoch.read().load(Ordering::Acquire);
        let revision = self.state_revision.load(Ordering::Acquire);
        let cutoff = Self::now().as_u64() / 1_000;
        let cursor = self.order_cursor_us.load(Ordering::Acquire);
        let order_start = (cursor != 0).then(|| cursor.saturating_sub(FILL_OVERLAP_US));
        let mut old_active: Vec<_> = self
            .order_records
            .lock()
            .values()
            .filter(|record| {
                order_status(record.state.as_deref().unwrap_or(&record.status))
                    .is_ok_and(|status| !is_terminal(status))
            })
            .map(|record| record.order_id.clone())
            .collect();

        for id in self.replacements.lock().keys() {
            if !old_active.iter().any(|old| old == id.as_str()) {
                old_active.push(id.to_string());
            }
        }
        self.account().await?;
        let history = self.orders(None, order_start, Some(cutoff)).await?;
        let open_records = self
            .http
            .open_orders(&self.address, self.config.subaccount_id)
            .await?;
        let open_ids: HashSet<_> = open_records
            .iter()
            .map(|record| record.order_id.clone())
            .collect();
        let mut reports_by_id: HashMap<_, _> = history
            .into_iter()
            .map(|report| (report.venue_order_id, report))
            .collect();

        for report in self.cache_order_records(open_records)? {
            reports_by_id.insert(report.venue_order_id, report);
        }
        // An older resting order can terminate while its update is absent from the history
        // window. Absence from openOrders never implies cancellation: query that ID directly.
        for order_id in old_active {
            let id = VenueOrderId::new_checked(&order_id)?;

            if !open_ids.contains(&order_id)
                && !reports_by_id
                    .get(&id)
                    .is_some_and(|report| is_terminal(report.order_status))
            {
                let record = self
                    .http
                    .order_status(&self.address, self.config.subaccount_id, &order_id)
                    .await?;
                let report = self
                    .cache_order_records(vec![record])?
                    .pop()
                    .context("Arcus order lookup returned no report")?;
                reports_by_id.insert(id, report);
            }
        }
        let mut orders: Vec<_> = reports_by_id.into_values().collect();
        let watermark = *self.watermark_us.lock();
        let start = (watermark != 0).then(|| watermark.saturating_sub(FILL_OVERLAP_US));
        let fills = self.fills(None, start, None).await?;
        let positions = self.positions(None).await?;
        anyhow::ensure!(
            epoch == self.stream_epoch.read().load(Ordering::Acquire),
            "Arcus stream changed during reconciliation"
        );
        let mut by_order: HashMap<VenueOrderId, Vec<FillReport>> = HashMap::new();

        for fill in fills {
            if !self.delivered.lock().contains(&fill) {
                by_order.entry(fill.venue_order_id).or_default().push(fill);
            }
        }
        // History windows can omit an old order with a recent fill; fetch it explicitly.
        for order_id in by_order.keys() {
            if !orders.iter().any(|order| order.venue_order_id == *order_id) {
                let record = self
                    .http
                    .order_status(&self.address, self.config.subaccount_id, order_id.as_str())
                    .await?;
                orders.extend(self.cache_order_records(vec![record])?);
            }
        }
        anyhow::ensure!(
            epoch == self.stream_epoch.read().load(Ordering::Acquire),
            "Arcus stream changed while linking fills"
        );
        let _commit = self.commit.lock();

        if revision & 1 != 0 || revision != self.state_revision.load(Ordering::Acquire) {
            self.reconcile_notify.notify_one();
            return Ok(());
        }
        let mut resolved = Vec::new();

        for mut order in orders {
            let record = self
                .order_records
                .lock()
                .get(order.venue_order_id.as_str())
                .cloned()
                .context("Arcus reconciled order record is missing")?;

            if !self.resolve_modify(&record, &mut order)? {
                continue;
            }
            let mut fills = by_order.remove(&order.venue_order_id).unwrap_or_default();
            fills.sort_by_key(|fill| fill.ts_event);

            if let Some(first) = fills.first() {
                order.ts_accepted = order.ts_accepted.min(first.ts_event);
            }
            let mut delivered = self.delivered.lock();
            fills.retain(|fill| !delivered.contains(fill));
            let keys: Vec<_> = fills
                .iter()
                .map(|f| (f.ts_event.as_u64(), DeliveredFills::key(f)))
                .collect();

            if let Some(timestamp) = keys.iter().map(|key| key.0).max() {
                delivered.prune(timestamp);
            }
            delivered.validate_capacity(keys.len())?;

            if order.order_status != OrderStatus::Submitted
                && let Some(id) = order.client_order_id
            {
                resolved.push(id);
            }
            self.emitter
                .try_send_execution_report(ExecutionReport::OrderWithFills(
                    Box::new(order),
                    fills,
                ))?;

            for key in keys {
                let mut watermark = self.watermark_us.lock();
                *watermark = (*watermark).max(key.0 / 1_000);
                delivered.remember(key.0, key.1)?;
            }
        }

        // A pending operation can suppress its order snapshot, never its real executions.
        for fills in by_order.into_values() {
            for fill in fills {
                let mut delivered = self.delivered.lock();

                if delivered.contains(&fill) {
                    continue;
                }
                let key = (fill.ts_event.as_u64(), DeliveredFills::key(&fill));
                delivered.prune(key.0);
                delivered.validate_capacity(1)?;
                self.emitter
                    .try_send_execution_report(ExecutionReport::Fill(Box::new(fill)))?;
                delivered.remember(key.0, key.1)?;
                let mut watermark = self.watermark_us.lock();
                *watermark = (*watermark).max(key.0 / 1_000);
            }
        }

        for position in positions {
            self.emitter
                .try_send_execution_report(ExecutionReport::Position(Box::new(position)))?;
        }
        // Commit the REST cursor only after the complete batch was accepted for delivery.
        // WebSocket timestamps and interrupted/rejected snapshots cannot advance this cursor.
        self.order_cursor_us.store(cutoff, Ordering::Release);
        let mut pending = self.pending.lock();

        for id in resolved {
            pending.remove(&id);
        }
        self.ready_epoch.store(epoch, Ordering::Release);
        self.ready.store(pending.is_empty(), Ordering::Release);
        Ok(())
    }

    async fn message(&self, frame: serde_json::Value) -> anyhow::Result<()> {
        let channel = frame["channel"]
            .as_str()
            .context("Arcus private frame has no channel")?;
        anyhow::ensure!(
            frame["id"]
                .as_str()
                .is_some_and(|address| address.eq_ignore_ascii_case(&self.address))
                && frame["accountIndex"].as_u64() == Some(u64::from(self.config.subaccount_id)),
            "Arcus private frame account identity mismatch"
        );

        if frame["type"] != "channel_data" {
            return Ok(());
        }
        let _revision = if matches!(channel, "orders" | "userFills") {
            self.state_revision.fetch_add(1, Ordering::AcqRel);
            Some(MessageRevision(&self.state_revision))
        } else {
            None
        };
        let contents = &frame["contents"];

        match channel {
            "userFills" => {
                let values: Vec<&serde_json::Value> =
                    if let Some(fills) = contents["fills"].as_array() {
                        fills.iter().collect()
                    } else if contents.get("tradeId").is_some() {
                        vec![contents]
                    } else {
                        anyhow::bail!("Arcus fill frame has no fills");
                    };

                for value in values {
                    let fill: ArcusFill = serde_json::from_value(value.clone())?;
                    let report = self.fill_report(&fill)?;
                    let key = (report.ts_event.as_u64(), DeliveredFills::key(&report));
                    let _commit = self.commit.lock();
                    let mut delivered = self.delivered.lock();

                    if delivered.contains(&report) {
                        continue;
                    }
                    delivered.prune(key.0);
                    delivered.validate_capacity(1)?;

                    for pending in
                        self.modifications.lock().values_mut().filter(|pending| {
                            pending.sent && pending.request.order_id == fill.order_id
                        })
                    {
                        if fill
                            .sequence_number
                            .zip(pending.prior.sequence_number)
                            .is_some_and(|(new, old)| new > old)
                        {
                            pending.baseline_filled += fill.size;
                        }
                    }
                    self.emitter
                        .try_send_execution_report(ExecutionReport::Fill(Box::new(report)))?;
                    delivered.remember(key.0, key.1)?;
                    let mut watermark = self.watermark_us.lock();
                    *watermark = (*watermark).max(key.0 / 1_000);
                }
            }
            "orders" => {
                let order_id = contents["orderId"]
                    .as_str()
                    .context("Arcus order frame has no order ID")?;
                let old = self.order_records.lock().get(order_id).cloned();

                if old
                    .as_ref()
                    .and_then(|order| order.sequence_number)
                    .zip(contents["sequenceNumber"].as_u64())
                    .is_some_and(|(old, new)| old >= new)
                {
                    return Ok(());
                }
                let mut value = match old {
                    Some(old) => serde_json::to_value(old)?,
                    None => serde_json::json!({}),
                };
                let object = value
                    .as_object_mut()
                    .context("Arcus order is not an object")?;

                if let Some(request) = contents["clientId"]
                    .as_str()
                    .and_then(|id| self.submitted.lock().get(id).cloned())
                {
                    object
                        .entry("type")
                        .or_insert(serde_json::to_value(request.order_type)?);
                    object
                        .entry("timeInForce")
                        .or_insert(serde_json::to_value(request.time_in_force)?);
                    object
                        .entry("price")
                        .or_insert(serde_json::to_value(request.price)?);
                    object
                        .entry("originalSize")
                        .or_insert(serde_json::to_value(request.quantity)?);
                    object
                        .entry("remainingSize")
                        .or_insert(serde_json::to_value(request.quantity)?);
                    object
                        .entry("reduceOnly")
                        .or_insert(request.reduce_only.into());
                    object
                        .entry("createdAt")
                        .or_insert((request.timestamp / 1_000).into());
                }

                for (key, value) in contents
                    .as_object()
                    .context("Arcus order contents is not an object")?
                {
                    object.insert(key.clone(), value.clone());
                }
                object.insert("accountIndex".into(), self.config.subaccount_id.into());

                if contents.get("updatedAt").is_none() {
                    object.insert("updatedAt".into(), (Self::now().as_u64() / 1_000).into());
                }

                if contents.get("state").is_none() {
                    object.insert("state".into(), serde_json::Value::Null);
                }

                if contents.get("filledSize").is_none() {
                    object.insert("filledSize".into(), serde_json::Value::Null);
                }

                if contents.get("cancelReason").is_none() {
                    object.insert("cancelReason".into(), serde_json::Value::Null);
                }

                if contents.get("rejectionReason").is_none() {
                    object.insert("rejectionReason".into(), serde_json::Value::Null);
                }
                let record = match serde_json::from_value::<ArcusOrder>(value) {
                    Ok(record) => record,
                    Err(_) => {
                        self.http
                            .order_status(&self.address, self.config.subaccount_id, order_id)
                            .await?
                    }
                };
                let _commit = self.commit.lock();
                let mut records = self.order_records.lock();

                if records
                    .get(order_id)
                    .and_then(|record| record.sequence_number)
                    .zip(record.sequence_number)
                    .is_some_and(|(old, new)| old >= new)
                {
                    return Ok(());
                }

                if is_modify_cancel(&record) {
                    let baseline = records
                        .get(order_id)
                        .filter(|prior| !is_modify_cancel(prior))
                        .map_or(Decimal::ZERO, |prior| {
                            prior
                                .filled_size
                                .unwrap_or(prior.original_size - prior.remaining_size)
                        });
                    self.replacements
                        .lock()
                        .entry(VenueOrderId::new_checked(order_id)?)
                        .or_insert(baseline);
                    self.modify_uncertain.store(true, Ordering::Release);
                    records.insert(order_id.to_owned(), record);
                    self.reconcile_notify.notify_one();
                    return Ok(());
                }
                drop(records);
                let mut report = self.order_report(&record)?;

                if !self.resolve_modify(&record, &mut report)? {
                    return Ok(());
                }

                if report.order_status != OrderStatus::Submitted
                    && let Some(id) = report.client_order_id
                {
                    self.pending.lock().remove(&id);
                }
                self.emitter
                    .try_send_execution_report(ExecutionReport::Order(Box::new(report)))?;

                let mut records = self.order_records.lock();

                if records.len() >= MAX_DEDUPE_FILLS && !records.contains_key(order_id) {
                    anyhow::bail!("Arcus order identity capacity reached");
                }
                records.insert(order_id.to_owned(), record);

                if !self.ready.load(Ordering::Acquire) {
                    self.reconcile_notify.notify_one();
                }
            }
            "positions" | "account" => {
                // Coalesce the distinct private delta schemas into an independent snapshot.
                // Fill consumption never waits on an account or position HTTP request.
                self.reconcile_notify.notify_one();
            }
            other => anyhow::bail!("Unexpected Arcus private channel {other}"),
        }
        Ok(())
    }

    fn order_report(&self, order: &ArcusOrder) -> anyhow::Result<OrderStatusReport> {
        anyhow::ensure!(
            !is_modify_cancel(order),
            "Arcus cancel-replace is pending; MODIFY_CANCELED is not terminal"
        );
        anyhow::ensure!(
            order.account_index == self.config.subaccount_id,
            "Arcus order subaccount mismatch"
        );
        let context = self.context_for_market(order.market_id)?;
        anyhow::ensure!(
            order.market_display_name == context.market.market_display_name,
            "Arcus order market identity mismatch"
        );
        let mut status = order_status(order.state.as_deref().unwrap_or(&order.status))?;
        let quantity = order.original_size;
        let filled = order.filled_size.unwrap_or(quantity - order.remaining_size);
        anyhow::ensure!(
            quantity > Decimal::ZERO && filled >= Decimal::ZERO && filled <= quantity,
            "Invalid Arcus cumulative fill quantity"
        );

        if status == OrderStatus::Accepted && filled > Decimal::ZERO {
            status = OrderStatus::PartiallyFilled;
        }

        if status == OrderStatus::PartiallyFilled
            && order.time_in_force == Some(ArcusTimeInForce::Ioc)
        {
            status = OrderStatus::Canceled;
        }
        let mut report = OrderStatusReport::new(
            self.config.account_id,
            context.instrument.id(),
            order
                .client_id
                .as_deref()
                .map(ClientOrderId::new_checked)
                .transpose()?,
            VenueOrderId::new_checked(&order.order_id)?,
            Some(side(order.side)),
            order
                .client_id
                .as_deref()
                .and_then(|id| self.order_types.lock().get(id).copied())
                .unwrap_or(match order.order_type {
                    Some(ArcusOrderType::Market) => OrderType::Market,
                    _ => OrderType::Limit,
                }),
            match order.time_in_force {
                Some(ArcusTimeInForce::Ioc) => TimeInForce::Ioc,
                Some(ArcusTimeInForce::Fok) => TimeInForce::Fok,
                _ => TimeInForce::Gtc,
            },
            status,
            Quantity::from_decimal_dp(quantity, context.instrument.size_precision())?,
            Quantity::from_decimal_dp(filled, context.instrument.size_precision())?,
            micros(order.created_at.unwrap_or(order.updated_at))?,
            micros(order.updated_at)?,
            Self::now(),
            None,
        );
        report.price = Some(Price::from_decimal_dp(
            order.price,
            context.instrument.price_precision(),
        )?);
        report.avg_px = order.avg_fill_price;
        report.reduce_only = order.reduce_only;
        report.post_only = order.time_in_force == Some(ArcusTimeInForce::Alo);
        report.cancel_reason = order
            .cancel_reason
            .clone()
            .or(order.rejection_reason.clone());
        Ok(report)
    }

    fn fill_report(&self, fill: &ArcusFill) -> anyhow::Result<FillReport> {
        let context = self.context_for_market(fill.market_id)?;
        anyhow::ensure!(
            fill.market_display_name == context.market.market_display_name,
            "Arcus fill market identity mismatch"
        );
        anyhow::ensure!(
            fill.size > Decimal::ZERO && fill.price > Decimal::ZERO,
            "Invalid Arcus fill size or price"
        );
        let liquidity = match fill.role.as_str() {
            "MAKER" => LiquiditySide::Maker,
            "TAKER" => LiquiditySide::Taker,
            role => anyhow::bail!("Unknown Arcus fill role {role}"),
        };
        Ok(FillReport::new(
            self.config.account_id,
            context.instrument.id(),
            VenueOrderId::new_checked(&fill.order_id)?,
            TradeId::new_checked(&fill.trade_id)?,
            side(fill.side),
            Quantity::from_decimal_dp(fill.size, context.instrument.size_precision())?,
            Price::from_decimal_dp(fill.price, context.instrument.price_precision())?,
            exact_money(fill.fee, settlement_currency()?)?,
            liquidity,
            fill.client_id
                .as_deref()
                .map(ClientOrderId::new_checked)
                .transpose()?,
            None,
            micros(fill.created_at)?,
            Self::now(),
            None,
        ))
    }

    fn position_report(
        &self,
        position: &ArcusPosition,
        context: &MarketContext,
    ) -> anyhow::Result<PositionStatusReport> {
        anyhow::ensure!(
            position.market_display_name == context.market.market_display_name,
            "Arcus position market identity mismatch"
        );
        let position_side = if position.size.is_zero() {
            PositionSide::Flat
        } else {
            match position.side.as_str() {
                "LONG" => PositionSide::Long,
                "SHORT" => PositionSide::Short,
                other => anyhow::bail!("Unknown Arcus position side {other}"),
            }
        };
        anyhow::ensure!(
            (position_side == PositionSide::Long && position.size > Decimal::ZERO)
                || (position_side == PositionSide::Short && position.size < Decimal::ZERO)
                || (position_side == PositionSide::Flat && position.size.is_zero()),
            "Arcus signed position side mismatch"
        );
        Ok(PositionStatusReport::new(
            self.config.account_id,
            context.instrument.id(),
            position_side,
            Quantity::from_decimal_dp(position.size.abs(), context.instrument.size_precision())?,
            Self::now(),
            Self::now(),
            None,
            None,
            (!position.size.is_zero()).then_some(position.average_entry_price),
        ))
    }
}

#[derive(Debug)]
enum Command {
    Submit(Box<OrderAny>, ArcusOrderRequest, Arc<MarketContext>),
    Cancel(CancelOrder, ArcusCancelRequest),
    CancelAll(CancelAllOrders),
    Reconcile,
    Modify(ClientOrderId, UUID4, Arc<MarketContext>, bool),
}

/// Arcus perpetuals execution client with bounded commands and fail-closed readiness.
#[derive(Debug)]
pub struct ArcusExecutionClient {
    core: ExecutionClientCore,
    session: Arc<Session>,
    commands: Option<tokio::sync::mpsc::Sender<Command>>,
    cancels: Option<tokio::sync::mpsc::Sender<Command>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    stream: Option<ArcusPrivateStream>,
}

impl Drop for ArcusExecutionClient {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl ArcusExecutionClient {
    /// Creates an Arcus execution client after validating configuration and credentials.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, credentials, or HTTP client construction.
    pub fn new(
        core: ExecutionClientCore,
        config: ArcusExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let address = config.resolved_address()?;
        let signer = config.resolved_signer()?;
        let http = ArcusHttpClient::new(
            config.resolved_http_url(),
            config.http_timeout_secs,
            Some(signer),
        )?;
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None,
        );
        Ok(Self {
            core,
            session: Arc::new(Session {
                http,
                config,
                address,
                emitter,
                markets: RwLock::new(HashMap::new()),
                ready: AtomicBool::new(false),
                stream_health: RwLock::new(Arc::new(AtomicBool::new(false))),
                pending: Mutex::new(HashSet::new()),
                stream_epoch: RwLock::new(Arc::new(AtomicU64::new(0))),
                ready_epoch: AtomicU64::new(0),
                delivered: Mutex::new(DeliveredFills::default()),
                watermark_us: Mutex::new(0),
                order_cursor_us: AtomicU64::new(0),
                account_sequence: AtomicU64::new(0),
                reconcile_gate: tokio::sync::Mutex::new(()),
                order_records: Mutex::new(HashMap::new()),
                submitted: Mutex::new(HashMap::new()),
                order_types: Mutex::new(HashMap::new()),
                reconcile_notify: tokio::sync::Notify::new(),
                state_revision: AtomicU64::new(0),
                commit: Mutex::new(()),
                modifications: Mutex::new(HashMap::new()),
                replacements: Mutex::new(HashMap::new()),
                modify_uncertain: AtomicBool::new(false),
            }),
            commands: None,
            cancels: None,
            tasks: Vec::new(),
            stream: None,
        })
    }

    fn enqueue(&self, command: Command) -> anyhow::Result<()> {
        let queue = if matches!(
            command,
            Command::Cancel(..) | Command::CancelAll(_) | Command::Modify(_, _, _, true)
        ) {
            &self.cancels
        } else {
            &self.commands
        };
        queue
            .as_ref()
            .context("Arcus execution client is disconnected")?
            .try_send(command)
            .map_err(|_| anyhow::anyhow!("Arcus execution command queue is full or closed"))
    }

    fn request(&self, order: &OrderAny) -> anyhow::Result<(ArcusOrderRequest, Arc<MarketContext>)> {
        anyhow::ensure!(
            self.session.ready(),
            "Arcus execution is not reconciled and healthy"
        );
        anyhow::ensure!(!order.is_closed(), "Arcus cannot submit a closed order");
        let context = self.session.context(order.instrument_id())?;
        anyhow::ensure!(
            self.session.config.load_ids.is_empty()
                || self
                    .session
                    .config
                    .load_ids
                    .contains(&order.instrument_id()),
            "Arcus instrument is outside execution scope"
        );
        anyhow::ensure!(
            !order.is_quote_quantity(),
            "Arcus quote-quantity orders are unsupported"
        );
        anyhow::ensure!(
            order.contingency_type().is_none(),
            "Arcus contingent orders are unsupported"
        );
        let quantity = order.quantity().as_decimal();
        let (price, tif) = match order.order_type() {
            OrderType::Limit => {
                let tif = match (order.time_in_force(), order.is_post_only()) {
                    (TimeInForce::Gtc, true) => ArcusTimeInForce::Alo,
                    (TimeInForce::Gtc, false) => ArcusTimeInForce::Gtt,
                    (TimeInForce::Ioc, false) => ArcusTimeInForce::Ioc,
                    (TimeInForce::Fok, false) => ArcusTimeInForce::Fok,
                    _ => anyhow::bail!("Unsupported Arcus time in force or post-only combination"),
                };
                (
                    order
                        .price()
                        .context("Arcus limit order requires a price")?
                        .as_decimal(),
                    tif,
                )
            }
            OrderType::Market => {
                anyhow::ensure!(
                    !order.is_post_only(),
                    "Arcus market orders cannot be post-only"
                );
                anyhow::ensure!(
                    matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Ioc),
                    "Arcus bounded market orders require GTC or IOC"
                );
                let cache = self.core.cache();
                let status = cache
                    .instrument_status(&order.instrument_id())
                    .context("Arcus bounded market order requires a current feed status")?;
                anyhow::ensure!(
                    status.is_quoting == Some(true) && status.is_trading != Some(false),
                    "Arcus bounded market order feed is not quoting or trading"
                );
                let quote = cache
                    .quote(&order.instrument_id())
                    .copied()
                    .context("Arcus bounded market order requires a cached quote")?;
                anyhow::ensure!(
                    quote.ts_init >= status.ts_init,
                    "Arcus cached quote predates feed readiness"
                );
                anyhow::ensure!(
                    Session::now()
                        .as_u64()
                        .saturating_sub(quote.ts_init.as_u64())
                        <= 5_000_000_000,
                    "Arcus bounded market order quote is stale"
                );
                let reference = match order.order_side() {
                    OrderSide::Buy => quote.ask_price.as_decimal(),
                    OrderSide::Sell => quote.bid_price.as_decimal(),
                };
                let slip =
                    Decimal::from(self.session.config.market_slippage_bps) / Decimal::from(10_000);
                let raw = if order.order_side() == OrderSide::Buy {
                    reference * (Decimal::ONE + slip)
                } else {
                    reference * (Decimal::ONE - slip)
                };
                let tick = context.market.tick_size;
                let rounded = if order.order_side() == OrderSide::Buy {
                    (raw / tick).floor() * tick
                } else {
                    (raw / tick).ceil() * tick
                };
                (rounded, ArcusTimeInForce::Ioc)
            }
            _ => anyhow::bail!("Arcus conditional and trailing orders are unsupported"),
        };
        anyhow::ensure!(
            context.market.status == "ONLINE",
            "Arcus market is not online"
        );
        anyhow::ensure!(
            quantity >= context.market.min_order_size
                && quantity <= context.market.max_order_size
                && quantity % context.market.step_size == Decimal::ZERO,
            "Arcus order quantity violates market limits"
        );
        anyhow::ensure!(
            price > Decimal::ZERO
                && price % context.market.tick_size == Decimal::ZERO
                && (order.is_reduce_only()
                    || price * quantity >= context.market.min_order_notional),
            "Arcus order price or notional violates market limits"
        );

        if !context.market.tick_tiers.is_empty() {
            let tier = context
                .market
                .tick_tiers
                .iter()
                .find(|tier| tier.up_to_price.is_none_or(|upper| price < upper))
                .context("Arcus order price has no valid tick tier")?;
            anyhow::ensure!(
                tier.tick > Decimal::ZERO && price % tier.tick == Decimal::ZERO,
                "Arcus order price violates its tick tier"
            );
        }
        let now = Session::now().as_u64();
        let request = ArcusOrderRequest {
            address: self.session.address.clone(),
            account_index: self.session.config.subaccount_id,
            market_id: context.market.market_id,
            order_side: match order.order_side() {
                OrderSide::Buy => ArcusOrderSide::Buy,
                OrderSide::Sell => ArcusOrderSide::Sell,
            },
            // A bounded IOC limit implements market semantics without unbounded price exposure.
            order_type: ArcusOrderType::Limit,
            time_in_force: tif,
            quantity,
            price,
            good_til_time: (now / 1_000 + 33 * 86_400 * 1_000_000).to_string(),
            reduce_only: order.is_reduce_only(),
            client_id: Some(order.client_order_id().to_string()),
            timestamp: now,
        };
        crate::signing::order_signing_payload(&request, &context.market, now)?;
        Ok((request, context))
    }

    fn modification(
        &self,
        cmd: &ModifyOrder,
    ) -> anyhow::Result<(PendingModify, Arc<MarketContext>)> {
        anyhow::ensure!(
            self.session.ready(),
            "Arcus modification requires a healthy reconciled session"
        );
        anyhow::ensure!(
            cmd.trigger_price.is_none() && (cmd.price.is_some() || cmd.quantity.is_some()),
            "Arcus modify requires price or total quantity and does not support trigger changes"
        );
        let order = self.core.get_order(&cmd.client_order_id)?;
        anyhow::ensure!(
            order.instrument_id() == cmd.instrument_id && order.strategy_id() == cmd.strategy_id,
            "Arcus modify command does not match the cached order identity"
        );
        anyhow::ensure!(
            order.order_type() == OrderType::Limit
                && !order.is_closed()
                && !order.is_pending_cancel(),
            "Arcus can modify only an open resting limit order"
        );
        let venue_id = cmd
            .venue_order_id
            .or(order.venue_order_id())
            .context("Arcus modify requires a confirmed server order ID")?;
        anyhow::ensure!(
            order.venue_order_id().is_none_or(|id| id == venue_id),
            "Arcus modify server order ID mismatch"
        );
        let prior = self
            .session
            .order_records
            .lock()
            .get(venue_id.as_str())
            .cloned()
            .context("Arcus modify requires a confirmed venue order record")?;
        anyhow::ensure!(
            prior.account_index == self.session.config.subaccount_id
                && prior
                    .client_id
                    .as_deref()
                    .is_none_or(|id| id == cmd.client_order_id.as_str()),
            "Arcus modify venue record identity mismatch"
        );
        anyhow::ensure!(
            matches!(
                order_status(prior.state.as_deref().unwrap_or(&prior.status))?,
                OrderStatus::Accepted | OrderStatus::PartiallyFilled
            ) && prior.cancel_reason.as_deref() != Some("MODIFY_CANCELED"),
            "Arcus modify target is not resting"
        );
        anyhow::ensure!(
            prior.sequence_number.is_some(),
            "Arcus modify requires an authoritative order sequence"
        );
        let context = self.session.context(cmd.instrument_id)?;
        anyhow::ensure!(
            prior.market_id == context.market.market_id
                && prior.market_display_name == context.market.market_display_name
                && context.market.status == "ONLINE",
            "Arcus modify market identity or trading status mismatch"
        );
        let expected_tif = if order.is_post_only() {
            ArcusTimeInForce::Alo
        } else {
            ArcusTimeInForce::Gtt
        };
        anyhow::ensure!(
            order.time_in_force() == TimeInForce::Gtc
                && prior.time_in_force == Some(expected_tif)
                && prior
                    .order_type
                    .is_none_or(|kind| kind == ArcusOrderType::Limit)
                && prior.reduce_only == order.is_reduce_only()
                && side(prior.side) == order.order_side(),
            "Arcus modify cannot change side, type, time in force, or reduce-only"
        );
        let baseline_filled = prior
            .filled_size
            .unwrap_or(prior.original_size - prior.remaining_size)
            .max(order.filled_qty().as_decimal());
        anyhow::ensure!(
            baseline_filled >= Decimal::ZERO && baseline_filled <= prior.original_size,
            "Arcus modify cumulative fills are inconsistent"
        );
        let quantity = cmd
            .quantity
            .map_or(prior.original_size, |quantity| quantity.as_decimal());
        let price = cmd.price.map_or(prior.price, |price| price.as_decimal());
        anyhow::ensure!(
            quantity > baseline_filled,
            "Arcus modify total quantity must exceed already filled quantity; use cancel to stop the remainder"
        );
        anyhow::ensure!(
            quantity != prior.original_size || price != prior.price,
            "Arcus modify does not change price or total quantity"
        );
        let good_til_time = prior
            .good_til_time
            .clone()
            .or_else(|| {
                self.session
                    .submitted
                    .lock()
                    .get(cmd.client_order_id.as_str())
                    .map(|request| request.good_til_time.clone())
            })
            .context("Arcus modify requires the resting order's exact goodTilTime")?;
        let request = ArcusModifyRequest {
            address: self.session.address.clone(),
            account_index: self.session.config.subaccount_id,
            market_id: prior.market_id,
            order_id: venue_id.to_string(),
            quantity,
            price,
            side: prior.side,
            time_in_force: expected_tif,
            reduce_only: prior.reduce_only,
            good_til_time,
            client_time: Session::now().as_u64().to_string(),
        };
        crate::signing::modify_signing_payload(
            &request,
            &context.market,
            request.client_time.parse()?,
        )
        .context("Arcus modify cannot preserve an invalid or insufficiently distant expiry")?;
        Ok((
            PendingModify {
                command: cmd.clone(),
                request,
                prior,
                baseline_filled,
                sent: false,
                uncertain: false,
                rejection_reported: false,
                emit_pending: !order.is_pending_update(),
            },
            context,
        ))
    }
}

#[async_trait(?Send)]
impl ExecutionClient for ArcusExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected() && self.session.ready()
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
    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.session
            .emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }
    fn start(&mut self) -> anyhow::Result<()> {
        if !self.core.is_started() {
            let mut emitter = self.session.emitter.clone();
            emitter.set_sender(get_exec_event_sender());
            self.core.set_started();
        }
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        let _commit = self.session.commit.lock();
        self.session.ready.store(false, Ordering::Release);
        self.session.mark_modifies_uncertain();

        if let Some(stream) = self.stream.take() {
            stream.abort();
        }

        for task in self.tasks.drain(..) {
            task.abort();
        }
        self.commands.take();
        self.cancels.take();
        self.session
            .reject_unsent_modifies("Arcus execution stopped before modification dispatch");
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
        if self.core.is_connected()
            && self
                .commands
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
        {
            return Ok(());
        }

        if self.commands.is_some() {
            self.disconnect().await?;
        }

        if !self.core.is_started() {
            self.start()?;
        }
        self.session.load_markets().await?;
        let (stream, mut events) = ArcusPrivateStream::connect(
            self.session.config.resolved_ws_url(),
            &self.session.address,
            self.session.config.subaccount_id,
            self.session.config.queue_capacity,
            Duration::from_secs(self.session.config.ws_idle_timeout_secs),
        )
        .await?;
        *self.session.stream_health.write() = stream.health();
        *self.session.stream_epoch.write() = stream.epoch();
        let (tx, mut rx) = tokio::sync::mpsc::channel(self.session.config.queue_capacity);
        let (cancel_tx, mut cancel_rx) =
            tokio::sync::mpsc::channel(self.session.config.queue_capacity);
        let session = self.session.clone();

        self.tasks.push(get_runtime().spawn(async move {
            while let Some(event) = events.recv().await {
                let result = match event {
                    ArcusWsEvent::Disconnected
                    | ArcusWsEvent::Error(_)
                    | ArcusWsEvent::Connected => {
                        session.ready.store(false, Ordering::Release);
                        session.mark_modifies_uncertain();
                        Ok(())
                    }
                    ArcusWsEvent::Ready => {
                        session.reconcile_notify.notify_one();
                        Ok(())
                    }
                    ArcusWsEvent::Message { epoch, value } => {
                        if epoch == session.stream_epoch.read().load(Ordering::Acquire) {
                            session.message(value).await
                        } else {
                            session.ready.store(false, Ordering::Release);
                            Ok(())
                        }
                    }
                };

                if let Err(e) = result {
                    session.ready.store(false, Ordering::Release);
                    log::warn!("Arcus private execution update failed: {e}");
                }
            }
            session.ready.store(false, Ordering::Release);
        }));
        let session = self.session.clone();

        self.tasks.push(get_runtime().spawn(async move {
            loop {
                let result = tokio::select! {
                    biased;
                    command = cancel_rx.recv() => match command {
                        Some(command) => dispatch(&session, command).await,
                        None => break,
                    },
                    command = rx.recv() => match command {
                        Some(command) => dispatch(&session, command).await,
                        None => break,
                    },
                };

                if let Err(e) = result {
                    session.ready.store(false, Ordering::Release);
                    log::warn!("Arcus execution reconciliation or dispatch failed: {e}");
                }
            }
            session.ready.store(false, Ordering::Release);
        }));
        let session = self.session.clone();

        self.tasks.push(get_runtime().spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(session.config.reconciliation_interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_reconciliation: Option<tokio::time::Instant> = None;

            loop {
                tokio::select! { () = session.reconcile_notify.notified() => {}, _ = interval.tick() => {} }

                if !session.stream_health.read().load(Ordering::Acquire) { continue; }
                // A one-page snapshot costs 214 read-weight units; ten seconds between starts
                // keeps event-driven refreshes inside the sustained 25-weight/s budget.
                if let Some(previous) = last_reconciliation {
                    tokio::time::sleep_until(previous + Duration::from_secs(10)).await;
                }

                if !session.stream_health.read().load(Ordering::Acquire) { continue; }
                last_reconciliation = Some(tokio::time::Instant::now());

                if let Err(e) = session.reconcile().await {
                    session.ready.store(false, Ordering::Release);
                    log::warn!("Arcus reconciliation failed: {e}");
                }
            }
        }));
        self.commands = Some(tx);
        self.cancels = Some(cancel_tx);
        self.stream = Some(stream);
        self.core.set_connected();
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        {
            let _commit = self.session.commit.lock();
            self.session.ready.store(false, Ordering::Release);
            self.session.mark_modifies_uncertain();
        }

        if let Some(mut stream) = self.stream.take() {
            stream.close().await;
        }

        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.commands.take();
        self.cancels.take();
        self.session
            .reject_unsent_modifies("Arcus execution disconnected before modification dispatch");
        self.core.set_disconnected();
        Ok(())
    }
    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let result = self.request(&order).and_then(|(request, market)| {
            anyhow::ensure!(
                self.session.submitted.lock().len() < MAX_DEDUPE_FILLS,
                "Arcus order intent capacity reached"
            );
            anyhow::ensure!(
                !self
                    .session
                    .submitted
                    .lock()
                    .contains_key(order.client_order_id().as_str()),
                "Arcus client order IDs cannot be reused within one session"
            );
            self.session
                .submitted
                .lock()
                .insert(order.client_order_id().to_string(), request.clone());
            self.session
                .order_types
                .lock()
                .insert(order.client_order_id().to_string(), order.order_type());
            let result = self.enqueue(Command::Submit(Box::new(order.clone()), request, market));

            if result.is_err() {
                self.session
                    .submitted
                    .lock()
                    .remove(order.client_order_id().as_str());
            }
            result
        });

        if let Err(e) = &result {
            self.session
                .emitter
                .emit_order_denied(&order, &e.to_string());
        }
        result
    }
    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        for order in self.core.get_orders_for_list(&cmd.order_list)? {
            self.session
                .emitter
                .emit_order_denied(&order, "Arcus order lists are unsupported");
        }
        anyhow::bail!("Arcus order lists are unsupported")
    }
    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let result = self.modification(&cmd).and_then(|(pending, market)| {
            let mut modifications = self.session.modifications.lock();
            anyhow::ensure!(
                !modifications.contains_key(&cmd.client_order_id),
                "Arcus order already has a pending modification"
            );
            anyhow::ensure!(
                modifications.len() < self.session.config.queue_capacity,
                "Arcus pending modification capacity reached"
            );
            let prioritized = pending.request.time_in_force == ArcusTimeInForce::Alo;
            modifications.insert(cmd.client_order_id, pending);
            drop(modifications);
            let result = self.enqueue(Command::Modify(
                cmd.client_order_id,
                cmd.command_id,
                market,
                prioritized,
            ));

            if result.is_err() {
                self.session
                    .modifications
                    .lock()
                    .remove(&cmd.client_order_id);
            }
            result
        });

        if let Err(e) = &result {
            self.session.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &e.to_string(),
                Session::now(),
            );
        }
        result
    }
    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let result = (|| {
            let context = self.session.context(cmd.instrument_id)?;
            let request = ArcusCancelRequest {
                address: self.session.address.clone(),
                account_index: self.session.config.subaccount_id,
                market_id: context.market.market_id,
                kind: if cmd.venue_order_id.is_some() {
                    "orderId"
                } else {
                    "clientId"
                }
                .to_string(),
                order_id: cmd.venue_order_id.map(|id| id.to_string()),
                client_id: cmd
                    .venue_order_id
                    .is_none()
                    .then(|| cmd.client_order_id.to_string()),
                timestamp: Session::now().as_u64(),
            };
            crate::signing::cancel_signing_payload(&request, request.timestamp)?;
            self.enqueue(Command::Cancel(cmd.clone(), request))
        })();

        if let Err(e) = &result {
            self.session.emitter.emit_order_cancel_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &e.to_string(),
                Session::now(),
            );
        }
        result
    }
    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let mut failure = None;

        for cancel in cmd.cancels {
            if let Err(e) = self.cancel_order(cancel) {
                failure = Some(e);
            }
        }

        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        self.enqueue(Command::CancelAll(cmd))
    }
    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        self.enqueue(Command::Reconcile)
    }
    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        self.enqueue(Command::Reconcile)
    }
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if let Some(id) = cmd.venue_order_id {
            let record = self
                .session
                .http
                .order_status(
                    &self.session.address,
                    self.session.config.subaccount_id,
                    id.as_str(),
                )
                .await?;
            let report = self
                .session
                .cache_order_records(vec![record])?
                .pop()
                .context("Arcus order is in an unresolved replacement transition")?;
            self.session.validate_public_order_report(&report)?;
            anyhow::ensure!(
                cmd.instrument_id
                    .is_none_or(|id| id == report.instrument_id),
                "Arcus queried order instrument mismatch"
            );
            anyhow::ensure!(
                cmd.client_order_id
                    .is_none_or(|id| Some(id) == report.client_order_id),
                "Arcus queried order client ID mismatch"
            );
            return Ok(Some(report));
        }
        let id = cmd
            .client_order_id
            .context("Arcus order query requires an order identifier")?;
        let report = self
            .session
            .orders(cmd.instrument_id, None, None)
            .await?
            .into_iter()
            .find(|report| report.client_order_id == Some(id));

        if let Some(report) = &report {
            self.session.validate_public_order_report(report)?;
        }
        Ok(report)
    }
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut reports = self
            .session
            .orders(
                cmd.instrument_id,
                cmd.start.map(|t| t.as_u64() / 1_000),
                cmd.end.map(|t| t.as_u64() / 1_000),
            )
            .await?;

        anyhow::ensure!(
            self.session.replacements.lock().is_empty(),
            "Arcus replacement outcomes remain pending"
        );

        for report in &reports {
            self.session.validate_public_order_report(report)?;
        }

        if cmd.open_only {
            reports.retain(|r| {
                !matches!(
                    r.order_status,
                    OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Rejected
                )
            });
        }
        Ok(reports)
    }
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let mut reports = self
            .session
            .fills(
                cmd.instrument_id,
                cmd.start.map(|t| t.as_u64() / 1_000),
                cmd.end.map(|t| t.as_u64() / 1_000),
            )
            .await?;

        if let Some(id) = cmd.venue_order_id {
            reports.retain(|r| r.venue_order_id == id);
        }
        Ok(reports)
    }
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.session.positions(cmd.instrument_id).await
    }
}

async fn dispatch_modify(
    session: &Session,
    client_id: ClientOrderId,
    generation: UUID4,
    market: &MarketContext,
) -> anyhow::Result<()> {
    let Some(mut pending) = session
        .modifications
        .lock()
        .get(&client_id)
        .filter(|pending| pending.command.command_id == generation)
        .cloned()
    else {
        return Ok(());
    };
    let validation = (|| -> anyhow::Result<()> {
        anyhow::ensure!(
            session.ready(),
            "Arcus modification dispatch requires a healthy reconciled session"
        );
        let latest = session
            .order_records
            .lock()
            .get(&pending.request.order_id)
            .cloned()
            .context("Arcus modify target record disappeared")?;
        anyhow::ensure!(
            !is_modify_cancel(&latest)
                && matches!(
                    order_status(latest.state.as_deref().unwrap_or(&latest.status))?,
                    OrderStatus::Accepted | OrderStatus::PartiallyFilled
                ),
            "Arcus modify target is no longer resting"
        );
        anyhow::ensure!(
            latest.sequence_number.is_some(),
            "Arcus modify target lost its authoritative sequence"
        );
        anyhow::ensure!(
            latest.side == pending.request.side
                && latest.time_in_force == Some(pending.request.time_in_force)
                && latest.reduce_only == pending.request.reduce_only
                && latest
                    .good_til_time
                    .as_ref()
                    .is_none_or(|expiry| expiry == &pending.request.good_til_time),
            "Arcus modify immutable attributes changed while queued"
        );

        if pending.command.quantity.is_none() {
            pending.request.quantity = latest.original_size;
        }

        if pending.command.price.is_none() {
            pending.request.price = latest.price;
        }
        pending.baseline_filled = pending.baseline_filled.max(
            latest
                .filled_size
                .unwrap_or(latest.original_size - latest.remaining_size),
        );
        anyhow::ensure!(
            pending.request.quantity > pending.baseline_filled,
            "Arcus modify total quantity is no longer above cumulative fills; use cancel"
        );
        pending.prior = latest;
        pending.request.client_time = Session::now().as_u64().to_string();
        crate::signing::modify_signing_payload(
            &pending.request,
            &market.market,
            pending.request.client_time.parse()?,
        )?;
        Ok(())
    })();

    if let Err(e) = validation {
        let _commit = session.commit.lock();

        if session
            .modifications
            .lock()
            .get(&client_id)
            .is_some_and(|current| current.command.command_id == generation)
        {
            session.modifications.lock().remove(&client_id);
            session.reject_modify(&pending, &e.to_string());
            session.refresh_modify_uncertainty();
        }
        return Ok(());
    }
    {
        let _commit = session.commit.lock();
        let still_queued = session
            .modifications
            .lock()
            .get(&client_id)
            .is_some_and(|current| current.command.command_id == generation);

        if !still_queued {
            return Ok(());
        }

        if !session.ready() {
            session.modifications.lock().remove(&client_id);
            session.reject_modify(
                &pending,
                "Arcus session lost readiness before modification dispatch",
            );
            session.refresh_modify_uncertainty();
            return Ok(());
        }
        pending.sent = true;
        session
            .modifications
            .lock()
            .insert(client_id, pending.clone());
        session.state_revision.fetch_add(2, Ordering::AcqRel);

        if pending.emit_pending {
            session
                .emitter
                .send_order_event(OrderEventAny::PendingUpdate(OrderPendingUpdate::new(
                    session.emitter.trader_id(),
                    pending.command.strategy_id,
                    pending.command.instrument_id,
                    client_id,
                    Some(session.config.account_id),
                    UUID4::new(),
                    Session::now(),
                    Session::now(),
                    false,
                    Some(VenueOrderId::from(pending.request.order_id.as_str())),
                )));
        }
    }
    let result = session
        .http
        .modify_order(&pending.request, &market.market)
        .await;
    let _commit = session.commit.lock();
    let mut modifications = session.modifications.lock();
    let Some(current) = modifications
        .get_mut(&client_id)
        .filter(|current| current.command.command_id == generation)
    else {
        return Ok(());
    }; // An earlier WS outcome already resolved this generation.

    match result {
        Ok(response) => {
            if response
                .order_id
                .as_deref()
                .is_some_and(|id| id != current.request.order_id)
            {
                current.uncertain = true;
                session.reconcile_notify.notify_one();
            } else if response.status == "REJECTED" {
                let reason = response
                    .rejection_reason
                    .as_deref()
                    .unwrap_or("Arcus modify was rejected");

                if !current.rejection_reported {
                    session.reject_modify(current, reason);
                    current.rejection_reported = true;
                }

                if modify_rejection_preserves_order(reason)
                    && !session
                        .replacements
                        .lock()
                        .contains_key(&VenueOrderId::from(current.request.order_id.as_str()))
                {
                    modifications.remove(&client_id);
                } else {
                    current.uncertain = true;
                    session.reconcile_notify.notify_one();
                }
            } else if response.status != "ACK" {
                session.reconcile_notify.notify_one();
            }
        }
        Err(e) => {
            if let Some(reason) = definite_modify_error(&e) {
                session.reject_modify(current, &reason);
                modifications.remove(&client_id);
            } else {
                current.uncertain = true;
                session.reconcile_notify.notify_one();
                log::warn!("Arcus modify outcome requires reconciliation for {client_id}: {e}");
            }
        }
    }
    drop(modifications);
    session.refresh_modify_uncertainty();
    Ok(())
}

fn definite_modify_error(error: &ArcusHttpError) -> Option<String> {
    match error {
        ArcusHttpError::Rejected { message, .. }
        | ArcusHttpError::Validation(message)
        | ArcusHttpError::Transport(message) => Some(message.clone()),
        ArcusHttpError::RateLimited {
            reason,
            retry_after_ms,
        } => Some(format!(
            "Arcus rate limited {reason}; retry after {retry_after_ms} ms"
        )),
        _ => None,
    }
}

async fn dispatch(session: &Session, command: Command) -> anyhow::Result<()> {
    match command {
        Command::Modify(client_id, generation, market, _) => {
            dispatch_modify(session, client_id, generation, &market).await
        }
        Command::Submit(order, mut request, market) => {
            if !session.ready() {
                session.pending.lock().remove(&order.client_order_id());
                session.emitter.emit_order_denied(
                    &order,
                    "Arcus private stream or reconciliation is unhealthy",
                );
                return Ok(());
            }
            request.timestamp = Session::now().as_u64();
            request.good_til_time =
                (request.timestamp / 1_000 + 33 * 86_400 * 1_000_000).to_string();
            session
                .submitted
                .lock()
                .insert(order.client_order_id().to_string(), request.clone());
            session.pending.lock().insert(order.client_order_id());
            session.emitter.emit_order_submitted(&order);

            match session.http.place_order(&request, &market.market).await {
                Ok(_) => {} // HTTP 202 is pending, never an accepted event
                Err(
                    ArcusHttpError::Rejected {
                        message: reason, ..
                    }
                    | ArcusHttpError::Validation(reason)
                    | ArcusHttpError::Transport(reason),
                ) => {
                    session.pending.lock().remove(&order.client_order_id());
                    session.emitter.emit_order_rejected(
                        &order,
                        &reason,
                        Session::now(),
                        order.is_post_only(),
                    );
                }
                Err(ArcusHttpError::RateLimited {
                    retry_after_ms,
                    reason,
                }) => {
                    session.pending.lock().remove(&order.client_order_id());
                    session.emitter.emit_order_rejected(
                        &order,
                        &format!("Arcus rate limited {reason}; retry after {retry_after_ms} ms"),
                        Session::now(),
                        false,
                    );
                }
                Err(e) => {
                    session.ready.store(false, Ordering::Release);
                    session.reconcile_notify.notify_one();
                    log::warn!("Arcus submit outcome requires reconciliation: {e}");
                }
            }
            Ok(())
        }
        Command::Cancel(cmd, mut request) => {
            request.timestamp = Session::now().as_u64();

            if let Err(e) = session.http.cancel_order(&request).await {
                match e {
                    ArcusHttpError::Rejected { message, .. }
                    | ArcusHttpError::Validation(message)
                    | ArcusHttpError::Transport(message) => {
                        session.emitter.emit_order_cancel_rejected_event(
                            cmd.strategy_id,
                            cmd.instrument_id,
                            cmd.client_order_id,
                            cmd.venue_order_id,
                            &message,
                            Session::now(),
                        );
                    }
                    ArcusHttpError::RateLimited {
                        retry_after_ms,
                        reason,
                    } => {
                        session.emitter.emit_order_cancel_rejected_event(
                            cmd.strategy_id,
                            cmd.instrument_id,
                            cmd.client_order_id,
                            cmd.venue_order_id,
                            &format!(
                                "Arcus rate limited {reason}; retry after {retry_after_ms} ms"
                            ),
                            Session::now(),
                        );
                    }
                    other => {
                        session.ready.store(false, Ordering::Release);
                        log::warn!(
                            "Arcus cancel outcome requires reconciliation for {}: {other}",
                            cmd.client_order_id
                        );
                    }
                }
            }
            session.reconcile_notify.notify_one();
            Ok(())
        }
        Command::CancelAll(cmd) => {
            if cmd.order_side.is_none() {
                let context = session.context(cmd.instrument_id)?;

                match session
                    .http
                    .cancel_market(
                        &session.address,
                        session.config.subaccount_id,
                        context.market.market_id,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        session.ready.store(false, Ordering::Release);
                        session.reconcile_notify.notify_one();
                        return Err(e.into());
                    }
                }
                session.reconcile_notify.notify_one();
                return Ok(());
            }
            // The venue-wide endpoint exceeds an instrument/side-scoped command's authority.
            for order in session.orders(Some(cmd.instrument_id), None, None).await? {
                if matches!(
                    order.order_status,
                    OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Rejected
                ) || cmd
                    .order_side
                    .is_some_and(|side| Some(side) != order.order_side)
                {
                    continue;
                }
                let context = session.context(order.instrument_id)?;
                let request = ArcusCancelRequest {
                    address: session.address.clone(),
                    account_index: session.config.subaccount_id,
                    market_id: context.market.market_id,
                    kind: "orderId".to_string(),
                    order_id: Some(order.venue_order_id.to_string()),
                    client_id: None,
                    timestamp: Session::now().as_u64(),
                };
                session.http.cancel_order(&request).await?;
            }
            session.reconcile_notify.notify_one();
            Ok(())
        }
        Command::Reconcile => {
            session.reconcile_notify.notify_one();
            Ok(())
        }
    }
}

fn side(value: ArcusOrderSide) -> OrderSide {
    match value {
        ArcusOrderSide::Buy => OrderSide::Buy,
        ArcusOrderSide::Sell => OrderSide::Sell,
    }
}

fn micros(value: u64) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(1_000)
        .map(UnixNanos::from)
        .context("Arcus timestamp overflows nanoseconds")
}

fn exact_money(value: Decimal, currency: Currency) -> anyhow::Result<Money> {
    let money = Money::from_decimal(value, currency)?;
    anyhow::ensure!(
        money.as_decimal() == value,
        "Arcus money exceeds settlement currency precision"
    );
    Ok(money)
}

fn order_status(status: &str) -> anyhow::Result<OrderStatus> {
    Ok(match status {
        "OPEN" | "PLACED" | "RESTING" => OrderStatus::Accepted,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "MARGIN_CANCELED" | "TPSL_CANCELED" => OrderStatus::Canceled,
        "LIQUIDATED" | "ADL" => OrderStatus::Filled,
        "PENDING"
        | "ACK"
        | "CANCEL_ACKNOWLEDGED"
        | "CANCEL_ALL_ACKNOWLEDGED"
        | "CANCEL_PENDING" => OrderStatus::Submitted,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        other => anyhow::bail!("Unknown or non-final Arcus order status {other}"),
    })
}

fn is_modify_cancel(record: &ArcusOrder) -> bool {
    record.cancel_reason.as_deref() == Some("MODIFY_CANCELED")
        && matches!(
            record.state.as_deref().unwrap_or(&record.status),
            "CANCELED" | "MARGIN_CANCELED"
        )
}

fn modify_rejection_preserves_order(reason: &str) -> bool {
    matches!(
        reason,
        "MODIFY_CHANGED_IMMUTABLE_FIELD"
            | "MODIFY_WOULD_CROSS_OUTSIDE_RTH_TRADING_BOUNDARY"
            | "MODIFY_TPSL_NOT_SUPPORTED"
    )
}

fn modify_rejection_cancels_order(reason: &str) -> bool {
    matches!(
        reason,
        "MODIFY_SIZE_ALREADY_FILLED"
            | "MODIFY_SUPERSEDED_BY_CANCEL"
            | "FILL_WILL_EXCEED_TRADING_BOUND"
    )
}

fn is_terminal(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::atomic::AtomicUsize};

    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use nautilus_common::{
        cache::Cache, live::runner::replace_exec_event_sender, messages::ExecutionEvent,
    };
    use nautilus_model::{
        events::OrderEventAny,
        identifiers::{StrategyId, TraderId},
        orders::OrderTestBuilder,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};

    use super::*;

    const ADDRESS: &str = "0x1111111111111111111111111111111111111111";

    fn market() -> ArcusMarket {
        serde_json::from_value(json!({
            "marketDisplayName":"BTC-USD", "marketId":1, "baseAsset":"BTC", "quoteAsset":"USD",
            "status":"ONLINE", "type":"PERPETUAL", "tickSize":"0.1", "stepSize":"0.001",
            "minOrderSize":"0.001", "maxOrderSize":"100", "minOrderNotional":"10",
            "oraclePrice":"50000", "markPrice":"50000", "initialMarginFraction":"0.1",
            "maintenanceMarginFraction":"0.05"
        }))
        .unwrap()
    }

    fn order(kind: OrderType) -> OrderAny {
        let mut builder = OrderTestBuilder::new(kind);
        builder
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(InstrumentId::from("BTC-USD-PERP.ARCUS"))
            .client_order_id(ClientOrderId::from("ORDER-1"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .time_in_force(TimeInForce::Gtc);

        if kind == OrderType::Limit {
            builder.price(Price::from("50000.0"));
        }
        builder.build()
    }

    fn client(
        url: String,
    ) -> (
        ArcusExecutionClient,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let (client, events, _) = client_with_cache(url);
        (client, events)
    }

    type TestCache = Rc<RefCell<Cache>>;

    fn client_with_cache(
        url: String,
    ) -> (
        ArcusExecutionClient,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
        TestCache,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let config = ArcusExecutionClientConfig {
            address: Some(ADDRESS.into()),
            api_secret: Some("07".repeat(32)),
            base_url_http: Some(url),
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("ARCUS"),
            Venue::from("ARCUS"),
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let mut client = ArcusExecutionClient::new(core, config).unwrap();
        client.start().unwrap();
        let market = market();
        let instrument = parse_instrument(&market, Session::now()).unwrap();
        client.session.markets.write().insert(
            instrument.id(),
            Arc::new(MarketContext { market, instrument }),
        );
        client.session.ready.store(true, Ordering::Release);
        client
            .session
            .stream_health
            .read()
            .store(true, Ordering::Release);
        (client, rx, cache)
    }

    #[rstest]
    #[case(true, TimeInForce::Gtc, false, ArcusTimeInForce::Alo)]
    #[case(false, TimeInForce::Fok, false, ArcusTimeInForce::Fok)]
    #[case(false, TimeInForce::Ioc, false, ArcusTimeInForce::Ioc)]
    #[case(false, TimeInForce::Gtc, true, ArcusTimeInForce::Gtt)]
    fn limit_execution_instructions_preserve_intent(
        #[case] post_only: bool,
        #[case] time_in_force: TimeInForce,
        #[case] reduce_only: bool,
        #[case] expected_tif: ArcusTimeInForce,
    ) {
        let (client, _) = client("http://127.0.0.1:1".into());
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        builder
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(InstrumentId::from("BTC-USD-PERP.ARCUS"))
            .client_order_id(ClientOrderId::from("FLAGS-1"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .price(Price::from("50000.0"))
            .time_in_force(time_in_force)
            .post_only(post_only)
            .reduce_only(reduce_only);
        let order = builder.build();
        let (request, context) = client.request(&order).unwrap();
        assert_eq!(request.time_in_force, expected_tif);
        assert_eq!(request.reduce_only, reduce_only);
        assert_eq!(request.order_type, ArcusOrderType::Limit);
        assert_eq!(request.quantity, dec!(0.010));
        assert_eq!(request.price, dec!(50000.0));
        crate::signing::order_signing_payload(&request, &context.market, request.timestamp)
            .unwrap();
    }

    #[rstest]
    #[case(OrderSide::Buy,dec!(50251.0))]
    #[case(OrderSide::Sell,dec!(49750.0))]
    fn market_ioc_requires_fresh_quote_after_valid_feed_status(
        #[case] side: OrderSide,
        #[case] expected_price: Decimal,
    ) {
        use nautilus_model::{
            data::{InstrumentStatus, QuoteTick},
            enums::MarketStatusAction,
        };

        let (client, _, cache) = client_with_cache("http://127.0.0.1:1".into());
        let id = InstrumentId::from("BTC-USD-PERP.ARCUS");
        let mut builder = OrderTestBuilder::new(OrderType::Market);
        builder
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(id)
            .client_order_id(ClientOrderId::from("MARKET-1"))
            .side(side)
            .quantity(Quantity::from("0.010"))
            .time_in_force(TimeInForce::Ioc);
        let order = builder.build();
        assert!(client.request(&order).is_err());
        let now = Session::now();
        cache
            .borrow_mut()
            .add_quote(QuoteTick::new(
                id,
                Price::from("50000.0"),
                Price::from("50001.0"),
                Quantity::from("1.000"),
                Quantity::from("1.000"),
                now,
                now,
            ))
            .unwrap();
        // A quote alone cannot establish that the feed is currently usable.
        assert!(client.request(&order).is_err());
        cache
            .borrow_mut()
            .add_instrument_status(InstrumentStatus::new(
                id,
                MarketStatusAction::Trading,
                now,
                now,
                None,
                None,
                Some(true),
                Some(true),
                None,
            ))
            .unwrap();
        let (request, context) = client.request(&order).unwrap();
        assert_eq!(request.order_type, ArcusOrderType::Limit);
        assert_eq!(request.time_in_force, ArcusTimeInForce::Ioc);
        assert_eq!(request.price, expected_price);
        assert_eq!(
            request.order_side,
            if side == OrderSide::Buy {
                ArcusOrderSide::Buy
            } else {
                ArcusOrderSide::Sell
            }
        );
        crate::signing::order_signing_payload(&request, &context.market, request.timestamp)
            .unwrap();
        let later = UnixNanos::from(now.as_u64() + 1);
        cache
            .borrow_mut()
            .add_instrument_status(InstrumentStatus::new(
                id,
                MarketStatusAction::Trading,
                later,
                later,
                None,
                None,
                Some(true),
                Some(false),
                None,
            ))
            .unwrap();
        assert!(client.request(&order).is_err());
        cache
            .borrow_mut()
            .add_instrument_status(InstrumentStatus::new(
                id,
                MarketStatusAction::Trading,
                later,
                later,
                None,
                None,
                Some(true),
                Some(true),
                None,
            ))
            .unwrap();
        // A readiness recovery cannot revive a quote from before the recovery boundary.
        assert!(client.request(&order).is_err());
        cache
            .borrow_mut()
            .add_quote(QuoteTick::new(
                id,
                Price::from("50000.0"),
                Price::from("50001.0"),
                Quantity::from("1.000"),
                Quantity::from("1.000"),
                later,
                later,
            ))
            .unwrap();
        assert!(client.request(&order).is_ok());
        cache
            .borrow_mut()
            .add_instrument_status(InstrumentStatus::new(
                id,
                MarketStatusAction::Halt,
                later,
                later,
                None,
                None,
                Some(false),
                Some(true),
                None,
            ))
            .unwrap();
        assert!(client.request(&order).is_err());
    }

    #[derive(Clone)]
    struct MockState {
        requests: Arc<AtomicUsize>,
        status: StatusCode,
    }

    async fn place(
        State(state): State<MockState>,
        headers: HeaderMap,
        Json(value): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.requests.fetch_add(1, Ordering::AcqRel);
        let request: ArcusOrderRequest = serde_json::from_value(value).unwrap();
        let timestamp: u64 = headers["X-Timestamp"].to_str().unwrap().parse().unwrap();
        let public_key: [u8; 32] =
            nautilus_core::hex::decode(headers["X-API-Key"].to_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap();
        let signature = Signature::from_slice(
            &nautilus_core::hex::decode(headers["X-Signature"].to_str().unwrap()).unwrap(),
        )
        .unwrap();
        let payload =
            crate::signing::order_signing_payload(&request, &market(), timestamp).unwrap();
        VerifyingKey::from_bytes(&public_key)
            .unwrap()
            .verify(payload.as_bytes(), &signature)
            .unwrap();
        assert_eq!(request.time_in_force, ArcusTimeInForce::Gtt);
        assert_eq!(request.client_id.as_deref(), Some("ORDER-1"));
        assert!(Session::now().as_u64().saturating_sub(timestamp) < 10_000_000_000);

        if state.status == StatusCode::ACCEPTED {
            (
                state.status,
                Json(json!({"status":"ACK", "clientId":"ORDER-1"})),
            )
        } else {
            (state.status, Json(json!({"error":"Rejected by mock"})))
        }
    }

    async fn server(status: StatusCode) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route("/v1/placeOrder", post(place))
            .route("/v1/account", get(|| async { Json(json!({"address":ADDRESS, "accountIndex":0,
                "equity":"1000", "freeCollateral":"1000", "netQuoteBalance":"1000", "netDeposits":"1000",
                "positions":{}, "sequenceNumber":1})) }))
            .route("/v1/orders", get(|| async { Json(json!({"orders":[]})) }))
            .route("/v1/openOrders", get(|| async { Json(json!({"orders":[]})) }))
            .route("/v1/fills", get(|| async { Json(json!({"fills":[]})) }))
            .route("/v1/positions", get(|| async { Json(json!({"positions":{}})) }))
            .with_state(MockState { requests: requests.clone(), status });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (url, requests, task)
    }

    #[tokio::test]
    async fn signed_submit_reaches_http_once_and_ack_is_not_accepted() {
        let (url, requests, server) = server(StatusCode::ACCEPTED).await;
        let (client, mut events) = client(url);
        let order = order(OrderType::Limit);
        let (mut request, market) = client.request(&order).unwrap();
        // Dispatch refreshes an old queued timestamp before signing and sending the intent.
        request.timestamp = 1;
        dispatch(
            &client.session,
            Command::Submit(Box::new(order.clone()), request, market),
        )
        .await
        .unwrap();
        assert_eq!(requests.load(Ordering::Acquire), 1);
        assert!(
            client
                .session
                .pending
                .lock()
                .contains(&order.client_order_id())
        );
        assert!(client.session.ready());
        let mut submitted = 0;

        while let Ok(event) = events.try_recv() {
            if let ExecutionEvent::Order(event) = event {
                match event {
                    OrderEventAny::Submitted(_) => submitted += 1,
                    other => panic!("Unexpected terminal or accepted event: {other:?}"),
                }
            }
        }
        assert_eq!(submitted, 1);
        server.abort();
    }

    #[tokio::test]
    async fn unknown_submit_is_not_rejected_or_retried() {
        let (url, requests, server) = server(StatusCode::BAD_GATEWAY).await;
        let (client, mut events) = client(url);
        let order = order(OrderType::Limit);
        let (request, market) = client.request(&order).unwrap();
        dispatch(
            &client.session,
            Command::Submit(Box::new(order.clone()), request, market),
        )
        .await
        .unwrap();
        assert_eq!(requests.load(Ordering::Acquire), 1);
        assert!(
            client
                .session
                .pending
                .lock()
                .contains(&order.client_order_id())
        );
        assert!(!client.session.ready());

        while let Ok(event) = events.try_recv() {
            assert!(!matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::Rejected(_))
            ));
        }
        server.abort();
    }

    #[rstest]
    fn disconnected_stream_blocks_new_risk_before_queueing() {
        let (client, _) = client("http://127.0.0.1:1".into());
        client
            .session
            .stream_health
            .read()
            .store(false, Ordering::Release);
        assert!(client.request(&order(OrderType::Limit)).is_err());
    }

    #[rstest]
    fn pending_and_cancel_acknowledgements_are_not_terminal() {
        for status in ["ACK", "PENDING", "CANCEL_PENDING", "CANCEL_ACKNOWLEDGED"] {
            assert_eq!(order_status(status).unwrap(), OrderStatus::Submitted);
        }
        assert!(order_status("UNRECOGNIZED").is_err());
    }

    #[rstest]
    fn signed_short_position_preserves_negative_exposure() {
        let (client, _) = client("http://127.0.0.1:1".into());
        let position: ArcusPosition = serde_json::from_value(json!({
            "marketId":1,"marketDisplayName":"BTC-USD","side":"SHORT","size":"-0.125",
            "averageEntryPrice":"50000","marginMode":"CROSS","leverage":"2","marginUsed":"3125",
            "unrealizedPnl":"0","markPx":"50000"
        }))
        .unwrap();
        let context = client.session.context_for_market(1).unwrap();
        let report = client.session.position_report(&position, &context).unwrap();
        assert_eq!(report.position_side, PositionSide::Short);
        assert_eq!(report.signed_decimal_qty, dec!(-0.125));
        assert_eq!(report.quantity.as_decimal(), dec!(0.125));
    }

    #[tokio::test]
    async fn reconciliation_cursor_commits_only_complete_batches_and_queries_missing_old_open() {
        #[derive(Clone)]
        struct CursorState {
            queries: Arc<Mutex<Vec<HashMap<String, String>>>>,
            lookups: Arc<AtomicUsize>,
            open: Value,
            closed: Value,
        }
        let now = Session::now().as_u64() / 1_000;
        let open = json!({"orderId":"abc123","clientId":"OLD-1","accountIndex":0,
            "marketId":1,"marketDisplayName":"BTC-USD","side":"BUY","type":"LIMIT","status":"OPEN",
            "price":"50000.0","originalSize":"0.010","remainingSize":"0.010","timeInForce":"GTT",
            "createdAt":now-86_400_000_000_u64,"updatedAt":now-86_400_000_000_u64,"sequenceNumber":1});
        let mut closed = open.clone();
        closed["status"] = "CANCELED".into();
        closed["sequenceNumber"] = 2.into();
        closed["updatedAt"] = now.into();
        let state = CursorState {
            queries: Arc::new(Mutex::new(Vec::new())),
            lookups: Arc::new(AtomicUsize::new(0)),
            open,
            closed,
        };
        let router = Router::new()
            .route("/v1/account", get(|| async { Json(json!({"address":ADDRESS,"accountIndex":0,"equity":"1000",
                "freeCollateral":"1000","netQuoteBalance":"1000","netDeposits":"1000","positions":{},"sequenceNumber":1})) }))
            .route("/v1/orders", get(|State(state):State<CursorState>, axum::extract::Query(query):axum::extract::Query<HashMap<String,String>>| async move {
                let mut queries=state.queries.lock(); let first=queries.is_empty(); queries.push(query);
                Json(json!({"orders":if first {vec![state.open]} else {Vec::<Value>::new()}}))
            }))
            .route("/v1/openOrders", get(|State(state):State<CursorState>| async move {
                Json(json!({"orders":if state.queries.lock().len()==1 {vec![state.open]} else {Vec::<Value>::new()}}))
            }))
            .route("/v1/order/abc123", get(|State(state):State<CursorState>| async move {
                state.lookups.fetch_add(1,Ordering::AcqRel); Json(state.closed)
            }))
            .route("/v1/fills", get(|| async {Json(json!({"fills":[]}))}))
            .route("/v1/positions", get(|| async {Json(json!({"positions":{}}))}))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let (client, mut events) = client(url);
        client.session.state_revision.store(1, Ordering::Release);
        client.session.reconcile().await.unwrap();
        assert_eq!(client.session.order_cursor_us.load(Ordering::Acquire), 0);
        client.session.state_revision.store(2, Ordering::Release);
        client.session.reconcile().await.unwrap();
        let cursor = client.session.order_cursor_us.load(Ordering::Acquire);
        assert!(cursor > 0);
        assert_eq!(state.lookups.load(Ordering::Acquire), 1);
        client.session.reconcile().await.unwrap();
        let queries = state.queries.lock();
        assert_eq!(queries.len(), 3);
        assert!(!queries[0].contains_key("from"));
        assert!(!queries[1].contains_key("from"));
        assert_eq!(
            queries[2]["from"],
            cursor.saturating_sub(FILL_OVERLAP_US).to_string()
        );
        assert!(queries.iter().all(|query| query.contains_key("to")));
        let mut closed_seen = false;

        while let Ok(event) = events.try_recv() {
            if let ExecutionEvent::Report(ExecutionReport::OrderWithFills(order, _)) = event
                && order.venue_order_id == VenueOrderId::from("abc123")
            {
                assert_eq!(order.order_status, OrderStatus::Canceled);
                closed_seen = true;
            }
        }
        assert!(closed_seen);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_trait_submit_fill_reconnect_compensation_and_cancel() {
        use futures_util::{SinkExt, StreamExt};
        use nautilus_common::{
            live::runner::replace_data_event_sender, messages::execution::SubmitOrder,
        };
        use nautilus_core::UUID4;
        use tokio_tungstenite::{accept_async, tungstenite::Message};

        #[derive(Clone)]
        struct LiveState {
            orders: Arc<Mutex<Vec<Value>>>,
            fills: Arc<Mutex<Vec<Value>>>,
            frames: tokio::sync::broadcast::Sender<Value>,
            posts: Arc<AtomicUsize>,
            history_queries: Arc<Mutex<Vec<Option<String>>>>,
        }
        let (frames, _) = tokio::sync::broadcast::channel(32);
        let state = LiveState {
            orders: Arc::new(Mutex::new(Vec::new())),
            fills: Arc::new(Mutex::new(Vec::new())),
            frames,
            posts: Arc::new(AtomicUsize::new(0)),
            history_queries: Arc::new(Mutex::new(Vec::new())),
        };
        let router = Router::new()
            .route("/v1/markets", get(|| async { Json(json!({"markets":[market()]})) }))
            .route("/v1/account", get(|| async { Json(json!({"address":ADDRESS,"accountIndex":0,"equity":"1000",
                "freeCollateral":"1000","netQuoteBalance":"1000","netDeposits":"1000","positions":{},"sequenceNumber":1})) }))
            .route("/v1/orders", get(|State(state):State<LiveState>, axum::extract::Query(query):axum::extract::Query<HashMap<String,String>>| async move {
                state.history_queries.lock().push(query.get("from").cloned());
                let from = query.get("from").map(|value| value.parse::<u64>().unwrap());
                let to = query.get("to").map(|value| value.parse::<u64>().unwrap());
                let orders: Vec<_> = state.orders.lock().iter().filter(|order| {
                    let updated = order["updatedAt"].as_u64().unwrap();
                    from.is_none_or(|start| updated >= start) && to.is_none_or(|end| updated <= end)
                }).cloned().collect();
                Json(json!({"orders":orders}))
            }))
            .route("/v1/openOrders", get(|State(state):State<LiveState>| async move {
                let orders: Vec<_> = state.orders.lock().iter().filter(|order| {
                    !is_terminal(order_status(order["status"].as_str().unwrap()).unwrap())
                }).cloned().collect();
                Json(json!({"orders":orders}))
            }))
            .route("/v1/fills", get(|State(state):State<LiveState>, axum::extract::Query(query):axum::extract::Query<HashMap<String,String>>| async move {
                assert!(query.get("from").is_none_or(|value| value.parse::<u64>().unwrap() >= 100_000_000_000_000));
                Json(json!({"fills":state.fills.lock().clone()}))
            }))
            .route("/v1/positions", get(|| async { Json(json!({"positions":{}})) }))
            .route("/v1/placeOrder", post(|State(state):State<LiveState>, Json(value):Json<Value>| async move {
                state.posts.fetch_add(1, Ordering::AcqRel);
                let request: ArcusOrderRequest = serde_json::from_value(value).unwrap();
                crate::signing::order_signing_payload(&request,&market(),request.timestamp).unwrap();
                let record=json!({"orderId":"abc123","clientId":request.client_id,"accountIndex":0,
                    "marketId":1,"marketDisplayName":"BTC-USD","side":"BUY","type":"LIMIT","status":"OPEN",
                    "price":request.price,"originalSize":request.quantity,"remainingSize":request.quantity,
                    "timeInForce":"GTT","createdAt":request.timestamp/1000,"updatedAt":request.timestamp/1000,"sequenceNumber":2});
                state.orders.lock().push(record.clone());
                let _ = state.frames.send(json!({"type":"channel_data","channel":"orders","id":ADDRESS,"accountIndex":0,"contents":record}));
                (StatusCode::ACCEPTED,Json(json!({"status":"ACK","orderId":"abc123","clientId":"ORDER-1"})))
            }))
            .route("/v1/cancelOrder", post(|State(state):State<LiveState>, Json(_):Json<Value>| async move {
                let record = {
                    let mut orders=state.orders.lock(); orders[0]["status"]="CANCELED".into();
                    orders[0]["sequenceNumber"]=5.into(); orders[0]["updatedAt"]=(Session::now().as_u64()/1000).into(); orders[0].clone()
                };
                let _=state.frames.send(json!({"type":"channel_data","channel":"orders","id":ADDRESS,"accountIndex":0,"contents":record}));
                (StatusCode::ACCEPTED,Json(json!({"status":"CANCEL_ACKNOWLEDGED","orderId":"abc123"})))
            })).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_url = format!("http://{}", listener.local_addr().unwrap());

        let http_task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let ws_frames = state.frames.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let ws_connections = connections.clone();

        let ws_task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                ws_connections.fetch_add(1, Ordering::AcqRel);
                let mut inbox = ws_frames.subscribe();

                loop {
                    tokio::select! {
                        frame=inbox.recv()=>{
                            let frame=frame.unwrap();

                            if frame["_close"]==true {let _=socket.close(None).await;break;}

                            if socket.send(Message::Text(frame.to_string().into())).await.is_err(){break;}
                        }
                        message=socket.next()=>match message {
                            Some(Ok(Message::Text(text)))=>{
                                let mut subscription:Value=serde_json::from_str(&text).unwrap();
                                subscription["type"]="subscribed".into(); subscription["contents"]=json!({"isSnapshot":true});

                                if socket.send(Message::Text(subscription.to_string().into())).await.is_err(){break;}
                            }
                            Some(Ok(Message::Ping(payload)))=>{if socket.send(Message::Pong(payload)).await.is_err(){break;}},
                            Some(Ok(Message::Close(_)) | Err(_)) | None=>break,
                            _=>{}
                        }
                    }
                }
            }
        });
        let cache = Rc::new(RefCell::new(Cache::default()));
        let config = ArcusExecutionClientConfig {
            address: Some(ADDRESS.into()),
            api_secret: Some("07".repeat(32)),
            base_url_http: Some(http_url),
            base_url_ws: Some(ws_url),
            reconciliation_interval_secs: 60,
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("ARCUS"),
            Venue::from("ARCUS"),
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let (exec_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(exec_tx);
        let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let mut client = ArcusExecutionClient::new(core, config).unwrap();
        client.start().unwrap();
        client.connect().await.unwrap();
        wait_ready(&client).await;

        while let Ok(DataEvent::Instrument(instrument)) = data_rx.try_recv() {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
        let order = order(OrderType::Limit);
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        client
            .submit_order(SubmitOrder::from_order(
                &order,
                order.trader_id(),
                None,
                None,
                UUID4::new(),
                Session::now(),
            ))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ExecutionEvent::Report(
                    ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _),
                ) = events.recv().await.unwrap()
                    && report.order_status == OrderStatus::Accepted
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(state.posts.load(Ordering::Acquire), 1);
        let now = Session::now().as_u64() / 1000;
        let fill1 = json!({"tradeId":"FILL-1","orderId":"abc123","clientId":"ORDER-1","marketId":1,
            "marketDisplayName":"BTC-USD","side":"BUY","originalSize":"0.010","size":"0.003","price":"50000.0",
            "fee":"0.01","role":"TAKER","createdAt":now,"sequenceNumber":3});
        state.fills.lock().push(fill1.clone());
        state.orders.lock()[0]["remainingSize"] = "0.007".into();
        state.orders.lock()[0]["filledSize"] = "0.003".into();
        state.orders.lock()[0]["status"] = "PARTIALLY_FILLED".into();
        state.orders.lock()[0]["sequenceNumber"] = 3.into();
        state.frames.send(json!({"type":"channel_data","channel":"userFills","id":ADDRESS,"accountIndex":0,"contents":{"fills":[fill1.clone(),fill1]}})).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ExecutionEvent::Report(ExecutionReport::Fill(fill)) =
                    events.recv().await.unwrap()
                {
                    break fill;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(first.trade_id, TradeId::from("FILL-1"));
        assert_eq!(first.last_qty.as_decimal(), dec!(0.003));
        state.frames.send(json!({"_close":true})).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while client.session.stream_health.read().load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!client.is_connected());
        assert!(client.request(&order).is_err());
        state.fills.lock().push(json!({"tradeId":"FILL-2","orderId":"abc123","clientId":"ORDER-1","marketId":1,
            "marketDisplayName":"BTC-USD","side":"BUY","originalSize":"0.010","size":"0.002","price":"50000.0",
            "fee":"0.02","role":"TAKER","createdAt":now+1,"sequenceNumber":4}));
        state.orders.lock()[0]["remainingSize"] = "0.005".into();
        state.orders.lock()[0]["filledSize"] = "0.005".into();
        state.orders.lock()[0]["sequenceNumber"] = 4.into();
        state.orders.lock()[0]["updatedAt"] = (now + 1).into();
        wait_ready(&client).await;
        assert!(connections.load(Ordering::Acquire) >= 2);
        let queries = state.history_queries.lock().clone();
        assert_eq!(queries.first(), Some(&None));
        assert!(queries.iter().skip(1).any(Option::is_some));
        let mut recovered = Vec::new();

        while let Ok(event) = events.try_recv() {
            match event {
                ExecutionEvent::Report(ExecutionReport::Fill(fill)) => {
                    recovered.push(fill.trade_id.to_string());
                }
                ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, fills)) => {
                    recovered.extend(fills.into_iter().map(|fill| fill.trade_id.to_string()));
                }
                _ => {}
            }
        }
        assert_eq!(recovered, vec!["FILL-2"]);
        client
            .cancel_order(CancelOrder::new(
                order.trader_id(),
                None,
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                Some(VenueOrderId::from("abc123")),
                UUID4::new(),
                Session::now(),
                None,
                None,
            ))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ExecutionEvent::Report(
                    ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _),
                ) = events.recv().await.unwrap()
                    && report.order_status == OrderStatus::Canceled
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        client.disconnect().await.unwrap();
        client.stop().unwrap();
        client.stop().unwrap();
        http_task.abort();
        ws_task.abort();
    }

    async fn wait_ready(client: &ArcusExecutionClient) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !client.is_connected() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
