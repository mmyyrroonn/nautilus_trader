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

//! Live execution client for the Aster DEX Futures V3 API.
//!
//! Order entry, cancellation, and account queries go through [`AsterHttpClient`] (EIP-712
//! signed); order and account state arrives on the private user data stream, whose Binance-USD-M
//! frames are decoded by [`nautilus_binance`] and turned into Nautilus reports.
//!
//! # Scope
//!
//! One-way (net) position mode with `LIMIT` and `MARKET` orders. Hedge mode is detected at
//! connect and rejected rather than silently mishandled, and conditional/algo order types are
//! rejected at submission. Order modification is not supported by this adapter: cancel and
//! resubmit instead.

use std::{collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use futures_util::StreamExt;
use nautilus_binance::{
    common::{
        enums::{BinanceEnvironment, BinanceProductType},
        symbol::format_binance_symbol,
    },
    futures::{
        http::{client::BinanceFuturesHttpClient, error::BinanceFuturesHttpError},
        websocket::streams::{
            messages::{BinanceFuturesAccountUpdateMsg, BinanceFuturesWsStreamsMessage},
            parse_exec::{
                parse_futures_order_update_to_fill, parse_futures_order_update_to_order_status,
            },
        },
    },
};
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::{get_exec_event_sender, try_get_data_event_sender},
    messages::{
        DataEvent,
        execution::{
            CancelAllOrders, CancelOrder, GenerateFillReports, GenerateFillReportsBuilder,
            GenerateOrderStatusReport, GenerateOrderStatusReports,
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReports,
            GeneratePositionStatusReportsBuilder, ModifyOrder, QueryAccount, QueryOrder,
            SubmitOrder,
        },
    },
};
use nautilus_core::{
    Params, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter,
    task::{TaskGroup, TaskSpawner},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType, OrderSide, OrderType, PositionSide, TimeInForce},
    events::AccountState,
    identifiers::{AccountId, ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Quantity},
};
use nautilus_network::retry::{RetryConfig, RetryManager};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{
        consts::ASTER_LISTEN_KEY_RENEWAL_SECS, credential::AsterCredential,
        currency::resolve_currency,
    },
    config::{AsterExecutionClientConfig, DEFAULT_WS_CONNECT_TIMEOUT_SECS},
    http::{
        AsterHttpClient, AsterParams,
        client::{ASTER_HISTORY_MAX_INTERVAL_MS, ASTER_HISTORY_PAGE_LIMIT},
        error::AsterHttpError,
        models::{
            AsterBalance, AsterOrder, AsterUserTrade, millis_to_nanos, parse_decimal,
            parse_order_side,
        },
    },
    websocket::AsterUserStreamClient,
};

/// Settlement currency for Aster USD-margined perpetuals.
const ASTER_SETTLEMENT_ASSET: &str = "USDT";

/// How long to wait before retrying a dropped user data stream session.
const USER_STREAM_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Delays before each repeat of the first user data stream connect.
///
/// The first session is opened inline in `connect`, so a transport fault there would otherwise
/// fail the whole client. Transport faults are the normal failure on a slow or proxied egress
/// path, so the attempt is repeated on a 1 s / 2 s / 4 s backoff. A venue *answer* (a rejected
/// listen key, for example) is never repeated: it would fail the same way every time.
const USER_STREAM_CONNECT_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// Delays before each attempt to resolve an ambiguous order submission.
///
/// An ambiguous submission (`-1006`, `-1007`, or a transport fault) leaves the order in flight:
/// it may be resting, filled, or absent. The order is queried after each delay until the venue
/// answers definitively. It is never resubmitted.
const AMBIGUOUS_SUBMIT_QUERY_DELAYS: [Duration; 3] = [
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(15),
];

/// Trade IDs retained per symbol for deduplicating REST compensation against stream fills.
const MAX_TRACKED_TRADE_IDS: usize = 4_096;

/// How far back compensation looks for fills on a symbol the stream never reported one for.
///
/// Anything older belongs to the engine's startup reconciliation, not to an outage this session
/// observed.
const COMPENSATION_FILL_LOOKBACK_MS: i64 = 60 * 60 * 1_000;

/// Window used by the report paths when the command names no start time.
///
/// Matches the venue's own default lookback on `allOrders` / `userTrades`, so an unbounded
/// request covers exactly what the venue would have returned unpaged.
const DEFAULT_REPORT_LOOKBACK_MS: i64 = ASTER_HISTORY_MAX_INTERVAL_MS;

/// Snapshot of the instruments this client can trade, indexed for both lookup directions.
#[derive(Debug, Default)]
struct InstrumentIndex {
    by_id: AHashMap<InstrumentId, InstrumentAny>,
    by_symbol: AHashMap<Ustr, InstrumentAny>,
}

impl InstrumentIndex {
    fn replace(&mut self, instruments: Vec<InstrumentAny>) {
        self.by_id.clear();
        self.by_symbol.clear();

        for instrument in instruments {
            self.by_symbol
                .insert(instrument.raw_symbol().inner(), instrument.clone());
            self.by_id.insert(instrument.id(), instrument);
        }
    }

    fn by_id(&self, instrument_id: &InstrumentId) -> Option<&InstrumentAny> {
        self.by_id.get(instrument_id)
    }

    fn by_symbol(&self, symbol: &Ustr) -> Option<&InstrumentAny> {
        self.by_symbol.get(symbol)
    }

    /// Replaces one instrument in place, keeping both indexes consistent.
    fn replace_one(&mut self, instrument: InstrumentAny) {
        self.by_symbol
            .insert(instrument.raw_symbol().inner(), instrument.clone());
        self.by_id.insert(instrument.id(), instrument);
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Returns the loaded instruments in a stable order for iteration outside the lock.
    fn snapshot(&self) -> Vec<InstrumentAny> {
        let mut instruments: Vec<InstrumentAny> = self.by_id.values().cloned().collect();
        instruments.sort_by_key(|instrument| instrument.id());
        instruments
    }
}

/// Makes an order report's timeline consistent with the fills reported for the same order.
///
/// The execution engine sorts every reconciliation event by `ts_event` before applying it
/// (`ExecutionManager::reconcile_execution_mass_status`), and the events it builds for an
/// external order take their timestamps straight from the reports: `OrderAccepted` from
/// `ts_accepted`, each `OrderFilled` from its fill's `ts_event`. If an order's `ts_accepted` is
/// later than one of its own fills, that fill sorts *before* the acceptance and is applied to an
/// order still in `Initialized`, which the order state machine rejects with
/// `InvalidStateTrigger: ... did not apply OrderFilled`.
///
/// Aster does not always give a usable creation time for a historical order: `time` can be
/// absent on `allOrders` rows, in which case the report falls back to `updateTime` and finally
/// to "now", which is later than every historical fill. Rather than trust the venue's clock,
/// the acceptance is pulled back to the earliest fill it must precede.
///
/// Returns whether the report was adjusted.
fn align_report_with_fills(report: &mut OrderStatusReport, fills: &[&FillReport]) -> bool {
    let Some(earliest_fill) = fills.iter().map(|fill| fill.ts_event).min() else {
        return false;
    };

    if report.ts_accepted <= earliest_fill {
        return false;
    }

    report.ts_accepted = earliest_fill;
    report.ts_last = report.ts_last.max(earliest_fill);
    true
}

/// Marks the session as live from `now`, so compensation cannot reach behind the connect.
///
/// Called once per successful `connect`. Everything older belongs to the execution engine's
/// startup reconciliation, which delivers it as reports against the orders it already knows.
fn mark_session_start(state: &Arc<RwLock<StreamState>>, now_ms: i64) {
    state.write().session_start_ms = now_ms;
}

/// Returns whether a failed user data stream connect is worth repeating.
///
/// Only transport faults are: a TLS handshake that never completed, a TCP connect failure, or
/// the per-attempt timeout expiring. Everything the venue actually answered — a rejected listen
/// key, an unfunded wallet, a bad signature — would fail identically on every repeat, and
/// repeating it only delays the error the operator needs to see.
#[must_use]
fn is_retryable_stream_connect_error(error: &AsterHttpError) -> bool {
    error.is_retryable_transport()
}

/// Returns a copy of `instrument` carrying the account's real commission rates.
///
/// Returns `None` for instrument types that have no fee fields, which Aster's USD-M
/// `exchangeInfo` does not produce but which the shared Binance parser can in principle return.
#[must_use]
fn with_commission_rates(
    instrument: &InstrumentAny,
    maker_fee: Decimal,
    taker_fee: Decimal,
) -> Option<InstrumentAny> {
    let mut updated = instrument.clone();

    match &mut updated {
        InstrumentAny::CryptoPerpetual(inner) => {
            inner.maker_fee = maker_fee;
            inner.taker_fee = taker_fee;
        }
        InstrumentAny::PerpetualContract(inner) => {
            inner.maker_fee = maker_fee;
            inner.taker_fee = taker_fee;
        }
        InstrumentAny::CryptoFuture(inner) => {
            inner.maker_fee = maker_fee;
            inner.taker_fee = taker_fee;
        }
        _ => return None,
    }

    Some(updated)
}

/// Precisions and identity resolved for one venue symbol.
#[derive(Debug, Clone, Copy)]
struct SymbolContext {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

/// The trades already applied for one symbol, with the newest trade time observed.
///
/// The set is bounded: compensation only needs to recognise trades the stream already
/// delivered around the outage, not the account's whole history.
#[derive(Debug, Default)]
struct AppliedTrades {
    ids: BTreeSet<i64>,
    last_ts_ms: i64,
}

impl AppliedTrades {
    fn record(&mut self, trade_id: i64, ts_ms: i64) -> bool {
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);

        if !self.ids.insert(trade_id) {
            return false;
        }

        while self.ids.len() > MAX_TRACKED_TRADE_IDS {
            let oldest = *self.ids.iter().next().expect("set is non-empty");
            self.ids.remove(&oldest);
        }

        true
    }

    fn contains(&self, trade_id: i64) -> bool {
        self.ids.contains(&trade_id)
    }
}

/// Cross-task view of what the private stream has already reported.
///
/// The execution client's cache is `Rc`-based and cannot cross into the spawned stream tasks,
/// so the two facts compensation needs — which orders this client believes are working, and
/// which fills it has already applied — are tracked here instead.
#[derive(Debug, Default)]
struct StreamState {
    /// Client order ID to venue symbol, for every order believed to be working at the venue.
    working_orders: AHashMap<Ustr, Ustr>,
    /// Applied trade IDs per venue symbol.
    applied_trades: AHashMap<Ustr, AppliedTrades>,
    /// Millisecond timestamp at which this client connected.
    ///
    /// Compensation never reaches behind it. Fills older than the connect belong to the
    /// engine's own startup reconciliation, and re-delivering them as live session events is
    /// what makes the engine reject an `OrderFilled` for an order it already holds as filled.
    session_start_ms: i64,
}

impl StreamState {
    fn track_working_order(&mut self, client_order_id: Ustr, symbol: Ustr) {
        self.working_orders.insert(client_order_id, symbol);
    }

    fn forget_working_order(&mut self, client_order_id: &Ustr) {
        self.working_orders.remove(client_order_id);
    }

    /// Records a fill, returning whether it had not been applied before.
    fn record_fill(&mut self, symbol: Ustr, trade_id: i64, ts_ms: i64) -> bool {
        self.applied_trades
            .entry(symbol)
            .or_default()
            .record(trade_id, ts_ms)
    }

    fn has_fill(&self, symbol: &Ustr, trade_id: i64) -> bool {
        self.applied_trades
            .get(symbol)
            .is_some_and(|applied| applied.contains(trade_id))
    }

    /// Returns the newest trade time seen for a symbol, if any.
    fn last_fill_ms(&self, symbol: &Ustr) -> Option<i64> {
        self.applied_trades
            .get(symbol)
            .map(|applied| applied.last_ts_ms)
            .filter(|ts| *ts > 0)
    }

    /// Returns the millisecond floor compensation may query from for `symbol`.
    ///
    /// Never earlier than the connect: an outage this session observed cannot have hidden a
    /// fill that happened before the session existed.
    fn compensation_start_ms(&self, symbol: &Ustr, default_start_ms: i64) -> i64 {
        self.last_fill_ms(symbol)
            .unwrap_or(default_start_ms)
            .max(self.session_start_ms)
    }
}

/// The `Send`-safe half of the execution client, shared with the private stream tasks.
///
/// Everything the background session needs — the signed client, the event emitter, the
/// instrument index and the stream state — lives here so it can be cloned into spawned tasks.
/// The execution client's `Rc`-based cache deliberately stays out.
#[derive(Debug, Clone)]
struct SessionContext {
    http_client: AsterHttpClient,
    emitter: ExecutionEventEmitter,
    account_id: AccountId,
    instruments: Arc<RwLock<InstrumentIndex>>,
    state: Arc<RwLock<StreamState>>,
    clock: &'static AtomicTime,
    treat_expired_as_canceled: bool,
}

impl SessionContext {
    fn context_for(&self, symbol: &Ustr) -> Option<SymbolContext> {
        let guard = self.instruments.read();
        AsterExecutionClient::context_for_symbol(&guard, symbol)
    }

    fn now_ms(&self) -> i64 {
        (self.clock.get_time_ns().as_u64() / 1_000_000) as i64
    }

    /// Converts a venue order into a report, or `Ok(None)` when its symbol is not loaded.
    ///
    /// An unloaded symbol is out of this client's scope (the account may trade instruments the
    /// configuration never asked for), which is different from a payload this client asked for
    /// and could not parse: that returns `Err`.
    fn order_to_report(
        &self,
        order: &AsterOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(context) = self.context_for(&order.symbol) else {
            return Ok(None);
        };

        order
            .to_order_status_report(
                self.account_id,
                context.instrument_id,
                context.price_precision,
                context.size_precision,
                self.treat_expired_as_canceled,
                ts_init,
            )
            .map(Some)
    }

    /// Records what a status report implies about the order's working state.
    fn track_order_state(&self, report: &OrderStatusReport, symbol: Ustr) {
        let Some(client_order_id) = report.client_order_id else {
            return;
        };

        let mut state = self.state.write();
        if report.order_status.is_closed() {
            state.forget_working_order(&client_order_id.inner());
        } else {
            state.track_working_order(client_order_id.inner(), symbol);
        }
    }

    /// Fetches every historical order for `symbol` in `[start_ms, end_ms]`.
    ///
    /// Aster caps `allOrders` at 1000 rows per call and refuses `orderId` together with a time
    /// window, so each 7-day slice is opened by a time-bounded request and then continued with
    /// the `orderId` cursor, filtering back to the slice client-side. Rows are deduplicated by
    /// order ID, and the cursor must strictly advance, so a page that repeats itself fails
    /// rather than looping.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or pagination cannot make progress. A
    /// truncated result is never returned as if it were complete.
    async fn query_all_orders_paged(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<AsterOrder>> {
        anyhow::ensure!(
            start_ms <= end_ms,
            "Aster order history start {start_ms} must not exceed end {end_ms}"
        );

        let mut orders: Vec<AsterOrder> = Vec::new();
        let mut seen: AHashSet<i64> = AHashSet::new();
        let mut window_start = start_ms;

        loop {
            let window_end = window_start
                .saturating_add(ASTER_HISTORY_MAX_INTERVAL_MS)
                .min(end_ms);
            let mut cursor: Option<i64> = None;

            loop {
                let page = if let Some(order_id) = cursor {
                    self.http_client
                        .query_all_orders(
                            symbol,
                            None,
                            None,
                            Some(order_id),
                            Some(ASTER_HISTORY_PAGE_LIMIT),
                        )
                        .await
                } else {
                    self.http_client
                        .query_all_orders(
                            symbol,
                            Some(window_start),
                            Some(window_end),
                            None,
                            Some(ASTER_HISTORY_PAGE_LIMIT),
                        )
                        .await
                }
                .map_err(|e| {
                    anyhow::anyhow!("Aster historical order query failed for {symbol}: {e}")
                })?;

                if page.is_empty() {
                    break;
                }

                let page_len = page.len();
                let max_order_id = page
                    .iter()
                    .map(|order| order.order_id)
                    .max()
                    .expect("non-empty");
                let passed_window_end = page
                    .iter()
                    .any(|order| order.time.is_some_and(|time| time > window_end));

                orders.extend(page.into_iter().filter(|order| {
                    order
                        .time
                        .is_none_or(|time| time >= window_start && time <= window_end)
                        && seen.insert(order.order_id)
                }));

                if page_len < ASTER_HISTORY_PAGE_LIMIT as usize || passed_window_end {
                    break;
                }

                let next_cursor = max_order_id
                    .checked_add(1)
                    .context("Aster order ID overflow during pagination")?;
                anyhow::ensure!(
                    cursor.is_none_or(|current| next_cursor > current),
                    "Aster allOrders pagination made no progress for {symbol}"
                );
                cursor = Some(next_cursor);
            }

            if window_end >= end_ms {
                break;
            }
            window_start = window_end.saturating_add(1);
        }

        Ok(orders)
    }

    /// Fetches every account trade for `symbol` in `[start_ms, end_ms]`.
    ///
    /// Mirrors [`Self::query_all_orders_paged`] with the `fromId` cursor `userTrades` uses.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or pagination cannot make progress.
    async fn query_user_trades_paged(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<AsterUserTrade>> {
        anyhow::ensure!(
            start_ms <= end_ms,
            "Aster trade history start {start_ms} must not exceed end {end_ms}"
        );

        let mut trades: Vec<AsterUserTrade> = Vec::new();
        let mut seen: AHashSet<i64> = AHashSet::new();
        let mut window_start = start_ms;

        loop {
            let window_end = window_start
                .saturating_add(ASTER_HISTORY_MAX_INTERVAL_MS)
                .min(end_ms);
            let mut cursor: Option<i64> = None;

            loop {
                let page = if let Some(from_id) = cursor {
                    self.http_client
                        .query_user_trades(
                            symbol,
                            None,
                            None,
                            Some(from_id),
                            Some(ASTER_HISTORY_PAGE_LIMIT),
                        )
                        .await
                } else {
                    self.http_client
                        .query_user_trades(
                            symbol,
                            Some(window_start),
                            Some(window_end),
                            None,
                            Some(ASTER_HISTORY_PAGE_LIMIT),
                        )
                        .await
                }
                .map_err(|e| anyhow::anyhow!("Aster user trades query failed for {symbol}: {e}"))?;

                if page.is_empty() {
                    break;
                }

                let page_len = page.len();
                let max_trade_id = page.iter().map(|trade| trade.id).max().expect("non-empty");
                let passed_window_end = page.iter().any(|trade| trade.time > window_end);

                trades.extend(page.into_iter().filter(|trade| {
                    trade.time >= window_start && trade.time <= window_end && seen.insert(trade.id)
                }));

                if page_len < ASTER_HISTORY_PAGE_LIMIT as usize || passed_window_end {
                    break;
                }

                let next_cursor = max_trade_id
                    .checked_add(1)
                    .context("Aster trade ID overflow during pagination")?;
                anyhow::ensure!(
                    cursor.is_none_or(|current| next_cursor > current),
                    "Aster userTrades pagination made no progress for {symbol}"
                );
                cursor = Some(next_cursor);
            }

            if window_end >= end_ms {
                break;
            }
            window_start = window_end.saturating_add(1);
        }

        Ok(trades)
    }

    /// Resolves an order submission whose outcome the venue never reported.
    ///
    /// The order stays in flight and is *never* resubmitted: the venue is asked what happened
    /// to `client_order_id` after each delay in [`AMBIGUOUS_SUBMIT_QUERY_DELAYS`]. A definitive
    /// "no such order" answer means the submission never landed, which is the only case that
    /// terminalises the order locally.
    async fn resolve_ambiguous_submit(&self, order: OrderAny, symbol: String, cause: String) {
        let client_order_id = order.client_order_id();

        for delay in AMBIGUOUS_SUBMIT_QUERY_DELAYS {
            tokio::time::sleep(delay).await;

            match self
                .http_client
                .query_order(&symbol, None, Some(client_order_id.as_str()))
                .await
            {
                Ok(venue_order) => {
                    match self.order_to_report(&venue_order, self.clock.get_time_ns()) {
                        Ok(Some(report)) => {
                            log::info!(
                                "Resolved ambiguous Aster submission for {client_order_id} as \
                                 {:?}",
                                report.order_status,
                            );
                            self.track_order_state(&report, venue_order.symbol);
                            self.emitter.send_order_status_report(report);
                        }
                        Ok(None) => log::error!(
                            "Aster reported {client_order_id} on unloaded symbol {}; the order \
                             state cannot be resolved",
                            venue_order.symbol,
                        ),
                        Err(e) => log::error!(
                            "Aster order {client_order_id} could not be parsed while resolving \
                             an ambiguous submission: {e}"
                        ),
                    }
                    return;
                }
                Err(e) if e.is_unknown_order() => {
                    log::warn!(
                        "Aster never received order {client_order_id} ({cause}); rejecting it \
                         locally"
                    );
                    self.emitter.emit_order_rejected(
                        &order,
                        &format!("submit-order-unknown-status: {cause}"),
                        self.clock.get_time_ns(),
                        false, // due_post_only
                    );
                    self.state
                        .write()
                        .forget_working_order(&client_order_id.inner());
                    return;
                }
                Err(e) => log::error!(
                    "Aster order query failed while resolving an ambiguous submission for \
                     {client_order_id}: {e}"
                ),
            }
        }

        log::error!(
            "Aster order {client_order_id} still has an unknown execution status after \
             {} query attempts ({cause}); it is left in flight and is not resubmitted",
            AMBIGUOUS_SUBMIT_QUERY_DELAYS.len(),
        );
    }

    /// Re-establishes the account baseline after a private stream outage.
    ///
    /// The stream carries no history, so every event between the drop and the reconnect is
    /// simply absent. This pass restores the four things that gap can invalidate: which orders
    /// are still working, which fills happened, the balances, and the positions. Each step is
    /// reported independently, so one failing endpoint does not suppress the others; failures
    /// are logged at error level because the local state stays stale until the next pass.
    async fn compensate(&self, reason: &str) {
        log::info!("Compensating Aster session state after {reason}");

        if let Err(e) = self.compensate_orders().await {
            log::error!("Aster order compensation after {reason} failed: {e}");
        }
        if let Err(e) = self.compensate_fills().await {
            log::error!("Aster fill compensation after {reason} failed: {e}");
        }
        if let Err(e) = self.refresh_account_state().await {
            log::error!("Aster balance refresh after {reason} failed: {e}");
        }
        if let Err(e) = self.refresh_positions().await {
            log::error!("Aster position refresh after {reason} failed: {e}");
        }
    }

    /// Reports the venue's open orders, then resolves every order this client still believes is
    /// working but the venue no longer lists (filled or cancelled during the outage).
    async fn compensate_orders(&self) -> anyhow::Result<()> {
        let open_orders = self
            .http_client
            .query_open_orders(None)
            .await
            .map_err(|e| anyhow::anyhow!("Aster open order query failed: {e}"))?;

        let ts_init = self.clock.get_time_ns();
        let mut still_open: AHashSet<Ustr> = AHashSet::new();

        for order in &open_orders {
            if !order.client_order_id.is_empty() {
                still_open.insert(Ustr::from(&order.client_order_id));
            }

            match self.order_to_report(order, ts_init) {
                Ok(Some(report)) => {
                    self.track_order_state(&report, order.symbol);
                    self.emitter.send_order_status_report(report);
                }
                Ok(None) => log::debug!(
                    "Ignoring Aster open order on unloaded symbol {}",
                    order.symbol
                ),
                Err(e) => log::error!("Failed to parse Aster open order {}: {e}", order.order_id),
            }
        }

        let vanished: Vec<(Ustr, Ustr)> = {
            let state = self.state.read();
            state
                .working_orders
                .iter()
                .filter(|(client_order_id, _)| !still_open.contains(*client_order_id))
                .map(|(client_order_id, symbol)| (*client_order_id, *symbol))
                .collect()
        };

        for (client_order_id, symbol) in vanished {
            match self
                .http_client
                .query_order(&symbol, None, Some(client_order_id.as_str()))
                .await
            {
                Ok(order) => match self.order_to_report(&order, self.clock.get_time_ns()) {
                    Ok(Some(report)) => {
                        self.track_order_state(&report, order.symbol);
                        self.emitter.send_order_status_report(report);
                    }
                    Ok(None) => log::debug!("Ignoring Aster order on unloaded symbol {symbol}"),
                    Err(e) => {
                        log::error!("Failed to parse Aster order {client_order_id}: {e}");
                    }
                },
                Err(e) if e.is_unknown_order() => {
                    log::warn!("Aster no longer knows order {client_order_id}; dropping it");
                    self.state.write().forget_working_order(&client_order_id);
                }
                Err(e) => log::error!("Aster order query failed for {client_order_id}: {e}"),
            }
        }

        Ok(())
    }

    /// Applies fills the stream missed, skipping any trade ID it already delivered.
    ///
    /// Missed fills are grouped by venue order so the order status can be sent with them, the
    /// same bundling the stream path uses: a bare fill would let the engine bootstrap a
    /// synthetic order at the fill quantity and reject the order's later events.
    async fn compensate_fills(&self) -> anyhow::Result<()> {
        let symbols: Vec<Ustr> = {
            let guard = self.instruments.read();
            guard.by_symbol.keys().copied().collect()
        };
        let now_ms = self.now_ms();
        let default_start = now_ms.saturating_sub(COMPENSATION_FILL_LOOKBACK_MS);

        for symbol in symbols {
            let start_ms = self
                .state
                .read()
                .compensation_start_ms(&symbol, default_start);

            if start_ms > now_ms {
                continue;
            }

            let trades = self
                .query_user_trades_paged(symbol.as_str(), start_ms, now_ms)
                .await?;

            let missed: Vec<&AsterUserTrade> = trades
                .iter()
                .filter(|trade| !self.state.read().has_fill(&symbol, trade.id))
                .collect();

            if missed.is_empty() {
                continue;
            }

            let mut by_order: AHashMap<i64, Vec<&AsterUserTrade>> = AHashMap::new();
            for trade in missed {
                by_order.entry(trade.order_id).or_default().push(trade);
            }

            for (venue_order_id, trades) in by_order {
                self.emit_missed_fills(&symbol, venue_order_id, &trades)
                    .await;
            }
        }

        Ok(())
    }

    async fn emit_missed_fills(
        &self,
        symbol: &Ustr,
        venue_order_id: i64,
        trades: &[&AsterUserTrade],
    ) {
        let Some(context) = self.context_for(symbol) else {
            log::debug!("Ignoring Aster fills on unloaded symbol {symbol}");
            return;
        };

        let ts_init = self.clock.get_time_ns();
        let mut fills = Vec::with_capacity(trades.len());

        for trade in trades {
            match trade.to_fill_report(
                self.account_id,
                context.instrument_id,
                context.price_precision,
                context.size_precision,
                AsterExecutionClient::settlement_currency(),
                ts_init,
            ) {
                Ok(report) => fills.push((report, trade.id, trade.time)),
                Err(e) => log::error!("Failed to parse Aster trade {} on {symbol}: {e}", trade.id),
            }
        }

        if fills.is_empty() {
            return;
        }

        let status = match self
            .http_client
            .query_order(symbol.as_str(), Some(venue_order_id), None)
            .await
        {
            Ok(order) => match self.order_to_report(&order, ts_init) {
                Ok(report) => {
                    if let Some(report) = report.as_ref() {
                        self.track_order_state(report, order.symbol);
                    }
                    report
                }
                Err(e) => {
                    log::error!("Failed to parse Aster order {venue_order_id}: {e}");
                    None
                }
            },
            Err(e) => {
                log::error!("Aster order query failed for {venue_order_id} on {symbol}: {e}");
                None
            }
        };

        {
            let mut state = self.state.write();
            for (_, trade_id, ts_ms) in &fills {
                state.record_fill(*symbol, *trade_id, *ts_ms);
            }
        }

        let reports: Vec<FillReport> = fills.into_iter().map(|(report, _, _)| report).collect();
        log::info!(
            "Applied {} Aster fills missed by the user stream for order {venue_order_id}",
            reports.len(),
        );

        match status {
            Some(status) => self.emitter.send_order_with_fills(status, reports),
            None => {
                for report in reports {
                    self.emitter.send_fill_report(report);
                }
            }
        }
    }

    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        self.emitter.emit_account_state(
            parse_account_balances(&balances),
            Vec::new(),
            true, // reported
            self.clock.get_time_ns(),
            None, // info
        );
        Ok(())
    }

    /// Reports every loaded instrument's position, including flat ones.
    ///
    /// A position closed during the outage must be reported as flat, otherwise the engine keeps
    /// the stale quantity.
    async fn refresh_positions(&self) -> anyhow::Result<()> {
        let positions = self
            .http_client
            .query_position_risk(None)
            .await
            .map_err(|e| anyhow::anyhow!("Aster position risk query failed: {e}"))?;

        let ts_now = self.clock.get_time_ns();

        for position in &positions {
            let Some(context) = self.context_for(&position.symbol) else {
                continue;
            };

            let signed_quantity = match position.signed_quantity() {
                Ok(value) => value,
                Err(e) => {
                    log::error!("Failed to parse Aster position {}: {e}", position.symbol);
                    continue;
                }
            };

            let side = if signed_quantity > Decimal::ZERO {
                PositionSide::Long
            } else if signed_quantity < Decimal::ZERO {
                PositionSide::Short
            } else {
                PositionSide::Flat
            };

            let quantity =
                match Quantity::from_decimal_dp(signed_quantity.abs(), context.size_precision) {
                    Ok(quantity) => quantity,
                    Err(e) => {
                        log::error!("Failed to parse Aster position {}: {e}", position.symbol);
                        continue;
                    }
                };

            self.emitter.send_position_report(PositionStatusReport::new(
                self.account_id,
                context.instrument_id,
                side,
                quantity,
                position.update_time.map_or(ts_now, millis_to_nanos),
                ts_now,
                Some(UUID4::new()),
                None, // venue_position_id: one-way mode
                parse_decimal(&position.entry_price, "entryPrice").ok(),
            ));
        }

        Ok(())
    }
}

/// Live execution client for the Aster DEX.
#[derive(Debug)]
pub struct AsterExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: AsterExecutionClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: AsterHttpClient,
    instrument_http_client: BinanceFuturesHttpClient,
    instruments: Arc<RwLock<InstrumentIndex>>,
    stream_state: Arc<RwLock<StreamState>>,
    session_tasks: TaskGroup,
    pending_tasks: TaskGroup,
    venue: Venue,
}

impl AsterExecutionClient {
    /// Creates a new [`AsterExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if credentials cannot be resolved or an HTTP client cannot be built.
    pub fn new(
        core: ExecutionClientCore,
        config: AsterExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;

        let credential = AsterCredential::resolve(
            config.signer_private_key.as_deref(),
            config.signer_address.as_deref(),
            config.user_address.as_deref(),
            config.environment,
        )
        .context("Aster execution client requires a signer private key")?;

        let venue = config.resolved_venue();
        let http_base = config.resolved_http_url();

        let http_client = AsterHttpClient::new(
            &http_base,
            Some(credential),
            config.http_timeout_secs,
            config.proxy_url.clone(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to build Aster HTTP client: {e}"))?;

        // Instruments come from Aster's Binance-compatible public `exchangeInfo`, which needs
        // no signing, so this client is deliberately credential-free.
        let instrument_http_client = BinanceFuturesHttpClient::new(
            BinanceProductType::UsdM,
            BinanceEnvironment::Live,
            get_atomic_clock_realtime(),
            None, // api_key
            None, // api_secret
            Some(http_base),
            None, // recv_window: not used by Aster V3
            config.http_timeout_secs,
            config.proxy_url.clone(),
            config.treat_expired_as_canceled,
        )
        .map_err(|e| anyhow::anyhow!("Failed to build Aster instrument client: {e}"))?
        .with_venue(venue);

        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None, // base_currency: multi-asset margin account
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            instrument_http_client,
            instruments: Arc::new(RwLock::new(InstrumentIndex::default())),
            stream_state: Arc::new(RwLock::new(StreamState::default())),
            session_tasks: TaskGroup::new(),
            pending_tasks: TaskGroup::new(),
            venue,
        })
    }

    /// Returns the `Send`-safe session view shared with the private stream tasks.
    ///
    /// Built on demand rather than stored, because the emitter only receives its event sender
    /// in [`ExecutionClient::start`] and a clone taken before that would drop every event.
    fn session(&self) -> SessionContext {
        SessionContext {
            http_client: self.http_client.clone(),
            emitter: self.emitter.clone(),
            account_id: self.core.account_id,
            instruments: self.instruments.clone(),
            state: self.stream_state.clone(),
            clock: self.clock,
            treat_expired_as_canceled: self.config.treat_expired_as_canceled,
        }
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &AsterExecutionClientConfig {
        &self.config
    }

    /// Returns a clone of the signed HTTP client.
    #[must_use]
    pub fn http_client(&self) -> AsterHttpClient {
        self.http_client.clone()
    }

    /// Returns the number of instruments loaded for this session.
    #[must_use]
    pub fn instrument_count(&self) -> usize {
        self.instruments.read().len()
    }

    /// Returns the settlement currency used for commissions and balances.
    fn settlement_currency() -> Currency {
        Currency::from(ASTER_SETTLEMENT_ASSET)
    }

    /// Resolves the venue symbol and precisions for an instrument.
    fn symbol_context(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<(String, SymbolContext)> {
        let guard = self.instruments.read();
        let instrument = guard.by_id(instrument_id).ok_or_else(|| {
            anyhow::anyhow!("Aster instrument {instrument_id} is not loaded; check `load_ids`")
        })?;

        Ok((
            format_binance_symbol(instrument_id),
            SymbolContext {
                instrument_id: *instrument_id,
                price_precision: instrument.price_precision(),
                size_precision: instrument.size_precision(),
            },
        ))
    }

    /// Resolves the instrument identity and precisions for a venue symbol.
    fn context_for_symbol(instruments: &InstrumentIndex, symbol: &Ustr) -> Option<SymbolContext> {
        instruments
            .by_symbol(symbol)
            .map(|instrument| SymbolContext {
                instrument_id: instrument.id(),
                price_precision: instrument.price_precision(),
                size_precision: instrument.size_precision(),
            })
    }

    /// Loads the tradable instrument set, retrying `exchangeInfo` on transport faults.
    ///
    /// `exchangeInfo` is served by the Binance USD-M client, which carries no retry of its own,
    /// and it is the first request of every connect, so a single proxy hiccup there would fail
    /// the whole session. Only transport faults are repeated; an Aster error body is returned
    /// unchanged, which matters most for the rate limit Aster applies to this endpoint.
    async fn load_instruments(&self) -> anyhow::Result<()> {
        let retry: RetryManager<BinanceFuturesHttpError> =
            RetryManager::new(instrument_load_retry_config());

        let instruments = retry
            .execute_with_retry(
                "exchangeInfo",
                || {
                    self.instrument_http_client
                        .request_instruments_with_config(&self.config.instrument_provider)
                },
                |e| {
                    matches!(
                        e,
                        BinanceFuturesHttpError::NetworkError(_)
                            | BinanceFuturesHttpError::Timeout(_)
                    )
                },
                |e| BinanceFuturesHttpError::NetworkError(format!("exchangeInfo retry: {e}")),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load Aster instruments: {e}"))?;

        anyhow::ensure!(
            !instruments.is_empty(),
            "Aster instrument load returned no instruments; check `instrument_provider.load_ids`"
        );

        let count = instruments.len();
        self.instruments.write().replace(instruments);
        self.core.set_instruments_initialized();

        self.warn_unlisted_load_ids();

        log::info!("Loaded {count} Aster instruments for execution");
        Ok(())
    }

    /// Replaces the venue's default fee fields with the account's real commission rates.
    ///
    /// `exchangeInfo` carries no commission data, so the Binance USD-M instrument parser fills
    /// `maker_fee` / `taker_fee` with its own VIP-0 defaults. Those defaults are Binance's, not
    /// Aster's, and they are not this account's rates either, so every strategy or probe that
    /// reads the cached instrument would be costing trades against a fabricated number.
    ///
    /// `GET /fapi/v3/commissionRate` is queried once per loaded instrument at connect, and the
    /// updated instruments are re-published on the data event channel so the data engine and
    /// cache carry the corrected values. A symbol whose query fails keeps the venue default and
    /// is named in a warning as **unverified**: the fee is then a placeholder, not a measurement.
    async fn refresh_commission_rates(&self) {
        let instruments = self.instruments.read().snapshot();
        let sender = try_get_data_event_sender();

        if sender.is_none() {
            log::warn!(
                "No data event sender is registered, so Aster commission rates cannot be \
                 published to the data engine; the execution client's own instrument index is \
                 still updated"
            );
        }

        let mut unverified: Vec<String> = Vec::new();
        let mut verified = 0usize;

        for instrument in instruments {
            let symbol = format_binance_symbol(&instrument.id());

            let rate = match self.http_client.query_commission_rate(&symbol).await {
                Ok(rate) => rate,
                Err(e) => {
                    unverified.push(format!("{symbol} ({e})"));
                    continue;
                }
            };

            let (maker, taker) = match (rate.maker_rate(), rate.taker_rate()) {
                (Ok(maker), Ok(taker)) => (maker, taker),
                (maker, taker) => {
                    let error = maker.err().or_else(|| taker.err()).expect("one error");
                    unverified.push(format!("{symbol} ({error})"));
                    continue;
                }
            };

            let Some(updated) = with_commission_rates(&instrument, maker, taker) else {
                unverified.push(format!("{symbol} (instrument type carries no fee fields)"));
                continue;
            };

            self.instruments.write().replace_one(updated.clone());
            verified += 1;

            if let Some(sender) = sender.as_ref()
                && let Err(e) = sender.send(DataEvent::Instrument(updated))
            {
                log::warn!("Failed to publish the Aster instrument for {symbol}: {e}");
            }
        }

        if verified > 0 {
            log::info!("Applied Aster account commission rates to {verified} instruments");
        }

        if !unverified.is_empty() {
            log::warn!(
                "Aster commission rates are UNVERIFIED for {} instruments, which keep the venue \
                 default fees and must not be treated as this account's costs: {}",
                unverified.len(),
                unverified.join(", "),
            );
        }
    }

    /// Warns about configured `load_ids` the venue did not list.
    ///
    /// Aster's testnet carries a smaller symbol set than mainnet (it has no `NVDAUSDT`, for
    /// example), and the instrument selector silently drops IDs that `exchangeInfo` never
    /// returns. Connecting still succeeds as long as *some* instrument loaded, but the missing
    /// IDs must be visible, otherwise the first order for one of them fails much later with an
    /// opaque "instrument is not loaded" error.
    fn warn_unlisted_load_ids(&self) {
        let Some(load_ids) = self.config.instrument_provider.load_ids.as_ref() else {
            return;
        };

        let guard = self.instruments.read();
        let missing: Vec<&str> = load_ids
            .iter()
            .filter(|raw| InstrumentId::from_str(raw).is_ok_and(|id| guard.by_id(&id).is_none()))
            .map(String::as_str)
            .collect();

        if !missing.is_empty() {
            log::warn!(
                "Aster does not list {} of the configured `load_ids`: {}; orders for them \
                 will be denied",
                missing.len(),
                missing.join(", "),
            );
        }
    }

    /// Fails loudly when the account runs in hedge (dual-side) mode.
    ///
    /// Every position and order path in this adapter assumes one-way mode; silently trading a
    /// hedge-mode account would mis-attribute positions.
    async fn assert_one_way_mode(&self) -> anyhow::Result<()> {
        match self.http_client.query_position_mode().await {
            Ok(mode) => {
                anyhow::ensure!(
                    !mode.dual_side_position,
                    "Aster account is in hedge (dual-side) position mode, which this adapter does \
                     not support; switch the account to one-way mode"
                );
                Ok(())
            }
            Err(e) if e.is_auth_failure() => Err(anyhow::anyhow!(
                "Aster rejected the first signed request: {e}"
            )),
            Err(e) => {
                // A venue that does not expose the endpoint must not block the session; the
                // position reports still reflect reality in one-way mode.
                log::warn!("Aster position mode query failed, assuming one-way mode: {e}");
                Ok(())
            }
        }
    }

    async fn fetch_account_state(
        &self,
    ) -> anyhow::Result<(Vec<AccountBalance>, Vec<MarginBalance>)> {
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        // Aster's `/fapi/v3/balance` reports wallet balances only; per-asset initial and
        // maintenance margin are not part of the payload, so no margin balances are emitted.
        Ok((parse_account_balances(&balances), Vec::new()))
    }

    async fn emit_account_state(&self) -> anyhow::Result<()> {
        let (balances, margins) = self.fetch_account_state().await?;

        if balances.is_empty() {
            log::warn!("Aster account reports no non-zero balances");
        }

        let ts_event = self.clock.get_time_ns();
        self.emitter
            .emit_account_state(balances, margins, true, ts_event, None);
        Ok(())
    }

    /// Reconciles open orders at connect so externally placed orders are known to the engine.
    async fn reconcile_open_orders(&self) -> anyhow::Result<()> {
        let session = self.session();
        let orders = match self.http_client.query_open_orders(None).await {
            Ok(orders) => orders,
            Err(e) => {
                log::warn!("Aster open order reconciliation failed: {e}");
                return Ok(());
            }
        };

        let ts_init = self.clock.get_time_ns();
        let mut reported = 0usize;

        for order in &orders {
            match session.order_to_report(order, ts_init) {
                Ok(Some(report)) => {
                    session.track_order_state(&report, order.symbol);
                    self.emitter.send_order_status_report(report);
                    reported += 1;
                }
                Ok(None) => log::debug!(
                    "Ignoring Aster open order on unloaded symbol {}",
                    order.symbol
                ),
                Err(e) => log::warn!("Skipping Aster open order {}: {e}", order.order_id),
            }
        }

        log::info!("Reconciled {reported} open Aster orders");
        Ok(())
    }

    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let future = async move {
            if let Err(e) = fut.await {
                log::warn!("Aster {description} failed: {e:?}");
            }
        };

        if let Err(e) = self.pending_tasks.spawn(future) {
            log::warn!("Skipping Aster {description} after shutdown began: {e}");
        }
    }

    fn spawner(&self) -> Option<TaskSpawner> {
        match self.pending_tasks.spawner() {
            Ok(spawner) => Some(spawner),
            Err(e) => {
                log::warn!("Aster execution client is shutting down: {e}");
                None
            }
        }
    }

    /// Opens the first user data stream session, awaiting the listen key and the socket.
    ///
    /// `connect` must not report the client as connected while the private stream is still
    /// coming up: orders submitted in that window would produce no events at all. The first
    /// session is therefore established inline, and only the *subsequent* sessions are left to
    /// the background retry loop. A listen key rejection surfaces here as a connect error; the
    /// timeout only guards a hang.
    ///
    /// # Errors
    ///
    /// Returns an error if the listen key cannot be obtained, the socket cannot connect, or
    /// neither happens within [`USER_STREAM_CONNECT_TIMEOUT`].
    async fn open_user_stream(&self) -> anyhow::Result<AsterUserStreamClient> {
        let timeout_secs = self
            .config
            .ws_connect_timeout_secs
            .unwrap_or(DEFAULT_WS_CONNECT_TIMEOUT_SECS);
        // One attempt is a signed `listenKey` POST followed by the socket handshake, each with
        // its own budget. The outer bound is their sum, so it only fires when something hangs
        // past both rather than pre-empting a request that is still within its own timeout.
        let attempt_timeout = Duration::from_secs(
            timeout_secs.saturating_add(self.config.http_timeout_secs.unwrap_or(60)),
        );

        let mut stream_client = AsterUserStreamClient::new(
            self.http_client.clone(),
            &self.config.resolved_ws_url(),
            nautilus_network::websocket::TransportBackend::default(),
            self.config.proxy_url.clone(),
            self.config.ws_heartbeat_secs,
        )
        .with_connect_timeout_secs(Some(timeout_secs));

        let mut attempt = 0usize;

        loop {
            let outcome = match tokio::time::timeout(attempt_timeout, stream_client.connect()).await
            {
                Ok(result) => result,
                Err(_) => Err(AsterHttpError::Timeout(format!(
                    "Aster user stream connect exceeded {}s",
                    attempt_timeout.as_secs(),
                ))),
            };

            let error = match outcome {
                Ok(()) => {
                    log::info!("Aster user data stream connected");
                    return Ok(stream_client);
                }
                Err(e) => e,
            };

            if !is_retryable_stream_connect_error(&error)
                || attempt >= USER_STREAM_CONNECT_RETRY_DELAYS.len()
            {
                return Err(anyhow::anyhow!(
                    "Aster user stream connect failed after {} attempt(s): {error}",
                    attempt + 1,
                ));
            }

            let delay = USER_STREAM_CONNECT_RETRY_DELAYS[attempt];
            log::warn!(
                "Aster user stream connect attempt {} failed ({error}); retrying in {}s",
                attempt + 1,
                delay.as_secs(),
            );
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    /// Runs the user data stream session loop over an already connected client.
    ///
    /// Every session after the first is preceded by a compensation pass, because the gap
    /// between the drop and the new listen key carries no events at all. The same pass runs on
    /// the `Reconnected` frame the shared Binance streams client raises when it re-establishes
    /// the socket underneath us without the session ending.
    fn start_user_stream(&self, mut stream_client: AsterUserStreamClient) -> anyhow::Result<()> {
        let session = self.session();

        self.session_tasks.spawn(async move {
            let mut is_first_session = true;

            loop {
                if !is_first_session {
                    if let Err(e) = stream_client.connect().await {
                        log::error!("Aster user stream connect failed: {e}");
                        tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                        continue;
                    }

                    log::info!("Aster user data stream reconnected with a new listen key");
                    session.compensate("a user data stream reconnect").await;
                }
                is_first_session = false;

                let Some(stream) = stream_client.stream() else {
                    log::error!("Aster user stream produced no message stream");
                    stream_client.close().await;
                    tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                    continue;
                };
                let mut stream = Box::pin(stream);
                let mut renewal =
                    tokio::time::interval(Duration::from_secs(ASTER_LISTEN_KEY_RENEWAL_SECS));
                renewal.tick().await; // The first tick completes immediately.

                loop {
                    tokio::select! {
                        _ = renewal.tick() => {
                            if let Err(e) = stream_client.keepalive().await {
                                log::warn!("Aster listen key renewal failed: {e}");
                            } else {
                                log::debug!("Aster listen key renewed");
                            }
                        }
                        message = stream.next() => {
                            let Some(message) = message else {
                                log::warn!("Aster user data stream ended; reconnecting");
                                break;
                            };

                            if matches!(message, BinanceFuturesWsStreamsMessage::ListenKeyExpired) {
                                log::warn!("Aster listen key expired; reconnecting with a new key");
                                break;
                            }

                            session.dispatch_user_stream_message(&message).await;
                        }
                    }
                }

                stream_client.close().await;
                tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
            }
        })?;

        Ok(())
    }

    async fn await_task_groups(&self) {
        self.session_tasks.begin_shutdown();
        self.pending_tasks.begin_shutdown();

        if let Err(e) = self
            .session_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
        {
            log::warn!("Failed to terminate Aster session tasks: {e}");
        }

        if let Err(e) = self
            .pending_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
        {
            log::warn!("Failed to terminate Aster pending tasks: {e}");
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Order request construction
// ------------------------------------------------------------------------------------------------

/// The Aster wire representation of a Nautilus order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsterOrderRequest {
    /// Venue symbol.
    pub symbol: String,
    /// `BUY` or `SELL`.
    pub side: &'static str,
    /// `LIMIT` or `MARKET`.
    pub order_type: &'static str,
    /// Order quantity formatted at the instrument's size precision.
    pub quantity: String,
    /// Limit price formatted at the instrument's price precision.
    pub price: Option<String>,
    /// `GTC`, `IOC`, `FOK`, or `GTX` for post-only.
    pub time_in_force: Option<&'static str>,
    /// Whether the order may only reduce an existing position.
    pub reduce_only: bool,
    /// Client order ID sent verbatim as `newClientOrderId`.
    pub client_order_id: String,
}

impl AsterOrderRequest {
    /// Converts this request into ordered request parameters.
    #[must_use]
    pub fn to_params(&self) -> AsterParams {
        AsterParams::new()
            .with("symbol", &self.symbol)
            .with("side", self.side)
            .with("type", self.order_type)
            .with("quantity", &self.quantity)
            .with_opt("price", self.price.as_deref())
            .with_opt("timeInForce", self.time_in_force)
            .with_opt("reduceOnly", self.reduce_only.then_some("true"))
            .with("newClientOrderId", &self.client_order_id)
    }
}

/// Maximum length Aster accepts for `newClientOrderId`.
const MAX_CLIENT_ORDER_ID_LEN: usize = 36;

/// Returns whether `value` satisfies Aster's `^[\.A-Z\:/a-z0-9_-]{1,36}$` client-ID rule.
#[must_use]
pub fn is_valid_client_order_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CLIENT_ORDER_ID_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b':' | b'/' | b'_' | b'-'))
}

/// Builds the Aster wire request for a Nautilus order.
///
/// # Errors
///
/// Returns an error if the order type, time in force, or client order ID is not supported by
/// Aster's V3 order endpoint.
pub fn build_order_request(order: &OrderAny, symbol: String) -> anyhow::Result<AsterOrderRequest> {
    // Aster's `quantity` is always denominated in the base asset. An order carrying a quote
    // amount would be sent as that many *contracts*: `quote_quantity = true, quantity = 100` on
    // BTCUSDT means "buy 100 USDT of BTC" but would leave as "buy 100 BTC". The risk engine
    // only computes an effective quantity for its own checks and leaves the order untouched, so
    // there is no upstream conversion to rely on.
    anyhow::ensure!(
        !order.is_quote_quantity(),
        "Aster order quantities are denominated in the base asset; quote-denominated \
         quantities (`quote_quantity`) are not supported"
    );

    let client_order_id = order.client_order_id().to_string();
    anyhow::ensure!(
        is_valid_client_order_id(&client_order_id),
        "Client order ID '{client_order_id}' is not accepted by Aster: it must match \
         ^[.A-Z:/a-z0-9_-]{{1,36}}$"
    );

    let side = match order.order_side() {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };

    let quantity = order.quantity().to_string();

    match order.order_type() {
        OrderType::Market => Ok(AsterOrderRequest {
            symbol,
            side,
            order_type: "MARKET",
            quantity,
            price: None,
            time_in_force: None,
            reduce_only: order.is_reduce_only(),
            client_order_id,
        }),
        OrderType::Limit => {
            let price = order
                .price()
                .ok_or_else(|| anyhow::anyhow!("LIMIT order requires a price"))?;

            let time_in_force = if order.is_post_only() {
                // Aster spells post-only as the GTX time in force, like Binance USD-M.
                "GTX"
            } else {
                match order.time_in_force() {
                    TimeInForce::Gtc => "GTC",
                    TimeInForce::Ioc => "IOC",
                    TimeInForce::Fok => "FOK",
                    other => anyhow::bail!(
                        "Aster supports GTC, IOC, and FOK for LIMIT orders, received {other:?}"
                    ),
                }
            };

            Ok(AsterOrderRequest {
                symbol,
                side,
                order_type: "LIMIT",
                quantity,
                price: Some(price.to_string()),
                time_in_force: Some(time_in_force),
                reduce_only: order.is_reduce_only(),
                client_order_id,
            })
        }
        other => anyhow::bail!(
            "Aster execution supports LIMIT and MARKET orders only, received {other:?}"
        ),
    }
}

// ------------------------------------------------------------------------------------------------
// Account state
// ------------------------------------------------------------------------------------------------

/// Retry policy for the instrument load, mirroring the signed client's `GET` policy:
/// three repeats with a 500 ms / 1 s / 2 s backoff.
fn instrument_load_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 3,
        initial_delay_ms: 500,
        max_delay_ms: 2_000,
        backoff_factor: 2.0,
        jitter_ms: 0,
        operation_timeout_ms: None,
        immediate_first: false,
        max_elapsed_ms: None,
    }
}

/// Warns once per process about Aster reporting `availableBalance` above `walletBalance`.
static FREE_ABOVE_TOTAL_WARNED: std::sync::Once = std::sync::Once::new();

/// Converts venue balance rows into Nautilus account balances.
///
/// **Every row the venue reports is kept, including explicit zeros.** Account state is applied
/// per currency ([`nautilus_model::accounts::base::BaseAccount::update_balances`] inserts by
/// currency), so an asset omitted from the snapshot keeps whatever the cache already holds.
/// Dropping a zero row would therefore leave a withdrawn asset showing its old amount forever;
/// the zero row is exactly what clears it. Only rows the venue omits entirely are absent, which
/// is the venue's statement, not this adapter's.
///
/// Asset codes are resolved through [`resolve_currency`], which registers unknown venue codes
/// instead of panicking (Aster's testnet lists `ASTER` and `AFEE`, neither of which the Nautilus
/// currency map knows).
///
/// Aster reports `availableBalance` above `walletBalance` on cross-margin accounts, because
/// availability there includes headroom from other assets. Nautilus requires
/// `total == locked + free`, so [`AccountBalance::from_total_and_free`] clamps `free` into
/// `[0, total]`, which yields `locked = 0` and `free = total` for those rows. The venue value
/// is deliberately *not* used as the total: doing so would inflate reported equity by margin
/// that is not this asset's. The first such row per process is logged at warning level.
fn parse_account_balances(balances: &[AsterBalance]) -> Vec<AccountBalance> {
    let mut account_balances = Vec::with_capacity(balances.len());

    for balance in balances {
        let currency = resolve_currency(balance.asset.as_str());

        let (Ok(total), Ok(free)) = (balance.total(), balance.free()) else {
            log::warn!("Skipping Aster balance for {currency}: unparsable amounts");
            continue;
        };

        if free > total {
            FREE_ABOVE_TOTAL_WARNED.call_once(|| {
                log::warn!(
                    "Aster reports availableBalance {free} above walletBalance {total} \
                     for {currency}; free is clamped to keep total = locked + free"
                );
            });
        }

        match AccountBalance::from_total_and_free(total, free, currency) {
            Ok(account_balance) => account_balances.push(account_balance),
            Err(e) => log::warn!("Skipping Aster balance for {currency}: {e}"),
        }
    }

    account_balances
}

// ------------------------------------------------------------------------------------------------
// User data stream dispatch
// ------------------------------------------------------------------------------------------------

/// Converts an Aster `ACCOUNT_UPDATE` payload into a Nautilus account state.
///
/// The shared Binance parser drops balance rows whose wallet balance is zero. On Aster that
/// silently defeats the only mechanism the venue has for reporting a drained asset: account
/// updates are applied per currency, so the dropped zero leaves the previous amount cached.
/// The `B` array is therefore parsed here instead of changing the Binance parser, which other
/// venues depend on.
///
/// Returns `None` when the payload carries no balance rows at all, which is nothing to report.
#[must_use]
fn parse_aster_account_update(
    msg: &BinanceFuturesAccountUpdateMsg,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> Option<AccountState> {
    let balances: Vec<AccountBalance> = msg
        .account
        .balances
        .iter()
        .filter_map(|balance| {
            let currency = resolve_currency(balance.asset.as_str());
            match AccountBalance::from_total_and_free(
                balance.wallet_balance,
                balance.cross_wallet_balance,
                currency,
            ) {
                Ok(account_balance) => Some(account_balance),
                Err(e) => {
                    log::warn!("Skipping Aster stream balance for {currency}: {e}");
                    None
                }
            }
        })
        .collect();

    if balances.is_empty() {
        return None;
    }

    let ts_event = if msg.event_time > 0 {
        millis_to_nanos(msg.event_time)
    } else {
        ts_init
    };

    Some(AccountState::new(
        account_id,
        AccountType::Margin,
        balances,
        Vec::new(), // margins: reported separately
        true,       // is_reported
        UUID4::new(),
        ts_event,
        ts_init,
        None, // base_currency: multi-asset margin account
    ))
}

impl SessionContext {
    /// Turns one decoded user data stream message into Nautilus reports and emits them.
    ///
    /// Unknown or irrelevant message types are logged at debug level and dropped; Aster
    /// multiplexes venue-specific announcement events onto the same stream.
    async fn dispatch_user_stream_message(&self, message: &BinanceFuturesWsStreamsMessage) {
        let account_id = self.account_id;
        let ts_init = self.clock.get_time_ns();

        match message {
            BinanceFuturesWsStreamsMessage::OrderUpdate(msg) => {
                let symbol = msg.order.symbol;
                let Some(context) = self.context_for(&symbol) else {
                    log::debug!("Ignoring Aster order update for unloaded symbol {symbol}");
                    return;
                };

                let status = match parse_futures_order_update_to_order_status(
                    msg,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    account_id,
                    self.treat_expired_as_canceled,
                    ts_init,
                ) {
                    Ok(report) => Some(report),
                    Err(e) => {
                        log::error!("Failed to parse Aster order status report: {e}");
                        None
                    }
                };

                if let Some(report) = status.as_ref() {
                    self.track_order_state(report, symbol);
                }

                // `lastFilledQty` is non-zero only on the TRADE execution type.
                let has_fill = parse_decimal(&msg.order.last_filled_qty, "lastFilledQty")
                    .is_ok_and(|qty| !qty.is_zero());

                let fill = if has_fill {
                    match parse_futures_order_update_to_fill(
                        msg,
                        account_id,
                        context.instrument_id,
                        context.price_precision,
                        context.size_precision,
                        None, // taker_fee: Aster reports the commission on the event
                        Some(AsterExecutionClient::settlement_currency()),
                        AsterExecutionClient::settlement_currency(),
                        None, // venue_position_id: one-way mode
                        ts_init,
                    ) {
                        Ok(report) => Some(report),
                        Err(e) => {
                            log::error!("Failed to parse Aster fill report: {e}");
                            None
                        }
                    }
                } else {
                    None
                };

                if fill.is_some() {
                    // Recorded before emitting so a compensation pass racing this dispatch
                    // cannot re-apply the same trade from `userTrades`.
                    self.state.write().record_fill(
                        symbol,
                        msg.order.trade_id,
                        msg.order.trade_time,
                    );
                }

                // Sending a fill on its own would let the engine bootstrap a synthetic order at
                // the fill quantity, which then closes early and rejects later fills for the
                // same venue order, so both reports are bundled whenever both parsed.
                match (status, fill) {
                    (Some(status), Some(fill)) => {
                        self.emitter.send_order_with_fills(status, vec![fill]);
                    }
                    (Some(status), None) => self.emitter.send_order_status_report(status),
                    (None, Some(fill)) => self.emitter.send_fill_report(fill),
                    (None, None) => {}
                }
            }
            BinanceFuturesWsStreamsMessage::AccountUpdate(msg) => {
                if let Some(state) = parse_aster_account_update(msg, account_id, ts_init) {
                    self.emitter.send_account_state(state);
                }
            }
            BinanceFuturesWsStreamsMessage::MarginCall(msg) => {
                log::warn!(
                    "Aster margin call: cross_wallet_balance={}, positions_at_risk={}",
                    msg.cross_wallet_balance,
                    msg.positions.len()
                );
            }
            BinanceFuturesWsStreamsMessage::Reconnected => {
                // The shared streams client re-established the socket underneath this session,
                // so the listen key is unchanged but the gap carries no events.
                log::warn!("Aster user data stream reconnected; compensating for the gap");
                self.compensate("a socket-level reconnect").await;
            }
            BinanceFuturesWsStreamsMessage::Error(msg) => {
                log::error!("Aster user data stream error: {msg:?}");
            }
            other => {
                log::debug!("Ignoring Aster user stream message: {other:?}");
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------
// ExecutionClient
// ------------------------------------------------------------------------------------------------

#[async_trait(?Send)]
impl ExecutionClient for AsterExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.venue
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
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();

        log::info!(
            "Started Aster execution client: client_id={}, account_id={}, venue={}, environment={}",
            self.core.client_id,
            self.core.account_id,
            self.venue,
            self.config.environment,
        );

        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        log::info!("Stopping Aster execution client");

        self.session_tasks.abort();
        self.pending_tasks.abort();
        self.core.set_stopped();
        self.core.set_disconnected();

        log::info!("Aster execution client stopped");
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        self.load_instruments().await?;
        self.assert_one_way_mode().await?;
        self.refresh_commission_rates().await;
        self.emit_account_state().await?;

        // Everything older than this instant is the execution engine's startup reconciliation
        // to deliver; compensation must never replay it as a live session event.
        mark_session_start(
            &self.stream_state,
            (self.clock.get_time_ns().as_u64() / 1_000_000) as i64,
        );

        // The private stream must be live before the client reports as connected, otherwise an
        // order submitted in the gap produces no events at all.
        let stream_client = self.open_user_stream().await?;
        self.start_user_stream(stream_client)?;
        self.reconcile_open_orders().await?;

        self.core.set_connected();
        log::info!("Aster execution client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        log::info!("Disconnecting Aster execution client");
        self.await_task_groups().await;
        self.core.set_disconnected();
        log::info!("Aster execution client disconnected");
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let (symbol, _) = match self.symbol_context(&order.instrument_id()) {
            Ok(resolved) => resolved,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        // Validate before emitting OrderSubmitted: Initialized -> Denied is a legal transition
        // but Submitted -> Denied is not.
        let request = match build_order_request(&order, symbol.clone()) {
            Ok(request) => request,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        let Some(spawner) = self.spawner() else {
            self.emitter
                .emit_order_denied(&order, "Aster execution client is shutting down");
            return Ok(());
        };

        let session = self.session();
        let http_client = self.http_client.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();

        self.stream_state
            .write()
            .track_working_order(client_order_id.inner(), Ustr::from(&symbol));

        self.emitter.emit_order_submitted(&order);

        spawner.spawn(async move {
            match http_client.submit_order(request.to_params()).await {
                Ok(response) => {
                    log::debug!(
                        "Aster order accepted: client_order_id={client_order_id}, venue_order_id={}",
                        response.order_id,
                    );
                }
                Err(e) if e.is_ambiguous_execution() => {
                    // The venue either never answered, or answered that it does not know what
                    // happened (`-1006` / `-1007`). The order may be resting or filled, so it
                    // must not be terminalised here and must never be resubmitted; the venue is
                    // asked what became of it instead.
                    log::error!(
                        "Aster left the execution status of {client_order_id} unknown ({e}); \
                         the order stays in flight pending reconciliation"
                    );
                    session
                        .resolve_ambiguous_submit(order, symbol, e.to_string())
                        .await;
                }
                Err(e) if e.is_venue_rejection() => {
                    let due_post_only = e.is_post_only_violation();
                    log::warn!("Aster rejected order {client_order_id}: {e}");
                    session.emitter.emit_order_rejected(
                        &order,
                        &format!("submit-order-error: {e}"),
                        clock.get_time_ns(),
                        due_post_only,
                    );
                    session
                        .state
                        .write()
                        .forget_working_order(&client_order_id.inner());
                }
                Err(e) => {
                    // A local failure (missing credentials, signing, validation) never reached
                    // the venue, so the order cannot be resting.
                    log::error!("Aster could not send order {client_order_id}: {e}");
                    session.emitter.emit_order_rejected(
                        &order,
                        &format!("submit-order-error: {e}"),
                        clock.get_time_ns(),
                        false, // due_post_only
                    );
                    session
                        .state
                        .write()
                        .forget_working_order(&client_order_id.inner());
                }
            }
        })?;

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        log::warn!(
            "Aster execution does not support order modification; cancel and resubmit \
             (client_order_id={})",
            cmd.client_order_id,
        );

        if let Ok(order) = self.core.cache().try_order_owned(&cmd.client_order_id) {
            self.emitter.emit_order_modify_rejected(
                &order,
                cmd.venue_order_id,
                "Aster does not support order modification",
                self.clock.get_time_ns(),
            );
        }

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        // Cancelling by client order ID avoids a race with orders whose venue ID has not yet
        // been observed on the user stream.
        let venue_order_id_i64 = venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());
        let use_client_id = venue_order_id_i64.is_none();

        spawner.spawn(async move {
            let result = if use_client_id {
                http_client
                    .cancel_order(&symbol, None, Some(client_order_id.as_str()))
                    .await
            } else {
                http_client
                    .cancel_order(&symbol, venue_order_id_i64, None)
                    .await
            };

            match result {
                Ok(response) => log::debug!(
                    "Aster cancel accepted: client_order_id={client_order_id}, status={:?}",
                    response.status,
                ),
                Err(e) if e.is_unknown_order() => {
                    log::warn!("Aster reported order {client_order_id} as unknown on cancel: {e}");
                }
                Err(e) => log::error!("Aster cancel failed for {client_order_id}: {e}"),
            }
        })?;

        Ok(())
    }

    /// Cancels the open orders for an instrument, honouring the command's side filter.
    ///
    /// Aster's `DELETE /fapi/v3/allOpenOrders` takes no side parameter, so it is only used for a
    /// side-less command. A side-filtered command instead lists the venue's open orders and
    /// cancels the matching ones individually: cancelling the whole symbol would take out the
    /// opposite side's resting orders, which is a different command from the one the strategy
    /// issued (a `SELL` exit would disappear when only the `BUY` entries were meant to).
    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;
        let http_client = self.http_client.clone();

        let Some(order_side) = cmd.order_side else {
            self.spawn_task("cancel_all_orders", async move {
                match http_client.cancel_all_orders(&symbol).await {
                    Ok(response) if response.is_success() => {
                        log::debug!("Aster cancelled all open orders for {symbol}");
                        Ok(())
                    }
                    Ok(response) => {
                        anyhow::bail!(
                            "Aster cancel-all returned {}: {}",
                            response.code,
                            response.msg
                        )
                    }
                    Err(e) => anyhow::bail!("Aster cancel-all failed for {symbol}: {e}"),
                }
            });

            return Ok(());
        };

        self.spawn_task("cancel_all_orders_for_side", async move {
            let open_orders = http_client
                .query_open_orders(Some(&symbol))
                .await
                .map_err(|e| anyhow::anyhow!("Aster open order query failed for {symbol}: {e}"))?;

            let targets: Vec<i64> = open_orders
                .iter()
                .filter(|order| parse_order_side(order.side) == order_side)
                .map(|order| order.order_id)
                .collect();

            if targets.is_empty() {
                log::debug!("No open {order_side:?} orders to cancel for {symbol}");
                return Ok(());
            }

            let mut failed = Vec::new();
            for venue_order_id in &targets {
                match http_client
                    .cancel_order(&symbol, Some(*venue_order_id), None)
                    .await
                {
                    Ok(_) => {}
                    Err(e) if e.is_unknown_order() => {
                        log::debug!(
                            "Aster order {venue_order_id} on {symbol} was already gone: {e}"
                        );
                    }
                    Err(e) => failed.push(format!("{venue_order_id} ({e})")),
                }
            }

            anyhow::ensure!(
                failed.is_empty(),
                "Aster failed to cancel {} of {} {order_side:?} orders for {symbol}: {}",
                failed.len(),
                targets.len(),
                failed.join(", "),
            );

            log::debug!(
                "Aster cancelled {} open {order_side:?} orders for {symbol}",
                targets.len(),
            );
            Ok(())
        });

        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let balances = http_client
                .query_balances()
                .await
                .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

            let account_balances = parse_account_balances(&balances);

            emitter.emit_account_state(
                account_balances,
                Vec::new(),
                true,
                clock.get_time_ns(),
                None,
            );
            Ok(())
        });

        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let (symbol, context) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let clock = self.clock;
        let treat_expired_as_canceled = self.config.treat_expired_as_canceled;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());

        spawner.spawn(async move {
            let result = if venue_order_id.is_some() {
                http_client.query_order(&symbol, venue_order_id, None).await
            } else {
                http_client
                    .query_order(&symbol, None, Some(client_order_id.as_str()))
                    .await
            };

            match result {
                Ok(order) => match order.to_order_status_report(
                    account_id,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    treat_expired_as_canceled,
                    clock.get_time_ns(),
                ) {
                    Ok(report) => emitter.send_order_status_report(report),
                    Err(e) => log::error!("Failed to build Aster order status report: {e}"),
                },
                Err(e) => log::warn!("Aster order query failed for {client_order_id}: {e}"),
            }
        })?;

        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument_id = cmd.instrument_id.ok_or_else(|| {
            anyhow::anyhow!("Aster order status report requires an instrument ID")
        })?;
        let (symbol, context) = self.symbol_context(&instrument_id)?;

        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());
        let client_order_id = cmd.client_order_id.map(|id| id.to_string());

        anyhow::ensure!(
            venue_order_id.is_some() || client_order_id.is_some(),
            "Aster order status report requires a venue or client order ID"
        );

        let result = if venue_order_id.is_some() {
            self.http_client
                .query_order(&symbol, venue_order_id, None)
                .await
        } else {
            self.http_client
                .query_order(&symbol, None, client_order_id.as_deref())
                .await
        };

        match result {
            Ok(order) => Ok(Some(order.to_order_status_report(
                self.core.account_id,
                context.instrument_id,
                context.price_precision,
                context.size_precision,
                self.config.treat_expired_as_canceled,
                self.clock.get_time_ns(),
            )?)),
            Err(e) if e.is_unknown_order() => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Aster order status query failed: {e}")),
        }
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let session = self.session();
        let ts_init = self.clock.get_time_ns();
        let now_ms = session.now_ms();
        // The end of the window is pinned up front so pagination cannot chase orders placed
        // while it runs and never terminate.
        let start_ms = cmd.start.map_or(now_ms - DEFAULT_REPORT_LOOKBACK_MS, |ts| {
            (ts.as_u64() / 1_000_000) as i64
        });
        let end_ms = cmd
            .end
            .map_or(now_ms, |ts| (ts.as_u64() / 1_000_000) as i64);

        let orders = if cmd.open_only {
            let symbol = cmd
                .instrument_id
                .map(|id| self.symbol_context(&id).map(|(symbol, _)| symbol))
                .transpose()?;

            self.http_client
                .query_open_orders(symbol.as_deref())
                .await
                .map_err(|e| anyhow::anyhow!("Aster open orders query failed: {e}"))?
        } else {
            // Aster requires a symbol for the historical order endpoint, so the request is
            // fanned out across loaded instruments when none is given.
            let symbols: Vec<String> = match cmd.instrument_id {
                Some(instrument_id) => vec![self.symbol_context(&instrument_id)?.0],
                None => {
                    let guard = self.instruments.read();
                    guard.by_id.keys().map(format_binance_symbol).collect()
                }
            };

            let mut orders = Vec::new();
            for symbol in symbols {
                // A failed page is not "no orders": returning a partial history as a complete
                // one is what lets the engine infer fills that never happened.
                let mut batch = session
                    .query_all_orders_paged(&symbol, start_ms, end_ms)
                    .await?;
                orders.append(&mut batch);
            }
            orders
        };

        let mut reports = Vec::with_capacity(orders.len());
        for order in &orders {
            match session.order_to_report(order, ts_init)? {
                Some(report) => reports.push(report),
                None => log::debug!(
                    "Ignoring Aster order {} on unloaded symbol {}",
                    order.order_id,
                    order.symbol,
                ),
            }
        }

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let session = self.session();
        let ts_init = self.clock.get_time_ns();
        let now_ms = session.now_ms();
        let start_ms = cmd.start.map_or(now_ms - DEFAULT_REPORT_LOOKBACK_MS, |ts| {
            (ts.as_u64() / 1_000_000) as i64
        });
        let end_ms = cmd
            .end
            .map_or(now_ms, |ts| (ts.as_u64() / 1_000_000) as i64);

        // Aster requires a symbol on `userTrades`, so the request is fanned out across loaded
        // instruments when the command does not name one.
        let targets: Vec<(String, SymbolContext)> = match cmd.instrument_id {
            Some(instrument_id) => vec![self.symbol_context(&instrument_id)?],
            None => {
                let guard = self.instruments.read();
                guard
                    .by_id
                    .keys()
                    .filter_map(|id| {
                        Self::context_for_symbol(&guard, &Ustr::from(&format_binance_symbol(id)))
                            .map(|context| (format_binance_symbol(id), context))
                    })
                    .collect()
            }
        };

        let mut reports = Vec::new();

        for (symbol, context) in targets {
            let trades = session
                .query_user_trades_paged(&symbol, start_ms, end_ms)
                .await?;

            for trade in &trades {
                // A fill whose price, quantity or commission cannot be parsed is a hole in the
                // history, not a fill to skip: the caller must not treat the result as complete.
                let report = trade
                    .to_fill_report(
                        self.core.account_id,
                        context.instrument_id,
                        context.price_precision,
                        context.size_precision,
                        Self::settlement_currency(),
                        ts_init,
                    )
                    .with_context(|| {
                        format!("Failed to parse Aster trade {} on {symbol}", trade.id)
                    })?;
                reports.push(report);

                // Reported to the engine's reconciliation, so a later compensation pass must
                // not deliver the same trade again as a live fill.
                self.stream_state
                    .write()
                    .record_fill(Ustr::from(&symbol), trade.id, trade.time);
            }
        }

        Ok(reports)
    }

    /// Composes the startup reconciliation snapshot.
    ///
    /// Overridden rather than inherited for two reasons the default composition cannot know
    /// about:
    ///
    /// 1. **The window is bounded.** Aster's history endpoints only answer for a time range, so
    ///    this snapshot is complete *from `start`*, not from the account's first trade. The
    ///    window is declared with [`ExecutionMassStatus::set_report_window`]; without it the
    ///    engine treats the history as complete and may synthesise position-opening fills to
    ///    explain a position whose opening trade simply predates the lookback.
    /// 2. **Order and fill timelines must agree.** The engine sorts every reconciliation event
    ///    by `ts_event`, so a fill timestamped before its own order's `ts_accepted` is applied
    ///    to an order still in `Initialized` and rejected. Each report is therefore aligned
    ///    against its own fills (see [`align_report_with_fills`]).
    ///
    /// Fills whose order is not in the report set are dropped: a one-way-mode fill carries no
    /// venue position ID, which is what the engine's orphan-fill path requires, so reporting it
    /// could only add an unreconcilable event.
    ///
    /// The three sources are requested one after another rather than concurrently, because each
    /// already fans out across instruments and pages, and Aster rate-limits aggressively.
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.clock.get_time_ns();
        let now_ms = (ts_init.as_u64() / 1_000_000) as i64;
        let start_ms = match lookback_mins {
            Some(mins) => now_ms.saturating_sub(
                i64::try_from(mins)
                    .ok()
                    .and_then(|mins| mins.checked_mul(60_000))
                    .ok_or_else(|| anyhow::anyhow!("lookback minutes overflow: {mins}"))?,
            ),
            None => now_ms - DEFAULT_REPORT_LOOKBACK_MS,
        };
        let start = UnixNanos::from(start_ms.max(0) as u64 * 1_000_000);

        // All three sources share one window, so an order and its fills cannot straddle the edge.
        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_init)
            .open_only(false)
            .start(Some(start))
            .end(Some(ts_init))
            .build()
            .context("failed to build Aster order status reports command")?;
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_init)
            .start(Some(start))
            .end(Some(ts_init))
            .build()
            .context("failed to build Aster fill reports command")?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_init)
            .start(Some(start))
            .end(Some(ts_init))
            .build()
            .context("failed to build Aster position status reports command")?;

        let mut order_reports = self.generate_order_status_reports(&order_cmd).await?;
        let fill_reports = self.generate_fill_reports(fill_cmd).await?;
        let position_reports = self.generate_position_status_reports(&position_cmd).await?;

        let reported_orders: AHashSet<Ustr> = order_reports
            .iter()
            .map(|report| report.venue_order_id.inner())
            .collect();

        let (matched_fills, orphan_fills): (Vec<FillReport>, Vec<FillReport>) = fill_reports
            .into_iter()
            .partition(|fill| reported_orders.contains(&fill.venue_order_id.inner()));

        if !orphan_fills.is_empty() {
            log::warn!(
                "Dropping {} Aster fill(s) whose order is outside the reported window; a \
                 one-way-mode fill carries no venue position ID, so the engine cannot \
                 reconcile it on its own",
                orphan_fills.len(),
            );
        }

        let mut fills_by_order: AHashMap<Ustr, Vec<&FillReport>> = AHashMap::new();
        for fill in &matched_fills {
            fills_by_order
                .entry(fill.venue_order_id.inner())
                .or_default()
                .push(fill);
        }

        let mut realigned = 0usize;
        for report in &mut order_reports {
            if let Some(fills) = fills_by_order.get(&report.venue_order_id.inner())
                && align_report_with_fills(report, fills)
            {
                realigned += 1;
            }
        }

        if realigned > 0 {
            log::debug!(
                "Pulled the reported acceptance time back to the first fill for {realigned} \
                 Aster order(s)"
            );
        }

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.venue,
            ts_init,
            None, // report_id
        );
        // The history is complete only from `start`; saying otherwise invites the engine to
        // invent opening fills for a position it cannot see the start of.
        mass_status.set_report_window(Some(start), true);
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(matched_fills);
        mass_status.add_position_reports(position_reports);

        Ok(Some(mass_status))
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let symbol = cmd
            .instrument_id
            .map(|id| self.symbol_context(&id).map(|(symbol, _)| symbol))
            .transpose()?;

        let positions = self
            .http_client
            .query_position_risk(symbol.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("Aster position risk query failed: {e}"))?;

        let ts_now = self.clock.get_time_ns();
        let mut reports = Vec::new();

        for position in &positions {
            let context = {
                let guard = self.instruments.read();
                Self::context_for_symbol(&guard, &position.symbol)
            };
            let Some(context) = context else {
                log::debug!(
                    "Skipping Aster position for unloaded symbol {}",
                    position.symbol
                );
                continue;
            };

            // Parsed before the flat check: `is_flat` reports an unparsable quantity as flat,
            // so skipping on it first would turn missing data into "no position".
            let signed_quantity = position
                .signed_quantity()
                .with_context(|| format!("Failed to parse Aster position {}", position.symbol))?;

            if signed_quantity.is_zero() {
                continue;
            }

            let position_side = if signed_quantity > Decimal::ZERO {
                PositionSide::Long
            } else {
                PositionSide::Short
            };

            let quantity = Quantity::from_decimal_dp(signed_quantity.abs(), context.size_precision)
                .with_context(|| format!("Failed to parse Aster position {}", position.symbol))?;

            let avg_px = parse_decimal(&position.entry_price, "entryPrice").ok();
            let ts_last = position.update_time.map_or(ts_now, millis_to_nanos);

            reports.push(PositionStatusReport::new(
                self.core.account_id,
                context.instrument_id,
                position_side,
                quantity,
                ts_last,
                ts_now,
                Some(UUID4::new()),
                None, // venue_position_id: one-way mode carries no venue position ID
                avg_px,
            ));
        }

        Ok(reports)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        accounts::{Account, MarginAccount},
        enums::{CurrencyType, OrderSide, OrderType, TimeInForce},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        instruments::{CryptoPerpetual, stubs::crypto_perpetual_ethusdt},
        orders::builder::OrderTestBuilder,
        types::{Money, Price, Quantity},
    };
    use rstest::rstest;

    use super::*;
    use crate::http::error::{
        ASTER_CODE_INVALID_LISTEN_KEY, ASTER_CODE_INVALID_SIGNATURE, ASTER_CODE_TOO_MANY_REQUESTS,
        ASTER_CODE_UNFUNDED_WALLET,
    };

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.ASTER")
    }

    fn account_id() -> AccountId {
        AccountId::from("ASTER-001")
    }

    /// Builds a margin account seeded with `balances`.
    fn margin_account(balances: Vec<AccountBalance>) -> MarginAccount {
        MarginAccount::new(
            AccountState::new(
                account_id(),
                AccountType::Margin,
                balances,
                Vec::new(),
                true, // is_reported
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
                None, // base_currency
            ),
            true, // calculate_account_state
        )
    }

    /// Builds an `ACCOUNT_UPDATE` frame carrying the given `B` array.
    fn account_update(balances_json: &str) -> BinanceFuturesAccountUpdateMsg {
        serde_json::from_str(&format!(
            r#"{{"e":"ACCOUNT_UPDATE","E":1788571663397,"T":1788571663397,
                 "a":{{"m":"ORDER","B":{balances_json},"P":[]}}}}"#
        ))
        .expect("fixture must deserialize")
    }

    fn limit_order(
        side: OrderSide,
        time_in_force: TimeInForce,
        post_only: bool,
        reduce_only: bool,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-1"))
            .side(side)
            .quantity(Quantity::from("0.010"))
            .price(Price::from("50000.00"))
            .time_in_force(time_in_force)
            .post_only(post_only)
            .reduce_only(reduce_only)
            .build()
    }

    fn market_order(side: OrderSide) -> OrderAny {
        OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-2"))
            .side(side)
            .quantity(Quantity::from("0.010"))
            .build()
    }

    #[rstest]
    fn test_build_limit_order_request() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, false, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.side, "BUY");
        assert_eq!(request.order_type, "LIMIT");
        assert_eq!(request.quantity, "0.010");
        assert_eq!(request.price.as_deref(), Some("50000.00"));
        assert_eq!(request.time_in_force, Some("GTC"));
        assert!(!request.reduce_only);
        assert_eq!(request.client_order_id, "O-20260101-000000-001-001-1");
    }

    #[rstest]
    fn test_build_market_order_request_omits_price_and_tif() {
        let order = market_order(OrderSide::Sell);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.side, "SELL");
        assert_eq!(request.order_type, "MARKET");
        assert_eq!(request.price, None);
        assert_eq!(request.time_in_force, None);
    }

    #[rstest]
    fn test_post_only_maps_to_gtx() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, true, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.time_in_force, Some("GTX"));
    }

    #[rstest]
    #[case(TimeInForce::Gtc, "GTC")]
    #[case(TimeInForce::Ioc, "IOC")]
    #[case(TimeInForce::Fok, "FOK")]
    fn test_supported_time_in_force(#[case] tif: TimeInForce, #[case] expected: &str) {
        let order = limit_order(OrderSide::Buy, tif, false, false);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert_eq!(request.time_in_force, Some(expected));
    }

    #[rstest]
    fn test_reduce_only_is_forwarded() {
        let order = limit_order(OrderSide::Sell, TimeInForce::Gtc, false, true);

        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        assert!(request.reduce_only);
        assert_eq!(request.to_params().get("reduceOnly"), Some("true"));
    }

    #[rstest]
    fn test_unsupported_time_in_force_is_rejected() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Day, false, false);

        let error = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap_err()
            .to_string();

        assert!(error.contains("GTC, IOC, and FOK"), "{error}");
    }

    #[rstest]
    fn test_unsupported_order_type_is_rejected() {
        let order = OrderTestBuilder::new(OrderType::StopMarket)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-3"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .trigger_price(Price::from("50000.00"))
            .build();

        let error = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap_err()
            .to_string();

        assert!(error.contains("LIMIT and MARKET"), "{error}");
    }

    #[rstest]
    fn test_order_request_parameter_order_matches_the_venue_example() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, false, false);
        let request = build_order_request(&order, "BTCUSDT".to_string()).unwrap();

        let params = request.to_params();
        let keys: Vec<&str> = params.entries().iter().map(|(k, _)| k.as_str()).collect();

        assert_eq!(
            keys,
            [
                "symbol",
                "side",
                "type",
                "quantity",
                "price",
                "timeInForce",
                "newClientOrderId",
            ]
        );
    }

    #[rstest]
    fn test_market_order_params_omit_price_and_time_in_force() {
        let order = market_order(OrderSide::Buy);
        let params = build_order_request(&order, "BTCUSDT".to_string())
            .unwrap()
            .to_params();

        assert_eq!(params.get("price"), None);
        assert_eq!(params.get("timeInForce"), None);
        assert_eq!(params.get("type"), Some("MARKET"));
    }

    #[rstest]
    #[case("O-20260101-000000-001-001-1", true)]
    #[case("abc_123-XYZ.7", true)]
    #[case("a/b:c", true)]
    #[case("", false)]
    #[case("has space", false)]
    #[case("bad#char", false)]
    fn test_client_order_id_validation(#[case] value: &str, #[case] expected: bool) {
        assert_eq!(is_valid_client_order_id(value), expected);
    }

    #[rstest]
    fn test_client_order_id_length_limit() {
        assert!(is_valid_client_order_id(&"a".repeat(36)));
        assert!(!is_valid_client_order_id(&"a".repeat(37)));
    }

    #[rstest]
    fn test_settlement_currency_is_usdt() {
        assert_eq!(
            AsterExecutionClient::settlement_currency(),
            Currency::from("USDT")
        );
    }

    // ------------------------------------------------------------------------------------------
    // Account balances
    // ------------------------------------------------------------------------------------------

    const BALANCE_TESTNET: &str =
        include_str!("../test_data/http_balance_testnet_unknown_assets.json");

    fn testnet_balances() -> Vec<AsterBalance> {
        let doc: serde_json::Value =
            serde_json::from_str(BALANCE_TESTNET).expect("fixture must be valid JSON");
        serde_json::from_value(doc["response"].clone()).expect("fixture must deserialize")
    }

    #[rstest]
    fn test_parse_account_balances_registers_unknown_venue_assets() {
        // `Currency::from("ASTER")` used to panic the whole node here.
        let balances = parse_account_balances(&testnet_balances());

        let codes: Vec<&str> = balances.iter().map(|b| b.currency.code.as_str()).collect();
        assert_eq!(codes, vec!["USDT", "BTC", "ASTER", "AFEE"]);

        for code in ["ASTER", "AFEE"] {
            let currency = Currency::try_from_str(code).expect("registered by resolve_currency");
            assert_eq!(currency.currency_type, CurrencyType::Crypto);
            assert_eq!(currency.precision, 8);
        }
    }

    #[rstest]
    fn test_parse_account_balances_clamps_free_above_total() {
        let balances = parse_account_balances(&testnet_balances());
        let usdt = balances
            .iter()
            .find(|b| b.currency == Currency::USDT())
            .expect("USDT balance");

        // The venue reports walletBalance 1000.00000000 and availableBalance 1590.98089862.
        // Nautilus requires total == locked + free, so free is clamped to the wallet balance
        // rather than inflating the total with cross-margin headroom from other assets.
        assert_eq!(usdt.total.as_decimal().to_string(), "1000.00000000");
        assert_eq!(usdt.free.as_decimal(), usdt.total.as_decimal());
        assert!(usdt.locked.as_decimal().is_zero());
    }

    #[rstest]
    fn test_parse_account_balances_keeps_explicit_zero_rows() {
        // Balances are applied per currency, so an omitted asset keeps its cached amount. The
        // venue's explicit zero row is the only thing that can clear a withdrawn asset, and
        // dropping it here is what left `USDT 100` cached after the account went to zero.
        let balances: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"0.0","availableBalance":"0.0"},
                {"asset":"BTC","balance":"5.0","availableBalance":"5.0"}]"#,
        )
        .unwrap();

        let parsed = parse_account_balances(&balances);

        assert_eq!(parsed.len(), 2);
        let usdt = parsed
            .iter()
            .find(|b| b.currency == Currency::USDT())
            .expect("the zero row must survive");
        assert!(usdt.total.as_decimal().is_zero());
        assert!(usdt.free.as_decimal().is_zero());
        assert!(usdt.locked.as_decimal().is_zero());
    }

    #[rstest]
    fn test_rest_balance_transition_to_zero_clears_the_cached_amount() {
        let funded: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"100.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        )
        .unwrap();
        let drained: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"0.0","availableBalance":"0.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        )
        .unwrap();

        let mut account = margin_account(parse_account_balances(&funded));
        assert_eq!(
            account.balance_total(Some(Currency::USDT())).unwrap(),
            Money::new(100.0, Currency::USDT()),
        );

        account
            .base
            .update_balances(&parse_account_balances(&drained));

        assert_eq!(
            account.balance_total(Some(Currency::USDT())).unwrap(),
            Money::new(0.0, Currency::USDT()),
            "a non-zero to zero transition must clear the cached amount",
        );
        assert_eq!(
            account.balance_total(Some(Currency::BTC())).unwrap(),
            Money::new(0.5, Currency::BTC()),
            "other assets must be untouched",
        );
    }

    #[rstest]
    fn test_stream_balance_transition_to_zero_clears_the_cached_amount() {
        // The shared Binance parser drops `wb == 0` rows, which would leave the stale amount
        // cached; the Aster crate parses the `B` array itself for exactly this case.
        let funded = account_update(
            r#"[{"a":"USDT","wb":"100.0","cw":"100.0"},
                                        {"a":"BTC","wb":"0.5","cw":"0.5"}]"#,
        );
        let drained = account_update(
            r#"[{"a":"USDT","wb":"0.0","cw":"0.0"},
                                         {"a":"BTC","wb":"0.5","cw":"0.5"}]"#,
        );

        let first = parse_aster_account_update(&funded, account_id(), UnixNanos::default())
            .expect("balances present");
        let mut account = MarginAccount::new(first, true);

        let second = parse_aster_account_update(&drained, account_id(), UnixNanos::default())
            .expect("the zero row must survive");
        assert_eq!(second.balances.len(), 2);
        account.base.update_balances(&second.balances);

        assert_eq!(
            account.balance_total(Some(Currency::USDT())).unwrap(),
            Money::new(0.0, Currency::USDT()),
        );
        assert_eq!(
            account.balance_total(Some(Currency::BTC())).unwrap(),
            Money::new(0.5, Currency::BTC()),
        );
    }

    #[rstest]
    fn test_stream_account_update_registers_unknown_assets() {
        let update = account_update(r#"[{"a":"AFEE","wb":"1.5","cw":"1.5"}]"#);

        let state = parse_aster_account_update(&update, account_id(), UnixNanos::default())
            .expect("balances present");

        assert_eq!(state.balances.len(), 1);
        assert_eq!(state.balances[0].currency.code.as_str(), "AFEE");
    }

    #[rstest]
    fn test_stream_account_update_without_balances_is_dropped() {
        let update = account_update("[]");

        assert!(parse_aster_account_update(&update, account_id(), UnixNanos::default()).is_none());
    }

    #[rstest]
    fn test_parse_account_balances_skips_unparsable_amounts() {
        let balances: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"not-a-number","availableBalance":"1.0"}]"#,
        )
        .unwrap();

        assert!(parse_account_balances(&balances).is_empty());
    }

    #[rstest]
    fn test_instrument_index_replace_indexes_both_directions() {
        let mut index = InstrumentIndex::default();
        assert_eq!(index.len(), 0);

        index.replace(Vec::new());
        assert_eq!(index.len(), 0);
        assert!(index.by_id(&instrument_id()).is_none());
        assert!(index.by_symbol(&Ustr::from("BTCUSDT")).is_none());
    }

    // ------------------------------------------------------------------------------------------
    // Quote-denominated quantities
    // ------------------------------------------------------------------------------------------

    #[rstest]
    fn test_limit_order_with_quote_quantity_is_rejected_before_any_request() {
        // `quantity = 100` with `quote_quantity` means 100 USDT of BTC, but Aster's `quantity`
        // is denominated in the base asset, so sending it verbatim would order 100 BTC.
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-9"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("100"))
            .price(Price::from("50000.00"))
            .quote_quantity(true)
            .build();

        let error = build_order_request(&order, "BTCUSDT".to_string()).unwrap_err();

        assert!(error.to_string().contains("quote_quantity"), "{error}");
    }

    #[rstest]
    fn test_market_order_with_quote_quantity_is_rejected_before_any_request() {
        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument_id())
            .client_order_id(ClientOrderId::from("O-20260101-000000-001-001-10"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("100"))
            .quote_quantity(true)
            .build();

        let error = build_order_request(&order, "BTCUSDT".to_string()).unwrap_err();

        assert!(error.to_string().contains("base asset"), "{error}");
    }

    #[rstest]
    fn test_base_quantity_orders_are_still_accepted() {
        let order = limit_order(OrderSide::Buy, TimeInForce::Gtc, false, false);

        assert!(!order.is_quote_quantity());
        assert_eq!(
            build_order_request(&order, "BTCUSDT".to_string())
                .unwrap()
                .quantity,
            "0.010",
        );
    }

    // ------------------------------------------------------------------------------------------
    // Commission rates
    // ------------------------------------------------------------------------------------------

    #[rstest]
    fn test_with_commission_rates_replaces_the_venue_defaults() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());

        // Binance's VIP-0 defaults are 0.0002 / 0.0005; Aster's live testnet answers 0.00005 /
        // 0.0004 for BTCUSDT, so the two are distinguishable in the assertion.
        let updated = with_commission_rates(
            &instrument,
            Decimal::from_str_exact("0.000050").unwrap(),
            Decimal::from_str_exact("0.000400").unwrap(),
        )
        .expect("perpetuals carry fee fields");

        let InstrumentAny::CryptoPerpetual(perp) = updated else {
            panic!("instrument type must be preserved");
        };
        assert_eq!(perp.maker_fee, Decimal::from_str_exact("0.000050").unwrap());
        assert_eq!(perp.taker_fee, Decimal::from_str_exact("0.000400").unwrap());
        assert_eq!(perp.id, instrument.id());
    }

    #[rstest]
    fn test_instrument_index_replace_one_updates_both_directions() {
        let mut index = InstrumentIndex::default();
        let original: CryptoPerpetual = crypto_perpetual_ethusdt();
        index.replace(vec![InstrumentAny::CryptoPerpetual(original.clone())]);

        let updated = with_commission_rates(
            &InstrumentAny::CryptoPerpetual(original.clone()),
            Decimal::from_str_exact("0.000050").unwrap(),
            Decimal::from_str_exact("0.000400").unwrap(),
        )
        .unwrap();
        index.replace_one(updated);

        assert_eq!(index.len(), 1);
        for found in [
            index.by_id(&original.id).unwrap(),
            index.by_symbol(&original.raw_symbol.inner()).unwrap(),
        ] {
            assert_eq!(
                found.maker_fee(),
                Decimal::from_str_exact("0.000050").unwrap()
            );
            assert_eq!(
                found.taker_fee(),
                Decimal::from_str_exact("0.000400").unwrap()
            );
        }
    }

    // ------------------------------------------------------------------------------------------
    // Stream state
    // ------------------------------------------------------------------------------------------

    #[rstest]
    fn test_applied_trades_deduplicates_and_tracks_the_newest_time() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");

        assert!(state.record_fill(symbol, 10, 1_000));
        assert!(!state.record_fill(symbol, 10, 1_000), "a repeat is not new");
        assert!(state.record_fill(symbol, 11, 2_000));

        assert!(state.has_fill(&symbol, 10));
        assert!(!state.has_fill(&symbol, 12));
        assert_eq!(state.last_fill_ms(&symbol), Some(2_000));
        assert_eq!(state.last_fill_ms(&Ustr::from("ETHUSDT")), None);
    }

    #[rstest]
    fn test_compensation_never_reaches_behind_the_session_start() {
        // A fill from a previous process is the engine's startup reconciliation to deliver;
        // replaying it as a live session fill is what produced the rejected `OrderFilled`.
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        state.session_start_ms = 1_000_000;

        assert_eq!(
            state.compensation_start_ms(&symbol, 500_000),
            1_000_000,
            "a lookback older than the connect must be clamped to the connect",
        );

        state.record_fill(symbol, 1, 2_000_000);
        assert_eq!(
            state.compensation_start_ms(&symbol, 500_000),
            2_000_000,
            "once a fill is seen, that is the floor",
        );
    }

    // ------------------------------------------------------------------------------------------
    // Reconciliation report alignment
    // ------------------------------------------------------------------------------------------

    fn fill_at(ts_event: u64) -> FillReport {
        FillReport::new(
            account_id(),
            instrument_id(),
            nautilus_model::identifiers::VenueOrderId::new("910001"),
            nautilus_model::identifiers::TradeId::new("1"),
            OrderSide::Buy,
            Quantity::from("0.010"),
            Price::from("50000.00"),
            Money::new(0.02, Currency::USDT()),
            nautilus_model::enums::LiquiditySide::Maker,
            None, // client_order_id
            None, // venue_position_id
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_event),
            None, // report_id
        )
    }

    fn report_accepted_at(ts_accepted: u64, ts_last: u64) -> OrderStatusReport {
        OrderStatusReport::new(
            account_id(),
            instrument_id(),
            Some(ClientOrderId::from("O-1")),
            nautilus_model::identifiers::VenueOrderId::new("910001"),
            Some(OrderSide::Buy),
            OrderType::Limit,
            TimeInForce::Gtc,
            nautilus_model::enums::OrderStatus::Filled,
            Quantity::from("0.010"),
            Quantity::from("0.010"),
            UnixNanos::from(ts_accepted),
            UnixNanos::from(ts_last),
            UnixNanos::from(ts_last),
            None, // report_id
        )
    }

    #[rstest]
    fn test_report_acceptance_is_pulled_back_to_its_first_fill() {
        // The engine orders reconciliation events by `ts_event`, so an acceptance stamped after
        // the order's own fill puts the fill first and the order state machine rejects it.
        let mut report = report_accepted_at(3_000, 3_000);
        let fills = [fill_at(1_000), fill_at(2_000)];
        let refs: Vec<&FillReport> = fills.iter().collect();

        assert!(align_report_with_fills(&mut report, &refs));

        assert_eq!(report.ts_accepted, UnixNanos::from(1_000u64));
        assert_eq!(
            report.ts_last,
            UnixNanos::from(3_000u64),
            "the last update must not be pulled backwards",
        );
    }

    #[rstest]
    fn test_report_already_preceding_its_fills_is_left_alone() {
        let mut report = report_accepted_at(1_000, 5_000);
        let fills = [fill_at(2_000)];
        let refs: Vec<&FillReport> = fills.iter().collect();

        assert!(!align_report_with_fills(&mut report, &refs));

        assert_eq!(report.ts_accepted, UnixNanos::from(1_000u64));
        assert_eq!(report.ts_last, UnixNanos::from(5_000u64));
    }

    #[rstest]
    fn test_report_without_fills_is_left_alone() {
        let mut report = report_accepted_at(9_000, 9_000);

        assert!(!align_report_with_fills(&mut report, &[]));

        assert_eq!(report.ts_accepted, UnixNanos::from(9_000u64));
    }

    #[rstest]
    fn test_recorded_fill_is_not_offered_again() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");

        assert!(state.record_fill(symbol, 4_242, 1_500_000));

        assert!(state.has_fill(&symbol, 4_242));
        assert!(
            !state.record_fill(symbol, 4_242, 1_500_000),
            "a trade ID already applied must never be delivered twice",
        );
    }

    #[rstest]
    fn test_applied_trades_is_bounded() {
        let mut applied = AppliedTrades::default();

        for trade_id in 0..(MAX_TRACKED_TRADE_IDS as i64 + 100) {
            applied.record(trade_id, trade_id);
        }

        assert_eq!(applied.ids.len(), MAX_TRACKED_TRADE_IDS);
        // The oldest IDs are dropped first, so the newest window stays recognisable.
        assert!(applied.contains(MAX_TRACKED_TRADE_IDS as i64 + 99));
        assert!(!applied.contains(0));
    }

    // ------------------------------------------------------------------------------------------
    // User stream connect retry classification
    // ------------------------------------------------------------------------------------------

    #[rstest]
    fn test_stream_connect_retries_transport_faults() {
        // These are what a slow or proxied egress path produces, and what the live testnet run
        // hit twice in a row ("connection timed out after 5s").
        for error in [
            AsterHttpError::Timeout("Aster user stream connect exceeded 20s".to_string()),
            AsterHttpError::NetworkError(
                "Failed to connect Aster user stream: I/O error: connection timed out".to_string(),
            ),
            AsterHttpError::NetworkError("tls handshake eof".to_string()),
        ] {
            assert!(is_retryable_stream_connect_error(&error), "{error}");
        }
    }

    #[rstest]
    #[case(ASTER_CODE_INVALID_LISTEN_KEY)]
    #[case(ASTER_CODE_UNFUNDED_WALLET)]
    #[case(ASTER_CODE_INVALID_SIGNATURE)]
    #[case(ASTER_CODE_TOO_MANY_REQUESTS)]
    fn test_stream_connect_does_not_retry_venue_answers(#[case] code: i64) {
        // The venue answered; repeating the request answers the same way and only hides the
        // error the operator needs to act on.
        let error = AsterHttpError::AsterError {
            code,
            message: "denied".to_string(),
        };

        assert!(!is_retryable_stream_connect_error(&error), "code={code}");
    }

    #[rstest]
    fn test_stream_connect_does_not_retry_local_faults() {
        assert!(!is_retryable_stream_connect_error(
            &AsterHttpError::MissingCredentials
        ));
        assert!(!is_retryable_stream_connect_error(
            &AsterHttpError::SigningError("bad key".to_string())
        ));
    }

    #[rstest]
    fn test_stream_connect_retry_schedule_is_bounded_and_increasing() {
        assert_eq!(USER_STREAM_CONNECT_RETRY_DELAYS.len(), 3);
        assert_eq!(
            USER_STREAM_CONNECT_RETRY_DELAYS.map(|d| d.as_secs()),
            [1, 2, 4],
        );
    }

    #[rstest]
    fn test_default_ws_connect_timeout_exceeds_the_shared_binance_default() {
        // The shared Binance stream pool times out at 5 s, which this host's egress path
        // exceeds; the Aster default must be able to lift that ceiling.
        assert_eq!(DEFAULT_WS_CONNECT_TIMEOUT_SECS, 20);
        assert_eq!(
            nautilus_binance::futures::websocket::streams::client::BINANCE_WS_CONNECT_TIMEOUT_MS,
            5_000,
            "the Aster default exists to lift this ceiling; if it moves, revisit the default",
        );
        assert_eq!(
            AsterExecutionClientConfig::default().ws_connect_timeout_secs,
            Some(DEFAULT_WS_CONNECT_TIMEOUT_SECS),
        );
    }

    #[rstest]
    fn test_working_orders_are_tracked_and_forgotten() {
        let mut state = StreamState::default();
        let client_order_id = Ustr::from("O-1");

        state.track_working_order(client_order_id, Ustr::from("BTCUSDT"));
        assert_eq!(state.working_orders.len(), 1);

        state.forget_working_order(&client_order_id);
        assert!(state.working_orders.is_empty());
    }
}
