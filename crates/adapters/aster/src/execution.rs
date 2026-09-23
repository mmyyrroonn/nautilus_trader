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
        fees::{FeeScope, clear_instrument_fee, clear_scope_fees, register_instrument_fees},
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
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
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

/// Delays before repeating a compensation pass's trade-history query.
///
/// A transient `userTrades` failure must not cost the session its real fills, and the order
/// states that depend on them are held back until it answers, so it is worth a short retry
/// rather than waiting for the next reconnect.
const COMPENSATION_FILL_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_secs(1), Duration::from_secs(3)];

/// Trade IDs retained per symbol for deduplicating REST compensation against stream fills.
const MAX_TRACKED_TRADE_IDS: usize = 4_096;

/// How far back compensation looks for fills on a symbol the stream never reported one for.
///
/// Anything older belongs to the engine's startup reconciliation, not to an outage this session
/// observed.
const COMPENSATION_FILL_LOOKBACK_MS: i64 = 60 * 60 * 1_000;
/// The shortest interval between two account snapshots a missing available amount may command.
///
/// A burst of account updates without a verified available amount then costs one account read,
/// not one per update.
const OWED_BALANCE_REFRESH_INTERVAL_MS: i64 = 5_000;

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

/// Adds an explicit flat position row for every instrument that traded in the window but has no
/// position report.
///
/// Aster's `positionRisk` answers with a row per symbol the account holds, so a symbol it omits
/// is one the account is flat in. That omission is unambiguous to a human and invisible to the
/// engine: under a bounded report window
/// (`ExecutionManager::order_only_venue_order_ids`) it looks up the expected quantity per
/// instrument from the position reports, and an instrument that has none falls through to "the
/// reports do not explain the position" — demoting every historical order for it to order-only
/// projection and logging an error, even when the windowed fills net to exactly zero.
///
/// Restating the omission as a flat row lets that check succeed. It states nothing the venue did
/// not: a flat report and an absent report carry the same claim.
fn with_flat_rows_for_traded_instruments(
    mut reports: Vec<PositionStatusReport>,
    order_reports: &[OrderStatusReport],
    fills: &[FillReport],
    account_id: AccountId,
    ts_init: UnixNanos,
    instruments: &InstrumentIndex,
) -> Vec<PositionStatusReport> {
    let already_reported: AHashSet<InstrumentId> =
        reports.iter().map(|report| report.instrument_id).collect();

    let mut traded: Vec<InstrumentId> = order_reports
        .iter()
        .filter(|report| !report.filled_qty.is_zero())
        .map(|report| report.instrument_id)
        .chain(fills.iter().map(|fill| fill.instrument_id))
        .filter(|instrument_id| !already_reported.contains(instrument_id))
        .collect::<AHashSet<InstrumentId>>()
        .into_iter()
        .collect();
    traded.sort(); // Deterministic report order

    for instrument_id in traded {
        let Some(instrument) = instruments.by_id(&instrument_id) else {
            continue;
        };

        let Ok(quantity) = Quantity::from_decimal_dp(Decimal::ZERO, instrument.size_precision())
        else {
            log::warn!("Cannot report a flat Aster position for {instrument_id}");
            continue;
        };

        reports.push(PositionStatusReport::new(
            account_id,
            instrument_id,
            PositionSide::Flat,
            quantity,
            ts_init,
            ts_init,
            Some(UUID4::new()),
            None, // venue_position_id: one-way mode
            None, // avg_px_open
        ));
    }

    reports
}

#[must_use]
/// Returns whether a status report must be withheld because its fills are not covered.
///
/// A report carrying a filled quantity is a fill in the engine's eyes: it reconciles the
/// difference by inventing one. That is correct only when the real trades have already been
/// delivered. When the trade history could not be read, publishing it replaces a real trade
/// (with its ID and commission) by a synthetic one *permanently* — the real fill arriving on
/// a later pass is then rejected by the overfill guard.
///
/// The order is left in the working set, so the next pass retries it once the history reads
/// again. A report with nothing filled cannot trigger inference and is published as usual.
fn defer_uncovered_status(report: &OrderStatusReport, coverage: FillCoverage) -> bool {
    if coverage == FillCoverage::Complete || report.filled_qty.is_zero() {
        return false;
    }

    log::warn!(
        "Holding back the Aster status for {} ({}): it reports {} filled and the trade \
         history is unavailable, so publishing it would fabricate the fill behind it",
        report
            .client_order_id
            .map_or_else(|| report.venue_order_id.to_string(), |id| id.to_string()),
        report.instrument_id,
        report.filled_qty,
    );
    true
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

/// Returns whether a request failed before it could reach the venue.
///
/// A missing credential, a signing fault, or a request the client itself refused to build never
/// touched the matching engine. For a cancel that makes the outcome definitive rather than
/// ambiguous: the order is certainly still working, so the command must be reported as rejected
/// instead of being left to reconciliation.
#[must_use]
fn is_local_request_failure(error: &AsterHttpError) -> bool {
    matches!(
        error,
        AsterHttpError::MissingCredentials
            | AsterHttpError::SigningError(_)
            | AsterHttpError::ValidationError(_)
    )
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

/// Whether a compensation pass could see every fill it needed.
///
/// The difference matters more than it looks: an empty delivered set means either "no trades
/// were missed" or "the trade history could not be read", and those demand opposite behaviour
/// from the order pass that follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillCoverage {
    /// Every symbol's trade history answered, so the delivered set is the whole truth.
    Complete,
    /// At least one trade query failed; nothing may be published that implies a fill.
    Unreliable,
}

/// What one compensation pass established about the fills it read.
///
/// `delivered` names the orders whose trades reached the engine bundled with their status, so
/// the order pass does not send a second, fill-inferring status for them. `uncovered` names the
/// orders whose trades were read but could *not* be delivered: their status carries a filled
/// quantity the engine has no trades for, so it must be withheld on this pass too.
#[derive(Debug, Default)]
struct CompensatedFills {
    delivered: AHashSet<Ustr>,
    uncovered: AHashSet<Ustr>,
}

impl CompensatedFills {
    fn merge(&mut self, other: Self) {
        self.delivered.extend(other.delivered);
        self.uncovered.extend(other.uncovered);
    }

    /// Returns the coverage that applies to one venue order on this pass.
    ///
    /// A pass that read every trade history is still not covered for an order whose own trades
    /// could not be delivered, so the two conditions are combined here rather than separately.
    fn coverage_for(&self, venue_order_id: i64, pass: FillCoverage) -> FillCoverage {
        let venue_order_id = Ustr::from(&venue_order_id.to_string());

        if pass == FillCoverage::Complete && !self.uncovered.contains(&venue_order_id) {
            FillCoverage::Complete
        } else {
            FillCoverage::Unreliable
        }
    }
}

/// One trade a report request covered, pending the dedupe commit.
#[derive(Debug, Clone, Copy)]
struct DeliveredFill {
    symbol: Ustr,
    venue_order_id: Ustr,
    trade_id: i64,
    ts_ms: i64,
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
    /// Trade IDs reported but not applied, per symbol.
    ///
    /// The next compensation pass fetches them by ID, so a trade stays recoverable once
    /// whatever blocked it — a missing order, most often — has resolved, however far the
    /// window this pass reads has since moved on.
    pending_trades: AHashMap<Ustr, BTreeSet<i64>>,
    /// Millisecond timestamp at which this client connected.
    ///
    /// Compensation never reaches behind it. Fills older than the connect belong to the
    /// engine's own startup reconciliation, and re-delivering them as live session events is
    /// what makes the engine reject an `OrderFilled` for an order it already holds as filled.
    session_start_ms: i64,
    /// The balances a full account snapshot last verified, keyed by asset.
    ///
    /// The REST snapshot is the only surface on which the venue states how much of an asset is
    /// available to open new positions. A user-data `ACCOUNT_UPDATE` carries the wallet balance
    /// (`wb`) and the cross wallet balance (`cw`), and `cw` is not spendable cash, so a stream
    /// row is merged against this map instead of being allowed to rewrite `free`.
    verified_balances: AHashMap<Ustr, AccountBalance>,
    /// The venue event time of the last account update applied, in milliseconds.
    ///
    /// An update older than this one describes a moment the account has already passed, so it
    /// is dropped rather than allowed to move balances backwards.
    last_balance_event_ms: i64,
    /// Local millisecond time before which an owed balance snapshot must not be retried.
    ///
    /// A burst of account updates without a verified available amount then costs one account
    /// read, not one per update.
    next_balance_refresh_ms: i64,
}

impl StreamState {
    fn track_working_order(&mut self, client_order_id: Ustr, symbol: Ustr) {
        self.working_orders.insert(client_order_id, symbol);
    }

    fn forget_working_order(&mut self, client_order_id: &Ustr) {
        self.working_orders.remove(client_order_id);
    }

    /// Replaces the verified reference with one full account snapshot.
    ///
    /// A snapshot is the complete account, so an asset it does not carry is no longer verified:
    /// merging a later stream row against a stale entry would carry an available amount the
    /// venue may have withdrawn since.
    fn replace_verified_balances(&mut self, balances: &[AccountBalance]) {
        self.verified_balances = balances
            .iter()
            .map(|balance| (Ustr::from(balance.currency.code.as_str()), balance.clone()))
            .collect();
    }

    /// Records a fill, returning whether it had not been applied before.
    fn record_fill(&mut self, symbol: Ustr, trade_id: i64, ts_ms: i64) -> bool {
        if let Some(pending) = self.pending_trades.get_mut(&symbol) {
            pending.remove(&trade_id);
            if pending.is_empty() {
                self.pending_trades.remove(&symbol);
            }
        }

        self.applied_trades
            .entry(symbol)
            .or_default()
            .record(trade_id, ts_ms)
    }

    /// Notes a trade that was reported but not applied, so it stays recoverable.
    fn note_pending_fill(&mut self, symbol: Ustr, trade_id: i64) {
        if self.has_fill(&symbol, trade_id) {
            return;
        }

        self.pending_trades
            .entry(symbol)
            .or_default()
            .insert(trade_id);
    }

    /// Returns the trade IDs still waiting to be recovered for a symbol, oldest first.
    fn pending_trade_ids(&self, symbol: &Ustr) -> BTreeSet<i64> {
        self.pending_trades.get(symbol).cloned().unwrap_or_default()
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
    ///
    /// Trades still awaiting recovery deliberately do *not* pull this floor back to their own
    /// timestamp. The dedupe set is bounded, so a window reaching behind it re-reads trades it
    /// can no longer recognise and re-delivers them as live fills; those trades are fetched by
    /// ID instead (see [`SessionContext::query_pending_trades`]).
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

    /// Fetches the trades still awaiting recovery for `symbol`, addressed by ID.
    ///
    /// A held-back trade can be older than the window the current pass reads, and pulling that
    /// window back to its timestamp is not a safe way to reach it: the dedupe set is bounded,
    /// so every trade in between whose ID it has already evicted would come back looking
    /// missed and be re-delivered as a live fill. `userTrades` refuses `fromId` together with a
    /// time window, so this pages forward from the oldest pending ID until the newest has been
    /// passed and keeps only the rows still pending — the trades in between are read but never
    /// delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or pagination cannot make progress.
    async fn query_pending_trades(
        &self,
        symbol: &str,
        pending: &BTreeSet<i64>,
    ) -> anyhow::Result<Vec<AsterUserTrade>> {
        let (Some(oldest), Some(newest)) = (pending.first().copied(), pending.last().copied())
        else {
            return Ok(Vec::new());
        };

        let mut recovered: Vec<AsterUserTrade> = Vec::new();
        let mut cursor = oldest;

        loop {
            let page = self
                .http_client
                .query_user_trades(
                    symbol,
                    None,
                    None,
                    Some(cursor),
                    Some(ASTER_HISTORY_PAGE_LIMIT),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Aster pending trade query failed for {symbol}: {e}")
                })?;

            if page.is_empty() {
                break;
            }

            let page_len = page.len();
            let max_trade_id = page.iter().map(|trade| trade.id).max().expect("non-empty");

            recovered.extend(page.into_iter().filter(|trade| pending.contains(&trade.id)));

            if max_trade_id >= newest || page_len < ASTER_HISTORY_PAGE_LIMIT as usize {
                break;
            }

            let next_cursor = max_trade_id
                .checked_add(1)
                .context("Aster trade ID overflow during pending trade recovery")?;
            anyhow::ensure!(
                next_cursor > cursor,
                "Aster userTrades pagination made no progress for {symbol}"
            );
            cursor = next_cursor;
        }

        Ok(recovered)
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

        // Fills go first, and each one is delivered *with* its order status as a single
        // `OrderWithFills` report. Sending the order status on its own first would hand the
        // engine a terminal quantity with no trades behind it, so it infers a fill of its own;
        // the real trade then arrives against an already-complete order, trips the overfill
        // guard, and is dropped — leaving the order holding a synthetic trade ID and no
        // commission. The economics, not just the log line, depend on this ordering.
        let mut fills = CompensatedFills::default();
        let mut coverage = FillCoverage::Unreliable;

        for attempt in 0..=COMPENSATION_FILL_RETRY_DELAYS.len() {
            match self.compensate_fills().await {
                Ok(recovered) => {
                    fills = recovered;
                    coverage = FillCoverage::Complete;
                    break;
                }
                Err(e) => {
                    log::error!("Aster fill compensation after {reason} failed: {e}");
                    if let Some(delay) = COMPENSATION_FILL_RETRY_DELAYS.get(attempt) {
                        tokio::time::sleep(*delay).await;
                    }
                }
            }
        }

        if coverage == FillCoverage::Unreliable {
            log::error!(
                "Aster trade history is unavailable after {reason}; order states carrying a \
                 filled quantity are held back until it can be read, so the engine is not \
                 invited to invent the trades behind them"
            );
        }

        if let Err(e) = self.compensate_orders(&fills, coverage).await {
            log::error!("Aster order compensation after {reason} failed: {e}");
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
    async fn compensate_orders(
        &self,
        fills: &CompensatedFills,
        coverage: FillCoverage,
    ) -> anyhow::Result<()> {
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

            if fills
                .delivered
                .contains(&Ustr::from(&order.order_id.to_string()))
            {
                // Already reported with its real fills in the pass above.
                continue;
            }

            match self.order_to_report(order, ts_init) {
                Ok(Some(report)) => {
                    let order_coverage = fills.coverage_for(order.order_id, coverage);
                    if defer_uncovered_status(&report, order_coverage) {
                        continue;
                    }
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
            if fills.delivered.contains(&client_order_id) {
                // The fill pass already delivered this order together with its trades.
                continue;
            }

            match self
                .http_client
                .query_order(&symbol, None, Some(client_order_id.as_str()))
                .await
            {
                Ok(order) => match self.order_to_report(&order, self.clock.get_time_ns()) {
                    Ok(Some(report)) => {
                        let order_coverage = fills.coverage_for(order.order_id, coverage);
                        if defer_uncovered_status(&report, order_coverage) {
                            continue;
                        }
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
    async fn compensate_fills(&self) -> anyhow::Result<CompensatedFills> {
        let mut recovered = CompensatedFills::default();
        let symbols: Vec<Ustr> = {
            let guard = self.instruments.read();
            guard.by_symbol.keys().copied().collect()
        };
        let now_ms = self.now_ms();
        let default_start = now_ms.saturating_sub(COMPENSATION_FILL_LOOKBACK_MS);

        for symbol in symbols {
            let (start_ms, pending) = {
                let state = self.state.read();
                (
                    state.compensation_start_ms(&symbol, default_start),
                    state.pending_trade_ids(&symbol),
                )
            };

            let mut trades = if start_ms > now_ms {
                Vec::new()
            } else {
                self.query_user_trades_paged(symbol.as_str(), start_ms, now_ms)
                    .await?
            };

            // Trades held back earlier are reached by ID, not by widening the window: they can
            // predate it, and the dedupe set no longer recognises what lies in between.
            if !pending.is_empty() {
                let read: AHashSet<i64> = trades.iter().map(|trade| trade.id).collect();
                let held_back = self.query_pending_trades(symbol.as_str(), &pending).await?;
                trades.extend(
                    held_back
                        .into_iter()
                        .filter(|trade| !read.contains(&trade.id)),
                );
                trades.sort_by_key(|trade| trade.id);
            }

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
                recovered.merge(
                    self.emit_missed_fills(&symbol, venue_order_id, &trades)
                        .await,
                );
            }
        }

        Ok(recovered)
    }

    /// Delivers the missed fills for one venue order, bundled with its order status.
    ///
    /// A trade and the order state it produced are only ever published together. Neither half
    /// is publishable alone: a bare fill lets the engine bootstrap a synthetic order at that
    /// fill's quantity and drop every later fill for the same venue order, and a bare status
    /// carrying a filled quantity lets it invent the trade behind it, which permanently
    /// replaces the real trade ID and its commission. So when either half cannot be built the
    /// whole order is held back: its trades stay pending (so the next pass fetches them by
    /// ID) and it is named as uncovered, so the order pass on the same compensation withholds
    /// its status too.
    async fn emit_missed_fills(
        &self,
        symbol: &Ustr,
        venue_order_id: i64,
        trades: &[&AsterUserTrade],
    ) -> CompensatedFills {
        let mut outcome = CompensatedFills::default();

        let Some(context) = self.context_for(symbol) else {
            log::debug!("Ignoring Aster fills on unloaded symbol {symbol}");
            return outcome;
        };

        let ts_init = self.clock.get_time_ns();
        let mut fills = Vec::with_capacity(trades.len());
        let mut unparsable = 0usize;

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
                Err(e) => {
                    log::error!("Failed to parse Aster trade {} on {symbol}: {e}", trade.id);
                    unparsable += 1;
                }
            }
        }

        // The order is only queried once every one of its trades has been built: a partial
        // bundle understates the order's filled quantity by exactly the trades that failed,
        // which is the gap the engine fills with a fabricated trade.
        let status = if unparsable == 0 && !fills.is_empty() {
            self.query_order_report(symbol, venue_order_id, ts_init)
                .await
        } else {
            None
        };

        let Some(status) = status else {
            {
                let mut state = self.state.write();
                for trade in trades {
                    state.note_pending_fill(*symbol, trade.id);
                }
            }

            log::warn!(
                "Holding back {} Aster fill(s) for order {venue_order_id} on {symbol}: they \
                 stay eligible for recovery rather than reaching the engine without the order \
                 state they belong to",
                trades.len(),
            );
            outcome
                .uncovered
                .insert(Ustr::from(&venue_order_id.to_string()));
            return outcome;
        };

        {
            let mut state = self.state.write();
            for (_, trade_id, ts_ms) in &fills {
                state.record_fill(*symbol, *trade_id, *ts_ms);
            }
        }

        outcome
            .delivered
            .insert(Ustr::from(&venue_order_id.to_string()));
        if let Some(client_order_id) = status.client_order_id {
            outcome.delivered.insert(client_order_id.inner());
        }

        let reports: Vec<FillReport> = fills.into_iter().map(|(report, _, _)| report).collect();
        log::info!(
            "Applied {} Aster fills missed by the user stream for order {venue_order_id}",
            reports.len(),
        );

        // One report, so the engine applies the trades and the resulting status together and
        // never has a window in which it must invent the missing quantity.
        self.emitter.send_order_with_fills(status, reports);

        outcome
    }

    /// Returns the status report for one venue order, or `None` when it cannot be built.
    ///
    /// Every failure is logged and answered with `None`: the caller holds the order's trades
    /// back rather than publishing either half on its own.
    async fn query_order_report(
        &self,
        symbol: &Ustr,
        venue_order_id: i64,
        ts_init: UnixNanos,
    ) -> Option<OrderStatusReport> {
        let order = match self
            .http_client
            .query_order(symbol.as_str(), Some(venue_order_id), None)
            .await
        {
            Ok(order) => order,
            Err(e) => {
                log::error!("Aster order query failed for {venue_order_id} on {symbol}: {e}");
                return None;
            }
        };

        match self.order_to_report(&order, ts_init) {
            Ok(Some(report)) => {
                self.track_order_state(&report, order.symbol);
                Some(report)
            }
            Ok(None) => {
                log::error!("Ignoring Aster order {venue_order_id} on unloaded symbol {symbol}");
                None
            }
            Err(e) => {
                log::error!("Failed to parse Aster order {venue_order_id}: {e}");
                None
            }
        }
    }

    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        let account_balances = parse_account_balances(&balances);
        // This snapshot is the verified reference every later stream update is merged against.
        self.state
            .write()
            .replace_verified_balances(&account_balances);
        self.emitter.emit_account_state(
            account_balances,
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
    fee_scope: FeeScope,
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
        let http_base_for_scope = http_base.clone();
        let config_account_id = config.account_id;

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
            // Account fees belong to this account at this endpoint, not to the venue: mainnet
            // and testnet, or two deployments, share the `ASTER` venue while holding entirely
            // different rates.
            fee_scope: FeeScope::new(&http_base_for_scope, config_account_id.as_str()),
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

    /// Builds the fill reports for a window, together with the trades they cover.
    ///
    /// The dedupe records are *returned*, not applied. Recording a trade as delivered while
    /// the request is still being built loses it outright when a later row fails: the whole
    /// call returns `Err`, the engine never receives the earlier trade, and the next
    /// compensation pass skips it as already applied. Only a caller that has finished
    /// successfully may commit them (see [`Self::commit_delivered_fills`]).
    async fn fetch_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<(Vec<FillReport>, Vec<DeliveredFill>)> {
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
        let mut delivered = Vec::new();

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
                delivered.push(DeliveredFill {
                    symbol: Ustr::from(&symbol),
                    venue_order_id: Ustr::from(&trade.order_id.to_string()),
                    trade_id: trade.id,
                    ts_ms: trade.time,
                });
            }
        }

        Ok((reports, delivered))
    }

    /// Keeps trades recoverable that were reported but could not be applied.
    ///
    /// The next compensation pass fetches them by ID, so a later trade for the same symbol
    /// advancing the watermark past them does not put them out of reach.
    fn hold_back_fills(&self, pending: &[DeliveredFill]) {
        if pending.is_empty() {
            return;
        }

        log::warn!(
            "{} Aster fill(s) have no order the engine can attach them to; they stay eligible \
             for recovery rather than being marked delivered",
            pending.len(),
        );

        let mut state = self.stream_state.write();
        for fill in pending {
            state.note_pending_fill(fill.symbol, fill.trade_id);
        }
    }

    /// Records trades as delivered so a later compensation pass does not repeat them.
    ///
    /// Called only once the request that produced them has succeeded in full.
    fn commit_delivered_fills(&self, delivered: Vec<DeliveredFill>) {
        if delivered.is_empty() {
            return;
        }

        let mut state = self.stream_state.write();
        for fill in delivered {
            state.record_fill(fill.symbol, fill.trade_id, fill.ts_ms);
        }
    }

    /// Fetches one order report by venue order ID, for a fill whose order the window missed.
    ///
    /// `allOrders` filters on the order's *creation* time, so an order opened before the
    /// lookback and filled inside it never appears on the order page even though its trade
    /// does. `GET /fapi/v3/order?orderId=` is not time-filtered, so it can still supply the
    /// order the fill belongs to.
    ///
    /// Returns `Ok(None)` when the instrument is not loaded (nothing can be built for it) and
    /// an error when the venue could not answer; the caller keeps the fill either way and marks
    /// the snapshot incomplete.
    async fn fetch_order_report_by_venue_id(
        &self,
        instrument_id: &InstrumentId,
        venue_order_id: VenueOrderId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let order_id: i64 = venue_order_id
            .as_str()
            .parse()
            .with_context(|| format!("Aster venue order ID {venue_order_id} is not numeric"))?;
        let symbol = format_binance_symbol(instrument_id);

        let order = self
            .http_client
            .query_order(&symbol, Some(order_id), None)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Aster order query failed for {venue_order_id} on {symbol}: {e}")
            })?;

        self.session().order_to_report(&order, ts_init)
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
                    // Drop anything an earlier session registered for this symbol: a rate that
                    // can no longer be confirmed must not keep being applied as if it were.
                    clear_instrument_fee(&self.fee_scope, instrument.raw_symbol().inner());
                    unverified.push(format!("{symbol} ({e})"));
                    continue;
                }
            };

            let (maker, taker) = match (rate.maker_rate(), rate.taker_rate()) {
                (Ok(maker), Ok(taker)) => (maker, taker),
                (maker, taker) => {
                    let error = maker.err().or_else(|| taker.err()).expect("one error");
                    clear_instrument_fee(&self.fee_scope, instrument.raw_symbol().inner());
                    unverified.push(format!("{symbol} ({error})"));
                    continue;
                }
            };

            let Some(updated) = with_commission_rates(&instrument, maker, taker) else {
                unverified.push(format!("{symbol} (instrument type carries no fee fields)"));
                continue;
            };

            self.instruments.write().replace_one(updated.clone());
            // Registered against this account's endpoint so the shared instrument parser
            // applies these rates to every later load against it. The market-data path rebuilds
            // instruments from `exchangeInfo` on request and on its periodic refresh, and would
            // otherwise restore the Binance placeholder fees over the rates just verified here.
            if register_instrument_fees(
                &self.fee_scope,
                instrument.raw_symbol().inner(),
                maker,
                taker,
            ) {
                verified += 1;
            } else {
                // Another account already owns this endpoint's rates; the parser cannot tell
                // the two apart, so ours are not applied and must not be reported as verified.
                unverified.push(format!("{symbol} (endpoint owned by another account)"));
                continue;
            }

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
    ///
    /// The check fails closed. Only a definitive venue answer that the endpoint is unavailable
    /// lets the session continue on the one-way assumption, because that answer is the same on
    /// every attempt and says nothing about the account. A transport fault, a `5xx`, or a rate
    /// limit leaves the mode *unknown*, and an unknown mode is not a one-way mode: assuming it
    /// is would connect a hedge-mode account to an adapter that mis-attributes every position.
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
            // A venue that answers with a decision of its own — an unknown endpoint, an
            // invalid parameter — does not expose the mode and never will, so it must not
            // block the session. A rate limit is a structured answer too, but it is a
            // "try again", not a decision, so it is excluded here.
            Err(e) if e.is_venue_rejection() && !e.is_rate_limited() => {
                log::warn!("Aster position mode query failed, assuming one-way mode: {e}");
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(
                "Aster position mode could not be confirmed, so one-way mode cannot be \
                 assumed: {e}"
            )),
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
        let account_balances = parse_account_balances(&balances);
        // This snapshot is the verified reference every later stream update is merged against.
        self.stream_state
            .write()
            .replace_verified_balances(&account_balances);
        Ok((account_balances, Vec::new()))
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
/// This is the mapping contract for a full snapshot: `total` is the wallet balance and `free` is
/// the venue's `availableBalance`, which is the only amount this adapter may publish as spendable.
/// A row whose `availableBalance` is missing is unknown and skipped rather than published as
/// `free = total`; the next complete snapshot is what states it. Aster reports `availableBalance`
/// above `walletBalance` on cross-margin accounts, because availability there includes headroom
/// from other assets; [`AccountBalance::from_total_and_free`] clamps `free` into `[0, total]` so
/// `total == locked + free` holds, and the first such row per process is logged at warning level.
fn parse_account_balances(balances: &[AsterBalance]) -> Vec<AccountBalance> {
    let mut account_balances = Vec::with_capacity(balances.len());

    for balance in balances {
        let currency = resolve_currency(balance.asset.as_str());

        let (Ok(total), Ok(available)) = (balance.total(), balance.available()) else {
            log::warn!("Skipping Aster balance for {currency}: unparsable amounts");
            continue;
        };

        let Some(free) = available else {
            log::warn!(
                "Skipping Aster balance for {currency}: availableBalance is missing, so the \
                 spendable amount is unknown"
            );
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

/// What one user-data `ACCOUNT_UPDATE` leaves behind.
struct AsterAccountUpdateMerge {
    /// The account state to publish, when the update applied and at least one row could be
    /// stated whole.
    state: Option<AccountState>,
    /// True when at least one row had no freshly verified available amount, so only a full
    /// REST snapshot can restore a fully verified account.
    refresh_owed: bool,
    /// The venue event time to remember, or `None` when the update was stale, changed nothing
    /// and must not move the mark.
    applied_event_ms: Option<i64>,
}

/// Merges one Aster `ACCOUNT_UPDATE` into the balances a full snapshot verified.
///
/// The payload's `wb` is the wallet balance and `cw` is the **cross wallet balance** - not the
/// amount available to open new positions. This function never maps `cw` to `free`:
///
/// - `wb == 0` is a verified zero (a withdrawal), the one amount that is whole on its own;
/// - a non-zero `wb` keeps the verified `free`, never raising it (the locked amount absorbs the
///   change), and owes a full snapshot: the payload never states the available amount, and an
///   order can move funds between locked and free without changing `wb` at all;
/// - with no verified `free` at all the row is withheld and a snapshot is owed, instead of
///   publishing the wallet balance as available margin.
///
/// The shared Binance parser drops balance rows whose wallet balance is zero. On Aster that
/// silently defeats the only mechanism the venue has for reporting a drained asset: account
/// updates are applied per currency, so the dropped zero leaves the previous amount cached.
/// The `B` array is therefore merged here instead of changing the Binance parser, which other
/// venues depend on.
fn merge_aster_account_update(
    msg: &BinanceFuturesAccountUpdateMsg,
    account_id: AccountId,
    ts_init: UnixNanos,
    verified: &AHashMap<Ustr, AccountBalance>,
    last_event_ms: i64,
) -> AsterAccountUpdateMerge {
    if msg.event_time > 0 && msg.event_time < last_event_ms {
        log::warn!(
            "Dropping a stale Aster account update dated {} ms: balances through {} ms were \
             already applied",
            msg.event_time,
            last_event_ms,
        );

        return AsterAccountUpdateMerge {
            state: None,
            refresh_owed: false,
            applied_event_ms: None,
        };
    }

    let mut balances = Vec::with_capacity(msg.account.balances.len());
    let mut refresh_owed = false;

    for update in &msg.account.balances {
        let currency = resolve_currency(update.asset.as_str());
        let total = update.wallet_balance;

        if total.is_zero() {
            // An explicit zero is the one amount that is whole without a snapshot: nothing can
            // be available while the wallet is empty, and this is how a withdrawal is stated.
            match AccountBalance::from_total_and_free(total, Decimal::ZERO, currency) {
                Ok(balance) => balances.push(balance),
                Err(e) => log::warn!("Skipping Aster stream balance for {currency}: {e}"),
            }
            continue;
        }

        let Some(previous) = verified.get(&update.asset) else {
            log::warn!(
                "Withholding Aster stream balance for {currency}: the payload states no \
                 available amount and no snapshot verified one; a full account read is owed"
            );
            refresh_owed = true;
            continue;
        };

        // The payload states the wallet balance but never the available amount: an order can
        // move funds between locked and free without changing `wb` at all, so even an unchanged
        // total leaves the split unverified. Publish the last verified available amount (never
        // raised; a total below it clamps it down) and owe a full snapshot.
        let free = previous.free.as_decimal().min(total);
        match AccountBalance::from_total_and_free(total, free, currency) {
            Ok(balance) => balances.push(balance),
            Err(e) => log::warn!("Skipping Aster stream balance for {currency}: {e}"),
        }
        refresh_owed = true;
    }

    let state = if balances.is_empty() {
        None
    } else {
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
    };

    AsterAccountUpdateMerge {
        state,
        refresh_owed,
        applied_event_ms: Some(msg.event_time.max(last_event_ms)),
    }
}

impl SessionContext {
    /// Reads one full account snapshot because a stream row had no verified available amount.
    ///
    /// The user-data payload is not an account snapshot: REST is the only surface that states how
    /// much of an asset is spendable. The read replaces the verified reference, so the next stream
    /// update can be merged against it, and publishes the snapshot itself. A failed read changes
    /// nothing about the balances the account already holds - withheld values stay withheld and
    /// carried ones stay carried - and the next owed update retries it after the interval.
    async fn refresh_owed_balances(&self) {
        let now_ms = self.now_ms();
        {
            let mut state = self.state.write();
            if now_ms < state.next_balance_refresh_ms {
                return;
            }
            state.next_balance_refresh_ms = now_ms.saturating_add(OWED_BALANCE_REFRESH_INTERVAL_MS);
        }

        let balances = match self.http_client.query_balances().await {
            Ok(balances) => balances,
            Err(e) => {
                log::warn!("Aster owed balance snapshot failed: {e}");
                return;
            }
        };

        let account_balances = parse_account_balances(&balances);
        self.state
            .write()
            .replace_verified_balances(&account_balances);
        self.emitter.emit_account_state(
            account_balances,
            Vec::new(),
            true,
            self.clock.get_time_ns(),
            None,
        );
    }

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
                let merge = {
                    let state = self.state.read();
                    merge_aster_account_update(
                        msg,
                        account_id,
                        ts_init,
                        &state.verified_balances,
                        state.last_balance_event_ms,
                    )
                };

                if let Some(event_ms) = merge.applied_event_ms {
                    let mut state = self.state.write();
                    if event_ms > state.last_balance_event_ms {
                        state.last_balance_event_ms = event_ms;
                    }
                }

                if let Some(state) = merge.state {
                    self.emitter.send_account_state(state);
                }

                if merge.refresh_owed {
                    self.refresh_owed_balances().await;
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
        // The endpoint's fee registrations belong to this client; releasing them lets a later
        // client for another account take the endpoint over, and stops a stale rate from
        // outliving the session that verified it.
        clear_scope_fees(&self.fee_scope);
        self.core.set_stopped();
        self.core.set_disconnected();

        log::info!("Aster execution client stopped");
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() && self.session_tasks.is_open() && self.pending_tasks.is_open()
        {
            return Ok(());
        }

        // A previous `disconnect` or `stop` closed both task generations permanently. They are
        // drained and reopened before anything spawns a task or opens a listen key, otherwise
        // the stream task is silently refused and the client reports as connected with no
        // private stream behind it.
        if !self.session_tasks.is_open() || !self.pending_tasks.is_open() {
            self.await_task_groups().await;

            self.session_tasks.start_generation().map_err(|e| {
                anyhow::anyhow!("Failed to start Aster session task generation: {e}")
            })?;
            self.pending_tasks.start_generation().map_err(|e| {
                anyhow::anyhow!("Failed to start Aster pending task generation: {e}")
            })?;
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

        // The stream session owns its listen key and releases it when its own loop ends, but a
        // disconnect cancels that loop instead of ending it, so the key would be left open at
        // the venue until it expired. It is released here, once the session task can no longer
        // be renewing it.
        if let Err(e) = self.http_client.close_listen_key().await {
            log::debug!("Aster listen key close failed (key may already be expired): {e}");
        }

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

    /// Cancels one order, reporting a definitive refusal back to the engine.
    ///
    /// A cancel the venue refuses outright, or one that never left this process, leaves the
    /// order working. The engine holds it in `PendingCancel` until its in-flight check expires
    /// and then reconciles it as *canceled* (`ExecutionManager`), so a silent failure here ends
    /// with the engine believing an order is gone while it still rests on the book.
    /// `OrderCancelRejected` is what puts the order back into its real state.
    ///
    /// An ambiguous failure is different: the venue may still act on the request, so it is left
    /// to reconciliation rather than reported as a rejection that never happened.
    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        // The venue order ID is preferred because it is unambiguous: the venue always knows it,
        // including for an order placed outside this client whose client order ID it never
        // received. The client order ID is the fallback for an order whose venue ID this
        // session has not observed yet.
        let venue_order_id_i64 = venue_order_id
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok());

        spawner.spawn(async move {
            let result = match venue_order_id_i64 {
                Some(order_id) => {
                    http_client
                        .cancel_order(&symbol, Some(order_id), None)
                        .await
                }
                None => {
                    http_client
                        .cancel_order(&symbol, None, Some(client_order_id.as_str()))
                        .await
                }
            };

            match result {
                Ok(response) => log::debug!(
                    "Aster cancel accepted: client_order_id={client_order_id}, status={:?}",
                    response.status,
                ),
                Err(e) if e.is_unknown_order() => {
                    log::warn!("Aster reported order {client_order_id} as unknown on cancel: {e}");
                }
                Err(e) if e.is_venue_rejection() || is_local_request_failure(&e) => {
                    log::warn!("Aster rejected the cancel for {client_order_id}: {e}");
                    emitter.emit_order_cancel_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        venue_order_id,
                        &format!("cancel-order-error: {e}"),
                        clock.get_time_ns(),
                    );
                }
                Err(e) => log::warn!(
                    "Ambiguous Aster cancel failure for {client_order_id}, awaiting \
                     reconciliation: {e}"
                ),
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
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;

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

            // The venue echoes the client order ID it holds for each open order, which is
            // what a cancel rejection has to name: an order the engine cannot identify cannot
            // be put back into its real state.
            let targets: Vec<(i64, Option<ClientOrderId>)> = open_orders
                .iter()
                .filter(|order| parse_order_side(order.side) == order_side)
                .map(|order| {
                    let client_order_id = (!order.client_order_id.is_empty())
                        .then(|| ClientOrderId::new(&order.client_order_id));
                    (order.order_id, client_order_id)
                })
                .collect();

            if targets.is_empty() {
                log::debug!("No open {order_side:?} orders to cancel for {symbol}");
                return Ok(());
            }

            let mut failed = Vec::new();
            for (venue_order_id, client_order_id) in &targets {
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
                    Err(e) => {
                        // Same contract as `cancel_order`: a refusal the venue already decided,
                        // or one that never reached it, leaves the order working and must be
                        // reported so the engine does not later reconcile it as canceled.
                        if e.is_venue_rejection() || is_local_request_failure(&e) {
                            match client_order_id {
                                Some(client_order_id) => {
                                    emitter.emit_order_cancel_rejected_event(
                                        strategy_id,
                                        instrument_id,
                                        *client_order_id,
                                        Some(VenueOrderId::new(venue_order_id.to_string())),
                                        &format!("cancel-order-error: {e}"),
                                        clock.get_time_ns(),
                                    );
                                }
                                None => log::warn!(
                                    "Aster rejected the cancel for {venue_order_id} on {symbol} \
                                     and reported no client order ID, so the rejection cannot \
                                     be attributed to an order: {e}"
                                ),
                            }
                        }
                        failed.push(format!("{venue_order_id} ({e})"));
                    }
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
        let stream_state = self.stream_state.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let balances = http_client
                .query_balances()
                .await
                .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

            let account_balances = parse_account_balances(&balances);
            // A full snapshot is the verified reference later stream updates merge against.
            stream_state
                .write()
                .replace_verified_balances(&account_balances);

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
        let (reports, delivered) = self.fetch_fill_reports(cmd).await?;

        // Only now, with the whole request answered, are the trades recorded as delivered.
        self.commit_delivered_fills(delivered);

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
    ///    against its own fills (see `align_report_with_fills`).
    ///
    /// 3. **Every reported fill is kept.** A fill whose order is not on the order page is not
    ///    an orphan — `allOrders` filters on creation time, so an order opened before the
    ///    window and filled inside it is simply absent — so the order is fetched by ID and
    ///    linked. A fill that still cannot be linked is retained and the snapshot is declared
    ///    incomplete; a real trade is never discarded to quiet a warning.
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
        // Fetched without committing the dedupe records: the positions request below can still
        // fail the whole snapshot, and a trade the engine never saw must stay deliverable.
        let (fill_reports, delivered) = self.fetch_fill_reports(fill_cmd).await?;
        let position_reports = self.generate_position_status_reports(&position_cmd).await?;

        // A fill inside the window whose order was *created* before it is not an orphan: the
        // venue's `allOrders` filters on creation time, so a GTC placed two minutes ago and
        // filled one second ago is simply missing from the order page. Dropping the fill would
        // discard a real trade and its commission; the order is fetched by ID instead.
        let mut reported_orders: AHashSet<Ustr> = order_reports
            .iter()
            .map(|report| report.venue_order_id.inner())
            .collect();
        let mut unlinked_fills = 0usize;
        // One query per missing order, however many of its trades are in the window.
        let mut unfetchable: AHashSet<Ustr> = AHashSet::new();

        for fill in &fill_reports {
            let venue_order_id = fill.venue_order_id.inner();
            if reported_orders.contains(&venue_order_id) {
                continue;
            }

            if unfetchable.contains(&venue_order_id) {
                unlinked_fills += 1;
                continue;
            }

            match self
                .fetch_order_report_by_venue_id(&fill.instrument_id, fill.venue_order_id, ts_init)
                .await
            {
                Ok(Some(report)) => {
                    log::debug!(
                        "Linked Aster fill {} to order {venue_order_id}, which predates the \
                         report window",
                        fill.trade_id,
                    );
                    reported_orders.insert(venue_order_id);
                    order_reports.push(report);
                }
                Ok(None) | Err(_) => {
                    // Keep the fill: a trade the venue reported is evidence, and losing it
                    // silently is worse than admitting the snapshot is partial.
                    unfetchable.insert(venue_order_id);
                    unlinked_fills += 1;
                }
            }
        }

        // Every fill is retained; the completeness flag carries whether they could all be
        // attributed to an order.
        let matched_fills = fill_reports;
        let reports_complete = unlinked_fills == 0;

        if !reports_complete {
            log::warn!(
                "{unlinked_fills} Aster fill(s) could not be linked to an order report; the \
                 startup snapshot is reported as incomplete rather than silently trimmed"
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

        // Under a bounded window the engine confirms that the reported fills explain the
        // reported position, per instrument. An instrument with activity but no position row
        // has nothing to confirm against, and every one of its orders is then demoted to
        // order-only projection with an error. A flat row states what the venue's omission
        // already means, so the check can be answered instead of skipped.
        let position_reports = with_flat_rows_for_traded_instruments(
            position_reports,
            &order_reports,
            &matched_fills,
            self.core.account_id,
            ts_init,
            &self.instruments.read(),
        );

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.venue,
            ts_init,
            None, // report_id
        );
        // The history is complete only from `start`; saying otherwise invites the engine to
        // invent opening fills for a position it cannot see the start of. `reports_complete`
        // additionally states whether every reported fill could be attributed to an order.
        mass_status.set_report_window(Some(start), reports_complete);
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(matched_fills);
        mass_status.add_position_reports(position_reports);

        // Only the trades whose order the snapshot could name are the engine's to reconcile.
        // A one-way fill with no cached order and no venue position ID is not turned into a
        // fill event at all (`ExecutionManager` skips it), so "the report call returned Ok" is
        // not "the fill was applied". Marking those delivered would make the next compensation
        // pass skip a trade nothing ever applied.
        let (linked, unlinked): (Vec<DeliveredFill>, Vec<DeliveredFill>) = delivered
            .into_iter()
            .partition(|fill| reported_orders.contains(&fill.venue_order_id));

        self.commit_delivered_fills(linked);
        self.hold_back_fills(&unlinked);

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

            // The quantity is parsed before anything branches on flatness: an unparsable
            // value must fail the report rather than read as "no position".
            let signed_quantity = position
                .signed_quantity()
                .with_context(|| format!("Failed to parse Aster position {}", position.symbol))?;

            // Flat rows are reported, not dropped. "The venue holds nothing here" is a fact the
            // engine needs: it closes a position the cache still believes in, and under a
            // bounded report window it is the only thing that can confirm the windowed fills
            // net out (see `generate_mass_status`).
            let position_side = if signed_quantity > Decimal::ZERO {
                PositionSide::Long
            } else if signed_quantity < Decimal::ZERO {
                PositionSide::Short
            } else {
                PositionSide::Flat
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

    /// Builds the verified-balance reference a full REST snapshot leaves behind.
    fn verified_balances(rows: &str) -> AHashMap<Ustr, AccountBalance> {
        let balances: Vec<AsterBalance> = serde_json::from_str(rows).expect("balance fixture");
        parse_account_balances(&balances)
            .into_iter()
            .map(|balance| (Ustr::from(balance.currency.code.as_str()), balance))
            .collect()
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    /// A `cw` equal to the wallet balance is the **cross wallet balance**, not spendable
    /// cash: a stream update never raises the available amount a snapshot verified.
    #[rstest]
    fn test_stream_update_never_raises_available_with_cross_wallet_balance() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(r#"[{"a":"USDT","wb":"100.0","cw":"100.0"}]"#);

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let usdt = &merged.state.expect("the row is fully stated").balances[0];

        assert_eq!(usdt.total.as_decimal(), dec("100"));
        assert_eq!(usdt.free.as_decimal(), dec("20"));
        assert_eq!(usdt.locked.as_decimal(), dec("80"));
        assert!(
            merged.refresh_owed,
            "the payload never states the available amount, so a snapshot is owed",
        );
        assert_eq!(merged.applied_event_ms, Some(1788571663397));
    }

    /// A wallet balance change without an available amount keeps the last verified
    /// available amount, never raises it, and owes a full snapshot.
    #[rstest]
    fn test_stream_update_carries_verified_available_through_a_wallet_change() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(r#"[{"a":"USDT","wb":"150.0","cw":"150.0"}]"#);

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let usdt = &merged.state.expect("the total is still stated").balances[0];

        assert_eq!(usdt.total.as_decimal(), dec("150"));
        assert_eq!(
            usdt.free.as_decimal(),
            dec("20"),
            "available must never rise with the wallet balance",
        );
        assert_eq!(usdt.locked.as_decimal(), dec("130"));
        assert!(merged.refresh_owed);
    }

    /// Without a verified available amount the row is withheld: the wallet balance is not
    /// the spendable balance, and a full snapshot is owed.
    #[rstest]
    fn test_stream_update_without_verified_available_withholds_the_row() {
        let update = account_update(r#"[{"a":"USDT","wb":"100.0","cw":"100.0"}]"#);

        let merged = merge_aster_account_update(
            &update,
            account_id(),
            UnixNanos::default(),
            &AHashMap::new(),
            0,
        );

        assert!(
            merged.state.is_none(),
            "cw is not a publishable available amount",
        );
        assert!(merged.refresh_owed);
    }

    /// An explicit zero wallet balance is a verified zero, however the asset looked before.
    #[rstest]
    fn test_stream_update_zero_is_a_verified_zero() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(r#"[{"a":"USDT","wb":"0.0","cw":"0.0"}]"#);

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let usdt = &merged.state.expect("the zero row survives").balances[0];

        assert_eq!(usdt.total.as_decimal(), dec("0"));
        assert_eq!(usdt.free.as_decimal(), dec("0"));
        assert_eq!(usdt.locked.as_decimal(), dec("0"));
        assert!(
            !merged.refresh_owed,
            "zero is a verified amount, not a missing one"
        );
    }

    /// An update older than one already applied changes nothing; the account never moves
    /// backwards.
    #[rstest]
    fn test_stream_update_stale_event_is_dropped() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(r#"[{"a":"USDT","wb":"150.0","cw":"150.0"}]"#);

        let merged = merge_aster_account_update(
            &update,
            account_id(),
            UnixNanos::default(),
            &verified,
            1788571663398,
        );

        assert!(merged.state.is_none());
        assert!(!merged.refresh_owed);
        assert_eq!(
            merged.applied_event_ms, None,
            "a stale event must not move the mark",
        );
    }

    /// Each row of a multi-asset update is merged on its own: one verified asset keeps its
    /// split while an unverified one is withheld, and a single snapshot is owed for all.
    #[rstest]
    fn test_stream_update_merges_each_asset_row_independently() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(
            r#"[{"a":"USDT","wb":"100.0","cw":"100.0"},
                {"a":"BTC","wb":"1.0","cw":"1.0"}]"#,
        );

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let balances = merged.state.expect("the verified row is stated").balances;

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].currency, Currency::USDT());
        assert!(
            merged.refresh_owed,
            "the withheld BTC row still owes a snapshot"
        );
    }

    /// The changed-amount path never lets the available amount rise, even when the wallet
    /// balance goes negative.
    #[rstest]
    fn test_stream_update_negative_wallet_never_raises_available() {
        let verified =
            verified_balances(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
        let update = account_update(r#"[{"a":"USDT","wb":"-5.0","cw":"-5.0"}]"#);

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let usdt = &merged.state.expect("the total is still stated").balances[0];

        assert_eq!(usdt.total.as_decimal(), dec("-5"));
        assert!(usdt.free.as_decimal() <= Decimal::ZERO);
        assert!(merged.refresh_owed);
    }

    /// A snapshot the venue sends without the available amount is unknown, not
    /// `free == total`: the row is skipped rather than inflating the account.
    #[rstest]
    fn test_rest_balance_without_available_is_unknown_not_total() {
        let balances: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0"},
                {"asset":"BTC","balance":"0.0","availableBalance":"0.0"}]"#,
        )
        .unwrap();

        let parsed = parse_account_balances(&balances);

        assert_eq!(
            parsed.len(),
            1,
            "the row with no available amount is unknown"
        );
        assert_eq!(parsed[0].currency, Currency::BTC());
    }

    /// A full REST snapshot is the new verified reference: assets it does not carry are no
    /// longer treated as verified.
    #[rstest]
    fn test_rest_snapshot_replaces_the_verified_reference() {
        let funded = verified_balances(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        );
        let mut state = StreamState {
            verified_balances: funded,
            ..StreamState::default()
        };

        let drained: Vec<AsterBalance> =
            serde_json::from_str(r#"[{"asset":"USDT","balance":"0.0","availableBalance":"0.0"}]"#)
                .unwrap();
        state.replace_verified_balances(&parse_account_balances(&drained));

        assert_eq!(state.verified_balances.len(), 1);
        assert!(
            state
                .verified_balances
                .get(&Ustr::from("USDT"))
                .is_some_and(|b| b.total.as_decimal().is_zero()),
        );
    }

    #[rstest]
    fn test_stream_balance_transition_to_zero_clears_the_cached_amount() {
        // The shared Binance parser drops `wb == 0` rows, which would leave the stale amount
        // cached; the Aster crate parses the `B` array itself so a withdrawal is stated, and
        // the zero row is the one stream statement that is complete on its own.
        let verified = verified_balances(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"100.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        );
        let drained = account_update(
            r#"[{"a":"USDT","wb":"0.0","cw":"0.0"},
                {"a":"BTC","wb":"0.5","cw":"0.5"}]"#,
        );

        let merged =
            merge_aster_account_update(&drained, account_id(), UnixNanos::default(), &verified, 0);
        let balances = merged.state.expect("the zero row must survive").balances;
        assert_eq!(balances.len(), 2);

        let account = margin_account(balances);

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
        let verified = [(
            Ustr::from("AFEE"),
            AccountBalance::from_total_and_free(
                Decimal::from(1),
                Decimal::from(1),
                Currency::from("AFEE"),
            )
            .unwrap(),
        )]
        .into_iter()
        .collect();
        let update = account_update(r#"[{"a":"AFEE","wb":"1.5","cw":"1.5"}]"#);

        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), &verified, 0);
        let state = merged.state.expect("balances present");

        assert_eq!(state.balances.len(), 1);
        assert_eq!(state.balances[0].currency.code.as_str(), "AFEE");
    }

    #[rstest]
    fn test_stream_account_update_without_balances_is_dropped() {
        let update = account_update("[]");

        let merged = merge_aster_account_update(
            &update,
            account_id(),
            UnixNanos::default(),
            &AHashMap::new(),
            0,
        );

        assert!(merged.state.is_none());
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

    fn flat_position_inputs() -> (Vec<OrderStatusReport>, Vec<FillReport>, InstrumentIndex) {
        let mut index = InstrumentIndex::default();
        index.replace(vec![InstrumentAny::CryptoPerpetual(
            crypto_perpetual_ethusdt(),
        )]);
        let instrument_id = crypto_perpetual_ethusdt().id;

        let mut report = report_accepted_at(1_000, 2_000);
        report.instrument_id = instrument_id;
        let mut fill = fill_at(1_500);
        fill.instrument_id = instrument_id;

        (vec![report], vec![fill], index)
    }

    #[rstest]
    fn test_flat_row_is_added_for_an_instrument_the_venue_omitted() {
        // Aster omits a symbol from `positionRisk` when the account is flat in it, and the
        // engine's bounded check then has no expected quantity to confirm the fills against.
        let (orders, fills, index) = flat_position_inputs();
        let instrument_id = crypto_perpetual_ethusdt().id;

        let reports = with_flat_rows_for_traded_instruments(
            Vec::new(),
            &orders,
            &fills,
            account_id(),
            UnixNanos::default(),
            &index,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument_id);
        assert_eq!(reports[0].position_side, PositionSide::Flat);
        assert!(reports[0].signed_decimal_qty.is_zero());
        assert!(reports[0].venue_position_id.is_none());
    }

    #[rstest]
    fn test_existing_position_row_is_never_replaced_by_a_flat_one() {
        let (orders, fills, index) = flat_position_inputs();
        let instrument_id = crypto_perpetual_ethusdt().id;
        let existing = PositionStatusReport::new(
            account_id(),
            instrument_id,
            PositionSide::Long,
            Quantity::from("0.010"),
            UnixNanos::default(),
            UnixNanos::default(),
            None, // report_id
            None, // venue_position_id
            None, // avg_px_open
        );

        let reports = with_flat_rows_for_traded_instruments(
            vec![existing],
            &orders,
            &fills,
            account_id(),
            UnixNanos::default(),
            &index,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].position_side, PositionSide::Long);
    }

    #[rstest]
    fn test_no_flat_row_without_activity() {
        let (_, _, index) = flat_position_inputs();

        let reports = with_flat_rows_for_traded_instruments(
            Vec::new(),
            &[],
            &[],
            account_id(),
            UnixNanos::default(),
            &index,
        );

        assert!(reports.is_empty());
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
    fn test_a_pending_fill_is_recovered_by_id_not_by_widening_the_window() {
        // A trade reported but never applied must stay reachable after later trades have moved
        // the watermark past it — but reaching it by pulling the window back to its timestamp
        // would re-read the history in between, which the bounded dedupe set can no longer
        // recognise. It is listed for a targeted query by ID instead.
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        state.session_start_ms = 1_000;

        state.note_pending_fill(symbol, 500);
        state.record_fill(symbol, 600, 9_000);

        assert_eq!(
            state.compensation_start_ms(&symbol, 1_000),
            9_000,
            "the window must stay at the watermark, whatever is still awaiting recovery",
        );
        assert_eq!(
            state.pending_trade_ids(&symbol),
            BTreeSet::from([500]),
            "the trade must stay listed for a targeted query",
        );

        // Once it is applied, nothing is left to recover.
        state.record_fill(symbol, 500, 2_000);
        assert_eq!(state.compensation_start_ms(&symbol, 1_000), 9_000);
        assert!(state.pending_trade_ids(&symbol).is_empty());
    }

    #[rstest]
    fn test_a_pending_trade_survives_the_eviction_of_its_generation() {
        // The dedupe set is bounded, so a long history evicts the oldest IDs. A trade still
        // awaiting recovery must not be lost with them, and the trades that *were* applied must
        // stay recognised as applied so a pass that reads them again does not re-deliver them.
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");

        state.note_pending_fill(symbol, 1);
        for trade_id in 2..=(MAX_TRACKED_TRADE_IDS as i64 + 1) {
            state.record_fill(symbol, trade_id, 1_000 + trade_id);
        }

        assert!(
            !state.has_fill(&symbol, 1),
            "the pending trade was never applied"
        );
        assert_eq!(
            state.pending_trade_ids(&symbol),
            BTreeSet::from([1]),
            "eviction must not drop a trade still awaiting recovery",
        );
        assert!(
            state.has_fill(&symbol, MAX_TRACKED_TRADE_IDS as i64 + 1),
            "the newest applied trades stay recognised",
        );
    }

    #[rstest]
    fn test_a_pending_note_for_an_applied_fill_is_ignored() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        state.record_fill(symbol, 500, 5_000);

        state.note_pending_fill(symbol, 500);

        assert!(state.pending_trade_ids(&symbol).is_empty());
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
    fn test_a_filled_status_is_withheld_when_the_trade_history_is_unavailable() {
        // Publishing it would make the engine invent the trade behind it, and the real one
        // would then be rejected as an overfill — permanently, not just for this pass.
        let report = report_accepted_at(1_000, 2_000);
        assert!(!report.filled_qty.is_zero());

        assert!(defer_uncovered_status(&report, FillCoverage::Unreliable));
        assert!(
            !defer_uncovered_status(&report, FillCoverage::Complete),
            "with the history read, the real fills were already delivered alongside it",
        );
    }

    #[rstest]
    fn test_an_unfilled_status_is_published_even_without_the_trade_history() {
        // Nothing is filled, so there is no quantity for the engine to explain.
        let mut report = report_accepted_at(1_000, 2_000);
        report.filled_qty = Quantity::from("0.000");
        report.order_status = nautilus_model::enums::OrderStatus::Accepted;

        assert!(!defer_uncovered_status(&report, FillCoverage::Unreliable));
    }

    #[rstest]
    fn test_compensation_fill_retry_schedule_is_bounded() {
        assert_eq!(COMPENSATION_FILL_RETRY_DELAYS.len(), 2);
        assert_eq!(
            COMPENSATION_FILL_RETRY_DELAYS.map(|delay| delay.as_secs()),
            [1, 3],
        );
    }

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
            status: Some(400),
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
