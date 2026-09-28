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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use futures_util::StreamExt;
use nautilus_binance::{
    common::{
        enums::{BinanceEnvironment, BinancePositionSide, BinanceProductType},
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
        DataEvent, ExecutionReport,
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
    ExecutionClientCore, ExecutionEventEmitter, SocketControlFactory,
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
use nautilus_network::{
    SocketState,
    retry::{RetryConfig, RetryManager},
};
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
            AsterBalance, AsterOrder, AsterPositionRisk, AsterUserTrade, millis_to_nanos,
            parse_decimal, parse_order_side,
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

/// Maximum number of exact trade IDs retained per symbol for the fast duplicate path.
///
/// Once an ID leaves this cache, the owning order is marked as having incomplete ID coverage.
/// A later status/query path must then verify the order against its complete real trade history;
/// an evicted ID is never treated as delivered merely because it is numerically older.
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
/// The longest a failed owed snapshot waits before it is retried.
const OWED_BALANCE_REFRESH_MAX_BACKOFF_MS: i64 = 60_000;

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
    let mut state = state.write();
    state.session_start_ms = now_ms;
    state.history_checkpoints.clear();
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
/// The ledger stores a bounded exact ID cache. When an ID leaves it, the owning order becomes
/// coverage-unknown and must be checked against complete REST history before another cumulative
/// status can be emitted. A lower ID that was never delivered therefore remains distinguishable
/// from an old replay, even when the cache is full.
#[derive(Debug, Default)]
struct AppliedTrades {
    ids: BTreeMap<i64, AppliedTrade>,
    last_ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedTrade {
    venue_order_id: Ustr,
    qty: Decimal,
    ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AppliedTradeResult {
    New,
    Duplicate,
    Evicted(AppliedTrade),
}

impl AppliedTrades {
    fn record(&mut self, trade_id: i64, trade: AppliedTrade) -> AppliedTradeResult {
        if self.ids.contains_key(&trade_id) {
            return AppliedTradeResult::Duplicate;
        }

        self.last_ts_ms = self.last_ts_ms.max(trade.ts_ms);
        self.ids.insert(trade_id, trade);

        let evicted = if self.ids.len() > MAX_TRACKED_TRADE_IDS {
            let oldest = *self.ids.first_key_value().expect("cache is non-empty").0;
            self.ids.remove(&oldest)
        } else {
            None
        };

        evicted.map_or(AppliedTradeResult::New, AppliedTradeResult::Evicted)
    }

    fn contains(&self, trade_id: i64) -> bool {
        self.ids.contains_key(&trade_id)
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
#[derive(Debug, Clone)]
struct DeliveredFill {
    symbol: Ustr,
    trade_id: i64,
}

/// Adds every verified REST row to the recovery ledger candidates.
///
/// A mass-status response is not an acknowledgement from the execution manager. In particular,
/// the manager may filter the order after this method returns. Keeping the complete verified
/// history here ensures rows outside the initial lookback window remain recoverable by ID.
fn append_delivered_fills(
    delivered: &mut Vec<DeliveredFill>,
    symbol: Ustr,
    fills: &[FillReport],
) -> anyhow::Result<()> {
    for fill in fills {
        let trade_id = fill
            .trade_id
            .as_str()
            .parse::<i64>()
            .with_context(|| format!("Aster trade ID {} is not numeric", fill.trade_id))?;
        delivered.push(DeliveredFill { symbol, trade_id });
    }
    Ok(())
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
    /// Venue order IDs that are still working. This lets the bounded trade cache distinguish an
    /// active order, whose evicted ID is a recovery debt, from a terminal order whose exact
    /// lifecycle has already completed and whose fast-path metadata may be reclaimed.
    working_venue_orders: AHashSet<Ustr>,
    /// Applied trade IDs per venue symbol.
    applied_trades: AHashMap<Ustr, AppliedTrades>,
    /// Trade IDs reported but not applied, per symbol.
    ///
    /// The next compensation pass fetches them by ID, so a trade stays recoverable once
    /// whatever blocked it — a missing order, most often — has resolved, however far the
    /// window this pass reads has since moved on.
    pending_trades: AHashMap<Ustr, BTreeSet<i64>>,
    /// Venue orders whose cumulative filled quantity still needs real trade evidence.
    ///
    /// The order ID is retained independently of `working_orders`: a filled order can disappear
    /// from `openOrders` before its user-trade history becomes readable.
    pending_orders: AHashMap<Ustr, Ustr>,
    /// Exact quantities covered by trade IDs already delivered to the engine, keyed by venue
    /// order ID. A cumulative order status is safe only when this amount matches it.
    confirmed_fill_qty: AHashMap<Ustr, Decimal>,
    /// Orders for which the bounded ID cache no longer contains every delivered trade.
    ///
    /// These orders must be verified from complete REST history before another cumulative
    /// status is emitted. The marker is cleared only by that full-history proof.
    coverage_unknown: AHashSet<Ustr>,
    /// Terminal status evidence that this adapter has already accepted through its emitter.
    ///
    /// This is separate from the bounded trade-ID cache: a duplicate terminal status with no
    /// fresh real fills must not be sent after the execution engine purges its closed order and
    /// position cache, or the cumulative quantity would be inferred as a new synthetic fill.
    terminal_delivered_orders: AHashSet<Ustr>,
    /// Millisecond timestamp at which this client connected.
    ///
    /// Compensation never reaches behind it. Fills older than the connect belong to the
    /// engine's own startup reconciliation, and re-delivering them as live session events is
    /// what makes the engine reject an `OrderFilled` for an order it already holds as filled.
    session_start_ms: i64,
    /// End of the last complete, successfully delivered history pass per symbol.
    ///
    /// This is the only compensation watermark. A single WebSocket fill may arrive out of order
    /// relative to an external order that was never in `working_orders`; advancing from that
    /// fill's timestamp would leave the older real trade outside the next query window.
    history_checkpoints: AHashMap<Ustr, i64>,
    /// The conservative upper bound of each asset's available amount, keyed by asset.
    ///
    /// A REST snapshot is the only surface on which the venue states how much of an asset is
    /// available to open new positions, so it seeds this map. A user-data `ACCOUNT_UPDATE`
    /// carries the wallet balance (`wb`) and the cross wallet balance (`cw`), and `cw` is not
    /// spendable cash, so a stream row is merged against this bound and may only tighten it: the
    /// merged `free` is written back, and no later row can raise it again until a newer snapshot
    /// says so.
    balance_bounds: AHashMap<Ustr, AccountBalance>,
    /// The venue event time of the last account update applied, in milliseconds.
    ///
    /// An update older than this one describes a moment the account has already passed, so it
    /// is dropped rather than allowed to move balances backwards.
    last_balance_event_ms: i64,
    /// Whether a stream row is still waiting for a full snapshot to verify it.
    ///
    /// The debt is state, not a return value: it survives the throttle window and a failed
    /// read, so a connection that goes quiet still gets its snapshot.
    owed_balance_refresh: bool,
    /// Consecutive failed owed snapshots, for bounded backoff.
    balance_refresh_failures: u32,
    /// Local millisecond time before which an owed balance snapshot must not be retried.
    ///
    /// A burst of account updates without a verified available amount then costs one account
    /// read, not one per update. A successful read keeps its own gate too: the next update only
    /// marks the debt dirty and waits for this window, so ordinary traffic cannot turn every
    /// update into a REST round trip inside the stream dispatch.
    next_balance_refresh_ms: i64,
    /// Whether an owed snapshot is in flight, so a timer tick and a stream row cannot both read.
    balance_refresh_in_flight: bool,
    /// Bumped whenever the conservative bounds change, whether by a stream row or a snapshot.
    ///
    /// A REST response is committed only if this is unchanged from before its request: a
    /// response that completes later is not a newer reading, and a bound that moved while it was
    /// in flight may be newer than the response.
    balance_epoch: u64,
    /// Bumped whenever an accepted stream row leaves the available amount unverified.
    ///
    /// The balance epoch moves only when the bound's numbers move, which is not the question a
    /// response about to clear the debt has to answer: a row can restate the same wallet balance
    /// and still leave the split unverified, and a response read before that row cannot verify
    /// it. A snapshot request captures this generation, and a response may only clear the debt
    /// while the generation is unchanged.
    balance_refresh_generation: u64,
}

impl StreamState {
    fn track_working_order(&mut self, client_order_id: Ustr, symbol: Ustr) {
        self.working_orders.insert(client_order_id, symbol);
    }

    fn track_working_venue_order(&mut self, venue_order_id: Ustr) {
        self.working_venue_orders.insert(venue_order_id);
    }

    fn forget_working_order(&mut self, client_order_id: &Ustr) {
        self.working_orders.remove(client_order_id);
    }

    fn forget_working_venue_order(&mut self, venue_order_id: &Ustr) {
        self.working_venue_orders.remove(venue_order_id);
    }

    fn note_pending_order(&mut self, venue_order_id: Ustr, symbol: Ustr) {
        self.pending_orders.insert(venue_order_id, symbol);
    }

    fn forget_pending_order(&mut self, venue_order_id: &Ustr) {
        self.pending_orders.remove(venue_order_id);
    }

    fn pending_orders(&self) -> Vec<(Ustr, Ustr)> {
        self.pending_orders
            .iter()
            .map(|(venue_order_id, symbol)| (*venue_order_id, *symbol))
            .collect()
    }

    fn has_recovery_debt(&self) -> bool {
        !self.pending_trades.is_empty()
            || !self.pending_orders.is_empty()
            || self.unresolved_coverage_count() > 0
    }

    /// Returns coverage markers that still belong to an order whose lifecycle is unresolved.
    ///
    /// An eviction from the bounded fast cache is not, by itself, a missing fill. Once a
    /// terminal order has delivered its exact bundle, its cache rows may be reclaimed without
    /// holding the whole session in `Degraded`. An active or pending order is different: a later
    /// status or trade cannot safely be interpreted until the complete order history is read.
    fn unresolved_coverage_count(&self) -> usize {
        self.coverage_unknown
            .iter()
            .filter(|venue_order_id| {
                self.working_venue_orders.contains(venue_order_id)
                    || self.pending_orders.contains_key(venue_order_id)
            })
            .count()
    }

    /// Replaces the conservative bounds with one full account snapshot.
    ///
    /// A snapshot is the complete account, so an asset it does not carry is no longer bounded:
    /// merging a later stream row against a stale entry would carry an available amount the
    /// venue may have withdrawn since. The replacement moves the balance epoch, which is what
    /// lets a concurrent read discover that this snapshot landed while it was in flight.
    fn replace_balance_bounds(&mut self, balances: &[AccountBalance]) {
        self.balance_bounds = balances
            .iter()
            .map(|balance| (Ustr::from(balance.currency.code.as_str()), balance.clone()))
            .collect();
        self.balance_epoch = self.balance_epoch.wrapping_add(1);
    }

    /// Tightens the bounds with the rows one stream update published.
    ///
    /// The published rows already carry the merged, never-raised `free`, so writing them back
    /// is what keeps "only tighten" true across a sequence of updates instead of only against
    /// the last snapshot. A row that changed the bound moves the balance epoch; a row that only
    /// carried the bound forward does not, so an ordinary update does not invalidate a snapshot
    /// that is already in flight.
    fn record_balance_bounds(&mut self, balances: &[AccountBalance]) {
        for balance in balances {
            let key = Ustr::from(balance.currency.code.as_str());
            let changed = self.balance_bounds.get(&key).is_none_or(|current| {
                current.total != balance.total
                    || current.free != balance.free
                    || current.locked != balance.locked
            });
            if changed {
                self.balance_epoch = self.balance_epoch.wrapping_add(1);
            }
            self.balance_bounds.insert(key, balance.clone());
        }
    }

    /// Notes that a stream row had no verified available amount to stand on.
    ///
    /// Every such row moves the refresh generation, including one whose merged numbers match the
    /// current bound: a snapshot read before the row cannot clear the debt the row raises.
    fn note_balance_refresh_owed(&mut self) {
        self.owed_balance_refresh = true;
        self.balance_refresh_generation = self.balance_refresh_generation.wrapping_add(1);
    }

    /// Returns whether the owed snapshot is due at `now_ms`.
    fn balance_refresh_due(&self, now_ms: i64) -> bool {
        self.owed_balance_refresh && now_ms >= self.next_balance_refresh_ms
    }

    /// Clears the debt after a full snapshot was read, and keeps the success gate.
    ///
    /// The gate is not reset to zero: the next stream row marks the debt dirty and waits for the
    /// window, so a successful read does not turn the very next update into another read.
    fn note_balance_refresh_success(&mut self, now_ms: i64) {
        self.owed_balance_refresh = false;
        self.balance_refresh_failures = 0;
        self.next_balance_refresh_ms = now_ms.saturating_add(OWED_BALANCE_REFRESH_INTERVAL_MS);
    }

    /// Keeps the debt and waits longer before the next attempt after a failed read.
    ///
    /// `now_ms` is the moment the read finished, not the moment it started: a slow request that
    /// consumed the whole backoff must not leave the next attempt already due.
    fn note_balance_refresh_failure(&mut self, now_ms: i64) {
        self.balance_refresh_failures = self.balance_refresh_failures.saturating_add(1);
        let shift = (self.balance_refresh_failures - 1).min(4);
        let delay = OWED_BALANCE_REFRESH_INTERVAL_MS
            .saturating_mul(1 << shift)
            .min(OWED_BALANCE_REFRESH_MAX_BACKOFF_MS);
        self.next_balance_refresh_ms = now_ms.saturating_add(delay);
    }

    /// Establishes the debt for another base window without counting a failure.
    ///
    /// A snapshot that was partial, or possibly older than a bound-changing update, did not fail
    /// to arrive: it simply cannot verify the account, so it retries at the base interval rather
    /// than backing off as a transport error would. The debt is established here instead of
    /// assumed: a partial snapshot can be the first thing a fresh state sees, and the timer only
    /// retries a debt that is set.
    fn note_balance_refresh_pending(&mut self, now_ms: i64) {
        self.owed_balance_refresh = true;
        self.next_balance_refresh_ms = now_ms.saturating_add(OWED_BALANCE_REFRESH_INTERVAL_MS);
    }

    /// Commits one REST snapshot and returns the rows to publish.
    ///
    /// `epoch_at_start` and `generation_at_start` are captured before the request was sent. A
    /// response that completes later is not a newer reading: if a bound moved while it was in
    /// flight, or a stream row raised a verification debt it cannot answer for, the response may
    /// predate that update, so only a tightening is applied, the debt stays, and an asset with
    /// no bound is left withheld rather than bounded by a possibly older row. When neither
    /// moved, the snapshot is the new bound; a snapshot that left an asset unknown publishes
    /// what it did state and keeps the debt for what it did not.
    fn commit_balance_snapshot(
        &mut self,
        parsed: &ParsedAccountBalances,
        epoch_at_start: u64,
        generation_at_start: u64,
        now_ms: i64,
    ) -> Vec<AccountBalance> {
        if epoch_at_start != self.balance_epoch
            || generation_at_start != self.balance_refresh_generation
        {
            let mut tightened = Vec::new();
            for row in &parsed.balances {
                let key = Ustr::from(row.currency.code.as_str());
                match self.balance_bounds.get(&key) {
                    Some(bound) if row.free.as_decimal() < bound.free.as_decimal() => {
                        self.balance_bounds.insert(key, row.clone());
                        self.balance_epoch = self.balance_epoch.wrapping_add(1);
                        tightened.push(row.clone());
                    }
                    Some(bound) => tightened.push(bound.clone()),
                    None => {}
                }
            }
            self.note_balance_refresh_pending(now_ms);

            return tightened;
        }

        self.replace_balance_bounds(&parsed.balances);
        if parsed.unverified.is_empty() {
            self.note_balance_refresh_success(now_ms);
        } else {
            self.note_balance_refresh_pending(now_ms);
        }

        parsed.balances.clone()
    }

    fn record_trade(
        &mut self,
        symbol: Ustr,
        venue_order_id: Ustr,
        trade_id: i64,
        ts_ms: i64,
        qty: Decimal,
    ) -> AppliedTradeResult {
        if let Some(pending) = self.pending_trades.get_mut(&symbol) {
            pending.remove(&trade_id);
            if pending.is_empty() {
                self.pending_trades.remove(&symbol);
            }
        }

        let (result, current_retained) = {
            let applied = self.applied_trades.entry(symbol).or_default();
            let result = applied.record(
                trade_id,
                AppliedTrade {
                    venue_order_id,
                    qty,
                    ts_ms,
                },
            );
            let current_retained = applied.contains(trade_id);
            (result, current_retained)
        };

        if let AppliedTradeResult::Evicted(evicted) = &result {
            // The exact metadata of the evicted ID is intentionally not treated as a floor.
            // Mark the order that owned that ID, and if the newly inserted ID itself fell out
            // of the bounded cache (a sparse, late low ID), mark its order as unknown too.
            if !evicted.venue_order_id.as_str().is_empty()
                && (self.working_venue_orders.contains(&evicted.venue_order_id)
                    || self.pending_orders.contains_key(&evicted.venue_order_id))
            {
                self.coverage_unknown.insert(evicted.venue_order_id);
                self.pending_orders.insert(evicted.venue_order_id, symbol);
            }
            if !current_retained
                && !venue_order_id.as_str().is_empty()
                && (self.working_venue_orders.contains(&venue_order_id)
                    || self.pending_orders.contains_key(&venue_order_id))
            {
                self.coverage_unknown.insert(venue_order_id);
                self.pending_orders.insert(venue_order_id, symbol);
            }
        }

        result
    }

    /// Records a fill whose execution report has already been accepted by the emitter.
    ///
    /// The exact-ID cache and the economic contribution have different lifetimes. A new trade
    /// can be accepted by the engine and immediately evicted from the bounded cache when it is
    /// the lowest ID in a full sparse window. In that case the contribution still belongs in the
    /// confirmed aggregate, while the order must become unknown and stay pending so a later
    /// cumulative status cannot manufacture (or replay) the missing trade. `allow_new_coverage`
    /// is true for the remainder of one already-accepted bundle after its first row creates that
    /// marker; callers must pass false when the order was unknown before the bundle was sent.
    fn record_delivered_fill_for_order(
        &mut self,
        symbol: Ustr,
        venue_order_id: Ustr,
        trade_id: i64,
        ts_ms: i64,
        qty: Decimal,
        allow_new_coverage: bool,
    ) -> bool {
        if self.coverage_unknown.contains(&venue_order_id) && !allow_new_coverage {
            self.note_pending_order(venue_order_id, symbol);
            self.note_pending_fill(symbol, trade_id);
            return false;
        }

        let result = self.record_trade(symbol, venue_order_id, trade_id, ts_ms, qty);
        if matches!(result, AppliedTradeResult::Duplicate) {
            return false;
        }

        // The report was already accepted, so retain its exact economic contribution even when
        // record_trade evicted the metadata that would have made a later replay distinguishable.
        *self.confirmed_fill_qty.entry(venue_order_id).or_default() += qty;

        if self.coverage_unknown.contains(&venue_order_id) {
            self.note_pending_order(venue_order_id, symbol);
            self.note_pending_fill(symbol, trade_id);
            return false;
        }

        true
    }

    fn commit_verified_order_history(
        &mut self,
        symbol: Ustr,
        venue_order_id: Ustr,
        fills: &[FillReport],
        filled_qty: Decimal,
    ) {
        for fill in fills {
            let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() else {
                continue;
            };
            self.record_trade(
                symbol,
                venue_order_id,
                trade_id,
                (fill.ts_event.as_u64() / 1_000_000) as i64,
                fill.last_qty.as_decimal(),
            );
        }

        // The complete history is the source of truth for the aggregate, regardless of which
        // individual IDs remain in the bounded fast cache after this commit.
        self.confirmed_fill_qty.insert(venue_order_id, filled_qty);
        self.coverage_unknown.remove(&venue_order_id);
    }

    fn confirmed_fill_qty(&self, venue_order_id: &Ustr) -> Decimal {
        self.confirmed_fill_qty
            .get(venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    /// Marks an order unknown when a later status needs IDs that the bounded cache no longer has.
    ///
    /// Terminal cache eviction alone is harmless: it does not create a debt or prevent a clean
    /// reconnect. The first status/fill that needs the aggregate is the point at which the
    /// missing exact IDs become material. At that point the order is held for a complete history
    /// proof instead of allowing a bare cumulative status to manufacture a trade after the
    /// execution engine has pruned its own order cache.
    fn mark_coverage_unknown_if_incomplete(&mut self, symbol: &Ustr, venue_order_id: Ustr) {
        if self.coverage_unknown.contains(&venue_order_id) {
            return;
        }

        let confirmed = self.confirmed_fill_qty(&venue_order_id);
        if confirmed.is_zero() {
            return;
        }

        let cached = self
            .applied_trades
            .get(symbol)
            .map_or(Decimal::ZERO, |applied| {
                applied
                    .ids
                    .values()
                    .filter(|trade| trade.venue_order_id == venue_order_id)
                    .map(|trade| trade.qty)
                    .sum()
            });
        if cached != confirmed {
            self.coverage_unknown.insert(venue_order_id);
            self.pending_orders.insert(venue_order_id, *symbol);
        }
    }

    fn coverage_unknown(&self, venue_order_id: &Ustr) -> bool {
        self.coverage_unknown.contains(venue_order_id)
    }

    fn terminal_was_delivered(&self, venue_order_id: &Ustr) -> bool {
        self.terminal_delivered_orders.contains(venue_order_id)
    }

    /// Classifies a status while holding the state write lock.
    ///
    /// A repeated terminal status with no fill evidence and the same cumulative quantity is
    /// already accounted for by the accepted terminal bundle. Check that marker before marking
    /// an evicted exact-ID cache as unknown; otherwise an ordinary duplicate after cache
    /// eviction creates a permanent recovery debt before the emitter can suppress it.
    fn inspect_status_coverage(
        &mut self,
        symbol: &Ustr,
        venue_order_id: Ustr,
        is_terminal: bool,
        filled_qty: Decimal,
        has_fill_evidence: bool,
    ) -> (bool, bool, bool) {
        let duplicate_terminal = is_terminal
            && !has_fill_evidence
            && self.terminal_was_delivered(&venue_order_id)
            && self.confirmed_fill_qty(&venue_order_id) == filled_qty;
        if !duplicate_terminal {
            self.mark_coverage_unknown_if_incomplete(symbol, venue_order_id);
        }

        (
            self.coverage_unknown(&venue_order_id),
            self.can_rebase_unknown_order(&venue_order_id),
            duplicate_terminal,
        )
    }

    fn mark_terminal_delivered(&mut self, venue_order_id: Ustr) {
        self.terminal_delivered_orders.insert(venue_order_id);
    }

    fn clear_terminal_delivery(&mut self, venue_order_id: &Ustr) {
        self.terminal_delivered_orders.remove(venue_order_id);
    }

    /// Full-history replay is unambiguous only when no prior quantity for the order was
    /// delivered. Once the bounded cache has evicted an ID from an order with a nonzero
    /// aggregate, this client cannot prove which historical rows the execution engine still
    /// owns (the engine cache may have pruned the order), so it must stay pending rather than
    /// replaying old economics.
    fn can_rebase_unknown_order(&self, venue_order_id: &Ustr) -> bool {
        self.coverage_unknown(venue_order_id) && self.confirmed_fill_qty(venue_order_id).is_zero()
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

    /// Returns the next complete-history checkpoint for `symbol`.
    ///
    /// Before the first successful pass this starts at the connection instant (or the bounded
    /// fallback for an unconnected unit state). A high-timestamp WebSocket row never advances
    /// it, so an external order with an older fill cannot be skipped by a later query.
    fn compensation_start_ms(&self, symbol: &Ustr, default_start_ms: i64) -> i64 {
        self.history_checkpoints
            .get(symbol)
            .copied()
            .unwrap_or(default_start_ms)
            .max(self.session_start_ms)
    }

    /// Advances the symbol checkpoint only after the history request and every missed order in
    /// it were delivered successfully. WebSocket observations never call this method.
    fn advance_history_checkpoint(&mut self, symbol: Ustr, end_ms: i64) {
        self.history_checkpoints
            .insert(symbol, end_ms.max(self.session_start_ms));
    }
}

/// How far a session has come from connecting to being trusted for new risk.
///
/// "Connected" is deliberately not a phase: a socket that is up says nothing about whether the
/// account view behind it is complete, and every path that adds risk depends on that view.
/// `Connecting` and `Stopping` admit nothing; `Reconciling` and `Degraded` admit only actions
/// that provably reduce risk or leave it unchanged; only `Ready` admits new risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadinessPhase {
    /// The session is still being established.
    Connecting,
    /// A recovery pass is running; the account view is not yet verified.
    Reconciling,
    /// The account view is verified and new risk may be sent.
    Ready,
    /// The account view was invalidated; only risk-reducing actions may be sent.
    Degraded,
    /// The client is shutting down.
    Stopping,
}

impl ReadinessPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Reconciling => "reconciling",
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Stopping => "stopping",
        }
    }
}

impl fmt::Display for ReadinessPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The shared execution readiness of the account.
///
/// The phase is what admission decisions read; the generation is what keeps a recovery pass
/// from publishing a readiness it no longer owns. `mark_ready` takes the generation that was
/// current when the pass began, so a pass that finishes after a later one superseded it cannot
/// report the account as verified.
///
/// `mode_verified` is deliberately independent of the phase: the position mode is proven once
/// per session by the venue (or by an explicit exemption), and a recovery pass restores the
/// account view — it does not re-prove the mode. A pass must never promote an account whose
/// mode was never proven, so `mark_ready` requires both conditions.
#[derive(Debug)]
struct Readiness {
    phase: ReadinessPhase,
    generation: u64,
    mode_verified: bool,
    reason: Option<String>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self {
            phase: ReadinessPhase::Connecting,
            generation: 0,
            mode_verified: false,
            reason: Some("the session has not connected".to_string()),
        }
    }
}

impl Readiness {
    fn allows_new_risk(&self) -> bool {
        self.phase == ReadinessPhase::Ready
    }

    fn allows_reduce_only(&self) -> bool {
        matches!(
            self.phase,
            ReadinessPhase::Ready | ReadinessPhase::Reconciling | ReadinessPhase::Degraded
        )
    }

    /// Returns why a submission is refused at the current readiness, if it is.
    fn refusal(&self, reduce_only: bool) -> Option<String> {
        let allowed = if reduce_only {
            self.allows_reduce_only()
        } else {
            self.allows_new_risk()
        };
        if allowed {
            return None;
        }

        let reason = self.reason.as_deref().unwrap_or("no reason recorded");
        Some(format!(
            "Aster execution readiness is {}: {reason}",
            self.phase
        ))
    }

    /// Enters `Connecting` for a new session and returns its generation.
    ///
    /// A new session must prove the position mode again: the venue may have changed it, and the
    /// previous session's proof does not carry over.
    fn begin_connect(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.phase = ReadinessPhase::Connecting;
        self.mode_verified = false;
        self.reason = None;
        self.generation
    }

    /// Enters `Reconciling` for a recovery pass and returns its generation.
    ///
    /// A client that is stopping is not brought back to reconciling: the pass may still run,
    /// but it cannot move the readiness out of `Stopping`.
    fn begin_reconcile(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        if self.phase != ReadinessPhase::Stopping {
            self.phase = ReadinessPhase::Reconciling;
            self.reason = None;
        }
        self.generation
    }

    /// Records that the venue confirmed one-way mode, or that the explicit exemption applies.
    ///
    /// This is the one proof a recovery pass cannot restore; only a connect can establish it.
    fn verify_position_mode(&mut self) {
        self.mode_verified = true;
    }

    /// Marks the account ready when `generation` still owns the readiness and the position mode
    /// was proven for this session.
    ///
    /// A phase that was degraded while the pass ran, a generation a later pass has already
    /// superseded, or a session whose position mode was never proven is not promoted: the
    /// verification belongs to a pass that no longer represents the account.
    fn mark_ready(&mut self, generation: u64) -> bool {
        if !self.mode_verified
            || self.generation != generation
            || !matches!(
                self.phase,
                ReadinessPhase::Connecting | ReadinessPhase::Reconciling
            )
        {
            return false;
        }

        self.phase = ReadinessPhase::Ready;
        self.reason = None;
        true
    }

    /// Invalidates the account view, unless the client is already stopping.
    fn degrade(&mut self, reason: impl Into<String>) {
        if self.phase == ReadinessPhase::Stopping {
            return;
        }

        self.generation = self.generation.wrapping_add(1);
        self.phase = ReadinessPhase::Degraded;
        self.reason = Some(reason.into());
    }

    /// Invalidates the account view because the private transport was lost.
    ///
    /// The generation moves with the invalidation, so a recovery pass that began before the
    /// drop cannot publish readiness afterwards: the socket must come back and a new pass must
    /// verify the account before new risk is admitted.
    fn socket_disconnected(&mut self) {
        if self.phase == ReadinessPhase::Stopping {
            return;
        }

        self.generation = self.generation.wrapping_add(1);
        self.phase = ReadinessPhase::Degraded;
        self.reason = Some("the user data stream socket disconnected".to_string());
    }

    fn stop(&mut self) {
        self.phase = ReadinessPhase::Stopping;
        self.reason = Some("the client is stopping".to_string());
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
    readiness: Arc<RwLock<Readiness>>,
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

    /// Invalidates the account view and records why.
    fn degrade(&self, reason: impl Into<String>) {
        let reason = reason.into();
        log::warn!("Aster execution readiness degraded: {reason}");
        self.readiness.write().degrade(reason);
    }

    /// Runs a recovery pass and publishes readiness from what it could verify.
    ///
    /// The pass owns the readiness it began with: if it cannot verify every part of the
    /// account, the session stays degraded rather than reporting a completeness it did not
    /// reach. A pass superseded by a later one cannot mark the account ready at all.
    async fn recover(&self, reason: &str) {
        let generation = self.readiness.write().begin_reconcile();

        match self.compensate(reason).await {
            Ok(()) => {
                // Hold the state read lock through the readiness transition. A concurrent
                // evidence handler takes state -> readiness while recording debt, so it cannot
                // add a marker between this final check and `mark_ready`.
                let state = self.state.read();
                let mut readiness = self.readiness.write();
                if state.has_recovery_debt() {
                    readiness.degrade(
                        "recovery completed while real order or fill evidence was still pending",
                    );
                } else if readiness.mark_ready(generation) {
                    log::info!("Aster execution readiness restored after {reason}");
                }
            }
            Err(e) => self.degrade(format!("recovery after {reason} failed: {e}")),
        }
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
        let mut state = self.state.write();
        let venue_order_id = report.venue_order_id.inner();
        if report.order_status.is_closed() {
            if let Some(client_order_id) = report.client_order_id {
                state.forget_working_order(&client_order_id.inner());
            }
            state.forget_working_venue_order(&venue_order_id);
        } else {
            // A venue order ID becoming working again is a new lifecycle observation; do not
            // suppress its first later terminal report because an earlier lifecycle used the
            // same ID.
            state.clear_terminal_delivery(&venue_order_id);
            state.track_working_venue_order(venue_order_id);
            if let Some(client_order_id) = report.client_order_id {
                state.track_working_order(client_order_id.inner(), symbol);
            }
        }

        if report.filled_qty.is_zero() {
            state.forget_pending_order(&venue_order_id);
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
                            if self
                                .emit_order_evidence(
                                    venue_order.symbol,
                                    venue_order.order_id,
                                    Some(report),
                                    Vec::new(),
                                    false,
                                )
                                .await
                            {
                                log::info!(
                                    "Resolved ambiguous Aster submission for {client_order_id}"
                                );
                                return;
                            }
                            log::warn!(
                                "Aster found ambiguous submission {client_order_id}, but its \
                                 real fill evidence is still incomplete"
                            );
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
    ///
    /// Returns what the pass could not verify. Every step still runs after an earlier one
    /// failed — a missing trade history must not also cost the account snapshot — but any
    /// failure leaves the pass incomplete, and an incomplete pass is not allowed to report the
    /// account as ready.
    async fn compensate(&self, reason: &str) -> Result<(), String> {
        log::info!("Compensating Aster session state after {reason}");

        // Fills go first, and each one is delivered *with* its order status as a single
        // `OrderWithFills` report. Sending the order status on its own first would hand the
        // engine a terminal quantity with no trades behind it, so it infers a fill of its own;
        // the real trade then arrives against an already-complete order, trips the overfill
        // guard, and is dropped — leaving the order holding a synthetic trade ID and no
        // commission. The economics, not just the log line, depend on this ordering.
        // Snapshot order debt before the fill pass. A newly uncovered trade/order must remain
        // pending until the next bounded recovery pass; retrying it in this same pass would turn
        // a transient order-query failure into a partial bundle and defeat the holdback contract.
        let pending_orders_before = self.state.read().pending_orders();
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

        let mut failure = None;
        let mut fills_uncovered = false;

        if coverage == FillCoverage::Unreliable {
            log::error!(
                "Aster trade history is unavailable after {reason}; order states carrying a \
                 filled quantity are held back until it can be read, so the engine is not \
                 invited to invent the trades behind them"
            );
            failure = Some("trade history is unavailable".to_string());
        } else {
            // A complete trade history is not a complete recovery: an order whose own query or
            // parse failed is held back, and its trades stay pending until a later pass reaches
            // them. Neither is allowed to look like a verified account.
            let pending = self.state.read().pending_trades.len();
            if !fills.uncovered.is_empty() || pending > 0 {
                log::error!(
                    "Aster compensation after {reason} left {} order(s) uncovered and {pending} \
                     trade(s) pending; the account stays unverified until they are recovered",
                    fills.uncovered.len(),
                );
                fills_uncovered = true;
            }
        }

        if let Err(e) = self
            .compensate_orders(&fills, coverage, &pending_orders_before)
            .await
        {
            log::error!("Aster order compensation after {reason} failed: {e}");
            failure.get_or_insert_with(|| format!("order compensation failed: {e}"));
        }
        if let Err(e) = self.refresh_account_state().await {
            log::error!("Aster balance refresh after {reason} failed: {e}");
            failure.get_or_insert_with(|| format!("balance refresh failed: {e}"));
        }
        let infer_flat = {
            let state = self.state.read();
            coverage == FillCoverage::Complete
                && failure.is_none()
                && !fills_uncovered
                && state.pending_trades.is_empty()
                && state.pending_orders.is_empty()
                && state.unresolved_coverage_count() == 0
        };
        if let Err(e) = self.refresh_positions(infer_flat).await {
            log::error!("Aster position refresh after {reason} failed: {e}");
            failure.get_or_insert_with(|| format!("position refresh failed: {e}"));
        }

        // The state may have changed while the account, position, or targeted order requests
        // were in flight. Read the live debt unconditionally at the end of the pass instead of
        // trusting the earlier fill snapshot; readiness must never be restored over a debt that
        // was raised during recovery.
        let (pending_trades, pending_orders, unknown_coverage) = {
            let state = self.state.read();
            (
                state.pending_trades.len(),
                state.pending_orders.len(),
                state.unresolved_coverage_count(),
            )
        };
        if pending_trades > 0 || pending_orders > 0 || unknown_coverage > 0 {
            failure.get_or_insert_with(|| {
                format!(
                    "{} uncovered order(s), {pending_trades} pending trade(s), and \
                     {pending_orders} pending order(s) ({unknown_coverage} unknown coverage)",
                    fills.uncovered.len(),
                )
            });
        } else if fills_uncovered {
            // Keep the conservative diagnostic if a future implementation records an uncovered
            // result without retaining a marker, while still returning success only for a clean
            // live state.
            failure.get_or_insert_with(|| {
                format!(
                    "{} uncovered order(s) remain unresolved",
                    fills.uncovered.len()
                )
            });
        }

        failure.map_or(Ok(()), Err)
    }

    /// Reports the venue's open orders, then resolves every order this client still believes is
    /// working but the venue no longer lists (filled or cancelled during the outage).
    async fn compensate_orders(
        &self,
        fills: &CompensatedFills,
        coverage: FillCoverage,
        pending_orders_before: &[(Ustr, Ustr)],
    ) -> anyhow::Result<()> {
        let open_orders = self
            .http_client
            .query_open_orders(None)
            .await
            .map_err(|e| anyhow::anyhow!("Aster open order query failed: {e}"))?;

        let ts_init = self.clock.get_time_ns();
        let mut still_open: AHashSet<Ustr> = AHashSet::new();
        // An order the venue holds but this pass cannot interpret leaves the account view
        // incomplete, so it remains pending and is reported as a failure if it is still pending
        // after the targeted recovery pass below.
        let mut failure: Option<String> = None;

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
                        self.hold_order_evidence(
                            order.symbol,
                            Ustr::from(&order.order_id.to_string()),
                            &[],
                            "the compensation fill pass did not prove this cumulative status",
                        );
                        log::warn!(
                            "Aster open order {} status remains pending because its fills are \
                             uncovered",
                            report.venue_order_id
                        );
                        continue;
                    }
                    if !self
                        .emit_order_evidence(
                            order.symbol,
                            order.order_id,
                            Some(report),
                            Vec::new(),
                            false,
                        )
                        .await
                    {
                        log::warn!(
                            "Aster open order {} remains pending until its real fills are read",
                            order.order_id
                        );
                    }
                }
                Ok(None) => log::debug!(
                    "Ignoring Aster open order on unloaded symbol {}",
                    order.symbol
                ),
                Err(e) => {
                    log::error!("Failed to parse Aster open order {}: {e}", order.order_id);
                    failure.get_or_insert_with(|| {
                        format!("failed to parse open order {}: {e}", order.order_id)
                    });
                }
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
                            self.hold_order_evidence(
                                order.symbol,
                                Ustr::from(&order.order_id.to_string()),
                                &[],
                                "the compensation fill pass did not prove this cumulative status",
                            );
                            log::warn!(
                                "Aster vanished order {} status remains pending because its \
                                 fills are uncovered",
                                report.venue_order_id
                            );
                            continue;
                        }
                        if !self
                            .emit_order_evidence(
                                order.symbol,
                                order.order_id,
                                Some(report),
                                Vec::new(),
                                false,
                            )
                            .await
                        {
                            log::warn!(
                                "Aster vanished order {client_order_id} remains pending until \
                                 its real fills are read"
                            );
                        }
                    }
                    Ok(None) => log::debug!("Ignoring Aster order on unloaded symbol {symbol}"),
                    Err(e) => {
                        log::error!("Failed to parse Aster order {client_order_id}: {e}");
                        failure.get_or_insert_with(|| {
                            format!("failed to parse order {client_order_id}: {e}")
                        });
                    }
                },
                Err(e) if e.is_unknown_order() => {
                    log::warn!("Aster no longer knows order {client_order_id}; dropping it");
                    self.state.write().forget_working_order(&client_order_id);
                }
                Err(e) => {
                    log::error!("Aster order query failed for {client_order_id}: {e}");
                    failure.get_or_insert_with(|| {
                        format!("order query failed for {client_order_id}: {e}")
                    });
                }
            }
        }

        // A filled order can disappear from openOrders before its user-trade history becomes
        // readable. Keep retrying every such order by venue ID instead of letting the bounded
        // open-order pass forget the cumulative status and manufacture a fill.
        for &(venue_order_id, symbol) in pending_orders_before {
            if fills.delivered.contains(&venue_order_id) {
                // The fill pass already emitted this order together with its real trades. A
                // second status-only query would duplicate terminal evidence and could make
                // consumers infer a synthetic fill from the cumulative quantity.
                continue;
            }

            let Ok(order_id) = venue_order_id.as_str().parse::<i64>() else {
                failure.get_or_insert_with(|| {
                    format!("pending Aster order ID {venue_order_id} is not numeric")
                });
                continue;
            };

            match self
                .http_client
                .query_order(symbol.as_str(), Some(order_id), None)
                .await
            {
                Ok(order) => match self.order_to_report(&order, self.clock.get_time_ns()) {
                    Ok(Some(report)) => {
                        if !self
                            .emit_order_evidence(symbol, order_id, Some(report), Vec::new(), false)
                            .await
                        {
                            log::warn!("Aster pending order {venue_order_id} remains uncovered");
                        }
                    }
                    Ok(None) => {
                        failure.get_or_insert_with(|| {
                            format!("pending order {venue_order_id} has an unloaded symbol")
                        });
                    }
                    Err(e) => {
                        failure.get_or_insert_with(|| {
                            format!("failed to parse pending order {venue_order_id}: {e}")
                        });
                    }
                },
                Err(e) => {
                    failure.get_or_insert_with(|| {
                        format!("pending order query failed for {venue_order_id}: {e}")
                    });
                }
            }
        }

        let pending_count = self.state.read().pending_orders().len();
        if pending_count > 0 {
            failure.get_or_insert_with(|| {
                format!("{pending_count} Aster order(s) still need real fill evidence")
            });
        }

        failure.map_or(Ok(()), |e| Err(anyhow::anyhow!(e)))
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
                if self.state.read().pending_trade_ids(&symbol).is_empty() {
                    self.state
                        .write()
                        .advance_history_checkpoint(symbol, now_ms);
                }
                continue;
            }

            let mut by_order: AHashMap<i64, Vec<&AsterUserTrade>> = AHashMap::new();
            for trade in missed {
                by_order.entry(trade.order_id).or_default().push(trade);
            }

            let mut symbol_complete = true;
            for (venue_order_id, trades) in by_order {
                let outcome = self
                    .emit_missed_fills(&symbol, venue_order_id, &trades)
                    .await;
                if !outcome.uncovered.is_empty() {
                    symbol_complete = false;
                }
                recovered.merge(outcome);
            }

            if symbol_complete && self.state.read().pending_trade_ids(&symbol).is_empty() {
                self.state
                    .write()
                    .advance_history_checkpoint(symbol, now_ms);
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

        // A partial parse is evidence that the history is incomplete, so the shared resolver
        // must read the order and its trades again before anything is published. This also
        // handles a status query failure and checks the cumulative quantity instead of trusting
        // a bare status.
        let status = if unparsable == 0 && !fills.is_empty() {
            self.query_order_report(symbol, venue_order_id, ts_init)
                .await
        } else {
            None
        };
        let status_client_order_id = status
            .as_ref()
            .and_then(|report| report.client_order_id.map(|id| id.inner()));
        let fill_count = fills.len();
        let reports: Vec<FillReport> = fills.into_iter().map(|(report, _, _)| report).collect();
        let delivered = self
            .emit_order_evidence(*symbol, venue_order_id, status, reports, unparsable > 0)
            .await;

        if delivered {
            outcome
                .delivered
                .insert(Ustr::from(&venue_order_id.to_string()));
            if let Some(client_order_id) = status_client_order_id {
                outcome.delivered.insert(client_order_id);
            }
            log::info!(
                "Applied {fill_count} Aster fills missed by the user stream for order {venue_order_id}",
            );
        } else {
            // Keep every source trade pending, including rows that failed to parse. A later
            // pass addresses pending IDs directly and can therefore recover one outside the
            // moving time window once the venue returns a complete row.
            let mut state = self.state.write();
            for trade in trades {
                state.note_pending_fill(*symbol, trade.id);
            }
            outcome
                .uncovered
                .insert(Ustr::from(&venue_order_id.to_string()));
        }

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
            Ok(Some(report)) => Some(report),
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

    /// Fetches real trades for one order so a cumulative status can be checked against evidence.
    async fn query_order_fills(
        &self,
        symbol: &Ustr,
        venue_order_id: i64,
    ) -> anyhow::Result<Vec<FillReport>> {
        let Some(context) = self.context_for(symbol) else {
            anyhow::bail!("Aster symbol {symbol} is not loaded");
        };

        let now_ms = self.now_ms();
        // A direct order/status query may refer to an order older than the live compensation
        // window. Use the venue's largest supported history window here; if the order predates
        // it, the exact status is deliberately reported as incomplete rather than fabricated.
        let start_ms = now_ms.saturating_sub(DEFAULT_REPORT_LOOKBACK_MS);
        let trades = self
            .query_user_trades_paged(symbol.as_str(), start_ms, now_ms)
            .await?;

        trades
            .iter()
            .filter(|trade| trade.order_id == venue_order_id)
            .map(|trade| {
                trade
                    .to_fill_report(
                        self.account_id,
                        context.instrument_id,
                        context.price_precision,
                        context.size_precision,
                        AsterExecutionClient::settlement_currency(),
                        self.clock.get_time_ns(),
                    )
                    .with_context(|| {
                        format!("Failed to parse Aster trade {} on {symbol}", trade.id)
                    })
            })
            .collect()
    }

    /// Returns every real fill in `fills` for `report` when their exact quantities explain the
    /// venue's cumulative status. This is the recovery path after the bounded ID cache has
    /// evicted a trade: the cache is no longer allowed to decide whether a repeated ID is new,
    /// so the complete order history becomes the source of truth and the engine receives the
    /// whole bundle again for its own exact trade-ID reconciliation.
    fn complete_order_fills_for_status(
        report: &OrderStatusReport,
        fills: &[FillReport],
    ) -> anyhow::Result<Option<Vec<FillReport>>> {
        let mut seen_trade_ids = AHashSet::new();
        let mut complete = Vec::new();
        let mut covered_qty = Decimal::ZERO;

        for fill in fills {
            if fill.venue_order_id != report.venue_order_id {
                continue;
            }

            let trade_id: i64 = fill
                .trade_id
                .as_str()
                .parse()
                .with_context(|| format!("Aster trade ID {} is not numeric", fill.trade_id))?;
            if seen_trade_ids.insert(trade_id) {
                covered_qty += fill.last_qty.as_decimal();
                complete.push(fill.clone());
            }
        }

        if covered_qty == report.filled_qty.as_decimal() {
            Ok(Some(complete))
        } else {
            Ok(None)
        }
    }

    /// Returns fresh fills and whether they cover the status cumulative quantity.
    fn fresh_fills_for_status(
        &self,
        symbol: &Ustr,
        report: &OrderStatusReport,
        fills: &[FillReport],
    ) -> anyhow::Result<(Vec<FillReport>, bool)> {
        let venue_order_id = report.venue_order_id.inner();
        let state = self.state.read();
        if state.coverage_unknown(&venue_order_id) {
            // An ID may have left the bounded cache. The persistent aggregate is not enough to
            // decide whether a later trade is a replay, so only complete order history can clear
            // this status; callers use `complete_order_fills_for_status` for that proof.
            return Ok((Vec::new(), false));
        }
        let mut covered_qty = state.confirmed_fill_qty(&venue_order_id);
        let mut seen_trade_ids = AHashSet::new();
        let mut fresh = Vec::new();

        for fill in fills {
            if fill.venue_order_id != report.venue_order_id {
                continue;
            }

            let trade_id: i64 = fill
                .trade_id
                .as_str()
                .parse()
                .with_context(|| format!("Aster trade ID {} is not numeric", fill.trade_id))?;
            if !seen_trade_ids.insert(trade_id) {
                continue;
            }

            let known = state.has_fill(symbol, trade_id);
            if !known {
                covered_qty += fill.last_qty.as_decimal();
                fresh.push(fill.clone());
            }
        }

        // A status is safe only when the known and newly read real trades explain its exact
        // cumulative quantity. Both an under-report and an over-report indicate that one side
        // of the evidence is stale or incomplete, and either side would make the execution
        // engine invent or discard economics.
        Ok((fresh, covered_qty == report.filled_qty.as_decimal()))
    }

    /// Resolves an order status and its real fills before any cumulative quantity is emitted.
    ///
    /// A direct stream fill is used when it proves the status. Otherwise the order's REST trade
    /// history is read. A status that still lacks enough real quantity remains pending so the
    /// execution engine cannot manufacture a fill with a lost trade ID or commission.
    async fn resolve_order_evidence(
        &self,
        symbol: &Ustr,
        venue_order_id: i64,
        report: Option<OrderStatusReport>,
        direct_fills: &[FillReport],
        require_trade_evidence: bool,
    ) -> anyhow::Result<Option<(OrderStatusReport, Vec<FillReport>, bool)>> {
        let report = match report {
            Some(report) => report,
            // `emit_missed_fills` has already attempted the order query for a non-empty trade
            // batch. Retrying that same query in the order-evidence helper would turn a
            // transient failure into a partially applied pass, publishing the bundle earlier
            // than the compensation retry contract allows. A malformed stream update, on the
            // other hand, has no direct fills and still needs the REST repair here.
            None if !require_trade_evidence && !direct_fills.is_empty() => return Ok(None),
            None => match self
                .query_order_report(symbol, venue_order_id, self.clock.get_time_ns())
                .await
            {
                Some(report) => report,
                None => return Ok(None),
            },
        };

        let (coverage_unknown, can_rebase_unknown, duplicate_terminal) = {
            let mut state = self.state.write();
            let venue_order_id_str = report.venue_order_id.inner();
            state.inspect_status_coverage(
                symbol,
                venue_order_id_str,
                report.order_status.is_closed(),
                report.filled_qty.as_decimal(),
                !direct_fills.is_empty(),
            )
        };
        if duplicate_terminal {
            return Ok(Some((report, Vec::new(), false)));
        }
        if coverage_unknown {
            if !can_rebase_unknown {
                // Historical rows may already have been applied while the engine's order cache
                // was later pruned. Replaying the complete history would recreate old fees and
                // position quantity, so fail closed until a lifecycle-level reconciliation can
                // prove ownership of those IDs.
                return Ok(None);
            }
            // Once the bounded cache has evicted any trade for this order, neither the stored
            // aggregate nor a status alone can distinguish a replay from a late sparse ID. Read
            // the complete order history and rebase the aggregate only from that exact bundle.
            let queried = match self.query_order_fills(symbol, venue_order_id).await {
                Ok(queried) => queried,
                Err(e) => {
                    self.hold_order_evidence(
                        *symbol,
                        report.venue_order_id.inner(),
                        &[],
                        &e.to_string(),
                    );
                    return Err(e);
                }
            };
            let Some(complete) = Self::complete_order_fills_for_status(&report, &queried)? else {
                return Ok(None);
            };
            return Ok(Some((report, complete, true)));
        }

        // A REST status with zero cumulative quantity is complete evidence that no fill exists.
        // It is also the repair that clears a pending marker left by a malformed zero-fill
        // stream update, so it must not remain blocked by the stream-side parse failure.
        if require_trade_evidence && report.filled_qty.is_zero() && direct_fills.is_empty() {
            return Ok(Some((report, Vec::new(), false)));
        }

        let (fresh, covered) = self.fresh_fills_for_status(symbol, &report, direct_fills)?;
        if covered {
            return Ok(Some((report, fresh, false)));
        }

        let queried = self.query_order_fills(symbol, venue_order_id).await?;
        let (fresh, covered) = self.fresh_fills_for_status(symbol, &report, &queried)?;
        if covered {
            // This is the ordinary bounded path: any rows already represented in the exact
            // cache have reached the engine before. Only the genuinely fresh rows belong in
            // this report. Replaying the complete REST history here would duplicate fees and
            // position quantity after the engine later purges its order cache. The only safe
            // full-history rebase is the explicit zero-delivery unknown-coverage branch above.
            return Ok(Some((report, fresh, false)));
        }

        Ok(None)
    }

    /// Verifies that a status report can be returned without sending a second, fill-inferring
    /// event. A report request has no place to carry newly discovered fills, so a status whose
    /// quantity is explained only by fresh REST trades must stay pending for the fill report
    /// path instead.
    async fn ensure_status_covered(
        &self,
        symbol: &Ustr,
        report: &OrderStatusReport,
    ) -> anyhow::Result<()> {
        let (coverage_unknown, can_rebase_unknown, duplicate_terminal) = {
            let mut state = self.state.write();
            let venue_order_id = report.venue_order_id.inner();
            state.inspect_status_coverage(
                symbol,
                venue_order_id,
                report.order_status.is_closed(),
                report.filled_qty.as_decimal(),
                false,
            )
        };
        if duplicate_terminal {
            return Ok(());
        }
        if coverage_unknown {
            if !can_rebase_unknown {
                self.hold_order_evidence(
                    *symbol,
                    report.venue_order_id.inner(),
                    &[],
                    "bounded trade coverage was evicted after prior fills; engine ownership is unproven",
                );
                anyhow::bail!(
                    "Aster order {} cannot safely replay evicted fills",
                    report.venue_order_id
                );
            }
            let venue_order_id: i64 =
                report.venue_order_id.as_str().parse().with_context(|| {
                    format!(
                        "Aster venue order ID {} is not numeric",
                        report.venue_order_id
                    )
                })?;
            let queried = match self.query_order_fills(symbol, venue_order_id).await {
                Ok(queried) => queried,
                Err(e) => {
                    self.hold_order_evidence(
                        *symbol,
                        report.venue_order_id.inner(),
                        &[],
                        &e.to_string(),
                    );
                    return Err(e);
                }
            };
            let complete = Self::complete_order_fills_for_status(report, &queried)?;
            self.hold_order_evidence(
                *symbol,
                report.venue_order_id.inner(),
                complete.as_deref().unwrap_or(&[]),
                "bounded trade coverage was evicted; status requires a bundled full-history fill report",
            );
            anyhow::bail!(
                "Aster order {} requires full-history fills before a bare status can be emitted",
                report.venue_order_id
            );
        }

        let (fresh, covered) = self.fresh_fills_for_status(symbol, report, &[])?;
        if covered && fresh.is_empty() {
            return Ok(());
        }

        let venue_order_id: i64 = report.venue_order_id.as_str().parse().with_context(|| {
            format!(
                "Aster venue order ID {} is not numeric",
                report.venue_order_id
            )
        })?;
        let queried = match self.query_order_fills(symbol, venue_order_id).await {
            Ok(queried) => queried,
            Err(e) => {
                self.hold_order_evidence(
                    *symbol,
                    report.venue_order_id.inner(),
                    &[],
                    &e.to_string(),
                );
                return Err(e);
            }
        };
        let (fresh, covered) = self.fresh_fills_for_status(symbol, report, &queried)?;
        if covered && fresh.is_empty() {
            return Ok(());
        }

        self.hold_order_evidence(
            *symbol,
            report.venue_order_id.inner(),
            &fresh,
            "a status report found fills that have not yet been delivered",
        );
        anyhow::bail!(
            "Aster order {} has cumulative quantity without delivered real fills",
            report.venue_order_id
        )
    }

    /// Retains unresolved order/fill evidence for a later compensation pass.
    fn hold_order_evidence(
        &self,
        symbol: Ustr,
        venue_order_id: Ustr,
        fills: &[FillReport],
        reason: &str,
    ) {
        let mut state = self.state.write();
        state.note_pending_order(venue_order_id, symbol);
        for fill in fills {
            if let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() {
                state.note_pending_fill(symbol, trade_id);
            }
        }

        // Keep state -> readiness ordering with `recover`: the debt marker and the generation
        // invalidation become one critical section from the point of view of readiness.
        let reason =
            format!("real Aster fill evidence for order {venue_order_id} is incomplete: {reason}");
        log::warn!("Aster execution readiness degraded: {reason}");
        self.readiness.write().degrade(reason);
    }

    /// Emits an order status only after its real fills are proven and deduplicated.
    async fn emit_order_evidence(
        &self,
        symbol: Ustr,
        venue_order_id: i64,
        report: Option<OrderStatusReport>,
        direct_fills: Vec<FillReport>,
        require_trade_evidence: bool,
    ) -> bool {
        let evidence = self
            .resolve_order_evidence(
                &symbol,
                venue_order_id,
                report,
                &direct_fills,
                require_trade_evidence,
            )
            .await;

        let Some((report, fills, full_history)) = (match evidence {
            Ok(evidence) => evidence,
            Err(e) => {
                self.hold_order_evidence(
                    symbol,
                    Ustr::from(&venue_order_id.to_string()),
                    &direct_fills,
                    &e.to_string(),
                );
                return false;
            }
        }) else {
            self.hold_order_evidence(
                symbol,
                Ustr::from(&venue_order_id.to_string()),
                &direct_fills,
                "the venue has not returned enough real trades",
            );
            return false;
        };

        // A REST order row can be stamped after an earlier trade in the same bundle (and an
        // out-of-order WS/REST repair makes this common). The execution state machine orders
        // reports by acceptance before applying fills, so pull acceptance back to the earliest
        // real fill before sending the atomic bundle.
        let mut report = report;
        let fill_refs: Vec<&FillReport> = fills.iter().collect();
        align_report_with_fills(&mut report, &fill_refs);
        let venue_order_id_str = report.venue_order_id.inner();
        let (_deliverable, send_error) = {
            let mut state = self.state.write();
            if !full_history && state.coverage_unknown(&venue_order_id_str) {
                state.note_pending_order(venue_order_id_str, symbol);
                for fill in &fills {
                    if let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() {
                        state.note_pending_fill(symbol, trade_id);
                    }
                }
                let reason = format!(
                    "real Aster fill evidence for order {venue_order_id_str} is incomplete: \
                     bounded trade coverage was evicted while applying this bundle"
                );
                log::warn!("Aster execution readiness degraded: {reason}");
                self.readiness.write().degrade(reason);
                (
                    Vec::new(),
                    Some("bounded trade coverage was evicted".to_string()),
                )
            } else {
                let deliverable = if full_history {
                    // The bounded cache is only a fast path. Once coverage was lost, rebase the
                    // aggregate from the complete history and send every real fill; the
                    // execution engine deduplicates repeated IDs without changing economics.
                    fills.clone()
                } else {
                    fills
                        .iter()
                        .filter(|fill| {
                            fill.trade_id
                                .as_str()
                                .parse::<i64>()
                                .is_ok_and(|trade_id| !state.has_fill(&symbol, trade_id))
                        })
                        .cloned()
                        .collect()
                };
                if report.order_status.is_closed()
                    && deliverable.is_empty()
                    && state.terminal_was_delivered(&venue_order_id_str)
                {
                    // The first terminal report was accepted by the emitter already. A later
                    // identical status carries no fresh real fill, so sending it after the
                    // engine purges its closed order would make the cumulative quantity look
                    // like a new synthetic fill.
                    log::debug!(
                        "Suppressing duplicate terminal Aster evidence for order {venue_order_id_str}"
                    );
                    (Vec::new(), None)
                } else {
                    let execution_report = if deliverable.is_empty() {
                        ExecutionReport::Order(Box::new(report.clone()))
                    } else {
                        ExecutionReport::OrderWithFills(
                            Box::new(report.clone()),
                            deliverable.clone(),
                        )
                    };

                    match self.emitter.try_send_execution_report(execution_report) {
                        Ok(()) => {
                            if full_history {
                                state.commit_verified_order_history(
                                    symbol,
                                    venue_order_id_str,
                                    &fills,
                                    report.filled_qty.as_decimal(),
                                );
                            } else {
                                // The complete bundle was accepted before this bookkeeping runs.
                                // Keep recording later rows even if the first row makes the bounded
                                // cache unknown; their economic contributions have already reached
                                // the engine and must not be mistaken for zero-delivery history.
                                for fill in &deliverable {
                                    let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() else {
                                        continue;
                                    };
                                    state.record_delivered_fill_for_order(
                                        symbol,
                                        venue_order_id_str,
                                        trade_id,
                                        (fill.ts_event.as_u64() / 1_000_000) as i64,
                                        fill.last_qty.as_decimal(),
                                        true,
                                    );
                                }
                            }

                            if report.order_status.is_closed() {
                                state.mark_terminal_delivered(venue_order_id_str);
                            }

                            // A successful send can itself create a new active-order coverage debt
                            // when a sparse/late ID is evicted. Preserve that marker and degrade
                            // readiness before releasing the state lock; never clear it as if the
                            // order had been fully proven.
                            if !state.coverage_unknown(&venue_order_id_str) {
                                state.forget_pending_order(&venue_order_id_str);
                            }
                            if state.has_recovery_debt() {
                                let reason = format!(
                                    "real Aster fill evidence for order {venue_order_id_str} remains \
                                     incomplete after delivery"
                                );
                                log::warn!("Aster execution readiness degraded: {reason}");
                                self.readiness.write().degrade(reason);
                            }
                            (deliverable, None)
                        }
                        Err(e) => {
                            // The report was not accepted by the execution channel. Keep the whole
                            // evidence bundle pending and invalidate readiness while state is still
                            // held, so a later recovery can retry it without losing economics.
                            state.note_pending_order(venue_order_id_str, symbol);
                            for fill in &fills {
                                if let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() {
                                    state.note_pending_fill(symbol, trade_id);
                                }
                            }
                            let reason = format!(
                                "real Aster fill evidence for order {venue_order_id_str} could not be \
                                 delivered: {e}"
                            );
                            log::warn!("Aster execution readiness degraded: {reason}");
                            self.readiness.write().degrade(reason);
                            (Vec::new(), Some(e.to_string()))
                        }
                    }
                }
            }
        };

        if let Some(error) = send_error {
            log::warn!(
                "Aster order {venue_order_id_str} remains pending after execution report send \
                 failure: {error}"
            );
            return false;
        }

        self.track_order_state(&report, symbol);

        true
    }

    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        let (epoch_at_start, generation_at_start) = {
            let state = self.state.read();
            (state.balance_epoch, state.balance_refresh_generation)
        };
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        let parsed = parse_account_balances(&balances);
        // This snapshot seeds the conservative bounds every later stream update tightens, and
        // verifies whatever a stream row had left owed - unless a bound moved while it was in
        // flight, in which case only a tightening is applied and the debt stays.
        let (published, verification_pending) = {
            let mut state = self.state.write();
            let published = state.commit_balance_snapshot(
                &parsed,
                epoch_at_start,
                generation_at_start,
                self.now_ms(),
            );
            (published, state.owed_balance_refresh)
        };
        self.emitter.emit_account_state(
            published,
            Vec::new(),
            true, // reported
            self.clock.get_time_ns(),
            None, // info
        );

        anyhow::ensure!(
            parsed.unverified.is_empty(),
            "Aster balance snapshot did not verify available balances for {:?}",
            parsed.unverified,
        );
        anyhow::ensure!(
            !verification_pending,
            "Aster balance snapshot was superseded before it could verify the account",
        );
        Ok(())
    }

    /// Reports every loaded instrument's position, including flat ones.
    ///
    /// A position closed during the outage must be reported as flat, otherwise the engine keeps
    /// the stale quantity.
    async fn refresh_positions(&self, infer_flat: bool) -> anyhow::Result<()> {
        let mut scope = self.instruments.read().snapshot();
        let positions = self
            .http_client
            .query_position_risk(None)
            .await
            .map_err(|e| anyhow::anyhow!("Aster position risk query failed: {e}"))?;

        // Neither newly loaded nor unloaded instruments are covered by this in-flight request.
        retain_position_scope(&mut scope, &self.instruments.read());
        let parsed = parse_position_snapshot(
            &positions,
            &scope,
            self.account_id,
            self.clock.get_time_ns(),
            infer_flat,
        );
        for report in parsed.reports {
            self.emitter.send_position_report(report);
        }
        parsed.failure.map_or(Ok(()), Err)
    }
}

/// What the venue proved about the account's position mode at connect.
enum PositionMode {
    /// The venue confirmed the account trades one-way.
    OneWay,
    /// The venue definitively does not expose the mode, so trading it as one-way is unproven.
    Unconfirmed(String),
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
    readiness: Arc<RwLock<Readiness>>,
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
        Self::new_with_clock(core, config, get_atomic_clock_realtime())
    }

    /// Creates an Aster execution client with the supplied clock.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials cannot be resolved or an HTTP client cannot be built.
    pub fn new_with_clock(
        core: ExecutionClientCore,
        config: AsterExecutionClientConfig,
        clock: &'static AtomicTime,
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
            clock,
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
            readiness: Arc::new(RwLock::new(Readiness::default())),
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
            readiness: self.readiness.clone(),
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

    /// Returns whether the account view is verified and the client admits new risk.
    ///
    /// A connected client is not necessarily ready: an unconfirmed position mode, a lost
    /// private stream, or a recovery pass that could not verify the account all leave new risk
    /// refused while cancellations, queries, and provably reduce-only orders stay available.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.readiness.read().allows_new_risk()
    }

    /// Returns the current readiness phase name for diagnostics.
    #[must_use]
    pub fn readiness_phase(&self) -> &'static str {
        self.readiness.read().phase.as_str()
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
    /// compensation pass skips it as already applied. Only an accepted emitter bundle may
    /// commit them.
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
                    trade_id: trade.id,
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

    /// Resolves the account's position mode, refusing to guess when it cannot be proven.
    ///
    /// Every position and order path in this adapter assumes one-way mode; silently trading a
    /// hedge-mode account would mis-attribute positions.
    ///
    /// The check fails closed. A transport fault, a `5xx`, a rate limit, or an authentication
    /// failure does not prove anything about the mode, and the next connect attempt can ask
    /// again, so it aborts this one. A definitive venue answer that the endpoint is unavailable
    /// — an unknown endpoint, an invalid parameter — is the same on every attempt and says
    /// nothing about the account either, so it leaves the mode *unconfirmed*: the session may
    /// come up for account management, but it is not treated as a one-way account.
    async fn resolve_position_mode(&self) -> anyhow::Result<PositionMode> {
        match self.http_client.query_position_mode().await {
            Ok(mode) => {
                anyhow::ensure!(
                    !mode.dual_side_position,
                    "Aster account is in hedge (dual-side) position mode, which this adapter does \
                     not support; switch the account to one-way mode"
                );
                Ok(PositionMode::OneWay)
            }
            Err(e) if e.is_auth_failure() => Err(anyhow::anyhow!(
                "Aster rejected the first signed request: {e}"
            )),
            // A venue that answers with a decision of its own does not expose the mode and never
            // will. A rate limit is a structured answer too, but it is a "try again", not a
            // decision, so it is excluded here.
            Err(e) if e.is_venue_rejection() && !e.is_rate_limited() => {
                Ok(PositionMode::Unconfirmed(e.to_string()))
            }
            Err(e) => Err(anyhow::anyhow!(
                "Aster position mode could not be confirmed, so one-way mode cannot be \
                 assumed: {e}"
            )),
        }
    }

    async fn fetch_account_state(
        &self,
    ) -> anyhow::Result<(Vec<AccountBalance>, Vec<MarginBalance>, Vec<Ustr>)> {
        let (epoch_at_start, generation_at_start) = {
            let state = self.stream_state.read();
            (state.balance_epoch, state.balance_refresh_generation)
        };
        let balances = self
            .http_client
            .query_balances()
            .await
            .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

        // Aster's `/fapi/v3/balance` reports wallet balances only; per-asset initial and
        // maintenance margin are not part of the payload, so no margin balances are emitted.
        let parsed = parse_account_balances(&balances);
        // This snapshot seeds the conservative bounds every later stream update tightens, and
        // verifies whatever a stream row had left owed - unless a bound moved while it was in
        // flight, in which case only a tightening is applied and the debt stays.
        let published = {
            let mut state = self.stream_state.write();
            state.commit_balance_snapshot(
                &parsed,
                epoch_at_start,
                generation_at_start,
                (self.clock.get_time_ns().as_u64() / 1_000_000) as i64,
            )
        };
        Ok((published, Vec::new(), parsed.unverified))
    }

    async fn emit_account_state(&self) -> anyhow::Result<()> {
        let (balances, margins, unverified) = self.fetch_account_state().await?;

        if balances.is_empty() {
            log::warn!("Aster account reports no non-zero balances");
        }

        let ts_event = self.clock.get_time_ns();
        self.emitter
            .emit_account_state(balances, margins, true, ts_event, None);

        anyhow::ensure!(
            unverified.is_empty(),
            "Aster initial balance snapshot did not verify available balances for {unverified:?}",
        );
        Ok(())
    }

    /// Reconciles open orders at connect so externally placed orders are known to the engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the venue cannot list its open orders; the caller degrades readiness
    /// rather than reporting the account as verified with an incomplete order view.
    async fn reconcile_open_orders(&self) -> anyhow::Result<()> {
        let session = self.session();
        let orders = self
            .http_client
            .query_open_orders(None)
            .await
            .map_err(|e| anyhow::anyhow!("Aster open order reconciliation failed: {e}"))?;

        let ts_init = self.clock.get_time_ns();
        let mut reported = 0usize;
        // An open order on a loaded symbol that cannot be parsed leaves the local order view
        // incomplete. Valid rows are still published, but the caller must not treat the account
        // as verified.
        let mut failure: Option<String> = None;

        for order in &orders {
            match session.order_to_report(order, ts_init) {
                Ok(Some(report)) => {
                    if session
                        .emit_order_evidence(
                            order.symbol,
                            order.order_id,
                            Some(report),
                            Vec::new(),
                            false,
                        )
                        .await
                    {
                        reported += 1;
                    } else {
                        failure.get_or_insert_with(|| {
                            format!(
                                "open order {} is missing real fill evidence",
                                order.order_id
                            )
                        });
                    }
                }
                Ok(None) => log::debug!(
                    "Ignoring Aster open order on unloaded symbol {}",
                    order.symbol
                ),
                Err(e) => {
                    log::warn!("Skipping Aster open order {}: {e}", order.order_id);
                    failure.get_or_insert_with(|| {
                        format!("failed to parse open order {}: {e}", order.order_id)
                    });
                }
            }
        }

        log::info!("Reconciled {reported} open Aster orders");
        failure.map_or(Ok(()), |e| Err(anyhow::anyhow!(e)))
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
        .with_connect_timeout_secs(Some(timeout_secs))
        .with_socket_state(
            SocketControlFactory::new(self.core.client_id, Some(self.venue)),
            {
                let readiness = self.readiness.clone();
                move |state| {
                    // The transport tells us the socket is gone as soon as it knows, which is
                    // earlier than any business message: a reconnect attempt that never
                    // succeeds must not leave the account marked ready.
                    if state == SocketState::Disconnected {
                        readiness.write().socket_disconnected();
                    }
                }
            },
        );

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
                        session.degrade(format!("user data stream reconnect failed: {e}"));
                        tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                        continue;
                    }

                    log::info!("Aster user data stream reconnected with a new listen key");
                    session.recover("a user data stream reconnect").await;
                }
                is_first_session = false;

                let Some(stream) = stream_client.stream() else {
                    log::error!("Aster user stream produced no message stream");
                    session.degrade("user data stream produced no message stream");
                    stream_client.close().await;
                    tokio::time::sleep(USER_STREAM_RETRY_DELAY).await;
                    continue;
                };
                let mut stream = Box::pin(stream);
                let mut renewal =
                    tokio::time::interval(Duration::from_secs(ASTER_LISTEN_KEY_RENEWAL_SECS));
                renewal.tick().await; // The first tick completes immediately.
                // A stream row can owe a full snapshot and then the stream goes quiet; the debt
                // is state, so it is retried on this timer even without another message.
                let mut owed_refresh = tokio::time::interval(Duration::from_secs(1));
                owed_refresh.tick().await; // The first tick completes immediately.

                loop {
                    tokio::select! {
                        _ = renewal.tick() => {
                            if let Err(e) = stream_client.keepalive().await {
                                log::warn!("Aster listen key renewal failed: {e}");
                            } else {
                                log::debug!("Aster listen key renewed");
                            }
                        }
                        _ = owed_refresh.tick() => {
                            session.refresh_owed_balances().await;
                        }
                        message = stream.next() => {
                            let Some(message) = message else {
                                log::warn!("Aster user data stream ended; reconnecting");
                                session.degrade("user data stream ended");
                                break;
                            };

                            if matches!(message, BinanceFuturesWsStreamsMessage::ListenKeyExpired) {
                                log::warn!("Aster listen key expired; reconnecting with a new key");
                                session.degrade("listen key expired");
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

/// Keeps only instruments loaded both before and after a position request.
fn retain_position_scope(scope: &mut Vec<InstrumentAny>, current: &InstrumentIndex) {
    scope.retain(|instrument| {
        current
            .by_id(&instrument.id())
            .is_some_and(|loaded| loaded.raw_symbol() == instrument.raw_symbol())
    });
}

/// The usable rows of a position snapshot and any reason it cannot prove absence.
struct ParsedPositionSnapshot {
    reports: Vec<PositionStatusReport>,
    failure: Option<anyhow::Error>,
}

/// Parses the unpaginated positionRisk snapshot for a fixed set of loaded instruments.
///
/// The 2026-09-05 Aster testnet acceptance recorded an empty positionRisk response after
/// closing ETHUSDT (test_data/http_position_risk_closed_testnet.json). An omitted symbol in a
/// successful, entirely valid response therefore means flat within the requested scope.
/// A malformed or duplicate row invalidates that inference for the whole response; recovery
/// may still publish individually valid rows, but keeps the account degraded. Recovery also
/// withholds inferred flats until the preceding real-fill pass has no outstanding evidence.
fn parse_position_snapshot(
    positions: &[AsterPositionRisk],
    scope: &[InstrumentAny],
    account_id: AccountId,
    ts_now: UnixNanos,
    infer_flat: bool,
) -> ParsedPositionSnapshot {
    let mut parsed = ParsedPositionSnapshot {
        reports: Vec::new(),
        failure: None,
    };
    let mut seen = AHashSet::new();
    for position in positions {
        let result = (|| -> anyhow::Result<Option<PositionStatusReport>> {
            anyhow::ensure!(seen.insert(position.symbol), "duplicate position symbol");
            anyhow::ensure!(
                position
                    .position_side
                    .is_none_or(|side| side == BinancePositionSide::Both),
                "position snapshot is not in one-way mode",
            );
            // Validate every row before inferring absence, including symbols outside our scope.
            let signed = position.signed_quantity()?;
            let side = if signed > Decimal::ZERO {
                PositionSide::Long
            } else if signed < Decimal::ZERO {
                PositionSide::Short
            } else {
                PositionSide::Flat
            };
            let avg_px = match parse_decimal(&position.entry_price, "entryPrice") {
                Ok(price) => Some(price),
                Err(_) if side == PositionSide::Flat => None,
                Err(e) => return Err(e),
            };
            let Some(instrument) = scope
                .iter()
                .find(|i| i.raw_symbol().inner() == position.symbol)
            else {
                return Ok(None);
            };
            let quantity = Quantity::from_decimal_dp(signed.abs(), instrument.size_precision())?;
            Ok(Some(PositionStatusReport::new(
                account_id,
                instrument.id(),
                side,
                quantity,
                position.update_time.map_or(ts_now, millis_to_nanos),
                ts_now,
                Some(UUID4::new()),
                None,
                avg_px,
            )))
        })();
        match result {
            Ok(Some(report)) => parsed.reports.push(report),
            Ok(None) => {}
            Err(e) => {
                let e =
                    anyhow::anyhow!("Failed to parse Aster position {}: {e:#}", position.symbol);
                log::error!("{e:#}");
                parsed.failure.get_or_insert(e);
            }
        }
    }
    if infer_flat && parsed.failure.is_none() {
        let mut flat_reports = Vec::new();
        for instrument in scope {
            if seen.contains(&instrument.raw_symbol().inner()) {
                continue;
            }
            match Quantity::from_decimal_dp(Decimal::ZERO, instrument.size_precision()) {
                Ok(quantity) => flat_reports.push(PositionStatusReport::new(
                    account_id,
                    instrument.id(),
                    PositionSide::Flat,
                    quantity,
                    ts_now,
                    ts_now,
                    Some(UUID4::new()),
                    None,
                    None,
                )),
                Err(e) => {
                    parsed.failure.get_or_insert(e.into());
                }
            }
        }
        if parsed.failure.is_none() {
            parsed.reports.extend(flat_reports);
        }
    }
    parsed
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
/// The rows one REST balance response stated whole, and the assets it left unknown.
///
/// A response is not a success just because it parsed: a row whose `availableBalance` is missing
/// is skipped, and the caller must keep owing a refresh for it instead of treating the parsed
/// subset as the whole account.
#[derive(Debug, Default)]
struct ParsedAccountBalances {
    /// The balances the response verified.
    balances: Vec<AccountBalance>,
    /// The assets the response carried but could not state whole, keyed by asset.
    unverified: Vec<Ustr>,
}

/// Maps one full REST snapshot, keeping track of what it could not state.
///
/// This is the mapping contract for a full snapshot: `total` is the wallet balance and `free` is
/// the venue's `availableBalance`, which is the only amount this adapter may publish as spendable.
/// A row whose `availableBalance` is missing is unknown and skipped rather than published as
/// `free = total`; the next complete snapshot is what states it. Aster reports `availableBalance`
/// above `walletBalance` on cross-margin accounts, because availability there includes headroom
/// from other assets; [`AccountBalance::from_total_and_free`] clamps `free` into `[0, total]` so
/// `total == locked + free` holds, and the first such row per process is logged at warning level.
fn parse_account_balances(balances: &[AsterBalance]) -> ParsedAccountBalances {
    let mut parsed = ParsedAccountBalances {
        balances: Vec::with_capacity(balances.len()),
        unverified: Vec::new(),
    };

    for balance in balances {
        let currency = resolve_currency(balance.asset.as_str());
        let asset = Ustr::from(currency.code.as_str());

        let (Ok(total), Ok(available)) = (balance.total(), balance.available()) else {
            log::warn!("Skipping Aster balance for {currency}: unparsable amounts");
            parsed.unverified.push(asset);
            continue;
        };

        let Some(free) = available else {
            log::warn!(
                "Skipping Aster balance for {currency}: availableBalance is missing, so the \
                 spendable amount is unknown"
            );
            parsed.unverified.push(asset);
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
            Ok(account_balance) => parsed.balances.push(account_balance),
            Err(e) => {
                log::warn!("Skipping Aster balance for {currency}: {e}");
                parsed.unverified.push(asset);
            }
        }
    }

    parsed
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

/// Merges one Aster `ACCOUNT_UPDATE` into the conservative bounds a full snapshot seeded.
///
/// The payload's `wb` is the wallet balance and `cw` is the **cross wallet balance** - not the
/// amount available to open new positions. This function never maps `cw` to `free`:
///
/// - `wb == 0` is a verified zero (a withdrawal), the one amount that is whole on its own;
/// - a non-zero `wb` keeps the bounded `free`, never raising it (the locked amount absorbs the
///   change), and owes a full snapshot: the payload never states the available amount, and an
///   order can move funds between locked and free without changing `wb` at all;
/// - with no bounded `free` at all the row is withheld and a snapshot is owed, instead of
///   publishing the wallet balance as available margin.
///
/// The caller writes the published rows back into the bounds, so a sequence of updates can only
/// tighten the available amount; only a newer REST snapshot can raise it again.
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
    bounds: &AHashMap<Ustr, AccountBalance>,
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

        let Some(previous) = bounds.get(&update.asset) else {
            log::warn!(
                "Withholding Aster stream balance for {currency}: the payload states no \
                 available amount and no snapshot bounded one; a full account read is owed"
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
    /// much of an asset is spendable. The read commits through the same epoch protocol as every
    /// other snapshot, so a response that may predate a bound-changing stream update cannot relax
    /// the bound or clear the debt. A failed read changes nothing about the balances the account
    /// already holds - withheld values stay withheld and carried ones stay carried - and the debt
    /// survives a failed read or a throttle window, so the session timer retries it even if no
    /// further stream message arrives. One read is in flight at a time; a tick or a row that
    /// arrives while one runs only leaves the debt set for the next window.
    async fn refresh_owed_balances(&self) {
        let now_ms = self.now_ms();
        let (epoch_at_start, generation_at_start) = {
            let mut state = self.state.write();
            if state.balance_refresh_in_flight || !state.balance_refresh_due(now_ms) {
                return;
            }
            state.balance_refresh_in_flight = true;
            (state.balance_epoch, state.balance_refresh_generation)
        };

        let balances = match self.http_client.query_balances().await {
            Ok(balances) => balances,
            Err(e) => {
                // The backoff is measured from the completion, not the attempt: a slow request
                // that already spent the window must not leave the next attempt due at once.
                let mut state = self.state.write();
                state.balance_refresh_in_flight = false;
                state.note_balance_refresh_failure(self.now_ms());
                drop(state);
                log::warn!("Aster owed balance snapshot failed: {e}");
                self.degrade(format!("owed balance snapshot failed: {e}"));
                return;
            }
        };

        let parsed = parse_account_balances(&balances);
        let (published, verification_pending) = {
            let mut state = self.state.write();
            state.balance_refresh_in_flight = false;
            let published = state.commit_balance_snapshot(
                &parsed,
                epoch_at_start,
                generation_at_start,
                self.now_ms(),
            );
            (published, state.owed_balance_refresh)
        };
        if !parsed.unverified.is_empty() || verification_pending {
            self.degrade(format!(
                "owed balance snapshot did not verify available balances for {:?}",
                parsed.unverified,
            ));
        }

        self.emitter.emit_account_state(
            published,
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

                let parsed_status = match parse_futures_order_update_to_order_status(
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
                        self.degrade(format!("failed to parse an order status report: {e}"));
                        None
                    }
                };

                // A malformed half of an order update is evidence that this event cannot be
                // trusted on its own. Force the shared path to read the complete REST order and
                // trade history, while retaining the order as pending if that repair fails.
                let mut incomplete = parsed_status.is_none();
                let has_fill = match parse_decimal(&msg.order.last_filled_qty, "lastFilledQty") {
                    Ok(qty) => !qty.is_zero(),
                    Err(e) => {
                        log::error!("Failed to parse Aster lastFilledQty: {e}");
                        self.degrade(format!("failed to parse lastFilledQty: {e}"));
                        incomplete = true;
                        false
                    }
                };

                let fills = if has_fill {
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
                        Ok(report) => vec![report],
                        Err(e) => {
                            log::error!("Failed to parse Aster fill report: {e}");
                            self.degrade(format!("failed to parse a fill report: {e}"));
                            incomplete = true;
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                };

                if incomplete {
                    self.state
                        .write()
                        .note_pending_order(Ustr::from(&msg.order.order_id.to_string()), symbol);
                }

                let status = (!incomplete).then_some(parsed_status).flatten();
                let repaired = self
                    .emit_order_evidence(symbol, msg.order.order_id, status, fills, incomplete)
                    .await;

                // A malformed stream row degrades readiness even when REST repairs its order
                // evidence. Re-run the normal bounded account recovery before promoting Ready;
                // resolving one order alone cannot prove balances, positions, and other orders.
                if incomplete && repaired {
                    self.recover("a malformed order update was repaired").await;
                }
            }
            BinanceFuturesWsStreamsMessage::AccountUpdate(msg) => {
                // The merge reads the bounds it tightens, so it commits under one write lock:
                // writing the rows back and raising the debt apart would let a snapshot that
                // captured the state land between them and treat the row as verified.
                let merge = {
                    let mut state = self.state.write();
                    let merge = merge_aster_account_update(
                        msg,
                        account_id,
                        ts_init,
                        &state.balance_bounds,
                        state.last_balance_event_ms,
                    );

                    if let Some(event_ms) = merge.applied_event_ms
                        && event_ms > state.last_balance_event_ms
                    {
                        state.last_balance_event_ms = event_ms;
                    }

                    if let Some(account_state) = merge.state.as_ref() {
                        // The published rows are the new conservative bounds: a later update
                        // may only tighten them, and only a newer snapshot may raise them.
                        state.record_balance_bounds(&account_state.balances);
                    }

                    if merge.refresh_owed {
                        state.note_balance_refresh_owed();
                    }

                    merge
                };

                if let Some(account_state) = merge.state {
                    self.emitter.send_account_state(account_state);
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
                self.recover("a socket-level reconnect").await;
            }
            BinanceFuturesWsStreamsMessage::Error(msg) => {
                log::error!("Aster user data stream error: {msg:?}");
                self.degrade(format!("user data stream error: {msg:?}"));
            }
            other => {
                log::debug!("Ignoring Aster user stream message: {other:?}");
            }
        }
    }
}

/// Builds order status reports, optionally checking cumulative quantities against delivered
/// trades. A mass snapshot reads one shared trade window and performs that check after linking
/// the window, while ordinary report requests verify each order before returning it.
async fn build_order_status_reports(
    client: &AsterExecutionClient,
    cmd: &GenerateOrderStatusReports,
    verify_coverage: bool,
) -> anyhow::Result<Vec<OrderStatusReport>> {
    let session = client.session();
    let ts_init = client.clock.get_time_ns();
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
            .map(|id| client.symbol_context(&id).map(|(symbol, _)| symbol))
            .transpose()?;

        client
            .http_client
            .query_open_orders(symbol.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("Aster open orders query failed: {e}"))?
    } else {
        // Aster requires a symbol for the historical order endpoint, so the request is
        // fanned out across loaded instruments when none is given.
        let symbols: Vec<String> = match cmd.instrument_id {
            Some(instrument_id) => vec![client.symbol_context(&instrument_id)?.0],
            None => {
                let guard = client.instruments.read();
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
            Some(report) => {
                if verify_coverage {
                    session
                        .ensure_status_covered(&order.symbol, &report)
                        .await?;
                }
                // Keep the lifecycle marker in sync for status reports returned to the engine,
                // including reports produced by a mass snapshot. This is what lets a later
                // bounded-cache eviction distinguish an order that is still active from one
                // whose terminal lifecycle has already completed.
                session.track_order_state(&report, Ustr::from(order.symbol.as_str()));
                reports.push(report);
            }
            None => log::debug!(
                "Ignoring Aster order {} on unloaded symbol {}",
                order.order_id,
                order.symbol,
            ),
        }
    }

    Ok(reports)
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

        self.readiness.write().stop();
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

        let generation = self.readiness.write().begin_connect();

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

        match self.resolve_position_mode().await? {
            PositionMode::OneWay => {
                self.readiness.write().verify_position_mode();
            }
            PositionMode::Unconfirmed(reason) => {
                if self.config.assume_one_way_mode_when_unconfirmed {
                    log::warn!(
                        "Aster position mode is unconfirmed ({reason}); assuming one-way mode \
                         because `assume_one_way_mode_when_unconfirmed` is set"
                    );
                    self.readiness.write().verify_position_mode();
                } else {
                    log::warn!(
                        "Aster position mode is unconfirmed ({reason}); new risk stays denied \
                         until the mode can be confirmed"
                    );
                    self.readiness
                        .write()
                        .degrade(format!("position mode unconfirmed: {reason}"));
                }
            }
        }

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

        if let Err(e) = self.reconcile_open_orders().await {
            log::warn!("Aster open order reconciliation failed: {e}");
            self.readiness
                .write()
                .degrade(format!("open order reconciliation failed: {e}"));
        }

        if self.stream_state.read().has_recovery_debt() {
            // Pending evidence survives a stop/disconnect. A fresh socket and one open-order
            // sweep do not prove those old trades; run the same bounded recovery contract before
            // admitting new risk, including pending orders that no longer appear in openOrders.
            self.session()
                .recover("recovery debt carried into a new session")
                .await;
        } else {
            self.readiness.write().mark_ready(generation);
        }

        self.core.set_connected();
        log::info!("Aster execution client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        log::info!("Disconnecting Aster execution client");
        self.readiness
            .write()
            .degrade("the execution client disconnected");
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

        // Admission is checked before the order is reported as submitted, and again at the
        // actual send boundary inside the HTTP client: the first check keeps a doomed order out
        // of the engine's lifecycle, the second covers the wait between the two. Capture the
        // generation while taking the first check so a queued new-risk request cannot be
        // admitted by a later recovery of the same task.
        let (admission_generation, refusal) = {
            let readiness = self.readiness.read();
            (
                (!request.reduce_only).then_some(readiness.generation),
                readiness.refusal(request.reduce_only),
            )
        };
        if let Some(reason) = refusal {
            self.emitter.emit_order_denied(&order, &reason);
            return Ok(());
        }

        let Some(spawner) = self.spawner() else {
            self.emitter
                .emit_order_denied(&order, "Aster execution client is shutting down");
            return Ok(());
        };

        let session = self.session();
        let http_client = self.http_client.clone();
        let readiness = self.readiness.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();
        let reduce_only = request.reduce_only;

        self.stream_state
            .write()
            .track_working_order(client_order_id.inner(), Ustr::from(&symbol));

        self.emitter.emit_order_submitted(&order);

        spawner.spawn(async move {
            let admission = move || {
                let readiness = readiness.read();
                if let Some(reason) = readiness.refusal(reduce_only) {
                    return Err(reason);
                }

                if let Some(admitted_generation) = admission_generation
                    && readiness.generation != admitted_generation
                {
                    return Err(
                        "Aster execution readiness changed after order admission".to_string(),
                    );
                }

                Ok(())
            };

            match http_client
                .submit_order_admitted(request.to_params(), admission)
                .await
            {
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
            let (epoch_at_start, generation_at_start) = {
                let state = stream_state.read();
                (state.balance_epoch, state.balance_refresh_generation)
            };
            let balances = http_client
                .query_balances()
                .await
                .map_err(|e| anyhow::anyhow!("Aster balance request failed: {e}"))?;

            let parsed = parse_account_balances(&balances);
            // A full snapshot seeds the conservative bounds later stream updates tighten, and
            // verifies whatever a stream row had left owed - unless a bound moved while it was
            // in flight, in which case only a tightening is applied and the debt stays.
            let published = {
                let mut state = stream_state.write();
                state.commit_balance_snapshot(
                    &parsed,
                    epoch_at_start,
                    generation_at_start,
                    (clock.get_time_ns().as_u64() / 1_000_000) as i64,
                )
            };

            emitter.emit_account_state(published, Vec::new(), true, clock.get_time_ns(), None);
            Ok(())
        });

        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let (symbol, _) = self.symbol_context(&cmd.instrument_id)?;

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let session = self.session();
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
                Ok(order) => match session.order_to_report(&order, session.clock.get_time_ns()) {
                    Ok(Some(report)) => {
                        if !session
                            .emit_order_evidence(
                                order.symbol,
                                order.order_id,
                                Some(report),
                                Vec::new(),
                                false,
                            )
                            .await
                        {
                            log::warn!(
                                "Aster order query for {client_order_id} remains incomplete"
                            );
                        }
                    }
                    Ok(None) => log::debug!(
                        "Ignoring Aster order {} on unloaded symbol {}",
                        order.order_id,
                        order.symbol,
                    ),
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
            Ok(order) => {
                let report = order.to_order_status_report(
                    self.core.account_id,
                    context.instrument_id,
                    context.price_precision,
                    context.size_precision,
                    self.config.treat_expired_as_canceled,
                    self.clock.get_time_ns(),
                )?;
                let session = self.session();
                let symbol = Ustr::from(&symbol);
                session.ensure_status_covered(&symbol, &report).await?;
                Ok(Some(report))
            }
            Err(e) if e.is_unknown_order() => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Aster order status query failed: {e}")),
        }
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        build_order_status_reports(self, cmd, true).await
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let (reports, delivered) = self.fetch_fill_reports(cmd).await?;

        // Returning a vector is not proof that the execution engine accepted every row: the
        // caller may reject it later for scope, time, or missing order attribution. Keep the
        // exact IDs pending until an OrderWithFills path commits them after successful delivery;
        // a reconnect can then recover a row that a consumer discarded.
        self.hold_back_fills(&delivered);

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

        let mut order_reports = build_order_status_reports(self, &order_cmd, false).await?;
        // Fetched without committing the dedupe records: the positions request below can still
        // fail the whole snapshot, and a trade the engine never saw must stay deliverable.
        let (fill_reports, mut delivered) = self.fetch_fill_reports(fill_cmd).await?;
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
        let mut matched_fills = fill_reports;
        let mut reports_complete = unlinked_fills == 0;

        if !reports_complete {
            log::warn!(
                "{unlinked_fills} Aster fill(s) could not be linked to an order report; the \
                 startup snapshot is reported as incomplete rather than silently trimmed"
            );
        }

        // An order whose bounded trade-ID coverage was evicted cannot be checked against the
        // persistent quantity alone. Re-read its complete order history and append that exact
        // bundle to the mass status; if the history is unavailable, omit the status and leave a
        // pending debt for the targeted recovery pass.
        let unknown_orders: Vec<(Ustr, i64)> = {
            let state = self.stream_state.read();
            order_reports
                .iter()
                .filter_map(|report| {
                    state
                        .coverage_unknown(&report.venue_order_id.inner())
                        .then(|| {
                            (
                                Ustr::from(&format_binance_symbol(&report.instrument_id)),
                                report.venue_order_id.as_str().parse::<i64>().ok(),
                            )
                        })
                        .and_then(|(symbol, venue_order_id)| {
                            venue_order_id.map(|venue_order_id| (symbol, venue_order_id))
                        })
                })
                .collect()
        };
        let mut verified_histories: AHashMap<Ustr, Vec<FillReport>> = AHashMap::new();
        for (symbol, venue_order_id) in unknown_orders {
            if !self
                .stream_state
                .read()
                .can_rebase_unknown_order(&Ustr::from(&venue_order_id.to_string()))
            {
                // Prior fills exist but the cache no longer proves which of them the execution
                // engine owns. A full replay could recreate old fees after an engine cache
                // purge, so leave this order pending and fail the mass completeness check.
                reports_complete = false;
                continue;
            }
            let Some(report) = order_reports
                .iter()
                .find(|report| report.venue_order_id.as_str() == venue_order_id.to_string())
            else {
                continue;
            };
            let session = self.session();
            match session.query_order_fills(&symbol, venue_order_id).await {
                Ok(history) => {
                    match SessionContext::complete_order_fills_for_status(report, &history)? {
                        Some(complete) => {
                            let order_id = report.venue_order_id.inner();
                            append_delivered_fills(&mut delivered, symbol, &complete)?;
                            let mut existing_ids: AHashSet<i64> = matched_fills
                                .iter()
                                .filter(|fill| fill.venue_order_id == report.venue_order_id)
                                .filter_map(|fill| fill.trade_id.as_str().parse().ok())
                                .collect();
                            for fill in &complete {
                                let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() else {
                                    continue;
                                };
                                if existing_ids.insert(trade_id) {
                                    matched_fills.push(fill.clone());
                                }
                            }
                            verified_histories.insert(order_id, complete);
                        }
                        None => {
                            reports_complete = false;
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Aster full trade history for unknown-coverage order {venue_order_id} \
                         could not be verified: {e}"
                    );
                    reports_complete = false;
                }
            }
        }

        let mut fills_by_order: AHashMap<Ustr, Vec<&FillReport>> = AHashMap::new();
        for fill in &matched_fills {
            fills_by_order
                .entry(fill.venue_order_id.inner())
                .or_default()
                .push(fill);
        }

        // Aster's cumulative status may include fills older than this bounded window. Do not
        // pass such a status to `ExecutionManager`: it would infer the missing quantity as a
        // synthetic trade before the real history can be repaired. A status is retained only
        // when the already delivered quantity plus the exact trades in this snapshot explain it.
        let state = self.stream_state.read();
        let mut incomplete_orders = AHashSet::new();
        let mut pending_orders = Vec::new();
        for report in &order_reports {
            let symbol = Ustr::from(&format_binance_symbol(&report.instrument_id));
            let venue_order_id = report.venue_order_id.inner();
            let unknown_coverage = state.coverage_unknown(&venue_order_id);
            let mut covered_qty = if let Some(history) = verified_histories.get(&venue_order_id) {
                history.iter().map(|fill| fill.last_qty.as_decimal()).sum()
            } else if unknown_coverage {
                // A bounded-cache miss is never covered by the old aggregate or by the moving
                // window alone. Keep the status out of the mass report until full history proves
                // it.
                Decimal::ZERO
            } else {
                state.confirmed_fill_qty(&venue_order_id)
            };
            let mut seen_trade_ids = AHashSet::new();
            if !unknown_coverage && let Some(fills) = fills_by_order.get(&venue_order_id) {
                for fill in fills {
                    let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() else {
                        incomplete_orders.insert(venue_order_id);
                        continue;
                    };
                    if seen_trade_ids.insert(trade_id) && !state.has_fill(&symbol, trade_id) {
                        covered_qty += fill.last_qty.as_decimal();
                    }
                }
            }

            if covered_qty != report.filled_qty.as_decimal() {
                incomplete_orders.insert(venue_order_id);
                log::warn!(
                    "Holding back Aster order {} from mass status: cumulative filled quantity {} \
                     is not covered by real fills (covered {})",
                    report.venue_order_id,
                    report.filled_qty,
                    covered_qty,
                );
                pending_orders.push((venue_order_id, symbol));
            }
        }
        drop(state);

        if !pending_orders.is_empty() {
            let mut state = self.stream_state.write();
            for (venue_order_id, symbol) in pending_orders {
                state.note_pending_order(venue_order_id, symbol);
            }
        }

        if !incomplete_orders.is_empty() {
            reports_complete = false;
            order_reports
                .retain(|report| !incomplete_orders.contains(&report.venue_order_id.inner()));
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

        // A complete history proves the mass report's arithmetic, but returning that report is
        // not proof that the downstream manager accepted it. Keep the bounded-cache marker and
        // pending evidence until an accepted emitter bundle records the trade IDs; otherwise a
        // filtered mass report would permanently clear the only recovery path for its fills.

        // Under a bounded window the engine confirms that the reported fills explain the
        // reported position, per instrument. An instrument with activity but no position row
        // has nothing to confirm against, and every one of its orders is then demoted to
        // order-only projection with an error. A flat row states what the venue's omission
        // already means, so the check can be answered instead of skipped.
        let position_reports = if reports_complete {
            with_flat_rows_for_traded_instruments(
                position_reports,
                &order_reports,
                &matched_fills,
                self.core.account_id,
                ts_init,
                &self.instruments.read(),
            )
        } else {
            // An incomplete fill bundle cannot justify a closing inference. Keep open rows,
            // but defer flat reports until real trade IDs and commissions can be recovered.
            position_reports
                .into_iter()
                .filter(|r| r.position_side != PositionSide::Flat)
                .collect()
        };

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

        // Only an accepted emitter bundle may commit the exact-ID ledger. A mass-status return
        // has no consumer acknowledgement: the manager can filter an order or instrument after
        // this method returns. Keep every queried trade pending so the normal recovery path can
        // bundle it with its order and commit only after the execution channel accepts it. The
        // engine's own fill dedupe makes a later replay economically inert when the mass status
        // was consumed successfully.
        self.hold_back_fills(&delivered);

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

        let mut scope = self.instruments.read().snapshot();
        if let Some(instrument_id) = cmd.instrument_id {
            scope.retain(|instrument| instrument.id() == instrument_id);
        }

        let positions = self
            .http_client
            .query_position_risk(symbol.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("Aster position risk query failed: {e}"))?;

        if let Some(symbol) = symbol {
            anyhow::ensure!(
                positions
                    .iter()
                    .all(|position| position.symbol.as_str() == symbol),
                "Aster position snapshot contains symbols outside requested scope {symbol}",
            );
        }
        retain_position_scope(&mut scope, &self.instruments.read());
        let infer_flat = {
            let state = self.stream_state.read();
            state.pending_trades.is_empty()
                && state.pending_orders.is_empty()
                && state.unresolved_coverage_count() == 0
        };
        let parsed = parse_position_snapshot(
            &positions,
            &scope,
            self.core.account_id,
            self.clock.get_time_ns(),
            infer_flat,
        );
        if let Some(e) = parsed.failure {
            return Err(e);
        }
        Ok(parsed.reports)
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

    fn record_test_fill(state: &mut StreamState, symbol: Ustr, trade_id: i64, ts_ms: i64) -> bool {
        !matches!(
            state.record_trade(symbol, Ustr::default(), trade_id, ts_ms, Decimal::ZERO),
            AppliedTradeResult::Duplicate
        )
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
        let balances = parse_account_balances(&testnet_balances()).balances;

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
        let balances = parse_account_balances(&testnet_balances()).balances;
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

        let parsed = parse_account_balances(&balances).balances;

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

        let mut account = margin_account(parse_account_balances(&funded).balances);
        assert_eq!(
            account.balance_total(Some(Currency::USDT())).unwrap(),
            Money::new(100.0, Currency::USDT()),
        );

        account
            .base
            .update_balances(&parse_account_balances(&drained).balances);

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

    /// Builds the conservative bounds a full REST snapshot seeds.
    fn balance_bounds(rows: &str) -> AHashMap<Ustr, AccountBalance> {
        let balances: Vec<AsterBalance> = serde_json::from_str(rows).expect("balance fixture");
        parse_account_balances(&balances)
            .balances
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);
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

        let parsed = parse_account_balances(&balances).balances;

        assert_eq!(
            parsed.len(),
            1,
            "the row with no available amount is unknown"
        );
        assert_eq!(parsed[0].currency, Currency::BTC());
        assert_eq!(
            parse_account_balances(&balances).unverified,
            vec![Ustr::from("USDT")],
            "the unknown asset keeps the refresh owed"
        );
    }

    /// Applies one stream update to the bounds the way the dispatch does, and returns them.
    fn merge_into_bounds(
        bounds: &AHashMap<Ustr, AccountBalance>,
        balances_json: &str,
    ) -> AHashMap<Ustr, AccountBalance> {
        let update = account_update(balances_json);
        let merged =
            merge_aster_account_update(&update, account_id(), UnixNanos::default(), bounds, 0);
        let mut next = bounds.clone();
        if let Some(state) = merged.state {
            for balance in state.balances {
                next.insert(Ustr::from(balance.currency.code.as_str()), balance);
            }
        }
        next
    }

    /// A sequence of stream updates may only tighten the available amount: no later row can
    /// raise it again until a newer snapshot says so.
    #[rstest]
    fn test_stream_updates_only_tighten_the_available_bound() {
        let bounds =
            balance_bounds(r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#);

        let withdrawn = merge_into_bounds(&bounds, r#"[{"a":"USDT","wb":"10.0","cw":"10.0"}]"#);
        assert_eq!(
            withdrawn
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal(),
            dec("10"),
        );

        // A wallet increase with no available evidence must not restore the snapshot's 20.
        let raised = merge_into_bounds(&withdrawn, r#"[{"a":"USDT","wb":"15.0","cw":"15.0"}]"#);
        assert_eq!(
            raised.get(&Ustr::from("USDT")).unwrap().free.as_decimal(),
            dec("10"),
            "an update may only tighten the bound",
        );

        // An explicit zero invalidates the old funds, and a later non-zero wallet does not
        // resurrect an available amount that was never verified again.
        let zeroed = merge_into_bounds(&raised, r#"[{"a":"USDT","wb":"0.0","cw":"0.0"}]"#);
        assert!(
            zeroed
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal()
                .is_zero()
        );
        let refunded = merge_into_bounds(&zeroed, r#"[{"a":"USDT","wb":"100.0","cw":"100.0"}]"#);
        assert!(
            refunded
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal()
                .is_zero(),
            "the pre-withdrawal available amount must not come back",
        );
    }

    /// The owed snapshot is state: a failed read keeps the debt and backs off, and only a
    /// successful snapshot clears it.
    #[rstest]
    fn test_owed_balance_refresh_survives_failures_and_clears_on_success() {
        let mut state = StreamState::default();
        assert!(!state.balance_refresh_due(1_000));

        state.note_balance_refresh_owed();
        assert!(
            state.balance_refresh_due(1_000),
            "a fresh debt is due at once"
        );

        state.note_balance_refresh_failure(1_000);
        assert!(
            state.owed_balance_refresh,
            "a failed read does not cancel the debt"
        );
        assert!(
            !state.balance_refresh_due(1_000),
            "the retry waits for the interval"
        );
        assert!(
            state.balance_refresh_due(6_000),
            "the retry is due after 5s"
        );

        state.note_balance_refresh_failure(6_000);
        assert!(
            !state.balance_refresh_due(11_000),
            "the second failure backs off to 10s"
        );
        assert!(state.balance_refresh_due(16_000));

        state.note_balance_refresh_success(1_000);
        assert!(
            !state.balance_refresh_due(1_000_000),
            "success clears the debt"
        );
        // The success gate survives: the next update waits for the window instead of turning
        // into another read.
        state.note_balance_refresh_owed();
        assert!(!state.balance_refresh_due(1_000));
        assert!(!state.balance_refresh_due(5_999));
        assert!(state.balance_refresh_due(6_000));
    }

    /// A snapshot that left an asset unknown is not a verified account: the rows it stated are
    /// published, the unknown asset is withheld, and the commit itself establishes the debt so
    /// the timer retries without the caller having to pre-set it.
    #[rstest]
    fn test_partial_snapshot_establishes_the_debt_for_the_unknown_asset() {
        let mut state = StreamState::default();

        let balances: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        )
        .unwrap();
        let parsed = parse_account_balances(&balances);
        let epoch = state.balance_epoch;
        let generation = state.balance_refresh_generation;
        let published = state.commit_balance_snapshot(&parsed, epoch, generation, 1_000);

        assert_eq!(published.len(), 1);
        assert_eq!(published[0].currency, Currency::BTC());
        assert!(
            state.balance_bounds.get(&Ustr::from("USDT")).is_none(),
            "an unknown asset is not bounded by the subset that did parse"
        );
        assert!(
            state.owed_balance_refresh,
            "the unknown asset still owes a read"
        );
        assert!(
            !state.balance_refresh_due(1_000),
            "the retry waits for the interval"
        );
        assert!(state.balance_refresh_due(6_000));
    }

    /// A response that may predate a bound-changing update may only tighten, and never clears
    /// the debt: completing later is not being newer.
    #[rstest]
    fn test_a_late_response_cannot_relax_a_newer_bound_or_clear_its_debt() {
        let mut state = StreamState::default();
        state.note_balance_refresh_owed();
        let epoch_at_start = state.balance_epoch;
        let generation_at_start = state.balance_refresh_generation;

        // A newer read commits first: USDT 5 is verified, BTC is unknown, so the debt stays.
        let newer_rows: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"5.0"},
                {"asset":"BTC","balance":"1.0"}]"#,
        )
        .unwrap();
        let newer = parse_account_balances(&newer_rows);
        let _ = state.commit_balance_snapshot(&newer, epoch_at_start, generation_at_start, 1_000);
        assert_eq!(
            state
                .balance_bounds
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal(),
            dec("5"),
        );
        assert!(
            state.owed_balance_refresh,
            "the partial snapshot kept the debt"
        );

        // The older read completes with a higher free and must not raise the bound or clear the
        // debt the newer snapshot left.
        let older_rows: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#,
        )
        .unwrap();
        let older = parse_account_balances(&older_rows);
        let published =
            state.commit_balance_snapshot(&older, epoch_at_start, generation_at_start, 2_000);

        assert_eq!(
            state
                .balance_bounds
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal(),
            dec("5"),
        );
        assert_eq!(published[0].free.as_decimal(), dec("5"));
        assert!(
            state.owed_balance_refresh,
            "a possibly older response cannot clear the debt"
        );
    }

    /// A stream row that restates the same numbers still leaves the split unverified, so a
    /// response read before it may not clear the debt even though no bound moved.
    #[rstest]
    fn test_a_value_preserving_update_keeps_an_in_flight_response_from_clearing_the_debt() {
        let mut state = StreamState::default();
        let verified: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#,
        )
        .unwrap();
        state.replace_balance_bounds(&parse_account_balances(&verified).balances);
        state.note_balance_refresh_success(0);

        // A read starts while the account is verified and clean.
        let epoch_at_start = state.balance_epoch;
        let generation_at_start = state.balance_refresh_generation;

        // A newer row restates the same wallet balance: the merged bound does not move, but the
        // available split is no longer verified.
        let same = account_update(r#"[{"a":"USDT","wb":"100.0","cw":"100.0"}]"#);
        let merged = merge_aster_account_update(
            &same,
            account_id(),
            UnixNanos::default(),
            &state.balance_bounds,
            0,
        );
        assert!(merged.refresh_owed);
        let balances = merged
            .state
            .expect("the bounded row is publishable")
            .balances;
        state.record_balance_bounds(&balances);
        state.note_balance_refresh_owed();
        assert_eq!(
            state.balance_epoch, epoch_at_start,
            "the restated numbers did not move the bound",
        );

        // The old response completes: it cannot clear the debt the newer row raised, and the
        // timer still has to take the snapshot that can.
        let parsed = parse_account_balances(&verified);
        let published =
            state.commit_balance_snapshot(&parsed, epoch_at_start, generation_at_start, 1_000);

        assert_eq!(published[0].free.as_decimal(), dec("20"));
        assert!(
            state.owed_balance_refresh,
            "the newer row keeps the debt even though no bound moved"
        );
        assert!(
            !state.balance_refresh_due(1_000),
            "the retry waits for the interval"
        );
        assert!(state.balance_refresh_due(6_000));
    }

    /// A snapshot that no bound-changing update overtook is the new bound, including when it
    /// raises an amount a stream row had tightened: only a response that may predate an update
    /// is restricted to tightening.
    #[rstest]
    fn test_a_clean_snapshot_can_raise_a_tightened_bound() {
        let mut state = StreamState::default();
        let tight: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"5.0"}]"#,
        )
        .unwrap();
        state.replace_balance_bounds(&parse_account_balances(&tight).balances);
        state.note_balance_refresh_owed();

        let raised: Vec<AsterBalance> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"}]"#,
        )
        .unwrap();
        let parsed = parse_account_balances(&raised);
        let epoch = state.balance_epoch;
        let generation = state.balance_refresh_generation;
        let published = state.commit_balance_snapshot(&parsed, epoch, generation, 1_000);

        assert_eq!(published[0].free.as_decimal(), dec("20"));
        assert_eq!(
            state
                .balance_bounds
                .get(&Ustr::from("USDT"))
                .unwrap()
                .free
                .as_decimal(),
            dec("20"),
        );
        assert!(
            !state.owed_balance_refresh,
            "a full snapshot clears the debt"
        );
    }

    /// A full REST snapshot is the new conservative bound: assets it does not carry are no
    /// longer bounded, and it may raise an amount a stream row had tightened.
    #[rstest]
    fn test_rest_snapshot_replaces_the_verified_reference() {
        let funded = balance_bounds(
            r#"[{"asset":"USDT","balance":"100.0","availableBalance":"20.0"},
                {"asset":"BTC","balance":"0.5","availableBalance":"0.5"}]"#,
        );
        let mut state = StreamState {
            balance_bounds: funded,
            ..StreamState::default()
        };

        let drained: Vec<AsterBalance> =
            serde_json::from_str(r#"[{"asset":"USDT","balance":"0.0","availableBalance":"0.0"}]"#)
                .unwrap();
        state.replace_balance_bounds(&parse_account_balances(&drained).balances);

        assert_eq!(state.balance_bounds.len(), 1);
        assert!(
            state
                .balance_bounds
                .get(&Ustr::from("USDT"))
                .is_some_and(|b| b.total.as_decimal().is_zero()),
        );
    }

    #[rstest]
    fn test_stream_balance_transition_to_zero_clears_the_cached_amount() {
        // The shared Binance parser drops `wb == 0` rows, which would leave the stale amount
        // cached; the Aster crate parses the `B` array itself so a withdrawal is stated, and
        // the zero row is the one stream statement that is complete on its own.
        let verified = balance_bounds(
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
        // The bound is stated through the same resolver the merge uses: `Currency::from` panics
        // on a code nothing has registered yet when the test runs in its own process.
        let currency = resolve_currency("AFEE");
        let verified = [(
            Ustr::from("AFEE"),
            AccountBalance::from_total_and_free(Decimal::from(1), Decimal::from(1), currency)
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

        assert!(parse_account_balances(&balances).balances.is_empty());
        assert_eq!(
            parse_account_balances(&balances).unverified,
            vec![Ustr::from("USDT")],
            "an unparsable amount is unknown, not verified"
        );
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

        assert!(record_test_fill(&mut state, symbol, 10, 1_000));
        assert!(
            !record_test_fill(&mut state, symbol, 10, 1_000),
            "a repeat is not new"
        );
        assert!(record_test_fill(&mut state, symbol, 11, 2_000));

        assert!(state.has_fill(&symbol, 10));
        assert!(!state.has_fill(&symbol, 12));
        assert_eq!(state.applied_trades[&symbol].last_ts_ms, 2_000);
        assert!(!state.applied_trades.contains_key(&Ustr::from("ETHUSDT")));
    }

    #[rstest]
    fn test_order_fill_evidence_deduplicates_exact_quantity() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let venue_order_id = Ustr::from("910001");
        let qty = Decimal::from_str_exact("0.004000").unwrap();

        assert!(state.record_delivered_fill_for_order(
            symbol,
            venue_order_id,
            7,
            1_000,
            qty,
            false,
        ));
        assert!(!state.record_delivered_fill_for_order(
            symbol,
            venue_order_id,
            7,
            1_000,
            qty,
            false,
        ));
        assert_eq!(state.confirmed_fill_qty(&venue_order_id), qty);
        assert!(state.has_fill(&symbol, 7));
    }

    #[rstest]
    fn test_verified_history_rows_stay_pending_until_accepted() {
        let symbol = Ustr::from("BTCUSDT");
        let mut older = fill_at(1_000);
        older.trade_id = nautilus_model::identifiers::TradeId::new("100");
        let mut newer = fill_at(2_000);
        newer.trade_id = nautilus_model::identifiers::TradeId::new("200");

        // The initial lookback already covered the newer row. The verified order history adds
        // the older row that a filtered mass consumer would otherwise make unrecoverable.
        let mut delivered = vec![DeliveredFill {
            symbol,
            trade_id: 200,
        }];
        append_delivered_fills(&mut delivered, symbol, &[older, newer]).unwrap();

        let mut state = StreamState::default();
        for fill in delivered {
            state.note_pending_fill(fill.symbol, fill.trade_id);
        }

        assert_eq!(
            state.pending_trade_ids(&symbol),
            BTreeSet::from([100, 200]),
            "all verified history rows must survive a filtered mass response",
        );
    }

    #[rstest]
    fn test_pending_order_evidence_survives_until_resolution() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let venue_order_id = Ustr::from("910001");

        state.note_pending_order(venue_order_id, symbol);
        assert_eq!(state.pending_orders(), vec![(venue_order_id, symbol)]);

        state.forget_pending_order(&venue_order_id);
        assert!(state.pending_orders().is_empty());
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

        record_test_fill(&mut state, symbol, 1, 2_000_000);
        assert_eq!(
            state.compensation_start_ms(&symbol, 500_000),
            1_000_000,
            "a WebSocket fill must not advance the complete-history checkpoint",
        );

        state.advance_history_checkpoint(symbol, 3_000_000);
        assert_eq!(
            state.compensation_start_ms(&symbol, 500_000),
            3_000_000,
            "only a successful complete-history pass may advance the checkpoint",
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
    fn test_position_snapshot_scope_excludes_unloaded_and_newly_loaded_instruments() {
        let (_, _, mut index) = flat_position_inputs();
        let mut scope = index.snapshot();
        index.replace(Vec::new());
        retain_position_scope(&mut scope, &index);
        let parsed = parse_position_snapshot(&[], &scope, account_id(), UnixNanos::default(), true);
        assert!(parsed.reports.is_empty());
        index.replace(vec![InstrumentAny::CryptoPerpetual(
            crypto_perpetual_ethusdt(),
        )]);
        retain_position_scope(&mut scope, &index);
        assert!(
            scope.is_empty(),
            "a new load was not covered by the request"
        );
    }

    #[rstest]
    fn test_bad_unloaded_position_row_invalidates_flat_inference_for_loaded_symbols() {
        let (_, _, index) = flat_position_inputs();
        let positions: Vec<AsterPositionRisk> = serde_json::from_str(
            r#"[{"symbol":"UNLOADED","positionAmt":"bad","entryPrice":"0","positionSide":"BOTH"}]"#,
        )
        .unwrap();
        let parsed = parse_position_snapshot(
            &positions,
            &index.snapshot(),
            account_id(),
            UnixNanos::default(),
            true,
        );
        assert!(parsed.failure.is_some());
        assert!(parsed.reports.is_empty());
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
        record_test_fill(&mut state, symbol, 600, 9_000);

        assert_eq!(
            state.compensation_start_ms(&symbol, 1_000),
            1_000,
            "a WebSocket timestamp must not advance the complete-history checkpoint",
        );
        assert_eq!(
            state.pending_trade_ids(&symbol),
            BTreeSet::from([500]),
            "the trade must stay listed for a targeted query",
        );

        // Once it is applied, nothing is left to recover.
        record_test_fill(&mut state, symbol, 500, 2_000);
        assert_eq!(
            state.compensation_start_ms(&symbol, 1_000),
            1_000,
            "only a successful complete-history pass may advance the checkpoint",
        );
        assert!(state.pending_trade_ids(&symbol).is_empty());
    }

    #[rstest]
    fn test_a_pending_trade_stays_recoverable_alongside_a_long_history() {
        // A trade still awaiting recovery must remain separate from the exact delivered ledger,
        // and the trades that *were* applied must stay recognised as applied so a pass that reads
        // them again does not re-deliver them.
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");

        state.note_pending_fill(symbol, 1);
        for trade_id in 2..=4_097 {
            record_test_fill(&mut state, symbol, trade_id, 1_000 + trade_id);
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
            state.has_fill(&symbol, 4_097),
            "the applied ledger must retain exact membership",
        );
    }

    #[rstest]
    fn test_a_pending_note_for_an_applied_fill_is_ignored() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        record_test_fill(&mut state, symbol, 500, 5_000);

        state.note_pending_fill(symbol, 500);

        assert!(state.pending_trade_ids(&symbol).is_empty());
    }

    #[rstest]
    fn test_recorded_fill_is_not_offered_again() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");

        assert!(record_test_fill(&mut state, symbol, 4_242, 1_500_000));

        assert!(state.has_fill(&symbol, 4_242));
        assert!(
            !record_test_fill(&mut state, symbol, 4_242, 1_500_000),
            "a trade ID already applied must never be delivered twice",
        );
    }

    #[rstest]
    fn test_applied_trades_compacts_dense_ids_without_losing_membership() {
        let mut applied = AppliedTrades::default();

        for trade_id in 0..4_196 {
            applied.record(
                trade_id,
                AppliedTrade {
                    venue_order_id: Ustr::default(),
                    qty: Decimal::ZERO,
                    ts_ms: trade_id,
                },
            );
        }

        assert_eq!(applied.ids.len(), MAX_TRACKED_TRADE_IDS);
        assert!(!applied.contains(0), "the bounded cache evicts old IDs");
        assert!(applied.contains(4_195));
    }

    #[rstest]
    fn test_order_fill_evidence_deduplicates_after_a_long_history() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let venue_order_id = Ustr::from("910001");
        let qty = Decimal::from_str_exact("0.001").unwrap();
        state.track_working_venue_order(venue_order_id);

        for trade_id in 0..MAX_TRACKED_TRADE_IDS as i64 {
            assert!(state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                trade_id,
                trade_id,
                qty,
                false,
            ));
        }

        let expected = qty * Decimal::from(MAX_TRACKED_TRADE_IDS as u64);
        assert_eq!(state.confirmed_fill_qty(&venue_order_id), expected);
        assert!(
            !state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                MAX_TRACKED_TRADE_IDS as i64,
                MAX_TRACKED_TRADE_IDS as i64,
                qty,
                false,
            ),
            "the first cache eviction must require recovery",
        );
        assert!(
            !state.record_delivered_fill_for_order(symbol, venue_order_id, 0, 0, qty, false),
            "an evicted trade ID must fail closed until full history is read",
        );
        assert_eq!(
            state.confirmed_fill_qty(&venue_order_id),
            expected + qty,
            "the accepted fill remains in the economic aggregate even when its metadata is evicted",
        );
        assert!(state.coverage_unknown(&venue_order_id));
    }

    #[rstest]
    fn test_accepted_fill_eviction_keeps_qty_and_degrades_readiness() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let venue_order_id = Ustr::from("910003");
        let qty = Decimal::from_str_exact("0.001").unwrap();
        state.track_working_venue_order(venue_order_id);

        for trade_id in 1..=MAX_TRACKED_TRADE_IDS as i64 {
            assert!(state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                trade_id,
                trade_id,
                qty,
                true,
            ));
        }

        // A sparse/late low ID is accepted by the emitter but is immediately evicted from the
        // bounded cache. The economic contribution must remain exact even though recovery is now
        // required before another cumulative status can be trusted.
        assert!(!state.record_delivered_fill_for_order(symbol, venue_order_id, 0, 0, qty, true,));
        assert_eq!(
            state.confirmed_fill_qty(&venue_order_id),
            qty * Decimal::from((MAX_TRACKED_TRADE_IDS + 1) as u64),
        );
        assert!(state.coverage_unknown(&venue_order_id));
        assert!(state.has_recovery_debt());

        let mut readiness = Readiness::default();
        let generation = readiness.begin_connect();
        readiness.verify_position_mode();
        assert!(readiness.mark_ready(generation));
        readiness.degrade("accepted fill evicted its exact metadata");
        assert!(!readiness.allows_new_risk());
        assert!(readiness.refusal(false).is_some());
    }

    #[rstest]
    fn test_order_fill_evidence_accepts_a_late_lower_id_after_sparse_history() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let venue_order_id = Ustr::from("910002");
        let qty = Decimal::from_str_exact("0.001").unwrap();
        state.track_working_venue_order(venue_order_id);

        // Trade IDs are numeric, but delivery can be out of order and the account's history can
        // contain gaps. A lower ID that was never delivered must remain distinguishable from a
        // replay of an already delivered ID.
        for trade_id in (2..=8_194).step_by(2).take(MAX_TRACKED_TRADE_IDS) {
            assert!(state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                trade_id,
                trade_id,
                qty,
                false,
            ));
        }

        assert!(
            !state.record_delivered_fill_for_order(symbol, venue_order_id, 1, 1, qty, false),
            "a late sparse ID cannot be classified from the bounded cache and stays pending",
        );
        assert_eq!(
            state.confirmed_fill_qty(&venue_order_id),
            qty * Decimal::from((MAX_TRACKED_TRADE_IDS + 1) as u64),
        );
        assert!(
            state.coverage_unknown(&venue_order_id),
            "the late ID must require a complete order-history proof",
        );

        let mut applied = AppliedTrades::default();
        for trade_id in (2..=8_194).step_by(2) {
            applied.record(
                trade_id,
                AppliedTrade {
                    venue_order_id,
                    qty,
                    ts_ms: trade_id,
                },
            );
        }
        assert!(!applied.contains(1));
        assert!(
            !applied.contains(2) && applied.contains(8_194),
            "cache membership is exact IDs rather than a numeric floor",
        );
    }

    #[rstest]
    fn test_terminal_trade_cache_eviction_does_not_create_recovery_debt() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let qty = Decimal::from_str_exact("0.001").unwrap();

        // Each order is terminal before another order can evict its only trade from the bounded
        // fast cache. Reclaiming that optimization row must not turn a healthy session into an
        // unbounded list of unknown orders or hold readiness in Degraded.
        for index in 0..=MAX_TRACKED_TRADE_IDS as i64 {
            let venue_order_id = Ustr::from(&format!("terminal-{index}"));
            state.track_working_venue_order(venue_order_id);
            assert!(state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                index,
                index,
                qty,
                false,
            ));
            state.forget_working_venue_order(&venue_order_id);
        }

        assert!(state.coverage_unknown.is_empty());
        assert!(state.pending_orders.is_empty());
        assert!(state.pending_trades.is_empty());
        assert!(!state.has_recovery_debt());
    }

    #[rstest]
    fn test_terminal_duplicate_is_safe_after_trade_cache_eviction() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let terminal_order = Ustr::from("terminal-delivered");
        let qty = Decimal::from_str_exact("0.001").unwrap();

        state.track_working_venue_order(terminal_order);
        assert!(state.record_delivered_fill_for_order(symbol, terminal_order, 0, 0, qty, false,));
        state.mark_terminal_delivered(terminal_order);
        state.forget_working_venue_order(&terminal_order);

        for index in 1..=MAX_TRACKED_TRADE_IDS as i64 {
            let other_order = Ustr::from(&format!("terminal-other-{index}"));
            state.track_working_venue_order(other_order);
            assert!(state.record_delivered_fill_for_order(
                symbol,
                other_order,
                index,
                index,
                qty,
                false,
            ));
            state.forget_working_venue_order(&other_order);
        }

        assert!(
            !state.has_fill(&symbol, 0),
            "the terminal trade metadata was evicted"
        );
        let (unknown, _can_rebase, duplicate) =
            state.inspect_status_coverage(&symbol, terminal_order, true, qty, false);
        assert!(
            duplicate,
            "the accepted terminal marker still proves this status is a replay"
        );
        assert!(
            !unknown && !state.has_recovery_debt(),
            "a duplicate terminal status must not create recovery debt before suppression",
        );
        assert!(
            !state
                .inspect_status_coverage(&symbol, terminal_order, true, qty, true,)
                .2,
            "new fill evidence must not be suppressed"
        );
        assert!(
            !state
                .inspect_status_coverage(&symbol, terminal_order, true, qty + qty, false,)
                .2,
            "a larger cumulative quantity must not be suppressed"
        );
    }

    #[rstest]
    fn test_terminal_eviction_becomes_debt_only_when_a_status_needs_missing_ids() {
        let mut state = StreamState::default();
        let symbol = Ustr::from("BTCUSDT");
        let terminal_order = Ustr::from("terminal-evicted");
        let qty = Decimal::from_str_exact("0.001").unwrap();

        state.track_working_venue_order(terminal_order);
        assert!(state.record_delivered_fill_for_order(symbol, terminal_order, 0, 0, qty, false,));
        state.forget_working_venue_order(&terminal_order);

        for index in 1..=MAX_TRACKED_TRADE_IDS as i64 {
            let venue_order_id = Ustr::from(&format!("terminal-{index}"));
            state.track_working_venue_order(venue_order_id);
            assert!(state.record_delivered_fill_for_order(
                symbol,
                venue_order_id,
                index,
                index,
                qty,
                false,
            ));
            state.forget_working_venue_order(&venue_order_id);
        }

        assert!(!state.coverage_unknown(&terminal_order));
        assert!(!state.has_recovery_debt());

        state.mark_coverage_unknown_if_incomplete(&symbol, terminal_order);
        assert!(state.coverage_unknown(&terminal_order));
        assert_eq!(
            state.pending_orders(),
            vec![(terminal_order, symbol)],
            "only the later status observation turns a missing cache row into debt",
        );
        assert!(state.has_recovery_debt());
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
    fn test_readiness_admits_only_risk_reducing_actions_when_degraded() {
        let mut readiness = Readiness::default();

        assert!(!readiness.allows_new_risk());
        assert!(!readiness.allows_reduce_only());
        assert!(readiness.refusal(false).is_some());

        let generation = readiness.begin_connect();
        assert!(
            !readiness.mark_ready(generation),
            "an unproven position mode must not admit new risk"
        );
        readiness.verify_position_mode();
        assert!(readiness.mark_ready(generation));
        assert!(readiness.allows_new_risk());
        assert!(readiness.refusal(false).is_none());

        readiness.degrade("the private stream ended");
        assert!(!readiness.allows_new_risk());
        assert!(readiness.allows_reduce_only());
        assert!(
            readiness
                .refusal(false)
                .is_some_and(|reason| reason.contains("private stream")),
        );
        assert!(readiness.refusal(true).is_none());
    }

    #[rstest]
    fn test_a_superseded_recovery_pass_cannot_publish_ready() {
        let mut readiness = Readiness::default();
        readiness.verify_position_mode();
        let first = readiness.begin_reconcile();
        let second = readiness.begin_reconcile();

        assert!(
            !readiness.mark_ready(first),
            "a pass a later one superseded must not publish readiness",
        );
        assert!(readiness.mark_ready(second));
        assert!(readiness.allows_new_risk());

        readiness.degrade("the private stream ended");
        assert!(
            !readiness.mark_ready(second),
            "a degraded phase must not be promoted by the pass that was running",
        );
    }

    #[rstest]
    fn test_a_session_without_a_verified_mode_is_never_ready() {
        let mut readiness = Readiness::default();
        let generation = readiness.begin_connect();

        assert!(
            !readiness.mark_ready(generation),
            "a recovery pass must not promote a session whose mode was never proven",
        );
        assert!(!readiness.allows_new_risk());

        readiness.verify_position_mode();
        assert!(readiness.mark_ready(generation));
        assert!(readiness.allows_new_risk());

        // A new session must prove the mode again.
        let next = readiness.begin_connect();
        assert!(!readiness.mark_ready(next));
        assert!(!readiness.allows_new_risk());
    }

    #[rstest]
    fn test_stopping_readiness_admits_nothing_and_cannot_be_restored() {
        let mut readiness = Readiness::default();
        let generation = readiness.begin_connect();
        readiness.verify_position_mode();
        assert!(readiness.mark_ready(generation));

        readiness.stop();
        assert!(!readiness.allows_new_risk());
        assert!(!readiness.allows_reduce_only());
        assert!(readiness.refusal(false).is_some());

        readiness.degrade("a late failure");
        assert!(!readiness.mark_ready(generation));
        assert!(!readiness.allows_reduce_only());

        let late = readiness.begin_reconcile();
        assert!(
            !readiness.mark_ready(late),
            "a pass begun while stopping must not restore readiness",
        );
        assert!(!readiness.allows_reduce_only());
    }
}
