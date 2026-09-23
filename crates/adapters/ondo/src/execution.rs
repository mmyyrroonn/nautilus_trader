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

//! The Ondo Perps execution client: order commands, private reports and the two ledgers.
//!
//! # What this client owns
//!
//! - **The order write surface** (plan §6.2). Every command is mapped onto an [`OndoOrderCommand`],
//!   which validates it locally and produces the exact request body; a command this adapter cannot
//!   express is refused **before** a request exists, with a named reason. FOK and GTD are refused
//!   rather than downgraded, a post-only market order is refused rather than converted into a
//!   taker, and [`ModifyOrder`] is rejected rather than emulated as a cancel plus a new order
//!   ([`OndoExecutionClient::modify_order`]).
//! - **The order index and the fill ledger** (plan §6.3). An order is keyed by its Nautilus
//!   [`ClientOrderId`] and mapped to the venue's order id in both directions; a fill is deduped on
//!   `(account_id, fill.id)`. The fills are the only source of filled quantity - `lastFillSize` is
//!   kept as the informational field it is and never turns into a fill of its own - and a fill's
//!   own fee is the only fee ever charged, so the venue's order-level cumulative `fee` can never be
//!   counted a second time.
//! - **The private reports.** Everything emitted is a Nautilus event or a Nautilus report:
//!   [`ExecutionReport`](nautilus_common::messages::execution::ExecutionReport) for a status and
//!   for a fill. No adapter-private reporting type crosses this boundary.
//!
//! # The account, as this client reports it (plan §6.4, §R3.2)
//!
//! A concluded reconciliation pass is the internal reader: it publishes the account the machine
//! **verified** as a Nautilus account state through the same emitter and event channel every other
//! execution event travels, reads the account's open positions into native
//! [`PositionStatusReport`]s, and checkpoints the durable journal. Three things about that are
//! deliberate:
//!
//! - **Only a verified balance is published.** A pass that could not read the balance, or read one
//!   that is not the whole account, publishes nothing and says why
//!   ([`ReconciliationMachine::verified_balance`]). A negative equity *is* published, negative
//!   signs intact, because it is a state the venue stated.
//! - **Bulk position coverage stays `false`.** The venue documents its position list as every open
//!   position the account holds, and this adapter reports every row of it that it can name - but
//!   `provides_bulk_position_coverage` is a *promise about the rows it cannot name*, and a row whose
//!   direction or market this adapter cannot read is one it cannot report. The engine reads a
//!   coverage promise as "an instrument with no report is flat"; one unreadable row would make that
//!   a fabricated flat position, which is the false-clean judgment plan §6.3 forbids. So the
//!   promise is not made, and the reports are still produced.
//! - **The funding axis is separate, and it never invents a payment.** The public funding *rate* is
//!   market data, the balance's `totalFundingPayments` is a running total, and only a
//!   `FundingFeeTransfer` record is a payment; the first two are never multiplied together to
//!   produce the third (plan §R3.2).
//!
//! # The foreign account
//!
//! The account is not this client's alone, and the policy for everything on it that this run did
//! not create is one policy with four parts:
//!
//! 1. **Identified, never adopted.** An order the venue reports that this client does not track is
//!    a [`crate::reconciliation::Finding::ForeignOrder`] carrying the venue's own status. It is
//!    never inserted into the order index, never given a Nautilus identity, and never reported as
//!    one of this run's orders. A position this run did not create is not a finding at all: the
//!    first whole read adopts it into the position **baseline**, so it is carried and reconciled
//!    against this client's own fills rather than being mistaken for one of them.
//! 2. **Never cancelled and never modified.** Nothing in this adapter's own paths - recovery,
//!    reconciliation, the unknown-outcome probe, the dead man's switch's own trigger - cancels an
//!    order. The one path that can is [`ExecutionClient::cancel_all_orders`], which the *engine*
//!    asks for on an instrument this client trades, and which the venue implements as a
//!    market-wide cancel: the request speaks for a whole market, so it is not scoped to this run's
//!    orders however much this adapter would like it to be. It is issued only when a caller asks
//!    for it, never as a way to tidy the account - and the stop path that would otherwise be
//!    tempted to use it is plan §R3.3's, whose first requirement is that a stop never uses a
//!    market-wide delete to clean up after another run.
//! 3. **Reported read-only.** A foreign order's *readable* status is a known state: the account is
//!    not uncertain because of it, and this client keeps trading. An unreadable one - an unknown
//!    status, or an `untriggered` conditional this adapter does not create - keeps the account
//!    uncertain, because the alternative is treating a state nobody can read as a state that is
//!    fine.
//! 4. **Isolated by identity.** Its fills are not counted (a fill resolves only through this
//!    client's own order index) and its quantity never enters `applied_net`, so the position
//!    judgment compares the venue against this client's own fills *plus* the baseline - which is
//!    what keeps another run's trading from looking like this run's mistake.
//!
//! # What this client does not own
//!
//! The private WebSocket stream, the startup and reconnect reconciliation and the dead man's switch
//! are R3.1's and R3.3's (plan §6.4). The ingestion seams they drive
//! ([`OndoAccountRuntime::ingest_stream_order`] and [`OndoAccountRuntime::ingest_stream_fill`]) are
//! here, because the dedup ledger and the status machine are what make them safe to call from more
//! than one source.
//!
//! # The environment is closed before anything else happens
//!
//! [`OndoExecutionClient::new`] validates the configuration, resolves the REST base URL, and runs
//! [`validate_authenticated_environment`] on the pair. A production configuration is read-only, and
//! a production configuration without `account_read_only` is refused at validation; a base URL that
//! is not the endpoint this session's scope owns
//! ([`crate::common::endpoint::OndoEndpointPolicy`]: the session's own official host, or a loopback
//! test service) is refused before a credential is read and before any socket exists - and
//! [`OndoExecutionConfigError::ProductionOrdersUnsupported`] refuses `allow_production_orders`
//! whatever it is set to. There is no production write branch to configure open
//! (plan §1, §4.1, §R0.3).
//!
//! # The unknown outcome
//!
//! A submission whose answer never arrived, or arrived unreadable, may have been applied at the
//! venue: it is reported as neither accepted nor rejected, the order stays in flight, and it is
//! never resubmitted (plan §6.3). What the client does *not* do is judge the account clean: an
//! order left unresolved - an unknown status, an `untriggered` conditional, a terminal status whose
//! fills do not add up, or a terminal status whose filled quantity cannot be read at all - is
//! reported by the order judgment ([`crate::reconciliation::Finding::UnresolvedOrder`]) and keeps
//! the account out of [`crate::reconciliation::ReconciliationState::Ready`] until more evidence
//! arrives.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    messages::execution::{
        CancelAllOrders, CancelOrder, GenerateFillReports, GenerateOrderStatusReport,
        GenerateOrderStatusReports, GeneratePositionStatusReports, ModifyOrder, QueryOrder,
        SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    Params, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter,
    task::{TaskGroup, TaskSpawner},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::{Mutex, RwLock};
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::ONDO_SETTLEMENT_CURRENCY,
        credential::{
            OndoCredential, validate_authenticated_environment,
            validate_authenticated_websocket_environment,
        },
        enums::OndoAccountIdentity,
        parse::{instrument_id_to_market, market_to_instrument_id, parse_decimal, parse_timestamp},
    },
    config::OndoExecutionClientConfig,
    diagnostics::{OndoReadOnlyDiagnostics, OwnedShutdownStatus},
    http::{
        client::{
            NewRiskPermit, OndoCancelAnswer, OndoHttpClient, OndoNewRiskGuard, OndoNewRiskSendError,
        },
        error::{OndoAuthFailure, OndoHttpError, OndoHttpResult},
        orders::{
            ONDO_POST_ONLY_HAS_MATCH, OndoApiOrder, OndoCancelRejection, OndoOrderCommand,
            OndoOrderError, OndoOrderStatus, OndoRejectedOrder, OndoSide, client_lookup_value,
        },
        private::{
            OndoApiFill, OndoApiFundingFee, OndoFillDirection, OndoOrderHistoryStatus,
            OndoPrivateReadQuery, OndoPrivateResponse,
        },
        query::{CursorWalk, FILLS_PATH, ORDERS_PATH},
        rate_limit::OndoRequestPriority,
    },
    reconciliation::{
        AccountJudgment, AccountReading, Admission, BalanceReading, DeadMansSwitchMessage,
        DeadMansSwitchState, DrainedReports, FundingPayment, FundingReconciliation, JournalOrder,
        JournalSnapshot, JournalStatus, LedgerJournal, LiquidationState, MappedBalance,
        MetadataValidity, NewRiskRefusal, ONDO_STOP_SETTLE_POLL_MS, OrderReading,
        PositionDirection, PositionReading, ProbeOutcome, ProbeReport, ReconciliationBuffer,
        ReconciliationMachine, ReconciliationState, RecoveryPassRefusal, StopOutcome, StopReport,
        StopStep, UncertainOutcome, balance_member,
    },
    websocket::private::{OndoPrivateStream, PrivateRunSnapshot, SharedPrivateDiagnostics},
};

/// How many pages of orders or fills one report generation walks at most.
///
/// The walk itself refuses a repeated cursor; this only bounds an endpoint that keeps handing out
/// fresh ones ([`CursorWalk`]).
const REPORT_MAX_PAGES: usize = 100;

/// How long one client-owned shutdown may take, from the first pre-registration to the point the
/// transport is closed.
///
/// A stop is bounded rather than indefinite, and the bound is on the **whole** operation rather
/// than on its polling tail: the live node wraps the client's `disconnect` in its own
/// `timeout_disconnection` (default 10 s), so the cancel requests, the terminal confirmations, the
/// request-task drain and the switch release all draw on this one budget. The transport's own stop
/// is given its own small allowance after it, because a socket still being read is never left
/// behind.
pub const ONDO_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum production shutdown budget before the private stream's own final close allowance.
///
/// Production stop invalidates the pre-stop snapshot when it freezes new risk, then performs the
/// same multi-read signed reconciliation used at startup before a DMS release becomes eligible.
/// That sequence can legitimately exceed the generic five-second client budget. The production
/// envelope's absolute cleanup deadline still wins: [`ProductionAuthority::remaining`] can only
/// tighten this cap, never extend the approved run.
pub const ONDO_PRODUCTION_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum time a production read-only connection waits for private account readiness.
const ONDO_PRODUCTION_READONLY_READY_MAX_SECS: u64 = 30;

/// Returns the bounded readiness window for a production read-only connection.
///
/// A complete account pass makes several signed reads through the shared one-request-per-second
/// budget. Capping this at eight seconds made the documented fifteen-second HTTP configuration
/// impossible to honor and raced normal startup traffic from the data client.
fn production_readonly_readiness_timeout_secs(http_timeout_secs: u64) -> u64 {
    http_timeout_secs.clamp(1, ONDO_PRODUCTION_READONLY_READY_MAX_SECS)
}

/// The graceful slice of a shutdown budget given to in-flight request tasks before forced abort.
const ONDO_SHUTDOWN_TASK_GRACEFUL: Duration = Duration::from_secs(1);

/// The forced slice given to request tasks after the graceful slice expires.
const ONDO_SHUTDOWN_TASK_ABORT: Duration = Duration::from_secs(1);

/// The balance members the frozen REST spec documents.
///
/// A member outside this set is not folded into the USDC numbers: it is a second collateral asset,
/// a loan, or a member this adapter has not been taught, and the plan requires that to be reported
/// as unsupported rather than merged (plan §6.4).
const DOCUMENTED_BALANCE_MEMBERS: [&str; 16] = [
    "walletBalance",
    "realizedPnl",
    "unrealizedPnl",
    "marginBalance",
    "usedMargin",
    "availableMargin",
    "withdrawableMargin",
    "maintenanceMarginRequirement",
    "totalMaintenanceMargin",
    "marginRatio",
    "leverage",
    "underLiquidation",
    "totalFundingPayments",
    "totalTradingFees",
    "totalPnL",
    "netInvested",
];

/// The settlement currency every fee, commission and balance on this venue is denominated in.
fn settlement_currency() -> Currency {
    Currency::from(ONDO_SETTLEMENT_CURRENCY)
}

/// Builds the Nautilus balance and margin shapes from a verified balance mapping.
///
/// One currency, and the venue's own three numbers: `total` is equity, `locked` is the margin the
/// venue says is committed, and `free` is derived from the two at the settlement currency's
/// precision so the model's `total = locked + free` invariant holds by construction. Margins are
/// carried only when the venue sent a readable maintenance requirement - a `MarginBalance` is a
/// pair, and inventing one of its sides would be a fabricated risk figure - which leaves the
/// balances reported and the margins empty rather than the account unreported.
///
/// # Errors
///
/// Returns an error when a number the venue sent cannot be represented in the settlement currency,
/// which is a value this adapter refuses to round into something else.
fn account_state_parts(
    balance: &MappedBalance,
) -> anyhow::Result<(Vec<AccountBalance>, Vec<MarginBalance>)> {
    let currency = settlement_currency();
    let money = |value: Decimal| {
        Money::from_decimal(value, currency).map_err(|error| anyhow::anyhow!("{error}"))
    };

    let balances = vec![
        AccountBalance::from_total_and_locked(balance.total(), balance.locked(), currency)
            .map_err(|error| anyhow::anyhow!("{error}"))?,
    ];

    let margins = match balance.maintenance() {
        Some(maintenance) => vec![MarginBalance::new(
            money(balance.locked())?,
            money(maintenance)?,
            None,
        )],
        None => Vec::new(),
    };

    Ok((balances, margins))
}

/// The dedup key of an applied fill: `(account_id, fill.id)` (plan §6.3).
type FillKey = (AccountId, String);

/// The fill dedup ledger: the `(account_id, fill.id)` pairs this adapter has already applied.
///
/// A fill is applied **once**, whichever source delivered it. The REST history and the private
/// stream may both carry the same fill, and this is what keeps the second delivery from becoming a
/// second fill. Nothing here expires: plan §6.4 forbids an arbitrary short TTL that would let a
/// historical fill be counted again, and persisting the ledger across runs is Task 8's.
#[derive(Debug, Default)]
pub struct OndoFillLedger {
    applied: ahash::AHashSet<FillKey>,
}

impl OndoFillLedger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether `fill_id` has already been applied for `account_id`.
    #[must_use]
    pub fn contains(&self, account_id: AccountId, fill_id: &str) -> bool {
        self.applied.contains(&(account_id, fill_id.to_string()))
    }

    /// Records `fill_id`, returning `true` when it was new and `false` when it was already there.
    pub fn record(&mut self, account_id: AccountId, fill_id: &str) -> bool {
        self.applied.insert((account_id, fill_id.to_string()))
    }

    /// Removes `fill_id`, for a fill that was recorded but could not be reported after all.
    pub fn forget(&mut self, account_id: AccountId, fill_id: &str) {
        self.applied.remove(&(account_id, fill_id.to_string()));
    }

    /// Returns the number of fills this ledger has applied.
    #[must_use]
    pub fn len(&self) -> usize {
        self.applied.len()
    }

    /// Returns whether the ledger holds no fills at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    /// Returns every `(account_id, fill_id)` pair this ledger has applied.
    ///
    /// This is what a durable journal is written from (plan §6.4): the ledger has no expiry, so a
    /// restart that restores it does not count a historical fill again.
    #[must_use]
    pub fn entries(&self) -> Vec<(AccountId, String)> {
        self.applied.iter().cloned().collect()
    }

    /// Records pairs restored from a durable journal, returning how many were new.
    pub fn restore(&mut self, entries: impl IntoIterator<Item = (AccountId, String)>) -> usize {
        entries
            .into_iter()
            .filter(|entry| self.applied.insert(entry.clone()))
            .count()
    }
}

/// One order as this adapter knows it.
///
/// `filled` is the sum of the **applied fills**, not the venue's `filledSize`: plan §6.3 makes the
/// unique fills the driver of every increment. The venue's own number is compared against it
/// ([`Self::fill_gap`]) rather than adopted, so a venue that reports a filled quantity no fill
/// accounts for leaves the order unresolved instead of silently agreeing.
///
/// The two conditions under which that comparison cannot be satisfied are **different in kind** and
/// are kept apart ([`Self::unappliable_fill`], [`Self::fill_gap`]): one is a fact about the fills
/// this adapter has seen, which more fills can change, and the other is a fact about a fill it will
/// never be able to report, which nothing can.
#[derive(Clone, Debug)]
pub struct OndoOrderState {
    /// The Nautilus client order id: the order's identity in this process.
    pub client_order_id: ClientOrderId,
    /// The venue's own order id, once the venue has named one.
    pub venue_order_id: Option<VenueOrderId>,
    /// The Nautilus instrument the order's market maps to.
    pub instrument_id: InstrumentId,
    /// The order side.
    pub side: OrderSide,
    /// The order type: `Limit` or `Market`, the only two this adapter creates.
    pub order_type: OrderType,
    /// The order's time in force.
    pub time_in_force: TimeInForce,
    /// The order quantity.
    pub quantity: Quantity,
    /// The limit price, when the order is a limit order.
    pub price: Option<Price>,
    /// Whether the order may only reduce a position.
    pub reduce_only: bool,
    /// Whether the order must provide liquidity.
    pub post_only: bool,
    /// The venue's own status, verbatim for a status this adapter does not know.
    pub status: OndoOrderStatus,
    /// Whether the venue has acknowledged the order.
    pub accepted: bool,
    /// The quantity the **applied fills** add up to.
    pub filled: Quantity,
    /// The venue's order-level cumulative fee, kept as a diagnostic.
    ///
    /// It is never charged: the per-fill fees are what the fill reports carry, and adding this on
    /// top would charge the same fee twice (plan §6.3).
    pub venue_fee: Option<Decimal>,
    /// The venue's `lastFillSize`, informational only.
    pub last_fill_size: Option<Quantity>,
    /// The venue's own statement of the filled quantity, when it sent a readable one.
    pub venue_filled: Option<Quantity>,
    /// The last raw payload this adapter applied to the order.
    pub last_raw: String,
    /// Whether a fill this adapter accepted is one it can never express
    /// (`OndoReporter::mark_unappliable_fill`, which is private and so is named rather than
    /// linked).
    ///
    /// This is **permanent**, and that is the whole of what it means: the fill is in the venue's
    /// history and this client's ledger can never carry it, so the order's applied total can never
    /// reach the venue's and nothing that arrives later can change that. It is not the terminal
    /// filled-size disagreement - that one is recomputed from the numbers
    /// ([`Self::fill_gap`]) and clears when the fills catch up.
    pub unappliable_fill: bool,
    /// Whether the order ended in a state this adapter confirmed with the venue.
    pub resolved: bool,
    /// Fills this adapter accepted **before** the venue acknowledged the order.
    pub pending_fills: Vec<OndoApiFill>,
}

impl OndoOrderState {
    /// Reads an order back from a journal entry (plan §R3.2).
    ///
    /// Every member is parsed fallibly. A journal is a file, and a file is something a person can
    /// edit: a value this adapter cannot read is a journal it refuses - the run then reports
    /// [`JournalStatus::Failed`] and takes no new risk - rather than one it reads as a default.
    /// The two diagnostics a payload carries and the journal does not (`venue_fee`, `last_raw`)
    /// come back on the first pass that re-reads the order from the venue, which is the same pass
    /// that re-derives everything else.
    ///
    /// # Errors
    ///
    /// Returns an error naming the member that could not be read.
    pub fn from_journal(entry: &JournalOrder) -> anyhow::Result<Self> {
        fn read<T>(member: &str, value: &str) -> anyhow::Result<T>
        where
            T: std::str::FromStr,
            T::Err: std::fmt::Display,
        {
            value.parse::<T>().map_err(|error| {
                anyhow::anyhow!(
                    "the journal's `{member}` value `{value}` could not be read: {error}"
                )
            })
        }

        let instrument_id = entry
            .instrument_id
            .parse::<InstrumentId>()
            .map_err(|error| {
                anyhow::anyhow!(
                    "the journal's `instrument_id` value `{}` could not be read: {error}",
                    entry.instrument_id,
                )
            })?;

        let price = match entry.price.as_deref() {
            Some(price) => Some(read::<Price>("price", price)?),
            None => None,
        };

        let venue_filled = match entry.venue_filled.as_deref() {
            Some(filled) => Some(read::<Quantity>("venue_filled", filled)?),
            None => None,
        };

        Ok(Self {
            client_order_id: ClientOrderId::from(entry.client_order_id.as_str()),
            venue_order_id: entry.venue_order_id.as_deref().map(VenueOrderId::from),
            instrument_id,
            side: read::<OrderSide>("side", &entry.side)?,
            order_type: read::<OrderType>("order_type", &entry.order_type)?,
            time_in_force: read::<TimeInForce>("time_in_force", &entry.time_in_force)?,
            quantity: read::<Quantity>("quantity", &entry.quantity)?,
            price,
            reduce_only: entry.reduce_only,
            post_only: entry.post_only,
            status: OndoOrderStatus::from_raw(&entry.status),
            accepted: entry.accepted,
            filled: read::<Quantity>("filled", &entry.filled)?,
            venue_fee: None,
            last_fill_size: None,
            venue_filled,
            last_raw: String::new(),
            unappliable_fill: entry.unappliable_fill,
            resolved: entry.resolved,
            pending_fills: Vec::new(),
        })
    }

    /// Returns the venue's terminal filled quantity when the applied fills do not account for it.
    ///
    /// This is **recomputed, not accumulated**: plan §6.3 makes a terminal status terminal only once
    /// the fills agree with it, and the two orderings the venue's two streams may deliver are both
    /// ordinary. A terminal report that arrives before the fills that complete it is a disagreement
    /// now and no disagreement at all once they arrive, so an order that kept the first reading
    /// would stay unresolved over a fill it had already applied. Reading the condition off the
    /// numbers every time is what makes the fills catching up - which arrives on the fill path, not
    /// on the order path - the thing that settles it.
    #[must_use]
    pub fn fill_gap(&self) -> Option<Quantity> {
        if !self.status.is_terminal() {
            return None;
        }

        let venue_filled = self.venue_filled?;

        (venue_filled != self.filled).then_some(venue_filled)
    }

    /// Returns whether this adapter can account for the order's state.
    ///
    /// Accounting for an order means every increment this adapter applied is one the venue's own
    /// reading of the order supports. That fails in three ways: a status this adapter cannot read
    /// (an unknown one, or an `untriggered` conditional it did not create), a fill it can never
    /// report, and a terminal status whose total it cannot check. An order that is simply still
    /// working **is** accounted for - plan §6.3 is about the states the adapter cannot read, not
    /// about the ones it is waiting on.
    ///
    /// The third way is the one [`Self::fill_gap`] cannot see on its own. A terminal payload whose
    /// `filledSize` cannot be read states no total, so there is nothing to check the applied fills
    /// against. That is not the fills agreeing with the venue - it is the check being impossible -
    /// and an order whose terminal reading cannot be checked is not one this adapter can account
    /// for, however clean the total it happens to hold looks. `fill_gap` returning [`None`] there is
    /// right within its own definition ("is there a readable total that disagrees?"); reading that
    /// [`None`] as agreement is what was wrong.
    ///
    /// This is the **single** expression of the question. [`Self::is_unresolved`] and
    /// [`Self::is_settled`] - the latter being the `resolved` flag, recomputed on every path that
    /// can move it - are both read off it, so no two of them can answer differently about the same
    /// order. The account judgment is a third reader of the same facts and reaches the same answer
    /// for a tracked order (`ReconciliationMachine::judge_orders` in `crate::reconciliation`, which
    /// reads them off [`OrderReading`]).
    #[must_use]
    pub fn is_accounted_for(&self) -> bool {
        self.status.is_known()
            && self.status != OndoOrderStatus::Untriggered
            && !self.unappliable_fill
            && (!self.status.is_terminal()
                || self
                    .venue_filled
                    .is_some_and(|venue_filled| venue_filled == self.filled))
    }

    /// Returns whether this order's state is one this adapter **cannot** account for.
    ///
    /// The negation of [`Self::is_accounted_for`] and nothing else, so it may never claim less than
    /// the adapter can actually check. What judges the *account* is the order judgment
    /// (`ReconciliationMachine::judge_orders`); this is the same question asked of one order, and
    /// the two are read off the same facts (plan §6.3).
    #[must_use]
    pub fn is_unresolved(&self) -> bool {
        !self.is_accounted_for()
    }

    /// Returns whether this order is settled: the venue is done with it, this adapter can account
    /// for the total it states, and every fill it applied has been reported.
    ///
    /// The `resolved` flag is this value, recomputed - never accumulated - on every path that can
    /// move it, which is what lets a fill that satisfies a terminal reading settle the order it
    /// arrived after (plan §6.3).
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.status.is_terminal() && self.is_accounted_for() && self.pending_fills.is_empty()
    }
}

/// What applying one order payload did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OndoOrderApplication {
    /// The order's state advanced, and what that implies was emitted.
    Applied,
    /// The payload repeated state this adapter had already applied **and reported**: nothing was
    /// emitted. A repeated acknowledgement is the case this exists for (plan §6.3).
    Unchanged,
    /// The order is in a state this adapter does not resolve: a status it does not know, or an
    /// `untriggered` conditional it does not create.
    ///
    /// The raw payload is kept and the order stays unresolved, so an account holding one is never
    /// judged clean (plan §6.3).
    Unresolved {
        /// Why the order was left unresolved.
        reason: String,
        /// The payload, exactly as the venue sent it.
        raw: String,
    },
    /// The payload's market does not map onto an Ondo Perps instrument, so nothing about it can be
    /// expressed. The payload is kept raw.
    UnmappableMarket {
        /// The venue's market string.
        market: String,
        /// The payload, exactly as the venue sent it.
        raw: String,
    },
    /// The payload is about an order this client neither submitted nor tracks.
    ///
    /// Preserved rather than dropped: an order placed outside this client is still evidence about
    /// the account (plan §6.4).
    Untracked {
        /// The payload, exactly as the venue sent it.
        raw: String,
    },
}

/// What applying one fill did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OndoFillApplication {
    /// The fill is new and its report was emitted.
    Applied,
    /// The fill is new but arrived before the venue acknowledged its order, so it is held until
    /// the acknowledgement lands (plan §6.3: a fill may precede the order's ACK).
    Buffered,
    /// The fill id had already been applied: nothing was emitted. The second delivery of one fill -
    /// the REST history after the stream, or the other way round - is the case this exists for.
    Duplicate,
    /// The fill named an order this client neither submitted nor tracks, so it was neither
    /// recorded nor reported.
    Untracked,
}

/// Which of the two ingestion seams a private report went to.
///
/// The answer is [`OndoAccountRuntime::ingest_stream_order`]'s, never the transport's: a report
/// applied now and a report held for a pass are the two halves of the recovery's merge protocol,
/// and the decision between them is part of the protocol rather than a transport detail
/// (plan §R3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoStreamIngestion {
    /// No pass was reading the account, so the report was applied through the same state machine
    /// the REST pages go through, where the dedup ledger and the order index make it safe.
    Applied,
    /// A pass owns the account, so the report was held for the pass to replay after its REST pages,
    /// where the newest information wins (plan §6.4).
    Buffered,
}

/// How the acknowledgement of a submission **this client made** is reported.
///
/// The submission path holds the Nautilus order, so it reports the acceptance through the order
/// event API; a query, a cancel answer and a reconciliation read hold only the venue's payload, so
/// they report through status reports. Both are Nautilus's own types and both carry the same facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Acceptance {
    /// Emit the Nautilus order event, using the snapshot the submission held.
    Event,
    /// Emit a status report.
    Report,
}

/// The order index, the fill ledger and the watermark a journal is written from.
#[derive(Debug, Default)]
struct OndoPrivateState {
    orders: AHashMap<ClientOrderId, OndoOrderState>,
    by_venue_order_id: AHashMap<String, ClientOrderId>,
    fills: OndoFillLedger,
    /// The newest applied fill's instant, which is how far the ledger reaches.
    ///
    /// It is a fact about the fills this client **applied**, so it moves where they move and
    /// nowhere else. It survives a restart by being read back out of the journal's own watermark
    /// ([`OndoAccountRuntime::load_journal`] puts it back through the same monotone rule this uses,
    /// not by a second path), so a run that restored a ledger and then applied nothing new reports
    /// the coverage it actually holds rather than none.
    watermark: Option<UnixNanos>,
}

impl OndoPrivateState {
    /// Resolves the order an `ApiOrder` payload is about.
    fn resolve_order(&self, payload: &OndoApiOrder) -> Option<ClientOrderId> {
        if let Some(client_order_id) = payload.client_order_id() {
            let id = ClientOrderId::from(client_order_id);

            if self.orders.contains_key(&id) {
                return Some(id);
            }
        }

        self.by_venue_order_id.get(payload.order_id()).copied()
    }

    /// Resolves the order a fill is about.
    fn resolve_fill(&self, fill: &OndoApiFill) -> Option<ClientOrderId> {
        if let Some(client_order_id) = fill.client_order_id() {
            let id = ClientOrderId::from(client_order_id);

            if self.orders.contains_key(&id) {
                return Some(id);
            }
        }

        self.by_venue_order_id.get(fill.order_id()).copied()
    }

    /// Returns the venue order id this session has seen for a client order id.
    fn venue_order_id_for(&self, client_order_id: &ClientOrderId) -> Option<String> {
        self.orders
            .get(client_order_id)
            .and_then(|state| state.venue_order_id.as_ref())
            .map(ToString::to_string)
    }

    /// Returns the client order id this session has seen for a venue order id.
    fn client_order_id_for(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.by_venue_order_id.get(venue_order_id.as_str()).copied()
    }

    /// Puts back the order associations a restored journal holds.
    ///
    /// The index is rebuilt exactly as a submission builds it - the order under its Nautilus id,
    /// the venue's id pointing at it - so a payload that arrives after the restart resolves to the
    /// same order it resolved to before it.
    fn restore_orders(&mut self, orders: Vec<OndoOrderState>) {
        for order in orders {
            if let Some(venue_order_id) = order.venue_order_id {
                self.by_venue_order_id
                    .insert(venue_order_id.to_string(), order.client_order_id);
            }

            self.orders.insert(order.client_order_id, order);
        }
    }

    /// Moves the watermark to `ts_event` when it is newer than the one held.
    fn observe_fill(&mut self, ts_event: UnixNanos) {
        self.watermark = Some(match self.watermark {
            Some(current) => current.max(ts_event),
            None => ts_event,
        });
    }

    /// Returns the signed net quantity this session's applied fills add up to, per instrument.
    ///
    /// This is what a reconciliation pass compares against the venue's own positions (plan §6.4):
    /// the fills are the driver of every increment, so a position the fills do not explain is a
    /// discrepancy rather than a number to adopt.
    fn applied_net(&self) -> BTreeMap<InstrumentId, Decimal> {
        let mut net: BTreeMap<InstrumentId, Decimal> = BTreeMap::new();

        for order in self.orders.values() {
            let signed = match order.side {
                OrderSide::Buy => order.filled.as_decimal(),
                OrderSide::Sell => -order.filled.as_decimal(),
            };

            *net.entry(order.instrument_id).or_insert(Decimal::ZERO) += signed;
        }

        net
    }
}

/// The part of the execution client that applies payloads and emits reports.
///
/// It is deliberately separate from [`OndoExecutionClient`] and holds no cache and no task group,
/// so it is `Send`: a spawned request task can carry a clone of it and apply the answer it
/// received. The client delegates its ingestion seams to one of these, which is what makes the
/// stream path and the REST path the *same* state machine rather than two that resemble each
/// other.
#[derive(Debug, Clone)]
struct OndoReporter {
    account_id: AccountId,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    state: Arc<RwLock<OndoPrivateState>>,
}

impl OndoReporter {
    fn now(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Applies one `ApiOrder` payload.
    fn apply_order(
        &self,
        payload: &OndoApiOrder,
        acceptance: Acceptance,
        order: Option<&OrderAny>,
    ) -> OndoOrderApplication {
        let raw = payload.raw().to_string();

        if market_to_instrument_id(payload.market()).is_err() {
            log::error!(
                "Ondo order {} names market `{}`, which is not an Ondo Perps market; it is kept raw",
                payload.order_id(),
                payload.market(),
            );

            return OndoOrderApplication::UnmappableMarket {
                market: payload.market().to_string(),
                raw,
            };
        }

        let Some(client_order_id) = self.state.read().resolve_order(payload) else {
            log::warn!(
                "Ondo order {} is not one this client submitted or tracks; it is kept raw",
                payload.order_id(),
            );

            return OndoOrderApplication::Untracked { raw };
        };

        let status = payload.status().clone();

        if !status.is_known() || status == OndoOrderStatus::Untriggered {
            let reason = if status.is_known() {
                "the venue reports an `untriggered` conditional order, which this adapter does not \
                 create"
                    .to_string()
            } else {
                format!(
                    "the venue's status `{}` is not one this adapter knows",
                    status.as_str()
                )
            };

            self.record(payload, &client_order_id, &status);

            log::error!(
                "Ondo order {client_order_id} (venue {}) is unresolved: {reason}",
                payload.order_id(),
            );

            return OndoOrderApplication::Unresolved { reason, raw };
        }

        // Plan §6.3: `pending` stays pending and generates no fill. The venue holds the request
        // without working it, so nothing is acknowledged here.
        if status == OndoOrderStatus::Pending {
            self.record(payload, &client_order_id, &status);

            return OndoOrderApplication::Applied;
        }

        let Some(advance) = self.advance(payload, &client_order_id, &status, &raw) else {
            return OndoOrderApplication::Untracked { raw };
        };

        if !advance.newly_accepted && !advance.changed {
            return OndoOrderApplication::Unchanged;
        }

        match (advance.newly_accepted, acceptance, order) {
            (true, Acceptance::Event, Some(order)) => self.emitter.emit_order_accepted(
                order,
                VenueOrderId::from(payload.order_id()),
                event_time(payload, self.now()),
            ),
            _ => {
                self.emit_status_report(&advance.snapshot, payload, event_time(payload, self.now()))
            }
        }

        for fill in &advance.pending {
            self.flush_fill(&client_order_id, fill);
        }

        if !advance.pending.is_empty() {
            // The terminal state was decided before the fills it was waiting for were reported;
            // whether the order is resolved is only knowable once they are.
            self.settle(&client_order_id);
        }

        OndoOrderApplication::Applied
    }

    /// Recomputes whether an order is resolved, from its current state.
    ///
    /// It is called on **both** paths that can change the answer: a payload that advances the
    /// order's status, and a fill that moves the applied total towards the venue's. A terminal
    /// report and the fills that complete it are delivered by two streams in either order, so a
    /// fill whose arrival satisfies a terminal reading has to be able to settle it (plan §6.3).
    fn settle(&self, client_order_id: &ClientOrderId) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.resolved = entry.is_settled();
        }
    }

    /// Applies one `ApiFill` payload: the one path a fill is ever counted from.
    ///
    /// A fill is applied once per `(account_id, fill.id)`; a second delivery returns
    /// [`OndoFillApplication::Duplicate`] and emits nothing. A fill whose order this client does
    /// not track is neither recorded nor reported, so it can still be applied once the order is
    /// known.
    ///
    /// # Errors
    ///
    /// Returns an error when the fill cannot be expressed at all: a market that does not map onto
    /// an instrument, an absent or unreadable `size`/`price`, or a `direction` from which the order
    /// side cannot be read. Nothing is recorded in that case, so a corrected delivery still applies.
    fn apply_fill(&self, fill: &OndoApiFill) -> anyhow::Result<OndoFillApplication> {
        let account_id = self.account_id;

        let Some(client_order_id) = ({
            let state = self.state.read();

            if state.fills.contains(account_id, fill.id()) {
                return Ok(OndoFillApplication::Duplicate);
            }

            state.resolve_fill(fill)
        }) else {
            log::warn!(
                "Ondo fill {} names order {} which this client does not track; it is neither \
                 recorded nor reported",
                fill.id(),
                fill.order_id(),
            );

            return Ok(OndoFillApplication::Untracked);
        };

        // Built before anything is recorded: a fill this adapter cannot express must not be
        // counted as applied.
        let report = self.fill_report(Some(client_order_id), fill)?;
        let quantity = report.last_qty;
        let ts_event = report.ts_event;

        {
            let mut state = self.state.write();

            if !state.fills.record(account_id, fill.id()) {
                return Ok(OndoFillApplication::Duplicate);
            }

            state.observe_fill(ts_event);

            let Some(entry) = state.orders.get_mut(&client_order_id) else {
                state.fills.forget(account_id, fill.id());

                return Ok(OndoFillApplication::Untracked);
            };

            if !entry.accepted {
                entry.pending_fills.push(fill.clone());

                return Ok(OndoFillApplication::Buffered);
            }

            entry.filled = add_quantity(entry.filled, quantity)?;
        }

        // The fill may be the one a terminal reading was waiting for: a report that arrived first
        // is only a disagreement until the fills it counted arrive (plan §6.3).
        self.settle(&client_order_id);

        self.emitter.send_fill_report(report);

        Ok(OndoFillApplication::Applied)
    }

    /// Records a payload that carries no usable transition, keeping its raw data.
    ///
    /// Neither `pending` nor a status this adapter does not resolve acknowledges the order: plan
    /// §6.3 keeps `pending` pending, and an unreadable status confirms nothing at all. The order is
    /// left unaccepted, so no fill of it may be reported either.
    fn record(
        &self,
        payload: &OndoApiOrder,
        client_order_id: &ClientOrderId,
        status: &OndoOrderStatus,
    ) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.status = status.clone();
            entry.last_raw = payload.raw().to_string();
            entry.venue_order_id = Some(VenueOrderId::from(payload.order_id()));
            entry.venue_filled = payload.filled_quantity().ok();
            entry.resolved = false;
        }

        state
            .by_venue_order_id
            .insert(payload.order_id().to_string(), *client_order_id);
    }

    /// Advances a tracked order to the payload's state and returns what the caller must report.
    ///
    /// [`None`] means the order left the index between the read that resolved it and this write,
    /// which only a definitive refusal does.
    fn advance(
        &self,
        payload: &OndoApiOrder,
        client_order_id: &ClientOrderId,
        status: &OndoOrderStatus,
        raw: &str,
    ) -> Option<Advance> {
        let mut state = self.state.write();

        let entry = state.orders.get_mut(client_order_id)?;

        let venue_filled = payload.filled_quantity().ok();
        let newly_accepted = !entry.accepted;
        let changed =
            newly_accepted || entry.status != *status || entry.venue_filled != venue_filled;
        // Whether the order was short of a terminal reading before this payload moved it.
        let was_short = entry.fill_gap().is_some();

        entry.accepted = true;
        entry.status = status.clone();
        entry.last_raw = raw.to_string();
        entry.venue_order_id = Some(VenueOrderId::from(payload.order_id()));
        entry.venue_filled = venue_filled;
        entry.venue_fee = payload.fee().and_then(|fee| parse_decimal(fee, "fee").ok());
        entry.last_fill_size = payload
            .last_fill_size()
            .and_then(|size| parse_decimal(size, "lastFillSize").ok())
            .and_then(|size| Quantity::from_decimal(size).ok());

        // Plan §6.3: a terminal status is only terminal once the fills agree with it. A venue
        // filled quantity the applied fills do not account for leaves the order unresolved rather
        // than silently agreeing with it - and it stops doing so the moment the fills that account
        // for it are applied, which [`Self::settle`] is what notices. The condition itself is not
        // stored ([`OndoOrderState::fill_gap`]), so what is left here is saying so once, on the
        // payload that opened the disagreement, rather than on every read of it afterwards.
        if let Some(venue_filled) = entry.fill_gap() {
            if !was_short {
                log::error!(
                    "Ondo order {client_order_id} is {} at the venue with filledSize {venue_filled} \
                     but the applied fills total {}; the order stays unresolved",
                    status.as_str(),
                    entry.filled,
                );
            }
        } else if was_short && entry.is_accounted_for() {
            // "Settled" is only said of a reading this adapter can account for: a status it cannot
            // read, or a terminal one whose total is unreadable, is not an agreement however the
            // gap happened to close.
            log::info!(
                "Ondo order {client_order_id} is settled at the venue: the applied fills total {} \
                 and the venue's filledSize agree",
                entry.filled,
            );
        }

        entry.resolved = entry.is_settled();

        let snapshot = entry.clone();
        let pending = std::mem::take(&mut entry.pending_fills);

        state
            .by_venue_order_id
            .insert(payload.order_id().to_string(), *client_order_id);

        Some(Advance {
            snapshot,
            pending,
            newly_accepted,
            changed,
        })
    }

    /// Reports a fill that was held until its order's acknowledgement.
    fn flush_fill(&self, client_order_id: &ClientOrderId, fill: &OndoApiFill) {
        let report = match self.fill_report(Some(*client_order_id), fill) {
            Ok(report) => report,
            Err(error) => {
                // A fill this adapter accepted and then could not express leaves the order
                // unresolved rather than silently short of its venue quantity.
                self.mark_unappliable_fill(client_order_id);

                log::error!(
                    "Ondo buffered fill {} for {client_order_id} could not be reported: {error}",
                    fill.id(),
                );

                return;
            }
        };

        let quantity = report.last_qty;

        self.emitter.send_fill_report(report);

        if let Err(error) = self.credit(client_order_id, quantity) {
            log::error!("Ondo order {client_order_id} could not be credited {quantity}: {error}");

            return;
        }

        // The total moved, so whether a terminal reading is satisfied may have moved with it.
        self.settle(client_order_id);
    }

    /// Adds an applied fill's quantity to its order's total.
    fn credit(&self, client_order_id: &ClientOrderId, quantity: Quantity) -> anyhow::Result<()> {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.filled = add_quantity(entry.filled, quantity)?;
        }

        Ok(())
    }

    /// Returns the order reference a cancel or a query for this order should use.
    ///
    /// The venue order id is preferred because it is unambiguous - the venue always knows it,
    /// including for an order whose client order id it never received. The `client:{clientOrderId}`
    /// form is the fallback for an order whose venue id this session has not observed yet
    /// (plan §6.2, §6.3).
    fn order_ref(&self, client_order_id: &ClientOrderId) -> String {
        match self.state.read().venue_order_id_for(client_order_id) {
            Some(venue_order_id) => venue_order_id,
            None => client_lookup_value(client_order_id.as_str()),
        }
    }

    /// Inserts the state a submission starts from.
    ///
    /// This lives on the reporter rather than on the client because the submission task that needs
    /// it runs after the command has left the client's own borrow: the task owns a clone of the
    /// reporter and nothing else.
    fn track_submission(
        &self,
        command: &OndoOrderCommand,
        client_order_id: ClientOrderId,
        instrument_id: InstrumentId,
    ) {
        self.state.write().orders.insert(
            client_order_id,
            OndoOrderState {
                client_order_id,
                venue_order_id: None,
                instrument_id,
                side: command.side,
                order_type: command.order_type,
                time_in_force: command.time_in_force,
                quantity: command.quantity,
                price: command.price,
                reduce_only: command.reduce_only,
                post_only: command.post_only,
                status: OndoOrderStatus::Pending,
                accepted: false,
                filled: Quantity::from("0"),
                venue_fee: None,
                last_fill_size: None,
                venue_filled: None,
                last_raw: String::new(),
                unappliable_fill: false,
                resolved: false,
                pending_fills: Vec::new(),
            },
        );
    }

    /// Returns the orders this client is still tracking on `market`, with the venue order id each
    /// one is known by.
    fn tracked_orders_on(&self, market: &str) -> Vec<(ClientOrderId, Option<VenueOrderId>)> {
        self.state
            .read()
            .orders
            .values()
            .filter(|state| {
                instrument_id_to_market(&state.instrument_id)
                    .is_ok_and(|instrument_market| instrument_market == market)
            })
            .map(|state| (state.client_order_id, state.venue_order_id))
            .collect()
    }

    /// Drops an order from the index after a refusal that never reached the venue's book.
    fn forget(&self, client_order_id: &ClientOrderId) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.remove(client_order_id)
            && let Some(venue_order_id) = entry.venue_order_id
        {
            state.by_venue_order_id.remove(venue_order_id.as_str());
        }
    }

    /// Marks an order as one holding a fill this adapter accepted and can never express, so it is
    /// never reported as resolved.
    ///
    /// This is the one condition that is not recomputed, and it must not be: the fill is in the
    /// venue's history, this client's ledger cannot carry it, and the applied total can therefore
    /// never reach the venue's. Nothing that arrives later changes that, so nothing later may clear
    /// it. The terminal filled-size disagreement, which more fills *can* clear, is not this
    /// ([`OndoOrderState::fill_gap`]).
    fn mark_unappliable_fill(&self, client_order_id: &ClientOrderId) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.unappliable_fill = true;
            entry.resolved = false;
        }
    }

    /// Builds and sends the status report one payload implies.
    fn emit_status_report(
        &self,
        order: &OndoOrderState,
        payload: &OndoApiOrder,
        ts_event: UnixNanos,
    ) {
        match self.status_report(payload, Some(order), ts_event) {
            Ok(report) => self.emitter.send_order_status_report(report),
            Err(error) => log::error!(
                "Ondo order {} could not be reported as a status report: {error}",
                payload.order_id(),
            ),
        }
    }

    /// Builds the [`OrderStatusReport`] for one payload.
    ///
    /// `tracked` supplies what only this client knows - the Nautilus identity and the flags it
    /// sent. Without it the report is built from the payload alone, which is what a bulk read of
    /// orders this client did not place needs (plan §6.4: an external order is identified, not
    /// dropped).
    ///
    /// For an order this client **tracks**, `filled_qty` is the quantity the **applied fills** add
    /// up to, and `order_status` is the state those fills support: a terminal venue status the
    /// applied total does not account for is reported as the state the ledger *can* support rather
    /// than as the completion it cannot (plan §6.3). The reason is what the report does downstream:
    /// the engine reconciles a `Filled`/`PartiallyFilled` report whose quantity is above the
    /// cache's by **inferring the difference as a fill** at the report's average price, so a report
    /// that carried the venue's terminal quantity would have the cache - and the position built
    /// from it - complete and price an order on a fill this adapter never reported.
    ///
    /// Withholding the report instead was the alternative, and it is the worse one: a report is
    /// emitted only for a payload that moves the order, so the terminal state withheld from the
    /// cache would arrive only if the venue repeated itself, and until then the adapter's ledger
    /// and the cache would disagree with nothing left to reconcile them. The completion does reach
    /// the cache, by the path plan §6.3 makes the driver of every increment - the fills themselves,
    /// which cover the order's quantity when they arrive.
    ///
    /// An order this client does **not** track has no ledger to report from, so the payload is all
    /// there is: its `filledSize` and its status are reported as the venue sent them.
    fn status_report(
        &self,
        payload: &OndoApiOrder,
        tracked: Option<&OndoOrderState>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let venue_filled = payload.filled_quantity()?;

        let (order_status, filled_qty) = match tracked {
            None => (
                venue_order_status(payload.status(), venue_filled)?,
                venue_filled,
            ),
            Some(order) => (
                order_status_for_ledger(payload.status(), venue_filled, order)?,
                order.filled,
            ),
        };

        let instrument_id = match tracked {
            Some(order) => order.instrument_id,
            None => market_to_instrument_id(payload.market())?,
        };

        let client_order_id = match tracked {
            Some(order) => Some(order.client_order_id),
            None => payload.client_order_id().map(ClientOrderId::from),
        };

        let order_side = match tracked {
            Some(order) => Some(order.side),
            None => Some(payload.side().to_order_side()),
        };

        let order_type = match tracked {
            Some(order) => order.order_type,
            None => nautilus_order_type(payload.order_type())?,
        };

        let time_in_force = match tracked {
            Some(order) => order.time_in_force,
            None => nautilus_time_in_force(payload.order_type(), payload.time_in_force())?,
        };

        let mut report = OrderStatusReport::new(
            self.account_id,
            instrument_id,
            client_order_id,
            VenueOrderId::from(payload.order_id()),
            order_side,
            order_type,
            time_in_force,
            order_status,
            payload.quantity()?,
            filled_qty,
            payload.ts_created().unwrap_or(ts_event),
            ts_event,
            self.now(),
            None, // report_id
        );

        if let Some(order) = tracked {
            report.post_only = order.post_only;
            report.reduce_only = order.reduce_only;
            report.price = order.price;
        } else {
            // An order this adapter never submitted, so its own record is all there is. `reduceOnly`
            // is not in `ApiOrder`'s required set, and Nautilus's `OrderStatusReport.reduce_only` is
            // a plain `bool` with no way to say "the venue did not tell us" - so an absent member
            // reads as `false` here. That is a forced default, not evidence: for an order this
            // adapter *did* submit the tracked value above is used instead, and that path is the one
            // the execution client's own orders take.
            report.reduce_only = payload.reduce_only().unwrap_or(false);
            report.price = payload
                .price()
                .map(|price| parse_decimal(price, "price"))
                .transpose()?
                .map(Price::from_decimal)
                .transpose()?;
        }

        report.avg_px = average_price(payload);
        report.cancel_reason = payload.cancel_reason().map(ToString::to_string);

        Ok(report)
    }

    /// Builds the [`FillReport`] for one fill.
    ///
    /// The commission is **this fill's own fee**, in the settlement currency. The venue's
    /// order-level cumulative `fee` is never added: it already contains this fill's fee, and
    /// charging both would count one fee twice (plan §6.3).
    ///
    /// A fill whose `fee` cannot be read is refused, exactly as one whose `price` or `size` cannot
    /// be read is: `fee` is one of the ten members the frozen spec's `ApiFill` **requires**, so its
    /// absence is a payload this adapter does not understand rather than a zero-cost trade. Booking
    /// an unreadable fee as `0` would put a real cost into the PnL as a fabricated free trade, and
    /// nothing downstream could tell the two apart.
    fn fill_report(
        &self,
        client_order_id: Option<ClientOrderId>,
        fill: &OndoApiFill,
    ) -> anyhow::Result<FillReport> {
        let fee = fill
            .fee()
            .ok_or_else(|| anyhow::anyhow!("Ondo fill {} carries no `fee`", fill.id()))?;
        let commission = Money::from_decimal(parse_decimal(fee, "fee")?, settlement_currency())?;

        Ok(FillReport::new(
            self.account_id,
            market_to_instrument_id(fill.market())?,
            VenueOrderId::from(fill.order_id()),
            TradeId::from(fill.id()),
            fill_order_side(fill)?,
            fill_quantity(fill)?,
            fill_price(fill)?,
            commission,
            liquidity_side(fill),
            client_order_id,
            None, // venue_position_id: this venue is netting
            fill_timestamp(fill).unwrap_or_else(|| self.now()),
            self.now(),
            None, // report_id
        ))
    }
}

/// Writes one order's association and applied state into the form a restart reads back.
///
/// The enums are written as their `SCREAMING_SNAKE_CASE` names, which is what their own `FromStr`
/// reads, and the venue's status is written as the venue spells it - a status this adapter does not
/// know round-trips unchanged, because that is what the order index holds.
impl JournalOrder {
    #[must_use]
    pub fn from_state(state: &OndoOrderState) -> Self {
        Self {
            client_order_id: state.client_order_id.to_string(),
            venue_order_id: state.venue_order_id.as_ref().map(ToString::to_string),
            instrument_id: state.instrument_id.to_string(),
            side: state.side.as_ref().to_string(),
            order_type: state.order_type.as_ref().to_string(),
            time_in_force: state.time_in_force.as_ref().to_string(),
            quantity: state.quantity.to_string(),
            price: state.price.as_ref().map(ToString::to_string),
            reduce_only: state.reduce_only,
            post_only: state.post_only,
            status: state.status.as_str().to_string(),
            accepted: state.accepted,
            filled: state.filled.to_string(),
            venue_filled: state.venue_filled.as_ref().map(ToString::to_string),
            unappliable_fill: state.unappliable_fill,
            resolved: state.resolved,
        }
    }
}

/// What one payload did to a tracked order.
#[derive(Debug)]
struct Advance {
    /// The order as it stands after the payload.
    snapshot: OndoOrderState,
    /// The fills that were waiting for the order's acknowledgement, oldest first.
    pending: Vec<OndoApiFill>,
    /// Whether this payload is the one that acknowledged the order.
    newly_accepted: bool,
    /// Whether the payload said anything the order's recorded state did not already hold.
    changed: bool,
}

/// The orders one pass saw, each at the newest payload the pass applied for it.
///
/// A pass sees an order more than once - the venue's pages can repeat it, and a stream report can
/// name it again - and the last one it saw is the order's state. The reading is built from this
/// rather than from the pages alone, so an order only the stream mentioned is part of what the pass
/// read, and an order both sources carried is read at the state the pass left it in (plan §6.4).
#[derive(Debug, Default)]
struct ObservedOrders {
    /// The payloads, in the order the pass first saw each venue order id.
    entries: Vec<OndoApiOrder>,
    /// Where each venue order id's payload sits in `entries`.
    index: BTreeMap<String, usize>,
}

impl ObservedOrders {
    /// Records one payload as the newest this pass has seen for its order.
    fn observe(&mut self, payload: &OndoApiOrder) {
        match self.index.get(payload.order_id()) {
            Some(at) => self.entries[*at] = payload.clone(),
            None => {
                self.index
                    .insert(payload.order_id().to_string(), self.entries.len());
                self.entries.push(payload.clone());
            }
        }
    }
}

/// Whether a recovery pass owns the account right now (plan §6.4).
///
/// The claim is one atomic rather than a lock, because a pass holds it across its four reads and a
/// lock held across an await on the account's own state is a deadlock waiting for a slow venue.
///
/// The atomic carries the id of the claim that owns the account, not a flag, because a pass ends its
/// claim twice - once at its drain and once when its guard drops - and between the two another pass
/// may legitimately have claimed the account. An end that could only set "free" would then release a
/// claim that is no longer its own, and a third pass would run beside the second: two passes
/// advancing one account is the whole of what the claim exists to prevent.
#[derive(Debug, Default)]
struct PassOwnership {
    /// The id of the pass that owns the account, or zero when no pass does.
    claimed_by: AtomicU64,
    /// Issues claim ids; see [`Self::next_claim_id`].
    next_id: AtomicU64,
}

impl PassOwnership {
    /// Claims the account for one pass, or [`None`] when another pass already holds it.
    ///
    /// The guard is built on the successful branch alone: one built and dropped on the way out of a
    /// refused claim would release the claim of the pass that holds it.
    fn claim(&self) -> Option<PassGuard<'_>> {
        let id = self.next_claim_id();

        if self
            .claimed_by
            .compare_exchange(0, id, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }

        Some(PassGuard { owner: self, id })
    }

    /// Returns whether a pass owns the account right now.
    ///
    /// This is the **only** answer to that question in this crate. The inbound routing decision
    /// ([`OndoAccountRuntime::ingest_stream_order`]) reads it rather than keeping a flag of its own:
    /// a second source of "is a pass running" would be able to disagree with the claim, and the
    /// claim is what a pass actually holds.
    fn is_claimed(&self) -> bool {
        self.claimed_by.load(Ordering::Acquire) != 0
    }

    /// Issues the id that names the next claim.
    ///
    /// Zero means "no owner", so the counter starts at one and a wrapped counter skips past it: a
    /// claim issued the id zero would leave the account reading as free while its holder believed it
    /// owned it, which is the state this type exists to make impossible.
    fn next_claim_id(&self) -> u64 {
        match self.next_id.fetch_add(1, Ordering::Relaxed) {
            0 => self.next_id.fetch_add(1, Ordering::Relaxed),
            id => id,
        }
    }
}

/// A pass's claim on the account, released when it ends or when the pass leaves.
#[derive(Debug)]
struct PassGuard<'a> {
    owner: &'a PassOwnership,
    /// The claim this guard holds. Zero is never issued, so a guard always names a real claim.
    id: u64,
}

impl PassGuard<'_> {
    /// Ends the pass, so the next one may claim the account.
    ///
    /// It is idempotent on purpose: a pass ends twice, once at the drain - under the same boundary
    /// that takes the reports the stream buffered - and once when this guard drops, and the second
    /// one must not be able to re-open a claim or to close one the drain has already closed.
    ///
    /// The claim is released only while it is still **this** claim's. A pass that ended at its drain
    /// and then let another pass claim the account leaves that other pass's claim alone when its own
    /// guard drops: releasing it would let a third pass run beside the second, and the account would
    /// be advanced by two passes with nothing refused and nothing reported.
    fn end(&self) {
        let _ =
            self.owner
                .claimed_by
                .compare_exchange(self.id, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

impl Drop for PassGuard<'_> {
    fn drop(&mut self) {
        self.end();
    }
}

/// The live execution client for the Ondo Perps API.
///
/// Order entry, cancellation and the private order and fill reads go through [`OndoHttpClient`],
/// signed with the sandbox credential. The client is built from an [`ExecutionClientCore`], which
/// carries its identity and connection state, and an [`OndoExecutionClientConfig`], whose
/// environment is gated before the client exists.
#[derive(Debug)]
pub struct OndoExecutionClient {
    core: ExecutionClientCore,
    config: OndoExecutionClientConfig,
    http_client: OndoHttpClient,
    reporter: OndoReporter,
    /// The account's state, shared with every spawned task the way the reporter is: a submission
    /// that loses its answer records the unknown outcome from inside its own task.
    reconciliation: Arc<RwLock<ReconciliationMachine>>,
    /// The account half, shared with the private transport.
    ///
    /// A transport task must be `Send` and this client is not, so the transport is handed this
    /// rather than the client. It holds the same instances through the same `Arc`s, which is what
    /// makes the report buffer, the recovery claim, the reconciliation machine and the event
    /// emitter one set of objects rather than two that agree.
    account: OndoAccountRuntime,
    /// The sanitized, bounded diagnostics a read-only probe reads after the run.
    ///
    /// It shares the account and, once a transport is started, that transport's run-state and
    /// diagnostic handles. It holds counters and fixed labels only - never a frame, a credential,
    /// an account id or an amount.
    read_only_diagnostics: OndoReadOnlyDiagnostics,
    /// The credential, shared with the private transport's login rather than copied into it.
    ///
    /// One [`Arc<OndoCredential>`] signs the REST requests and the WebSocket login alike: the
    /// secret exists once in the process, and neither surface can print it.
    credential: Arc<OndoCredential>,
    /// The private transport, once [`Self::connect`] has started it.
    private_stream: Option<OndoPrivateStream>,
    tasks: TaskGroup,
    /// What the shutdown hooks left, preserved across repeated calls.
    ///
    /// A dirty record stops a later hook from reporting success over work it did not finish, and
    /// the synchronous fallback records that the ordered cleanup is still owed so a later
    /// `disconnect` runs it instead of early-returning on the absent transport.
    last_shutdown: Option<ShutdownRecord>,
}

/// A single bounded budget shared by every phase of one shutdown.
struct ShutdownBudget {
    deadline: std::time::Instant,
}

impl ShutdownBudget {
    fn new(timeout: Duration) -> Self {
        Self {
            deadline: std::time::Instant::now() + timeout,
        }
    }

    /// The time left before the budget expires; zero once it has.
    fn remaining(&self) -> Duration {
        self.deadline
            .saturating_duration_since(std::time::Instant::now())
    }
}

/// What the shutdown hooks left, preserved across repeated lifecycle calls.
#[derive(Clone, Debug)]
struct ShutdownRecord {
    /// The ordered stop's report, when the asynchronous hook ran one.
    report: Option<StopReport>,
    /// Whether the request task generation drained.
    tasks_drained: bool,
}

impl ShutdownRecord {
    /// Returns whether the shutdown finished with nothing outstanding.
    fn is_complete(&self) -> bool {
        self.tasks_drained && self.report.as_ref().is_some_and(StopReport::is_clean)
    }

    /// Renders the incomplete shutdown as an error naming what is left.
    fn error(&self) -> anyhow::Error {
        let outcome = self.report.as_ref().map_or_else(
            || "no ordered stop ran".to_string(),
            |report| format!("{:?}", report.outcome),
        );
        let outstanding = self
            .report
            .as_ref()
            .map(StopReport::outstanding)
            .unwrap_or_default();
        let released = self
            .report
            .as_ref()
            .is_some_and(|report| report.released_switch);

        anyhow::anyhow!(
            "the Ondo shutdown left work unresolved: outcome={outcome}, tasks_drained={}, \
             switch_released={released}, outstanding={outstanding:?}",
            self.tasks_drained,
        )
    }
}

/// Writes the ledger checkpoint when a stop leaves, however it leaves.
///
/// [`OndoExecutionClient::stop_and_wait`] writes the checkpoint at its end, but the caller may bound
/// the stop and drop the future before it reaches there - the live node wraps `disconnect` in
/// `timeout_disconnection`. Without this guard a stop cut short mid-wait would lose the unsettled
/// writes it had already registered. The checkpoint is a whole-file replace, so writing it twice is
/// harmless, and the write is synchronous, so dropping the future cannot cancel it.
struct JournalCheckpoint(OndoAccountRuntime);

impl Drop for JournalCheckpoint {
    fn drop(&mut self) {
        if let Some(guard) = &self.0.production {
            guard.note_release_shutdown_timeout();
        }
        self.0.persist_journal(self.0.now());
    }
}

/// Forces cancellation if the caller abandons the request-task drain.
struct RequestDrainGuard<'a> {
    tasks: &'a TaskGroup,
    completed: bool,
}

impl Drop for RequestDrainGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.tasks.abort();
        }
    }
}

impl OndoExecutionClient {
    /// Builds the client from its core identity and configuration.
    ///
    /// Nothing here performs I/O. The configuration validation and the environment gate run before
    /// the HTTP client is constructed, so a refused configuration has no object to send from:
    ///
    /// 1. [`OndoExecutionClientConfig::validate`], which refuses `allow_production_orders` and a
    ///    missing account;
    /// 2. [`validate_authenticated_environment`] on the resolved base URL, which refuses every
    ///    authority the endpoint allowlist does not carry - the session's own official host and a
    ///    loopback test service are the only two it admits;
    /// 3. the credential: the configuration's own pair when it carries one, otherwise
    ///    [`crate::common::credential::resolve_credential`] from the process environment, which
    ///    errors naming the missing variable instead of falling back to another account.
    ///
    /// # Errors
    ///
    /// Returns the named configuration, environment or credential error, or a transport
    /// construction error.
    pub fn new(
        core: ExecutionClientCore,
        config: OndoExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        Self::with_credential(core, config, None, None)
    }

    /// Builds the client from an optional credential and an optional shared REST budget.
    ///
    /// A [`None`] credential is resolved exactly as [`Self::new`] resolves it. This is the
    /// constructor the adapter's own tests use - they pass a credential they built themselves, so
    /// no test reads the process environment - and the one the factory uses to hand in the
    /// environment's shared budget. It runs the same configuration and environment checks as
    /// [`Self::new`], so it cannot be used to reach an environment the gate refuses.
    ///
    /// # Errors
    ///
    /// See [`Self::new`].
    pub fn with_credential(
        core: ExecutionClientCore,
        config: OndoExecutionClientConfig,
        credential: Option<OndoCredential>,
        budget: Option<crate::http::rate_limit::OndoRateBudget>,
    ) -> anyhow::Result<Self> {
        config.validate()?;

        // The authorization scope is the one input the endpoint gates, the credential resolver and
        // the authenticated transport share. A production configuration without `account_read_only`
        // was refused by `validate`, so this cannot name a production write scope.
        let scope = config.authentication_scope()?;

        // The endpoint gate runs on the configuration's own scope and base URL, before the
        // credential is resolved and before a client exists. The transport applies the same policy
        // again to the credential it is handed, so a refused endpoint is refused at both layers.
        let base_url = config.http_base_url().to_string();
        validate_authenticated_environment(scope, &base_url)?;

        // The private WebSocket is judged by the same policy, for its own scheme family, and also
        // before the credential is read: a session that may not sign for its REST endpoint may not
        // sign a login frame for an arbitrary socket either (plan §R3.1, §R0.3).
        let ws_url = config.ws_url().to_string();
        validate_authenticated_websocket_environment(scope, &ws_url)?;

        let credential = match credential {
            Some(credential) => credential,
            None if config.has_explicit_credentials() => OndoCredential::new(
                config.environment,
                config.api_key.clone().unwrap_or_default(),
                config.api_secret.clone().unwrap_or_default(),
            )
            .map_err(|error| anyhow::anyhow!("the configured credential is unusable: {error}"))?,
            None => crate::common::credential::resolve_credential(scope, &base_url)
                .map_err(|error| anyhow::anyhow!("no Ondo Perps credential: {error}"))?,
        };
        // One handle, two surfaces: the REST transport signs with this, and so does the private
        // login. Neither takes a copy of the secret, and nothing outside this crate can reach one.
        //
        // The credential and the scope are one decision: a sandbox key is never sent under a
        // production scope, or the other way round, whatever a caller passed in.
        if credential.environment() != scope.environment() {
            anyhow::bail!(
                "the Ondo Perps credential's environment does not match the configured \
                 authorization scope"
            );
        }

        let credential = Arc::new(credential);

        let account_id = core.account_id;
        let clock = get_atomic_clock_realtime();

        // The account's state machine is built before the transport, because the transport is built
        // with the guard that reads it: a signed write has to re-check the account at the send
        // point, and a client that cannot do that is a client this adapter does not order from.
        let reconciliation = Arc::new(RwLock::new(ReconciliationMachine::new(
            account_id,
            config.dms_timeout_secs,
        )));

        // The renewal bound travels from the configuration into the switch itself, capped there:
        // the switch is the thing that fails closed, so the bound it fails at is the switch's own
        // and not a number the transport has to remember (plan §R3.3).
        reconciliation
            .write()
            .dead_mans_switch_mut()
            .set_max_failed_renewals(config.capped_dms_max_failed_renewals());

        // A read-only session is a property of the client rather than a promise its caller keeps:
        // it is marked before anything can be sent, and there is no way back from it (plan §0).
        if config.account_read_only {
            reconciliation.write().mark_account_read_only();
        }

        let production = config
            .execution_envelope
            .clone()
            .map(|envelope| {
                crate::production::ProductionAuthority::new(
                    envelope,
                    config.diagnostics_run_id.clone().unwrap_or_default(),
                    config
                        .expected_venue_account_id
                        .as_deref()
                        .unwrap_or_default(),
                    config.journal_path.as_deref().unwrap_or_default(),
                    config.dms_timeout_secs,
                )
                .map_err(anyhow::Error::msg)
            })
            .transpose()?;
        let http_client = OndoHttpClient::builder()
            .base_url(base_url)
            .timeout_secs(config.http_timeout_secs)
            .maybe_budget(budget)
            .credential(Arc::clone(&credential))
            .authentication_scope(scope)
            .maybe_production_guard(production.clone())
            .new_risk_guard(Arc::new(RunAdmission::new(Arc::clone(&reconciliation)))
                as Arc<dyn OndoNewRiskGuard>)
            .build()
            .map_err(|error| {
                anyhow::anyhow!("Failed to build the Ondo Perps HTTP client: {error}")
            })?;

        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            account_id,
            core.account_type,
            core.base_currency,
        );

        let reporter = OndoReporter {
            account_id,
            clock,
            emitter,
            state: Arc::new(RwLock::new(OndoPrivateState::default())),
        };

        let pass = Arc::new(PassOwnership::default());
        let account = OndoAccountRuntime {
            reconciliation: Arc::clone(&reconciliation),
            buffer: Arc::new(RwLock::new(ReconciliationBuffer::new())),
            pass: Arc::clone(&pass),
            reporter: reporter.clone(),
            http_client: http_client.clone(),
            clock,
            reconcile_interval_secs: config.reconcile_interval_secs,
            dms_timeout_secs: config.dms_timeout_secs,
            journal: config
                .journal_path
                .as_deref()
                .filter(|path| !path.trim().is_empty())
                .map(|path| JournalHandle::new(std::path::PathBuf::from(path))),
            account_state_published: Arc::new(AtomicU64::new(0)),
            account_identity: Arc::new(RwLock::new(OndoAccountIdentity::Unknown)),
            production: production.clone(),
        };

        // The journal is restored here - before the client exists, and therefore before anything
        // can be admitted. A run whose journal cannot be read is a run whose ledger is missing, and
        // the machine refuses new risk from that moment until a human has looked at the file
        // (plan §R3.2).
        account.restore_journal(clock.get_time_ns());
        if let Some(guard) = &production {
            let reporter = reporter.clone();
            let machine = Arc::clone(&reconciliation);
            let journal = account.journal.clone();
            let identity = Arc::clone(&account.account_identity);
            guard
                .bind(Arc::new(move |checkpoint| {
                    let state = reporter.state.read();
                    let machine = machine.read();
                    let now = reporter.now();
                    let journal = journal.as_ref().ok_or("production journal missing")?;
                    if checkpoint {
                        journal.write(reporter.account_id, now, &state, &machine);
                    }
                    Ok(crate::production::ProductionEvidence {
                        ready: matches!(machine.admission(now), Admission::Granted { .. })
                            && machine.reading_is_fresh(now),
                        identity_matched: *identity.read() == OndoAccountIdentity::Matched,
                        journal_healthy: journal.write_failures() == 0
                            && journal.last_written_at().is_some()
                            && !matches!(machine.journal(), JournalStatus::Failed { .. }),
                        dms_verified: matches!(
                            machine.dead_mans_switch().state_at(now),
                            DeadMansSwitchState::Armed
                        ),
                        available_margin_usdc: machine
                            .verified_balance()
                            .and_then(|b| b.reading().available_margin),
                        orders: state
                            .orders
                            .iter()
                            .map(|(id, o)| {
                                (
                                    id.to_string(),
                                    (
                                        o.filled.as_decimal(),
                                        o.is_settled(),
                                        o.venue_order_id.map(|v| v.to_string()),
                                    ),
                                )
                            })
                            .collect(),
                        unknown: machine.unknown_submissions().len(),
                    })
                }))
                .map_err(anyhow::Error::msg)?;
        }

        let read_only_diagnostics = OndoReadOnlyDiagnostics::new(
            config.diagnostics_run_id.clone().unwrap_or_default(),
            account.clone(),
        );

        Ok(Self {
            core,
            config,
            http_client,
            reporter,
            reconciliation,
            account,
            read_only_diagnostics,
            credential,
            private_stream: None,
            tasks: TaskGroup::new(),
            last_shutdown: None,
        })
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &OndoExecutionClientConfig {
        &self.config
    }

    /// Returns a clone of the signed HTTP client.
    #[must_use]
    pub fn http_client(&self) -> OndoHttpClient {
        self.http_client.clone()
    }

    /// Returns the account half, shared with the private transport.
    ///
    /// This is the handle a transport drives the account through, and the one a caller outside this
    /// crate uses to reach the account without the client's own borrow.
    #[must_use]
    pub fn account(&self) -> OndoAccountRuntime {
        self.account.clone()
    }

    /// Returns a clone of the sanitized read-only diagnostics handle.
    ///
    /// The factory retains one of these so an application can read a bounded snapshot after a run
    /// without reaching into the native client through the node. It carries no frame, credential,
    /// account id or monetary field.
    #[must_use]
    pub fn read_only_diagnostics(&self) -> OndoReadOnlyDiagnostics {
        self.read_only_diagnostics.clone()
    }

    pub(crate) fn production_authority(
        &self,
    ) -> Option<Arc<crate::production::ProductionAuthority>> {
        self.account.production.clone()
    }
    /// Returns this client's completed native production reconciliation evidence.
    #[must_use]
    pub fn production_trade_snapshot(&self) -> Option<serde_json::Value> {
        self.account
            .production
            .as_ref()
            .and_then(|guard| guard.snapshot())
    }

    /// Returns whether this client is an account read-only session.
    #[must_use]
    pub fn is_account_read_only(&self) -> bool {
        self.account.is_account_read_only()
    }

    /// Returns the private session's run state and the detail that explains it.
    ///
    /// A client whose transport has not been started reports
    /// [`crate::websocket::private::PrivateRunState::Disconnected`]: no account session exists,
    /// which is exactly what that state says.
    #[must_use]
    pub fn private_run_state(&self) -> PrivateRunSnapshot {
        match &self.private_stream {
            Some(stream) => stream.run_state(),
            None => PrivateRunSnapshot::new(
                crate::websocket::private::PrivateRunState::Disconnected,
                "no private transport has been started",
            ),
        }
    }

    /// Returns the private session's diagnostic record, once a transport has been started.
    #[must_use]
    pub fn private_diagnostics(&self) -> Option<SharedPrivateDiagnostics> {
        self.private_stream
            .as_ref()
            .map(OndoPrivateStream::diagnostics)
    }

    /// Returns whether the private transport's task is still alive.
    ///
    /// `false` for a client whose transport has not been started, and `false` once it has ended -
    /// which is what makes a bounded shutdown checkable rather than asserted.
    #[must_use]
    pub fn private_stream_is_running(&self) -> bool {
        self.private_stream
            .as_ref()
            .is_some_and(OndoPrivateStream::is_running)
    }

    /// Returns the number of orders this client is tracking.
    #[must_use]
    pub fn tracked_order_count(&self) -> usize {
        self.reporter.state.read().orders.len()
    }

    /// Returns how many fills the dedup ledger has applied.
    #[must_use]
    pub fn applied_fill_count(&self) -> usize {
        self.reporter.state.read().fills.len()
    }

    /// Returns whether the order index holds an order for `client_order_id`.
    #[must_use]
    pub fn tracks(&self, client_order_id: &ClientOrderId) -> bool {
        self.reporter
            .state
            .read()
            .orders
            .contains_key(client_order_id)
    }

    /// Returns the state of one tracked order.
    #[must_use]
    pub fn order_state(&self, client_order_id: &ClientOrderId) -> Option<OndoOrderState> {
        self.reporter
            .state
            .read()
            .orders
            .get(client_order_id)
            .cloned()
    }

    /// Returns the orders whose state this adapter cannot account for.
    ///
    /// An unresolved order is one the venue answered about without this adapter being able to
    /// confirm it ([`OndoOrderState::is_unresolved`]): an unknown status, an `untriggered`
    /// conditional, a fill this adapter can never report, a terminal status whose fills do not add
    /// up, or a terminal status whose filled quantity cannot be read. An order that is merely still
    /// working is not one of these.
    ///
    /// **This is a local view, not the account gate.** It reads the orders this client *tracks*,
    /// and nothing in the production path calls it: what keeps an account from being judged clean is
    /// the order judgment, which runs over every order a pass read and reports
    /// [`crate::reconciliation::Finding::UnresolvedOrder`] for the same condition
    /// (`ReconciliationMachine::judge_orders`, plan §6.3, §6.4). The two agree about a tracked order
    /// because both are read off [`OndoOrderState::is_accounted_for`]; the judgment additionally
    /// covers orders this client does not track, and reports those as a foreign order. This accessor
    /// exists for a caller that wants the local list - a non-empty result here is not by itself what
    /// stops anything.
    #[must_use]
    pub fn unresolved_orders(&self) -> Vec<ClientOrderId> {
        self.reporter
            .state
            .read()
            .orders
            .iter()
            .filter(|(_id, state)| state.is_unresolved())
            .map(|(id, _state)| *id)
            .collect()
    }

    // --------------------------------------------------------------------------------------------
    // The account: recovery, judgments, the switch and the stop sequence (plan §6.4)
    // --------------------------------------------------------------------------------------------

    /// Returns the account's reconciliation state.
    #[must_use]
    pub fn reconciliation_state(&self) -> ReconciliationState {
        self.reconciliation.read().state()
    }

    /// Returns whether a new order may be submitted (plan Task 8).
    ///
    /// Fail-closed, and the whole reason the recovery exists: `true` only for a recovered account
    /// with usable metadata, a switch that permits orders and nothing left unsettled. Cancels and
    /// queries are not governed by this - they travel while the account is being recovered (§4.4's
    /// priority class).
    #[must_use]
    pub fn can_submit_new_orders(&self) -> bool {
        let now = self.account.now();

        self.reconciliation.read().can_submit_new_orders(now)
    }

    /// Returns whether this client must refuse a new order right now.
    ///
    /// Holds from construction: a client that has established nothing has verified nothing, so
    /// there is no account state a new order could be placed against.
    #[must_use]
    pub fn refuses_new_risk(&self) -> bool {
        let now = self.account.now();

        self.reconciliation.read().refuses_new_risk(now)
    }

    /// Returns why a new order would be refused, or [`None`] when one may be submitted.
    ///
    /// The instant is this runtime's own, read here rather than taken from the caller, so the
    /// question "may I place an order" is answered *now* by construction. A deadline is a condition
    /// at a time, and the one that governs is the time the question is asked at.
    #[must_use]
    pub fn new_risk_refusal(&self) -> Option<NewRiskRefusal> {
        let now = self.account.now();

        self.reconciliation.read().new_risk_refusal(now)
    }

    /// The one admission decision, taken under the lock that reads it.
    fn admission(&self) -> Admission {
        let now = self.account.now();

        self.reconciliation.read().admission(now)
    }

    /// Denies one command the account does not admit, by name and with its reason.
    fn deny_submission(&self, order: &OrderAny, reason: &NewRiskRefusal) {
        log::error!(
            "Ondo refused to submit {}: {}",
            order.client_order_id(),
            reason.reason(),
        );
        self.reporter
            .emitter
            .emit_order_denied(order, &new_risk_refusal_reason(reason));
    }

    /// Begins a recovery: the account is about to be read, and new risk stops until it converges.
    ///
    /// The private stream's connect and reconnect hooks call this, and so does the sandbox probe's
    /// account mode. It is the client's own `connect` that does **not** call it: a recovery is a
    /// decision to read the account, not a socket coming up.
    pub fn begin_recovery(&self, now: UnixNanos) {
        self.account.begin_recovery(now);
    }

    /// Returns how many consecutive agreeing passes the last recovery has seen.
    #[must_use]
    pub fn confirmations(&self) -> usize {
        self.reconciliation.read().confirmations()
    }

    /// Returns the last pass's reading of the account.
    #[must_use]
    pub fn last_reading(&self) -> Option<AccountReading> {
        self.reconciliation.read().last_reading().cloned()
    }

    /// Returns the last pass's judgment.
    #[must_use]
    pub fn last_judgment(&self) -> Option<AccountJudgment> {
        self.reconciliation.read().last_judgment().cloned()
    }

    /// Returns the submissions whose outcome is unknown.
    #[must_use]
    pub fn unknown_submissions(&self) -> Vec<UncertainOutcome> {
        self.reconciliation.read().unknown_submissions()
    }

    /// Returns the cancels no venue answer has settled.
    #[must_use]
    pub fn unconfirmed_cancels(&self) -> Vec<UncertainOutcome> {
        self.reconciliation.read().unconfirmed_cancels()
    }

    /// Returns the record one unsettled write is held under, whichever map holds it.
    #[must_use]
    pub fn uncertain_outcome(&self, client_order_id: &ClientOrderId) -> Option<UncertainOutcome> {
        self.reconciliation
            .read()
            .uncertain_outcome(client_order_id)
            .cloned()
    }

    /// Records whether the metadata this client trades on is usable (plan §4.1).
    pub fn set_metadata(&self, validity: MetadataValidity) {
        self.account.set_metadata(validity);
    }

    /// Arms the dead man's switch, returning the frame the private stream must send.
    ///
    /// New orders wait for the venue's confirmation: an unconfirmed arm is not an arm (§6.4).
    pub fn arm_dead_mans_switch(&self, now: UnixNanos) -> DeadMansSwitchMessage {
        self.account.arm_dead_mans_switch(now)
    }

    /// Applies the venue's confirmation of the switch.
    pub fn confirm_dead_mans_switch(&self, now: UnixNanos) {
        self.account.confirm_dead_mans_switch(now);
    }

    /// Builds the frame that renews an armed switch, or [`None`] when there is nothing to renew.
    ///
    /// Composing moves nothing: the deadline moves in
    /// [`Self::note_dead_mans_switch_renewed`], after the bytes were written (plan §R3.3).
    #[must_use]
    pub fn dead_mans_switch_renewal_frame(&self) -> Option<DeadMansSwitchMessage> {
        self.account.dead_mans_switch_renewal_frame()
    }

    /// Records that a renewal frame reached the socket at `now`, moving the switch's deadline.
    ///
    /// It also clears the run of consecutive renewal failures, and it says nothing more than that
    /// the bytes were written: no acknowledgement exists for a renewal, so this is a statement about
    /// this process and not about the venue (see [`DeadMansSwitch::note_renew_sent`]).
    pub fn note_dead_mans_switch_renewed(&self, now: UnixNanos) {
        self.account.note_dead_mans_switch_renewed(now);
    }

    /// Records that a renewal frame could not be written, failing the switch at the bound.
    ///
    /// The bound is this client's own - the configured limit, capped at the crate's ceiling - and
    /// reaching it fails the switch closed rather than leaving an unrenewed switch reading `Armed`
    /// (plan §R3.3).
    pub fn note_dead_mans_switch_renew_failed(&self, reason: String) {
        self.account.note_dead_mans_switch_renew_failed(reason);
    }

    /// Records that the switch failed, which stops new orders.
    pub fn note_dead_mans_switch_failed(&self, reason: String) {
        self.account.note_dead_mans_switch_failed(reason);
    }

    /// Records that the switch fired: the venue cancelled this account's resting orders.
    ///
    /// The account becomes uncertain, because what the cancellation left behind has to be read, and
    /// no position is closed by a switch (plan §6.4).
    pub fn note_dead_mans_switch_fired(&self, now: UnixNanos) {
        self.account.note_dead_mans_switch_fired(now);
    }

    /// Returns the steps a stop takes, in the order it takes them (plan §6.4).
    ///
    /// The switch is released only after this run's own orders are cancelled and confirmed: a
    /// released switch cancels nothing, and the orders it was covering would be left resting.
    ///
    /// This **describes** the sequence. [`Self::stop_and_wait`] is what carries it out, and the two
    /// are deliberately not two implementations of one rule: the executor asks this for the steps
    /// rather than restating them, so a step added here is a step taken there.
    #[must_use]
    pub fn stop_sequence(&self) -> Vec<StopStep> {
        let mut steps = vec![StopStep::CancelOwnOrders, StopStep::ConfirmOwnOrders];

        if self.reconciliation.read().dead_mans_switch().is_required() {
            steps.push(StopStep::ReleaseDeadMansSwitch);
        }

        steps.push(StopStep::ClosePrivateStream);

        steps
    }

    /// Returns the orders this run placed and is still holding.
    ///
    /// It is the **ownership** list the stop cancels against: every one of these was submitted under
    /// this client's own order id by this process, so cancelling it cannot touch another session's
    /// orders. Nothing here is derived from a market-wide read, which is what keeps the stop off
    /// `DELETE /v1/perps/orders?market=...` - an account-wide cancellation that would take out work
    /// this client never placed (plan §R3.3).
    #[must_use]
    pub fn tracked_orders(&self) -> Vec<ClientOrderId> {
        self.reporter.state.read().orders.keys().copied().collect()
    }

    /// Executes the stop sequence and reports what it left behind (plan §6.4, §R3.3).
    ///
    /// # The order, and why it is the order
    ///
    /// 1. **New risk stops first.** The account's session is ended before anything is cancelled, so
    ///    an order that arrives while the stop is running meets a refusal rather than a venue.
    /// 2. **This run's own orders are cancelled, one by one, by client order id.** The path is the
    ///    ordinary single-order cancel - the same code a strategy's `CancelOrder` travels - and
    ///    [`StopStep::CancelOwnOrders`] is deliberately not a market-wide delete: this client's
    ///    ownership is a list of its own order ids, not a market, and a market cancel would take out
    ///    orders it never placed.
    /// 3. **Their outcome is confirmed by a read**, not by the cancel call. A cancel API having been
    ///    called is not an order having been cancelled (plan §6.3), and the single-order path does
    ///    this itself: a venue answer that does not state the order is followed by a query.
    /// 4. **The switch is released only when nothing is unconfirmed.** An order whose end this
    ///    client cannot state is an order the switch is still covering; releasing it there would
    ///    trade a protection that is doing something for the tidier exit.
    /// 5. **The private stream is closed**, and the transport's own bounded stop is awaited.
    ///
    /// # What it refuses to pretend
    ///
    /// It does not report success it did not see. The three endings are kept apart in
    /// [`StopOutcome`]: everything settled, the wait for the settles ran out, or the transport
    /// outlived its own stop timeout. Whatever is left over - the orders still unconfirmed, the ones
    /// a timeout left in flight - is listed in the report **and checkpointed into the journal**, so
    /// a restart inherits the outstanding writes instead of beginning from an empty account. A run
    /// that restored such a journal does not become [`crate::reconciliation::ReconciliationState::Ready`]
    /// on a clean read: the restored entries are what the admission refuses on, exactly as they were
    /// before the restart.
    ///
    /// # Errors
    ///
    /// Nothing here is fallible. A stop that cannot do something says so in its report rather than
    /// returning an error: the caller is on its way out, and a stop that returned `Err` early would
    /// leave the socket up and the switch armed without saying which.
    pub async fn stop_and_wait(&mut self, now: UnixNanos, settle_timeout: Duration) -> StopReport {
        let budget = ShutdownBudget::new(settle_timeout);

        self.stop_and_wait_within(now, &budget).await
    }

    /// Executes the ordered stop against one shared shutdown budget.
    ///
    /// The budget covers the cancel requests, the confirmations, the switch release and the
    /// transport close, so no single slow request can spend the whole bound and leave later owned
    /// orders unregistered. Every owned order is registered and checkpointed **before** the first
    /// request, so a stop the caller cuts short still names everything it owes.
    async fn stop_and_wait_within(
        &mut self,
        now: UnixNanos,
        budget: &ShutdownBudget,
    ) -> StopReport {
        // Whatever happens from here - including the caller dropping this future mid-wait - the
        // ledger is checkpointed on the way out.
        let _checkpoint = JournalCheckpoint(self.account.clone());

        let read_only = self.account.is_account_read_only();
        let mut steps = self.stop_sequence();

        // A read-only session never cancels, arms or releases anything. Its stop is to report the
        // inherited state and close the socket, so the two cancel steps are not part of it.
        if read_only {
            steps.retain(|step| {
                !matches!(step, StopStep::CancelOwnOrders | StopStep::ConfirmOwnOrders)
            });
        }

        let mut report = StopReport {
            steps,
            outcome: StopOutcome::Complete,
            cancellations_issued: 0,
            unresolved_orders: Vec::new(),
            unconfirmed_cancels: Vec::new(),
            unknown_submissions: Vec::new(),
            released_switch: false,
            stream_ended: false,
        };

        // Step 1: new risk stops. Ending the session is what makes the admission refuse, and it
        // happens before a single cancel travels so nothing can slip in behind the stop.
        self.account.note_session_ended(now);

        if !read_only {
            // Step 2a: every owned working order is registered and checkpointed before any request
            // exists, so blocking the first cancel cannot erase the second order from the record.
            self.register_owned_cancels(now);
            self.account.persist_journal(self.account.now());

            // Step 2b: this run's own working orders, by id, on the ordinary cancel path, each
            // bounded by what is left of the shared budget.
            for client_order_id in self.working_orders() {
                let venue_order_id = self
                    .order_state(&client_order_id)
                    .and_then(|state| state.venue_order_id);

                if budget.remaining().is_zero() {
                    log::error!(
                        "Ondo's stop budget expired before it could cancel {client_order_id}; it is \
                         registered as unconfirmed"
                    );

                    continue;
                }

                let attempt = self
                    .account
                    .cancel_order(client_order_id, venue_order_id, now);

                match tokio::time::timeout(budget.remaining(), attempt).await {
                    Ok(Ok(())) => report.cancellations_issued += 1,
                    Ok(Err(error)) => {
                        log::error!(
                            "Ondo could not cancel {client_order_id} while stopping: {error}; it is \
                             registered as unconfirmed"
                        );
                        self.account.note_unconfirmed_cancel(
                            client_order_id,
                            venue_order_id,
                            format!("the stop could not issue the cancel: {error}"),
                            now,
                        );
                    }
                    Err(_) => {
                        log::error!(
                            "Ondo's stop budget expired while cancelling {client_order_id}; it is \
                             registered as unconfirmed"
                        );
                        self.account.note_unconfirmed_cancel(
                            client_order_id,
                            venue_order_id,
                            "the stop budget expired while the cancel was in flight".to_string(),
                            now,
                        );
                    }
                }
            }
        }

        // Step 3: wait, within the shared budget, for what was sent to settle. An order is settled
        // only when it is terminal, its fills agree with the venue's own total and it has no fills
        // still waiting to be reported.
        let settled = self.await_orders_settled(budget, &report.steps).await;

        let (unconfirmed_cancels, unknown_submissions) = self.unsettled_writes();

        report.unconfirmed_cancels = unconfirmed_cancels;
        report.unknown_submissions = unknown_submissions;
        report.unresolved_orders = self.unresolved_orders();
        report.outcome = if settled {
            StopOutcome::Complete
        } else {
            StopOutcome::TimedOut
        };

        // Step 4: release the switch only when every piece of owned work is settled, every cancel
        // and submission is confirmed, and the wait did not time out. A successful query is not a
        // confirmed cancel, and a terminal order with missing fills is not a settled one.
        let release_owed = report.steps.contains(&StopStep::ReleaseDeadMansSwitch);
        let release_allowed = release_owed
            && !read_only
            && report.outcome == StopOutcome::Complete
            && report.unconfirmed_cancels.is_empty()
            && report.unknown_submissions.is_empty()
            && report.unresolved_orders.is_empty();

        if release_allowed {
            if self.account.production.is_some() {
                let _ = tokio::time::timeout(
                    budget.remaining(),
                    self.account.reconcile_account(self.account.now()),
                )
                .await;
            }
            // Composing is not releasing: the frame is built without moving the switch, and the
            // switch is released locally only once the write succeeded.
            let frame = self
                .reconciliation
                .read()
                .dead_mans_switch()
                .release_frame();

            match self.private_stream.as_ref() {
                Some(stream) => {
                    let allowance = budget.remaining().max(Duration::from_millis(500));

                    match tokio::time::timeout(allowance, stream.send_switch_frame(&frame)).await {
                        Ok(Ok(())) => {
                            self.reconciliation
                                .write()
                                .dead_mans_switch_mut()
                                .release(self.account.now());
                            report.released_switch = true;
                        }
                        Ok(Err(error)) => log::error!(
                            "Ondo could not put the switch release on the wire while stopping: \
                             {error}; the switch stays armed and the venue's own timeout is what \
                             will fire it"
                        ),
                        Err(_) => {
                            if let Some(guard) = &self.account.production {
                                guard.note_release_shutdown_timeout();
                            }
                            log::error!(
                                "Ondo's switch release timed out; the switch stays armed and the \
                                 venue's own timeout is what will fire it"
                            );
                        }
                    }
                }
                None => {
                    if let Some(guard) = &self.account.production {
                        guard.note_release_no_connection();
                    }
                    log::error!(
                        "Ondo has no live transport to release the switch on, so the switch stays \
                         armed rather than claiming a release this process cannot put on the wire"
                    );
                }
            }
        } else if release_owed {
            if let Some(guard) = &self.account.production {
                guard.note_release_blocked_unsettled();
            }
            log::error!(
                "Ondo is leaving the dead man's switch armed: outcome={:?}, {} cancel(s), {} \
                 submission(s) and {} unresolved order(s) still stand",
                report.outcome,
                report.unconfirmed_cancels.len(),
                report.unknown_submissions.len(),
                report.unresolved_orders.len(),
            );
        }

        // Step 5: close the private stream and wait for the transport to end. This always runs,
        // even when the budget is spent: a socket still being read is never left behind.
        match self.private_stream.as_mut() {
            Some(stream) => {
                stream.stop().await;
                report.stream_ended = !stream.is_running();

                if !report.stream_ended {
                    report.outcome = StopOutcome::StreamOutlivedStop;
                }
            }
            None => report.stream_ended = true,
        }

        if report.stream_ended {
            self.private_stream = None;
        }

        // The stop's own record: whatever is outstanding is written where a restart reads it, and
        // it is written **after** the cancels so the checkpoint holds what the stop left, not what
        // it started from.
        self.account.persist_journal(self.account.now());

        report
    }

    /// Returns the writes this account has sent whose answers it never saw.
    fn unsettled_writes(&self) -> (Vec<ClientOrderId>, Vec<ClientOrderId>) {
        let machine = self.reconciliation.read();

        (
            machine
                .unconfirmed_cancels()
                .into_iter()
                .map(|outcome| outcome.client_order_id)
                .collect(),
            machine
                .unknown_submissions()
                .into_iter()
                .map(|outcome| outcome.client_order_id)
                .collect(),
        )
    }

    /// Waits, within the shared budget, for every order the stop touched to settle.
    ///
    /// It returns whether the wait finished rather than whether the orders did: an order still
    /// unsettled at the deadline is [`StopOutcome::TimedOut`]'s business and is reported as such.
    async fn await_orders_settled(&self, budget: &ShutdownBudget, steps: &[StopStep]) -> bool {
        if !steps.contains(&StopStep::ConfirmOwnOrders) {
            return true;
        }

        let mut pending = self.pending_orders();

        while !pending.is_empty() {
            let remaining = budget.remaining();

            if remaining.is_zero() {
                log::error!(
                    "Ondo's stop timed out with {} order(s) not settled: {pending:?}",
                    pending.len(),
                );

                return false;
            }

            tokio::time::sleep(remaining.min(Duration::from_millis(ONDO_STOP_SETTLE_POLL_MS)))
                .await;
            pending = self.pending_orders();
        }

        true
    }

    /// Returns the tracked orders this client cannot yet call settled.
    ///
    /// A terminal status is not enough: the venue's fill total has to agree with the fills this
    /// client applied, and no accepted fill may still be waiting to be reported
    /// ([`OndoOrderState::is_settled`]). That keeps a terminal order with missing fills from
    /// reading as finished and releasing the protection over it.
    fn pending_orders(&self) -> Vec<ClientOrderId> {
        self.reporter
            .state
            .read()
            .orders
            .values()
            .filter(|state| !state.is_settled())
            .map(|state| state.client_order_id)
            .collect()
    }

    /// Returns the tracked orders whose end this run has not yet stated to the venue.
    fn working_orders(&self) -> Vec<ClientOrderId> {
        self.reporter
            .state
            .read()
            .orders
            .values()
            .filter(|state| !state.status.is_terminal())
            .map(|state| state.client_order_id)
            .collect()
    }

    /// Registers every non-terminal owned order as an unconfirmed cancel, without sending anything.
    ///
    /// It runs before the first cancel request so the stop's checkpoint already names every order
    /// it owes, whatever the caller's timeout does to the requests that follow. It is idempotent
    /// and it is never used by a read-only session.
    fn register_owned_cancels(&self, now: UnixNanos) {
        for state in self.reporter.state.read().orders.values() {
            if !state.status.is_terminal() {
                self.account.note_unconfirmed_cancel(
                    state.client_order_id,
                    state.venue_order_id,
                    "the stop registered this order before issuing its cancel".to_string(),
                    now,
                );
            }
        }
    }

    /// Records a private report that arrived while the account was being read.
    ///
    /// The report is held rather than applied so the pass can replay it through the same state
    /// machine the REST pages go through, where it is deduped (plan §6.4). It is attributed to the
    /// recovery the account is in at this instant: a report recorded before this recovery began, or
    /// after the session it belonged to ended, is refused when the pass drains rather than applied
    /// as current state.
    ///
    /// A report the bounded buffer refuses for want of room is **not** dropped quietly: the account
    /// becomes uncertain until a pass has read it whole again (plan §6.4).
    pub fn buffer_stream_order(&self, payload: OndoApiOrder) {
        self.account.buffer_stream_order(payload);
    }

    /// Records a private fill that arrived while the account was being read.
    ///
    /// Buffered, attributed and bounded exactly as [`Self::buffer_stream_order`] buffers an order
    /// report.
    pub fn buffer_stream_fill(&self, fill: OndoApiFill) {
        self.account.buffer_stream_fill(fill);
    }

    /// Reads the account once and concludes a pass (plan §6.4).
    ///
    /// The reads are the venue's own lists, walked with the same bounded [`CursorWalk`] every other
    /// history read uses, and every payload is applied through [`Self::apply_order`] and
    /// [`Self::apply_fill`] - the one state machine, so the stream and this pass can never disagree
    /// about what a payload means. The reports the stream buffered while the pass read are replayed
    /// into it, after the last read and under the boundary that ends the pass, where the ledger and
    /// the order index dedupe them.
    ///
    /// One pass owns the account at a time: two would both drain the buffer - the second finding it
    /// empty - and each would conclude a reading the other half-wrote.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryPassRefusal::AlreadyRunning`] when another pass owns the account, without
    /// reading anything and without judging this one; and an error when the account cannot be read
    /// completely - a failed request, an unreadable page, a cursor that will not advance. The
    /// machine is left [`ReconciliationState::Uncertain`] in that case, because a pass that stopped
    /// early must not look like an account that was read.
    pub async fn reconcile_account(&self, now: UnixNanos) -> anyhow::Result<AccountJudgment> {
        self.account.reconcile_account(now).await
    }

    /// Probes every unsettled write a probe is due for, under the reference that identifies it.
    ///
    /// A submission is asked about under the client order id it was made with, a cancel under the
    /// venue order id when this session has one - a probe never invents a new client order id,
    /// because that would be a second order. A probe that finds the order applies it and settles
    /// the outcome; a 404 settles nothing (plan §6.3), and an inconclusive probe settles nothing
    /// either.
    #[must_use]
    pub async fn probe_unknown_submissions(&self, now: UnixNanos) -> Vec<ProbeReport> {
        self.account.probe_unknown_submissions(now).await
    }

    /// Expires the unknown submissions whose window has passed (plan §6.3).
    ///
    /// Probing stops, the submissions stay unknown, and what comes back is exactly what a human
    /// reconciling the account by hand needs: the client order ids and how long they have been
    /// unknown.
    #[must_use]
    pub fn abandon_unknown_submissions(
        &self,
        now: UnixNanos,
    ) -> Vec<crate::reconciliation::AbandonedSubmission> {
        self.account.abandon_unknown_submissions(now)
    }

    /// Applies one `ApiOrder` payload: the create answer, a lookup answer, a cancel answer or a
    /// reconciliation read.
    ///
    /// This is the ingestion seam the private stream and Task 8's reconciliation will drive. It is
    /// idempotent: a payload that repeats state already applied and reported returns
    /// [`OndoOrderApplication::Unchanged`] and emits nothing, so an acknowledgement cannot be
    /// reported twice. A payload that acknowledges the order flushes the fills that arrived before
    /// it, oldest first, so a fill that preceded the ACK is still reported after it.
    pub fn apply_order(&self, payload: &OndoApiOrder) -> OndoOrderApplication {
        self.account.apply_order(payload)
    }

    /// Applies one `ApiFill` payload: the one path a fill is ever counted from.
    ///
    /// # Errors
    ///
    /// See [`OndoReporter::apply_fill`].
    pub fn apply_fill(&self, fill: &OndoApiFill) -> anyhow::Result<OndoFillApplication> {
        self.account.apply_fill(fill)
    }

    /// Applies a whole page of fills, returning what each one did.
    ///
    /// # Errors
    ///
    /// Returns the first fill's error; the fills before it have already been applied, which is
    /// what the dedup ledger makes safe to retry.
    pub fn apply_fills(&self, fills: &[OndoApiFill]) -> anyhow::Result<Vec<OndoFillApplication>> {
        self.account.apply_fills(fills)
    }

    /// Returns a spawner for this client's task generation, or [`None`] when it is shut down.
    fn spawner(&self) -> Option<TaskSpawner> {
        match self.tasks.spawner() {
            Ok(spawner) => Some(spawner),
            Err(error) => {
                log::error!("The Ondo Perps execution client cannot spawn a task: {error}");
                None
            }
        }
    }

    /// Returns the order reference to use for a cancel or a query.
    ///
    /// A venue order id the caller already holds wins; otherwise the index is asked, and the
    /// `client:{clientOrderId}` form is the fallback for an order whose venue id this session has
    /// not observed yet (plan §6.2, §6.3).
    fn order_ref(
        &self,
        client_order_id: &ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
    ) -> String {
        match venue_order_id {
            Some(venue_order_id) => venue_order_id.to_string(),
            None => self.reporter.order_ref(client_order_id),
        }
    }

    /// Compares the authenticated account with the configured venue account id.
    ///
    /// With no expected id the identity is [`OndoAccountIdentity::Unknown`] and no account read is
    /// made for it. With one, `GET /v1/account` is read once and its `accountID` compared. A read
    /// that fails leaves the identity unknown: this adapter does not fabricate a match from an
    /// absent answer, and it does not refuse a read-only session for the venue's silence.
    ///
    /// # Errors
    ///
    /// Returns an error, naming no identifier, when the read answer identifies a different
    /// account.
    async fn verify_account_identity(&self) -> anyhow::Result<()> {
        let Some(expected) = self.config.expected_venue_account_id.as_deref() else {
            self.account
                .set_account_identity(OndoAccountIdentity::Unknown);

            return Ok(());
        };

        let identity = match self.read_authenticated_account().await {
            Ok(response) => match response.venue_account_id() {
                Some(account_id) if account_id == expected => OndoAccountIdentity::Matched,
                Some(_) => OndoAccountIdentity::Mismatch,
                None => OndoAccountIdentity::Unknown,
            },
            Err(error) => {
                log::warn!(
                    "Ondo could not read the authenticated account for identity verification: \
                     {error}; the identity stays unknown",
                );

                OndoAccountIdentity::Unknown
            }
        };

        self.account.set_account_identity(identity);

        if identity == OndoAccountIdentity::Mismatch {
            anyhow::bail!(
                "the authenticated Ondo Perps account does not match the configured venue account \
                 id; refusing to connect"
            );
        }

        Ok(())
    }

    /// Reads the authenticated account, retrying once when the venue rejects the signature for a
    /// clock this client has not learned yet.
    ///
    /// The **first** signed request is the one that teaches this client the venue's clock: the
    /// answer carries the `Date` header the offset is read from, and that is true of the rejection
    /// itself. A host whose clock runs ahead of the venue's forward tolerance therefore has its
    /// first signed request rejected - and every later *first* request would be rejected the same
    /// way, because nothing else observes a `Date` before the first signature.
    ///
    /// The retry signs with the offset the rejection just taught. A read is idempotent, so exactly
    /// one retry is allowed, and only for the venue's own
    /// [`OndoAuthFailure::TimestampTooFar`] answer. Every other answer - including a second clock
    /// rejection - is returned as it is: this is not a retry loop, and a rejection the clock cannot
    /// explain is terminal like any other.
    async fn read_authenticated_account(&self) -> OndoHttpResult<OndoPrivateResponse> {
        match self.http_client.get_account().await {
            Err(error) if is_clock_skew_rejection(&error) => self.http_client.get_account().await,
            result => result,
        }
    }
}

/// The account half of the execution client, shared with the private transport.
///
/// A private transport runs on the global Tokio runtime and must be `Send + 'static`, so it
/// cannot hold the client: [`OndoExecutionClient`] carries the RC-based cache and the task
/// group. What the transport needs is the account - its reconciliation machine, its report
/// buffer, its one recovery claim, its reporter and its signed HTTP client - and that is what
/// this type is. It holds the *same* instances the client does, through the same `Arc`s, so
/// there is one account with one state, not two that agree.
///
/// # The one recovery claim
///
/// `pass` is the client's own [`PassOwnership`], not a copy: whether a recovery is in flight
/// has exactly one truth source, and both the periodic pass and the transport's inbound routing
/// read it there (plan §R3.1).
///
/// # What a transport may and may not decide
///
/// It may decide *when* to begin a recovery, to reconcile, to probe, to refresh metadata and to
/// arm or renew the switch. It may not decide what a report does to the account: that is
/// [`Self::ingest_stream_order`] and [`Self::ingest_stream_fill`], which is the one place the
/// apply-or-buffer decision lives.
#[derive(Debug, Clone)]
pub struct OndoAccountRuntime {
    reconciliation: Arc<RwLock<ReconciliationMachine>>,
    buffer: Arc<RwLock<ReconciliationBuffer>>,
    pass: Arc<PassOwnership>,
    reporter: OndoReporter,
    http_client: OndoHttpClient,
    clock: &'static AtomicTime,
    reconcile_interval_secs: u64,
    dms_timeout_secs: u64,
    /// The durable journal, when one was configured (plan §R3.2).
    ///
    /// [`None`] is a supported mode and it is not a quiet one: nothing is written, the ledger
    /// lives for this process, and [`ReconciliationMachine::journal`] says so.
    journal: Option<JournalHandle>,
    /// How many verified account states this runtime has published into the engine.
    ///
    /// A diagnostic counter only: it is incremented after a successful `AccountState` emission, so
    /// a read-only probe can distinguish "the account was read and published" from "the socket was
    /// up".
    account_state_published: Arc<AtomicU64>,
    /// The authenticated account identity, once it has been compared.
    account_identity: Arc<RwLock<OndoAccountIdentity>>,
    production: Option<Arc<crate::production::ProductionAuthority>>,
}

/// Where a journal checkpoint is written, and how many writes have failed.
///
/// It is a handle rather than a method because not every writer is the runtime: a submission that
/// has just decided its own outcome checkpoints from inside its own task, where the client's own
/// borrow does not reach. Cloning it is what lets that task write the journal the next pass writes.
#[derive(Debug, Clone)]
struct JournalHandle {
    path: Arc<std::path::Path>,
    write_failures: Arc<AtomicU64>,
    write_lock: Arc<Mutex<()>>,
    /// The instant of the last write that succeeded, in nanoseconds; zero is "never".
    last_written_at: Arc<AtomicU64>,
    /// The newest applied fill that write covered, in nanoseconds; zero is "none".
    watermark: Arc<AtomicU64>,
}

impl JournalHandle {
    fn new(path: std::path::PathBuf) -> Self {
        Self {
            path: Arc::from(path),
            write_failures: Arc::new(AtomicU64::new(0)),
            write_lock: Arc::new(Mutex::new(())),
            last_written_at: Arc::new(AtomicU64::new(0)),
            watermark: Arc::new(AtomicU64::new(0)),
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::SeqCst)
    }

    /// Returns when the last write that reached the disk was taken, when one ever was.
    fn last_written_at(&self) -> Option<UnixNanos> {
        match self.last_written_at.load(Ordering::SeqCst) {
            0 => None,
            nanos => Some(UnixNanos::from(nanos)),
        }
    }

    /// Returns the newest applied fill the last successful write covered, when it covered one.
    fn watermark(&self) -> Option<UnixNanos> {
        match self.watermark.load(Ordering::SeqCst) {
            0 => None,
            nanos => Some(UnixNanos::from(nanos)),
        }
    }

    /// Writes one checkpoint of the account as it stands.
    ///
    /// A failed write is **not** lost memory - the ledger, the order index and the unsettled writes
    /// are all still in this process - so it does not stop risk the way an unreadable journal does.
    /// What it does mean is that the next restart may have to re-apply fills this run has already
    /// reported, which is what the count is for and why the failure is logged loudly.
    ///
    /// A submission and a concluded pass both call this, and neither is a transcript of the other:
    /// what is written is the account as it stands, so the later write is always the more complete
    /// one.
    fn write(
        &self,
        account_id: AccountId,
        now: UnixNanos,
        state: &OndoPrivateState,
        machine: &ReconciliationMachine,
    ) {
        let _write = self.write_lock.lock();
        let journal = LedgerJournal::from_snapshot(JournalSnapshot {
            account_id,
            watermark: state.watermark,
            written_at: now,
            ledger: &state.fills,
            orders: state
                .orders
                .values()
                .map(JournalOrder::from_state)
                .collect(),
            unsettled: machine.unsettled_journal_entries(),
        });

        match journal.store_atomic(self.path()) {
            Ok(()) => {
                // Recorded only when the write reached the disk: what an operator needs from a
                // failed journal is *when it stopped*, and a stamp taken before the write would
                // claim coverage this run does not have.
                self.last_written_at.store(now.as_u64(), Ordering::SeqCst);
                self.watermark.store(
                    state.watermark.map_or(0, |value| value.as_u64()),
                    Ordering::SeqCst,
                );

                log::debug!(
                    "Ondo wrote a ledger journal checkpoint to {}: {} fill(s), {} order(s), {}                  unsettled write(s)",
                    self.path().display(),
                    journal.len(),
                    journal.orders().len(),
                    journal.unsettled().len(),
                );
            }
            Err(error) => {
                self.write_failures.fetch_add(1, Ordering::SeqCst);

                log::error!(
                    "Ondo could not write the ledger journal to {}: {error}; this run keeps its                      ledger in memory, and a restart may re-apply fills it has already reported",
                    self.path().display(),
                );
            }
        }
    }
}

impl OndoAccountRuntime {
    pub(crate) fn production_authority(
        &self,
    ) -> Option<Arc<crate::production::ProductionAuthority>> {
        self.production.clone()
    }

    pub(crate) async fn verify_production_dms_baseline(&self) -> anyhow::Result<()> {
        let Some(guard) = &self.production else {
            return Ok(());
        };
        if !guard.permits_dms() {
            anyhow::bail!("production DMS cleanup deadline exhausted");
        }
        guard
            .verify_identity(self.http_client.get_account().await?.venue_account_id())
            .map_err(anyhow::Error::msg)?;
        if self.account_identity() != OndoAccountIdentity::Matched || !guard.permits_dms() {
            anyhow::bail!("production DMS identity or deadline is unverified");
        }
        self.reconcile_account(self.now()).await?;
        let machine = self.reconciliation.read();
        let reading = machine
            .last_reading()
            .ok_or_else(|| anyhow::anyhow!("production DMS account unread"))?;
        if machine.last_judgment().is_none_or(|j| !j.is_clean())
            || !machine.unknown_submissions().is_empty()
        {
            anyhow::bail!("production DMS requires reconciled owned exposure only");
        }
        let reading = reading.clone();
        drop(machine);
        guard
            .verify_dms_reading(&reading)
            .map_err(anyhow::Error::msg)
    }

    pub(crate) async fn refresh_production_metadata(
        &self,
        response: &crate::http::models::MarketsResponse,
    ) -> anyhow::Result<()> {
        let Some(guard) = &self.production else {
            return Ok(());
        };
        guard.metadata(None, None, self.now().as_u64());
        let fetched_at = self.now();
        let infos = response.market_infos(fetched_at)?;
        let info = infos
            .into_iter()
            .find(|i| i.instrument_id() == guard.envelope().instrument_id)
            .ok_or_else(|| anyhow::anyhow!("approved production market is absent"))?;
        let contracts = self.http_client.get_contracts().await?;
        let mut closed = None;
        let mut matches = 0;
        for raw in contracts {
            let value: serde_json::Value = serde_json::from_str(raw.get())?;
            if value.get("market").and_then(serde_json::Value::as_str) == Some(info.market()) {
                matches += 1;
                if value.get("disabled").and_then(serde_json::Value::as_bool) != Some(false) {
                    anyhow::bail!(
                        "approved production contract is disabled or has unknown disabled state"
                    );
                }
                closed = value.get("isClosed").and_then(serde_json::Value::as_bool);
            }
        }
        if matches != 1 {
            anyhow::bail!("approved production contract must have exactly one metadata record");
        }
        info.try_increments()?;
        if !info.is_tradable() || closed != Some(false) {
            anyhow::bail!("production market status or underlying-hours policy is not satisfied");
        }
        guard.metadata(Some(info), closed, fetched_at.as_u64());
        Ok(())
    }

    /// Returns the local wall clock this runtime stamps with.
    #[must_use]
    pub fn now(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Returns the configured interval between account reconciliations, in seconds.
    #[must_use]
    pub const fn reconcile_interval_secs(&self) -> u64 {
        self.reconcile_interval_secs
    }

    /// Returns the configured dead man's switch timeout, in seconds.
    #[must_use]
    pub const fn dms_timeout_secs(&self) -> u64 {
        self.dms_timeout_secs
    }

    /// Returns what this run's journal did (plan §R3.2).
    ///
    /// A journal that was restored and has since failed to write is **not** reported as restored.
    /// The two facts live in different places - what was read back is the machine's, and whether
    /// writes are still landing is the write handle's - so they are joined here, at the one place a
    /// caller asks. Reporting `Restored` for a run whose last checkpoint never reached the disk
    /// would be exactly the run the configuration says must never exist: one that believed it was
    /// durable.
    ///
    /// [`JournalStatus::Failed`] keeps its own answer: a run that could not read its journal at all
    /// is missing a memory, not merely behind on writing one, and that one refuses new risk
    /// whatever its writes do.
    #[must_use]
    pub fn journal_status(&self) -> JournalStatus {
        let status = self.reconciliation.read().journal().clone();

        let Some(handle) = self.journal.as_ref() else {
            return status;
        };

        let failures = handle.write_failures();

        match status {
            JournalStatus::Restored {
                path,
                watermark: restored,
                ..
            } if failures > 0 => JournalStatus::Degraded {
                path,
                failures,
                last_written_at: handle.last_written_at(),
                // What the durable file holds is this run's last successful write, or - when it has
                // not landed one - the journal that was restored into it, which is still the file's
                // content. Both are `None` only when there was nothing to restore either.
                watermark: handle.watermark().or(restored),
            },
            other => other,
        }
    }

    /// Returns what this client can say about the account's funding (plan §R3.2).
    ///
    /// It is the reconciliation between the venue's **cumulative** funding total and the
    /// **payments** this client has read, and it is the only thing that answers "is the account's
    /// funding accounted for". The funding *rate* is public market data and is no part of it.
    #[must_use]
    pub fn funding_reconciliation(&self) -> FundingReconciliation {
        self.reconciliation.read().funding().reconciliation()
    }

    /// Returns the funding payments this client has accounted, in identity order.
    ///
    /// Every one of them is a record the venue published; none is derived from a rate.
    #[must_use]
    pub fn funding_payments(&self) -> Vec<FundingPayment> {
        self.reconciliation.read().funding().payments()
    }

    /// Returns the account's liquidation condition, as the last pass read it.
    #[must_use]
    pub fn liquidation(&self) -> LiquidationState {
        self.reconciliation.read().liquidation().clone()
    }

    /// Returns the path the durable journal is written to, when one is configured.
    #[must_use]
    pub fn journal_path(&self) -> Option<&std::path::Path> {
        self.journal.as_ref().map(JournalHandle::path)
    }

    /// Returns how many journal checkpoint writes have failed.
    #[must_use]
    pub fn journal_write_failures(&self) -> u64 {
        self.journal
            .as_ref()
            .map_or(0, JournalHandle::write_failures)
    }

    /// Returns the journal as a handle a spawned task can write from.
    fn journal_handle(&self) -> Option<JournalHandle> {
        self.journal.clone()
    }

    /// Restores this run's journal, or records why it could not be (plan §R3.2).
    ///
    /// It is called once, before the client is handed to anything that could submit, and the
    /// ordering is the point: a run that has not read its own ledger does not know which fills it
    /// already applied, and the only safe place to be in that state is one where no order can be
    /// sent. When there is no journal path the machine records
    /// [`JournalStatus::NotConfigured`] and this is a no-op beyond saying so.
    ///
    /// Nothing here reads the venue. The account is still unverified after a restore - that is
    /// what the recovery that follows is for - so a restart restores its memory **and then**
    /// re-converges before any new risk, never instead of re-converging.
    pub fn restore_journal(&self, now: UnixNanos) {
        let Some(handle) = self.journal.as_ref() else {
            log::warn!(
                "Ondo has no journal path configured: the dedup ledger, the order index and the \
                 unsettled writes live in this process's memory only, and a restart begins with an \
                 empty one"
            );

            return;
        };

        let status = match self.load_journal(handle.path(), now) {
            Ok(status) => status,
            Err(error) => JournalStatus::Failed {
                path: handle.path().display().to_string(),
                reason: error.to_string(),
            },
        };

        match &status {
            JournalStatus::Failed { .. } => log::error!("Ondo {}", status.reason()),
            _ => log::info!("Ondo {}", status.reason()),
        }

        self.reconciliation.write().set_journal(status);
    }

    /// Reads the journal and puts back everything it holds.
    ///
    /// The order associations are read back **before** anything is restored: a journal holding one
    /// this adapter cannot express leaves the run exactly as it found it, because a half-restored
    /// ledger - fills remembered, the orders they belong to not - is worse than an empty one.
    fn load_journal(
        &self,
        path: &std::path::Path,
        now: UnixNanos,
    ) -> anyhow::Result<JournalStatus> {
        let journal = LedgerJournal::load(path, self.reporter.account_id)?;

        let orders = journal
            .orders()
            .iter()
            .map(OndoOrderState::from_journal)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let order_count = orders.len();

        let restored = {
            let mut state = self.reporter.state.write();
            let restored = journal.restore(&mut state.fills, self.reporter.account_id)?;

            state.restore_orders(orders);

            // The watermark is a fact about the fills this client **applied**, and the ledger just
            // restored is the record of exactly those. Reading it back here is what makes the
            // field's own claim true: a run that restored a ledger and has applied nothing new
            // still reports the coverage it holds rather than none, so a restart does not present
            // itself as a client that has never seen a fill. It is put back through
            // [`OndoPrivateState::observe_fill`] - the same monotone rule the live path uses -
            // rather than assigned, so there is one rule and not two that could disagree.
            if let Some(watermark) = journal.watermark() {
                state.observe_fill(watermark);
            }

            restored
        };

        let registered = self
            .reconciliation
            .write()
            .restore_unsettled(journal.unsettled(), now);

        Ok(JournalStatus::Restored {
            path: path.display().to_string(),
            fills: restored.fills,
            orders: order_count,
            unsettled: registered,
            watermark: journal.watermark(),
        })
    }

    /// Writes a checkpoint of the account as it stands, when a journal is configured.
    ///
    /// It is taken at the end of a pass rather than on every event, and that is the ordering the
    /// journal's own documentation fixes: what is reported is reported first, and the journal
    /// records it afterwards. A crash between the two leaves a fill the engine has seen and a
    /// journal that does not hold it, which the next run reads back from the venue's history and
    /// re-reports under the same trade id - one fill, not two. The other ordering would have the
    /// journal claim a fill the engine never saw, and nothing downstream could tell that apart from
    /// one it had already applied.
    fn persist_journal(&self, now: UnixNanos) {
        let Some(handle) = self.journal.as_ref() else {
            return;
        };

        let state = self.reporter.state.read();
        let machine = self.reconciliation.read();

        handle.write(self.reporter.account_id, now, &state, &machine);
    }

    /// Reports the account's balance to the engine, when a pass has verified one.
    ///
    /// This is the internal reader plan §R3.2 asks for. Nothing outside this client drives it: a
    /// concluded reconciliation pass is what publishes the account state, through the same emitter
    /// and event channel every other execution event travels, so the account the engine's cache
    /// holds is the account this adapter read.
    ///
    /// What it publishes is the balance the machine **verified**
    /// ([`ReconciliationMachine::verified_balance`]): the whole account, with the venue's own
    /// arithmetic already checked. A pass that verified no balance publishes nothing at all and
    /// says why - unknown or missing numbers are never published as a normal state, and neither is
    /// a USDC-only view of an account that holds something else.
    ///
    /// # A negative equity is published as it was read
    ///
    /// `total` is `marginBalance`, `locked` is `usedMargin` and `free` is `availableMargin`, and
    /// they are published exactly as the venue sent them. `AccountBalance` derives `free` from the
    /// other two at the currency's precision; for a negative total that derivation preserves the
    /// locked amount and lets `free` carry the shortfall, which is the statement the venue made
    /// rather than a repaired version of it. Clamping an underwater account to zero would report a
    /// healthy one (plan §6.4).
    fn publish_account_state(&self, now: UnixNanos) {
        let (balance, findings) = {
            let machine = self.reconciliation.read();

            (
                machine.verified_balance().cloned(),
                machine
                    .last_judgment()
                    .map(AccountJudgment::reasons)
                    .unwrap_or_default(),
            )
        };

        let Some(balance) = balance else {
            log::warn!(
                "Ondo published no account state: this pass verified no balance ({})",
                findings.join("; "),
            );

            return;
        };

        match account_state_parts(&balance) {
            Ok((balances, margins)) => {
                self.reporter
                    .emitter
                    .emit_account_state(balances, margins, true, now, None);
                self.account_state_published.fetch_add(1, Ordering::SeqCst);
            }
            Err(error) => log::error!(
                "Ondo could not report the account's balance as a Nautilus account state: {error}"
            ),
        }
    }

    /// Returns a clone of the signed HTTP client.
    #[must_use]
    pub fn http_client(&self) -> OndoHttpClient {
        self.http_client.clone()
    }

    /// Returns the account's reconciliation state.
    #[must_use]
    pub fn reconciliation_state(&self) -> ReconciliationState {
        self.reconciliation.read().state()
    }

    /// Returns whether this client is an account read-only session.
    #[must_use]
    pub fn is_account_read_only(&self) -> bool {
        self.reconciliation.read().is_account_read_only()
    }

    /// Returns how many verified account states this runtime has published.
    #[must_use]
    pub fn account_state_published(&self) -> u64 {
        self.account_state_published.load(Ordering::SeqCst)
    }

    /// Returns the authenticated account identity.
    #[must_use]
    pub fn account_identity(&self) -> OndoAccountIdentity {
        *self.account_identity.read()
    }

    /// Records the authenticated account identity.
    pub fn set_account_identity(&self, identity: OndoAccountIdentity) {
        *self.account_identity.write() = identity;
    }

    /// Returns whether the switch permits new orders at this instant.
    ///
    /// The deadline is part of the answer: an armed switch whose deadline has passed does not permit
    /// them, and this reads the clock rather than the stored state so that it says so.
    #[must_use]
    pub fn dead_mans_switch_permits_orders(&self) -> bool {
        let now = self.now();

        self.reconciliation
            .read()
            .dead_mans_switch()
            .permits_new_orders(now)
    }

    /// Returns the switch's state at this instant, with its deadline applied.
    ///
    /// An armed switch whose deadline has passed reads [`DeadMansSwitchState::Lapsed`] here, which is
    /// the state the admission and the run state both act on. The stored state still says `Armed`:
    /// the lapse is a projection over the clock rather than a transition, so reading it changes
    /// nothing (`DeadMansSwitch::state_at`).
    #[must_use]
    pub fn dead_mans_switch_state(&self) -> DeadMansSwitchState {
        let now = self.now();

        self.reconciliation.read().dead_mans_switch().state_at(now)
    }

    /// Returns whether the switch is waiting for the venue's acknowledgement.
    ///
    /// A venue error or a failed send while this is true means the arm may never have taken, which
    /// is why the transport fails the switch closed rather than assuming it did.
    #[must_use]
    pub fn dead_mans_switch_is_arming(&self) -> bool {
        self.dead_mans_switch_state() == DeadMansSwitchState::Arming
    }

    /// Returns how many times an armed switch has been renewed.
    #[must_use]
    pub fn dead_mans_switch_renewals(&self) -> u64 {
        self.reconciliation.read().dead_mans_switch().renewals()
    }

    /// Records that the session ended.
    ///
    /// The account is unverified from here: new risk stops until a recovery converges again, while
    /// cancels and queries still travel (plan §6.4).
    pub fn note_session_ended(&self, now: UnixNanos) {
        if let Some(guard) = &self.production {
            guard.invalidate();
        }
        self.reconciliation.write().note_disconnected(now);
    }

    /// Releases the switch, returning the frame the private stream must send (plan §6.4).
    ///
    /// The wrapper [`crate::reconciliation::DeadMansSwitch::release`] was missing: the switch could
    /// build the frame and nothing could put it on a socket, so
    /// [`crate::reconciliation::StopStep::ReleaseDeadMansSwitch`] was a step no caller could carry
    /// out.
    ///
    /// **This is not the stop path.** The order the stop sequence fixes is a requirement - this
    /// run's orders are cancelled and confirmed *before* anything is released, because a released
    /// switch cancels nothing and the orders it was covering would be left resting - and executing
    /// that sequence is plan §R3.3's. It exists here so that the step has a caller when it does,
    /// and it is the same shape as the five hooks beside it.
    pub fn release_dead_mans_switch(&self, now: UnixNanos) -> DeadMansSwitchMessage {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .release(now)
    }

    /// Routes one private order report to the seam that owns the account right now.
    ///
    /// **This is the one place that decision is made**, and it is made here rather than in the
    /// transport because it is part of the recovery's merge protocol: a report that arrives while a
    /// pass is reading the account is *held* so the pass can replay it after the REST pages, and a
    /// report that arrives when no pass is running is applied now, where the dedup ledger and the
    /// order index make it safe (plan §6.4, §R3.1).
    ///
    /// The check and the hold happen under the buffer's own write lock, which is the lock
    /// [`Self::close_pass`] takes to drain and end the pass. So a report that reads "a pass is
    /// running" is a report the running pass has not drained yet - not one stranded behind a drain
    /// that has already happened.
    ///
    /// What this cannot close, and does not pretend to: a report applied directly in the instant
    /// before a pass claims the account is already applied when that pass reads the REST pages. The
    /// buffer exists for the *ordering* hazard of a pass in flight, not for a venue whose REST
    /// history lags its own stream - and the recovery's second agreeing pass is what catches that.
    pub fn ingest_stream_order(&self, payload: OndoApiOrder) -> OndoStreamIngestion {
        if let Some(guard) = &self.production {
            guard.activity();
            if !payload.status().is_terminal()
                && payload.client_order_id().is_none_or(|id| !guard.owns(id))
            {
                guard.stop_creates();
            }
        }
        let mut buffer = self.buffer.write();

        if self.pass.is_claimed() {
            let generation = self.reconciliation.read().recovery_generation();
            buffer.record_order(payload, generation);

            let dropped = buffer.take_dropped();
            drop(buffer);
            self.note_refused_reports(dropped);

            return OndoStreamIngestion::Buffered;
        }

        drop(buffer);
        self.apply_order(&payload);

        OndoStreamIngestion::Applied
    }

    /// Routes one private fill report to the seam that owns the account right now.
    ///
    /// The same decision as [`Self::ingest_stream_order`], under the same boundary, for the same
    /// reason.
    pub fn ingest_stream_fill(&self, fill: OndoApiFill) -> OndoStreamIngestion {
        if let Some(guard) = &self.production {
            guard.activity();
        }
        let mut buffer = self.buffer.write();

        if self.pass.is_claimed() {
            let generation = self.reconciliation.read().recovery_generation();
            buffer.record_fill(fill, generation);

            let dropped = buffer.take_dropped();
            drop(buffer);
            self.note_refused_reports(dropped);

            return OndoStreamIngestion::Buffered;
        }

        drop(buffer);

        if let Err(error) = self.apply_fill(&fill) {
            // A fill the state machine cannot express at all is a report the account saw and could
            // not apply, so it is a loss rather than a silent skip (plan §6.4).
            self.note_lost_reports(
                1,
                format!("the fill {} could not be applied: {error}", fill.id()),
            );
        }

        OndoStreamIngestion::Applied
    }

    /// Begins a recovery: the account is about to be read, and new risk stops until it converges.
    ///
    /// The private stream's connect and reconnect hooks call this, and so does the sandbox probe's
    /// account mode. It is the client's own `connect` that does **not** call it: a recovery is a
    /// decision to read the account, not a socket coming up.
    pub fn begin_recovery(&self, now: UnixNanos) {
        self.reconciliation.write().begin_recovery(now);
    }

    /// Records whether the metadata this client trades on is usable (plan §4.1).
    pub fn set_metadata(&self, validity: MetadataValidity) {
        self.reconciliation.write().set_metadata(validity);
    }

    /// Arms the dead man's switch, returning the frame the private stream must send.
    ///
    /// New orders wait for the venue's confirmation: an unconfirmed arm is not an arm (§6.4).
    pub fn arm_dead_mans_switch(&self, now: UnixNanos) -> DeadMansSwitchMessage {
        if self.is_account_read_only() {
            return DeadMansSwitchMessage::new(
                crate::websocket::messages::WsOp::Subscribe,
                self.dms_timeout_secs,
            );
        }
        self.reconciliation.write().dead_mans_switch_mut().arm(now)
    }

    /// Applies the venue's confirmation of the switch.
    pub fn confirm_dead_mans_switch(&self, now: UnixNanos) {
        if self.production.is_some() {
            return;
        }
        self.confirm_dead_mans_switch_from_stream(now);
    }

    pub(crate) fn confirm_dead_mans_switch_from_stream(&self, now: UnixNanos) -> bool {
        if self.is_account_read_only() {
            return false;
        }
        if self
            .production
            .as_ref()
            .is_some_and(|guard| !guard.confirm_dms(now.as_u64()))
        {
            return false;
        }
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .confirm_armed(now);
        true
    }

    /// Builds the frame that renews an armed switch, or [`None`] when there is nothing to renew.
    ///
    /// Composing is not renewing: this takes `&self` and moves no deadline, because a frame that
    /// never reached the venue has not restarted its timer (plan §R3.3).
    #[must_use]
    pub fn dead_mans_switch_renewal_frame(&self) -> Option<DeadMansSwitchMessage> {
        if self.is_account_read_only() {
            return None;
        }
        self.reconciliation.read().dead_mans_switch().renew_frame()
    }

    /// Re-arms the switch as though the venue had confirmed it at `confirmed_at`.
    ///
    /// It exists for one reason: the deadline is what admission now reads, and the only way to reach
    /// a lapsed switch through this type is to place its confirmation in the past. Waiting for a
    /// real thirty-second timeout is not a test anybody would keep, and moving the process clock is
    /// not something this adapter offers - the transport reads the same clock, so a moved clock
    /// would move the thing under test with it.
    ///
    /// It is **not** a second arm: no frame is built and nothing is sent. What it moves is the
    /// deadline of a switch the venue has already confirmed, which is why it takes the instant of a
    /// confirmation rather than a timeout - [`Self::confirm_dead_mans_switch`] remains the only
    /// path that a confirmation actually travels.
    pub fn dead_mans_switch_confirmed_at(&self, confirmed_at: UnixNanos) {
        self.confirm_dead_mans_switch(confirmed_at);
    }

    /// Records that a renewal frame was written at `now`, moving the switch's deadline.
    ///
    /// It also clears the run of consecutive renewal failures. It is **not** a confirmation: no
    /// message acknowledges a renewal, so all this records is that the frame left this process.
    pub fn note_dead_mans_switch_renewed(&self, now: UnixNanos) {
        if self.is_account_read_only() {
            return;
        }
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .note_renew_sent(now);
    }

    /// Records that a renewal frame could not be written, failing the switch at the bound.
    ///
    /// The bound is the configured limit capped at the crate's ceiling, so a run of unsendable
    /// renewals ends in [`DeadMansSwitchState::Failed`] rather than in an unrenewed switch that
    /// still reads `Armed` (plan §R3.3).
    pub fn note_dead_mans_switch_renew_failed(&self, reason: String) {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .note_renew_failed(reason);
    }

    /// Records that the switch failed, which stops new orders.
    pub fn note_dead_mans_switch_failed(&self, reason: String) {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .fail(reason);
    }

    /// Records a cancel whose outcome no venue answer settled (plan §6.3).
    ///
    /// The stop calls this directly, because it issues its cancels itself rather than through the
    /// client's spawning path - and a cancel this client cannot account for is exactly what keeps
    /// the switch armed and what a restart reads back out of the journal.
    pub fn note_unconfirmed_cancel(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        reason: String,
        now: UnixNanos,
    ) {
        self.reconciliation.write().note_unconfirmed_cancel(
            client_order_id,
            venue_order_id,
            reason,
            now,
        );
    }

    /// Records that the switch fired: the venue cancelled this account's resting orders.
    ///
    /// The account becomes uncertain, because what the cancellation left behind has to be read, and
    /// no position is closed by a switch (plan §6.4).
    pub fn note_dead_mans_switch_fired(&self, now: UnixNanos) {
        self.reconciliation.write().note_switch_fired(now);
    }

    /// Cancels one order by client order id, and confirms the outcome before returning.
    ///
    /// It is the single-order cancel and nothing else: `DELETE /v1/perps/orders/{orderID}`, resolved
    /// from this client's own order index. The market-wide `DELETE /v1/perps/orders?market=...` is
    /// deliberately unreachable from here - a stop cancels the orders **this run placed**, and a
    /// market cancel would take out orders it never placed.
    ///
    /// It **awaits** rather than spawning, which is the whole reason it exists beside the client's
    /// own `cancel_order`: a stop has to know that the cancels it issued have been answered before
    /// it can say whether the switch may be released, and a spawned task cannot tell it.
    ///
    /// The outcome is confirmed by a read rather than assumed from the call (plan §6.3). A venue
    /// answer that states the order confirms it; an answer that does not is registered as an
    /// unconfirmed cancel *before* the confirming query, so a query that cannot be answered leaves
    /// the cancel outstanding instead of leaving nothing behind at all.
    ///
    /// The cancel is registered as unconfirmed **before** the request is issued, so a stop whose
    /// future the caller drops mid-wait still finds it in the ledger; the request's own answer
    /// clears it again. This is the awaiting cancel the stop uses, not the client's spawned one.
    ///
    /// # Errors
    ///
    /// Returns an error when the cancel never became a request - a transport failure, or a refusal
    /// that carries no code this adapter reads as requiring a query. The error is the caller's to
    /// record; an ambiguous answer is registered rather than returned.
    pub async fn cancel_order(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        now: UnixNanos,
    ) -> anyhow::Result<()> {
        let order_ref = match venue_order_id {
            Some(venue_order_id) => venue_order_id.to_string(),
            None => self.reporter.order_ref(&client_order_id),
        };

        // Registered **before** the request exists. A cancel that is on the wire is one whose
        // outcome this client cannot state yet, and a stop the caller cuts short while it waits has
        // to find it in the ledger rather than only in a task about to be dropped. The request's own
        // answer clears it again; an answer that does not is what leaves it here.
        self.reconciliation.write().note_unconfirmed_cancel(
            client_order_id,
            venue_order_id,
            "the cancel request was issued and its answer has not settled it".to_string(),
            now,
        );

        match self
            .http_client
            .cancel_order(&order_ref, OndoRequestPriority::High)
            .await
        {
            Ok(OndoCancelAnswer::Order(order)) => {
                self.reporter.apply_order(&order, Acceptance::Report, None);

                // The venue stated the order. That settles the cancel only when what it stated is
                // terminal: an order the venue still calls working is one this client may still
                // have to cancel.
                if order.status().is_terminal() {
                    self.reconciliation.write().confirm_cancel(&client_order_id);
                }

                Ok(())
            }
            Ok(OndoCancelAnswer::Unconfirmed { raw }) => {
                log::warn!(
                    "Ondo accepted the cancel of {client_order_id} without reporting the order, so \
                     its state is confirmed by a query ({raw})"
                );
                self.confirm_cancel(&client_order_id).await;

                Ok(())
            }
            Err(error) if is_definitive_refusal(&error) => {
                let rejection = cancel_rejection(&error);

                if rejection.requires_query() {
                    self.confirm_cancel(&client_order_id).await;

                    Ok(())
                } else {
                    // The cancel did not take and needs no query. The registration made above is
                    // cleared here and the caller is told, rather than left in the ledger as a
                    // cancel that never was.
                    self.reconciliation.write().confirm_cancel(&client_order_id);

                    Err(anyhow::anyhow!("{error}"))
                }
            }
            Err(error) => {
                // Ambiguous: the venue may still act on it, so the registration made above stands
                // and the query that would settle it is run (plan §6.3).
                log::warn!(
                    "Ondo's cancel of {client_order_id} was not answered ({error}); its state is \
                     confirmed by a query"
                );
                self.confirm_cancel(&client_order_id).await;

                Ok(())
            }
        }
    }

    /// Asks the venue what became of one order, and settles the cancel if it answers.
    ///
    /// The same query the client's cancel path makes, on the same reference: a cancel API having
    /// been called is not an order having been cancelled (plan §6.3).
    async fn confirm_cancel(&self, client_order_id: &ClientOrderId) {
        let order_ref = self.reporter.order_ref(client_order_id);

        match self
            .http_client
            .get_order(&order_ref, OndoRequestPriority::High)
            .await
        {
            Ok(order) => {
                self.reporter.apply_order(&order, Acceptance::Report, None);

                // A successful query is not a confirmed cancellation. The cancel is settled only
                // when the order the venue stated is settled: terminal, its fills agreeing with the
                // venue's own total and no accepted fill still waiting to be reported. An order
                // the venue still reports as working is still an order this client may have to
                // cancel.
                let settled = self
                    .reporter
                    .state
                    .read()
                    .orders
                    .get(client_order_id)
                    .is_some_and(OndoOrderState::is_settled);

                if settled {
                    self.reconciliation.write().confirm_cancel(client_order_id);
                }
            }
            Err(error) => log::warn!(
                "The confirming query after a cancel failed for {client_order_id} ({order_ref}): \
                 {error}; the order's state stays unconfirmed"
            ),
        }
    }

    /// Records a private report that arrived while the account was being read.
    ///
    /// The report is held rather than applied so the pass can replay it through the same state
    /// machine the REST pages go through, where it is deduped (plan §6.4). It is attributed to the
    /// recovery the account is in at this instant: a report recorded before this recovery began, or
    /// after the session it belonged to ended, is refused when the pass drains rather than applied
    /// as current state.
    ///
    /// A report the bounded buffer refuses for want of room is **not** dropped quietly: the account
    /// becomes uncertain until a pass has read it whole again (plan §6.4).
    pub fn buffer_stream_order(&self, payload: OndoApiOrder) {
        let generation = self.reconciliation.read().recovery_generation();
        let dropped = {
            let mut buffer = self.buffer.write();
            buffer.record_order(payload, generation);

            buffer.take_dropped()
        };

        self.note_refused_reports(dropped);
    }

    /// Records a private fill that arrived while the account was being read.
    ///
    /// Buffered, attributed and bounded exactly as [`Self::buffer_stream_order`] buffers an order
    /// report.
    pub fn buffer_stream_fill(&self, fill: OndoApiFill) {
        let generation = self.reconciliation.read().recovery_generation();
        let dropped = {
            let mut buffer = self.buffer.write();
            buffer.record_fill(fill, generation);

            buffer.take_dropped()
        };

        self.note_refused_reports(dropped);
    }

    /// Records the reports the buffer refused for want of room.
    ///
    /// The buffer's own count is what is reported, so the machine is told how many facts about the
    /// account this client saw and does not hold, not merely that something was dropped.
    fn note_refused_reports(&self, dropped: usize) {
        if dropped == 0 {
            return;
        }

        let capacity = self.buffer.read().capacity();

        self.note_lost_reports(
            dropped,
            format!("the recovery buffer holds {capacity} report(s) and refused {dropped} more"),
        );
    }

    /// Records reports the recovery could not apply to the account.
    ///
    /// Public because the private transport is a caller: a frame it could not decode is a report
    /// the account lost, and saying so is what stops the account reading Ready over a hole
    /// (plan §R3.1).
    pub fn note_lost_reports(&self, count: usize, reason: String) {
        log::error!("Ondo lost {count} report(s) during recovery: {reason}");
        self.reconciliation.write().note_lost_reports(count, reason);
    }

    /// Reads the account once and concludes a pass (plan §6.4).
    ///
    /// The reads are the venue's own lists, walked with the same bounded [`CursorWalk`] every other
    /// history read uses, and every payload is applied through [`Self::apply_order`] and
    /// [`Self::apply_fill`] - the one state machine, so the stream and this pass can never disagree
    /// about what a payload means. The reports the stream buffered while the pass read are replayed
    /// into it, after the last read and under the boundary that ends the pass, where the ledger and
    /// the order index dedupe them.
    ///
    /// One pass owns the account at a time: two would both drain the buffer - the second finding it
    /// empty - and each would conclude a reading the other half-wrote.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryPassRefusal::AlreadyRunning`] when another pass owns the account, without
    /// reading anything and without judging this one; and an error when the account cannot be read
    /// completely - a failed request, an unreadable page, a cursor that will not advance. The
    /// machine is left [`ReconciliationState::Uncertain`] in that case, because a pass that stopped
    /// early must not look like an account that was read.
    pub async fn reconcile_account(&self, now: UnixNanos) -> anyhow::Result<AccountJudgment> {
        let Some(pass) = self.pass.claim() else {
            log::error!("Ondo refused a recovery pass: another pass already owns the account");

            return Err(RecoveryPassRefusal::AlreadyRunning.into());
        };

        let activity = self.production.as_ref().map(|g| g.activity_generation());
        let reading = if let Some(guard) = &self.production {
            match tokio::time::timeout(guard.remaining(), self.read_account(&pass, now)).await {
                Ok(reading) => reading,
                Err(_) => Err(anyhow::anyhow!(
                    "production reconciliation cleanup deadline exhausted"
                )),
            }
        } else {
            self.read_account(&pass, now).await
        };
        match reading {
            Ok(reading) => {
                self.reconciliation.write().conclude_pass(&reading, now);

                // The two things a concluded pass produces outside the state machine: the
                // account's state, for the engine, and the checkpoint, for the next process.
                // Both are taken from the state the pass just concluded rather than from the
                // reading, so what is published is what the machine judged.
                self.publish_account_state(now);
                self.persist_journal(now);
                if let Some(guard) = &self.production {
                    let clean = self
                        .reconciliation
                        .read()
                        .last_judgment()
                        .is_some_and(AccountJudgment::is_clean);
                    guard.reconcile(&reading, clean, activity.unwrap_or_default());
                }

                Ok(self
                    .reconciliation
                    .read()
                    .last_judgment()
                    .cloned()
                    .unwrap_or_default())
            }
            Err(error) => {
                log::error!("Ondo account reconciliation failed: {error}");
                self.reconciliation
                    .write()
                    .note_pass_failed(error.to_string(), now);

                Err(error)
            }
        }
    }

    /// Probes every unsettled write a probe is due for, under the reference that identifies it.
    ///
    /// A submission is asked about under the client order id it was made with, a cancel under the
    /// venue order id when this session has one - a probe never invents a new client order id,
    /// because that would be a second order. A probe that finds the order applies it and settles
    /// the outcome; a 404 settles nothing (plan §6.3), and an inconclusive probe settles nothing
    /// either.
    #[must_use]
    pub async fn probe_unknown_submissions(&self, now: UnixNanos) -> Vec<ProbeReport> {
        let due = self.reconciliation.read().probe_due(now);
        let mut reports = Vec::new();

        for client_order_id in due {
            let Some(lookup) = self
                .reconciliation
                .read()
                .uncertain_outcome(&client_order_id)
                .map(|outcome| outcome.lookup.clone())
            else {
                continue;
            };

            let outcome = match self
                .http_client
                .get_order(&lookup, OndoRequestPriority::High)
                .await
            {
                Ok(payload) => {
                    self.apply_order(&payload);

                    // A working order settles an unknown submission - the order existing means the
                    // request was applied - but it does not settle an unconfirmed cancel. The
                    // machine is told which one this is.
                    let settled = self
                        .reporter
                        .state
                        .read()
                        .orders
                        .get(&client_order_id)
                        .is_some_and(OndoOrderState::is_settled);

                    ProbeOutcome::Found { settled }
                }
                Err(OndoHttpError::RequestRejected { status: 404, .. }) => ProbeOutcome::NotFound,
                Err(error) => ProbeOutcome::Inconclusive {
                    reason: error.to_string(),
                },
            };

            let disposition =
                self.reconciliation
                    .write()
                    .note_probe(&client_order_id, outcome, now);

            log::warn!(
                "Ondo probed the unsettled write {client_order_id} ({lookup}) at {:?}: \
                 {disposition:?}",
                self.reconciliation.read().state(),
            );

            reports.push(ProbeReport {
                client_order_id,
                disposition,
            });
        }

        reports
    }

    /// Expires the unknown submissions whose window has passed (plan §6.3).
    ///
    /// Probing stops, the submissions stay unknown, and what comes back is exactly what a human
    /// reconciling the account by hand needs: the client order ids and how long they have been
    /// unknown.
    #[must_use]
    pub fn abandon_unknown_submissions(
        &self,
        now: UnixNanos,
    ) -> Vec<crate::reconciliation::AbandonedSubmission> {
        let abandoned = self.reconciliation.write().expire_unknown_submissions(now);

        for submission in &abandoned {
            log::error!(
                "Ondo left submission {} unresolved after {} ns and {} probe(s); it is not \
                 resubmitted and needs manual reconciliation",
                submission.client_order_id,
                submission.elapsed_ns,
                submission.attempts,
            );
        }

        abandoned
    }

    /// Reads the account: orders, fills, positions, balance, and then the buffered reports.
    ///
    /// The order readings are built **after** every payload of the pass has been applied, not as
    /// the pages arrive. Pagination means the order list and the fill history are read minutes
    /// apart in the worst case, and nothing in the protocol orders the two: an order read before
    /// the fill that completed it would otherwise be judged against a filled quantity the same
    /// pass had not applied yet, and a reconciled account would look like a disagreement.
    ///
    /// The buffered reports are taken last of all, once the positions and the balance have been
    /// read, and under the boundary that ends the pass: a report arriving while this pass reads is
    /// either in what the drain took or in the buffer the next pass will drain, never in neither
    /// place and never in both. After the drain nothing here can fail, so a pass that took reports
    /// out of the buffer is a pass that applied them.
    async fn read_account(
        &self,
        pass: &PassGuard<'_>,
        now: UnixNanos,
    ) -> anyhow::Result<AccountReading> {
        let mut reading = AccountReading::default();
        let mut observed = ObservedOrders::default();

        reading.read_at = now;
        self.read_orders(&mut observed).await?;
        self.read_fills(&mut reading).await?;
        reading.positions = self.read_positions().await?;
        reading.balance = Some(self.read_balance().await?);

        // The funding history is a **separate axis** and a failure of it is not a failed pass. The
        // orders, the fills, the positions and the balance are what the account *is*, and a pass
        // that read all four has read the account; funding is a cashflow the venue records
        // elsewhere, and its read is bounded and re-taken every pass. What a failure must not do is
        // pass unnoticed: it is recorded on the reading and reported by the funding judgment, so
        // the pass says "the funding could not be read" rather than "the funding is fine"
        // (plan §R3.2).
        match self.read_funding(now).await {
            Ok(payments) => reading.funding = payments,
            Err(error) => {
                log::error!("Ondo funding history could not be read: {error}");
                reading.funding_error = Some(error.to_string());
            }
        }

        let drained = self.close_pass(pass);

        self.replay(&drained, &mut observed, &mut reading);

        reading.orders = observed
            .entries
            .iter()
            .map(|payload| self.order_reading(payload))
            .collect();
        reading.applied_net = self.reporter.state.read().applied_net();

        Ok(reading)
    }

    /// Takes the reports the stream buffered, and ends the pass, under one boundary (plan §6.4).
    ///
    /// The drain and the switch from "this pass owns the account" to "it does not" happen under the
    /// same write lock the private stream records under, and they happen after the last await the
    /// pass makes. A report arriving at that instant therefore either lands in the set this drain
    /// took or in the buffer the next pass will drain - never in neither, which would strand it, and
    /// never in both, which would apply it twice.
    ///
    /// What the drain takes is what belongs to the recovery the machine is on **now**, read here
    /// rather than taken from the pass: a recovery that began while this pass was reading supersedes
    /// the reports recorded before it, and a pass that replayed those would be applying the state of
    /// a session the machine has left as the state of the one it is reconciling.
    ///
    /// The generation is read under the buffer's own write lock, not before it, so that no report
    /// can be recorded between the read and the drain: the stream records under that lock, so while
    /// it is held the buffer holds exactly the reports stamped at or before the generation drained
    /// and none stamped after it. Read first, a recovery beginning in the gap would have the reports
    /// it recorded counted as superseded by the older drain, which costs a pass and an uncertain
    /// account over a report that is in fact current.
    fn close_pass(&self, pass: &PassGuard<'_>) -> DrainedReports {
        let mut buffer = self.buffer.write();
        let generation = self.reconciliation.read().recovery_generation();
        let drained = buffer.drain_generation(generation);

        pass.end();

        drained
    }

    /// Walks the venue's order list, applying every payload it carries.
    async fn read_orders(&self, observed: &mut ObservedOrders) -> anyhow::Result<()> {
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();

        loop {
            let response =
                self.http_client.get_orders(&query).await.map_err(|error| {
                    anyhow::anyhow!("the order list could not be read: {error}")
                })?;

            for item in response.items()? {
                let payload = OndoApiOrder::from_text(item.get())?;

                observed.observe(&payload);
                self.apply_order(&payload);
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        Ok(())
    }

    /// Walks the venue's fill history, applying every fill it carries.
    ///
    /// A fill the state machine cannot express at all - a market that does not map onto an
    /// instrument, an unreadable `size`/`price`/`fee` - is a fact about the account this pass saw and
    /// could not apply, so it is recorded as a loss rather than skipped past: the pass that follows
    /// may not judge an account it did not read whole (plan §6.4).
    async fn read_fills(&self, reading: &mut AccountReading) -> anyhow::Result<()> {
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();
        let mut unreadable = Vec::new();

        loop {
            let response =
                self.http_client.get_fills(&query).await.map_err(|error| {
                    anyhow::anyhow!("the fill history could not be read: {error}")
                })?;

            for fill in response.fills()? {
                match self.apply_fill(&fill) {
                    Ok(OndoFillApplication::Applied) => reading.fills.push(fill.id().to_string()),
                    Ok(_) => {}
                    Err(error) => {
                        log::error!(
                            "Ondo fill {} could not be applied during reconciliation: {error}",
                            fill.id(),
                        );
                        unreadable.push((fill.id().to_string(), error.to_string()));
                    }
                }
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        for (fill_id, reason) in unreadable {
            self.note_lost_reports(
                1,
                format!("the fill {fill_id} could not be applied: {reason}"),
            );
        }

        Ok(())
    }

    /// Replays the private reports one drain took, in the order they arrived (plan §6.4).
    ///
    /// They go through [`Self::apply_order`] and [`Self::apply_fill`] - the same state machine the
    /// pages go through - so an order's run of reports is applied in the order the stream delivered
    /// it, the last of them being the state the order is left in, and a report the pages already
    /// carried is deduped rather than applied a second time.
    ///
    /// What is *not* applied is reported. Reports held under a superseded recovery are refused by
    /// the drain, and a fill the state machine cannot express would otherwise leave no trace at all:
    /// an order's unreadable status reaches the judgment through the reading either way, but a fill
    /// that could not be applied has to be said out loud (plan §6.4).
    fn replay(
        &self,
        drained: &DrainedReports,
        observed: &mut ObservedOrders,
        reading: &mut AccountReading,
    ) {
        if drained.superseded > 0 {
            self.note_lost_reports(
                drained.superseded,
                "they were recorded for a recovery that has since been superseded".to_string(),
            );
        }

        for payload in &drained.orders {
            observed.observe(payload);
            self.apply_order(payload);
        }

        for fill in &drained.fills {
            match self.apply_fill(fill) {
                Ok(OndoFillApplication::Applied) => reading.fills.push(fill.id().to_string()),
                Ok(_) => {}
                Err(error) => {
                    log::error!(
                        "A buffered Ondo fill {} could not be applied during reconciliation: \
                         {error}",
                        fill.id(),
                    );
                    self.note_lost_reports(
                        1,
                        format!(
                            "the buffered fill {} could not be applied: {error}",
                            fill.id()
                        ),
                    );
                }
            }
        }

        if !drained.orders.is_empty() || !drained.fills.is_empty() {
            log::info!(
                "Replayed {} buffered Ondo order report(s) and {} fill(s) during reconciliation",
                drained.orders.len(),
                drained.fills.len(),
            );
        }
    }

    /// Reads the account's positions.
    async fn read_positions(&self) -> anyhow::Result<Vec<PositionReading>> {
        let response = self
            .http_client
            .get_positions(&OndoPrivateReadQuery::new())
            .await
            .map_err(|error| anyhow::anyhow!("the positions could not be read: {error}"))?;

        if self.production.is_some() && response.cursor().is_some() {
            anyhow::bail!("production positions coverage is incomplete");
        }
        let mut readings = Vec::new();

        for item in response.items()? {
            let value: serde_json::Value = serde_json::from_str(item.get())
                .map_err(|error| anyhow::anyhow!("a position could not be read: {error}"))?;

            let Some(market) = value.get("market").and_then(serde_json::Value::as_str) else {
                anyhow::bail!("a position carries no readable `market`");
            };

            let Some(direction) = value.get("direction").and_then(serde_json::Value::as_str) else {
                anyhow::bail!("the position on {market} carries no readable `direction`");
            };

            let Some(net_quantity) = value.get("netQuantity").and_then(serde_json::Value::as_str)
            else {
                anyhow::bail!("the position on {market} carries no readable `netQuantity`");
            };

            // The entry price is read but never required: an absent or non-string member leaves
            // the report's own optional field empty, and a member that is a string and is not a
            // decimal is a payload this adapter does not understand.
            let average_entry_price = match value
                .get("averageEntryPrice")
                .and_then(serde_json::Value::as_str)
            {
                Some(price) => Some(balance_member(price, "averageEntryPrice")?),
                None => None,
            };

            readings.push(PositionReading::new(
                market,
                direction,
                balance_member(net_quantity, "netQuantity")?,
                average_entry_price,
            ));
        }

        Ok(readings)
    }

    /// Walks the venue's funding history, returning the payments it carries.
    ///
    /// The window is applied here rather than sent: `startTime`/`endTime` are documented on this
    /// endpoint as UTC milliseconds, and a window this adapter cannot see the effect of, over an
    /// inclusive/exclusive convention the frozen spec states only in prose, is a second filter that
    /// could silently drop a payment. What is *not* optional is the payment's own `time`: a
    /// `FundingFeeTransfer` is an event with an instant, and the funding ledger is reconciled over a
    /// window, so a record that cannot be placed is one this adapter says it could not read rather
    /// than one it quietly counts.
    ///
    /// # The walk ends where the reconciliation starts
    ///
    /// The bound is the funding ledger's baseline instant
    /// ([`crate::reconciliation::FundingLedger::baseline`]): the moment of the first readable
    /// `totalFundingPayments`, which `judge_funding` establishes at `reading.read_at` - this same
    /// `now`. Before any pass has read such a total there is no baseline to bound by, and the bound
    /// is `now` itself, because that is the instant this pass's own judgment is about to establish
    /// one at: a payment older than that is not one the window that opens there reaches, and while
    /// there is no baseline `accounted_since_baseline` counts nothing at all.
    ///
    /// The venue returns this history most recent first - the frozen spec's own ordering - so a page
    /// whose every record lies before that bound is the last page worth asking for: every deeper
    /// page lies before it too. This is what keeps [`REPORT_MAX_PAGES`] a backstop rather than a
    /// cliff. The cap is not a window whose effect this adapter can see either, since no `limit` is
    /// sent and the venue's page size is its own default: "100 pages" is not "100 records", and an
    /// account whose funding history runs past the cap would otherwise fail this read on **every**
    /// pass, for ever, while this adapter had been wrong about nothing. Such a failure is honest -
    /// funding that cannot be read books nothing and is reported as such - but it is permanent, and
    /// it is a cashflow this adapter could have read in one page.
    ///
    /// Stopping early does not make the read incomplete, and that is the part to check rather than
    /// assume. The reconciliation counts exactly the payments at or after the window's start
    /// ([`crate::reconciliation::FundingLedger::accounted_since_baseline`]), and the walk returns
    /// every one of those: it stops only on a page that is *entirely* before the bound, having read
    /// every page between that one and the newest. The records it never asks for are records the
    /// judgment would not count. Nor is the stop load-bearing for correctness - were the venue to
    /// order a page against its documented order, a counted payment would go missing from the sum
    /// and the reconciliation would state the gap that leaves
    /// ([`FundingReconciliation::Unreconciled`]) rather than call the account clean.
    ///
    /// Only a page that **carries** records, all of them before the bound, ends the walk. An empty
    /// page says nothing about the order of what lies beyond it, so it is followed to the cursor it
    /// carried: reading "a page with no records" as "no records left" is the silent truncation this
    /// endpoint's `nextCursor` handling exists to avoid.
    async fn read_funding(&self, now: UnixNanos) -> anyhow::Result<Vec<FundingPayment>> {
        // Read before the first request, so no lock is held across the walk's awaits.
        let since = self
            .reconciliation
            .read()
            .funding()
            .baseline()
            .map_or(now, |(since, _baseline)| since);

        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();
        let mut payments = Vec::new();

        loop {
            let response = self
                .http_client
                .get_funding_fees(&query)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("the funding history could not be read: {error}")
                })?;

            let mut page = 0_usize;
            let mut before_the_window = true;

            for fee in response.funding_fees()? {
                let payment = funding_payment(&fee)?;

                page += 1;

                if payment.time >= since {
                    before_the_window = false;
                }

                payments.push(payment);
            }

            if page > 0 && before_the_window {
                break;
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        Ok(payments)
    }

    /// Reads the account's balance summary.
    async fn read_balance(&self) -> anyhow::Result<BalanceReading> {
        let response = self
            .http_client
            .get_balance()
            .await
            .map_err(|error| anyhow::anyhow!("the balance could not be read: {error}"))?;

        let raw = response.raw_result().to_string();
        let value: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|error| anyhow::anyhow!("the balance is not a JSON object: {error}"))?;

        let Some(object) = value.as_object() else {
            anyhow::bail!("the balance is not a JSON object");
        };

        let member = |name: &str| -> anyhow::Result<Option<Decimal>> {
            match object.get(name).and_then(serde_json::Value::as_str) {
                Some(value) => Ok(Some(balance_member(value, name)?)),
                None => Ok(None),
            }
        };

        let mut unmapped = Vec::new();

        for (name, value) in object {
            if DOCUMENTED_BALANCE_MEMBERS.contains(&name.as_str()) {
                continue;
            }

            // A member outside the documented set is a second collateral asset, a loan, or
            // something this adapter has not been taught. It is kept verbatim and reported as
            // unsupported rather than folded into the USDC numbers (plan §6.4).
            unmapped.push((
                name.clone(),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), ToString::to_string),
            ));
        }

        // `underLiquidation` is the one documented member that is not a decimal string: the frozen
        // schema makes it a boolean. A member that is present and is not a boolean is a payload
        // this adapter does not understand, and an absent one stays absent - "the venue did not
        // say" and "the venue said no" are different facts about an account the venue is closing.
        let under_liquidation = match object.get("underLiquidation") {
            Some(value) => Some(value.as_bool().ok_or_else(|| {
                anyhow::anyhow!("the balance member `underLiquidation` is not a boolean")
            })?),
            None => None,
        };

        Ok(BalanceReading {
            wallet_balance: member("walletBalance")?,
            margin_balance: member("marginBalance")?,
            used_margin: member("usedMargin")?,
            available_margin: member("availableMargin")?,
            withdrawable_margin: member("withdrawableMargin")?,
            maintenance_margin_requirement: member("maintenanceMarginRequirement")?,
            under_liquidation,
            total_funding_payments: member("totalFundingPayments")?,
            unmapped,
            raw,
        })
    }

    /// Builds one order's reading, from the state the pass left the order in.
    ///
    /// The payload is the newest the pass saw for this order, but it is not necessarily the state
    /// the order is in: a report that arrived after it - from the stream, or from a later page -
    /// has moved the order on, and what the pass judges has to be the state it left behind rather
    /// than the state one of its inputs described. `status` and `venue_filled` therefore come from
    /// the order index whenever it holds the order, and from the payload only for an order this
    /// client does not track, where the payload is all there is to read.
    fn order_reading(&self, payload: &OndoApiOrder) -> OrderReading {
        let state = {
            let state = self.reporter.state.read();

            state
                .resolve_order(payload)
                .and_then(|client_order_id| state.orders.get(&client_order_id).cloned())
        };

        OrderReading {
            venue_order_id: payload.order_id().to_string(),
            client_order_id: state
                .as_ref()
                .map(|state| state.client_order_id.to_string())
                .or_else(|| payload.client_order_id().map(ToString::to_string)),
            market: payload.market().to_string(),
            status: state
                .as_ref()
                .map_or_else(|| payload.status().clone(), |state| state.status.clone()),
            venue_filled: state.as_ref().map_or_else(
                || {
                    payload
                        .filled_quantity()
                        .ok()
                        .map(|quantity| quantity.as_decimal())
                },
                |state| state.venue_filled.map(|quantity| quantity.as_decimal()),
            ),
            applied_filled: state.as_ref().map(|state| state.filled.as_decimal()),
            tracked: state.is_some(),
        }
    }

    /// Applies one `ApiOrder` payload: the create answer, a lookup answer, a cancel answer or a
    /// reconciliation read.
    ///
    /// This is the ingestion seam the private stream and Task 8's reconciliation will drive. It is
    /// idempotent: a payload that repeats state already applied and reported returns
    /// [`OndoOrderApplication::Unchanged`] and emits nothing, so an acknowledgement cannot be
    /// reported twice. A payload that acknowledges the order flushes the fills that arrived before
    /// it, oldest first, so a fill that preceded the ACK is still reported after it.
    pub fn apply_order(&self, payload: &OndoApiOrder) -> OndoOrderApplication {
        let result = self.reporter.apply_order(payload, Acceptance::Report, None);
        if matches!(result, OndoOrderApplication::Applied) {
            if let Some(guard) = &self.production {
                guard.activity();
            }
        }
        result
    }

    /// Applies one `ApiFill` payload: the one path a fill is ever counted from.
    ///
    /// # Errors
    ///
    /// See [`OndoReporter::apply_fill`].
    pub fn apply_fill(&self, fill: &OndoApiFill) -> anyhow::Result<OndoFillApplication> {
        let result = self.reporter.apply_fill(fill);
        if matches!(result, Ok(OndoFillApplication::Applied)) || result.is_err() {
            if let Some(guard) = &self.production {
                guard.activity();
            }
        }
        result
    }

    /// Applies a whole page of fills, returning what each one did.
    ///
    /// # Errors
    ///
    /// Returns the first fill's error; the fills before it have already been applied, which is
    /// what the dedup ledger makes safe to retry.
    pub fn apply_fills(&self, fills: &[OndoApiFill]) -> anyhow::Result<Vec<OndoFillApplication>> {
        fills.iter().map(|fill| self.apply_fill(fill)).collect()
    }
}

#[async_trait(?Send)]
impl ExecutionClient for OndoExecutionClient {
    /// Returns whether this client's own connection has been established.
    ///
    /// **This is not "the account is verified", and it is deliberately not moved by the private
    /// socket.** Two facts are separate here and are reported separately:
    ///
    /// - *what this flag means*: the client has been connected and not disconnected. It describes
    ///   the client as the engine routes to it, and the transport that carries a cancel or a query
    ///   is REST, which a private socket's failure does not touch. A private socket loss that also
    ///   reported the whole client down would say its risk-clearing paths were unusable at exactly
    ///   the moment they are needed.
    /// - *what stops new risk*: the account's admission decision, and only that. A private
    ///   connection ending moves it ([`OndoAccountRuntime::note_session_ended`] ->
    ///   `ReconciliationState::Disconnected`), and every submission is refused from there until a
    ///   recovery converges - checked again at the send point
    ///   ([`crate::http::client::OndoNewRiskGuard`]), so no queued command slips through.
    ///
    /// The private session's own state is [`Self::private_run_state`], and it is the one to read
    /// for "is the account session up". Nothing in the engine gates on this flag (it is read by
    /// `check_connected` and the connection-status report), so moving it would buy no safety and
    /// would cost a true statement about a still-usable transport.
    fn is_connected(&self) -> bool {
        self.core.is_connected()
            && self
                .account
                .production
                .as_ref()
                .is_none_or(|guard| !guard.remaining().is_zero())
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

    /// Returns `false`: this phase has no bulk position report (plan §6.4 is Task 8's).
    ///
    /// The default `true` would make an absent position report evidence that a position is flat,
    /// which is exactly the false-clean judgement plan §6.3 forbids.
    fn provides_bulk_position_coverage(&self, _instrument_id: InstrumentId) -> bool {
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
        self.reporter
            .emitter
            .emit_account_state(balances, margins, reported, ts_event, info);

        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.reporter.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();

        log::info!(
            "Started Ondo Perps execution client: client_id={}, account_id={}, venue={}, \
             environment={:?}",
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            self.config.environment,
        );

        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        if self
            .last_shutdown
            .as_ref()
            .is_some_and(ShutdownRecord::is_complete)
            && self.private_stream.is_none()
            && self.tasks.all_finished()
        {
            self.core.set_stopped();
            self.core.set_disconnected();
            return Ok(());
        }

        log::info!("Stopping Ondo Perps execution client");

        self.read_only_diagnostics
            .set_shutdown(OwnedShutdownStatus::Stopping);

        let now = self.account.now();

        if let Some(guard) = &self.account.production {
            guard.stop_creates();
        }
        self.account.note_session_ended(now);

        // The ordered stop is asynchronous and this hook is not. When the transport is still here
        // the ordered cleanup is still owed: register and checkpoint every owned order so the work
        // survives, and leave the switch armed. The asynchronous [`Self::disconnect`] is what
        // drains the request tasks and carries out the bounded REST cleanup; this hook records
        // that it is owed rather than reporting it done.
        if self.private_stream.is_some() {
            if !self.account.is_account_read_only() {
                self.register_owned_cancels(now);
            }

            self.account.persist_journal(now);
        }

        // Uncertainty is checkpointed before forcing requests to stop. The async hook retains
        // responsibility for joining the closed generation, including after a caller timeout.
        if self.tasks.is_open() || !self.tasks.all_finished() {
            self.tasks.abort();
        }

        // Keep the transport owner so a later async hook can join the forced cancellation
        if let Some(stream) = self.private_stream.as_mut() {
            stream.abort();
        }
        self.core.set_stopped();
        self.core.set_disconnected();

        if self.last_shutdown.is_none() {
            self.last_shutdown = Some(ShutdownRecord {
                report: None,
                tasks_drained: false,
            });
        }

        self.read_only_diagnostics
            .set_shutdown(OwnedShutdownStatus::Dirty);
        Ok(())
    }

    /// Reopens the request path after a [`Self::disconnect`] drained the generation.
    ///
    /// The task generation the shutdown closed is what the submission and cancel paths spawn on,
    /// so resetting the client without reopening it would leave it connected but unable to send.
    /// The generation must have reached `Drained` - the asynchronous drain is what does that - so a
    /// synchronous `stop` that only closed admission does not by itself permit a reset.
    ///
    /// # Errors
    ///
    /// Returns an error when the previous generation has not finished, which is the task group's
    /// own refusal to run two generations at once.
    fn reset(&mut self) -> anyhow::Result<()> {
        if self.account.production.is_some() {
            anyhow::bail!("a production run cannot be reset or reused; create a new approved run");
        }
        if !self.tasks.is_open() {
            self.tasks
                .start_generation()
                .map_err(|error| anyhow::anyhow!("Ondo Perps task generation: {error}"))?;
        }

        self.last_shutdown = None;

        Ok(())
    }

    /// Marks the client connected and starts the private transport.
    ///
    /// The socket the client opens here is the **private** one: the account's reports and its dead
    /// man's switch are the account session, and this is what begins one. It is also the caller the
    /// recovery was waiting for - the transport's own connect hook calls [`Self::begin_recovery`],
    /// because a recovery is a decision to read the account and the transport is what makes that
    /// decision (plan §6.4).
    ///
    /// Before any of that, the authenticated account's identity is verified when the configuration
    /// sets [`OndoExecutionClientConfig::expected_venue_account_id`]. A mismatch refuses the
    /// connection here, before [`ExecutionClientCore::set_connected`], so a client that named the
    /// wrong account is never reported as connected. An answer that carries no comparable
    /// identifier is recorded as `unknown`, not as a match.
    ///
    /// # Errors
    ///
    /// Returns an error if the authenticated account does not match the configured venue account
    /// id, or if the transport cannot be started: no Tokio runtime to host it, or a reconnect
    /// policy that cannot be built. A client whose transport will not start is a client that cannot
    /// read its account, so the failure is reported rather than swallowed - it is refused before
    /// the connection state moves, so a failed start does not leave a client that looks connected.
    async fn connect(&mut self) -> anyhow::Result<()> {
        let readiness_account_events = self.account.account_state_published();
        self.verify_account_identity().await?;
        if let Some(guard) = &self.account.production {
            if self.account.account_identity() != OndoAccountIdentity::Matched {
                anyhow::bail!("production identity is unverified");
            }
            let response = self.http_client.get_markets().await?;
            self.account.refresh_production_metadata(&response).await?;
            self.account.reconcile_account(self.account.now()).await?;
            let machine = self.reconciliation.read();
            let reading = machine
                .last_reading()
                .ok_or_else(|| anyhow::anyhow!("production start account unread"))?;
            if reading
                .balance
                .as_ref()
                .and_then(|b| b.available_margin)
                .is_none_or(|v| v < guard.envelope().min_available_margin_usdc)
                || reading.positions.iter().any(|p| p.signed != Decimal::ZERO)
                || reading.orders.iter().any(|o| !o.status.is_terminal())
                || !self.reporter.state.read().orders.is_empty()
                || machine.last_judgment().is_none_or(|j| !j.is_clean())
                || !guard.permits_dms()
            {
                anyhow::bail!("production requires a fresh, flat, unoccupied whole account");
            }
        }

        if self.private_stream.is_none() {
            let stream = OndoPrivateStream::start(
                self.config.ws_url().to_string(),
                self.account.clone(),
                Arc::clone(&self.credential),
                self.config.stream_mode(),
                crate::common::consts::ONDO_WS_HEARTBEAT_SECS,
            )?;

            log::info!(
                "Ondo Perps private transport started: url={}, mode={}",
                stream.url(),
                self.config.stream_mode(),
            );

            self.read_only_diagnostics
                .attach_stream(stream.run_handle(), stream.diagnostics());
            self.private_stream = Some(stream);
        }

        if self.core.is_connected() {
            return Ok(());
        }

        if let Some(guard) = &self.account.production {
            tokio::time::timeout(guard.entry_remaining(), async {
                while guard.snapshot().is_none() {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("production start readiness was not verified"))?;
        }
        if self.config.environment == crate::common::enums::OndoEnvironment::Production
            && self.config.account_read_only
        {
            let ready = tokio::time::timeout(
                Duration::from_secs(production_readonly_readiness_timeout_secs(
                    self.config.http_timeout_secs,
                )),
                async {
                    loop {
                        let snapshot = self.read_only_diagnostics.snapshot();
                        if snapshot.logged_in
                            && snapshot.subscriptions_acked.contains(&"ordersPerps")
                            && snapshot.subscriptions_acked.contains(&"fillsPerps")
                            && snapshot.account_state_events > readiness_account_events
                            && matches!(snapshot.run_state, "recovering" | "read_only_synced")
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                },
            )
            .await;
            if ready.is_err() {
                let snapshot = self.read_only_diagnostics.snapshot();
                let category = if !snapshot.logged_in {
                    "private_login_not_acknowledged"
                } else if snapshot.subscriptions_acked.len() != 2 {
                    "private_subscriptions_not_acknowledged"
                } else {
                    "private_account_reconciliation_incomplete"
                };
                self.account.note_session_ended(self.account.now());
                self.read_only_diagnostics
                    .set_shutdown(OwnedShutdownStatus::Dirty);
                let _ = self.disconnect().await;
                anyhow::bail!("ondo_readonly_readiness:{category}");
            }
        }
        self.core.set_connected();
        log::info!("Ondo Perps execution client connected");

        Ok(())
    }

    /// Marks the client disconnected, and the account unverified with it.
    ///
    /// The graceful shutdown path carries out plan §R3.3 in one bounded budget:
    ///
    /// 1. new risk stops;
    /// 2. every owned order is registered as an unconfirmed cancel and the ledger is checkpointed
    ///    **before** any request exists, so a slow first request cannot erase a later order;
    /// 3. request admission closes and the task generation is drained (graceful then forced), so an
    ///    in-flight POST cannot mutate the account after shutdown and a later `reset` can reopen;
    /// 4. this run's own working orders are cancelled by id and confirmed, the switch is released
    ///    only when every piece of work is settled and confirmed, and the transport is closed and
    ///    awaited.
    ///
    /// A session that ends leaves the venue state the session established unread: new risk stops
    /// until a recovery converges again (plan §6.4). Cancels and queries still travel.
    ///
    /// A shutdown that leaves anything outstanding returns `Err` naming it, after the cleanup and
    /// the checkpoint have run. A repeated call preserves the original dirty outcome rather than
    /// relabelling it as success because the socket is gone.
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        // A completed shutdown is idempotent: nothing is left to do and the record is preserved.
        if self
            .last_shutdown
            .as_ref()
            .is_some_and(ShutdownRecord::is_complete)
            && self.private_stream.is_none()
            && self.tasks.all_finished()
        {
            return Ok(());
        }

        // One budget for the whole shutdown: the request-task drain, the cancel requests, the
        // confirmations, the switch release and the transport close.
        let disconnect_timeout = self
            .account
            .production
            .as_ref()
            .map_or(ONDO_DISCONNECT_TIMEOUT, |guard| {
                guard.remaining().min(ONDO_PRODUCTION_DISCONNECT_TIMEOUT)
            });
        let budget = ShutdownBudget::new(disconnect_timeout);
        let now = self.account.now();

        // Any exit from here - including the node's own disconnect bound dropping this future -
        // still checkpoints the ledger.
        let _checkpoint = JournalCheckpoint(self.account.clone());

        // Step 1: new risk stops.
        if let Some(guard) = &self.account.production {
            guard.stop_creates();
        }
        self.account.note_session_ended(now);

        // Step 2: every owned working order is registered and checkpointed before any await.
        if !self.account.is_account_read_only() {
            self.register_owned_cancels(now);
        }

        self.account.persist_journal(self.account.now());

        // Step 3: close request admission and drain the generation. The graceful slice lets an
        // in-flight request finish and register its own outcome; the forced slice boundedly drops
        // what is still running. Either way the generation reaches `Drained` so `reset` can reopen
        // it.
        self.tasks.begin_shutdown();

        let graceful = budget.remaining().min(ONDO_SHUTDOWN_TASK_GRACEFUL);
        let abort = budget
            .remaining()
            .saturating_sub(graceful)
            .min(ONDO_SHUTDOWN_TASK_ABORT);
        let tasks_drained = {
            let mut guard = RequestDrainGuard {
                tasks: &self.tasks,
                completed: false,
            };
            let result = self.tasks.finish_shutdown(graceful, abort).await;
            guard.completed = true;
            result.is_ok()
        };

        if !tasks_drained {
            log::error!(
                "The Ondo request task generation did not drain within its bound; {} task(s) \
                 remain owned",
                self.tasks.len(),
            );
        }

        // Step 4: the ordered stop on what is left of the budget.
        let report = self.stop_and_wait_within(now, &budget).await;

        self.account.persist_journal(self.account.now());

        if !self.core.is_disconnected() {
            self.core.set_disconnected();
        }

        let record = ShutdownRecord {
            report: Some(report),
            tasks_drained,
        };
        let complete = record.is_complete();
        let error = (!complete).then(|| record.error());

        self.read_only_diagnostics.set_shutdown(if complete {
            OwnedShutdownStatus::Clean
        } else {
            OwnedShutdownStatus::Dirty
        });

        if let Some(guard) = &self.account.production {
            guard.finish(complete);
        }
        self.last_shutdown = Some(record);

        match error {
            Some(error) => {
                log::error!("{error}");

                Err(error)
            }
            None => {
                log::info!(
                    "Ondo Perps execution client disconnected: {} cancel(s) settled, switch \
                     released, tasks drained",
                    self.last_shutdown
                        .as_ref()
                        .and_then(|record| record.report.as_ref())
                        .map_or(0, |report| report.cancellations_issued),
                );

                Ok(())
            }
        }
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    /// Submits one order: `POST /v1/perps/orders` (plan §6.2).
    ///
    /// The command is validated first, so an order this adapter cannot express is denied before a
    /// request exists: `Initialized -> Denied` is a legal transition and `Submitted -> Denied` is
    /// not, which is why nothing is emitted until the command is known good.
    ///
    /// Admission is decided three times - here, again inside the task immediately before the
    /// request exists, and once more at the send point itself (plan §6.4). The first gate is what a
    /// strategy sees; the second is what makes the decision binding on the command, because a
    /// command can be admitted and then wait for a task slot or a scheduling turn; the third is
    /// what makes it binding on the *request*, because between the second gate and the wire there is
    /// a wait for the shared rate budget - the one wait this client does not control - and the
    /// account can stop admitting new risk inside it. `Submitted` is emitted at the second gate
    /// rather than here, so a command the account stopped admitting is denied from `Initialized` -
    /// the legal transition - instead of from a state it never reached; a command refused at the
    /// third gate has already been submitted, so it is rejected rather than denied, which is the
    /// terminal transition `Submitted` does have.
    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        // Plan §6.4: new risk waits for a recovered account. A command refused here leaves no trace
        // at the venue, which is the point - the alternative is an order placed against an account
        // whose state is unknown.
        let permit = match self.admission() {
            Admission::Granted { generation } => generation,
            Admission::Refused { reason } => {
                self.deny_submission(&order, &reason);

                return Ok(());
            }
        };

        let command = match OndoOrderCommand::from_init(&cmd.order_init) {
            Ok(command) => command,
            Err(error) => {
                self.reporter
                    .emitter
                    .emit_order_denied(&order, &denial_reason(&error));

                return Ok(());
            }
        };

        if let Some(guard) = &self.account.production {
            let minimum = self
                .core
                .cache()
                .instrument(&cmd.instrument_id)
                .and_then(|i| i.min_notional());
            if minimum.is_some_and(|m| m.currency.code.as_str() != "USD") {
                self.reporter.emitter.emit_order_denied(
                    &order,
                    "venue minimum notional currency differs from the USD quote currency",
                );
                return Ok(());
            }
            if let Err(reason) = guard.prepare(
                cmd.client_order_id.to_string(),
                command.body(),
                self.core.cache().quote(&cmd.instrument_id).copied(),
                minimum.map(|m| m.as_decimal()),
            ) {
                self.reporter.emitter.emit_order_denied(&order, &reason);
                return Ok(());
            }
        }

        let Some(spawner) = self.spawner() else {
            self.reporter
                .emitter
                .emit_order_denied(&order, "the Ondo Perps execution client is shutting down");

            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let reconciliation = Arc::clone(&self.reconciliation);
        let journal = self.account.journal_handle();
        let account = self.account.clone();
        let client_order_id = cmd.client_order_id;
        let instrument_id = cmd.instrument_id;

        spawner.spawn(async move {
            // The instant this gate decides at is the instant it runs at, not the one the command
            // was admitted at: the wait between the two is the wait this gate exists to cover.
            let now = get_atomic_clock_realtime().get_time_ns();

            if let Admission::Refused { reason } = revalidate(
                &reconciliation,
                &Admission::Granted { generation: permit },
                now,
            ) {
                log::error!(
                    "Ondo refused to submit {client_order_id} at the request boundary: {}",
                    reason.reason(),
                );
                reporter
                    .emitter
                    .emit_order_denied(&order, &new_risk_refusal_reason(&reason));

                return;
            }

            reporter.track_submission(&command, client_order_id, instrument_id);
            reporter.emitter.emit_order_submitted(&order);

            match http_client
                .create_order(
                    &command,
                    OndoRequestPriority::Normal,
                    NewRiskPermit::new(permit),
                )
                .await
            {
                Ok(payload) => {
                    reporter.apply_order(&payload, Acceptance::Event, Some(&order));
                }
                Err(OndoNewRiskSendError::Refused { reason }) => {
                    // The account stopped admitting this order while it waited for the shared
                    // budget, so the request was never built and nothing rests at the venue. The
                    // order is not in flight and is not unknown: it is terminalised with the
                    // account's own reason, and no probe is owed for a request that never existed.
                    log::error!("Ondo refused order {client_order_id} at the send point: {reason}");
                    reporter
                        .emitter
                        .emit_order_rejected(&order, &reason, reporter.now(), false);
                    if let Some(guard) = &account.production {
                        guard.definite_zero(client_order_id.as_str());
                    }
                    reporter.forget(&client_order_id);
                    // The order never rested, so any cancel registered for it beforehand is moot.
                    reconciliation.write().confirm_cancel(&client_order_id);
                }
                Err(OndoNewRiskSendError::Local(error)) => {
                    // A single order has no batch-level validation to fail, so this arm is the
                    // batch's own refusal class reported the same way: nothing was sent.
                    log::error!("Ondo refused order {client_order_id} locally: {error}");
                    reporter.emitter.emit_order_rejected(
                        &order,
                        &format!("batch-order-error: {error}"),
                        reporter.now(),
                        false,
                    );
                    if let Some(guard) = &account.production {
                        guard.definite_zero(client_order_id.as_str());
                    }
                    reporter.forget(&client_order_id);
                    reconciliation.write().confirm_cancel(&client_order_id);
                }
                Err(OndoNewRiskSendError::Http(error)) if is_definitive_refusal(&error) => {
                    // The venue answered and refused, or the request never left this process.
                    // Either way the order is not resting, so it is rejected rather than left in
                    // flight.
                    log::warn!("Ondo refused order {client_order_id}: {error}");
                    reporter.emitter.emit_order_rejected(
                        &order,
                        &refusal_reason(&error),
                        reporter.now(),
                        is_post_only_refusal(&error),
                    );
                    if let Some(guard) = &account.production {
                        guard.definite_zero(client_order_id.as_str());
                    }
                    reporter.forget(&client_order_id);
                    reconciliation.write().confirm_cancel(&client_order_id);
                }
                Err(OndoNewRiskSendError::Http(error)) => {
                    // Plan §6.3: the request may have been applied. The order is neither accepted
                    // nor rejected, it is never resubmitted, and the bounded query window is what
                    // resolves it - under the same client order id, which is recorded here so a
                    // probe can find it and nothing can forget it. Recording it also stops new
                    // risk, immediately and without waiting for a pass.
                    log::error!(
                        "Ondo left the outcome of order {client_order_id} unknown ({error}); the \
                         order stays in flight and is not resubmitted"
                    );
                    reconciliation.write().note_unknown_submission(
                        client_order_id,
                        format!("the create request was not answered: {error}"),
                        reporter.now(),
                    );
                }
            }

            if let Some(guard) = &account.production {
                guard.activity();
                let _ = account.reconcile_account(account.now()).await;
            }

            // The submission's outcome is decided, so the order's association is checkpointed
            // **now** rather than at the next pass. It is what a restart needs to recognize the
            // order as this client's own: without it, an order placed moments before a crash would
            // come back as somebody else's, and the fills of it would be neither applied nor
            // reported (plan §R3.2). A refusal that never reached the venue has already dropped the
            // order from the index, so what is written is the index as it stands.
            if let Some(journal) = &journal {
                let state = reporter.state.read();
                let machine = reconciliation.read();

                journal.write(reporter.account_id, reporter.now(), &state, &machine);
            }
        })?;

        Ok(())
    }

    /// Submits a list as one native batch: `POST /v1/perps/orders/batch` (plan §6.2).
    ///
    /// A 2xx answer is **not** a success per item: the venue reports the added and the refused
    /// items separately and each is reported on its own. An item the venue refused without echoing
    /// a client order id is reported as unattributed rather than attributed to the wrong order.
    ///
    /// Admission is decided on exactly the same terms as a single order - the same decision, taken
    /// twice, for the same reason - and an item the venue's answer does not account for is
    /// registered as an unknown outcome rather than left as a log line (plan §6.3).
    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let journal = self.account.journal_handle();

        if cmd.order_inits.is_empty() {
            log::warn!("Cannot submit an empty order list");
            return Ok(());
        }

        // A batch is new risk too, so plan §6.4 stops it with the same decision a single order
        // meets. Every item is denied by name, and no batch request exists to have been applied.
        let permit = match self.admission() {
            Admission::Granted { generation } => generation,
            Admission::Refused { reason } => {
                log::error!(
                    "Ondo refused a {}-order batch: {}",
                    cmd.order_inits.len(),
                    reason.reason(),
                );

                for init in &cmd.order_inits {
                    if let Ok(order) = self.core.cache().try_order_owned(&init.client_order_id) {
                        self.reporter
                            .emitter
                            .emit_order_denied(&order, &new_risk_refusal_reason(&reason));
                    }
                }

                return Ok(());
            }
        };

        let mut commands = Vec::with_capacity(cmd.order_inits.len());
        let mut orders = Vec::with_capacity(cmd.order_inits.len());
        let mut instrument_ids = Vec::with_capacity(cmd.order_inits.len());

        {
            let cache = self.core.cache();

            for init in &cmd.order_inits {
                let order = cache.try_order_owned(&init.client_order_id)?;

                if order.is_closed() {
                    log::warn!("Cannot submit closed order {}", order.client_order_id());
                    return Ok(());
                }

                match OndoOrderCommand::from_init(init) {
                    Ok(command) => commands.push(command),
                    Err(error) => {
                        self.reporter
                            .emitter
                            .emit_order_denied(&order, &denial_reason(&error));

                        return Ok(());
                    }
                }

                orders.push(order);
                instrument_ids.push(init.instrument_id);
            }
        }

        let Some(spawner) = self.spawner() else {
            log::error!("The Ondo Perps execution client is shutting down");
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let reconciliation = Arc::clone(&self.reconciliation);
        let client_order_ids: Vec<ClientOrderId> = commands
            .iter()
            .map(|command| command.client_order_id)
            .collect();

        spawner.spawn(async move {
            let now = get_atomic_clock_realtime().get_time_ns();

            if let Admission::Refused { reason } = revalidate(
                &reconciliation,
                &Admission::Granted { generation: permit },
                now,
            ) {
                log::error!(
                    "Ondo refused a {}-order batch at the request boundary: {}",
                    orders.len(),
                    reason.reason(),
                );

                for order in &orders {
                    reporter
                        .emitter
                        .emit_order_denied(order, &new_risk_refusal_reason(&reason));
                }

                return;
            }

            for (command, instrument_id) in commands.iter().zip(instrument_ids.iter()) {
                reporter.track_submission(command, command.client_order_id, *instrument_id);
            }

            for order in &orders {
                reporter.emitter.emit_order_submitted(order);
            }

            let response = match http_client
                .create_orders_batch(
                    &commands,
                    OndoRequestPriority::Normal,
                    NewRiskPermit::new(permit),
                )
                .await
            {
                Ok(response) => response,
                Err(OndoNewRiskSendError::Refused { reason }) => {
                    // The account stopped admitting the list while it waited for the shared budget,
                    // so no item of it became a request. Every item is terminalised with the
                    // account's own reason, and none of them is an unknown outcome.
                    log::error!(
                        "Ondo refused a {}-order batch at the send point: {reason}",
                        orders.len(),
                    );

                    for order in &orders {
                        reporter
                            .emitter
                            .emit_order_rejected(order, &reason, reporter.now(), false);
                        reporter.forget(&order.client_order_id());
                        reconciliation
                            .write()
                            .confirm_cancel(&order.client_order_id());
                    }

                    return;
                }
                Err(OndoNewRiskSendError::Local(error)) => {
                    // The batch never became a request, so no item can be resting: this is the one
                    // batch failure that is definitively not an unknown outcome.
                    log::error!("Ondo refused the batch locally: {error}");

                    for order in &orders {
                        reporter.emitter.emit_order_rejected(
                            order,
                            &format!("batch-order-error: {error}"),
                            reporter.now(),
                            false,
                        );
                        reporter.forget(&order.client_order_id());
                        reconciliation
                            .write()
                            .confirm_cancel(&order.client_order_id());
                    }

                    return;
                }
                Err(OndoNewRiskSendError::Http(error)) => {
                    // The request was made and its answer could not be used, so **every** item's
                    // outcome is unknown: the venue may have applied all of them, some of them or
                    // none. Nothing is resubmitted, and each item keeps its own client order id so
                    // a probe can ask about it (plan §6.3).
                    log::error!(
                        "Ondo left the outcome of a {}-order batch unknown ({error}); the orders \
                         stay in flight and are not resubmitted",
                        client_order_ids.len(),
                    );

                    for client_order_id in &client_order_ids {
                        reconciliation.write().note_unknown_submission(
                            *client_order_id,
                            format!("the batch request was not answered: {error}"),
                            reporter.now(),
                        );
                    }

                    return;
                }
            };

            let mut reported: Vec<ClientOrderId> = Vec::new();

            for payload in response.added() {
                let order = order_for(&orders, payload);

                if order.is_none() {
                    // The venue added an order that names none of this client's submissions. It is
                    // applied - it is a real order the venue stated - but it accounts for none of
                    // them, so it does not settle the item that was sent.
                    log::error!(
                        "Ondo's batch answer added the order {} (client order id `{}`), which \
                         names no order this client submitted; it is applied without attributing \
                         any item to it",
                        payload.order_id(),
                        payload.client_order_id().unwrap_or("none"),
                    );
                }

                reporter.apply_order(payload, Acceptance::Event, order);

                if let Some(order) = order {
                    reported.push(order.client_order_id());
                }
            }

            for refused in response.failed() {
                report_refused_item(&reporter, &reconciliation, &orders, refused, &mut reported);
            }

            for client_order_id in &client_order_ids {
                if reported.contains(client_order_id) {
                    continue;
                }

                // A 2xx that names neither an added nor a refused order for an item leaves that
                // item's outcome unknown; it is not terminalised here, and it is registered so the
                // probe can find it and the account stops admitting new risk (plan §6.3).
                log::error!(
                    "Ondo's batch answer named neither an added nor a refused order for \
                     {client_order_id}; it stays in flight"
                );
                reconciliation.write().note_unknown_submission(
                    *client_order_id,
                    "the batch answer named neither an added nor a refused order for it"
                        .to_string(),
                    reporter.now(),
                );
            }

            // The batch's outcome is decided for every item it names, so the order associations go
            // to the journal now, exactly as a single submission's do (plan §R3.2).
            if let Some(journal) = &journal {
                let state = reporter.state.read();
                let machine = reconciliation.read();

                journal.write(reporter.account_id, reporter.now(), &state, &machine);
            }
        })?;

        Ok(())
    }

    /// Rejects a modification: this venue has no atomic amend endpoint (plan §6.2).
    ///
    /// Emulating one as a cancel plus a new order would produce a different order with a different
    /// venue order id, so it is refused by name rather than reported as a modification that
    /// happened.
    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        log::warn!(
            "Ondo Perps has no atomic amend endpoint, so order modification is not supported \
             (client_order_id={})",
            cmd.client_order_id,
        );

        if let Ok(order) = self.core.cache().try_order_owned(&cmd.client_order_id) {
            self.reporter.emitter.emit_order_modify_rejected(
                &order,
                cmd.venue_order_id,
                "Ondo Perps has no atomic amend endpoint, so a modification cannot be expressed; \
                 a cancel and resubmit is a different order",
                self.reporter.now(),
            );
        }

        Ok(())
    }

    /// Cancels one order: `DELETE /v1/perps/orders/{orderID}` (plan §6.2).
    ///
    /// A refusal whose code [`OndoCancelRejection::requires_query`] triggers a confirming query:
    /// the cancel API having been called is not a terminal state, and a write its own state
    /// machine calls unresolvable is exactly the case the query exists for (plan §6.3).
    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order_ref = self.order_ref(&cmd.client_order_id, cmd.venue_order_id);

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let reconciliation = Arc::clone(&self.reconciliation);
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        let instrument_id = cmd.instrument_id;
        let strategy_id = cmd.strategy_id;

        spawner.spawn(async move {
            match http_client
                .cancel_order(&order_ref, OndoRequestPriority::High)
                .await
            {
                Ok(OndoCancelAnswer::Order(order)) => {
                    reporter.apply_order(&order, Acceptance::Report, None);
                }
                Ok(OndoCancelAnswer::Unconfirmed { raw }) => {
                    // Plan §6.3: the cancel API being called is not the order being cancelled. The
                    // cancel is registered **before** the query that would settle it, so a query
                    // that cannot be answered leaves it outstanding rather than leaving nothing
                    // behind at all.
                    log::warn!(
                        "Ondo accepted the cancel of {client_order_id} without reporting the \
                         order, so its state is confirmed by a query ({raw})"
                    );
                    reconciliation.write().note_unconfirmed_cancel(
                        client_order_id,
                        venue_order_id,
                        format!("the cancel was accepted without an order payload ({raw})"),
                        reporter.now(),
                    );

                    confirm_cancel(&http_client, &reporter, &reconciliation, &client_order_id)
                        .await;
                }
                Err(error) if is_definitive_refusal(&error) => {
                    let rejection = cancel_rejection(&error);

                    log::warn!("Ondo refused the cancel of {client_order_id}: {error}");

                    if rejection.requires_query() {
                        // The venue's own state machine cannot say whether this order is
                        // cancelable, which is exactly the case the query exists for - and until
                        // it answers, the cancel is outstanding.
                        reconciliation.write().note_unconfirmed_cancel(
                            client_order_id,
                            venue_order_id,
                            format!(
                                "the cancel was refused with a code that requires a query: {error}"
                            ),
                            reporter.now(),
                        );

                        confirm_cancel(&http_client, &reporter, &reconciliation, &client_order_id)
                            .await;
                    } else {
                        reporter.emitter.emit_order_cancel_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            venue_order_id,
                            &format!("cancel-order-error: {error}"),
                            reporter.now(),
                        );
                    }
                }
                Err(error) => {
                    // Ambiguous: the venue may still act on it. A cancel API call is not a cancel,
                    // so the order's state is confirmed by a query - and until that query answers,
                    // the cancel is outstanding and this client is not ready (plan §6.3).
                    log::warn!(
                        "Ambiguous Ondo cancel failure for {client_order_id}; its state is \
                         confirmed by a query: {error}"
                    );
                    reconciliation.write().note_unconfirmed_cancel(
                        client_order_id,
                        venue_order_id,
                        format!("the cancel request was not answered: {error}"),
                        reporter.now(),
                    );

                    confirm_cancel(&http_client, &reporter, &reconciliation, &client_order_id)
                        .await;
                }
            }
        })?;

        Ok(())
    }

    /// Cancels every order on one market: `DELETE /v1/perps/orders?market=...` (plan §6.2).
    ///
    /// The venue's market cancel takes no side filter, so a side-filtered command is **not sent**:
    /// cancelling the whole market would take out the opposite side's resting orders, which is a
    /// different command from the one the strategy issued.
    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let market = match instrument_id_to_market(&cmd.instrument_id) {
            Ok(market) => market,
            Err(error) => {
                log::error!(
                    "Cannot cancel all orders for {}: {error}",
                    cmd.instrument_id
                );
                return Ok(());
            }
        };

        if let Some(order_side) = cmd.order_side {
            log::error!(
                "Ondo's market cancel takes no side filter, so a {order_side:?}-only cancel-all \
                 for {market} is not sent; cancel the orders individually instead"
            );

            return Ok(());
        }

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let reconciliation = Arc::clone(&self.reconciliation);

        spawner.spawn(async move {
            // The orders a market cancel speaks for are the ones this client is tracking on that
            // market. They are collected before the request so the ambiguous paths below have the
            // set to register.
            match http_client
                .cancel_market_orders(&market, OndoRequestPriority::High)
                .await
            {
                Ok(OndoCancelAnswer::Order(order)) => {
                    reporter.apply_order(&order, Acceptance::Report, None);
                }
                Ok(OndoCancelAnswer::Unconfirmed { raw }) => {
                    // The documented 200 of this endpoint carries no order (plan §6.3): the cancel
                    // API having answered is not the orders having been cancelled, so the state of
                    // every order on the market is confirmed from the venue's own list. They are
                    // registered first, so a confirming read that cannot be answered leaves them
                    // outstanding rather than leaving nothing behind.
                    log::warn!(
                        "Ondo accepted the market cancel for {market} without reporting an order, \
                         so the orders' states are confirmed by a query ({raw})"
                    );

                    register_market_cancel(
                        &reporter,
                        &reconciliation,
                        &market,
                        &format!(
                            "the market cancel for {market} was accepted without an order \
                                 payload ({raw})"
                        ),
                    );

                    if let Err(error) =
                        confirm_market_cancel(&http_client, &reporter, &reconciliation, &market)
                            .await
                    {
                        log::warn!(
                            "The confirming read after the Ondo market cancel for {market} failed: \
                             {error}; the orders' states stay unconfirmed"
                        );
                    }
                }
                Err(error) if is_definitive_refusal(&error) => {
                    // A refusal is definitive: the orders were not cancelled, which is the state
                    // this client already holds.
                    log::warn!("Ondo market cancel failed for {market}: {error}");
                }
                Err(error) => {
                    // Ambiguous: the venue may still act on it. A cancel API call is not a cancel,
                    // so the orders' states are confirmed by a query - and until that query
                    // answers, the cancels are outstanding (plan §6.3).
                    log::warn!(
                        "Ambiguous Ondo market cancel failure for {market}; the orders' states are \
                         confirmed by a query: {error}"
                    );

                    register_market_cancel(
                        &reporter,
                        &reconciliation,
                        &market,
                        &format!("the market cancel for {market} was not answered: {error}"),
                    );

                    if let Err(error) =
                        confirm_market_cancel(&http_client, &reporter, &reconciliation, &market)
                            .await
                    {
                        log::warn!(
                            "The confirming read after the Ondo market cancel for {market} failed: \
                             {error}; the orders' states stay unconfirmed"
                        );
                    }
                }
            }
        })?;

        Ok(())
    }

    /// Queries one order and reports it (plan §6.3).
    ///
    /// The query is high priority (§4.4): it is the traffic that resolves an unknown outcome, and
    /// it must not queue behind a metadata read.
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let order_ref = self.order_ref(&cmd.client_order_id, cmd.venue_order_id);

        let Some(spawner) = self.spawner() else {
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let client_order_id = cmd.client_order_id;

        spawner.spawn(async move {
            match http_client
                .get_order(&order_ref, OndoRequestPriority::High)
                .await
            {
                Ok(order) => {
                    reporter.apply_order(&order, Acceptance::Report, None);
                }
                Err(error) => log::warn!("Ondo order query failed for {client_order_id}: {error}"),
            }
        })?;

        Ok(())
    }

    /// Reads one order and returns its status report (plan §6.3).
    ///
    /// Either identifier is enough to ask with, which is what the venue documents: its single-order
    /// read takes one path parameter, *"Internal order ID, or `client:{clientOrderID}` for client
    /// order ID lookup"*. A venue order id therefore goes into the path as itself - the `client:`
    /// form is this adapter's convention for a client order id query (plan §6.2) and means
    /// something else entirely when wrapped around a venue id.
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let order_ref = match (cmd.client_order_id, cmd.venue_order_id) {
            (Some(client_order_id), venue_order_id) => {
                self.order_ref(&client_order_id, venue_order_id)
            }
            (None, Some(venue_order_id)) => venue_order_id.to_string(),
            (None, None) => anyhow::bail!(
                "An Ondo Perps order status report needs an order to read: neither a client order \
                 id nor a venue order id was given"
            ),
        };

        let payload = self
            .http_client
            .get_order(&order_ref, OndoRequestPriority::High)
            .await?;

        if let OndoOrderApplication::Unresolved { reason, .. } = self.apply_order(&payload) {
            // The identifier the caller gave, so the error names the order they asked about; a
            // venue-id-only read has only the venue's own id to name it by.
            let named = match cmd.client_order_id {
                Some(client_order_id) => client_order_id.to_string(),
                None => payload.order_id().to_string(),
            };

            anyhow::bail!("Ondo order {named} is unresolved: {reason}");
        }

        // The payload was applied a moment ago, so the index holds this read's own effect: an order
        // this client tracks is reported from its ledger, and an order it does not track is
        // reported from the venue's view of it. `status_report` keeps the payload's `clientOrderId`
        // in that second case, which for an order this client never placed is the only identity
        // there is ([`Self::generate_order_status_reports`] does the same, per plan §6.4).
        let state = self.reporter.state.read();

        let tracked = state
            .resolve_order(&payload)
            .and_then(|client_order_id| state.orders.get(&client_order_id));

        Ok(Some(self.reporter.status_report(
            &payload,
            tracked,
            event_time(&payload, self.reporter.now()),
        )?))
    }

    /// Reads the order history and returns one status report per order.
    ///
    /// Orders this client did not place are reported too, without a client order id: plan §6.4
    /// requires an order created by another strategy or by hand to be identified rather than
    /// dropped. An order whose status this adapter cannot resolve is **not** reported as a status -
    /// a status report would state a state the adapter has not confirmed.
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut reports = Vec::new();
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();

        if let Some(instrument_id) = cmd.instrument_id {
            query = query.with_market(instrument_id_to_market(&instrument_id)?);
        }

        // `open_only` is the venue's own filter, sent as the venue's own word for it: the spec's
        // `status` enum on this endpoint is `open`, `canceled`, `fullyfilled`, so a working order
        // is `open` at the venue and this is a mapping rather than an approximation. What it does
        // approximate is the other direction: that enum has no `pending` and no `untriggered`, so
        // **no** filter value asks for "every non-terminal order" and an order in one of those
        // states is not reachable through this read (`test_data/conflicts.md`).
        if cmd.open_only {
            query = query.with_status(OndoOrderHistoryStatus::Open);
        }

        // The window is sent as the venue declares it - whole milliseconds of UTC - and the result
        // is deliberately **not** trimmed here as well. Which of the order's own timestamps the
        // venue's window filters on is not something this adapter can observe, and re-applying the
        // window locally against a different one would drop orders the venue meant to return.
        if let Some(start) = cmd.start {
            query = query.with_start_time(start);
        }

        if let Some(end) = cmd.end {
            query = query.with_end_time(end);
        }

        loop {
            // The typed reader fixes its own priority at `Normal`; a reconciliation read goes
            // through the signed seam so §4.4's traffic class is the caller's to choose.
            let response = self
                .http_client
                .get_signed(&query.target(ORDERS_PATH), OndoRequestPriority::High)
                .await
                .map_err(|error| anyhow::anyhow!("Ondo order history read failed: {error}"))?;

            for item in response.items()? {
                let payload = OndoApiOrder::from_text(item.get())?;

                // The read asked the venue for `open` orders only. One that comes back in another
                // status is a venue that did not apply the filter, and it is named here and then
                // reported like any other order: the filter was the request, not a licence to drop
                // what the answer contains.
                if cmd.open_only && payload.status() != &OndoOrderStatus::Open {
                    log::error!(
                        "Ondo order {} came back from a history read filtered to `open` with the \
                         status `{}`: the venue did not apply the filter, and the order is \
                         reported rather than dropped",
                        payload.order_id(),
                        payload.status().as_str(),
                    );
                }

                match self.apply_order(&payload) {
                    OndoOrderApplication::Unresolved { reason, .. } => {
                        log::error!(
                            "Ondo order {} is unresolved and is not reported as a status: {reason}",
                            payload.order_id(),
                        );
                        continue;
                    }
                    OndoOrderApplication::UnmappableMarket { market, .. } => {
                        log::error!(
                            "Ondo order {} names the unmappable market `{market}` and is not \
                             reported",
                            payload.order_id(),
                        );
                        continue;
                    }
                    _ => {}
                }

                let ts_event = event_time(&payload, self.reporter.now());

                // A report of an order this client tracks is built from its **ledger**, exactly as
                // the ingest path builds it ([`OndoReporter::emit_status_report`]). Reporting the
                // venue's own view here instead would have one order answered two ways by one
                // adapter: a `fullyfilled` payload whose applied fills have not arrived yet is
                // `Accepted` through ingest and would be `Filled` here - an assertion of a
                // completion the ledger cannot support, which leaves the engine to synthesise an
                // inferred fill for the difference. The lookup is by resolution, not by the
                // payload's own client order id, so an order this session knows only by its venue
                // order id is found too; an order this client does not track resolves to [`None`]
                // and is reported from the venue's own view, which is all an external order has.
                //
                // The read guard is held across the call, as the single-order read holds it: the
                // order was applied a moment ago, so the index is the reading to report, and
                // `status_report` takes no lock of its own.
                let state = self.reporter.state.read();

                let tracked = state
                    .resolve_order(&payload)
                    .and_then(|client_order_id| state.orders.get(&client_order_id));

                reports.push(self.reporter.status_report(&payload, tracked, ts_event)?);
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        Ok(reports)
    }

    /// Reads the fill history and returns fill reports.
    ///
    /// The page is deduped by trade id within the call: a paginated repeat of one fill is not two
    /// fills. The client's own dedup ledger is deliberately not consulted, because a reconciliation
    /// read re-derives state rather than counting increments, and the engine dedupes reports by
    /// trade id on its side.
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        // A fill report's client order id is optional, so an order this session has not seen is
        // reported without one rather than under an invented id.
        let client_order_id = cmd.venue_order_id.as_ref().and_then(|venue_order_id| {
            self.reporter
                .state
                .read()
                .client_order_id_for(venue_order_id)
        });

        let mut reports = Vec::new();
        let mut seen: ahash::AHashSet<String> = ahash::AHashSet::new();
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();

        if let Some(instrument_id) = cmd.instrument_id {
            query = query.with_market(instrument_id_to_market(&instrument_id)?);
        }

        loop {
            let response = self
                .http_client
                .get_signed(&query.target(FILLS_PATH), OndoRequestPriority::High)
                .await
                .map_err(|error| anyhow::anyhow!("Ondo fill history read failed: {error}"))?;

            for fill in response.fills()? {
                if !seen.insert(fill.id().to_string()) {
                    continue;
                }

                if let Some(venue_order_id) = cmd.venue_order_id.as_ref()
                    && fill.order_id() != venue_order_id.as_str()
                {
                    continue;
                }

                // The venue's fill history documents a `startTime`/`endTime` window (UTC
                // milliseconds, both optional) and this read deliberately does not send it: the
                // command's window is applied here instead, on the fill's own timestamp. That is
                // the safer of the two filters, not a leftover. A window sent to the venue is a
                // second filter this adapter cannot see the effect of, over a field and an
                // inclusive/exclusive convention the frozen spec states only in prose; applied
                // here, the boundaries are this adapter's own to define, a fill the venue would
                // have dropped at the edge still arrives and is judged, and the one case a
                // venue-side window could not report at all - a fill whose timestamp cannot be
                // read - is reported loudly below.
                match fill_timestamp(&fill) {
                    Some(ts_event) => {
                        if cmd.start.is_some_and(|start| ts_event < start)
                            || cmd.end.is_some_and(|end| ts_event > end)
                        {
                            continue;
                        }
                    }
                    // A fill whose `time` is absent or unreadable cannot be placed in the window,
                    // and dropping it because it fails a test it could not be measured against is
                    // how a bounded reconciliation read (§6.4) loses a fill and never squares its
                    // position. It is reported - [`OndoReporter::fill_report`] falls back to the
                    // local clock for its required `ts_event`, the same way [`event_time`] does -
                    // and it is named here, so an unreadable history arrives as a loud one rather
                    // than as a shorter one.
                    None => log::error!(
                        "Ondo fill {} carries no readable `time`, so the read's time window cannot \
                         be applied to it; it is reported rather than dropped",
                        fill.id(),
                    ),
                }

                let order_id =
                    client_order_id.or_else(|| fill.client_order_id().map(ClientOrderId::from));

                match self.reporter.fill_report(order_id, &fill) {
                    Ok(report) => reports.push(report),
                    Err(error) => {
                        log::error!("Ondo fill {} could not be reported: {error}", fill.id());
                    }
                }
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        Ok(reports)
    }

    /// Reads the account's open positions and reports each of them natively (plan §R3.2).
    ///
    /// The read is the venue's own `GET /v1/perps/positions`, which the frozen spec documents as
    /// returning **all** open positions for the authenticated account with no pagination and no
    /// parameters - the same read, and the same completeness contract, the reconciliation's
    /// position judgment is built on. The report's quantity is the venue's `netQuantity` with the
    /// direction carrying the sign, read through the one mapping that fixes that convention
    /// ([`crate::reconciliation::signed_position_quantity`]), and its average open price is the
    /// venue's `averageEntryPrice`.
    ///
    /// A row this adapter cannot name an instrument for is **not** reported and not skipped in
    /// silence: it is logged, and it is what keeps [`Self::provides_bulk_position_coverage`]
    /// answering `false` rather than letting the engine read an absent report as a flat account.
    ///
    /// # The report's timestamps
    ///
    /// A position payload carries no timestamp of its own, so `ts_last` is this read's instant.
    /// That is the same choice [`event_time`] makes where the venue sends none, and it is
    /// deliberately not a value read from the market: a position's "last update" is not something
    /// this endpoint states.
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let now = self.reporter.now();
        let readings = self.account.read_positions().await?;
        let mut reports = Vec::new();

        for reading in readings {
            let Some(instrument_id) = reading.instrument_id else {
                log::error!(
                    "Ondo position on the unmappable market `{}` is not reported: no instrument \
                     this adapter knows corresponds to it",
                    reading.market,
                );

                continue;
            };

            // A direction this adapter cannot read is a row it cannot report. The venue stated a
            // position on an instrument this adapter can name, and what the row does *not* state is
            // which way it points - so there is no side to report and no quantity to sign. This is
            // the row that makes a bulk coverage promise unsound, and it is named here rather than
            // guessed at.
            if let PositionDirection::Unknown(direction) = &reading.direction {
                log::error!(
                    "Ondo position on {} states the direction `{direction}`, which this adapter                      cannot read: it is not reported, and no bulk position coverage is promised                      while a row of the account's own position list can be unreadable",
                    reading.market,
                );

                continue;
            }

            if let Some(wanted) = cmd.instrument_id
                && wanted != instrument_id
            {
                continue;
            }

            // The venue's own size, unsigned, with the direction as the report's side. A
            // `neutral` direction is the venue stating flat, and it is reported as a zero
            // quantity rather than as an absence.
            let quantity = Quantity::from_decimal(reading.signed.abs())
                .map_err(|error| anyhow::anyhow!("position on {instrument_id}: {error}"))?;

            let position_side = match reading.signed.cmp(&Decimal::ZERO) {
                std::cmp::Ordering::Greater => PositionSide::Long,
                std::cmp::Ordering::Less => PositionSide::Short,
                std::cmp::Ordering::Equal => PositionSide::Flat,
            };

            reports.push(PositionStatusReport::new(
                self.core.account_id,
                instrument_id,
                position_side,
                quantity,
                now,
                now,
                None, // report_id
                None, // venue_position_id: this venue is netting
                reading.average_entry_price,
            ));
        }

        Ok(reports)
    }
}

/// Confirms an order's real state after a cancel that decided nothing (plan §6.3).
///
/// The reference is resolved at the moment of the query rather than fixed when the cancel was
/// issued: the cancel's own answer may have been what taught this session the venue order id, and
/// querying by it is unambiguous.
async fn confirm_cancel(
    http_client: &OndoHttpClient,
    reporter: &OndoReporter,
    reconciliation: &Arc<RwLock<ReconciliationMachine>>,
    client_order_id: &ClientOrderId,
) {
    let order_ref = reporter.order_ref(client_order_id);

    match http_client
        .get_order(&order_ref, OndoRequestPriority::High)
        .await
    {
        Ok(order) => {
            reporter.apply_order(&order, Acceptance::Report, None);
            // The venue's own answer settled it, whatever that answer was.
            reconciliation.write().confirm_cancel(client_order_id);
        }
        Err(error) => log::warn!(
            "The confirming query after a cancel failed for {client_order_id} ({order_ref}): \
             {error}; the order's state stays unconfirmed"
        ),
    }
}

/// Confirms the state of every order a market cancel may have taken (plan §6.3).
///
/// A market cancel answers for a whole market rather than for one order, so the confirming read is
/// the venue's own order list for that market - the same endpoint §6.4's bounded reconciliation
/// reads - and each payload it carries is applied through the path every other answer takes. An
/// order the venue still reports as working stays working; one it reports as ended ends. Each order
/// the read names is also settled as a cancel: the venue's own answer about it is what the query
/// was for.
///
/// An order the venue does not list is **not** concluded to be gone. Absent evidence is not
/// evidence: the list is paginated and none of the frozen material states what it omits, so nothing
/// is reported for a missing order and it keeps the state it had - including its outstanding cancel,
/// which is what keeps this client from trading on a state no answer has stated.
///
/// # Errors
///
/// Returns an error when the list cannot be read at all, including an unreadable cursor: a walk
/// that stopped early must not look like a market whose orders were all seen.
async fn confirm_market_cancel(
    http_client: &OndoHttpClient,
    reporter: &OndoReporter,
    reconciliation: &Arc<RwLock<ReconciliationMachine>>,
    market: &str,
) -> anyhow::Result<()> {
    let mut query = OndoPrivateReadQuery::new().with_market(market);
    let mut walk = CursorWalk::new(REPORT_MAX_PAGES);

    loop {
        let response = http_client
            .get_orders(&query)
            .await
            .map_err(|error| anyhow::anyhow!("the order list could not be read: {error}"))?;

        for item in response.items()? {
            match OndoApiOrder::from_text(item.get()) {
                Ok(payload) => {
                    reporter.apply_order(&payload, Acceptance::Report, None);

                    if let Some(client_order_id) = payload.client_order_id() {
                        reconciliation
                            .write()
                            .confirm_cancel(&ClientOrderId::from(client_order_id));
                    }
                }
                Err(error) => log::error!(
                    "An order in the confirming read for {market} could not be read and is not \
                     applied: {error}"
                ),
            }
        }

        let Some(cursor) = walk.advance(response.cursor())? else {
            break;
        };

        query = query.with_cursor(cursor);
    }

    Ok(())
}

/// Registers every order this client tracks on `market` as a cancel no answer has settled.
///
/// A market cancel speaks for a whole market, so the orders it may have taken are the ones this
/// client is tracking there. Registering them is what stops new risk until the venue's own list
/// says what became of each (plan §6.3).
fn register_market_cancel(
    reporter: &OndoReporter,
    reconciliation: &Arc<RwLock<ReconciliationMachine>>,
    market: &str,
    reason: &str,
) {
    let mut machine = reconciliation.write();

    for (client_order_id, venue_order_id) in reporter.tracked_orders_on(market) {
        machine.note_unconfirmed_cancel(
            client_order_id,
            venue_order_id,
            reason.to_string(),
            reporter.now(),
        );
    }
}

/// Takes the admission decision under the same lock that reads it.
fn revalidate(
    reconciliation: &Arc<RwLock<ReconciliationMachine>>,
    permit: &Admission,
    now: UnixNanos,
) -> Admission {
    reconciliation.read().revalidate(permit, now)
}

/// Renders a refusal as the reason an order was denied.
fn new_risk_refusal_reason(refusal: &NewRiskRefusal) -> String {
    format!("order-denied: reconciliation: {}", refusal.reason())
}

/// This run's answer to the send point's question: does the account still admit new risk?
///
/// It holds the state machine rather than any copy of its verdict, because a verdict is only worth
/// something at the moment it is asked for. The transport asks *after* the request has waited for
/// the shared budget - which is after every decision this client could have taken - so the answer
/// has to be read from the account as it stands then, and it has to be read as a re-check of the
/// permit the command was admitted under rather than as a fresh question with a fresh answer
/// ([`ReconciliationMachine::revalidate`]).
#[derive(Debug)]
struct RunAdmission {
    reconciliation: Arc<RwLock<ReconciliationMachine>>,
}

impl RunAdmission {
    fn new(reconciliation: Arc<RwLock<ReconciliationMachine>>) -> Self {
        Self { reconciliation }
    }
}

impl OndoNewRiskGuard for RunAdmission {
    fn revalidate(&self, permit: NewRiskPermit) -> Result<(), String> {
        let permit = Admission::Granted {
            generation: permit.generation(),
        };
        // The send point decides at the send point: the question is whether the account admits new
        // risk now, and a deadline that passed while the request waited for the shared budget is
        // exactly what this gate is here to catch.
        let now = get_atomic_clock_realtime().get_time_ns();

        match self.reconciliation.read().revalidate(&permit, now) {
            Admission::Granted { .. } => Ok(()),
            Admission::Refused { reason } => Err(new_risk_refusal_reason(&reason)),
        }
    }
}

/// Reports one refused batch item against the order it belongs to.
///
/// The attribution is strictly by the client order id the venue echoed. An item that carries none
/// is reported as unattributed rather than matched to whichever order shares its market: naming the
/// wrong order would put a real order into a state that never happened.
fn report_refused_item(
    reporter: &OndoReporter,
    reconciliation: &Arc<RwLock<ReconciliationMachine>>,
    orders: &[OrderAny],
    refused: &OndoRejectedOrder,
    reported: &mut Vec<ClientOrderId>,
) {
    let Some(order) = refused.client_order_id().and_then(|client_order_id| {
        orders
            .iter()
            .find(|order| order.client_order_id().as_str() == client_order_id)
            .cloned()
    }) else {
        log::error!(
            "Ondo refused a batch item that names no order this client submitted (error code \
             {:?}); it is kept raw rather than attributed to the wrong order: {}",
            refused.error_code(),
            refused.raw(),
        );

        return;
    };

    let reason = match (refused.error_code(), refused.error()) {
        (Some(code), Some(message)) => format!("batch-order-error: {code}: {message}"),
        (Some(code), None) => format!("batch-order-error: {code}"),
        (None, Some(message)) => format!("batch-order-error: {message}"),
        (None, None) => "batch-order-error: the venue refused the item".to_string(),
    };

    reporter.emitter.emit_order_rejected(
        &order,
        &reason,
        reporter.now(),
        refused.error_code() == Some(ONDO_POST_ONLY_HAS_MATCH),
    );

    // The venue refused this item, so it never rested: leaving it in the index would keep an order
    // in flight that does not exist. Any cancel registered for it beforehand is moot for the same
    // reason.
    reporter.forget(&order.client_order_id());
    reconciliation
        .write()
        .confirm_cancel(&order.client_order_id());
    reported.push(order.client_order_id());
}

/// Returns the order a batch payload belongs to.
fn order_for<'a>(orders: &'a [OrderAny], payload: &OndoApiOrder) -> Option<&'a OrderAny> {
    payload.client_order_id().and_then(|client_order_id| {
        orders
            .iter()
            .find(|order| order.client_order_id().as_str() == client_order_id)
    })
}

/// Returns whether an error is a refusal the venue already decided, or one that never left here.
///
/// A 4xx rejection is terminal, and a credential, signing or environment failure is decided before
/// a socket: in both cases the order cannot be resting, so it is rejected rather than left in
/// flight. Everything else - a timeout, a transport failure, a 5xx, an unreadable answer - may have
/// been applied, and is left to reconciliation (plan §6.3).
fn is_definitive_refusal(error: &OndoHttpError) -> bool {
    matches!(
        error,
        OndoHttpError::RequestRejected { .. }
            | OndoHttpError::AuthRejected { .. }
            | OndoHttpError::NotAuthenticated { .. }
            | OndoHttpError::Signing(_)
            | OndoHttpError::Environment(_)
    )
}

/// Returns whether the venue refused the order because a post-only order would have matched.
fn is_post_only_refusal(error: &OndoHttpError) -> bool {
    matches!(
        error,
        OndoHttpError::RequestRejected { code: Some(code), .. } if code == ONDO_POST_ONLY_HAS_MATCH
    )
}

/// Returns whether the venue refused the request because the signature's clock was outside its
/// tolerance.
fn is_clock_skew_rejection(error: &OndoHttpError) -> bool {
    matches!(
        error,
        OndoHttpError::AuthRejected {
            failure: OndoAuthFailure::TimestampTooFar,
            ..
        }
    )
}

/// Classifies a cancel refusal from the venue's own error code.
fn cancel_rejection(error: &OndoHttpError) -> OndoCancelRejection {
    match error {
        OndoHttpError::RequestRejected { code, .. } | OndoHttpError::AuthRejected { code, .. } => {
            OndoCancelRejection::from_code(code.as_deref())
        }
        _ => OndoCancelRejection::Other,
    }
}

/// Renders the reason an order was refused, keeping the venue's own code and words when it sent
/// them (plan §6.3: a `post_only_has_match` rejection keeps its specific reason).
fn refusal_reason(error: &OndoHttpError) -> String {
    match error {
        OndoHttpError::RequestRejected { code, message, .. } => match code {
            Some(code) => format!("submit-order-error: {code}: {message}"),
            None => format!("submit-order-error: {message}"),
        },
        other => format!("submit-order-error: {other}"),
    }
}

/// Renders the reason a locally refused command was denied.
fn denial_reason(error: &OndoOrderError) -> String {
    format!("order-denied: {}: {error}", error.name())
}

/// Returns the venue's `type` member as the Nautilus order type this adapter creates.
///
/// The spec's enum is `limit`, `market`, `stopMarket` and `takeProfitMarket`, and only the first
/// two are types this adapter creates. The other two are refused by name rather than coerced, for
/// the same reason the status mapping above refuses an unknown status: an order the venue reports
/// as a stop must not reach the engine looking like a limit order. Plan §6.3 asks for a named
/// unsupported error for order types outside this phase.
///
/// This matters most on the path that needs it most. `tracked` is `None` when the order is one
/// this adapter never submitted - the bulk read of external orders plan §6.4 wants identified
/// rather than dropped - and there the venue's payload is the only evidence there is. Refusing
/// means such an order produces a logged refusal and no report; that is the lesser harm against
/// handing the engine a limit order that is really a stop.
///
/// # Errors
///
/// Returns an error naming the member the venue sent when it is not one of the two types this
/// adapter creates.
fn nautilus_order_type(order_type: &str) -> anyhow::Result<OrderType> {
    match order_type {
        "limit" => Ok(OrderType::Limit),
        "market" => Ok(OrderType::Market),
        other => anyhow::bail!(
            "the venue's order type `{other}` is not one of the types this adapter creates"
        ),
    }
}

/// Returns the Nautilus order status a venue status maps to, for a report that states the venue's
/// own view.
///
/// # Errors
///
/// Returns an error for a status this adapter has no Nautilus status for, exactly as it does
/// through [`order_status_for_ledger`]: a report states a state, and an invented one is worse than
/// none.
fn venue_order_status(
    status: &OndoOrderStatus,
    filled_qty: Quantity,
) -> anyhow::Result<OrderStatus> {
    match status {
        OndoOrderStatus::Open if filled_qty.is_positive() => Ok(OrderStatus::PartiallyFilled),
        OndoOrderStatus::Open => Ok(OrderStatus::Accepted),
        OndoOrderStatus::FullyFilled => Ok(OrderStatus::Filled),
        OndoOrderStatus::Canceled => Ok(OrderStatus::Canceled),
        other => anyhow::bail!(
            "the venue's status `{}` has no Nautilus order status",
            other.as_str()
        ),
    }
}

/// Returns the Nautilus order status a venue status maps to for an order this adapter holds a
/// ledger for (plan §6.3).
///
/// The status and the quantity a report states have to be consistent, and for a filled order the
/// quantity the report states is the applied fills total
/// ([`OndoReporter::status_report`]). A venue status of `fullyfilled` whose `filledSize` the
/// applied fills do not account for is therefore **not** reported as `Filled` - that would state a
/// completion of an order whose fills this adapter has not reported, and the engine turns exactly
/// that into an inferred fill. It is reported as the state the ledger does support: the venue's
/// order is finished, this client's account of it is not, and the difference is what
/// [`OndoOrderState::fill_gap`] leaves the reconciliation to report.
///
/// A `canceled` order is reported as canceled whatever the fills say: a cancel is a statement that
/// the order stopped, not that it completed, and withholding it would leave the cache holding an
/// order the venue has already ended.
///
/// # Errors
///
/// Returns an error for a status this adapter has no Nautilus status for.
fn order_status_for_ledger(
    status: &OndoOrderStatus,
    venue_filled: Quantity,
    order: &OndoOrderState,
) -> anyhow::Result<OrderStatus> {
    match status {
        OndoOrderStatus::FullyFilled if venue_filled != order.filled => {
            Ok(if order.filled.is_zero() {
                OrderStatus::Accepted
            } else if order.filled >= order.quantity {
                // The applied fills cover the order on their own, so the completion is one this
                // adapter can support even though the venue's number is not the one it reports.
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            })
        }
        other => venue_order_status(other, order.filled),
    }
}

/// Returns the venue's `timeInForce` member as the Nautilus value it was sent as.
///
/// `order_type` is the payload's own `type`, and it is not decoration: the spec documents an
/// absent `timeInForce` for exactly one case - *"Not returned for market orders"* - and a market
/// order's venue behaviour is immediate-or-cancel. An absent member on any other type is
/// undocumented, so it is refused rather than read as IOC.
///
/// The spec's enum is `GTC` and `IOC` and nothing else, so a third value is refused too. An
/// unrecognised member is not a member this adapter may pick a reading for: the failure mode of
/// guessing here is a reported time-in-force the venue never sent, and the whole point of the
/// untracked-order path is that the payload is the only evidence there is.
///
/// # Errors
///
/// Returns an error naming the member the venue sent when it is outside the documented pair, and
/// an error naming the order type when the member is absent on anything but a market order.
fn nautilus_time_in_force(
    order_type: &str,
    time_in_force: Option<&str>,
) -> anyhow::Result<TimeInForce> {
    match time_in_force {
        Some("GTC") => Ok(TimeInForce::Gtc),
        Some("IOC") => Ok(TimeInForce::Ioc),
        None if order_type == "market" => Ok(TimeInForce::Ioc),
        None => anyhow::bail!(
            "the venue returned no timeInForce for a `{order_type}` order, which the spec \
             documents for market orders only"
        ),
        Some(other) => anyhow::bail!(
            "the venue's timeInForce `{other}` is outside the documented GTC/IOC pair"
        ),
    }
}

/// Returns the order side a fill belongs to.
///
/// The venue's own `side` member is the answer when it is there. When it is not, the fill's
/// `direction` gives it for the four unambiguous cases - opening a long or a short is a buy or a
/// sell, and closing one is the opposite - and a flip, which determines neither, is a named error
/// rather than a guess.
///
/// # Errors
///
/// Returns an error when `side` is present but is neither `buy` nor `sell`, or when the side has to
/// be derived from a direction that does not determine one.
fn fill_order_side(fill: &OndoApiFill) -> anyhow::Result<OrderSide> {
    if let Some(side) = fill.side() {
        return OndoSide::from_raw(side)
            .map(OndoSide::to_order_side)
            .map_err(|error| anyhow::anyhow!("Ondo fill {}: {error}", fill.id()));
    }

    match fill.direction() {
        OndoFillDirection::OpenLong | OndoFillDirection::CloseShort => Ok(OrderSide::Buy),
        OndoFillDirection::OpenShort | OndoFillDirection::CloseLong => Ok(OrderSide::Sell),
        direction => anyhow::bail!(
            "Ondo fill {} reports the direction `{}` and no `side`; a flip does not determine the \
             order side",
            fill.id(),
            direction.as_str(),
        ),
    }
}

/// Returns a fill's size, exactly.
///
/// # Errors
///
/// Returns an error when the venue sent no `size`, or one that is not an exact decimal that fits.
fn fill_quantity(fill: &OndoApiFill) -> anyhow::Result<Quantity> {
    let size = fill
        .size()
        .ok_or_else(|| anyhow::anyhow!("Ondo fill {} carries no `size`", fill.id()))?;

    Ok(Quantity::from_decimal(parse_decimal(size, "size")?)?)
}

/// Returns a fill's price, exactly.
///
/// # Errors
///
/// Returns an error when the venue sent no `price`, or one that is not an exact decimal that fits.
fn fill_price(fill: &OndoApiFill) -> anyhow::Result<Price> {
    let price = fill
        .price()
        .ok_or_else(|| anyhow::anyhow!("Ondo fill {} carries no `price`", fill.id()))?;

    Ok(Price::from_decimal(parse_decimal(price, "price")?)?)
}

/// Returns the time of the event a fill reports, when the venue gave one this adapter can read.
///
/// [`None`] is "this fill cannot be placed in time": the venue sent no `time`, the lexeme is not a
/// timestamp this adapter can read, or it came out as the Unix epoch. Those three are one answer on
/// purpose - what the caller needs to know is that no instant was established, and a zero is not an
/// instant. A caller that has to emit a required timestamp falls back to the local clock, exactly as
/// [`event_time`] does for an order payload; a caller that is *filtering* must not, because a
/// fallback silently decides the very question the filter is asking. Both callers read this one
/// function so the two answers cannot drift apart.
fn fill_timestamp(fill: &OndoApiFill) -> Option<UnixNanos> {
    fill.time()
        .and_then(|time| parse_timestamp(time).ok())
        .filter(|ts| !ts.is_zero())
}

/// Reads one funding record as a payment, or says why it cannot be one.
///
/// The amount and the instant are what a payment **is**, and both are required by the frozen
/// schema: a record missing either is a payload this adapter cannot account for, and the pass says
/// the funding could not be read rather than counting a payment whose value it guessed. The rate
/// and the position size are evidence about the payment and are carried when the venue sent them;
/// nothing here multiplies one by the other.
fn funding_payment(fee: &OndoApiFundingFee) -> anyhow::Result<FundingPayment> {
    let time = parse_timestamp(fee.time()).map_err(|error| {
        anyhow::anyhow!("a funding payment's `time` could not be read: {error}")
    })?;

    Ok(FundingPayment {
        market: fee.market().to_string(),
        time,
        amount: balance_member(fee.amount(), "amount")?,
        rate: fee
            .rate()
            .map(|rate| balance_member(rate, "rate"))
            .transpose()?,
        position_size: fee
            .position_size()
            .map(|size| balance_member(size, "positionSize"))
            .transpose()?,
    })
}

/// Returns the liquidity side a fill carries.
///
/// An absent `isMaker` is [`LiquiditySide::NoLiquiditySide`]: this adapter does not guess which
/// side of the book a fill was on.
fn liquidity_side(fill: &OndoApiFill) -> LiquiditySide {
    match fill.is_maker() {
        Some(true) => LiquiditySide::Maker,
        Some(false) => LiquiditySide::Taker,
        None => LiquiditySide::NoLiquiditySide,
    }
}

/// Returns the average price the venue's own `filledCost` and `filledSize` imply.
///
/// The venue defines `cost` = `price` x `size` (see `test_data/README.md`), so this is the venue's
/// own arithmetic rather than an estimate. It is [`None`] when the payload carries no cost or no
/// positive filled size.
fn average_price(payload: &OndoApiOrder) -> Option<Decimal> {
    let cost = parse_decimal(payload.filled_cost()?, "filledCost").ok()?;
    let size = parse_decimal(payload.filled_size(), "filledSize").ok()?;

    (size > Decimal::ZERO).then(|| cost / size)
}

/// Returns the time of the event a payload reports.
///
/// The venue's own `canceledAt`, `filledAt` and `createdAt` are tried in that order - the newest
/// state a payload carries is the one it is about - and an unreadable, absent or zero one falls
/// back to `fallback`, the local clock. A timestamp this adapter cannot read never becomes a zero
/// time on an event.
fn event_time(payload: &OndoApiOrder, fallback: UnixNanos) -> UnixNanos {
    let candidates = [
        payload.ts_canceled().ok().flatten(),
        payload.ts_filled().ok().flatten(),
        payload.ts_created().ok(),
    ];

    candidates
        .into_iter()
        .flatten()
        .find(|ts| !ts.is_zero())
        .unwrap_or(fallback)
}

/// Adds a fill's quantity to a running total.
///
/// # Errors
///
/// Returns an error when the sum is not representable, which a total of representable quantities
/// never is in practice and which is still never silently replaced by a zero.
fn add_quantity(total: Quantity, quantity: Quantity) -> anyhow::Result<Quantity> {
    Ok(Quantity::from_decimal(
        total.as_decimal() + quantity.as_decimal(),
    )?)
}

#[cfg(test)]
mod tests {
    use nautilus_core::time::get_atomic_clock_static;
    use nautilus_model::{enums::AccountType, identifiers::TraderId};
    use rstest::rstest;

    use super::*;

    /// The frozen REST spec's own create answer, as the request fixtures use it.
    const ORDER_BODY: &str = r#"{"orderId":"197ec08e001658690721be129e7fa595","clientOrderId":"ondo_probe_1","side":"buy","price":"227.50","size":"0.10","market":"NVDA-USD.P","filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}"#;

    #[rstest]
    #[case::minimum(0, 1)]
    #[case::configured_default(15, 15)]
    #[case::bounded_maximum(60, 30)]
    fn test_production_readonly_readiness_uses_the_configured_http_window(
        #[case] http_timeout_secs: u64,
        #[case] expected: u64,
    ) {
        assert_eq!(
            production_readonly_readiness_timeout_secs(http_timeout_secs),
            expected,
        );
    }

    /// One `ApiFill`, built from the documented members with the caller's overrides.
    ///
    /// The fill type keeps its members private, so a fixture is written as the JSON the venue
    /// sends rather than as a struct literal - which is also what makes the fixture prove the
    /// parse rather than the constructor.
    #[derive(Clone, Copy, Debug)]
    struct FillFixture<'a> {
        id: &'a str,
        size: Option<&'a str>,
        price: Option<&'a str>,
        fee: &'a str,
        side: Option<&'a str>,
        direction: &'a str,
        is_maker: Option<bool>,
    }

    impl<'a> FillFixture<'a> {
        fn new() -> Self {
            Self {
                id: "f1",
                size: Some("0.20"),
                price: Some("227.50"),
                fee: "0.01",
                side: Some("buy"),
                direction: "openLong",
                is_maker: Some(false),
            }
        }

        fn build(&self) -> OndoApiFill {
            let member = |name: &str, value: &str| format!(r#""{name}":{value},"#);
            let optional = |name: &str, value: Option<String>| {
                value.map_or_else(String::new, |value| member(name, &value))
            };

            let text = format!(
                r#"{{{}{}{}{}{}{}{}"market":"NVDA-USD.P","orderId":"197ec08e001658690721be129e7fa595","clientOrderId":"ondo_probe_1","time":"2025-03-05T14:30:01.000000000Z"}}"#,
                member("id", &format!(r#""{}""#, self.id)),
                optional("size", self.size.map(|size| format!(r#""{size}""#))),
                optional("price", self.price.map(|price| format!(r#""{price}""#))),
                member("fee", &format!(r#""{}""#, self.fee)),
                optional("side", self.side.map(|side| format!(r#""{side}""#))),
                member("direction", &format!(r#""{}""#, self.direction)),
                optional("isMaker", self.is_maker.map(|maker| maker.to_string())),
            );

            OndoApiFill::from_raw(&serde_json::value::RawValue::from_string(text).expect("JSON"))
                .expect("the fill fixture is an ApiFill")
        }
    }

    /// One `ApiFill` in the documented shape.
    fn fill(id: &str, size: &str, fee: &str) -> OndoApiFill {
        FillFixture {
            id,
            size: Some(size),
            fee,
            ..FillFixture::new()
        }
        .build()
    }

    #[rstest]
    fn test_the_fill_ledger_dedupes_on_the_account_and_the_fill_id() {
        let mut ledger = OndoFillLedger::new();
        let account = AccountId::from("ONDO-SANDBOX-001");

        assert!(ledger.record(account, "f1"));
        assert!(!ledger.record(account, "f1"));
        assert!(ledger.contains(account, "f1"));
        assert_eq!(ledger.len(), 1);

        assert!(
            ledger.record(AccountId::from("ONDO-SANDBOX-002"), "f1"),
            "the same fill id at another account is a different fill",
        );

        ledger.forget(account, "f1");
        assert!(!ledger.contains(account, "f1"));
    }

    #[rstest]
    fn test_a_status_this_adapter_does_not_know_is_never_terminal() {
        assert!(!OndoOrderStatus::Unknown("settling".to_string()).is_terminal());
        assert!(!OndoOrderStatus::Untriggered.is_terminal());
        assert!(OndoOrderStatus::FullyFilled.is_terminal());
        assert!(OndoOrderStatus::Canceled.is_terminal());
    }

    #[rstest]
    fn test_the_venue_order_type_and_time_in_force_map_onto_the_ones_this_adapter_sends() {
        assert_eq!(nautilus_order_type("market").unwrap(), OrderType::Market);
        assert_eq!(nautilus_order_type("limit").unwrap(), OrderType::Limit);
        assert_eq!(
            nautilus_time_in_force("limit", Some("GTC")).unwrap(),
            TimeInForce::Gtc,
        );
        assert_eq!(
            nautilus_time_in_force("limit", Some("IOC")).unwrap(),
            TimeInForce::Ioc,
        );
        assert_eq!(
            nautilus_time_in_force("market", None).unwrap(),
            TimeInForce::Ioc,
            "the spec returns no timeInForce for a market order, whose venue behaviour is \
             immediate-or-cancel",
        );
    }

    #[rstest]
    fn test_a_documented_order_type_this_adapter_does_not_create_is_refused_by_name() {
        for order_type in ["stopMarket", "takeProfitMarket"] {
            let error = nautilus_order_type(order_type)
                .expect_err("neither is a type this adapter creates");

            assert!(
                error.to_string().contains(order_type),
                "the refusal names the member the venue sent: {error}",
            );
        }
    }

    #[rstest]
    fn test_a_time_in_force_outside_the_documented_pair_is_refused_and_not_guessed() {
        let undocumented = nautilus_time_in_force("limit", Some("FOK"))
            .expect_err("the spec's enum is GTC and IOC only");

        assert!(
            undocumented.to_string().contains("FOK"),
            "the refusal names the member the venue sent: {undocumented}",
        );

        let absent_on_a_limit = nautilus_time_in_force("limit", None)
            .expect_err("the spec documents an absent timeInForce for market orders only");

        assert!(
            absent_on_a_limit.to_string().contains("market"),
            "the refusal says which case the absence is not: {absent_on_a_limit}",
        );
    }

    #[rstest]
    fn test_the_order_side_falls_back_to_the_direction_only_when_it_determines_one() {
        assert_eq!(
            fill_order_side(&fill("f1", "0.20", "0.01")).unwrap(),
            OrderSide::Buy,
        );

        let directed = |direction: &'static str| {
            FillFixture {
                side: None,
                direction,
                ..FillFixture::new()
            }
            .build()
        };

        assert_eq!(
            fill_order_side(&directed("openLong")).unwrap(),
            OrderSide::Buy
        );
        assert_eq!(
            fill_order_side(&directed("open short")).unwrap(),
            OrderSide::Sell,
            "the venue's other spelling is read the same way",
        );
        assert_eq!(
            fill_order_side(&directed("closeLong")).unwrap(),
            OrderSide::Sell
        );
        assert_eq!(
            fill_order_side(&directed("closeShort")).unwrap(),
            OrderSide::Buy,
        );
        assert!(
            fill_order_side(&directed("flipLongToShort")).is_err(),
            "a flip determines no order side and is refused rather than guessed",
        );
    }

    #[rstest]
    fn test_the_average_price_is_the_venues_own_arithmetic() {
        let payload = OndoApiOrder::from_text(
            &ORDER_BODY
                .replace(r#""filledCost":"0.00""#, r#""filledCost":"45.500""#)
                .replace(r#""filledSize":"0.00""#, r#""filledSize":"0.20""#),
        )
        .unwrap();

        assert_eq!(average_price(&payload), Some(Decimal::new(2275, 1)));

        let unfilled = OndoApiOrder::from_text(ORDER_BODY).unwrap();
        assert_eq!(
            average_price(&unfilled),
            None,
            "a zero filled size has no average price",
        );
    }

    #[rstest]
    fn test_an_event_time_is_never_zero() {
        let payload = OndoApiOrder::from_text(ORDER_BODY).unwrap();
        let fallback = UnixNanos::from(7);

        assert_eq!(
            event_time(&payload, fallback),
            parse_timestamp("2025-03-05T14:30:00Z").unwrap(),
        );

        let unreadable =
            OndoApiOrder::from_text(&ORDER_BODY.replace("2025-03-05T14:30:00Z", "not a timestamp"))
                .unwrap();
        assert_eq!(event_time(&unreadable, fallback), fallback);
    }

    #[rstest]
    fn test_a_fill_without_a_readable_size_or_price_is_refused_rather_than_defaulted() {
        let order_fill = fill("f1", "0.20", "0.01");
        assert_eq!(fill_quantity(&order_fill).unwrap(), Quantity::from("0.20"));
        assert_eq!(fill_price(&order_fill).unwrap(), Price::from("227.50"));

        let sizeless = FillFixture {
            size: None,
            ..FillFixture::new()
        }
        .build();
        assert!(fill_quantity(&sizeless).is_err());

        let unreadable = FillFixture {
            size: Some("not a decimal"),
            ..FillFixture::new()
        }
        .build();
        assert!(fill_quantity(&unreadable).is_err());

        let priceless = FillFixture {
            price: None,
            ..FillFixture::new()
        }
        .build();
        assert!(fill_price(&priceless).is_err());
    }

    #[rstest]
    fn test_an_absent_liquidity_side_is_not_guessed() {
        assert_eq!(
            liquidity_side(&fill("f1", "0.20", "0.01")),
            LiquiditySide::Taker,
        );

        let maker = FillFixture {
            is_maker: Some(true),
            ..FillFixture::new()
        }
        .build();
        assert_eq!(liquidity_side(&maker), LiquiditySide::Maker);

        let unmarked = FillFixture {
            is_maker: None,
            ..FillFixture::new()
        }
        .build();
        assert_eq!(
            liquidity_side(&unmarked),
            LiquiditySide::NoLiquiditySide,
            "an absent isMaker is not guessed into maker or taker",
        );
    }

    #[rstest]
    fn test_a_definitive_refusal_is_told_apart_from_an_unknown_outcome() {
        let rejected = OndoHttpError::RequestRejected {
            status: 400,
            code: Some(ONDO_POST_ONLY_HAS_MATCH.to_string()),
            message: "post only order would match".to_string(),
        };

        assert!(is_definitive_refusal(&rejected));
        assert!(is_post_only_refusal(&rejected));
        assert_eq!(
            cancel_rejection(&rejected),
            OndoCancelRejection::Other,
            "a create refusal is not a cancel refusal",
        );

        assert!(!is_definitive_refusal(&OndoHttpError::Timeout(
            "no answer".to_string()
        )));
        assert!(!is_definitive_refusal(&OndoHttpError::Http {
            status: 503,
            body: "busy".to_string(),
        }));
        assert!(!is_post_only_refusal(&OndoHttpError::Timeout(
            "no answer".to_string()
        )));
    }

    #[rstest]
    fn test_a_cancel_refusal_that_requires_a_query_is_classified_from_its_code() {
        let already_filled = OndoHttpError::RequestRejected {
            status: 400,
            code: Some("order_already_fully_filled".to_string()),
            message: String::new(),
        };

        assert_eq!(
            cancel_rejection(&already_filled),
            OndoCancelRejection::AlreadyFilled,
        );
        assert!(cancel_rejection(&already_filled).requires_query());
    }

    #[rstest]
    fn test_an_order_is_unresolved_only_when_its_state_cannot_be_accounted_for() {
        let state =
            |status: OndoOrderStatus, venue_filled: Option<&str>, filled: &str| OndoOrderState {
                client_order_id: ClientOrderId::from("ondo_probe_1"),
                venue_order_id: Some(VenueOrderId::from("197ec08e001658690721be129e7fa595")),
                instrument_id: InstrumentId::from("NVDA-USD-PERP.ONDO"),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                quantity: Quantity::from("1.00"),
                price: Some(Price::from("227.50")),
                reduce_only: false,
                post_only: false,
                status,
                accepted: true,
                filled: Quantity::from(filled),
                venue_fee: None,
                last_fill_size: None,
                venue_filled: venue_filled.map(Quantity::from),
                last_raw: String::new(),
                unappliable_fill: false,
                resolved: false,
                pending_fills: Vec::new(),
            };

        assert!(
            !state(OndoOrderStatus::Open, None, "0").is_unresolved(),
            "an order that is still working is not one this adapter cannot account for",
        );
        assert!(!state(OndoOrderStatus::Pending, None, "0").is_unresolved());
        assert!(state(OndoOrderStatus::Untriggered, None, "0").is_unresolved());
        assert!(state(OndoOrderStatus::Unknown("settling".to_string()), None, "0").is_unresolved());
        assert!(
            state(OndoOrderStatus::FullyFilled, Some("1.00"), "0").is_unresolved(),
            "a terminal status whose fills do not add up is unresolved",
        );
        assert!(
            !state(OndoOrderStatus::FullyFilled, Some("1.00"), "1.00").is_unresolved(),
            "the same reading is not unresolved once the fills account for it",
        );
        assert!(
            !state(OndoOrderStatus::FullyFilled, Some("0.00"), "0").is_unresolved(),
            "a terminal status the fills agree with is not one this adapter cannot account for",
        );
        assert!(
            state(OndoOrderStatus::FullyFilled, None, "0").is_unresolved(),
            "a terminal status whose filled quantity cannot be read states no total to check the \
             applied fills against - which is not the fills agreeing with it",
        );
        assert!(
            state(OndoOrderStatus::Canceled, None, "0").is_unresolved(),
            "the same unreadable reading on the other terminal status",
        );
        assert!(
            state(OndoOrderStatus::Open, None, "0").is_accounted_for(),
            "and a working order with no readable total is still one this adapter can account for",
        );
        assert!(
            OndoOrderState {
                unappliable_fill: true,
                ..state(OndoOrderStatus::Open, None, "0")
            }
            .is_unresolved(),
            "a fill this adapter can never report leaves the order unresolved permanently",
        );
    }

    #[rstest]
    fn test_a_running_total_never_becomes_a_silent_zero() {
        assert_eq!(
            add_quantity(Quantity::from("0.20"), Quantity::from("0.30")).unwrap(),
            Quantity::from("0.50"),
        );
    }

    /// A reporter holding no orders, for the tests that drive the state machine directly.
    fn reporter() -> OndoReporter {
        let clock = get_atomic_clock_static();
        let account_id = AccountId::from("ONDO-SANDBOX-001");

        OndoReporter {
            account_id,
            clock,
            emitter: ExecutionEventEmitter::new(
                clock,
                TraderId::from("TESTER-001"),
                account_id,
                AccountType::Margin,
                None, // base_currency
            ),
            state: Arc::new(RwLock::new(OndoPrivateState::default())),
        }
    }

    /// F14: the two conditions that leave an order unresolved are different in kind, and the one
    /// that cannot be cleared must not be cleared by the one that can.
    ///
    /// A fill this adapter accepted and can never express is a fact about the fills it has seen:
    /// the venue holds it, the ledger cannot carry it, and the applied total can never reach the
    /// venue's - so nothing that arrives later changes it and nothing later may clear it. The
    /// terminal filled-size disagreement is the other kind: it is recomputed from the numbers and
    /// it is gone as soon as they agree.
    ///
    /// The flush path is driven directly here. A fill's expressibility is checked before it is ever
    /// buffered ([`OndoReporter::apply_fill`] builds the report before it records anything), so the
    /// error branch a buffered flush can reach is one this test has to place itself: what it pins is
    /// that the branch's condition survives the recomputation.
    #[rstest]
    fn test_a_fill_this_adapter_can_never_report_is_never_downgraded_to_resolved() {
        let reporter = reporter();
        let client_order_id = ClientOrderId::from("ondo_probe_1");

        // A terminal order whose applied fills add up to the venue's own total: nothing about it is
        // left to reconcile, and the recomputation settles it.
        let order = OndoOrderState {
            client_order_id,
            venue_order_id: Some(VenueOrderId::from("197ec08e001658690721be129e7fa595")),
            instrument_id: InstrumentId::from("NVDA-USD-PERP.ONDO"),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            quantity: Quantity::from("1.00"),
            price: Some(Price::from("227.50")),
            reduce_only: false,
            post_only: false,
            status: OndoOrderStatus::FullyFilled,
            accepted: true,
            filled: Quantity::from("1.00"),
            venue_fee: None,
            last_fill_size: None,
            venue_filled: Some(Quantity::from("1.00")),
            last_raw: String::new(),
            unappliable_fill: false,
            resolved: false,
            pending_fills: Vec::new(),
        };

        reporter.state.write().orders.insert(client_order_id, order);
        reporter.settle(&client_order_id);

        let state = reporter
            .state
            .read()
            .orders
            .get(&client_order_id)
            .cloned()
            .unwrap();

        assert!(state.resolved);
        assert!(!state.is_unresolved());

        // The fill this adapter accepted and cannot express: no readable size, so it can never be
        // reported, and the order is short of the venue's total for good.
        let unexpressible = FillFixture {
            id: "f2",
            size: None,
            ..FillFixture::new()
        }
        .build();

        reporter.flush_fill(&client_order_id, &unexpressible);

        // The recomputation runs again over numbers that now *do* agree, which is exactly what is
        // not allowed to clear it.
        reporter.settle(&client_order_id);

        let state = reporter
            .state
            .read()
            .orders
            .get(&client_order_id)
            .cloned()
            .unwrap();

        assert!(state.unappliable_fill);
        assert_eq!(
            state.fill_gap(),
            None,
            "the terminal totals agree: the recomputed condition has nothing left to report",
        );
        assert!(
            !state.resolved,
            "a fill this adapter can never report is not cleared by the numbers agreeing",
        );
        assert!(state.is_unresolved());
    }

    #[rstest]
    fn test_a_pass_ends_only_the_claim_it_still_holds() {
        let ownership = PassOwnership::default();
        let first = ownership.claim().expect("the account is free");

        assert!(
            ownership.claim().is_none(),
            "a second pass may not claim an account the first one owns",
        );

        // The drain ends the pass while its guard is still alive, so the next pass legitimately
        // claims the account before the first one has finished concluding.
        first.end();
        let second = ownership.claim().expect("the drain released the account");

        drop(first);

        assert!(
            ownership.claim().is_none(),
            "the first pass's guard must not release the claim the second pass holds now: two \
             passes would advance the account and neither would refuse the other",
        );

        drop(second);
        assert!(
            ownership.claim().is_some(),
            "the pass that owns the account releases it when it leaves",
        );
    }
}
