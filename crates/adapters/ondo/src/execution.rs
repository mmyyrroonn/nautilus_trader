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
//! # What this client does not own
//!
//! The private WebSocket stream, the startup and reconnect reconciliation, the account and balance
//! mapping, and the dead man's switch are Task 8's (plan §6.4). The ingestion seams they will drive
//! ([`OndoExecutionClient::apply_order`] and [`OndoExecutionClient::apply_fill`]) are here, because
//! the dedup ledger and the status machine are what make them safe to call from more than one
//! source.
//!
//! For the same reason this phase reports no positions through a bulk path:
//! [`OndoExecutionClient::provides_bulk_position_coverage`] answers `false`, so an absent position
//! report is never read as a flat account before Task 8 implements the real one.
//!
//! # The environment is closed before anything else happens
//!
//! [`OndoExecutionClient::new`] validates the configuration, resolves the REST base URL, and runs
//! [`validate_authenticated_environment`] on the pair. A production configuration is refused there,
//! as is a base URL that is not the endpoint this session may sign for
//! ([`crate::common::endpoint::OndoEndpointPolicy`]: the sandbox host, or a loopback test service),
//! before a credential is read and before any socket exists - and
//! [`OndoExecutionConfigError::ProductionOrdersUnsupported`] refuses `allow_production_orders`
//! whatever it is set to, production endpoint or not. There is no production write branch to
//! configure open (plan §1, §4.1, §R0.3).
//!
//! # The unknown outcome
//!
//! A submission whose answer never arrived, or arrived unreadable, may have been applied at the
//! venue: it is reported as neither accepted nor rejected, the order stays in flight, and it is
//! never resubmitted (plan §6.3). What the client does *not* do is judge the account clean: an
//! order left unresolved - an unknown status, an `untriggered` conditional, or a terminal status
//! whose fills do not add up - is listed by [`OndoExecutionClient::unresolved_orders`] until more
//! evidence arrives.

use std::{collections::BTreeMap, sync::Arc};

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
    enums::{LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::RwLock;
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::ONDO_SETTLEMENT_CURRENCY,
        credential::{OndoCredential, validate_authenticated_environment},
        parse::{instrument_id_to_market, market_to_instrument_id, parse_decimal, parse_timestamp},
    },
    config::OndoExecutionClientConfig,
    http::{
        client::{
            NewRiskPermit, OndoCancelAnswer, OndoHttpClient, OndoNewRiskGuard, OndoNewRiskSendError,
        },
        error::OndoHttpError,
        orders::{
            ONDO_POST_ONLY_HAS_MATCH, OndoApiOrder, OndoCancelRejection, OndoOrderCommand,
            OndoOrderError, OndoOrderStatus, OndoRejectedOrder, OndoSide, client_lookup_value,
        },
        private::{OndoApiFill, OndoFillDirection, OndoPrivateReadQuery},
        query::{CursorWalk, FILLS_PATH, ORDERS_PATH},
        rate_limit::OndoRequestPriority,
    },
    reconciliation::{
        AccountJudgment, AccountReading, Admission, BalanceReading, DeadMansSwitchMessage,
        MetadataValidity, NewRiskRefusal, OrderReading, PositionReading, ProbeOutcome, ProbeReport,
        ReconciliationBuffer, ReconciliationMachine, ReconciliationState, StopStep,
        UncertainOutcome, balance_member,
    },
};

/// How many pages of orders or fills one report generation walks at most.
///
/// The walk itself refuses a repeated cursor; this only bounds an endpoint that keeps handing out
/// fresh ones ([`CursorWalk`]).
const REPORT_MAX_PAGES: usize = 100;

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
/// ([`Self::reconciliation_needed`]) rather than adopted, so a venue that reports a filled quantity
/// no fill accounts for leaves the order unresolved instead of silently agreeing.
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
    /// Whether the venue's filled quantity disagreed with the applied fills.
    pub reconciliation_needed: bool,
    /// Whether the order ended in a state this adapter confirmed with the venue.
    pub resolved: bool,
    /// Fills this adapter accepted **before** the venue acknowledged the order.
    pub pending_fills: Vec<OndoApiFill>,
}

impl OndoOrderState {
    /// Returns whether this order's state is one this adapter **cannot** account for.
    ///
    /// That is a status it does not know, an `untriggered` conditional it does not create, or a
    /// terminal status whose fills do not add up. An order that is simply still working is neither
    /// resolved nor unresolved, and it does not belong here: plan §6.3 is about the states the
    /// adapter cannot read, not about the ones it is waiting on. This is the predicate an account
    /// is judged clean against.
    #[must_use]
    pub fn is_unresolved(&self) -> bool {
        !self.status.is_known()
            || self.status == OndoOrderStatus::Untriggered
            || self.reconciliation_needed
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

/// The order index and the fill ledger.
#[derive(Debug, Default)]
struct OndoPrivateState {
    orders: AHashMap<ClientOrderId, OndoOrderState>,
    by_venue_order_id: AHashMap<String, ClientOrderId>,
    fills: OndoFillLedger,
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
    fn settle(&self, client_order_id: &ClientOrderId) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.resolved = entry.status.is_terminal()
                && !entry.reconciliation_needed
                && entry.pending_fills.is_empty();
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

        {
            let mut state = self.state.write();

            if !state.fills.record(account_id, fill.id()) {
                return Ok(OndoFillApplication::Duplicate);
            }

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
        // filled quantity the applied fills do not account for leaves the order unresolved
        // rather than silently agreeing with it.
        if let Some(venue_filled) = venue_filled
            && status.is_terminal()
            && venue_filled != entry.filled
        {
            entry.reconciliation_needed = true;

            log::error!(
                "Ondo order {client_order_id} is {} at the venue with filledSize {venue_filled} \
                 but the applied fills total {}; the order stays unresolved",
                status.as_str(),
                entry.filled,
            );
        }

        entry.resolved =
            status.is_terminal() && !entry.reconciliation_needed && entry.pending_fills.is_empty();

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
                self.mark_reconciliation_needed(client_order_id);

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
        }
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
                reconciliation_needed: false,
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

    /// Marks an order as needing reconciliation, so it is never reported as resolved.
    fn mark_reconciliation_needed(&self, client_order_id: &ClientOrderId) {
        let mut state = self.state.write();

        if let Some(entry) = state.orders.get_mut(client_order_id) {
            entry.reconciliation_needed = true;
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
    /// `filled_qty` is the **venue's own** `filledSize`: a status report states the venue's view,
    /// and the applied-fills total is what [`OndoOrderState::reconciliation_needed`] compares it
    /// against.
    fn status_report(
        &self,
        payload: &OndoApiOrder,
        tracked: Option<&OndoOrderState>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let filled_qty = payload.filled_quantity()?;

        let order_status = match payload.status() {
            OndoOrderStatus::Open if filled_qty.is_positive() => OrderStatus::PartiallyFilled,
            OndoOrderStatus::Open => OrderStatus::Accepted,
            OndoOrderStatus::FullyFilled => OrderStatus::Filled,
            OndoOrderStatus::Canceled => OrderStatus::Canceled,
            other => anyhow::bail!(
                "the venue's status `{}` has no Nautilus order status",
                other.as_str()
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
    /// The private reports that arrived while a pass was reading the account (plan §6.4).
    buffer: Arc<RwLock<ReconciliationBuffer>>,
    tasks: TaskGroup,
}

impl OndoExecutionClient {
    /// Builds the client from its core identity and configuration.
    ///
    /// Nothing here performs I/O. The configuration validation and the environment gate run before
    /// the HTTP client is constructed, so a refused configuration has no object to send from:
    ///
    /// 1. [`OndoExecutionClientConfig::validate`], which refuses `allow_production_orders` and a
    ///    missing account;
    /// 2. [`validate_authenticated_environment`] on the resolved base URL, which refuses production
    ///    and every authority the endpoint allowlist does not carry - the sandbox host and a
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

        // The endpoint gate runs on the configuration's own environment and base URL, before the
        // credential is resolved and before a client exists. The transport applies the same policy
        // again to the credential it is handed, so a refused endpoint is refused at both layers.
        let base_url = config.http_base_url().to_string();
        validate_authenticated_environment(config.environment, &base_url)?;

        let credential = match credential {
            Some(credential) => credential,
            None if config.has_explicit_credentials() => OndoCredential::new(
                config.environment,
                config.api_key.clone().unwrap_or_default(),
                config.api_secret.clone().unwrap_or_default(),
            )
            .map_err(|error| anyhow::anyhow!("the configured credential is unusable: {error}"))?,
            None => crate::common::credential::resolve_credential(config.environment, &base_url)
                .map_err(|error| anyhow::anyhow!("no Ondo Perps credential: {error}"))?,
        };

        let account_id = core.account_id;
        let clock = get_atomic_clock_realtime();

        // The account's state machine is built before the transport, because the transport is built
        // with the guard that reads it: a signed write has to re-check the account at the send
        // point, and a client that cannot do that is a client this adapter does not order from.
        let reconciliation = Arc::new(RwLock::new(ReconciliationMachine::new(
            account_id,
            config.dms_timeout_secs,
        )));

        let http_client = OndoHttpClient::builder()
            .base_url(base_url)
            .timeout_secs(config.http_timeout_secs)
            .maybe_budget(budget)
            .credential(credential)
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

        Ok(Self {
            core,
            config,
            http_client,
            reporter,
            reconciliation,
            buffer: Arc::new(RwLock::new(ReconciliationBuffer::new())),
            tasks: TaskGroup::new(),
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
    /// conditional, or a terminal status whose fills do not add up. An order that is merely still
    /// working is not one of these. A non-empty result is what stops an account being judged clean
    /// (plan §6.3, §6.4).
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
        self.reconciliation.read().can_submit_new_orders()
    }

    /// Returns whether this client must refuse a new order right now.
    ///
    /// Holds from construction: a client that has established nothing has verified nothing, so
    /// there is no account state a new order could be placed against.
    #[must_use]
    pub fn refuses_new_risk(&self) -> bool {
        self.reconciliation.read().refuses_new_risk()
    }

    /// Returns why a new order would be refused, or [`None`] when one may be submitted.
    #[must_use]
    pub fn new_risk_refusal(&self) -> Option<NewRiskRefusal> {
        self.reconciliation.read().new_risk_refusal()
    }

    /// The one admission decision, taken under the lock that reads it.
    fn admission(&self) -> Admission {
        self.reconciliation.read().admission()
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
        self.reconciliation.write().begin_recovery(now);
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
        self.reconciliation.write().set_metadata(validity);
    }

    /// Arms the dead man's switch, returning the frame the private stream must send.
    ///
    /// New orders wait for the venue's confirmation: an unconfirmed arm is not an arm (§6.4).
    pub fn arm_dead_mans_switch(&self, now: UnixNanos) -> DeadMansSwitchMessage {
        self.reconciliation.write().dead_mans_switch_mut().arm(now)
    }

    /// Applies the venue's confirmation of the switch.
    pub fn confirm_dead_mans_switch(&self, now: UnixNanos) {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .confirm_armed(now);
    }

    /// Renews an armed switch, returning the frame to send, or [`None`] when there is nothing to
    /// renew.
    #[must_use]
    pub fn renew_dead_mans_switch(&self, now: UnixNanos) -> Option<DeadMansSwitchMessage> {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .renew(now)
    }

    /// Records that the switch failed, which stops new orders.
    pub fn note_dead_mans_switch_failed(&self, reason: String) {
        self.reconciliation
            .write()
            .dead_mans_switch_mut()
            .fail(reason);
    }

    /// Records that the switch fired: the venue cancelled this account's resting orders.
    ///
    /// The account becomes uncertain, because what the cancellation left behind has to be read, and
    /// no position is closed by a switch (plan §6.4).
    pub fn note_dead_mans_switch_fired(&self, now: UnixNanos) {
        self.reconciliation.write().note_switch_fired(now);
    }

    /// Returns the steps a stop takes, in the order it takes them (plan §6.4).
    ///
    /// The switch is released only after this run's own orders are cancelled and confirmed: a
    /// released switch cancels nothing, and the orders it was covering would be left resting.
    #[must_use]
    pub fn stop_sequence(&self) -> Vec<StopStep> {
        let mut steps = vec![StopStep::CancelOwnOrders, StopStep::ConfirmOwnOrders];

        if self.reconciliation.read().dead_mans_switch().is_required() {
            steps.push(StopStep::ReleaseDeadMansSwitch);
        }

        steps.push(StopStep::ClosePrivateStream);

        steps
    }

    /// Records a private report that arrived while the account was being read.
    ///
    /// The report is held rather than applied so the pass can replay it through the same state
    /// machine the REST pages go through, where it is deduped (plan §6.4).
    pub fn buffer_stream_order(&self, payload: OndoApiOrder) {
        self.buffer.write().record_order(payload);
    }

    /// Records a private fill that arrived while the account was being read.
    pub fn buffer_stream_fill(&self, fill: OndoApiFill) {
        self.buffer.write().record_fill(fill);
    }

    /// Reads the account once and concludes a pass (plan §6.4).
    ///
    /// The reads are the venue's own lists, walked with the same bounded [`CursorWalk`] every other
    /// history read uses, and every payload is applied through [`Self::apply_order`] and
    /// [`Self::apply_fill`] - the one state machine, so the stream and this pass can never disagree
    /// about what a payload means. The buffered stream reports are replayed after the pages, where
    /// the ledger and the order index dedupe them.
    ///
    /// # Errors
    ///
    /// Returns an error when the account cannot be read completely: a failed request, an
    /// unreadable page, a cursor that will not advance. The machine is left
    /// [`ReconciliationState::Uncertain`] in that case - a pass that stopped early must not look
    /// like an account that was read.
    pub async fn reconcile_account(&self, now: UnixNanos) -> anyhow::Result<AccountJudgment> {
        match self.read_account().await {
            Ok(reading) => {
                self.reconciliation.write().conclude_pass(&reading, now);

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
                    ProbeOutcome::Found
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

    /// Reads the account: orders, fills, the buffered reports, positions and balance.
    ///
    /// The order readings are built **after** every payload of the pass has been applied, not as
    /// the pages arrive. Pagination means the order list and the fill history are read minutes
    /// apart in the worst case, and nothing in the protocol orders the two: an order read before
    /// the fill that completed it would otherwise be judged against a filled quantity the same
    /// pass had not applied yet, and a reconciled account would look like a disagreement.
    async fn read_account(&self) -> anyhow::Result<AccountReading> {
        let mut reading = AccountReading::default();

        let orders = self.read_orders().await?;

        self.read_fills(&mut reading).await?;
        self.replay_buffered();

        reading.orders = orders
            .iter()
            .map(|payload| self.order_reading(payload))
            .collect();
        reading.positions = self.read_positions().await?;
        reading.balance = Some(self.read_balance().await?);
        reading.applied_net = self.reporter.state.read().applied_net();

        Ok(reading)
    }

    /// Walks the venue's order list, applying every payload it carries and returning them.
    async fn read_orders(&self) -> anyhow::Result<Vec<OndoApiOrder>> {
        let mut payloads = Vec::new();
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();

        loop {
            let response =
                self.http_client.get_orders(&query).await.map_err(|error| {
                    anyhow::anyhow!("the order list could not be read: {error}")
                })?;

            for item in response.items()? {
                let payload = OndoApiOrder::from_text(item.get())?;

                self.apply_order(&payload);
                payloads.push(payload);
            }

            let Some(cursor) = walk.advance(response.cursor())? else {
                break;
            };

            query = query.with_cursor(cursor);
        }

        Ok(payloads)
    }

    /// Walks the venue's fill history, applying every fill it carries.
    async fn read_fills(&self, reading: &mut AccountReading) -> anyhow::Result<()> {
        let mut walk = CursorWalk::new(REPORT_MAX_PAGES);
        let mut query = OndoPrivateReadQuery::new();

        loop {
            let response =
                self.http_client.get_fills(&query).await.map_err(|error| {
                    anyhow::anyhow!("the fill history could not be read: {error}")
                })?;

            for fill in response.fills()? {
                match self.apply_fill(&fill) {
                    Ok(OndoFillApplication::Applied) => reading.fills.push(fill.id().to_string()),
                    Ok(_) => {}
                    Err(error) => log::error!(
                        "Ondo fill {} could not be applied during reconciliation: {error}",
                        fill.id(),
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

    /// Replays the private reports that arrived while the account was being read.
    fn replay_buffered(&self) {
        let (orders, fills) = self.buffer.write().drain();

        for payload in &orders {
            self.apply_order(payload);
        }

        for fill in &fills {
            if let Err(error) = self.apply_fill(fill) {
                log::error!(
                    "A buffered Ondo fill {} could not be applied during reconciliation: {error}",
                    fill.id(),
                );
            }
        }

        if !orders.is_empty() || !fills.is_empty() {
            log::info!(
                "Replayed {} buffered Ondo order report(s) and {} fill(s) during reconciliation",
                orders.len(),
                fills.len(),
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

            readings.push(PositionReading::new(
                market,
                direction,
                balance_member(net_quantity, "netQuantity")?,
            ));
        }

        Ok(readings)
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

        Ok(BalanceReading {
            wallet_balance: member("walletBalance")?,
            margin_balance: member("marginBalance")?,
            used_margin: member("usedMargin")?,
            available_margin: member("availableMargin")?,
            withdrawable_margin: member("withdrawableMargin")?,
            unmapped,
            raw,
        })
    }

    /// Builds one order's reading, with what this client's index holds for it.
    fn order_reading(&self, payload: &OndoApiOrder) -> OrderReading {
        let state =
            payload
                .client_order_id()
                .map(ClientOrderId::from)
                .and_then(|client_order_id| {
                    self.reporter
                        .state
                        .read()
                        .orders
                        .get(&client_order_id)
                        .cloned()
                });

        OrderReading {
            venue_order_id: payload.order_id().to_string(),
            client_order_id: payload.client_order_id().map(ToString::to_string),
            market: payload.market().to_string(),
            status: payload.status().clone(),
            venue_filled: payload
                .filled_quantity()
                .ok()
                .map(|quantity| quantity.as_decimal()),
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
        self.reporter.apply_order(payload, Acceptance::Report, None)
    }

    /// Applies one `ApiFill` payload: the one path a fill is ever counted from.
    ///
    /// # Errors
    ///
    /// See [`OndoReporter::apply_fill`].
    pub fn apply_fill(&self, fill: &OndoApiFill) -> anyhow::Result<OndoFillApplication> {
        self.reporter.apply_fill(fill)
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
}

#[async_trait(?Send)]
impl ExecutionClient for OndoExecutionClient {
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

        log::info!("Stopping Ondo Perps execution client");
        self.tasks.abort();
        self.core.set_stopped();
        self.core.set_disconnected();
        // A stopped client holds no verified account: were it to be reset and reused, new risk
        // would wait for a recovery rather than resume on the state the last session read.
        self.reconciliation
            .write()
            .note_disconnected(self.reporter.now());

        Ok(())
    }

    /// Reopens the request path after a [`Self::stop`].
    ///
    /// The task generation `stop` closed is what the submission and cancel paths spawn on, so
    /// resetting the client without reopening it would leave it connected but unable to send.
    ///
    /// # Errors
    ///
    /// Returns an error when the previous generation has not finished, which is the task group's
    /// own refusal to run two generations at once.
    fn reset(&mut self) -> anyhow::Result<()> {
        if !self.tasks.is_open() {
            self.tasks
                .start_generation()
                .map_err(|error| anyhow::anyhow!("Ondo Perps task generation: {error}"))?;
        }

        Ok(())
    }

    /// Marks the client connected.
    ///
    /// This opens no socket: the private stream and its buffered reports are a later phase, and a
    /// recovery is begun by [`Self::begin_recovery`] - the hook the stream's connect and reconnect
    /// paths call - rather than by a socket coming up. What this method does settle is the order
    /// path's state, so the state the request path works in is explicit rather than implied.
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        self.core.set_connected();
        log::info!("Ondo Perps execution client connected");

        Ok(())
    }

    /// Marks the client disconnected, and the account unverified with it.
    ///
    /// A session that ends leaves the venue state the session established unread: new risk stops
    /// until a recovery converges again (plan §6.4). Cancels and queries still travel.
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        self.core.set_disconnected();
        self.reconciliation
            .write()
            .note_disconnected(self.reporter.now());
        log::info!("Ondo Perps execution client disconnected");

        Ok(())
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

        let Some(spawner) = self.spawner() else {
            self.reporter
                .emitter
                .emit_order_denied(&order, "the Ondo Perps execution client is shutting down");

            return Ok(());
        };

        let http_client = self.http_client.clone();
        let reporter = self.reporter.clone();
        let reconciliation = Arc::clone(&self.reconciliation);
        let client_order_id = cmd.client_order_id;
        let instrument_id = cmd.instrument_id;

        spawner.spawn(async move {
            if let Admission::Refused { reason } =
                revalidate(&reconciliation, &Admission::Granted { generation: permit })
            {
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
                    reporter.forget(&client_order_id);
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
                    reporter.forget(&client_order_id);
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
                    reporter.forget(&client_order_id);
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
            if let Admission::Refused { reason } =
                revalidate(&reconciliation, &Admission::Granted { generation: permit })
            {
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
                report_refused_item(&reporter, &orders, refused, &mut reported);
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
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(client_order_id) = cmd.client_order_id else {
            anyhow::bail!(
                "An Ondo Perps order status report needs a client order id: this venue has no \
                 account-wide order lookup by venue order id alone"
            );
        };

        let order_ref = self.order_ref(&client_order_id, cmd.venue_order_id);

        let payload = self
            .http_client
            .get_order(&order_ref, OndoRequestPriority::High)
            .await?;

        if let OndoOrderApplication::Unresolved { reason, .. } = self.apply_order(&payload) {
            anyhow::bail!("Ondo order {client_order_id} is unresolved: {reason}");
        }

        let state = self.reporter.state.read();

        Ok(Some(self.reporter.status_report(
            &payload,
            state.orders.get(&client_order_id),
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

                reports.push(self.reporter.status_report(&payload, None, ts_event)?);
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
                // milliseconds, both optional), but this read does not send one:
                // `OndoPrivateReadQuery` serializes `market`, `limit` and `cursor` only, and
                // widening the signed target is a change to the request bytes rather than to this
                // filter. Until that changes the command's window is applied here, on the fill's
                // own timestamp - and only to a fill whose timestamp this adapter can read.
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

    /// Returns no position reports: this phase implements none.
    ///
    /// The venue's positions are read by Task 8's reconciliation (plan §6.4), which is where the
    /// netting semantics and the account's margin mapping are settled. Returning an empty list is
    /// safe here only because [`Self::provides_bulk_position_coverage`] answers `false`.
    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        Ok(Vec::new())
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
) -> Admission {
    reconciliation.read().revalidate(permit)
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

        match self.reconciliation.read().revalidate(&permit) {
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
    // in flight that does not exist.
    reporter.forget(&order.client_order_id());
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
    use rstest::rstest;

    use super::*;

    /// The frozen REST spec's own create answer, as the request fixtures use it.
    const ORDER_BODY: &str = r#"{"orderId":"197ec08e001658690721be129e7fa595","clientOrderId":"ondo_probe_1","side":"buy","price":"227.50","size":"0.10","market":"NVDA-USD.P","filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}"#;

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
        let state = |status: OndoOrderStatus, reconciliation_needed: bool| OndoOrderState {
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
            filled: Quantity::from("0"),
            venue_fee: None,
            last_fill_size: None,
            venue_filled: None,
            last_raw: String::new(),
            reconciliation_needed,
            resolved: false,
            pending_fills: Vec::new(),
        };

        assert!(
            !state(OndoOrderStatus::Open, false).is_unresolved(),
            "an order that is still working is not one this adapter cannot account for",
        );
        assert!(!state(OndoOrderStatus::Pending, false).is_unresolved());
        assert!(state(OndoOrderStatus::Untriggered, false).is_unresolved());
        assert!(state(OndoOrderStatus::Unknown("settling".to_string()), false).is_unresolved());
        assert!(
            state(OndoOrderStatus::FullyFilled, true).is_unresolved(),
            "a terminal status whose fills do not add up is unresolved",
        );
    }

    #[rstest]
    fn test_a_running_total_never_becomes_a_silent_zero() {
        assert_eq!(
            add_quantity(Quantity::from("0.20"), Quantity::from("0.30")).unwrap(),
            Quantity::from("0.50"),
        );
    }
}
