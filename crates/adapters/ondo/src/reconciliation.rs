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

//! Startup, reconnect and steady-state reconciliation, the account snapshot and the dead man's
//! switch (plan §6.3, §6.4).
//!
//! Three things live here, and they are one state machine's three faces:
//!
//! 1. [`ReconciliationMachine`] - the account's state, and the one decision that governs whether
//!    new risk may be taken ([`ReconciliationMachine::admission`]). It answers `Granted` for
//!    exactly one combination: a recovered account, current metadata, a switch that permits orders,
//!    and no outcome this client has left unsettled. Everything else - including a machine that has
//!    never held a session - is [`Admission::Refused`], and the permit a submission is given is
//!    re-verified against the same run generation before its request exists
//!    ([`ReconciliationMachine::revalidate`]).
//! 2. The account's reading and its judgments: what the venue said ([`AccountReading`]), what that
//!    implies ([`AccountJudgment`], [`Finding`]), and the mappings the plan fixes - a `short`
//!    position carried by a positive number (§6.4), a `neutral` position zeroed explicitly, five
//!    balance members kept distinct with `total = free + locked` verified rather than assumed.
//! 3. [`DeadMansSwitch`] - the account-level cancel-all channel (§6.4) as this adapter would drive
//!    it, and [`LedgerJournal`] - the dedup ledger that has to survive a restart.
//!
//! # The two rules this module is built around
//!
//! *Fail closed.* Every question this module answers has an "I do not know" answer, and it is the
//! one that stops risk: `Ready` is the only state that permits a new order, an unreadable status,
//! an unreadable balance or an unexplained position all leave the account [`Finding`]-flagged, and
//! a reading that could not be completed is [`ReconciliationState::Uncertain`] rather than a
//! shorter reading.
//!
//! *Absent evidence is not evidence.* A 404 is not proof a submission never happened (§6.3), a
//! venue that does not list an order has not said the order is gone, and a dead man's switch that
//! fired cancels resting orders - it does not close a position. Nothing here concludes a position
//! is flat from a missing row.
//!
//! # What is not implemented here
//!
//! The private WebSocket stream itself. The frames the switch sends and the messages a stream
//! would buffer are produced by this module ([`DeadMansSwitchMessage`], [`ReconciliationBuffer`]),
//! and the private stream's connect and reconnect hooks are what will drive
//! [`ReconciliationMachine::begin_recovery`] and [`ReconciliationMachine::note_disconnected`]. The
//! switch's real renewal message and its trigger behaviour are **unverified against the venue**:
//! the frozen material documents the channel, the operation and `timeout_seconds`, and nothing
//! else.

use std::collections::{BTreeMap, BTreeSet};

use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    common::parse::{market_to_instrument_id, parse_decimal},
    execution::OndoFillLedger,
    http::{
        orders::{OndoApiOrder, OndoOrderStatus},
        private::OndoApiFill,
    },
    websocket::WsOp,
};

/// How long a submission whose outcome is unknown is probed before this adapter stops and leaves it
/// to a human (plan §6.3: thirty seconds).
pub const ONDO_SUBMISSION_UNKNOWN_SECS: u64 = 30;

/// The shortest interval between two probes of one unknown submission.
///
/// Six probes fit the thirty-second window. The interval is a local choice - nothing in the frozen
/// material fixes it - and it exists so a probe cannot become a busy loop against a rate-limited
/// venue.
pub const ONDO_SUBMISSION_PROBE_INTERVAL: u64 = 5;

/// How many consecutive agreeing passes a recovery requires before it reports
/// [`ReconciliationState::Ready`] (plan §6.4: a second converging confirmation when no consistent
/// sequence boundary can be proven).
pub const ONDO_RECONCILE_CONFIRMATIONS: usize = 2;

/// The dead man's switch channel (plan §6.4, `cancelAllOrdersAfterPerps`).
pub const ONDO_DMS_CHANNEL: &str = "cancelAllOrdersAfterPerps";

/// The four states a reconciliation is in (plan Task 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconciliationState {
    /// No session, or a session that ended. Nothing about the account is being read.
    Disconnected,
    /// The account is being read. New orders stop, cancels and queries still go.
    Recovering,
    /// Two agreeing passes read the same account, with nothing left unexplained.
    Ready,
    /// Something could not be accounted for. Never a licence to trade: only a later pass that
    /// reads a clean account leaves this state.
    Uncertain,
}

impl ReconciliationState {
    /// Returns the state's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::Uncertain => "uncertain",
        }
    }
}

/// Why a new order is refused (plan §6.4).
///
/// The variants are the conditions a submission is checked against, in the order
/// [`ReconciliationMachine::admission`] applies them. They are named rather than rendered so a
/// caller can act on one, and so a refusal reason is a statement about a specific condition rather
/// than a sentence assembled at the call site.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NewRiskRefusal {
    /// The admission a caller presented is not the current one: something that revokes permission
    /// happened after it was issued, even if the account is admissible again by now.
    ///
    /// Only [`ReconciliationMachine::revalidate`] answers this. A permit is good for the run
    /// generation that issued it and for nothing later.
    Superseded,
    /// Submissions whose outcome this client has not settled (plan §6.3).
    UnknownSubmissions {
        /// The client order ids the submissions were made under.
        client_order_ids: Vec<ClientOrderId>,
    },
    /// Cancels no venue answer has settled (plan §6.3).
    UnconfirmedCancels {
        /// The client order ids the cancels were made under.
        client_order_ids: Vec<ClientOrderId>,
    },
    /// The metadata this client trades on is not current, so nothing can be priced or sized on it.
    MetadataStale {
        /// Why the metadata is not usable.
        reason: String,
    },
    /// The dead man's switch does not permit orders.
    DeadMansSwitch(DeadMansSwitchState),
    /// The account's reconciliation state is not [`ReconciliationState::Ready`].
    AccountState(ReconciliationState),
}

impl NewRiskRefusal {
    /// Returns a human-readable statement of the refusal.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::Superseded => {
                "the admission this order was given is no longer current".to_string()
            }
            Self::UnknownSubmissions { client_order_ids } => format!(
                "the outcome of {} submission(s) is unknown: {}",
                client_order_ids.len(),
                names(client_order_ids),
            ),
            Self::UnconfirmedCancels { client_order_ids } => format!(
                "{} cancel(s) were not confirmed by any venue answer: {}",
                client_order_ids.len(),
                names(client_order_ids),
            ),
            Self::MetadataStale { reason } => {
                format!("the instrument metadata is not current: {reason}")
            }
            Self::DeadMansSwitch(state) => match state {
                DeadMansSwitchState::NotRequired => {
                    "the dead man's switch is not required".to_string()
                }
                DeadMansSwitchState::Disarmed => "the dead man's switch is not armed".to_string(),
                DeadMansSwitchState::Arming => {
                    "the dead man's switch is not yet confirmed".to_string()
                }
                DeadMansSwitchState::Armed => "the dead man's switch is armed".to_string(),
                DeadMansSwitchState::Failed { reason } => {
                    format!("the dead man's switch failed: {reason}")
                }
                DeadMansSwitchState::Expired => "the dead man's switch expired".to_string(),
            },
            Self::AccountState(state) => format!("the account is {}", state.as_str()),
        }
    }
}

/// Renders client order ids for a refusal reason.
fn names(client_order_ids: &[ClientOrderId]) -> String {
    client_order_ids
        .iter()
        .map(ClientOrderId::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The one decision that governs new risk (plan §6.4).
///
/// A granted admission carries the run generation it was granted under. The generation is the
/// machine's own count of the events that revoke permission, so a caller can hold a decision it was
/// given and ask whether it is still the current one before acting on it - which is what
/// [`ReconciliationMachine::revalidate`] answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// A new order may be submitted.
    Granted {
        /// The run generation this decision belongs to.
        generation: u64,
    },
    /// A new order must be refused.
    Refused {
        /// Why.
        reason: NewRiskRefusal,
    },
}

impl Admission {
    /// Returns whether this decision permits a new order.
    #[must_use]
    pub const fn is_granted(&self) -> bool {
        matches!(self, Self::Granted { .. })
    }

    /// Returns the generation this decision belongs to, when it grants.
    #[must_use]
    pub const fn generation(&self) -> Option<u64> {
        match self {
            Self::Granted { generation } => Some(*generation),
            Self::Refused { .. } => None,
        }
    }
}

/// Whether the metadata this client trades on is usable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataValidity {
    /// The instrument metadata was read and is current for this session.
    Current,
    /// The last refresh failed, or no metadata has been read: the previous version is kept and
    /// marked, and new orders stop while the state is unknown (plan §4.1).
    Stale {
        /// Why the metadata is not usable.
        reason: String,
    },
}

impl MetadataValidity {
    /// Returns whether new orders may rely on this metadata.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Current)
    }
}

/// One message on the dead man's switch channel.
///
/// The frozen schema is `{op, channel, timeout_seconds}` with `op` one of `subscribe` /
/// `unsubscribe`; the same shape arms, renews and releases the switch here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadMansSwitchMessage {
    /// The operation.
    pub op: WsOp,
    /// The channel name, always [`ONDO_DMS_CHANNEL`].
    pub channel: String,
    /// The timeout the venue is asked for, in seconds.
    pub timeout_seconds: u64,
}

impl DeadMansSwitchMessage {
    /// Builds the frame for `op` and `timeout_seconds`.
    #[must_use]
    pub fn new(op: WsOp, timeout_seconds: u64) -> Self {
        Self {
            op,
            channel: ONDO_DMS_CHANNEL.to_string(),
            timeout_seconds,
        }
    }
}

/// How the switch stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeadMansSwitchState {
    /// No switch was asked for: it does not govern this client's orders.
    NotRequired,
    /// A switch is required and has not been armed.
    Disarmed,
    /// The subscribe frame was sent and the venue has not confirmed it. Orders stop until it does.
    Arming,
    /// The venue confirmed the switch. Resting orders are cancelled if no renewal arrives within
    /// the timeout.
    Armed,
    /// Arming or renewing failed. New orders stop (plan §6.4: "DMS 失败停止挂新单").
    Failed {
        /// Why the switch failed.
        reason: String,
    },
    /// The switch fired: the venue cancelled the account's resting orders. It does **not** close a
    /// position, and it is not evidence that one is gone (plan §6.4).
    Expired,
}

/// The dead man's switch: a venue-side timer that cancels every resting order on the account.
///
/// This is the local half only. It builds the frames, tracks the deadline and decides what the
/// client may do next; the transport that carries the frames is the private stream's.
///
/// **The renewal is unverified.** The frozen schema documents the subscribe frame and the timeout,
/// and says nothing about which message renews an armed switch. This adapter renews by sending the
/// subscribe frame again, which is the only renewal the documentation admits - and that is an
/// assumption a sandbox session has to settle (plan §6.4, Task 8).
#[derive(Clone, Debug)]
pub struct DeadMansSwitch {
    required: bool,
    state: DeadMansSwitchState,
    timeout_seconds: u64,
    expires_at: Option<UnixNanos>,
    renewals: u64,
}

impl DeadMansSwitch {
    /// Creates a switch that nothing requires yet.
    #[must_use]
    pub const fn new(timeout_seconds: u64) -> Self {
        Self {
            required: false,
            state: DeadMansSwitchState::NotRequired,
            timeout_seconds,
            expires_at: None,
            renewals: 0,
        }
    }

    /// Returns the timeout this switch asks the venue for, in seconds.
    #[must_use]
    pub const fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    /// Returns the switch's state.
    #[must_use]
    pub fn state(&self) -> DeadMansSwitchState {
        self.state.clone()
    }

    /// Returns whether this client must keep a switch armed.
    #[must_use]
    pub const fn is_required(&self) -> bool {
        self.required
    }

    /// Returns whether new orders may be placed under this switch.
    ///
    /// A switch nobody required does not govern orders. A required switch permits them only while
    /// the venue has confirmed it: an unconfirmed arm is not an arm (plan §6.4).
    #[must_use]
    pub const fn permits_new_orders(&self) -> bool {
        !self.required || matches!(self.state, DeadMansSwitchState::Armed)
    }

    /// Returns the instant the switch fires, once the venue has confirmed it.
    #[must_use]
    pub const fn expires_at(&self) -> Option<UnixNanos> {
        self.expires_at
    }

    /// Returns how many times an armed switch has been renewed.
    #[must_use]
    pub const fn renewals(&self) -> u64 {
        self.renewals
    }

    /// Returns whether the deadline has passed at `now`.
    #[must_use]
    pub fn has_expired(&self, now: UnixNanos) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }

    /// Requires this client to keep a switch armed, without arming one.
    pub fn require(&mut self) {
        self.required = true;

        if matches!(self.state, DeadMansSwitchState::NotRequired) {
            self.state = DeadMansSwitchState::Disarmed;
        }
    }

    /// Arms the switch: the frame to send, and the state that waits for the venue's confirmation.
    pub fn arm(&mut self, _now: UnixNanos) -> DeadMansSwitchMessage {
        self.required = true;
        self.state = DeadMansSwitchState::Arming;
        self.expires_at = None;

        self.frame(WsOp::Subscribe)
    }

    /// Applies the venue's confirmation of the subscribe frame.
    ///
    /// The deadline starts here rather than when the frame was written: what the venue timed is
    /// its own receipt, and a deadline computed locally would fire early.
    pub fn confirm_armed(&mut self, now: UnixNanos) {
        self.state = DeadMansSwitchState::Armed;
        self.expires_at = Some(UnixNanos::from(
            now.as_u64() + self.timeout_seconds * 1_000_000_000,
        ));
    }

    /// Renews an armed switch, returning the frame to send.
    ///
    /// [`None`] when there is nothing to renew: an unarmed or unconfirmed switch has no deadline to
    /// move, and a failed one must not be renewed behind the account's back.
    pub fn renew(&mut self, now: UnixNanos) -> Option<DeadMansSwitchMessage> {
        if !matches!(self.state, DeadMansSwitchState::Armed) {
            return None;
        }

        self.expires_at = Some(UnixNanos::from(
            now.as_u64() + self.timeout_seconds * 1_000_000_000,
        ));
        self.renewals += 1;

        Some(self.frame(WsOp::Subscribe))
    }

    /// Records that the switch failed, which stops new orders.
    pub fn fail(&mut self, reason: String) {
        self.required = true;
        self.state = DeadMansSwitchState::Failed { reason };
        self.expires_at = None;
    }

    /// Releases the switch, returning the frame to send.
    ///
    /// Releasing returns the client to "no switch required", which permits new orders again - the
    /// reason the stop sequence cancels and confirms this run's orders *before* it releases
    /// anything (plan §6.4).
    pub fn release(&mut self, _now: UnixNanos) -> DeadMansSwitchMessage {
        self.required = false;
        self.state = DeadMansSwitchState::NotRequired;
        self.expires_at = None;

        self.frame(WsOp::Unsubscribe)
    }

    /// Records that the switch fired.
    pub fn note_fired(&mut self, _now: UnixNanos) {
        self.state = DeadMansSwitchState::Expired;
        self.expires_at = None;
    }

    /// Builds one frame for this switch.
    fn frame(&self, op: WsOp) -> DeadMansSwitchMessage {
        DeadMansSwitchMessage::new(op, self.timeout_seconds)
    }
}

/// The direction member of an `ApiPosition`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PositionDirection {
    /// A long position.
    Long,
    /// A short position.
    Short,
    /// No position: the explicit flat state.
    Neutral,
    /// A direction this adapter does not know, kept as the venue spelled it.
    Unknown(String),
}

impl PositionDirection {
    /// Reads the venue's `direction` member. An unknown spelling is preserved, never coerced.
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        match raw {
            "long" => Self::Long,
            "short" => Self::Short,
            "neutral" => Self::Neutral,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// Returns the venue's spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
            Self::Neutral => "neutral",
            Self::Unknown(raw) => raw,
        }
    }
}

/// Maps a position's direction and `netQuantity` onto a signed quantity (plan §6.4).
///
/// The venue documents `netQuantity` as the *size* of the position with the direction carrying the
/// sign - its own example is a `short` of `1.5489` - so the sign is applied here and only here. A
/// value that already carries a sign is taken as the venue sent it: negating it would invert a
/// position, and that is what the plan's "禁止双重取负" forbids.
///
/// A `neutral` direction is zero, whatever the number said: a flat position is a state, not an
/// arithmetic result.
#[must_use]
pub fn signed_position_quantity(direction: &PositionDirection, net_quantity: Decimal) -> Decimal {
    if net_quantity.is_sign_negative() && !net_quantity.is_zero() {
        return net_quantity;
    }

    match direction {
        PositionDirection::Long => net_quantity,
        PositionDirection::Short => -net_quantity,
        PositionDirection::Neutral => Decimal::ZERO,
        PositionDirection::Unknown(_) => net_quantity,
    }
}

/// One position as the venue stated it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionReading {
    /// The venue's market string.
    pub market: String,
    /// The instrument the market maps to, when it maps to one.
    pub instrument_id: Option<InstrumentId>,
    /// The venue's direction.
    pub direction: PositionDirection,
    /// The venue's `netQuantity`, verbatim.
    pub net_quantity: Decimal,
    /// The signed position: [`signed_position_quantity`] of the two above.
    pub signed: Decimal,
}

impl PositionReading {
    /// Reads one `ApiPosition` payload onto a reading.
    #[must_use]
    pub fn new(market: &str, direction: &str, net_quantity: Decimal) -> Self {
        let direction = PositionDirection::from_raw(direction);

        Self {
            market: market.to_string(),
            instrument_id: market_to_instrument_id(market).ok(),
            signed: signed_position_quantity(&direction, net_quantity),
            direction,
            net_quantity,
        }
    }

    /// Returns whether the venue states this position is flat.
    #[must_use]
    pub fn is_flat(&self) -> bool {
        self.signed.is_zero()
    }
}

/// One order as the venue stated it, plus what this client's own fills add up to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderReading {
    /// The venue's order id.
    pub venue_order_id: String,
    /// The client order id the venue echoed, when it echoed one.
    pub client_order_id: Option<String>,
    /// The venue's market string.
    pub market: String,
    /// The venue's status, verbatim for one this adapter does not know.
    pub status: OndoOrderStatus,
    /// The venue's own `filledSize`, when it sent a readable one.
    pub venue_filled: Option<Decimal>,
    /// The quantity this client's applied fills add up to, when it tracks the order.
    pub applied_filled: Option<Decimal>,
    /// Whether this client's index holds the order.
    pub tracked: bool,
}

/// The five balance members the venue documents, kept distinct (plan §6.4).
///
/// They are not one number: `walletBalance` is what was deposited, `marginBalance` is equity,
/// `usedMargin` is what is committed, and `availableMargin` and `withdrawableMargin` are what
/// remains, on two different definitions. Collapsing them into one balance would make a margin
/// call unreadable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BalanceReading {
    /// `walletBalance`: the deposited balance.
    pub wallet_balance: Option<Decimal>,
    /// `marginBalance`: equity, the number a negative reading of which is an account underwater.
    pub margin_balance: Option<Decimal>,
    /// `usedMargin`: the margin committed to positions.
    pub used_margin: Option<Decimal>,
    /// `availableMargin`: the margin free for new positions.
    pub available_margin: Option<Decimal>,
    /// `withdrawableMargin`: the margin that may leave the account.
    pub withdrawable_margin: Option<Decimal>,
    /// Members this adapter does not map: any balance member outside the documented set is kept
    /// here rather than folded into the USDC numbers, because a second collateral asset or a loan
    /// is a different account than this adapter knows how to trade (plan §6.4).
    pub unmapped: Vec<(String, String)>,
    /// The payload, exactly as the venue sent it.
    pub raw: String,
}

/// The Nautilus-facing shape of a [`BalanceReading`]: one currency, and the venue's arithmetic
/// verified rather than assumed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedBalance {
    reading: BalanceReading,
    total: Decimal,
    locked: Decimal,
    free: Decimal,
}

impl MappedBalance {
    /// Returns the reading this mapping came from.
    #[must_use]
    pub const fn reading(&self) -> &BalanceReading {
        &self.reading
    }

    /// Returns equity (`marginBalance`), the Nautilus total.
    #[must_use]
    pub const fn total(&self) -> Decimal {
        self.total
    }

    /// Returns the committed margin (`usedMargin`), the Nautilus locked amount.
    #[must_use]
    pub const fn locked(&self) -> Decimal {
        self.locked
    }

    /// Returns the free margin (`availableMargin`), the Nautilus free amount.
    #[must_use]
    pub const fn free(&self) -> Decimal {
        self.free
    }
}

/// Something about the account that this adapter cannot account for, or that it did not create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finding {
    /// An order whose status this adapter cannot account for, or whose venue filled quantity its
    /// own fills do not explain (plan §6.3).
    UnresolvedOrder {
        /// The venue's order id.
        venue_order_id: String,
        /// The client order id, when the venue echoed one.
        client_order_id: Option<String>,
        /// Why the order is unresolved.
        reason: String,
    },
    /// An order on a market that does not map onto an instrument.
    UnmappableMarket {
        /// The venue's order id.
        venue_order_id: String,
        /// The venue's market string.
        market: String,
    },
    /// An order this run did not create. It is identified rather than dropped, and it is never
    /// this adapter's to cancel (plan §6.4).
    ForeignOrder {
        /// The venue's order id.
        venue_order_id: String,
        /// The client order id the venue echoed, when it echoed one.
        client_order_id: Option<String>,
        /// The venue's market string.
        market: String,
        /// The venue's status, which decides whether this is a *known* foreign order.
        status: OndoOrderStatus,
    },
    /// A position whose market does not map onto an instrument.
    UnmappablePosition {
        /// The venue's market string.
        market: String,
    },
    /// A position whose direction this adapter cannot read.
    UnreadablePosition {
        /// The venue's market string.
        market: String,
        /// The direction the venue sent.
        direction: String,
    },
    /// The venue's position is not the one this client's fills and its adopted baseline explain.
    PositionMismatch {
        /// The instrument the position is on.
        instrument_id: InstrumentId,
        /// The signed position the venue stated.
        venue: Decimal,
        /// The signed position this client's fills and baseline imply.
        expected: Decimal,
    },
    /// An instrument the venue's **complete** position list does not carry, on which this client's
    /// fills and baseline are not flat (plan §6.4).
    ///
    /// This is a statement the venue made, not a silence it left: the endpoint returns all open
    /// positions, so an instrument missing from a read that succeeded and whose every row was
    /// readable is the venue saying the account holds nothing on it. A position this client's fills
    /// built and the venue does not carry, and a position carried into the run that the venue no
    /// longer lists, are one condition - the venue's position is flat and this client's expectation
    /// is not - and the baseline entry is retired with the finding so the disagreement is reported
    /// once rather than repeated against a carried-in position the venue has already closed.
    PositionAbsent {
        /// The instrument the venue's list does not carry.
        instrument_id: InstrumentId,
        /// The signed position this client's fills and baseline imply.
        expected: Decimal,
    },
    /// A balance member outside the documented set: a second collateral asset, a loan, or a member
    /// this adapter has not been taught. Unsupported rather than folded in (plan §6.4).
    UnsupportedCollateral {
        /// The member's name.
        member: String,
        /// The value, verbatim.
        value: String,
    },
    /// Equity is negative. The raw numbers are kept - clamping them to zero would report a healthy
    /// account - and new risk stops (plan §6.4).
    NegativeEquity {
        /// The `marginBalance` the venue sent.
        margin_balance: Decimal,
    },
    /// The venue's own balance members do not satisfy `total = free + locked`, so no Nautilus
    /// balance can be built from them without inventing one of the three.
    BalanceInconsistent {
        /// The mapped total.
        total: Decimal,
        /// The mapped locked amount.
        locked: Decimal,
        /// The mapped free amount.
        free: Decimal,
    },
    /// The balance read carried no member this adapter can map.
    BalanceUnreadable {
        /// Why.
        reason: String,
    },
    /// A submission whose outcome is unknown and not yet resolved (plan §6.3).
    UnknownSubmission {
        /// The client order id the submission was made under.
        client_order_id: ClientOrderId,
        /// When the outcome became unknown.
        first_seen: UnixNanos,
    },
    /// A cancel whose answer was lost and which no read has settled (plan §6.3).
    UnconfirmedCancel {
        /// The client order id the cancel was made under.
        client_order_id: ClientOrderId,
    },
    /// A pass that could not read the account at all.
    ReadFailed {
        /// Why the pass failed.
        reason: String,
    },
    /// Reports the recovery observed and could not apply to the account (plan §6.4).
    ///
    /// Three things are one condition: a report the bounded buffer refused for want of room, a
    /// report held under a recovery this machine has left, and a report the state machine could not
    /// read. Each is a fact about the account this client saw and does not hold, so the account is
    /// not one anybody has verified - the pass that judges it says so here, and the bounded re-read
    /// that follows is what clears it.
    LostReports {
        /// How many reports were not applied.
        count: usize,
        /// Why, as this client saw it.
        reason: String,
    },
}

impl Finding {
    /// Returns whether this finding means the account's state is **not** fully known.
    ///
    /// A readable order this run did not create is not one of these: it is a state the venue
    /// stated. An order of unknown status is, whether this run created it or not - plan §6.3's
    /// table puts both in the column that stops an account being judged clean.
    #[must_use]
    pub fn is_uncertain(&self) -> bool {
        match self {
            Self::ForeignOrder { status, .. } => {
                !status.is_known() || *status == OndoOrderStatus::Untriggered
            }
            _ => true,
        }
    }

    /// Returns a human-readable statement of the finding.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::UnresolvedOrder {
                venue_order_id,
                reason,
                ..
            } => format!("order {venue_order_id} is unresolved: {reason}"),
            Self::UnmappableMarket {
                venue_order_id,
                market,
            } => format!("order {venue_order_id} names the unmappable market `{market}`"),
            Self::ForeignOrder {
                venue_order_id,
                client_order_id,
                market,
                ..
            } => format!(
                "order {venue_order_id} ({}) on {market} was not created by this run",
                client_order_id.as_deref().unwrap_or("no client order id"),
            ),
            Self::UnmappablePosition { market } => {
                format!("position on the unmappable market `{market}`")
            }
            Self::UnreadablePosition { market, direction } => {
                format!("position on {market} has the unknown direction `{direction}`")
            }
            Self::PositionMismatch {
                instrument_id,
                venue,
                expected,
            } => format!(
                "the venue's position on {instrument_id} is {venue} but this client's fills and \
                 baseline imply {expected}"
            ),
            Self::PositionAbsent {
                instrument_id,
                expected,
            } => format!(
                "the venue's position list does not carry {instrument_id}, but this client's fills \
                 and baseline imply {expected}"
            ),
            Self::UnsupportedCollateral { member, value } => format!(
                "the balance member `{member}` = {value} is not one this adapter supports; a \
                 multi-collateral or borrowing account is out of scope"
            ),
            Self::NegativeEquity { margin_balance } => format!(
                "the account's equity is negative ({margin_balance}); the raw balance is kept and \
                 new risk stops"
            ),
            Self::BalanceInconsistent {
                total,
                locked,
                free,
            } => format!(
                "the venue's balance does not add up: total {total} != locked {locked} + free {free}"
            ),
            Self::BalanceUnreadable { reason } => {
                format!("the balance could not be mapped: {reason}")
            }
            Self::UnknownSubmission {
                client_order_id,
                first_seen,
            } => format!(
                "the outcome of submission {client_order_id} has been unknown since {first_seen}"
            ),
            Self::UnconfirmedCancel { client_order_id } => {
                format!("the cancel of {client_order_id} was not confirmed by any venue answer")
            }
            Self::ReadFailed { reason } => format!("the account could not be read: {reason}"),
            Self::LostReports { count, reason } => {
                format!("{count} report(s) the recovery could not apply: {reason}")
            }
        }
    }
}

/// What one pass concluded about the account.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountJudgment {
    /// Everything the pass could not account for, or found that it did not create.
    pub findings: Vec<Finding>,
}

impl AccountJudgment {
    /// Creates a judgment from its findings.
    #[must_use]
    pub fn new(findings: Vec<Finding>) -> Self {
        Self { findings }
    }

    /// Creates a judgment carrying one finding.
    #[must_use]
    pub fn single(finding: Finding) -> Self {
        Self {
            findings: vec![finding],
        }
    }

    /// Returns whether the pass found nothing at all to report.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }

    /// Returns whether the account's state is not fully known, which stops new risk.
    #[must_use]
    pub fn is_uncertain(&self) -> bool {
        self.findings.iter().any(Finding::is_uncertain)
    }

    /// Returns the findings that make the account uncertain.
    pub fn uncertain(&self) -> impl Iterator<Item = &Finding> {
        self.findings
            .iter()
            .filter(|finding| finding.is_uncertain())
    }

    /// Returns the findings as statements, for a log line or a report.
    #[must_use]
    pub fn reasons(&self) -> Vec<String> {
        self.findings.iter().map(Finding::reason).collect()
    }
}

/// One reconciliation pass's reading of the account.
///
/// `applied_net` is what this client's own applied fills add up to, per instrument. It is not a
/// position: the venue's position is compared against it plus the baseline the first pass adopted,
/// which is how a position carried into the run is told apart from one the fills do not explain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountReading {
    /// One entry per order the pass saw - the venue's pages and the stream reports it replayed - in
    /// the order it first saw each one, at the state that pass left it in.
    ///
    /// The state is the **merged** one, not the page's: the newest payload applied to the order is
    /// what its status and its venue filled quantity are read from, whether that payload came from a
    /// page or from the stream. An order only the stream mentioned is listed here too - a report is
    /// evidence about the account whether or not a page repeated it.
    pub orders: Vec<OrderReading>,
    /// One entry per position the venue listed.
    pub positions: Vec<PositionReading>,
    /// The account's balance summary, when the read carried one.
    pub balance: Option<BalanceReading>,
    /// The signed net quantity this client's applied fills add up to, per instrument.
    pub applied_net: BTreeMap<InstrumentId, Decimal>,
    /// The ids of the fills this pass applied, in the order they were applied.
    pub fills: Vec<String>,
}

impl AccountReading {
    /// Returns a fingerprint of the account's **stable** state.
    ///
    /// Two passes converge when their fingerprints agree: the same orders in the same states, the
    /// same positions, the same net of this client's fills.
    ///
    /// Two things are deliberately outside it. The balance is one: `unrealizedPnl`, `marginRatio`
    /// and the rest move with the market between two reads, so including them would mean two
    /// passes could never agree and a recovery could never finish (the balance's own consistency is
    /// judged separately, by [`Self::balance`]'s findings). [`Self::fills`] is the other: it is
    /// what *this pass* applied, not what the account is - the pass that reads a fill first
    /// applies it and the next finds it already applied, and two readings of one unchanging
    /// account would otherwise never be the agreeing pair the plan asks for. The state those fills
    /// produced is in [`Self::applied_net`], which is part of the fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        let mut orders: Vec<String> = self
            .orders
            .iter()
            .map(|order| {
                format!(
                    "{}|{}|{}|{}|{}|{}|{}",
                    order.venue_order_id,
                    order.client_order_id.as_deref().unwrap_or(""),
                    order.market,
                    order.status.as_str(),
                    order
                        .venue_filled
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    order
                        .applied_filled
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    order.tracked,
                )
            })
            .collect();

        orders.sort();

        let mut positions: Vec<String> = self
            .positions
            .iter()
            .map(|position| {
                format!(
                    "{}|{}|{}",
                    position.market,
                    position.direction.as_str(),
                    position.signed
                )
            })
            .collect();

        positions.sort();

        let applied: Vec<String> = self
            .applied_net
            .iter()
            .map(|(instrument_id, quantity)| format!("{instrument_id}|{quantity}"))
            .collect();

        orders.hash(&mut hasher);
        positions.hash(&mut hasher);
        applied.hash(&mut hasher);

        hasher.finish()
    }
}

/// How many private reports the recovery buffer holds before it refuses more.
///
/// The buffer is not a transcript of the account: it carries the reports that arrive while one pass
/// is reading it, and a pass holds it for the seconds its four reads take. The bound is what keeps a
/// stream healthier than the read - or a reconnect loop - from turning this adapter's own memory
/// into the failure. What the bound must never do is drop a report **quietly**: a refusal is counted
/// and reported to the machine, which is what makes the account uncertain until a pass has re-read
/// it (plan §6.4).
pub const ONDO_RECOVERY_BUFFER_CAPACITY: usize = 1024;

/// One buffered payload, with the recovery it was recorded under.
#[derive(Debug)]
struct BufferedReport<T> {
    /// The recovery generation in force when the report arrived
    /// ([`ReconciliationMachine::recovery_generation`]).
    generation: u64,
    /// The payload, exactly as the venue sent it.
    payload: T,
}

/// The reports one drain took, and what it found that it could not take (plan §6.4).
#[derive(Debug, Default)]
pub struct DrainedReports {
    /// The order payloads, oldest first.
    pub orders: Vec<OndoApiOrder>,
    /// The fill payloads, oldest first.
    pub fills: Vec<OndoApiFill>,
    /// How many reports were held under a recovery generation that has since been superseded.
    ///
    /// They are not returned: a report from a recovery this machine has left is a statement about an
    /// account state it is no longer reconciling, and replaying it now would apply a superseded
    /// state as the current one. The count is a loss the caller reports, not a silent discard.
    pub superseded: usize,
}

/// Holds the private reports that arrived while the account was being read (plan §6.4).
///
/// A stream report and the REST history carry the same facts, and either may arrive first. The
/// buffer keeps what the stream delivered while a pass was in flight so the pass can replay it
/// through the same state machine the REST pages go through - where `(account_id, fill.id)` and the
/// order index dedupe it - instead of dropping it or applying it twice.
///
/// What it holds is a **sequence per order**, not a set of orders. `open` with no fill, `open` with
/// a fill, and `canceled` are three facts about one order, and the pass that replays them has to see
/// all three, in the order they arrived: which of them is the order's state is exactly what arrival
/// order decides. Only a payload equal to one already held for that same venue order id is one fact
/// delivered twice, and that is the one [`Self::record_order`] drops. Fills are identified by the
/// venue's own fill id, so they dedupe on it.
#[derive(Debug)]
pub struct ReconciliationBuffer {
    orders: Vec<BufferedReport<OndoApiOrder>>,
    fills: Vec<BufferedReport<OndoApiFill>>,
    /// The payloads held for each venue order id, by their index in `orders`.
    ///
    /// The index is what makes the duplicate check exact without a second copy of the payloads: an
    /// order's report is compared against the reports already held for that order alone, never
    /// against another order's.
    order_indices: BTreeMap<String, Vec<usize>>,
    fill_ids: BTreeSet<String>,
    /// How many reports the buffer holds at most.
    capacity: usize,
    /// How many reports have been refused for want of room since the last [`Self::take_dropped`].
    dropped: usize,
}

impl Default for ReconciliationBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ReconciliationBuffer {
    /// Creates an empty buffer holding up to [`ONDO_RECOVERY_BUFFER_CAPACITY`] reports.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(ONDO_RECOVERY_BUFFER_CAPACITY)
    }

    /// Creates an empty buffer holding up to `capacity` reports.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            orders: Vec::new(),
            fills: Vec::new(),
            order_indices: BTreeMap::new(),
            fill_ids: BTreeSet::new(),
            capacity,
            dropped: 0,
        }
    }

    /// Returns how many reports the buffer holds at most.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Records one order payload, returning whether it was held.
    ///
    /// It is not held when it repeats a payload already held for the same venue order id - one fact
    /// delivered twice - or when the buffer is full. [`Self::take_dropped`] is what tells the two
    /// apart: a repeat is nothing at all, a report refused for want of room is a loss.
    pub fn record_order(&mut self, payload: OndoApiOrder, generation: u64) -> bool {
        if let Some(held) = self.order_indices.get(payload.order_id())
            && held.iter().any(|at| self.orders[*at].payload == payload)
        {
            return false;
        }

        if self.orders.len() + self.fills.len() >= self.capacity {
            self.dropped += 1;

            return false;
        }

        self.order_indices
            .entry(payload.order_id().to_string())
            .or_default()
            .push(self.orders.len());
        self.orders.push(BufferedReport {
            generation,
            payload,
        });

        true
    }

    /// Records one fill payload, returning whether it was held.
    ///
    /// A fill is identified by the venue's own `id`, so a second delivery of one fill is the same
    /// fact and is not held twice.
    pub fn record_fill(&mut self, fill: OndoApiFill, generation: u64) -> bool {
        if self.fill_ids.contains(fill.id()) {
            return false;
        }

        if self.orders.len() + self.fills.len() >= self.capacity {
            self.dropped += 1;

            return false;
        }

        self.fill_ids.insert(fill.id().to_string());
        self.fills.push(BufferedReport {
            generation,
            payload: fill,
        });

        true
    }

    /// Returns how many reports the buffer refused for want of room, and forgets them.
    ///
    /// The count is what the caller reports to the machine. A report the buffer could not hold is a
    /// fact about the account this client saw and does not have, which is a condition the account
    /// has to be judged on rather than a line in a log (plan §6.4).
    pub fn take_dropped(&mut self) -> usize {
        std::mem::take(&mut self.dropped)
    }

    /// Returns how many payloads the buffer holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.orders.len() + self.fills.len()
    }

    /// Returns whether the buffer holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty() && self.fills.is_empty()
    }

    /// Returns how many orders the buffer holds.
    #[must_use]
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Returns how many fills the buffer holds.
    #[must_use]
    pub fn fill_count(&self) -> usize {
        self.fills.len()
    }

    /// Takes everything the buffer holds under `generation`, oldest first, leaving it empty.
    ///
    /// Reports held under another generation are counted in [`DrainedReports::superseded`] and
    /// dropped: they were recorded for a recovery this machine has left, and a pass that replayed
    /// them would be applying a superseded state as the current one (plan §6.4).
    #[must_use]
    pub fn drain_generation(&mut self, generation: u64) -> DrainedReports {
        let mut drained = DrainedReports::default();

        for report in std::mem::take(&mut self.orders) {
            if report.generation == generation {
                drained.orders.push(report.payload);
            } else {
                drained.superseded += 1;
            }
        }

        for report in std::mem::take(&mut self.fills) {
            if report.generation == generation {
                drained.fills.push(report.payload);
            } else {
                drained.superseded += 1;
            }
        }

        self.order_indices.clear();
        self.fill_ids.clear();

        drained
    }
}

/// Which write an [`UncertainOutcome`] is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UncertainKind {
    /// A submission whose answer was lost. Its venue order id is unknown **by construction** -
    /// that is what an unanswered submission is - so the reference a probe uses is always the
    /// client order id it was made under.
    Submission,
    /// A cancel no venue answer has settled. The venue order id is usually known here, because the
    /// order was already resting when the cancel was sent.
    Cancel,
}

/// One write whose effect this client has not settled (plan §6.3).
///
/// A submission whose answer was lost and a cancel no answer has confirmed are the same problem -
/// a write the venue may or may not have applied - so they share one record and one bounded probe.
/// What differs is the reference the probe uses and the map the record lives in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UncertainOutcome {
    /// The client order id the write was made under.
    pub client_order_id: ClientOrderId,
    /// The venue's order id, when this session learned one before the outcome was lost.
    pub venue_order_id: Option<VenueOrderId>,
    /// Which write this is.
    pub kind: UncertainKind,
    /// The reference a probe must use to ask the venue about it.
    ///
    /// A **new** client order id would be a second order, so a probe is never given one: it is
    /// either the `client:{clientOrderId}` form or the venue order id this session already holds.
    pub lookup: String,
    /// Why the outcome is unknown, as this client saw it.
    pub reason: String,
    /// When the outcome became unknown.
    pub first_seen: UnixNanos,
    /// The last probe's instant, when one has been made.
    pub last_probe: Option<UnixNanos>,
    /// When the next probe is due.
    pub next_probe: UnixNanos,
    /// How many probes have been answered.
    pub attempts: u32,
    /// Why the last probe settled nothing, when it did not.
    pub last_probe_reason: Option<String>,
    /// Whether the window has passed and probing has stopped.
    pub abandoned: bool,
}

impl UncertainOutcome {
    /// Creates a record for one unsettled write, due for its first probe immediately.
    #[must_use]
    fn new(
        kind: UncertainKind,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        reason: String,
        now: UnixNanos,
    ) -> Self {
        Self {
            client_order_id,
            venue_order_id,
            kind,
            lookup: lookup_for(kind, client_order_id, venue_order_id),
            reason,
            first_seen: now,
            last_probe: None,
            next_probe: now,
            attempts: 0,
            last_probe_reason: None,
            abandoned: false,
        }
    }
}

/// Returns the reference a probe of one unsettled write uses.
///
/// A submission is asked about under the client order id it was made with. A cancel prefers the
/// venue order id, which is unambiguous, and falls back to the same client-order-id form for an
/// order whose venue id this session never observed (plan §6.2, §6.3).
fn lookup_for(
    kind: UncertainKind,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
) -> String {
    match (kind, venue_order_id) {
        (UncertainKind::Cancel, Some(venue_order_id)) => venue_order_id.to_string(),
        _ => format!("client:{client_order_id}"),
    }
}

/// A submission left to a human after its window expired (plan §6.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbandonedSubmission {
    /// The client order id the submission was made under.
    pub client_order_id: ClientOrderId,
    /// The lookup reference that identifies it to a human or a later read.
    pub lookup: String,
    /// How many probes were made before the window expired.
    pub attempts: u32,
    /// How long the outcome had been unknown.
    pub elapsed_ns: u64,
}

/// What a probe of an unknown submission found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The venue answered with the order, so the submission was applied.
    Found,
    /// The venue answered 404. **Not** proof the request was never applied: plan §6.3 gives a
    /// bounded window of retries for exactly this reason.
    NotFound,
    /// The probe itself could not be answered: a transport failure, a 5xx, an unreadable body.
    Inconclusive {
        /// Why the probe settled nothing.
        reason: String,
    },
}

/// What a probe left to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProbeDisposition {
    /// The venue's answer settled the submission.
    Resolved,
    /// The outcome is still unknown and the window is open.
    KeepProbing {
        /// When the next probe is due.
        next_probe: UnixNanos,
    },
    /// The window expired. Probing stops, the submission stays unknown, and its id is what a human
    /// needs.
    Abandoned,
}

/// One probe's report: which submission was probed and what the probe left to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeReport {
    /// The client order id the submission was made under.
    pub client_order_id: ClientOrderId,
    /// What the probe left to do.
    pub disposition: ProbeDisposition,
}

/// One step of the stop sequence (plan §6.4).
///
/// The order is the requirement: a dead man's switch that is released before this run's orders are
/// cancelled leaves them resting with nothing to cancel them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopStep {
    /// Cancel the orders this run placed.
    CancelOwnOrders,
    /// Confirm each of them reached a terminal state at the venue.
    ConfirmOwnOrders,
    /// Release the account-level switch.
    ReleaseDeadMansSwitch,
    /// Close the private stream.
    ClosePrivateStream,
}

/// The dedup ledger and its watermark, in a form that survives a restart (plan §6.4).
///
/// The ledger is the `(account_id, fill.id)` set a fill is applied once against, and the plan
/// forbids an arbitrary short TTL in its place: a restart that forgot it would count a historical
/// fill a second time. The watermark is the newest applied fill's instant, kept so a restart can
/// say how far the ledger reaches.
///
/// A journal is one account's. Restoring it into another account is refused rather than merged -
/// silently adopting another account's ids would suppress that account's first fills.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerJournal {
    schema_version: u32,
    account_id: String,
    #[serde(default)]
    watermark_ns: Option<u64>,
    #[serde(default)]
    fills: Vec<String>,
}

impl LedgerJournal {
    /// The journal schema this adapter writes and the only one it reads.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Snapshots `ledger`'s entries for `account_id`.
    #[must_use]
    pub fn from_ledger(
        ledger: &OndoFillLedger,
        account_id: AccountId,
        watermark: Option<UnixNanos>,
    ) -> Self {
        let mut fills: Vec<String> = ledger
            .entries()
            .into_iter()
            .filter(|(entry_account_id, _fill_id)| *entry_account_id == account_id)
            .map(|(_account_id, fill_id)| fill_id)
            .collect();

        fills.sort();

        Self {
            schema_version: Self::SCHEMA_VERSION,
            account_id: account_id.to_string(),
            watermark_ns: watermark.map(|value| value.as_u64()),
            fills,
        }
    }

    /// Returns the account this journal belongs to.
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Returns the watermark the journal was written with.
    #[must_use]
    pub fn watermark(&self) -> Option<UnixNanos> {
        self.watermark_ns.map(UnixNanos::from)
    }

    /// Returns how many fills the journal holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fills.len()
    }

    /// Returns whether the journal holds no fills.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fills.is_empty()
    }

    /// Renders the journal as JSON.
    ///
    /// # Errors
    ///
    /// Returns the serializer's error, which for this shape cannot happen.
    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self)
            .map_err(|error| anyhow::anyhow!("the ledger journal could not be serialized: {error}"))
    }

    /// Reads a journal back.
    ///
    /// # Errors
    ///
    /// Returns an error for a body that is not this journal's schema - including a
    /// `schema_version` this adapter does not know, which is refused rather than read as if it were
    /// version 1.
    pub fn from_json(text: &str) -> anyhow::Result<Self> {
        let journal: Self = serde_json::from_str(text)
            .map_err(|error| anyhow::anyhow!("the ledger journal could not be read: {error}"))?;

        if journal.schema_version != Self::SCHEMA_VERSION {
            anyhow::bail!(
                "the ledger journal is schema version {}, and this adapter reads version {}",
                journal.schema_version,
                Self::SCHEMA_VERSION,
            );
        }

        Ok(journal)
    }

    /// Restores this journal into `ledger`.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal belongs to another account. Nothing is restored in that
    /// case: a partially merged ledger is worse than an empty one.
    pub fn restore(
        &self,
        ledger: &mut OndoFillLedger,
        account_id: AccountId,
    ) -> anyhow::Result<usize> {
        if self.account_id != account_id.to_string() {
            anyhow::bail!(
                "the ledger journal belongs to account {} and this client reports for {account_id}",
                self.account_id,
            );
        }

        Ok(ledger.restore(
            self.fills
                .iter()
                .map(|fill_id| (account_id, fill_id.clone())),
        ))
    }
}

/// Why a recovery pass was refused before it read anything (plan §6.4).
///
/// Only one pass may advance an account at a time. Two would both read the venue, both drain the
/// buffer - the second finding it empty - and both conclude, and neither reading would then be the
/// account's: the reports the first drain took would be missing from the second pass's reading and
/// from its judgment, and the account would be judged on a stitched-together account.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryPassRefusal {
    /// Another pass owns this account's recovery.
    #[error("another recovery pass already owns this account")]
    AlreadyRunning,
}

/// One report the recovery could not apply to the account, awaiting the pass that judges it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReportLoss {
    /// How many reports.
    count: usize,
    /// Why, as this client saw it.
    reason: String,
}

/// The reconciliation state machine, the account's judgments and the switch, in one place.
#[derive(Debug)]
pub struct ReconciliationMachine {
    account_id: AccountId,
    state: ReconciliationState,
    session_established: bool,
    metadata: MetadataValidity,
    dms: DeadMansSwitch,
    unknown: BTreeMap<ClientOrderId, UncertainOutcome>,
    unconfirmed_cancels: BTreeMap<ClientOrderId, UncertainOutcome>,
    generation: u64,
    recovery_generation: u64,
    lost_reports: Vec<ReportLoss>,
    baseline: BTreeMap<InstrumentId, Decimal>,
    baseline_adopted: bool,
    confirmations: usize,
    last_fingerprint: Option<u64>,
    last_reading: Option<AccountReading>,
    last_judgment: Option<AccountJudgment>,
    last_balance: Option<MappedBalance>,
}

impl ReconciliationMachine {
    /// Creates a machine for `account_id` with a switch timeout of `timeout_seconds`.
    #[must_use]
    pub fn new(account_id: AccountId, timeout_seconds: u64) -> Self {
        Self {
            account_id,
            state: ReconciliationState::Disconnected,
            session_established: false,
            metadata: MetadataValidity::Stale {
                reason: "no instrument metadata has been read yet".to_string(),
            },
            dms: DeadMansSwitch::new(timeout_seconds),
            unknown: BTreeMap::new(),
            unconfirmed_cancels: BTreeMap::new(),
            generation: 0,
            recovery_generation: 0,
            lost_reports: Vec::new(),
            baseline: BTreeMap::new(),
            baseline_adopted: false,
            confirmations: 0,
            last_fingerprint: None,
            last_reading: None,
            last_judgment: None,
            last_balance: None,
        }
    }

    /// Returns the account this machine reconciles.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the state.
    #[must_use]
    pub const fn state(&self) -> ReconciliationState {
        self.state
    }

    /// Returns whether a session has been established at any point in this process's life.
    ///
    /// This is an observation, not a condition of anything. It used to excuse a machine that had
    /// never held a session from governing new risk, on the reasoning that there was no venue state
    /// to be uncertain about yet; that reasoning was wrong. A venue state nobody has read is the
    /// least verified state there is, and the entrance check is what stops an order against it.
    #[must_use]
    pub const fn session_established(&self) -> bool {
        self.session_established
    }

    /// Returns the one decision that governs new risk (plan §6.4, Task 8).
    ///
    /// Fail-closed, and total: `Granted` for exactly one combination - [`ReconciliationState::Ready`]
    /// with metadata this client can trade on, a switch that permits orders, and nothing left
    /// unsettled - and [`Admission::Refused`] for every other, including the machine that has never
    /// held a session.
    #[must_use]
    pub fn admission(&self) -> Admission {
        match self.new_risk_refusal() {
            None => Admission::Granted {
                generation: self.generation,
            },
            Some(reason) => Admission::Refused { reason },
        }
    }

    /// Re-verifies an admission permit immediately before a request is sent.
    ///
    /// The account is checked again rather than trusted, so a state that changed while a command
    /// queued is caught, and the permit's generation is checked with it: a permit is good only for
    /// the generation that issued it. The events that move the generation are exactly the ones that
    /// revoke permission ([`Self::invalidate_admissions`]), so a permit whose generation is stale
    /// was issued before something this client has since learned, and acting on it would act on a
    /// decision that is no longer the current one - even when the account is admissible again by
    /// the time the request would go out.
    #[must_use]
    pub fn revalidate(&self, permit: &Admission) -> Admission {
        let current = self.admission();

        if let Admission::Granted { generation } = permit
            && current.generation() == Some(*generation)
        {
            return current;
        }

        Admission::Refused {
            reason: match current {
                Admission::Refused { reason } => reason,
                Admission::Granted { .. } => NewRiskRefusal::Superseded,
            },
        }
    }

    /// Returns whether a **new** order may be submitted.
    ///
    /// [`Self::admission`]'s `Granted`, as a predicate.
    #[must_use]
    pub fn can_submit_new_orders(&self) -> bool {
        self.admission().is_granted()
    }

    /// Returns whether this client must refuse a new order right now.
    ///
    /// The negation of [`Self::can_submit_new_orders`], and it holds from construction: a client
    /// that has established nothing has verified nothing, so there is no state a new order could be
    /// placed against.
    #[must_use]
    pub fn refuses_new_risk(&self) -> bool {
        !self.can_submit_new_orders()
    }

    /// Returns why a new order may not be submitted, or [`None`] when it may be.
    ///
    /// The conditions are checked in the order they are most useful to report. An outcome this
    /// client has left unsettled comes first: it is the one that is specific to this run and the
    /// one a caller has to chase. The account's own state comes next, because it is the headline -
    /// a disconnected machine is disconnected whatever else is true of it - and the two conditions
    /// that qualify an otherwise tradable account - metadata that cannot price an order, a switch
    /// that does not permit one - come last.
    #[must_use]
    pub fn new_risk_refusal(&self) -> Option<NewRiskRefusal> {
        if !self.unknown.is_empty() {
            return Some(NewRiskRefusal::UnknownSubmissions {
                client_order_ids: self.unknown.keys().copied().collect(),
            });
        }

        if !self.unconfirmed_cancels.is_empty() {
            return Some(NewRiskRefusal::UnconfirmedCancels {
                client_order_ids: self.unconfirmed_cancels.keys().copied().collect(),
            });
        }

        if self.state != ReconciliationState::Ready {
            return Some(NewRiskRefusal::AccountState(self.state));
        }

        if let MetadataValidity::Stale { reason } = &self.metadata {
            return Some(NewRiskRefusal::MetadataStale {
                reason: reason.clone(),
            });
        }

        if !self.dms.permits_new_orders() {
            return Some(NewRiskRefusal::DeadMansSwitch(self.dms.state()));
        }

        None
    }

    /// Invalidates every admission permit issued before this point.
    ///
    /// The generation is a count of the events that revoke permission, not of every change: the
    /// steady-state transitions - a pass that reads an already-[`ReconciliationState::Ready`]
    /// account again, a renewal of an armed switch, the settlement of an unknown outcome - leave it
    /// alone, because a permit issued before them is still the current decision.
    fn invalidate_admissions(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    /// Returns how many consecutive agreeing passes the machine has seen.
    #[must_use]
    pub const fn confirmations(&self) -> usize {
        self.confirmations
    }

    /// Returns the last pass's reading.
    #[must_use]
    pub const fn last_reading(&self) -> Option<&AccountReading> {
        self.last_reading.as_ref()
    }

    /// Returns the last pass's judgment.
    #[must_use]
    pub const fn last_judgment(&self) -> Option<&AccountJudgment> {
        self.last_judgment.as_ref()
    }

    /// Returns the last pass's balance mapping.
    #[must_use]
    pub const fn last_balance(&self) -> Option<&MappedBalance> {
        self.last_balance.as_ref()
    }

    /// Returns the position baseline the first pass adopted, per instrument.
    #[must_use]
    pub const fn position_baseline(&self) -> &BTreeMap<InstrumentId, Decimal> {
        &self.baseline
    }

    /// Returns whether the metadata this client trades on is usable.
    #[must_use]
    pub const fn metadata(&self) -> &MetadataValidity {
        &self.metadata
    }

    /// Records whether the metadata this client trades on is usable.
    ///
    /// Metadata becoming usable only widens what is permitted, so it invalidates nothing; metadata
    /// that stops being usable revokes every permit issued while it was current.
    pub fn set_metadata(&mut self, validity: MetadataValidity) {
        if !validity.is_usable() {
            self.invalidate_admissions();
        }

        self.metadata = validity;
    }

    /// Returns the dead man's switch.
    #[must_use]
    pub const fn dead_mans_switch(&self) -> &DeadMansSwitch {
        &self.dms
    }

    /// Returns the dead man's switch for arming, renewing or releasing.
    pub const fn dead_mans_switch_mut(&mut self) -> &mut DeadMansSwitch {
        &mut self.dms
    }

    /// Begins a recovery: the account is about to be read, and new risk stops until it converges.
    ///
    /// A recovery already under way keeps its confirmations, so the second of the two agreeing
    /// passes is what a caller repeating this method cannot reset by accident.
    ///
    /// A recovery that **starts** is a different read of the account from the one it replaces, so
    /// the reports still held for the previous one stop being statements about the account this
    /// machine is reconciling ([`Self::recovery_generation`]). Repeating the call while a recovery
    /// is already under way is not a new recovery - it is the same one, whose confirmations it
    /// deliberately leaves alone - and it does not supersede anything either.
    pub fn begin_recovery(&mut self, _now: UnixNanos) {
        if self.state != ReconciliationState::Recovering {
            self.confirmations = 0;
            self.last_fingerprint = None;
            self.recovery_generation = self.recovery_generation.wrapping_add(1);
        }

        self.session_established = true;
        self.state = ReconciliationState::Recovering;
        self.invalidate_admissions();
    }

    /// Returns the recovery a private report recorded now belongs to.
    ///
    /// It is **not** the admission generation: that one counts every event that revokes a permit,
    /// including ones that say nothing about whether a buffered report is still a statement about
    /// this account. This one changes exactly when a report recorded earlier stops being one - when
    /// a recovery begins, and when the session ends - which is what a buffered report is checked
    /// against before it is replayed (plan §6.4).
    #[must_use]
    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }

    /// Records reports the recovery could not apply to the account (plan §6.4).
    ///
    /// The account becomes [`ReconciliationState::Uncertain`] here and now, and stays un-Ready until
    /// a pass has read it whole again: reports this client observed and does not hold are exactly
    /// the state it cannot vouch for, and a machine that waited for the next pass to notice would
    /// leave a window in which a converged account is treated as read. The next pass judges the
    /// loss with everything else, which is what makes the re-read that clears it a bounded one.
    pub fn note_lost_reports(&mut self, count: usize, reason: String) {
        self.lost_reports.push(ReportLoss { count, reason });
        self.state = ReconciliationState::Uncertain;
        self.confirmations = 0;
        self.last_fingerprint = None;
        self.invalidate_admissions();
    }

    /// Records that the session ended.
    ///
    /// The state becomes [`ReconciliationState::Disconnected`], and because a session has been
    /// established this machine keeps refusing new risk until a recovery converges again.
    pub fn note_disconnected(&mut self, _now: UnixNanos) {
        self.state = ReconciliationState::Disconnected;
        self.confirmations = 0;
        self.last_fingerprint = None;
        self.invalidate_admissions();
        // The session ended. A report still held was recorded for the session that ended, and the
        // recovery that follows reads the account rather than replaying it (plan §6.4).
        self.recovery_generation = self.recovery_generation.wrapping_add(1);
    }

    /// Records a cancel no venue answer has settled.
    ///
    /// A cancel API call is not a cancel (plan §6.3), so the caller registers this **before** it
    /// runs the confirming query: a query that fails then leaves the cancel outstanding rather than
    /// leaving nothing behind at all. Registration stops new risk immediately - the account is not
    /// tradable while an order's state is one no answer has stated.
    pub fn note_unconfirmed_cancel(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        reason: String,
        now: UnixNanos,
    ) {
        if self.unconfirmed_cancels.contains_key(&client_order_id) {
            return;
        }

        self.unconfirmed_cancels.insert(
            client_order_id,
            UncertainOutcome::new(
                UncertainKind::Cancel,
                client_order_id,
                venue_order_id,
                reason,
                now,
            ),
        );
        self.invalidate_admissions();
    }

    /// Returns the cancels no venue answer has settled, in client order id order.
    #[must_use]
    pub fn unconfirmed_cancels(&self) -> Vec<UncertainOutcome> {
        self.unconfirmed_cancels.values().cloned().collect()
    }

    /// Records that a venue answer settled a cancel.
    ///
    /// Returns whether the cancel was outstanding.
    pub fn confirm_cancel(&mut self, client_order_id: &ClientOrderId) -> bool {
        self.unconfirmed_cancels.remove(client_order_id).is_some()
    }

    /// Records a submission whose outcome became unknown.
    ///
    /// No venue order id is recorded because there is none to record: an unanswered submission is
    /// exactly one whose venue order id this session never observed. Registration stops new risk
    /// immediately, without waiting for a reconciliation pass to notice (plan §6.3).
    pub fn note_unknown_submission(
        &mut self,
        client_order_id: ClientOrderId,
        reason: String,
        now: UnixNanos,
    ) {
        if self.unknown.contains_key(&client_order_id) {
            return;
        }

        self.unknown.insert(
            client_order_id,
            UncertainOutcome::new(
                UncertainKind::Submission,
                client_order_id,
                None,
                reason,
                now,
            ),
        );
        self.invalidate_admissions();
    }

    /// Returns the submissions whose outcome is unknown, in client order id order.
    #[must_use]
    pub fn unknown_submissions(&self) -> Vec<UncertainOutcome> {
        self.unknown.values().cloned().collect()
    }

    /// Returns the record one unsettled write is held under, whichever map holds it.
    #[must_use]
    pub fn uncertain_outcome(&self, client_order_id: &ClientOrderId) -> Option<&UncertainOutcome> {
        self.unknown
            .get(client_order_id)
            .or_else(|| self.unconfirmed_cancels.get(client_order_id))
    }

    /// Returns the writes a probe is due for at `now`: unsettled submissions and unsettled cancels.
    ///
    /// An abandoned outcome is never due: plan §6.3 stops the probe once the window passes, and a
    /// probe that kept going would be a busy loop against the rate-limited venue rather than a
    /// reconciliation.
    #[must_use]
    pub fn probe_due(&self, now: UnixNanos) -> Vec<ClientOrderId> {
        self.unknown
            .values()
            .chain(self.unconfirmed_cancels.values())
            .filter(|outcome| !outcome.abandoned && now >= outcome.next_probe)
            .filter(|outcome| !past_window(outcome, now))
            .map(|outcome| outcome.client_order_id)
            .collect()
    }

    /// Applies one probe's outcome.
    ///
    /// `Found` settles the outcome, whichever map holds it: the venue's answer is the answer to
    /// both "was the submission applied" and "did the cancel take". `NotFound` and an inconclusive
    /// probe settle nothing: the outcome stays unsettled until the window expires or a later probe
    /// finds the order.
    pub fn note_probe(
        &mut self,
        client_order_id: &ClientOrderId,
        outcome: ProbeOutcome,
        now: UnixNanos,
    ) -> ProbeDisposition {
        let reason = match outcome {
            ProbeOutcome::Found => {
                self.unknown.remove(client_order_id);
                self.unconfirmed_cancels.remove(client_order_id);

                return ProbeDisposition::Resolved;
            }
            // A 404 is recorded as what it is - an answer that settles nothing - rather than as
            // the order not existing (plan §6.3).
            ProbeOutcome::NotFound => {
                "the venue answered 404, which does not settle it".to_string()
            }
            ProbeOutcome::Inconclusive { reason } => reason,
        };

        let Some(submission) = self
            .unknown
            .get_mut(client_order_id)
            .or_else(|| self.unconfirmed_cancels.get_mut(client_order_id))
        else {
            return ProbeDisposition::Abandoned;
        };

        submission.attempts += 1;
        submission.last_probe = Some(now);
        submission.last_probe_reason = Some(reason);
        submission.next_probe =
            UnixNanos::from(now.as_u64() + ONDO_SUBMISSION_PROBE_INTERVAL * 1_000_000_000);

        if past_window(submission, now) {
            submission.abandoned = true;

            return ProbeDisposition::Abandoned;
        }

        ProbeDisposition::KeepProbing {
            next_probe: submission.next_probe,
        }
    }

    /// Expires every unsettled outcome whose window has passed, returning the abandoned ones.
    ///
    /// An abandoned outcome is **not** discarded: the write it describes may still have been
    /// applied, so it keeps blocking new risk and stays in the record a human has to reconcile. What
    /// ends here is the probing.
    pub fn expire_unknown_submissions(&mut self, now: UnixNanos) -> Vec<AbandonedSubmission> {
        let mut abandoned = Vec::new();

        for outcome in self
            .unknown
            .values_mut()
            .chain(self.unconfirmed_cancels.values_mut())
        {
            if outcome.abandoned || !past_window(outcome, now) {
                continue;
            }

            outcome.abandoned = true;

            abandoned.push(AbandonedSubmission {
                client_order_id: outcome.client_order_id,
                lookup: outcome.lookup.clone(),
                attempts: outcome.attempts,
                elapsed_ns: now.as_u64().saturating_sub(outcome.first_seen.as_u64()),
            });
        }

        abandoned
    }

    /// Clears an unknown submission that has been settled outside this machine.
    ///
    /// The caller is the venue's answer, or a human reconciling the account by hand. Nothing inside
    /// this adapter clears one on its own: the order was neither confirmed nor denied. An
    /// unconfirmed cancel is settled the same way through [`Self::confirm_cancel`], which is the
    /// same statement - a venue answer, or a human's, stating what became of the order.
    pub fn clear_unknown_submission(
        &mut self,
        client_order_id: &ClientOrderId,
    ) -> Option<UncertainOutcome> {
        self.unknown.remove(client_order_id)
    }

    /// Records that the dead man's switch fired.
    ///
    /// The venue cancelled the account's resting orders. What that leaves behind is **unverified**:
    /// the orders this client tracked have to be re-read, and no position is closed by a switch
    /// (plan §6.4), so the account goes [`ReconciliationState::Uncertain`] and stays there until a
    /// pass reads it clean again.
    pub fn note_switch_fired(&mut self, now: UnixNanos) {
        self.dms.note_fired(now);
        self.state = ReconciliationState::Uncertain;
        self.confirmations = 0;
        self.last_fingerprint = None;
        self.invalidate_admissions();
    }

    /// Records that a pass could not read the account, and judges it accordingly.
    pub fn note_pass_failed(&mut self, reason: String, _now: UnixNanos) -> ReconciliationState {
        let judgment = AccountJudgment::single(Finding::ReadFailed { reason });

        self.last_judgment = Some(judgment);
        self.state = ReconciliationState::Uncertain;
        self.confirmations = 0;
        self.last_fingerprint = None;
        self.invalidate_admissions();

        self.state
    }

    /// Judges a reading and records it, without concluding a pass.
    ///
    /// This is the pure half of [`Self::conclude_pass`]: the same judgments, made from a reading
    /// that was constructed rather than read.
    pub fn evaluate(&mut self, reading: &AccountReading) -> AccountJudgment {
        let mut findings = self.judge_orders(reading);

        findings.extend(self.judge_positions(reading));
        findings.extend(self.judge_balance(reading));

        for submission in self.unknown.values() {
            findings.push(Finding::UnknownSubmission {
                client_order_id: submission.client_order_id,
                first_seen: submission.first_seen,
            });
        }

        for client_order_id in self.unconfirmed_cancels.keys() {
            findings.push(Finding::UnconfirmedCancel {
                client_order_id: *client_order_id,
            });
        }

        for loss in &self.lost_reports {
            findings.push(Finding::LostReports {
                count: loss.count,
                reason: loss.reason.clone(),
            });
        }

        let judgment = AccountJudgment::new(findings);

        self.last_reading = Some(reading.clone());
        self.last_judgment = Some(judgment.clone());

        judgment
    }

    /// Concludes one pass and returns the state it leaves the machine in.
    ///
    /// The transition rules, in full:
    ///
    /// - a judgment with an uncertain finding leaves the machine
    ///   [`ReconciliationState::Uncertain`], whatever the readings agreed on;
    /// - otherwise, a reading that repeats the previous one confirms it, and
    ///   [`ONDO_RECONCILE_CONFIRMATIONS`] consecutive confirmations make the machine
    ///   [`ReconciliationState::Ready`];
    /// - a reading that differs from the previous one is a first confirmation, not a second, and
    ///   leaves the machine [`ReconciliationState::Recovering`];
    /// - a machine that was already [`ReconciliationState::Ready`] and reads the account clean
    ///   again stays ready: an account that trades is not an account that is unrecovered.
    pub fn conclude_pass(
        &mut self,
        reading: &AccountReading,
        _now: UnixNanos,
    ) -> ReconciliationState {
        let was_ready = self.state == ReconciliationState::Ready;
        let judgment = self.evaluate(reading);

        // The loss has been judged. What keeps the account uncertain from here is that judgment -
        // the pass that carried it is the one that has to be repeated - so the record of it is
        // cleared rather than kept, which would make it a condition nothing could ever clear.
        self.lost_reports.clear();

        if judgment.is_uncertain() {
            self.confirmations = 0;
            self.last_fingerprint = None;
            self.state = ReconciliationState::Uncertain;
            self.invalidate_admissions();

            return self.state;
        }

        let fingerprint = reading.fingerprint();
        let previous = self.last_fingerprint.replace(fingerprint);

        self.confirmations = match previous {
            Some(previous) if previous == fingerprint => self.confirmations + 1,
            _ => 1,
        };

        self.state = if was_ready || self.confirmations >= ONDO_RECONCILE_CONFIRMATIONS {
            ReconciliationState::Ready
        } else {
            ReconciliationState::Recovering
        };

        // A pass that leaves the account tradable revokes nothing: the permit a caller holds was
        // issued under this same decision, and a periodic pass reading an unchanged account is not
        // a reason to refuse a submission the strategy has already made. Any other outcome
        // invalidates, whether or not the state was Ready before it.
        if self.state != ReconciliationState::Ready {
            self.invalidate_admissions();
        }

        self.state
    }

    /// Judges the orders one reading listed.
    fn judge_orders(&self, reading: &AccountReading) -> Vec<Finding> {
        let mut findings = Vec::new();

        for order in &reading.orders {
            if market_to_instrument_id(&order.market).is_err() {
                findings.push(Finding::UnmappableMarket {
                    venue_order_id: order.venue_order_id.clone(),
                    market: order.market.clone(),
                });

                continue;
            }

            if !order.tracked {
                // An order this run did not create is identified, never adopted and never
                // cancelled. A readable status is a known state; an unreadable one is not, and
                // plan §6.3 keeps such an account from being judged clean.
                findings.push(Finding::ForeignOrder {
                    venue_order_id: order.venue_order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    market: order.market.clone(),
                    status: order.status.clone(),
                });

                continue;
            }

            if !order.status.is_known() || order.status == OndoOrderStatus::Untriggered {
                findings.push(Finding::UnresolvedOrder {
                    venue_order_id: order.venue_order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    reason: format!(
                        "the venue's status `{}` is not one this adapter can account for",
                        order.status.as_str()
                    ),
                });

                continue;
            }

            if !order.status.is_terminal() {
                // A working order whose fills lag is simply working: the fills are still arriving.
                continue;
            }

            // A terminal status is only terminal once the fills agree with it (plan §6.3), and a
            // terminal status whose filled quantity cannot be read agrees with nothing.
            let Some(venue_filled) = order.venue_filled else {
                findings.push(Finding::UnresolvedOrder {
                    venue_order_id: order.venue_order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    reason: format!(
                        "the venue reports a {} order with no readable filledSize, so the applied \
                         fills cannot be checked against it",
                        order.status.as_str(),
                    ),
                });

                continue;
            };

            if Some(venue_filled) != order.applied_filled {
                findings.push(Finding::UnresolvedOrder {
                    venue_order_id: order.venue_order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    reason: format!(
                        "the venue reports filledSize {venue_filled} on a {} order but the applied \
                         fills total {}",
                        order.status.as_str(),
                        order
                            .applied_filled
                            .map_or_else(|| "nothing".to_string(), |value| value.to_string()),
                    ),
                });
            }
        }

        findings
    }

    /// Judges the positions one reading carries, against the baseline and this client's fills.
    ///
    /// The comparison is `venue == baseline + applied`, and it is made over the **union** of the
    /// instruments the reading names: the ones the venue listed, the ones this client's fills have
    /// touched, and the ones the baseline still holds. A loop over the listed rows alone judges only
    /// what the venue chose to mention, which leaves the two halves of a position this adapter can
    /// be wrong about unjudged - a position it carried in and the venue no longer lists, and a
    /// position its own fills built that the venue never listed.
    ///
    /// The baseline is adopted **once**, by the first pass that reads the account whole, as the
    /// venue's position less this client's fills at that moment: whatever the account held when this
    /// run started is the starting point. Adopting per instrument instead would let a position that
    /// appears later explain itself away, which is exactly the disagreement this judgment exists to
    /// catch - so only a read that could be read whole adopts, and until one has, a pass judges
    /// nothing about positions (plan §6.4).
    ///
    /// An instrument the venue's complete list does not carry is the venue stating the position is
    /// flat, which is a statement and not a silence (plan §6.4 resolves this endpoint as the whole
    /// set of open positions). What it is compared against is `venue == 0`, and a baseline entry the
    /// venue's flat statement contradicts is retired rather than kept: an expectation nothing can
    /// ever reconcile or retire is what would leave the account uncertain for good over a position
    /// the venue has already closed.
    fn judge_positions(&mut self, reading: &AccountReading) -> Vec<Finding> {
        let mut findings = Vec::new();

        // What the venue stated, per instrument, for the rows this adapter can read.
        let mut listed: BTreeMap<InstrumentId, Decimal> = BTreeMap::new();
        // Whether this response is one the venue documents as the complete set of open positions.
        // Every row it does carry has to map to an instrument and state a direction this adapter
        // can read for that to hold; a row that does not is a gap in the list, and a gap is not
        // evidence that the positions the list does not mention are gone.
        let mut covered = true;

        for position in &reading.positions {
            let Some(instrument_id) = position.instrument_id else {
                findings.push(Finding::UnmappablePosition {
                    market: position.market.clone(),
                });

                covered = false;

                continue;
            };

            if let PositionDirection::Unknown(direction) = &position.direction {
                findings.push(Finding::UnreadablePosition {
                    market: position.market.clone(),
                    direction: direction.clone(),
                });

                covered = false;

                continue;
            }

            listed.insert(instrument_id, position.signed);
        }

        if !self.baseline_adopted {
            if !covered {
                // Nothing is calibrated and this read cannot calibrate it: an instrument whose row
                // could not be read would be left out of the baseline for good, and every later
                // pass would then report the position it was carrying as one the fills do not
                // explain. The pass has said what it could not read, which is what makes the
                // account uncertain; the next whole read is what adopts.
                return findings;
            }

            for (instrument_id, venue) in &listed {
                let applied = reading
                    .applied_net
                    .get(instrument_id)
                    .copied()
                    .unwrap_or(Decimal::ZERO);

                self.baseline.insert(*instrument_id, *venue - applied);
            }

            self.baseline_adopted = true;
        }

        let mut instruments: BTreeSet<InstrumentId> = self.baseline.keys().copied().collect();

        instruments.extend(reading.applied_net.keys().copied());
        instruments.extend(listed.keys().copied());

        for instrument_id in instruments {
            let applied = reading
                .applied_net
                .get(&instrument_id)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let baseline = self
                .baseline
                .get(&instrument_id)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let expected = baseline + applied;

            if let Some(venue) = listed.get(&instrument_id).copied() {
                if expected != venue {
                    findings.push(Finding::PositionMismatch {
                        instrument_id,
                        venue,
                        expected,
                    });
                }
            } else if covered && expected != Decimal::ZERO {
                // The venue's complete list does not carry this instrument and this client expects
                // the account to hold something on it. The disagreement is reported here, and the
                // carried-in position the baseline still asserts is retired with it: the
                // expectation from here on is what this client's own fills say. An entry the venue
                // already agrees with is left where it is - a carried-in position this client's own
                // fills closed out needs that entry to net the venue's flat, and retiring it would
                // turn the closing fills into an unexplained net no later pass could clear.
                findings.push(Finding::PositionAbsent {
                    instrument_id,
                    expected,
                });

                // Retired means **removed**, not zeroed. A zero entry is not a baseline this pass
                // adopted; it is the same stale claim as the entry being retired, kept in a map
                // that grows once per instrument ever seen (`position_baseline` hands the map out).
                // Nothing is lost by removing it: every read of `baseline` here is a `.get(...)`
                // that falls back to zero, so an absent key and a zero entry judge identically -
                // only a key that is present can be mistaken for a claim.
                self.baseline.remove(&instrument_id);
            }
        }

        findings
    }

    /// Maps the balance one reading carried, judging it as it goes.
    fn judge_balance(&mut self, reading: &AccountReading) -> Vec<Finding> {
        self.last_balance = None;

        let Some(balance) = reading.balance.as_ref() else {
            return vec![Finding::BalanceUnreadable {
                reason: "the pass carried no balance".to_string(),
            }];
        };

        let mut findings = Vec::new();

        for (member, value) in &balance.unmapped {
            findings.push(Finding::UnsupportedCollateral {
                member: member.clone(),
                value: value.clone(),
            });
        }

        let (Some(total), Some(free)) = (balance.margin_balance, balance.available_margin) else {
            findings.push(Finding::BalanceUnreadable {
                reason: "the venue sent no readable `marginBalance` and `availableMargin`"
                    .to_string(),
            });

            return findings;
        };

        let locked = balance.used_margin.unwrap_or(Decimal::ZERO);

        if total < Decimal::ZERO {
            findings.push(Finding::NegativeEquity {
                margin_balance: total,
            });
        }

        if total != locked + free {
            findings.push(Finding::BalanceInconsistent {
                total,
                locked,
                free,
            });

            return findings;
        }

        self.last_balance = Some(MappedBalance {
            reading: balance.clone(),
            total,
            locked,
            free,
        });

        findings
    }
}

/// Returns whether an unsettled outcome's window has passed at `now` (plan §6.3).
fn past_window(outcome: &UncertainOutcome, now: UnixNanos) -> bool {
    now.as_u64().saturating_sub(outcome.first_seen.as_u64())
        >= ONDO_SUBMISSION_UNKNOWN_SECS * 1_000_000_000
}

/// Reads one balance member as a decimal, keeping the venue's lexeme.
///
/// # Errors
///
/// Returns an error for a member the venue sent that is not a decimal string.
pub fn balance_member(value: &str, member: &str) -> anyhow::Result<Decimal> {
    parse_decimal(value, member)
}
