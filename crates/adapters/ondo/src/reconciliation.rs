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
//! 1. [`ReconciliationMachine`] - the account's state, and the one predicate that decides whether
//!    new risk may be taken ([`ReconciliationMachine::can_submit_new_orders`]).
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
use nautilus_model::identifiers::{AccountId, ClientOrderId, InstrumentId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    common::parse::{market_to_instrument_id, parse_decimal},
    execution::OndoFillLedger,
    http::orders::OndoOrderStatus,
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
    /// One entry per order the venue listed, in the order the pages returned them.
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

/// Holds the private reports that arrived while the account was being read (plan §6.4).
///
/// A stream report and the REST history carry the same facts, and either may arrive first. The
/// buffer keeps what the stream delivered while a pass was in flight so the pass can replay it
/// through the same state machine the REST pages go through - where `(account_id, fill.id)` and the
/// order index dedupe it - instead of dropping it or applying it twice.
#[derive(Debug, Default)]
pub struct ReconciliationBuffer {
    orders: Vec<crate::http::orders::OndoApiOrder>,
    fills: Vec<crate::http::private::OndoApiFill>,
    order_ids: BTreeSet<String>,
    fill_ids: BTreeSet<String>,
}

impl ReconciliationBuffer {
    /// Creates an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one order payload, returning `false` when its venue order id is already held.
    pub fn record_order(&mut self, payload: crate::http::orders::OndoApiOrder) -> bool {
        if !self.order_ids.insert(payload.order_id().to_string()) {
            return false;
        }

        self.orders.push(payload);

        true
    }

    /// Records one fill payload, returning `false` when its fill id is already held.
    pub fn record_fill(&mut self, fill: crate::http::private::OndoApiFill) -> bool {
        if !self.fill_ids.insert(fill.id().to_string()) {
            return false;
        }

        self.fills.push(fill);

        true
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

    /// Takes everything the buffer holds, oldest first, leaving it empty.
    #[must_use]
    pub fn drain(
        &mut self,
    ) -> (
        Vec<crate::http::orders::OndoApiOrder>,
        Vec<crate::http::private::OndoApiFill>,
    ) {
        self.order_ids.clear();
        self.fill_ids.clear();

        (
            std::mem::take(&mut self.orders),
            std::mem::take(&mut self.fills),
        )
    }
}

/// A submission whose outcome is unknown (plan §6.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownSubmission {
    /// The client order id the submission was made under, which is the one a probe uses.
    pub client_order_id: ClientOrderId,
    /// The lookup reference a probe must use: `client:{clientOrderId}`.
    ///
    /// A **new** client order id would be a second order, so this is the only reference the probe
    /// is ever given.
    pub lookup: String,
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

/// The reconciliation state machine, the account's judgments and the switch, in one place.
#[derive(Debug)]
pub struct ReconciliationMachine {
    account_id: AccountId,
    state: ReconciliationState,
    session_established: bool,
    metadata: MetadataValidity,
    dms: DeadMansSwitch,
    unknown: BTreeMap<ClientOrderId, UnknownSubmission>,
    unconfirmed_cancels: BTreeMap<ClientOrderId, UnixNanos>,
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
    /// A machine that has never held a session does not govern new risk: there is no venue state it
    /// could be uncertain about, and the client's own local refusals are all there is to apply. A
    /// session that has begun - and then been disconnected, however long ago - is governed for the
    /// rest of the process's life, because the venue state it established is now unverified.
    #[must_use]
    pub const fn session_established(&self) -> bool {
        self.session_established
    }

    /// Returns whether a **new** order may be submitted (plan Task 8).
    ///
    /// Fail-closed: this is `true` only for [`ReconciliationState::Ready`], with metadata this
    /// client can trade on and a switch that permits orders. Every other combination - recovering,
    /// uncertain, disconnected, stale metadata, a required switch that is not armed - answers
    /// `false`.
    #[must_use]
    pub fn can_submit_new_orders(&self) -> bool {
        self.state == ReconciliationState::Ready
            && self.metadata.is_usable()
            && self.dms.permits_new_orders()
    }

    /// Returns whether this client must refuse a new order right now.
    ///
    /// [`Self::can_submit_new_orders`] once a session exists. Before any session the machine does
    /// not govern: this is the one difference between the two, and it is deliberate - the predicate
    /// answers a question about a recovered account, and there is no account yet.
    #[must_use]
    pub fn refuses_new_risk(&self) -> bool {
        self.session_established && !self.can_submit_new_orders()
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
    pub fn set_metadata(&mut self, validity: MetadataValidity) {
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
    pub fn begin_recovery(&mut self, _now: UnixNanos) {
        if self.state != ReconciliationState::Recovering {
            self.confirmations = 0;
            self.last_fingerprint = None;
        }

        self.session_established = true;
        self.state = ReconciliationState::Recovering;
    }

    /// Records that the session ended.
    ///
    /// The state becomes [`ReconciliationState::Disconnected`], and because a session has been
    /// established this machine keeps refusing new risk until a recovery converges again.
    pub fn note_disconnected(&mut self, _now: UnixNanos) {
        self.state = ReconciliationState::Disconnected;
        self.confirmations = 0;
        self.last_fingerprint = None;
    }

    /// Records the frames to send for an unconfirmed cancel.
    pub fn note_unconfirmed_cancel(&mut self, client_order_id: ClientOrderId, now: UnixNanos) {
        self.unconfirmed_cancels
            .entry(client_order_id)
            .or_insert(now);
    }

    /// Returns the cancels no venue answer has settled.
    #[must_use]
    pub fn unconfirmed_cancels(&self) -> Vec<ClientOrderId> {
        self.unconfirmed_cancels.keys().copied().collect()
    }

    /// Records that a venue answer settled a cancel.
    ///
    /// Returns whether the cancel was outstanding.
    pub fn confirm_cancel(&mut self, client_order_id: &ClientOrderId) -> bool {
        self.unconfirmed_cancels.remove(client_order_id).is_some()
    }

    /// Records a submission whose outcome became unknown.
    pub fn note_unknown_submission(&mut self, client_order_id: ClientOrderId, now: UnixNanos) {
        self.unknown
            .entry(client_order_id)
            .or_insert(UnknownSubmission {
                client_order_id,
                lookup: format!("client:{client_order_id}"),
                first_seen: now,
                last_probe: None,
                next_probe: now,
                attempts: 0,
                last_probe_reason: None,
                abandoned: false,
            });
    }

    /// Returns the submissions whose outcome is unknown, in client order id order.
    #[must_use]
    pub fn unknown_submissions(&self) -> Vec<UnknownSubmission> {
        self.unknown.values().cloned().collect()
    }

    /// Returns the unknown submissions a probe is due for at `now`.
    ///
    /// An abandoned submission is never due: plan §6.3 stops the probe once the window passes, and
    /// a probe that kept going would be a busy loop against the venue rather than a reconciliation.
    #[must_use]
    pub fn probe_due(&self, now: UnixNanos) -> Vec<ClientOrderId> {
        self.unknown
            .values()
            .filter(|submission| !submission.abandoned && now >= submission.next_probe)
            .filter(|submission| !past_window(submission, now))
            .map(|submission| submission.client_order_id)
            .collect()
    }

    /// Applies one probe's outcome.
    ///
    /// `Found` settles the submission. `NotFound` and an inconclusive probe settle nothing: the
    /// outcome stays unknown until the window expires or a later probe finds the order.
    pub fn note_probe(
        &mut self,
        client_order_id: &ClientOrderId,
        outcome: ProbeOutcome,
        now: UnixNanos,
    ) -> ProbeDisposition {
        let reason = match outcome {
            ProbeOutcome::Found => {
                self.unknown.remove(client_order_id);

                return ProbeDisposition::Resolved;
            }
            // A 404 is recorded as what it is - an answer that settles nothing - rather than as
            // the order not existing (plan §6.3).
            ProbeOutcome::NotFound => {
                "the venue answered 404, which does not settle it".to_string()
            }
            ProbeOutcome::Inconclusive { reason } => reason,
        };

        let Some(submission) = self.unknown.get_mut(client_order_id) else {
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

    /// Expires every unknown submission whose window has passed, returning the abandoned ones.
    pub fn expire_unknown_submissions(&mut self, now: UnixNanos) -> Vec<AbandonedSubmission> {
        let mut abandoned = Vec::new();

        for submission in self.unknown.values_mut() {
            if submission.abandoned || !past_window(submission, now) {
                continue;
            }

            submission.abandoned = true;

            abandoned.push(AbandonedSubmission {
                client_order_id: submission.client_order_id,
                lookup: submission.lookup.clone(),
                attempts: submission.attempts,
                elapsed_ns: now.as_u64().saturating_sub(submission.first_seen.as_u64()),
            });
        }

        abandoned
    }

    /// Clears an unknown submission that has been settled outside this machine.
    ///
    /// The caller is the venue's answer, or a human reconciling the account by hand. Nothing inside
    /// this adapter clears one on its own: the order was neither confirmed nor denied.
    pub fn clear_unknown_submission(
        &mut self,
        client_order_id: &ClientOrderId,
    ) -> Option<UnknownSubmission> {
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
    }

    /// Records that a pass could not read the account, and judges it accordingly.
    pub fn note_pass_failed(&mut self, reason: String, _now: UnixNanos) -> ReconciliationState {
        let judgment = AccountJudgment::single(Finding::ReadFailed { reason });

        self.last_judgment = Some(judgment);
        self.state = ReconciliationState::Uncertain;
        self.confirmations = 0;
        self.last_fingerprint = None;

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

        if judgment.is_uncertain() {
            self.confirmations = 0;
            self.last_fingerprint = None;
            self.state = ReconciliationState::Uncertain;

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

    /// Judges the positions one reading listed, against the baseline and this client's fills.
    ///
    /// The comparison is `venue == baseline + applied`. The baseline is adopted **once**, by the
    /// first pass, as the venue's position less this client's fills at that moment: whatever the
    /// account held when this run started is the starting point - a position carried in, or a
    /// carried-in position on an instrument this pass did not list, which is flat. Adopting per
    /// instrument instead would let a position that appears later explain itself away, which is
    /// exactly the disagreement this judgment exists to catch.
    ///
    /// An instrument the venue did not list is left alone by the *comparison*: a missing row is not
    /// a statement that a position is gone, so nothing is reported for it.
    fn judge_positions(&mut self, reading: &AccountReading) -> Vec<Finding> {
        let mut findings = Vec::new();
        let adopt = !self.baseline_adopted;

        for position in &reading.positions {
            let Some(instrument_id) = position.instrument_id else {
                findings.push(Finding::UnmappablePosition {
                    market: position.market.clone(),
                });

                continue;
            };

            if let PositionDirection::Unknown(direction) = &position.direction {
                findings.push(Finding::UnreadablePosition {
                    market: position.market.clone(),
                    direction: direction.clone(),
                });

                continue;
            }

            let applied = reading
                .applied_net
                .get(&instrument_id)
                .copied()
                .unwrap_or(Decimal::ZERO);

            if adopt {
                self.baseline
                    .insert(instrument_id, position.signed - applied);
            }

            let baseline = self
                .baseline
                .get(&instrument_id)
                .copied()
                .unwrap_or(Decimal::ZERO);

            let expected = baseline + applied;

            if !adopt && expected != position.signed {
                findings.push(Finding::PositionMismatch {
                    instrument_id,
                    venue: position.signed,
                    expected,
                });
            }
        }

        self.baseline_adopted = true;

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

/// Returns whether an unknown submission's window has passed at `now` (plan §6.3).
fn past_window(submission: &UnknownSubmission, now: UnixNanos) -> bool {
    now.as_u64().saturating_sub(submission.first_seen.as_u64())
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
