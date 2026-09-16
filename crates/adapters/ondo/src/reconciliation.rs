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
    /// This client was configured as an account **read-only** session.
    ///
    /// It is not a state and nothing clears it: a read-only client reads the account and never
    /// places an order, whatever the account reads and whoever asks. It is checked before every
    /// other condition because it is the client's own definition rather than a condition to chase,
    /// and naming anything else first would send an operator looking for a problem that does not
    /// exist.
    AccountIsReadOnly,
    /// The admission a caller presented is not the current one: something that revokes permission
    /// happened after it was issued, even if the account is admissible again by now.
    ///
    /// Only [`ReconciliationMachine::revalidate`] answers this. A permit is good for the run
    /// generation that issued it and for nothing later.
    Superseded,
    /// A journal path was configured and the journal could not be restored, so this run does not
    /// know which fills it has already applied.
    ///
    /// It is not a condition of the account and no read of the venue clears it: without the ledger
    /// a historical fill could be counted twice or a report the engine already saw could be
    /// dropped, and neither is something a reconciliation pass can tell from the outside. Only a
    /// journal that reads back restores the client's own memory (plan §6.4, §R3.2).
    JournalUnavailable {
        /// Why the journal could not be restored.
        reason: String,
    },
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
    /// The venue is liquidating the account, or its liquidation condition is not one this client
    /// read.
    ///
    /// The venue is closing the account's positions itself. New risk against an account the venue
    /// is closing is not a decision this adapter makes on a stale reading of the venue's intent,
    /// and an unread liquidation condition is not a clear one (plan §R3.2).
    Liquidation(LiquidationState),
}

impl NewRiskRefusal {
    /// Returns a human-readable statement of the refusal.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::AccountIsReadOnly => {
                "this client was configured as an account read-only session, which places no \
                 orders at all"
                    .to_string()
            }
            Self::Superseded => {
                "the admission this order was given is no longer current".to_string()
            }
            Self::JournalUnavailable { reason } => format!(
                "the ledger journal could not be restored, so this run does not know which fills \
                 it already applied: {reason}"
            ),
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
            Self::Liquidation(state) => state.reason(),
        }
    }
}

/// The venue's liquidation condition for the account (plan §R3.2).
///
/// The venue states it as the required `underLiquidation` member of its balance summary, and this
/// adapter keeps **three** answers apart where the wire carries a boolean and a payload can be
/// wrong: the venue is liquidating the account, it said it is not, or this client cannot say.
/// Collapsing the third into the second is the mistake the type exists to prevent - it is exactly
/// "the venue did not tell us" read as "the venue said no".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiquidationState {
    /// The venue stated the account is not under liquidation.
    Clear,
    /// The venue is liquidating the account.
    UnderLiquidation,
    /// No balance has been read, or the balance carried no readable `underLiquidation`.
    Unknown {
        /// Why this client cannot say.
        reason: String,
    },
}

impl LiquidationState {
    /// Reads the venue's `underLiquidation` member, with an absent one kept as unknown.
    #[must_use]
    pub fn from_member(under_liquidation: Option<bool>) -> Self {
        match under_liquidation {
            Some(true) => Self::UnderLiquidation,
            Some(false) => Self::Clear,
            None => Self::Unknown {
                reason: "the venue's balance carried no readable `underLiquidation`".to_string(),
            },
        }
    }

    /// Returns the state a machine that has read no balance is in.
    #[must_use]
    pub fn unread() -> Self {
        Self::Unknown {
            reason: "no balance has been read yet".to_string(),
        }
    }

    /// Returns whether this condition permits new orders.
    #[must_use]
    pub const fn permits_new_orders(&self) -> bool {
        matches!(self, Self::Clear)
    }

    /// Returns the state's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::UnderLiquidation => "under_liquidation",
            Self::Unknown { .. } => "unknown",
        }
    }

    /// Returns a human-readable statement of the condition.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::Clear => "the account is not under liquidation".to_string(),
            Self::UnderLiquidation => {
                "the venue is liquidating this account, so no new risk is taken on it".to_string()
            }
            Self::Unknown { reason } => format!(
                "the account's liquidation condition is not known: {reason}; an unread condition \
                 is not a clear one"
            ),
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

    /// Serializes the frame the private stream sends.
    ///
    /// This is the send half the type was missing: [`crate::reconciliation::DeadMansSwitch`] built
    /// the frame and nothing could put it on a socket, so
    /// [`crate::reconciliation::StopStep::ReleaseDeadMansSwitch`] was a step no caller could carry
    /// out. The body is public - an operation, a channel name and a timeout - and carries no
    /// credential, so unlike a login frame it is recorded by the private transport as the fact that
    /// it was sent.
    ///
    /// # Errors
    ///
    /// Returns an error if the body cannot be serialized, which cannot happen for a well-formed
    /// value of this type and is surfaced rather than unwrapped.
    pub fn to_json_text(&self) -> anyhow::Result<String> {
        serde_json::to_string(self)
            .map_err(|error| anyhow::anyhow!("failed to serialize the Ondo switch frame: {error}"))
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
    /// The venue's `averageEntryPrice`, when it sent a readable one.
    ///
    /// It is what a Nautilus position status report carries as its average open price. An
    /// unreadable one leaves the report's own optional field empty rather than refusing the
    /// position: the entry price is not what makes a position a position, and the quantity it is
    /// read with is judged on its own.
    pub average_entry_price: Option<Decimal>,
}

impl PositionReading {
    /// Reads one `ApiPosition` payload onto a reading.
    #[must_use]
    pub fn new(
        market: &str,
        direction: &str,
        net_quantity: Decimal,
        average_entry_price: Option<Decimal>,
    ) -> Self {
        let direction = PositionDirection::from_raw(direction);

        Self {
            market: market.to_string(),
            instrument_id: market_to_instrument_id(market).ok(),
            signed: signed_position_quantity(&direction, net_quantity),
            direction,
            net_quantity,
            average_entry_price,
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
    /// `maintenanceMarginRequirement`: the maintenance margin the open positions require. It is
    /// what a Nautilus [`MarginBalance`](nautilus_model::types::MarginBalance) carries as its
    /// maintenance side.
    pub maintenance_margin_requirement: Option<Decimal>,
    /// `underLiquidation`: whether the venue is liquidating this account.
    ///
    /// **Three states, not two.** The frozen schema requires the member, so an absent or
    /// unreadable one is a payload this adapter does not understand - and a liquidation condition
    /// nobody read is not a liquidation condition that is clear.
    /// [`LiquidationState`] is where the three are kept apart; this is the raw reading.
    pub under_liquidation: Option<bool>,
    /// `totalFundingPayments`: the venue's **cumulative** funding total for the account.
    ///
    /// It is a running total, not this interval's payment: what it says about a period is the
    /// difference between two readings of it, and what proves a payment is a
    /// [`FundingPayment`] record from the funding history. The two are reconciled against each
    /// other ([`FundingLedger`]) and never against a rate multiplied by a position.
    pub total_funding_payments: Option<Decimal>,
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
    maintenance: Option<Decimal>,
}

impl MappedBalance {
    /// Returns the reading this mapping came from.
    #[must_use]
    pub const fn reading(&self) -> &BalanceReading {
        &self.reading
    }

    /// Returns the maintenance margin the venue stated, when it sent a readable one.
    #[must_use]
    pub const fn maintenance(&self) -> Option<Decimal> {
        self.maintenance
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

/// One funding payment the venue stated, from a `FundingFeeTransfer` record.
///
/// This is the **only** thing that books funding. The frozen schema makes every member required,
/// including `amount` - "the actual amount of USDC transferred, positive indicates a fee you
/// earned, negative a fee you paid" - so a record is the venue stating a payment that happened.
/// A funding *rate* is a public estimate and a `totalFundingPayments` member is a running total;
/// neither is a payment, and multiplying the first by a position size to produce one is the
/// arithmetic the plan forbids (plan §R3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingPayment {
    /// The venue's market string.
    pub market: String,
    /// `time`: when the venue applied the payment.
    pub time: UnixNanos,
    /// `amount`: the signed USDC transferred, positive for a fee earned.
    pub amount: Decimal,
    /// `rate`: the funding rate that led to this payment, when the venue sent a readable one.
    ///
    /// It is carried as evidence and never as a factor: a rate is public market data, and the
    /// payment is the account's.
    pub rate: Option<Decimal>,
    /// `positionSize`: the base size of the position when the payment was applied, when readable.
    pub position_size: Option<Decimal>,
}

impl FundingPayment {
    /// Returns the identity a payment is accounted once against.
    ///
    /// The venue gives a funding record no id. The identity is therefore the payment's own facts -
    /// the market it was paid on, the instant it was applied and the amount transferred - which is
    /// what a second page, a second read or a second process has to agree on to be the same
    /// payment. It is deliberately **not** `(market, time)`: two payments on one market in one
    /// interval are two payments, and collapsing them would drop one.
    #[must_use]
    pub fn identity(&self) -> String {
        format!("{}|{}|{}", self.market, self.time, self.amount)
    }
}

/// The venue's cumulative funding total and the payment records, when they disagree.
///
/// The comparison is between two **changes over one window**, both measured from the baseline the
/// first readable `totalFundingPayments` established: what the venue's running total says funding
/// has done to this account since then, and what the payment records this client has read account
/// for over the same window. A difference is stated here rather than booked: the missing payment
/// is one this adapter cannot prove, and a number it invented would be indistinguishable from one
/// it read (plan §R3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingGap {
    /// What the venue's cumulative total says funding has changed by since the baseline.
    pub stated_change: Decimal,
    /// What the payment records account for over the same window.
    pub accounted_change: Decimal,
    /// `stated_change - accounted_change`.
    pub difference: Decimal,
    /// When the baseline was read.
    pub since: UnixNanos,
    /// How many payment records the window accounted for.
    pub payments: usize,
}

/// What this client can say about the account's funding (plan §R3.2).
///
/// The three facts the plan keeps apart are kept apart here and nowhere else: the funding **rate**
/// is public market data and is not consulted at all, the **cumulative** total is a balance member,
/// and the **payments** are records from the account's own funding history. Only the last books
/// anything, and the second is what the last is checked against.
///
/// # A restart re-baselines rather than replays
///
/// The ledger is not part of the journal, and it does not need to be. The level comparison is
/// taken from whatever `totalFundingPayments` the first balance read of this process carried, so a
/// restart cannot double-count a payment (the baseline moves past it) and cannot lose one (every
/// record at or after the new baseline is accounted). What a restart cannot do is claim a payment
/// that happened before it read anything, which is why the baseline is a reading and not a
/// construction.
#[derive(Clone, Debug, Default)]
pub struct FundingLedger {
    baseline: Option<Decimal>,
    baseline_at: Option<UnixNanos>,
    stated: Option<Decimal>,
    payments: BTreeMap<String, FundingPayment>,
    read_error: Option<String>,
}

impl FundingLedger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the account's cumulative funding total from a balance read.
    ///
    /// The first readable total is the baseline: everything the account was paid before this
    /// process read it is already inside it, and only payments at or after this instant are what
    /// the venue's total is expected to have moved by.
    pub fn observe_cumulative(&mut self, total: Option<Decimal>, now: UnixNanos) {
        let Some(total) = total else {
            return;
        };

        if self.baseline.is_none() {
            self.baseline = Some(total);
            self.baseline_at = Some(now);
        }

        self.stated = Some(total);
    }

    /// Records why the funding history could not be read this pass.
    pub fn note_read_error(&mut self, error: String) {
        self.read_error = Some(error);
    }

    /// Clears the read error, for a pass whose funding history was read.
    pub fn clear_read_error(&mut self) {
        self.read_error = None;
    }

    /// Accounts one payment, returning `true` when it was one this ledger had not seen.
    ///
    /// A payment already accounted for is not counted again: the venue's history is paginated and
    /// re-read every pass, so the same record arrives on every read and the identity is what makes
    /// it one payment rather than one per read.
    pub fn account(&mut self, payment: FundingPayment) -> bool {
        self.payments.insert(payment.identity(), payment).is_none()
    }

    /// Accounts a whole read, returning how many of the payments were new.
    pub fn account_all(&mut self, payments: impl IntoIterator<Item = FundingPayment>) -> usize {
        payments
            .into_iter()
            .filter(|payment| self.account(payment.clone()))
            .count()
    }

    /// Returns the cumulative total the venue last stated.
    #[must_use]
    pub const fn stated(&self) -> Option<Decimal> {
        self.stated
    }

    /// Returns the baseline the level comparison is measured from.
    #[must_use]
    pub const fn baseline(&self) -> Option<(UnixNanos, Decimal)> {
        match (self.baseline_at, self.baseline) {
            (Some(at), Some(value)) => Some((at, value)),
            _ => None,
        }
    }

    /// Returns how many distinct payments this ledger has accounted.
    #[must_use]
    pub fn len(&self) -> usize {
        self.payments.len()
    }

    /// Returns whether this ledger has accounted no payments at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payments.is_empty()
    }

    /// Returns every payment this ledger has accounted, in identity order.
    #[must_use]
    pub fn payments(&self) -> Vec<FundingPayment> {
        self.payments.values().cloned().collect()
    }

    /// Returns the sum of the payments the window since the baseline accounted for.
    #[must_use]
    pub fn accounted_since_baseline(&self) -> Decimal {
        let Some((since, _baseline)) = self.baseline() else {
            return Decimal::ZERO;
        };

        self.payments
            .values()
            .filter(|payment| payment.time >= since)
            .fold(Decimal::ZERO, |sum, payment| sum + payment.amount)
    }

    /// Returns how many payments the window since the baseline accounted for.
    #[must_use]
    pub fn payments_since_baseline(&self) -> usize {
        let Some((since, _baseline)) = self.baseline() else {
            return 0;
        };

        self.payments
            .values()
            .filter(|payment| payment.time >= since)
            .count()
    }

    /// Returns the account's funding reconciliation, derived from what has been read.
    ///
    /// It is **recomputed, not accumulated**: both sides of the comparison are read off the
    /// baseline every time, so a payment the venue published after the balance read that already
    /// included it shows as a gap in the pass that saw only one of the two and is closed by the
    /// pass that sees both, rather than being carried forward as a discrepancy of its own.
    #[must_use]
    pub fn reconciliation(&self) -> FundingReconciliation {
        if let Some(reason) = &self.read_error {
            return FundingReconciliation::Unreadable {
                reason: reason.clone(),
            };
        }

        let (Some((since, baseline)), Some(stated)) = (self.baseline(), self.stated) else {
            return FundingReconciliation::Unreadable {
                reason: "the venue's balance has not carried a readable `totalFundingPayments`"
                    .to_string(),
            };
        };

        let accounted_change = self.accounted_since_baseline();
        let stated_change = stated - baseline;

        if stated_change == accounted_change {
            return FundingReconciliation::Reconciled {
                accounted: accounted_change,
            };
        }

        FundingReconciliation::Unreconciled(FundingGap {
            stated_change,
            accounted_change,
            difference: stated_change - accounted_change,
            since,
            payments: self.payments_since_baseline(),
        })
    }
}

/// What this client can say about the account's funding, as one of three answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FundingReconciliation {
    /// The venue's cumulative total and the payment records agree over the window.
    Reconciled {
        /// What the payments accounted for since the baseline.
        accounted: Decimal,
    },
    /// They do not, and the difference is what this client cannot prove.
    Unreconciled(FundingGap),
    /// Nothing about the account's funding could be read this pass.
    Unreadable {
        /// Why.
        reason: String,
    },
}

impl FundingReconciliation {
    /// Returns whether the account's funding is fully accounted for.
    #[must_use]
    pub const fn is_reconciled(&self) -> bool {
        matches!(self, Self::Reconciled { .. })
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
    /// The account's funding could not be read this pass: the funding history failed, or the
    /// balance carried no readable cumulative total to check it against.
    ///
    /// Nothing is booked from it - a payment this client did not read is not a payment it may
    /// invent - and the gap stays visible here until a pass reads both sides (plan §R3.2).
    FundingUnreadable {
        /// Why.
        reason: String,
    },
    /// The venue's cumulative funding total and the payment records this client has read do not
    /// agree over the window they are compared on (plan §R3.2).
    ///
    /// The difference is **stated, not booked**: it is a payment (or a repayment) this adapter
    /// cannot prove, and the alternative - pricing a rate against a position size - would produce
    /// a number indistinguishable from a read one.
    FundingUnreconciled {
        /// What the venue's running total says funding changed by.
        stated_change: Decimal,
        /// What the payment records account for.
        accounted_change: Decimal,
        /// The unaccounted difference.
        difference: Decimal,
        /// When the baseline the comparison starts from was read.
        since: UnixNanos,
        /// How many payment records the window carried.
        payments: usize,
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
    ///
    /// The two funding findings are deliberately not among them either, and the reason is what an
    /// account being uncertain *buys*: it stops new risk until a pass reads the account whole.
    /// Funding is a cashflow axis, not the account's order and position state - the balance that
    /// carries it is read fresh by every pass and is the venue's own number, so a payment this
    /// client could not account for does not make the position the venue reported any less read.
    /// It is reported, it is never booked, and it is never turned into an order refusal it cannot
    /// justify.
    #[must_use]
    pub fn is_uncertain(&self) -> bool {
        match self {
            Self::ForeignOrder { status, .. } => {
                !status.is_known() || *status == OndoOrderStatus::Untriggered
            }
            Self::FundingUnreadable { .. } | Self::FundingUnreconciled { .. } => false,
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
            Self::FundingUnreadable { reason } => {
                format!("the account's funding could not be read: {reason}")
            }
            Self::FundingUnreconciled {
                stated_change,
                accounted_change,
                difference,
                since,
                payments,
            } => format!(
                "the venue's cumulative funding has changed by {stated_change} since {since} and \
                 the {payments} payment record(s) read account for {accounted_change}; the \
                 difference of {difference} is not booked"
            ),
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
    /// The funding payments the venue's funding history carried for this pass.
    ///
    /// A pass that could not read the history carries none and says so in
    /// [`Self::funding_error`]; the two are never the same fact.
    pub funding: Vec<FundingPayment>,
    /// Why the funding history could not be read, when it could not.
    pub funding_error: Option<String>,
    /// The instant this pass read the account at.
    ///
    /// It is the instant a balance's cumulative funding total is stamped with, and therefore the
    /// instant the funding window opens at ([`FundingLedger`]).
    pub read_at: UnixNanos,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// One order's association and applied state, in the form a restart reads back (plan §R3.2).
///
/// The venue order id is what resolves a payload to a Nautilus order, and `filled` is the sum of
/// the fills this client applied to it. Both have to survive a restart, and for one reason: a
/// ledger that remembered its fills but not the orders they belong to would re-read the venue's
/// history, find every fill already applied, and never move the order's applied total - so a
/// terminal order would stay unresolved for good and `applied_net` would claim a position on an
/// instrument the account does not hold.
///
/// Every member is the string the venue or the model spelled, so a journal is readable by a human
/// and a value that cannot be read back is refused rather than quietly defaulted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalOrder {
    pub(crate) client_order_id: String,
    pub(crate) venue_order_id: Option<String>,
    pub(crate) instrument_id: String,
    pub(crate) side: String,
    pub(crate) order_type: String,
    pub(crate) time_in_force: String,
    pub(crate) quantity: String,
    pub(crate) price: Option<String>,
    pub(crate) reduce_only: bool,
    pub(crate) post_only: bool,
    pub(crate) status: String,
    pub(crate) accepted: bool,
    pub(crate) filled: String,
    pub(crate) venue_filled: Option<String>,
    pub(crate) unappliable_fill: bool,
    pub(crate) resolved: bool,
}

impl JournalOrder {
    /// Returns the Nautilus client order id.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    /// Returns the venue's order id, when this session had learned one.
    #[must_use]
    pub fn venue_order_id(&self) -> Option<&str> {
        self.venue_order_id.as_deref()
    }

    /// Returns the Nautilus instrument id.
    #[must_use]
    pub fn instrument_id(&self) -> &str {
        &self.instrument_id
    }

    /// Returns the venue's status, verbatim.
    #[must_use]
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns the quantity the applied fills add up to.
    #[must_use]
    pub fn filled(&self) -> &str {
        &self.filled
    }

    /// Returns whether the venue had acknowledged the order.
    #[must_use]
    pub const fn accepted(&self) -> bool {
        self.accepted
    }

    /// Returns whether the order ended in a state this adapter confirmed.
    #[must_use]
    pub const fn resolved(&self) -> bool {
        self.resolved
    }
}

/// One write this client had left unsettled, in the form a restart reads back (plan §R3.2).
///
/// An unknown submission and an unconfirmed cancel both block new risk, and both are facts about
/// the *run* rather than about the account: no read of the venue can tell this client that it once
/// sent a request whose answer it never saw. A restart that dropped them would start with an empty
/// map and a clear conscience, which is exactly the "清空 map 获得 Ready" the plan forbids.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalUnsettled {
    /// The Nautilus client order id the write was made under.
    pub client_order_id: String,
    /// The venue's order id, when this session learned one before the outcome was lost.
    pub venue_order_id: Option<String>,
    /// Which write this is.
    pub kind: UncertainKind,
    /// The reference a probe must use to ask the venue about it.
    pub lookup: String,
    /// Why the outcome is unknown, as this client saw it.
    pub reason: String,
    /// When the outcome became unknown, in nanoseconds since the Unix epoch.
    pub first_seen_ns: u64,
    /// How many probes had been answered when this was written.
    pub attempts: u32,
    /// Whether the probe window had already expired when this was written.
    pub abandoned: bool,
}

impl JournalUnsettled {
    /// Returns the client order id the write was made under.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    /// Returns the reference a probe asks the venue about it under.
    #[must_use]
    pub fn lookup(&self) -> &str {
        &self.lookup
    }

    /// Returns which write this is.
    #[must_use]
    pub const fn kind(&self) -> UncertainKind {
        self.kind
    }

    /// Returns when the outcome became unknown.
    #[must_use]
    pub fn first_seen(&self) -> UnixNanos {
        UnixNanos::from(self.first_seen_ns)
    }

    /// Returns whether the probe window had already expired when this was written.
    #[must_use]
    pub const fn abandoned(&self) -> bool {
        self.abandoned
    }

    /// Returns how many probes had been answered when this was written.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Writes one unsettled write into the form a restart reads back.
    ///
    /// The probe's own bookkeeping - when the next probe is due, why the last one settled nothing -
    /// is deliberately not written. The instant a probe is due is a decision the run that probes
    /// makes, and the window that bounds it is measured from `first_seen`, which **is** written: a
    /// restart therefore resumes the window where it stood rather than starting a fresh one.
    #[must_use]
    fn from_outcome(outcome: &UncertainOutcome) -> Self {
        Self {
            client_order_id: outcome.client_order_id.to_string(),
            venue_order_id: outcome.venue_order_id.as_ref().map(ToString::to_string),
            kind: outcome.kind,
            lookup: outcome.lookup.clone(),
            reason: outcome.reason.clone(),
            first_seen_ns: outcome.first_seen.as_u64(),
            attempts: outcome.attempts,
            abandoned: outcome.abandoned,
        }
    }
}

/// What one journal write covers, so a read can check the file against itself.
///
/// A journal is written whole and replaced atomically, so a file that parses is a file that was
/// written in one piece. The counts are what makes that checkable rather than assumed: a
/// truncated body, a hand-edited file or a merge of two runs shows up as a checkpoint that
/// disagrees with the lists beside it, and the read is refused rather than restored (plan §R3.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalCheckpoint {
    pub(crate) written_ns: u64,
    pub(crate) fills: usize,
    pub(crate) orders: usize,
    pub(crate) unsettled: usize,
}

impl JournalCheckpoint {
    /// Returns when the journal was written.
    #[must_use]
    pub fn written_at(&self) -> UnixNanos {
        UnixNanos::from(self.written_ns)
    }

    /// Returns how many fills the write covered.
    #[must_use]
    pub const fn fills(&self) -> usize {
        self.fills
    }

    /// Returns how many orders the write covered.
    #[must_use]
    pub const fn orders(&self) -> usize {
        self.orders
    }

    /// Returns how many unsettled writes the write covered.
    #[must_use]
    pub const fn unsettled(&self) -> usize {
        self.unsettled
    }
}

/// Everything one journal write records, taken from the account as it stands.
///
/// It is what a checkpoint is written from, and it is deliberately not `Default`: a snapshot with
/// no account, no ledger and no instant is not a snapshot of anything, and a journal built from
/// one would restore an empty ledger over a real one.
#[derive(Debug)]
pub struct JournalSnapshot<'a> {
    /// The account the journal belongs to.
    pub account_id: AccountId,
    /// The newest applied fill's instant, when one has been applied.
    pub watermark: Option<UnixNanos>,
    /// The instant the write is taken at.
    pub written_at: UnixNanos,
    /// The dedup ledger as it stands.
    pub ledger: &'a OndoFillLedger,
    /// The order associations and applied state, as they stand.
    pub orders: Vec<JournalOrder>,
    /// The writes this run has left unsettled, as they stand.
    pub unsettled: Vec<JournalUnsettled>,
}

/// How many journal writes this process has made.
///
/// It is what makes a temporary file's name unique: two writers - a concluded pass and a submission
/// that has just decided its outcome - write their own temporary file and publish it by renaming it
/// onto the journal's own name, so neither can interleave inside the other's body. What a reader
/// sees is one of the two complete snapshots rather than a mixture of them, and the later writer
/// wins.
static JOURNAL_WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The one member every version of the journal carries, read on its own to refuse a version this
/// adapter does not read by its version rather than by a member it does not have.
#[derive(Deserialize)]
struct SchemaProbe {
    schema_version: u32,
}

/// What a restore put back, so the caller can say what it read (plan §R3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRestore {
    /// How many fills the dedup ledger took.
    pub fills: usize,
    /// The order associations to rebuild the order index from.
    pub orders: Vec<JournalOrder>,
    /// The unsettled writes to re-register with the reconciliation machine.
    pub unsettled: Vec<JournalUnsettled>,
}

impl JournalRestore {
    /// Returns whether the journal held anything at all.
    ///
    /// A first run reads an absent file as an empty journal: nothing was restored, and that is a
    /// statement about the file rather than about the account.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fills == 0 && self.orders.is_empty() && self.unsettled.is_empty()
    }
}

/// The dedup ledger, the order associations and the unsettled writes, in a form that survives a
/// restart (plan §6.4, §R3.2).
///
/// The ledger is the `(account_id, fill.id)` set a fill is applied once against, and the plan
/// forbids an arbitrary short TTL in its place: a restart that forgot it would count a historical
/// fill a second time. What else has to survive is everything a read of the venue cannot
/// re-derive: which order a venue order id belongs to, what the applied fills on it add up to, and
/// which writes this client sent whose answers it never saw.
///
/// A journal is one account's. Restoring it into another account is refused rather than merged -
/// silently adopting another account's ids would suppress that account's first fills.
///
/// # It is written whole, and it trails the events it records
///
/// The file is replaced atomically, so a reader sees one complete journal or the previous one.
/// What the journal deliberately does **not** try to be is a transcript of every event: it is
/// written at checkpoints - the end of a reconciliation pass - while fills are reported the moment
/// they are applied. A crash between the two therefore leaves a fill the engine has seen and a
/// journal that does not hold it, and that ordering is the safe one: the recovery re-reads the
/// fill from the venue's history and reports it again under the **same** trade id
/// (`FillReport.trade_id` is the venue's fill id), which is what makes the replay one fill rather
/// than two. The opposite order would have the journal claim a fill the engine never saw, and
/// nothing downstream could tell that from a fill it had already applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerJournal {
    schema_version: u32,
    account_id: String,
    #[serde(default)]
    watermark_ns: Option<u64>,
    #[serde(default)]
    fills: Vec<String>,
    #[serde(default)]
    orders: Vec<JournalOrder>,
    #[serde(default)]
    unsettled: Vec<JournalUnsettled>,
    checkpoint: JournalCheckpoint,
}

impl LedgerJournal {
    /// The journal schema this adapter writes and the only one it reads.
    ///
    /// Version 2 added the order associations, the unsettled writes and the checkpoint to the fill
    /// ledger version 1 held. A version 1 file is **refused**, not read with defaults: the fields
    /// it does not carry are exactly the ones a restart needs, and a restore that silently
    /// supplied them would claim an order index and an unsettled set it never had.
    pub const SCHEMA_VERSION: u32 = 2;

    /// Builds the journal one write records.
    #[must_use]
    pub fn from_snapshot(snapshot: JournalSnapshot<'_>) -> Self {
        let mut fills: Vec<String> = snapshot
            .ledger
            .entries()
            .into_iter()
            .filter(|(entry_account_id, _fill_id)| *entry_account_id == snapshot.account_id)
            .map(|(_account_id, fill_id)| fill_id)
            .collect();

        fills.sort();

        let mut orders = snapshot.orders;
        orders.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));

        let mut unsettled = snapshot.unsettled;
        unsettled.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));

        Self {
            schema_version: Self::SCHEMA_VERSION,
            account_id: snapshot.account_id.to_string(),
            watermark_ns: snapshot.watermark.map(|value| value.as_u64()),
            checkpoint: JournalCheckpoint {
                written_ns: snapshot.written_at.as_u64(),
                fills: fills.len(),
                orders: orders.len(),
                unsettled: unsettled.len(),
            },
            fills,
            orders,
            unsettled,
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

    /// Returns the fill ids the journal holds, in id order.
    #[must_use]
    pub fn fills(&self) -> &[String] {
        &self.fills
    }

    /// Returns the order associations the journal holds, in client order id order.
    #[must_use]
    pub fn orders(&self) -> &[JournalOrder] {
        &self.orders
    }

    /// Returns the unsettled writes the journal holds, in client order id order.
    #[must_use]
    pub fn unsettled(&self) -> &[JournalUnsettled] {
        &self.unsettled
    }

    /// Returns the checkpoint this journal was written with.
    #[must_use]
    pub const fn checkpoint(&self) -> &JournalCheckpoint {
        &self.checkpoint
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
        // The version is read first, on its own, so a file this adapter does not read is refused
        // **by its version** rather than by whichever member this version added and that file
        // happens not to carry. The two are the same refusal, and only one of them tells an
        // operator what to do about it.
        let probe: SchemaProbe = serde_json::from_str(text)
            .map_err(|error| anyhow::anyhow!("the ledger journal could not be read: {error}"))?;

        if probe.schema_version != Self::SCHEMA_VERSION {
            anyhow::bail!(
                "the ledger journal is schema version {}, and this adapter reads version {}",
                probe.schema_version,
                Self::SCHEMA_VERSION,
            );
        }

        serde_json::from_str(text)
            .map_err(|error| anyhow::anyhow!("the ledger journal could not be read: {error}"))
    }

    /// Checks this journal against itself, and restores it into `ledger`.
    ///
    /// The two refusals are deliberately one call: a journal that disagrees with its own
    /// checkpoint, and a journal that belongs to another account, are both journals nothing may be
    /// restored from, and a caller that could check the first without the second would be a caller
    /// that can forget to.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint does not describe the lists beside it, or when the
    /// journal belongs to another account. **Nothing is restored in either case**: a partially
    /// merged ledger is worse than an empty one, and the caller is expected to treat a refused
    /// restore as a run that must not accept new risk ([`JournalStatus::Failed`]).
    pub fn restore(
        &self,
        ledger: &mut OndoFillLedger,
        account_id: AccountId,
    ) -> anyhow::Result<JournalRestore> {
        self.verify()?;

        if self.account_id != account_id.to_string() {
            anyhow::bail!(
                "the ledger journal belongs to account {} and this client reports for {account_id}",
                self.account_id,
            );
        }

        let fills = ledger.restore(
            self.fills
                .iter()
                .map(|fill_id| (account_id, fill_id.clone())),
        );

        Ok(JournalRestore {
            fills,
            orders: self.orders.clone(),
            unsettled: self.unsettled.clone(),
        })
    }

    /// Checks this journal's checkpoint against the lists it was written with.
    ///
    /// # Errors
    ///
    /// Returns an error naming the list whose count disagrees with the checkpoint.
    pub fn verify(&self) -> anyhow::Result<()> {
        let checks = [
            ("fills", self.checkpoint.fills, self.fills.len()),
            ("orders", self.checkpoint.orders, self.orders.len()),
            ("unsettled", self.checkpoint.unsettled, self.unsettled.len()),
        ];

        for (name, recorded, actual) in checks {
            if recorded != actual {
                anyhow::bail!(
                    "the ledger journal's checkpoint records {recorded} {name} and the file holds \
                     {actual}; the write did not complete and nothing is restored from it"
                );
            }
        }

        Ok(())
    }

    /// Writes this journal to `path`, replacing whatever was there in one step.
    ///
    /// The body is written to a sibling temporary file, flushed to the disk, and then renamed onto
    /// the journal's own name. A rename replaces a file as one operation, so a reader - the next
    /// process, or this one after a crash - sees either the previous journal or this one and never
    /// a half-written body; the flush is what makes that true across a power loss rather than only
    /// across a process death. The parent directory is created when it is missing, because a
    /// configured journal path is an explicit request to keep the file there.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created, the temporary file cannot be
    /// written or flushed, or the rename fails. A failed write removes its own temporary file and
    /// leaves the previous journal in place; a process that dies mid-write leaves a `.tmp-*` beside
    /// the journal, which nothing reads and the next write does not reuse.
    pub fn store_atomic(&self, path: &std::path::Path) -> anyhow::Result<()> {
        use std::io::Write;

        let text = self.to_json()?;

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                anyhow::anyhow!(
                    "the journal directory {} could not be created: {error}",
                    parent.display(),
                )
            })?;
        }

        let mut temporary = path.as_os_str().to_os_string();
        temporary.push(format!(
            ".tmp-{}",
            JOURNAL_WRITES.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        ));
        let temporary = std::path::PathBuf::from(temporary);

        let write = || -> std::io::Result<()> {
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()
        };

        if let Err(error) = write() {
            let _ = std::fs::remove_file(&temporary);

            anyhow::bail!(
                "the journal could not be written to {}: {error}",
                temporary.display(),
            );
        }

        std::fs::rename(&temporary, path).map_err(|error| {
            let _ = std::fs::remove_file(&temporary);

            anyhow::anyhow!(
                "the journal could not be moved onto {}: {error}",
                path.display(),
            )
        })
    }

    /// Reads the journal at `path` for `account_id`.
    ///
    /// An absent file is an empty journal **for this account** rather than an error: the first run
    /// of a configured journal has nothing to restore, and that is a statement about the file, not
    /// a failure to read one. The account is a parameter for exactly this case - an empty journal
    /// that named no account would be refused by [`Self::restore`], and a first run would then be
    /// a run that cannot start.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and cannot be read, does not parse, is a schema this
    /// adapter does not read, or disagrees with its own checkpoint.
    pub fn load(path: &std::path::Path, account_id: AccountId) -> anyhow::Result<Self> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::from_snapshot(JournalSnapshot {
                    account_id,
                    watermark: None,
                    written_at: UnixNanos::default(),
                    ledger: &OndoFillLedger::new(),
                    orders: Vec::new(),
                    unsettled: Vec::new(),
                }));
            }
            Err(error) => {
                anyhow::bail!("the journal {} could not be read: {error}", path.display());
            }
        };

        let journal = Self::from_json(&text)?;
        journal.verify()?;

        Ok(journal)
    }
}

/// What this run's durable journal did (plan §R3.2).
///
/// Four answers, and they are not degrees of one another. A run with no journal path has said, in
/// its configuration, that its ledger lives for one process; a run whose journal was restored has
/// its own memory back; a run whose journal could not be read is a run **missing** a memory it
/// declared it would have - which is the one that must not accept new risk, because nothing about
/// the account can tell it which fills it already counted; and a run whose journal stopped
/// accepting writes holds that memory **up to an instant**, which is stated and refuses nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalStatus {
    /// No journal path is configured.
    ///
    /// This is a supported mode - the offline phase runs in it - and it is **stated** rather than
    /// passed over: the ledger, the order index and the unsettled writes are this process's alone,
    /// and a restart begins with an empty one. A caller that needs durability configures a path.
    NotConfigured {
        /// Why there is no journal.
        reason: String,
    },
    /// A journal was read and restored into this run.
    Restored {
        /// The path it was restored from.
        path: String,
        /// How many fills the dedup ledger took.
        fills: usize,
        /// How many order associations were put back.
        orders: usize,
        /// How many unsettled writes were re-registered.
        unsettled: usize,
        /// The watermark the journal was written with.
        watermark: Option<UnixNanos>,
    },
    /// A journal was restored, and a checkpoint written since has failed.
    ///
    /// This is **not** [`Self::Failed`] and it refuses nothing. What it says is narrower and, to an
    /// operator, more actionable: the memory this run restored is intact and complete **to the
    /// instant named here**, and nothing after that instant has reached the disk. A run in this
    /// state has lapsed back to the durability of [`Self::NotConfigured`] - with the one difference
    /// that it declared durability and no longer has it, which is the thing that must never pass
    /// unstated.
    ///
    /// A crash from here replays: fills this run has already reported are read back from the
    /// venue's history and re-emitted under their own trade ids, which the engine dedupes. That is
    /// a recoverable outcome rather than a reason to stop trading, so this state blocks nothing -
    /// it is stated, counted and logged.
    Degraded {
        /// The path that stopped accepting writes.
        path: String,
        /// How many checkpoint writes have failed since this run started.
        failures: u64,
        /// The instant of the last write that succeeded, when one ever did.
        last_written_at: Option<UnixNanos>,
        /// The newest applied fill that write covered, when it covered one.
        watermark: Option<UnixNanos>,
    },
    /// A journal path is configured and the journal could not be restored.
    Failed {
        /// The path that could not be restored.
        path: String,
        /// Why.
        reason: String,
    },
}

impl JournalStatus {
    /// Returns the state's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotConfigured { .. } => "not_configured",
            Self::Restored { .. } => "restored",
            Self::Degraded { .. } => "degraded",
            Self::Failed { .. } => "failed",
        }
    }

    /// Returns whether this status permits new risk.
    ///
    /// Only [`Self::Failed`] does not. [`Self::Degraded`] is deliberately among the ones that do: a
    /// journal that stopped accepting writes loses durability, not memory, and a crash from there
    /// replays under trade ids the engine dedupes - the same outcome as the supported
    /// [`Self::NotConfigured`] mode, which no configuration refuses.
    #[must_use]
    pub const fn permits_new_orders(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    /// Returns a human-readable statement of the status.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::NotConfigured { reason } => format!(
                "no journal is configured, so the dedup ledger lives in memory for this process \
                 only: {reason}"
            ),
            Self::Restored {
                path,
                fills,
                orders,
                unsettled,
                ..
            } => format!(
                "the journal at {path} was restored: {fills} fill(s), {orders} order(s), \
                 {unsettled} unsettled write(s)"
            ),
            Self::Degraded {
                path,
                failures,
                last_written_at,
                watermark,
            } => {
                let last = match last_written_at {
                    Some(at) => {
                        format!(
                            "the last write that reached the disk was at {} ns",
                            at.as_u64()
                        )
                    }
                    None => "no checkpoint from this run has reached the disk".to_string(),
                };
                let covered = match watermark {
                    Some(watermark) => format!(
                        ", and it covered applied fills up to {} ns",
                        watermark.as_u64(),
                    ),
                    None => String::new(),
                };

                format!(
                    "the journal at {path} has stopped accepting writes: {failures} checkpoint \
                     write(s) have failed, {last}{covered}"
                )
            }
            Self::Failed { path, reason } => {
                format!("the journal at {path} could not be restored: {reason}")
            }
        }
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
    /// Whether this client is an account read-only session. Set once, and never cleared.
    account_read_only: bool,
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
    /// Whether the last balance this machine judged is one it may report as the account's.
    balance_verified: bool,
    liquidation: LiquidationState,
    funding: FundingLedger,
    journal: JournalStatus,
}

impl ReconciliationMachine {
    /// Creates a machine for `account_id` with a switch timeout of `timeout_seconds`.
    #[must_use]
    pub fn new(account_id: AccountId, timeout_seconds: u64) -> Self {
        Self {
            account_id,
            account_read_only: false,
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
            balance_verified: false,
            liquidation: LiquidationState::unread(),
            funding: FundingLedger::new(),
            journal: JournalStatus::NotConfigured {
                reason: "no journal path was configured for this machine".to_string(),
            },
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

    /// Marks this client as an account read-only session.
    ///
    /// A read-only session reads the account and never places an order
    /// ([`NewRiskRefusal::AccountIsReadOnly`]), which is what makes "read-only" a property of the
    /// client rather than a promise its caller keeps. The private transport subscribes to the
    /// account's reports for such a client and never arms the switch, whose arm has a cancelling
    /// side effect ([`crate::websocket::private::session::PrivateStreamMode::ReadOnly`], plan §0).
    ///
    /// It is idempotent and there is no way back: a caller cannot make a read-only client tradable
    /// by forgetting that it asked for one.
    pub fn mark_account_read_only(&mut self) {
        self.account_read_only = true;
        self.invalidate_admissions();
    }

    /// Returns whether this client is an account read-only session.
    #[must_use]
    pub const fn is_account_read_only(&self) -> bool {
        self.account_read_only
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
        if self.account_read_only {
            return Some(NewRiskRefusal::AccountIsReadOnly);
        }

        // The journal comes before every condition about the account, because it is not one: it is
        // this client's own memory of its own traffic. A run that cannot read it does not know
        // which fills it already applied, and no reading of the venue can tell it.
        if let JournalStatus::Failed { reason, .. } = &self.journal {
            return Some(NewRiskRefusal::JournalUnavailable {
                reason: reason.clone(),
            });
        }

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

        // An account the venue is closing is not one to open a position on, and a liquidation
        // condition nobody read is not a clear one. It is checked here rather than left to the
        // judgment because it is a **known** state: it makes no reading uncertain, it makes the
        // account untradable, and a named refusal is what says so.
        if !self.liquidation.permits_new_orders() {
            return Some(NewRiskRefusal::Liquidation(self.liquidation.clone()));
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
    ///
    /// This is the mapping, not licence to report it: a balance this adapter mapped is not
    /// necessarily the whole account ([`Self::verified_balance`]).
    #[must_use]
    pub const fn last_balance(&self) -> Option<&MappedBalance> {
        self.last_balance.as_ref()
    }

    /// Returns the balance this machine has **verified**, which is the only one it reports.
    ///
    /// Verified is stricter than mapped, and the difference is what a Nautilus `AccountState`
    /// claims. A mapped balance is one whose numbers this adapter could read and whose own
    /// arithmetic held; a verified one is also one that is the **whole** account, which means no
    /// member outside the documented set. A balance carrying a second collateral asset or a loan
    /// maps to USDC numbers that are not the account's, and reporting them would put a partial
    /// account into the cache under the account's name - so nothing is reported at all until a
    /// balance that is the whole account is read (plan §6.4, §R3.2).
    ///
    /// # A negative equity is verified
    ///
    /// Being underwater is a state the venue stated, not a reading this adapter could not make: the
    /// numbers are reported exactly as they were read, negative signs included, because clamping
    /// them to zero would report a healthy account and reporting nothing would leave the cache
    /// holding whatever it last believed.
    #[must_use]
    pub fn verified_balance(&self) -> Option<&MappedBalance> {
        self.last_balance.as_ref().filter(|_| self.balance_verified)
    }

    /// Returns the account's liquidation condition, as the last pass read it.
    #[must_use]
    pub const fn liquidation(&self) -> &LiquidationState {
        &self.liquidation
    }

    /// Returns the account's funding ledger.
    #[must_use]
    pub const fn funding(&self) -> &FundingLedger {
        &self.funding
    }

    /// Returns what this run's journal did.
    #[must_use]
    pub const fn journal(&self) -> &JournalStatus {
        &self.journal
    }

    /// Records what this run's journal did (plan §R3.2).
    ///
    /// A journal that could not be restored revokes every permit issued before it: a submission
    /// admitted under a decision taken without the ledger is a submission admitted under a
    /// decision this client can no longer make.
    pub fn set_journal(&mut self, status: JournalStatus) {
        if !status.permits_new_orders() {
            self.invalidate_admissions();
        }

        self.journal = status;
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

    /// Re-registers the unsettled writes a restored journal holds (plan §R3.2).
    ///
    /// A submission whose answer was lost and a cancel no answer settled are facts about **this
    /// client's own traffic**, and no read of the venue can re-derive them: the order may not exist,
    /// may exist unfilled, or may have been filled and closed, and nothing about the account says
    /// which. So they are read back from the journal, and they block new risk from the moment they
    /// are registered exactly as they did before the restart.
    ///
    /// The probe window is inherited rather than restarted: `first_seen` is the instant the outcome
    /// became unknown, and it does not move because the process did. A write whose window had
    /// already passed is registered as abandoned - recorded, blocking, and no longer probed - which
    /// is what a restart of a run that had stopped probing should mean.
    ///
    /// A write the machine already holds is left as it is: the live record is this run's own and is
    /// newer than the file.
    ///
    /// Returns how many writes were registered.
    pub fn restore_unsettled(&mut self, unsettled: &[JournalUnsettled], now: UnixNanos) -> usize {
        let mut restored = 0;

        for entry in unsettled {
            let client_order_id = ClientOrderId::from(entry.client_order_id.as_str());

            if self.unknown.contains_key(&client_order_id)
                || self.unconfirmed_cancels.contains_key(&client_order_id)
            {
                continue;
            }

            let outcome = UncertainOutcome {
                client_order_id,
                venue_order_id: entry.venue_order_id.as_deref().map(VenueOrderId::from),
                kind: entry.kind,
                lookup: entry.lookup.clone(),
                reason: entry.reason.clone(),
                first_seen: UnixNanos::from(entry.first_seen_ns),
                last_probe: None,
                next_probe: now,
                attempts: entry.attempts,
                last_probe_reason: None,
                abandoned: entry.abandoned,
            };

            match entry.kind {
                UncertainKind::Submission => self.unknown.insert(client_order_id, outcome),
                UncertainKind::Cancel => self.unconfirmed_cancels.insert(client_order_id, outcome),
            };

            restored += 1;
        }

        if restored > 0 {
            self.invalidate_admissions();
        }

        restored
    }

    /// Returns the unsettled writes as journal entries, in client order id order.
    ///
    /// Both maps are one list here, which is what a restart needs: the maps say which probe a
    /// record answers and the journal says what the record is.
    #[must_use]
    pub fn unsettled_journal_entries(&self) -> Vec<JournalUnsettled> {
        let mut entries: Vec<JournalUnsettled> = self
            .unknown
            .values()
            .chain(self.unconfirmed_cancels.values())
            .map(JournalUnsettled::from_outcome)
            .collect();

        entries.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));

        entries
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
        findings.extend(self.judge_funding(reading));

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
    ///
    /// It also records the two things about the balance that are not Nautilus shapes: the venue's
    /// liquidation condition ([`LiquidationState`]), which refuses new risk rather than making the
    /// account uncertain, and the balance's right to be reported at all
    /// ([`ReconciliationMachine::verified_balance`]).
    fn judge_balance(&mut self, reading: &AccountReading) -> Vec<Finding> {
        self.last_balance = None;
        self.balance_verified = false;
        self.liquidation = LiquidationState::unread();

        let Some(balance) = reading.balance.as_ref() else {
            return vec![Finding::BalanceUnreadable {
                reason: "the pass carried no balance".to_string(),
            }];
        };

        self.liquidation = LiquidationState::from_member(balance.under_liquidation);

        let mut findings = Vec::new();

        for (member, value) in &balance.unmapped {
            findings.push(Finding::UnsupportedCollateral {
                member: member.clone(),
                value: value.clone(),
            });
        }

        // A balance with a member outside the documented set is not the whole account, so the
        // USDC numbers it does carry are not the account's balance: they are mapped, and
        // deliberately not verified (see `verified_balance`).
        let whole_account = balance.unmapped.is_empty();

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
            maintenance: balance.maintenance_margin_requirement,
        });
        self.balance_verified = whole_account;

        findings
    }

    /// Accounts the funding one reading carried, judging it as it goes (plan §R3.2).
    ///
    /// Two findings and no booking are possible here: this method never derives a payment. What it
    /// does is record the payments the venue stated, compare the venue's cumulative total against
    /// them over one window, and say so when the two disagree. A rate the public feed carries is
    /// not consulted, because there is nothing it could prove about this account's cash.
    fn judge_funding(&mut self, reading: &AccountReading) -> Vec<Finding> {
        let stated = reading
            .balance
            .as_ref()
            .and_then(|balance| balance.total_funding_payments);

        self.funding.observe_cumulative(stated, reading.read_at);

        let mut findings = Vec::new();

        // Three answers, and only one of them books anything. A pass that could not read the
        // history, and a pass whose balance carried no readable cumulative total to check it
        // against, are both passes that cannot say what the account's funding is - and a stale
        // total from an earlier read is exactly the number that must not be used to say it.
        if let Some(error) = &reading.funding_error {
            self.funding.note_read_error(error.clone());
        } else if stated.is_none() {
            self.funding.note_read_error(
                "the venue's balance carried no readable `totalFundingPayments`".to_string(),
            );
        } else {
            self.funding.clear_read_error();
            self.funding.account_all(reading.funding.iter().cloned());
        }

        match self.funding.reconciliation() {
            FundingReconciliation::Reconciled { .. } => {}
            FundingReconciliation::Unreadable { reason } => {
                findings.push(Finding::FundingUnreadable { reason });
            }
            FundingReconciliation::Unreconciled(gap) => {
                findings.push(Finding::FundingUnreconciled {
                    stated_change: gap.stated_change,
                    accounted_change: gap.accounted_change,
                    difference: gap.difference,
                    since: gap.since,
                    payments: gap.payments,
                });
            }
        }

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
