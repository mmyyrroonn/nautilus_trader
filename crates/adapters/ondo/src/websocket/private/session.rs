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

//! The private session: the protocol state machine, with **no I/O at all**.
//!
//! It is handed a frame's text and returns the events to apply and the frames to send, and it never
//! touches a socket, a clock it does not own, or a credential. That is what makes the whole login →
//! subscribed → armed sequence testable offline, which matters because no sandbox key exists in
//! this phase (plan §R3.1, `test_data/README.md`).
//!
//! # Why the session never sees the credential
//!
//! A login frame carries the API key id and an HMAC signature. The session does not build it: it
//! emits [`PrivateAction::Login`] and the transport, which holds the credential, composes the frame
//! ([`super::stream`]). One place holds a secret, and it is the same place that can redact.
//!
//! # The order the venue fixes
//!
//! 1. `login`, and then wait for `loggedIn` - the private channels are login-required;
//! 2. then subscribe to the report channels;
//! 3. then arm the switch, which is itself one of the login-required private channels
//!    (`cancelAllOrdersAfterPerps`), so it cannot be armed before step 1 completes.
//!
//! # What is verified and what is not
//!
//! The login frame, the subscriptions and the acknowledgements are the frozen spec's documented
//! shapes. Two things are **not** documented and are carried as assumptions with one named place
//! to change:
//!
//! - **The renewal message.** The frozen material documents the subscribe frame and the timeout and
//!   says nothing about what renews an armed switch. This adapter renews by re-sending the subscribe
//!   frame, which is what [`crate::reconciliation::DeadMansSwitch::renew_frame`] builds; a sandbox
//!   session is what settles it (plan §R3.3).
//! - **What an `update` on the switch channel means.** The channel's own page documents an
//!   `update` frame for it, and the adapter cannot tell "armed" from "fired" in one. It is treated
//!   as a state this adapter cannot read ([`PrivateEvent::SwitchChannelUpdate`]), which stops new
//!   orders rather than assuming the permissive reading (plan §0: an unknown risk state is not
//!   treated as a normal one).

use std::fmt;

use nautilus_core::UnixNanos;

use crate::{
    http::{orders::OndoApiOrder, private::OndoApiFill},
    websocket::{
        messages::WsMessageType,
        private::{
            messages::PrivateChannel,
            parse::{
                PrivateEnvelope, PrivatePayload, decode_private_updates, parse_private_message,
            },
        },
    },
};

/// Consecutive failed logins after which the session stops trying.
///
/// A credential the venue refuses does not start working on the fourth attempt, and the plan
/// forbids an unbounded retry loop against a rate-limited venue (plan §R3.1: a permanent error has
/// a terminal classification). Three is the whole of the reason for the number.
pub const ONDO_WS_LOGIN_MAX_ATTEMPTS: u64 = 3;

/// What a venue error message must contain to be read as permanent.
///
/// A rejection of the credential or of the signed instant is not something a retry fixes: the same
/// key and the same clock would be offered again. Everything else - a connection slot already
/// taken, a malformed frame the venue did not like - is retried under the reconnect backoff and is
/// bounded by [`ONDO_WS_LOGIN_MAX_ATTEMPTS`] anyway.
///
/// This list is a *diagnosis*, not a protocol probe: the adapter never retries with a different
/// header set, digest order or environment (`test_data/conflicts.md`: no automatic protocol or
/// environment switching).
const PERMANENT_ERROR_MARKERS: [&str; 9] = [
    "signature",
    "api key",
    "api_key",
    "apikey",
    "unauthorized",
    "forbidden",
    "permission",
    "ip_not_permitted",
    "timestamp",
];

/// Whether a private session may place orders at all.
///
/// The mode decides one thing and one thing only: whether the session subscribes to the account's
/// dead man's switch. It does not decide what the account state machine admits - that is
/// [`crate::reconciliation`]'s, driven by the configuration - and the two are deliberately not
/// folded together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateStreamMode {
    /// The session arms the switch and may reach a trading-ready state.
    Trading,
    /// The session reads the account and never arms the switch: the switch cancels resting orders,
    /// which is a side effect a read-only session does not ask for (plan §0).
    ReadOnly,
}

impl PrivateStreamMode {
    /// The channels this mode subscribes to, in subscribe order.
    #[must_use]
    pub const fn channels(self) -> &'static [PrivateChannel] {
        match self {
            Self::Trading => &PrivateChannel::TRADING,
            Self::ReadOnly => &PrivateChannel::READ_ONLY,
        }
    }

    /// Returns whether this mode arms the switch.
    #[must_use]
    pub const fn arms_the_switch(self) -> bool {
        matches!(self, Self::Trading)
    }
}

impl fmt::Display for PrivateStreamMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Trading => "trading",
            Self::ReadOnly => "read-only",
        })
    }
}

/// Where a session stands in the login and subscribe sequence.
///
/// This is the *protocol* phase, not the account's state: a session can be [`Self::Subscribed`]
/// while the account is unread, which is exactly the distinction the run state
/// ([`super::stream::PrivateRunState`]) keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivateSessionPhase {
    /// No connection.
    Disconnected,
    /// A socket is up and the login frame has been sent; `loggedIn` has not arrived.
    LoggingIn,
    /// `loggedIn` arrived; the report subscriptions have been sent and are not all acknowledged.
    Subscribing,
    /// Every report channel of this mode is acknowledged.
    Subscribed,
    /// The session is over: a permanent refusal, or the attempt bound was reached.
    Failed {
        /// Why, as the venue or the attempt bound stated it.
        reason: String,
    },
}

impl PrivateSessionPhase {
    /// Returns the phase's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::LoggingIn => "logging_in",
            Self::Subscribing => "subscribing",
            Self::Subscribed => "subscribed",
            Self::Failed { .. } => "failed",
        }
    }
}

/// One frame the transport must send.
///
/// The session names the *action*; the transport composes the body. That split is what keeps the
/// credential - which only [`PrivateAction::Login`] needs - out of this module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateAction {
    /// Compose and send the login frame for the credential this transport holds.
    Login,
    /// Subscribe to one channel.
    Subscribe(PrivateChannel),
    /// Unsubscribe from one channel.
    Unsubscribe(PrivateChannel),
    /// Arm the switch: `subscribe` on `cancelAllOrdersAfterPerps` with this session's timeout.
    ///
    /// The body is the client's own ([`crate::reconciliation::DeadMansSwitchMessage`]), not this
    /// session's, because the deadline it starts is the account's to keep.
    ArmSwitch,
    /// Release the switch.
    ReleaseSwitch,
    /// Send the application-level heartbeat.
    Heartbeat,
}

impl PrivateAction {
    /// Returns the action's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Login => "login",
            Self::Subscribe(_) => "subscribe",
            Self::Unsubscribe(_) => "unsubscribe",
            Self::ArmSwitch => "arm_switch",
            Self::ReleaseSwitch => "release_switch",
            Self::Heartbeat => "heartbeat",
        }
    }

    /// Returns whether this action's frame must never be offered to any recording boundary.
    ///
    /// The login frame carries the API key id and the HMAC signature; every other action's body is
    /// composed from public values. The transport reads this rather than deciding for itself.
    #[must_use]
    pub const fn carries_a_credential(self) -> bool {
        matches!(self, Self::Login)
    }
}

/// One thing an inbound frame told the session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivateEvent {
    /// The venue acknowledged the login.
    LoggedIn,
    /// The venue acknowledged a subscription.
    Subscribed(PrivateChannel),
    /// The venue acknowledged an unsubscription.
    Unsubscribed(PrivateChannel),
    /// A heartbeat response.
    Pong,
    /// An order report for the account.
    Order(Box<OndoApiOrder>),
    /// A fill report for the account.
    Fill(Box<OndoApiFill>),
    /// A frame the session recognises and does not act on, named so it is not silently ignored.
    Ignored {
        /// What was recognised.
        detail: String,
    },
    /// A frame the session does not handle: an unknown message type, a private channel this
    /// adapter does not carry, or a channel with no readable name.
    Unsupported {
        /// Why the frame was not handled.
        reason: String,
    },
    /// A permanent protocol problem: a frame that does not decode, or one whose item array does.
    /// The connection survives it; the account is told it lost reports.
    ProtocolError {
        /// Why the frame was rejected.
        reason: String,
    },
    /// A venue error frame.
    VenueError {
        /// What the venue said, verbatim.
        reason: String,
        /// Whether this adapter reads it as permanent.
        permanent: bool,
    },
    /// An `update` on the switch channel, whose meaning this adapter has not verified.
    SwitchChannelUpdate {
        /// Why the frame stopped new orders.
        reason: String,
    },
}

/// What one inbound frame produced: the events to apply, and the frames to send because of it.
#[derive(Clone, Debug, Default)]
pub struct PrivateFrameOutcome {
    /// The events, in the order the frame stated them.
    pub events: Vec<PrivateEvent>,
    /// The frames to send, in the order they must go out.
    pub actions: Vec<PrivateAction>,
}

/// Per-session counters, for diagnostics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrivateSessionCounters {
    /// Frames that reached [`OndoPrivateSession::handle_frame`].
    pub frames_received: u64,
    /// Frames larger than the inbound bound, rejected without being parsed.
    pub oversized_frames: u64,
    /// Successful logins.
    pub logins: u64,
    /// Login attempts that failed, by venue error or by the attempt bound.
    pub login_failures: u64,
    /// Login acknowledgements that arrived while no login was outstanding.
    pub unsolicited_login_acks: u64,
    /// Subscription acknowledgements.
    pub subscribed_acks: u64,
    /// Unsubscription acknowledgements.
    pub unsubscribed_acks: u64,
    /// Heartbeat responses.
    pub pongs: u64,
    /// Order reports decoded.
    pub orders: u64,
    /// Fill reports decoded.
    pub fills: u64,
    /// Frames that could not be decoded, or whose items could not.
    pub decode_failures: u64,
    /// Frames whose message type is not one this adapter knows.
    pub unknown_message_types: u64,
    /// Private channels this adapter does not carry.
    pub unknown_channels: u64,
    /// Venue error frames.
    pub venue_errors: u64,
    /// Venue error frames read as permanent.
    pub permanent_errors: u64,
}

/// The protocol state machine for one private connection.
///
/// One session serves one connection. [`Self::on_disconnected`] returns it to
/// [`PrivateSessionPhase::Disconnected`] and clears what the connection had confirmed, so a
/// subscription acknowledged on a socket that has since closed is not treated as live on the next
/// one: the venue's confirmations belong to the connection that received them, and the next
/// connection re-establishes everything from the login forward.
#[derive(Debug)]
pub struct OndoPrivateSession {
    mode: PrivateStreamMode,
    phase: PrivateSessionPhase,
    /// The channels this mode subscribes to, in subscribe order.
    wanted: &'static [PrivateChannel],
    /// The channels the current connection has acknowledged.
    confirmed: Vec<PrivateChannel>,
    /// Consecutive failed logins since the last success.
    failed_logins: u64,
    counters: PrivateSessionCounters,
}

impl OndoPrivateSession {
    /// Creates a session for one mode.
    #[must_use]
    pub fn new(mode: PrivateStreamMode) -> Self {
        Self {
            mode,
            phase: PrivateSessionPhase::Disconnected,
            wanted: mode.channels(),
            confirmed: Vec::new(),
            failed_logins: 0,
            counters: PrivateSessionCounters::default(),
        }
    }

    /// Returns the mode this session runs in.
    #[must_use]
    pub const fn mode(&self) -> PrivateStreamMode {
        self.mode
    }

    /// Returns the protocol phase.
    #[must_use]
    pub const fn phase(&self) -> &PrivateSessionPhase {
        &self.phase
    }

    /// Returns the channels the current connection has acknowledged.
    #[must_use]
    pub fn confirmed(&self) -> &[PrivateChannel] {
        &self.confirmed
    }

    /// Returns the channels this mode subscribes to.
    #[must_use]
    pub const fn wanted(&self) -> &'static [PrivateChannel] {
        self.wanted
    }

    /// Returns whether every report channel of this mode is acknowledged.
    ///
    /// A trading session is established once its report channels are: the switch is a separate
    /// requirement, tracked by the account rather than here, because an unconfirmed arm is not an
    /// arm ([`crate::reconciliation::DeadMansSwitch::permits_new_orders`]).
    #[must_use]
    pub fn is_established(&self) -> bool {
        self.wanted
            .iter()
            .filter(|channel| channel.is_report())
            .all(|channel| self.confirmed.contains(channel))
    }

    /// Returns the counters.
    #[must_use]
    pub const fn counters(&self) -> PrivateSessionCounters {
        self.counters
    }

    /// Returns why this session ended, when it has.
    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        match &self.phase {
            PrivateSessionPhase::Failed { reason } => Some(reason),
            _ => None,
        }
    }

    /// Returns whether this session has ended and must not be connected again.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self.phase, PrivateSessionPhase::Failed { .. })
    }

    /// Records a new connection: the login frame is what goes out first.
    ///
    /// A session that ended permanently stays ended. The refusal was about the credential or the
    /// signed instant, and a reconnect offers the same credential at the same instant, so a socket
    /// coming up is not evidence that anything changed. The transport stops reconnecting when a
    /// session ends, and this is the state machine refusing to be revived even if it did not.
    pub fn on_connected(&mut self) -> PrivateFrameOutcome {
        if self.is_failed() {
            return PrivateFrameOutcome::default();
        }

        self.phase = PrivateSessionPhase::LoggingIn;
        self.confirmed.clear();

        PrivateFrameOutcome {
            events: Vec::new(),
            actions: vec![PrivateAction::Login],
        }
    }

    /// Records that the connection ended.
    ///
    /// The confirmations go with it: they were the venue's statements about a socket that no
    /// longer exists.
    pub fn on_disconnected(&mut self) {
        self.confirmed.clear();

        if !self.is_failed() {
            self.phase = PrivateSessionPhase::Disconnected;
        }
    }

    /// Records that the login was not answered in time.
    ///
    /// A login the venue never answers is a failed attempt, not a hung session: it counts against
    /// [`ONDO_WS_LOGIN_MAX_ATTEMPTS`] exactly as a refusal does, so a venue that silently drops
    /// logins cannot be retried forever either.
    pub fn note_login_timeout(&mut self) -> PrivateEvent {
        self.counters.login_failures += 1;
        self.failed_logins += 1;

        let reason = format!(
            "the venue did not answer the login within the timeout (attempt {} of {ONDO_WS_LOGIN_MAX_ATTEMPTS})",
            self.failed_logins,
        );

        self.note_failed_attempt(&reason)
    }

    /// Returns the heartbeat body's action.
    ///
    /// The heartbeat is the application-level `{"op":"ping"}`: the venue idles a connection out
    /// after 180 s without a client *request*, and a protocol-level ping is not one.
    #[must_use]
    pub const fn heartbeat(&self) -> PrivateAction {
        PrivateAction::Heartbeat
    }

    /// Returns the actions that release the switch, when this mode ever armed one.
    ///
    /// A read-only session never armed a switch, so there is nothing to release and nothing is
    /// sent: a release is a frame with a side effect, and a session that did not take the
    /// responsibility does not get to end it.
    #[must_use]
    pub fn release_actions(&self) -> Vec<PrivateAction> {
        if self.mode.arms_the_switch() {
            vec![PrivateAction::ReleaseSwitch]
        } else {
            Vec::new()
        }
    }

    /// Handles one inbound frame.
    ///
    /// Never fails: a frame this adapter cannot read is an event
    /// ([`PrivateEvent::ProtocolError`] or [`PrivateEvent::Unsupported`]) rather than an error,
    /// because the connection survives it. The frozen WS schemas declare `required: []`, so the
    /// venue may legally send a payload the Rust decoders cannot do without, and treating that as
    /// fatal would let one surprising frame take the account's stream down.
    pub fn handle_frame(&mut self, text: &str, _ts_init: UnixNanos) -> PrivateFrameOutcome {
        self.counters.frames_received += 1;

        let envelope = match parse_private_message(text) {
            Ok(envelope) => envelope,
            Err(error) => {
                self.counters.decode_failures += 1;

                return PrivateFrameOutcome {
                    events: vec![PrivateEvent::ProtocolError {
                        reason: error.to_string(),
                    }],
                    actions: Vec::new(),
                };
            }
        };

        match envelope.kind {
            WsMessageType::Pong => {
                self.counters.pongs += 1;

                event(PrivateEvent::Pong)
            }
            WsMessageType::LoggedIn => self.handle_logged_in(),
            WsMessageType::Subscribed => self.handle_subscribed(&envelope),
            WsMessageType::Unsubscribed => self.handle_unsubscribed(&envelope),
            WsMessageType::Update => self.handle_update(&envelope),
            WsMessageType::Error => self.handle_error(&envelope),
            WsMessageType::Unknown => {
                self.counters.unknown_message_types += 1;

                event(PrivateEvent::Unsupported {
                    reason: format!(
                        "message type `{}` is not one this adapter knows",
                        envelope.kind_raw,
                    ),
                })
            }
        }
    }

    /// Applies a `loggedIn` acknowledgement.
    fn handle_logged_in(&mut self) -> PrivateFrameOutcome {
        if self.phase != PrivateSessionPhase::LoggingIn {
            self.counters.unsolicited_login_acks += 1;

            return event(PrivateEvent::Unsupported {
                reason: format!(
                    "a login acknowledgement arrived while the session was {}",
                    self.phase.as_str(),
                ),
            });
        }

        self.counters.logins += 1;
        self.failed_logins = 0;
        self.phase = PrivateSessionPhase::Subscribing;

        // The order is the venue's: the report channels first, then the switch. The switch is an
        // action of its own because its body is the account's (`timeout_seconds`), and because a
        // read-only session never sends it at all.
        let mut actions: Vec<PrivateAction> = self
            .wanted
            .iter()
            .filter(|channel| channel.is_report())
            .map(|channel| PrivateAction::Subscribe(*channel))
            .collect();

        if self.mode.arms_the_switch() {
            actions.push(PrivateAction::ArmSwitch);
        }

        PrivateFrameOutcome {
            events: vec![PrivateEvent::LoggedIn],
            actions,
        }
    }

    /// Applies a `subscribed` acknowledgement.
    fn handle_subscribed(&mut self, envelope: &PrivateEnvelope) -> PrivateFrameOutcome {
        self.counters.subscribed_acks += 1;

        let Some(channel) = envelope.channel else {
            self.counters.unknown_channels += 1;

            return event(PrivateEvent::Unsupported {
                reason: format!(
                    "a subscription was acknowledged for channel `{}`, which is not a private \
                     channel this adapter carries",
                    envelope.channel_raw.as_deref().unwrap_or("<none>"),
                ),
            });
        };

        if !self.confirmed.contains(&channel) {
            self.confirmed.push(channel);
        }

        if self.phase == PrivateSessionPhase::Subscribing && self.is_established() {
            self.phase = PrivateSessionPhase::Subscribed;
        }

        event(PrivateEvent::Subscribed(channel))
    }

    /// Applies an `unsubscribed` acknowledgement.
    fn handle_unsubscribed(&mut self, envelope: &PrivateEnvelope) -> PrivateFrameOutcome {
        self.counters.unsubscribed_acks += 1;

        let Some(channel) = envelope.channel else {
            self.counters.unknown_channels += 1;

            return event(PrivateEvent::Unsupported {
                reason: format!(
                    "an unsubscription was acknowledged for channel `{}`, which is not a private \
                     channel this adapter carries",
                    envelope.channel_raw.as_deref().unwrap_or("<none>"),
                ),
            });
        };

        self.confirmed.retain(|confirmed| *confirmed != channel);

        event(PrivateEvent::Unsubscribed(channel))
    }

    /// Applies an `update` frame.
    fn handle_update(&mut self, envelope: &PrivateEnvelope) -> PrivateFrameOutcome {
        let Some(channel) = envelope.channel else {
            self.counters.unknown_channels += 1;

            return event(PrivateEvent::Unsupported {
                reason: format!(
                    "an update arrived on channel `{}`, which is not a private channel this \
                     adapter carries",
                    envelope.channel_raw.as_deref().unwrap_or("<none>"),
                ),
            });
        };

        let Some(data) = envelope.data.as_ref() else {
            self.counters.decode_failures += 1;

            return event(PrivateEvent::ProtocolError {
                reason: format!(
                    "an update on `{}` carries no `data` member",
                    channel.as_str(),
                ),
            });
        };

        if channel == PrivateChannel::CancelAllOrdersAfterPerps {
            // The frozen page documents an `update` on this channel and does not say what it
            // means. Reading it as a confirmation would arm the account on a frame that may be
            // the switch firing; reading it as the switch firing would stop trading on a frame
            // that may be a confirmation. Neither is knowledge this adapter has, so the frame is
            // reported as a state it cannot read and new orders stop (plan §0).
            self.counters.decode_failures += 1;

            return event(PrivateEvent::SwitchChannelUpdate {
                reason: "the venue sent an `update` on the switch channel, whose meaning this \
                         adapter has not verified: the frozen material documents the frame and \
                         not what it says"
                    .to_string(),
            });
        }

        match decode_private_updates(channel, data) {
            Ok(payloads) => {
                let events = payloads
                    .into_iter()
                    .map(|payload| match payload {
                        PrivatePayload::Order(order) => {
                            self.counters.orders += 1;

                            PrivateEvent::Order(order)
                        }
                        PrivatePayload::Fill(fill) => {
                            self.counters.fills += 1;

                            PrivateEvent::Fill(fill)
                        }
                    })
                    .collect();

                PrivateFrameOutcome {
                    events,
                    actions: Vec::new(),
                }
            }
            Err(error) => {
                self.counters.decode_failures += 1;

                event(PrivateEvent::ProtocolError {
                    reason: error.to_string(),
                })
            }
        }
    }

    /// Applies a venue `error` frame.
    fn handle_error(&mut self, envelope: &PrivateEnvelope) -> PrivateFrameOutcome {
        self.counters.venue_errors += 1;

        let reason = match (&envelope.code, &envelope.message) {
            (Some(code), Some(message)) => format!("{code}: {message}"),
            (Some(code), None) => code.clone(),
            (None, Some(message)) => message.clone(),
            (None, None) => "the venue sent an error frame with no code and no message".to_string(),
        };

        let permanent = is_permanent_error(&reason);

        if permanent {
            self.counters.permanent_errors += 1;
        }

        if self.phase == PrivateSessionPhase::LoggingIn {
            self.counters.login_failures += 1;
            self.failed_logins += 1;

            let bounded = format!(
                "{reason} (login attempt {} of {ONDO_WS_LOGIN_MAX_ATTEMPTS})",
                self.failed_logins,
            );

            self.note_failed_login(&bounded, permanent);

            return event(PrivateEvent::VenueError {
                reason: bounded,
                permanent,
            });
        }

        PrivateFrameOutcome {
            events: vec![PrivateEvent::VenueError { reason, permanent }],
            actions: Vec::new(),
        }
    }

    /// Moves the session on from a login the venue refused.
    ///
    /// A refusal the adapter read as permanent ends the session at once. Any other refusal is
    /// retried, and it is the attempt bound - not the adapter's reading of the venue's prose -
    /// that ends the session for good.
    fn note_failed_login(&mut self, reason: &str, permanent: bool) {
        if permanent || self.failed_logins >= ONDO_WS_LOGIN_MAX_ATTEMPTS {
            self.note_failure(reason);
        } else {
            self.phase = PrivateSessionPhase::Disconnected;
        }
    }

    /// Ends the session with a reason.
    fn note_failure(&mut self, reason: &str) {
        self.phase = PrivateSessionPhase::Failed {
            reason: reason.to_string(),
        };
    }

    /// Records a failed attempt that did not come from a venue error frame.
    fn note_failed_attempt(&mut self, reason: &str) -> PrivateEvent {
        self.note_failed_login(reason, false);

        PrivateEvent::VenueError {
            reason: reason.to_string(),
            permanent: false,
        }
    }
}

/// Returns whether a venue error message reads as a permanent refusal.
fn is_permanent_error(reason: &str) -> bool {
    let lowered = reason.to_ascii_lowercase();

    PERMANENT_ERROR_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// Builds an outcome carrying one event and no action.
fn event(event: PrivateEvent) -> PrivateFrameOutcome {
    PrivateFrameOutcome {
        events: vec![event],
        actions: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn session(mode: PrivateStreamMode) -> OndoPrivateSession {
        OndoPrivateSession::new(mode)
    }

    fn now() -> UnixNanos {
        UnixNanos::from(1_789_384_200_000_000_000)
    }

    fn frame(kind: &str, channel: Option<&str>, rest: &str) -> String {
        let channel = channel.map_or(String::new(), |channel| {
            format!(r#","channel":"{channel}""#)
        });

        format!(r#"{{"type":"{kind}"{channel}{rest}}}"#)
    }

    fn logged_in(session: &mut OndoPrivateSession) -> PrivateFrameOutcome {
        session.on_connected();

        session.handle_frame(r#"{"type":"loggedIn","msg":"Login successful"}"#, now())
    }

    fn subscribe_all(session: &mut OndoPrivateSession) {
        logged_in(session);

        for channel in PrivateChannel::TRADING {
            session.handle_frame(&frame("subscribed", Some(channel.as_str()), ""), now());
        }
    }

    /// The documented sequence, and nothing before it: a socket coming up sends the login and
    /// nothing else, because every private channel is login-required.
    #[rstest]
    fn test_a_new_connection_sends_the_login_and_nothing_else() {
        let mut session = session(PrivateStreamMode::Trading);
        let outcome = session.on_connected();

        assert_eq!(session.phase(), &PrivateSessionPhase::LoggingIn);
        assert_eq!(outcome.actions, vec![PrivateAction::Login]);
        assert!(PrivateAction::Login.carries_a_credential());
    }

    /// The switch cannot be armed before the login completes: it is itself a login-required
    /// private channel, so the subscribe is only ever emitted after `loggedIn`.
    #[rstest]
    fn test_the_switch_is_armed_only_after_the_login_is_acknowledged() {
        let mut session = session(PrivateStreamMode::Trading);
        let connecting = session.on_connected();

        assert!(
            !connecting.actions.contains(&PrivateAction::ArmSwitch),
            "an arm before the login is a frame the venue can only refuse",
        );

        let acknowledged = logged_in(&mut session);

        assert_eq!(session.phase(), &PrivateSessionPhase::Subscribing);
        assert_eq!(
            acknowledged.actions,
            vec![
                PrivateAction::Subscribe(PrivateChannel::OrdersPerps),
                PrivateAction::Subscribe(PrivateChannel::FillsPerps),
                PrivateAction::ArmSwitch,
            ],
            "the order is the venue's: reports first, then the switch",
        );
    }

    /// A read-only session never arms the switch, on any path: arming it has a cancelling side
    /// effect, and a read-only session is not made ready by taking one (plan §0).
    #[rstest]
    fn test_a_read_only_session_never_sends_a_switch_frame() {
        let mut session = session(PrivateStreamMode::ReadOnly);
        let acknowledged = logged_in(&mut session);

        assert_eq!(
            acknowledged.actions,
            vec![
                PrivateAction::Subscribe(PrivateChannel::OrdersPerps),
                PrivateAction::Subscribe(PrivateChannel::FillsPerps),
            ],
        );
        assert!(session.release_actions().is_empty());
        assert!(!PrivateStreamMode::ReadOnly.arms_the_switch());
    }

    /// Establishment is about the report channels; the switch is the account's requirement, not
    /// this session's, so an unarmed switch cannot make a session look established.
    #[rstest]
    fn test_establishment_is_the_report_channels() {
        let mut session = session(PrivateStreamMode::Trading);
        logged_in(&mut session);

        session.handle_frame(&frame("subscribed", Some("ordersPerps"), ""), now());
        assert!(!session.is_established());

        session.handle_frame(&frame("subscribed", Some("fillsPerps"), ""), now());
        assert!(session.is_established());
        assert_eq!(session.phase(), &PrivateSessionPhase::Subscribed);
        assert_eq!(
            session.confirmed(),
            &[PrivateChannel::OrdersPerps, PrivateChannel::FillsPerps],
            "the switch's acknowledgement is the account's business, not recorded as a report here",
        );

        session.handle_frame(
            &frame("subscribed", Some("cancelAllOrdersAfterPerps"), ""),
            now(),
        );
        assert!(session.is_established());
    }

    /// The documented examples decode into order and fill reports, through the REST decoders.
    #[rstest]
    fn test_an_update_carries_the_accounts_reports() {
        let mut session = session(PrivateStreamMode::Trading);
        subscribe_all(&mut session);

        let orders = r#"{"type":"update","channel":"ordersPerps","data":[{"orderId":"197ec08e001658690721be129e7fa595","side":"buy","price":"227.50","size":"10.00","market":"AAPL-USD.P","filledSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC"}]}"#;
        let outcome = session.handle_frame(orders, now());

        let [PrivateEvent::Order(order)] = outcome.events.as_slice() else {
            panic!(
                "an `ordersPerps` update carries orders: {:?}",
                outcome.events
            );
        };

        assert_eq!(order.order_id(), "197ec08e001658690721be129e7fa595");
        assert_eq!(session.counters().orders, 1);

        let fills = r#"{"type":"update","channel":"fillsPerps","data":[{"id":"70a37d8f972f2494837f9dba8364cbb4","orderId":"197ec08e001658690721be129e7fa595","market":"AAPL-USD.P","price":"227.50","size":"5.00","side":"buy","direction":"open long","filledCost":"1137.50","fee":"0.57","time":"2025-03-05T14:30:00Z","isMaker":false}]}"#;
        let outcome = session.handle_frame(fills, now());

        let [PrivateEvent::Fill(fill)] = outcome.events.as_slice() else {
            panic!("a `fillsPerps` update carries fills: {:?}", outcome.events);
        };

        assert_eq!(fill.id(), "70a37d8f972f2494837f9dba8364cbb4");
        assert_eq!(session.counters().fills, 1);
    }

    /// A frame the decoder cannot read is a protocol error the connection survives, and it is
    /// counted: the account is told it lost a report rather than the socket being torn down.
    #[rstest]
    fn test_a_frame_that_will_not_decode_keeps_the_connection() {
        let mut session = session(PrivateStreamMode::Trading);
        subscribe_all(&mut session);

        let partial = r#"{"type":"update","channel":"ordersPerps","data":[{"orderId":"197ec08e001658690721be129e7fa595","market":"AAPL-USD.P"}]}"#;
        let outcome = session.handle_frame(partial, now());

        let [PrivateEvent::ProtocolError { reason }] = outcome.events.as_slice() else {
            panic!(
                "an undecodable item is a protocol error: {:?}",
                outcome.events
            );
        };

        assert!(reason.contains("ordersPerps"), "{reason}");
        assert!(outcome.actions.is_empty());
        assert_eq!(session.counters().decode_failures, 1);
        assert_eq!(
            session.phase(),
            &PrivateSessionPhase::Subscribed,
            "the session is unaffected: the socket is still good",
        );

        let malformed = session.handle_frame("not json at all", now());
        assert!(matches!(
            malformed.events.as_slice(),
            [PrivateEvent::ProtocolError { .. }],
        ));
        assert_eq!(session.counters().decode_failures, 2);
    }

    /// An `update` on the switch channel is a state this adapter cannot read, and the fail-closed
    /// reading is the one it takes: new orders stop rather than the account being assumed armed.
    #[rstest]
    fn test_an_unreadable_switch_frame_stops_new_orders() {
        let mut session = session(PrivateStreamMode::Trading);
        subscribe_all(&mut session);

        let outcome = session.handle_frame(
            &frame(
                "update",
                Some("cancelAllOrdersAfterPerps"),
                r#","data":{"timeout_seconds":30}"#,
            ),
            now(),
        );

        let [PrivateEvent::SwitchChannelUpdate { reason }] = outcome.events.as_slice() else {
            panic!(
                "an update on the switch channel is its own event: {:?}",
                outcome.events
            );
        };

        assert!(reason.contains("has not verified"), "{reason}");
        assert!(outcome.actions.is_empty(), "nothing is armed on a guess");
    }

    /// A credential refusal ends the session rather than being retried: the same key and the same
    /// clock would be offered again, and the plan forbids an unbounded retry (plan §R3.1).
    #[rstest]
    #[case::a_signature_mismatch("signature_mismatch")]
    #[case::an_unknown_key("invalid_api_key")]
    #[case::a_permission_refusal("ip_not_permitted for key ondoKeyId_x")]
    #[case::a_clock_the_venue_refused("timestamp_too_far")]
    #[case::a_forbidden_code("403 forbidden")]
    fn test_a_permanent_login_refusal_ends_the_session(#[case] message: &str) {
        let mut session = session(PrivateStreamMode::Trading);
        session.on_connected();

        let outcome = session.handle_frame(
            &frame("error", None, &format!(r#","msg":"{message}""#)),
            now(),
        );

        let [PrivateEvent::VenueError { permanent, .. }] = outcome.events.as_slice() else {
            panic!("a venue error is reported: {:?}", outcome.events);
        };

        assert!(*permanent, "`{message}` is not something a retry fixes");
        assert!(
            session.is_failed(),
            "`{message}` ends the session after one attempt",
        );
        assert!(
            outcome.actions.is_empty(),
            "no frame follows a permanent refusal"
        );
    }

    /// A refusal the adapter cannot read as permanent is bounded instead of immediate: it is
    /// retried, and the attempts are counted, so even a venue that keeps answering something
    /// unrecognised cannot be retried forever.
    #[rstest]
    fn test_an_unclassified_login_refusal_is_bounded_by_attempts() {
        let mut session = session(PrivateStreamMode::Trading);

        for attempt in 1..ONDO_WS_LOGIN_MAX_ATTEMPTS {
            session.on_connected();

            let outcome = session.handle_frame(
                &frame(
                    "error",
                    None,
                    r#","msg":"already logged in on this connection""#,
                ),
                now(),
            );

            let [PrivateEvent::VenueError { permanent, .. }] = outcome.events.as_slice() else {
                panic!("a venue error is reported: {:?}", outcome.events);
            };

            assert!(!permanent);
            assert_eq!(
                session.phase(),
                &PrivateSessionPhase::Disconnected,
                "attempt {attempt} leaves the session able to reconnect",
            );
        }

        session.on_connected();
        session.handle_frame(
            &frame(
                "error",
                None,
                r#","msg":"already logged in on this connection""#,
            ),
            now(),
        );

        assert!(
            session.is_failed(),
            "the attempt bound ends the session at {ONDO_WS_LOGIN_MAX_ATTEMPTS}",
        );
        assert_eq!(
            session.counters().login_failures,
            ONDO_WS_LOGIN_MAX_ATTEMPTS
        );
    }

    /// A login the venue never answers is an attempt too: silence is not a reason to retry forever.
    #[rstest]
    fn test_an_unanswered_login_counts_against_the_bound() {
        let mut session = session(PrivateStreamMode::Trading);

        for _ in 1..ONDO_WS_LOGIN_MAX_ATTEMPTS {
            session.on_connected();
            session.note_login_timeout();
            assert!(!session.is_failed());
        }

        session.on_connected();
        session.note_login_timeout();

        assert!(session.is_failed());
        assert_eq!(
            session.counters().login_failures,
            ONDO_WS_LOGIN_MAX_ATTEMPTS
        );
    }

    /// A successful login clears the attempt count: three failures spread over a long session are
    /// not three consecutive ones.
    #[rstest]
    fn test_a_successful_login_clears_the_attempt_count() {
        let mut session = session(PrivateStreamMode::Trading);

        session.on_connected();
        session.handle_frame(
            &frame("error", None, r#","msg":"already logged in""#),
            now(),
        );
        session.on_connected();
        session.handle_frame(
            &frame("error", None, r#","msg":"already logged in""#),
            now(),
        );

        session.on_connected();
        session.handle_frame(r#"{"type":"loggedIn","msg":"Login successful"}"#, now());

        assert_eq!(session.phase(), &PrivateSessionPhase::Subscribing);
        assert_eq!(session.counters().logins, 1);

        session.on_connected();
        session.handle_frame(
            &frame("error", None, r#","msg":"already logged in""#),
            now(),
        );

        assert!(!session.is_failed());
    }

    /// The venue's confirmations belong to the connection that received them.
    #[rstest]
    fn test_a_disconnect_clears_what_the_socket_confirmed() {
        let mut session = session(PrivateStreamMode::Trading);
        subscribe_all(&mut session);

        assert!(session.is_established());

        session.on_disconnected();

        assert_eq!(session.phase(), &PrivateSessionPhase::Disconnected);
        assert!(session.confirmed().is_empty());
        assert!(!session.is_established());
    }

    /// A session that ended permanently is not brought back by a reconnect.
    #[rstest]
    fn test_a_failed_session_is_not_revived_by_a_new_connection() {
        let mut session = session(PrivateStreamMode::Trading);
        session.on_connected();
        session.handle_frame(
            &frame("error", None, r#","msg":"signature_mismatch""#),
            now(),
        );

        session.on_connected();

        assert!(
            session.is_failed(),
            "a reconnect does not clear a refusal the adapter read as permanent",
        );
        assert!(session.failure().is_some());
    }

    /// An acknowledgement that names no channel this adapter carries is not a confirmation of
    /// anything, and it says so rather than being ignored.
    #[rstest]
    fn test_an_unknown_channel_is_reported_and_confirms_nothing() {
        let mut session = session(PrivateStreamMode::Trading);
        logged_in(&mut session);

        let outcome = session.handle_frame(&frame("subscribed", Some("balancePerps"), ""), now());

        assert!(matches!(
            outcome.events.as_slice(),
            [PrivateEvent::Unsupported { .. }],
        ));
        assert!(session.confirmed().is_empty());
        assert_eq!(session.counters().unknown_channels, 1);
    }

    /// An unknown message type is reported rather than folded into a known one.
    #[rstest]
    fn test_an_unknown_message_type_is_unsupported() {
        let mut session = session(PrivateStreamMode::Trading);
        logged_in(&mut session);

        let outcome = session.handle_frame(&frame("snapshot", Some("ordersPerps"), ""), now());

        let [PrivateEvent::Unsupported { reason }] = outcome.events.as_slice() else {
            panic!("an unknown type is unsupported: {:?}", outcome.events);
        };

        assert!(reason.contains("snapshot"), "{reason}");
        assert_eq!(session.counters().unknown_message_types, 1);
    }

    /// The heartbeat is the application-level ping, and a pong is not an account report.
    #[rstest]
    fn test_the_heartbeat_is_the_application_level_ping() {
        let idle = session(PrivateStreamMode::Trading);

        assert_eq!(idle.heartbeat(), PrivateAction::Heartbeat);
        assert!(!PrivateAction::Heartbeat.carries_a_credential());

        let mut session = session(PrivateStreamMode::Trading);
        let outcome = session.handle_frame(r#"{"type":"pong"}"#, now());

        assert_eq!(outcome.events, vec![PrivateEvent::Pong]);
        assert_eq!(session.counters().pongs, 1);
    }

    /// Only one action carries a credential, and it is the one the transport must keep out of
    /// every recording boundary.
    #[rstest]
    fn test_only_the_login_action_carries_a_credential() {
        let credential_actions: Vec<PrivateAction> = [
            PrivateAction::Login,
            PrivateAction::Subscribe(PrivateChannel::OrdersPerps),
            PrivateAction::Unsubscribe(PrivateChannel::OrdersPerps),
            PrivateAction::ArmSwitch,
            PrivateAction::ReleaseSwitch,
            PrivateAction::Heartbeat,
        ]
        .into_iter()
        .filter(|action| action.carries_a_credential())
        .collect();

        assert_eq!(credential_actions, vec![PrivateAction::Login]);
    }
}
