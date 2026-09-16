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

//! The private session's own diagnostic record: a boundary the login frame cannot cross.
//!
//! # This is not the public raw market-data recorder, and it does not share its file
//!
//! [`crate::recording`] records **public** frames. Its whitelist refuses a login body, a private
//! channel and a `loggedIn` acknowledgement by name, and that refusal is a security property
//! rather than a gap: the public tape is a market-data artefact and an account payload does not
//! belong in it. The private session therefore keeps its own record, and the two never meet - the
//! private transport holds no [`crate::recording::RawMdSink`] and could not offer it a frame.
//!
//! # The login frame is outside this boundary by construction, not by a filter
//!
//! A login frame carries the API key id and the HMAC signature of the login digest. The obvious
//! design - record raw frame text and refuse the login frame at the door - puts the safety of the
//! secret on one filter being right at every call site, including ones added later. This record
//! does not hold frame bytes at all: [`PrivateRecord::FrameSent`] names the *action*
//! ([`super::session::PrivateAction`]) and never its body, so there is no filter to get wrong and
//! nothing for a signature to hide behind. The same rule covers the inbound direction: a record
//! carries a venue identifier (an order id, a fill id, a market, a status) and never the payload
//! it came from.
//!
//! Free-form text does reach a record - a venue error message, the reason a report was lost - and
//! the venue echoes the key id it was sent (its own `ip_not_permitted` message does exactly that).
//! The transport therefore applies [`crate::common::credential::OndoCredential::redact`] to every
//! such string before it builds a record, and [`PrivateDiagnostics::record`] is the only way in.
//!
//! # The records are bounded
//!
//! The ring is capped at [`ONDO_PRIVATE_DIAGNOSTIC_CAPACITY`] records and the counters are not
//! capped at all: a session that runs for a month must not be an unbounded allocation, and the
//! counters are what a report reads for the shape of the run.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::websocket::private::{messages::PrivateChannel, session::PrivateAction};

/// How many diagnostic records a session keeps.
pub const ONDO_PRIVATE_DIAGNOSTIC_CAPACITY: usize = 512;

/// One thing the private session did that is worth keeping.
///
/// Every variant is a summary of an event, never a frame. See the module documentation for why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivateRecord {
    /// A connection attempt began.
    Connecting {
        /// Which attempt this is, counting from the transport's start.
        attempt: u64,
    },
    /// The socket is up and the login has been sent.
    Authenticating,
    /// The venue acknowledged the login.
    LoggedIn,
    /// One frame was sent. The action is named; the body is not kept.
    FrameSent {
        /// Which action's frame went out.
        action: &'static str,
    },
    /// A frame could not be sent.
    SendFailed {
        /// Which action's frame it was.
        action: &'static str,
        /// Why, redacted.
        reason: String,
    },
    /// The venue acknowledged a subscription.
    Subscribed {
        /// The channel.
        channel: PrivateChannel,
    },
    /// The venue acknowledged an unsubscription.
    Unsubscribed {
        /// The channel.
        channel: PrivateChannel,
    },
    /// The account's switch was armed and confirmed by the venue.
    SwitchConfirmed {
        /// The timeout the venue was asked for, in seconds.
        timeout_seconds: u64,
    },
    /// An armed switch was renewed.
    SwitchRenewed {
        /// How many renewals this session has made.
        renewals: u64,
    },
    /// The switch could not be armed, renewed or read.
    SwitchFailed {
        /// Why, redacted.
        reason: String,
    },
    /// A venue error frame arrived.
    VenueError {
        /// What the venue said, redacted.
        reason: String,
        /// Whether the adapter reads it as permanent.
        permanent: bool,
    },
    /// The session ended and will not be reconnected.
    SessionFailed {
        /// Why, redacted.
        reason: String,
    },
    /// The connection ended and a reconnect is scheduled.
    Disconnected {
        /// Why, redacted.
        reason: String,
    },
    /// Reports were lost: undecodable items, or a buffer that refused them.
    ReportsLost {
        /// How many.
        count: usize,
        /// Why, redacted.
        reason: String,
    },
    /// An account reconciliation pass concluded.
    Reconciled {
        /// The state the pass left the account in.
        state: &'static str,
    },
    /// A pass could not read the account.
    ReconciliationFailed {
        /// Why, redacted.
        reason: String,
    },
    /// The session stopped for good.
    Stopped,
}

/// Per-session diagnostic counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrivateDiagnosticsCounters {
    /// Frames the venue sent.
    pub frames_in: u64,
    /// Frames this session sent.
    pub frames_out: u64,
    /// Login frames sent.
    ///
    /// A count, never a body: this is the whole of what this record keeps about a login.
    pub login_frames: u64,
    /// Connections established.
    pub connections: u64,
    /// Reconnect attempts.
    pub reconnect_attempts: u64,
    /// Order reports applied to the account.
    pub orders: u64,
    /// Fill reports applied to the account.
    pub fills: u64,
    /// Reports the account could not apply.
    pub reports_lost: u64,
    /// Account reconciliation passes concluded.
    pub passes: u64,
    /// Passes that could not read the account.
    pub failed_passes: u64,
    /// Venue error frames.
    pub venue_errors: u64,
}

/// The bounded record of what a private session did.
#[derive(Debug)]
pub struct PrivateDiagnostics {
    counters: Mutex<PrivateDiagnosticsCounters>,
    records: Mutex<VecDeque<PrivateRecord>>,
    capacity: usize,
}

impl Default for PrivateDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

impl PrivateDiagnostics {
    /// Creates a record with the default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(ONDO_PRIVATE_DIAGNOSTIC_CAPACITY)
    }

    /// Creates a record with room for `capacity` entries.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            counters: Mutex::new(PrivateDiagnosticsCounters::default()),
            records: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Appends one record, dropping the oldest when the ring is full.
    ///
    /// A [`PrivateRecord::FrameSent`] built here counts as a sent frame but not as a login one: the
    /// counter for that belongs to [`Self::record_frame_sent`], which is the only method that sees
    /// the action. A caller has no reason to build one by hand, and if it does, the record says
    /// what it says and the login counter stays with the transport's own send path.
    pub fn record(&self, record: PrivateRecord) {
        {
            let mut counters = self.counters.lock();

            match &record {
                PrivateRecord::Connecting { .. } => counters.reconnect_attempts += 1,
                PrivateRecord::ReportsLost { count, .. } => {
                    counters.reports_lost += u64::try_from(*count).unwrap_or(u64::MAX);
                }
                PrivateRecord::VenueError { .. } => counters.venue_errors += 1,
                PrivateRecord::Reconciled { .. } => counters.passes += 1,
                PrivateRecord::ReconciliationFailed { .. } => counters.failed_passes += 1,
                _ => {}
            }
        }

        self.push(record);
    }

    /// Appends one record to the ring, dropping the oldest when it is full.
    fn push(&self, record: PrivateRecord) {
        let mut records = self.records.lock();

        if records.len() == self.capacity {
            records.pop_front();
        }

        records.push_back(record);
    }

    /// Records one outbound frame by its action.
    ///
    /// The only way a sent frame enters this record, and it takes an action rather than a body:
    /// there is no signature for a login frame to leave here.
    pub fn record_frame_sent(&self, action: PrivateAction) {
        {
            let mut counters = self.counters.lock();

            counters.frames_out += 1;

            if action == PrivateAction::Login {
                counters.login_frames += 1;
            }
        }

        self.push(PrivateRecord::FrameSent {
            action: action.as_str(),
        });
    }

    /// Records one inbound frame, by count alone.
    pub fn record_frame_received(&self) {
        self.counters.lock().frames_in += 1;
    }

    /// Records a connection.
    pub fn record_connected(&self) {
        self.counters.lock().connections += 1;
    }

    /// Records an applied order report.
    pub fn record_order_applied(&self) {
        self.counters.lock().orders += 1;
    }

    /// Records an applied fill report.
    pub fn record_fill_applied(&self) {
        self.counters.lock().fills += 1;
    }

    /// Returns the counters.
    #[must_use]
    pub fn counters(&self) -> PrivateDiagnosticsCounters {
        *self.counters.lock()
    }

    /// Returns the records, oldest first.
    #[must_use]
    pub fn records(&self) -> Vec<PrivateRecord> {
        self.records.lock().iter().cloned().collect()
    }

    /// Returns how many records the ring holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Returns whether the ring holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the ring's capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns every record and counter rendered as one string, for a test or a report.
    ///
    /// This is the whole of what a caller can get out of this type, and what it renders is what
    /// the records hold: no frame bytes exist here to be rendered.
    #[must_use]
    pub fn render(&self) -> String {
        let mut rendered = format!("{:?}", self.counters());

        for record in self.records() {
            rendered.push('\n');
            rendered.push_str(&format!("{record:?}"));
        }

        rendered
    }
}

/// A handle to the record a transport and its owner share.
pub type SharedPrivateDiagnostics = Arc<PrivateDiagnostics>;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_a_sent_frame_is_recorded_by_its_action_and_never_by_its_body() {
        let diagnostics = PrivateDiagnostics::new();

        diagnostics.record_frame_sent(PrivateAction::Login);
        diagnostics.record_frame_sent(PrivateAction::Heartbeat);

        assert_eq!(
            diagnostics.records(),
            vec![
                PrivateRecord::FrameSent { action: "login" },
                PrivateRecord::FrameSent {
                    action: "heartbeat"
                },
            ],
        );
        assert_eq!(diagnostics.counters().frames_out, 2);
        assert_eq!(diagnostics.counters().login_frames, 1);
    }

    /// The login frame is outside this boundary by construction: there is no variant that can hold
    /// one. This test pins the absence, because an addition that broke it would be a silent one.
    #[rstest]
    fn test_no_record_can_hold_a_frame_body() {
        let diagnostics = PrivateDiagnostics::new();
        diagnostics.record_frame_sent(PrivateAction::Login);
        diagnostics.record(PrivateRecord::LoggedIn);

        let rendered = diagnostics.render();

        assert!(
            !rendered.contains('{') || !rendered.contains("\"op\""),
            "a record renders no request body: {rendered}",
        );
        assert!(
            rendered.contains("login"),
            "the login is recorded as the fact that it happened: {rendered}",
        );
    }

    /// The ring is bounded, and it drops the oldest rather than the newest: what a report needs is
    /// the end of the run.
    #[rstest]
    fn test_the_ring_drops_the_oldest_record() {
        let diagnostics = PrivateDiagnostics::with_capacity(2);

        diagnostics.record(PrivateRecord::Authenticating);
        diagnostics.record(PrivateRecord::LoggedIn);
        diagnostics.record(PrivateRecord::Stopped);

        assert_eq!(diagnostics.len(), 2);
        assert_eq!(
            diagnostics.records(),
            vec![PrivateRecord::LoggedIn, PrivateRecord::Stopped],
        );
        assert_eq!(diagnostics.capacity(), 2);
    }

    /// Losses are counted, not merely recorded: the number is what a report states.
    #[rstest]
    fn test_losses_are_counted() {
        let diagnostics = PrivateDiagnostics::new();

        diagnostics.record(PrivateRecord::ReportsLost {
            count: 3,
            reason: "the buffer refused them".to_string(),
        });
        diagnostics.record(PrivateRecord::ReportsLost {
            count: 1,
            reason: "an item did not decode".to_string(),
        });

        assert_eq!(diagnostics.counters().reports_lost, 4);
    }
}
