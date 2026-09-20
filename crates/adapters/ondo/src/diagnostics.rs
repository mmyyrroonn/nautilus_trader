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

//! A bounded, sanitized, read-only view of one execution client's private-session diagnostics.
//!
//! The application needs to report what a native read-only production run actually did - whether the
//! venue accepted the login, which report channels it acknowledged, where the run state ended,
//! whether a recovery converged and whether the shutdown was clean - without seeing a frame, a
//! credential, an account id, an order id or a balance. This module is that view, and it is
//! deliberately the only one: [`OndoReadOnlyDiagnostics`] holds the account runtime, the run token
//! the application supplied, and, once a private transport is started, that transport's run-state
//! and diagnostic handles; [`OndoReadOnlyDiagnostics::snapshot`] projects them into a value whose
//! every member is a counter, a fixed label or an enum name.
//!
//! # Bounded by construction, and not by the record ring
//!
//! Nothing here copies a frame or a payload. The counters, the accepted-login flag and the
//! acknowledged-channel set live beside the bounded [`PrivateRecord`] ring
//! ([`crate::websocket::private::diagnostics`]), so a login acknowledgment or a channel
//! acknowledgement survives the ring dropping its record. The channel set cannot exceed the handful
//! of channels this adapter subscribes to.
//!
//! # One store per client, and one token per run
//!
//! The store is created by the execution client from the configuration's `diagnostics_run_id` and
//! lives for that client's run. The execution factory retains a clone of the *handle*, so the
//! application can read the snapshot from the factory it built the client with, and it can never
//! write to it: the Python surface exposes the snapshot and no setter. A second client built by the
//! same factory replaces the handle, so counters are never carried across runs; the
//! `diagnostics_run_id` the snapshot echoes is what the application uses to prove the snapshot is
//! its own run's.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::{
    execution::OndoAccountRuntime,
    websocket::private::{
        PrivateChannel, PrivateRunSnapshot, PrivateRunState, SharedPrivateDiagnostics,
    },
};

/// How an owned shutdown ended.
///
/// The spelling of [`Self::Clean`] (`complete`) and [`Self::Dirty`] (`incomplete`) matches the
/// application's declared shutdown vocabulary and the native [`StopOutcome`] variants. The two
/// lifecycle states (`not_attempted`, `running`) and `stopping` are this adapter's own and are
/// recorded in `native-readonly/interface.md`; an application allowlist that does not carry them
/// reads `unknown`, never a positive.
///
/// [`StopOutcome`]: crate::reconciliation::StopOutcome
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OwnedShutdownStatus {
    /// No stop or disconnect has been attempted.
    #[default]
    NotAttempted,
    /// The client is connected and its private transport is running.
    Running,
    /// A stop is in progress.
    Stopping,
    /// The ordered stop drained and confirmed everything it owned.
    Clean,
    /// The shutdown left work unresolved.
    Dirty,
}

impl OwnedShutdownStatus {
    /// Returns the status's name, for a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Clean => "complete",
            Self::Dirty => "incomplete",
        }
    }
}

/// The run-state and diagnostic handles of a started private transport.
#[derive(Clone, Debug)]
struct StreamDiagnostics {
    run: Arc<Mutex<PrivateRunSnapshot>>,
    diagnostics: SharedPrivateDiagnostics,
}

/// A sanitized, bounded snapshot of a read-only run.
///
/// Every member is a counter, a fixed label or an enum name: there is no frame, credential, account
/// id, order id or monetary field to render. The member names are the application's required
/// contract (`native-readonly/interface.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoReadOnlySnapshot {
    /// The run token the application supplied through `diagnostics_run_id`.
    pub run_id: String,
    /// Whether the venue acknowledged a private login this run.
    pub logged_in: bool,
    /// The report channels the venue acknowledged, deduplicated, in the adapter's spelling.
    pub subscriptions_acked: Vec<&'static str>,
    /// The private transport's run state.
    pub run_state: &'static str,
    /// Reconnect attempts this run.
    pub reconnects: u64,
    /// Account reconciliation passes this run concluded.
    pub recoveries: u64,
    /// Verified account states published into the engine.
    pub account_state_events: u64,
    /// The authenticated account identity.
    pub identity_match: &'static str,
    /// The owned shutdown's status.
    pub shutdown_status: &'static str,
}

/// A per-client, read-only diagnostics store.
///
/// The execution client owns one and the factory retains a clone of the handle, so an application
/// can read a bounded snapshot after a run. The store is mutated only from Rust; the Python surface
/// can read it and cannot set it.
#[derive(Clone, Debug)]
pub struct OndoReadOnlyDiagnostics {
    run_id: String,
    account: OndoAccountRuntime,
    stream: Arc<RwLock<Option<StreamDiagnostics>>>,
    shutdown: Arc<RwLock<OwnedShutdownStatus>>,
}

impl OndoReadOnlyDiagnostics {
    /// Creates a store for `run_id` over `account`.
    #[must_use]
    pub fn new(run_id: String, account: OndoAccountRuntime) -> Self {
        Self {
            run_id,
            account,
            stream: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(RwLock::new(OwnedShutdownStatus::NotAttempted)),
        }
    }

    /// Attaches a started private transport's live handles.
    pub fn attach_stream(
        &self,
        run: Arc<Mutex<PrivateRunSnapshot>>,
        diagnostics: SharedPrivateDiagnostics,
    ) {
        *self.stream.write() = Some(StreamDiagnostics { run, diagnostics });
        *self.shutdown.write() = OwnedShutdownStatus::Running;
    }

    /// Records the owned shutdown's status.
    pub fn set_shutdown(&self, status: OwnedShutdownStatus) {
        *self.shutdown.write() = status;
    }

    /// Returns the owned shutdown's status.
    #[must_use]
    pub fn shutdown_status(&self) -> OwnedShutdownStatus {
        *self.shutdown.read()
    }

    /// Builds a bounded, sanitized snapshot.
    #[must_use]
    pub fn snapshot(&self) -> OndoReadOnlySnapshot {
        let counters = self
            .stream
            .read()
            .as_ref()
            .map(|stream| stream.diagnostics.counters())
            .unwrap_or_default();

        let acked = self.acknowledged_report_channels();

        let run_state = {
            let stream = self.stream.read();

            match stream.as_ref() {
                Some(stream) => stream.run.lock().state.as_str(),
                None => PrivateRunState::Disconnected.as_str(),
            }
        };

        OndoReadOnlySnapshot {
            run_id: self.run_id.clone(),
            logged_in: counters.logins_acknowledged > 0,
            subscriptions_acked: acked,
            run_state,
            reconnects: counters.reconnect_attempts,
            recoveries: counters.passes,
            account_state_events: self.account.account_state_published(),
            identity_match: self.account.account_identity().as_str(),
            shutdown_status: self.shutdown_status().as_str(),
        }
    }

    /// Returns the acknowledged report channels, in the fixed report-channel order.
    ///
    /// The set is kept beside the bounded record ring, so a channel acknowledgement survives the
    /// ring dropping its [`PrivateRecord::Subscribed`] entry.
    fn acknowledged_report_channels(&self) -> Vec<&'static str> {
        let guard = self.stream.read();
        let Some(stream) = guard.as_ref() else {
            return Vec::new();
        };

        let acknowledged = stream.diagnostics.acknowledged_channels();

        PrivateChannel::READ_ONLY
            .into_iter()
            .filter(|channel| acknowledged.contains(channel))
            .map(PrivateChannel::as_str)
            .collect()
    }
}

/// Every value the identity enum can take, as the snapshot renders it.
#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::enums::OndoAccountIdentity;

    #[rstest]
    #[case(OwnedShutdownStatus::NotAttempted, "not_attempted")]
    #[case(OwnedShutdownStatus::Running, "running")]
    #[case(OwnedShutdownStatus::Stopping, "stopping")]
    #[case(OwnedShutdownStatus::Clean, "complete")]
    #[case(OwnedShutdownStatus::Dirty, "incomplete")]
    fn test_owned_shutdown_status_names(
        #[case] status: OwnedShutdownStatus,
        #[case] expected: &str,
    ) {
        assert_eq!(status.as_str(), expected);
    }

    #[rstest]
    fn test_identity_names_are_matched_mismatch_and_unknown() {
        assert_eq!(OndoAccountIdentity::default(), OndoAccountIdentity::Unknown);
        assert_eq!(OndoAccountIdentity::Matched.as_str(), "matched");
        assert_eq!(OndoAccountIdentity::Mismatch.as_str(), "mismatch");
        assert_eq!(OndoAccountIdentity::Unknown.as_str(), "unknown");
    }
}
