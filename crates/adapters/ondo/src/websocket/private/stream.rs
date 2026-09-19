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

//! The private transport: the handle, the task-private state, and the run loop.
//!
//! **This is the only file in the crate that touches a private socket, and the only place the
//! account's hooks are called.** Everything decided here is a decision about *when*; what a report
//! does to the account is [`crate::execution::OndoAccountRuntime`]'s, and the transport reaches it
//! through that type's methods rather than by touching the account's state.
//!
//! # One owner, one socket
//!
//! There is one transport per execution client, one socket per transport, and one task per socket.
//! The run loop is a single `tokio::select!` over {socket read, heartbeat, idle bound, login
//! deadline, reconcile timer, switch renewal deadline, metadata refresh, shutdown}, so every
//! transition happens on one task and nothing races the socket.
//!
//! # What runs beside the loop, and how it is owned
//!
//! Two things outlive a single turn of the loop - an account reconciliation pass and a metadata
//! read - because both make HTTP requests, and running them inline would stop the loop reading the
//! socket and sending the heartbeat for as long as the venue took to answer. A slow pass would then
//! delay an order report and could let the venue idle the connection out.
//!
//! They run in a [`JoinSet`] this state owns, which is what makes "no dangling task" provable
//! rather than asserted: the loop drains the set before it returns, and the handle's liveness token
//! is alive exactly while the task's frame is. Two *passes* cannot overlap anyway - the account
//! refuses a second claim through its single
//! [`crate::execution::PassOwnership`](crate::execution) - but that is the account's rule and this
//! set is the transport's own bookkeeping.
//!
//! # Socket up is not account ready
//!
//! [`PrivateRunState`] keeps the two apart, and the loop recomputes it from both the session's
//! protocol phase and the account's own reconciliation state. A socket that is logged in and
//! subscribed while the account is unrecovered reads [`PrivateRunState::Recovering`], never
//! [`PrivateRunState::TradingReady`].

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use futures_util::StreamExt;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_network::{
    backoff::ExponentialBackoff,
    transport::{Message, TransportError},
    websocket::{MessageReader, TransportBackend, WebSocketClient, WebSocketConfig},
};
use parking_lot::Mutex;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    common::credential::OndoCredential,
    execution::{OndoAccountRuntime, OndoStreamIngestion},
    reconciliation::{
        DeadMansSwitchMessage, DeadMansSwitchState, MetadataValidity, ReconciliationState,
    },
    signing::{now_millis, sign_ws},
    websocket::{
        client::{
            ONDO_WS_IDLE_TIMEOUT_SECS, ONDO_WS_MAX_CLIENT_MESSAGE_BYTES, reconnect_backoff,
            request_quota,
        },
        messages::{PingRequest, WsOp},
        private::{
            diagnostics::{PrivateDiagnostics, PrivateRecord, SharedPrivateDiagnostics},
            messages::{LoginRequest, PrivateChannel, PrivateSubscriptionRequest},
            session::{
                OndoPrivateSession, PrivateAction, PrivateEvent, PrivateSessionPhase,
                PrivateStreamMode,
            },
        },
    },
};

/// How long the venue is given to answer a login, in seconds.
///
/// A login the venue never answers is a failed attempt rather than a hung session: it counts
/// against [`crate::websocket::private::session::ONDO_WS_LOGIN_MAX_ATTEMPTS`] and the connection is
/// dropped, so a venue that silently swallows logins cannot be retried forever either.
pub const ONDO_WS_LOGIN_TIMEOUT_SECS: u64 = 10;

/// The interval between low-priority account metadata refreshes, in seconds.
///
/// The same cadence the data client's own refresh uses
/// ([`crate::common::consts::ONDO_METADATA_REFRESH_INTERVAL_SECS`]): one number for "how stale the
/// venue's instrument metadata may be before this adapter stops trusting it".
pub const ONDO_ACCOUNT_METADATA_REFRESH_SECS: u64 = 60;

/// How long the transport waits for its task to end before abandoning the wait, in seconds.
pub const ONDO_PRIVATE_STREAM_STOP_TIMEOUT_SECS: u64 = 2;

/// How often the run state is recomputed from scratch, in milliseconds.
///
/// [`PrivateRunState`] is a statement about two things - the session's phase and the account's own
/// state - and the second can be moved by a caller the transport does not own: the probe's account
/// mode drives [`crate::execution::OndoAccountRuntime::reconcile_account`] itself, and a test does
/// the same. The transport's own passes say so through its wake channel; this tick is what makes
/// the report correct no matter who moved the account, and it costs two lock reads.
pub const ONDO_PRIVATE_STATE_TICK_MS: u64 = 250;

/// Where a private session stands, as its owner sees it.
///
/// This is **not** the account's reconciliation state and not the session's protocol phase: it is
/// the answer to "what is this client able to do right now", which needs both. A socket being up is
/// not the account being ready, and a session whose switch is unconfirmed is not trading ready
/// however clean the account reads
/// ([`crate::reconciliation::DeadMansSwitch::permits_new_orders`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateRunState {
    /// No connection is up.
    Disconnected,
    /// A socket is up and the login frame has been sent.
    Authenticating,
    /// Logged in; the account has not converged.
    Recovering,
    /// A read-only session with a converged account: the account was read, and no order will be
    /// placed from it.
    ReadOnlySynced,
    /// A trading session with a converged account and a confirmed switch.
    TradingReady,
    /// Something could not be accounted for: a lost report, an unreadable switch frame, a session
    /// the venue refused, or a pass that could not read the account.
    Uncertain,
    /// A stop is in progress.
    Stopping,
    /// The transport task has ended.
    Stopped,
}

impl PrivateRunState {
    /// Returns the state's name, for a log line or a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Authenticating => "authenticating",
            Self::Recovering => "recovering",
            Self::ReadOnlySynced => "read_only_synced",
            Self::TradingReady => "trading_ready",
            Self::Uncertain => "uncertain",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    /// Returns whether this state is the one that places orders.
    ///
    /// It is a report, not the authority: the account's own admission decision is what governs a
    /// submission, and it is checked again at the send point
    /// ([`crate::http::client::OndoNewRiskGuard`]).
    #[must_use]
    pub const fn permits_new_orders(self) -> bool {
        matches!(self, Self::TradingReady)
    }
}

/// The run state and the detail that explains it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivateRunSnapshot {
    /// The state.
    pub state: PrivateRunState,
    /// Why, in the adapter's own words.
    pub detail: String,
}

impl PrivateRunSnapshot {
    /// Builds a snapshot.
    #[must_use]
    pub fn new(state: PrivateRunState, detail: impl Into<String>) -> Self {
        Self {
            state,
            detail: detail.into(),
        }
    }
}

/// The live private session: its run state, its diagnostics and the task that owns its socket.
///
/// The client owns one of these for the life of a connection and drives it with [`Self::stop`].
/// Dropping it cancels the task; a caller that needs the task *proven* finished calls
/// [`Self::stop`] and then [`Self::is_running`].
#[derive(Debug)]
pub struct OndoPrivateStream {
    url: String,
    run: Arc<Mutex<PrivateRunSnapshot>>,
    diagnostics: SharedPrivateDiagnostics,
    /// The connection that is up right now, when one is.
    ///
    /// The run loop owns the connection and this is a second handle to the same client, so a frame
    /// can be written from outside the loop - which is what the stop sequence's release step needs,
    /// because a frame queued behind a task that is about to be cancelled is a frame never sent.
    /// [`WebSocketClient::send_text`] takes `&self`, so the two writers cannot conflict.
    active: Arc<Mutex<Option<Arc<WebSocketClient>>>>,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    /// Alive exactly while the task's frame is: a panic drops it too, which a flag would not.
    liveness: Weak<()>,
}

impl Drop for OndoPrivateStream {
    fn drop(&mut self) {
        self.cancellation.cancel();

        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl OndoPrivateStream {
    /// Starts a private transport for `url`.
    ///
    /// The task is spawned immediately and attempts its first connection without the caller having
    /// to do anything, exactly as the public transport does.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no Tokio runtime to host the transport task, or if the
    /// reconnect backoff cannot be constructed.
    pub fn start(
        url: String,
        account: OndoAccountRuntime,
        credential: Arc<OndoCredential>,
        mode: PrivateStreamMode,
        heartbeat_secs: u64,
    ) -> anyhow::Result<Self> {
        let backoff = reconnect_backoff()?;
        let run = Arc::new(Mutex::new(PrivateRunSnapshot::new(
            PrivateRunState::Disconnected,
            "the transport has not connected yet",
        )));
        let diagnostics = Arc::new(PrivateDiagnostics::new());
        let cancellation = CancellationToken::new();
        let liveness = Arc::new(());
        let (wake, wake_rx) = mpsc::unbounded_channel();
        let active: Arc<Mutex<Option<Arc<WebSocketClient>>>> = Arc::new(Mutex::new(None));

        let state = PrivateTransportState {
            url: url.clone(),
            heartbeat_secs: heartbeat_secs.max(1),
            mode,
            account,
            credential,
            diagnostics: Arc::clone(&diagnostics),
            run: Arc::clone(&run),
            cancellation: cancellation.clone(),
            active: Arc::clone(&active),
            wake,
        };

        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            anyhow::anyhow!(
                "the Ondo private WebSocket transport needs a Tokio runtime to host its \
                 connection task: {error}"
            )
        })?;
        let task = runtime.spawn(state.run(backoff, Arc::clone(&liveness), wake_rx));

        Ok(Self {
            url,
            run,
            diagnostics,
            active,
            cancellation,
            task: Some(task),
            liveness: Arc::downgrade(&liveness),
        })
    }

    /// Returns the WebSocket URL this transport connects to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the run state and the detail that explains it.
    #[must_use]
    pub fn run_state(&self) -> PrivateRunSnapshot {
        self.run.lock().clone()
    }

    /// Returns the session's diagnostic record.
    #[must_use]
    pub fn diagnostics(&self) -> SharedPrivateDiagnostics {
        Arc::clone(&self.diagnostics)
    }

    /// Returns whether the transport task is still alive.
    ///
    /// The answer comes from the task's own liveness token rather than from a flag the task would
    /// have to remember to clear, so a task that panicked reads as ended.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.liveness.strong_count() > 0
    }

    /// Writes one switch frame on the live socket, and says whether it reached it.
    ///
    /// It exists for the stop sequence's release step, which has to put its frame on the wire
    /// *before* the transport is closed - [`Self::stop`] cancels the task, and a frame queued behind
    /// a cancelled task is a frame that was never sent. The socket is shared with the run loop
    /// through its own lock, so the write is the same write the loop would make.
    ///
    /// # Errors
    ///
    /// Returns an error when there is no connection to write to, when the frame cannot be
    /// serialized, or when the socket refuses it. None of them is recoverable at this point - the
    /// caller is stopping - so the error is reported rather than retried.
    pub async fn send_switch_frame(&self, frame: &DeadMansSwitchMessage) -> anyhow::Result<()> {
        let body = frame.to_json_text()?;
        // Cloned out of the slot rather than held across the await: the guard is not `Send`, and
        // the client is a handle whose writes are serialized by the socket itself.
        let client = self
            .active
            .lock()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("the private transport has no live connection"))?;

        if !client.is_active() {
            anyhow::bail!("the private transport's connection is no longer active");
        }

        send_body(&client, &body).await?;
        self.diagnostics
            .record_frame_sent(PrivateAction::ReleaseSwitch);

        Ok(())
    }

    /// Stops the transport and waits, boundedly, for its task to end.
    ///
    /// The wait is bounded so a shutdown can never hang on a wedged socket; the task is aborted
    /// when the bound expires, and [`Self::is_running`] then still reports it as alive until it has
    /// actually finished.
    pub async fn stop(&mut self) {
        self.set_run(PrivateRunState::Stopping, "the transport was asked to stop");
        self.cancellation.cancel();

        if let Some(task) = self.task.as_mut() {
            if tokio::time::timeout(
                Duration::from_secs(ONDO_PRIVATE_STREAM_STOP_TIMEOUT_SECS),
                &mut *task,
            )
            .await
            .is_err()
            {
                task.abort();

                if tokio::time::timeout(Duration::from_secs(1), &mut *task)
                    .await
                    .is_err()
                {
                    log::error!("Ondo private transport has not finished forced cancellation");
                    return;
                }
            }

            self.task = None;
        }
    }

    /// Forces the transport to stop while retaining its task for the async close to join.
    pub(crate) fn abort(&mut self) {
        self.set_run(
            PrivateRunState::Stopping,
            "the transport was forced to stop",
        );
        self.cancellation.cancel();
        self.active.lock().take();

        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    /// Forces the run state.
    fn set_run(&self, state: PrivateRunState, detail: impl Into<String>) {
        *self.run.lock() = PrivateRunSnapshot::new(state, detail);
    }
}

/// The task-private transport state: one connection at a time, its session, and the reconnect loop.
#[derive(Debug)]
struct PrivateTransportState {
    url: String,
    heartbeat_secs: u64,
    mode: PrivateStreamMode,
    account: OndoAccountRuntime,
    credential: Arc<OndoCredential>,
    diagnostics: SharedPrivateDiagnostics,
    run: Arc<Mutex<PrivateRunSnapshot>>,
    cancellation: CancellationToken,
    /// Tells the run loop that the account moved and its run state is worth recomputing.
    ///
    /// The run state is a statement about two things - the session's phase and the account's own
    /// state - and it is the loop that holds the session. A pass finishing beside the loop, or a
    /// metadata read landing, is therefore a change the loop cannot see; this is how it hears
    /// about it. The sender lives here for the whole run, so the loop's `recv` never produces the
    /// [`None`] that would leave the branch ready and unmatched.
    wake: mpsc::UnboundedSender<()>,
    /// The connection that is up right now, shared with [`OndoPrivateStream`].
    ///
    /// The run loop publishes each connection here for exactly as long as it serves it, so a frame
    /// written from outside the loop reaches the same socket rather than waiting for a task that
    /// may be cancelled underneath it.
    active: Arc<Mutex<Option<Arc<WebSocketClient>>>>,
}

impl PrivateTransportState {
    /// The reconnect loop: connect, serve, note the end, wait, repeat.
    async fn run(
        self,
        mut backoff: ExponentialBackoff,
        liveness: Arc<()>,
        mut wake_rx: mpsc::UnboundedReceiver<()>,
    ) {
        // Held for the whole of the task's life. Dropping it - on the way out, or on a panic that
        // unwinds through here - is what tells the handle the task has finished.
        let _liveness = liveness;

        let mut session = OndoPrivateSession::new(self.mode);
        let mut attempt = 0_u64;
        let mut passes = JoinSet::new();

        self.set_run(PrivateRunState::Disconnected, "no connection is up");

        loop {
            if self.cancellation.is_cancelled() {
                break;
            }

            attempt += 1;
            self.diagnostics
                .record(PrivateRecord::Connecting { attempt });

            match self.connect_once().await {
                Ok((reader, client)) => {
                    backoff.reset();
                    self.diagnostics.record_connected();
                    *self.active.lock() = Some(Arc::clone(&client));

                    self.serve_connection(&mut session, reader, &client, &mut passes, &mut wake_rx)
                        .await;

                    // Cleared before the disconnect, so a release that arrives mid-teardown finds
                    // no connection rather than a dead one.
                    *self.active.lock() = None;
                    client.disconnect().await;
                }
                Err(error) => {
                    log::warn!("Ondo private WebSocket connection attempt failed: {error}");
                    self.note_disconnected(&error.to_string());
                }
            }

            session.on_disconnected();
            self.account.note_session_ended(self.clock());
            self.refresh_run(&session, "the connection ended");

            if session.is_failed() || self.cancellation.is_cancelled() {
                break;
            }

            let delay = backoff.next_duration();
            log::debug!("Ondo private WebSocket reconnecting in {delay:?}");

            tokio::select! {
                () = self.cancellation.cancelled() => break,
                () = tokio::time::sleep(delay) => {}
            }
        }

        // The loop does not return until everything it started has ended: an abandoned pass would
        // be a task nobody owns, which is what a bounded shutdown is for.
        passes.shutdown().await;

        if let Some(reason) = session.failure() {
            self.diagnostics.record(PrivateRecord::SessionFailed {
                reason: self.redact(reason),
            });
        }

        let reason = session.failure().map_or_else(
            || "the transport was stopped".to_string(),
            ToString::to_string,
        );

        self.set_run(PrivateRunState::Stopped, reason);
        self.diagnostics.record(PrivateRecord::Stopped);
    }

    /// Serves one connection from the login to its end.
    async fn serve_connection(
        &self,
        session: &mut OndoPrivateSession,
        mut reader: MessageReader,
        client: &WebSocketClient,
        passes: &mut JoinSet<()>,
        wake_rx: &mut mpsc::UnboundedReceiver<()>,
    ) {
        let heartbeat = Duration::from_secs(self.heartbeat_secs);
        let idle = Duration::from_secs(ONDO_WS_IDLE_TIMEOUT_SECS);
        let reconcile = Duration::from_secs(self.account.reconcile_interval_secs().max(1));
        let metadata = Duration::from_secs(ONDO_ACCOUNT_METADATA_REFRESH_SECS);
        // Half the switch's own timeout: the renewal has to reach the venue before the venue's
        // timer does, and one lost renewal must not be fatal.
        let renewal = Duration::from_secs((self.account.dms_timeout_secs() / 2).max(1));

        let mut heartbeat_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
        let mut reconcile_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + reconcile, reconcile);
        let mut metadata_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + metadata, metadata);
        let mut renewal_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + renewal, renewal);
        let mut state_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_millis(ONDO_PRIVATE_STATE_TICK_MS),
            Duration::from_millis(ONDO_PRIVATE_STATE_TICK_MS),
        );
        let mut idle_deadline = tokio::time::Instant::now() + idle;
        let login_deadline =
            tokio::time::Instant::now() + Duration::from_secs(ONDO_WS_LOGIN_TIMEOUT_SECS);

        let mut logged_in = false;

        // A recovery is a decision to read the account rather than a socket coming up - but a
        // socket coming up is exactly when this process stops being able to vouch for what it
        // holds, so it is also when the read is asked for.
        self.account.begin_recovery(self.clock());
        self.diagnostics.record(PrivateRecord::Authenticating);

        for action in session.on_connected().actions {
            if !self.send_action(client, action).await {
                return;
            }
        }

        loop {
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep_until(idle_deadline) => {
                    log::warn!(
                        "Ondo private WebSocket received no frame for {ONDO_WS_IDLE_TIMEOUT_SECS}s; \
                         reconnecting",
                    );

                    return;
                }
                () = tokio::time::sleep_until(login_deadline), if !logged_in => {
                    let reason = format!(
                        "the venue did not answer the login within {ONDO_WS_LOGIN_TIMEOUT_SECS}s",
                    );

                    log::warn!("Ondo private login was not answered in time");
                    session.note_login_timeout();
                    self.diagnostics.record(PrivateRecord::VenueError {
                        reason,
                        permanent: false,
                    });

                    return;
                }
                _ = heartbeat_tick.tick() => {
                    let action = session.heartbeat();

                    if !self.send_action(client, action).await {
                        return;
                    }
                }
                Some(()) = wake_rx.recv() => {
                    self.refresh_run(session, "the account moved beside the loop");
                }
                _ = state_tick.tick() => {
                    self.refresh_run(session, "the session is running");
                }
                _ = reconcile_tick.tick() => self.spawn_reconciliation(passes),
                _ = metadata_tick.tick() => self.spawn_metadata_refresh(passes),
                _ = renewal_tick.tick() => {
                    if !self.renew_switch(client).await {
                        return;
                    }
                }
                message = reader.next() => {
                    idle_deadline = tokio::time::Instant::now() + idle;

                    let text = match classify_frame(message) {
                        Inbound::Text(text) => text,
                        Inbound::Ignore => continue,
                        Inbound::End => return,
                    };

                    self.diagnostics.record_frame_received();

                    let ts_init = self.clock();
                    let outcome = session.handle_frame(&text, ts_init);

                    for event in outcome.events {
                        if matches!(event, PrivateEvent::LoggedIn) {
                            logged_in = true;
                            // Not before the login: the metadata read is an authenticated
                            // session's own evidence about what it may trade, and a session that
                            // has not authenticated has nothing to price.
                            self.spawn_metadata_refresh(passes);
                        }

                        if self.apply_event(event).await {
                            return;
                        }
                    }

                    for action in outcome.actions {
                        if !self.send_action(client, action).await {
                            return;
                        }
                    }

                    self.refresh_run(session, "the session is running");
                }
            }
        }
    }

    /// Applies one event to the account.
    ///
    /// Returns `true` when the connection must end.
    async fn apply_event(&self, event: PrivateEvent) -> bool {
        match event {
            PrivateEvent::Pong | PrivateEvent::Ignored { .. } => false,
            PrivateEvent::LoggedIn => {
                log::info!("Ondo private session authenticated");
                self.diagnostics.record(PrivateRecord::LoggedIn);

                false
            }
            PrivateEvent::Subscribed(channel) => {
                self.diagnostics
                    .record(PrivateRecord::Subscribed { channel });

                if channel == PrivateChannel::CancelAllOrdersAfterPerps {
                    self.account.confirm_dead_mans_switch(self.clock());
                    self.diagnostics.record(PrivateRecord::SwitchConfirmed {
                        timeout_seconds: self.account.dms_timeout_secs(),
                    });
                }

                false
            }
            PrivateEvent::Unsubscribed(channel) => {
                self.diagnostics
                    .record(PrivateRecord::Unsubscribed { channel });

                false
            }
            PrivateEvent::Order(order) => {
                if self.account.ingest_stream_order(*order) == OndoStreamIngestion::Applied {
                    self.diagnostics.record_order_applied();
                }

                false
            }
            PrivateEvent::Fill(fill) => {
                if self.account.ingest_stream_fill(*fill) == OndoStreamIngestion::Applied {
                    self.diagnostics.record_fill_applied();
                }

                false
            }
            PrivateEvent::Unsupported { reason } => {
                log::warn!("Ondo private stream did not handle a frame: {reason}");

                false
            }
            // A frame this adapter could not read is a report the account lost. It is not the
            // socket's problem - the frozen schemas declare `required: []`, so the venue may
            // legally send one - so the connection stays and the account is told, which is what
            // stops it reading Ready over a hole (plan §R3.1).
            PrivateEvent::ProtocolError { reason } => {
                log::error!("Ondo private stream could not decode a frame: {reason}");
                let redacted = self.redact(&reason);

                self.diagnostics.record(PrivateRecord::ReportsLost {
                    count: 1,
                    reason: redacted.clone(),
                });
                self.account.note_lost_reports(1, redacted);

                false
            }
            PrivateEvent::VenueError { reason, permanent } => {
                log::error!("Ondo private WebSocket error: {reason}");
                let redacted = self.redact(&reason);

                self.diagnostics.record(PrivateRecord::VenueError {
                    reason: redacted.clone(),
                    permanent,
                });

                // An error while the switch is still waiting for its acknowledgement means the
                // acknowledgement may never have been sent. Fail closed rather than assume the arm
                // took (plan §6.4: an unconfirmed arm is not an arm).
                if self.account.dead_mans_switch_is_arming() {
                    self.note_switch_failed(redacted);
                }

                permanent
            }
            PrivateEvent::SwitchChannelUpdate { reason } => {
                log::error!("Ondo private switch channel: {reason}");
                self.note_switch_failed(self.redact(&reason));

                false
            }
        }
    }

    /// Sends one frame the session asked for.
    ///
    /// Returns `false` when the connection must end.
    async fn send_action(&self, client: &WebSocketClient, action: PrivateAction) -> bool {
        let body = match self.build_body(action) {
            Ok(body) => body,
            Err(error) => {
                log::error!(
                    "Ondo private stream could not build a {} frame: {error}",
                    action.as_str(),
                );
                let redacted = self.redact(&error.to_string());

                self.diagnostics.record(PrivateRecord::SendFailed {
                    action: action.as_str(),
                    reason: redacted.clone(),
                });

                if matches!(
                    action,
                    PrivateAction::ArmSwitch | PrivateAction::ReleaseSwitch
                ) {
                    self.note_switch_failed(redacted);
                }

                // A login this adapter cannot even compose is not a connection worth keeping; a
                // later subscribe or heartbeat that fails to build is not worth dropping one over.
                return action != PrivateAction::Login;
            }
        };

        if let Err(error) = send_body(client, &body).await {
            log::warn!(
                "Ondo private stream failed to send a {} frame: {error}",
                action.as_str(),
            );
            let redacted = self.redact(&error.to_string());

            self.diagnostics.record(PrivateRecord::SendFailed {
                action: action.as_str(),
                reason: redacted.clone(),
            });

            if action == PrivateAction::ArmSwitch {
                // The arm may not have reached the venue, so the account must not trade as if it
                // had.
                self.note_switch_failed(redacted);
            }

            return false;
        }

        // Recorded by its action and never by its body: this is the whole of what any boundary in
        // this crate keeps about a login frame (see `super::diagnostics`).
        self.diagnostics.record_frame_sent(action);

        true
    }

    /// Builds the body of one action.
    fn build_body(&self, action: PrivateAction) -> anyhow::Result<String> {
        match action {
            PrivateAction::Login => {
                let timestamp_ms = now_millis()
                    .map_err(|error| anyhow::anyhow!("the login instant is unreadable: {error}"))?;
                let signature = sign_ws(&self.credential, timestamp_ms);

                LoginRequest::new(
                    self.credential.key_id().to_string(),
                    timestamp_ms,
                    signature,
                )
                .to_json_text()
            }
            PrivateAction::Subscribe(channel) => {
                PrivateSubscriptionRequest::account_wide(WsOp::Subscribe, channel).to_json_text()
            }
            PrivateAction::Unsubscribe(channel) => {
                PrivateSubscriptionRequest::account_wide(WsOp::Unsubscribe, channel).to_json_text()
            }
            PrivateAction::ArmSwitch => self
                .account
                .arm_dead_mans_switch(self.clock())
                .to_json_text(),
            PrivateAction::ReleaseSwitch => self
                .account
                .release_dead_mans_switch(self.clock())
                .to_json_text(),
            PrivateAction::Heartbeat => PingRequest { op: WsOp::Ping }.to_json_text(),
        }
    }

    /// Renews an armed switch, returning `false` when the connection must end.
    ///
    /// **The renewal message is unverified.** The frozen material documents the subscribe frame and
    /// the timeout and says nothing about what renews an armed switch, so this re-sends the
    /// subscribe frame - the only renewal the documentation admits - which
    /// [`crate::reconciliation::DeadMansSwitch::renew_frame`] is the single place to change. A
    /// sandbox session is what settles it (plan §R3.3).
    ///
    /// **Composing the frame is not renewing**, so the deadline is moved only after the bytes were
    /// written, and it is computed from an instant read *before* the write. The venue restarts its
    /// timer at its own receipt, which cannot precede that instant, so the deadline derived from it
    /// can only fall early - never late, which is the direction that would leave this client trading
    /// on a switch the venue had already fired (plan §R3.3).
    async fn renew_switch(&self, client: &WebSocketClient) -> bool {
        let Some(frame) = self.account.dead_mans_switch_renewal_frame() else {
            return true;
        };

        let now = self.clock();

        let body = match frame.to_json_text() {
            Ok(body) => body,
            Err(error) => {
                log::error!("Ondo private stream could not build the switch renewal: {error}");
                // A renewal that could not even be composed is a renewal that did not go out, so it
                // counts against the bound rather than failing the switch outright: the arm still
                // stands and the next tick may yet renew it (plan §R3.3). Nothing moved: composing
                // never touches the deadline.
                self.note_renew_failed(self.redact(&error.to_string()));

                return true;
            }
        };

        if let Err(error) = send_body(client, &body).await {
            log::warn!("Ondo private stream failed to renew the switch: {error}");
            // The deadline is left where the confirmation put it, which is the honest state: this
            // renewal did not reach the venue, so the venue's timer did not restart (plan §R3.3).
            self.note_renew_failed(self.redact(&error.to_string()));

            return false;
        }

        // Written, and nothing more than written: the venue does not acknowledge a renewal, so what
        // the write buys is the deadline and nothing else - no confirmation is claimed, and a switch
        // that already failed or lapsed is left alone (plan §R3.3).
        self.account.note_dead_mans_switch_renewed(now);

        self.diagnostics.record(PrivateRecord::SwitchRenewed {
            renewals: self.account.dead_mans_switch_renewals(),
        });

        true
    }

    /// Starts an account reconciliation pass beside the loop.
    ///
    /// The pass is claimed by the *account*, not by this method: a tick that lands while a pass is
    /// running starts a task that is refused at the claim and reads nothing, which is the single
    /// place the "never two passes" rule lives (plan §R3.1).
    fn spawn_reconciliation(&self, passes: &mut JoinSet<()>) {
        let account = self.account.clone();
        let diagnostics = Arc::clone(&self.diagnostics);
        let wake = self.wake.clone();

        passes.spawn(async move {
            let now = get_atomic_clock_realtime().get_time_ns();

            for report in account.probe_unknown_submissions(now).await {
                log::debug!("Ondo probed an unsettled write: {report:?}");
            }

            match account.reconcile_account(now).await {
                Ok(judgment) => {
                    if !judgment.is_clean() {
                        diagnostics.record(PrivateRecord::ReconciliationFailed {
                            reason: judgment.reasons().join("; "),
                        });
                    }

                    diagnostics.record(PrivateRecord::Reconciled {
                        state: account.reconciliation_state().as_str(),
                    });
                }
                Err(error) => {
                    // A pass that could not claim the account is another pass running, not a
                    // failure: the account states its own reason and nothing is lost.
                    log::debug!("Ondo account reconciliation did not conclude: {error}");
                    diagnostics.record(PrivateRecord::ReconciliationFailed {
                        reason: error.to_string(),
                    });
                }
            }

            // The account moved, so what the client can do may have moved with it.
            let _ = wake.send(());
        });
    }

    /// Starts a metadata refresh beside the loop.
    ///
    /// The refresh is what makes [`MetadataValidity`] a statement about *this session* rather than a
    /// constant: it reads the venue's perps market metadata and records whether that read
    /// succeeded. What it claims is exactly what it read - `GET /v1/markets` answered, with the
    /// documented envelope and at least one perps market in it
    /// ([`crate::http::models::parse_markets`] is the judge) - and not that the engine's instrument
    /// cache holds them, which is the data client's to load.
    ///
    /// It is deliberately **not** the instrument conversion. `parse_instruments` fails closed when a
    /// market's status cannot be classified, which is right when the caller is about to trade that
    /// market and wrong here: a single halted market would read the whole account's metadata as
    /// stale, and markets halt as a matter of course. What this read answers is "can this session
    /// still see the venue's metadata", which is the question the account's admission asks.
    ///
    /// It is not on its own a licence to trade either: a converged account and a confirmed switch
    /// are still required (plan §R3.1).
    fn spawn_metadata_refresh(&self, passes: &mut JoinSet<()>) {
        let account = self.account.clone();
        let diagnostics = Arc::clone(&self.diagnostics);
        let wake = self.wake.clone();

        passes.spawn(async move {
            match account.http_client().get_markets().await {
                Ok(response) => {
                    let markets = response.trading_pairs().len();

                    account.set_metadata(MetadataValidity::Current);
                    log::debug!("Ondo account metadata refreshed: {markets} market(s)");
                }
                Err(error) => {
                    let reason = format!("the market metadata could not be read: {error}");
                    account.set_metadata(MetadataValidity::Stale {
                        reason: reason.clone(),
                    });
                    diagnostics.record(PrivateRecord::ReconciliationFailed { reason });
                }
            }

            // Metadata becoming usable - or stopping being usable - is a change to what the
            // client may do.
            let _ = wake.send(());
        });
    }

    /// Opens one connection.
    ///
    /// The client is handed back behind an [`Arc`] so the run loop can publish the live connection
    /// to [`OndoPrivateStream`] without giving up its own handle to it. Nothing about the connection
    /// changes: [`WebSocketClient::send_text`] takes `&self`, so the loop and a caller writing a
    /// switch frame cannot conflict.
    async fn connect_once(&self) -> Result<(MessageReader, Arc<WebSocketClient>), TransportError> {
        let (reader, client) = WebSocketClient::stream_builder()
            .config(WebSocketConfig {
                url: self.url.clone(),
                headers: Vec::new(),
                heartbeat_interval_secs: None,
                heartbeat_payload: None,
                connect_timeout_ms: None,
                reconnect_delay_initial_ms: None,
                reconnect_delay_max_ms: None,
                reconnect_backoff_factor: None,
                reconnect_jitter_ms: None,
                reconnect_max_attempts: None,
                heartbeat_timeout_secs: None,
                idle_timeout_ms: None,
                backend: TransportBackend::default(),
                proxy_url: None,
            })
            .default_quota(request_quota())
            .connect()
            .await?;

        Ok((reader, Arc::new(client)))
    }

    /// Recomputes the run state from the session and the account.
    fn refresh_run(&self, session: &OndoPrivateSession, detail: &str) {
        let snapshot = match session.phase() {
            PrivateSessionPhase::Disconnected => {
                PrivateRunSnapshot::new(PrivateRunState::Disconnected, detail)
            }
            PrivateSessionPhase::LoggingIn => {
                PrivateRunSnapshot::new(PrivateRunState::Authenticating, detail)
            }
            PrivateSessionPhase::Failed { reason } => {
                PrivateRunSnapshot::new(PrivateRunState::Uncertain, reason.clone())
            }
            PrivateSessionPhase::Subscribing | PrivateSessionPhase::Subscribed => {
                self.account_run_state(session, detail)
            }
        };

        self.set_run(snapshot.state, snapshot.detail);
    }

    /// The account's contribution to the run state.
    fn account_run_state(&self, session: &OndoPrivateSession, detail: &str) -> PrivateRunSnapshot {
        if !session.is_established() {
            return PrivateRunSnapshot::new(
                PrivateRunState::Recovering,
                "the report subscriptions are not all acknowledged",
            );
        }

        match self.account.reconciliation_state() {
            ReconciliationState::Ready => {
                if self.mode == PrivateStreamMode::ReadOnly {
                    PrivateRunSnapshot::new(
                        PrivateRunState::ReadOnlySynced,
                        "the account was read and this session is read-only",
                    )
                } else if self.account.dead_mans_switch_permits_orders() {
                    PrivateRunSnapshot::new(
                        PrivateRunState::TradingReady,
                        "the account was read and the switch is confirmed",
                    )
                } else {
                    // The account reads clean and the switch is the whole reason this session is not
                    // trading ready, so the sentence says which way it is not ready: a switch still
                    // waiting for the venue, one whose own deadline passed, one the venue fired and
                    // one that failed are different things to go and look at, and a single "not
                    // confirmed" would hide the difference between silence and an answer
                    // (plan §R3.3).
                    let detail = match self.account.dead_mans_switch_state() {
                        DeadMansSwitchState::Disarmed => {
                            "the account was read but the switch this session asked for has not \
                             been armed"
                        }
                        DeadMansSwitchState::Arming => {
                            "the account was read but the switch is still waiting for the venue to \
                             acknowledge it"
                        }
                        DeadMansSwitchState::Lapsed { .. } => {
                            "the account was read but this client's switch reached its deadline \
                             without the venue confirming that it fired"
                        }
                        DeadMansSwitchState::Expired => {
                            "the account was read but the venue has fired the switch"
                        }
                        DeadMansSwitchState::Failed { .. } => {
                            "the account was read but the switch has failed"
                        }
                        // Neither can reach this branch - both permit orders, so the arm above was
                        // taken - and they are spelled out rather than folded into a wildcard so
                        // that a state added later has to be given a sentence of its own.
                        DeadMansSwitchState::Armed | DeadMansSwitchState::NotRequired => {
                            "the account was read but the switch is not confirmed"
                        }
                    };

                    PrivateRunSnapshot::new(PrivateRunState::Recovering, detail)
                }
            }
            ReconciliationState::Uncertain => PrivateRunSnapshot::new(
                PrivateRunState::Uncertain,
                "the account could not be accounted for",
            ),
            // A live socket whose account reads Disconnected or Recovering is exactly the case
            // this state exists for: the connection is not the account.
            ReconciliationState::Disconnected | ReconciliationState::Recovering => {
                PrivateRunSnapshot::new(PrivateRunState::Recovering, detail)
            }
        }
    }

    /// Records that a switch operation failed, which stops new orders.
    fn note_switch_failed(&self, reason: String) {
        self.account.note_dead_mans_switch_failed(reason.clone());
        self.diagnostics
            .record(PrivateRecord::SwitchFailed { reason });
    }

    /// Records one renewal that could not be written, letting the switch count the run (plan §R3.3).
    ///
    /// It is deliberately **not** [`Self::note_switch_failed`]: a single unsendable renewal is not
    /// the switch failing, it is one failure of a bounded run, and it is the switch that decides
    /// when the run has gone on long enough to stop trusting itself. The record is written here so
    /// the transport's own diagnostics show every one of them.
    fn note_renew_failed(&self, reason: String) {
        self.account
            .note_dead_mans_switch_renew_failed(reason.clone());
        self.diagnostics
            .record(PrivateRecord::SwitchRenewFailed { reason });
    }

    /// Records that the connection ended.
    fn note_disconnected(&self, reason: &str) {
        self.diagnostics.record(PrivateRecord::Disconnected {
            reason: self.redact(reason),
        });
    }

    /// Applies the credential's redaction to a string that may reach a record or a log.
    fn redact(&self, text: &str) -> String {
        self.credential.redact(text)
    }

    /// Returns the local wall clock.
    fn clock(&self) -> UnixNanos {
        get_atomic_clock_realtime().get_time_ns()
    }

    /// Forces the run state.
    fn set_run(&self, state: PrivateRunState, detail: impl Into<String>) {
        *self.run.lock() = PrivateRunSnapshot::new(state, detail);
    }
}

/// What one read from the socket means for the connection.
#[derive(Debug)]
enum Inbound {
    /// A text frame to hand to the session.
    Text(String),
    /// A frame that carries nothing for this protocol, and a connection that stays up.
    Ignore,
    /// The connection is over.
    End,
}

/// Classifies one read from the socket.
fn classify_frame(message: Option<Result<Message, TransportError>>) -> Inbound {
    match message {
        Some(Ok(Message::Text(bytes))) => match std::str::from_utf8(&bytes) {
            Ok(text) => Inbound::Text(text.to_string()),
            Err(_) => {
                // The transport contract says text is UTF-8; a violation is reported with its byte
                // length rather than lossily converted, and the connection stays up.
                log::error!(
                    "Ondo private WebSocket sent a text frame with {} bytes of invalid UTF-8",
                    bytes.len(),
                );

                Inbound::Ignore
            }
        },
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Inbound::Ignore,
        Some(Ok(Message::Binary(bytes))) => {
            log::warn!(
                "Ondo private WebSocket sent a binary frame of {} bytes, which this protocol does \
                 not use",
                bytes.len(),
            );

            Inbound::Ignore
        }
        Some(Ok(Message::Close(frame))) => {
            log::debug!("Ondo private WebSocket closed by the venue: {frame:?}");

            Inbound::End
        }
        Some(Err(error)) => {
            log::warn!("Ondo private WebSocket read failed: {error}");

            Inbound::End
        }
        None => {
            log::debug!("Ondo private WebSocket stream ended");

            Inbound::End
        }
    }
}

/// Sends one request body, refusing to exceed the venue's client message cap.
///
/// There is no recorder parameter and no recorder call: the private transport holds no public
/// recorder, and the only record of a frame it keeps is its action
/// ([`super::diagnostics`]).
async fn send_body(client: &WebSocketClient, body: &str) -> Result<(), TransportError> {
    if body.len() > ONDO_WS_MAX_CLIENT_MESSAGE_BYTES {
        return Err(TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "request of {} bytes exceeds the {} byte client message cap",
                body.len(),
                ONDO_WS_MAX_CLIENT_MESSAGE_BYTES,
            ),
        )));
    }

    client
        .send_text(body.to_string(), None)
        .await
        .map_err(|error| {
            log::debug!("Ondo private WebSocket send failed: {error}");
            TransportError::Io(std::io::Error::other(error.to_string()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Only the external teardown is blocked; the production stream owns the real Tokio task.
    async fn blocked_transport() -> OndoPrivateStream {
        let liveness = Arc::new(());
        let weak = Arc::downgrade(&liveness);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _liveness = liveness;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.expect("the teardown task started");

        OndoPrivateStream {
            url: "ws://127.0.0.1:1".to_string(),
            run: Arc::new(Mutex::new(PrivateRunSnapshot::new(
                PrivateRunState::Stopping,
                "test",
            ))),
            diagnostics: Arc::new(PrivateDiagnostics::new()),
            active: Arc::new(Mutex::new(None)),
            cancellation: CancellationToken::new(),
            task: Some(task),
            liveness: weak,
        }
    }

    #[tokio::test]
    async fn test_shutdown_ownership_survives_canceled_transport_stop() {
        let mut stream = blocked_transport().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.stop())
                .await
                .is_err()
        );

        let alive = stream.liveness.clone();
        drop(stream);
        tokio::time::timeout(Duration::from_millis(500), async {
            while alive.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the retained owner aborts the original transport");
    }

    #[tokio::test]
    async fn test_shutdown_ownership_aborts_and_joins_after_transport_timeout() {
        let mut stream = blocked_transport().await;
        stream.stop().await;
        assert!(
            !stream.is_running(),
            "a timed-out teardown must be aborted and joined"
        );
        stream.stop().await;
        assert!(
            !stream.is_running(),
            "repeating stop must preserve the completed result"
        );
    }
}
