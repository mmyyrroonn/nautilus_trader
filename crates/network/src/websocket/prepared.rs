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

//! Cancellable, single-use writes admitted at the actual transport handoff.
//!
//! Admission runs synchronously after backend sink readiness. It must not await or re-enter
//! network lifecycle methods. Lock ordering is lifecycle, adapter proof, then write control.

use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::{Sink, Stream, StreamExt, stream::SplitSink};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::types::MessageReader;
use crate::{
    dst::time::Instant,
    error::SendError,
    mode::ConnectionMode,
    transport::{BoxedWsTransport, Message, TransportError},
};

/// Transport-level outcome; neither variant confirms venue acceptance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedWriteOutcome {
    /// The actual transport's `start_send` was never called.
    NotWritten { reason: String },
    /// The actual transport's `start_send` was called, even if it returned an error.
    /// `None` means flushing completed, not that the venue received or accepted the message.
    MayHaveWritten { error: Option<String> },
}

#[derive(Default)]
struct WriteState {
    claimed: bool,
    outcome: Option<PreparedWriteOutcome>,
}

/// Shared cancellation and result state for exactly one prepared write.
///
/// Keep a clone when spawning the send future. Cancelling or dropping that future cannot
/// turn a queued command into a later write. `cancel` and transport handoff use the same lock.
#[derive(Clone)]
pub struct PreparedWriteControl {
    state: Arc<Mutex<WriteState>>,
    cancelled: CancellationToken,
    deadline: Instant,
}

impl Debug for PreparedWriteControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWriteControl")
            .field("deadline", &self.deadline)
            .field("outcome", &self.outcome())
            .finish_non_exhaustive()
    }
}

impl PreparedWriteControl {
    /// Creates a single-use control with an absolute monotonic deadline.
    #[must_use]
    pub fn new(deadline: Instant) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriteState::default())),
            cancelled: CancellationToken::new(),
            deadline,
        }
    }

    /// Cancels pending admission and returns the current write classification.
    #[must_use]
    pub fn cancel(&self) -> PreparedWriteOutcome {
        self.reject("prepared write cancelled")
    }

    /// Returns the observed outcome, or `None` while waiting for admission.
    #[must_use]
    pub fn outcome(&self) -> Option<PreparedWriteOutcome> {
        self.state.lock().outcome.clone()
    }

    pub(crate) fn claim(&self) -> bool {
        let mut state = self.state.lock();
        if state.claimed {
            return false;
        }
        state.claimed = true;
        true
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }

    pub(crate) fn reject(&self, reason: impl Into<String>) -> PreparedWriteOutcome {
        let outcome = {
            let mut state = self.state.lock();
            state
                .outcome
                .get_or_insert_with(|| PreparedWriteOutcome::NotWritten {
                    reason: reason.into(),
                })
                .clone()
        };
        self.cancelled.cancel();
        outcome
    }

    pub(crate) fn finish(&self, error: Option<String>) -> PreparedWriteOutcome {
        let mut state = self.state.lock();
        if matches!(
            state.outcome,
            Some(PreparedWriteOutcome::MayHaveWritten { .. })
        ) {
            state.outcome = Some(PreparedWriteOutcome::MayHaveWritten { error });
        }
        state
            .outcome
            .clone()
            .unwrap_or_else(|| PreparedWriteOutcome::NotWritten {
                reason: "transport did not start the write".into(),
            })
    }
}

/// One-use synchronous continuation, to be consumed while the adapter proof lock is held.
pub struct PreparedWriteContinuation<'a> {
    start: Box<dyn FnOnce() -> Result<(), SendError> + 'a>,
}

impl Debug for PreparedWriteContinuation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWriteContinuation")
            .finish_non_exhaustive()
    }
}

impl PreparedWriteContinuation<'_> {
    /// Performs one actual sink handoff without an await or a retry.
    ///
    /// Any error after the underlying `start_send` is invoked is `MayHaveWritten`.
    /// Cancellation/deadline checks can reject this continuation without invoking the sink.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Timeout`] when cancelled or expired before handoff, or
    /// [`SendError::BrokenPipe`] when the actual transport returns an error during handoff.
    pub fn start_send(self) -> Result<(), SendError> {
        (self.start)()
    }
}

/// Synchronous admission callback. Returning without consuming the continuation denies the write.
///
/// Acquire the adapter proof lock, recheck immutable intent, reserve/mark write-start, and consume
/// the continuation while holding that lock. Do not call network lifecycle methods in this callback.
pub type PreparedWriteAdmission =
    Box<dyn for<'a> FnOnce(PreparedWriteContinuation<'a>) -> Result<(), String> + Send>;

pub(crate) struct PreparedCommand {
    pub(crate) control: PreparedWriteControl,
    pub(crate) admission: PreparedWriteAdmission,
    pub(crate) expected_epoch: u64,
    pub(crate) message: Message,
    pub(crate) epoch: Arc<AtomicU64>,
    pub(crate) mode: Arc<AtomicU8>,
    pub(crate) lifecycle: Arc<Mutex<()>>,
}

/// Sink half of a WebSocket transport supporting both ordinary and prepared writes.
///
/// A `SplitSink` alone admits into its local slot before backend readiness. This wrapper retains
/// prepared admission until the actual transport handoff. Use [`Self::split`] when constructing
/// a client with an existing backend transport; normal client builders create it automatically.
pub struct MessageWriter {
    inner: SplitSink<BoxedWsTransport, Message>,
    pending: Arc<Mutex<Option<PreparedCommand>>>,
    slot_pending: Arc<AtomicBool>,
}

impl Debug for MessageWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageWriter").finish_non_exhaustive()
    }
}

impl MessageWriter {
    /// Splits an existing transport while retaining prepared admission at actual backend handoff.
    pub fn split(transport: BoxedWsTransport) -> (Self, MessageReader) {
        split_transport(transport)
    }

    pub(crate) fn prepare(&self, command: PreparedCommand) -> Result<(), String> {
        let mut pending = self.pending.lock();
        if pending.is_some() || self.slot_pending.load(Ordering::Acquire) {
            return Err("an earlier prepared transport slot is still pending".into());
        }
        *pending = Some(command);
        Ok(())
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending.lock().is_some()
    }
}

impl Sink<Message> for MessageWriter {
    type Error = TransportError;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.slot_pending.store(true, Ordering::Release);
        Pin::new(&mut self.inner).start_send(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

struct PreparedTransport {
    inner: BoxedWsTransport,
    pending: Arc<Mutex<Option<PreparedCommand>>>,
    slot_pending: Arc<AtomicBool>,
}

impl Stream for PreparedTransport {
    type Item = Result<Message, TransportError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn rejected_transport(reason: impl Into<String>) -> TransportError {
    TransportError::Io(std::io::Error::other(reason.into()))
}

impl Sink<Message> for PreparedTransport {
    type Error = TransportError;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.slot_pending.store(false, Ordering::Release);
        let command = self.pending.lock().take();
        let Some(command) = command else {
            return self.inner.as_mut().start_send(item);
        };
        let _lifecycle = command.lifecycle.lock();
        let control = command.control;
        if item != command.message {
            control.reject("prepared payload does not own this transport slot");
            return Err(rejected_transport("prepared transport payload mismatch"));
        }
        if !ConnectionMode::from_atomic(&command.mode).is_active()
            || command.epoch.load(Ordering::Acquire) != command.expected_epoch
        {
            control.reject("connection changed before transport handoff");
            return Err(rejected_transport("prepared connection changed"));
        }
        if control.outcome().is_some() || Instant::now() >= control.deadline {
            control.reject("prepared deadline expired before transport handoff");
            return Err(rejected_transport("prepared write cancelled or expired"));
        }
        let mut handoff_error = None;
        let continuation = PreparedWriteContinuation {
            start: Box::new(|| {
                let mut state = control.state.lock();
                if state.outcome.is_some() || Instant::now() >= control.deadline {
                    state
                        .outcome
                        .get_or_insert_with(|| PreparedWriteOutcome::NotWritten {
                            reason: "cancelled or expired at transport handoff".into(),
                        });
                    return Err(SendError::Timeout);
                }
                // Set before invoking the sink: even an immediate sink error can follow partial IO.
                state.outcome = Some(PreparedWriteOutcome::MayHaveWritten {
                    error: Some("flush pending".into()),
                });
                self.inner.as_mut().start_send(item).map_err(|error| {
                    let reason = error.to_string();
                    handoff_error = Some(reason.clone());
                    SendError::BrokenPipe(reason)
                })
            }),
        };
        let admission = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (command.admission)(continuation)
        }))
        .unwrap_or_else(|_| Err("prepared admission panicked".into()));
        if let Some(reason) = handoff_error {
            return Err(rejected_transport(reason));
        }
        match admission {
            Ok(())
                if matches!(
                    control.outcome(),
                    Some(PreparedWriteOutcome::MayHaveWritten { .. })
                ) =>
            {
                Ok(())
            }
            Ok(()) => {
                control.reject("admission did not consume continuation");
                Err(rejected_transport("prepared admission denied"))
            }
            Err(reason) => {
                control.reject(reason.clone());
                Err(rejected_transport(reason))
            }
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().poll_close(cx)
    }
}

pub(crate) fn split_transport(transport: BoxedWsTransport) -> (MessageWriter, MessageReader) {
    let pending = Arc::new(Mutex::new(None));
    let slot_pending = Arc::new(AtomicBool::new(false));
    let wrapped: BoxedWsTransport = Box::pin(PreparedTransport {
        inner: transport,
        pending: Arc::clone(&pending),
        slot_pending: Arc::clone(&slot_pending),
    });
    let (inner, reader) = wrapped.split();
    (
        MessageWriter {
            inner,
            pending,
            slot_pending,
        },
        reader,
    )
}

/// Cancels pending commands if a caller aborts its future, including while quota is pending.
pub(crate) struct PreparedSendGuard(pub(crate) PreparedWriteControl);
impl Drop for PreparedSendGuard {
    fn drop(&mut self) {
        _ = self.0.cancel();
    }
}

#[cfg(all(test, not(all(feature = "simulation", madsim))))]
#[path = "prepared_tests.rs"]
mod tests;
