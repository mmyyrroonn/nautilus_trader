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

//! Bounded, opt-in execution timing. No prices, signatures, credentials, or payloads are captured.

use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    time::Instant,
};

use nautilus_core::UUID4;
use nautilus_model::identifiers::ClientOrderId;
use serde::Serialize;

const MAX_QUEUE: usize = 4096;
const MAX_RECORDS: u64 = 100_000;

/// Observable boundaries; enqueue boundaries never imply a socket write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingStage {
    NativeReceived,
    TaskQueued,
    TaskStarted,
    SubmittedEventStart,
    SubmittedEventEnd,
    PostStarted,
    LimiterStart,
    LimiterEnd,
    SignStart,
    SignEnd,
    SignError,
    InflightStart,
    InflightEnd,
    CommandLockStart,
    CommandEnqueue,
    HandlerReceived,
    SerializeStart,
    SerializeEnd,
    TransportAttempt,
    WriterEnqueued,
    TransportError,
    ResponseDequeued,
    EnvelopeParsed,
    CallerResumed,
    ResponseParseStart,
    ResponseParseEnd,
    ResponseParseError,
    PostSuccess,
    PostRejected,
    PostError,
    Timeout,
    Reconnected,
    RejectionEventStart,
    RejectionEventEnd,
    OrderDispatchStart,
    OrderDispatchEnd,
    FillDispatchStart,
    FillDispatchEnd,
    SubmissionFinished,
}

/// One local monotonic observation. Only equal `session_id` values share a clock origin.
#[derive(Debug, Clone, Serialize)]
pub struct TimingRecord {
    pub session_id: UUID4,
    pub trace_id: u64,
    pub post_id: Option<u64>,
    pub client_order_id: Option<ClientOrderId>,
    pub stage: TimingStage,
    pub elapsed_ns: u64,
    /// Number of observations lost before this record was captured.
    pub dropped: u64,
}

/// A nonblocking bounded collector. A full queue drops diagnostics, never execution work.
#[derive(Debug, Clone, Default)]
pub struct ExecutionTiming(Option<Arc<TimingSession>>);

impl ExecutionTiming {
    /// Creates a collector and receiver, clamping capacity to 1..=4096 records.
    ///
    /// Each session attempts at most 100,000 records. Drain outside execution tasks.
    #[must_use]
    pub fn bounded(capacity: usize) -> (Self, Receiver<TimingRecord>) {
        let (tx, rx) = sync_channel(capacity.clamp(1, MAX_QUEUE));
        (
            Self(Some(Arc::new(TimingSession {
                session_id: UUID4::new(),
                origin: Instant::now(),
                next_trace: AtomicU64::new(1),
                attempted: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                tx,
            }))),
            rx,
        )
    }

    /// Returns the number of observations lost to overflow, exhaustion, or a closed receiver.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.0
            .as_ref()
            .map_or(0, |s| s.dropped.load(Ordering::Relaxed))
    }

    pub(crate) fn from_env() -> Self {
        let Some(directory) = std::env::var_os("NAUTILUS_HYPERLIQUID_TIMING_DIR") else {
            return Self::default();
        };
        let (timing, rx) = Self::bounded(MAX_QUEUE);
        let session = timing.0.as_ref().expect("collector initialized");
        let path = std::path::PathBuf::from(directory)
            .join(format!("hyperliquid-timing-{}.jsonl", session.session_id));
        let file = match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(file) => file,
            Err(e) => {
                log::warn!("Cannot open Hyperliquid timing capture: {e}");
                return Self::default();
            }
        };

        // File I/O and serialization stay on a dedicated thread, never on execution tasks
        if let Err(e) = std::thread::Builder::new()
            .name("hyperliquid-timing".to_owned())
            .spawn(move || {
                let mut writer = BufWriter::new(file);

                for record in rx {
                    if serde_json::to_writer(&mut writer, &record).is_err()
                        || writer.write_all(b"\n").is_err()
                        || writer.flush().is_err()
                    {
                        log::warn!("Hyperliquid timing capture stopped after a write error");
                        break;
                    }
                }
            })
        {
            log::warn!("Cannot start Hyperliquid timing writer: {e}");
            return Self::default();
        }
        timing
    }

    pub(crate) fn trace(&self, client_order_id: Option<ClientOrderId>) -> TimingTrace {
        TimingTrace {
            timing: self.clone(),
            trace_id: self
                .0
                .as_ref()
                .map_or(0, |s| s.next_trace.fetch_add(1, Ordering::Relaxed)),
            post_id: None,
            client_order_id,
        }
    }

    pub(crate) fn order_event(&self, client_order_id: Option<ClientOrderId>, stage: TimingStage) {
        TimingTrace {
            timing: self.clone(),
            trace_id: 0,
            post_id: None,
            client_order_id,
        }
        .mark(stage);
    }
}

#[derive(Debug)]
struct TimingSession {
    session_id: UUID4,
    origin: Instant,
    next_trace: AtomicU64,
    attempted: AtomicU64,
    dropped: AtomicU64,
    tx: SyncSender<TimingRecord>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TimingTrace {
    timing: ExecutionTiming,
    trace_id: u64,
    post_id: Option<u64>,
    client_order_id: Option<ClientOrderId>,
}

impl TimingTrace {
    pub(crate) fn with_post_id(mut self, post_id: u64) -> Self {
        self.post_id = Some(post_id);
        self
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.timing.0.is_some()
    }

    pub(crate) fn now(&self) -> Option<Instant> {
        self.timing.0.as_ref().map(|_| Instant::now())
    }

    pub(crate) fn mark(&self, stage: TimingStage) {
        self.mark_at(stage, self.now());
    }

    pub(crate) fn mark_at(&self, stage: TimingStage, instant: Option<Instant>) {
        let (Some(session), Some(instant)) = (&self.timing.0, instant) else {
            return;
        };

        if session.attempted.fetch_add(1, Ordering::Relaxed) >= MAX_RECORDS {
            session.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let elapsed_ns = instant.saturating_duration_since(session.origin).as_nanos();
        let record = TimingRecord {
            session_id: session.session_id,
            trace_id: self.trace_id,
            post_id: self.post_id,
            client_order_id: self.client_order_id,
            stage,
            elapsed_ns: u64::try_from(elapsed_ns).unwrap_or(u64::MAX),
            dropped: session.dropped.load(Ordering::Relaxed),
        };

        if session.tx.try_send(record).is_err() {
            session.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn overflow_is_bounded_and_reports_gaps_without_blocking() {
        let (timing, rx) = ExecutionTiming::bounded(1);
        let trace = timing.trace(Some(ClientOrderId::from("TIMING-BOUND")));
        trace.mark(TimingStage::NativeReceived);
        trace.mark(TimingStage::TaskQueued);
        assert_eq!(timing.dropped(), 1);
        assert_eq!(rx.try_recv().unwrap().stage, TimingStage::NativeReceived);
        trace.mark(TimingStage::TaskStarted);
        assert_eq!(rx.try_recv().unwrap().dropped, 1);
        drop(rx);
        trace.mark(TimingStage::SignStart);
        assert_eq!(timing.dropped(), 2);
    }

    #[rstest]
    fn capture_stops_at_session_record_limit() {
        let (timing, rx) = ExecutionTiming::bounded(1);
        let trace = timing.trace(None);
        for _ in 0..MAX_RECORDS {
            trace.mark(TimingStage::PostStarted);
            rx.try_recv().unwrap();
        }
        trace.mark(TimingStage::PostStarted);
        assert!(rx.try_recv().is_err());
        assert_eq!(timing.dropped(), 1);
    }

    #[rstest]
    fn elapsed_values_preserve_capture_time_and_session_identity() {
        let (timing, rx) = ExecutionTiming::bounded(16);
        let trace = timing.trace(None).with_post_id(42);
        let start = trace.now().unwrap();
        trace.mark_at(TimingStage::SignStart, Some(start));
        trace.mark_at(
            TimingStage::SignEnd,
            Some(start + Duration::from_millis(27)),
        );
        let records: Vec<_> = rx.try_iter().collect();
        assert_eq!(records[1].elapsed_ns - records[0].elapsed_ns, 27_000_000);
        assert_eq!(records[0].session_id, records[1].session_id);
        assert_eq!(records[0].trace_id, records[1].trace_id);
        assert_eq!(records[1].post_id, Some(42));
        let (other, other_rx) = ExecutionTiming::bounded(1);
        other.trace(None).mark(TimingStage::PostStarted);
        assert_ne!(
            records[0].session_id,
            other_rx.try_recv().unwrap().session_id
        );
    }

    #[rstest]
    fn disabled_trace_does_not_read_clock_or_allocate_session() {
        let timing = ExecutionTiming::default();
        let trace = timing.trace(None);
        assert!(!trace.is_enabled());
        assert!(trace.now().is_none());
        trace.mark(TimingStage::SignStart);
        assert_eq!(timing.dropped(), 0);
    }
}
