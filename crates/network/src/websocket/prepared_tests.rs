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

//! Tests exercise the backend sink after the `SplitSink` local slot.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Sink, SinkExt, Stream, task::AtomicWaker};
use parking_lot::Mutex;

use super::*;

#[derive(Default)]
struct BackendState {
    ready: AtomicBool,
    flush: AtomicBool,
    ready_entered: AtomicBool,
    flush_entered: AtomicBool,
    fail_ready: AtomicBool,
    fail_start: AtomicBool,
    fail_flush: AtomicBool,
    ready_waker: AtomicWaker,
    flush_waker: AtomicWaker,
    messages: Mutex<Vec<Message>>,
}
impl BackendState {
    fn release_ready(&self) {
        self.ready.store(true, Ordering::Release);
        self.ready_waker.wake();
    }
    fn release_flush(&self) {
        self.flush.store(true, Ordering::Release);
        self.flush_waker.wake();
    }
}
struct Backend(Arc<BackendState>);
impl Stream for Backend {
    type Item = Result<Message, TransportError>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
impl Sink<Message> for Backend {
    type Error = TransportError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.ready_waker.register(cx.waker());
        self.0.ready_entered.store(true, Ordering::Release);
        if self.0.fail_ready.load(Ordering::Acquire) {
            return Poll::Ready(Err(rejected_transport("readiness failed")));
        }
        if self.0.ready.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.0.messages.lock().push(item);
        if self.0.fail_start.load(Ordering::Acquire) {
            Err(rejected_transport("handoff failed"))
        } else {
            Ok(())
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.flush_waker.register(cx.waker());
        self.0.flush_entered.store(true, Ordering::Release);
        if self.0.fail_flush.load(Ordering::Acquire) {
            return Poll::Ready(Err(rejected_transport("flush failed")));
        }
        if self.0.flush.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

fn writer(state: &Arc<BackendState>) -> MessageWriter {
    let transport: BoxedWsTransport = Box::pin(Backend(Arc::clone(state)));
    split_transport(transport).0
}
fn command(control: &PreparedWriteControl, calls: Arc<AtomicUsize>) -> PreparedCommand {
    PreparedCommand {
        control: control.clone(),
        admission: Box::new(move |continuation| {
            calls.fetch_add(1, Ordering::SeqCst);
            continuation.start_send().map_err(|error| error.to_string())
        }),
        expected_epoch: 0,
        message: Message::text("owned"),
        epoch: Arc::new(AtomicU64::new(0)),
        mode: Arc::new(AtomicU8::new(ConnectionMode::Active.as_u8())),
        lifecycle: Arc::new(Mutex::new(())),
    }
}
async fn wait_flag(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !flag.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend poll was not reached");
}

#[tokio::test]
async fn prepared_admission_waits_for_real_backend_readiness() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::clone(&calls)))
        .unwrap();
    let task = tokio::spawn(async move { writer.send(Message::text("owned")).await });
    wait_flag(&backend.ready_entered).await;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(backend.messages.lock().is_empty());
    assert!(control.outcome().is_none());
    backend.release_ready();
    task.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(*backend.messages.lock(), vec![Message::text("owned")]);
    assert_eq!(
        control.finish(None),
        PreparedWriteOutcome::MayHaveWritten { error: None }
    );
}

#[tokio::test]
async fn cancellation_during_backend_backpressure_never_hands_off() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::clone(&calls)))
        .unwrap();
    let task = tokio::spawn(async move { writer.send(Message::text("owned")).await });
    wait_flag(&backend.ready_entered).await;
    assert!(matches!(
        control.cancel(),
        PreparedWriteOutcome::NotWritten { .. }
    ));
    backend.release_ready();
    assert!(task.await.unwrap().is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(backend.messages.lock().is_empty());
}

#[tokio::test]
async fn connection_epoch_change_during_backpressure_never_hands_off() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut writer = writer(&backend);
    let prepared = command(&control, Arc::clone(&calls));
    let epoch = Arc::clone(&prepared.epoch);
    let lifecycle = Arc::clone(&prepared.lifecycle);
    writer.prepare(prepared).unwrap();
    let task = tokio::spawn(async move { writer.send(Message::text("owned")).await });
    wait_flag(&backend.ready_entered).await;
    {
        let _admission = lifecycle.lock();
        epoch.store(1, Ordering::Release);
    }
    backend.release_ready();
    assert!(task.await.unwrap().is_err());
    assert!(matches!(
        control.outcome(),
        Some(PreparedWriteOutcome::NotWritten { .. })
    ));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(backend.messages.lock().is_empty());
}

#[tokio::test(start_paused = true)]
async fn absolute_deadline_during_backpressure_never_hands_off() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_millis(50));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::clone(&calls)))
        .unwrap();
    let task = tokio::spawn(async move { writer.send(Message::text("owned")).await });
    wait_flag(&backend.ready_entered).await;
    tokio::time::advance(Duration::from_millis(50)).await;
    backend.release_ready();
    assert!(task.await.unwrap().is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(backend.messages.lock().is_empty());
}

#[tokio::test]
async fn cancellation_inside_admission_after_slot_take_never_hands_off() {
    let backend = Arc::new(BackendState::default());
    backend.release_ready();
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let cancel = control.clone();
    let mut prepared = command(&control, Arc::new(AtomicUsize::new(0)));
    prepared.admission = Box::new(move |continuation| {
        assert!(matches!(
            cancel.cancel(),
            PreparedWriteOutcome::NotWritten { .. }
        ));
        assert!(continuation.start_send().is_err());
        Ok(())
    });
    let mut writer = writer(&backend);
    writer.prepare(prepared).unwrap();
    assert!(writer.send(Message::text("owned")).await.is_err());
    assert!(backend.messages.lock().is_empty());
}

#[tokio::test]
async fn sink_start_error_is_possible_write_even_when_callback_ignores_error() {
    let backend = Arc::new(BackendState::default());
    backend.release_ready();
    backend.release_flush();
    backend.fail_start.store(true, Ordering::Release);
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let mut prepared = command(&control, Arc::new(AtomicUsize::new(0)));
    prepared.admission = Box::new(|continuation| {
        _ = continuation.start_send();
        Ok(())
    });
    let mut writer = writer(&backend);
    writer.prepare(prepared).unwrap();
    assert!(writer.send(Message::text("owned")).await.is_err());
    assert!(matches!(
        control.cancel(),
        PreparedWriteOutcome::MayHaveWritten { .. }
    ));
    assert_eq!(backend.messages.lock().len(), 1);
}

#[tokio::test]
async fn flush_backpressure_cannot_be_reclassified_as_not_written() {
    let backend = Arc::new(BackendState::default());
    backend.release_ready();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::new(AtomicUsize::new(0))))
        .unwrap();
    let task = tokio::spawn(async move { writer.send(Message::text("owned")).await });
    wait_flag(&backend.flush_entered).await;
    assert_eq!(backend.messages.lock().len(), 1);
    assert!(matches!(
        control.cancel(),
        PreparedWriteOutcome::MayHaveWritten { .. }
    ));
    backend.release_flush();
    task.await.unwrap().unwrap();
    assert_eq!(backend.messages.lock().len(), 1);
}

#[tokio::test]
async fn callback_panics_before_and_after_handoff_preserve_classification() {
    for started in [false, true] {
        let backend = Arc::new(BackendState::default());
        backend.release_ready();
        backend.release_flush();
        let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
        let mut prepared = command(&control, Arc::new(AtomicUsize::new(0)));
        prepared.admission = Box::new(move |continuation| {
            if started {
                continuation.start_send().unwrap();
            }
            panic!("admission failure");
        });
        let mut writer = writer(&backend);
        writer.prepare(prepared).unwrap();
        assert!(writer.send(Message::text("owned")).await.is_err());
        assert_eq!(backend.messages.lock().len(), usize::from(started));
        assert_eq!(
            matches!(
                control.cancel(),
                PreparedWriteOutcome::MayHaveWritten { .. }
            ),
            started
        );
    }
}

#[tokio::test]
async fn refusing_or_not_consuming_admission_does_not_write() {
    for denied in [false, true] {
        let backend = Arc::new(BackendState::default());
        backend.release_ready();
        backend.release_flush();
        let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
        let mut prepared = command(&control, Arc::new(AtomicUsize::new(0)));
        prepared.admission = Box::new(move |_| {
            if denied {
                Err("proof revoked".into())
            } else {
                Ok(())
            }
        });
        let mut writer = writer(&backend);
        writer.prepare(prepared).unwrap();
        assert!(writer.send(Message::text("owned")).await.is_err());
        assert!(matches!(
            control.outcome(),
            Some(PreparedWriteOutcome::NotWritten { .. })
        ));
        assert!(backend.messages.lock().is_empty());
    }
}

#[tokio::test]
async fn wrong_payload_does_not_consume_another_intents_admission() {
    let backend = Arc::new(BackendState::default());
    backend.release_ready();
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::clone(&calls)))
        .unwrap();
    assert!(writer.send(Message::text("different")).await.is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(backend.messages.lock().is_empty());
    assert!(matches!(
        control.outcome(),
        Some(PreparedWriteOutcome::NotWritten { .. })
    ));
}

#[tokio::test]
async fn pending_ordinary_slot_cannot_be_relabelled_as_a_prepared_intent() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let mut writer = writer(&backend);
    // A cancelled ordinary send retains a SplitSink slot, even with the same payload bytes.
    let mut send = Box::pin(writer.send(Message::text("owned")));
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    drop(send);
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    assert!(
        writer
            .prepare(command(&control, Arc::new(AtomicUsize::new(0))))
            .is_err()
    );
    backend.release_ready();
    writer.flush().await.unwrap();
    assert_eq!(backend.messages.lock().len(), 1);
    assert!(control.outcome().is_none());
}

#[tokio::test]
async fn cancelled_prepared_slot_stays_cancelled_during_transport_close() {
    let backend = Arc::new(BackendState::default());
    backend.release_flush();
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::new(AtomicUsize::new(0))))
        .unwrap();
    let mut send = Box::pin(writer.send(Message::text("owned")));
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    drop(send);
    assert!(matches!(
        control.cancel(),
        PreparedWriteOutcome::NotWritten { .. }
    ));
    backend.release_ready();
    assert!(writer.close().await.is_err());
    assert!(backend.messages.lock().is_empty());
}

#[tokio::test]
async fn flush_error_after_handoff_preserves_possible_write() {
    let backend = Arc::new(BackendState::default());
    backend.release_ready();
    backend.fail_flush.store(true, Ordering::Release);
    let control = PreparedWriteControl::new(Instant::now() + Duration::from_secs(2));
    let mut writer = writer(&backend);
    writer
        .prepare(command(&control, Arc::new(AtomicUsize::new(0))))
        .unwrap();
    assert!(writer.send(Message::text("owned")).await.is_err());
    assert!(matches!(
        control.cancel(),
        PreparedWriteOutcome::MayHaveWritten { .. }
    ));
    assert_eq!(backend.messages.lock().len(), 1);
}
