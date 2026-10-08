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

//! Prepared API tests use the real writer task and bounded local transport peers.

use std::{
    num::NonZeroU32,
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};

use futures_util::{Sink, Stream, task::AtomicWaker};

use super::*;

#[derive(Default)]
struct Backend {
    ready: AtomicBool,
    flush: AtomicBool,
    polled: AtomicBool,
    flushed: AtomicBool,
    ready_waker: AtomicWaker,
    flush_waker: AtomicWaker,
    frames: Mutex<Vec<Message>>,
}
struct GatedTransport(Arc<Backend>);
impl Stream for GatedTransport {
    type Item = Result<Message, TransportError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
impl Sink<Message> for GatedTransport {
    type Error = TransportError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.ready_waker.register(cx.waker());
        self.0.polled.store(true, Ordering::Release);
        if self.0.ready.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        self.0.frames.lock().push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.flush_waker.register(cx.waker());
        self.0.flushed.store(true, Ordering::Release);
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
impl Backend {
    fn release(&self) {
        self.ready.store(true, Ordering::Release);
        self.flush.store(true, Ordering::Release);
        self.ready_waker.wake();
        self.flush_waker.wake();
    }
}

struct Harness {
    client: Arc<WebSocketClient>,
    inner: WebSocketClientInner,
}
impl Harness {
    fn new(backend: &Arc<Backend>, quota: Option<Quota>) -> Self {
        let transport: BoxedWsTransport = Box::pin(GatedTransport(Arc::clone(backend)));
        let (writer, _reader) = split_transport(transport);
        let config = WebSocketConfig::builder()
            .url("ws://127.0.0.1:1".to_string())
            .build()
            .unwrap();
        let inner =
            WebSocketClientInner::new_with_writer_and_state_sink(config, writer, None).unwrap();
        Self::from_inner(inner, quota)
    }

    fn from_inner(inner: WebSocketClientInner, quota: Option<Quota>) -> Self {
        let lifecycle = Arc::new(ControllerLifecycle::new());
        // The normal writer is used; lifecycle transitions are driven explicitly by these unit
        // tests so a real backend readiness stall remains deterministic on every platform.
        let controller_task = tokio::spawn(std::future::pending());
        lifecycle.set_abort_handle(controller_task.abort_handle());
        let client = Arc::new(WebSocketClient {
            controller_task,
            connection_mode: Arc::clone(&inner.connection_mode),
            connection_epoch: Arc::clone(&inner.connection_epoch),
            write_admission: Arc::clone(&inner.write_admission),
            state_notify: Arc::clone(&inner.state_notify),
            connect_timeout: Duration::from_secs(1),
            rate_limiter: Arc::new(RateLimiter::new_with_quota(quota, Vec::new())),
            writer_tx: inner.writer_tx.clone(),
            auth_tracker: Arc::clone(&inner.auth_tracker),
            reconnect_buffer_waits_for_auth: Arc::clone(&inner.reconnect_buffer_waits_for_auth),
            reconnect_headers: inner.reconnect_headers.clone(),
            state_sink: None,
            controller_lifecycle: lifecycle,
            controller_notify: Arc::clone(&inner.controller_notify),
            reconnect_published: Arc::clone(&inner.reconnect_published),
            reconnect_supported: true,
        });
        Self { client, inner }
    }
    async fn stop(mut self) {
        {
            let _admission = self.inner.write_admission.lock();
            self.inner
                .connection_mode
                .store(ConnectionMode::Closed.as_u8(), Ordering::SeqCst);
        }
        self.inner.state_notify.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), &mut self.inner.write_task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn admit() -> PreparedWriteAdmission {
    Box::new(|continuation| continuation.start_send().map_err(|error| error.to_string()))
}
async fn flag(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !flag.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn send_task(
    client: Arc<WebSocketClient>,
    control: PreparedWriteControl,
    payload: &str,
) -> tokio::task::JoinHandle<PreparedWriteOutcome> {
    let data = payload.to_string();
    let deadline = control.deadline();
    tokio::spawn(async move {
        client
            .send_prepared_text_on_connection(data, None, 0, deadline, control, admit())
            .await
    })
}

#[tokio::test]
async fn prepared_api_abort_during_backpressure_cancels_queued_transport_slot() {
    let backend = Arc::new(Backend::default());
    let harness = Harness::new(&backend, None);
    let control = PreparedWriteControl::new(dst::time::Instant::now() + Duration::from_secs(2));
    let task = send_task(
        Arc::clone(&harness.client),
        control.clone(),
        "cancel-on-drop",
    );
    flag(&backend.polled).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        control.outcome(),
        Some(PreparedWriteOutcome::NotWritten { .. })
    ));
    backend.release();
    tokio::task::yield_now().await;
    harness.stop().await;
    assert!(backend.frames.lock().is_empty());
}

#[tokio::test]
async fn prepared_api_queue_timeout_cannot_write_when_previous_send_releases() {
    let backend = Arc::new(Backend::default());
    let harness = Harness::new(&backend, None);
    let first_control =
        PreparedWriteControl::new(dst::time::Instant::now() + Duration::from_secs(2));
    let first = send_task(Arc::clone(&harness.client), first_control, "first");
    flag(&backend.polled).await;
    let deadline = dst::time::Instant::now() + Duration::from_millis(40);
    let control = PreparedWriteControl::new(deadline);
    let outcome = harness
        .client
        .send_prepared_text_on_connection(
            "expired-in-queue".into(),
            None,
            0,
            deadline,
            control.clone(),
            admit(),
        )
        .await;
    assert!(matches!(outcome, PreparedWriteOutcome::NotWritten { .. }));
    backend.release();
    first.await.unwrap();
    // A following ownership-bound command is a FIFO fence after the expired prepared command.
    harness
        .client
        .send_text_on_connection("fence".into(), None, 0)
        .await
        .unwrap();
    assert_eq!(
        *backend.frames.lock(),
        vec![Message::text("first"), Message::text("fence")]
    );
    harness.stop().await;
}

#[tokio::test]
async fn prepared_api_flush_timeout_is_possible_write_and_never_replayed() {
    let backend = Arc::new(Backend::default());
    backend.ready.store(true, Ordering::Release);
    let harness = Harness::new(&backend, None);
    let deadline = dst::time::Instant::now() + Duration::from_millis(40);
    let control = PreparedWriteControl::new(deadline);
    let outcome = harness
        .client
        .send_prepared_text_on_connection(
            "once".into(),
            None,
            0,
            deadline,
            control.clone(),
            admit(),
        )
        .await;
    assert!(matches!(
        outcome,
        PreparedWriteOutcome::MayHaveWritten { .. }
    ));
    assert_eq!(*backend.frames.lock(), vec![Message::text("once")]);
    // A second use of the same control retains uncertainty, never a contradictory NotWritten.
    let reused = harness
        .client
        .send_prepared_text_on_connection("twice".into(), None, 0, deadline, control, admit())
        .await;
    assert!(matches!(
        reused,
        PreparedWriteOutcome::MayHaveWritten { .. }
    ));
    backend.release();
    let replacement = Arc::new(Backend::default());
    replacement.release();
    let transport: BoxedWsTransport = Box::pin(GatedTransport(Arc::clone(&replacement)));
    let (writer, _reader) = split_transport(transport);
    let (tx, rx) = tokio::sync::oneshot::channel();
    harness
        .client
        .writer_tx
        .send(WriterCommand::Update(writer, tx))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .unwrap()
            .unwrap(),
        1
    );
    {
        let _admission = harness.client.write_admission.lock();
        harness
            .client
            .connection_mode
            .store(ConnectionMode::Active.as_u8(), Ordering::SeqCst);
    }
    harness
        .client
        .send_text_on_connection("replacement-fence".into(), None, 1)
        .await
        .unwrap();
    assert_eq!(
        *replacement.frames.lock(),
        vec![Message::text("replacement-fence")]
    );
    assert_eq!(*backend.frames.lock(), vec![Message::text("once")]);
    harness.stop().await;
}

#[tokio::test]
async fn prepared_api_quota_deadline_denies_without_admission_or_backend_write() {
    let backend = Arc::new(Backend::default());
    backend.release();
    let quota = Quota::per_minute(NonZeroU32::MIN);
    let harness = Harness::new(&backend, Some(quota));
    let keys = [Ustr::from("prepared-quota")];
    harness
        .client
        .send_text_on_connection("quota-used".into(), Some(&keys), 0)
        .await
        .unwrap();
    let deadline = dst::time::Instant::now() + Duration::from_millis(40);
    let control = PreparedWriteControl::new(deadline);
    let called = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&called);
    let outcome = harness
        .client
        .send_prepared_text_on_connection(
            "quota-timeout".into(),
            Some(&keys),
            0,
            deadline,
            control,
            Box::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .await;
    assert!(matches!(outcome, PreparedWriteOutcome::NotWritten { .. }));
    assert_eq!(called.load(Ordering::Acquire), 0);
    assert_eq!(*backend.frames.lock(), vec![Message::text("quota-used")]);
    harness.stop().await;
}

#[tokio::test]
async fn prepared_native_codec_bounded_io_queue_timeout_never_writes_after_backpressure_releases() {
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
    use tokio_tungstenite::tungstenite::Message as TgMessage;

    use super::super::MessageWriter;

    // Observe actual Pending from bounded byte IO, without inventing backend readiness events.
    struct ObservedIo {
        stream: DuplexStream,
        pending: Arc<AtomicBool>,
    }
    impl AsyncRead for ObservedIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(cx, buffer)
        }
    }
    impl AsyncWrite for ObservedIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let result = Pin::new(&mut self.stream).poll_write(cx, bytes);
            if result.is_pending() {
                self.pending.store(true, Ordering::Release);
            }
            result
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
    let (stream, peer_stream) = tokio::io::duplex(4096);
    let native_write_pending = Arc::new(AtomicBool::new(false));
    let stream = ObservedIo {
        stream,
        pending: Arc::clone(&native_write_pending),
    };
    let (release, released) = tokio::sync::oneshot::channel();
    let (paused, pause) = tokio::sync::oneshot::channel();
    let (frames, mut received) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut ws = tokio_tungstenite::accept_async(peer_stream).await.unwrap();
        paused.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), released)
            .await
            .unwrap()
            .unwrap();
        while let Some(message) = ws.next().await {
            match message.unwrap() {
                TgMessage::Text(text) => {
                    frames.send(text.to_string()).unwrap();
                }
                TgMessage::Close(_) => break,
                _ => {}
            }
        }
    });
    let url = "ws://127.0.0.1:1".to_string();
    let (ws, _) = tokio_tungstenite::client_async(url.clone(), stream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), pause)
        .await
        .unwrap()
        .unwrap();
    let transport: BoxedWsTransport = Box::pin(TungsteniteTransport::new(ws));
    let (writer, _reader) = MessageWriter::split(transport);
    let config = WebSocketConfig::builder()
        .url(url)
        .backend(TransportBackend::Tungstenite)
        .build()
        .unwrap();
    let inner = WebSocketClientInner::new_with_writer(config, writer)
        .await
        .unwrap();
    let harness = Harness::from_inner(inner, None);
    let sender = Arc::clone(&harness.client);
    let bytes = 64 * 1024;
    let first = tokio::spawn(async move {
        sender
            .send_text_on_connection("x".repeat(bytes), None, 0)
            .await
    });
    flag(&native_write_pending).await;
    assert!(
        !first.is_finished(),
        "actual native backend flush must be blocked on byte IO"
    );
    let deadline = dst::time::Instant::now() + Duration::from_millis(40);
    let calls = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&calls);
    let control = PreparedWriteControl::new(deadline);
    let outcome = harness
        .client
        .send_prepared_text_on_connection(
            "queue-expired".into(),
            None,
            0,
            deadline,
            control,
            Box::new(move |continuation| {
                called.fetch_add(1, Ordering::SeqCst);
                continuation.start_send().map_err(|error| error.to_string())
            }),
        )
        .await;
    assert!(matches!(outcome, PreparedWriteOutcome::NotWritten { .. }));
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap()
            .len(),
        bytes
    );
    harness
        .client
        .send_text_on_connection("after-queue-fence".into(), None, 0)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap(),
        "after-queue-fence"
    );
    assert_eq!(calls.load(Ordering::Acquire), 0);
    harness.client.send_close_message().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    assert!(received.try_recv().is_err());
    harness.stop().await;
}
