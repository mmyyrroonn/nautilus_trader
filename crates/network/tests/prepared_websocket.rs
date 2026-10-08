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

//! Normal client factories against bounded numeric-loopback WebSocket peers.
#![cfg(not(feature = "turmoil"))]
#![cfg(not(all(feature = "simulation", madsim)))]

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use nautilus_network::{
    dst::time::Instant,
    ratelimiter::quota::Quota,
    websocket::{
        PreparedWriteAdmission, PreparedWriteControl, PreparedWriteOutcome, TransportBackend,
        WebSocketClient, WebSocketConfig, channel_message_handler,
    },
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

fn admit() -> PreparedWriteAdmission {
    Box::new(|continuation| continuation.start_send().map_err(|error| error.to_string()))
}
fn config(address: std::net::SocketAddr) -> WebSocketConfig {
    config_with_backend(address, TransportBackend::Tungstenite)
}

fn config_with_backend(
    address: std::net::SocketAddr,
    backend: TransportBackend,
) -> WebSocketConfig {
    WebSocketConfig::builder()
        .url(format!("ws://{address}"))
        .backend(backend)
        .connect_timeout_ms(1_000)
        .reconnect_delay_initial_ms(10)
        .reconnect_delay_max_ms(10)
        .reconnect_backoff_factor(1.0)
        .reconnect_jitter_ms(0)
        .build()
        .unwrap()
}

struct Peer {
    address: std::net::SocketAddr,
    frames: mpsc::UnboundedReceiver<String>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, frames) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Text(text) => {
                        tx.send(text.to_string()).unwrap();
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });
        Self {
            address,
            frames,
            task,
        }
    }
    async fn frame(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(3), self.frames.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn stop(self, client: &WebSocketClient) {
        client.disconnect().await;
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn prepared_public_factory_admits_once_and_keeps_ordinary_api_usable() {
    verify_public_factory(TransportBackend::Tungstenite).await;
}

#[cfg(feature = "transport-sockudo")]
#[tokio::test]
async fn prepared_sockudo_public_factory_admits_once_and_keeps_ordinary_api_usable() {
    verify_public_factory(TransportBackend::Sockudo).await;
}

async fn verify_public_factory(backend: TransportBackend) {
    let mut peer = Peer::new().await;
    let (handler, _receiver) = channel_message_handler();
    let client = WebSocketClient::builder()
        .config(config_with_backend(peer.address, backend))
        .message_handler(handler)
        .connect()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let admitted = client
        .send_prepared_text_on_connection(
            "admitted".into(),
            None,
            0,
            deadline,
            PreparedWriteControl::new(deadline),
            admit(),
        )
        .await;
    assert_eq!(
        admitted,
        PreparedWriteOutcome::MayHaveWritten { error: None }
    );
    assert_eq!(peer.frame().await, "admitted");
    let rejected = client
        .send_prepared_text_on_connection(
            "denied".into(),
            None,
            0,
            deadline,
            PreparedWriteControl::new(deadline),
            Box::new(|_| Err("adapter proof revoked".into())),
        )
        .await;
    assert!(matches!(rejected, PreparedWriteOutcome::NotWritten { .. }));
    client
        .send_text_on_connection("ordinary-fence".into(), None, 0)
        .await
        .unwrap();
    assert_eq!(peer.frame().await, "ordinary-fence");
    assert!(peer.frames.try_recv().is_err());
    peer.stop(&client).await;
}

#[tokio::test]
async fn prepared_public_quota_deadline_never_calls_admission_or_writes() {
    let mut peer = Peer::new().await;
    let (handler, _receiver) = channel_message_handler();
    let client = WebSocketClient::builder()
        .config(config(peer.address))
        .message_handler(handler)
        .default_quota(Quota::per_minute(NonZeroU32::MIN))
        .connect()
        .await
        .unwrap();
    let keys = [Ustr::from("prepared-quota")];
    client
        .send_text_on_connection("quota-used".into(), Some(&keys), 0)
        .await
        .unwrap();
    assert_eq!(peer.frame().await, "quota-used");
    let deadline = Instant::now() + Duration::from_millis(40);
    let calls = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&calls);
    let outcome = client
        .send_prepared_text_on_connection(
            "quota-timeout".into(),
            Some(&keys),
            0,
            deadline,
            PreparedWriteControl::new(deadline),
            Box::new(move |continuation| {
                called.fetch_add(1, Ordering::SeqCst);
                continuation.start_send().map_err(|error| error.to_string())
            }),
        )
        .await;
    assert!(matches!(outcome, PreparedWriteOutcome::NotWritten { .. }));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(60), peer.frames.recv())
            .await
            .is_err()
    );
    peer.stop(&client).await;
}

#[tokio::test]
async fn prepared_public_abort_while_waiting_for_quota_retains_not_written() {
    let mut peer = Peer::new().await;
    let (handler, _receiver) = channel_message_handler();
    let client = Arc::new(
        WebSocketClient::builder()
            .config(config(peer.address))
            .message_handler(handler)
            .default_quota(Quota::per_minute(NonZeroU32::MIN))
            .connect()
            .await
            .unwrap(),
    );
    let keys = [Ustr::from("prepared-quota")];
    client
        .send_text_on_connection("quota-used".into(), Some(&keys), 0)
        .await
        .unwrap();
    assert_eq!(peer.frame().await, "quota-used");
    let deadline = Instant::now() + Duration::from_secs(2);
    let control = PreparedWriteControl::new(deadline);
    let retained = control.clone();
    let sender = Arc::clone(&client);
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        started.send(()).unwrap();
        sender
            .send_prepared_text_on_connection(
                "aborted".into(),
                Some(&keys),
                0,
                deadline,
                control,
                admit(),
            )
            .await
    });
    ready.await.unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished(), "the explicit quota must hold the send");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        retained.outcome(),
        Some(PreparedWriteOutcome::NotWritten { .. })
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(60), peer.frames.recv())
            .await
            .is_err()
    );
    peer.stop(&client).await;
}

#[tokio::test]
async fn prepared_public_reconnect_rejects_old_epoch_without_replaying() {
    verify_public_reconnect(TransportBackend::Tungstenite).await;
}

#[cfg(feature = "transport-sockudo")]
#[tokio::test]
async fn prepared_sockudo_public_reconnect_rejects_old_epoch_without_replaying() {
    verify_public_reconnect(TransportBackend::Sockudo).await;
}

async fn verify_public_reconnect(backend: TransportBackend) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for epoch in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let tx = tx.clone();
            sockets.push(tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                ws.send(Message::text(format!("connected-{epoch}")))
                    .await
                    .unwrap();
                while let Some(message) = ws.next().await {
                    match message.unwrap() {
                        Message::Text(text) => {
                            tx.send((epoch, text.to_string())).unwrap();
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            }));
        }
        for socket in sockets {
            socket.await.unwrap();
        }
    });
    let (handler, mut incoming) = channel_message_handler();
    let client = WebSocketClient::builder()
        .config(config_with_backend(address, backend))
        .message_handler(handler)
        .connect()
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), incoming.recv())
            .await
            .unwrap()
            .unwrap(),
        Message::text("connected-0")
    );
    assert!(client.request_reconnect());
    tokio::time::timeout(Duration::from_secs(3), async {
        while client.connection_epoch() != 1 || !client.is_active() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let calls = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&calls);
    let rejected = client
        .send_prepared_text_on_connection(
            "old-epoch".into(),
            None,
            0,
            deadline,
            PreparedWriteControl::new(deadline),
            Box::new(move |continuation| {
                called.fetch_add(1, Ordering::SeqCst);
                continuation.start_send().map_err(|error| error.to_string())
            }),
        )
        .await;
    assert!(matches!(rejected, PreparedWriteOutcome::NotWritten { .. }));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    let outcome = client
        .send_prepared_text_on_connection(
            "new-epoch".into(),
            None,
            1,
            deadline,
            PreparedWriteControl::new(deadline),
            admit(),
        )
        .await;
    assert_eq!(
        outcome,
        PreparedWriteOutcome::MayHaveWritten { error: None }
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap(),
        (1, "new-epoch".into())
    );
    client.disconnect().await;
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    assert!(rx.try_recv().is_err());
}
