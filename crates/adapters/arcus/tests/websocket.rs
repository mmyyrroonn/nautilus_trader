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

use std::{sync::atomic::Ordering, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nautilus_arcus::websocket::{ArcusPrivateStream, ArcusWebSocket, ArcusWsEvent};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

async fn frame(socket: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        match socket.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            _ => {}
        }
    }
}

async fn ack(socket: &mut WebSocketStream<TcpStream>, mut subscription: Value) {
    subscription["type"] = json!("subscribed");
    subscription["contents"] = json!({});
    socket
        .send(Message::Text(subscription.to_string().into()))
        .await
        .unwrap();
}

async fn event(events: &mut mpsc::Receiver<ArcusWsEvent>) -> ArcusWsEvent {
    timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn reconnect_replays_subscription_and_exposes_boundaries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());

    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let subscription = frame(&mut first).await;
        assert_eq!(subscription["nLevels"], 100);
        ack(&mut first, subscription.clone()).await;
        first.close(None).await.unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(frame(&mut second).await, subscription);
        ack(&mut second, subscription).await;

        while second.next().await.is_some() {}
    });
    let (mut socket, mut events) = ArcusWebSocket::connect(url, 32, Duration::from_secs(5))
        .await
        .unwrap();
    socket
        .subscribe(json!({"channel":"l2OrderbookUpdates","id":"BTC-USD","nLevels":100}))
        .await
        .unwrap();
    let mut connections = 0;
    let mut disconnected = false;

    loop {
        match event(&mut events).await {
            ArcusWsEvent::Connected => connections += 1,
            ArcusWsEvent::Disconnected => disconnected = true,
            ArcusWsEvent::Ready if connections == 2 => break,
            _ => {}
        }
    }
    assert!(disconnected);
    socket.close().await;
    assert!(!socket.health().load(Ordering::Acquire));
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn private_readiness_waits_for_all_four_acks_and_degraded_clears_health() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (three_tx, three_rx) = oneshot::channel();
    let (four_tx, four_rx) = oneshot::channel();
    let (degrade_tx, degrade_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut subscriptions = Vec::new();

        for _ in 0..4 {
            subscriptions.push(frame(&mut socket).await);
        }

        for subscription in &subscriptions {
            assert_eq!(subscription["accountIndex"], 7);
            assert_eq!(
                subscription["id"],
                "0xabcdef0123456789abcdef0123456789abcdef01"
            );
        }

        for subscription in subscriptions.iter().take(3) {
            ack(&mut socket, subscription.clone()).await;
        }
        three_tx.send(()).unwrap();
        four_rx.await.unwrap();
        ack(&mut socket, subscriptions[3].clone()).await;
        degrade_rx.await.unwrap();
        let mut degraded = subscriptions[0].clone();
        degraded["type"] = json!("degraded");
        degraded["reason"] = json!("snapshot_stale");
        socket
            .send(Message::Text(degraded.to_string().into()))
            .await
            .unwrap();

        while socket.next().await.is_some() {}
    });
    let (mut stream, mut events) = ArcusPrivateStream::connect(
        url,
        "0xABCDEF0123456789ABCDEF0123456789ABCDEF01",
        7,
        32,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    three_rx.await.unwrap();
    let mut received = 0;

    while received < 3 {
        if matches!(event(&mut events).await, ArcusWsEvent::Message { .. }) {
            received += 1;
        }
    }
    assert!(!stream.health().load(Ordering::Acquire));
    four_tx.send(()).unwrap();

    while !matches!(event(&mut events).await, ArcusWsEvent::Ready) {}
    assert!(stream.health().load(Ordering::Acquire));
    degrade_tx.send(()).unwrap();

    loop {
        if let ArcusWsEvent::Message { value, .. } = event(&mut events).await
            && value["type"] == "degraded"
        {
            break;
        }
    }
    assert!(!stream.health().load(Ordering::Acquire));
    stream.close().await;
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn bounded_event_overflow_invalidates_before_consumer_drains_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (flood_tx, flood_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let subscription = frame(&mut socket).await;
        ack(&mut socket, subscription).await;
        flood_rx.await.unwrap();

        for sequence in 0..64 {
            if socket.send(Message::Text(json!({"type":"channel_data","channel":"bbo","id":"BTC-USD","contents":{"lastSequenceId":sequence}}).to_string().into())).await.is_err() { break; }
        }

        while socket.next().await.is_some() {}
    });
    let (mut socket, mut events) = ArcusWebSocket::connect(url, 2, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(event(&mut events).await, ArcusWsEvent::Connected));
    socket
        .subscribe(json!({"channel":"bbo","id":"BTC-USD"}))
        .await
        .unwrap();

    while !matches!(event(&mut events).await, ArcusWsEvent::Message { .. }) {}
    assert!(socket.health().load(Ordering::Acquire));
    flood_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), async {
        while socket.health().load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut messages = 0;
    let mut current_messages = 0;

    loop {
        match event(&mut events).await {
            ArcusWsEvent::Message { epoch, .. } => {
                messages += 1;

                if epoch == socket.epoch().load(Ordering::Acquire) {
                    current_messages += 1;
                }
            }
            ArcusWsEvent::Disconnected => break,
            _ => {}
        }
    }
    assert!(messages <= 2);
    assert_eq!(
        current_messages, 0,
        "queued frames from the overflowed connection must be rejected"
    );
    timeout(Duration::from_millis(200), socket.close())
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn idle_socket_reconnects_and_shutdown_joins_even_with_full_event_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());

    let server = tokio::spawn(async move {
        let _socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let (mut socket, mut events) = ArcusWebSocket::connect(url, 1, Duration::from_millis(90))
        .await
        .unwrap();
    assert!(matches!(event(&mut events).await, ArcusWsEvent::Connected));
    assert!(matches!(
        event(&mut events).await,
        ArcusWsEvent::Disconnected
    ));
    timeout(Duration::from_millis(200), socket.close())
        .await
        .unwrap();
    assert!(events.recv().await.is_none());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn successful_handshakes_do_not_reset_backoff_when_subscriptions_keep_failing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (attempts_tx, mut attempts_rx) = mpsc::channel(3);

    let server = tokio::spawn(async move {
        for attempt in 0..3 {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let subscription = frame(&mut socket).await;
            attempts_tx.send(tokio::time::Instant::now()).await.unwrap();

            if attempt < 2 {
                socket
                    .send(Message::Text(
                        json!({"type":"error","message":"subscription unavailable"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();

                while socket.next().await.is_some() {}
            } else {
                ack(&mut socket, subscription).await;

                while socket.next().await.is_some() {}
            }
        }
    });
    let (mut socket, mut events) = ArcusWebSocket::connect(url, 32, Duration::from_secs(5))
        .await
        .unwrap();
    socket
        .subscribe(json!({"channel":"bbo","id":"BTC-USD"}))
        .await
        .unwrap();

    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let attempts = timeout(Duration::from_secs(10), async {
        let first = attempts_rx.recv().await.unwrap();
        let second = attempts_rx.recv().await.unwrap();
        let third = attempts_rx.recv().await.unwrap();
        [first, second, third]
    })
    .await
    .unwrap();
    assert!(attempts[1].duration_since(attempts[0]) >= Duration::from_millis(1_400));
    assert!(attempts[2].duration_since(attempts[1]) >= Duration::from_millis(2_900));
    socket.close().await;
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), drain)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn private_stream_rejects_account_index_ten_before_connecting() {
    let result = ArcusPrivateStream::connect(
        "ws://127.0.0.1:1".to_owned(),
        "0xabcdef0123456789abcdef0123456789abcdef01",
        10,
        16,
        Duration::from_secs(5),
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("between 0 and 9"));
}
