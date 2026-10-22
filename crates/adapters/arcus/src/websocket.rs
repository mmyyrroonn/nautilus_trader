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

//! Bounded, reconnecting Arcus subscription transport.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use nautilus_common::live::dst::{
    task::{self, JoinHandle},
    time,
};
use nautilus_cryptography::providers::install_cryptographic_provider;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MIN_RECONNECT_DELAY: Duration = Duration::from_millis(1_500);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
const STABLE_STREAM_DURATION: Duration = Duration::from_secs(30);

/// Events preserve connection boundaries, so a consumer can invalidate cached state.
#[derive(Debug)]
pub enum ArcusWsEvent {
    Connected,
    Disconnected,
    Ready,
    Message { epoch: u64, value: Value },
    Error(String),
}

/// Owns one subscription worker and bounded command and event queues.
#[derive(Debug)]
pub struct ArcusWebSocket {
    commands: tokio::sync::mpsc::Sender<Command>,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    health: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
}

impl ArcusWebSocket {
    /// Connects to Arcus and starts the subscription worker.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits or a failed initial handshake.
    pub async fn connect(
        url: String,
        queue_capacity: usize,
        idle_timeout: Duration,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Receiver<ArcusWsEvent>)> {
        Self::connect_with_minimum(url, queue_capacity, idle_timeout, 1).await
    }

    async fn connect_with_minimum(
        url: String,
        queue_capacity: usize,
        idle_timeout: Duration,
        minimum_subscriptions: usize,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Receiver<ArcusWsEvent>)> {
        install_cryptographic_provider();
        anyhow::ensure!(queue_capacity > 0, "Arcus queue capacity must be positive");
        anyhow::ensure!(
            !idle_timeout.is_zero(),
            "Arcus idle timeout must be positive"
        );
        let socket = open(&url).await?;
        let (commands, command_rx) = tokio::sync::mpsc::channel(queue_capacity);
        let (event_tx, events) = tokio::sync::mpsc::channel(queue_capacity);
        let cancel = CancellationToken::new();
        let health = Arc::new(AtomicBool::new(false));
        let connected = Arc::new(AtomicBool::new(true));
        let epoch = Arc::new(AtomicU64::new(1));

        let task = task::spawn(run(
            url,
            socket,
            command_rx,
            event_tx,
            cancel.clone(),
            WorkerConfig {
                idle_timeout,
                minimum_subscriptions,
                health: health.clone(),
                connected: connected.clone(),
                epoch: epoch.clone(),
            },
        ));
        Ok((
            Self {
                commands,
                cancel,
                task: Some(task),
                health,
                connected,
                epoch,
            },
            events,
        ))
    }

    /// Enqueues a subscription without waiting for queue space.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is full or closed.
    pub fn try_subscribe(&self, subscription: Value) -> anyhow::Result<()> {
        validate_subscription(&subscription)?;
        self.commands
            .try_send(Command::Subscribe(subscription))
            .context("Arcus command queue unavailable")
    }

    /// Enqueues removal of a subscription without waiting for queue space.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is full or closed.
    pub fn try_unsubscribe(&self, subscription: Value) -> anyhow::Result<()> {
        validate_subscription(&subscription)?;
        self.commands
            .try_send(Command::Unsubscribe(subscription))
            .context("Arcus command queue unavailable")
    }

    /// Enqueues a subscription, waiting for bounded queue space.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport is closed or the subscription is invalid.
    pub async fn subscribe(&self, subscription: Value) -> anyhow::Result<()> {
        validate_subscription(&subscription)?;
        self.commands
            .send(Command::Subscribe(subscription))
            .await
            .context("Arcus transport closed")
    }

    /// Enqueues removal of a subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport is closed or the subscription is invalid.
    pub async fn unsubscribe(&self, subscription: Value) -> anyhow::Result<()> {
        validate_subscription(&subscription)?;
        self.commands
            .send(Command::Unsubscribe(subscription))
            .await
            .context("Arcus transport closed")
    }

    /// Requests a fresh connection and fresh snapshots for every subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the command queue is unavailable.
    pub fn reconnect(&self) -> anyhow::Result<()> {
        self.health.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.commands
            .try_send(Command::Reconnect)
            .context("Arcus command queue unavailable")
    }

    /// Returns the synchronous stream readiness flag.
    pub fn health(&self) -> Arc<AtomicBool> {
        self.health.clone()
    }

    /// Returns the connection generation, advanced before any stale events can be drained.
    pub fn epoch(&self) -> Arc<AtomicU64> {
        self.epoch.clone()
    }

    /// Returns the transport connection flag, cleared synchronously on failure.
    pub fn connected(&self) -> Arc<AtomicBool> {
        self.connected.clone()
    }

    /// Cancels the worker without waiting for it.
    pub fn abort(&self) {
        self.health.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.cancel.cancel();

        if let Some(task) = &self.task {
            task.abort();
        }
    }

    /// Cancels and joins the worker.
    pub async fn close(&mut self) {
        self.health.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.cancel.cancel();

        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ArcusWebSocket {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Account subscriptions are public reads; writes are signed independently.
#[derive(Debug)]
pub struct ArcusPrivateStream(ArcusWebSocket);

impl ArcusPrivateStream {
    /// Connects and subscribes to the selected account's reconciliation channels.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid account identity or a failed handshake.
    pub async fn connect(
        url: String,
        address: &str,
        subaccount: u8,
        queue_capacity: usize,
        idle_timeout: Duration,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Receiver<ArcusWsEvent>)> {
        anyhow::ensure!(
            queue_capacity >= 8,
            "Arcus private stream queue capacity must be at least eight"
        );
        anyhow::ensure!(
            subaccount <= 9,
            "Arcus account index must be between 0 and 9"
        );
        anyhow::ensure!(
            address.len() == 42
                && address.starts_with("0x")
                && address[2..].bytes().all(|b| b.is_ascii_hexdigit()),
            "Invalid Arcus account address"
        );
        let (socket, events) =
            ArcusWebSocket::connect_with_minimum(url, queue_capacity, idle_timeout, 4).await?;

        for channel in ["account", "orders", "positions", "userFills"] {
            socket.subscribe(json!({"type":"subscribe", "channel":channel, "id":address.to_ascii_lowercase(), "accountIndex":subaccount})).await?;
        }
        Ok((Self(socket), events))
    }

    /// Returns readiness, cleared immediately on a broken stream.
    pub fn health(&self) -> Arc<AtomicBool> {
        self.0.health()
    }

    /// Returns the generation for rejecting events from an ended connection.
    pub fn epoch(&self) -> Arc<AtomicU64> {
        self.0.epoch()
    }

    /// Returns the physical connection flag.
    pub fn connected(&self) -> Arc<AtomicBool> {
        self.0.connected()
    }

    /// Cancels the worker immediately.
    pub fn abort(&self) {
        self.0.abort();
    }

    /// Cancels and joins the worker.
    pub async fn close(&mut self) {
        self.0.close().await;
    }
}

#[derive(Debug)]
enum Command {
    Subscribe(Value),
    Unsubscribe(Value),
    Reconnect,
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn validate_subscription(subscription: &Value) -> anyhow::Result<()> {
    anyhow::ensure!(
        subscription
            .get("channel")
            .and_then(Value::as_str)
            .is_some(),
        "Arcus subscription requires a channel"
    );
    Ok(())
}

fn identity(mut subscription: Value) -> Value {
    if let Some(object) = subscription.as_object_mut() {
        object.remove("type");
    }
    subscription
}

fn same_subscription(left: &Value, right: &Value) -> bool {
    [
        "channel",
        "id",
        "accountIndex",
        "market",
        "sigFigs",
        "roundStep",
    ]
    .iter()
    .all(|field| left.get(*field) == right.get(*field))
}

async fn open(url: &str) -> anyhow::Result<Socket> {
    let (socket, _) = time::timeout(CONNECT_TIMEOUT, connect_async(url))
        .await
        .context("Arcus WebSocket connect timed out")??;
    Ok(socket)
}

async fn send(socket: &mut Socket, mut subscription: Value, kind: &str) -> anyhow::Result<()> {
    subscription["type"] = kind.into();
    time::timeout(
        SEND_TIMEOUT,
        socket.send(Message::Text(subscription.to_string().into())),
    )
    .await
    .context("Arcus WebSocket send timed out")??;
    Ok(())
}

async fn emit(
    events: &tokio::sync::mpsc::Sender<ArcusWsEvent>,
    cancel: &CancellationToken,
    event: ArcusWsEvent,
) -> bool {
    tokio::select! { biased; () = cancel.cancelled() => false, result = events.send(event) => result.is_ok() }
}

struct WorkerConfig {
    idle_timeout: Duration,
    minimum_subscriptions: usize,
    health: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
}

async fn run(
    url: String,
    initial: Socket,
    mut commands: tokio::sync::mpsc::Receiver<Command>,
    events: tokio::sync::mpsc::Sender<ArcusWsEvent>,
    cancel: CancellationToken,
    config: WorkerConfig,
) {
    let WorkerConfig {
        idle_timeout,
        minimum_subscriptions,
        health,
        connected,
        epoch,
    } = config;
    let mut subscriptions = Vec::<Value>::new();
    let mut next_socket = Some(initial);
    let mut backoff = MIN_RECONNECT_DELAY;

    loop {
        let mut socket = match next_socket.take() {
            Some(socket) => socket,
            None => {
                tokio::select! { biased;
                    () = cancel.cancelled() => return,
                    () = time::sleep(backoff) => {},
                }
                backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
                let result = tokio::select! { biased; () = cancel.cancelled() => return, result = open(&url) => result };

                match result {
                    Ok(socket) => socket,
                    Err(e) => {
                        if !emit(&events, &cancel, ArcusWsEvent::Error(e.to_string())).await {
                            return;
                        }
                        continue;
                    }
                }
            }
        };
        let session_epoch = epoch.load(Ordering::Acquire);
        connected.store(true, Ordering::Release);

        if !emit(&events, &cancel, ArcusWsEvent::Connected).await {
            return;
        }
        let mut failed = false;

        for subscription in &subscriptions {
            let result = tokio::select! { biased; () = cancel.cancelled() => return, result = send(&mut socket, subscription.clone(), "subscribe") => result };

            if result.is_err() {
                failed = true;
                break;
            }
        }
        let mut acknowledged = Vec::<Value>::new();
        let mut healthy_since = None::<time::Instant>;

        if !failed {
            let heartbeat_period = (idle_timeout / 3)
                .min(Duration::from_secs(15))
                .max(Duration::from_millis(1));
            let mut heartbeat = time::interval(heartbeat_period);
            heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            let mut last_received = time::Instant::now();

            loop {
                let result: anyhow::Result<()> = async { tokio::select! { biased;
                    () = cancel.cancelled() => anyhow::bail!("Arcus transport cancelled"),
                    command = commands.recv() => {
                        match command {
                            None => anyhow::bail!("Arcus command queue closed"),
                            Some(Command::Reconnect) => anyhow::bail!("Arcus reconnect requested"),
                            Some(Command::Subscribe(value)) => {
                                let key = identity(value);

                                if subscriptions.iter().any(|value| same_subscription(value, &key)) { Ok(()) } else {
                                    health.store(false, Ordering::Release);
                                    healthy_since = None;
                                    subscriptions.push(key.clone());
                                    send(&mut socket, key, "subscribe").await
                                }
                            }
                            Some(Command::Unsubscribe(value)) => {
                                let key = identity(value);
                                subscriptions.retain(|value| !same_subscription(value, &key));
                                acknowledged.retain(|value| !same_subscription(value, &key));
                                send(&mut socket, key, "unsubscribe").await
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        if last_received.elapsed() >= idle_timeout { anyhow::bail!("Arcus WebSocket idle timeout"); }

                        if health.load(Ordering::Acquire) && healthy_since.is_some_and(|since| since.elapsed() >= STABLE_STREAM_DURATION) { backoff = MIN_RECONNECT_DELAY; }
                        time::timeout(SEND_TIMEOUT, socket.send(Message::Ping(Vec::new().into()))).await.context("Arcus heartbeat send timed out")?.context("Arcus heartbeat send failed")
                    }
                    message = socket.next() => {
                        last_received = time::Instant::now();

                        match message {
                            Some(Ok(Message::Text(text))) => {
                                let value = serde_json::from_str::<Value>(&text).context("Invalid Arcus WebSocket JSON");

                                match value {
                                    Ok(value) => {
                                        if value["type"] == "error" {
                                            health.store(false, Ordering::Release);
                                            anyhow::bail!("Arcus rejected a WebSocket subscription");
                                        }

                                        if matches!(value["type"].as_str(), Some("subscribed" | "degraded")) {
                                            for subscription in &subscriptions {
                                                let matches = ["channel", "id", "accountIndex", "market", "sigFigs", "roundStep"].iter().all(|field| subscription.get(*field) == value.get(*field));

                                                if matches {
                                                    acknowledged.retain(|item| item != subscription);

                                                    if value["type"] == "subscribed" { acknowledged.push(subscription.clone()); }
                                                }
                                            }
                                            let ready = subscriptions.len() >= minimum_subscriptions && subscriptions.iter().all(|key| acknowledged.contains(key));
                                            let was_ready = health.swap(ready, Ordering::AcqRel);

                                            if !ready { healthy_since = None; } else if !was_ready { healthy_since = Some(time::Instant::now()); }

                                            if ready && !was_ready { events.try_send(ArcusWsEvent::Ready).context("Arcus event queue overflow or closed")?; }
                                        }
                                        events.try_send(ArcusWsEvent::Message { epoch: session_epoch, value }).context("Arcus event queue overflow or closed")
                                    },
                                    Err(e) => Err(e),
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => time::timeout(SEND_TIMEOUT, socket.send(Message::Pong(payload))).await.context("Arcus pong send timed out")?.context("Arcus pong send failed"),
                            Some(Ok(Message::Pong(_))) => Ok(()),
                            Some(Ok(Message::Close(_))) | None => anyhow::bail!("Arcus socket closed"),
                            Some(Ok(_)) => anyhow::bail!("Unsupported Arcus WebSocket frame"),
                            Some(Err(e)) => Err(e.into()),
                        }
                    }
                } }.await;

                if let Err(e) = result {
                    health.store(false, Ordering::Release);

                    if connected.swap(false, Ordering::AcqRel) {
                        epoch.fetch_add(1, Ordering::AcqRel);
                    }
                    log::warn!("Arcus WebSocket reconnecting: {e}");
                    break;
                }
            }
        }
        health.store(false, Ordering::Release);

        if connected.swap(false, Ordering::AcqRel) {
            epoch.fetch_add(1, Ordering::AcqRel);
        }
        drop(socket);

        if !emit(&events, &cancel, ArcusWsEvent::Disconnected).await {
            return;
        }
    }
}
