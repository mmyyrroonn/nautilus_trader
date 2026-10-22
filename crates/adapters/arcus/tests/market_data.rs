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

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{Json, Router, extract::Query, routing::get};
use futures_util::{SinkExt, StreamExt};
use nautilus_arcus::{config::ArcusDataClientConfig, data::ArcusDataClient};
use nautilus_common::{
    clients::DataClient,
    live::runner::set_data_event_sender,
    messages::{
        DataEvent,
        data::{
            DataResponse, RequestFundingRates, SubscribeFundingRates, SubscribeIndexPrices,
            SubscribeMarkPrices, UnsubscribeFundingRates, UnsubscribeIndexPrices,
            UnsubscribeMarkPrices,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, InstrumentId},
};
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

struct Harness {
    client: ArcusDataClient,
    events: mpsc::UnboundedReceiver<DataEvent>,
    wire: mpsc::Receiver<Value>,
    incoming: mpsc::Sender<Value>,
    queries: mpsc::Receiver<HashMap<String, String>>,
    history_release: mpsc::Sender<()>,
    http_task: JoinHandle<()>,
    ws_task: JoinHandle<()>,
}

impl Harness {
    async fn new() -> Self {
        let market: Value = serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_url = format!("http://{}", http.local_addr().unwrap());
        let (query_tx, queries) = mpsc::channel(16);
        let (history_release, history_gate) = mpsc::channel::<()>(16);
        let history_gate = Arc::new(tokio::sync::Mutex::new(history_gate));
        let app = Router::new().route("/v1/markets", get(move || { let market = market.clone(); async move { Json(json!({"markets":[market]})) } })).route("/v1/fundingRates", get(move |Query(query): Query<HashMap<String, String>>| {
            let query_tx = query_tx.clone();
            let history_gate = history_gate.clone();
            async move {
                let from = query["from"].parse::<u64>().unwrap();
                let first = (from / 1000 + 1) * 1000;
                query_tx.send(query).await.unwrap();
                let _ = history_gate.lock().await.recv().await;
                Json(json!({"fundingRates":[{"marketId":1,"marketDisplayName":"BTC-USD","fundingRate":"0.00002","time":first+1000},{"marketId":1,"marketDisplayName":"BTC-USD","fundingRate":"-0.00001","time":first}]}))
            }
        }));

        let http_task = tokio::spawn(async move {
            axum::serve(http, app).await.unwrap();
        });
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", ws.local_addr().unwrap());
        let (wire_tx, wire) = mpsc::channel(16);
        let (incoming, mut incoming_rx) = mpsc::channel::<Value>(16);

        let ws_task = tokio::spawn(async move {
            loop {
                let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();
                wire_tx.send(json!({"_connected":true})).await.unwrap();

                loop {
                    tokio::select! { biased;
                        message = incoming_rx.recv() => {
                            let Some(message) = message else { return; };

                            if message["_close"] == true { let _ = socket.close(None).await; break; }

                            if socket.send(Message::Text(message.to_string().into())).await.is_err() { break; }
                        }
                        message = socket.next() => match message {
                            Some(Ok(Message::Text(text))) => {
                                let mut value: Value = serde_json::from_str(&text).unwrap();
                                wire_tx.send(value.clone()).await.unwrap();
                                value["type"] = if value["type"] == "subscribe" { "subscribed".into() } else { "unsubscribed".into() };
                                value["contents"] = json!({});

                                if socket.send(Message::Text(value.to_string().into())).await.is_err() { break; }
                            }
                            Some(Ok(Message::Ping(payload))) => { if socket.send(Message::Pong(payload)).await.is_err() { break; } }
                            Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                            _ => {},
                        },
                    }
                }
            }
        });
        let (sender, events) = mpsc::unbounded_channel();
        set_data_event_sender(sender);
        let mut client = ArcusDataClient::new(
            client_id(),
            ArcusDataClientConfig {
                base_url_http: Some(http_url),
                base_url_ws: Some(ws_url),
                queue_capacity: 16,
                ..Default::default()
            },
        )
        .unwrap();
        client.connect().await.unwrap();
        let mut harness = Self {
            client,
            events,
            wire,
            incoming,
            queries,
            history_release,
            http_task,
            ws_task,
        };
        assert_eq!(harness.next_wire().await["_connected"], true);
        harness
    }

    async fn next_wire(&mut self) -> Value {
        timeout(Duration::from_secs(4), self.wire.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn next_market_event(&mut self) -> DataEvent {
        loop {
            let event = timeout(Duration::from_secs(4), self.events.recv())
                .await
                .unwrap()
                .unwrap();

            if matches!(
                event,
                DataEvent::Data(Data::MarkPrice(_) | Data::IndexPrice(_))
                    | DataEvent::FundingRate(_)
                    | DataEvent::Response(DataResponse::FundingRates(_))
            ) {
                return event;
            }
        }
    }
    async fn close(mut self) {
        self.client.disconnect().await.unwrap();
        self.http_task.abort();
        self.ws_task.abort();
        let _ = self.http_task.await;
        let _ = self.ws_task.await;
    }
}

fn id() -> InstrumentId {
    InstrumentId::from("BTC-USD-PERP.ARCUS")
}
fn client_id() -> ClientId {
    ClientId::from("ARCUS")
}
fn mark() -> SubscribeMarkPrices {
    SubscribeMarkPrices::new(
        id(),
        Some(client_id()),
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    )
}
fn index() -> SubscribeIndexPrices {
    SubscribeIndexPrices::new(
        id(),
        Some(client_id()),
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    )
}
fn funding() -> SubscribeFundingRates {
    SubscribeFundingRates::new(
        id(),
        Some(client_id()),
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    )
}
fn oracle(epoch: u64) -> Value {
    json!({"type":"channel_data","channel":"oraclePrices","id":"BTC-USD","contents":{"epoch":epoch,"prices":[{"marketId":1,"marketDisplayName":"BTC-USD","price":"50000.1234","markPrice":"50000.9876","markEpochNanos":epoch}]}})
}

#[tokio::test]
async fn trait_shares_oracle_subscription_filters_consumers_and_unsubscribes_only_last() {
    let mut h = Harness::new().await;
    h.client.subscribe_mark_prices(mark()).unwrap();
    h.client.subscribe_index_prices(index()).unwrap();
    h.client.subscribe_mark_prices(mark()).unwrap();
    let command = h.next_wire().await;
    assert_eq!(command["type"], "subscribe");
    assert_eq!(command["channel"], "oraclePrices");
    assert_eq!(command["id"], "BTC-USD");
    assert!(
        timeout(Duration::from_millis(30), h.wire.recv())
            .await
            .is_err()
    );
    h.client.subscribe_funding_rates(funding()).unwrap();
    assert_eq!(h.next_wire().await["channel"], "predictedFunding");
    h.incoming.send(oracle(100)).await.unwrap();
    assert!(matches!(
        h.next_market_event().await,
        DataEvent::Data(Data::MarkPrice(_))
    ));
    assert!(matches!(
        h.next_market_event().await,
        DataEvent::Data(Data::IndexPrice(_))
    ));
    h.incoming.send(oracle(100)).await.unwrap();
    assert!(
        timeout(Duration::from_millis(30), h.events.recv())
            .await
            .is_err()
    );
    h.client
        .unsubscribe_mark_prices(&UnsubscribeMarkPrices::new(
            id(),
            Some(client_id()),
            None,
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        ))
        .unwrap();
    assert!(
        timeout(Duration::from_millis(30), h.wire.recv())
            .await
            .is_err()
    );
    h.incoming.send(oracle(101)).await.unwrap();
    assert!(matches!(
        h.next_market_event().await,
        DataEvent::Data(Data::IndexPrice(_))
    ));
    assert!(
        timeout(Duration::from_millis(30), h.events.recv())
            .await
            .is_err()
    );
    h.client
        .unsubscribe_index_prices(&UnsubscribeIndexPrices::new(
            id(),
            Some(client_id()),
            None,
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        ))
        .unwrap();
    let command = h.next_wire().await;
    assert_eq!(command["type"], "unsubscribe");
    assert_eq!(command["channel"], "oraclePrices");
    h.client
        .unsubscribe_funding_rates(&UnsubscribeFundingRates::new(
            id(),
            Some(client_id()),
            None,
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(h.next_wire().await["channel"], "predictedFunding");
    h.close().await;
}

#[tokio::test]
async fn trait_reconnect_restores_reference_subscriptions_and_resets_source_deduplication() {
    let mut h = Harness::new().await;
    h.client.subscribe_mark_prices(mark()).unwrap();
    h.client.subscribe_funding_rates(funding()).unwrap();
    assert_eq!(h.next_wire().await["channel"], "oraclePrices");
    assert_eq!(h.next_wire().await["channel"], "predictedFunding");
    h.incoming.send(oracle(100)).await.unwrap();
    assert!(matches!(
        h.next_market_event().await,
        DataEvent::Data(Data::MarkPrice(_))
    ));
    h.incoming.send(json!({"_close":true})).await.unwrap();
    assert_eq!(h.next_wire().await["_connected"], true);
    assert_eq!(h.next_wire().await["channel"], "oraclePrices");
    assert_eq!(h.next_wire().await["channel"], "predictedFunding");
    h.incoming.send(oracle(100)).await.unwrap();
    assert!(matches!(
        h.next_market_event().await,
        DataEvent::Data(Data::MarkPrice(_))
    ));
    h.incoming.send(json!({"type":"channel_data","channel":"predictedFunding","id":"BTC-USD","contents":{"market":"BTC-USD","rate1h":"-0.00001"}})).await.unwrap();
    let DataEvent::FundingRate(rate) = h.next_market_event().await else {
        panic!("Expected funding")
    };
    assert_eq!(rate.rate, dec!(-0.00001));
    assert_eq!(rate.interval, Some(60));
    assert_eq!(rate.next_funding_ns, None);
    h.close().await;
}

#[tokio::test]
async fn trait_historical_funding_preserves_correlation_precision_inclusive_bounds_and_order() {
    let h = Harness::new().await;
    let request = RequestFundingRates::new(
        id(),
        Some("2026-10-21T12:00:00.000000001Z".parse().unwrap()),
        Some("2026-10-21T12:00:00.002999999Z".parse().unwrap()),
        NonZeroUsize::new(2),
        Some(client_id()),
        UUID4::new(),
        UnixNanos::from(1),
        None,
    );
    let request_id = request.request_id;
    let start_ns = u64::try_from(request.start.unwrap().as_nanosecond()).unwrap();
    let end_ns = u64::try_from(request.end.unwrap().as_nanosecond()).unwrap();

    let mut h = std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        h.client.request_funding_rates(request).unwrap();
        h
    })
    .join()
    .unwrap();
    let query = timeout(Duration::from_secs(4), h.queries.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(query["from"].parse::<u64>().unwrap(), start_ns / 1000 + 1);
    assert_eq!(query["to"].parse::<u64>().unwrap(), end_ns / 1000);
    h.history_release.send(()).await.unwrap();
    let DataEvent::Response(mut response) = h.next_market_event().await else {
        panic!("Expected historical funding response")
    };
    response.trim_to_bounds();
    let DataResponse::FundingRates(response) = response else {
        panic!("Expected historical funding response")
    };
    assert_eq!(response.correlation_id, request_id);
    assert_eq!(response.data.len(), 2);
    assert_eq!(response.data[0].rate, dec!(-0.00001));
    assert_eq!(response.data[1].rate, dec!(0.00002));
    assert!(response.data[0].ts_event < response.data[1].ts_event);

    for rate in response.data {
        assert_eq!(rate.interval, Some(60));
        assert_eq!(rate.next_funding_ns, None);
        assert_eq!(rate.ts_init, rate.ts_event);
        assert!(rate.ts_event.as_u64() >= start_ns && rate.ts_event.as_u64() <= end_ns);
    }
    h.close().await;
}

#[tokio::test]
async fn trait_invalid_funding_history_is_rejected_synchronously_without_http() {
    let mut h = Harness::new().await;
    let invalid_time = RequestFundingRates::new(
        id(),
        Some("1970-01-01T00:00:00Z".parse().unwrap()),
        None,
        NonZeroUsize::new(1),
        Some(client_id()),
        UUID4::new(),
        UnixNanos::from(1),
        None,
    );
    assert!(h.client.request_funding_rates(invalid_time).is_err());
    let excessive_limit = RequestFundingRates::new(
        id(),
        None,
        None,
        NonZeroUsize::new(64001),
        Some(client_id()),
        UUID4::new(),
        UnixNanos::from(1),
        None,
    );
    assert!(h.client.request_funding_rates(excessive_limit).is_err());
    assert!(
        timeout(Duration::from_millis(30), h.queries.recv())
            .await
            .is_err()
    );
    h.close().await;
}

#[tokio::test]
async fn historical_request_admission_is_bounded_and_reset_cancels_pending_responses() {
    let mut h = Harness::new().await;

    for _ in 0..16 {
        let request = RequestFundingRates::new(
            id(),
            Some("2026-10-21T12:00:00.000000001Z".parse().unwrap()),
            Some("2026-10-21T12:00:00.002999999Z".parse().unwrap()),
            NonZeroUsize::new(2),
            Some(client_id()),
            UUID4::new(),
            UnixNanos::from(1),
            None,
        );
        h.client.request_funding_rates(request).unwrap();
    }
    let seventeenth = RequestFundingRates::new(
        id(),
        Some("2026-10-21T12:00:00.000000001Z".parse().unwrap()),
        Some("2026-10-21T12:00:00.002999999Z".parse().unwrap()),
        NonZeroUsize::new(2),
        Some(client_id()),
        UUID4::new(),
        UnixNanos::from(1),
        None,
    );
    assert!(
        h.client
            .request_funding_rates(seventeenth)
            .unwrap_err()
            .to_string()
            .contains("queue is full")
    );
    timeout(Duration::from_secs(4), h.queries.recv())
        .await
        .unwrap()
        .unwrap();
    h.client.reset().unwrap();

    for _ in 0..16 {
        h.history_release.send(()).await.unwrap();
    }

    while let Ok(event) = h.events.try_recv() {
        assert!(!matches!(
            event,
            DataEvent::Response(DataResponse::FundingRates(_))
        ));
    }
    assert!(
        timeout(Duration::from_millis(100), h.events.recv())
            .await
            .is_err()
    );
    h.close().await;
}
