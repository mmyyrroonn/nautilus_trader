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

use nautilus_arcus::{
    data::{ArcusBookState, parse_quote, parse_trades},
    instruments::parse_instrument,
    models::ArcusMarket,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{BookAction, BookType, RecordFlag},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
};
use rstest::rstest;
use serde_json::{Value, json};

fn instrument() -> InstrumentAny {
    let market: ArcusMarket =
        serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
    parse_instrument(&market, UnixNanos::from(1)).unwrap()
}

fn book(kind: &str, sequence: u64, bids: Value, asks: Value) -> Value {
    let mut frame = json!({"type":kind,"channel":"l2OrderbookUpdates","id":"BTC-USD","contents":{"lastSequenceId":sequence,"timestamp":1790000000000000_u64}});
    frame["contents"]["bids"] = bids;
    frame["contents"]["asks"] = asks;
    frame
}

#[rstest]
fn snapshot_boundary_gap_is_valid_but_midstream_gap_requires_new_snapshot() {
    let instrument = instrument();
    let mut state = ArcusBookState::default();
    let now = UnixNanos::from(1);
    let snapshot = state
        .parse(
            &instrument,
            &book(
                "subscribed",
                100,
                json!([["50000.1", "1"]]),
                json!([["50000.2", "2"]]),
            ),
            now,
        )
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.deltas[0].action, BookAction::Clear);
    assert_ne!(snapshot.flags & RecordFlag::F_LAST as u8, 0);
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 105, json!([["50000.1", "3"]]), json!([])),
                now
            )
            .unwrap()
            .is_some()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 105, json!([]), json!([])),
                now
            )
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 106, json!([]), json!([])),
                now
            )
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 108, json!([]), json!([])),
                now
            )
            .is_err()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 107, json!([]), json!([])),
                now
            )
            .is_err()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("subscribed", 110, json!([]), json!([])),
                now
            )
            .unwrap()
            .is_some()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 120, json!([]), json!([])),
                now
            )
            .is_ok()
    );
}

#[rstest]
fn repeated_price_updates_preserve_last_write_and_zero_deletes() {
    let instrument = instrument();
    let mut state = ArcusBookState::default();
    let now = UnixNanos::from(1);
    let snapshot = state
        .parse(
            &instrument,
            &book(
                "subscribed",
                1,
                json!([["50000.1", "1"]]),
                json!([["50000.2", "2"]]),
            ),
            now,
        )
        .unwrap()
        .unwrap();
    let mut order_book = OrderBook::new(instrument.id(), BookType::L2_MBP);
    order_book.apply_deltas(&snapshot).unwrap();
    let update = state
        .parse(
            &instrument,
            &book(
                "channel_data",
                2,
                json!([["50000.1", "3"], ["50000.1", "4"]]),
                json!([["50000.2", "0"]]),
            ),
            now,
        )
        .unwrap()
        .unwrap();
    order_book.apply_deltas(&update).unwrap();
    assert_eq!(
        order_book.best_bid_size().unwrap().to_string(),
        "4.00000000"
    );
    assert!(order_book.best_ask_price().is_none());
    assert_eq!(update.deltas.last().unwrap().action, BookAction::Delete);
}

#[rstest]
fn delta_without_snapshot_is_rejected_and_malformed_snapshot_is_not_committed() {
    let instrument = instrument();
    let mut state = ArcusBookState::default();
    let now = UnixNanos::from(1);
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 1, json!([]), json!([])),
                now
            )
            .is_err()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("subscribed", 2, json!([["50000.11", "1"]]), json!([])),
                now
            )
            .is_err()
    );
    assert!(
        state
            .parse(
                &instrument,
                &book("channel_data", 3, json!([]), json!([])),
                now
            )
            .is_err()
    );
}

#[rstest]
fn bbo_sequence_jumps_are_normal_and_timestamps_are_microseconds() {
    let instrument = instrument();

    for sequence in [1_u64, 2, 4500] {
        let quote = parse_quote(&instrument, &json!({"bestBid":{"price":"50000.1","size":"1"},"bestAsk":{"price":"50000.2","size":"2"},"timestamp":1790000000000000_u64,"lastSequenceId":sequence}), UnixNanos::from(1)).unwrap();
        assert_eq!(quote.ts_event.as_u64(), 1790000000000000000);
        assert_eq!(quote.bid_price.to_string(), "50000.1");
    }
    assert!(
        parse_quote(
            &instrument,
            &json!({"bestBid":null,"bestAsk":null}),
            UnixNanos::from(1)
        )
        .is_err()
    );
}

#[rstest]
fn fills_retain_ids_and_exact_sizes_and_reject_invalid_identifiers() {
    let instrument = instrument();
    let fill = json!({"tradeId":"trade-789","price":"50000.1","size":"0.00000001","timestamp":1790000000000000_u64});
    let trades = parse_trades(&instrument, &json!([fill]), UnixNanos::from(1)).unwrap();
    assert_eq!(trades[0].trade_id.as_str(), "trade-789");
    assert_eq!(trades[0].size.to_string(), "0.00000001");
    let mut invalid = fill;
    invalid["tradeId"] = json!("\u{4e2d}\u{6587}");
    assert!(parse_trades(&instrument, &json!([invalid]), UnixNanos::from(1)).is_err());
}

#[tokio::test]
async fn live_data_client_loads_catalogue_publishes_and_clears_book_on_disconnect() {
    use axum::{Json, Router, routing::get};
    use futures_util::{SinkExt, StreamExt};
    use nautilus_arcus::{config::ArcusDataClientConfig, data::ArcusDataClient};
    use nautilus_common::{
        clients::DataClient,
        live::runner::set_data_event_sender,
        messages::{DataEvent, data::SubscribeBookDeltas},
    };
    use nautilus_core::UUID4;
    use nautilus_model::{
        data::Data,
        identifiers::{ClientId, InstrumentId},
    };
    use tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot},
        time::{Duration, timeout},
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let market: Value = serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url = format!("http://{}", http.local_addr().unwrap());
    let app = Router::new().route(
        "/v1/markets",
        get(move || {
            let market = market.clone();
            async move { Json(json!({"markets":[market]})) }
        }),
    );

    let http_task = tokio::spawn(async move {
        axum::serve(http, app).await.unwrap();
    });
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws.local_addr().unwrap());
    let (close_tx, close_rx) = oneshot::channel();

    let ws_task = tokio::spawn(async move {
        let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();

        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let subscription: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(subscription["channel"], "l2OrderbookUpdates");
                    assert_eq!(subscription["id"], "BTC-USD");
                    break;
                }
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
                _ => {}
            }
        }
        socket
            .send(Message::Text(
                book(
                    "subscribed",
                    100,
                    json!([["50000.1", "1"]]),
                    json!([["50000.2", "2"]]),
                )
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        close_rx.await.unwrap();
        socket.close(None).await.unwrap();
    });
    let (sender, mut events) = mpsc::unbounded_channel();
    set_data_event_sender(sender);
    let mut client = ArcusDataClient::new(
        ClientId::from("ARCUS"),
        ArcusDataClientConfig {
            base_url_http: Some(http_url),
            base_url_ws: Some(ws_url),
            ..Default::default()
        },
    )
    .unwrap();
    client.connect().await.unwrap();
    let id = InstrumentId::from("BTC-USD-PERP.ARCUS");
    let subscription = SubscribeBookDeltas::new(
        id,
        BookType::L2_MBP,
        Some(ClientId::from("ARCUS")),
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        false,
        None,
        None,
    );
    client.subscribe_book_deltas(subscription).unwrap();
    let mut baseline_seen = false;
    timeout(Duration::from_secs(2), async {
        loop {
            match events.recv().await.unwrap() {
                DataEvent::Data(Data::Deltas(deltas)) => {
                    assert_eq!(deltas.sequence, 100);
                    baseline_seen = true;
                }
                DataEvent::InstrumentStatus(status) if status.is_quoting == Some(true) => {
                    assert!(baseline_seen);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    close_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), async {
        let mut cleared = false;

        loop {
            match events.recv().await.unwrap() {
                DataEvent::Data(Data::Deltas(deltas)) if deltas.sequence == 0 => {
                    cleared = true;
                    assert_eq!(deltas.deltas[0].action, BookAction::Clear);
                }
                DataEvent::InstrumentStatus(status) if status.is_quoting == Some(false) => {
                    assert!(cleared);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(!client.is_connected());
    client.disconnect().await.unwrap();
    http_task.abort();
    let _ = http_task.await;
    ws_task.await.unwrap();
}
