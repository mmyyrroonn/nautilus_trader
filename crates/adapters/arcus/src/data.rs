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

//! Arcus public market data, with snapshot-gated incremental books.

use std::{
    collections::HashMap,
    mem,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{
        dst::task::{self, JoinHandle},
        get_runtime,
        runner::get_data_event_sender,
    },
    messages::{DataEvent, data::*},
};
use nautilus_core::{
    UnixNanos, datetime::try_datetime_to_unix_nanos, time::get_atomic_clock_realtime,
};
use nautilus_model::{
    data::{
        BookOrder, Data, InstrumentStatus, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, BookType, MarketStatusAction, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use ustr::Ustr;

use crate::{
    config::ArcusDataClientConfig,
    consts::ARCUS_VENUE,
    http::ArcusHttpClient,
    instruments::parse_instrument,
    market_data::{ReferenceDecoder, ReferenceKind, ReferenceSubscriptions},
    websocket::{ArcusWebSocket, ArcusWsEvent},
};

/// Live Arcus data client.
#[derive(Debug)]
pub struct ArcusDataClient {
    client_id: ClientId,
    config: ArcusDataClientConfig,
    http: ArcusHttpClient,
    instruments: HashMap<InstrumentId, InstrumentAny>,
    socket: Option<Arc<ArcusWebSocket>>,
    publisher: Option<JoinHandle<()>>,
    connected: Arc<AtomicBool>,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    reference_subscriptions: Arc<Mutex<ReferenceSubscriptions>>,
    pending_requests: Mutex<Vec<JoinHandle<()>>>,
}

impl ArcusDataClient {
    /// Creates a public, unauthenticated data client.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or HTTP client construction.
    pub fn new(client_id: ClientId, config: ArcusDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let http =
            ArcusHttpClient::new(config.resolved_http_url(), config.http_timeout_secs, None)?;
        Ok(Self {
            client_id,
            config,
            http,
            instruments: HashMap::new(),
            socket: None,
            publisher: None,
            connected: Arc::new(AtomicBool::new(false)),
            sender: get_data_event_sender(),
            reference_subscriptions: Arc::new(Mutex::new(ReferenceSubscriptions::default())),
            pending_requests: Mutex::new(Vec::new()),
        })
    }

    fn shutdown(&mut self) {
        self.connected.store(false, Ordering::Release);
        self.reference_subscriptions.lock().clear();

        for request in self.pending_requests.get_mut().drain(..) {
            request.abort();
        }

        if let Some(socket) = self.socket.take() {
            socket.abort();
        }

        if let Some(publisher) = self.publisher.take() {
            publisher.abort();
        }
        invalidate(&self.sender, self.instruments.keys().copied());
    }

    fn reference_subscription(
        &self,
        instrument_id: InstrumentId,
        kind: ReferenceKind,
        active: bool,
    ) -> anyhow::Result<()> {
        let instrument = self
            .instruments
            .get(&instrument_id)
            .context("Arcus instrument has not been loaded")?;
        let socket = self
            .socket
            .as_ref()
            .context("Arcus data client is disconnected")?;
        self.reference_subscriptions.lock().set(
            instrument_id,
            instrument.raw_symbol().as_str(),
            kind,
            active,
            socket,
        )
    }

    fn subscription(
        &self,
        instrument_id: InstrumentId,
        channel: &str,
        subscribe: bool,
        depth: Option<usize>,
    ) -> anyhow::Result<()> {
        let instrument = self
            .instruments
            .get(&instrument_id)
            .context("Arcus instrument has not been loaded")?;
        let socket = self
            .socket
            .as_ref()
            .context("Arcus data client is disconnected")?;
        let mut subscription = json!({"channel":channel,"id":instrument.raw_symbol().as_str()});

        if let Some(depth) = depth {
            subscription["nLevels"] = depth.into();
        }

        if subscribe {
            socket.try_subscribe(subscription)
        } else {
            socket.try_unsubscribe(subscription)
        }
    }
}

impl Drop for ArcusDataClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[async_trait(?Send)]
impl DataClient for ArcusDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }
    fn venue(&self) -> Option<Venue> {
        Some(*ARCUS_VENUE)
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.shutdown();
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.shutdown();
        self.instruments.clear();
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.reset()
    }
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
            && self
                .socket
                .as_ref()
                .is_some_and(|socket| socket.connected().load(Ordering::Acquire))
    }
    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.socket.is_some() {
            return Ok(());
        }
        let now = get_atomic_clock_realtime().get_time_ns();
        let mut instruments = HashMap::new();

        for market in self.http.markets().await? {
            if market.market_type != "PERPETUAL" {
                continue;
            }
            let instrument = parse_instrument(&market, now)?;

            if self.config.load_ids.is_empty() || self.config.load_ids.contains(&instrument.id()) {
                instruments.insert(instrument.id(), instrument);
            }
        }

        for id in &self.config.load_ids {
            anyhow::ensure!(
                instruments.contains_key(id),
                "Arcus catalogue does not contain {id}"
            );
        }
        let (socket, mut events) = ArcusWebSocket::connect(
            self.config.resolved_ws_url(),
            self.config.queue_capacity,
            Duration::from_secs(self.config.ws_idle_timeout_secs),
        )
        .await?;

        for instrument in instruments.values() {
            self.sender
                .send(DataEvent::Instrument(instrument.clone()))
                .context("Data engine receiver closed")?;
        }
        let socket = Arc::new(socket);
        let worker_socket = socket.clone();
        let stream_epoch = socket.epoch();
        let sender = self.sender.clone();
        let reference_subscriptions = self.reference_subscriptions.clone();
        let connected = self.connected.clone();
        let by_symbol: HashMap<String, InstrumentAny> = instruments
            .values()
            .map(|instrument| (instrument.raw_symbol().to_string(), instrument.clone()))
            .collect();
        self.instruments = instruments;
        self.connected.store(true, Ordering::Release);

        self.publisher = Some(task::spawn(async move {
            let mut streams = MarketStreams::default();
            let mut references = ReferenceDecoder::default();

            while let Some(event) = events.recv().await {
                match event {
                    ArcusWsEvent::Connected => {
                        connected.store(true, Ordering::Release);
                        streams.clear();
                        references.clear();
                    }
                    ArcusWsEvent::Disconnected => {
                        connected.store(false, Ordering::Release);
                        streams.clear();
                        references.clear();
                        invalidate(&sender, by_symbol.values().map(InstrumentAny::id));
                    }
                    ArcusWsEvent::Error(e) => log::warn!("Arcus data transport: {e}"),
                    ArcusWsEvent::Ready => {}
                    ArcusWsEvent::Message { epoch, value } => {
                        let result = if epoch == stream_epoch.load(Ordering::Acquire)
                            && matches!(
                                value["channel"].as_str(),
                                Some("oraclePrices" | "predictedFunding")
                            ) {
                            references
                                .decode(
                                    &by_symbol,
                                    &value,
                                    &reference_subscriptions.lock(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                )
                                .and_then(|events| {
                                    for event in events {
                                        sender
                                            .send(event)
                                            .context("Data engine receiver closed")?;
                                    }
                                    Ok(true)
                                })
                        } else {
                            process_market_message(
                                &sender,
                                &by_symbol,
                                &value,
                                &mut streams,
                                epoch,
                                &stream_epoch,
                            )
                        };

                        if matches!(result, Ok(false)) {
                            connected.store(false, Ordering::Release);
                            references.clear();
                            continue;
                        }

                        if let Err(e) = result {
                            log::warn!("Arcus market data invalidated: {e}");
                            connected.store(false, Ordering::Release);
                            streams.clear();
                            references.clear();
                            invalidate(&sender, by_symbol.values().map(InstrumentAny::id));

                            if worker_socket.reconnect().is_err() {
                                worker_socket.abort();
                                break;
                            }
                        }
                    }
                }
            }
            connected.store(false, Ordering::Release);
        }));
        self.socket = Some(socket);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected.store(false, Ordering::Release);
        self.reference_subscriptions.lock().clear();
        let requests = mem::take(self.pending_requests.get_mut());

        for request in &requests {
            request.abort();
        }

        for request in requests {
            let _ = request.await;
        }

        if let Some(publisher) = self.publisher.take() {
            publisher.abort();
            let _ = publisher.await;
        }

        if let Some(socket) = self.socket.take() {
            socket.abort();

            if let Ok(mut socket) = Arc::try_unwrap(socket) {
                socket.close().await;
            }
        }
        invalidate(&self.sender, self.instruments.keys().copied());
        Ok(())
    }

    fn subscribe_instruments(&mut self, _: SubscribeInstruments) -> anyhow::Result<()> {
        for instrument in self.instruments.values() {
            self.sender
                .send(DataEvent::Instrument(instrument.clone()))
                .context("Data engine receiver closed")?;
        }
        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        let instrument = self
            .instruments
            .get(&cmd.instrument_id)
            .context("Arcus instrument has not been loaded")?;
        self.sender
            .send(DataEvent::Instrument(instrument.clone()))
            .context("Data engine receiver closed")
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.subscription(cmd.instrument_id, "bbo", true, None)
    }
    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.subscription(cmd.instrument_id, "bbo", false, None)
    }
    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.subscription(cmd.instrument_id, "trades", true, None)
    }
    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.subscription(cmd.instrument_id, "trades", false, None)
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "Arcus supports L2_MBP books only"
        );
        let depth = cmd.depth.map_or(100, |depth| depth.get());
        anyhow::ensure!(
            (1..=100).contains(&depth),
            "Arcus book depth must be between 1 and 100"
        );
        self.subscription(cmd.instrument_id, "l2OrderbookUpdates", true, Some(depth))
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.subscription(cmd.instrument_id, "l2OrderbookUpdates", false, None)
    }

    fn request_instrument(&self, cmd: RequestInstrument) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.start.is_none() && cmd.end.is_none(),
            "Arcus historical instrument metadata is unavailable"
        );
        let instrument = self
            .instruments
            .get(&cmd.instrument_id)
            .context("Arcus instrument has not been loaded")?;
        self.sender
            .send(DataEvent::Response(DataResponse::Instrument(Box::new(
                InstrumentResponse::new(
                    cmd.request_id,
                    self.client_id,
                    cmd.instrument_id,
                    instrument.clone(),
                    None,
                    None,
                    get_atomic_clock_realtime().get_time_ns(),
                    cmd.params,
                ),
            ))))
            .context("Data engine receiver closed")
    }

    fn request_instruments(&self, cmd: RequestInstruments) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.start.is_none() && cmd.end.is_none(),
            "Arcus historical instrument metadata is unavailable"
        );
        self.sender
            .send(DataEvent::Response(DataResponse::Instruments(
                InstrumentsResponse::new(
                    cmd.request_id,
                    self.client_id,
                    *ARCUS_VENUE,
                    self.instruments.values().cloned().collect(),
                    None,
                    None,
                    get_atomic_clock_realtime().get_time_ns(),
                    cmd.params,
                ),
            )))
            .context("Data engine receiver closed")
    }

    fn subscribe_book_depth10(&mut self, _: SubscribeBookDepth10) -> anyhow::Result<()> {
        anyhow::bail!("Arcus depth10 subscription is unsupported; subscribe to L2 book deltas")
    }
    fn subscribe_bars(&mut self, _: SubscribeBars) -> anyhow::Result<()> {
        anyhow::bail!("Arcus bar subscription is unsupported")
    }
    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Mark, true)
    }
    fn unsubscribe_mark_prices(&mut self, cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Mark, false)
    }
    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Index, true)
    }
    fn unsubscribe_index_prices(&mut self, cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Index, false)
    }
    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Funding, true)
    }
    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.reference_subscription(cmd.instrument_id, ReferenceKind::Funding, false)
    }
    fn subscribe(&mut self, _: SubscribeCustomData) -> anyhow::Result<()> {
        anyhow::bail!("Arcus custom data subscription is unsupported")
    }
    fn request_data(&self, _: RequestCustomData) -> anyhow::Result<()> {
        anyhow::bail!("Arcus custom data requests are unsupported")
    }
    fn request_book_snapshot(&self, _: RequestBookSnapshot) -> anyhow::Result<()> {
        anyhow::bail!("Arcus book requests are unsupported; subscribe to L2 book deltas")
    }
    fn request_book_deltas(&self, _: RequestBookDeltas) -> anyhow::Result<()> {
        anyhow::bail!("Arcus historical book deltas are unsupported")
    }
    fn request_book_depth(&self, _: RequestBookDepth) -> anyhow::Result<()> {
        anyhow::bail!("Arcus historical book depth is unsupported")
    }
    fn request_funding_rates(&self, cmd: RequestFundingRates) -> anyhow::Result<()> {
        anyhow::ensure!(self.socket.is_some(), "Arcus data client is disconnected");
        let instrument = self
            .instruments
            .get(&cmd.instrument_id)
            .context("Arcus instrument has not been loaded")?
            .clone();
        let start = cmd.start.map(try_datetime_to_unix_nanos).transpose()?;
        let end = cmd.end.map(try_datetime_to_unix_nanos).transpose()?;
        anyhow::ensure!(
            start.zip(end).is_none_or(|(start, end)| start <= end),
            "Arcus funding history start exceeds end"
        );
        let from_us =
            start.map(|value| value.as_u64() / 1000 + u64::from(value.as_u64() % 1000 != 0));
        let to_us = end.map(|value| value.as_u64() / 1000);
        let empty_window = from_us.zip(to_us).is_some_and(|(from, to)| from > to);
        let upper = if empty_window {
            None
        } else {
            Some(ArcusHttpClient::validate_funding_rates_query(
                instrument.raw_symbol().as_str(),
                from_us,
                to_us,
                cmd.limit.map(|limit| limit.get()),
            )?)
        };
        let request_id = cmd.request_id;
        let mut pending = self.pending_requests.lock();
        pending.retain(|task| !task.is_finished());
        anyhow::ensure!(
            pending.len() < self.config.queue_capacity,
            "Arcus historical request queue is full"
        );
        let http = self.http.clone();
        let sender = self.sender.clone();
        let client_id = self.client_id;

        pending.push(get_runtime().spawn(async move {
            let result: anyhow::Result<()> = async {
                let rows = if empty_window {
                    Vec::new()
                } else {
                    http.funding_rates(
                        instrument.raw_symbol().as_str(),
                        from_us,
                        upper,
                        cmd.limit.map(|limit| limit.get()),
                    )
                    .await?
                };
                let now = get_atomic_clock_realtime().get_time_ns();
                let mut data = rows
                    .iter()
                    .map(|row| crate::market_data::funding_history_update(&instrument, row))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                data.retain(|update| {
                    start.is_none_or(|start| update.ts_event >= start)
                        && end.is_none_or(|end| update.ts_event <= end)
                });
                sender
                    .send(DataEvent::Response(DataResponse::FundingRates(
                        FundingRatesResponse::new(
                            cmd.request_id,
                            client_id,
                            cmd.instrument_id,
                            data,
                            start,
                            end,
                            now,
                            cmd.params,
                        ),
                    )))
                    .context("Data engine receiver closed")?;
                Ok(())
            }
            .await;

            if let Err(e) = result {
                log::error!("Arcus funding history request {request_id} failed: {e}");
            }
        }));
        Ok(())
    }
    fn request_quotes(&self, _: RequestQuotes) -> anyhow::Result<()> {
        anyhow::bail!("Arcus historical quotes are unsupported")
    }
    fn request_trades(&self, _: RequestTrades) -> anyhow::Result<()> {
        anyhow::bail!("Arcus historical trades are unsupported")
    }
    fn request_bars(&self, _: RequestBars) -> anyhow::Result<()> {
        anyhow::bail!("Arcus historical bars are unsupported")
    }
}

/// Sequence state for one incremental L2 subscription.
#[derive(Debug, Default)]
pub struct ArcusBookState {
    sequence: Option<u64>,
    applying_deltas: bool,
}

impl ArcusBookState {
    /// Converts a snapshot or contiguous delta, preserving array order.
    ///
    /// # Errors
    ///
    /// Returns an error on malformed data, missing baseline, or a mid-stream gap.
    pub fn parse(
        &mut self,
        instrument: &InstrumentAny,
        message: &Value,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderBookDeltas>> {
        let result = self.parse_inner(instrument, message, ts_init);

        if result.is_err() {
            self.sequence = None;
            self.applying_deltas = false;
        }
        result
    }

    fn parse_inner(
        &mut self,
        instrument: &InstrumentAny,
        message: &Value,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderBookDeltas>> {
        let contents = &message["contents"];
        let sequence = contents["lastSequenceId"]
            .as_u64()
            .context("Arcus book sequence missing")?;
        let snapshot = message["type"] == "subscribed";

        if !snapshot {
            let previous = self
                .sequence
                .context("Arcus book delta received before snapshot")?;

            if sequence <= previous {
                return Ok(None);
            }
            anyhow::ensure!(
                !self.applying_deltas || previous.checked_add(1) == Some(sequence),
                "Arcus book sequence gap: {previous} to {sequence}"
            );
        }
        let ts_event = event_time(contents, ts_init)?;
        let mut deltas = Vec::new();

        if snapshot {
            deltas.push(OrderBookDelta::clear(
                instrument.id(),
                sequence,
                ts_event,
                ts_init,
            ));
        }

        for (field, side) in [("bids", OrderSide::Buy), ("asks", OrderSide::Sell)] {
            for level in contents[field]
                .as_array()
                .context("Arcus book levels missing")?
            {
                let price = price(&level[0], instrument)?;
                let size = quantity(&level[1], instrument)?;
                let action = if size.is_zero() {
                    BookAction::Delete
                } else {
                    BookAction::Update
                };
                deltas.push(OrderBookDelta::new_checked(
                    instrument.id(),
                    action,
                    BookOrder::new(side, price, size, 0),
                    if snapshot {
                        RecordFlag::F_SNAPSHOT as u8
                    } else {
                        0
                    },
                    sequence,
                    ts_event,
                    ts_init,
                )?);
            }
        }
        self.sequence = Some(sequence);
        self.applying_deltas = !snapshot;

        if let Some(last) = deltas.last_mut() {
            last.flags |= RecordFlag::F_LAST as u8;
        } else {
            return Ok(None);
        }
        Ok(Some(OrderBookDeltas::new_checked(instrument.id(), deltas)?))
    }
}

/// Converts a complete Arcus top-of-book frame without gap-checking its sparse sequence.
///
/// # Errors
///
/// Returns an error if either side is absent or malformed.
pub fn parse_quote(
    instrument: &InstrumentAny,
    contents: &Value,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    QuoteTick::new_checked(
        instrument.id(),
        price(&contents["bestBid"]["price"], instrument)?,
        price(&contents["bestAsk"]["price"], instrument)?,
        quantity(&contents["bestBid"]["size"], instrument)?,
        quantity(&contents["bestAsk"]["size"], instrument)?,
        event_time(contents, ts_init)?,
        ts_init,
    )
}

/// Converts public fills, retaining venue trade IDs and exact prices and quantities.
///
/// # Errors
///
/// Returns an error on malformed fills or invalid numeric values.
pub fn parse_trades(
    instrument: &InstrumentAny,
    contents: &Value,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<TradeTick>> {
    contents
        .as_array()
        .context("Arcus trades must be an array")?
        .iter()
        .map(|fill| {
            let side = match fill["takerSide"].as_str() {
                Some("BUY") => AggressorSide::Buy,
                Some("SELL") => AggressorSide::Sell,
                _ => AggressorSide::NoAggressor,
            };
            let id = fill["tradeId"].as_str().context("Arcus trade ID missing")?;
            anyhow::ensure!(
                !id.is_empty() && id.len() <= 36,
                "Arcus trade ID has invalid length"
            );
            TradeTick::new_checked(
                instrument.id(),
                price(&fill["price"], instrument)?,
                quantity(&fill["size"], instrument)?,
                side,
                TradeId::new_checked(id)?,
                event_time(fill, ts_init)?,
                ts_init,
            )
        })
        .collect()
}

fn price(value: &Value, instrument: &InstrumentAny) -> anyhow::Result<Price> {
    let decimal = Decimal::from_str(
        value
            .as_str()
            .context("Arcus price must be a decimal string")?,
    )?;
    anyhow::ensure!(decimal > Decimal::ZERO, "Arcus price must be positive");
    let price = Price::from_decimal_dp(decimal, instrument.price_precision())?;
    anyhow::ensure!(
        price.as_decimal() == decimal,
        "Arcus price exceeds instrument precision"
    );
    Ok(price)
}

fn quantity(value: &Value, instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let decimal = Decimal::from_str(
        value
            .as_str()
            .context("Arcus quantity must be a decimal string")?,
    )?;
    anyhow::ensure!(
        decimal >= Decimal::ZERO,
        "Arcus quantity must be nonnegative"
    );
    let quantity = Quantity::from_decimal_dp(decimal, instrument.size_precision())?;
    anyhow::ensure!(
        quantity.as_decimal() == decimal,
        "Arcus quantity exceeds instrument precision"
    );
    Ok(quantity)
}

fn event_time(contents: &Value, fallback: UnixNanos) -> anyhow::Result<UnixNanos> {
    match contents.get("timestamp") {
        Some(value) => Ok(UnixNanos::from(
            value
                .as_u64()
                .context("Arcus timestamp must be integer microseconds")?
                .checked_mul(1_000)
                .context("Arcus timestamp overflow")?,
        )),
        None => Ok(fallback),
    }
}

#[derive(Default)]
struct MarketStreams {
    books: HashMap<InstrumentId, ArcusBookState>,
    quote_ready: HashMap<InstrumentId, bool>,
}

impl MarketStreams {
    fn clear(&mut self) {
        self.books.clear();
        self.quote_ready.clear();
    }
}

fn process_market_message(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &HashMap<String, InstrumentAny>,
    message: &Value,
    streams: &mut MarketStreams,
    event_epoch: u64,
    current_epoch: &AtomicU64,
) -> anyhow::Result<bool> {
    if event_epoch != current_epoch.load(Ordering::Acquire) {
        streams.clear();
        invalidate(sender, instruments.values().map(InstrumentAny::id));
        return Ok(false);
    }

    if let Some(instrument) = message["id"].as_str().and_then(|id| instruments.get(id)) {
        publish_message(sender, instrument, message, streams)?;
    }
    Ok(true)
}

fn publish_message(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instrument: &InstrumentAny,
    message: &Value,
    streams: &mut MarketStreams,
) -> anyhow::Result<()> {
    let now = get_atomic_clock_realtime().get_time_ns();

    if message["type"] == "degraded" {
        streams.quote_ready.insert(instrument.id(), false);

        if message["channel"] == "bbo" {
            sender.send(quote_status(instrument.id(), false, now))?;
            return Ok(());
        }
        streams.books.remove(&instrument.id());
        invalidate(sender, std::iter::once(instrument.id()));
        return Ok(());
    }

    if message["type"] == "unsubscribed" && message["channel"] == "l2OrderbookUpdates" {
        streams.books.remove(&instrument.id());
        streams.quote_ready.insert(instrument.id(), false);
        invalidate(sender, std::iter::once(instrument.id()));
        return Ok(());
    }

    if !matches!(
        message["type"].as_str(),
        Some("subscribed" | "channel_data")
    ) {
        return Ok(());
    }

    match message["channel"].as_str() {
        Some("bbo") => {
            if message["contents"]
                .get("bestBid")
                .is_some_and(Value::is_null)
                || message["contents"]
                    .get("bestAsk")
                    .is_some_and(Value::is_null)
            {
                if streams.quote_ready.insert(instrument.id(), false) != Some(false) {
                    sender.send(quote_status(instrument.id(), false, now))?;
                }
                return Ok(());
            }
            sender.send(DataEvent::Data(Data::Quote(parse_quote(
                instrument,
                &message["contents"],
                now,
            )?)))?;

            if streams.quote_ready.insert(instrument.id(), true) != Some(true) {
                sender.send(quote_status(instrument.id(), true, now))?;
            }
        }
        Some("trades") => {
            if message["type"] == "subscribed" && !message["contents"].is_array() {
                return Ok(());
            }

            for trade in parse_trades(instrument, &message["contents"], now)? {
                sender.send(DataEvent::Data(Data::Trade(trade)))?;
            }
        }
        Some("l2OrderbookUpdates" | "l2Orderbook") => {
            if let Some(deltas) = streams
                .books
                .entry(instrument.id())
                .or_default()
                .parse(instrument, message, now)?
            {
                sender.send(DataEvent::Data(Data::Deltas(Box::new(deltas))))?;

                if message["type"] == "subscribed"
                    && streams.quote_ready.get(&instrument.id()) != Some(&false)
                {
                    sender.send(feed_status(instrument.id(), true, now))?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn quote_status(id: InstrumentId, ready: bool, now: UnixNanos) -> DataEvent {
    stream_status(
        id,
        ready,
        now,
        if ready {
            "adapter:quote_ready"
        } else {
            "adapter:quote_unavailable"
        },
    )
}

fn feed_status(id: InstrumentId, ready: bool, now: UnixNanos) -> DataEvent {
    stream_status(
        id,
        ready,
        now,
        if ready {
            "adapter:snapshot_ready"
        } else {
            "adapter:disconnected"
        },
    )
}

fn stream_status(id: InstrumentId, ready: bool, now: UnixNanos, reason: &str) -> DataEvent {
    DataEvent::InstrumentStatus(InstrumentStatus::new(
        id,
        MarketStatusAction::None,
        now,
        now,
        Some(Ustr::from(reason)),
        None,
        None,
        Some(ready),
        None,
    ))
}

fn invalidate(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ids: impl Iterator<Item = InstrumentId>,
) {
    let now = get_atomic_clock_realtime().get_time_ns();

    for id in ids {
        let mut clear = OrderBookDelta::clear(id, 0, now, now);
        clear.flags |= RecordFlag::F_LAST as u8;
        let _ = sender.send(DataEvent::Data(Data::Deltas(Box::new(
            OrderBookDeltas::new(id, vec![clear]),
        ))));
        let _ = sender.send(feed_status(id, false, now));
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::models::ArcusMarket;

    #[rstest]
    fn queued_old_epoch_quotes_are_not_published_after_overflow_or_recovery() {
        let market: ArcusMarket =
            serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
        let instrument = parse_instrument(&market, UnixNanos::from(1)).unwrap();
        let instruments = HashMap::from([("BTC-USD".to_owned(), instrument)]);
        let (sender, mut published) = tokio::sync::mpsc::unbounded_channel();
        let (queued, mut receiver) = tokio::sync::mpsc::channel(2);
        let quote = json!({"type":"channel_data","channel":"bbo","id":"BTC-USD","contents":{"bestBid":{"price":"50000.1","size":"1"},"bestAsk":{"price":"50000.2","size":"2"}}});
        queued.try_send((1, quote.clone())).unwrap();
        queued.try_send((1, quote.clone())).unwrap();
        let current_epoch = AtomicU64::new(2);
        let mut streams = MarketStreams::default();

        while let Ok((epoch, value)) = receiver.try_recv() {
            assert!(
                !process_market_message(
                    &sender,
                    &instruments,
                    &value,
                    &mut streams,
                    epoch,
                    &current_epoch
                )
                .unwrap()
            );
        }

        while let Ok(event) = published.try_recv() {
            assert!(!matches!(event, DataEvent::Data(Data::Quote(_))));
        }
        assert!(
            process_market_message(
                &sender,
                &instruments,
                &quote,
                &mut streams,
                2,
                &current_epoch
            )
            .unwrap()
        );
        assert!(matches!(
            published.try_recv().unwrap(),
            DataEvent::Data(Data::Quote(_))
        ));
    }

    #[rstest]
    #[case(true, false)]
    #[case(false, true)]
    #[case(true, true)]
    fn empty_bbo_invalidates_only_quotes_and_recovers_without_zero_prices(
        #[case] null_bid: bool,
        #[case] null_ask: bool,
    ) {
        let market: ArcusMarket =
            serde_json::from_str(include_str!("../test_data/market.json")).unwrap();
        let instrument = parse_instrument(&market, UnixNanos::from(1)).unwrap();
        let id = instrument.id();
        let (sender, mut published) = tokio::sync::mpsc::unbounded_channel();
        let mut streams = MarketStreams::default();
        let quote = json!({"type":"channel_data","channel":"bbo","id":"BTC-USD","contents":{"bestBid":{"price":"50000.1","size":"1"},"bestAsk":{"price":"50000.2","size":"2"}}});
        publish_message(&sender, &instrument, &quote, &mut streams).unwrap();
        assert!(matches!(
            published.try_recv().unwrap(),
            DataEvent::Data(Data::Quote(_))
        ));
        let DataEvent::InstrumentStatus(ready) = published.try_recv().unwrap() else {
            panic!("Expected quote readiness")
        };
        assert_eq!(ready.is_quoting, Some(true));
        let mut empty = quote.clone();

        if null_bid {
            empty["contents"]["bestBid"] = Value::Null;
        }

        if null_ask {
            empty["contents"]["bestAsk"] = Value::Null;
        }
        publish_message(&sender, &instrument, &empty, &mut streams).unwrap();
        let DataEvent::InstrumentStatus(unavailable) = published.try_recv().unwrap() else {
            panic!("Empty BBO must publish only unavailability")
        };
        assert_eq!(unavailable.is_quoting, Some(false));
        assert_eq!(
            unavailable.reason.unwrap().as_str(),
            "adapter:quote_unavailable"
        );
        assert!(published.try_recv().is_err());
        let snapshot = json!({"type":"subscribed","channel":"l2OrderbookUpdates","id":"BTC-USD","contents":{"bids":[],"asks":[],"lastSequenceId":1}});
        publish_message(&sender, &instrument, &snapshot, &mut streams).unwrap();
        assert!(matches!(
            published.try_recv().unwrap(),
            DataEvent::Data(Data::Deltas(_))
        ));
        assert!(
            published.try_recv().is_err(),
            "A book snapshot must not override empty quote readiness"
        );
        assert!(!streams.quote_ready[&id]);
        publish_message(&sender, &instrument, &quote, &mut streams).unwrap();
        let DataEvent::Data(Data::Quote(recovered)) = published.try_recv().unwrap() else {
            panic!("Expected recovered quote")
        };
        let DataEvent::InstrumentStatus(ready) = published.try_recv().unwrap() else {
            panic!("Expected recovered readiness")
        };
        assert_eq!(ready.is_quoting, Some(true));
        assert_eq!(ready.ts_init, recovered.ts_init);
        assert!(streams.quote_ready[&id]);
    }
}
