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

//! Parsers from Ondo Perps WebSocket payloads to Nautilus domain types.
//!
//! This module is the only place where a wire lexeme becomes a Nautilus value. Two rules from the
//! protocol freeze are enforced here rather than at the call sites:
//!
//! - **Exactness.** A wire number is parsed with
//!   [`crate::common::parse::parse_decimal`], which rejects a lexeme carrying more significant
//!   digits than [`Decimal`] holds instead of rounding it away.
//! - **Named rounding.** The venue is free to quote finer than the increment it declared for a
//!   market, and Nautilus values carry the declared precision. When a conversion has to reduce
//!   scale, it rounds explicitly with `RoundingStrategy::MidpointAwayFromZero` - never truncates,
//!   never routes through `f64` - and the conversion reports that it rounded, so a caller can
//!   count it. See [`convert_price`] and [`convert_quantity`].
//!
//! A `depthBooksPerps` frame is a full replacement of the range it covers and carries no exchange
//! sequence number or checksum, so [`parse_book_snapshot`] emits one `CLEAR` + `ADD...` batch and
//! uses sequence `0`, the Nautilus convention for "the venue supplies no sequence".

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        BookOrder, FundingRateUpdate, MarkPriceUpdate, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick, depth::DEPTH10_LEN,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::TradeId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::RoundingStrategy;

use crate::{
    common::parse::{parse_decimal, parse_timestamp},
    websocket::messages::{
        BookSnapshotItem, FundingRateItem, MarkPriceItem, RawServerMessage, TradeItem, WsChannel,
        WsMessageType,
    },
};

/// The Nautilus sequence number used when the venue supplies none.
///
/// A `depthBooksPerps` frame carries no exchange sequence and no checksum, so there is nothing to
/// place in `OrderBookDelta::sequence`. Nautilus reads `0` as "no sequence" (the same convention
/// `OrderBookDeltas` uses for a venue without sequence numbers), and the adapter keeps its own
/// local `session_id`/`recv_seq` in [`crate::websocket::book::OndoBookState`] instead of
/// presenting a local counter as an exchange sequence.
pub const NO_EXCHANGE_SEQUENCE: u64 = 0;

/// A wire decimal converted into a Nautilus value, with the evidence of what happened to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Converted<T> {
    /// The converted value.
    pub value: T,
    /// The wire lexeme the value came from, kept verbatim.
    pub wire: String,
    /// Whether the conversion reduced scale (the wire value carried more fractional digits than
    /// the instrument declares) and therefore rounded.
    pub rounded: bool,
}

/// The source an event time was taken from.
///
/// A frame can carry an item-level price event time, an envelope send/batch time, or neither. The
/// source is reported rather than assumed, because "the venue told us when this happened" and "we
/// received it at this time" are different facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventTimeSource {
    /// The item-level `time` member: the price event time.
    ItemTime,
    /// The envelope `timestamp` member: the server send/batch time.
    EnvelopeTimestamp,
    /// The local receive time, because the frame carried no other usable time.
    LocalReceive,
}

/// A parsed server frame.
#[derive(Clone, Debug)]
pub struct ServerMessage {
    /// The classified message type.
    pub kind: WsMessageType,
    /// The wire message type, kept verbatim so an unknown type stays diagnosable.
    pub kind_raw: String,
    /// The raw channel name, as sent.
    pub channel: Option<String>,
    /// The server send/batch time, parsed exactly.
    pub timestamp: Option<UnixNanos>,
    /// The server send/batch time, verbatim.
    pub timestamp_raw: Option<String>,
    /// The raw `data` member.
    pub data: Option<serde_json::Value>,
    /// An error description, when the venue sent one.
    pub message: Option<String>,
    /// An error code, when the venue sent one.
    pub code: Option<String>,
}

/// One routed market item decoded from an `update` frame.
///
/// Every variant carries the item's own full `market` string, and nothing else: routing is by that
/// field and never by position or by a neighbouring item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsUpdate {
    /// A full book snapshot for one market.
    Book(BookSnapshotItem),
    /// A public trade for one market.
    Trade(TradeItem),
    /// A funding rate forecast for one market.
    FundingRate(FundingRateItem),
    /// A mark price for one market.
    MarkPrice(MarkPriceItem),
}

impl WsUpdate {
    /// Returns the venue market string this item routes by.
    #[must_use]
    pub fn market(&self) -> &str {
        match self {
            Self::Book(item) => &item.market,
            Self::Trade(item) => &item.market,
            Self::FundingRate(item) => &item.market,
            Self::MarkPrice(item) => &item.market,
        }
    }
}

/// One book level as the venue sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireLevel {
    /// The price lexeme.
    pub price: String,
    /// The quantity lexeme.
    pub size: String,
}

/// A full book snapshot as the venue sent it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WireBookSnapshot {
    /// The bid levels, in venue order.
    pub bids: Vec<WireLevel>,
    /// The ask levels, in venue order.
    pub asks: Vec<WireLevel>,
}

/// A parsed book snapshot: the atomic Nautilus batch plus the wire content it came from.
#[derive(Clone, Debug)]
pub struct ParsedBookSnapshot {
    /// One `CLEAR` + `ADD...` batch carrying `F_SNAPSHOT`, with `F_LAST` on the final delta.
    pub deltas: OrderBookDeltas,
    /// The wire levels, used for exact duplicate detection and for the covered-range cache.
    pub wire: WireBookSnapshot,
    /// How many levels needed a rounding conversion; `0` means the frame was represented exactly.
    pub rounded_levels: usize,
}

/// A parsed funding rate forecast.
#[derive(Clone, Debug)]
pub struct ParsedFundingRate {
    /// The Nautilus update. `rate` keeps the venue's hourly decimal fraction verbatim and
    /// `next_funding_ns` carries the settlement time.
    pub update: FundingRateUpdate,
    /// Where `ts_event` came from.
    pub event_time_source: EventTimeSource,
    /// How many premium samples the frame carried.
    ///
    /// The samples themselves stay raw strings in [`FundingRateItem`]: their `premiumIndex`, `bid`
    /// and `ask` lexemes carry up to 38 significant digits, which [`Decimal`] cannot hold, and no
    /// Nautilus data event needs them. Converting them would mean inventing or silently dropping
    /// precision, so they are counted and preserved rather than approximated.
    pub premium_samples: usize,
}

/// A parsed mark price.
#[derive(Clone, Debug)]
pub struct ParsedMarkPrice {
    /// The Nautilus update.
    pub update: MarkPriceUpdate,
    /// Where `ts_event` came from.
    pub event_time_source: EventTimeSource,
}

/// Parses a server frame into its classified envelope.
///
/// # Errors
///
/// Returns an error if the frame is not JSON for the envelope, or if it carries an envelope
/// `timestamp` that is not an RFC 3339 instant. A frame with no `timestamp` member is accepted:
/// the member is sent by the server but not declared by the official spec
/// (`test_data/conflicts.md` conflict 7).
pub fn parse_server_message(text: &str) -> anyhow::Result<ServerMessage> {
    let raw: RawServerMessage =
        serde_json::from_str(text).context("failed to decode an Ondo WebSocket frame")?;

    let (timestamp, timestamp_raw) = match raw.timestamp {
        Some(raw_timestamp) => {
            let parsed = parse_timestamp(&raw_timestamp).with_context(|| {
                format!("invalid envelope `timestamp` `{raw_timestamp}` in an Ondo frame")
            })?;

            (Some(parsed), Some(raw_timestamp))
        }
        None => (None, None),
    };

    let code = raw.code.as_ref().map(|code| match code {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    });

    Ok(ServerMessage {
        kind: WsMessageType::from_wire(&raw.kind),
        kind_raw: raw.kind,
        channel: raw.channel,
        timestamp,
        timestamp_raw,
        data: raw.data,
        message: raw.message,
        code,
    })
}

/// Decodes the `data` member of an `update` frame for one channel.
///
/// The server's `data` is an array and every item is routed by its own `market`. A member that is
/// not an array, or an item that does not decode as the channel's schema, is an error naming the
/// offending content rather than an item that is quietly dropped.
///
/// # Errors
///
/// Returns an error if `data` is absent, is not an array, or carries an item that does not decode.
pub fn decode_updates(
    channel: WsChannel,
    data: &serde_json::Value,
) -> anyhow::Result<Vec<WsUpdate>> {
    let items = data.as_array().with_context(|| {
        format!(
            "channel `{}` sent a `data` member that is not an array: {data}",
            channel.as_str()
        )
    })?;

    items
        .iter()
        .map(|item| {
            let decoded = match channel {
                WsChannel::TopOfBooksPerps | WsChannel::DepthBooksPerps => {
                    serde_json::from_value::<BookSnapshotItem>(item.clone()).map(WsUpdate::Book)
                }
                WsChannel::TradesPerps => {
                    serde_json::from_value::<TradeItem>(item.clone()).map(WsUpdate::Trade)
                }
                WsChannel::FundingRatesPerps => {
                    serde_json::from_value::<FundingRateItem>(item.clone())
                        .map(WsUpdate::FundingRate)
                }
                WsChannel::MarkPricesPerps => {
                    serde_json::from_value::<MarkPriceItem>(item.clone()).map(WsUpdate::MarkPrice)
                }
            };

            decoded.with_context(|| {
                format!(
                    "channel `{}` sent an item that does not decode as its schema: {item}",
                    channel.as_str()
                )
            })
        })
        .collect()
}

/// Converts a wire price lexeme into a [`Price`] at the instrument's declared price precision.
///
/// The lexeme is parsed exactly first. If it carries more fractional digits than the instrument
/// declares, the value is rounded to that precision with
/// `RoundingStrategy::MidpointAwayFromZero` (commercial rounding: a value exactly halfway between
/// two representable prices is rounded away from zero) and the conversion reports `rounded: true`.
/// Nothing is truncated and nothing goes through `f64`.
///
/// # Errors
///
/// Returns an error if the lexeme is not an exact decimal, if it is not representable at the
/// declared precision, or if it cannot be represented as a [`Price`].
pub fn convert_price(value: &str, field: &str, precision: u8) -> anyhow::Result<Converted<Price>> {
    let decimal = parse_decimal(value, field)?;
    // Rounding mode: MidpointAwayFromZero. Chosen because it is the mode NautilusTrader's own
    // fixed-point helpers use for a value that has to be reduced to a precision, so a rounded
    // price here matches what the rest of the engine would produce from the same input.
    let rounded_decimal = decimal
        .round_dp_with_strategy(u32::from(precision), RoundingStrategy::MidpointAwayFromZero);
    let rounded = rounded_decimal != decimal;

    let price = Price::from_decimal_dp(rounded_decimal, precision).with_context(|| {
        format!(
            "`{field}` value `{value}` is not representable as a Price at precision {precision}"
        )
    })?;

    Ok(Converted {
        value: price,
        wire: value.to_string(),
        rounded,
    })
}

/// Converts a wire quantity lexeme into a [`Quantity`] at the instrument's declared size precision.
///
/// The same exactness and rounding-mode rules as [`convert_price`] apply.
///
/// # Errors
///
/// Returns an error if the lexeme is not an exact decimal, if it is not representable at the
/// declared precision, or if it cannot be represented as a [`Quantity`].
pub fn convert_quantity(
    value: &str,
    field: &str,
    precision: u8,
) -> anyhow::Result<Converted<Quantity>> {
    let decimal = parse_decimal(value, field)?;
    // Rounding mode: MidpointAwayFromZero (see `convert_price`).
    let rounded_decimal = decimal
        .round_dp_with_strategy(u32::from(precision), RoundingStrategy::MidpointAwayFromZero);
    let rounded = rounded_decimal != decimal;

    let quantity = Quantity::from_decimal_dp(rounded_decimal, precision).with_context(|| {
        format!(
            "`{field}` value `{value}` is not representable as a Quantity at precision {precision}"
        )
    })?;

    Ok(Converted {
        value: quantity,
        wire: value.to_string(),
        rounded,
    })
}

/// Reads a book side into wire levels.
///
/// # Errors
///
/// Returns an error if a level does not carry exactly two entries, or if a level carries a
/// non-positive quantity (a book level with no size is not a level).
pub fn wire_levels(
    levels: &[Vec<String>],
    market: &str,
    field: &'static str,
) -> anyhow::Result<Vec<WireLevel>> {
    levels
        .iter()
        .map(|level| {
            let (price, size) = crate::websocket::messages::split_level(level, market, field)?;
            let size_decimal = parse_decimal(size, field).with_context(|| {
                format!("market `{market}` sent a non-decimal `{field}` size `{size}`")
            })?;
            ensure!(
                size_decimal.is_sign_positive() && !size_decimal.is_zero(),
                "market `{market}` sent a `{field}` level with size `{size}`"
            );

            Ok(WireLevel {
                price: price.to_string(),
                size: size.to_string(),
            })
        })
        .collect()
}

/// Converts a wire book snapshot into one atomic Nautilus delta batch.
///
/// The batch is built exactly as the venue's frame semantics require:
///
/// - every market frame **fully replaces** the range it covers, so the batch starts with one
///   [`BookAction::Clear`];
/// - every level is an [`BookAction::Add`] carrying [`RecordFlag::F_SNAPSHOT`];
/// - the final delta of the batch also carries [`RecordFlag::F_LAST`];
/// - an empty snapshot publishes the `CLEAR` alone, carrying the trailing flag, so both sides are
///   cleared and nothing stale survives;
/// - a one-sided snapshot is published as-is: because the batch starts with a `CLEAR`, the absent
///   side is emptied rather than kept;
/// - levels the venue did not send (a limited snapshot) simply do not appear, and the preceding
///   `CLEAR` is what stops them from lingering.
///
/// The sequence number on every delta is [`NO_EXCHANGE_SEQUENCE`].
///
/// # Errors
///
/// Returns an error if any level is malformed, or if a level cannot be represented at the
/// instrument's declared precision.
pub fn parse_book_snapshot(
    item: &BookSnapshotItem,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<ParsedBookSnapshot> {
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let bids = wire_levels(&item.bids, &item.market, "bids")?;
    let asks = wire_levels(&item.asks, &item.market, "asks")?;
    let total_levels = bids.len() + asks.len();

    let mut rounded_levels = 0_usize;
    let mut deltas = Vec::with_capacity(total_levels + 1);

    let mut clear = OrderBookDelta::clear(instrument_id, NO_EXCHANGE_SEQUENCE, ts_event, ts_init);
    if total_levels == 0 {
        clear.flags |= RecordFlag::F_LAST as u8;
    }
    deltas.push(clear);

    let mut processed = 0_usize;
    for level in bids.iter().chain(asks.iter()) {
        processed += 1;
        let side = if processed <= bids.len() {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let price = convert_price(&level.price, "book price", price_precision)?;
        let size = convert_quantity(&level.size, "book size", size_precision)?;
        if price.rounded || size.rounded {
            rounded_levels += 1;
        }

        let mut flags = RecordFlag::F_SNAPSHOT as u8;
        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        deltas.push(OrderBookDelta::new(
            instrument_id,
            BookAction::Add,
            BookOrder::new(side, price.value, size.value, 0),
            flags,
            NO_EXCHANGE_SEQUENCE,
            ts_event,
            ts_init,
        ));
    }

    let deltas = OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to build an OrderBookDeltas batch from an Ondo book snapshot")?;

    Ok(ParsedBookSnapshot {
        deltas,
        wire: WireBookSnapshot { bids, asks },
        rounded_levels,
    })
}

/// Builds a top-of-book quote from a book snapshot's best levels.
///
/// A quote needs both sides, so a snapshot with an empty side cannot be expressed as a
/// [`QuoteTick`]; the caller is told by [`None`] rather than being given a zero-filled quote.
///
/// # Errors
///
/// Returns an error if a level cannot be represented at the instrument's declared precision, or if
/// the venue sent a crossed book (the engine validates the quote it is given).
pub fn parse_quote_tick(
    wire: &WireBookSnapshot,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let (Some(bid), Some(ask)) = (wire.bids.first(), wire.asks.first()) else {
        return Ok(None);
    };

    let bid_price = convert_price(&bid.price, "bid price", instrument.price_precision())?;
    let ask_price = convert_price(&ask.price, "ask price", instrument.price_precision())?;
    let bid_size = convert_quantity(&bid.size, "bid size", instrument.size_precision())?;
    let ask_size = convert_quantity(&ask.size, "ask size", instrument.size_precision())?;

    let quote = QuoteTick::new_checked(
        instrument.id(),
        bid_price.value,
        ask_price.value,
        bid_size.value,
        ask_size.value,
        ts_event,
        ts_init,
    )
    .context("failed to build a QuoteTick from an Ondo book snapshot")?;

    Ok(Some(quote))
}

/// Builds a Nautilus [`OrderBookDepth10`] from an accepted book's levels.
///
/// This is a projection of the book the venue streamed on the book channel - the same levels the
/// `OrderBookDeltas` batch carries - so an on-demand depth10 request never opens a second
/// subscription. Levels beyond the tenth are dropped and the remaining slots carry zeroed orders
/// with zero counts, which is how Nautilus represents an unfilled depth slot.
///
/// # Errors
///
/// Returns an error if a level cannot be represented at the instrument's declared precision.
pub fn parse_depth10(
    wire: &WireBookSnapshot,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let mut bids = [BookOrder::default(); DEPTH10_LEN];
    let mut asks = [BookOrder::default(); DEPTH10_LEN];
    let mut bid_counts = [0_u32; DEPTH10_LEN];
    let mut ask_counts = [0_u32; DEPTH10_LEN];

    for (idx, level) in wire.bids.iter().take(DEPTH10_LEN).enumerate() {
        let price = convert_price(&level.price, "bid price", price_precision)?;
        let size = convert_quantity(&level.size, "bid size", size_precision)?;
        bids[idx] = BookOrder::new(OrderSide::Buy, price.value, size.value, 0);
        bid_counts[idx] = 1;
    }

    for (idx, level) in wire.asks.iter().take(DEPTH10_LEN).enumerate() {
        let price = convert_price(&level.price, "ask price", price_precision)?;
        let size = convert_quantity(&level.size, "ask size", size_precision)?;
        asks[idx] = BookOrder::new(OrderSide::Sell, price.value, size.value, 0);
        ask_counts[idx] = 1;
    }

    for order in bids.iter_mut().skip(wire.bids.len().min(DEPTH10_LEN)) {
        *order = BookOrder::new(
            OrderSide::Buy,
            Price::zero(price_precision),
            Quantity::zero(size_precision),
            0,
        );
    }

    for order in asks.iter_mut().skip(wire.asks.len().min(DEPTH10_LEN)) {
        *order = BookOrder::new(
            OrderSide::Sell,
            Price::zero(price_precision),
            Quantity::zero(size_precision),
            0,
        );
    }

    Ok(OrderBookDepth10::new(
        instrument_id,
        bids,
        asks,
        bid_counts,
        ask_counts,
        (RecordFlag::F_SNAPSHOT as u8) | (RecordFlag::F_LAST as u8),
        NO_EXCHANGE_SEQUENCE,
        ts_event,
        ts_init,
    ))
}

/// Parses a trade item into a Nautilus [`TradeTick`].
///
/// `item.time` is the price event time and becomes `ts_event`, so the envelope `timestamp` can
/// never take its place.
///
/// # Errors
///
/// Returns an error if `time` is absent or malformed, if the price or size cannot be represented,
/// if the aggressor side is not `buy` or `sell`, or if the trade carries no venue id (a trade
/// without its venue identity cannot be deduplicated, and inventing one would double-count it).
pub fn parse_trade_tick(
    item: &TradeItem,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let time = item.time.as_deref().with_context(|| {
        format!(
            "trade for market `{}` carries no `time`, which is the price event time",
            item.market
        )
    })?;
    let ts_event = parse_timestamp(time)
        .with_context(|| format!("invalid trade `time` `{time}` for market `{}`", item.market))?;

    let aggressor_side = match item.aggressor_side.as_str() {
        "buy" => AggressorSide::Buy,
        "sell" => AggressorSide::Sell,
        other => anyhow::bail!(
            "trade for market `{}` reports an unknown `aggressor_side` `{other}`",
            item.market
        ),
    };

    let trade_id_raw = item
        .id
        .as_deref()
        .with_context(|| format!("trade for market `{}` carries no venue `id`", item.market))?;
    let trade_id = TradeId::new_checked(trade_id_raw)
        .with_context(|| format!("invalid trade `id` `{trade_id_raw}`"))?;

    let price = convert_price(&item.price, "trade price", instrument.price_precision())?;
    let size = convert_quantity(&item.size, "trade size", instrument.size_precision())?;

    TradeTick::new_checked(
        instrument.id(),
        price.value,
        size.value,
        aggressor_side,
        trade_id,
        ts_event,
        ts_init,
    )
    .with_context(|| {
        format!(
            "failed to build a TradeTick for market `{}` from `{}` x `{}`",
            item.market, item.price, item.size
        )
    })
}

/// Parses a funding rate forecast into a Nautilus [`FundingRateUpdate`].
///
/// Two venue times must not be confused:
///
/// - `rate` is the **hourly decimal fraction** the venue sent (`0.0000063` = 0.063 bp/h), kept
///   verbatim: no division by 100 and no multiplication by 8;
/// - `intervalEnds` is a **settlement time**. It maps onto `next_funding_ns` and is never written
///   into `ts_event`, because the settlement interval has not happened yet.
///
/// A funding frame carries no item-level event time, so `ts_event` comes from the envelope
/// server send time when one is present and otherwise from the local receive time; which of the
/// two was used is reported in [`ParsedFundingRate::event_time_source`].
///
/// # Errors
///
/// Returns an error if `rate` or `intervalEnds` is not an exact decimal/timestamp.
pub fn parse_funding_rate(
    item: &FundingRateItem,
    instrument: &InstrumentAny,
    envelope_ts: Option<UnixNanos>,
    ts_init: UnixNanos,
) -> anyhow::Result<ParsedFundingRate> {
    let rate = parse_decimal(&item.rate, "rate").with_context(|| {
        format!(
            "invalid funding `rate` `{}` for market `{}`",
            item.rate, item.market
        )
    })?;

    let next_funding_ns = match item.interval_ends.as_deref() {
        Some(interval_ends) => Some(parse_timestamp(interval_ends).with_context(|| {
            format!(
                "invalid funding `intervalEnds` `{interval_ends}` for market `{}`",
                item.market
            )
        })?),
        None => None,
    };

    let ts_event = match envelope_ts {
        Some(envelope_ts) => envelope_ts,
        None => ts_init,
    };
    let event_time_source = if envelope_ts.is_some() {
        EventTimeSource::EnvelopeTimestamp
    } else {
        EventTimeSource::LocalReceive
    };

    Ok(ParsedFundingRate {
        update: FundingRateUpdate::new(
            instrument.id(),
            rate,
            None,
            next_funding_ns,
            ts_event,
            ts_init,
        ),
        event_time_source,
        premium_samples: item.premiums.len(),
    })
}

/// Parses a mark price into a Nautilus [`MarkPriceUpdate`].
///
/// A mark price frame has no price event time of its own when the venue omits `time` (the official
/// example carries only `market` and `markPrice`), so the event time falls back to the envelope
/// server send time, then to the local receive time; the source is always reported.
///
/// # Errors
///
/// Returns an error if `markPrice` cannot be represented at the instrument's price precision, or if
/// a `time` member is present but malformed.
pub fn parse_mark_price(
    item: &MarkPriceItem,
    instrument: &InstrumentAny,
    envelope_ts: Option<UnixNanos>,
    ts_init: UnixNanos,
) -> anyhow::Result<ParsedMarkPrice> {
    let price = convert_price(&item.mark_price, "markPrice", instrument.price_precision())?;

    let (ts_event, event_time_source) = match item.time.as_deref() {
        Some(time) => (
            parse_timestamp(time).with_context(|| {
                format!(
                    "invalid mark price `time` `{time}` for market `{}`",
                    item.market
                )
            })?,
            EventTimeSource::ItemTime,
        ),
        None => match envelope_ts {
            Some(envelope_ts) => (envelope_ts, EventTimeSource::EnvelopeTimestamp),
            None => (ts_init, EventTimeSource::LocalReceive),
        },
    };

    Ok(ParsedMarkPrice {
        update: MarkPriceUpdate::new(instrument.id(), price.value, ts_event, ts_init),
        event_time_source,
    })
}

/// Parses a wire market string only for its exact text, rejecting nothing else.
///
/// This is the identity projection used where a market string must be passed back to the venue
/// unchanged (for example in an unsubscribe request); it exists so a round trip never goes through
/// the Nautilus product marker.
///
/// # Errors
///
/// Returns an error if the string is empty.
pub fn market_wire_text(market: &str) -> anyhow::Result<String> {
    ensure!(!market.is_empty(), "market string is empty");

    Ok(market.to_string())
}

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::Instrument;
    use rstest::rstest;

    use super::*;
    use crate::{common::parse::market_to_instrument_id, http::models::parse_instruments};

    fn ts(value: &str) -> UnixNanos {
        parse_timestamp(value).unwrap()
    }

    /// Builds a test instrument through the shared metadata boundary rather than a second model:
    /// the increments come from the P0 `GET /v1/markets` fixture, so a test can never disagree
    /// with the production precision path.
    fn instrument(market: &str) -> InstrumentAny {
        let body = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/rest/markets_synthetic.json"
        ))
        .unwrap();
        let instrument_id = market_to_instrument_id(market).unwrap();

        parse_instruments(&body, &[instrument_id], UnixNanos::from(1))
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    fn book_item(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> BookSnapshotItem {
        BookSnapshotItem {
            market: "NVDA-USD.P".to_string(),
            time: Some("2026-09-14T11:09:59.570112122Z".to_string()),
            bids: bids
                .iter()
                .map(|(p, s)| vec![p.to_string(), s.to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| vec![p.to_string(), s.to_string()])
                .collect(),
            depth_levels: None,
        }
    }

    #[rstest]
    fn test_convert_price_reports_rounding_and_keeps_scale() {
        let exact = convert_price("212.25", "book price", 2).unwrap();

        assert!(!exact.rounded);
        assert_eq!(exact.wire, "212.25");
        assert_eq!(exact.value.precision, 2);

        let rounded = convert_price("212.2549", "book price", 2).unwrap();

        assert!(rounded.rounded, "a reduced-scale conversion is reported");
        assert_eq!(rounded.value.to_string(), "212.25");

        let midpoint = convert_price("212.255", "book price", 2).unwrap();

        assert!(midpoint.rounded);
        assert_eq!(
            midpoint.value.to_string(),
            "212.26",
            "MidpointAwayFromZero rounds the exact midpoint up in magnitude"
        );
    }

    #[rstest]
    fn test_convert_price_rejects_a_lexeme_wider_than_decimal() {
        let error =
            convert_price("212.22994961526383480047124429696391461", "book price", 2).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("212.22994961526383480047124429696391461")
        );
    }

    #[rstest]
    fn test_parse_book_snapshot_is_one_clear_plus_add_batch() {
        let instrument = instrument("NVDA-USD.P");
        let item = book_item(
            &[("100.00", "1.00"), ("99.00", "2.00")],
            &[("101.00", "1.00")],
        );
        let parsed = parse_book_snapshot(
            &item,
            &instrument,
            ts("2026-09-14T11:09:59.570112122Z"),
            UnixNanos::from(7),
        )
        .unwrap();

        assert_eq!(parsed.rounded_levels, 0);
        assert_eq!(parsed.deltas.deltas.len(), 4, "one CLEAR plus three levels");
        assert_eq!(parsed.deltas.deltas[0].action, BookAction::Clear);
        assert_eq!(parsed.deltas.sequence, NO_EXCHANGE_SEQUENCE);
        assert_eq!(parsed.deltas.deltas[1].action, BookAction::Add);
        assert_eq!(parsed.deltas.deltas[1].order.side, Some(OrderSide::Buy));
        assert_eq!(parsed.deltas.deltas[3].order.side, Some(OrderSide::Sell));
        assert_eq!(
            parsed.deltas.deltas[3].flags & RecordFlag::F_LAST as u8,
            RecordFlag::F_LAST as u8
        );
        for delta in &parsed.deltas.deltas {
            assert_eq!(
                delta.flags & RecordFlag::F_SNAPSHOT as u8,
                RecordFlag::F_SNAPSHOT as u8,
                "every delta of a book snapshot carries F_SNAPSHOT"
            );
        }
        assert_eq!(
            instrument.id(),
            market_to_instrument_id("NVDA-USD.P").unwrap()
        );
    }

    #[rstest]
    fn test_parse_book_snapshot_clears_an_empty_snapshot_with_the_trailing_flag() {
        let instrument = instrument("NVDA-USD.P");
        let item = book_item(&[], &[]);
        let parsed = parse_book_snapshot(
            &item,
            &instrument,
            ts("2026-09-14T11:09:59Z"),
            UnixNanos::from(7),
        )
        .unwrap();

        assert_eq!(parsed.deltas.deltas.len(), 1, "CLEAR only");
        assert_eq!(parsed.deltas.deltas[0].action, BookAction::Clear);
        assert_eq!(
            parsed.deltas.deltas[0].flags,
            (RecordFlag::F_SNAPSHOT as u8) | (RecordFlag::F_LAST as u8)
        );
        assert!(parsed.wire.bids.is_empty());
        assert!(parsed.wire.asks.is_empty());
    }

    #[rstest]
    fn test_parse_quote_tick_needs_both_sides() {
        let instrument = instrument("NVDA-USD.P");
        let two_sided = WireBookSnapshot {
            bids: vec![WireLevel {
                price: "100.00".to_string(),
                size: "1.00".to_string(),
            }],
            asks: vec![WireLevel {
                price: "101.00".to_string(),
                size: "2.00".to_string(),
            }],
        };
        let quote = parse_quote_tick(
            &two_sided,
            &instrument,
            UnixNanos::from(1),
            UnixNanos::from(2),
        )
        .unwrap()
        .expect("two-sided snapshot yields a quote");

        assert_eq!(quote.bid_price.to_string(), "100.00");
        assert_eq!(quote.ask_size.to_string(), "2.00");

        let one_sided = WireBookSnapshot {
            bids: two_sided.bids.clone(),
            asks: Vec::new(),
        };

        assert!(
            parse_quote_tick(
                &one_sided,
                &instrument,
                UnixNanos::from(1),
                UnixNanos::from(2)
            )
            .unwrap()
            .is_none()
        );
    }

    #[rstest]
    fn test_parse_depth10_projects_the_same_levels() {
        let instrument = instrument("NVDA-USD.P");
        let wire = WireBookSnapshot {
            bids: (0..12)
                .map(|i| WireLevel {
                    price: format!("{}.00", 100 - i),
                    size: "1.00".to_string(),
                })
                .collect(),
            asks: vec![WireLevel {
                price: "101.00".to_string(),
                size: "1.00".to_string(),
            }],
        };
        let depth10 =
            parse_depth10(&wire, &instrument, UnixNanos::from(1), UnixNanos::from(2)).unwrap();

        assert_eq!(depth10.bids[0].price.to_string(), "100.00");
        assert_eq!(depth10.bids[9].price.to_string(), "91.00");
        assert_eq!(depth10.bid_counts[9], 1);
        assert_eq!(depth10.asks[0].price.to_string(), "101.00");
        assert_eq!(depth10.ask_counts[1], 0);
        assert_eq!(
            depth10.flags,
            (RecordFlag::F_SNAPSHOT as u8) | (RecordFlag::F_LAST as u8)
        );
        assert_eq!(depth10.sequence, NO_EXCHANGE_SEQUENCE);
    }

    #[rstest]
    fn test_wire_levels_reject_a_non_positive_size() {
        let level = vec!["100.00".to_string(), "0".to_string()];
        let error = wire_levels(&[level], "NVDA-USD.P", "bids").unwrap_err();

        assert!(error.to_string().contains("size `0`"));
    }

    #[rstest]
    fn test_wire_levels_reject_a_malformed_level() {
        let level = vec![
            "100.00".to_string(),
            "1.00".to_string(),
            "extra".to_string(),
        ];
        let error = wire_levels(&[level], "NVDA-USD.P", "bids").unwrap_err();

        assert!(error.to_string().contains("entries"));
    }

    #[rstest]
    fn test_market_wire_text_round_trips_the_venue_string() {
        assert_eq!(market_wire_text("NVDA-USD.P").unwrap(), "NVDA-USD.P");
        assert!(market_wire_text("").is_err());
    }
}
