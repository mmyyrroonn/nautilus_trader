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

//! Bounded public stream decoding, without connection ownership or cached quote freshness.

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{MarkPriceUpdate, QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;
use thiserror::Error;

use crate::{
    instruments::BackpackInstrumentMetadata,
    parsing::{
        BackpackInstrumentError, decimal, exact_price, exact_quantity, unix_microseconds_to_nanos,
    },
};

/// Maximum public frame size accepted before JSON allocation.
pub const MAX_PUBLIC_FRAME_BYTES: usize = 1_048_576;
/// Maximum aggregate ask and bid level count accepted in one depth message.
pub const MAX_DEPTH_FRAME_LEVELS: usize = 2_000;

/// One decoded public event. This type conveys observations, not ongoing freshness.
#[derive(Debug)]
pub enum BackpackPublicEvent {
    /// Complete two-sided quote, using engine event time.
    Quote(QuoteTick),
    /// A valid empty-sided quote; no usable two-sided quote can be manufactured.
    QuoteUnavailable {
        /// Exact venue update ID.
        update_id: u64,
        /// The available bid side, if any.
        bid: Option<(Price, Quantity)>,
        /// The available ask side, if any.
        ask: Option<(Price, Quantity)>,
        /// Engine event timestamp.
        ts_event: UnixNanos,
    },
    /// A deduplicated per-symbol public trade.
    Trade(TradeTick),
    /// Mark price plus a raw funding estimate whose fractional unit remains unverified.
    Mark {
        /// Exact mark value; theoretical prices need not lie on the executable price grid.
        price: MarkPriceUpdate,
        /// Funding estimate, separately from historical rates or settled payments.
        funding: BackpackFundingEstimate,
    },
    /// A validated bounded absolute-quantity depth update.
    Depth(BackpackDepthUpdate),
    /// An already observed or older quote/trade update within the same generation.
    Duplicate,
}

/// A raw funding estimate with verified time units, without an inferred rate denominator.
#[derive(Clone, Debug)]
pub struct BackpackFundingEstimate {
    /// Exact venue `f` value; fraction/percent/bps is not established.
    pub raw_rate: Decimal,
    /// Next funding time, from the documented millisecond `n` field.
    pub next_funding_ns: UnixNanos,
    /// Metadata interval in milliseconds, or unknown.
    pub interval_ms: Option<u64>,
}

/// An immutable validated depth message for one instrument and session generation.
#[derive(Clone, Debug)]
pub struct BackpackDepthUpdate {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) generation: u64,
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) asks: Vec<(Price, Quantity)>,
    pub(crate) bids: Vec<(Price, Quantity)>,
    pub(crate) ts_event: UnixNanos,
    pub(crate) ts_init: UnixNanos,
}

/// Public decoder scoped to an already validated instrument and one connection generation.
#[derive(Debug)]
pub struct BackpackPublicStreamParser {
    metadata: BackpackInstrumentMetadata,
    generation: u64,
    last_quote: Option<u64>,
    last_trade: Option<u64>,
}

impl BackpackPublicStreamParser {
    /// Creates a decoder with empty generation-local quote and trade watermarks.
    #[must_use]
    pub fn new(metadata: BackpackInstrumentMetadata, generation: u64) -> Self {
        Self {
            metadata,
            generation,
            last_quote: None,
            last_trade: None,
        }
    }

    /// Decodes one correctly enveloped public frame for the selected instrument.
    ///
    /// # Errors
    ///
    /// Returns an error for old-generation delivery, unsupported topics, mismatched identity,
    /// malformed/oversized data, invalid grids, crossed quotes, or checked timestamp overflow.
    /// The transport owner must invalidate associated freshness/book state on such errors.
    pub fn decode(
        &mut self,
        generation: u64,
        frame: &[u8],
        received_at: UnixNanos,
    ) -> Result<BackpackPublicEvent, BackpackPublicError> {
        if generation != self.generation {
            return Err(BackpackPublicError::Generation);
        }

        if frame.len() > MAX_PUBLIC_FRAME_BYTES {
            return Err(BackpackPublicError::Bound);
        }

        let envelope: Envelope = serde_json::from_slice(frame)?;
        let data: Header = serde_json::from_str(envelope.data.get())?;
        let symbol = self.metadata.raw_symbol.as_str();
        let expected = format!("{}.{symbol}", data.event);
        let valid_topic = envelope.stream == expected
            || (data.event == "depth"
                && ["200ms", "600ms", "1000ms"]
                    .iter()
                    .any(|period| envelope.stream == format!("depth.{period}.{symbol}")));

        if !valid_topic || data.symbol != symbol {
            return Err(BackpackPublicError::Identity);
        }

        unix_microseconds_to_nanos(data.event_time)?;
        let event_time = unix_microseconds_to_nanos(data.engine_time)?;
        let id = self.metadata.instrument_id;

        match data.event.as_str() {
            "bookTicker" => {
                let value: Quote = serde_json::from_str(envelope.data.get())?;
                let update_id = value.update_id.to_u64()?;
                let bid = quote_side(&value.bid, &value.bid_size, &self.metadata)?;
                let ask = quote_side(&value.ask, &value.ask_size, &self.metadata)?;

                if let (Some((bid, _)), Some((ask, _))) = (bid, ask)
                    && bid >= ask
                {
                    return Err(BackpackPublicError::Field("crossed or locked quote"));
                }

                if self
                    .last_quote
                    .is_some_and(|previous| update_id <= previous)
                {
                    return Ok(BackpackPublicEvent::Duplicate);
                }

                let event = match (bid, ask) {
                    (Some((bid, bid_size)), Some((ask, ask_size))) => BackpackPublicEvent::Quote(
                        QuoteTick::new_checked(
                            id,
                            bid,
                            ask,
                            bid_size,
                            ask_size,
                            event_time,
                            received_at,
                        )
                        .map_err(|e| BackpackPublicError::Domain(e.to_string()))?,
                    ),
                    (bid, ask) => BackpackPublicEvent::QuoteUnavailable {
                        update_id,
                        bid,
                        ask,
                        ts_event: event_time,
                    },
                };
                self.last_quote = Some(update_id);
                Ok(event)
            }
            "trade" => {
                let value: Trade = serde_json::from_str(envelope.data.get())?;
                let price = grid_price(&value.price, &self.metadata)?;
                let size = grid_quantity(&value.quantity, &self.metadata, false)?;
                let trade_id = TradeId::new_checked(value.trade_id.to_string())
                    .map_err(|e| BackpackPublicError::Domain(e.to_string()))?;

                if self
                    .last_trade
                    .is_some_and(|previous| value.trade_id <= previous)
                {
                    return Ok(BackpackPublicEvent::Duplicate);
                }

                let tick = TradeTick::new_checked(
                    id,
                    price,
                    size,
                    if value.buyer_maker {
                        AggressorSide::Sell
                    } else {
                        AggressorSide::Buy
                    },
                    trade_id,
                    event_time,
                    received_at,
                )
                .map_err(|e| BackpackPublicError::Domain(e.to_string()))?;
                self.last_trade = Some(value.trade_id);
                Ok(BackpackPublicEvent::Trade(tick))
            }
            "markPrice" => {
                let value: Mark = serde_json::from_str(envelope.data.get())?;
                let mark = decimal(&value.price, "mark price")?;

                if mark <= Decimal::ZERO {
                    return Err(BackpackPublicError::Field("mark price"));
                }

                let price = exact_price(mark, mark.normalize().scale() as u8)?;
                let next_funding_ns = value
                    .next_funding
                    .checked_mul(1_000_000)
                    .map(UnixNanos::from)
                    .ok_or(BackpackPublicError::Field(
                        "next funding timestamp overflow",
                    ))?;
                let raw_rate = decimal(&value.funding, "estimated funding rate")?;
                Ok(BackpackPublicEvent::Mark {
                    price: MarkPriceUpdate::new(id, price, event_time, received_at),
                    funding: BackpackFundingEstimate {
                        raw_rate,
                        next_funding_ns,
                        interval_ms: self.metadata.funding_interval_ms,
                    },
                })
            }
            "depth" => {
                let value: Depth = serde_json::from_str(envelope.data.get())?;

                if value.first > value.last {
                    return Err(BackpackPublicError::Field("depth sequence range"));
                }

                if value.asks.len().saturating_add(value.bids.len()) > MAX_DEPTH_FRAME_LEVELS {
                    return Err(BackpackPublicError::Bound);
                }

                Ok(BackpackPublicEvent::Depth(BackpackDepthUpdate {
                    instrument_id: id,
                    generation,
                    first: value.first,
                    last: value.last,
                    asks: parse_levels(&value.asks, &self.metadata, true)?,
                    bids: parse_levels(&value.bids, &self.metadata, true)?,
                    ts_event: event_time,
                    ts_init: received_at,
                }))
            }
            _ => Err(BackpackPublicError::UnsupportedTopic),
        }
    }
}

/// A public protocol failure requiring explicit state/freshness handling by the caller.
#[derive(Debug, Error)]
pub enum BackpackPublicError {
    /// A frame or snapshot belongs to another generation.
    #[error("Backpack public session generation mismatch")]
    Generation,
    /// Topic, payload symbol, or instrument identity disagrees.
    #[error("Backpack public topic or instrument identity mismatch")]
    Identity,
    /// This decoder does not implement the selected event.
    #[error("unsupported Backpack public topic")]
    UnsupportedTopic,
    /// A bounded protocol resource was exceeded.
    #[error("Backpack public protocol resource bound exceeded")]
    Bound,
    /// Invalid wire field.
    #[error("invalid Backpack public field: {0}")]
    Field(&'static str),
    /// Invalid JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Exact metadata/domain parsing failed.
    #[error(transparent)]
    Metadata(#[from] BackpackInstrumentError),
    /// Domain construction failed.
    #[error("Backpack public domain construction failed: {0}")]
    Domain(String),
    /// Depth continuity was lost.
    #[error("Backpack depth continuity lost")]
    Gap,
    /// A stale synchronizer must start a new bootstrap before accepting data.
    #[error("Backpack depth is stale and requires a new bootstrap")]
    Stale,
}

#[derive(Deserialize)]
struct Envelope {
    stream: String,
    data: Box<RawValue>,
}
#[derive(Deserialize)]
struct Header {
    #[serde(rename = "e")]
    event: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T")]
    engine_time: u64,
}
#[derive(Deserialize)]
struct Quote {
    #[serde(rename = "a", deserialize_with = "required_optional_string")]
    ask: Option<String>,
    #[serde(rename = "A", deserialize_with = "required_optional_string")]
    ask_size: Option<String>,
    #[serde(rename = "b", deserialize_with = "required_optional_string")]
    bid: Option<String>,
    #[serde(rename = "B", deserialize_with = "required_optional_string")]
    bid_size: Option<String>,
    #[serde(rename = "u")]
    update_id: WireSequence,
}
#[derive(Deserialize)]
struct Trade {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "m")]
    buyer_maker: bool,
}
#[derive(Deserialize)]
struct Mark {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "f")]
    funding: String,
    #[serde(rename = "n")]
    next_funding: u64,
}
#[derive(Deserialize)]
struct Depth {
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "U")]
    first: u64,
    #[serde(rename = "u")]
    last: u64,
}

pub(crate) fn parse_sequence(value: &str) -> Result<u64, BackpackPublicError> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(BackpackPublicError::Field("sequence ID"));
    }

    value
        .parse()
        .map_err(|_| BackpackPublicError::Field("sequence ID overflow"))
}

fn quote_side(
    price: &Option<String>,
    size: &Option<String>,
    metadata: &BackpackInstrumentMetadata,
) -> Result<Option<(Price, Quantity)>, BackpackPublicError> {
    match (price, size) {
        (None, None) => Ok(None),
        (Some(price), Some(size)) => Ok(Some((
            grid_price(price, metadata)?,
            grid_quantity(size, metadata, false)?,
        ))),
        _ => Err(BackpackPublicError::Field("unpaired quote side")),
    }
}

pub(crate) fn parse_levels(
    values: &[[String; 2]],
    metadata: &BackpackInstrumentMetadata,
    allow_zero: bool,
) -> Result<Vec<(Price, Quantity)>, BackpackPublicError> {
    let mut prices = std::collections::BTreeSet::new();
    let mut levels = Vec::with_capacity(values.len());

    for [price, quantity] in values {
        let price = grid_price(price, metadata)?;

        if !prices.insert(price) {
            return Err(BackpackPublicError::Field("duplicate depth price"));
        }

        levels.push((price, grid_quantity(quantity, metadata, allow_zero)?));
    }

    Ok(levels)
}

fn grid_price(
    value: &str,
    metadata: &BackpackInstrumentMetadata,
) -> Result<Price, BackpackPublicError> {
    let value = decimal(value, "public price")?;

    if value <= Decimal::ZERO
        || value.checked_rem(metadata.price_increment.as_decimal()) != Some(Decimal::ZERO)
    {
        return Err(BackpackPublicError::Field("price grid"));
    }

    exact_price(value, metadata.price_increment.precision).map_err(Into::into)
}

fn grid_quantity(
    value: &str,
    metadata: &BackpackInstrumentMetadata,
    allow_zero: bool,
) -> Result<Quantity, BackpackPublicError> {
    let value = decimal(value, "public quantity")?;

    if value < Decimal::ZERO
        || (!allow_zero && value == Decimal::ZERO)
        || value.checked_rem(metadata.size_increment.as_decimal()) != Some(Decimal::ZERO)
    {
        return Err(BackpackPublicError::Field("quantity grid"));
    }

    exact_quantity(value, metadata.size_increment.precision).map_err(Into::into)
}

fn required_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireSequence {
    Text(String),
    Integer(u64),
}

impl WireSequence {
    fn to_u64(&self) -> Result<u64, BackpackPublicError> {
        match self {
            Self::Text(value) => parse_sequence(value),
            Self::Integer(value) => Ok(*value),
        }
    }
}
