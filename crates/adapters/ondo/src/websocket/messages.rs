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

//! WebSocket message schema for the Ondo Perps public channels.
//!
//! The field paths modelled here are the ones recorded for this venue in
//! `crates/adapters/ondo/test_data/README.md` (protocol table) and exercised by the fixtures in
//! `crates/adapters/ondo/test_data/ws/`. Read every fixture's `kind` from
//! `test_data/manifest.json`, never from its file name.
//!
//! Three members of an update frame carry a time, and they are three different times:
//!
//! - `data[].time` is the **price event time** of an item, and is what a Nautilus `ts_event` is
//!   built from (see [`BookSnapshotItem::time`] and [`TradeItem::time`]);
//! - the envelope `timestamp` is the **server send/batch time**, wired through as
//!   [`RawServerMessage::timestamp`] so it can never be mistaken for an item's event time;
//! - the local receive time is not on the wire at all and is supplied by the transport through
//!   `ts_init`.
//!
//! `depthLevels` is a **price grouping** (the official spec example is `"0.01"`), never a level
//! count, so it is modelled as an optional decimal string and omitted by default. `limit` is a
//! maximum level count, so `limit=100` means "at most 100 levels", never "the whole book".

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A public Ondo Perps WebSocket channel used by the data client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WsChannel {
    /// `depthBooksPerps` - full-depth book snapshots (a complete replacement per frame).
    #[serde(rename = "depthBooksPerps")]
    DepthBooksPerps,
    /// `topOfBooksPerps` - best bid and offer.
    #[serde(rename = "topOfBooksPerps")]
    TopOfBooksPerps,
    /// `tradesPerps` - public trades.
    #[serde(rename = "tradesPerps")]
    TradesPerps,
    /// `fundingRatesPerps` - the hourly funding rate forecast.
    #[serde(rename = "fundingRatesPerps")]
    FundingRatesPerps,
    /// `markPricesPerps` - mark prices.
    #[serde(rename = "markPricesPerps")]
    MarkPricesPerps,
}

impl WsChannel {
    /// Every public channel the data client subscribes to, on one connection.
    pub const ALL: [Self; 5] = [
        Self::DepthBooksPerps,
        Self::TopOfBooksPerps,
        Self::TradesPerps,
        Self::FundingRatesPerps,
        Self::MarkPricesPerps,
    ];

    /// Returns the exact wire channel name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DepthBooksPerps => "depthBooksPerps",
            Self::TopOfBooksPerps => "topOfBooksPerps",
            Self::TradesPerps => "tradesPerps",
            Self::FundingRatesPerps => "fundingRatesPerps",
            Self::MarkPricesPerps => "markPricesPerps",
        }
    }

    /// Classifies a wire channel name.
    ///
    /// Returns [`None`] for a channel this adapter does not implement; the caller surfaces that as
    /// a diagnosable outcome rather than silently ignoring the frame.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == value)
    }

    /// Returns `true` for the channel whose frames fully replace the covered book range.
    #[must_use]
    pub const fn is_book(self) -> bool {
        matches!(self, Self::DepthBooksPerps)
    }
}

/// A client-to-server operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WsOp {
    /// Subscribe to one channel for a set of markets.
    Subscribe,
    /// Unsubscribe from one channel for a set of markets.
    Unsubscribe,
    /// The application-level heartbeat. The protocol-level ping is not sufficient: the venue
    /// idles a connection out after 180 s without an application-level request.
    Ping,
}

impl WsOp {
    /// Returns the exact wire operation name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Unsubscribe => "unsubscribe",
            Self::Ping => "ping",
        }
    }
}

/// A server-to-client message type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WsMessageType {
    /// A response to the application-level ping.
    Pong,
    /// A successful login acknowledgement (private channels only; not used by this client).
    LoggedIn,
    /// A subscription acknowledgement.
    Subscribed,
    /// An unsubscription acknowledgement.
    Unsubscribed,
    /// A market data update.
    Update,
    /// A venue-reported error.
    Error,
    /// A message type this adapter does not know.
    Unknown,
}

impl WsMessageType {
    /// Classifies a wire message type.
    #[must_use]
    pub fn from_wire(raw: &str) -> Self {
        match raw {
            "pong" => Self::Pong,
            "loggedIn" => Self::LoggedIn,
            "subscribed" => Self::Subscribed,
            "unsubscribed" => Self::Unsubscribed,
            "update" => Self::Update,
            "error" => Self::Error,
            _ => Self::Unknown,
        }
    }

    /// Returns the exact wire message type name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pong => "pong",
            Self::LoggedIn => "loggedIn",
            Self::Subscribed => "subscribed",
            Self::Unsubscribed => "unsubscribed",
            Self::Update => "update",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }
}

/// The wire form of a server message, before per-channel decoding.
///
/// Unknown members are ignored, which is what keeps the observed (and undeclared) envelope
/// `timestamp` from failing the decode: `test_data/conflicts.md` conflict 7 records that the
/// server sends it while the official spec does not declare it.
#[derive(Clone, Debug, Deserialize)]
pub struct RawServerMessage {
    /// The wire message type, kept verbatim because [`WsMessageType`] collapses unknown values.
    #[serde(rename = "type")]
    pub kind: String,
    /// The raw channel name, as sent.
    #[serde(default)]
    pub channel: Option<String>,
    /// The server send/batch time, raw. Never an item event time.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// The `data` member, decoded per channel.
    #[serde(default)]
    pub data: Option<Value>,
    /// An error description, when the venue sends one.
    #[serde(default)]
    pub message: Option<String>,
    /// An error code, when the venue sends one.
    #[serde(default)]
    pub code: Option<Value>,
}

/// One item of an `update` frame for the `topOfBooksPerps` or `depthBooksPerps` channel.
///
/// The venue routes every item by its **full** `market` string, so a frame may carry several
/// markets and one item is never assumed to belong to the market of another.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookSnapshotItem {
    /// The venue market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The price event time of this snapshot.
    #[serde(default)]
    pub time: Option<String>,
    /// The bid levels, each `[price, quantity]`.
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    /// The ask levels, each `[price, quantity]`.
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    /// The price grouping the venue applied, when it reports one.
    #[serde(default)]
    pub depth_levels: Option<String>,
}

/// One item of an `update` frame for the `tradesPerps` channel.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct TradeItem {
    /// The venue market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The trade price in quote USD per base unit.
    pub price: String,
    /// The trade size in base units.
    pub size: String,
    /// The trade cost in quote USD (`price` x `size` as sent by the venue).
    #[serde(default)]
    pub cost: Option<String>,
    /// The aggressor side, `buy` or `sell` (snake_case on the wire).
    pub aggressor_side: String,
    /// The price event time of this trade.
    #[serde(default)]
    pub time: Option<String>,
    /// The venue trade id, usable for dedupe.
    #[serde(default)]
    pub id: Option<String>,
}

/// One per-minute premium sample of a [`FundingRateItem`].
///
/// The samples are kept as raw strings only. Their `premiumIndex`, `bid` and `ask` lexemes carry
/// up to 38 significant digits (see `test_data/ws/funding_observed.json`), which
/// [`crate::common::parse::parse_decimal`] rejects rather than rounds, and no Nautilus data event
/// needs them. Converting them would mean either inventing precision or dropping it silently, so
/// they stay exactly as the venue sent them and a consumer that needs them must say how it rounds.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PremiumSample {
    /// The venue market string of the sample.
    pub market: String,
    /// The sample time.
    #[serde(default)]
    pub time: Option<String>,
    /// The mark price at the sample time.
    #[serde(default)]
    pub mark: Option<String>,
    /// The bid price at the sample time.
    #[serde(default)]
    pub bid: Option<String>,
    /// The ask price at the sample time.
    #[serde(default)]
    pub ask: Option<String>,
    /// The premium index ratio at the sample time.
    #[serde(default)]
    pub premium_index: Option<String>,
}

/// One item of an `update` frame for the `fundingRatesPerps` channel.
///
/// `rate` is an **hourly decimal fraction** (`0.0000063` = 0.063 bp/h) and is kept exactly as the
/// venue sent it: it is never divided by 100 and never multiplied by 8.
///
/// `interval_ends` is a **settlement time**, not the time the rate was published. It maps onto
/// `FundingRateUpdate::next_funding_ns` and is never written into `ts_event`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingRateItem {
    /// The venue market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The hourly funding rate as a decimal fraction.
    pub rate: String,
    /// The settlement time of the interval this forecast applies to.
    #[serde(default)]
    pub interval_ends: Option<String>,
    /// The trailing premium samples.
    #[serde(default)]
    pub premiums: Vec<PremiumSample>,
}

/// One item of an `update` frame for the `markPricesPerps` channel.
///
/// No production frame for this channel exists in this phase: the P0 probe never subscribed to it,
/// so `test_data/ws/markprices_observed.json` is an `official-example` (manifest `kind`), not an
/// observation, and the optional `time` member is modelled because the spec does not confirm
/// whether it is sent.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkPriceItem {
    /// The venue market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The mark price in quote currency.
    pub mark_price: String,
    /// The price event time of this mark price, when the venue sends one.
    #[serde(default)]
    pub time: Option<String>,
}

/// A `subscribe` or `unsubscribe` request body.
///
/// The serialized form is the exact request body the venue documents; `limit` is only sent for the
/// book channel and `numPastTrades` only for the trades channel, which is what the archived
/// `test_data/ws/subscribe_observed.json` request set looks like.
///
/// `depth_levels` is omitted by default. It is a **price grouping**, and no confirmed
/// un-aggregated value exists for any market in this phase, so this type never defaults it: a
/// caller must supply a value it has confirmed for a specific market.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRequest {
    /// The operation.
    pub op: WsOp,
    /// The channel to operate on.
    pub channel: WsChannel,
    /// The venue market strings, as in `NVDA-USD.P`.
    pub markets: Vec<String>,
    /// The maximum number of book levels requested, for the book channel only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// The number of past trades requested, for the trades channel only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_past_trades: Option<u32>,
    /// The price grouping, when a caller has confirmed one for these markets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth_levels: Option<String>,
}

impl SubscriptionRequest {
    /// Creates a request for a channel and a set of markets.
    ///
    /// `limit` and `num_past_trades` are attached only to the channel that accepts them, so a
    /// caller cannot accidentally send a book limit on the trades channel.
    #[must_use]
    pub fn new(
        op: WsOp,
        channel: WsChannel,
        markets: Vec<String>,
        limit: Option<u32>,
        num_past_trades: Option<u32>,
        depth_levels: Option<String>,
    ) -> Self {
        Self {
            op,
            channel,
            markets,
            limit: limit.filter(|_| channel.is_book()),
            num_past_trades: num_past_trades.filter(|_| channel == WsChannel::TradesPerps),
            depth_levels,
        }
    }

    /// Serializes the request body.
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be serialized, which cannot happen for a
    /// well-formed value of this type and is surfaced rather than unwrapped.
    pub fn to_json_text(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("failed to serialize an Ondo subscription request")
    }
}

/// The application-level heartbeat body: `{"op":"ping"}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingRequest {
    /// The operation, always [`WsOp::Ping`].
    pub op: WsOp,
}

impl PingRequest {
    /// Serializes the heartbeat body.
    ///
    /// # Errors
    ///
    /// Returns an error if the body cannot be serialized.
    pub fn to_json_text(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("failed to serialize the Ondo ping request")
    }
}

/// Splits a `[price, quantity]` level into its two wire lexemes.
///
/// A level that is not exactly two entries is a schema violation, so it is reported with the
/// offending lexemes attached instead of being coerced to a one- or two-sided default.
///
/// # Errors
///
/// Returns an error if a level does not carry exactly two entries.
pub fn split_level<'a>(
    level: &'a [String],
    market: &str,
    field: &'static str,
) -> anyhow::Result<(&'a str, &'a str)> {
    ensure!(
        level.len() == 2,
        "market `{market}` sent a `{field}` level with {} entries: {level:?}",
        level.len()
    );

    Ok((level[0].as_str(), level[1].as_str()))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_channel_wire_names_round_trip() {
        for channel in WsChannel::ALL {
            assert_eq!(WsChannel::from_wire(channel.as_str()), Some(channel));
            assert_eq!(
                serde_json::to_value(channel).unwrap(),
                serde_json::Value::String(channel.as_str().to_string())
            );
        }
        assert_eq!(WsChannel::from_wire("kLinePerps"), None);
    }

    #[rstest]
    #[case::pong("pong", WsMessageType::Pong)]
    #[case::logged_in("loggedIn", WsMessageType::LoggedIn)]
    #[case::subscribed("subscribed", WsMessageType::Subscribed)]
    #[case::unsubscribed("unsubscribed", WsMessageType::Unsubscribed)]
    #[case::update("update", WsMessageType::Update)]
    #[case::error("error", WsMessageType::Error)]
    #[case::invented("snapshot", WsMessageType::Unknown)]
    fn test_message_type_from_wire(#[case] raw: &str, #[case] expected: WsMessageType) {
        assert_eq!(WsMessageType::from_wire(raw), expected);
    }

    #[rstest]
    fn test_subscription_request_attaches_only_the_fields_its_channel_accepts() {
        let book = SubscriptionRequest::new(
            WsOp::Subscribe,
            WsChannel::DepthBooksPerps,
            vec!["NVDA-USD.P".to_string()],
            Some(100),
            Some(0),
            None,
        );
        let text = book.to_json_text().unwrap();

        assert_eq!(
            text,
            r#"{"op":"subscribe","channel":"depthBooksPerps","markets":["NVDA-USD.P"],"limit":100}"#
        );
        assert!(!text.contains("depthLevels"), "never aggregate by default");

        let trades = SubscriptionRequest::new(
            WsOp::Subscribe,
            WsChannel::TradesPerps,
            vec!["NVDA-USD.P".to_string()],
            Some(100),
            Some(0),
            None,
        );

        assert_eq!(
            trades.to_json_text().unwrap(),
            r#"{"op":"subscribe","channel":"tradesPerps","markets":["NVDA-USD.P"],"numPastTrades":0}"#
        );

        let top = SubscriptionRequest::new(
            WsOp::Unsubscribe,
            WsChannel::TopOfBooksPerps,
            vec!["NVDA-USD.P".to_string()],
            None,
            None,
            None,
        );

        assert_eq!(
            top.to_json_text().unwrap(),
            r#"{"op":"unsubscribe","channel":"topOfBooksPerps","markets":["NVDA-USD.P"]}"#
        );
    }

    #[rstest]
    fn test_subscription_request_carries_a_confirmed_grouping_verbatim() {
        let request = SubscriptionRequest::new(
            WsOp::Subscribe,
            WsChannel::DepthBooksPerps,
            vec!["ENA-USD.P".to_string()],
            Some(100),
            None,
            Some("0.0001".to_string()),
        );

        assert_eq!(
            request.to_json_text().unwrap(),
            r#"{"op":"subscribe","channel":"depthBooksPerps","markets":["ENA-USD.P"],"limit":100,"depthLevels":"0.0001"}"#
        );
    }

    #[rstest]
    fn test_ping_body_is_the_application_level_heartbeat() {
        assert_eq!(
            PingRequest { op: WsOp::Ping }.to_json_text().unwrap(),
            r#"{"op":"ping"}"#
        );
    }

    #[rstest]
    fn test_split_level_reports_a_malformed_level() {
        let level = ["212.25".to_string(), "4.7".to_string()];
        let (price, size) = split_level(&level, "NVDA-USD.P", "bids").unwrap();

        assert_eq!(price, "212.25");
        assert_eq!(size, "4.7");

        let error = split_level(&["212.25".to_string()], "NVDA-USD.P", "bids").unwrap_err();

        assert!(error.to_string().contains("212.25"));
        assert!(error.to_string().contains("NVDA-USD.P"));
    }
}
