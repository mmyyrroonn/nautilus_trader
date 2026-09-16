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

//! Wire schema for the Ondo Perps **private** WebSocket channels.
//!
//! Wire only: no conversion, no I/O, and no domain type. Everything here is either a body this
//! adapter composes or a shape it reads verbatim.
//!
//! # The three private channels this adapter uses
//!
//! [`PrivateChannel`] carries exactly the three the account runtime needs: the order reports, the
//! fill reports, and the account-level dead man's switch. `positionsPerps`, `balancePerps` and the
//! rest of the private surface are read over REST during a reconciliation pass
//! ([`crate::execution`]), which is why they are not modelled here: a subscription this adapter
//! never sends is a subscription it cannot get wrong.
//!
//! # The login frame carries a credential, and says so
//!
//! [`LoginArgs`] holds the API key id and the HMAC signature of the login digest. Neither is ever
//! printed: [`LoginRequest`] and [`LoginArgs`] implement [`std::fmt::Debug`] by hand, masking the
//! key id and redacting the signature, and neither is `Display`. The frame has no `to_json_text`
//! caller outside the private transport, and the private transport is the only place a login body
//! exists at all - it is never offered to a recorder
//! ([`crate::websocket::private::diagnostics`] records the *fact* that a login was sent and never
//! its bytes).
//!
//! # `markets` is optional on the private channels, and this adapter omits it
//!
//! The frozen spec's `ordersPerps` and `fillsPerps` requests carry `markets` as an optional member:
//! *"Markets to filter by. Optional; if omitted, all available markets are used."* An account
//! session wants every order and fill the account has, including on a market the data client never
//! subscribed to, so [`PrivateSubscriptionRequest`] serializes `markets` only when a caller names
//! one, and the runtime names none.

use std::fmt;

use nautilus_core::string::secret::{REDACTED, mask_api_key};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::websocket::messages::WsOp;

/// A private (login-required) Ondo Perps WebSocket channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrivateChannel {
    /// `ordersPerps` - order reports for the account.
    #[serde(rename = "ordersPerps")]
    OrdersPerps,
    /// `fillsPerps` - fill reports for the account.
    #[serde(rename = "fillsPerps")]
    FillsPerps,
    /// `cancelAllOrdersAfterPerps` - the account-level dead man's switch.
    ///
    /// Subscribing to this channel is **not** read-only: it arms a venue-side timer that cancels
    /// every resting order on the account when no renewal arrives in time. This is why an account
    /// read-only session never subscribes to it (plan §0).
    #[serde(rename = "cancelAllOrdersAfterPerps")]
    CancelAllOrdersAfterPerps,
}

impl PrivateChannel {
    /// The channels a trading session subscribes to, in the order it subscribes.
    ///
    /// Orders before fills: a fill for an order this session has not seen would be held as
    /// un-attributable, and the order report is what attributes it.
    pub const TRADING: [Self; 3] = [
        Self::OrdersPerps,
        Self::FillsPerps,
        Self::CancelAllOrdersAfterPerps,
    ];

    /// The channels a read-only session subscribes to, in the order it subscribes.
    ///
    /// The switch is deliberately absent: it has a cancelling side effect, and a read-only session
    /// does not arm one to look ready (plan §0, review §4).
    pub const READ_ONLY: [Self; 2] = [Self::OrdersPerps, Self::FillsPerps];

    /// Returns the exact wire channel name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrdersPerps => "ordersPerps",
            Self::FillsPerps => "fillsPerps",
            Self::CancelAllOrdersAfterPerps => "cancelAllOrdersAfterPerps",
        }
    }

    /// Classifies a wire channel name.
    ///
    /// Returns [`None`] for a private channel this adapter does not carry and for every public
    /// channel: the two surfaces are separate, and a public channel arriving on the private socket
    /// is not something this adapter asked for.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        [
            Self::OrdersPerps,
            Self::FillsPerps,
            Self::CancelAllOrdersAfterPerps,
        ]
        .into_iter()
        .find(|channel| channel.as_str() == value)
    }

    /// Returns whether this channel carries account reports rather than the switch.
    #[must_use]
    pub const fn is_report(self) -> bool {
        matches!(self, Self::OrdersPerps | Self::FillsPerps)
    }
}

/// The `args` member of an API-key login: exactly the three values [`crate::signing`] produces.
///
/// The frozen `LoginArgs` schema admits either `{token}` (JWT) or `{key, time, sign}`. This adapter
/// takes the second: it holds an API key and a secret, and it has no way to obtain a JWT, so the
/// JWT member is not modelled rather than modelled and left empty.
///
/// `time` is a **string** of Unix milliseconds on the wire, exactly as the schema documents it, and
/// `sign` is the lowercase hex HMAC-SHA256 of the login digest.
#[derive(Serialize)]
pub struct LoginArgs {
    /// The API key id, `ondoKeyId_` prefix included.
    key: String,
    /// Unix milliseconds, as a string.
    time: String,
    /// The lowercase hex HMAC-SHA256 signature.
    sign: String,
}

impl LoginArgs {
    /// Builds the arguments for one login.
    #[must_use]
    pub fn new(key: String, timestamp_ms: u64, signature: String) -> Self {
        Self {
            key,
            time: timestamp_ms.to_string(),
            sign: signature,
        }
    }
}

impl fmt::Debug for LoginArgs {
    /// Masks the key id and redacts the signature.
    ///
    /// The signature is `hex(HMAC_SHA256(secret, digest))`: it is not the secret, and it still must
    /// not be printed, because a signature plus its exact timestamp is a credential-shaped value
    /// that a log aggregator would keep.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LoginArgs))
            .field("key", &mask_api_key(&self.key))
            .field("time", &self.time)
            .field("sign", &REDACTED)
            .finish()
    }
}

/// The login request body: `{"op":"login","args":{...}}`.
///
/// The `op` member is a [`WsOp`], which `#[serde(rename_all = "lowercase")]` spells `"login"` for
/// free - the same idiom [`crate::websocket::messages::SubscriptionRequest`] and
/// [`crate::websocket::messages::PingRequest`] use.
#[derive(Serialize)]
pub struct LoginRequest {
    /// The operation, always [`WsOp::Login`].
    op: WsOp,
    /// The credential arguments.
    args: LoginArgs,
}

impl LoginRequest {
    /// Builds the login body for one API-key credential.
    #[must_use]
    pub fn new(key: String, timestamp_ms: u64, signature: String) -> Self {
        Self {
            op: WsOp::Login,
            args: LoginArgs::new(key, timestamp_ms, signature),
        }
    }

    /// Serializes the login body.
    ///
    /// This is the whole of the API surface: the body is written to a socket by
    /// [`super::stream`] and to nothing else. There is no accessor for the arguments, so a caller
    /// cannot lift the signature out of a built request.
    ///
    /// # Errors
    ///
    /// Returns an error if the body cannot be serialized, which cannot happen for a well-formed
    /// value of this type and is surfaced rather than unwrapped.
    pub fn to_json_text(&self) -> anyhow::Result<String> {
        serde_json::to_string(self)
            .map_err(|error| anyhow::anyhow!("failed to serialize an Ondo login request: {error}"))
    }
}

impl fmt::Debug for LoginRequest {
    /// Renders the operation and the redacted arguments, never the signature.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LoginRequest))
            .field("op", &self.op)
            .field("args", &self.args)
            .finish()
    }
}

/// A private `subscribe` or `unsubscribe` request body.
///
/// `markets` is omitted when it is [`None`], which is what the venue documents as "all available
/// markets" for the private channels. The public [`crate::websocket::messages::SubscriptionRequest`]
/// is a different type on purpose: it requires `markets`, sends `depthLevels` for the book channel
/// and `numPastTrades` for the trades channel, and none of those exist here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateSubscriptionRequest {
    /// The operation.
    pub op: WsOp,
    /// The channel to operate on.
    pub channel: PrivateChannel,
    /// The markets to filter by, when a caller wants a filter.
    ///
    /// Absent means "every market the account has", which is what an account session wants: an
    /// order on a market the data client never subscribed to is still this account's order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markets: Option<Vec<String>>,
}

impl PrivateSubscriptionRequest {
    /// Creates a request for one channel and no market filter.
    #[must_use]
    pub const fn account_wide(op: WsOp, channel: PrivateChannel) -> Self {
        Self {
            op,
            channel,
            markets: None,
        }
    }

    /// Creates a request for one channel narrowed to `markets`.
    #[must_use]
    pub const fn for_markets(op: WsOp, channel: PrivateChannel, markets: Vec<String>) -> Self {
        Self {
            op,
            channel,
            markets: Some(markets),
        }
    }

    /// Serializes the request body.
    ///
    /// # Errors
    ///
    /// Returns an error if the body cannot be serialized.
    pub fn to_json_text(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|error| {
            anyhow::anyhow!("failed to serialize an Ondo private subscription request: {error}")
        })
    }
}

/// The raw envelope of a private frame, read once and classified elsewhere.
///
/// The public [`crate::websocket::messages::RawServerMessage`] decodes `data` into a
/// [`serde_json::Value`], which has already lost the original bytes. That is fine for the public
/// channels, whose items are decoded from a `Value`, and it is fatal for the private ones:
/// [`crate::http::orders::OndoApiOrder::from_text`] and
/// [`crate::http::private::OndoApiFill::from_raw`] are the *same* decoders the REST pages go
/// through, and the fill decoder takes a `&RawValue`. Decoding the private `data` into a `Value`
/// first would mean re-serializing it, which is a second encoding of a payload the adapter is
/// supposed to keep verbatim ([`crate::http::orders::OndoApiOrder::raw`]).
///
/// So this struct is the public one with `data` (and `code`) borrowed as raw text, and the
/// classification that both share lives in [`crate::websocket::parse`] rather than being written
/// twice.
#[derive(Debug, Deserialize)]
pub struct RawPrivateMessage<'a> {
    /// The wire message type, kept verbatim because the classifier collapses unknown values.
    #[serde(rename = "type")]
    pub kind: &'a str,
    /// The raw channel name, as sent.
    #[serde(default, borrow)]
    pub channel: Option<&'a str>,
    /// The server send/batch time, raw.
    #[serde(default, borrow)]
    pub timestamp: Option<&'a str>,
    /// The `data` member, kept as the bytes the venue sent.
    #[serde(default, borrow)]
    pub data: Option<&'a RawValue>,
    /// An error or acknowledgement description, when the venue sends one.
    ///
    /// The frozen private pages spell this member **`msg`**: the login page's success example is
    /// `{"type":"loggedIn","msg":"Login successful"}` and its failure examples are
    /// `{"type":"error","msg":"already logged in on this connection"}`. The public envelope's
    /// reference to `message` is modelled as the fallback, so a frame carrying either spelling
    /// yields the text rather than a blank - and a login refusal with no readable reason is exactly
    /// the case where the difference matters, because the reason is what decides whether the
    /// session is retried.
    #[serde(default, borrow, alias = "message")]
    pub msg: Option<&'a str>,
    /// An error code, when the venue sends one.
    #[serde(default, borrow)]
    pub code: Option<&'a RawValue>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_the_private_channel_wire_names_round_trip() {
        for channel in PrivateChannel::TRADING {
            assert_eq!(PrivateChannel::from_wire(channel.as_str()), Some(channel));
            assert_eq!(
                serde_json::to_value(channel).unwrap(),
                serde_json::Value::String(channel.as_str().to_string()),
            );
        }

        // A public channel is not a private one, and the private classifier says so rather than
        // folding the two surfaces together.
        assert_eq!(PrivateChannel::from_wire("depthBooksPerps"), None);
        assert_eq!(PrivateChannel::from_wire("positionsPerps"), None);
        assert_eq!(PrivateChannel::from_wire("kLinePerps"), None);
    }

    #[rstest]
    fn test_a_read_only_session_never_subscribes_to_the_switch() {
        assert_eq!(
            PrivateChannel::TRADING,
            [
                PrivateChannel::OrdersPerps,
                PrivateChannel::FillsPerps,
                PrivateChannel::CancelAllOrdersAfterPerps,
            ],
        );
        assert!(
            !PrivateChannel::READ_ONLY.contains(&PrivateChannel::CancelAllOrdersAfterPerps),
            "the switch cancels resting orders; a read-only session does not arm one",
        );
        assert!(!PrivateChannel::CancelAllOrdersAfterPerps.is_report());
        assert!(PrivateChannel::OrdersPerps.is_report());
    }

    /// The login body is the documented shape, and the operation is spelled by the `WsOp` serde
    /// rename rather than written out here.
    #[rstest]
    fn test_the_login_body_is_the_documented_shape() {
        let request = LoginRequest::new(
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            1_789_384_200_000,
            "29b1e3ca1a71e3771d4f6bc375308b86e52577c16fd2edede0ee58a06070b4b4".to_string(),
        );

        assert_eq!(
            request.to_json_text().unwrap(),
            r#"{"op":"login","args":{"key":"ondoKeyId_UNIT_TEST_ONLY","time":"1789384200000","sign":"29b1e3ca1a71e3771d4f6bc375308b86e52577c16fd2edede0ee58a06070b4b4"}}"#,
        );
    }

    /// Neither the key id nor the signature survives a rendering of the request or its arguments.
    #[rstest]
    fn test_a_login_request_never_renders_its_credential() {
        let request = LoginRequest::new(
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            1_789_384_200_000,
            "29b1e3ca1a71e3771d4f6bc375308b86e52577c16fd2edede0ee58a06070b4b4".to_string(),
        );

        let rendered = format!("{:?} {:?}", request, request.args);

        assert!(!rendered.contains("UNIT_TEST_ONLY"), "{rendered}");
        assert!(!rendered.contains("29b1e3ca"), "{rendered}");
        assert!(rendered.contains(REDACTED), "{rendered}");
        assert!(
            rendered.contains("1789384200000"),
            "the signed instant is not a credential: {rendered}",
        );
    }

    /// `markets` is omitted, not sent empty: the venue reads an absent member as "all markets" and
    /// an empty array is not the same statement.
    #[rstest]
    fn test_an_account_wide_request_omits_the_market_filter() {
        let orders =
            PrivateSubscriptionRequest::account_wide(WsOp::Subscribe, PrivateChannel::OrdersPerps);

        assert_eq!(
            orders.to_json_text().unwrap(),
            r#"{"op":"subscribe","channel":"ordersPerps"}"#,
        );

        let fillers =
            PrivateSubscriptionRequest::account_wide(WsOp::Subscribe, PrivateChannel::FillsPerps);

        assert_eq!(
            fillers.to_json_text().unwrap(),
            r#"{"op":"subscribe","channel":"fillsPerps"}"#,
        );

        let narrowed = PrivateSubscriptionRequest::for_markets(
            WsOp::Unsubscribe,
            PrivateChannel::OrdersPerps,
            vec!["NVDA-USD.P".to_string()],
        );

        assert_eq!(
            narrowed.to_json_text().unwrap(),
            r#"{"op":"unsubscribe","channel":"ordersPerps","markets":["NVDA-USD.P"]}"#,
        );
    }

    /// The private envelope keeps `data` as the bytes the venue sent, which is what lets the REST
    /// decoders be reused instead of re-serializing a payload through a `Value`.
    #[rstest]
    fn test_the_raw_envelope_borrows_its_data_without_re_encoding_it() {
        let text = r#"{"type":"update","channel":"ordersPerps","timestamp":"2026-09-15T10:00:00.000000000Z","data":[{"orderId":"197ec08e001658690721be129e7fa595","size":"10.00"}]}"#;
        let raw: RawPrivateMessage<'_> = serde_json::from_str(text).expect("the envelope decodes");

        assert_eq!(raw.kind, "update");
        assert_eq!(raw.channel, Some("ordersPerps"));
        assert_eq!(
            raw.timestamp,
            Some("2026-09-15T10:00:00.000000000Z"),
            "the undeclared envelope timestamp is kept as the text it arrived as",
        );

        let data = raw.data.expect("the frame carries data");
        assert_eq!(
            data.get(),
            r#"[{"orderId":"197ec08e001658690721be129e7fa595","size":"10.00"}]"#,
            "the data member is the venue's own bytes, not a re-encoding of a Value",
        );
    }
}
