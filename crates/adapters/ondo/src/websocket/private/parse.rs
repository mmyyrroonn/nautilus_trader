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

//! Private wire to domain values: the one place a private wire lexeme becomes a value.
//!
//! # The REST decoders are reused, deliberately
//!
//! An order report on `ordersPerps` and the same order in `GET /v1/perps/orders` carry the same
//! facts, and [`crate::reconciliation::ReconciliationBuffer`] holds a stream report as the **REST**
//! type precisely so that a recovery pass replays it through the same state machine the REST pages
//! go through, where `(account_id, fill.id)` dedupes the two. Decoding the stream into anything
//! else would silently defeat that dedup - the second delivery would look like a different fact -
//! so [`decode_private_item`] calls [`OndoApiOrder::from_text`] and [`OndoApiFill::from_raw`] and
//! adds nothing of its own.
//!
//! That is also why [`PrivateEnvelope::data`] is a [`RawValue`] and not a [`serde_json::Value`]:
//! `from_raw` wants the bytes, and a `Value` has already lost them.
//!
//! # A frame that will not decode is a protocol error, not a transport error
//!
//! The frozen WS schemas declare `required: []`, so the venue may legally send a frame that omits
//! a member the Rust decoder cannot do without. That is a payload this adapter does not understand,
//! and it is the account stream's problem rather than the socket's: the connection stays up, the
//! account is told it lost reports, and the next frame is read normally. Killing the connection
//! over one surprising payload would take the whole account offline.

use anyhow::Context;
use nautilus_core::UnixNanos;
use serde_json::{Value, value::RawValue};

use crate::{
    http::{orders::OndoApiOrder, private::OndoApiFill},
    websocket::{
        messages::WsMessageType,
        parse::FrameHeader,
        private::messages::{PrivateChannel, RawPrivateMessage},
    },
};

/// A classified private frame, with its `data` member still in the bytes the venue sent.
#[derive(Clone, Debug)]
pub struct PrivateEnvelope {
    /// The classified message type.
    pub kind: WsMessageType,
    /// The wire message type, kept verbatim so an unknown type stays diagnosable.
    pub kind_raw: String,
    /// The channel, when it is one this adapter carries.
    pub channel: Option<PrivateChannel>,
    /// The channel name as sent, kept even when [`Self::channel`] is [`None`].
    pub channel_raw: Option<String>,
    /// The server send/batch time, parsed exactly.
    pub timestamp: Option<UnixNanos>,
    /// The `data` member, verbatim.
    pub data: Option<Box<RawValue>>,
    /// An error or acknowledgement description, when the venue sent one.
    pub message: Option<String>,
    /// An error code, when the venue sent one.
    pub code: Option<String>,
}

/// A bounded summary of one DMS-channel `update`.
///
/// The venue's DMS update contract is not sufficiently specific to use as an acknowledgement.
/// This summary therefore keeps only the fixed classifications the production report exposes;
/// it never retains the update's text, unknown keys or free-form values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DmsUpdateSummary {
    /// The top-level JSON kind of `data`.
    pub data_kind: &'static str,
    /// The allowlisted `op` classification.
    pub op: &'static str,
    /// The allowlisted `timeout_seconds` classification.
    pub timeout: &'static str,
    /// The allowlisted `status` classification.
    pub status: &'static str,
    /// The allowlisted `enabled` classification.
    pub enabled: &'static str,
}

impl Default for DmsUpdateSummary {
    fn default() -> Self {
        Self {
            data_kind: "missing",
            op: "missing",
            timeout: "missing",
            status: "missing",
            enabled: "missing",
        }
    }
}

/// Summarizes one DMS-channel update without retaining any payload text.
#[must_use]
pub fn summarize_dms_update(data: Option<&RawValue>) -> DmsUpdateSummary {
    let Some(data) = data else {
        return DmsUpdateSummary::default();
    };

    let data_kind = classify_json_kind(data.get());
    let mut summary = DmsUpdateSummary {
        data_kind,
        ..DmsUpdateSummary::default()
    };

    if data_kind != "object" {
        return summary;
    }

    // `RawValue` is already validated by the envelope parser. This temporary value is discarded
    // before the summary leaves this function, and only the four fixed keys below are projected.
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(data.get()) else {
        return summary;
    };

    summary.op = object.get("op").map_or("missing", classify_update_op);
    summary.timeout = object
        .get("timeout_seconds")
        .map_or("missing", classify_update_timeout);
    summary.status = object
        .get("status")
        .map_or("missing", classify_update_status);
    summary.enabled = object
        .get("enabled")
        .map_or("missing", classify_update_enabled);

    summary
}

fn classify_json_kind(raw: &str) -> &'static str {
    match raw.trim_start().as_bytes().first().copied() {
        None => "missing",
        Some(b'n') => "null",
        Some(b'{') => "object",
        Some(b'[') => "array",
        Some(b'"') => "string",
        Some(b'-' | b'0'..=b'9') => "number",
        Some(b't' | b'f') => "boolean",
        Some(_) => "missing",
    }
}

fn classify_update_op(value: &Value) -> &'static str {
    match value.as_str() {
        Some("subscribe") => "subscribe",
        Some("unsubscribe") => "unsubscribe",
        Some(_) | None => "unrecognized",
    }
}

fn classify_update_timeout(value: &Value) -> &'static str {
    let Value::Number(number) = value else {
        return "invalid";
    };

    if let Some(value) = number.as_i64() {
        if value == 0 {
            "zero"
        } else if value < 0 {
            "negative"
        } else {
            "positive"
        }
    } else if let Some(value) = number.as_u64() {
        if value == 0 { "zero" } else { "positive" }
    } else {
        // The venue contract calls this an integer number of seconds. Decimal and exponent forms
        // are therefore retained only as the fixed `invalid` classification.
        "invalid"
    }
}

fn classify_update_status(value: &Value) -> &'static str {
    match value.as_str() {
        Some("armed") => "armed",
        Some("disarmed") => "disarmed",
        Some("enabled") => "enabled",
        Some("disabled") => "disabled",
        Some("active") => "active",
        Some("inactive") => "inactive",
        Some("released") => "released",
        Some("cancelled") => "cancelled",
        Some("canceled") => "canceled",
        Some("success") => "success",
        Some("ok") => "ok",
        Some(_) | None => "unrecognized",
    }
}

fn classify_update_enabled(value: &Value) -> &'static str {
    match value {
        Value::Bool(true) => "true",
        Value::Bool(false) => "false",
        _ => "invalid",
    }
}

/// Parses a private frame into its classified envelope.
///
/// # Errors
///
/// Returns an error if the frame is not JSON for the envelope, or if it carries an envelope
/// `timestamp` that is not an RFC 3339 instant. Both are the same refusals
/// [`crate::websocket::parse::parse_server_message`] makes, through the same
/// [`FrameHeader::classify`].
pub fn parse_private_message(text: &str) -> anyhow::Result<PrivateEnvelope> {
    let raw: RawPrivateMessage<'_> =
        serde_json::from_str(text).context("failed to decode an Ondo private WebSocket frame")?;

    let code = raw.code.map(|code| code.get().to_string());
    let header = FrameHeader::classify(
        raw.kind,
        raw.channel,
        raw.timestamp,
        raw.msg,
        code.as_deref(),
    )?;

    // An owned copy of the venue's own bytes: `RawValue` is borrowed from the frame text, which the
    // caller owns and may drop, and the decoders below want a `&RawValue` that outlives the parse.
    let data = raw
        .data
        .map(|data| RawValue::from_string(data.get().to_string()))
        .transpose()
        .context("failed to retain an Ondo private frame's `data` member")?;

    Ok(PrivateEnvelope {
        kind: header.kind,
        kind_raw: header.kind_raw,
        channel: raw.channel.and_then(PrivateChannel::from_wire),
        channel_raw: header.channel,
        timestamp: header.timestamp,
        data,
        message: header.message,
        code: header.code,
    })
}

/// One decoded private payload.
///
/// The variant is fixed by the channel the frame arrived on, never by the shape of the payload: a
/// `Fills` item is decoded as a fill because it arrived on `fillsPerps`, so a payload that happens
/// to look like the other kind is a decode failure rather than a promotion.
#[derive(Clone, Debug)]
pub enum PrivatePayload {
    /// An order report from `ordersPerps`.
    Order(Box<OndoApiOrder>),
    /// A fill report from `fillsPerps`.
    Fill(Box<OndoApiFill>),
}

/// Decodes the `data` member of a private `update` frame for one channel.
///
/// # Errors
///
/// Returns an error if `data` is not an array, if an item does not decode as the channel's schema,
/// or if `channel` is not a channel that carries items at all.
pub fn decode_private_updates(
    channel: PrivateChannel,
    data: &RawValue,
) -> anyhow::Result<Vec<PrivatePayload>> {
    // The channel is checked first, so a frame for a channel that carries no items is answered with
    // that fact rather than with a complaint about the shape of a `data` member it never had a
    // schema for.
    if channel == PrivateChannel::CancelAllOrdersAfterPerps {
        anyhow::bail!(
            "the `{}` channel carries no item array in the frozen schema, so a frame for it is not \
             decoded as one",
            channel.as_str(),
        );
    }

    let items: Vec<&RawValue> = serde_json::from_str(data.get()).with_context(|| {
        format!(
            "channel `{}` sent a `data` member that is not an array of items: {}",
            channel.as_str(),
            data.get(),
        )
    })?;

    items
        .into_iter()
        .map(|item| decode_private_item(channel, item))
        .collect()
}

/// Decodes one item of a private `update` frame.
///
/// # Errors
///
/// Returns an error naming the offending item when it does not decode as `channel`'s schema. The
/// item is never dropped quietly: a payload this adapter could not read is a report it lost.
pub fn decode_private_item(
    channel: PrivateChannel,
    item: &RawValue,
) -> anyhow::Result<PrivatePayload> {
    // The item's own text, redacted of nothing: it is the venue's account payload and it is
    // carried into the error so a decode failure is diagnosable. It is never a secret - the only
    // private frame that carries one is the login *request*, which this adapter composes and never
    // decodes.
    let context = || {
        format!(
            "channel `{}` sent an item that does not decode as its schema: {}",
            channel.as_str(),
            item.get(),
        )
    };

    match channel {
        PrivateChannel::OrdersPerps => OndoApiOrder::from_text(item.get())
            .map(|order| PrivatePayload::Order(Box::new(order)))
            .with_context(context),
        PrivateChannel::FillsPerps => OndoApiFill::from_raw(item)
            .map(|fill| PrivatePayload::Fill(Box::new(fill)))
            .with_context(context),
        PrivateChannel::CancelAllOrdersAfterPerps => Err(anyhow::anyhow!(
            "the `{}` channel carries no item array in the frozen schema",
            channel.as_str(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// The frozen spec's own `Order` example, verbatim. `test_data/manifest.json`'s `kind_legend`
    /// calls this `official-example`: it is the venue's documented shape, not an observation.
    const ORDER_EXAMPLE: &str = r#"{"orderId":"197ec08e001658690721be129e7fa595","side":"buy","price":"227.50","size":"10.00","market":"AAPL-USD.P","filledSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC"}"#;

    /// The frozen spec's own `Fill` example, verbatim. Note `"direction":"open long"`: the WS
    /// spelling is spaced while REST spells it `openLong`, which is `conflicts.md` conflict 4 and
    /// is why the decoder that already handles both is the one reused here.
    const FILL_EXAMPLE: &str = r#"{"id":"70a37d8f972f2494837f9dba8364cbb4","orderId":"197ec08e001658690721be129e7fa595","market":"AAPL-USD.P","price":"227.50","size":"5.00","side":"buy","direction":"open long","filledCost":"1137.50","fee":"0.57","time":"2025-03-05T14:30:00Z","isMaker":false}"#;

    fn frame(channel: &str, data: &str) -> String {
        format!(r#"{{"type":"update","channel":"{channel}","data":{data}}}"#)
    }

    /// The claim the whole design rests on, tested rather than assumed: a frame built from the
    /// frozen WS spec's own example decodes through the **REST** decoders, byte-compatibly.
    #[rstest]
    fn test_the_frozen_ws_examples_decode_through_the_rest_decoders() {
        let orders = frame("ordersPerps", &format!("[{ORDER_EXAMPLE}]"));
        let envelope = parse_private_message(&orders).expect("the envelope decodes");

        assert_eq!(envelope.kind, WsMessageType::Update);
        assert_eq!(envelope.channel, Some(PrivateChannel::OrdersPerps));

        let decoded = decode_private_updates(
            PrivateChannel::OrdersPerps,
            envelope.data.as_ref().expect("the frame carries data"),
        )
        .expect("the documented order decodes through the REST decoder");

        let [PrivatePayload::Order(order)] = decoded.as_slice() else {
            panic!("an `ordersPerps` item decodes as an order: {decoded:?}");
        };

        assert_eq!(order.order_id(), "197ec08e001658690721be129e7fa595");
        assert_eq!(order.market(), "AAPL-USD.P");
        assert_eq!(
            order.raw(),
            ORDER_EXAMPLE,
            "the decoder keeps the venue's own bytes, which is what makes a buffered report \
             replayable through the REST path",
        );

        let fills = frame("fillsPerps", &format!("[{FILL_EXAMPLE}]"));
        let envelope = parse_private_message(&fills).expect("the envelope decodes");

        let decoded = decode_private_updates(
            PrivateChannel::FillsPerps,
            envelope.data.as_ref().expect("the frame carries data"),
        )
        .expect("the documented fill decodes through the REST decoder");

        let [PrivatePayload::Fill(fill)] = decoded.as_slice() else {
            panic!("a `fillsPerps` item decodes as a fill: {decoded:?}");
        };

        assert_eq!(fill.id(), "70a37d8f972f2494837f9dba8364cbb4");
        assert_eq!(fill.order_id(), "197ec08e001658690721be129e7fa595");
        assert_eq!(
            fill.fee(),
            Some("0.57"),
            "the fill's own fee is read, and it is the only fee this adapter charges",
        );
    }

    /// `required: []` in the frozen schema means the venue may legally omit a member the decoder
    /// needs. That is a protocol error the connection survives, and it is reported with the
    /// offending item attached rather than as a silent skip.
    #[rstest]
    fn test_an_item_that_does_not_decode_names_itself() {
        let partial = r#"{"orderId":"197ec08e001658690721be129e7fa595","market":"AAPL-USD.P"}"#;
        let text = frame("ordersPerps", &format!("[{partial}]"));
        let envelope = parse_private_message(&text).expect("the envelope decodes");

        let error = decode_private_updates(
            PrivateChannel::OrdersPerps,
            envelope.data.as_ref().expect("the frame carries data"),
        )
        .expect_err("an order with no status does not decode");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("ordersPerps"), "{rendered}");
        assert!(
            rendered.contains("197ec08e001658690721be129e7fa595"),
            "{rendered}"
        );
    }

    /// An item is decoded as the channel says, never as its own shape suggests: a fill arriving on
    /// the order channel is a decode failure, not an order.
    #[rstest]
    fn test_the_channel_decides_the_item_kind() {
        let text = frame("ordersPerps", &format!("[{FILL_EXAMPLE}]"));
        let envelope = parse_private_message(&text).expect("the envelope decodes");

        assert!(
            decode_private_updates(
                PrivateChannel::OrdersPerps,
                envelope.data.as_ref().expect("the frame carries data"),
            )
            .is_err(),
            "a fill payload is not an order",
        );
    }

    /// The `data` member of a switch frame is an object in the frozen schema, so it is not walked
    /// as an item array - and saying so is this function's job rather than a caller's.
    #[rstest]
    fn test_the_switch_channel_carries_no_item_array() {
        let text = frame("cancelAllOrdersAfterPerps", r#"{"timeout_seconds":30}"#);
        let envelope = parse_private_message(&text).expect("the envelope decodes");

        assert_eq!(
            envelope.channel,
            Some(PrivateChannel::CancelAllOrdersAfterPerps),
        );

        let error = decode_private_updates(
            PrivateChannel::CancelAllOrdersAfterPerps,
            envelope.data.as_ref().expect("the frame carries data"),
        )
        .expect_err("the switch channel carries no items");

        assert!(error.to_string().contains("no item array"), "{error}");
    }

    /// The channel name is classified here, so no later layer turns a lexeme into a value - and a
    /// private channel this adapter does not carry stays diagnosable as the text it was.
    #[rstest]
    fn test_the_channel_is_classified_here_and_kept_verbatim_when_unknown() {
        let known =
            parse_private_message(&frame("fillsPerps", "[]")).expect("the envelope decodes");

        assert_eq!(known.channel, Some(PrivateChannel::FillsPerps));
        assert_eq!(known.channel_raw.as_deref(), Some("fillsPerps"));

        let unknown =
            parse_private_message(&frame("balancePerps", "[]")).expect("the envelope decodes");

        assert_eq!(unknown.channel, None);
        assert_eq!(unknown.channel_raw.as_deref(), Some("balancePerps"));
    }

    /// The envelope's undeclared `timestamp` is read exactly, and an unreadable one is refused
    /// rather than dropped - the same rule the public parser applies, through the same classifier.
    #[rstest]
    fn test_the_envelope_timestamp_is_read_exactly_and_refused_when_unreadable() {
        let text = r#"{"type":"loggedIn","msg":"Login successful","timestamp":"2026-09-15T10:00:00.000000000Z"}"#;
        let envelope = parse_private_message(text).expect("the envelope decodes");

        assert_eq!(envelope.kind, WsMessageType::LoggedIn);
        assert!(envelope.timestamp.is_some());
        assert!(envelope.channel.is_none(), "an ack names no channel");

        let bad = r#"{"type":"loggedIn","timestamp":"not a time"}"#;
        assert!(parse_private_message(bad).is_err());
    }

    /// The acknowledgement the public session reports as `Unsupported` is exactly what the private
    /// session is waiting for, so it has to classify here.
    #[rstest]
    fn test_the_login_acknowledgement_classifies() {
        let envelope =
            parse_private_message(r#"{"type":"loggedIn","msg":"Login successful"}"#).expect("JSON");

        assert_eq!(envelope.kind, WsMessageType::LoggedIn);
        assert_eq!(envelope.message.as_deref(), Some("Login successful"));
    }

    #[rstest]
    #[case("0", "zero")]
    #[case("30", "positive")]
    #[case("-1", "negative")]
    #[case("0.0", "invalid")]
    #[case("1.5", "invalid")]
    #[case("true", "invalid")]
    #[case("null", "invalid")]
    #[case(r#""30""#, "invalid")]
    fn test_dms_timeout_summary_requires_integer(#[case] value: &str, #[case] expected: &str) {
        let raw = RawValue::from_string(format!(r#"{{"timeout_seconds":{value}}}"#)).unwrap();
        assert_eq!(summarize_dms_update(Some(&raw)).timeout, expected);
    }

    #[rstest]
    fn test_dms_summary_drops_all_unknown_private_fields_and_values() {
        let raw = RawValue::from_string(
            r#"{"op":"PRIVATE_SECRET","status":"PRIVATE_SECRET","timeout_seconds":"PRIVATE_SECRET","enabled":"PRIVATE_SECRET","account":"PRIVATE_SECRET"}"#.into(),
        ).unwrap();
        let summary = summarize_dms_update(Some(&raw));
        assert_eq!(summary.data_kind, "object");
        assert_eq!(summary.op, "unrecognized");
        assert_eq!(summary.status, "unrecognized");
        assert_eq!(summary.timeout, "invalid");
        assert_eq!(summary.enabled, "invalid");
        assert!(!format!("{summary:?}").contains("PRIVATE_SECRET"));
        assert_eq!(summarize_dms_update(None).data_kind, "missing");
    }
}
