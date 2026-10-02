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

//! Official private events with exact decimals and checked microsecond timestamps.
//!
//! Source: https://docs.backpack.exchange/ (Private Streams, observed 2026-10-02).
//! Subscription success ACK is not specified by the evidence used here. Generic
//! result/id controls remain unconfirmed. Initial position rows do not prove coverage.
use std::collections::{BTreeMap, BTreeSet};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::PositionSide,
    identifiers::{InstrumentId, PositionId},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    account::{
        BackpackAccountError, BackpackEvidenceGap,
        models::{
            BackpackDecimal, BackpackFill, BackpackOrder, BackpackOrderCreatedAt,
            BackpackWalletBalance,
        },
        reports::{
            BackpackReportContext, BackpackWalletReport, fill_report, order_report, wallet_report,
        },
    },
    parsing::{exact_quantity, unix_microseconds_to_nanos},
};
/// Maximum accepted JSON frame before parsing/allocation.
pub const MAX_PRIVATE_FRAME_BYTES: usize = 1_048_576;
#[derive(Debug)]
pub enum BackpackPrivateFact {
    Wallet {
        asset: String,
        balance: BackpackWalletReport,
        ts_event: UnixNanos,
    },
    Order(Box<OrderStatusReport>),
    Fill(Box<FillReport>),
    Position(Box<PositionStatusReport>),
}
/// Supported facts alongside explicit uncertainty. Empty facts are never a flat snapshot.
#[derive(Debug)]
pub struct BackpackPrivateObservation {
    pub topic: Option<String>,
    pub facts: Vec<BackpackPrivateFact>,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct Envelope {
    stream: Option<String>,
    data: Option<Value>,
}
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct Balance {
    e: String,
    E: u64,
    T: u64,
    a: String,
    A: BackpackDecimal,
    L: BackpackDecimal,
    S: BackpackDecimal,
}
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct Order {
    e: String,
    E: u64,
    T: u64,
    s: String,
    c: Option<u32>,
    S: String,
    o: String,
    f: String,
    q: Option<BackpackDecimal>,
    Q: Option<BackpackDecimal>,
    p: Option<BackpackDecimal>,
    r: bool,
    X: String,
    i: String,
    z: BackpackDecimal,
    Z: BackpackDecimal,
    V: String,
    O: String,
    y: Option<bool>,
    t: Option<i64>,
    l: Option<BackpackDecimal>,
    L: Option<BackpackDecimal>,
    m: Option<bool>,
    n: Option<BackpackDecimal>,
    N: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct Position {
    e: Option<String>,
    E: u64,
    T: u64,
    s: String,
    q: BackpackDecimal,
    B: BackpackDecimal,
    i: String,
    // Remaining documented economics are required and preserved by decoding exact decimals.
    b: BackpackDecimal,
    f: BackpackDecimal,
    M: BackpackDecimal,
    m: BackpackDecimal,
    Q: BackpackDecimal,
    n: BackpackDecimal,
    p: BackpackDecimal,
    P: BackpackDecimal,
    l: Option<BackpackDecimal>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}
/// Decodes one current-session frame. Connection/epoch ownership is enforced by the owner.
///
/// # Errors
/// Returns sanitized errors for malformed shapes, unknown topics, identity mismatch,
/// invalid timestamps, contradictions and unsupported native precision.

pub fn decode_private(
    frame: &[u8],
    context: &BackpackReportContext<'_>,
) -> Result<BackpackPrivateObservation, BackpackAccountError> {
    if frame.len() > MAX_PRIVATE_FRAME_BYTES {
        return Err(BackpackAccountError::Budget);
    }
    let env: Envelope = serde_json::from_slice(frame).map_err(|_| BackpackAccountError::Decode)?;
    let mut observation = BackpackPrivateObservation {
        topic: env.stream.clone(),
        facts: Vec::new(),
        gaps: BTreeSet::from([BackpackEvidenceGap::AccountIdentityUnverified]),
    };
    let Some(topic) = env.stream else {
        // Generic controls, including result=null, cannot establish private success.
        if env.data.is_some() {
            return Err(BackpackAccountError::Decode);
        }
        observation
            .gaps
            .insert(BackpackEvidenceGap::UnknownVenueState);
        return Ok(observation);
    };
    let raw = env.data.ok_or(BackpackAccountError::Decode)?;
    if topic == "account.balanceUpdate" {
        let b: Balance = serde_json::from_value(raw).map_err(|_| BackpackAccountError::Decode)?;
        if b.e != "balanceUpdate" {
            return Err(BackpackAccountError::InvalidField("balance event"));
        }
        unix_microseconds_to_nanos(b.E)
            .map_err(|_| BackpackAccountError::InvalidField("event time"))?;
        let ts = unix_microseconds_to_nanos(b.T)
            .map_err(|_| BackpackAccountError::InvalidField("engine time"))?;
        let balance = wallet_report(
            &b.a,
            &BackpackWalletBalance {
                available: b.A,
                locked: b.L,
                staked: b.S,
                extra: BTreeMap::new(),
            },
        )?;
        observation.facts.push(BackpackPrivateFact::Wallet {
            asset: b.a,
            balance,
            ts_event: ts,
        });
    } else if topic == "account.orderUpdate" || topic.starts_with("account.orderUpdate.") {
        let o: Order = serde_json::from_value(raw).map_err(|_| BackpackAccountError::Decode)?;
        validate_topic(&topic, "account.orderUpdate", &o.s, context)?;
        unix_microseconds_to_nanos(o.E)
            .map_err(|_| BackpackAccountError::InvalidField("event time"))?;
        let ts = unix_microseconds_to_nanos(o.T)
            .map_err(|_| BackpackAccountError::InvalidField("engine time"))?;
        if ![
            "orderAccepted",
            "orderCancelled",
            "orderExpired",
            "orderFill",
            "orderModified",
            "triggerPlaced",
            "triggerFailed",
        ]
        .contains(&o.e.as_str())
        {
            return Err(BackpackAccountError::Unsupported("order event"));
        }
        if (o.e == "orderAccepted" && o.X != "New")
            || (o.e == "orderCancelled" && o.X != "Cancelled")
            || (o.e == "orderExpired" && o.X != "Expired")
        {
            return Err(BackpackAccountError::InvalidField("event status"));
        }
        // USER is the sole nonsystem origin. Unknown system origins never become locally owned.
        let system = (o.O != "USER").then(|| o.O.clone());
        let order = BackpackOrder {
            id: o.i.clone(),
            client_id: o.c,
            created_at: BackpackOrderCreatedAt::Resting(0),
            executed_quantity: Some(o.z),
            executed_quote_quantity: Some(o.Z),
            expiry_reason: None,
            order_type: match o.o.as_str() {
                "LIMIT" => "Limit",
                "MARKET" => "Market",
                other => other,
            }
            .into(),
            post_only: o.y,
            price: o.p,
            quantity: o.q,
            quote_quantity: o.Q,
            reduce_only: Some(o.r),
            self_trade_prevention: o.V,
            status: o.X,
            side: o.S.clone(),
            symbol: o.s.clone(),
            time_in_force: o.f,
            system_order_type: system.clone(),
            extra: o.extra,
        };
        let converted = order_report(order, context)?;
        observation.gaps.extend(converted.gaps);
        if let Some(mut report) = converted.report {
            report.ts_last = ts;
            if o.e == "orderAccepted" {
                report.ts_accepted = ts;
            }
            observation
                .facts
                .push(BackpackPrivateFact::Order(Box::new(report)));
        }
        if o.e == "orderFill" {
            let required = || BackpackAccountError::InvalidField("true fill fields");
            let trade = o.t.ok_or_else(required)?;
            let micro = i64::try_from(o.T).map_err(|_| required())?;
            let timestamp = jiff::Timestamp::from_microsecond(micro)
                .map_err(|_| required())?
                .to_zoned(jiff::tz::TimeZone::UTC)
                .datetime()
                .to_string();
            let fill = BackpackFill {
                client_id: o.c.map(|c| c.to_string()),
                fee: o.n.ok_or_else(required)?,
                fee_symbol: o.N.ok_or_else(required)?,
                is_maker: o.m.ok_or_else(required)?,
                order_id: o.i,
                price: o.L.ok_or_else(required)?,
                quantity: o.l.ok_or_else(required)?,
                side: o.S,
                symbol: o.s,
                system_order_type: system,
                timestamp,
                trade_id: Some(trade),
                extra: BTreeMap::new(),
            };
            let converted = fill_report(fill, context)?;
            observation.gaps.extend(converted.gaps);
            let report = converted.report.ok_or_else(required)?;
            observation
                .facts
                .push(BackpackPrivateFact::Fill(Box::new(report)));
        }
    } else if topic == "account.positionUpdate" || topic.starts_with("account.positionUpdate.") {
        let p: Position = serde_json::from_value(raw).map_err(|_| BackpackAccountError::Decode)?;
        validate_topic(&topic, "account.positionUpdate", &p.s, context)?;
        unix_microseconds_to_nanos(p.E)
            .map_err(|_| BackpackAccountError::InvalidField("event time"))?;
        let ts = unix_microseconds_to_nanos(p.T)
            .map_err(|_| BackpackAccountError::InvalidField("engine time"))?;
        if p.e
            .as_deref()
            .is_some_and(|e| !["positionAdjusted", "positionOpened", "positionClosed"].contains(&e))
            || (p.e.as_deref() == Some("positionClosed") && p.q.0 != Decimal::ZERO)
        {
            return Err(BackpackAccountError::InvalidField("position event"));
        }
        let meta = context
            .instruments
            .all()
            .find(|m| m.raw_symbol.as_str() == p.s)
            .ok_or(BackpackAccountError::Unsupported("instrument"))?;
        let side = if p.q.0 > Decimal::ZERO {
            PositionSide::Long
        } else if p.q.0 < Decimal::ZERO {
            PositionSide::Short
        } else {
            PositionSide::Flat
        };
        let quantity = exact_quantity(p.q.0.abs(), meta.size_increment.precision)
            .map_err(|_| BackpackAccountError::InvalidField("position quantity"))?;
        let id = PositionId::new_checked(&p.i)
            .map_err(|_| BackpackAccountError::InvalidField("position ID"))?;
        let report = PositionStatusReport::new(
            context.account_id,
            meta.instrument_id,
            side,
            quantity,
            ts,
            context.ts_init,
            None,
            Some(id),
            Some(p.B.0),
        );
        // Deprecated l is not a liquidation price. Other financial fields are exact but
        // cannot establish usable margin, authenticated identity or snapshot coverage.
        let _financial = (p.b, p.f, p.M, p.m, p.Q, p.n, p.p, p.P, p.l);
        if !p.extra.is_empty() {
            observation.gaps.insert(BackpackEvidenceGap::UnknownFields);
        }
        observation
            .facts
            .push(BackpackPrivateFact::Position(Box::new(report)));
    } else {
        return Err(BackpackAccountError::Unsupported("private topic"));
    }
    Ok(observation)
}
fn validate_topic(
    topic: &str,
    prefix: &str,
    symbol: &str,
    context: &BackpackReportContext<'_>,
) -> Result<InstrumentId, BackpackAccountError> {
    if topic != prefix && topic != format!("{prefix}.{symbol}") {
        return Err(BackpackAccountError::InvalidField("topic identity"));
    }
    context
        .instruments
        .all()
        .find(|m| m.raw_symbol.as_str() == symbol)
        .map(|m| m.instrument_id)
        .ok_or(BackpackAccountError::Unsupported("instrument"))
}
