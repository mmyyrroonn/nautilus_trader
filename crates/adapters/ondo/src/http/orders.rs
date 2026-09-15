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

//! The order write surface: the create request, the order payload, and the two refusals that matter.
//!
//! # One serialization, written once
//!
//! [`OndoOrderCommand`] is the adapter's validated order command. It is built from a Nautilus
//! [`OrderInitialized`] *and validated locally*: an order this adapter cannot express is refused
//! here, before any request is created, with a named error that says what was unsupported
//! (plan §6.2). [`OndoOrderCommand::body`] is the exact JSON text a
//! `POST /v1/perps/orders` carries, and it is the same `Vec<u8>` the signature covers and the
//! transport sends - nothing re-encodes it.
//!
//! The wire form is the `AddOrderReq` member order the plan's §6.2 example fixes:
//!
//! ```text
//! {"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.01","price":"100.00",
//!  "timeInForce":"GTC","postOnly":true,"reduceOnly":false,"clientOrderId":"ondo_probe_1"}
//! ```
//!
//! A market order sends the base `size` and **no** `price` and **no** `timeInForce` (plan §6.2).
//! `size` and `price` are the venue-aligned decimal strings of the Nautilus domain types, so no
//! `f64` ever touches an order.
//!
//! # What is refused locally, and why it is refused here
//!
//! - **FOK** is not in the create schema's `timeInForce` enum (`GTC`/`IOC` only, `test_data/
//!   conflicts.md` conflict 3), even though the WebSocket order enum carries it. It is refused as
//!   [`OndoOrderError::UnsupportedTimeInForce`], never quietly downgraded to IOC or GTC.
//! - **`postOnly` on a market order** is refused: a market order takes, so a post-only market order
//!   is not a combination this venue supports. A failed post-only order is a *rejection*
//!   ([`ONDO_POST_ONLY_HAS_MATCH`]) and is never converted into a taker order (plan §6.2).
//! - **Conditional orders** (`stopMarket`, `takeProfitMarket`, and Nautilus's stop, if-touched and
//!   trailing families), `quoteSize`, iceberg `displayQty`, `GTD`/`DAY` and every other
//!   `timeInForce` than `GTC`/`IOC` are refused with a name.
//! - A `ClientOrderId` outside the allowed character set (alphanumeric, `_`, `-`, at most 64
//!   characters) is refused, never truncated.
//!
//! # The order payload
//!
//! [`OndoApiOrder`] reads the documented `ApiOrder` schema (`test_data/README.md`'s protocol table
//! records the create request and the fill schema; the order schema comes from the frozen REST spec
//! and the plan's §6.3 status table). Every decimal stays the string the venue sent, and the venue's
//! own `status` string is preserved verbatim - including a status this adapter does not know, which
//! is kept as [`OndoOrderStatus::Unknown`] rather than coerced (plan §6.3, "unknown status keeps its
//! raw data").

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    events::OrderInitialized,
    identifiers::{ClientOrderId, InstrumentId},
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

use crate::{
    common::parse::{instrument_id_to_market, parse_decimal, parse_timestamp},
    http::{
        error::{OndoHttpError, OndoHttpResult},
        query::{ORDERS_PATH, OndoRequestTarget, client_order_lookup, percent_encode},
    },
};

/// The native batch create endpoint (at most [`ONDO_MAX_BATCH_ORDERS`] items).
pub const ORDERS_BATCH_PATH: &str = "/v1/perps/orders/batch";

/// The venue's documented batch size cap (`BatchAddOrderReq.orders.maxItems`).
pub const ONDO_MAX_BATCH_ORDERS: usize = 20;

/// The longest client order id the venue accepts (`clientOrderId` max length, in characters).
pub const ONDO_MAX_CLIENT_ORDER_ID_LEN: usize = 64;

/// The error code a `postOnly` order that would match at placement is rejected with.
///
/// A 400 carrying this code is an [`OndoHttpError::RequestRejected`] whose `code` is this string.
/// The order is rejected - it is never re-sent as a taker order (plan §6.2).
pub const ONDO_POST_ONLY_HAS_MATCH: &str = "post_only_has_match";

/// Every cancel refusal the frozen REST spec documents, plus the plan's own spellings.
///
/// The frozen spec spells its two with one `l` (`order_already_canceled`,
/// `order_already_fully_filled`); plan §6.3 writes the first with two (`order_already_cancelled`) and
/// the second without `fully` (`order_already_filled`). All four are recognised: a cancel that
/// arrives after the fact is the *same race* whichever the venue prints, and either way it triggers a
/// confirming query instead of being read as "cancelled".
const CANCEL_ALREADY_FILLED: [&str; 2] = ["order_already_fully_filled", "order_already_filled"];
const CANCEL_ALREADY_CANCELED: [&str; 2] = ["order_already_canceled", "order_already_cancelled"];
const CANCEL_NOT_FOUND: [&str; 1] = ["order_not_found"];
/// The refusals that mean the order is not in a state the venue will cancel.
///
/// `trading_disabled` belongs here rather than in [`OndoCancelRejection::Other`]: it is a
/// *documented cancel refusal*, listed by the frozen spec in both cancel error-code enums
/// (`cancelOrderByID` and `batchCancelOrders`), so classifying it as "any other refusal" would
/// mean not querying on a code the venue does document. A refusal that means the order was
/// **not** cancelled is exactly the case the confirming query exists for.
///
/// `order_negative_size` is the opposite case: a *create* code that the spec lists on neither
/// cancel endpoint. It stays here deliberately. Were the venue ever to answer a cancel with it,
/// "not cancelable" - query the order - is the safe reading, whereas [`OndoCancelRejection::Other`]
/// skips the query. The conservative direction is to over-classify, not to fall through.
const CANCEL_NOT_CANCELABLE: [&str; 3] = [
    "order_not_in_cancelable_state",
    "order_negative_size",
    "trading_disabled",
];

/// The lookup value form for a client order id: `client:{clientOrderId}` (plan §6.2).
///
/// Re-exported here so the execution client does not need to reach into the query module for the
/// one rule that fixes the form.
#[must_use]
pub fn client_lookup_value(client_order_id: &str) -> String {
    client_order_lookup(client_order_id)
}

/// Returns the request target for an order lookup by a venue order id or a `client:` value.
///
/// The lookup is a **path segment**, so it is percent-encoded exactly as signed
/// ([`percent_encode`]): a colon becomes `%3A`, and the string the signature covers is the string
/// the transport sends (plan §6.1, §6.2).
#[must_use]
pub fn order_lookup_target(order_id: &str) -> OndoRequestTarget {
    OndoRequestTarget::new(&format!(
        "{ORDERS_PATH}/{}",
        percent_encode(order_id.trim())
    ))
}

/// Returns the request target for a client order id lookup: `GET /v1/perps/orders/client%3A{id}`.
#[must_use]
pub fn client_order_lookup_target(client_order_id: &str) -> OndoRequestTarget {
    order_lookup_target(&client_lookup_value(client_order_id))
}

/// Returns the per-market cancel target: `DELETE /v1/perps/orders?market=...` (plan §6.2).
#[must_use]
pub fn market_cancel_target(market: &str) -> OndoRequestTarget {
    OndoRequestTarget::new(ORDERS_PATH).with_query_param("market", market)
}

/// Returns the create target: `POST /v1/perps/orders`.
#[must_use]
pub fn create_order_target() -> OndoRequestTarget {
    OndoRequestTarget::new(ORDERS_PATH)
}

/// Returns the native batch create target: `POST /v1/perps/orders/batch`.
#[must_use]
pub fn batch_create_target() -> OndoRequestTarget {
    OndoRequestTarget::new(ORDERS_BATCH_PATH)
}

/// Why this adapter refuses an order locally.
///
/// Every variant names what was unsupported. There is no catch-all "invalid order": a caller that
/// sees one of these can tell FOK from a conditional order from a client order id that is too long.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OndoOrderError {
    /// The order type is outside this phase's create scope (`limit`, `market` only).
    #[error(
        "the Ondo Perps create schema has no `{order_type}` order: this adapter creates `limit` \
         and `market` orders only (plan §6.2), and a conditional order may not be emulated"
    )]
    UnsupportedOrderType {
        /// The refused Nautilus order type, as rendered.
        order_type: String,
    },
    /// The time in force is outside the create schema's `GTC`/`IOC` enum.
    #[error(
        "the Ondo Perps create schema (`GTC`/`IOC` only) has no `{time_in_force}` time in force; \
         FOK in particular is refused locally rather than downgraded (plan §6.2, conflict 3)"
    )]
    UnsupportedTimeInForce {
        /// The refused Nautilus time in force, as rendered.
        time_in_force: String,
    },
    /// A post-only market order is not a combination the venue supports.
    #[error("a `postOnly` market order is refused locally: a market order takes liquidity")]
    PostOnlyWithMarketOrder,
    /// A limit order carrying no price.
    #[error("a limit order without a price cannot be expressed by the Ondo Perps create schema")]
    MissingLimitPrice,
    /// A quote-denominated quantity (`quoteSize`) is out of this phase's scope.
    #[error(
        "a quote-denominated quantity (`quoteSize`) is not supported by this phase (plan §6.2)"
    )]
    QuoteQuantityUnsupported,
    /// An iceberg order's `displayQty` is out of this phase's scope.
    #[error("an iceberg `displayQty` is not supported by this phase (plan §6.2)")]
    DisplayQuantityUnsupported,
    /// A conditional order's trigger price is out of this phase's scope.
    #[error("a trigger price is not supported by this phase (plan §6.2)")]
    TriggerPriceUnsupported,
    /// An order that expires cannot be expressed: the create schema has no `GTD`.
    #[error("an order expiration cannot be expressed: the create schema has no `GTD` (plan §6.2)")]
    ExpireTimeUnsupported,
    /// The quantity is zero or negative.
    #[error("the order quantity `{quantity}` is not positive")]
    NonPositiveQuantity {
        /// The refused quantity, as rendered.
        quantity: String,
    },
    /// The instrument does not map onto an Ondo Perps market string.
    #[error("the instrument `{instrument_id}` does not map onto an Ondo Perps market: {reason}")]
    UnsupportedMarket {
        /// The refused instrument.
        instrument_id: InstrumentId,
        /// Why it does not map.
        reason: String,
    },
    /// The client order id is outside the venue's allowed charset or length.
    #[error(
        "the client order id `{value}` is refused: {reason} (the allowed form is alphanumeric plus \
         `_`/`-`, at most {ONDO_MAX_CLIENT_ORDER_ID_LEN} characters; it is never truncated)"
    )]
    InvalidClientOrderId {
        /// The refused value, verbatim.
        value: String,
        /// Why it was refused.
        reason: String,
    },
}

impl OndoOrderError {
    /// Returns the stable name of this refusal, for an `OrderRejected` reason and a report.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::UnsupportedOrderType { .. } => "unsupported_order_type",
            Self::UnsupportedTimeInForce { .. } => "unsupported_time_in_force",
            Self::PostOnlyWithMarketOrder => "post_only_with_market_order",
            Self::MissingLimitPrice => "missing_limit_price",
            Self::QuoteQuantityUnsupported => "quote_size_unsupported",
            Self::DisplayQuantityUnsupported => "display_quantity_unsupported",
            Self::TriggerPriceUnsupported => "trigger_price_unsupported",
            Self::ExpireTimeUnsupported => "expire_time_unsupported",
            Self::NonPositiveQuantity { .. } => "non_positive_quantity",
            Self::UnsupportedMarket { .. } => "unsupported_market",
            Self::InvalidClientOrderId { .. } => "invalid_client_order_id",
        }
    }
}

/// Validates a Nautilus client order id against the venue's allowed form.
///
/// The rule is the create schema's own: alphanumeric plus `_` and `-`, at most
/// [`ONDO_MAX_CLIENT_ORDER_ID_LEN`] characters. A value outside it is refused; it is **never**
/// truncated into a value the venue would accept (plan §6.2).
///
/// # Errors
///
/// Returns [`OndoOrderError::InvalidClientOrderId`] naming the value and the reason.
pub fn validate_client_order_id(client_order_id: &ClientOrderId) -> Result<(), OndoOrderError> {
    let value = client_order_id.as_str();
    let refuse = |reason: &str| {
        Err(OndoOrderError::InvalidClientOrderId {
            value: value.to_string(),
            reason: reason.to_string(),
        })
    };

    if value.is_empty() {
        return refuse("it is empty");
    }

    if value.chars().count() > ONDO_MAX_CLIENT_ORDER_ID_LEN {
        return refuse("it is longer than the venue's limit");
    }

    if let Some(character) = value
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return refuse(&format!(
            "it contains `{character}`, which the venue's charset does not allow"
        ));
    }

    Ok(())
}

/// A validated create-order command: the exact request this adapter will send, and nothing else.
///
/// Build one with [`Self::from_init`], which validates, or with [`Self::validate`] on a struct
/// literal a caller built itself. There is no way to obtain the body of an invalid command: the body
/// is produced only by [`Self::body`], which is reachable only through a validated value's
/// constructor. (The struct's members are public so a test or a later task can inspect what was
/// mapped, not so an unvalidated one can be sent.)
#[derive(Clone, Debug, PartialEq)]
pub struct OndoOrderCommand {
    /// The venue market string, as in `NVDA-USD.P`.
    pub market: String,
    /// The order side.
    pub side: OrderSide,
    /// The order type: `Limit` or `Market`.
    pub order_type: OrderType,
    /// The base-denominated order quantity.
    pub quantity: Quantity,
    /// The limit price, absent for a market order.
    pub price: Option<Price>,
    /// The order's time in force as the Nautilus order carried it.
    pub time_in_force: TimeInForce,
    /// Whether the order must provide liquidity.
    pub post_only: bool,
    /// Whether the order may only reduce an existing position.
    pub reduce_only: bool,
    /// The client order id the venue is given.
    pub client_order_id: ClientOrderId,
}

impl OndoOrderCommand {
    /// Maps a Nautilus [`OrderInitialized`] onto a validated Ondo Perps create command.
    ///
    /// # Errors
    ///
    /// Returns the named [`OndoOrderError`] for the first thing this phase does not support: an
    /// order type outside `limit`/`market`, a `timeInForce` outside `GTC`/`IOC` (FOK included), a
    /// post-only market order, a limit order with no price, `quoteSize`, an iceberg `displayQty`, a
    /// trigger price, an expiration, a non-positive quantity, an instrument that does not map onto a
    /// market, or a client order id outside the venue's charset.
    pub fn from_init(init: &OrderInitialized) -> Result<Self, OndoOrderError> {
        let market = instrument_id_to_market(&init.instrument_id).map_err(|error| {
            OndoOrderError::UnsupportedMarket {
                instrument_id: init.instrument_id,
                reason: error.to_string(),
            }
        })?;

        Self {
            market,
            side: init.order_side,
            order_type: init.order_type,
            quantity: init.quantity,
            price: init.price,
            time_in_force: init.time_in_force,
            post_only: init.post_only,
            reduce_only: init.reduce_only,
            client_order_id: init.client_order_id,
        }
        .validated(
            init.quote_quantity,
            init.display_qty,
            init.trigger_price,
            init.expire_time,
        )
    }

    /// Validates an already-mapped command.
    ///
    /// # Errors
    ///
    /// See [`Self::from_init`].
    pub fn validate(&self) -> Result<(), OndoOrderError> {
        self.check(None, None, None, None)
    }

    /// Validates the command with the members that live on the order event rather than on the
    /// command, and returns it.
    fn validated(
        self,
        quote_quantity: bool,
        display_qty: Option<Quantity>,
        trigger_price: Option<Price>,
        expire_time: Option<UnixNanos>,
    ) -> Result<Self, OndoOrderError> {
        self.check(
            Some(quote_quantity),
            display_qty,
            trigger_price,
            expire_time,
        )?;

        Ok(self)
    }

    /// The one validation, written once and reachable from both constructors.
    fn check(
        &self,
        quote_quantity: Option<bool>,
        display_qty: Option<Quantity>,
        trigger_price: Option<Price>,
        expire_time: Option<UnixNanos>,
    ) -> Result<(), OndoOrderError> {
        if quote_quantity == Some(true) {
            return Err(OndoOrderError::QuoteQuantityUnsupported);
        }

        if display_qty.is_some() {
            return Err(OndoOrderError::DisplayQuantityUnsupported);
        }

        if trigger_price.is_some() {
            return Err(OndoOrderError::TriggerPriceUnsupported);
        }

        if expire_time.is_some() {
            return Err(OndoOrderError::ExpireTimeUnsupported);
        }

        match self.order_type {
            OrderType::Limit | OrderType::Market => {}
            other => {
                return Err(OndoOrderError::UnsupportedOrderType {
                    order_type: other.to_string(),
                });
            }
        }

        if self.order_type == OrderType::Limit && self.price.is_none() {
            return Err(OndoOrderError::MissingLimitPrice);
        }

        if self.order_type == OrderType::Market && self.post_only {
            return Err(OndoOrderError::PostOnlyWithMarketOrder);
        }

        if !matches!(self.time_in_force, TimeInForce::Gtc | TimeInForce::Ioc) {
            return Err(OndoOrderError::UnsupportedTimeInForce {
                time_in_force: self.time_in_force.to_string(),
            });
        }

        if !self.quantity.is_positive() {
            return Err(OndoOrderError::NonPositiveQuantity {
                quantity: self.quantity.to_string(),
            });
        }

        validate_client_order_id(&self.client_order_id)
    }

    /// Returns the venue's `side` spelling: `buy` or `sell`.
    #[must_use]
    pub const fn side_str(&self) -> &'static str {
        match self.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
    }

    /// Returns the venue's `type` spelling: `limit` or `market`.
    #[must_use]
    pub const fn order_type_str(&self) -> &'static str {
        match self.order_type {
            OrderType::Market => "market",
            _ => "limit",
        }
    }

    /// Returns the base-denominated size as the venue's decimal string.
    #[must_use]
    pub fn size_str(&self) -> String {
        self.quantity.to_string()
    }

    /// Returns the limit price as the venue's decimal string, when the order has one.
    ///
    /// A market order returns [`None`]: the create schema's `price` member is for limit orders and
    /// a market order sends no price at all (plan §6.2).
    #[must_use]
    pub fn price_str(&self) -> Option<String> {
        if self.order_type == OrderType::Market {
            return None;
        }

        self.price.as_ref().map(ToString::to_string)
    }

    /// Returns the `timeInForce` the request carries, or [`None`] for a market order.
    ///
    /// A market order sends no `timeInForce` (plan §6.2): the request schema says the field "cannot
    /// be set for market orders" (defaulting to `GTC` otherwise), and the response schema says it is
    /// "not returned for market orders" - so the member is meaningless on both sides of the call.
    #[must_use]
    pub const fn time_in_force_str(&self) -> Option<&'static str> {
        match self.order_type {
            OrderType::Market => None,
            _ => Some(match self.time_in_force {
                TimeInForce::Ioc => "IOC",
                _ => "GTC",
            }),
        }
    }

    /// Returns the exact JSON body of `POST /v1/perps/orders`.
    ///
    /// The members are written in the order the plan's §6.2 example fixes, a market order omits
    /// `price` and `timeInForce`, and every decimal is the domain type's own string.
    ///
    /// # Panics
    ///
    /// Panics if `serde_json` cannot serialize this fixed shape of strings and booleans, which
    /// cannot happen for the members the type holds.
    #[must_use]
    pub fn body(&self) -> Vec<u8> {
        let raw = RawAddOrderRequest {
            market: &self.market,
            side: self.side_str(),
            order_type: self.order_type_str(),
            size: self.size_str(),
            price: self.price_str(),
            time_in_force: self.time_in_force_str(),
            post_only: self.post_only,
            reduce_only: self.reduce_only,
            client_order_id: self.client_order_id.as_str(),
        };

        serde_json::to_vec(&raw)
            .expect("the create-request shape of strings and booleans always serializes")
    }

    /// Returns the exact JSON text of the body, for a diagnostic or a report.
    ///
    /// # Panics
    ///
    /// See [`Self::body`].
    #[must_use]
    pub fn body_text(&self) -> String {
        String::from_utf8(self.body()).expect("the create-request body is UTF-8")
    }
}

/// The `AddOrderReq` members, in the wire order, with the optional two omitted when absent.
///
/// `size` and `price` are **owned** [`String`]s because both come from a decimal's `to_string()` and
/// therefore hold no borrow: `size_str` and `price_str` allocate a fresh string on every call, so a
/// `&'a str` field could only ever point at a temporary. In a single request that temporary would
/// happen to live long enough to serialize, but [`batch_body`] builds these inside an iterator
/// adapter, where the temporary is dropped as the closure returns and the borrow could not be
/// returned at all. Owning the two strings is what makes one shape work for both call sites, and it
/// changes nothing on the wire: serde emits the same decimal text either way.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RawAddOrderRequest<'a> {
    market: &'a str,
    side: &'a str,
    #[serde(rename = "type")]
    order_type: &'a str,
    size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<&'a str>,
    post_only: bool,
    reduce_only: bool,
    client_order_id: &'a str,
}

/// The `BatchAddOrderReq` envelope: `{"orders":[...]}`.
#[derive(Serialize)]
struct RawBatchAddOrderRequest<'a> {
    orders: &'a [RawAddOrderRequest<'a>],
}

/// Returns the exact JSON body of `POST /v1/perps/orders/batch` for `commands`.
///
/// The batch array keeps the caller's order, so a per-item result can be matched back to the request
/// that produced it (plan §6.2: each item is reported independently, and a 2xx does **not** mean
/// every item succeeded).
///
/// # Errors
///
/// Returns [`OndoOrderError::UnsupportedOrderType`]'s sibling for an empty batch or one past
/// [`ONDO_MAX_BATCH_ORDERS`]: the venue refuses both, and this adapter refuses them locally rather
/// than sending a request it knows will be rejected.
pub fn batch_body(commands: &[OndoOrderCommand]) -> Result<Vec<u8>, OndoBatchError> {
    if commands.is_empty() {
        return Err(OndoBatchError::Empty);
    }

    if commands.len() > ONDO_MAX_BATCH_ORDERS {
        return Err(OndoBatchError::TooMany {
            count: commands.len(),
        });
    }

    for command in commands {
        command.validate().map_err(|source| OndoBatchError::Item {
            client_order_id: command.client_order_id,
            source,
        })?;
    }

    let raw: Vec<RawAddOrderRequest<'_>> = commands
        .iter()
        .map(|command| RawAddOrderRequest {
            market: &command.market,
            side: command.side_str(),
            order_type: command.order_type_str(),
            size: command.size_str(),
            price: command.price_str(),
            time_in_force: command.time_in_force_str(),
            post_only: command.post_only,
            reduce_only: command.reduce_only,
            client_order_id: command.client_order_id.as_str(),
        })
        .collect();

    Ok(
        serde_json::to_vec(&RawBatchAddOrderRequest { orders: &raw })
            .expect("the batch shape of strings and booleans always serializes"),
    )
}

/// Why a batch of create commands cannot be sent.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OndoBatchError {
    /// The batch carried no orders; the venue answers 400 `batch_order_empty`.
    #[error("a batch order request must carry at least one order")]
    Empty,
    /// The batch carried more orders than the venue accepts; it answers 400
    /// `batch_order_too_many_orders`.
    #[error(
        "the batch carries {count} orders; the venue accepts at most {ONDO_MAX_BATCH_ORDERS} \
         (400 `batch_order_too_many_orders`)"
    )]
    TooMany {
        /// How many orders the batch carried.
        count: usize,
    },
    /// One item of the batch is an order this adapter refuses locally.
    #[error("batch item `{client_order_id}` is refused locally: {source}")]
    Item {
        /// The refused item's client order id.
        client_order_id: ClientOrderId,
        /// The local refusal.
        #[source]
        source: OndoOrderError,
    },
}

/// The venue's own order side spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoSide {
    /// `buy`.
    Buy,
    /// `sell`.
    Sell,
}

impl OndoSide {
    /// Reads the venue's `side` member.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::InvalidField`] for anything but `buy`/`sell`, with the raw value.
    pub fn from_raw(raw: &str) -> OndoHttpResult<Self> {
        match raw {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            _ => Err(OndoHttpError::InvalidField {
                context: "an ApiOrder".to_string(),
                field: "side",
                value: raw.to_string(),
                reason: "the venue's order sides are `buy` and `sell`".to_string(),
            }),
        }
    }

    /// Returns the Nautilus order side.
    #[must_use]
    pub const fn to_order_side(self) -> OrderSide {
        match self {
            Self::Buy => OrderSide::Buy,
            Self::Sell => OrderSide::Sell,
        }
    }

    /// Returns the venue's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

/// The venue's own order status, with an unreadable one kept verbatim.
///
/// plan §6.3: `pending` stays pending, `open` is working, `fullyfilled` is terminal only once the
/// fills agree, `canceled` ends the remainder, and an **unknown** status keeps its raw string and
/// prevents the account from being judged clean. That last property is why
/// [`Self::is_terminal`] answers `false` for [`Self::Unknown`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OndoOrderStatus {
    /// `pending`: accepted by the venue but not yet working. No fill may be generated here.
    Pending,
    /// `open`: working at the venue.
    Open,
    /// `fullyfilled`: the venue reports the order complete.
    FullyFilled,
    /// `canceled`: the venue reports the remainder ended.
    Canceled,
    /// `untriggered`: a conditional order this adapter did not create.
    Untriggered,
    /// A status this adapter does not know, kept as the venue spelled it.
    Unknown(String),
}

impl OndoOrderStatus {
    /// Reads the venue's `status` member. An unknown spelling is preserved, never coerced.
    ///
    /// The six arms below are exactly the spec's `ApiOrder.status` enum, and nothing else is
    /// accepted. In particular the two-`l` `cancelled` is **not** read as [`Self::Canceled`]: that
    /// spelling is the *plan's*, in its discussion of the `order_already_cancelled` error code
    /// (plan §6.3), and it belongs to the error-code vocabulary rather than to this field - the
    /// status enum spells it `canceled` alone. Tolerating it here would let a spelling the venue
    /// does not use land on a terminal, "confirmed end" state, which is the reading
    /// [`Self::is_terminal`] denies to everything it does not recognise.
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        match raw {
            "pending" => Self::Pending,
            "open" => Self::Open,
            "fullyfilled" => Self::FullyFilled,
            "canceled" => Self::Canceled,
            "untriggered" => Self::Untriggered,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// Returns the venue's spelling, which for [`Self::Unknown`] is the raw string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Open => "open",
            Self::FullyFilled => "fullyfilled",
            Self::Canceled => "canceled",
            Self::Untriggered => "untriggered",
            Self::Unknown(raw) => raw,
        }
    }

    /// Returns whether this status is a venue-confirmed end state.
    ///
    /// An unknown status is **not** terminal: nothing about it has been confirmed.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::FullyFilled | Self::Canceled)
    }

    /// Returns whether this status is one this adapter understands.
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown(_))
    }
}

/// The recorded `ApiOrder` fields this adapter reads, with every decimal still a string.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawApiOrder {
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    #[serde(default)]
    parent_order_id: Option<String>,
    side: String,
    market: String,
    size: String,
    #[serde(default)]
    price: Option<String>,
    filled_size: String,
    #[serde(default)]
    last_fill_size: Option<String>,
    #[serde(default)]
    filled_cost: Option<String>,
    #[serde(default)]
    fee: Option<String>,
    #[serde(default)]
    fee_rebate: Option<String>,
    #[serde(default)]
    realized_pnl: Option<String>,
    status: String,
    created_at: String,
    #[serde(default)]
    filled_at: Option<String>,
    #[serde(default)]
    canceled_at: Option<String>,
    #[serde(default)]
    cancel_reason: Option<String>,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(default)]
    time_in_force: Option<String>,
    #[serde(default)]
    reduce_only: Option<bool>,
    #[serde(default)]
    liquidation_id: Option<String>,
    #[serde(default)]
    close_position: Option<bool>,
    #[serde(default)]
    stop_order_type: Option<String>,
    #[serde(default)]
    trigger_price: Option<String>,
}

/// One `ApiOrder`: the create answer, the lookup answer and the cancel answer are all this shape.
///
/// The type keeps the venue's text - `raw` is the exact JSON of the order - so a status, a member or
/// a decimal this adapter does not act on is still available to a report or to a later task. Its
/// accessors convert exactly, through [`parse_decimal`] and [`parse_timestamp`].
///
/// The equality this type carries is **the whole payload**, `raw` included: two `ApiOrder`s are
/// equal when the venue sent the same bytes, which is what makes one of them the other's duplicate
/// rather than a second update. Nothing here compares a subset of the members, because a member this
/// adapter does not read is still a member the venue may have changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoApiOrder {
    raw: String,
    order_id: String,
    client_order_id: Option<String>,
    parent_order_id: Option<String>,
    side: OndoSide,
    market: String,
    size: String,
    price: Option<String>,
    filled_size: String,
    last_fill_size: Option<String>,
    filled_cost: Option<String>,
    fee: Option<String>,
    fee_rebate: Option<String>,
    realized_pnl: Option<String>,
    status: OndoOrderStatus,
    created_at: String,
    filled_at: Option<String>,
    canceled_at: Option<String>,
    cancel_reason: Option<String>,
    order_type: String,
    time_in_force: Option<String>,
    reduce_only: Option<bool>,
    liquidation_id: Option<String>,
    close_position: Option<bool>,
    stop_order_type: Option<String>,
    trigger_price: Option<String>,
}

impl OndoApiOrder {
    /// Reads one order from its exact JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the text is not an `ApiOrder`. That includes a
    /// payload **missing a member this adapter cannot do without** (`orderId`, `market`, `side`,
    /// `size`, `filledSize`, `status`, `createdAt`, `type`): the deserializer refuses it and names
    /// the member inside the decode error, so this function never returns
    /// [`OndoHttpError::MissingField`]. Returns [`OndoHttpError::InvalidField`] when `side` carries
    /// a spelling that is not `buy` or `sell`. The members it can read without are optional.
    pub fn from_text(text: &str) -> OndoHttpResult<Self> {
        let raw: RawApiOrder = serde_json::from_str(text)
            .map_err(|error| OndoHttpError::Decode(format!("not an ApiOrder: {error}")))?;

        let side = OndoSide::from_raw(&raw.side)?;

        Ok(Self {
            raw: text.to_string(),
            order_id: raw.order_id,
            client_order_id: raw.client_order_id,
            parent_order_id: raw.parent_order_id,
            side,
            market: raw.market,
            size: raw.size,
            price: raw.price,
            filled_size: raw.filled_size,
            last_fill_size: raw.last_fill_size,
            filled_cost: raw.filled_cost,
            fee: raw.fee,
            fee_rebate: raw.fee_rebate,
            realized_pnl: raw.realized_pnl,
            status: OndoOrderStatus::from_raw(&raw.status),
            created_at: raw.created_at,
            filled_at: raw.filled_at,
            canceled_at: raw.canceled_at,
            cancel_reason: raw.cancel_reason,
            order_type: raw.order_type,
            time_in_force: raw.time_in_force,
            reduce_only: raw.reduce_only,
            liquidation_id: raw.liquidation_id,
            close_position: raw.close_position,
            stop_order_type: raw.stop_order_type,
            trigger_price: raw.trigger_price,
        })
    }

    /// Returns the exact JSON text the venue sent for this order.
    #[must_use]
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the venue's order id.
    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    /// Returns the client order id the venue echoed, when it echoed one.
    #[must_use]
    pub fn client_order_id(&self) -> Option<&str> {
        self.client_order_id.as_deref()
    }

    /// Returns the parent order id, when the venue sent one (a TWAP child, for example).
    #[must_use]
    pub fn parent_order_id(&self) -> Option<&str> {
        self.parent_order_id.as_deref()
    }

    /// Returns the order side.
    #[must_use]
    pub const fn side(&self) -> OndoSide {
        self.side
    }

    /// Returns the venue's market string, as in `NVDA-USD.P`.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the order size as the venue's decimal string.
    #[must_use]
    pub fn size(&self) -> &str {
        &self.size
    }

    /// Returns the limit price as the venue's decimal string, when it sent one.
    #[must_use]
    pub fn price(&self) -> Option<&str> {
        self.price.as_deref()
    }

    /// Returns the filled size as the venue's decimal string.
    #[must_use]
    pub fn filled_size(&self) -> &str {
        &self.filled_size
    }

    /// Returns the last fill size as the venue's decimal string, when it sent one.
    ///
    /// Informational only (plan §6.3): it must never generate a fill of its own alongside the fill
    /// stream.
    #[must_use]
    pub fn last_fill_size(&self) -> Option<&str> {
        self.last_fill_size.as_deref()
    }

    /// Returns the filled cost as the venue's decimal string, when it sent one.
    #[must_use]
    pub fn filled_cost(&self) -> Option<&str> {
        self.filled_cost.as_deref()
    }

    /// Returns the order's cumulative fee as the venue's decimal string, when it sent one.
    ///
    /// The adapter does not add this to the per-fill fees; see
    /// [`crate::execution::OndoOrderState::cumulative_fee`].
    #[must_use]
    pub fn fee(&self) -> Option<&str> {
        self.fee.as_deref()
    }

    /// Returns the order's fee rebate as the venue's decimal string, when it sent one.
    #[must_use]
    pub fn fee_rebate(&self) -> Option<&str> {
        self.fee_rebate.as_deref()
    }

    /// Returns the order's realized pnl as the venue's decimal string, when it sent one.
    #[must_use]
    pub fn realized_pnl(&self) -> Option<&str> {
        self.realized_pnl.as_deref()
    }

    /// Returns the venue's status, verbatim for a status this adapter does not know.
    #[must_use]
    pub const fn status(&self) -> &OndoOrderStatus {
        &self.status
    }

    /// Returns the creation time as the venue's string. Its parsing is [`Self::ts_created`].
    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    /// Returns the fill time as the venue's string, when the order filled.
    #[must_use]
    pub fn filled_at(&self) -> Option<&str> {
        self.filled_at.as_deref()
    }

    /// Returns the cancellation time as the venue's string, when the order was cancelled.
    #[must_use]
    pub fn canceled_at(&self) -> Option<&str> {
        self.canceled_at.as_deref()
    }

    /// Returns the venue's cancel reason, when it sent one.
    #[must_use]
    pub fn cancel_reason(&self) -> Option<&str> {
        self.cancel_reason.as_deref()
    }

    /// Returns the venue's `type` member.
    #[must_use]
    pub fn order_type(&self) -> &str {
        &self.order_type
    }

    /// Returns the venue's `timeInForce` member, absent for a market order.
    #[must_use]
    pub fn time_in_force(&self) -> Option<&str> {
        self.time_in_force.as_deref()
    }

    /// Returns the venue's `reduceOnly` flag, when it sent one.
    #[must_use]
    pub const fn reduce_only(&self) -> Option<bool> {
        self.reduce_only
    }

    /// Returns the venue's liquidation id, when the order is a liquidation order.
    #[must_use]
    pub fn liquidation_id(&self) -> Option<&str> {
        self.liquidation_id.as_deref()
    }

    /// Returns whether the venue says this is a position-level TP/SL order.
    #[must_use]
    pub const fn close_position(&self) -> Option<bool> {
        self.close_position
    }

    /// Returns the venue's stop-order type, when the order is a conditional one.
    #[must_use]
    pub fn stop_order_type(&self) -> Option<&str> {
        self.stop_order_type.as_deref()
    }

    /// Returns the venue's trigger price, when the order is a conditional one.
    #[must_use]
    pub fn trigger_price(&self) -> Option<&str> {
        self.trigger_price.as_deref()
    }

    /// Returns the creation time in nanoseconds.
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's `createdAt` is not an RFC 3339 timestamp.
    pub fn ts_created(&self) -> anyhow::Result<UnixNanos> {
        parse_timestamp(&self.created_at)
    }

    /// Returns the fill time in nanoseconds, when the order carried one.
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's `filledAt` is not an RFC 3339 timestamp.
    pub fn ts_filled(&self) -> anyhow::Result<Option<UnixNanos>> {
        self.filled_at.as_deref().map(parse_timestamp).transpose()
    }

    /// Returns the cancellation time in nanoseconds, when the order carried one.
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's `canceledAt` is not an RFC 3339 timestamp.
    pub fn ts_canceled(&self) -> anyhow::Result<Option<UnixNanos>> {
        self.canceled_at.as_deref().map(parse_timestamp).transpose()
    }

    /// Returns the order size as an exact [`Quantity`].
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's `size` is not an exact decimal or does not fit.
    pub fn quantity(&self) -> anyhow::Result<Quantity> {
        let value = parse_decimal(&self.size, "size")?;

        Quantity::from_decimal(value)
            .map_err(|error| anyhow::anyhow!("size `{}`: {error}", self.size))
    }

    /// Returns the filled size as an exact [`Quantity`].
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's `filledSize` is not an exact decimal or does not fit.
    pub fn filled_quantity(&self) -> anyhow::Result<Quantity> {
        let value = parse_decimal(&self.filled_size, "filledSize")?;

        Quantity::from_decimal(value)
            .map_err(|error| anyhow::anyhow!("filledSize `{}`: {error}", self.filled_size))
    }
}

/// One item of a batch create the venue did not add, with the venue's own reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoRejectedOrder {
    raw: String,
    client_order_id: Option<String>,
    market: Option<String>,
    error: Option<String>,
    error_code: Option<String>,
}

impl OndoRejectedOrder {
    /// Returns the client order id the venue echoed for the refused item, when it echoed one.
    ///
    /// This is how a failed batch item is attributed back to the command that produced it; an item
    /// that carries none is reported unattributed rather than dropped.
    #[must_use]
    pub fn client_order_id(&self) -> Option<&str> {
        self.client_order_id.as_deref()
    }

    /// Returns the market the refused item referred to, when the venue echoed one.
    #[must_use]
    pub fn market(&self) -> Option<&str> {
        self.market.as_deref()
    }

    /// Returns the venue's human-readable error, when it sent one.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Returns the venue's semantic error code, when it sent one.
    #[must_use]
    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    /// Returns the exact JSON text of this refused item.
    #[must_use]
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Reads one refused batch item from its exact JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the text is not an `ErroredAddOrderReq`.
    pub fn from_text(text: &str) -> OndoHttpResult<Self> {
        /// The `ErroredAddOrderReq` members this adapter reads. `order` is the refused `AddOrderReq`
        /// itself, so its client order id and market are read from inside it.
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawErroredAddOrderReq {
            #[serde(default)]
            order: Option<RawRefusedOrder>,
            #[serde(default)]
            error: Option<String>,
            #[serde(default)]
            error_code: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawRefusedOrder {
            #[serde(default)]
            client_order_id: Option<String>,
            #[serde(default)]
            market: Option<String>,
        }

        let raw: RawErroredAddOrderReq = serde_json::from_str(text).map_err(|error| {
            OndoHttpError::Decode(format!("not an ErroredAddOrderReq: {error}"))
        })?;

        Ok(Self {
            raw: text.to_string(),
            client_order_id: raw
                .order
                .as_ref()
                .and_then(|order| order.client_order_id.clone()),
            market: raw.order.as_ref().and_then(|order| order.market.clone()),
            error: raw.error,
            error_code: raw.error_code,
        })
    }
}

/// The `BatchAddOrderRes`: the added orders, the refused ones, and nothing assumed in between.
///
/// A 2xx answer carrying both is a legitimate answer: "HTTP 2xx does not mean every item succeeded"
/// (plan §6.2).
#[derive(Clone, Debug)]
pub struct OndoBatchAddOrderResponse {
    added: Vec<OndoApiOrder>,
    failed: Vec<OndoRejectedOrder>,
}

impl OndoBatchAddOrderResponse {
    /// Reads a batch answer from its exact JSON text (the `result` of the envelope).
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the text is not a `BatchAddOrderRes`.
    pub fn from_text(text: &str) -> OndoHttpResult<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawBatchAddOrderRes {
            #[serde(default)]
            added_orders: Vec<Box<serde_json::value::RawValue>>,
            #[serde(default)]
            failed_orders: Vec<Box<serde_json::value::RawValue>>,
        }

        let raw: RawBatchAddOrderRes = serde_json::from_str(text)
            .map_err(|error| OndoHttpError::Decode(format!("not a BatchAddOrderRes: {error}")))?;

        let added = raw
            .added_orders
            .iter()
            .map(|item| OndoApiOrder::from_text(item.get()))
            .collect::<OndoHttpResult<Vec<_>>>()?;
        let failed = raw
            .failed_orders
            .iter()
            .map(|item| OndoRejectedOrder::from_text(item.get()))
            .collect::<OndoHttpResult<Vec<_>>>()?;

        Ok(Self { added, failed })
    }

    /// Returns the orders the venue added.
    #[must_use]
    pub fn added(&self) -> &[OndoApiOrder] {
        &self.added
    }

    /// Returns the orders the venue refused.
    #[must_use]
    pub fn failed(&self) -> &[OndoRejectedOrder] {
        &self.failed
    }
}

/// How a cancel request was refused, when it was.
///
/// Every one of these means "ask the venue again", never "the order is cancelled": the cancel API
/// being called is not a terminal state (plan §6.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoCancelRejection {
    /// The order is already fully filled (`order_already_fully_filled` / `order_already_filled`).
    AlreadyFilled,
    /// The order is already cancelled (`order_already_canceled` / `order_already_cancelled`).
    AlreadyCanceled,
    /// The venue does not know the order (`order_not_found`).
    NotFound,
    /// The order is in a state the venue will not cancel (`order_not_in_cancelable_state`, ...).
    NotCancelable,
    /// Any other refusal, which the caller keeps verbatim.
    Other,
}

impl OndoCancelRejection {
    /// Classifies a `RequestRejected` error code.
    ///
    /// Both of the frozen spec's one-`l` spellings and the plan's two-`l` spellings are recognised,
    /// so either a documented or a plan-spelled code triggers the confirming query.
    #[must_use]
    pub fn from_code(code: Option<&str>) -> Self {
        let Some(code) = code else {
            return Self::Other;
        };

        if CANCEL_ALREADY_FILLED.contains(&code) {
            return Self::AlreadyFilled;
        }

        if CANCEL_ALREADY_CANCELED.contains(&code) {
            return Self::AlreadyCanceled;
        }

        if CANCEL_NOT_FOUND.contains(&code) {
            return Self::NotFound;
        }

        if CANCEL_NOT_CANCELABLE.contains(&code) {
            return Self::NotCancelable;
        }

        Self::Other
    }

    /// Returns whether this refusal obliges the adapter to query the order before it reports
    /// anything about it.
    #[must_use]
    pub const fn requires_query(self) -> bool {
        matches!(
            self,
            Self::AlreadyFilled | Self::AlreadyCanceled | Self::NotFound | Self::NotCancelable
        )
    }

    /// Returns a stable name for a log line or a report.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AlreadyFilled => "order_already_filled",
            Self::AlreadyCanceled => "order_already_canceled",
            Self::NotFound => "order_not_found",
            Self::NotCancelable => "order_not_in_cancelable_state",
            Self::Other => "cancel_rejected",
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::parse::parse_timestamp;

    /// The plan's §6.2 example thread, as an `OrderInitialized`.
    fn initialized(
        order_type: OrderType,
        time_in_force: TimeInForce,
        post_only: bool,
        reduce_only: bool,
    ) -> OrderInitialized {
        use nautilus_model::identifiers::{StrategyId, TraderId};

        OrderInitialized::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-001"),
            InstrumentId::from("NVDA-USD-PERP.ONDO"),
            ClientOrderId::from("ondo_probe_example_1"),
            OrderSide::Buy,
            order_type,
            Quantity::from("0.01"),
            time_in_force,
            post_only,
            reduce_only,
            false,
            false,
            nautilus_core::UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(Price::from("100.00")),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[rstest]
    fn test_a_limit_gtc_post_only_create_request_is_the_plans_serialization() {
        let command = OndoOrderCommand::from_init(&initialized(
            OrderType::Limit,
            TimeInForce::Gtc,
            true,
            false,
        ))
        .expect("the plan's own example is a supported combination");

        assert_eq!(
            command.body_text(),
            r#"{"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.01","price":"100.00","timeInForce":"GTC","postOnly":true,"reduceOnly":false,"clientOrderId":"ondo_probe_example_1"}"#,
        );
    }

    #[rstest]
    fn test_a_market_order_sends_the_base_size_and_neither_price_nor_time_in_force() {
        let mut init = initialized(OrderType::Market, TimeInForce::Ioc, false, true);
        init.price = None;

        let command = OndoOrderCommand::from_init(&init).expect("a market IOC order is supported");

        assert_eq!(
            command.body_text(),
            r#"{"market":"NVDA-USD.P","side":"buy","type":"market","size":"0.01","postOnly":false,"reduceOnly":true,"clientOrderId":"ondo_probe_example_1"}"#,
        );
    }

    #[rstest]
    fn test_a_limit_ioc_order_carries_ioc() {
        let command = OndoOrderCommand::from_init(&initialized(
            OrderType::Limit,
            TimeInForce::Ioc,
            false,
            false,
        ))
        .unwrap();

        assert!(command.body_text().contains(r#""timeInForce":"IOC""#));
        assert!(command.body_text().contains(r#""price":"100.00""#));
    }

    #[rstest]
    fn test_the_create_target_and_the_lookup_targets_are_the_documented_ones() {
        assert_eq!(create_order_target().as_str(), ORDERS_PATH);
        assert_eq!(batch_create_target().as_str(), ORDERS_BATCH_PATH);
        assert_eq!(
            order_lookup_target("70a37d8f972f2494837f9dba8364cbb4").as_str(),
            "/v1/perps/orders/70a37d8f972f2494837f9dba8364cbb4",
        );
        assert_eq!(
            client_order_lookup_target("ondo_probe_example_1").as_str(),
            "/v1/perps/orders/client%3Aondo_probe_example_1",
            "the `client:` lookup is a path segment, percent-encoded exactly as signed",
        );
        assert_eq!(
            market_cancel_target("NVDA-USD.P").as_str(),
            "/v1/perps/orders?market=NVDA-USD.P",
        );
    }

    #[rstest]
    fn test_the_client_order_lookup_value_is_the_documented_client_form() {
        assert_eq!(
            client_lookup_value("ondo_probe_example_1"),
            "client:ondo_probe_example_1",
        );
    }

    #[rstest]
    #[case::fok(TimeInForce::Fok, "unsupported_time_in_force")]
    #[case::gtd(TimeInForce::Gtd, "unsupported_time_in_force")]
    #[case::day(TimeInForce::Day, "unsupported_time_in_force")]
    #[case::at_the_open(TimeInForce::AtTheOpen, "unsupported_time_in_force")]
    fn test_an_unsupported_time_in_force_is_refused_locally_by_name(
        #[case] time_in_force: TimeInForce,
        #[case] expected: &str,
    ) {
        let init = initialized(OrderType::Limit, time_in_force, false, false);
        let error = OndoOrderCommand::from_init(&init).expect_err("the schema has GTC/IOC only");

        assert_eq!(error.name(), expected);
        assert!(matches!(
            error,
            OndoOrderError::UnsupportedTimeInForce { .. }
        ));
    }

    #[rstest]
    #[case::fok(TimeInForce::Fok)]
    fn test_fok_is_refused_even_though_the_ws_enum_carries_it(#[case] time_in_force: TimeInForce) {
        let init = initialized(OrderType::Market, time_in_force, false, false);

        // A market order sends no `timeInForce`, but FOK is still not a member this adapter may
        // accept: the model must not carry a time in force the venue cannot express.
        assert!(
            OndoOrderCommand::from_init(&init).is_err(),
            "FOK must be refused locally on every order type",
        );
    }

    #[rstest]
    #[case::stop_market(OrderType::StopMarket)]
    #[case::stop_limit(OrderType::StopLimit)]
    #[case::market_if_touched(OrderType::MarketIfTouched)]
    #[case::limit_if_touched(OrderType::LimitIfTouched)]
    #[case::trailing_stop_market(OrderType::TrailingStopMarket)]
    fn test_an_out_of_scope_order_type_is_refused_by_name(#[case] order_type: OrderType) {
        let init = initialized(order_type, TimeInForce::Gtc, false, false);
        let error = OndoOrderCommand::from_init(&init).expect_err("only limit and market exist");

        assert_eq!(error.name(), "unsupported_order_type");
        assert!(error.to_string().contains(&order_type.to_string()));
    }

    #[rstest]
    fn test_a_post_only_market_order_is_refused_and_never_converted() {
        let init = initialized(OrderType::Market, TimeInForce::Ioc, true, false);
        let error = OndoOrderCommand::from_init(&init).expect_err("a market order takes liquidity");

        assert_eq!(error, OndoOrderError::PostOnlyWithMarketOrder);
        assert_eq!(error.name(), "post_only_with_market_order");
    }

    #[rstest]
    fn test_a_limit_order_without_a_price_is_refused() {
        let mut init = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        init.price = None;

        assert_eq!(
            OndoOrderCommand::from_init(&init).expect_err("no price"),
            OndoOrderError::MissingLimitPrice,
        );
    }

    #[rstest]
    fn test_the_model_members_this_phase_does_not_support_are_refused_by_name() {
        let mut quote = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        quote.quote_quantity = true;
        assert_eq!(
            OndoOrderCommand::from_init(&quote).expect_err("quoteSize"),
            OndoOrderError::QuoteQuantityUnsupported,
        );

        let mut display = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        display.display_qty = Some(Quantity::from("0.005"));
        assert_eq!(
            OndoOrderCommand::from_init(&display).expect_err("displayQty"),
            OndoOrderError::DisplayQuantityUnsupported,
        );

        let mut conditional = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        conditional.trigger_price = Some(Price::from("99.00"));
        assert_eq!(
            OndoOrderCommand::from_init(&conditional).expect_err("a trigger price"),
            OndoOrderError::TriggerPriceUnsupported,
        );

        let mut expiring = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        expiring.expire_time = Some(UnixNanos::from(1_000_000_000));
        assert_eq!(
            OndoOrderCommand::from_init(&expiring).expect_err("a GTD expiration"),
            OndoOrderError::ExpireTimeUnsupported,
        );
    }

    #[rstest]
    fn test_a_non_positive_quantity_is_refused() {
        let mut init = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        init.quantity = Quantity::from("0");

        assert_eq!(
            OndoOrderCommand::from_init(&init).expect_err("a zero quantity"),
            OndoOrderError::NonPositiveQuantity {
                quantity: "0".to_string()
            },
        );
    }

    #[rstest]
    #[case::too_long("a".repeat(65), "longer")]
    #[case::space("ondo probe", "contains")]
    #[case::dot("ondo.probe", "contains")]
    #[case::colon("client:1", "contains")]
    fn test_a_client_order_id_outside_the_charset_is_refused_never_truncated(
        #[case] value: String,
        #[case] expected_reason: &str,
    ) {
        let mut init = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        init.client_order_id = ClientOrderId::from(value.as_str());

        let error = OndoOrderCommand::from_init(&init).expect_err("the id is illegal");

        assert!(
            matches!(
                &error,
                OndoOrderError::InvalidClientOrderId { value: refused, reason }
                    if refused == &value && reason.contains(expected_reason)
            ),
            "was {error:?}",
        );
        assert_eq!(error.name(), "invalid_client_order_id");
    }

    #[rstest]
    fn test_the_longest_allowed_client_order_id_is_accepted() {
        let mut init = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        init.client_order_id = ClientOrderId::from("a".repeat(64).as_str());

        assert!(OndoOrderCommand::from_init(&init).is_ok());
    }

    #[rstest]
    fn test_an_instrument_that_is_not_an_ondo_perp_market_is_refused() {
        let mut init = initialized(OrderType::Limit, TimeInForce::Gtc, false, false);
        init.instrument_id = InstrumentId::from("NVDAUSDT-PERP.ASTER");

        let error = OndoOrderCommand::from_init(&init).expect_err("not an ONDO perp");
        assert_eq!(error.name(), "unsupported_market");
    }

    #[rstest]
    fn test_a_batch_serializes_every_item_and_keeps_their_order() {
        let first = OndoOrderCommand::from_init(&initialized(
            OrderType::Limit,
            TimeInForce::Gtc,
            true,
            false,
        ))
        .unwrap();
        let mut second_init = initialized(OrderType::Limit, TimeInForce::Ioc, false, true);
        second_init.client_order_id = ClientOrderId::from("ondo_probe_example_2");
        second_init.order_side = OrderSide::Sell;
        let second = OndoOrderCommand::from_init(&second_init).unwrap();

        let body = batch_body(&[first, second]).expect("a two-item batch is legal");
        let text = String::from_utf8(body).unwrap();

        assert!(
            text.starts_with(r#"{"orders":[{"market":"NVDA-USD.P","side":"buy""#),
            "{text}"
        );
        assert!(
            text.contains(
                r#""clientOrderId":"ondo_probe_example_1"},{"market":"NVDA-USD.P","side":"sell""#
            ),
            "the batch keeps the submitted order: {text}",
        );
        assert!(text.ends_with("}]}"), "{text}");
    }

    #[rstest]
    fn test_an_empty_or_oversized_batch_is_refused_locally() {
        assert_eq!(
            batch_body(&[]).expect_err("the venue refuses an empty batch"),
            OndoBatchError::Empty,
        );

        let command = OndoOrderCommand::from_init(&initialized(
            OrderType::Limit,
            TimeInForce::Gtc,
            false,
            false,
        ))
        .unwrap();
        let oversized = vec![command; ONDO_MAX_BATCH_ORDERS + 1];

        assert_eq!(
            batch_body(&oversized).expect_err("the venue refuses more than 20"),
            OndoBatchError::TooMany {
                count: ONDO_MAX_BATCH_ORDERS + 1
            },
        );
    }

    /// The documented create answer (`200 Created order`, the REST spec's own example).
    const ORDER_BODY: &str = r#"{"orderId":"197ec08e001658690721be129e7fa595","clientOrderId":"order123","side":"buy","price":"227.50","size":"10.00","market":"AAPL-USD.P","filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}"#;

    #[rstest]
    fn test_an_order_payload_keeps_every_decimal_string_and_its_own_status() {
        let order = OndoApiOrder::from_text(ORDER_BODY).unwrap();

        assert_eq!(order.order_id(), "197ec08e001658690721be129e7fa595");
        assert_eq!(order.client_order_id(), Some("order123"));
        assert_eq!(order.side(), OndoSide::Buy);
        assert_eq!(order.market(), "AAPL-USD.P");
        assert_eq!(order.size(), "10.00");
        assert_eq!(order.price(), Some("227.50"));
        assert_eq!(order.filled_size(), "0.00");
        assert_eq!(order.last_fill_size(), Some("0.00"));
        assert_eq!(order.status(), &OndoOrderStatus::Open);
        assert_eq!(order.order_type(), "limit");
        assert_eq!(order.time_in_force(), Some("GTC"));
        assert_eq!(order.reduce_only(), Some(false));
        assert_eq!(order.raw(), ORDER_BODY, "the payload is kept verbatim");
        assert_eq!(order.quantity().unwrap(), Quantity::from("10.00"));
        assert_eq!(order.filled_quantity().unwrap(), Quantity::from("0.00"));
        assert_eq!(
            order.ts_created().unwrap(),
            parse_timestamp("2025-03-05T14:30:00Z").unwrap(),
        );
    }

    #[rstest]
    #[case::pending("pending", false, true)]
    #[case::open("open", false, true)]
    #[case::fullyfilled("fullyfilled", true, true)]
    #[case::canceled("canceled", true, true)]
    #[case::untriggered("untriggered", false, true)]
    #[case::unknown("something_new", false, false)]
    // The plan spells this one `cancelled` when it discusses the `order_already_cancelled` *error
    // code*; the status field's own enum spells it `canceled` only, and a two-`l` status must not
    // be read as a confirmed end state.
    #[case::two_l_cancelled("cancelled", false, false)]
    fn test_the_status_classification_keeps_an_unknown_one_unresolved(
        #[case] raw: &str,
        #[case] terminal: bool,
        #[case] known: bool,
    ) {
        let status = OndoOrderStatus::from_raw(raw);

        assert_eq!(status.as_str(), raw);
        assert_eq!(status.is_terminal(), terminal);
        assert_eq!(status.is_known(), known);
        assert!(
            !OndoOrderStatus::Unknown("something_new".to_string()).is_terminal(),
            "an unknown status is never a venue-confirmed end state",
        );
    }

    #[rstest]
    fn test_an_order_payload_without_a_readable_side_is_reported() {
        let body = ORDER_BODY.replace(r#""side":"buy""#, r#""side":"BUY""#);
        let error = OndoApiOrder::from_text(&body).expect_err("the venue spells sides lowercase");

        assert!(
            matches!(
                &error,
                OndoHttpError::InvalidField { field: "side", value, .. } if value == "BUY"
            ),
            "was {error:?}",
        );
    }

    #[rstest]
    fn test_an_order_payload_that_is_not_an_order_is_reported() {
        assert!(matches!(
            OndoApiOrder::from_text("{}"),
            Err(OndoHttpError::Decode(_)),
        ));
        assert!(matches!(
            OndoApiOrder::from_text("not json"),
            Err(OndoHttpError::Decode(_)),
        ));
    }

    #[rstest]
    fn test_a_batch_answer_reads_added_and_refused_items_separately() {
        let body = format!(
            r#"{{"addedOrders":[{ORDER_BODY}],"failedOrders":[{{"order":{{"clientOrderId":"second","market":"NVDA-USD.P","side":"buy"}},"error":"post only order would match","errorCode":"post_only_has_match"}}]}}"#,
        );

        let response = OndoBatchAddOrderResponse::from_text(&body).unwrap();

        assert_eq!(response.added().len(), 1);
        assert_eq!(response.failed().len(), 1);
        assert_eq!(response.added()[0].client_order_id(), Some("order123"));
        assert_eq!(response.failed()[0].client_order_id(), Some("second"));
        assert_eq!(
            response.failed()[0].error_code(),
            Some("post_only_has_match")
        );
        assert_eq!(
            response.failed()[0].error(),
            Some("post only order would match"),
        );
    }

    #[rstest]
    fn test_a_refused_batch_item_without_an_echoed_id_is_still_reported() {
        let item = OndoRejectedOrder::from_text(
            r#"{"error":"insufficient margin","errorCode":"insufficient_margin"}"#,
        )
        .unwrap();

        assert!(item.client_order_id().is_none());
        assert_eq!(item.error_code(), Some("insufficient_margin"));
    }

    #[rstest]
    #[case::already_filled_spec(
        Some("order_already_fully_filled"),
        OndoCancelRejection::AlreadyFilled
    )]
    #[case::already_filled_plan(Some("order_already_filled"), OndoCancelRejection::AlreadyFilled)]
    #[case::already_canceled_spec(
        Some("order_already_canceled"),
        OndoCancelRejection::AlreadyCanceled
    )]
    #[case::already_canceled_plan(
        Some("order_already_cancelled"),
        OndoCancelRejection::AlreadyCanceled
    )]
    #[case::not_found(Some("order_not_found"), OndoCancelRejection::NotFound)]
    #[case::not_cancelable(
        Some("order_not_in_cancelable_state"),
        OndoCancelRejection::NotCancelable
    )]
    #[case::trading_disabled(Some("trading_disabled"), OndoCancelRejection::NotCancelable)]
    #[case::other(Some("some_undocumented_code"), OndoCancelRejection::Other)]
    #[case::absent(None, OndoCancelRejection::Other)]
    fn test_the_cancel_race_codes_triggers_a_query(
        #[case] code: Option<&str>,
        #[case] expected: OndoCancelRejection,
    ) {
        let rejection = OndoCancelRejection::from_code(code);

        assert_eq!(rejection, expected);
        assert_eq!(
            rejection.requires_query(),
            expected != OndoCancelRejection::Other,
        );
    }
}
