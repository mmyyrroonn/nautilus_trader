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

//! The private (authenticated) read surface: request shapes and response schemas.
//!
//! # What is documented, and what is verified
//!
//! `test_data/README.md` and `conflicts.md` were frozen without a single authenticated response: no
//! sandbox key existed, so **no private schema in this module has been confirmed against the host**.
//! The distinction that matters is between *documented* and *verified*, and this module keeps it
//! explicit:
//!
//! - the **documented** things are used as documented. `docs/api-reference/rest-spec.json` (sha256
//!   `860a96ca…`, the hash the freeze records) declares every endpoint and member this module
//!   reads: `GET /v1/account` ([`ACCOUNT_PATH`]), `GET /v1/perps/positions` ([`POSITIONS_PATH`]),
//!   `GET /v1/perps/balance` ([`BALANCE_PATH`]), the query parameters a list read may carry
//!   ([`CURSOR_PARAM`], [`STATUS_PARAM`], [`START_TIME_PARAM`], [`END_TIME_PARAM`] - and the
//!   `status` enum those come with, [`OndoOrderHistoryStatus`])
//!   and the `PageInfo` members a cursor is read from ([`CURSOR_FIELDS`]). The `ApiFill` field list
//!   is in the protocol table (`test_data/README.md`) and the REST auth error codes are in the
//!   API-key page;
//! - **documented is not verified.** No authenticated request has ever been made, so a path the
//!   spec declares is still a path no host has answered. Nothing here treats the two as one fact:
//!   every path and member name in this module is declared once, at the top, with the sandbox step
//!   that confirms it;
//! - nothing is silently dropped. [`OndoPrivateResponse`] keeps the **exact** JSON text of `result`
//!   (so every decimal string is the lexeme the venue sent, byte for byte), the HTTP status, the
//!   envelope's `success` flag and the venue's own cursor token, and an item that cannot be read is
//!   a named error rather than an empty page.
//!
//! # The two response shapes this module reads
//!
//! Most documented Ondo Perps REST responses use the `GenericResponse` envelope
//! (`success` + `result`) that `GET /v1/markets` and `GET /v1/perps/contracts` already use, but not
//! all of them: across the frozen spec's 72 operations, 17 answer 200 with something other than
//! `GenericResponse` + `result`, and 10 of those are a bare `GenericResponse` whose only member is
//! `success`. A
//! paginated list endpoint composes it as
//! `allOf: [GenericResponse, {result: array<item>, pageInfo: PageInfo}]` - `pageInfo` a **sibling**
//! of the `result` array, at the envelope root - and hands the next page's token out in
//! `PageInfo.nextCursor` ([`crate::http::query::CursorWalk`]). [`OndoPrivateResponse`] is that
//! envelope plus the cursor, with `result` kept as text.
//!
//! Among the endpoints this adapter writes, one documents the *other* shape:
//! `DELETE /v1/perps/orders` (cancel all orders) answers with a bare `GenericResponse` whose only
//! required member is `success`. [`OndoPrivateResponse::decode`]
//! stays strict about `result` for every read - a list whose `result` went missing is a schema
//! failure, not an empty page - and that one documented exception has its own constructor,
//! [`OndoPrivateResponse::decode_optional_result`].

use nautilus_core::UnixNanos;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::http::{
    error::{OndoHttpError, OndoHttpResult},
    query::OndoRequestTarget,
};

/// `GET /v1/account` - the authenticated account.
///
/// **DOCUMENTED, NOT YET VERIFIED.** The frozen REST spec declares this path (`summary: "Get
/// Account"`, answering a `GenericResponse` whose `result` is an `AccountInfo`); no authenticated
/// request has been made, so no host has answered it. This is the sandbox step's first
/// confirmation. The spec declares **no** `/v1/perps/account`.
pub const ACCOUNT_PATH: &str = "/v1/account";

/// `GET /v1/perps/positions` - the account's open positions.
///
/// **DOCUMENTED, NOT YET VERIFIED.** The frozen REST spec declares this path (`summary: "Get
/// Positions"`, `result` an array of `ApiPosition`) and documents it as taking **no** query
/// parameters at all - see [`OndoPrivateReadQuery::target`] for what this adapter sends anyway.
pub const POSITIONS_PATH: &str = "/v1/perps/positions";

/// `GET /v1/perps/balance` - the account's balances.
///
/// **DOCUMENTED, NOT YET VERIFIED.** The frozen REST spec declares this path (`summary: "Get
/// Balance"`, `result` a `MarginAccountBalanceSummary`) and documents it as taking **no** query
/// parameters.
pub const BALANCE_PATH: &str = "/v1/perps/balance";

/// The request parameter a cursor travels in.
///
/// **DOCUMENTED.** The frozen REST spec gives `cursor` as a query parameter of seven paginated
/// GETs: `/v1/perps/orders`, `/v1/perps/fills`, `/v1/perps/trades`, `/v1/perps/twap/orders/history`,
/// `/v1/perps/funding_rate_history`, `/v1/perps/funding_fees` and `/v1/perps/liquidation_history`,
/// described as "Pagination cursor". The `csv` forms are *not* among them, and
/// `/v1/perps/trades` carries no time window. No host has answered one yet.
pub const CURSOR_PARAM: &str = "cursor";

/// The request parameter an order-history read's status filter travels in.
///
/// **DOCUMENTED.** The frozen REST spec gives `status` as a query parameter of
/// `GET /v1/perps/orders` alone, described as *"Filter by order status"*. `GET /v1/perps/fills` has
/// no `status` at all, which is why only a read of the order history can carry one.
pub const STATUS_PARAM: &str = "status";

/// The request parameter an order-history read's window opens at.
///
/// **DOCUMENTED.** `GET /v1/perps/orders` and `GET /v1/perps/fills` both declare `startTime`, an
/// integer in UTC milliseconds. The orders list describes it as *"Filter orders placed at or after
/// this time"*, so the bound is inclusive and is stated in terms of when the order was placed.
pub const START_TIME_PARAM: &str = "startTime";

/// The request parameter an order-history read's window closes at.
///
/// **DOCUMENTED**, as [`START_TIME_PARAM`] is: *"at or before this time"* on the orders list, so
/// this end is inclusive too. Both are milliseconds of UTC.
pub const END_TIME_PARAM: &str = "endTime";

/// `GET /v1/perps/funding_fees` - the account's funding payments.
///
/// **DOCUMENTED, NOT YET VERIFIED.** The frozen REST spec declares this path (`summary: "Get
/// Funding Fee Payments"`, `result` an array of `FundingFeeTransfer` with a `PageInfo` beside it)
/// and documents **every** member of a record as required, `amount` among them: *"the actual
/// amount of USDC transferred. Positive indicates a fee you earned, negative a fee you paid."* No
/// authenticated request has been made, so no host has answered it.
///
/// It is read because it is the only thing that proves a funding payment: the public funding
/// **rate** is an estimate for a market and the balance's `totalFundingPayments` is a running
/// total, and neither of those is a payment this account made (plan §R3.2).
pub const FUNDING_FEES_PATH: &str = "/v1/perps/funding_fees";

/// The response members a cursor is read from, in the order they are tried.
///
/// `nextCursor` is the documented one: the frozen REST spec's `PageInfo` is
/// `{prevCursor, nextCursor}`, and a paginated list carries it as `pageInfo` beside the `result`
/// array. The other three spellings are kept because they cost nothing to read and the read is
/// tolerant of an envelope that nests them; `prevCursor` is deliberately **not** among them, since
/// following the *previous* page's token would walk the history backwards forever.
///
/// The scopes, in order, are the envelope root, then an object `pageInfo`, then an object `result`.
/// A member that is present but is not a non-empty, non-blank string fails the read closed
/// ([`OndoHttpError::InvalidField`]) instead of being read as "no more pages": an unreadable cursor
/// must never look like the end of the history.
pub const CURSOR_FIELDS: [&str; 4] = ["cursor", "nextCursor", "next_cursor", "next"];

/// The `status` values the venue's order-history filter accepts.
///
/// **DOCUMENTED.** The frozen REST spec declares the query enum as exactly these three. It is
/// deliberately **not** [`crate::http::orders::OndoOrderStatus`]: the two are different
/// vocabularies, and the venue's filter has no `pending` and no `untriggered`, so no value of this
/// type asks for "every non-terminal order" - that set is not expressible as a filter, which
/// `test_data/conflicts.md` records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoOrderHistoryStatus {
    /// Orders the venue is working.
    Open,
    /// Orders the venue ended without filling them completely.
    Canceled,
    /// Orders the venue filled completely.
    FullyFilled,
}

impl OndoOrderHistoryStatus {
    /// Returns the value exactly as the venue's query parameter spells it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Canceled => "canceled",
            Self::FullyFilled => "fullyfilled",
        }
    }
}

/// The filters a private list read accepts.
///
/// The parameters are serialized by [`Self::target`] in a fixed order - `market`, `limit`,
/// `cursor`, `status`, `startTime`, `endTime` - through [`OndoRequestTarget`], so the bytes a
/// signature covers are the bytes the transport sends (plan §6.1). Nothing re-orders them, and a
/// filter that was never set contributes no bytes at all: a read that sets none is the path alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OndoPrivateReadQuery {
    market: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
    status: Option<OndoOrderHistoryStatus>,
    start_time: Option<UnixNanos>,
    end_time: Option<UnixNanos>,
}

impl OndoPrivateReadQuery {
    /// Creates an unfiltered query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Narrows the read to one venue market, as in `NVDA-USD.P`.
    #[must_use]
    pub fn with_market(mut self, market: impl Into<String>) -> Self {
        self.market = Some(market.into());
        self
    }

    /// Continues a paginated read from the venue's own cursor token.
    #[must_use]
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Limits the number of items the venue returns.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Narrows the read to orders the venue reports with this status.
    ///
    /// Only `GET /v1/perps/orders` has a `status` filter ([`STATUS_PARAM`]); a fill history read
    /// that sets one would be sending a parameter that endpoint does not declare.
    #[must_use]
    pub fn with_status(mut self, status: OndoOrderHistoryStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Opens the read's time window at `start`.
    ///
    /// The value is sent in the venue's own unit - whole milliseconds of UTC
    /// ([`START_TIME_PARAM`]) - so a caller passes the instant it means and this is where the
    /// nanosecond precision Nautilus carries is narrowed once.
    #[must_use]
    pub fn with_start_time(mut self, start: UnixNanos) -> Self {
        self.start_time = Some(start);
        self
    }

    /// Closes the read's time window at `end`, inclusive, as [`Self::with_start_time`] opens it.
    #[must_use]
    pub fn with_end_time(mut self, end: UnixNanos) -> Self {
        self.end_time = Some(end);
        self
    }

    /// Returns the request target for `path`: the path plus this query, serialized once.
    ///
    /// An unfiltered query is the path alone, which is what an endpoint documented as taking no
    /// parameters gets. **Open, pending sandbox verification:** `GET /v1/perps/positions` and
    /// `GET /v1/perps/balance` are documented with an empty `parameters` list, and this adapter's
    /// typed reads for them pass a query through anyway - an empty one today, so the bytes sent are
    /// the documented path, but nothing here prevents a caller from adding one. Whether the host
    /// refuses an undeclared parameter is not known, and this is recorded rather than changed:
    /// dropping the parameter seam is a decision for the sandbox session that can observe it.
    #[must_use]
    pub fn target(&self, path: &str) -> OndoRequestTarget {
        let mut target = OndoRequestTarget::new(path);

        if let Some(market) = self.market.as_deref() {
            target = target.with_query_param("market", market);
        }

        if let Some(limit) = self.limit {
            target = target.with_query_param("limit", &limit.to_string());
        }

        if let Some(cursor) = self.cursor.as_deref() {
            target = target.with_query_param(CURSOR_PARAM, cursor);
        }

        if let Some(status) = self.status {
            target = target.with_query_param(STATUS_PARAM, status.as_str());
        }

        if let Some(start_time) = self.start_time {
            target = target.with_query_param(START_TIME_PARAM, &start_time.as_millis().to_string());
        }

        if let Some(end_time) = self.end_time {
            target = target.with_query_param(END_TIME_PARAM, &end_time.as_millis().to_string());
        }

        target
    }

    /// Returns the market this read is narrowed to, when it is narrowed.
    #[must_use]
    pub fn market(&self) -> Option<&str> {
        self.market.as_deref()
    }

    /// Returns the cursor this read continues from, when it continues one.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Returns the item limit this read asks for, when it asks for one.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Returns the status this read is narrowed to, when it is narrowed to one.
    #[must_use]
    pub const fn status(&self) -> Option<OndoOrderHistoryStatus> {
        self.status
    }

    /// Returns the start of this read's time window, when it has one.
    #[must_use]
    pub const fn start_time(&self) -> Option<UnixNanos> {
        self.start_time
    }

    /// Returns the end of this read's time window, when it has one.
    #[must_use]
    pub const fn end_time(&self) -> Option<UnixNanos> {
        self.end_time
    }
}

/// The `GenericResponse` envelope of a private read, with `result` kept as its exact JSON text.
#[derive(Clone, Debug)]
pub struct OndoPrivateResponse {
    http_status: u16,
    success: Option<bool>,
    cursor: Option<String>,
    cursor_field: Option<&'static str>,
    result: Box<RawValue>,
}

/// The envelope members this module reads. `result` is borrowed so it is never re-serialized.
#[derive(Deserialize)]
struct PrivateEnvelope<'a> {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default, borrow)]
    result: Option<&'a RawValue>,
}

/// The one `AccountInfo` member identity is read from.
///
/// `accountID` is the documented stable venue account identifier. The other `AccountInfo` members
/// (`identifier`, balances, terms, and so on) are deliberately not read here: identity comparison
/// needs one stable identifier, and an email, a wallet address or a monetary field is not one.
#[derive(Deserialize)]
struct AccountInfoId {
    #[serde(default, rename = "accountID")]
    account_id: Option<String>,
}

impl OndoPrivateResponse {
    /// Decodes one answer to an authenticated read.
    ///
    /// `http_status` is the status the venue answered with; it is preserved verbatim because it is
    /// the only statement about the request that outlives the call.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the body is not a UTF-8 JSON object with the
    /// envelope's shape, [`OndoHttpError::Unsuccessful`] when the envelope reports failure,
    /// [`OndoHttpError::MissingField`] when `result` is absent, and
    /// [`OndoHttpError::InvalidField`] when a cursor member is present but unreadable. An empty
    /// `result` array is *not* an error: an account with no orders is a legitimate empty page.
    pub fn decode(http_status: u16, body: &[u8]) -> OndoHttpResult<Self> {
        let (envelope, text) = read_envelope(body)?;

        let Some(result) = envelope.result else {
            return Err(OndoHttpError::MissingField {
                context: "an authenticated read".to_string(),
                field: "result",
            });
        };

        assemble(http_status, envelope.success, result, text)
    }

    /// Decodes the answer to the one endpoint the spec documents **without** a `result`.
    ///
    /// `DELETE /v1/perps/orders` (cancel all orders) is the single exception to
    /// [`Self::decode`]'s rule: the frozen REST spec gives its 200 as a bare `GenericResponse`,
    /// whose `required` list is `["success"]` and whose schema has no `result` member at all. So
    /// there, and only there, a success carrying no payload is the documented success shape rather
    /// than a schema failure - which is exactly the distinction the strict decoder cannot make,
    /// because for every *read* a missing `result` is a truncated answer.
    ///
    /// The strictness is not relaxed anywhere: this is a second, named constructor, and the errors
    /// are [`Self::decode`]'s, minus the missing-`result` case, which answers `Ok(None)` here.
    ///
    /// `None` means "the cancel API answered and said nothing else". It is **not** "the orders are
    /// cancelled": plan §6.3 requires the confirming read after any cancel, and this answer is the
    /// one that most needs it.
    ///
    /// # Errors
    ///
    /// As [`Self::decode`], except that an absent `result` is not an error.
    pub fn decode_optional_result(http_status: u16, body: &[u8]) -> OndoHttpResult<Option<Self>> {
        let (envelope, text) = read_envelope(body)?;

        let Some(result) = envelope.result else {
            return Ok(None);
        };

        assemble(http_status, envelope.success, result, text).map(Some)
    }

    /// Returns the HTTP status the venue answered with.
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        self.http_status
    }

    /// Returns the envelope's `success` flag, when the payload carried one.
    #[must_use]
    pub const fn success(&self) -> Option<bool> {
        self.success
    }

    /// Returns the venue's own cursor token for the next page, when the answer carried one.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Returns the member the cursor was read from, when one was read.
    ///
    /// Recorded because more than one spelling is accepted ([`CURSOR_FIELDS`]) while the spec
    /// documents one: a sandbox session can log which spelling the venue actually sent without a
    /// second read, and an envelope that used a different one is visible rather than silently
    /// equivalent.
    #[must_use]
    pub const fn cursor_field(&self) -> Option<&'static str> {
        self.cursor_field
    }

    /// Returns the exact JSON text of `result`.
    ///
    /// This is the schema data as the venue sent it: every price, quantity and fee is the decimal
    /// lexeme that arrived, with no re-serialization and no `f64` in between.
    #[must_use]
    pub fn raw_result(&self) -> &str {
        self.result.get()
    }

    /// Returns the authenticated account's venue identifier, when the answer carries one.
    ///
    /// This reads the documented `AccountInfo.accountID`. It is **not** the Nautilus `AccountId`,
    /// and the two are never compared to each other by string splitting or by prefix guessing.
    /// [`None`] means the answer carried no comparable identifier, which the caller reports as
    /// [`crate::common::enums::OndoAccountIdentity::Unknown`] rather than as a match. The value is
    /// for comparison only and is never logged.
    #[must_use]
    pub fn venue_account_id(&self) -> Option<String> {
        let info: AccountInfoId = serde_json::from_str(self.result.get()).ok()?;

        info.account_id
            .filter(|account_id| !account_id.trim().is_empty())
    }

    /// Returns `result` as a list of items, each keeping its exact JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when `result` is not a JSON array - a shape this module
    /// does not read is reported, not treated as an empty page.
    pub fn items(&self) -> OndoHttpResult<Vec<Box<RawValue>>> {
        serde_json::from_str(self.result.get()).map_err(|error| {
            OndoHttpError::Decode(format!(
                "the `result` of an authenticated list read is not an array: {error}"
            ))
        })
    }

    /// Returns `result` as a list of fills, each keeping its exact decimal strings.
    ///
    /// # Errors
    ///
    /// Returns the [`Self::items`] error, or the fill schema error
    /// ([`OndoApiFill::from_raw`]) for an item that is not one of the documented `ApiFill` shapes.
    pub fn fills(&self) -> OndoHttpResult<Vec<OndoApiFill>> {
        self.items()?
            .iter()
            .map(|item| OndoApiFill::from_raw(item))
            .collect()
    }

    /// Returns `result` as a list of funding payments, each keeping its exact decimal strings.
    ///
    /// # Errors
    ///
    /// Returns the [`Self::items`] error, or the funding record's own schema error
    /// ([`OndoApiFundingFee::from_raw`]) for an item that is not one of the documented
    /// `FundingFeeTransfer` shapes.
    pub fn funding_fees(&self) -> OndoHttpResult<Vec<OndoApiFundingFee>> {
        self.items()?
            .iter()
            .map(|item| OndoApiFundingFee::from_raw(item))
            .collect()
    }
}

/// Reads the envelope every private answer carries, refusing the two shapes that decide themselves.
///
/// The UTF-8 check, the JSON parse and the `success == false` refusal are the same for both
/// constructors, so they are decided once here; what differs between a read and the cancel-all
/// answer is only what an absent `result` means, and that is the caller's to say.
fn read_envelope(body: &[u8]) -> OndoHttpResult<(PrivateEnvelope<'_>, &str)> {
    let text = std::str::from_utf8(body).map_err(|error| {
        OndoHttpError::Decode(format!("the private response is not UTF-8: {error}"))
    })?;

    let envelope: PrivateEnvelope<'_> =
        serde_json::from_str(text).map_err(|error| OndoHttpError::Decode(error.to_string()))?;

    if envelope.success == Some(false) {
        return Err(OndoHttpError::Unsuccessful);
    }

    Ok((envelope, text))
}

/// Builds the response once an envelope and its `result` are known.
fn assemble(
    http_status: u16,
    success: Option<bool>,
    result: &RawValue,
    text: &str,
) -> OndoHttpResult<OndoPrivateResponse> {
    let (cursor, cursor_field) = read_cursor(text)?;

    Ok(OndoPrivateResponse {
        http_status,
        success,
        cursor,
        cursor_field,
        result: result.to_owned(),
    })
}

/// The envelope's cursor, read from the documented member names in order.
///
/// The scopes are tried in priority order and the first member found decides, so a shape that
/// carries the token in more than one place reads the highest-priority one:
///
/// 1. the envelope root - `{"success":true,"cursor":"…", …}`;
/// 2. an object `pageInfo` - **the documented shape**, where a paginated list is
///    `allOf: [GenericResponse, {result: array<…>, pageInfo: {prevCursor, nextCursor}}]` and the
///    token is therefore a *sibling* of the `result` array, not a member inside it;
/// 3. an object `result` - for an envelope that nests the payload's own container.
///
/// Reading only the root and an object `result` is what made a two-page history look like a
/// one-page one: the documented `pageInfo.nextCursor` was never in scope, `(None, None)` was read
/// as "no more pages", and the walk stopped without an error. `pageInfo` is searched only when it
/// is an object: a `pageInfo` that is absent or `null` is a page with no next page, not a failure.
fn read_cursor(text: &str) -> OndoHttpResult<(Option<String>, Option<&'static str>)> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| OndoHttpError::Decode(error.to_string()))?;

    let page_info = value.get("pageInfo").filter(|info| info.is_object());
    let nested = value.get("result").filter(|result| result.is_object());
    let scopes: [Option<&serde_json::Value>; 3] = [Some(&value), page_info, nested];

    for scope in scopes.into_iter().flatten() {
        for field in CURSOR_FIELDS {
            let Some(member) = scope.get(field) else {
                continue;
            };

            return match member {
                // An opaque token that is empty *or blank* is not a cursor: a venue that sent
                // `"next":" "` has told this adapter nothing, and reading that as "no more pages"
                // would silently truncate the history. The token itself is passed through
                // verbatim, whitespace and all, when it carries anything at all.
                serde_json::Value::String(cursor) if !cursor.trim().is_empty() => {
                    Ok((Some(cursor.clone()), Some(field)))
                }
                other => Err(OndoHttpError::InvalidField {
                    context: "an authenticated read".to_string(),
                    field: "cursor",
                    value: other.to_string(),
                    reason: format!("`{field}` is present but is not a non-empty string"),
                }),
            };
        }
    }

    Ok((None, None))
}

/// One `ApiFill` from `GET /v1/perps/fills`.
///
/// The field list is the protocol table's (`test_data/README.md`, `GET /v1/perps/fills`), and every
/// decimal member stays the string the venue sent: the values are exposed as `&str` so a caller
/// converts them exactly ([`crate::common::parse::parse_decimal`]) when it needs a number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoApiFill {
    id: String,
    order_id: String,
    client_order_id: Option<String>,
    parent_order_id: Option<String>,
    market: String,
    price: Option<String>,
    size: Option<String>,
    side: Option<String>,
    direction: OndoFillDirection,
    filled_cost: Option<String>,
    fee: Option<String>,
    fee_rebate: Option<String>,
    pnl: Option<String>,
    time: Option<String>,
    is_maker: Option<bool>,
    is_adl: Option<bool>,
}

/// The documented `ApiFill` members, as the venue spells them.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawApiFill {
    id: String,
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    #[serde(default, rename = "parentOrderID")]
    parent_order_id: Option<String>,
    market: String,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    filled_cost: Option<String>,
    #[serde(default)]
    fee: Option<String>,
    #[serde(default)]
    fee_rebate: Option<String>,
    #[serde(default)]
    pnl: Option<String>,
    #[serde(default)]
    time: Option<String>,
    #[serde(default)]
    is_maker: Option<bool>,
    #[serde(default, rename = "isADL")]
    is_adl: Option<bool>,
}

impl OndoApiFill {
    /// Reads one fill from its raw JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the item is not an `ApiFill` (a missing `id`,
    /// `orderId` or `market` names itself in the error), and [`OndoHttpError::InvalidField`] when
    /// `direction` carries a spelling this adapter does not recognise - with the raw value attached,
    /// never mapped to a default direction (`test_data/conflicts.md` conflict 4).
    pub fn from_raw(raw: &RawValue) -> OndoHttpResult<Self> {
        let fill: RawApiFill = serde_json::from_str(raw.get())
            .map_err(|error| OndoHttpError::Decode(format!("not an ApiFill: {error}")))?;

        let direction = fill.direction.as_deref().map_or(
            Err(OndoHttpError::MissingField {
                context: "an ApiFill".to_string(),
                field: "direction",
            }),
            OndoFillDirection::from_raw,
        )?;

        Ok(Self {
            id: fill.id,
            order_id: fill.order_id,
            client_order_id: fill.client_order_id,
            parent_order_id: fill.parent_order_id,
            market: fill.market,
            price: fill.price,
            size: fill.size,
            side: fill.side,
            direction,
            filled_cost: fill.filled_cost,
            fee: fill.fee,
            fee_rebate: fill.fee_rebate,
            pnl: fill.pnl,
            time: fill.time,
            is_maker: fill.is_maker,
            is_adl: fill.is_adl,
        })
    }

    /// Returns the venue's fill id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the venue's order id this fill belongs to.
    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    /// Returns the client order id, when the payload carried one.
    #[must_use]
    pub fn client_order_id(&self) -> Option<&str> {
        self.client_order_id.as_deref()
    }

    /// Returns the parent order id, when the payload carried one.
    #[must_use]
    pub fn parent_order_id(&self) -> Option<&str> {
        self.parent_order_id.as_deref()
    }

    /// Returns the venue's market string, as in `NVDA-USD.P`.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the fill price as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn price(&self) -> Option<&str> {
        self.price.as_deref()
    }

    /// Returns the fill size as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn size(&self) -> Option<&str> {
        self.size.as_deref()
    }

    /// Returns the raw venue side string, when it was sent.
    #[must_use]
    pub fn side(&self) -> Option<&str> {
        self.side.as_deref()
    }

    /// Returns the normalised fill direction.
    #[must_use]
    pub const fn direction(&self) -> OndoFillDirection {
        self.direction
    }

    /// Returns the filled cost as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn filled_cost(&self) -> Option<&str> {
        self.filled_cost.as_deref()
    }

    /// Returns the fee as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn fee(&self) -> Option<&str> {
        self.fee.as_deref()
    }

    /// Returns the fee rebate as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn fee_rebate(&self) -> Option<&str> {
        self.fee_rebate.as_deref()
    }

    /// Returns the fill's pnl as the venue's decimal string, when it was sent.
    #[must_use]
    pub fn pnl(&self) -> Option<&str> {
        self.pnl.as_deref()
    }

    /// Returns the fill time as the venue's timestamp string, when it was sent.
    ///
    /// Kept as the wire string: parse it with
    /// [`crate::common::parse::parse_timestamp`], which preserves nine fractional digits.
    #[must_use]
    pub fn time(&self) -> Option<&str> {
        self.time.as_deref()
    }

    /// Returns whether the venue marked the fill as maker, when it reported it.
    #[must_use]
    pub const fn is_maker(&self) -> Option<bool> {
        self.is_maker
    }

    /// Returns whether the venue marked the fill as ADL, when it reported it.
    #[must_use]
    pub const fn is_adl(&self) -> Option<bool> {
        self.is_adl
    }
}

/// One `FundingFeeTransfer` from `GET /v1/perps/funding_fees`.
///
/// The frozen schema makes every member required. Three of them are read as required here -
/// `market`, `time` and `amount` - because they are what identifies a payment and what it was
/// worth, and a record missing one is a payload this adapter does not understand rather than a
/// zero-valued payment. `rate` and `positionSize` are kept as sent when they are there: they are
/// evidence about a payment, and **nothing** in this adapter multiplies them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoApiFundingFee {
    market: String,
    time: String,
    amount: String,
    rate: Option<String>,
    position_size: Option<String>,
}

/// The documented `FundingFeeTransfer` members, as the venue spells them.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFundingFee {
    market: String,
    time: String,
    amount: String,
    #[serde(default)]
    rate: Option<String>,
    #[serde(default)]
    position_size: Option<String>,
}

impl OndoApiFundingFee {
    /// Reads one funding payment from its raw JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the item is not a `FundingFeeTransfer`; a missing
    /// `market`, `time` or `amount` names itself in the error.
    pub fn from_raw(raw: &RawValue) -> OndoHttpResult<Self> {
        let fee: RawFundingFee = serde_json::from_str(raw.get())
            .map_err(|error| OndoHttpError::Decode(format!("not a FundingFeeTransfer: {error}")))?;

        Ok(Self {
            market: fee.market,
            time: fee.time,
            amount: fee.amount,
            rate: fee.rate,
            position_size: fee.position_size,
        })
    }

    /// Reads one funding payment from its raw JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Decode`] when the text is not a `FundingFeeTransfer`.
    pub fn from_text(text: &str) -> OndoHttpResult<Self> {
        let raw: Box<serde_json::value::RawValue> = serde_json::from_str(text)
            .map_err(|error| OndoHttpError::Decode(format!("not a FundingFeeTransfer: {error}")))?;

        Self::from_raw(&raw)
    }

    /// Returns the venue's market string the payment was made on.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the payment's `time`, as the venue's timestamp string.
    ///
    /// Kept as the wire string: parse it with
    /// [`crate::common::parse::parse_timestamp`], which preserves nine fractional digits.
    #[must_use]
    pub fn time(&self) -> &str {
        &self.time
    }

    /// Returns the signed amount transferred, as the venue's decimal string.
    ///
    /// Positive is a fee earned, negative a fee paid - the venue's own convention, kept verbatim.
    #[must_use]
    pub fn amount(&self) -> &str {
        &self.amount
    }

    /// Returns the funding rate that led to this payment, when the venue sent one.
    #[must_use]
    pub fn rate(&self) -> Option<&str> {
        self.rate.as_deref()
    }

    /// Returns the position size at the time of the payment, when the venue sent one.
    #[must_use]
    pub fn position_size(&self) -> Option<&str> {
        self.position_size.as_deref()
    }
}

/// A fill's direction, normalised across the two spellings the frozen material contains.
///
/// `test_data/conflicts.md` conflict 4: the REST schema's `enum` uses `openLong` while its own
/// `description` (and the WS schema) uses `open long`. Both are read here, and the normalisation is
/// a single whitespace/underscore boundary - an unrecognised spelling is a named
/// [`OndoHttpError::InvalidField`] carrying the raw value, never a default direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoFillDirection {
    /// Opening a long position.
    OpenLong,
    /// Opening a short position.
    OpenShort,
    /// Closing a long position.
    CloseLong,
    /// Closing a short position.
    CloseShort,
    /// Flipping a long position to a short one.
    FlipLongToShort,
    /// Flipping a short position to a long one.
    FlipShortToLong,
}

impl OndoFillDirection {
    /// Normalises one raw venue direction string.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::InvalidField`] for anything that is not one of the documented
    /// spellings, with the raw value attached.
    pub fn from_raw(raw: &str) -> OndoHttpResult<Self> {
        let normalised: String = raw
            .chars()
            .filter(|character| !matches!(character, ' ' | '_'))
            .flat_map(char::to_lowercase)
            .collect();

        match normalised.as_str() {
            "openlong" => Ok(Self::OpenLong),
            "openshort" => Ok(Self::OpenShort),
            "closelong" => Ok(Self::CloseLong),
            "closeshort" => Ok(Self::CloseShort),
            "fliplongtoshort" => Ok(Self::FlipLongToShort),
            "flipshorttolong" => Ok(Self::FlipShortToLong),
            _ => Err(OndoHttpError::InvalidField {
                context: "an ApiFill".to_string(),
                field: "direction",
                value: raw.to_string(),
                reason: "the raw direction is not a documented `ApiFill.direction` value"
                    .to_string(),
            }),
        }
    }

    /// Returns the normalised direction in snake case, as in `open_long`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenLong => "open_long",
            Self::OpenShort => "open_short",
            Self::CloseLong => "close_long",
            Self::CloseShort => "close_short",
            Self::FlipLongToShort => "flip_long_to_short",
            Self::FlipShortToLong => "flip_short_to_long",
        }
    }

    /// Returns whether this direction opens a new position.
    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(self, Self::OpenLong | Self::OpenShort)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_a_query_serializes_market_limit_and_cursor_in_one_fixed_order() {
        let query = OndoPrivateReadQuery::new()
            .with_cursor("opaque-token")
            .with_limit(100)
            .with_market("NVDA-USD.P");

        assert_eq!(
            query.target("/v1/perps/fills").as_str(),
            "/v1/perps/fills?market=NVDA-USD.P&limit=100&cursor=opaque-token",
            "the order is fixed by this method, so the signature and the wire bytes agree",
        );
    }

    #[rstest]
    fn test_an_unfiltered_query_is_the_path_alone() {
        assert_eq!(
            OndoPrivateReadQuery::default()
                .target(ACCOUNT_PATH)
                .as_str(),
            ACCOUNT_PATH,
        );
        assert!(OndoPrivateReadQuery::new().market().is_none());
        assert!(OndoPrivateReadQuery::new().cursor().is_none());
        assert!(OndoPrivateReadQuery::new().limit().is_none());
    }

    #[rstest]
    fn test_the_cursor_is_read_from_the_top_level_first_then_page_info_then_an_object_result() {
        let (cursor, field) =
            read_cursor(r#"{"success":true,"cursor":"top","result":{"cursor":"nested"}}"#).unwrap();
        assert_eq!(cursor.as_deref(), Some("top"));
        assert_eq!(field, Some("cursor"));

        // The documented shape: `pageInfo` is the `result` array's sibling at the envelope root.
        let (cursor, field) = read_cursor(
            r#"{"success":true,"result":[{"id":"1"}],"pageInfo":{"prevCursor":"back","nextCursor":"page-2"}}"#,
        )
        .expect("the documented pageInfo is searched");
        assert_eq!(cursor.as_deref(), Some("page-2"));
        assert_eq!(field, Some("nextCursor"));

        let (cursor, field) = read_cursor(r#"{"success":true,"result":{"nextCursor":"page-2"}}"#)
            .expect("an object result is searched");
        assert_eq!(cursor.as_deref(), Some("page-2"));
        assert_eq!(field, Some("nextCursor"));

        let (cursor, field) = read_cursor(r#"{"success":true,"result":[{"id":"1"}]}"#).unwrap();
        assert_eq!(cursor, None);
        assert_eq!(field, None);

        // A `pageInfo` with no next token is the last page, not a failure - and its `prevCursor`
        // is never followed, however tempting a token looks.
        let (cursor, field) =
            read_cursor(r#"{"success":true,"result":[],"pageInfo":{"prevCursor":"back"}}"#)
                .unwrap();
        assert_eq!(cursor, None);
        assert_eq!(field, None);
    }

    #[rstest]
    #[case::number(r#"{"success":true,"result":[],"cursor":7}"#)]
    #[case::empty_string(r#"{"success":true,"result":[],"next":" "}"#)]
    #[case::null(r#"{"success":true,"result":[],"next_cursor":null}"#)]
    fn test_a_present_but_unreadable_cursor_fails_closed(#[case] body: &str) {
        let error = OndoPrivateResponse::decode(200, body.as_bytes())
            .expect_err("an unreadable cursor is not the end of the history");

        assert!(
            matches!(
                &error,
                OndoHttpError::InvalidField { field: "cursor", value, .. }
                    if !value.is_empty()
            ),
            "was {error:?}",
        );
        assert!(
            error.to_string().contains("not a non-empty string"),
            "{error}",
        );
    }

    #[rstest]
    fn test_the_response_keeps_the_status_and_the_exact_decimal_lexemes() {
        let body = r#"{"success":true,"result":[{"id":"f1","price":"212.2299496152638348004712442969639146","size":"0.01"}]}"#;

        let response = OndoPrivateResponse::decode(200, body.as_bytes()).unwrap();

        assert_eq!(response.http_status(), 200);
        assert_eq!(response.success(), Some(true));
        assert_eq!(response.cursor(), None);
        assert_eq!(
            response.raw_result(),
            r#"[{"id":"f1","price":"212.2299496152638348004712442969639146","size":"0.01"}]"#,
            "the result is the venue's own text, so a wide lexeme is never rounded",
        );
        assert_eq!(response.items().unwrap().len(), 1);
    }

    #[rstest]
    fn test_an_absent_result_is_a_failure_and_an_empty_list_is_not() {
        let absent =
            OndoPrivateResponse::decode(200, br#"{"success":true}"#).expect_err("no result");
        assert!(
            matches!(
                absent,
                OndoHttpError::MissingField {
                    field: "result",
                    ..
                }
            ),
            "was {absent:?}",
        );

        let empty = OndoPrivateResponse::decode(200, br#"{"success":true,"result":[]}"#)
            .expect("an account with no orders is a legitimate empty page");
        assert!(empty.items().unwrap().is_empty());

        let failed = OndoPrivateResponse::decode(200, br#"{"success":false,"result":null}"#)
            .expect_err("the envelope reported failure");
        assert!(
            matches!(failed, OndoHttpError::Unsuccessful),
            "was {failed:?}"
        );
    }

    /// The one endpoint whose documented 200 has no `result`, and the strict rule it does not break.
    ///
    /// `{"success":true}` is exactly what the frozen REST spec gives as the 200 of
    /// `DELETE /v1/perps/orders` (cancel all orders): a bare `GenericResponse`, `required:
    /// ["success"]`. The strict [`OndoPrivateResponse::decode`] refuses it - correctly, because
    /// that is what a *read* whose `result` went missing looks like - and the named
    /// [`OndoPrivateResponse::decode_optional_result`] reads it as the documented success it is.
    #[rstest]
    fn test_a_bare_success_is_refused_by_the_strict_reader_and_named_by_the_cancel_all_one() {
        let body: &[u8] = br#"{"success":true}"#;

        let strict = OndoPrivateResponse::decode(200, body).expect_err("a read carries `result`");
        assert!(
            matches!(
                strict,
                OndoHttpError::MissingField {
                    field: "result",
                    ..
                }
            ),
            "was {strict:?}",
        );

        assert!(
            OndoPrivateResponse::decode_optional_result(200, body)
                .expect("the cancel-all answer is documented without a result")
                .is_none(),
            "an absent result is exactly what this endpoint documents",
        );

        // Everything else about the answer is still read: the status and the envelope's own flag.
        let carried = OndoPrivateResponse::decode_optional_result(
            200,
            br#"{"success":true,"result":{"orderId":"o1"}}"#,
        )
        .expect("a payload is read when there is one")
        .expect("and is not read as an absent one");
        assert_eq!(carried.success(), Some(true));
        assert_eq!(carried.http_status(), 200);
        assert_eq!(carried.raw_result(), r#"{"orderId":"o1"}"#);

        // The two refusals that are *not* about `result` are the strict reader's, unchanged: an
        // unsuccessful envelope is a failure even here, and so is a body that is not the envelope.
        assert!(matches!(
            OndoPrivateResponse::decode_optional_result(200, br#"{"success":false}"#),
            Err(OndoHttpError::Unsuccessful),
        ));
        assert!(matches!(
            OndoPrivateResponse::decode_optional_result(200, b"not json"),
            Err(OndoHttpError::Decode(_)),
        ));
    }

    #[rstest]
    fn test_an_item_list_that_is_not_an_array_is_reported() {
        let response =
            OndoPrivateResponse::decode(200, br#"{"success":true,"result":{"id":"o1"}}"#).unwrap();

        assert!(response.items().is_err(), "an object result is not a list");

        // ... and the object itself stays readable, which is what a create/account answer is.
        assert_eq!(response.raw_result(), r#"{"id":"o1"}"#);
    }

    #[rstest]
    fn test_a_private_response_that_is_not_utf8_or_not_json_fails_closed() {
        assert!(matches!(
            OndoPrivateResponse::decode(200, &[0xff, 0xfe]),
            Err(OndoHttpError::Decode(_)),
        ));
        assert!(matches!(
            OndoPrivateResponse::decode(200, b"not json"),
            Err(OndoHttpError::Decode(_)),
        ));
    }

    /// Identity reads exactly the documented `accountID` and nothing else. The email/wallet
    /// `identifier` and every monetary member are deliberately invisible here: an answer that
    /// carries only those is an unknown identity, not a match.
    #[rstest]
    fn test_venue_account_id_reads_only_the_documented_account_member() {
        let full = OndoPrivateResponse::decode(
            200,
            br#"{"success":true,"result":{"accountID":"10458932786832481","identifier":"someone@example.com","withdrawalFeeUSD":"0"}}"#,
        )
        .expect("the account envelope decodes");

        assert_eq!(
            full.venue_account_id().as_deref(),
            Some("10458932786832481"),
        );

        for body in [
            r#"{"success":true,"result":{"identifier":"someone@example.com"}}"#,
            r#"{"success":true,"result":{"accountID":"   "}}"#,
            r#"{"success":true,"result":{"accountId":"10458932786832481"}}"#,
        ] {
            let response =
                OndoPrivateResponse::decode(200, body.as_bytes()).expect("the envelope decodes");

            assert_eq!(
                response.venue_account_id(),
                None,
                "`{body}` carries no comparable `accountID`",
            );
        }
    }
}
