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

//! REST endpoint paths, request-target serialization and cursor pagination state for the Ondo
//! Perps API.
//!
//! Paths are taken verbatim from the protocol table in `crates/adapters/ondo/test_data/README.md`;
//! a path that table does not record is not declared here.
//!
//! # Serialize once
//!
//! [`OndoRequestTarget`] builds the path and query string exactly once. The bytes it produces are
//! both what goes on the wire and what a later HMAC signature is computed over, so a query can
//! never be encoded one way for the signature and another way on the wire (plan §6.1). Transport,
//! retries and the rate budget live in [`crate::http::client`] and [`crate::http::rate_limit`].

use std::collections::HashSet;

use crate::http::error::{OndoHttpError, OndoHttpResult};

/// `GET /status` - venue status.
pub const STATUS_PATH: &str = "/status";

/// `GET /v1/markets` - perps trading pairs and spot token configuration.
pub const MARKETS_PATH: &str = "/v1/markets";

/// `GET /v1/perps/contracts` - perps contract metadata, including fees and status.
pub const CONTRACTS_PATH: &str = "/v1/perps/contracts";

/// `/v1/perps/orders` - order creation, single-order query and single-order cancel.
pub const ORDERS_PATH: &str = "/v1/perps/orders";

/// `GET /v1/perps/fills` - account fill history.
pub const FILLS_PATH: &str = "/v1/perps/fills";

/// The default page cap of a [`CursorWalk`].
///
/// One hundred pages is far beyond any documented list endpoint's useful depth, and it exists only
/// so an endpoint that keeps handing out fresh cursors cannot spin forever.
pub const ONDO_MAX_PAGES: usize = 100;

/// The value that looks an order up by its client order id (`client:{clientOrderId}`, plan §6.2).
///
/// The colon is a query-value character, not a URL segment character, so the value must be
/// percent-encoded before it is signed or sent;
/// [`OndoRequestTarget::with_query_param`] does that, turning the colon into `%3A`.
#[must_use]
pub fn client_order_lookup(client_order_id: &str) -> String {
    format!("client:{client_order_id}")
}

/// A REST request target: an endpoint path plus its serialized query string.
///
/// The target is serialized exactly once, by [`Self::as_str`], and those same bytes are both sent
/// on the wire and covered by the signature (plan §6.1: serialize once, then reuse). Query
/// parameters keep the order they were appended in, because the venue observes the order it is
/// given and a signature cannot re-sort them behind the caller's back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OndoRequestTarget {
    target: String,
}

impl OndoRequestTarget {
    /// Creates a target for `path`, with no query string.
    #[must_use]
    pub fn new(path: &str) -> Self {
        Self {
            target: path.to_string(),
        }
    }

    /// Appends `key=value`, percent-encoding both.
    ///
    /// The first parameter is introduced with `?` and every later one with `&`. `value` is
    /// percent-encoded byte by byte over its UTF-8 form, leaving only the RFC 3986 unreserved set
    /// (`A-Z a-z 0-9 - . _ ~`) alone and using uppercase hex, so a space is `%20` and not `+`.
    #[must_use]
    pub fn with_query_param(mut self, key: &str, value: &str) -> Self {
        let separator = if self.target.contains('?') { '&' } else { '?' };
        self.target.push(separator);
        self.target.push_str(&percent_encode(key));
        self.target.push('=');
        self.target.push_str(&percent_encode(value));
        self
    }

    /// Returns the exact path-and-query byte string this target is.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.target
    }
}

/// Returns `true` for the unreserved characters of RFC 3986 §2.3.
fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// Percent-encodes every byte outside the unreserved set, with uppercase hex digits.
///
/// Also the encoder an order path segment goes through ([`crate::http::orders::order_lookup_target`]):
/// one encoder means a value is encoded the same way wherever this adapter puts it in a target, and
/// the bytes a signature covers are the bytes the transport sends (plan §6.1).
pub(crate) fn percent_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        if is_unreserved(byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }

    encoded
}

/// Progress state for a cursor-paginated read.
///
/// The request side is documented and this is where it is written down: exactly 7 of the frozen
/// spec's GETs take a `cursor` parameter - `/v1/perps/orders`, `/v1/perps/fills`,
/// `/v1/perps/trades`, `/v1/perps/twap/orders/history`, `/v1/perps/funding_rate_history`,
/// `/v1/perps/funding_fees` and `/v1/perps/liquidation_history` (the `csv` forms are *not* among
/// them). What the spec does not pin down is the **response** side: which member carries the next
/// cursor, and how exhaustion is signalled. None of those 7 paths has been called against a live
/// host in this project, so this helper is deliberately shape-agnostic - a cursor is any opaque
/// string, and the caller decides which request parameter it belongs to.
///
/// What it does guarantee is termination. It refuses a cursor it has already returned - the usual
/// shape of a broken or exhausted paginator, which would otherwise repeat one page forever - and it
/// caps the number of pages a single walk may request. A walk that trips either guard returns
/// [`OndoHttpError::Pagination`] rather than a short read, so a caller never mistakes a stopped walk
/// for a complete history.
#[derive(Clone, Debug)]
pub struct CursorWalk {
    seen: HashSet<String>,
    max_pages: usize,
    pages: usize,
}

impl CursorWalk {
    /// Creates a walk that may request at most `max_pages` pages.
    #[must_use]
    pub fn new(max_pages: usize) -> Self {
        Self {
            seen: HashSet::new(),
            max_pages,
            pages: 0,
        }
    }

    /// Returns the number of pages this walk has requested.
    #[must_use]
    pub fn pages(&self) -> usize {
        self.pages
    }

    /// Returns the cursor to request next, or [`None`] when there is no next page.
    ///
    /// `next` is the cursor the last response carried: [`None`] ends the walk. A cursor that has
    /// already been requested, or a walk past its page cap, is an error instead of a repeated page.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Pagination`] when `next` repeats a cursor this walk has already
    /// returned, or when the page cap is reached. The cursor itself is not echoed into the error,
    /// because it is an opaque venue token.
    pub fn advance(&mut self, next: Option<&str>) -> OndoHttpResult<Option<String>> {
        let Some(cursor) = next else {
            return Ok(None);
        };

        if !self.seen.insert(cursor.to_string()) {
            return Err(OndoHttpError::Pagination {
                pages: self.pages,
                reason: "the endpoint repeated a cursor this walk had already requested"
                    .to_string(),
            });
        }

        if self.pages >= self.max_pages {
            return Err(OndoHttpError::Pagination {
                pages: self.pages,
                reason: format!("the page cap of {} was reached", self.max_pages),
            });
        }

        self.pages += 1;

        Ok(Some(cursor.to_string()))
    }
}

impl Default for CursorWalk {
    fn default() -> Self {
        Self::new(ONDO_MAX_PAGES)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_a_target_without_query_parameters_is_the_path_alone() {
        assert_eq!(OndoRequestTarget::new(MARKETS_PATH).as_str(), MARKETS_PATH);
        assert_eq!(OndoRequestTarget::new(STATUS_PATH).as_str(), STATUS_PATH);
    }

    #[rstest]
    fn test_the_first_query_parameter_is_introduced_with_a_question_mark() {
        let target = OndoRequestTarget::new(FILLS_PATH)
            .with_query_param("market", "NVDA-USD.P")
            .with_query_param("limit", "100");

        assert_eq!(
            target.as_str(),
            "/v1/perps/fills?market=NVDA-USD.P&limit=100",
        );
    }

    #[rstest]
    fn test_the_unreserved_set_is_left_alone_and_everything_else_is_uppercase_hex() {
        assert_eq!(percent_encode("AZaz09-._~"), "AZaz09-._~");
        assert_eq!(
            percent_encode("client:1 /+&=%"),
            "client%3A1%20%2F%2B%26%3D%25"
        );
        assert_eq!(percent_encode("é"), "%C3%A9");
    }

    #[rstest]
    fn test_the_client_order_lookup_value_is_the_documented_client_form() {
        assert_eq!(
            client_order_lookup("ondo_probe_example_1"),
            "client:ondo_probe_example_1"
        );
    }

    #[rstest]
    fn test_the_default_page_cap_is_bounded() {
        assert!(ONDO_MAX_PAGES > 0);
        assert_eq!(CursorWalk::default().pages(), 0);
    }
}
