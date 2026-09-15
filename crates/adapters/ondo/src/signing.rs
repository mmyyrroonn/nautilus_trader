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

//! Request signing for the authenticated Ondo Perps endpoints.
//!
//! # REST
//!
//! The API-key page (`docs/api-reference/api_key_authentication.md`) defines the signed message as
//! four values concatenated, keyed with the **full** API secret including its `ondoApiSecret_`
//! prefix:
//!
//! ```text
//! message   = timestamp_ms + UPPERCASE_METHOD + exact_path_and_query + exact_body
//! signature = hex(HMAC_SHA256(secret_with_prefix, message))
//! ```
//!
//! [`sign_rest`] is that function and nothing else: it takes the path-and-query and the body as the
//! bytes they already are, so a caller cannot re-encode a query or re-serialize a JSON body between
//! the signature and the wire. The transport pairs it with the same
//! [`crate::http::query::OndoRequestTarget`] bytes it sends (serialize once, reuse), and the
//! headers it sends are [`ONDO_KEY_ID_HEADER`], [`ONDO_TIMESTAMP_HEADER`] and [`ONDO_SIGN_HEADER`].
//!
//! # The header conflict
//!
//! `test_data/conflicts.md` conflict 1 records two readings of the REST auth headers: the API-key
//! page's three headers (`ONDO-KEY-ID`, `ONDO-TIMESTAMP`, `ONDO-SIGN`) and the REST spec's single
//! `X-API-KEY-ID`. Reading A is the only one that can authenticate anything - the single header
//! cannot carry a timestamp or a signature - so it is what this module implements, and
//! `X-API-KEY-ID` is deliberately **not** emitted anywhere, not even as a second attempt. The
//! header contract stays UNVERIFIED until a sandbox request succeeds; the naming constants live in
//! one place, so a confirmed contract is a change here and nowhere else.
//!
//! # WebSocket
//!
//! `conflicts.md` conflict 2 is a second unresolved conflict, over the login digest's
//! concatenation order: the login page's prose says `time + "ondo_perps_ws_login"` (reading A)
//! while the shared OpenAPI description in `connect.md`/`ws-spec.json` says
//! `"ondo_perps_ws_login" + time` (reading B). [`sign_ws`] implements reading A, and reading A is
//! the only order this crate can produce: [`ws_login_message`] is the single place the order is
//! written, and nothing tries the other one. If sandbox answers reading A with a
//! `signature_mismatch`, flipping the concatenation in that one function is the whole change - it
//! is never an automatic fallback against a live venue.
//!
//! # The clock
//!
//! The venue accepts a timestamp within ±30 seconds of its own
//! ([`ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS`]). A local clock that is further away than that
//! cannot produce a signature the venue will accept, so [`check_clock_skew`] refuses to sign rather
//! than let the venue answer `timestamp_too_far`; the authenticated client feeds it the offset the
//! venue's HTTP `Date` header implies. That header has one-second granularity, so it can never
//! *prove* millisecond synchronisation: the offset is used to refuse, never to adjust the
//! timestamp it signs, and an offset the evidence cannot prove tolerable is refused.

use std::{collections::HashMap, time::SystemTime};

use aws_lc_rs::hmac;
use nautilus_core::hex;

use crate::common::credential::OndoCredential;

/// The header carrying the API key id, prefixes intact (conflict 1, reading A).
///
/// The REST spec's `components.securitySchemes.ApiKeyAuth` names `X-API-KEY-ID` instead, which
/// cannot carry a timestamp or a signature. That reading is recorded and **not** implemented, and
/// must not be emitted as a fallback: see the module documentation.
pub const ONDO_KEY_ID_HEADER: &str = "ONDO-KEY-ID";

/// The header carrying the millisecond timestamp the signature was computed over.
pub const ONDO_TIMESTAMP_HEADER: &str = "ONDO-TIMESTAMP";

/// The header carrying the lowercase hex HMAC-SHA256 signature.
pub const ONDO_SIGN_HEADER: &str = "ONDO-SIGN";

/// The venue's tolerance around the signed timestamp, in seconds.
///
/// `api_key_authentication.md`: the `ONDO-TIMESTAMP` value "must be within 30 seconds of the time
/// the request is received".
pub const ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS: u64 = 30;

/// Granularity of the HTTP `Date` header, in seconds.
///
/// A timestamp read from that header is exact only to the second, which is why the skew rule
/// accounts for it instead of pretending the venue's clock is known to the millisecond.
pub const ONDO_HTTP_DATE_GRANULARITY_SECS: u64 = 1;

/// The WebSocket login phrase, verbatim from the login page.
pub const ONDO_WS_LOGIN_PHRASE: &str = "ondo_perps_ws_login";

/// Why a request was refused a signature.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OndoSigningError {
    /// The local clock is too far from the venue's to produce a timestamp it will accept.
    #[error(
        "refusing to sign: the local clock is {offset_secs} s from the venue's, beyond the \
         {tolerance_secs} s signature timestamp tolerance (the offset came from an HTTP `Date` \
         header, which is only second-accurate, so millisecond synchronisation cannot be proven)"
    )]
    ClockSkew {
        /// Local time minus venue time, in seconds, as last observed.
        offset_secs: i64,
        /// The venue's tolerance, in seconds.
        tolerance_secs: u64,
    },
    /// The system clock is before the Unix epoch, so it cannot produce a millisecond timestamp.
    #[error("refusing to sign: the system clock is before the Unix epoch")]
    ClockBeforeEpoch,
}

/// Signs an authenticated REST request and returns the lowercase hex signature.
///
/// `method` is upper-cased, `path_query` and `body` are signed as the exact bytes they are. The
/// caller must send those same bytes: see [`crate::http::query::OndoRequestTarget`] for the
/// serialize-once seam.
#[must_use]
pub fn sign_rest(
    credential: &OndoCredential,
    timestamp_ms: u64,
    method: &str,
    path_query: &str,
    body: &[u8],
) -> String {
    let message = rest_message(timestamp_ms, method, path_query, body);

    hmac_sha256_hex(credential.api_secret(), &message)
}

/// Signs the WebSocket API-key login message and returns the lowercase hex signature.
///
/// The phrase's concatenation order is [`ws_login_message`]'s decision and is reading A of
/// `conflicts.md` conflict 2 (`time` first).
#[must_use]
pub fn sign_ws(credential: &OndoCredential, timestamp_ms: u64) -> String {
    hmac_sha256_hex(
        credential.api_secret(),
        ws_login_message(timestamp_ms).as_bytes(),
    )
}

/// The signed message of a REST request: the four values concatenated, with no separator.
fn rest_message(timestamp_ms: u64, method: &str, path_query: &str, body: &[u8]) -> Vec<u8> {
    let timestamp = timestamp_ms.to_string();
    let method = method.to_uppercase();

    let mut message =
        Vec::with_capacity(timestamp.len() + method.len() + path_query.len() + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.extend_from_slice(method.as_bytes());
    message.extend_from_slice(path_query.as_bytes());
    message.extend_from_slice(body);

    message
}

/// The signed message of a WebSocket API-key login.
///
/// **This is the one place `conflicts.md` conflict 2's concatenation order is written.** Reading A
/// (`time` first) is implemented; reading B is `format!("{ONDO_WS_LOGIN_PHRASE}{timestamp_ms}")`.
/// A sandbox `signature_mismatch` under reading A makes that a one-line change here - and the
/// `ws_login_cases` reference fixture already records both digests, so the test that covers this
/// function proves which order the code produces.
fn ws_login_message(timestamp_ms: u64) -> String {
    format!("{timestamp_ms}{ONDO_WS_LOGIN_PHRASE}")
}

/// Returns `hex(HMAC_SHA256(key, message))`, lowercase.
fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&key, message);

    hex::encode(tag.as_ref())
}

/// Returns the authenticated headers of a signed REST request.
///
/// Crate-private: the header contract is a property of this module and of the transport that sends
/// it, and there is exactly one implementation of it (`test_data/conflicts.md` conflict 1).
pub(crate) fn signed_headers(
    credential: &OndoCredential,
    timestamp_ms: u64,
    signature: &str,
) -> HashMap<String, String> {
    HashMap::from([
        (
            ONDO_KEY_ID_HEADER.to_string(),
            credential.key_id().to_string(),
        ),
        (ONDO_TIMESTAMP_HEADER.to_string(), timestamp_ms.to_string()),
        (ONDO_SIGN_HEADER.to_string(), signature.to_string()),
    ])
}

/// Refuses a clock offset the venue's ±30 s tolerance cannot absorb.
///
/// `offset_secs` is local time minus venue time. The observed offset carries
/// [`ONDO_HTTP_DATE_GRANULARITY_SECS`] of uncertainty, so the refusal starts at the first offset
/// the evidence cannot prove is tolerable rather than at the tolerance itself.
///
/// # Errors
///
/// Returns [`OndoSigningError::ClockSkew`] when `|offset_secs| + granularity >= tolerance`.
pub fn check_clock_skew(offset_secs: i64) -> Result<(), OndoSigningError> {
    if offset_secs
        .unsigned_abs()
        .saturating_add(ONDO_HTTP_DATE_GRANULARITY_SECS)
        >= ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS
    {
        return Err(OndoSigningError::ClockSkew {
            offset_secs,
            tolerance_secs: ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS,
        });
    }

    Ok(())
}

/// Returns the offset (venue minus local) an HTTP `Date` header implies, in whole seconds.
///
/// Only the IMF-fixdate form a modern server sends (`Sun, 06 Nov 1994 08:49:37 GMT`) is read; the
/// obsolete RFC 850 and asctime forms are [`None`], the same refusal `Retry-After`'s HTTP-date form
/// gets in [`crate::http::client`]. A header this adapter cannot read is not clock evidence.
pub(crate) fn http_date_offset_secs(local_secs: i64, http_date: &str) -> Option<i64> {
    parse_http_date_secs(http_date).map(|venue_secs| venue_secs - local_secs)
}

/// Returns the local wall clock in whole seconds since the Unix epoch.
pub(crate) fn now_secs() -> Result<i64, OndoSigningError> {
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| OndoSigningError::ClockBeforeEpoch)?;

    i64::try_from(elapsed.as_secs()).map_err(|_| OndoSigningError::ClockBeforeEpoch)
}

/// Returns the local wall clock in milliseconds since the Unix epoch.
pub(crate) fn now_millis() -> Result<u64, OndoSigningError> {
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| OndoSigningError::ClockBeforeEpoch)?;

    u64::try_from(elapsed.as_millis()).map_err(|_| OndoSigningError::ClockBeforeEpoch)
}

/// Parses an IMF-fixdate timestamp into whole seconds since the Unix epoch.
fn parse_http_date_secs(value: &str) -> Option<i64> {
    let mut parts = value.split_ascii_whitespace();

    let weekday = parts.next()?;
    if weekday.len() != 4 || !weekday.ends_with(',') {
        return None;
    }

    let day = parse_digits::<2>(parts.next()?)?;
    let month = month_of(parts.next()?)?;
    let year = parse_digits::<4>(parts.next()?)?;
    let time = parts.next()?;
    if parts.next()? != "GMT" || parts.next().is_some() {
        return None;
    }

    let mut clock = time.split(':');
    let hour = parse_digits::<2>(clock.next()?)?;
    let minute = parse_digits::<2>(clock.next()?)?;
    let second = parse_digits::<2>(clock.next()?)?;
    if clock.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    // A leap second is clamped to the last second of its minute: the header is second-accurate
    // evidence about a clock, and a leap second must not make the whole header unreadable.
    let second = second.min(59);

    if day == 0 || day > days_in_month(year, month) {
        return None;
    }

    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second))
}

/// Parses exactly `N` ASCII digits, rejecting anything else (including a signed or longer number).
fn parse_digits<const N: usize>(value: &str) -> Option<u32> {
    if value.len() != N || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    value.parse::<u32>().ok()
}

/// Maps the three-letter English month name of an IMF-fixdate onto its number.
fn month_of(value: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    MONTHS
        .iter()
        .position(|month| *month == value)
        .map(|index| u32::try_from(index).unwrap_or(0) + 1)
}

/// Returns whether `year` is a leap year in the proleptic Gregorian calendar.
const fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// Returns the number of days in `month` of `year`.
const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Returns days since 1970-01-01 for a proleptic-Gregorian date (Howard Hinnant's `days_from_civil`).
const fn days_from_civil(year: u32, month: u32, day: u32) -> i64 {
    let year = year as i64;
    let month = month as i64;
    let day = day as i64;

    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = (month + 9) % 12;
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::enums::OndoEnvironment;

    fn credential() -> OndoCredential {
        OndoCredential::new(
            OndoEnvironment::Sandbox,
            "ondoKeyId_UNIT_TEST_ONLY".to_string(),
            "ondoApiSecret_UNIT_TEST_ONLY".to_string(),
        )
        .expect("the fake credential builds")
    }

    /// The Task 6 brief's own vector, computed by Python (`hashlib` + `hmac`):
    /// `hmac.new(b"ondoApiSecret_UNIT_TEST_ONLY", b"1789384200000GET/v1/perps/orders?market=NVDA-USD.P&limit=2",
    /// hashlib.sha256).hexdigest()`.
    #[rstest]
    fn test_the_message_is_the_four_values_concatenated() {
        assert_eq!(
            rest_message(
                1_789_384_200_000,
                "get",
                "/v1/perps/orders?market=NVDA-USD.P&limit=2",
                b"",
            ),
            b"1789384200000GET/v1/perps/orders?market=NVDA-USD.P&limit=2",
        );

        let signature = sign_rest(
            &credential(),
            1_789_384_200_000,
            "GET",
            "/v1/perps/orders?market=NVDA-USD.P&limit=2",
            b"",
        );
        assert_eq!(
            signature,
            "df4ba6005e3f028f9928a80b0f8dd33ad6af488418dffed317a9543d2d9bb816",
        );
    }

    #[rstest]
    fn test_the_ws_login_message_is_the_timestamp_first_order() {
        assert_eq!(
            ws_login_message(1_789_384_200_000),
            "1789384200000ondo_perps_ws_login",
        );
        // Reading A's digest of reading A's message, taken from the independently generated
        // `test_data/signing_rest_vectors.json` (`ws_login_cases`): `hex(HMAC_SHA256(
        // b"ondoApiSecret_UNIT_TEST_ONLY", b"1789384200000ondo_perps_ws_login"))`. Reading B's
        // digest (`c753ee5e...`) is recorded in the same fixture and is what makes this an order
        // assertion rather than a round-trip.
        assert_eq!(
            sign_ws(&credential(), 1_789_384_200_000),
            "29b1e3ca1a71e3771d4f6bc375308b86e52577c16fd2edede0ee58a06070b4b4",
        );
    }

    #[rstest]
    fn test_the_header_names_are_the_api_key_page_contract() {
        assert_eq!(ONDO_KEY_ID_HEADER, "ONDO-KEY-ID");
        assert_eq!(ONDO_TIMESTAMP_HEADER, "ONDO-TIMESTAMP");
        assert_eq!(ONDO_SIGN_HEADER, "ONDO-SIGN");

        // The REST spec's alternative is recorded and not emitted.
        assert!(!ONDO_KEY_ID_HEADER.eq_ignore_ascii_case("X-API-KEY-ID"));
    }

    #[rstest]
    fn test_the_signed_headers_carry_the_key_id_the_timestamp_and_the_signature() {
        let headers = signed_headers(&credential(), 1_789_384_200_000, "deadbeef");

        assert_eq!(
            headers.len(),
            3,
            "exactly the three headers, never a fourth"
        );
        assert_eq!(
            headers.get(ONDO_KEY_ID_HEADER).map(String::as_str),
            Some("ondoKeyId_UNIT_TEST_ONLY"),
        );
        assert_eq!(
            headers.get(ONDO_TIMESTAMP_HEADER).map(String::as_str),
            Some("1789384200000"),
        );
        assert_eq!(
            headers.get(ONDO_SIGN_HEADER).map(String::as_str),
            Some("deadbeef"),
        );
    }

    #[rstest]
    #[case::epoch("Thu, 01 Jan 1970 00:00:00 GMT", 0)]
    #[case::observed_date("Mon, 14 Sep 2026 15:15:02 GMT", 1_789_398_902)]
    #[case::leap_day("Thu, 29 Feb 2024 12:00:00 GMT", 1_709_208_000)]
    #[case::end_of_year("Wed, 31 Dec 1969 23:59:59 GMT", -1)]
    fn test_imf_fixdate_parsing(#[case] value: &str, #[case] expected: i64) {
        assert_eq!(parse_http_date_secs(value), Some(expected), "{value}");
    }

    #[rstest]
    #[case::rfc850_obsolete("Sunday, 06-Nov-94 08:49:37 GMT")]
    #[case::asctime_obsolete("Sun Nov  6 08:49:37 1994")]
    #[case::bad_month("Mon, 14 Foo 2026 15:15:02 GMT")]
    #[case::impossible_day("Mon, 30 Feb 2026 15:15:02 GMT")]
    #[case::day_zero("Mon, 00 Sep 2026 15:15:02 GMT")]
    #[case::bad_clock("Mon, 14 Sep 2026 25:15:02 GMT")]
    #[case::not_gmt("Mon, 14 Sep 2026 15:15:02 UTC")]
    #[case::trailing_junk("Mon, 14 Sep 2026 15:15:02 GMT extra")]
    #[case::empty("")]
    fn test_unreadable_http_dates_are_never_clock_evidence(#[case] value: &str) {
        assert_eq!(parse_http_date_secs(value), None, "{value}");
    }

    #[rstest]
    fn test_a_leap_second_is_clamped_rather_than_refused() {
        assert_eq!(
            parse_http_date_secs("Sat, 31 Dec 2016 23:59:60 GMT"),
            parse_http_date_secs("Sat, 31 Dec 2016 23:59:59 GMT"),
        );
    }

    #[rstest]
    fn test_the_offset_is_the_venue_minus_the_local_clock() {
        const VENUE_SECS: i64 = 1_789_398_902;

        // Local clock 40 s ahead of the venue.
        assert_eq!(
            http_date_offset_secs(VENUE_SECS + 40, "Mon, 14 Sep 2026 15:15:02 GMT"),
            Some(-40),
        );
        assert_eq!(
            http_date_offset_secs(VENUE_SECS - 40, "Mon, 14 Sep 2026 15:15:02 GMT"),
            Some(40),
        );
        assert_eq!(
            http_date_offset_secs(VENUE_SECS, "Mon, 14 Sep 2026 15:15:02 GMT"),
            Some(0),
        );
        assert_eq!(
            http_date_offset_secs(VENUE_SECS, "not a date"),
            None,
            "an unreadable header is not evidence of a skew",
        );
    }

    #[rstest]
    fn test_every_signing_error_renders_without_a_credential() {
        let errors = [
            check_clock_skew(45).unwrap_err(),
            OndoSigningError::ClockBeforeEpoch,
        ];

        for error in errors {
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("ondoApiSecret"), "{rendered}");
            assert!(!rendered.contains("ondoKeyId"), "{rendered}");
        }
    }
}
