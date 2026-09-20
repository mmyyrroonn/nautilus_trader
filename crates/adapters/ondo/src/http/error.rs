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

//! HTTP error taxonomy for the Ondo Perps REST API.
//!
//! The variants separate the failure classes a caller acts on differently: a transport failure is
//! retryable, a definitive venue rejection is not, and a metadata or schema failure fails closed
//! at start-up rather than being skipped.

use nautilus_model::identifiers::InstrumentId;
use thiserror::Error;

use crate::{
    common::{credential::OndoEnvironmentError, enums::OndoAuthenticationScope},
    signing::OndoSigningError,
};

/// Result alias for Ondo Perps HTTP operations.
pub type OndoHttpResult<T> = Result<T, OndoHttpError>;

/// The member names a venue error body may use for its error code.
const ERROR_CODE_FIELDS: [&str; 3] = ["code", "errorCode", "error_code"];

/// The named ways an authenticated Ondo Perps request can be refused.
///
/// `docs/api-reference/api_key_authentication.md` names every one of these (except the two
/// fallbacks) as an error code the venue returns, and `test_data/conflicts.md` conflict 1 requires
/// them to stay distinct: a sandbox `signature_mismatch` or `api_key_not_found` is a different
/// failure from a header contract mismatch and must be reported as such. Nothing here switches
/// account, retries differently, or falls back to another environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum OndoAuthFailure {
    /// The API key id the request carried is unknown to the venue (`api_key_not_found`).
    #[error("the venue does not know the API key id it was sent (`api_key_not_found`)")]
    ApiKeyNotFound,
    /// The timestamp could not be parsed (`failed_to_parse_timestamp`).
    #[error("the venue could not parse the signed timestamp (`failed_to_parse_timestamp`)")]
    FailedToParseTimestamp,
    /// The timestamp is outside the venue's ±30 s tolerance (`timestamp_too_far`).
    #[error(
        "the signed timestamp is outside the venue's \
         {} s tolerance (`timestamp_too_far`)",
        crate::signing::ONDO_SIGNATURE_TIMESTAMP_TOLERANCE_SECS
    )]
    TimestampTooFar,
    /// The signature was not decodable hex (`failed_to_decode_hex_signature`).
    #[error("the venue could not decode the hex signature (`failed_to_decode_hex_signature`)")]
    FailedToDecodeHexSignature,
    /// The signature did not match (`signature_mismatch`).
    #[error("the venue rejected the signature (`signature_mismatch`)")]
    SignatureMismatch,
    /// The key exists but does not carry the endpoint's scope (`key_doesnt_have_scope`).
    #[error("the API key does not carry the required scope (`key_doesnt_have_scope`)")]
    KeyDoesntHaveScope,
    /// The calling IP is not whitelisted for this key (`ip_not_permitted`).
    #[error("the calling IP is not whitelisted for this API key (`ip_not_permitted`)")]
    IpNotPermitted,
    /// The venue refused the request with HTTP 401 and no code this adapter recognises.
    #[error("the venue refused the credentials with HTTP 401 (`unauthorized`)")]
    Unauthorized,
    /// The venue refused the request with HTTP 403 and no code this adapter recognises.
    #[error("the venue refused the request with HTTP 403 (`forbidden`)")]
    Forbidden,
}

impl OndoAuthFailure {
    /// Returns the venue's own error code for this failure, or this adapter's name for a fallback.
    ///
    /// A stable, loggable identifier: it is the string the venue's documentation uses, so a report
    /// can be compared with the API-key page word for word.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ApiKeyNotFound => "api_key_not_found",
            Self::FailedToParseTimestamp => "failed_to_parse_timestamp",
            Self::TimestampTooFar => "timestamp_too_far",
            Self::FailedToDecodeHexSignature => "failed_to_decode_hex_signature",
            Self::SignatureMismatch => "signature_mismatch",
            Self::KeyDoesntHaveScope => "key_doesnt_have_scope",
            Self::IpNotPermitted => "ip_not_permitted",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
        }
    }
}

/// Builds the named auth rejection for a non-success answer to an authenticated request.
///
/// The venue sends its code inside the response body, in a JSON member
/// ([`extract_error_code`]) or as the plain text of a message; both are matched as the literal codes
/// the API-key page documents. A 401 or a 403 with no recognised code stays a named fallback rather
/// than being coerced into one of the specific failures - an unrecognised answer is not evidence of
/// a wrong key.
///
/// The caller redacts the credential from `body` before it is kept
/// ([`crate::common::credential::OndoCredential::redact`]): the venue's own `ip_not_permitted`
/// message echoes the key id it was sent.
#[must_use]
pub fn classify_auth_rejection(status: u16, body: String) -> OndoHttpError {
    let failure = classify_auth_failure(status, &body);
    let code = extract_error_code(&body);

    OndoHttpError::AuthRejected {
        failure,
        status,
        code,
        message: body,
    }
}

/// Classifies the venue's documented auth codes, then falls back to the status.
fn classify_auth_failure(status: u16, body: &str) -> OndoAuthFailure {
    /// Every code `api_key_authentication.md` lists, as the literal wire form.
    const KNOWN: [(&str, OndoAuthFailure); 7] = [
        ("api_key_not_found", OndoAuthFailure::ApiKeyNotFound),
        (
            "failed_to_parse_timestamp",
            OndoAuthFailure::FailedToParseTimestamp,
        ),
        ("timestamp_too_far", OndoAuthFailure::TimestampTooFar),
        (
            "failed_to_decode_hex_signature",
            OndoAuthFailure::FailedToDecodeHexSignature,
        ),
        ("signature_mismatch", OndoAuthFailure::SignatureMismatch),
        ("key_doesnt_have_scope", OndoAuthFailure::KeyDoesntHaveScope),
        ("ip_not_permitted", OndoAuthFailure::IpNotPermitted),
    ];

    if let Some((_code, failure)) = KNOWN.iter().find(|(code, _failure)| body.contains(code)) {
        return *failure;
    }

    if status == 403 {
        return OndoAuthFailure::Forbidden;
    }

    OndoAuthFailure::Unauthorized
}

/// Errors emitted by the Ondo Perps HTTP layer.
#[derive(Debug, Clone, Error)]
pub enum OndoHttpError {
    /// A network-level failure (transport, DNS, TLS).
    #[error("network error: {0}")]
    Network(String),
    /// A transport-level timeout: the request was sent and no answer arrived in time.
    ///
    /// This is distinct from [`Self::Network`] only so a caller can tell "the venue did not answer"
    /// from "the connection failed"; both are retryable.
    #[error("request timed out: {0}")]
    Timeout(String),
    /// An HTTP-level failure with its status code and body.
    ///
    /// The body is preserved raw; callers must sanitize it before logging or surfacing it.
    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },
    /// The venue rate limit was exceeded.
    #[error("rate limit exceeded: {0}")]
    RateLimit(String),
    /// The venue refused the request with HTTP 429.
    ///
    /// `retry_after_secs` is the `Retry-After` header's value when the venue sent it as a whole
    /// number of seconds. The header's HTTP-date form is not a wait this adapter can honour (the
    /// venue's clock is unverified), so it stays [`None`] and the retry falls back to the
    /// policy's exponential backoff. The body is preserved raw.
    #[error(
        "the venue rate limited the request (HTTP 429, Retry-After {retry_after_secs:?}): {body}"
    )]
    RateLimited {
        retry_after_secs: Option<u64>,
        /// The response body, verbatim. Sanitize it before logging or surfacing it.
        body: String,
    },
    /// The venue answered and rejected the request (a 4xx response other than 429).
    ///
    /// This is terminal: the same request produces the same answer, so retrying it only multiplies
    /// the rejection. `code` is the error code the body carried when it carried one
    /// ([`extract_error_code`]); `message` is the body verbatim and must be sanitized before it is
    /// logged or surfaced.
    #[error("the venue rejected the request (HTTP {status}, error code {code:?}): {message}")]
    RequestRejected {
        status: u16,
        code: Option<String>,
        /// The response body, verbatim.
        message: String,
    },
    /// A cursor-paginated read could not make progress and was stopped.
    ///
    /// Either the endpoint repeated a cursor it had already returned, or the walk reached its page
    /// cap. Both are loop guards: a retry would repeat the same failure.
    #[error("pagination stopped after {pages} page(s): {reason}")]
    Pagination {
        pages: usize,
        /// Why the walk stopped. The cursor itself is not echoed: it is an opaque venue token.
        reason: String,
    },
    /// The response body could not be decoded into the expected schema.
    #[error("decode error: {0}")]
    Decode(String),
    /// The response envelope reported failure.
    #[error("the venue reported an unsuccessful response")]
    Unsuccessful,
    /// A required member was absent from the response.
    #[error("missing field `{field}` in {context}")]
    MissingField {
        context: String,
        field: &'static str,
    },
    /// A present member was unusable.
    #[error("invalid field `{field}` value `{value}` in {context}: {reason}")]
    InvalidField {
        context: String,
        field: &'static str,
        value: String,
        reason: String,
    },
    /// A venue market string cannot be mapped to an instrument without renaming it.
    #[error("unsupported market `{market}`: {reason}")]
    UnsupportedMarket { market: String, reason: String },
    /// A market's raw status is not a value this adapter can classify.
    #[error("market `{market}` reports an unclassifiable status `{raw}`")]
    UnknownMarketStatus { market: String, raw: String },
    /// A requested instrument is absent from the response.
    #[error("the response does not carry the requested instrument `{instrument_id}`")]
    MissingInstrument { instrument_id: InstrumentId },
    /// The response carried no perps market data at all.
    #[error("the response carried no perps market data")]
    EmptyResult,
    /// An authenticated session was refused before any credential was read or any socket opened.
    ///
    /// The environment gate's refusal ([`crate::common::credential::validate_authenticated_environment`]).
    /// It is terminal by construction: there is no second environment to fall back to.
    #[error(transparent)]
    Environment(#[from] OndoEnvironmentError),
    /// A signed request was refused a signature before it was sent.
    ///
    /// A request that cannot be signed is never sent, so this says nothing about the venue.
    #[error(transparent)]
    Signing(#[from] OndoSigningError),
    /// The venue refused an authenticated request, with its own named reason.
    ///
    /// This is the authenticated path's rejection: it keeps the HTTP status, the venue's error code
    /// and the body (with the credential redacted out of it). It is never retried, and never
    /// re-signed against another environment or account.
    #[error(
        "the venue rejected the authenticated request (HTTP {status}, {failure}, error code {code:?}): {message}"
    )]
    AuthRejected {
        /// The named failure class.
        failure: OndoAuthFailure,
        /// The HTTP status the venue answered with.
        status: u16,
        /// The venue's error code, when the body carried one.
        code: Option<String>,
        /// The redacted response body, verbatim otherwise.
        message: String,
    },
    /// A signed request was attempted on a client that holds no credential.
    ///
    /// The client is not repaired by reading the environment: credentials are resolved before it is
    /// built, or not at all.
    #[error("the Ondo Perps HTTP client has no credential, so it cannot sign `{target}`")]
    NotAuthenticated {
        /// The request target whose signature was required.
        target: String,
    },
    /// A signed write was attempted on a session whose authorization scope refuses every write.
    ///
    /// This is the dispatch guard the read-only scopes require: it runs at the top of the signed
    /// `POST` and `DELETE` paths, before the rate budget is acquired and before the new-risk guard
    /// is consulted, so a read-only session sends neither a submission nor a cancel whatever the
    /// account's own admission says. It is a local refusal: no request was built.
    #[error(
        "this Ondo Perps session is {scope:?} and refuses every signed {method}: read-only sessions \
         never place or cancel orders (plan §6.1)"
    )]
    WriteNotPermitted {
        /// The scope that refused the write.
        scope: OndoAuthenticationScope,
        /// The HTTP method the write carried, as the dispatch names it.
        method: &'static str,
    },
}

/// Returns `true` when a request producing this error is safe to retry.
///
/// Only transport-level failures and rate limits are transient. Schema and metadata failures are
/// deterministic: the same response fails the same way, so a retry only multiplies the damage.
/// A definitive venue rejection ([`OndoHttpError::RequestRejected`]) is terminal: 401 and 403 in
/// particular must never be retried in a loop. A pagination guard is not retried either, because
/// the walk that produced it would fail identically.
#[must_use]
pub fn should_retry_ondo_http_error(error: &OndoHttpError) -> bool {
    match error {
        OndoHttpError::Network(_)
        | OndoHttpError::Timeout(_)
        | OndoHttpError::RateLimit(_)
        | OndoHttpError::RateLimited { .. } => true,
        OndoHttpError::Http { status, .. } => *status >= 500,
        OndoHttpError::RequestRejected { .. }
        | OndoHttpError::Pagination { .. }
        | OndoHttpError::Decode(_)
        | OndoHttpError::Unsuccessful
        | OndoHttpError::MissingField { .. }
        | OndoHttpError::InvalidField { .. }
        | OndoHttpError::UnsupportedMarket { .. }
        | OndoHttpError::UnknownMarketStatus { .. }
        | OndoHttpError::MissingInstrument { .. }
        | OndoHttpError::EmptyResult
        | OndoHttpError::Environment(_)
        | OndoHttpError::Signing(_)
        | OndoHttpError::AuthRejected { .. }
        | OndoHttpError::NotAuthenticated { .. }
        | OndoHttpError::WriteNotPermitted { .. } => false,
    }
}

/// Extracts the venue's error code from a rejection body, when the body carries one.
///
/// Two shapes are handled, and neither routes a number through `f64`: a JSON object with a `code`,
/// `errorCode` or `error_code` member that is a string, or an exact integer printed as its own
/// digits; and the plain-text form the live host returned behind Cloudflare, `error code: 1010`.
/// Anything else is [`None`], because a code this adapter cannot read is not a code.
pub(crate) fn extract_error_code(body: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        for field in ERROR_CODE_FIELDS {
            let Some(code) = value.get(field) else {
                continue;
            };

            match code {
                serde_json::Value::String(code) if !code.is_empty() => return Some(code.clone()),
                serde_json::Value::Number(number) if number.is_u64() || number.is_i64() => {
                    return Some(number.to_string());
                }
                _ => continue,
            }
        }
    }

    const MARKER: &str = "error code:";
    let index = body.find(MARKER)?;
    let code: String = body[index + MARKER.len()..]
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();

    (!code.is_empty()).then_some(code)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::network(OndoHttpError::Network("dns failure".to_string()), true)]
    #[case::timeout(OndoHttpError::Timeout("no answer in 15s".to_string()), true)]
    #[case::rate_limit(OndoHttpError::RateLimit("429".to_string()), true)]
    #[case::rate_limited(
        OndoHttpError::RateLimited { retry_after_secs: Some(1), body: "slow down".to_string() },
        true
    )]
    #[case::server_error(OndoHttpError::Http { status: 503, body: "busy".to_string() }, true)]
    #[case::client_error(OndoHttpError::Http { status: 400, body: "bad".to_string() }, false)]
    #[case::forbidden(OndoHttpError::Http { status: 403, body: "error code: 1010".to_string() }, false)]
    #[case::rejected(
        OndoHttpError::RequestRejected {
            status: 403,
            code: Some("1010".to_string()),
            message: "error code: 1010".to_string(),
        },
        false
    )]
    #[case::pagination(
        OndoHttpError::Pagination { pages: 2, reason: "repeated cursor".to_string() },
        false
    )]
    #[case::decode(OndoHttpError::Decode("bad json".to_string()), false)]
    #[case::unsuccessful(OndoHttpError::Unsuccessful, false)]
    #[case::empty(OndoHttpError::EmptyResult, false)]
    fn test_should_retry(#[case] error: OndoHttpError, #[case] expected: bool) {
        assert_eq!(should_retry_ondo_http_error(&error), expected);
    }

    #[rstest]
    #[case::string_code(r#"{"code":"1010","message":"forbidden"}"#, Some("1010"))]
    #[case::camel_code(r#"{"errorCode":"UNAUTHORIZED"}"#, Some("UNAUTHORIZED"))]
    #[case::snake_code(r#"{"error_code":"ORDER_NOT_FOUND"}"#, Some("ORDER_NOT_FOUND"))]
    #[case::integer_code(r#"{"code":1010}"#, Some("1010"))]
    #[case::nested_envelope(
        r#"{"success":false,"errorCode":"INSUFFICIENT_MARGIN"}"#,
        Some("INSUFFICIENT_MARGIN")
    )]
    #[case::observed_cloudflare_text("error code: 1010", Some("1010"))]
    #[case::observed_cloudflare_text_with_suffix("error code: 1010 (Cloudflare)", Some("1010"))]
    #[case::float_is_not_a_code(r#"{"code":10.10}"#, None)]
    #[case::no_code(r#"{"message":"forbidden"}"#, None)]
    #[case::not_json("forbidden", None)]
    #[case::empty(r#"{"code":""}"#, None)]
    fn test_extract_error_code(#[case] body: &str, #[case] expected: Option<&str>) {
        assert_eq!(extract_error_code(body).as_deref(), expected);
    }

    #[rstest]
    fn test_a_rejection_renders_its_status_and_code() {
        let error = OndoHttpError::RequestRejected {
            status: 401,
            code: Some("UNAUTHORIZED".to_string()),
            message: r#"{"errorCode":"UNAUTHORIZED"}"#.to_string(),
        };

        let rendered = error.to_string();

        assert!(rendered.contains("401"), "{rendered}");
        assert!(rendered.contains("UNAUTHORIZED"), "{rendered}");
    }

    #[rstest]
    fn test_unknown_market_status_preserves_the_raw_value() {
        let error = OndoHttpError::UnknownMarketStatus {
            market: "SYNTH-USD.P".to_string(),
            raw: "halted".to_string(),
        };

        assert!(error.to_string().contains("halted"));
        assert!(error.to_string().contains("SYNTH-USD.P"));
    }

    #[rstest]
    fn test_missing_instrument_names_the_instrument() {
        let error = OndoHttpError::MissingInstrument {
            instrument_id: InstrumentId::from_str("NVDA-USD-PERP.ONDO").unwrap(),
        };

        assert!(error.to_string().contains("NVDA-USD-PERP.ONDO"));
    }
}
