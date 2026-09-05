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

//! Aster HTTP error types.
//!
//! Aster returns Binance-shaped error payloads, `{"code": -1121, "msg": "Invalid symbol."}`.
//! The codes carry the same meanings as Binance USD-M, so the classification helpers below
//! let the execution client decide whether a failure is a hard venue rejection, a transient
//! rate limit, or an ambiguous outcome that reconciliation should resolve.

use std::fmt::Display;

use nautilus_network::http::error::HttpClientError;

/// Rate limit exceeded (`TOO_MANY_REQUESTS`).
pub const ASTER_CODE_TOO_MANY_REQUESTS: i64 = -1003;
/// An unexpected response reached the venue's message bus (`UNEXPECTED_RESP`).
///
/// Aster documents this as "execution status unknown": an order submitted under this code may
/// already be resting on the book or filled.
pub const ASTER_CODE_UNEXPECTED_RESP: i64 = -1006;
/// The venue timed out waiting for its own backend (`TIMEOUT`).
///
/// Aster documents this as "Send status unknown; execution status unknown", so the same
/// ambiguity as [`ASTER_CODE_UNEXPECTED_RESP`] applies.
pub const ASTER_CODE_TIMEOUT: i64 = -1007;
/// Nonce outside the accepted window, or already used (`INVALID_TIMESTAMP`).
pub const ASTER_CODE_INVALID_NONCE: i64 = -1021;
/// Signature rejected (`INVALID_SIGNATURE`).
pub const ASTER_CODE_INVALID_SIGNATURE: i64 = -1022;
/// Listen key rejected (`INVALID_LISTEN_KEY`).
pub const ASTER_CODE_INVALID_LISTEN_KEY: i64 = -1125;
/// New order rejected (`NEW_ORDER_REJECTED`).
pub const ASTER_CODE_NEW_ORDER_REJECTED: i64 = -2010;
/// Cancel rejected because the order is unknown (`CANCEL_REJECTED`).
pub const ASTER_CODE_CANCEL_REJECTED: i64 = -2011;
/// Order does not exist (`NO_SUCH_ORDER`).
pub const ASTER_CODE_NO_SUCH_ORDER: i64 = -2013;
/// Balance insufficient (`BALANCE_NOT_SUFFICIENT`).
pub const ASTER_CODE_BALANCE_NOT_SUFFICIENT: i64 = -2018;
/// Margin insufficient (`MARGIN_NOT_SUFFICIENT`).
pub const ASTER_CODE_MARGIN_NOT_SUFFICIENT: i64 = -2019;
/// Post-only order would have matched immediately (`ORDER_WOULD_IMMEDIATELY_TRIGGER`).
pub const ASTER_CODE_ORDER_WOULD_IMMEDIATELY_TRIGGER: i64 = -2021;
/// Order notional below the venue minimum (`MIN_NOTIONAL`).
pub const ASTER_CODE_MIN_NOTIONAL: i64 = -4164;
/// The master wallet has never deposited, so signed endpoints are unavailable.
pub const ASTER_CODE_UNFUNDED_WALLET: i64 = -5050;

/// The Aster codes that leave the execution status of a request unknown.
///
/// Every other structured `{code, msg}` body is a decision the venue already made, so it can be
/// terminalised. These two are explicitly documented as "execution status unknown" and must be
/// resolved by querying the order instead.
///
/// # References
///
/// - <https://asterdex.github.io/aster-api-website/futures-v3/error-codes/>
pub const ASTER_EXECUTION_STATUS_UNKNOWN_CODES: [i64; 2] =
    [ASTER_CODE_UNEXPECTED_RESP, ASTER_CODE_TIMEOUT];

/// Aster HTTP client error.
#[derive(Debug)]
pub enum AsterHttpError {
    /// No signing credentials were configured for an authenticated request.
    MissingCredentials,
    /// The Aster API returned a structured `{code, msg}` error payload.
    AsterError {
        /// Aster error code (negative).
        code: i64,
        /// Error message from Aster.
        message: String,
    },
    /// Request signing failed.
    SigningError(String),
    /// JSON parsing or serialization error.
    JsonError(String),
    /// Request validation error raised before anything was sent.
    ValidationError(String),
    /// Network or connection error.
    NetworkError(String),
    /// Request timed out.
    Timeout(String),
    /// Unexpected HTTP status without a parsable Aster error payload.
    UnexpectedStatus {
        /// HTTP status code.
        status: u16,
        /// Response body.
        body: String,
    },
}

impl Display for AsterHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredentials => write!(f, "Missing Aster signing credentials"),
            Self::AsterError { code, message } => write!(f, "Aster error {code}: {message}"),
            Self::SigningError(msg) => write!(f, "Signing error: {msg}"),
            Self::JsonError(msg) => write!(f, "JSON error: {msg}"),
            Self::ValidationError(msg) => write!(f, "Validation error: {msg}"),
            Self::NetworkError(msg) => write!(f, "Network error: {msg}"),
            Self::Timeout(msg) => write!(f, "Timeout: {msg}"),
            Self::UnexpectedStatus { status, body } => {
                write!(f, "Unexpected status {status}: {body}")
            }
        }
    }
}

impl std::error::Error for AsterHttpError {}

impl AsterHttpError {
    /// Returns the Aster error code when this is a structured venue error.
    #[must_use]
    pub const fn code(&self) -> Option<i64> {
        match self {
            Self::AsterError { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns whether the venue left the execution status of the request unknown.
    ///
    /// True for the two Aster codes documented as "execution status unknown"
    /// ([`ASTER_CODE_UNEXPECTED_RESP`] and [`ASTER_CODE_TIMEOUT`]). The venue answered, but its
    /// answer says nothing about whether the order reached the book, so the outcome must be
    /// resolved by querying the order rather than terminalised.
    #[must_use]
    pub fn is_execution_status_unknown(&self) -> bool {
        self.code()
            .is_some_and(|code| ASTER_EXECUTION_STATUS_UNKNOWN_CODES.contains(&code))
    }

    /// Returns whether the outcome of a non-idempotent request is ambiguous.
    ///
    /// True whenever the failure leaves open the possibility that the venue acted on the
    /// request anyway:
    ///
    /// - a transport fault that never produced a response ([`Self::is_retryable_transport`]);
    /// - a structured body that declines to report the execution status
    ///   ([`Self::is_execution_status_unknown`]);
    /// - a response body that could not be decoded, which says nothing about what the venue
    ///   did with the request;
    /// - a `5xx` or `408` status without an Aster error body, typically produced by an edge
    ///   proxy that may well have forwarded the request.
    ///
    /// False for failures that provably never reached the matching engine: missing credentials,
    /// signing and validation faults raised before the send, and `4xx` statuses, which are the
    /// venue refusing the request outright.
    ///
    /// An ambiguous submission must never be resubmitted and must never be terminalised; the
    /// execution client resolves it with `GET /fapi/v3/order`.
    #[must_use]
    pub fn is_ambiguous_execution(&self) -> bool {
        match self {
            Self::NetworkError(_) | Self::Timeout(_) | Self::JsonError(_) => true,
            Self::UnexpectedStatus { status, .. } => *status >= 500 || *status == 408,
            Self::AsterError { .. } => self.is_execution_status_unknown(),
            Self::MissingCredentials | Self::SigningError(_) | Self::ValidationError(_) => false,
        }
    }

    /// Returns whether this is a definitive venue rejection.
    ///
    /// True only when the venue answered with a structured `{code, msg}` body that reports a
    /// decision it already made: an invalid symbol, a filter violation, insufficient margin, a
    /// rejected signature, and so on. The order never reached the book, so the execution client
    /// can emit `OrderRejected` without waiting for reconciliation.
    ///
    /// False for transport failures *and* for the "execution status unknown" codes, which are
    /// structured bodies that carry no decision (see [`Self::is_execution_status_unknown`]).
    /// Treating those as rejections was the failure this predicate exists to prevent: the order
    /// can be resting or filled while the local state says rejected.
    #[must_use]
    pub fn is_venue_rejection(&self) -> bool {
        matches!(self, Self::AsterError { .. }) && !self.is_execution_status_unknown()
    }

    /// Returns whether the venue rate-limited the request.
    #[must_use]
    pub fn is_rate_limited(&self) -> bool {
        self.code() == Some(ASTER_CODE_TOO_MANY_REQUESTS)
    }

    /// Returns whether the request was rejected for authentication reasons.
    ///
    /// Covers a bad signature, a nonce outside the accepted window, and the "wallet has never
    /// deposited" gate that Aster applies to every signed V3 endpoint.
    #[must_use]
    pub fn is_auth_failure(&self) -> bool {
        matches!(
            self.code(),
            Some(
                ASTER_CODE_INVALID_NONCE
                    | ASTER_CODE_INVALID_SIGNATURE
                    | ASTER_CODE_INVALID_LISTEN_KEY
                    | ASTER_CODE_UNFUNDED_WALLET
            )
        )
    }

    /// Returns whether the venue reported the order as unknown.
    #[must_use]
    pub fn is_unknown_order(&self) -> bool {
        matches!(
            self.code(),
            Some(ASTER_CODE_CANCEL_REJECTED | ASTER_CODE_NO_SUCH_ORDER)
        )
    }

    /// Returns whether the venue rejected the request for insufficient funds or margin.
    #[must_use]
    pub fn is_insufficient_funds(&self) -> bool {
        matches!(
            self.code(),
            Some(ASTER_CODE_BALANCE_NOT_SUFFICIENT | ASTER_CODE_MARGIN_NOT_SUFFICIENT)
        )
    }

    /// Returns whether the order was rejected because it would have crossed as a taker.
    ///
    /// Aster reports post-only (`GTX`) violations with the Binance
    /// `ORDER_WOULD_IMMEDIATELY_TRIGGER` code.
    #[must_use]
    pub fn is_post_only_violation(&self) -> bool {
        self.code() == Some(ASTER_CODE_ORDER_WOULD_IMMEDIATELY_TRIGGER)
    }

    /// Returns whether the order notional was below the venue minimum.
    #[must_use]
    pub fn is_min_notional(&self) -> bool {
        self.code() == Some(ASTER_CODE_MIN_NOTIONAL)
    }

    /// Returns whether the failure is a transport fault that a repeat attempt could clear.
    ///
    /// True only when the request never produced an HTTP response: a TLS handshake EOF, a TCP
    /// connect failure, a DNS failure, a connection reset, or a client-side timeout. Those are
    /// the failures this host's proxy produces intermittently, and repeating an *idempotent*
    /// request is then free of venue-side effects.
    ///
    /// False for everything the venue actually answered ([`Self::AsterError`],
    /// [`Self::UnexpectedStatus`]) and for local faults a retry cannot fix
    /// ([`Self::MissingCredentials`], [`Self::SigningError`], [`Self::ValidationError`],
    /// [`Self::JsonError`]). In particular an Aster error body such as `-1121 Invalid symbol`
    /// is a definitive answer and is never retried.
    ///
    /// This says nothing about whether a request is *safe* to retry; only `GET` requests are
    /// repeated (see `AsterHttpClient`), never order submission or cancellation.
    #[must_use]
    pub const fn is_retryable_transport(&self) -> bool {
        matches!(self, Self::NetworkError(_) | Self::Timeout(_))
    }
}

impl From<serde_json::Error> for AsterHttpError {
    fn from(err: serde_json::Error) -> Self {
        Self::JsonError(err.to_string())
    }
}

impl From<HttpClientError> for AsterHttpError {
    fn from(err: HttpClientError) -> Self {
        match err {
            HttpClientError::TimeoutError(msg) => Self::Timeout(msg),
            HttpClientError::InvalidProxy(msg)
            | HttpClientError::ClientBuildError(msg)
            | HttpClientError::Error(msg) => Self::NetworkError(msg),
        }
    }
}

/// Result type for Aster HTTP operations.
pub type AsterHttpResult<T> = Result<T, AsterHttpError>;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn venue_error(code: i64) -> AsterHttpError {
        AsterHttpError::AsterError {
            code,
            message: "test".to_string(),
        }
    }

    #[rstest]
    fn test_display_formats_code_and_message() {
        let error = AsterHttpError::AsterError {
            code: -1121,
            message: "Invalid symbol.".to_string(),
        };

        assert_eq!(error.to_string(), "Aster error -1121: Invalid symbol.");
    }

    #[rstest]
    #[case(ASTER_CODE_TOO_MANY_REQUESTS)]
    #[case(ASTER_CODE_INVALID_NONCE)]
    #[case(ASTER_CODE_NEW_ORDER_REJECTED)]
    #[case(ASTER_CODE_MARGIN_NOT_SUFFICIENT)]
    #[case(ASTER_CODE_MIN_NOTIONAL)]
    #[case(-1121)] // INVALID_SYMBOL
    #[case(-1111)] // BAD_PRECISION
    #[case(-1013)] // INVALID_MESSAGE / filter failure
    #[case(-1102)] // MANDATORY_PARAM_EMPTY_OR_MALFORMED
    #[case(-4003)] // quantity less than zero
    #[case(-4004)] // quantity less than the minimum
    #[case(-4005)] // quantity greater than the maximum
    fn test_venue_rejection_classification(#[case] code: i64) {
        let error = venue_error(code);

        assert!(error.is_venue_rejection(), "code={code}");
        assert!(!error.is_execution_status_unknown(), "code={code}");
        assert!(!error.is_ambiguous_execution(), "code={code}");
        assert_eq!(error.code(), Some(code));
    }

    #[rstest]
    #[case(ASTER_CODE_UNEXPECTED_RESP)]
    #[case(ASTER_CODE_TIMEOUT)]
    fn test_execution_status_unknown_codes_are_not_rejections(#[case] code: i64) {
        // Aster documents -1006/-1007 as "execution status unknown": the order may already be
        // resting or filled, so terminalising it as rejected would desynchronise the engine.
        let error = venue_error(code);

        assert!(error.is_execution_status_unknown(), "code={code}");
        assert!(error.is_ambiguous_execution(), "code={code}");
        assert!(!error.is_venue_rejection(), "code={code}");
        assert!(!error.is_unknown_order(), "code={code}");
    }

    #[rstest]
    fn test_execution_status_unknown_codes_are_still_not_retried() {
        // The venue answered, so the request must not be repeated; only the *classification*
        // changes, resolution goes through an order query.
        for code in ASTER_EXECUTION_STATUS_UNKNOWN_CODES {
            assert!(!venue_error(code).is_retryable_transport(), "code={code}");
        }
    }

    #[rstest]
    fn test_transport_errors_are_not_venue_rejections() {
        let error = AsterHttpError::Timeout("elapsed".to_string());

        assert!(!error.is_venue_rejection());
        assert!(error.is_ambiguous_execution());
        assert_eq!(error.code(), None);
        assert!(!error.is_auth_failure());
    }

    #[rstest]
    fn test_local_failures_are_not_ambiguous() {
        // A request that never left the process cannot have reached the book.
        assert!(!AsterHttpError::MissingCredentials.is_ambiguous_execution());
        assert!(!AsterHttpError::SigningError("bad key".to_string()).is_ambiguous_execution());
        assert!(!AsterHttpError::ValidationError("no id".to_string()).is_ambiguous_execution());
    }

    #[rstest]
    #[case(500, true)]
    #[case(502, true)]
    #[case(503, true)]
    #[case(408, true)]
    #[case(400, false)]
    #[case(401, false)]
    #[case(429, false)]
    fn test_unexpected_status_ambiguity_follows_the_status_class(
        #[case] status: u16,
        #[case] expected: bool,
    ) {
        // A gateway 5xx may still have forwarded the order; a 4xx is the request being refused.
        let error = AsterHttpError::UnexpectedStatus {
            status,
            body: "edge".to_string(),
        };

        assert_eq!(error.is_ambiguous_execution(), expected, "status={status}");
        assert!(!error.is_venue_rejection());
    }

    #[rstest]
    fn test_undecodable_response_body_is_ambiguous() {
        // The venue answered something; not being able to read it is not a rejection.
        let error = AsterHttpError::JsonError("expected value at line 1".to_string());

        assert!(error.is_ambiguous_execution());
        assert!(!error.is_venue_rejection());
    }

    #[rstest]
    fn test_rate_limit_classification() {
        assert!(venue_error(ASTER_CODE_TOO_MANY_REQUESTS).is_rate_limited());
        assert!(!venue_error(ASTER_CODE_NO_SUCH_ORDER).is_rate_limited());
    }

    #[rstest]
    #[case(ASTER_CODE_INVALID_NONCE)]
    #[case(ASTER_CODE_INVALID_SIGNATURE)]
    #[case(ASTER_CODE_INVALID_LISTEN_KEY)]
    #[case(ASTER_CODE_UNFUNDED_WALLET)]
    fn test_auth_failure_classification(#[case] code: i64) {
        assert!(venue_error(code).is_auth_failure(), "code={code}");
    }

    #[rstest]
    #[case(ASTER_CODE_CANCEL_REJECTED)]
    #[case(ASTER_CODE_NO_SUCH_ORDER)]
    fn test_unknown_order_classification(#[case] code: i64) {
        assert!(venue_error(code).is_unknown_order(), "code={code}");
    }

    #[rstest]
    fn test_insufficient_funds_and_min_notional_and_post_only() {
        assert!(venue_error(ASTER_CODE_MARGIN_NOT_SUFFICIENT).is_insufficient_funds());
        assert!(venue_error(ASTER_CODE_BALANCE_NOT_SUFFICIENT).is_insufficient_funds());
        assert!(venue_error(ASTER_CODE_MIN_NOTIONAL).is_min_notional());
        assert!(venue_error(ASTER_CODE_ORDER_WOULD_IMMEDIATELY_TRIGGER).is_post_only_violation());
        assert!(!venue_error(ASTER_CODE_NEW_ORDER_REJECTED).is_post_only_violation());
    }

    #[rstest]
    fn test_transport_errors_are_retryable() {
        assert!(AsterHttpError::Timeout("elapsed".to_string()).is_retryable_transport());
        assert!(
            AsterHttpError::NetworkError("tls handshake eof".to_string()).is_retryable_transport()
        );
        assert!(
            AsterHttpError::NetworkError("tcp connect error: os error 10060".to_string())
                .is_retryable_transport()
        );
    }

    #[rstest]
    #[case(-1121)]
    #[case(ASTER_CODE_NEW_ORDER_REJECTED)]
    #[case(ASTER_CODE_TOO_MANY_REQUESTS)]
    #[case(ASTER_CODE_INVALID_NONCE)]
    fn test_venue_error_bodies_are_not_retryable(#[case] code: i64) {
        assert!(
            !venue_error(code).is_retryable_transport(),
            "code={code} must not be retried; the venue answered"
        );
    }

    #[rstest]
    fn test_local_and_http_status_failures_are_not_retryable() {
        assert!(!AsterHttpError::MissingCredentials.is_retryable_transport());
        assert!(!AsterHttpError::SigningError("bad key".to_string()).is_retryable_transport());
        assert!(
            !AsterHttpError::ValidationError("no order id".to_string()).is_retryable_transport()
        );
        assert!(!AsterHttpError::JsonError("eof".to_string()).is_retryable_transport());
        assert!(
            !AsterHttpError::UnexpectedStatus {
                status: 503,
                body: "maintenance".to_string(),
            }
            .is_retryable_transport()
        );
    }

    #[rstest]
    fn test_http_client_error_conversion_preserves_kind() {
        let timeout: AsterHttpError = HttpClientError::TimeoutError("slow".to_string()).into();
        let network: AsterHttpError = HttpClientError::Error("reset".to_string()).into();

        assert!(matches!(timeout, AsterHttpError::Timeout(_)));
        assert!(matches!(network, AsterHttpError::NetworkError(_)));
    }
}
