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

    /// Returns whether this is a structured venue rejection rather than a transport failure.
    ///
    /// Structured rejections are definitive: the order never reached the book, so the
    /// execution client can emit `OrderRejected` instead of waiting for reconciliation.
    #[must_use]
    pub const fn is_venue_rejection(&self) -> bool {
        matches!(self, Self::AsterError { .. })
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
    fn test_venue_rejection_classification(#[case] code: i64) {
        assert!(venue_error(code).is_venue_rejection());
        assert_eq!(venue_error(code).code(), Some(code));
    }

    #[rstest]
    fn test_transport_errors_are_not_venue_rejections() {
        let error = AsterHttpError::Timeout("elapsed".to_string());

        assert!(!error.is_venue_rejection());
        assert_eq!(error.code(), None);
        assert!(!error.is_auth_failure());
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
    fn test_http_client_error_conversion_preserves_kind() {
        let timeout: AsterHttpError = HttpClientError::TimeoutError("slow".to_string()).into();
        let network: AsterHttpError = HttpClientError::Error("reset".to_string()).into();

        assert!(matches!(timeout, AsterHttpError::Timeout(_)));
        assert!(matches!(network, AsterHttpError::NetworkError(_)));
    }
}
