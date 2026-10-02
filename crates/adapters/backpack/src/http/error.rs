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

//! Redacted request errors preserving transmission evidence.

use std::{fmt, time::Duration};

use nautilus_network::http::HttpClientError;

/// Request transmission evidence; it never establishes an order lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackRequestOutcome {
    NotSent,
    VenueRejected,
    Unknown,
}

/// Failure source, without raw authenticated payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackHttpErrorKind {
    Validation,
    Credentials,
    Admission,
    Transport,
    Venue,
    Decode,
    Cancelled,
    Budget,
}

/// A sanitized request error with status, safe venue code and transmission evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct BackpackHttpError {
    pub(crate) kind: BackpackHttpErrorKind,
    pub(crate) outcome: BackpackRequestOutcome,
    pub(crate) status: Option<u16>,
    pub(crate) code: Option<String>,
    pub(crate) retry_after: Option<Duration>,
}

impl BackpackHttpError {
    pub(crate) const fn local(kind: BackpackHttpErrorKind) -> Self {
        Self {
            kind,
            outcome: BackpackRequestOutcome::NotSent,
            status: None,
            code: None,
            retry_after: None,
        }
    }
    pub(crate) const fn unknown(kind: BackpackHttpErrorKind) -> Self {
        Self {
            kind,
            outcome: BackpackRequestOutcome::Unknown,
            status: None,
            code: None,
            retry_after: None,
        }
    }
    /// Returns the failure source.
    #[must_use]
    pub const fn kind(&self) -> BackpackHttpErrorKind {
        self.kind
    }
    /// Returns evidence across all attempts of the request.
    #[must_use]
    pub const fn outcome(&self) -> BackpackRequestOutcome {
        self.outcome
    }
    /// Returns the response status when available.
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        self.status
    }
    /// Returns a validated error code, excluding free-form messages.
    #[must_use]
    pub fn venue_code(&self) -> Option<&str> {
        self.code.as_deref()
    }
    pub(crate) fn preserve_transmission(mut self, transmitted: bool) -> Self {
        if transmitted && self.outcome == BackpackRequestOutcome::NotSent {
            self.outcome = BackpackRequestOutcome::Unknown;
        }
        self
    }
    pub(crate) fn is_retryable_read(&self) -> bool {
        self.kind == BackpackHttpErrorKind::Transport
            || self
                .status
                .is_some_and(|status| status == 429 || (500..=599).contains(&status))
    }
}

impl From<HttpClientError> for BackpackHttpError {
    fn from(value: HttpClientError) -> Self {
        match value {
            HttpClientError::AdmissionDenied(_) => Self::local(BackpackHttpErrorKind::Admission),
            HttpClientError::ClientBuildError(_) | HttpClientError::InvalidProxy(_) => {
                Self::local(BackpackHttpErrorKind::Validation)
            }
            HttpClientError::Error(_) | HttpClientError::TimeoutError(_) => {
                Self::unknown(BackpackHttpErrorKind::Transport)
            }
        }
    }
}
impl fmt::Debug for BackpackHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackHttpError")
            .field("kind", &self.kind)
            .field("outcome", &self.outcome)
            .field("status", &self.status)
            .field("code", &self.code)
            .finish()
    }
}
impl fmt::Display for BackpackHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Backpack {:?} failure ({:?})", self.kind, self.outcome)?;
        if let Some(status) = self.status {
            write!(f, ": HTTP {status}")?;
        }
        if let Some(code) = &self.code {
            write!(f, ", {code}")?;
        }
        Ok(())
    }
}
impl std::error::Error for BackpackHttpError {}
