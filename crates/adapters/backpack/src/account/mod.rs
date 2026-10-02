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

//! Authenticated account observations, bounded traversal evidence and economic delivery contracts.
//!
//! Protocol: <https://docs.backpack.exchange/>. These primitives do not establish
//! execution readiness, authenticated account identity, transactional snapshots,
//! history retention or replication completeness. No mutation endpoint is exposed.

pub mod client;
pub mod models;
pub mod pagination;
pub mod reconciliation;
pub mod reports;

/// Sanitized account protocol failures, excluding private payloads and identifiers.
#[derive(Debug, thiserror::Error)]
pub enum BackpackAccountError {
    #[error("invalid Backpack account field: {0}")]
    InvalidField(&'static str),
    #[error("invalid Backpack account response shape")]
    Decode,
    #[error("invalid Backpack history budget")]
    InvalidBudget,
    #[error("Backpack history traversal exceeded its finite budget")]
    Budget,
    #[error("missing or inconsistent Backpack pagination evidence")]
    Pagination,
    #[error("Backpack history made no unique progress")]
    NoProgress,
    #[error("conflicting Backpack economic observations")]
    Conflict,
    #[error("unsupported Backpack domain representation: {0}")]
    Unsupported(&'static str),
    #[error("unknown Backpack economic acknowledgement")]
    UnknownAcknowledgement,
    #[error("Backpack economic acknowledgement was not durably committed")]
    Acknowledgement,
    #[error(transparent)]
    Http(#[from] crate::http::error::BackpackHttpError),
}

/// A documented uncertainty preventing absence, flatness or readiness conclusions.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackpackEvidenceGap {
    AccountIdentityUnverified,
    OwnershipUnverified,
    NonAtomicSnapshot,
    RetentionOrReplicationUnknown,
    CutoffNotServerEnforced,
    OrderTimeUnknown,
    PositionTimeUnknown,
    FundingCurrencyUnknown,
    FundingTimezoneUnknown,
    MissingTradeId,
    UnknownVenueState,
    UnsupportedAccountPolicy,
    UnsupportedCollateral,
    UnknownFields,
}
