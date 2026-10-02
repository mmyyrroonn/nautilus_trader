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

//! Loopback-only guarded order commands; native client integration remains separate.

pub mod command;
pub mod guard;
pub mod owner;

use std::fmt;

use crate::http::error::{BackpackHttpError, BackpackRequestOutcome};

/// Local refusal class, excluding account/order payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackExecutionErrorKind {
    UnsupportedProduction,
    UnsupportedCommand,
    Validation,
    Readiness,
    Capacity,
    Ownership,
    Duplicate,
    Identity,
    Stopped,
    Storage,
}

/// Redacted local refusal or transmission-aware HTTP failure.
#[derive(Debug, thiserror::Error)]
pub enum BackpackExecutionError {
    /// Local failure before any request is dispatched.
    #[error("Backpack execution refused: {0:?}")]
    Local(BackpackExecutionErrorKind),
    /// Shared HTTP failure with original transmission evidence.
    #[error(transparent)]
    Http(#[from] BackpackHttpError),
}
impl BackpackExecutionError {
    /// Returns evidence, not an order lifecycle state.
    #[must_use]
    pub const fn outcome(&self) -> BackpackRequestOutcome {
        match self {
            Self::Local(_) => BackpackRequestOutcome::NotSent,
            Self::Http(error) => error.outcome(),
        }
    }
}

impl From<BackpackExecutionErrorKind> for BackpackExecutionError {
    fn from(value: BackpackExecutionErrorKind) -> Self {
        Self::Local(value)
    }
}

/// Receipt classification; no variant manufactures a fill or a native terminal event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackMutationStatus {
    /// Matched order response observed; economic/lifecycle application remains pending.
    ResponseObserved,
    /// DELETE 202: request accepted without proof of cancellation.
    CancelPending,
}

/// Matched response evidence for the account/report integration boundary.
pub struct BackpackMutationReceipt {
    pub(crate) status: BackpackMutationStatus,
    pub(crate) body: Vec<u8>,
}
impl BackpackMutationReceipt {
    /// Returns the HTTP evidence classification.
    #[must_use]
    pub const fn status(&self) -> BackpackMutationStatus {
        self.status
    }
    /// Returns exact response bytes for later native report parsing and economic acknowledgement.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
impl fmt::Debug for BackpackMutationReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackMutationReceipt")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
