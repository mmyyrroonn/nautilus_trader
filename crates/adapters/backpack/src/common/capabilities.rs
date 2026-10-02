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

//! Static implementation boundaries; runtime health remains a separate observation.

use thiserror::Error;

/// A Backpack adapter capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackCapability {
    /// Public instrument discovery and market data.
    PublicMarketData,
    /// Authenticated account and reconciliation reads.
    ReadOnlyAccount,
    /// A deliberately bounded order execution surface.
    RestrictedExecution,
}

impl BackpackCapability {
    /// Requires an implemented runtime capability.
    ///
    /// # Errors
    ///
    /// Returns an unsupported error for account and execution runtimes.
    pub const fn require_implemented(self) -> Result<(), BackpackUnsupportedCapabilityError> {
        match self {
            Self::PublicMarketData => Ok(()),
            _ => Err(BackpackUnsupportedCapabilityError { capability: self }),
        }
    }
}

/// A request for a capability this adapter has not implemented.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Backpack capability {capability:?} is not implemented")]
pub struct BackpackUnsupportedCapabilityError {
    /// The capability that was refused.
    pub capability: BackpackCapability,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(BackpackCapability::ReadOnlyAccount)]
    #[case(BackpackCapability::RestrictedExecution)]
    fn test_unimplemented_capabilities_are_explicitly_refused(
        #[case] capability: BackpackCapability,
    ) {
        assert_eq!(
            capability.require_implemented(),
            Err(BackpackUnsupportedCapabilityError { capability }),
        );
    }
}
