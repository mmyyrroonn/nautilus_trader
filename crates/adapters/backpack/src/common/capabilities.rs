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
    /// Production mutation admission remains unsupported. Implementation presence is
    /// independent of live transport health, account verification or execution admission.
    pub const fn require_implemented(self) -> Result<(), BackpackUnsupportedCapabilityError> {
        match self {
            Self::PublicMarketData | Self::ReadOnlyAccount | Self::RestrictedExecution => Ok(()),
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
    fn test_read_only_implementation_does_not_claim_live_readiness() {
        assert_eq!(
            BackpackCapability::ReadOnlyAccount.require_implemented(),
            Ok(())
        );
        let health = crate::execution_client::BackpackAccountTelemetry::default().snapshot();
        assert!(!health.transport_connected);
        assert!(!health.private_subscription_confirmed);
        assert!(health.evidence_gaps.contains("AccountIdentityUnverified"));
    }
    #[rstest]
    fn test_guarded_implementation_does_not_claim_production_readiness() {
        assert_eq!(
            BackpackCapability::RestrictedExecution.require_implemented(),
            Ok(())
        );
        assert!(
            !crate::telemetry::BackpackPublicTelemetry::default()
                .snapshot()
                .execution_ready
        );
        assert!(
            !crate::execution_client::BackpackAccountTelemetry::default()
                .snapshot()
                .private_subscription_confirmed
        );
    }
}
