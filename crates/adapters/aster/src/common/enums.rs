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

//! Enumerations for the Aster adapter.

use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// EIP-712 chain id for the Aster mainnet signing domain.
pub const ASTER_MAINNET_CHAIN_ID: u64 = 1666;

/// EIP-712 chain id for the Aster testnet signing domain.
pub const ASTER_TESTNET_CHAIN_ID: u64 = 714;

/// Aster environment type.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.aster",
        eq,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.aster")
)]
pub enum AsterEnvironment {
    /// Live exchange environment.
    #[default]
    Mainnet,
    /// Testnet environment.
    Testnet,
}

impl AsterEnvironment {
    /// Returns true if this is the testnet environment.
    #[must_use]
    pub const fn is_testnet(self) -> bool {
        matches!(self, Self::Testnet)
    }

    /// Returns the EIP-712 chain id used when signing Futures V3 requests.
    ///
    /// Aster's signing domain uses a venue-specific chain id rather than an EVM network id.
    #[must_use]
    pub const fn chain_id(self) -> u64 {
        match self {
            Self::Mainnet => ASTER_MAINNET_CHAIN_ID,
            Self::Testnet => ASTER_TESTNET_CHAIN_ID,
        }
    }
}

impl Display for AsterEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mainnet => write!(f, "Mainnet"),
            Self::Testnet => write!(f, "Testnet"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_default_is_mainnet() {
        assert_eq!(AsterEnvironment::default(), AsterEnvironment::Mainnet);
        assert!(!AsterEnvironment::default().is_testnet());
        assert!(AsterEnvironment::Testnet.is_testnet());
    }

    #[rstest]
    fn test_chain_id() {
        assert_eq!(AsterEnvironment::Mainnet.chain_id(), 1666);
        assert_eq!(AsterEnvironment::Testnet.chain_id(), 714);
    }

    #[rstest]
    fn test_display() {
        assert_eq!(AsterEnvironment::Mainnet.to_string(), "Mainnet");
        assert_eq!(AsterEnvironment::Testnet.to_string(), "Testnet");
    }
}
