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

//! Sanitized public client errors, without raw frames or response payloads.

use thiserror::Error;

/// A public data operation which did not meet its explicit protocol or lifecycle contract.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BackpackDataError {
    /// An unsupported framework command.
    #[error("unsupported Backpack public data operation: {0}")]
    Unsupported(&'static str),
    /// A caller configuration outside the implemented scope.
    #[error("invalid Backpack public data configuration: {0}")]
    Configuration(&'static str),
    /// A failed complete metadata refresh or domain construction.
    #[error("Backpack complete metadata refresh failed")]
    Metadata,
    /// Public transport setup, write or read failed.
    #[error("Backpack public transport failed")]
    Transport,
    /// Client ownership or shutdown was not valid.
    #[error("Backpack public lifecycle failed: {0}")]
    Lifecycle(&'static str),
    /// A bounded replay was malformed or exceeded its bounds.
    #[error("invalid or oversized Backpack public replay")]
    Replay,
}
