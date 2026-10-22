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

//! Native market data and execution integration for Arcus perpetual futures.
//!
//! # Feature Flags
//!
//! - `high-precision`: Uses 128-bit fixed-point values (enabled by default).
//! - `python`: Exposes configuration and LiveNode factories through PyO3.
//! - `extension-module`: Builds the Python extension module.

#![deny(unsafe_code)]

pub mod config;
pub mod consts;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod instruments;
pub mod models;
pub mod signing;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;
