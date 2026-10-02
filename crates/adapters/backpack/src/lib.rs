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

//! Foundations for the Backpack Exchange adapter.
//!
//! This crate validates an explicit USDC perpetual allowlist and endpoint selection.
//! It performs no network I/O and accepts no credentials. Public market data, private
//! account reads, and restricted execution are planned but unavailable in this phase.
//! There are no Python bindings. The optional `high-precision` feature
//! propagates the Nautilus domain precision mode; default features remain empty.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod depth;
pub mod instruments;
pub mod models;
pub mod parsing;
pub mod provider;
pub mod public;
