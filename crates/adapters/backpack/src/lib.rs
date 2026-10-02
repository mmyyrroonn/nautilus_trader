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
//! It provides audience-bound credentials, protocol signing, and a restricted GET transport.
//! It parses exact public metadata and bounded public streams with depth synchronization.
//! A native public data owner and factory manage discovery and bounded public feeds.
//! It persists durable local order identity and unsigned intents.
//! It provides typed account reads, conservative coverage evidence and staged fill reconciliation.
//! A native read-only account client provides bounded REST/private-stream lifecycle.
//! Loopback-only guarded mutations are available; restricted engine execution integration
//! and production writes remain unsupported.
//! The optional `python` feature exposes public configuration and the native data factory.
//! The optional `high-precision` feature propagates
//! the Nautilus domain precision mode; default features remain empty.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod account;
pub mod common;
pub mod config;
pub mod data;
pub mod data_error;
pub mod depth;
pub mod execution;
pub mod execution_client;
pub mod factories;
pub mod http;
pub mod identity;
pub mod instruments;
pub mod models;
pub mod parsing;
pub mod provider;
pub mod public;
pub mod replay;
mod runtime;
pub mod signing;
pub mod telemetry;

#[cfg(feature = "python")]
pub mod python;
