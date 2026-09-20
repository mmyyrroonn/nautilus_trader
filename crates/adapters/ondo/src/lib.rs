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

//! [NautilusTrader](https://nautilustrader.io) adapter for [Ondo Perps](https://docs.ondoperps.xyz).
//!
//! Ondo Perps is a venue for tokenized-equity perpetuals quoted in USD and settled in USDC.
//! This crate owns the protocol surface for the venue: instrument identity, exact decimal
//! precision, nanosecond timestamps, and the market metadata that drives both.
//!
//! # Scope
//!
//! The crate is built in phases. This phase provides the protocol core shared by every later
//! layer:
//!
//! - [`common`] - venue identity, environment, the raw market status mapping, fee-rate
//!   provenance, decimal/timestamp/symbol parsing.
//! - [`http`] - the `GET /v1/markets` response schema, the single conversion boundary that turns it
//!   into Nautilus instruments, the REST transport, and the private (authenticated) read surface.
//!   The schema is parsed once and shared with the WebSocket layer, so precision is never derived
//!   twice.
//! - [`websocket`] - the two connections the venue needs: the public feed (message schema,
//!   wire-to-domain parsing, the book state machine, the connection lifecycle) and
//!   [`websocket::private`], the login-required account session with its own socket.
//! - [`data`] - the `DataClient`: market metadata and its refresh, the public subscriptions, and the
//!   publication of feed states and market data into the data engine.
//! - [`execution`] - the `ExecutionClient`: the order write surface with its local refusals, the
//!   order index, the `(account_id, fill.id)` dedup ledger, and the private order and fill reports.
//!   Its account half ([`execution::OndoAccountRuntime`]) is shared with the private transport,
//!   which is what makes one account rather than two that agree.
//! - [`reconciliation`] - the account: the four-state recovery machine and its fail-closed
//!   `can_submit_new_orders`, the judgments a pass makes over what the venue said, the durable
//!   ledger journal, and the account-level dead man's switch.
//! - [`config`] - the data and execution client configurations.
//! - [`factories`] - the client factories the client registry and the Python projection consume.
//! - [`signing`] - the REST and WebSocket request signing: the signed message, the auth header
//!   contract, the WebSocket login digest, and the clock-skew refusal.
//! - [`recording`] - the bounded-queue recording of the **public** raw frames (plan §5.1): the
//!   whitelist, the rotation and the drop accounting. It is built only when the configuration names
//!   a `raw_md_path`, and it can only record what its whitelist accepted.
//! - [`python`] - the Python surface (`python` feature): venue constants, the environment, the two
//!   client configurations and their factories, and the public HTTP client.
//!
//! # The two connections, and why they are two
//!
//! The public data client reads no key and loads no `.env`, and the account's channels require a
//! login. Those are one decision, not two: [`websocket::private`] owns the second socket, the
//! credential, and the login handshake, and [`data`] keeps none of them. The account's state - what
//! a report means, whether a recovery may run, what the dead man's switch permits - stays in
//! [`reconciliation`] and [`execution`], whichever connection delivered the report.
//!
//! The private transport is offline-verified only in this phase. No sandbox key exists, so the
//! login digest's concatenation order, the switch's renewal message and the shape of a real private
//! frame are all unverified against the venue (plan §R5.2).
//!
//! Every price, quantity and fee this adapter reads is a decimal string. Values are kept as
//! [`rust_decimal::Decimal`] or Nautilus domain types, and nothing routes through `f64`.
//!
//! The scope of that sentence is deliberate, because the spec does not let it be universal: the
//! frozen REST spec's `BuilderCodeReq` carries `feeRateBpsFractional` as a JSON `number` and its
//! deprecated sibling `feeRateBps` as an `integer`, and those two are the only non-string members
//! whose names read as money anywhere in its schemas. This adapter neither reads nor sends builder
//! codes - [`execution`]'s write surface never constructs one - so no such number reaches it. The
//! claim is about what this adapter reads, not about what the venue may put on the wire.
//!
//! # Credentials
//!
//! Public market data needs none: the data client reads no key and loads no `.env`. The
//! authenticated surface's credential lives in [`common::credential::OndoCredential`] and nowhere
//! else, is never printable, and may only be used with the sandbox environment that
//! [`common::credential::validate_authenticated_environment`] admits.
//!
//! # Test data
//!
//! Protocol fixtures and their provenance live in `crates/adapters/ondo/test_data`. Read a
//! fixture's kind from `test_data/manifest.json`, never from its file name, and see
//! `test_data/README.md` for the field-level protocol table.
//!
//! # Precision mode
//!
//! [High-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation#precision-mode)
//! (128-bit value types) is enabled by default.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod diagnostics;
pub mod execution;
pub mod factories;
pub mod http;
pub mod production;
pub mod reconciliation;
pub mod recording;
pub mod signing;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;
