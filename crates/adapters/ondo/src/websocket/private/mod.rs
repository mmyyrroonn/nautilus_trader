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

//! The Ondo Perps **private** WebSocket surface: the account's own connection.
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`messages`] | The private wire schema: the login frame, the private channel enum, the raw envelope. |
//! | [`parse`] | Private wire to domain, through the *same* REST decoders a reconciliation pass uses. |
//! | [`session`] | The protocol state machine - **zero I/O**, offline-testable from the login forward. |
//! | [`diagnostics`] | The private session's own record, which a login frame cannot enter. |
//! | [`stream`] | The transport: the only socket, the only task, and the only caller of the account's hooks. |
//!
//! # Why the private channels get a second connection
//!
//! [`crate::websocket::messages::WsChannel`] is a closed enum of exactly five **public** channels,
//! and the public data client is defined by reading no key and loading no `.env`
//! ([`crate::lib`]): a login handshake cannot be added to it. So the private stream owns its own
//! socket, and it is not merely the convenient answer - the account's credential, its state and its
//! reports stay on the execution side of that line, where [`crate::reconciliation`] already lives.
//!
//! # The division of labour this module copies
//!
//! The public side's split ([`crate::websocket`]) is what makes a transport testable without a
//! venue: wire types that only describe, a parser that is the one place a lexeme becomes a value, a
//! session that performs no I/O, and a transport that owns the socket. The private side mirrors it
//! exactly, because the same thing has to be true here - **no sandbox key exists in this phase**,
//! so everything except the socket itself has to be provable offline (plan §R3.1, §R5.2).
//!
//! # What the transport does *not* decide
//!
//! Which account seam a private report goes to - applied now, or held for a running pass - is
//! [`crate::execution::OndoAccountRuntime::ingest_stream_order`]'s decision, because it is part of
//! the recovery's merge protocol and has exactly one truth source. The transport reads no claim and
//! holds no flag.

pub mod diagnostics;
pub mod messages;
pub mod parse;
pub mod session;
pub mod stream;

pub use diagnostics::{
    ONDO_PRIVATE_DIAGNOSTIC_CAPACITY, PrivateDiagnostics, PrivateDiagnosticsCounters,
    PrivateRecord, SharedPrivateDiagnostics,
};
pub use messages::{
    LoginArgs, LoginRequest, PrivateChannel, PrivateSubscriptionRequest, RawPrivateMessage,
};
pub use parse::{
    PrivateEnvelope, PrivatePayload, decode_private_item, decode_private_updates,
    parse_private_message,
};
pub use session::{
    ONDO_WS_LOGIN_MAX_ATTEMPTS, OndoPrivateSession, PrivateAction, PrivateEvent,
    PrivateFrameOutcome, PrivateSessionCounters, PrivateSessionPhase, PrivateStreamMode,
};
pub use stream::{
    ONDO_ACCOUNT_METADATA_REFRESH_SECS, ONDO_PRIVATE_STREAM_STOP_TIMEOUT_SECS,
    ONDO_WS_LOGIN_TIMEOUT_SECS, OndoPrivateStream, PrivateRunSnapshot, PrivateRunState,
};
