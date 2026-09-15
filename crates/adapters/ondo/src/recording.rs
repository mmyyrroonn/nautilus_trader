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

//! Recording of the Ondo Perps **public** raw frames (plan §5.1: 有界队列的公开原始帧录制、轮转、丢帧
//! 与结束统计).
//!
//! One run of the adapter that is configured with `raw_md_path` writes one JSONL stream of the frames
//! it received and sent, exactly as they were on the wire, plus the lifecycle markers and the REST
//! metadata snapshots that make the stream usable as provenance. Nothing else is written: the
//! recorder cannot persist a login message, a private payload or an HTTP header.
//!
//! # The whitelist is structural
//!
//! [`RawMdSink`] only accepts what the whitelist produced, and the whitelist has no path around it:
//!
//! - the writer's input is [`PublicFrame`], whose payload is private and whose only constructors are
//!   [`PublicFrame::inbound`] and [`PublicFrame::outbound`]. There is no public constructor, so a
//!   caller cannot build one from a login request, a login response or a private payload - that is a
//!   compile error, not a convention;
//! - [`PublicFrame::inbound`] classifies the frame and answers [`NotPublicFrame`] for everything that
//!   is not provably public: a `loggedIn` acknowledgement, an unknown message type, a frame that does
//!   not decode at all, and an `update` on anything other than the five public channels;
//! - [`PublicFrame::outbound`] classifies the request body and answers [`NotPublicFrame`] for
//!   everything that is not one of the adapter's own public operations (`subscribe`, `unsubscribe`,
//!   `ping`), so `{"op":"login"}` is refused at the boundary even though the transport's `send` takes
//!   an arbitrary string;
//! - a REST metadata snapshot is filtered by [`MetadataSnapshot::new`], which keeps only the headers
//!   in [`RAW_MD_HEADER_WHITELIST`] and computes the body hash itself, so a caller cannot add an
//!   unwhitelisted header to a record.
//!
//! A rejected frame is never written and its payload is never logged: the rejection names the kind it
//! saw, never the bytes it saw, so no credential can reach a file or a log from here.
//!
//! # The queue, the rotation and the drops
//!
//! - **Bounded queue.** 4096 records by default ([`RAW_MD_QUEUE_CAPACITY`]). A full queue never grows
//!   and never blocks the receive path: the frame is counted as dropped and the receive-number window
//!   it belongs to is remembered.
//! - **Periodic flush.** The writer thread wakes at least once per second
//!   ([`RAW_MD_FLUSH_INTERVAL`]), and at every wake it drains what is queued and flushes the file.
//! - **Rotation.** 128 MiB per segment ([`RAW_MD_ROTATE_BYTES`]), the same cap the application's tape
//!   uses. Segments are `<raw_md_path>/raw_md.jsonl`, `raw_md_part0002.jsonl`, `raw_md_part0003.jsonl`,
//!   …, so plain name sort is rotation order.
//! - **A gap is a record.** The flush that follows a drop writes a `gap` record naming the inclusive
//!   receive-number range that was dropped and how many frames it covers, and the run's `run_end` then
//!   reports `clean: false`. A run that dropped frames is never reported as a clean recording, and
//!   nothing may be replayed across the gap.
//! - **A disk error aborts.** The first write, flush or segment-open failure marks the recording
//!   failed, stops it, logs one error, and answers [`RecordOutcome::RecordingFailed`] from then on. No
//!   `run_end` is written, so a reader's "no `run_end` means incomplete" rule identifies the recording
//!   as incomplete as well. The failure is surfaced to the owning data client, which reads it from
//!   [`RawMdRecorder::finish`] and reports it.
//! - **Disabled is nothing at all.** With `raw_md_path == None` no recorder is constructed: no
//!   directory is created, no file is opened and no thread is started (the data client only builds a
//!   recorder when the configuration names a path).
//!
//! # The run id
//!
//! A recording is attributable to the run it belongs to by `run_id`. The caller configures it
//! explicitly ([`RawMdRecorderConfig::new`], reached from
//! [`OndoDataClientConfig::raw_md_run_id`](crate::config::OndoDataClientConfig::raw_md_run_id)): the
//! application passes its process stamp, which is the same string every tape record of the run
//! carries, so the raw frames and the tape join *by construction* whatever the run directory is
//! called. Without a configured id the recorder falls back to [`derive_run_id`], which takes the raw
//! directory's **parent name** and therefore agrees with the tape only when the run directory is
//! itself named after the run; the run header records `run_id_source` (`configured` or
//! `derived_from_path`) and the data client logs a warning for the fallback, so which rule produced
//! the id is never left to be guessed.
//!
//! # Records
//!
//! Every line is one JSON object with `schema_version: 1`, the run id, the session id, the endpoint and
//! the environment. A `frame` record adds the direction, the frame class, `recv_seq` (the receive
//! order, one per whitelisted frame, never reordered by rotation), the local receive times and the raw
//! payload verbatim. The `run_start`, `gap`, `run_end` and `rest_metadata` records carry no
//! `recv_seq`, so a reader's "strictly increasing" check applies to the frame records alone.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use nautilus_core::{UUID4, UnixNanos};
use serde::Serialize;

use crate::{
    common::enums::OndoEnvironment,
    websocket::{
        messages::{WsChannel, WsMessageType, WsOp},
        parse::parse_server_message,
    },
};

/// The schema version every raw record carries.
pub const RAW_SCHEMA_VERSION: u32 = 1;

/// The bounded queue's capacity in records (plan §5.1: 默认 4096 条).
pub const RAW_MD_QUEUE_CAPACITY: usize = 4096;

/// How long the writer thread may hold a queued record before it flushes (plan §5.1: 每秒 flush).
pub const RAW_MD_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// The size at which a segment is rotated (plan §5.1: 128 MiB 文件轮转, the same cap as the tape).
pub const RAW_MD_ROTATE_BYTES: u64 = 128 * 1024 * 1024;

/// The stem of the segment files: `raw_md.jsonl`, then `raw_md_part0002.jsonl`, ….
pub const RAW_MD_SEGMENT_STEM: &str = "raw_md";

/// The response headers a REST metadata snapshot may carry.
///
/// This is also the set the public REST transport retains from a response, so the whitelist is the
/// whole of what can be recorded: `retry-after` is what the retry policy reads, and the other three
/// are the provenance facts a metadata snapshot is worth keeping for. No other header is asked for,
/// kept or written - a session cookie or an authentication header has no path into a record.
pub const RAW_MD_HEADER_WHITELIST: [&str; 4] =
    ["content-type", "content-length", "date", "retry-after"];

/// How many queued records the writer takes per lock acquisition.
const RAW_MD_DRAIN_CHUNK: usize = 512;

/// The direction a recorded frame travelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FrameDirection {
    /// A frame the venue sent.
    Inbound,
    /// A request this adapter sent.
    Outbound,
}

impl FrameDirection {
    /// Returns the exact string a record carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

/// The whitelisted class of a recorded frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicFrameClass {
    /// A market data update on one of the five public channels.
    MarketUpdate,
    /// A subscription or unsubscription acknowledgement.
    SubscriptionAck,
    /// A heartbeat: a `pong`, or the `ping` that asks for one.
    Heartbeat,
    /// A venue error frame.
    VenueError,
    /// A public `subscribe` or `unsubscribe` request.
    SubscriptionRequest,
}

impl PublicFrameClass {
    /// Returns the exact string a record carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketUpdate => "market_update",
            Self::SubscriptionAck => "subscription_ack",
            Self::Heartbeat => "heartbeat",
            Self::VenueError => "venue_error",
            Self::SubscriptionRequest => "subscription_request",
        }
    }
}

/// Why a frame is not part of the public stream.
///
/// The reason names the kind that was seen - a message type, a channel name or an operation name -
/// and never the frame's payload, so a rejection can be logged without a secret in the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotPublicFrame {
    reason: String,
}

impl NotPublicFrame {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Returns why the frame is not recorded.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl std::fmt::Display for NotPublicFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for NotPublicFrame {}

/// One frame the whitelist has accepted for recording.
///
/// The payload is private and there is no public constructor: the only ways to obtain a value are
/// [`Self::inbound`] and [`Self::outbound`], which classify the frame and reject anything that is not
/// provably public. The writer takes this type and nothing else, so a login request, a login response,
/// a private payload or an HTTP header cannot be recorded even by a caller that tries.
///
/// ```compile_fail
/// use nautilus_ondo::recording::PublicFrame;
///
/// // The payload is private: a frame cannot be built around bytes the whitelist never classified.
/// let forged = PublicFrame {
///     payload: "{\"type\":\"loggedIn\"}".to_string(),
///     direction: nautilus_ondo::recording::FrameDirection::Inbound,
///     class: nautilus_ondo::recording::PublicFrameClass::SubscriptionAck,
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicFrame {
    payload: String,
    direction: FrameDirection,
    class: PublicFrameClass,
}

/// The public request operations the whitelist accepts on the outbound path.
const RAW_MD_PUBLIC_OPS: [WsOp; 3] = [WsOp::Subscribe, WsOp::Unsubscribe, WsOp::Ping];

impl PublicFrame {
    /// Classifies one inbound frame.
    ///
    /// # Errors
    ///
    /// Returns [`NotPublicFrame`] when the frame is not provably part of the public stream: it does
    /// not decode as a server message, its message type is not one of the documented public types
    /// (a `loggedIn` acknowledgement is the case that matters), or it is an `update` on a channel
    /// other than the five public channels.
    pub fn inbound(raw: &str) -> Result<Self, NotPublicFrame> {
        let Ok(message) = parse_server_message(raw) else {
            return Err(NotPublicFrame::new(
                "a frame that does not decode as a JSON server message cannot be proven public",
            ));
        };

        let class = match message.kind {
            WsMessageType::Update => {
                let channel = message.channel.as_deref().unwrap_or_default();
                if WsChannel::from_wire(channel).is_none() {
                    return Err(NotPublicFrame::new(format!(
                        "an update on channel `{channel}` is not one of the five public channels"
                    )));
                }

                PublicFrameClass::MarketUpdate
            }
            WsMessageType::Subscribed | WsMessageType::Unsubscribed => {
                PublicFrameClass::SubscriptionAck
            }
            WsMessageType::Pong => PublicFrameClass::Heartbeat,
            WsMessageType::Error => PublicFrameClass::VenueError,
            WsMessageType::LoggedIn => {
                return Err(NotPublicFrame::new(
                    "a login acknowledgement is never part of the public stream",
                ));
            }
            WsMessageType::Unknown => {
                return Err(NotPublicFrame::new(
                    "only the documented public message types are recorded",
                ));
            }
        };

        Ok(Self {
            payload: raw.to_string(),
            direction: FrameDirection::Inbound,
            class,
        })
    }

    /// Classifies one outbound request body, before it is sent.
    ///
    /// # Errors
    ///
    /// Returns [`NotPublicFrame`] when the body is not a JSON object carrying one of the adapter's own
    /// public operations. A login request, an order command and anything else the transport's
    /// arbitrary-string `send` could carry are refused here.
    pub fn outbound(body: &str) -> Result<Self, NotPublicFrame> {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return Err(NotPublicFrame::new(
                "a request body that is not a JSON object is not classified as public",
            ));
        };
        let op = value.get("op").and_then(serde_json::Value::as_str);

        match op.and_then(public_op) {
            Some(WsOp::Ping) => Ok(Self {
                payload: body.to_string(),
                direction: FrameDirection::Outbound,
                class: PublicFrameClass::Heartbeat,
            }),
            Some(WsOp::Subscribe | WsOp::Unsubscribe) => Ok(Self {
                payload: body.to_string(),
                direction: FrameDirection::Outbound,
                class: PublicFrameClass::SubscriptionRequest,
            }),
            None => Err(NotPublicFrame::new(format!(
                "request operation `{}` is not one of this adapter's public operations",
                op.unwrap_or("<none>")
            ))),
        }
    }

    /// Returns the frame exactly as it was on the wire.
    #[must_use]
    pub fn payload(&self) -> &str {
        &self.payload
    }

    /// Returns the direction the frame travelled.
    #[must_use]
    pub const fn direction(&self) -> FrameDirection {
        self.direction
    }

    /// Returns the whitelisted class of the frame.
    #[must_use]
    pub const fn class(&self) -> PublicFrameClass {
        self.class
    }
}

/// Returns the operation `op` names, when it is one of the adapter's public operations.
fn public_op(op: &str) -> Option<WsOp> {
    RAW_MD_PUBLIC_OPS
        .into_iter()
        .find(|candidate| candidate.as_str() == op)
}

/// A REST metadata snapshot: the public metadata read that is worth keeping as provenance.
///
/// [`Self::new`] applies the whitelist: only headers named in [`RAW_MD_HEADER_WHITELIST`] are kept,
/// names are compared case-insensitively, and the body hash is computed here rather than accepted from
/// the caller, so no record can carry an unwhitelisted header or a hash of something else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataSnapshot {
    target: String,
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
    body_hash: String,
    request_started_at_ns: u64,
    request_ended_at_ns: u64,
}

impl MetadataSnapshot {
    /// Builds a snapshot of one public REST read.
    ///
    /// `headers` may be the whole header map a transport produced; only the whitelisted names survive.
    /// `request_started_at_ns` and `request_ended_at_ns` are the local wall-clock times around the
    /// request, and are what makes the snapshot attributable to a moment in the run.
    #[must_use]
    pub fn new(
        target: impl Into<String>,
        status: u16,
        headers: impl IntoIterator<Item = (String, String)>,
        body: impl Into<String>,
        request_started_at_ns: u64,
        request_ended_at_ns: u64,
    ) -> Self {
        let body = body.into();
        let whitelisted = headers
            .into_iter()
            .filter_map(|(name, value)| {
                let name = name.to_ascii_lowercase();
                is_whitelisted_header(&name).then_some((name, value))
            })
            .collect();

        Self {
            body_hash: blake3::hash(body.as_bytes()).to_hex().to_string(),
            target: target.into(),
            status,
            headers: whitelisted,
            body,
            request_started_at_ns,
            request_ended_at_ns,
        }
    }

    /// Returns the request target that was read.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the HTTP status the venue answered with.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the whitelisted response headers, by lower-case name.
    #[must_use]
    pub const fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    /// Returns the response body exactly as it was received.
    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Returns the BLAKE3 hash of the body, as lower-case hex.
    #[must_use]
    pub fn body_hash(&self) -> &str {
        &self.body_hash
    }

    /// Returns the local time the request started.
    #[must_use]
    pub const fn request_started_at_ns(&self) -> u64 {
        self.request_started_at_ns
    }

    /// Returns the local time the request ended.
    #[must_use]
    pub const fn request_ended_at_ns(&self) -> u64 {
        self.request_ended_at_ns
    }
}

/// Returns `true` when a lower-case header name is one a snapshot may carry.
#[must_use]
pub fn is_whitelisted_header(name: &str) -> bool {
    RAW_MD_HEADER_WHITELIST.contains(&name)
}

/// What offering something to the recorder did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum RecordOutcome {
    /// The record is queued for writing.
    Recorded,
    /// The queue was full: the frame is counted as dropped and a `gap` record will name its
    /// receive-number window. The run will not report a clean recording.
    Dropped,
    /// The whitelist refused the frame: it is not part of the public stream and nothing was queued.
    NotPublic,
    /// The recording has failed or has already finished: nothing was written.
    RecordingFailed,
}

/// What a recording run wrote, and how it ended.
///
/// This is the end-of-run statistic of plan §5.1: what was written, how much of it, how many frames
/// were dropped, how many times the file rotated, and whether the run ended cleanly.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordingStats {
    /// The schema version of the records these statistics describe.
    pub schema_version: u32,
    /// Whitelisted records offered to the recorder (frames and metadata snapshots).
    pub offered: u64,
    /// Data records written (frames and metadata snapshots, never a marker).
    pub records: u64,
    /// Lifecycle markers written (`run_start`, `gap`, `run_end`).
    pub markers: u64,
    /// Bytes flushed to the segment files, including the markers.
    pub bytes: u64,
    /// Frames dropped because the queue was full.
    pub dropped: u64,
    /// `gap` records written for those drops.
    pub gaps: u64,
    /// Segment rotations performed by this session.
    pub rotations: u64,
    /// Segment files this session opened.
    pub segments: u32,
    /// The last receive number assigned, `0` when no frame was offered.
    pub last_recv_seq: u64,
    /// Whether the run wrote its `run_end` record.
    pub finished: bool,
    /// Why the recording aborted, when it did.
    pub failed: Option<String>,
    /// Whether the run ended cleanly: it wrote its `run_end`, it did not fail and it dropped nothing.
    pub clean: bool,
}

/// How a recording's `run_id` was established.
///
/// The distinction is recorded in the run header so a reader can tell an id the application
/// configured (which is the same string its tape records carry) from one the recorder inferred from
/// the directory layout, which agrees with the tape only when that layout is the run directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunIdSource {
    /// Configured explicitly by the caller ([`RawMdRecorderConfig::new`]): the raw frames and the
    /// tape join by construction.
    Configured,
    /// Derived from the raw directory's parent name ([`RawMdRecorderConfig::derived_from_path`],
    /// [`derive_run_id`]): the documented fallback for a run directory that *is* named after the run.
    DerivedFromPath,
}

impl RunIdSource {
    /// Returns the exact string a record carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::DerivedFromPath => "derived_from_path",
        }
    }
}

/// The configuration of one recording session.
///
/// [`Self::new`] is the production configuration: the plan's queue bound, flush interval and rotation
/// cap, the run id the caller states, and a session id derived from that run id the way the
/// application's tape derives its own, so a raw frame and a tape record of one run are attributable
/// to the same session. [`Self::derived_from_path`] is the fallback that infers the run id from the
/// raw directory's parent name (see [`RunIdSource`]).
#[derive(Clone, Debug)]
pub struct RawMdRecorderConfig {
    /// The directory the segments are written into. Created if it does not exist.
    pub path: PathBuf,
    /// The run this recording belongs to.
    pub run_id: String,
    /// How `run_id` was established, recorded in the run header.
    pub run_id_source: RunIdSource,
    /// This process's session id within the run.
    pub session_id: String,
    /// The endpoint the frames came from and went to.
    pub endpoint: String,
    /// The environment that endpoint belongs to.
    pub environment: OndoEnvironment,
    /// The bounded queue's capacity in records.
    pub queue_capacity: usize,
    /// How long the writer may hold a queued record before it flushes.
    pub flush_interval: Duration,
    /// The size at which a segment is rotated.
    pub rotate_bytes: u64,
}

impl RawMdRecorderConfig {
    /// Creates the production configuration for `path` with the caller's own `run_id`.
    ///
    /// This is the path the application takes: its `run_id` is the process stamp every tape record
    /// carries, so the two halves of a run join by construction whatever the run directory is
    /// called.
    #[must_use]
    pub fn new(
        path: impl Into<PathBuf>,
        run_id: impl Into<String>,
        endpoint: impl Into<String>,
        environment: OndoEnvironment,
    ) -> Self {
        let run_id = run_id.into();
        let session_id = session_id_for(&run_id);

        Self {
            path: path.into(),
            run_id,
            run_id_source: RunIdSource::Configured,
            session_id,
            endpoint: endpoint.into(),
            environment,
            queue_capacity: RAW_MD_QUEUE_CAPACITY,
            flush_interval: RAW_MD_FLUSH_INTERVAL,
            rotate_bytes: RAW_MD_ROTATE_BYTES,
        }
    }

    /// Creates the configuration for `path` with the run id derived from the directory layout.
    ///
    /// The documented fallback for a caller that has no run id to hand over: the id is
    /// [`derive_run_id`]'s answer (the raw directory's parent name) and the header says
    /// [`RunIdSource::DerivedFromPath`], because that rule only agrees with the tape when the run
    /// directory is itself named after the run.
    #[must_use]
    pub fn derived_from_path(
        path: impl Into<PathBuf>,
        endpoint: impl Into<String>,
        environment: OndoEnvironment,
    ) -> Self {
        let path = path.into();
        let run_id = derive_run_id(&path);
        let mut config = Self::new(path, run_id, endpoint, environment);
        config.run_id_source = RunIdSource::DerivedFromPath;

        config
    }
}

/// Returns the run id a raw directory belongs to.
///
/// This is the **fallback** the recorder uses when no run id was configured: the application names
/// the directory the recorder owns after the run it belongs to, so the id is the run directory's own
/// name (with the default `--out reports/stage1` that is `stage1`, and for a per-run directory such
/// as `reports/ondo-acceptance/20260914T000000Z/raw_ondo` it is the run's stamp). A run whose
/// directory is *not* named after it must state its run id explicitly instead
/// ([`RawMdRecorderConfig::new`]); [`RawMdRecorderConfig::derived_from_path`] is the only caller that
/// takes this rule, and it records [`RunIdSource::DerivedFromPath`] when it does.
#[must_use]
pub fn derive_run_id(path: &Path) -> String {
    let parent = path
        .parent()
        .and_then(Path::file_name)
        .or_else(|| path.file_name());

    parent
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| RAW_MD_SEGMENT_STEM.to_string())
}

/// Returns the session id of one recording session of `run_id`.
fn session_id_for(run_id: &str) -> String {
    let uuid = UUID4::new().to_string().replace('-', "");

    format!("{run_id}-{}", &uuid[..12])
}

/// One entry in the bounded queue.
#[derive(Debug)]
enum Entry {
    Frame {
        direction: FrameDirection,
        class: PublicFrameClass,
        payload: String,
        received_at_ns: u64,
        received_mono_ns: u64,
        recv_seq: u64,
    },
    Metadata(MetadataSnapshot),
}

/// The receive-number window of a run of drops that no `gap` record has named yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GapWindow {
    from_seq: u64,
    to_seq: u64,
    dropped: u64,
}

impl GapWindow {
    fn new(seq: u64) -> Self {
        Self {
            from_seq: seq,
            to_seq: seq,
            dropped: 1,
        }
    }

    fn extend(&mut self, seq: u64) {
        self.to_seq = seq;
        self.dropped += 1;
    }
}

/// The state the receive path and the writer thread share.
#[derive(Debug)]
struct Shared {
    ledger: Mutex<Ledger>,
    ready: Condvar,
    epoch: Instant,
}

#[derive(Debug)]
struct Ledger {
    queue: VecDeque<Entry>,
    capacity: usize,
    next_seq: u64,
    offered: u64,
    dropped: u64,
    pending_gap: Option<GapWindow>,
    records: u64,
    markers: u64,
    bytes: u64,
    gaps: u64,
    rotations: u64,
    segments: u32,
    stop: bool,
    finished: bool,
    failed: Option<String>,
}

impl Ledger {
    fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity,
            next_seq: 1,
            offered: 0,
            dropped: 0,
            pending_gap: None,
            records: 0,
            markers: 0,
            bytes: 0,
            gaps: 0,
            rotations: 0,
            segments: 0,
            stop: false,
            finished: false,
            failed: None,
        }
    }

    fn is_dead(&self) -> bool {
        self.failed.is_some() || self.stop
    }
}

/// The handle the receive path uses: one cheap clone per connection, and nothing to drain on it.
///
/// Every method answers what happened to the frame, and none of them blocks on disk: a frame is
/// either queued, counted as dropped, refused by the whitelist, or refused because the recording has
/// already failed or finished.
#[derive(Clone, Debug)]
pub struct RawMdSink {
    shared: Arc<Shared>,
}

impl RawMdSink {
    /// Offers one inbound frame, classifying it first.
    #[must_use]
    pub fn record_inbound(&self, raw: &str, received_at_ns: UnixNanos) -> RecordOutcome {
        match PublicFrame::inbound(raw) {
            Ok(frame) => self.offer_frame(&frame, received_at_ns.as_u64(), self.monotonic_ns()),
            Err(rejected) => {
                // The reason names the kind, never the payload: no frame content can reach a log.
                log::debug!("Ondo raw recording refused an inbound frame: {rejected}");

                RecordOutcome::NotPublic
            }
        }
    }

    /// Offers one outbound request body, classifying it first.
    #[must_use]
    pub fn record_outbound(&self, body: &str, sent_at_ns: UnixNanos) -> RecordOutcome {
        match PublicFrame::outbound(body) {
            Ok(frame) => self.offer_frame(&frame, sent_at_ns.as_u64(), self.monotonic_ns()),
            Err(rejected) => {
                log::debug!("Ondo raw recording refused an outbound request: {rejected}");

                RecordOutcome::NotPublic
            }
        }
    }

    /// Offers one REST metadata snapshot.
    ///
    /// A snapshot is never dropped against the bounded queue: a metadata read happens at most once a
    /// minute, and its provenance is what makes a file usable after the fact. The bound is the frame
    /// stream's, and a snapshot adds one entry to it.
    #[must_use]
    pub fn record_metadata_snapshot(&self, snapshot: &MetadataSnapshot) -> RecordOutcome {
        let mut ledger = lock(&self.shared.ledger);
        if ledger.is_dead() {
            return RecordOutcome::RecordingFailed;
        }

        ledger.offered += 1;
        ledger.queue.push_back(Entry::Metadata(snapshot.clone()));

        RecordOutcome::Recorded
    }

    /// Returns the recording's statistics as they are now.
    #[must_use]
    pub fn stats(&self) -> RecordingStats {
        let ledger = lock(&self.shared.ledger);

        RecordingStats {
            schema_version: RAW_SCHEMA_VERSION,
            offered: ledger.offered,
            records: ledger.records,
            markers: ledger.markers,
            bytes: ledger.bytes,
            dropped: ledger.dropped,
            gaps: ledger.gaps,
            rotations: ledger.rotations,
            segments: ledger.segments,
            last_recv_seq: ledger.next_seq.saturating_sub(1),
            finished: ledger.finished,
            failed: ledger.failed.clone(),
            clean: ledger.finished && ledger.failed.is_none() && ledger.dropped == 0,
        }
    }

    fn offer_frame(
        &self,
        frame: &PublicFrame,
        received_at_ns: u64,
        received_mono_ns: u64,
    ) -> RecordOutcome {
        let mut ledger = lock(&self.shared.ledger);
        if ledger.is_dead() {
            return RecordOutcome::RecordingFailed;
        }

        // The receive number is assigned in receive order, before the queue is consulted: a dropped
        // frame still consumed the number it was received with, which is what makes a gap's window
        // name the whole hole rather than a shifted one.
        let recv_seq = ledger.next_seq;
        ledger.next_seq += 1;
        ledger.offered += 1;

        if ledger.queue.len() >= ledger.capacity {
            ledger.dropped += 1;
            match &mut ledger.pending_gap {
                Some(window) => window.extend(recv_seq),
                None => ledger.pending_gap = Some(GapWindow::new(recv_seq)),
            }

            return RecordOutcome::Dropped;
        }

        ledger.queue.push_back(Entry::Frame {
            direction: frame.direction,
            class: frame.class,
            payload: frame.payload().to_string(),
            received_at_ns,
            received_mono_ns,
            recv_seq,
        });

        RecordOutcome::Recorded
    }

    fn monotonic_ns(&self) -> u64 {
        let elapsed = self.shared.epoch.elapsed().as_nanos();

        u64::try_from(elapsed).unwrap_or(u64::MAX)
    }
}

/// A recorder: the writer thread, the queue it drains, and the statistics of the run.
///
/// Opening one creates the directory if it does not exist ([`Self::start`]); not opening one is the
/// whole of "recording is disabled" - the data client only ever builds a recorder when
/// `raw_md_path` names a directory.
#[derive(Debug)]
pub struct RawMdRecorder {
    sink: RawMdSink,
    shared: Arc<Shared>,
    handle: Option<JoinHandle<()>>,
    finished: Option<RecordingStats>,
}

impl RawMdRecorder {
    /// Starts a recording session in `config.path`, creating the directory when it is missing.
    ///
    /// The `run_start` record is written before the writer thread starts, so a file never carries
    /// frames without the header that explains them.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created (a path that is an existing file is the
    /// case a caller is most likely to hit), when the first segment cannot be opened, when the
    /// `run_start` record cannot be written, or when the writer thread cannot be spawned. A recording
    /// the operator asked for and this adapter could not start is a start-up failure, not a silent
    /// absence.
    pub fn start(config: RawMdRecorderConfig) -> anyhow::Result<Self> {
        ensure!(
            config.queue_capacity > 0,
            "the raw recording needs a bounded queue of at least one record"
        );

        std::fs::create_dir_all(&config.path).with_context(|| {
            format!(
                "the raw recording directory `{}` could not be created",
                config.path.display()
            )
        })?;
        ensure!(
            config.path.is_dir(),
            "the raw recording path `{}` is not a directory",
            config.path.display()
        );

        let index = next_segment_index(&config.path)?;
        let shared = Arc::new(Shared {
            ledger: Mutex::new(Ledger::new(config.queue_capacity)),
            ready: Condvar::new(),
            epoch: Instant::now(),
        });
        let mut writer = RawMdWriter::new(&config, index);
        writer
            .begin(&shared)
            .map_err(|reason| anyhow::anyhow!("the Ondo raw recording did not start: {reason}"))?;

        let handle = {
            let shared = Arc::clone(&shared);

            std::thread::Builder::new()
                .name("ondo-raw-md".to_string())
                .spawn(move || writer.run(&shared))
                .context("the Ondo raw recording thread could not be started")?
        };
        log::debug!(
            "Ondo raw recording started in `{}` as session {}",
            config.path.display(),
            config.session_id
        );

        Ok(Self {
            sink: RawMdSink {
                shared: Arc::clone(&shared),
            },
            shared,
            handle: Some(handle),
            finished: None,
        })
    }

    /// Returns the handle the receive path records through.
    #[must_use]
    pub fn sink(&self) -> RawMdSink {
        self.sink.clone()
    }

    /// Returns the statistics of the run so far.
    #[must_use]
    pub fn stats(&self) -> RecordingStats {
        self.finished.clone().unwrap_or_else(|| self.sink.stats())
    }

    /// Ends the run: drains the queue, writes the `run_end` record and stops the writer.
    ///
    /// Returns the final statistics. A failure the writer recorded is in [`RecordingStats::failed`],
    /// and a run that dropped frames ends with [`RecordingStats::clean`] `false`; either way the
    /// caller owns the failure and is expected to report it.
    pub fn finish(&mut self) -> RecordingStats {
        if let Some(handle) = self.handle.take() {
            {
                let mut ledger = lock(&self.shared.ledger);
                ledger.stop = true;
            }
            self.shared.ready.notify_all();

            if handle.join().is_err() {
                log::error!("The Ondo raw recording thread panicked");
            }
        }

        let stats = self.sink.stats();
        self.finished = Some(stats.clone());

        stats
    }
}

impl Drop for RawMdRecorder {
    fn drop(&mut self) {
        self.finish();
    }
}

/// The writer: one segment file at a time, rotation at the cap, and the end-of-run record.
#[derive(Debug)]
struct RawMdWriter {
    identity: Identity,
    directory: PathBuf,
    configured_path: String,
    flush_interval: Duration,
    rotate_bytes: u64,
    segment_index: u32,
    segment_bytes: u64,
    pending_bytes: u64,
    file: Option<BufWriter<File>>,
}

#[derive(Clone, Debug)]
struct Identity {
    run_id: String,
    run_id_source: RunIdSource,
    session_id: String,
    endpoint: String,
    environment: OndoEnvironment,
}

/// Whether a written line is a data record or a lifecycle marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Counted {
    Record,
    Marker,
}

impl RawMdWriter {
    fn new(config: &RawMdRecorderConfig, segment_index: u32) -> Self {
        Self {
            identity: Identity {
                run_id: config.run_id.clone(),
                run_id_source: config.run_id_source,
                session_id: config.session_id.clone(),
                endpoint: config.endpoint.clone(),
                environment: config.environment,
            },
            directory: config.path.clone(),
            configured_path: config.path.display().to_string(),
            flush_interval: config.flush_interval,
            rotate_bytes: config.rotate_bytes.max(1),
            segment_index,
            segment_bytes: 0,
            pending_bytes: 0,
            file: None,
        }
    }

    /// Opens the first segment and writes the run header.
    fn begin(&mut self, shared: &Shared) -> Result<(), String> {
        self.open_segment(shared, self.segment_index)?;

        let record = StartRecord {
            schema_version: RAW_SCHEMA_VERSION,
            kind: "run_start",
            run_id: &self.identity.run_id,
            run_id_source: self.identity.run_id_source.as_str(),
            session_id: &self.identity.session_id,
            endpoint: &self.identity.endpoint,
            environment: self.identity.environment,
            path: &self.configured_path,
            queue_capacity: lock(&shared.ledger).capacity,
            flush_interval_ms: u64::try_from(self.flush_interval.as_millis()).unwrap_or(u64::MAX),
            rotate_bytes: self.rotate_bytes,
            started_at_ns: nautilus_core::time::get_atomic_clock_realtime()
                .get_time_ns()
                .as_u64(),
        };
        let line = serde_json::to_string(&record)
            .map_err(|e| format!("the run header could not be serialized: {e}"))?;

        self.append(shared, &line, Counted::Marker)?;
        self.flush(shared)
    }

    /// Drains the queue until the recorder is stopped, then writes the run's end record.
    fn run(mut self, shared: &Shared) {
        if let Err(reason) = self.run_until_stopped(shared) {
            self.abort(shared, &reason);
        }
    }

    fn run_until_stopped(&mut self, shared: &Shared) -> Result<(), String> {
        let mut deadline = Instant::now() + self.flush_interval;

        loop {
            let (batch, gap, stop, dropped) = {
                let mut ledger = lock(&shared.ledger);

                while !ledger.stop && ledger.queue.is_empty() && ledger.pending_gap.is_none() {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    let (guard, _timeout) = shared
                        .ready
                        .wait_timeout(ledger, deadline - now)
                        .map_err(|_| "the raw recording queue lock was poisoned".to_string())?;
                    ledger = guard;
                }

                let gap = ledger.pending_gap.take();
                let mut batch = Vec::new();
                while batch.len() < RAW_MD_DRAIN_CHUNK {
                    match ledger.queue.pop_front() {
                        Some(entry) => batch.push(entry),
                        None => break,
                    }
                }

                (batch, gap, ledger.stop, ledger.dropped)
            };

            if let Some(window) = gap {
                self.write_gap(shared, &window)?;
            }
            for entry in &batch {
                self.write_entry(shared, entry)?;
            }
            self.flush(shared)?;

            if stop {
                return self.write_run_end(shared, dropped);
            }

            if Instant::now() >= deadline {
                deadline = Instant::now() + self.flush_interval;
            }
        }
    }

    fn write_entry(&mut self, shared: &Shared, entry: &Entry) -> Result<(), String> {
        match entry {
            Entry::Frame {
                direction,
                class,
                payload,
                received_at_ns,
                received_mono_ns,
                recv_seq,
            } => {
                let record = FrameRecord {
                    schema_version: RAW_SCHEMA_VERSION,
                    kind: "frame",
                    run_id: &self.identity.run_id,
                    session_id: &self.identity.session_id,
                    endpoint: &self.identity.endpoint,
                    environment: self.identity.environment,
                    recv_seq: *recv_seq,
                    received_at_ns: *received_at_ns,
                    received_mono_ns: *received_mono_ns,
                    direction: *direction,
                    frame_class: *class,
                    raw: payload,
                };
                let line = serde_json::to_string(&record)
                    .map_err(|e| format!("a raw frame record could not be serialized: {e}"))?;

                self.append(shared, &line, Counted::Record)
            }
            Entry::Metadata(snapshot) => {
                let record = MetadataRecord {
                    schema_version: RAW_SCHEMA_VERSION,
                    kind: "rest_metadata",
                    run_id: &self.identity.run_id,
                    session_id: &self.identity.session_id,
                    endpoint: &self.identity.endpoint,
                    environment: self.identity.environment,
                    target: snapshot.target(),
                    request_started_at_ns: snapshot.request_started_at_ns(),
                    request_ended_at_ns: snapshot.request_ended_at_ns(),
                    status: snapshot.status(),
                    body_hash_blake3: snapshot.body_hash(),
                    headers: snapshot.headers(),
                    body: snapshot.body(),
                };
                let line = serde_json::to_string(&record)
                    .map_err(|e| format!("a metadata snapshot could not be serialized: {e}"))?;

                self.append(shared, &line, Counted::Record)
            }
        }
    }

    fn write_gap(&mut self, shared: &Shared, window: &GapWindow) -> Result<(), String> {
        let record = GapRecord {
            schema_version: RAW_SCHEMA_VERSION,
            kind: "gap",
            run_id: &self.identity.run_id,
            session_id: &self.identity.session_id,
            endpoint: &self.identity.endpoint,
            environment: self.identity.environment,
            dropped: window.dropped,
            dropped_from_seq: window.from_seq,
            dropped_to_seq: window.to_seq,
        };
        let line = serde_json::to_string(&record)
            .map_err(|e| format!("a gap record could not be serialized: {e}"))?;

        self.append(shared, &line, Counted::Marker)?;
        lock(&shared.ledger).gaps += 1;

        Ok(())
    }

    fn write_run_end(&mut self, shared: &Shared, dropped: u64) -> Result<(), String> {
        let (records, markers, bytes, gaps, rotations, segments, last_recv_seq) = {
            let ledger = lock(&shared.ledger);

            (
                ledger.records,
                ledger.markers,
                ledger.bytes,
                ledger.gaps,
                ledger.rotations,
                ledger.segments,
                ledger.next_seq.saturating_sub(1),
            )
        };
        let clean = dropped == 0;
        let reason = (!clean).then(|| {
            format!(
                "{dropped} frame(s) were dropped when the queue was full; the gap records name the \
                 receive numbers that are missing"
            )
        });
        let record = EndRecord {
            schema_version: RAW_SCHEMA_VERSION,
            kind: "run_end",
            run_id: &self.identity.run_id,
            session_id: &self.identity.session_id,
            endpoint: &self.identity.endpoint,
            environment: self.identity.environment,
            ended_at_ns: nautilus_core::time::get_atomic_clock_realtime()
                .get_time_ns()
                .as_u64(),
            clean,
            reason: reason.as_deref(),
            records,
            markers,
            bytes,
            dropped,
            gaps,
            rotations,
            segments,
            last_recv_seq,
        };
        let line = serde_json::to_string(&record)
            .map_err(|e| format!("the end-of-run record could not be serialized: {e}"))?;

        // The run's own end record is a marker, and it is written even when frames were dropped: a
        // run that dropped frames must be identifiable as unclean *in the file*, never silently.
        self.append(shared, &line, Counted::Marker)?;
        self.flush(shared)?;
        self.file = None;
        lock(&shared.ledger).finished = true;

        Ok(())
    }

    fn append(&mut self, shared: &Shared, line: &str, counted: Counted) -> Result<(), String> {
        let path = segment_path(&self.directory, self.segment_index);
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| format!("the raw segment `{}` is no longer open", path.display()))?;

        file.write_all(line.as_bytes())
            .and_then(|()| file.write_all(b"\n"))
            .map_err(|e| {
                format!(
                    "the raw recording could not write to `{}`: {e}",
                    path.display()
                )
            })?;

        let written = line.len() as u64 + 1;
        self.segment_bytes += written;
        self.pending_bytes += written;
        {
            let mut ledger = lock(&shared.ledger);
            match counted {
                Counted::Record => ledger.records += 1,
                Counted::Marker => ledger.markers += 1,
            }
        }

        if counted == Counted::Record && self.segment_bytes >= self.rotate_bytes {
            self.rotate(shared)?;
        }

        Ok(())
    }

    /// Flushes the current segment, and books the bytes only once the flush has succeeded.
    fn flush(&mut self, shared: &Shared) -> Result<(), String> {
        let path = segment_path(&self.directory, self.segment_index);
        if let Some(file) = self.file.as_mut() {
            file.flush().map_err(|e| {
                format!(
                    "the raw recording could not flush `{}`: {e}",
                    path.display()
                )
            })?;
        }

        let flushed = std::mem::take(&mut self.pending_bytes);
        lock(&shared.ledger).bytes += flushed;

        Ok(())
    }

    fn rotate(&mut self, shared: &Shared) -> Result<(), String> {
        self.flush(shared)?;
        self.file = None;

        let next = self.segment_index.saturating_add(1);
        self.open_segment(shared, next)?;
        lock(&shared.ledger).rotations += 1;

        Ok(())
    }

    fn open_segment(&mut self, shared: &Shared, index: u32) -> Result<(), String> {
        let path = segment_path(&self.directory, index);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| {
                format!(
                    "the raw segment `{}` could not be created: {e}",
                    path.display()
                )
            })?;

        self.file = Some(BufWriter::new(file));
        self.segment_index = index;
        self.segment_bytes = 0;
        lock(&shared.ledger).segments += 1;

        Ok(())
    }

    /// Aborts the recording: the failure is kept, the writer stops, and no end record is written.
    ///
    /// A missing `run_end` is the reader's incomplete-recording rule, so an aborted run is identifiable
    /// from the file as well as from the statistics.
    fn abort(&mut self, shared: &Shared, reason: &str) {
        self.file = None;
        {
            let mut ledger = lock(&shared.ledger);
            // The failure is kept before the writer stops: `finish` reads it back out and the owning
            // data client reports it, so an aborted run is surfaced rather than swallowed.
            ledger.failed = Some(reason.to_string());
            ledger.stop = true;
        }
        log::error!("Ondo raw recording aborted: {reason}");
    }
}

/// Locks a mutex, recovering a poisoned lock.
///
/// The state behind it is a bounded queue of records and its counters, with no invariant a panic could
/// break, so a poisoned lock is recovered rather than propagated into the receive path.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Returns the index of the next segment to write in `directory`.
///
/// A session that starts in a directory that already holds another session's segments continues the
/// numbering instead of overwriting or interleaving with them, which is what makes the file order of a
/// restart readable. Only files count: an entry that merely shares a segment's name (a directory, for
/// one) is not a segment this recorder wrote, and letting it advance the numbering would skip a name
/// the rotation is about to open - where a collision is exactly the failure the numbering exists to
/// avoid.
fn next_segment_index(directory: &Path) -> std::io::Result<u32> {
    let mut highest = 0;

    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(index) = segment_index_of(name) {
            highest = highest.max(index);
        }
    }

    Ok(highest + 1)
}

/// Returns the rotation index a segment file name carries.
fn segment_index_of(name: &str) -> Option<u32> {
    if name == format!("{RAW_MD_SEGMENT_STEM}.jsonl") {
        return Some(1);
    }

    name.strip_prefix(&format!("{RAW_MD_SEGMENT_STEM}_part"))?
        .strip_suffix(".jsonl")?
        .parse::<u32>()
        .ok()
}

/// Returns the file name of the segment at `index`.
fn segment_name(index: u32) -> String {
    if index <= 1 {
        format!("{RAW_MD_SEGMENT_STEM}.jsonl")
    } else {
        format!("{RAW_MD_SEGMENT_STEM}_part{index:04}.jsonl")
    }
}

/// Returns the path of the segment at `index` in `directory`.
fn segment_path(directory: &Path, index: u32) -> PathBuf {
    directory.join(segment_name(index))
}

/// The header record of one recording session.
#[derive(Debug, Serialize)]
struct StartRecord<'a> {
    schema_version: u32,
    kind: &'static str,
    run_id: &'a str,
    /// How `run_id` was established (`configured` / `derived_from_path`): the recorded answer to
    /// "does this id have to agree with the application's tape?", never left silent.
    run_id_source: &'a str,
    session_id: &'a str,
    endpoint: &'a str,
    environment: OndoEnvironment,
    path: &'a str,
    queue_capacity: usize,
    flush_interval_ms: u64,
    rotate_bytes: u64,
    started_at_ns: u64,
}

/// One recorded public frame.
#[derive(Debug, Serialize)]
struct FrameRecord<'a> {
    schema_version: u32,
    kind: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    endpoint: &'a str,
    environment: OndoEnvironment,
    recv_seq: u64,
    received_at_ns: u64,
    received_mono_ns: u64,
    direction: FrameDirection,
    frame_class: PublicFrameClass,
    raw: &'a str,
}

/// The marker that makes a run of dropped frames identifiable.
#[derive(Debug, Serialize)]
struct GapRecord<'a> {
    schema_version: u32,
    kind: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    endpoint: &'a str,
    environment: OndoEnvironment,
    dropped: u64,
    dropped_from_seq: u64,
    dropped_to_seq: u64,
}

/// The end-of-run statistics, written as the session's last record.
#[derive(Debug, Serialize)]
struct EndRecord<'a> {
    schema_version: u32,
    kind: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    endpoint: &'a str,
    environment: OndoEnvironment,
    ended_at_ns: u64,
    clean: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    records: u64,
    markers: u64,
    bytes: u64,
    dropped: u64,
    gaps: u64,
    rotations: u64,
    segments: u32,
    last_recv_seq: u64,
}

/// One REST metadata snapshot.
#[derive(Debug, Serialize)]
struct MetadataRecord<'a> {
    schema_version: u32,
    kind: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    endpoint: &'a str,
    environment: OndoEnvironment,
    target: &'a str,
    request_started_at_ns: u64,
    request_ended_at_ns: u64,
    status: u16,
    body_hash_blake3: &'a str,
    headers: &'a BTreeMap<String, String>,
    body: &'a str,
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use rstest::rstest;

    use super::*;

    const DEPTH_FIXTURE: &str = include_str!("../test_data/ws/depth_observed.json");

    /// A directory under the system temp root, unique to `label` and to this process.
    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("nautilus-ondo-raw-{label}-{}", UUID4::new()))
    }

    fn cleanup(root: &Path) {
        let _ = fs::remove_dir_all(root);
    }

    /// Every segment of `directory`, in rotation order (plain name sort).
    fn segments(directory: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(directory)
            .expect("the raw directory is readable")
            .map(|entry| entry.expect("a directory entry").path())
            .filter(|path| {
                // A file only: a directory that merely shares a segment's name is not a segment.
                path.is_file()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| segment_index_of(name).is_some())
            })
            .collect();
        paths.sort();

        paths
    }

    /// Every record of every segment, in file order.
    fn records(directory: &Path) -> Vec<serde_json::Value> {
        let mut all = Vec::new();

        for path in segments(directory) {
            let text = fs::read_to_string(&path).expect("a segment is readable");
            for line in text.lines() {
                all.push(serde_json::from_str(line).expect("every line is one JSON record"));
            }
        }

        all
    }

    fn records_with_kind(directory: &Path, kind: &str) -> Vec<serde_json::Value> {
        records(directory)
            .into_iter()
            .filter(|record| record["kind"] == kind)
            .collect()
    }

    /// The production frame of the archived depth channel.
    fn public_frame() -> &'static str {
        DEPTH_FIXTURE
    }

    /// A login acknowledgement: never part of the public stream.
    fn login_frame() -> &'static str {
        r#"{"type":"loggedIn","data":{"token":"not-recorded"}}"#
    }

    /// A private payload: an update on a channel this adapter does not subscribe to.
    fn private_frame() -> &'static str {
        r#"{"type":"update","channel":"orderUpdatesPerps","data":[{"orderId":"42","status":"OPEN"}]}"#
    }

    fn config(root: &Path, run_id: &str) -> RawMdRecorderConfig {
        RawMdRecorderConfig::new(
            root.join("raw_ondo"),
            run_id,
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        )
    }

    fn offer(sink: &RawMdSink, frame: &str) -> RecordOutcome {
        sink.record_inbound(frame, UnixNanos::from(1_789_384_200_000_000_000))
    }

    /// Offers a public frame that must be recorded, for the cases that only need it queued.
    fn record(sink: &RawMdSink, frame: &str) {
        assert_eq!(offer(sink, frame), RecordOutcome::Recorded);
    }

    #[rstest]
    fn test_the_plans_limits_are_the_defaults_of_a_recording() {
        let config = RawMdRecorderConfig::new(
            PathBuf::from("/tmp/raw_ondo"),
            "fixture",
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        );

        assert_eq!(RAW_SCHEMA_VERSION, 1);
        assert_eq!(config.queue_capacity, 4096);
        assert_eq!(config.flush_interval, Duration::from_secs(1));
        assert_eq!(config.rotate_bytes, 128 * 1024 * 1024);
        assert_eq!(config.run_id, "fixture");
        assert!(
            config.session_id.starts_with("fixture-"),
            "the session id is the run id plus this process's own suffix, was `{}`",
            config.session_id
        );
        assert_eq!(config.session_id.len(), "fixture-".len() + 12);
    }

    #[rstest]
    fn test_the_run_id_is_the_run_directory_the_recorder_owns() {
        assert_eq!(
            derive_run_id(Path::new("reports/stage1/raw_ondo")),
            "stage1",
            "the application passes <runDir>/raw_ondo, so the run directory names the run"
        );
        assert_eq!(
            derive_run_id(Path::new(
                "reports/ondo-acceptance/20260914T000000Z/raw_ondo"
            )),
            "20260914T000000Z",
            "a stamped run directory gives the run id the application's tape also carries"
        );
        assert_eq!(derive_run_id(Path::new("raw_ondo")), "raw_ondo");
    }

    #[rstest]
    fn test_the_header_carries_the_configured_run_id_the_tape_also_uses() {
        // The application's default `--out reports/stage1` is *not* named after the run: the
        // configured run id (its process stamp) is what makes the raw frames join the tape.
        let root = temp_dir("configured-run-id");
        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig::new(
            root.join("stage1").join("raw_ondo"),
            "20260914T000000Z",
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        ))
        .expect("the recorder starts");
        record(&recorder.sink(), public_frame());
        recorder.finish();

        let records = records(&root.join("stage1").join("raw_ondo"));
        assert_eq!(records[0]["kind"], "run_start");
        assert_eq!(records[0]["run_id"], "20260914T000000Z");
        assert_eq!(records[0]["run_id_source"], "configured");
        assert_eq!(records[0]["session_id"], records[1]["session_id"]);
        assert_eq!(
            records[1]["run_id"], "20260914T000000Z",
            "every record of the run carries the configured run id"
        );

        cleanup(&root);
    }

    #[rstest]
    fn test_the_derived_run_id_says_it_was_derived_from_the_path() {
        let root = temp_dir("derived-run-id");
        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig::derived_from_path(
            root.join("stage1").join("raw_ondo"),
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        ))
        .expect("the recorder starts");
        record(&recorder.sink(), public_frame());
        recorder.finish();

        let records = records(&root.join("stage1").join("raw_ondo"));
        assert_eq!(
            records[0]["run_id"], "stage1",
            "the parent directory names it"
        );
        assert_eq!(
            records[0]["run_id_source"], "derived_from_path",
            "the file says so, so a tape join cannot be assumed silently"
        );
        assert!(
            records[0]["session_id"]
                .as_str()
                .expect("a session id")
                .starts_with("stage1-"),
            "the session id is built from the derived run id, was {}",
            records[0]["session_id"]
        );

        cleanup(&root);
    }

    #[rstest]
    #[case::public_book_frame(DEPTH_FIXTURE, true)]
    #[case::pong(r#"{"type":"pong"}"#, true)]
    #[case::subscribed(r#"{"type":"subscribed","channel":"depthBooksPerps","data":{}}"#, true)]
    #[case::venue_error(r#"{"type":"error","message":"unknown channel","code":400}"#, true)]
    #[case::login_response(login_frame(), false)]
    #[case::private_update(private_frame(), false)]
    #[case::unknown_message_type(r#"{"type":"snapshot","channel":"depthBooksPerps"}"#, false)]
    #[case::not_json("not a frame", false)]
    #[case::update_without_a_channel(r#"{"type":"update","data":[]}"#, false)]
    fn test_the_inbound_whitelist_admits_only_public_frames(
        #[case] frame: &str,
        #[case] public: bool,
    ) {
        let classified = PublicFrame::inbound(frame);

        assert_eq!(
            classified.is_ok(),
            public,
            "`{frame}` should be public: {classified:?}"
        );
        if let Ok(classified) = classified {
            assert_eq!(classified.direction(), FrameDirection::Inbound);
            assert_eq!(classified.payload(), frame, "the payload is verbatim");
        }
    }

    #[rstest]
    #[case::subscribe(
        r#"{"op":"subscribe","channel":"depthBooksPerps","markets":["NVDA-USD.P"]}"#,
        true
    )]
    #[case::unsubscribe(
        r#"{"op":"unsubscribe","channel":"tradesPerps","markets":["NVDA-USD.P"]}"#,
        true
    )]
    #[case::ping(r#"{"op":"ping"}"#, true)]
    #[case::login_request(r#"{"op":"login"}"#, false)]
    #[case::order_command(r#"{"op":"placeOrder","market":"NVDA-USD.P"}"#, false)]
    #[case::no_op(r#"{"channel":"depthBooksPerps"}"#, false)]
    #[case::not_json("subscribe", false)]
    fn test_the_outbound_whitelist_admits_only_this_adapters_public_requests(
        #[case] body: &str,
        #[case] public: bool,
    ) {
        let classified = PublicFrame::outbound(body);

        assert_eq!(classified.is_ok(), public, "`{body}`: {classified:?}");
        if let Ok(classified) = classified {
            assert_eq!(classified.direction(), FrameDirection::Outbound);
            assert_eq!(classified.payload(), body);
        }
    }

    #[rstest]
    fn test_a_session_records_the_public_frame_and_neither_the_login_nor_the_private_payload() {
        let root = temp_dir("whitelist");
        let mut recorder =
            RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        let sink = recorder.sink();

        // The three frames a public connection can see, offered the same way: the whitelist decides.
        let _outcome = offer(&sink, public_frame());
        let _outcome = offer(&sink, login_frame());
        let _outcome = offer(&sink, private_frame());
        let _outcome = sink.record_outbound(r#"{"op":"login"}"#, UnixNanos::from(2));
        let stats = recorder.finish();

        let directory = root.join("raw_ondo");
        let frames = records_with_kind(&directory, "frame");
        assert_eq!(
            frames.len(),
            1,
            "only the public frame is recorded, was {frames:?}"
        );
        assert_eq!(frames[0]["raw"], public_frame());
        assert_eq!(frames[0]["frame_class"], "market_update");
        assert_eq!(stats.records, 1, "the refused frames are not records");
        assert_eq!(stats.offered, 1, "a refused frame is not even offered");
        assert_eq!(
            offer(&sink, login_frame()),
            RecordOutcome::NotPublic,
            "the login frame is refused, explicitly"
        );
        assert_eq!(
            offer(&sink, private_frame()),
            RecordOutcome::NotPublic,
            "the private payload is refused, explicitly"
        );

        let text =
            fs::read_to_string(segment_path(&directory, 1)).expect("the segment is readable");
        assert!(
            !text.contains("loggedIn"),
            "no login acknowledgement reaches the file"
        );
        assert!(
            !text.contains("orderUpdatesPerps"),
            "no private payload reaches the file"
        );
        assert!(!text.contains("not-recorded"));

        cleanup(&root);
    }

    #[rstest]
    fn test_a_record_round_trips_with_every_required_field() {
        let root = temp_dir("round-trip");
        let mut recorder =
            RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        record(&recorder.sink(), public_frame());
        recorder.finish();
        let directory = root.join("raw_ondo");

        let records = records(&directory);
        assert_eq!(
            records
                .iter()
                .map(|r| r["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["run_start", "frame", "run_end"],
            "one header, one frame and one end record"
        );

        let header = &records[0];
        assert_eq!(header["schema_version"], 1);
        assert_eq!(header["path"], root.join("raw_ondo").display().to_string());
        assert_eq!(header["queue_capacity"], 4096);
        assert_eq!(header["rotate_bytes"], 128 * 1024 * 1024);
        assert_eq!(header["flush_interval_ms"], 1000);
        assert_eq!(header["endpoint"], "wss://api.ondoperps.xyz/ws");
        assert_eq!(header["environment"], "production");

        let frame = &records[1];
        assert_eq!(frame["schema_version"], 1);
        assert_eq!(frame["kind"], "frame");
        assert_eq!(frame["run_id"], "run-a");
        assert_eq!(frame["session_id"], header["session_id"]);
        assert_eq!(frame["recv_seq"], 1);
        assert_eq!(frame["received_at_ns"], 1_789_384_200_000_000_000_u64);
        assert!(
            frame["received_mono_ns"].as_u64().is_some(),
            "a monotonic receive time is recorded"
        );
        assert_eq!(frame["endpoint"], "wss://api.ondoperps.xyz/ws");
        assert_eq!(frame["environment"], "production");
        assert_eq!(frame["direction"], "inbound");
        assert_eq!(frame["frame_class"], "market_update");
        assert_eq!(
            frame["raw"],
            public_frame(),
            "the payload is what the venue sent, byte for byte"
        );

        let end = &records[2];
        assert_eq!(end["kind"], "run_end");
        assert_eq!(end["clean"], true);
        assert_eq!(end["records"], 1);
        assert_eq!(end["dropped"], 0);
        assert_eq!(end["gaps"], 0);
        assert_eq!(end["rotations"], 0);
        assert_eq!(end["segments"], 1);
        assert_eq!(end["last_recv_seq"], 1);
        assert!(end["bytes"].as_u64().is_some_and(|bytes| bytes > 0));
        assert!(end["reason"].is_null(), "a clean run names no reason");

        cleanup(&root);
    }

    #[rstest]
    fn test_a_full_queue_accumulates_a_gap_and_never_ends_clean() {
        let root = temp_dir("gap");
        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig {
            queue_capacity: 2,
            // A flush interval longer than the test: once the writer has settled into its wait, the
            // queue it is not draining is the queue the offers below fill.
            flush_interval: Duration::from_secs(3600),
            rotate_bytes: RAW_MD_ROTATE_BYTES,
            ..config(&root, "run-a")
        })
        .expect("the recorder starts");
        // Give the writer its first look (an empty queue, so it waits) before filling the queue: its
        // first pass drains whatever is already queued, which would make the drop count a race.
        std::thread::sleep(Duration::from_millis(200));
        let sink = recorder.sink();
        let mut outcomes = Vec::new();
        for _ in 0..5 {
            outcomes.push(offer(&sink, public_frame()));
        }

        assert_eq!(
            outcomes,
            vec![
                RecordOutcome::Recorded,
                RecordOutcome::Recorded,
                RecordOutcome::Dropped,
                RecordOutcome::Dropped,
                RecordOutcome::Dropped,
            ],
            "a full queue sheds the frames that do not fit instead of blocking the receive path"
        );

        let stats = recorder.finish();
        assert_eq!(stats.records, 2, "two records fit the queue");
        assert_eq!(stats.dropped, 3, "three frames were dropped");
        assert_eq!(stats.gaps, 1, "one gap record names the hole");
        assert!(!stats.clean, "a run that dropped frames is not clean");
        assert!(stats.finished, "the run still ends with its statistics");

        let directory = root.join("raw_ondo");
        let records = records(&directory);
        let seqs: Vec<u64> = records
            .iter()
            .filter(|record| record["kind"] == "frame")
            .map(|record| record["recv_seq"].as_u64().unwrap())
            .collect();
        assert_eq!(
            seqs,
            vec![1, 2],
            "the frames that fit kept their receive order"
        );

        let gaps = records_with_kind(&directory, "gap");
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0]["dropped"], 3);
        assert_eq!(gaps[0]["dropped_from_seq"], 3);
        assert_eq!(gaps[0]["dropped_to_seq"], 5);

        let end = records_with_kind(&directory, "run_end");
        assert_eq!(end.len(), 1);
        assert_eq!(end[0]["clean"], false);
        assert_eq!(end[0]["dropped"], 3);
        assert_eq!(end[0]["gaps"], 1);
        assert!(
            end[0]["reason"].as_str().is_some_and(|r| r.contains("3")),
            "the unclean end names the drop count, was {}",
            end[0]["reason"]
        );

        cleanup(&root);
    }

    #[rstest]
    fn test_rotation_keeps_the_receive_numbering_continuous() {
        let root = temp_dir("rotation");
        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig {
            queue_capacity: 64,
            flush_interval: Duration::from_secs(30),
            rotate_bytes: 400,
            ..config(&root, "run-a")
        })
        .expect("the recorder starts");
        let sink = recorder.sink();

        for _ in 0..6 {
            record(&sink, public_frame());
        }
        let stats = recorder.finish();
        let directory = root.join("raw_ondo");

        let files: Vec<String> = segments(&directory)
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert!(
            files.len() > 1,
            "400 bytes per segment rotates a fixture frame, was {files:?}"
        );
        assert_eq!(files[0], "raw_md.jsonl");
        assert_eq!(files[1], "raw_md_part0002.jsonl");
        assert_eq!(
            stats.rotations as usize,
            files.len() - 1,
            "every further segment is one rotation"
        );
        assert_eq!(stats.segments as usize, files.len());

        let seqs: Vec<u64> = records(&directory)
            .iter()
            .filter(|record| record["kind"] == "frame")
            .map(|record| record["recv_seq"].as_u64().unwrap())
            .collect();
        assert_eq!(
            seqs,
            (1..=6).collect::<Vec<u64>>(),
            "receiving order is untouched by rotation"
        );

        let end = records_with_kind(&directory, "run_end");
        assert_eq!(end.len(), 1, "the run ends exactly once");
        assert_eq!(end[0]["last_recv_seq"], 6);
        assert_eq!(end[0]["clean"], true);

        cleanup(&root);
    }

    #[rstest]
    fn test_a_disk_error_aborts_the_recording_and_surfaces_it() {
        let root = temp_dir("disk-error");
        let directory = root.join("raw_ondo");
        fs::create_dir_all(&directory).expect("the raw directory is created");
        // The next segment's name is taken by a directory, so the rotation cannot open it.
        fs::create_dir(directory.join("raw_md_part0002.jsonl")).expect("the block is created");

        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig {
            rotate_bytes: 1,
            ..config(&root, "run-a")
        })
        .expect("the header is written before anything rotates");
        let sink = recorder.sink();

        assert_eq!(
            offer(&sink, public_frame()),
            RecordOutcome::Recorded,
            "the frame is queued before the rotation fails"
        );

        // The writer fails on the next drain, which is the periodic flush that follows the offer.
        let deadline = Instant::now() + Duration::from_secs(5);
        while sink.stats().failed.is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            sink.stats().failed.is_some(),
            "a write failure aborts the recording"
        );
        assert_eq!(
            offer(&sink, public_frame()),
            RecordOutcome::RecordingFailed,
            "once the writer has aborted, the receive path is told"
        );

        let stats = recorder.finish();
        assert!(!stats.clean, "an aborted recording is not clean");
        assert!(
            !stats.finished,
            "no run_end is written, so a reader sees the recording as incomplete"
        );
        let reason = stats
            .failed
            .expect("the failure is surfaced, not swallowed");
        assert!(
            reason.contains("raw_md_part0002.jsonl"),
            "the failure names the segment it could not create, was `{reason}`"
        );
        assert_eq!(stats.rotations, 0, "the failed rotation never counted");

        // The header and the first frame are on disk, and the run has no end record.
        let records = records(&directory);
        assert_eq!(
            records
                .iter()
                .map(|r| r["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["run_start", "frame"]
        );

        cleanup(&root);
    }

    #[rstest]
    fn test_a_missing_directory_is_created_and_a_path_that_is_a_file_is_refused() {
        let root = temp_dir("directory");
        let nested = root.join("a").join("b").join("raw_ondo");
        assert!(!nested.exists());

        let mut recorder = RawMdRecorder::start(RawMdRecorderConfig::new(
            &nested,
            "run-a",
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        ))
        .expect("a missing directory is created");
        record(&recorder.sink(), public_frame());
        recorder.finish();
        assert!(nested.join("raw_md.jsonl").is_file());

        let file = root.join("not-a-directory");
        fs::write(&file, b"").expect("the file is created");
        let error = RawMdRecorder::start(RawMdRecorderConfig::new(
            &file,
            "run-a",
            "wss://api.ondoperps.xyz/ws",
            OndoEnvironment::Production,
        ))
        .expect_err("a path that is a file cannot become a recording directory");
        assert!(
            error.to_string().contains(&file.display().to_string()),
            "the error names the path, was `{error}`"
        );

        cleanup(&root);
    }

    #[rstest]
    fn test_a_second_session_continues_the_numbering_without_touching_the_first_file() {
        let root = temp_dir("restart");
        let directory = root.join("raw_ondo");

        let mut first = RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        record(&first.sink(), public_frame());
        first.finish();
        let first_bytes = fs::read_to_string(segment_path(&directory, 1)).expect("readable");

        let mut second = RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        record(&second.sink(), public_frame());
        let stats = second.finish();

        assert_eq!(
            stats.segments, 1,
            "the session wrote one segment of its own"
        );
        let files: Vec<String> = segments(&directory)
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(files, vec!["raw_md.jsonl", "raw_md_part0002.jsonl"]);
        assert_eq!(
            fs::read_to_string(segment_path(&directory, 1)).expect("readable"),
            first_bytes,
            "the previous session's segment is left exactly as it was"
        );

        let sessions: Vec<String> = records(&directory)
            .iter()
            .filter(|record| record["kind"] == "run_start")
            .map(|record| record["session_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(sessions.len(), 2, "each session writes its own header");
        assert_ne!(sessions[0], sessions[1]);

        cleanup(&root);
    }

    #[rstest]
    fn test_the_metadata_snapshot_keeps_only_the_whitelisted_headers_and_hashes_the_body() {
        let body = r#"{"success":true,"result":{"perps":{"tradingPairs":[]}}}"#;
        let snapshot = MetadataSnapshot::new(
            "/v1/markets",
            200,
            [
                ("content-type".to_string(), "application/json".to_string()),
                ("Content-Length".to_string(), "57".to_string()),
                ("x-request-id".to_string(), "abc123".to_string()),
                ("set-cookie".to_string(), "dropped".to_string()),
            ],
            body,
            1_789_384_200_000_000_000_u64,
            1_789_384_200_000_500_000_u64,
        );

        assert_eq!(
            snapshot.headers().keys().collect::<Vec<_>>(),
            vec!["content-length", "content-type"],
            "only whitelisted header names survive, by lower-case name"
        );
        assert_eq!(snapshot.body(), body);
        assert_eq!(
            snapshot.body_hash(),
            blake3::hash(body.as_bytes()).to_hex().as_str(),
            "the snapshot hashes the body it was given"
        );
        assert_eq!(snapshot.body_hash().len(), 64);

        let root = temp_dir("metadata");
        let mut recorder =
            RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        assert_eq!(
            recorder.sink().record_metadata_snapshot(&snapshot),
            RecordOutcome::Recorded
        );
        recorder.finish();

        let directory = root.join("raw_ondo");
        let records = records_with_kind(&directory, "rest_metadata");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["target"], "/v1/markets");
        assert_eq!(records[0]["status"], 200);
        assert_eq!(
            records[0]["request_started_at_ns"],
            1_789_384_200_000_000_000_u64
        );
        assert_eq!(
            records[0]["request_ended_at_ns"],
            1_789_384_200_000_500_000_u64
        );
        assert_eq!(records[0]["body"], body);
        assert_eq!(records[0]["body_hash_blake3"], snapshot.body_hash());
        assert_eq!(records[0]["headers"]["content-type"], "application/json");
        assert!(
            records[0]["headers"].get("set-cookie").is_none(),
            "an unwhitelisted header has no place in a record"
        );
        let text = fs::read_to_string(segment_path(&directory, 1)).expect("readable");
        assert!(!text.contains("set-cookie"));
        assert!(
            !text.contains("abc123"),
            "the request id is not recorded either"
        );

        cleanup(&root);
    }

    #[rstest]
    fn test_the_segment_naming_round_trips_and_sorts_in_rotation_order() {
        assert_eq!(segment_name(1), "raw_md.jsonl");
        assert_eq!(segment_name(2), "raw_md_part0002.jsonl");
        assert_eq!(segment_name(1234), "raw_md_part1234.jsonl");
        assert_eq!(segment_index_of("raw_md.jsonl"), Some(1));
        assert_eq!(segment_index_of("raw_md_part0002.jsonl"), Some(2));
        assert_eq!(segment_index_of("raw_md_part1234.jsonl"), Some(1234));
        assert_eq!(segment_index_of("raw_md_part.jsonl"), None);
        assert_eq!(segment_index_of("manifest.json"), None);

        let mut names = vec![
            segment_name(3).to_string(),
            segment_name(1).to_string(),
            segment_name(2).to_string(),
            segment_name(10).to_string(),
        ];
        names.sort();
        assert_eq!(
            names,
            vec![
                "raw_md.jsonl",
                "raw_md_part0002.jsonl",
                "raw_md_part0003.jsonl",
                "raw_md_part0010.jsonl"
            ],
            "plain name sort is rotation order"
        );
    }

    #[rstest]
    fn test_the_next_segment_index_continues_a_directory_a_previous_session_used() {
        let root = temp_dir("index");
        fs::create_dir_all(&root).expect("the directory is created");
        assert_eq!(next_segment_index(&root).unwrap(), 1);

        fs::write(root.join(segment_name(1)), b"").unwrap();
        assert_eq!(next_segment_index(&root).unwrap(), 2);

        fs::write(root.join(segment_name(7)), b"").unwrap();
        fs::write(root.join("raw.jsonl"), b"").unwrap();
        assert_eq!(next_segment_index(&root).unwrap(), 8);

        // An entry that merely shares a segment's name is not a segment: letting it advance the
        // numbering would skip the very name the next rotation opens.
        fs::create_dir(root.join(segment_name(9))).unwrap();
        assert_eq!(next_segment_index(&root).unwrap(), 8);

        cleanup(&root);
    }

    #[rstest]
    fn test_the_statistics_report_the_run_and_its_cleanliness() {
        let root = temp_dir("stats");
        let mut recorder =
            RawMdRecorder::start(config(&root, "run-a")).expect("the recorder starts");
        let running = recorder.stats();
        assert_eq!(running.schema_version, 1);
        assert_eq!(running.offered, 0);
        assert_eq!(running.records, 0);
        assert_eq!(
            running.markers, 1,
            "the header is written before the writer thread starts"
        );
        assert!(running.bytes > 0, "the header is flushed");
        assert_eq!(running.segments, 1);
        assert!(!running.finished);
        assert!(
            !running.clean,
            "a recording that has not ended is not a clean one"
        );

        record(&recorder.sink(), public_frame());
        let stats = recorder.finish();

        assert_eq!(stats.offered, 1);
        assert_eq!(stats.records, 1);
        assert_eq!(stats.markers, 2, "the header and the end record");
        assert_eq!(stats.last_recv_seq, 1);
        assert!(stats.bytes > 0);
        assert!(stats.finished);
        assert!(stats.clean);
        assert_eq!(
            stats,
            recorder.stats(),
            "the finished statistics are what the recorder keeps reporting"
        );

        cleanup(&root);
    }
}
