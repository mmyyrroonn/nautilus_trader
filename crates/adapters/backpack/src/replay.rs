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

//! Bounded credential-free JSONL replay preserving recorded engine and receipt timestamps.
use std::io::BufRead;

use nautilus_core::UnixNanos;
use nautilus_model::data::Data;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    config::BackpackPublicLifecycleConfig,
    data_error::BackpackDataError,
    depth::BackpackDepthSynchronizer,
    instruments::BackpackInstrumentMetadata,
    public::{BackpackPublicEvent, BackpackPublicStreamParser},
};
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Frame,
    Snapshot,
    Restart,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record<'a> {
    kind: Kind,
    generation: u64,
    received_at_ns: u64,
    #[serde(borrow)]
    payload: Option<&'a RawValue>,
}
/// An offline owner for one validated instrument, with bounded frames and depth storage.
/// Historical replay does not establish live freshness or execution readiness.
#[derive(Debug)]
pub struct BackpackPublicReplay {
    metadata: BackpackInstrumentMetadata,
    generation: u64,
    parser: BackpackPublicStreamParser,
    book: BackpackDepthSynchronizer,
    policy: BackpackPublicLifecycleConfig,
}
impl BackpackPublicReplay {
    /// Starts replay with the same checked public policy as the native owner.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid policy or depth bounds.
    pub fn new_checked(
        metadata: BackpackInstrumentMetadata,
        generation: u64,
        policy: BackpackPublicLifecycleConfig,
    ) -> Result<Self, BackpackDataError> {
        policy.validate()?;
        let book = BackpackDepthSynchronizer::new_checked(
            metadata.clone(),
            generation,
            policy.depth_snapshot_limit,
            policy.max_buffer_frames,
            policy.max_levels_per_side,
        )
        .map_err(|_| BackpackDataError::Replay)?;
        Ok(Self {
            parser: BackpackPublicStreamParser::new(metadata.clone(), generation),
            metadata,
            generation,
            book,
            policy,
        })
    }
    /// Applies one exact JSON record. Old generations are discarded before touching parser state.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, oversized, future-generation, or invalid public data.
    pub fn apply_record(&mut self, line: &[u8]) -> Result<Option<Data>, BackpackDataError> {
        let result = self.apply_checked(line);
        if result.is_err() {
            self.book.invalidate();
        }
        result
    }
    fn apply_checked(&mut self, line: &[u8]) -> Result<Option<Data>, BackpackDataError> {
        if line.len() > self.policy.max_ws_message_bytes {
            self.book.invalidate();
            return Err(BackpackDataError::Replay);
        }
        let record: Record<'_> = serde_json::from_slice(line).map_err(|_| {
            self.book.invalidate();
            BackpackDataError::Replay
        })?;

        if record.generation < self.generation {
            return Ok(None);
        }
        let result = match record.kind {
            Kind::Restart => {
                if record.payload.is_some() || record.generation <= self.generation {
                    return Err(BackpackDataError::Replay);
                }
                self.book
                    .restart(record.generation)
                    .map_err(|_| BackpackDataError::Replay)?;
                self.generation = record.generation;
                self.parser =
                    BackpackPublicStreamParser::new(self.metadata.clone(), self.generation);
                Ok(None)
            }
            _ if record.generation != self.generation => Err(BackpackDataError::Replay),
            Kind::Snapshot => {
                let payload = record.payload.ok_or(BackpackDataError::Replay)?;
                self.book
                    .install_snapshot(
                        self.generation,
                        payload.get().as_bytes(),
                        UnixNanos::from(record.received_at_ns),
                    )
                    .map(|d| Some(Data::from(d)))
                    .map_err(|_| BackpackDataError::Replay)
            }
            Kind::Frame => {
                let payload = record.payload.ok_or(BackpackDataError::Replay)?;

                match self
                    .parser
                    .decode(
                        self.generation,
                        payload.get().as_bytes(),
                        UnixNanos::from(record.received_at_ns),
                    )
                    .map_err(|_| BackpackDataError::Replay)?
                {
                    BackpackPublicEvent::Quote(q) => Ok(Some(Data::Quote(q))),
                    BackpackPublicEvent::Trade(t) => Ok(Some(Data::Trade(t))),
                    BackpackPublicEvent::Mark { price, .. } => Ok(Some(Data::MarkPrice(price))),
                    BackpackPublicEvent::Depth(d) => self
                        .book
                        .apply(d)
                        .map(|d| d.map(Data::from))
                        .map_err(|_| BackpackDataError::Replay),
                    BackpackPublicEvent::Duplicate
                    | BackpackPublicEvent::QuoteUnavailable { .. } => Ok(None),
                }
            }
        };

        if result.is_err() {
            self.book.invalidate();
        }
        result
    }
    /// Streams at most `max_buffer_frames` JSONL records and 64 MiB without accumulating outputs.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O, record/file bounds, or invalid recorded public protocol data.
    pub fn read_json_lines<R: BufRead, F: FnMut(Data)>(
        &mut self,
        reader: &mut R,
        mut publish: F,
    ) -> Result<usize, BackpackDataError> {
        use std::io::Read;
        let mut records = 0;
        let mut total = 0;
        let mut line = Vec::new();
        loop {
            line.clear();
            let length = reader
                .by_ref()
                .take(self.policy.max_ws_message_bytes as u64 + 1)
                .read_until(b'\n', &mut line)
                .map_err(|_| BackpackDataError::Replay)?;

            if length == 0 {
                break;
            }
            total += length;
            records += 1;
            if records > self.policy.max_buffer_frames
                || length > self.policy.max_ws_message_bytes
                || total > 64 * 1024 * 1024
            {
                self.book.invalidate();
                return Err(BackpackDataError::Replay);
            }

            if let Some(data) = self.apply_record(&line)? {
                publish(data);
            }
        }
        Ok(records)
    }
}
