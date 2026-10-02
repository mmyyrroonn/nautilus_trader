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

//! Bounded snapshot/delta synchronization with explicit continuity and truncated coverage.

use std::collections::{BTreeMap, VecDeque};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use serde::Deserialize;

use crate::{
    instruments::BackpackInstrumentMetadata,
    parsing::unix_microseconds_to_nanos,
    public::{
        BackpackDepthUpdate, BackpackPublicError, MAX_PUBLIC_FRAME_BYTES, parse_levels,
        parse_sequence,
    },
};

/// The synchronized view's state; continuity is separate from full-depth/freshness claims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackBookState {
    /// Buffering while a fresh REST snapshot is in flight.
    Buffering,
    /// A snapshot and every accepted subsequent message form a continuous bounded view.
    Continuous,
    /// A fault invalidated all state; explicit rebootstrap is required.
    Stale,
}

/// Snapshot-derived coverage, never a claim that a truncated book is complete.
#[derive(Clone, Copy, Debug)]
pub struct BackpackBookCoverage {
    /// Explicit REST snapshot limit requested per side.
    pub snapshot_limit: usize,
    /// Worst known initial bid boundary, retained even after that level is deleted.
    pub initial_bid_floor: Option<Price>,
    /// Worst known initial ask boundary, retained even after that level is deleted.
    pub initial_ask_ceiling: Option<Price>,
}

/// Official public depth snapshot shape, without an inferred response symbol.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpackDepthSnapshot {
    /// Absolute ask levels as decimal-string pairs.
    pub asks: Vec<[String; 2]>,
    /// Absolute bid levels as decimal-string pairs.
    pub bids: Vec<[String; 2]>,
    /// Exact decimal-string venue sequence ID.
    pub last_update_id: String,
    /// Venue snapshot Unix microsecond timestamp.
    pub timestamp: u64,
}

/// A protocol-only order book with bounded messages, buffered updates, and stored levels.
#[derive(Debug)]
pub struct BackpackDepthSynchronizer {
    metadata: BackpackInstrumentMetadata,
    generation: u64,
    snapshot_limit: usize,
    max_buffer_frames: usize,
    max_levels_per_side: usize,
    state: BackpackBookState,
    buffered: VecDeque<BackpackDepthUpdate>,
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
    last: Option<u64>,
    coverage: Option<BackpackBookCoverage>,
}

impl BackpackDepthSynchronizer {
    /// Creates a bounded buffering synchronizer for one validated instrument/generation.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported explicit snapshot limit, a zero buffer bound,
    /// or a per-side storage bound smaller than the requested limit.
    pub fn new_checked(
        metadata: BackpackInstrumentMetadata,
        generation: u64,
        snapshot_limit: usize,
        max_buffer_frames: usize,
        max_levels_per_side: usize,
    ) -> Result<Self, BackpackPublicError> {
        if ![5, 10, 20, 50, 100, 500, 1000].contains(&snapshot_limit)
            || max_buffer_frames == 0
            || max_levels_per_side < snapshot_limit
        {
            return Err(BackpackPublicError::Bound);
        }

        Ok(Self {
            metadata,
            generation,
            snapshot_limit,
            max_buffer_frames,
            max_levels_per_side,
            state: BackpackBookState::Buffering,
            buffered: VecDeque::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last: None,
            coverage: None,
        })
    }

    /// Starts a new snapshot attempt, clearing all levels and buffered events.
    ///
    /// Recovery may reuse a WebSocket connection, but it requires a new parser and
    /// generation token. The caller must cancel old snapshot attempts. Generation tokens
    /// prevent late snapshot results or frames from crossing bootstrap attempts.
    ///
    /// # Errors
    ///
    /// Returns an error unless the new generation is strictly greater than the previous one.
    pub fn restart(&mut self, generation: u64) -> Result<(), BackpackPublicError> {
        if generation <= self.generation {
            return Err(BackpackPublicError::Generation);
        }

        self.invalidate();
        self.generation = generation;
        self.state = BackpackBookState::Buffering;
        Ok(())
    }

    /// Invalidates book usability before the transport owner attempts recovery.
    pub fn invalidate(&mut self) {
        self.state = BackpackBookState::Stale;
        self.buffered.clear();
        self.bids.clear();
        self.asks.clear();
        self.last = None;
        self.coverage = None;
    }

    /// Returns the explicit current state.
    #[must_use]
    pub const fn state(&self) -> BackpackBookState {
        self.state
    }

    /// Returns whether the current bounded view has proven sequence continuity.
    ///
    /// This does not establish wall-clock freshness, complete depth, or executable BBO.
    #[must_use]
    pub const fn is_continuous(&self) -> bool {
        matches!(self.state, BackpackBookState::Continuous)
    }

    /// Returns initial truncated coverage only for a continuous view.
    #[must_use]
    pub const fn coverage(&self) -> Option<BackpackBookCoverage> {
        self.coverage
    }

    /// Returns the best bid only when it remains inside the initial covered price band.
    #[must_use]
    pub fn best_covered_bid(&self) -> Option<(Price, Quantity)> {
        let floor = self.coverage?.initial_bid_floor?;
        self.bids
            .last_key_value()
            .filter(|(price, _)| **price >= floor)
            .map(|(p, q)| (*p, *q))
    }

    /// Returns the best ask only when it remains inside the initial covered price band.
    #[must_use]
    pub fn best_covered_ask(&self) -> Option<(Price, Quantity)> {
        let ceiling = self.coverage?.initial_ask_ceiling?;
        self.asks
            .first_key_value()
            .filter(|(price, _)| **price <= ceiling)
            .map(|(p, q)| (*p, *q))
    }

    /// Buffers before snapshot, or atomically applies a strictly contiguous live update.
    ///
    /// # Errors
    ///
    /// Returns an error for stale state, another generation/instrument, a gap/partial
    /// live overlap, a crossed resulting book, or an exceeded buffer/storage bound.
    /// Every such failure invalidates state before returning.
    pub fn apply(
        &mut self,
        update: BackpackDepthUpdate,
    ) -> Result<Option<OrderBookDeltas>, BackpackPublicError> {
        let result = self.apply_checked(update);

        if result.is_err() {
            self.invalidate();
        }

        result
    }

    /// Decodes and installs one bounded REST snapshot for the current bootstrap token.
    ///
    /// Snapshot identity comes from the public request routing; the REST schema has no
    /// symbol. Initial buffered overlap is an explicit inference: the first surviving
    /// range must contain `snapshot_id+1`; all later ranges require exact continuity.
    /// Snapshot plus replay is emitted as one CLEAR/rebuild batch only after validation.
    ///
    /// # Errors
    ///
    /// Returns an error and invalidates state for a mismatched generation, malformed or
    /// oversized response, wrong snapshot limit, invalid levels, crossed book, or replay gap.
    pub fn install_snapshot(
        &mut self,
        generation: u64,
        body: &[u8],
        received_at: UnixNanos,
    ) -> Result<OrderBookDeltas, BackpackPublicError> {
        let result = self.install_checked(generation, body, received_at);

        if result.is_err() {
            self.invalidate();
        }

        result
    }

    fn apply_checked(
        &mut self,
        update: BackpackDepthUpdate,
    ) -> Result<Option<OrderBookDeltas>, BackpackPublicError> {
        if update.generation != self.generation
            || update.instrument_id != self.metadata.instrument_id
        {
            return Err(BackpackPublicError::Generation);
        }

        match self.state {
            BackpackBookState::Stale => Err(BackpackPublicError::Stale),
            BackpackBookState::Buffering => {
                if self.buffered.len() >= self.max_buffer_frames {
                    return Err(BackpackPublicError::Bound);
                }

                self.buffered.push_back(update);
                Ok(None)
            }
            BackpackBookState::Continuous => {
                let previous = self.last.ok_or(BackpackPublicError::Stale)?;

                if update.last <= previous {
                    return Ok(None);
                }

                if previous.checked_add(1) != Some(update.first) {
                    return Err(BackpackPublicError::Gap);
                }

                let mut bids = self.bids.clone();
                let mut asks = self.asks.clone();
                apply_levels(&mut bids, &update.bids);
                apply_levels(&mut asks, &update.asks);
                self.validate_book(&bids, &asks)?;
                let batch = self.update_batch(&update)?;
                self.bids = bids;
                self.asks = asks;
                self.last = Some(update.last);
                Ok(batch)
            }
        }
    }

    fn install_checked(
        &mut self,
        generation: u64,
        body: &[u8],
        received_at: UnixNanos,
    ) -> Result<OrderBookDeltas, BackpackPublicError> {
        if generation != self.generation {
            return Err(BackpackPublicError::Generation);
        }

        if self.state != BackpackBookState::Buffering {
            return Err(BackpackPublicError::Stale);
        }

        if body.len() > MAX_PUBLIC_FRAME_BYTES {
            return Err(BackpackPublicError::Bound);
        }

        let snapshot: BackpackDepthSnapshot = serde_json::from_slice(body)?;

        if snapshot.bids.len() > self.snapshot_limit || snapshot.asks.len() > self.snapshot_limit {
            return Err(BackpackPublicError::Bound);
        }

        let snapshot_id = parse_sequence(&snapshot.last_update_id)?;
        let mut last = snapshot_id;
        let mut event_time = unix_microseconds_to_nanos(snapshot.timestamp)?;
        let mut init_time = received_at;
        let mut bids: BTreeMap<_, _> = parse_levels(&snapshot.bids, &self.metadata, false)?
            .into_iter()
            .collect();
        let mut asks: BTreeMap<_, _> = parse_levels(&snapshot.asks, &self.metadata, false)?
            .into_iter()
            .collect();
        self.validate_book(&bids, &asks)?;
        let coverage = BackpackBookCoverage {
            snapshot_limit: self.snapshot_limit,
            initial_bid_floor: bids.first_key_value().map(|(price, _)| *price),
            initial_ask_ceiling: asks.last_key_value().map(|(price, _)| *price),
        };
        let mut first = true;

        for update in &self.buffered {
            if update.last <= last {
                continue;
            }

            let next = last.checked_add(1).ok_or(BackpackPublicError::Gap)?;

            if (first && !(update.first <= next && next <= update.last))
                || (!first && update.first != next)
            {
                return Err(BackpackPublicError::Gap);
            }

            apply_levels(&mut bids, &update.bids);
            apply_levels(&mut asks, &update.asks);
            self.validate_book(&bids, &asks)?;
            last = update.last;
            event_time = update.ts_event;
            init_time = update.ts_init;
            first = false;
        }

        let id = self.metadata.instrument_id;
        let mut deltas = vec![OrderBookDelta::clear(id, last, event_time, init_time)];

        for (side, levels) in [(OrderSide::Buy, &bids), (OrderSide::Sell, &asks)] {
            let mut sorted: Vec<_> = levels.iter().collect();

            if side == OrderSide::Buy {
                sorted.reverse();
            }

            for (price, size) in sorted {
                deltas.push(
                    OrderBookDelta::new_checked(
                        id,
                        BookAction::Add,
                        BookOrder::new(side, *price, *size, 0),
                        RecordFlag::F_SNAPSHOT as u8,
                        last,
                        event_time,
                        init_time,
                    )
                    .map_err(|e| BackpackPublicError::Domain(e.to_string()))?,
                );
            }
        }

        let batch = finish_batch(id, deltas)?;
        self.bids = bids;
        self.asks = asks;
        self.last = Some(last);
        self.coverage = Some(coverage);
        self.buffered.clear();
        self.state = BackpackBookState::Continuous;
        Ok(batch)
    }

    fn validate_book(
        &self,
        bids: &BTreeMap<Price, Quantity>,
        asks: &BTreeMap<Price, Quantity>,
    ) -> Result<(), BackpackPublicError> {
        if bids.len() > self.max_levels_per_side || asks.len() > self.max_levels_per_side {
            return Err(BackpackPublicError::Bound);
        }

        if let (Some((bid, _)), Some((ask, _))) = (bids.last_key_value(), asks.first_key_value())
            && bid >= ask
        {
            return Err(BackpackPublicError::Field("crossed or locked book"));
        }

        Ok(())
    }

    fn update_batch(
        &self,
        update: &BackpackDepthUpdate,
    ) -> Result<Option<OrderBookDeltas>, BackpackPublicError> {
        let mut deltas = Vec::with_capacity(update.bids.len() + update.asks.len());

        for (side, levels) in [
            (OrderSide::Buy, &update.bids),
            (OrderSide::Sell, &update.asks),
        ] {
            for (price, size) in levels {
                deltas.push(
                    OrderBookDelta::new_checked(
                        self.metadata.instrument_id,
                        if size.is_zero() {
                            BookAction::Delete
                        } else {
                            BookAction::Update
                        },
                        BookOrder::new(side, *price, *size, 0),
                        0,
                        update.last,
                        update.ts_event,
                        update.ts_init,
                    )
                    .map_err(|e| BackpackPublicError::Domain(e.to_string()))?,
                );
            }
        }

        if deltas.is_empty() {
            return Ok(None);
        }

        finish_batch(self.metadata.instrument_id, deltas).map(Some)
    }
}

fn apply_levels(book: &mut BTreeMap<Price, Quantity>, levels: &[(Price, Quantity)]) {
    for (price, size) in levels {
        if size.is_zero() {
            book.remove(price);
        } else {
            book.insert(*price, *size);
        }
    }
}

fn finish_batch(
    id: InstrumentId,
    mut deltas: Vec<OrderBookDelta>,
) -> Result<OrderBookDeltas, BackpackPublicError> {
    let last = deltas
        .last_mut()
        .ok_or(BackpackPublicError::Field("empty book batch"))?;
    last.flags |= RecordFlag::F_LAST as u8;
    OrderBookDeltas::new_checked(id, deltas).map_err(|e| BackpackPublicError::Domain(e.to_string()))
}
