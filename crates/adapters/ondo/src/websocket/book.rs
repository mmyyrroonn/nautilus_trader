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

//! The book state machine for the Ondo Perps `depthBooksPerps` channel.
//!
//! The venue streams a `BookSnapshot` with no exchange sequence number and no checksum, and each
//! frame **fully replaces the range it covers**. That gives the state machine three jobs, and no
//! others:
//!
//! 1. **Replace, never merge.** A snapshot is the whole covered range, so accepting one clears the
//!    local cache and installs the frame's levels. A level that the venue did not send does not
//!    linger, a one-sided snapshot replaces both sides, and an empty snapshot leaves the cache
//!    empty and the book unusable (an empty book cannot be quoted against).
//! 2. **Order by the item's own event time.** An older `item.time` must never overwrite newer
//!    state. The same `item.time` with identical content is a duplicate and is suppressed; the same
//!    `item.time` with different content is processed in receive order and counted as a conflict,
//!    because equal timestamps on this venue do not prove equal content.
//! 3. **Stay per session.** A reconnect makes the old book immediately unusable, and a callback
//!    from the previous session must not mutate the new connection's state. `session_id` and
//!    `recv_seq` are local bookkeeping; they are never presented as an exchange sequence (the
//!    Nautilus batch uses [`crate::websocket::parse::NO_EXCHANGE_SEQUENCE`], the "no sequence"
//!    convention, because the venue supplies none).

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::OrderBookDeltas,
    enums::{BookAction, OrderSide},
};
use rust_decimal::Decimal;

use crate::websocket::parse::{ParsedBookSnapshot, WireBookSnapshot};

/// The local price-level cache of one instrument's covered book range.
///
/// Levels are keyed by the converted price, so the cache holds exactly what is published to the
/// engine. The cache is bounded by the venue's `limit` per side and is never used as an exchange
/// book: it exists so the adapter can report the best levels it has actually covered and can
/// project an on-demand depth10 from the same subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderBookLevels {
    bids: std::collections::BTreeMap<Decimal, Decimal>,
    asks: std::collections::BTreeMap<Decimal, Decimal>,
}

impl OrderBookLevels {
    /// Creates an empty book cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes every level from both sides.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Applies one delta batch, in order.
    ///
    /// A `Clear` delta empties both sides; an `Add` or `Update` inserts a level; a `Delete` removes
    /// it. Applying the adapter's own batch is how the cache is kept equal to what the engine
    /// receives, so there is no second book implementation to drift from the published one.
    ///
    /// # Errors
    ///
    /// Returns an error if a delta carries a non-positive price, which cannot be represented as a
    /// key.
    pub fn apply(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        for delta in &deltas.deltas {
            match delta.action {
                BookAction::Clear => self.clear(),
                BookAction::Add | BookAction::Update => {
                    let price = delta.order.price.as_decimal();
                    let size = delta.order.size.as_decimal();
                    anyhow::ensure!(
                        size.is_sign_positive() && !size.is_zero(),
                        "delta for `{}` carries a non-positive size",
                        delta.instrument_id,
                    );

                    if delta.order.side == Some(OrderSide::Buy) {
                        self.bids.insert(price, size);
                    } else {
                        self.asks.insert(price, size);
                    }
                }
                BookAction::Delete => {
                    let price = delta.order.price.as_decimal();
                    if delta.order.side == Some(OrderSide::Buy) {
                        self.bids.remove(&price);
                    } else {
                        self.asks.remove(&price);
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the best bid as `(price, size)`, when one side is covered.
    #[must_use]
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    /// Returns the best ask as `(price, size)`, when one side is covered.
    #[must_use]
    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    /// Returns the number of covered bid and ask levels.
    #[must_use]
    pub fn level_counts(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Returns `true` when neither side carries a level.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

/// What the state machine decided about one incoming snapshot frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// The snapshot was installed and must be published. `conflict` is set when the frame carried
    /// the same `item.time` as the state but different content, which is processed in receive order
    /// and counted rather than dropped.
    Accepted {
        /// Whether the frame repeated an already-seen `item.time` with different content.
        conflict: bool,
    },
    /// The frame repeated an already-seen `item.time` with identical content, so publishing it
    /// would duplicate state. The raw frame is still recorded by the transport.
    DuplicateSuppressed,
    /// The frame's `item.time` is older than the state's, so it must not overwrite newer state.
    StaleRejected {
        /// The event time the state currently holds.
        state: UnixNanos,
        /// The event time the rejected frame carried.
        incoming: UnixNanos,
    },
    /// The frame belongs to a connection this state no longer serves, so it must not mutate it.
    ForeignSession {
        /// The session this state serves.
        expected: u64,
        /// The session the frame was received on.
        received: u64,
    },
}

/// Per-book counters, for diagnostics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BookCounters {
    /// Snapshots installed and published.
    pub accepted: u64,
    /// Frames suppressed as identical repeats.
    pub duplicates: u64,
    /// Accepted frames that repeated an event time with different content.
    pub conflicts: u64,
    /// Frames rejected for carrying an older event time.
    pub stale_rejected: u64,
    /// Frames rejected for belonging to another session.
    pub foreign_session: u64,
    /// Levels whose conversion reduced scale and therefore rounded explicitly.
    pub rounded_levels: u64,
}

/// The book state of one instrument on one connection session.
#[derive(Clone, Debug)]
pub struct OndoBookState {
    session_id: u64,
    recv_seq: u64,
    coverage_limit: Option<u32>,
    book: OrderBookLevels,
    wire: Option<WireBookSnapshot>,
    valid: bool,
    last_event_ns: Option<UnixNanos>,
    invalid_reason: Option<String>,
    counters: BookCounters,
}

impl OndoBookState {
    /// Creates a state for one session.
    ///
    /// `session_id` is a local counter that identifies the connection the state belongs to; it is
    /// never sent anywhere and never presented as an exchange sequence.
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            recv_seq: 0,
            coverage_limit: None,
            book: OrderBookLevels::new(),
            wire: None,
            valid: false,
            last_event_ns: None,
            invalid_reason: None,
            counters: BookCounters::default(),
        }
    }

    /// Returns the local session this state serves.
    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Returns the local count of frames this state has seen on its session.
    ///
    /// This is receive order, not an exchange sequence.
    #[must_use]
    pub const fn recv_seq(&self) -> u64 {
        self.recv_seq
    }

    /// Returns the maximum number of levels the subscription asked for, when one was asked for.
    ///
    /// This is a maximum, never a guarantee: `limit=100` means "at most 100 levels", not "the whole
    /// book".
    #[must_use]
    pub const fn coverage_limit(&self) -> Option<u32> {
        self.coverage_limit
    }

    /// Records the level limit the book subscription used.
    pub fn set_coverage_limit(&mut self, limit: Option<u32>) {
        self.coverage_limit = limit;
    }

    /// Returns `true` when the state holds a current-session book that is usable.
    ///
    /// A book is unusable while no snapshot has been accepted, after a disconnect, and when the
    /// latest accepted snapshot carried no levels at all.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.valid
    }

    /// Returns why the book is currently unusable.
    #[must_use]
    pub fn invalid_reason(&self) -> Option<&str> {
        self.invalid_reason.as_deref()
    }

    /// Returns the event time of the last accepted snapshot.
    #[must_use]
    pub const fn last_event_ns(&self) -> Option<UnixNanos> {
        self.last_event_ns
    }

    /// Returns the covered levels of the current book.
    #[must_use]
    pub fn book(&self) -> &OrderBookLevels {
        &self.book
    }

    /// Returns the wire levels of the last accepted snapshot.
    ///
    /// This is the same covered range the Nautilus book holds; an on-demand depth10 is projected
    /// from it rather than from a second subscription.
    #[must_use]
    pub fn wire(&self) -> Option<&WireBookSnapshot> {
        self.wire.as_ref()
    }

    /// Returns the per-book counters.
    #[must_use]
    pub const fn counters(&self) -> BookCounters {
        self.counters
    }

    /// Applies one incoming snapshot frame.
    ///
    /// `frame_session` is the session the frame was received on. A frame from another session is
    /// rejected without touching any state, so a late callback from a torn-down connection cannot
    /// restore an old book.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be applied to the cache.
    pub fn apply_snapshot(
        &mut self,
        frame_session: u64,
        event_ns: UnixNanos,
        parsed: &ParsedBookSnapshot,
    ) -> anyhow::Result<SnapshotOutcome> {
        if frame_session != self.session_id {
            self.counters.foreign_session += 1;

            return Ok(SnapshotOutcome::ForeignSession {
                expected: self.session_id,
                received: frame_session,
            });
        }

        self.recv_seq += 1;

        if let Some(state_ns) = self.last_event_ns {
            if event_ns < state_ns {
                self.counters.stale_rejected += 1;

                return Ok(SnapshotOutcome::StaleRejected {
                    state: state_ns,
                    incoming: event_ns,
                });
            }

            if event_ns == state_ns && self.wire.as_ref() == Some(&parsed.wire) {
                self.counters.duplicates += 1;

                return Ok(SnapshotOutcome::DuplicateSuppressed);
            }
        }

        let conflict = self.last_event_ns == Some(event_ns);

        self.book.clear();
        self.book.apply(&parsed.deltas)?;

        // An empty snapshot clears both sides and leaves nothing to quote against, so the book
        // stays unusable until a snapshot with levels arrives.
        self.valid = !self.book.is_empty();
        self.invalid_reason = if self.valid {
            None
        } else {
            Some("snapshot carried no levels".to_string())
        };
        self.wire = Some(parsed.wire.clone());
        self.last_event_ns = Some(event_ns);
        self.counters.accepted += 1;
        self.counters.rounded_levels += parsed.rounded_levels as u64;
        if conflict {
            self.counters.conflicts += 1;
        }

        Ok(SnapshotOutcome::Accepted { conflict })
    }

    /// Invalidates the book without changing the connection session it serves.
    ///
    /// This is what an instrument update does when the venue's metadata changes what a frame
    /// decodes to: the levels this state holds were converted at the previous precision or
    /// increment, and keeping them beside levels converted at the new one would be a book of two
    /// versions. The state is emptied instead, and the subscription's next snapshot - a full
    /// replacement, as every `depthBooksPerps` frame is - rebuilds it. The event time is cleared
    /// with it, so that snapshot is accepted whatever time it carries, and the invalid -> valid
    /// transition publishes `adapter:snapshot_ready` again once it lands.
    ///
    /// Returns `true` when the state changed (a book was held, or one had already been accepted).
    pub fn invalidate(&mut self, reason: &str) -> bool {
        self.invalidate_to(self.session_id, reason)
    }

    /// Invalidates the book and moves it to the connection session `session_id`.
    ///
    /// This is what a disconnect does: the old book becomes immediately unusable, every frame from
    /// the previous connection becomes a [`SnapshotOutcome::ForeignSession`] because it carries the
    /// session it was received on, and the book is restored only by a snapshot accepted on the new
    /// session. The session counter belongs to the connection, not to this book, so the caller
    /// supplies it and every book on one connection moves together.
    ///
    /// Returns `true` when the state changed (a book was held, or a session was already open).
    pub fn invalidate_to(&mut self, session_id: u64, reason: &str) -> bool {
        let changed = self.valid || self.wire.is_some() || self.last_event_ns.is_some();

        self.session_id = session_id;
        self.recv_seq = 0;
        self.book.clear();
        self.wire = None;
        self.valid = false;
        self.last_event_ns = None;
        self.invalid_reason = Some(reason.to_string());

        changed
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{enums::RecordFlag, instruments::InstrumentAny};
    use rstest::rstest;

    use super::*;
    use crate::{
        common::parse::{market_to_instrument_id, parse_timestamp},
        http::models::parse_instruments,
        websocket::{messages::BookSnapshotItem, parse::parse_book_snapshot},
    };

    const DEPTH_FIXTURE: &str = include_str!("../../test_data/ws/depth_observed.json");

    fn instrument(market: &str) -> InstrumentAny {
        let body = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/rest/markets_synthetic.json"
        ))
        .unwrap();

        parse_instruments(
            &body,
            &[market_to_instrument_id(market).unwrap()],
            UnixNanos::from(1),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    }

    fn parsed(
        instrument: &InstrumentAny,
        time: &str,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> ParsedBookSnapshot {
        let item = BookSnapshotItem {
            market: "NVDA-USD.P".to_string(),
            time: Some(time.to_string()),
            bids: bids
                .iter()
                .map(|(p, s)| vec![p.to_string(), s.to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| vec![p.to_string(), s.to_string()])
                .collect(),
            depth_levels: None,
        };
        let ts_event = parse_timestamp(time).unwrap();

        parse_book_snapshot(&item, instrument, ts_event, UnixNanos::from(1)).unwrap()
    }

    #[rstest]
    fn test_snapshot_replaces_the_whole_covered_range() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);

        let a = parsed(
            &instrument,
            "2026-09-14T11:09:00Z",
            &[("100", "1"), ("99", "2")],
            &[("101", "1")],
        );
        let outcome = state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:00Z").unwrap(), &a)
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::Accepted { conflict: false });
        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "100.00");
        assert_eq!(state.book().best_ask().unwrap().0.to_string(), "101.00");

        let b = parsed(
            &instrument,
            "2026-09-14T11:09:01Z",
            &[("98", "3")],
            &[("102", "4")],
        );
        state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:01Z").unwrap(), &b)
            .unwrap();

        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "98.00");
        assert_eq!(state.book().best_ask().unwrap().0.to_string(), "102.00");
        assert_eq!(state.book().level_counts(), (1, 1));
        assert!(state.is_valid());
    }

    #[rstest]
    fn test_an_empty_snapshot_clears_both_sides_and_the_book() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let a = parsed(
            &instrument,
            "2026-09-14T11:09:00Z",
            &[("100", "1")],
            &[("101", "1")],
        );
        state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:00Z").unwrap(), &a)
            .unwrap();
        assert!(state.is_valid());

        let empty = parsed(&instrument, "2026-09-14T11:09:02Z", &[], &[]);
        let outcome = state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:02Z").unwrap(), &empty)
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::Accepted { conflict: false });
        assert!(state.book().is_empty());
        assert!(!state.is_valid());
        assert_eq!(state.invalid_reason(), Some("snapshot carried no levels"));
        assert_eq!(empty.deltas.deltas.len(), 1);
        assert_eq!(
            empty.deltas.deltas[0].flags,
            (RecordFlag::F_SNAPSHOT as u8) | (RecordFlag::F_LAST as u8)
        );
    }

    #[rstest]
    fn test_a_one_sided_snapshot_replaces_both_sides() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let both = parsed(
            &instrument,
            "2026-09-14T11:09:00Z",
            &[("100", "1")],
            &[("101", "1")],
        );
        state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:00Z").unwrap(), &both)
            .unwrap();

        let bids_only = parsed(&instrument, "2026-09-14T11:09:01Z", &[("99", "2")], &[]);
        state
            .apply_snapshot(
                1,
                parse_timestamp("2026-09-14T11:09:01Z").unwrap(),
                &bids_only,
            )
            .unwrap();

        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "99.00");
        assert!(
            state.book().best_ask().is_none(),
            "the absent side must not linger"
        );
        assert_eq!(state.book().level_counts(), (1, 0));
    }

    #[rstest]
    fn test_an_older_event_time_cannot_overwrite_newer_state() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let newer = parsed(
            &instrument,
            "2026-09-14T11:09:05Z",
            &[("100", "1")],
            &[("101", "1")],
        );
        state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:05Z").unwrap(), &newer)
            .unwrap();

        let older = parsed(
            &instrument,
            "2026-09-14T11:09:04Z",
            &[("50", "1")],
            &[("51", "1")],
        );
        let outcome = state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:04Z").unwrap(), &older)
            .unwrap();

        assert_eq!(
            outcome,
            SnapshotOutcome::StaleRejected {
                state: parse_timestamp("2026-09-14T11:09:05Z").unwrap(),
                incoming: parse_timestamp("2026-09-14T11:09:04Z").unwrap(),
            }
        );
        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "100.00");
        assert_eq!(state.counters().stale_rejected, 1);
    }

    #[rstest]
    fn test_equal_time_with_identical_content_is_a_suppressed_duplicate() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let time = "2026-09-14T11:09:00Z";
        let a = parsed(&instrument, time, &[("100", "1")], &[("101", "1")]);
        state
            .apply_snapshot(1, parse_timestamp(time).unwrap(), &a)
            .unwrap();

        let repeat = parsed(&instrument, time, &[("100", "1")], &[("101", "1")]);
        let outcome = state
            .apply_snapshot(1, parse_timestamp(time).unwrap(), &repeat)
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::DuplicateSuppressed);
        assert_eq!(state.counters().duplicates, 1);
        assert_eq!(state.counters().accepted, 1);
        assert_eq!(state.recv_seq(), 2, "the frame was still received in order");
    }

    #[rstest]
    fn test_equal_time_with_different_content_is_processed_and_counted_as_a_conflict() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let time = "2026-09-14T11:09:00Z";
        let a = parsed(&instrument, time, &[("100", "1")], &[("101", "1")]);
        state
            .apply_snapshot(1, parse_timestamp(time).unwrap(), &a)
            .unwrap();

        let different = parsed(&instrument, time, &[("99", "1")], &[("102", "1")]);
        let outcome = state
            .apply_snapshot(1, parse_timestamp(time).unwrap(), &different)
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::Accepted { conflict: true });
        assert_eq!(state.counters().conflicts, 1);
        assert_eq!(
            state.book().best_bid().unwrap().0.to_string(),
            "99.00",
            "the later frame is applied in receive order"
        );
    }

    #[rstest]
    fn test_a_callback_from_the_old_session_cannot_mutate_the_new_connection_state() {
        let instrument = instrument("NVDA-USD.P");
        let mut state = OndoBookState::new(1);
        let a = parsed(
            &instrument,
            "2026-09-14T11:09:00Z",
            &[("100", "1")],
            &[("101", "1")],
        );
        state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:00Z").unwrap(), &a)
            .unwrap();

        assert!(state.invalidate_to(2, "adapter:disconnected"));
        assert!(!state.is_valid());
        assert_eq!(state.session_id(), 2);
        assert_eq!(state.invalid_reason(), Some("adapter:disconnected"));

        let late = parsed(
            &instrument,
            "2026-09-14T11:09:30Z",
            &[("500", "1")],
            &[("501", "1")],
        );
        let outcome = state
            .apply_snapshot(1, parse_timestamp("2026-09-14T11:09:30Z").unwrap(), &late)
            .unwrap();

        assert_eq!(
            outcome,
            SnapshotOutcome::ForeignSession {
                expected: 2,
                received: 1
            }
        );
        assert!(!state.is_valid());
        assert!(state.book().is_empty());
        assert_eq!(state.counters().foreign_session, 1);

        let recovered = parsed(
            &instrument,
            "2026-09-14T11:10:00Z",
            &[("98", "3")],
            &[("102", "4")],
        );
        let outcome = state
            .apply_snapshot(
                2,
                parse_timestamp("2026-09-14T11:10:00Z").unwrap(),
                &recovered,
            )
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::Accepted { conflict: false });
        assert!(state.is_valid());
        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "98.00");
    }

    #[rstest]
    fn test_the_observed_depth_fixture_is_covered_and_published_at_declared_precision() {
        let instrument = instrument("NVDA-USD.P");
        let message = crate::websocket::parse::parse_server_message(DEPTH_FIXTURE).unwrap();
        let channel =
            crate::websocket::messages::WsChannel::from_wire(message.channel.as_deref().unwrap())
                .unwrap();
        let updates =
            crate::websocket::parse::decode_updates(channel, message.data.as_ref().unwrap())
                .unwrap();

        assert_eq!(updates.len(), 1);

        let crate::websocket::parse::WsUpdate::Book(item) = &updates[0] else {
            panic!("the depth channel decodes to book items");
        };

        let item_time = parse_timestamp(item.time.as_deref().unwrap()).unwrap();
        let parsed = parse_book_snapshot(item, &instrument, item_time, UnixNanos::from(1)).unwrap();
        let mut state = OndoBookState::new(1);
        state.apply_snapshot(1, item_time, &parsed).unwrap();

        assert_eq!(state.book().level_counts(), (10, 10));
        assert_eq!(state.book().best_bid().unwrap().0.to_string(), "212.22");
        assert_eq!(state.book().best_ask().unwrap().0.to_string(), "212.25");
        assert_eq!(
            parsed.rounded_levels, 0,
            "the fixture is exactly representable"
        );
        assert!(state.counters().rounded_levels == 0);
    }
}
