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

use std::collections::{BTreeMap, VecDeque};

use ahash::AHashMap;
use nautilus_common::messages::DataEvent;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    instruments::Instrument,
    types::Price,
};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug)]
enum QueueSlot {
    Event(DataEvent),
    Book(InstrumentId, u64),
}

#[derive(Debug)]
enum BookSlotKind {
    Snapshot {
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        bids: BTreeMap<Price, OrderBookDelta>,
        asks: BTreeMap<Price, OrderBookDelta>,
    },
    Incremental {
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        bids: BTreeMap<Price, OrderBookDelta>,
        asks: BTreeMap<Price, OrderBookDelta>,
    },
}

#[derive(Debug)]
struct ActiveBookSlot {
    generation: u64,
    kind: BookSlotKind,
}

fn apply_snapshot_deltas(
    bids: &mut BTreeMap<Price, OrderBookDelta>,
    asks: &mut BTreeMap<Price, OrderBookDelta>,
    deltas: &[OrderBookDelta],
    instrument_id: InstrumentId,
) {
    for &delta in deltas {
        let Some(side) = delta.order.side else {
            continue;
        };
        let side_map = match side {
            OrderSide::Buy => &mut *bids,
            OrderSide::Sell => &mut *asks,
        };
        if delta.action == BookAction::Delete || delta.order.size.is_zero() {
            side_map.remove(&delta.order.price);
        } else {
            side_map.insert(
                delta.order.price,
                OrderBookDelta::new(
                    instrument_id,
                    BookAction::Add,
                    delta.order,
                    RecordFlag::F_SNAPSHOT as u8,
                    delta.sequence,
                    delta.ts_event,
                    delta.ts_init,
                ),
            );
        }
    }
}

fn apply_incremental_deltas(
    bids: &mut BTreeMap<Price, OrderBookDelta>,
    asks: &mut BTreeMap<Price, OrderBookDelta>,
    deltas: Vec<OrderBookDelta>,
) {
    for mut delta in deltas {
        delta.flags &= !(RecordFlag::F_LAST as u8);
        match delta.order.side {
            Some(OrderSide::Buy) => {
                bids.insert(delta.order.price, delta);
            }
            Some(OrderSide::Sell) => {
                asks.insert(delta.order.price, delta);
            }
            None => {}
        }
    }
}

impl BookSlotKind {
    fn from_snapshot(
        deltas: &[OrderBookDelta],
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        instrument_id: InstrumentId,
    ) -> Self {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        apply_snapshot_deltas(&mut bids, &mut asks, deltas, instrument_id);
        Self::Snapshot {
            sequence,
            ts_event,
            ts_init,
            bids,
            asks,
        }
    }

    fn from_incremental(deltas: OrderBookDeltas) -> Self {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        let sequence = deltas.sequence;
        let ts_event = deltas.ts_event;
        let ts_init = deltas.ts_init;
        apply_incremental_deltas(&mut bids, &mut asks, deltas.deltas);
        Self::Incremental {
            sequence,
            ts_event,
            ts_init,
            bids,
            asks,
        }
    }

    fn apply_incremental(&mut self, deltas: OrderBookDeltas, instrument_id: InstrumentId) {
        let next_sequence = deltas.sequence;
        let next_ts_event = deltas.ts_event;
        let next_ts_init = deltas.ts_init;
        match self {
            Self::Snapshot {
                sequence,
                ts_event,
                ts_init,
                bids,
                asks,
            } => {
                *sequence = next_sequence;
                *ts_event = next_ts_event;
                *ts_init = next_ts_init;
                apply_snapshot_deltas(bids, asks, &deltas.deltas, instrument_id);
            }
            Self::Incremental {
                sequence,
                ts_event,
                ts_init,
                bids,
                asks,
            } => {
                *sequence = next_sequence;
                *ts_event = next_ts_event;
                *ts_init = next_ts_init;
                apply_incremental_deltas(bids, asks, deltas.deltas);
            }
        }
    }

    fn into_data_event(self, instrument_id: InstrumentId) -> DataEvent {
        match self {
            Self::Snapshot {
                sequence,
                ts_event,
                ts_init,
                bids,
                asks,
            } => {
                let mut vec = Vec::with_capacity(1 + bids.len() + asks.len());
                vec.push(OrderBookDelta::clear(
                    instrument_id,
                    sequence,
                    ts_event,
                    ts_init,
                ));
                vec.extend(bids.into_values().rev());
                vec.extend(asks.into_values());
                if let Some(last) = vec.last_mut() {
                    last.flags |= RecordFlag::F_LAST as u8;
                    last.sequence = sequence;
                    last.ts_event = ts_event;
                    last.ts_init = ts_init;
                }
                DataEvent::Data(Data::Deltas(Box::new(OrderBookDeltas::new(
                    instrument_id,
                    vec,
                ))))
            }
            Self::Incremental {
                sequence,
                ts_event,
                ts_init,
                bids,
                asks,
            } => {
                let mut vec = Vec::with_capacity(bids.len() + asks.len());
                vec.extend(bids.into_values().rev());
                vec.extend(asks.into_values());
                if let Some(last) = vec.last_mut() {
                    last.flags |= RecordFlag::F_LAST as u8;
                    last.sequence = sequence;
                    last.ts_event = ts_event;
                    last.ts_init = ts_init;
                }
                DataEvent::Data(Data::Deltas(Box::new(OrderBookDeltas::new(
                    instrument_id,
                    vec,
                ))))
            }
        }
    }
}

/// Coalesces queued complete `OrderBookDeltas` per instrument without reordering barriers.
#[derive(Debug, Default)]
pub(crate) struct DataEventCoalescer {
    queue: VecDeque<QueueSlot>,
    books: AHashMap<InstrumentId, ActiveBookSlot>,
    sealed_books: AHashMap<(InstrumentId, u64), DataEvent>,
    next_generation: u64,
}

impl DataEventCoalescer {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub(crate) fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn drain_from(&mut self, rx: &mut UnboundedReceiver<DataEvent>, max_drain: usize) {
        for _ in 0..max_drain {
            match rx.try_recv() {
                Ok(event) => self.push(event),
                Err(_) => break,
            }
        }
    }

    pub(crate) fn push(&mut self, event: DataEvent) {
        match event {
            DataEvent::Instrument(instrument) => {
                self.seal_instrument(instrument.id());
                self.queue
                    .push_back(QueueSlot::Event(DataEvent::Instrument(instrument)));
            }
            DataEvent::InstrumentStatus(status) => {
                self.seal_instrument(status.instrument_id);
                self.queue
                    .push_back(QueueSlot::Event(DataEvent::InstrumentStatus(status)));
            }
            DataEvent::Data(Data::InstrumentStatus(status)) => {
                self.seal_instrument(status.instrument_id);
                self.queue
                    .push_back(QueueSlot::Event(DataEvent::Data(Data::InstrumentStatus(
                        status,
                    ))));
            }
            DataEvent::Data(Data::Delta(delta)) => {
                self.seal_instrument(delta.instrument_id);
                self.queue
                    .push_back(QueueSlot::Event(DataEvent::Data(Data::Delta(delta))));
            }
            DataEvent::Data(Data::Depth10(depth)) => {
                self.seal_instrument(depth.instrument_id);
                self.queue
                    .push_back(QueueSlot::Event(DataEvent::Data(Data::Depth10(depth))));
            }
            DataEvent::Data(Data::Deltas(deltas)) => self.push_deltas(*deltas),
            other => self.queue.push_back(QueueSlot::Event(other)),
        }
    }

    fn push_deltas(&mut self, deltas: OrderBookDeltas) {
        let instrument_id = deltas.instrument_id;
        let is_last = RecordFlag::F_LAST.matches(deltas.flags);
        let last_clear_idx = deltas
            .deltas
            .iter()
            .rposition(|d| d.action == BookAction::Clear);

        if is_last && let Some(clear_idx) = last_clear_idx {
            let tail = &deltas.deltas[clear_idx + 1..];
            if tail
                .iter()
                .all(|d| matches!(d.order.side, Some(OrderSide::Buy | OrderSide::Sell)))
            {
                let kind = BookSlotKind::from_snapshot(
                    tail,
                    deltas.sequence,
                    deltas.ts_event,
                    deltas.ts_init,
                    instrument_id,
                );
                if let Some(active) = self.books.get_mut(&instrument_id) {
                    active.kind = kind;
                } else {
                    let generation = self.next_generation;
                    self.next_generation = self.next_generation.wrapping_add(1);
                    self.books
                        .insert(instrument_id, ActiveBookSlot { generation, kind });
                    self.queue
                        .push_back(QueueSlot::Book(instrument_id, generation));
                }
                return;
            }
        }

        let has_snapshot = deltas
            .deltas
            .iter()
            .any(|d| RecordFlag::F_SNAPSHOT.matches(d.flags));
        let all_sided = deltas
            .deltas
            .iter()
            .all(|d| matches!(d.order.side, Some(OrderSide::Buy | OrderSide::Sell)));

        if is_last
            && last_clear_idx.is_none()
            && !has_snapshot
            && all_sided
            && !deltas.deltas.is_empty()
        {
            if let Some(active) = self.books.get_mut(&instrument_id) {
                active.kind.apply_incremental(deltas, instrument_id);
            } else {
                let kind = BookSlotKind::from_incremental(deltas);
                let generation = self.next_generation;
                self.next_generation = self.next_generation.wrapping_add(1);
                self.books
                    .insert(instrument_id, ActiveBookSlot { generation, kind });
                self.queue
                    .push_back(QueueSlot::Book(instrument_id, generation));
            }
            return;
        }

        self.seal_instrument(instrument_id);
        self.queue
            .push_back(QueueSlot::Event(DataEvent::Data(Data::Deltas(Box::new(
                deltas,
            )))));
    }

    fn seal_instrument(&mut self, instrument_id: InstrumentId) {
        if let Some(active) = self.books.remove(&instrument_id) {
            self.sealed_books.insert(
                (instrument_id, active.generation),
                active.kind.into_data_event(instrument_id),
            );
        }
    }

    pub(crate) fn pop_next(&mut self) -> Option<DataEvent> {
        while let Some(slot) = self.queue.pop_front() {
            match slot {
                QueueSlot::Event(event) => return Some(event),
                QueueSlot::Book(instrument_id, generation) => {
                    if let Some(event) = self.sealed_books.remove(&(instrument_id, generation)) {
                        return Some(event);
                    }
                    if let Some(active) = self.books.get(&instrument_id)
                        && active.generation == generation
                    {
                        let active = self.books.remove(&instrument_id).expect("checked active");
                        return Some(active.kind.into_data_event(instrument_id));
                    }
                }
            }
        }
        None
    }
}

pub(crate) async fn recv_data_event(
    data_evt_rx: &mut UnboundedReceiver<DataEvent>,
    coalescer: Option<&mut DataEventCoalescer>,
) -> Option<DataEvent> {
    match coalescer {
        None => data_evt_rx.recv().await,
        Some(coalescer) => {
            if coalescer.is_empty() {
                let first = data_evt_rx.recv().await?;
                if data_evt_rx.is_empty() {
                    return Some(first);
                }
                coalescer.push(first);
            }
            coalescer.drain_from(data_evt_rx, 4096);
            coalescer.pop_next()
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{BookOrder, InstrumentStatus, QuoteTick},
        enums::{BookType, MarketStatusAction},
        orderbook::OrderBook,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn inst_a() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.ASTER")
    }

    fn snap(
        id: InstrumentId,
        seq: u64,
        ts: u64,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> OrderBookDeltas {
        let mut deltas = vec![OrderBookDelta::clear(id, seq, ts.into(), ts.into())];
        for (px, qty) in bids {
            deltas.push(OrderBookDelta::new(
                id,
                BookAction::Add,
                BookOrder::new(OrderSide::Buy, Price::from(*px), Quantity::from(*qty), 0),
                RecordFlag::F_SNAPSHOT as u8,
                seq,
                ts.into(),
                ts.into(),
            ));
        }
        for (px, qty) in asks {
            deltas.push(OrderBookDelta::new(
                id,
                BookAction::Add,
                BookOrder::new(OrderSide::Sell, Price::from(*px), Quantity::from(*qty), 0),
                RecordFlag::F_SNAPSHOT as u8,
                seq,
                ts.into(),
                ts.into(),
            ));
        }
        if let Some(last) = deltas.last_mut() {
            last.flags |= RecordFlag::F_LAST as u8;
        }
        OrderBookDeltas::new(id, deltas)
    }

    fn inc(
        id: InstrumentId,
        seq: u64,
        ts: u64,
        updates: &[(BookAction, OrderSide, &str, &str)],
        is_last: bool,
    ) -> OrderBookDeltas {
        let mut deltas: Vec<_> = updates
            .iter()
            .map(|&(action, side, px, qty)| {
                OrderBookDelta::new(
                    id,
                    action,
                    BookOrder::new(side, Price::from(px), Quantity::from(qty), 0),
                    0,
                    seq,
                    ts.into(),
                    ts.into(),
                )
            })
            .collect();
        if is_last && let Some(last) = deltas.last_mut() {
            last.flags |= RecordFlag::F_LAST as u8;
        }
        OrderBookDeltas::new(id, deltas)
    }

    fn delta_event(deltas: OrderBookDeltas) -> DataEvent {
        DataEvent::Data(Data::Deltas(Box::new(deltas)))
    }

    fn apply_all(book: &mut OrderBook, events: &[OrderBookDeltas]) {
        for deltas in events {
            book.apply_deltas(deltas).unwrap();
        }
    }

    #[rstest]
    fn test_multiple_snapshots_collapse_to_latest() {
        let id = inst_a();
        let raw = vec![
            snap(id, 1, 100, &[("99.0", "1.0")], &[("100.0", "1.0")]),
            snap(id, 2, 200, &[("99.5", "2.0")], &[("100.5", "2.0")]),
            snap(id, 3, 300, &[("98.0", "5.0")], &[("101.0", "4.0")]),
        ];
        let mut expected = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut expected, &raw);

        let mut coalescer = DataEventCoalescer::new();
        for ev in raw {
            coalescer.push(delta_event(ev));
        }
        assert_eq!(coalescer.pending_len(), 1);
        let merged = match coalescer.pop_next().unwrap() {
            DataEvent::Data(Data::Deltas(d)) => *d,
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(coalescer.is_empty());

        let mut actual = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut actual, &[merged]);

        assert_eq!(actual.bids_as_map(None), expected.bids_as_map(None));
        assert_eq!(actual.asks_as_map(None), expected.asks_as_map(None));
        assert_eq!(actual.ts_last, expected.ts_last);
        assert_eq!(actual.sequence, expected.sequence);
    }

    #[rstest]
    fn test_incremental_updates_fold_exact_final_book() {
        let id = inst_a();
        let seed = snap(
            id,
            10,
            100,
            &[("100.0", "1.0"), ("99.0", "2.0")],
            &[("101.0", "1.0")],
        );
        let updates = vec![
            inc(
                id,
                11,
                110,
                &[
                    (BookAction::Update, OrderSide::Buy, "100.0", "3.0"),
                    (BookAction::Update, OrderSide::Buy, "98.5", "4.0"),
                ],
                true,
            ),
            inc(
                id,
                12,
                120,
                &[
                    (BookAction::Delete, OrderSide::Buy, "100.0", "0.0"),
                    (BookAction::Update, OrderSide::Sell, "101.5", "5.0"),
                ],
                true,
            ),
            inc(
                id,
                13,
                130,
                &[
                    (BookAction::Update, OrderSide::Buy, "98.5", "7.0"),
                    (BookAction::Delete, OrderSide::Sell, "101.0", "0.0"),
                ],
                true,
            ),
        ];

        let mut expected = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut expected, std::slice::from_ref(&seed));
        apply_all(&mut expected, &updates);

        let mut actual = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut actual, &[seed]);
        let mut coalescer = DataEventCoalescer::new();
        for ev in updates {
            coalescer.push(delta_event(ev));
        }
        assert_eq!(coalescer.pending_len(), 1);
        let merged = match coalescer.pop_next().unwrap() {
            DataEvent::Data(Data::Deltas(d)) => *d,
            other => panic!("unexpected event: {other:?}"),
        };
        apply_all(&mut actual, &[merged]);

        assert_eq!(actual.bids_as_map(None), expected.bids_as_map(None));
        assert_eq!(actual.asks_as_map(None), expected.asks_as_map(None));
        assert_eq!(actual.ts_last, expected.ts_last);
        assert_eq!(actual.sequence, expected.sequence);
    }

    #[rstest]
    fn test_snapshot_then_incrementals_fold_into_single_snapshot() {
        let id = inst_a();
        let events = vec![
            snap(
                id,
                1,
                100,
                &[("100.0", "1.0"), ("99.0", "2.0")],
                &[("101.0", "3.0")],
            ),
            inc(
                id,
                2,
                200,
                &[
                    (BookAction::Delete, OrderSide::Buy, "100.0", "0.0"),
                    (BookAction::Update, OrderSide::Buy, "99.5", "4.0"),
                    (BookAction::Update, OrderSide::Sell, "102.0", "5.0"),
                ],
                true,
            ),
        ];

        let mut expected = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut expected, &events);

        let mut coalescer = DataEventCoalescer::new();
        for ev in events {
            coalescer.push(delta_event(ev));
        }
        assert_eq!(coalescer.pending_len(), 1);
        let merged = coalescer.pop_next().unwrap();
        let DataEvent::Data(Data::Deltas(ref d)) = merged else {
            panic!("expected coalesced deltas");
        };
        assert_eq!(d.deltas[0].action, BookAction::Clear);

        let mut actual = OrderBook::new(id, BookType::L2_MBP);
        apply_all(&mut actual, &[*d.clone()]);
        assert_eq!(actual.bids_as_map(None), expected.bids_as_map(None));
        assert_eq!(actual.asks_as_map(None), expected.asks_as_map(None));
        assert_eq!(actual.ts_last, 200);
        assert_eq!(actual.sequence, 2);
    }

    #[rstest]
    fn test_status_and_partial_barriers_preserve_exact_order() {
        let id = inst_a();
        let status = DataEvent::InstrumentStatus(InstrumentStatus::new(
            id,
            MarketStatusAction::Trading,
            150.into(),
            150.into(),
            None,
            None,
            None,
            None,
            None,
        ));
        let quote = DataEvent::Data(Data::Quote(QuoteTick::default()));

        let mut coalescer = DataEventCoalescer::new();
        coalescer.push(delta_event(snap(
            id,
            1,
            100,
            &[("99.0", "1.0")],
            &[("100.0", "1.0")],
        )));
        coalescer.push(quote);
        coalescer.push(status);
        coalescer.push(delta_event(inc(
            id,
            2,
            200,
            &[(BookAction::Update, OrderSide::Buy, "98.0", "2.0")],
            false,
        )));
        coalescer.push(delta_event(inc(
            id,
            3,
            300,
            &[(BookAction::Update, OrderSide::Buy, "97.0", "3.0")],
            true,
        )));

        assert_eq!(coalescer.pending_len(), 5);
        assert!(
            matches!(coalescer.pop_next(), Some(DataEvent::Data(Data::Deltas(d))) if d.sequence == 1)
        );
        assert!(matches!(
            coalescer.pop_next(),
            Some(DataEvent::Data(Data::Quote(_)))
        ));
        assert!(matches!(
            coalescer.pop_next(),
            Some(DataEvent::InstrumentStatus(_))
        ));
        assert!(
            matches!(coalescer.pop_next(), Some(DataEvent::Data(Data::Deltas(d))) if d.sequence == 2)
        );
        assert!(
            matches!(coalescer.pop_next(), Some(DataEvent::Data(Data::Deltas(d))) if d.sequence == 3)
        );
        assert!(coalescer.is_empty());
    }
}
