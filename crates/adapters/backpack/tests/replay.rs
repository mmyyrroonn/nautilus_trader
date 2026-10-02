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

//! Bounded offline replay uses only public fixtures and explicitly synthetic adverse records.
use std::io::Cursor;

use nautilus_backpack::{
    config::{BackpackConfig, BackpackPublicLifecycleConfig},
    models::BackpackMarket,
    parsing::parse_market,
    replay::BackpackPublicReplay,
};
use nautilus_core::UnixNanos;
use nautilus_model::data::Data;
use rstest::rstest;
use serde_json::{Value, json};
fn replay() -> BackpackPublicReplay {
    let scope = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap();
    let market: BackpackMarket =
        serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
    BackpackPublicReplay::new_checked(
        parse_market(&market, &scope, UnixNanos::from(1)).unwrap(),
        1,
        BackpackPublicLifecycleConfig {
            depth_snapshot_limit: 5,
            ..Default::default()
        },
    )
    .unwrap()
}
fn record(kind: &str, generation: u64, received_at_ns: u64, payload: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"kind":kind,"generation":generation,"received_at_ns":received_at_ns,"payload":payload})).unwrap()
}
fn snapshot() -> Value {
    json!({"asks":[["101.0","1.00000"]],"bids":[["100.0","1.00000"]],"lastUpdateId":"100","timestamp":500})
}
#[rstest]
fn replay_preserves_original_times_and_discards_old_before_parser() {
    let mut replay = replay();
    let frame: Value =
        serde_json::from_str(include_str!("../test_data/public_bookticker_btc.json")).unwrap();
    let Data::Quote(quote) = replay
        .apply_record(&record("frame", 1, 77, frame.clone()))
        .unwrap()
        .unwrap()
    else {
        panic!("missing quote")
    };
    assert_eq!(quote.ts_init, UnixNanos::from(77));
    assert_eq!(
        quote.ts_event.as_u64(),
        frame["data"]["T"].as_u64().unwrap() * 1000
    );
    let old = record("frame", 0, 99, json!({"invalid":"old payload ignored"}));
    assert!(replay.apply_record(&old).unwrap().is_none());
    let Data::Deltas(batch) = replay
        .apply_record(&record("snapshot", 1, 88, snapshot()))
        .unwrap()
        .unwrap()
    else {
        panic!("missing snapshot")
    };
    assert_eq!(batch.ts_init, UnixNanos::from(88));
    assert_eq!(batch.ts_event, UnixNanos::from(500_000));
}
#[rstest]
fn replay_duplicate_known_field_invalidates_book_until_restart() {
    let mut replay = replay();
    replay
        .apply_record(&record("snapshot", 1, 1, snapshot()))
        .unwrap();
    let invalid=br#"{"kind":"frame","generation":1,"received_at_ns":2,"payload":{"stream":"depth.BTC_USDC_PERP","data":{"e":"depth","E":1,"T":1,"s":"BTC_USDC_PERP","U":101,"u":101,"u":102,"a":[],"b":[]}}}"#;
    assert!(replay.apply_record(invalid).is_err());
    assert!(
        replay
            .apply_record(&record("snapshot", 1, 3, snapshot()))
            .is_err()
    );
    assert!(
        replay
            .apply_record(br#"{"kind":"restart","generation":2,"received_at_ns":3}"#)
            .unwrap()
            .is_none()
    );
    assert!(
        replay
            .apply_record(&record(
                "snapshot",
                1,
                4,
                json!({"bad":"old snapshot ignored"})
            ))
            .unwrap()
            .is_none()
    );
    assert!(
        replay
            .apply_record(&record("snapshot", 2, 4, snapshot()))
            .unwrap()
            .is_some()
    );
}
#[rstest]
fn replay_reader_bounds_lines_records_and_streams_outputs() {
    let mut replay = replay();
    let line = record("snapshot", 1, 88, snapshot());
    let mut output = vec![];
    assert_eq!(
        replay
            .read_json_lines(&mut Cursor::new(line), |d| output.push(d))
            .unwrap(),
        1
    );
    assert_eq!(output.len(), 1);
    let oversized = vec![b' '; 1_048_577];
    assert!(
        replay
            .read_json_lines(&mut Cursor::new(oversized), |_| panic!(
                "oversized record published"
            ))
            .is_err()
    );
}
