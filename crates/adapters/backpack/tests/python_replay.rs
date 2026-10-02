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

//! Real embedded Python replay acceptance; all inputs are offline public/synthetic fixtures.

use nautilus_backpack::python::{
    config::{PyBackpackDataClientConfig, PyBackpackInstrumentEconomics},
    replay::PyBackpackPublicReplay,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use rstest::rstest;

fn locals(py: Python<'_>) -> Bound<'_, PyDict> {
    let module = PyModule::new(py, "backpack_replay_test").unwrap();
    module.add_class::<PyBackpackInstrumentEconomics>().unwrap();
    module.add_class::<PyBackpackDataClientConfig>().unwrap();
    module.add_class::<PyBackpackPublicReplay>().unwrap();
    let locals = PyDict::new(py);
    locals.set_item("bp", module).unwrap();
    locals
        .set_item(
            "market_json",
            include_str!("../test_data/btc_usdc_perp.json"),
        )
        .unwrap();
    locals
        .set_item(
            "quote_json",
            include_str!("../test_data/public_bookticker_btc.json"),
        )
        .unwrap();
    locals
        .set_item(
            "trade_json",
            include_str!("../test_data/public_trade_btc.json"),
        )
        .unwrap();
    locals
        .set_item(
            "mark_json",
            include_str!("../test_data/public_markprice_btc.json"),
        )
        .unwrap();
    py.run(c"
import json
economics = bp.BackpackInstrumentEconomics('0.1', '0.05', '-0.00001', '0.0005', 'Synthetic', 'offline fixture')
config = bp.BackpackDataClientConfig(['BTC_USDC_PERP'], {'BTC_USDC_PERP': economics})
replay = bp.BackpackPublicReplay(config, market_json, 88)
def record(kind, generation, received, payload=None):
    value = dict(kind=kind, generation=generation, received_at_ns=received)
    if payload is not None:
        value['payload'] = payload
    return json.dumps(value).encode()
",Some(&locals),None).unwrap();
    locals
}

#[rstest]
fn replay_python_returns_native_exact_data_and_provenance_without_claiming_live_run() {
    Python::initialize();
    Python::attach(|py| {
        let locals = locals(py);
        py.run(
            c"
assert type(replay.instrument).__name__ == 'CryptoPerpetual'
assert str(replay.instrument.id) == 'BTC_USDC_PERP.BACKPACK'
assert replay.instrument.ts_init == 88
assert not replay.instrument.info['backpack_execution_ready']
assert str(replay.instrument.maker_fee) == '-0.00001'
frame = json.loads(quote_json)
line = record('frame', 1, 77, frame)
quote = replay.apply_record(line)
assert type(quote).__name__ == 'QuoteTick'
assert quote.ts_init == 77
assert quote.ts_event == frame['data']['T'] * 1000
assert str(quote.bid_price) == frame['data']['b']
assert str(quote.bid_size) == frame['data']['B']
assert replay.apply_record(line) is None
trade_frame = json.loads(trade_json)
trade = replay.apply_record(record('frame', 1, 78, trade_frame))
assert type(trade).__name__ == 'TradeTick'
assert str(trade.trade_id) == str(trade_frame['data']['t'])
assert str(trade.price) == trade_frame['data']['p']
assert str(trade.size) == trade_frame['data']['q']
assert trade.ts_event == trade_frame['data']['T'] * 1000 and trade.ts_init == 78
mark_frame = json.loads(mark_json)
mark = replay.apply_record(record('frame', 1, 79, mark_frame))
assert type(mark).__name__ == 'MarkPriceUpdate'
assert str(mark.value) == mark_frame['data']['p']
assert mark.ts_event == mark_frame['data']['T'] * 1000 and mark.ts_init == 79
assert replay.apply_record(record('frame', 0, 99, {'bad': 'old ignored'})) is None
health = json.loads(config.telemetry_snapshot_json())
assert health['run_id'] is None and not health['connected']
assert repr(replay) == 'BackpackPublicReplay(offline)'
",
            Some(&locals),
            None,
        )
        .unwrap();
    });
}

#[rstest]
fn replay_python_depth_errors_require_new_generation_and_preserve_batch_times() {
    Python::initialize();
    Python::attach(|py| {
        let locals = locals(py);
        py.run(c"
snapshot = dict(asks=[['101.0','1.00000']], bids=[['100.0','1.00000']], lastUpdateId='100', timestamp=500)
batch = replay.apply_record(record('snapshot', 1, 88, snapshot))
assert type(batch).__name__ == 'OrderBookDeltas'
assert batch.sequence == 100 and batch.ts_init == 88 and batch.ts_event == 500000
try:
    replay.apply_record(b'{bad-sensitive-fixture')
except ValueError as error:
    assert 'sensitive-fixture' not in str(error)
else:
    raise AssertionError('malformed record accepted')
try:
    replay.apply_record(record('snapshot', 1, 90, snapshot))
except ValueError:
    pass
else:
    raise AssertionError('invalidated book restored without restart')
assert replay.apply_record(record('restart', 2, 91)) is None
assert replay.apply_record(record('snapshot', 1, 92, {'old': 'ignored'})) is None
batch = replay.apply_record(record('snapshot', 2, 93, snapshot))
assert batch.ts_init == 93 and batch.sequence == 100
try:
    replay.apply_record(b' ' * 1048577)
except ValueError:
    pass
else:
    raise AssertionError('oversized record accepted')
",Some(&locals),None).unwrap();
    });
}

#[rstest]
fn replay_python_metadata_is_bounded_allowlisted_and_sanitized() {
    Python::initialize();
    Python::attach(|py| {
        let locals = locals(py);
        py.run(c"
for metadata in [' ' * 1048577, '{sensitive-fixture', market_json.replace('BTC_USDC_PERP', 'ETH_USDC_PERP')]:
    try:
        bp.BackpackPublicReplay(config, metadata, 0)
    except ValueError as error:
        assert 'sensitive-fixture' not in str(error)
    else:
        raise AssertionError('invalid metadata accepted')
try:
    bp.BackpackPublicReplay(config, market_json, -1)
except OverflowError:
    pass
else:
    raise AssertionError('negative timestamp accepted')
",Some(&locals),None).unwrap();
    });
}
