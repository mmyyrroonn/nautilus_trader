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

#![cfg(feature = "python")]

use std::{cell::RefCell, rc::Rc};

use nautilus_backpack::{
    config::BackpackDataClientConfig,
    python::{
        self, PyBackpackDataClientFactory,
        config::{PyBackpackDataClientConfig, PyBackpackInstrumentEconomics},
    },
};
use nautilus_common::{
    cache::Cache, clock::TestClock, live::runner::replace_data_event_sender, messages::DataEvent,
};
use nautilus_model::identifiers::{ClientId, Venue};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use rstest::rstest;

fn local_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "backpack").unwrap();
    module.add_class::<PyBackpackInstrumentEconomics>().unwrap();
    module.add_class::<PyBackpackDataClientConfig>().unwrap();
    module.add_class::<PyBackpackDataClientFactory>().unwrap();
    module
}

fn configured<'py>(py: Python<'py>, module: &Bound<'py, PyModule>) -> Bound<'py, PyAny> {
    let economics = module
        .getattr("BackpackInstrumentEconomics")
        .unwrap()
        .call1((
            "0.1",
            "0.05",
            "-0.00001",
            "0.0005",
            "Synthetic",
            "loopback fixture",
        ))
        .unwrap();
    let map = PyDict::new(py);
    map.set_item("BTC_USDC_PERP", economics).unwrap();
    module
        .getattr("BackpackDataClientConfig")
        .unwrap()
        .call1((vec!["BTC_USDC_PERP"], map))
        .unwrap()
}

#[rstest]
fn test_exact_economics_and_config_round_trip_without_io() {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        let config = configured(py, &module);
        let locals = PyDict::new(py);
        locals.set_item("config", &config).unwrap();
        py.run(
            c"
assert config.symbols == ['BTC_USDC_PERP']
econ = config.economics['BTC_USDC_PERP']
assert econ.margin_init == '0.1'
assert econ.margin_maint == '0.05'
assert econ.maker_fee == '-0.00001'
assert econ.taker_fee == '0.0005'
assert econ.source == 'Synthetic'
assert econ.source_reference == 'loopback fixture'
assert config.base_url_http == 'https://api.backpack.exchange'
assert config.base_url_ws == 'wss://ws.backpack.exchange'
assert config.quote_stale_after_ms == 3000
import json
health = json.loads(config.telemetry_snapshot_json())
assert health['schema_version'] == 1
assert health['run_id'] is None
assert not health['connected']
assert not health['quotes_fresh']
assert repr(config) == 'BackpackDataClientConfig'
assert repr(econ) == 'BackpackInstrumentEconomics'
",
            Some(&locals),
            None,
        )
        .unwrap();
        assert!(config.setattr("quote_stale_after_ms", 90000).is_err());
        assert!(config.setattr("symbols", vec!["ETH_USDC_PERP"]).is_err());
    });
}

#[rstest]
#[case("1e-2")]
#[case("0.12345678901234567890123456789")]
#[case("NaN")]
#[case("0.1x")]
#[case("+0.1")]
fn test_economics_reject_lossy_or_noncanonical_decimal(#[case] value: &str) {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        assert!(
            module
                .getattr("BackpackInstrumentEconomics")
                .unwrap()
                .call1((value, "0.05", "0", "0", "Synthetic", "fixture"))
                .is_err()
        );
    });
}

#[rstest]
fn test_python_surface_rejects_float_economics_and_missing_provenance() {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        let economics = module.getattr("BackpackInstrumentEconomics").unwrap();
        assert!(
            economics
                .call1((0.1_f64, "0.05", "0", "0", "Synthetic", "fixture"))
                .is_err()
        );
        assert!(
            economics
                .call1(("0.1", "0.05", "0", "0", "Synthetic", ""))
                .is_err()
        );
        assert!(
            economics
                .call1(("0.1", "0.05", "0", "0", "Unknown", "fixture"))
                .is_err()
        );
    });
}

#[rstest]
fn test_python_allowlist_and_economics_are_checked_together() {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        let original = configured(py, &module);
        let constructor = module.getattr("BackpackDataClientConfig").unwrap();
        assert!(
            constructor
                .call1((Vec::<String>::new(), original.getattr("economics").unwrap()))
                .is_err()
        );
        assert!(
            constructor
                .call1((
                    vec!["BTC_USDC_PERP", "BTC_USDC_PERP"],
                    original.getattr("economics").unwrap()
                ))
                .is_err()
        );
        assert!(
            constructor
                .call1((
                    vec!["ETH_USDC_PERP"],
                    original.getattr("economics").unwrap()
                ))
                .is_err()
        );
        assert!(
            constructor
                .call1((vec!["BTC_USDC_PERP"], PyDict::new(py)))
                .is_err()
        );
    });
}

#[rstest]
#[case("quote_stale_after_ms", 0)]
#[case("quote_stale_after_ms", 30001)]
#[case("http_timeout_secs", 61)]
#[case("max_buffer_frames", 2049)]
#[case("depth_snapshot_limit", 7)]
#[case("max_ws_message_bytes", 1048577)]
fn test_python_lifecycle_bounds_use_native_validation(#[case] key: &str, #[case] value: u64) {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        let original = configured(py, &module);
        let kwargs = PyDict::new(py);
        kwargs.set_item(key, value).unwrap();
        assert!(
            module
                .getattr("BackpackDataClientConfig")
                .unwrap()
                .call(
                    (
                        vec!["BTC_USDC_PERP"],
                        original.getattr("economics").unwrap()
                    ),
                    Some(&kwargs)
                )
                .is_err()
        );
    });
}

#[rstest]
fn test_loopback_override_requires_both_numeric_local_origins() {
    Python::initialize();
    Python::attach(|py| {
        let module = local_module(py);
        let original = configured(py, &module);
        let constructor = module.getattr("BackpackDataClientConfig").unwrap();
        let kwargs = PyDict::new(py);
        kwargs
            .set_item("base_url_http", "http://127.0.0.1:12345")
            .unwrap();
        let args = (
            vec!["BTC_USDC_PERP"],
            original.getattr("economics").unwrap(),
        );
        assert!(constructor.call(args.clone(), Some(&kwargs)).is_err());
        kwargs
            .set_item("base_url_ws", "ws://127.0.0.1:12346")
            .unwrap();
        let config = constructor.call(args.clone(), Some(&kwargs)).unwrap();
        assert_eq!(
            config
                .getattr("base_url_http")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "http://127.0.0.1:12345/"
        );
        kwargs
            .set_item("base_url_http", "http://localhost:12345")
            .unwrap();
        assert!(constructor.call(args.clone(), Some(&kwargs)).is_err());
        kwargs
            .set_item("base_url_http", "https://api.backpack.exchange")
            .unwrap();
        assert!(constructor.call(args, Some(&kwargs)).is_err());
    });
}

#[rstest]
fn test_python_registry_constructs_native_factory_with_shared_run_telemetry() {
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(sender);
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "backpack").unwrap();
        python::backpack(&module).unwrap();
        let config = configured(py, &module);
        let factory = module
            .getattr("BackpackDataClientFactory")
            .unwrap()
            .call0()
            .unwrap();
        let capabilities: serde_json::Value = serde_json::from_str(
            &factory
                .call_method0("capabilities_json")
                .unwrap()
                .extract::<String>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(capabilities["schema_version"], 1);
        assert_eq!(capabilities["public_market_data"], true);
        assert_eq!(capabilities["restricted_execution"], false);
        let registry = get_global_pyo3_registry();
        let extracted_factory = registry.extract_factory(py, factory.unbind()).unwrap();
        let extracted_config = registry
            .extract_config(py, config.clone().unbind())
            .unwrap();
        assert!(extracted_config.as_any().is::<BackpackDataClientConfig>());
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let client = extracted_factory
            .create(
                "BACKPACK-PYTHON",
                extracted_config.as_ref(),
                cache.clone().into(),
                clock.clone(),
            )
            .unwrap();
        assert_eq!(client.client_id(), ClientId::from("BACKPACK-PYTHON"));
        assert_eq!(client.venue(), Some(Venue::from("BACKPACK")));
        let health: serde_json::Value = serde_json::from_str(
            &config
                .call_method0("telemetry_snapshot_json")
                .unwrap()
                .extract::<String>()
                .unwrap(),
        )
        .unwrap();
        assert!(health["run_id"].is_number());
        assert_eq!(health["connected"], false);
        assert!(
            extracted_factory
                .create(
                    "BACKPACK-CONFLICT",
                    extracted_config.as_ref(),
                    cache.into(),
                    clock
                )
                .is_err()
        );
        drop(client);
        assert!(!module.hasattr("BackpackExecutionClientFactory").unwrap());
        assert!(!module.hasattr("BackpackHttpClient").unwrap());
    });
}
