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
use base64::{Engine, engine::general_purpose::STANDARD};
use nautilus_backpack::{
    execution_client::BackpackExecutionClientConfig,
    python::{
        self, PyBackpackDataClientFactory,
        account::{PyBackpackCredential, PyBackpackExecutionClientConfig, PyBackpackQuota},
        execution::PyBackpackExecutionClientFactory,
    },
};
use nautilus_common::enums::Environment;
use nautilus_live::node::builder::LiveNodeBuilder;
use nautilus_model::identifiers::TraderId;
use nautilus_system::get_global_pyo3_registry;
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use rstest::rstest;
use tempfile::TempDir;

fn module(py: Python<'_>) -> Bound<'_, PyModule> {
    let m = PyModule::new(py, "backpack").unwrap();
    m.add_class::<PyBackpackCredential>().unwrap();
    m.add_class::<PyBackpackQuota>().unwrap();
    m.add_class::<PyBackpackExecutionClientConfig>().unwrap();
    m.add_class::<PyBackpackExecutionClientFactory>().unwrap();
    m.add_class::<PyBackpackDataClientFactory>().unwrap();
    m
}
fn locals<'py>(py: Python<'py>, m: &Bound<'py, PyModule>, root: &TempDir) -> Bound<'py, PyDict> {
    let locals = PyDict::new(py);
    locals.set_item("m", m).unwrap();
    locals
        .set_item("seed", STANDARD.encode([7_u8; 32]))
        .unwrap();
    locals
        .set_item("directory", root.path().join("identity").to_str().unwrap())
        .unwrap();
    py.run(
        c"
http = 'http://127.0.0.1:12345'
ws = 'ws://127.0.0.1:12346'
quota = m.BackpackQuota()
credential = m.BackpackCredential(seed, base_url_http=http, base_url_ws=ws)
kwargs = dict(quota=quota, subaccount='2', base_url_http=http, base_url_ws=ws)
args = (['BTC_USDC_PERP'], credential, 'BACKPACK-SYNTHETIC', 'account-101', directory)
",
        Some(&locals),
        None,
    )
    .unwrap();
    locals
}
#[test]
fn test_opaque_audience_bound_constructors_and_exact_policy_no_io() {
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let m = module(py);
        let l = locals(py, &m, &root);
        py.run(c"
import json
config = m.BackpackExecutionClientConfig(*args, **kwargs)
public = m.BackpackDataClientFactory(quota=quota)
factory = m.BackpackExecutionClientFactory()
assert public.quota.shares_scope(quota)
assert config.quota.shares_scope(public.quota)
assert not config.quota.shares_scope(m.BackpackQuota())
assert config.symbols == ['BTC_USDC_PERP']
assert config.account_id == 'BACKPACK-SYNTHETIC'
assert config.identity_directory == directory
assert config.identity_account == 'account-101' and config.subaccount == '2'
assert config.base_url_http == http + '/'
assert config.base_url_ws == ws + '/'
assert config.connect_timeout_ms == 20000
assert config.shutdown_timeout_ms == 3000
assert config.recovery_interval_ms == 30000
assert config.recovery_lookback_ms == 3600000
assert config.input_capacity == 256 and config.fill_capacity == 100000
assert (config.page_size, config.max_pages, config.max_items, config.read_timeout_ms) == (1000, 10, 10000, 30000)
health = json.loads(config.telemetry_snapshot_json())
assert health['schema_version'] == 1 and health['run_id'] is None
assert not health['transport_connected'] and not health['private_subscription_confirmed']
assert not health['rest_snapshot_observed'] and health['pending_fills'] == 0
assert 'AccountIdentityUnverified' in health['evidence_gaps']
assert seed not in repr(config) + repr(credential) + repr(public) + repr(factory) + repr(quota)
assert repr(credential) == 'BackpackCredential(<redacted>)'
assert not hasattr(credential, 'seed') and not hasattr(credential, 'sign')
assert not hasattr(credential, '__dict__')
assert not hasattr(config, 'credential') and not hasattr(m, 'BackpackHttpClient')
assert not hasattr(factory, 'submit_order') and not hasattr(factory, 'create')
assert factory.name() == 'BACKPACK' and factory.config_type == 'BackpackExecutionClientConfig'
caps = json.loads(factory.capabilities_json())
assert caps['read_only_account'] and not caps['restricted_execution']
assert not caps['production_writes'] and not caps['durable_economic_ack']
try:
    config.symbols = []
except AttributeError:
    pass
else:
    raise AssertionError('config must be immutable')
production = m.BackpackCredential(seed)
plan = m.BackpackExecutionClientConfig(['BTC_USDC_PERP'], production, 'BACKPACK-SYNTHETIC', 'account-101', directory, quota=quota)
assert plan.base_url_http == 'https://api.backpack.exchange'
assert json.loads(plan.telemetry_snapshot_json())['run_id'] is None
", Some(&l), None).unwrap();
        assert!(!root.path().join("identity").exists());
    });
}
#[rstest]
#[case("connect_timeout_ms", 0)]
#[case("connect_timeout_ms", 60001)]
#[case("shutdown_timeout_ms", 0)]
#[case("shutdown_timeout_ms", 30001)]
#[case("recovery_interval_ms", 99)]
#[case("recovery_interval_ms", 3600001)]
#[case("recovery_lookback_ms", 0)]
#[case("recovery_lookback_ms", 86400001)]
#[case("input_capacity", 0)]
#[case("input_capacity", 4097)]
#[case("fill_capacity", 1000001)]
#[case("page_size", 1001)]
#[case("max_pages", 1001)]
#[case("max_items", 999)]
#[case("read_timeout_ms", 300001)]
fn test_account_limits_reuse_checked_native_boundaries(#[case] key: &str, #[case] value: u64) {
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let m = module(py);
        let l = locals(py, &m, &root);
        l.set_item("key", key).unwrap();
        l.set_item("value", value).unwrap();
        py.run(
            c"
kwargs[key] = value
try:
    m.BackpackExecutionClientConfig(*args, **kwargs)
except ValueError as error:
    assert seed not in str(error)
else:
    raise AssertionError('invalid policy accepted')
",
            Some(&l),
            None,
        )
        .unwrap();
        assert!(!root.path().join("identity").exists());
    });
}
#[test]
fn test_credentials_namespace_and_quota_failures_are_redacted_without_discovery() {
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let m = module(py);
        let l = locals(py, &m, &root);
        py.run(c"
sentinel = 'PRIVATE-SEED-MUST-NOT-APPEAR'
for invalid in [sentinel, '', seed[:-1], seed + 'x']:
    try:
        m.BackpackCredential(invalid)
    except ValueError as error:
        assert invalid not in str(error) if invalid else True
    else:
        raise AssertionError('invalid credential accepted')
for changed in [dict(base_url_http='http://127.0.0.1:12347'), dict(base_url_ws='ws://127.0.0.1:12348'), dict(base_url_http=None, base_url_ws=None)]:
    options = kwargs | changed
    try:
        m.BackpackExecutionClientConfig(*args, **options)
    except ValueError as error:
        assert seed not in str(error)
    else:
        raise AssertionError('credential audience crossed')
for changed in [dict(subaccount=''), dict(subaccount='bad\ncomponent')]:
    try:
        m.BackpackExecutionClientConfig(*args, **(kwargs | changed))
    except ValueError:
        pass
    else:
        raise AssertionError('invalid namespace accepted')
for label in ['ASTER-SYNTHETIC', 'backpack-SYNTHETIC', 'BACKPACKX-SYNTHETIC']:
    try:
        m.BackpackExecutionClientConfig(args[0], args[1], label, *args[3:], **kwargs)
    except ValueError as error:
        assert seed not in str(error)
    else:
        raise AssertionError('wrong venue issuer accepted')
for bad_credential in [None, seed, {}]:
    try:
        m.BackpackExecutionClientConfig(args[0], bad_credential, *args[2:], **kwargs)
    except TypeError as error:
        assert seed not in str(error)
    else:
        raise AssertionError('credential implicitly discovered')
try:
    m.BackpackExecutionClientConfig(*args)
except TypeError:
    pass
else:
    raise AssertionError('independent default quota forbidden for account')
for periods in [dict(standard_period_ms=29), dict(historical_period_ms=1999), dict(standard_period_ms=60001), dict(historical_period_ms=300001)]:
    try:
        m.BackpackQuota(**periods)
    except ValueError:
        pass
    else:
        raise AssertionError('invalid quota accepted')
", Some(&l), None).unwrap();
        assert!(!root.path().join("identity").exists());
    });
}
#[tokio::test(flavor = "current_thread")]
async fn test_execution_registry_builds_actual_native_livenode_and_claims_identity() {
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let m = PyModule::new(py, "backpack").unwrap();
        python::backpack(&m).unwrap();
        let l = locals(py, &m, &root);
        py.run(
            c"config = m.BackpackExecutionClientConfig(*args, **kwargs)
factory = m.BackpackExecutionClientFactory()",
            Some(&l),
            None,
        )
        .unwrap();
        let registry = get_global_pyo3_registry();
        let config = l.get_item("config").unwrap().unwrap();
        let extracted = registry
            .extract_config(py, config.clone().unbind())
            .unwrap();
        assert!(extracted.as_any().is::<BackpackExecutionClientConfig>());
        let factory = registry
            .extract_exec_factory(py, l.get_item("factory").unwrap().unwrap().unbind())
            .unwrap();
        let node = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .add_exec_client(Some("BACKPACK-READONLY".into()), factory, extracted)
            .unwrap()
            .build()
            .unwrap();
        assert!(root.path().join("identity").exists());
        let health: serde_json::Value = serde_json::from_str(
            &config
                .call_method0("telemetry_snapshot_json")
                .unwrap()
                .extract::<String>()
                .unwrap(),
        )
        .unwrap();
        assert!(health["run_id"].is_number());
        assert_eq!(health["transport_connected"], false);
        assert_eq!(health["private_subscription_confirmed"], false);
        // OS namespace ownership survives config extraction, and second handles fail closed.
        let duplicate = registry
            .extract_config(py, config.clone().unbind())
            .unwrap();
        let duplicate_factory = registry
            .extract_exec_factory(
                py,
                m.getattr("BackpackExecutionClientFactory")
                    .unwrap()
                    .call0()
                    .unwrap()
                    .unbind(),
            )
            .unwrap();
        assert!(
            duplicate_factory
                .create(
                    TraderId::from("TRADER-001"),
                    "DUPLICATE",
                    duplicate.as_ref(),
                    std::rc::Rc::new(std::cell::RefCell::new(
                        nautilus_common::cache::Cache::default()
                    ))
                    .into()
                )
                .is_err()
        );
        drop(node);
        py.run(
            c"kwargs['subaccount'] = 'different'
other = m.BackpackExecutionClientConfig(*args, **kwargs)",
            Some(&l),
            None,
        )
        .unwrap();
        let mismatch = registry
            .extract_config(py, l.get_item("other").unwrap().unwrap().unbind())
            .unwrap();
        assert!(
            duplicate_factory
                .create(
                    TraderId::from("TRADER-001"),
                    "MISMATCH",
                    mismatch.as_ref(),
                    std::rc::Rc::new(std::cell::RefCell::new(
                        nautilus_common::cache::Cache::default()
                    ))
                    .into()
                )
                .is_err()
        );
    });
}
