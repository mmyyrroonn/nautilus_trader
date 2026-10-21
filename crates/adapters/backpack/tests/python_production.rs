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
use nautilus_backpack::python;
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use rstest::rstest;
use tempfile::TempDir;

fn locals<'py>(py: Python<'py>, root: &TempDir) -> Bound<'py, PyDict> {
    let m = PyModule::new(py, "backpack").unwrap();
    python::backpack(&m).unwrap();
    let l = PyDict::new(py);
    l.set_item("m", m).unwrap();
    l.set_item("seed", STANDARD.encode([7_u8; 32])).unwrap();
    l.set_item("directory", root.path().join("identity").to_str().unwrap())
        .unwrap();
    l.set_item(
        "economic_directory",
        root.path().join("economics").to_str().unwrap(),
    )
    .unwrap();
    py.run(
        c"
import json
http = 'http://127.0.0.1:12345'
ws = 'ws://127.0.0.1:12346'
quota = m.BackpackQuota()
credential = m.BackpackCredential(seed, base_url_http=http, base_url_ws=ws)
limits = dict(expires_at_ms=2000000000000, max_account_age_ms=2000, max_market_age_ms=1500,
    max_order_notional='10.125', max_reserved_notional='20.250', max_reserved_margin='5.000001',
    max_unsettled_orders=4, allow_new_risk=True, allow_reduction=True, allow_owned_cancel=True)
authority = m.BackpackProductionExecutionAuthority(**limits)
economics = m.BackpackInstrumentEconomics('0.1','0.05','-0.00001','0.005','Synthetic','signed-peer')
account = m.BackpackExecutionClientConfig(['BTC_USDC_PERP','ETH_USDC_PERP'],credential,
    'BACKPACK-SIMULATION','peer-account',directory,quota=quota,base_url_http=http,base_url_ws=ws)
public = m.BackpackDataClientConfig(['BTC_USDC_PERP','ETH_USDC_PERP'],
    {'BTC_USDC_PERP':economics,'ETH_USDC_PERP':economics},base_url_http=http,base_url_ws=ws)
kwargs = dict(authority=authority,mutation_budget_ms=1000,receive_window_ms=5000,
    economic_state_directory=economic_directory)
config = m.BackpackProductionExecutionClientConfig(account,public,**kwargs)
factory = m.BackpackProductionExecutionClientFactory()
control = config.control
",
        Some(&l),
        None,
    )
    .unwrap();
    l
}
#[rstest]
fn exact_production_config_readback_has_no_io_or_fact_injection() {
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let l = locals(py, &root);
        py.run(c"
import inspect
for name,value in limits.items():
    assert getattr(config.authority,name) == value, name
assert config.account_config.symbols == ['BTC_USDC_PERP','ETH_USDC_PERP']
assert config.public_config.economics['BTC_USDC_PERP'].maker_fee == '-0.00001'
assert config.account_config.quota.shares_scope(quota)
assert config.mutation_budget_ms == 1000 and config.receive_window_ms == 5000
assert config.economic_state_directory == economic_directory
assert config.account_config.base_url_http == http and config.public_config.base_url_ws == ws
for name in ['begin_session','accept_account','refresh_market','invalidate','acknowledge_fill']:
    assert not hasattr(control,name),name
assert control.shutdown_report_json() is None
for name in ['admission_status_json','persist_economics','reconcile_terminal_evidence','pending_fills_json','telemetry_snapshot_json']:
    try:
        getattr(control,name)()
    except RuntimeError:
        pass
    else:
        raise AssertionError('unattached native control admitted '+name)
for name in kwargs:
    missing = dict(kwargs)
    del missing[name]
    try:
        m.BackpackProductionExecutionClientConfig(account,public,**missing)
    except TypeError:
        pass
    else:
        raise AssertionError('missing explicit permission '+name)
for name,value in [('economic_state_directory',''),('mutation_budget_ms',0),('receive_window_ms',0)]:
    invalid = dict(kwargs)
    invalid[name] = value
    try:
        m.BackpackProductionExecutionClientConfig(account,public,**invalid)
    except ValueError:
        pass
    else:
        raise AssertionError((name,value))
try:
    m.BackpackProductionExecutionClientConfig(account,public,
        **dict(kwargs,authority=m.BackpackLoopbackExecutionAuthority(**limits)))
except TypeError:
    pass
else:
    raise AssertionError('synthetic authority crossed production API')
caps = json.loads(factory.capabilities_json())
assert caps['production_writes'] and caps['restricted_execution']
assert not caps['read_only_account'] and not caps['private_subscription_confirmed']
assert not caps['durable_economic_ack']
readonly = json.loads(m.BackpackExecutionClientFactory().capabilities_json())
assert readonly['read_only_account'] and not readonly['production_writes']
production_credential = m.BackpackCredential(seed)
production_account = m.BackpackExecutionClientConfig(['BTC_USDC_PERP'],production_credential,
    'BACKPACK-LABEL','unverified-identity',directory,quota=quota)
production_public = m.BackpackDataClientConfig(['BTC_USDC_PERP'],{'BTC_USDC_PERP':economics})
try:
    m.BackpackProductionExecutionClientConfig(production_account,production_public,**kwargs)
except ValueError:
    pass
else:
    raise AssertionError('synthetic economics admitted official production origins')
configured = m.BackpackInstrumentEconomics('0.1','0.05','-0.00001','0.005','Configured','explicit-model')
production_public = m.BackpackDataClientConfig(['BTC_USDC_PERP'],{'BTC_USDC_PERP':configured})
production_config = m.BackpackProductionExecutionClientConfig(production_account,production_public,**kwargs)
assert production_config.account_config.base_url_http.startswith('https://')
assert production_config.public_config.economics['BTC_USDC_PERP'].source == 'Configured'
try:
    m.BackpackProductionExecutionClientConfig(account,production_public,**kwargs)
except ValueError:
    pass
else:
    raise AssertionError('public and private endpoint/symbol mismatch admitted')
",Some(&l),None).unwrap();
        assert!(!root.path().join("identity").exists());
        assert!(!root.path().join("economics").exists());
    });
}
#[tokio::test(flavor = "current_thread")]
async fn production_registry_builds_native_owner_and_disposal_releases_identity() {
    use nautilus_common::enums::Environment;
    use nautilus_live::node::builder::LiveNodeBuilder;
    use nautilus_model::identifiers::TraderId;
    use nautilus_system::get_global_pyo3_registry;
    Python::initialize();
    Python::attach(|py| {
        let root = TempDir::new().unwrap();
        let l = locals(py, &root);
        py.run(
            c"public_factory = m.BackpackDataClientFactory(quota=quota)",
            Some(&l),
            None,
        )
        .unwrap();
        let registry = get_global_pyo3_registry();
        let factory = registry
            .extract_exec_factory(py, l.get_item("factory").unwrap().unwrap().unbind())
            .unwrap();
        assert_eq!(
            factory.config_type(),
            "BackpackProductionExecutionClientConfig"
        );
        let config = registry
            .extract_config(py, l.get_item("config").unwrap().unwrap().unbind())
            .unwrap();
        let data_factory = registry
            .extract_factory(py, l.get_item("public_factory").unwrap().unwrap().unbind())
            .unwrap();
        let data_config = registry
            .extract_config(py, l.get_item("public").unwrap().unwrap().unbind())
            .unwrap();
        let node = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .add_data_client(None, data_factory, data_config)
            .unwrap()
            .add_exec_client(None, factory, config)
            .unwrap()
            .build()
            .unwrap();
        assert!(root.path().join("identity").exists());
        py.run(
            c"
health = json.loads(control.telemetry_snapshot_json())
assert health['run_id'] is not None
assert not health['transport_connected'] and not health['private_subscription_confirmed']
assert json.loads(control.pending_fills_json()) == []
",
            Some(&l),
            None,
        )
        .unwrap();
        drop(node);
        py.run(
            c"
try:
    control.telemetry_snapshot_json()
except RuntimeError:
    pass
else:
    raise AssertionError('control retained disposed owner')
report = json.loads(control.shutdown_report_json())
assert report['dirty'] is True
assert report['positions_unknown'] is True
assert report['positions_nonzero'] is False
assert report['complete_account_proof'] is False
assert report['reason'] == 'disconnect_incomplete'
assert report['account_observed_at_ms'] is None
replacement_account = m.BackpackExecutionClientConfig(['BTC_USDC_PERP','ETH_USDC_PERP'],credential,
    'BACKPACK-SIMULATION','peer-account',directory,quota=quota,base_url_http=http,base_url_ws=ws)
replacement_public = m.BackpackDataClientConfig(['BTC_USDC_PERP','ETH_USDC_PERP'],
    {'BTC_USDC_PERP':economics,'ETH_USDC_PERP':economics},base_url_http=http,base_url_ws=ws)
replacement_config = m.BackpackProductionExecutionClientConfig(replacement_account,replacement_public,**kwargs)
replacement_factory = m.BackpackProductionExecutionClientFactory()
",
            Some(&l),
            None,
        )
        .unwrap();
        let data_factory = registry
            .extract_factory(py, l.get_item("public_factory").unwrap().unwrap().unbind())
            .unwrap();
        let data_config = registry
            .extract_config(
                py,
                l.get_item("replacement_public").unwrap().unwrap().unbind(),
            )
            .unwrap();
        let factory = registry
            .extract_exec_factory(
                py,
                l.get_item("replacement_factory").unwrap().unwrap().unbind(),
            )
            .unwrap();
        let config = registry
            .extract_config(
                py,
                l.get_item("replacement_config").unwrap().unwrap().unbind(),
            )
            .unwrap();
        let replacement = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .add_data_client(None, data_factory, data_config)
            .unwrap()
            .add_exec_client(None, factory, config)
            .unwrap()
            .build()
            .unwrap();
        py.run(c"
assert json.loads(replacement_config.control.telemetry_snapshot_json())['run_id'] != health['run_id']
try:
    control.pending_fills_json()
except RuntimeError:
    pass
else:
    raise AssertionError('old control redirected to replacement owner')
", Some(&l), None).unwrap();
        drop(replacement);
    });
}
