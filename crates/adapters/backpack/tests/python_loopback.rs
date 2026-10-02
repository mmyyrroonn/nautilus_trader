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
use nautilus_backpack::python::loopback::{
    PyBackpackLoopbackAccountFacts, PyBackpackLoopbackExecutionAuthority,
};
use pyo3::{prelude::*, types::PyDict};

#[test]
fn exact_authority_requires_every_permission_and_rejects_invalid_limits() {
    Python::initialize();
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "Authority",
                py.get_type::<PyBackpackLoopbackExecutionAuthority>(),
            )
            .unwrap();
        py.run(
            c"
import inspect
kwargs = dict(expires_at_ms=2000000000000, max_account_age_ms=2000, max_market_age_ms=1000,
              max_order_notional='10.125', max_reserved_notional='20.25',
              max_reserved_margin='5.000001', max_unsettled_orders=2,
              allow_new_risk=False, allow_reduction=True, allow_owned_cancel=True)
authority = Authority(**kwargs)
for name, value in kwargs.items():
    assert getattr(authority, name) == value, name
assert all(p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
           for p in inspect.signature(Authority).parameters.values())
for name in kwargs:
    missing = dict(kwargs)
    del missing[name]
    try:
        Authority(**missing)
    except TypeError:
        pass
    else:
        raise AssertionError(name)
for name, value in [('expires_at_ms', 0), ('max_account_age_ms', 0), ('max_market_age_ms', 0),
                    ('max_unsettled_orders', 0), ('max_order_notional', '0'),
                    ('max_reserved_notional', '9'), ('max_reserved_margin', '-1'),
                    ('max_order_notional', 'do-not-repeat-this-input')]:
    invalid = dict(kwargs)
    invalid[name] = value
    try:
        Authority(**invalid)
    except ValueError as e:
        assert 'do-not-repeat-this-input' not in str(e)
    else:
        raise AssertionError((name, value))
assert 'synthetic-only' in repr(authority)
",
            Some(&locals),
            None,
        )
        .unwrap();
    });
}

#[test]
fn explicit_complete_account_facts_preserve_exact_positions_without_policy_defaults() {
    Python::initialize();
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item("Facts", py.get_type::<PyBackpackLoopbackAccountFacts>())
            .unwrap();
        py.run(c"
import inspect
kwargs = dict(observed_at_ms=12345, available_margin='19.000001', margin_per_notional='0.10',
              fee_buffer_per_notional='0.005', economics_reference='synthetic-model-v1',
              net_positions={'BTC_USDC_PERP.BACKPACK': '-0.00001'},
              auto_borrow=False, auto_lend=False, auto_repay=False, liquidating=False, complete=True)
facts = Facts(**kwargs)
for name, value in kwargs.items():
    assert getattr(facts, name) == value, name
assert all(p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
           for p in inspect.signature(Facts).parameters.values())
for name in kwargs:
    missing = dict(kwargs)
    del missing[name]
    try:
        Facts(**missing)
    except TypeError:
        pass
    else:
        raise AssertionError(name)
for name, value in [('observed_at_ms', 0), ('available_margin', '-0.001'),
                    ('margin_per_notional', '1.001'), ('fee_buffer_per_notional', '-0.01'),
                    ('net_positions', {}), ('net_positions', {'BTC_USDC_PERP.ASTER': '0'}),
                    ('economics_reference', ''), ('auto_borrow', True), ('auto_lend', True),
                    ('auto_repay', True), ('liquidating', True), ('complete', False),
                    ('available_margin', 'do-not-repeat-this-input')]:
    invalid = dict(kwargs)
    invalid[name] = value
    try:
        Facts(**invalid)
    except ValueError as e:
        assert 'do-not-repeat-this-input' not in str(e)
    else:
        raise AssertionError((name, value))
assert 'synthetic-only' in repr(facts)
assert not hasattr(facts, 'generation') and not hasattr(facts, 'namespace')
", Some(&locals), None).unwrap();
    });
}

fn peer_locals<'py>(py: Python<'py>, root: &tempfile::TempDir) -> pyo3::Bound<'py, PyDict> {
    use base64::Engine;
    let module = pyo3::types::PyModule::new(py, "backpack").unwrap();
    module
        .add_class::<PyBackpackLoopbackExecutionAuthority>()
        .unwrap();
    module
        .add_class::<PyBackpackLoopbackAccountFacts>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::account::PyBackpackCredential>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::account::PyBackpackQuota>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::account::PyBackpackExecutionClientConfig>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::config::PyBackpackDataClientConfig>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::config::PyBackpackInstrumentEconomics>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::PyBackpackDataClientFactory>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::loopback_runtime::PyBackpackLoopbackControl>()
        .unwrap();
    module
        .add_class::<nautilus_backpack::python::loopback_runtime::PyBackpackLoopbackSession>()
        .unwrap();
    module.add_class::<nautilus_backpack::python::loopback_runtime::PyBackpackLoopbackExecutionClientConfig>().unwrap();
    module.add_class::<nautilus_backpack::python::loopback_runtime::PyBackpackLoopbackExecutionClientFactory>().unwrap();
    let locals = PyDict::new(py);
    locals.set_item("m", module).unwrap();
    locals
        .set_item(
            "seed",
            base64::engine::general_purpose::STANDARD.encode([7_u8; 32]),
        )
        .unwrap();
    locals
        .set_item("directory", root.path().join("identity").to_str().unwrap())
        .unwrap();
    py.run(
        c"
import json
http = 'http://127.0.0.1:12345'
ws = 'ws://127.0.0.1:12346'
quota = m.BackpackQuota()
credential = m.BackpackCredential(seed, base_url_http=http, base_url_ws=ws)
authority = m.BackpackLoopbackExecutionAuthority(expires_at_ms=2000000000000,
    max_account_age_ms=2000, max_market_age_ms=2000, max_order_notional='10',
    max_reserved_notional='20', max_reserved_margin='10', max_unsettled_orders=2,
    allow_new_risk=True, allow_reduction=True, allow_owned_cancel=True)
economics = m.BackpackInstrumentEconomics('0.1', '0.05', '0', '0.005', 'Synthetic', 'local-peer-v1')
kwargs = dict(authority=authority, mutation_budget_ms=1000, receive_window_ms=5000)
def fresh():
    account = m.BackpackExecutionClientConfig(['BTC_USDC_PERP'], credential,
        'BACKPACK-SYNTHETIC', 'peer-account', directory, quota=quota,
        base_url_http=http, base_url_ws=ws)
    public = m.BackpackDataClientConfig(['BTC_USDC_PERP'], {'BTC_USDC_PERP': economics},
        base_url_http=http, base_url_ws=ws)
    return m.BackpackLoopbackExecutionClientConfig(account, public, **kwargs)
config = fresh()
factory = m.BackpackLoopbackExecutionClientFactory()
control = config.control
",
        Some(&locals),
        None,
    )
    .unwrap();
    locals
}

#[test]
fn static_loopback_construction_is_no_io_and_refuses_production_and_non_synthetic_plans() {
    Python::initialize();
    Python::attach(|py| {
        let root = tempfile::TempDir::new().unwrap();
        let l = peer_locals(py, &root);
        l.set_item(
            "stub_source",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../../python/nautilus_trader/adapters/backpack/__init__.pyi"
            )),
        )
        .unwrap();
        l.set_item(
            "facade_source",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../../python/nautilus_trader/adapters/backpack/__init__.py"
            )),
        )
        .unwrap();
        py.run(c"
import inspect
import ast
stub = ast.parse(stub_source)
facade = ast.parse(facade_source)
def exports(tree):
    return next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == '__all__' for target in node.targets))
assert exports(stub) == exports(facade)
classes = {node.name: node for node in stub.body if isinstance(node, ast.ClassDef)}
for name in [name for name in exports(stub) if name.startswith('BackpackLoopback')]:
    cls = getattr(m, name)
    for method in classes[name].body:
        if not isinstance(method, ast.FunctionDef) or any(isinstance(d, ast.Name) and d.id == 'property' for d in method.decorator_list):
            continue
        constructor = method.name == '__new__'
        runtime = inspect.signature(cls if constructor else getattr(cls, method.name))
        positional = [a.arg for a in method.args.posonlyargs + method.args.args if a.arg not in ['self', 'cls']]
        keyword = [a.arg for a in method.args.kwonlyargs]
        actual = {n:p for n,p in runtime.parameters.items() if n not in ['self', 'cls']}
        assert list(actual) == positional + keyword, (name, method.name)
        assert all(actual[n].kind is inspect.Parameter.KEYWORD_ONLY for n in keyword)
        assert all(actual[n].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for n in positional)
        assert all(p.default is inspect.Parameter.empty for p in actual.values())
assert config.mutation_budget_ms == 1000 and config.receive_window_ms == 5000
assert config.authority.max_order_notional == '10'
assert config.read_only_config.quota.shares_scope(quota)
assert config.public_config.symbols == ['BTC_USDC_PERP']
assert all(not hasattr(control, name) for name in ['acknowledge', 'acknowledge_with', 'submit', 'cancel', 'sign', 'restore_applied_fills'])
assert json.loads(factory.capabilities_json())['durable_economic_ack'] is False
assert json.loads(factory.capabilities_json())['production_writes'] is False
for cls in [m.BackpackLoopbackSession, m.BackpackLoopbackControl]:
    try:
        cls()
    except TypeError:
        pass
    else:
        raise AssertionError('forged native control/session')
try:
    control.begin_session()
except RuntimeError:
    pass
else:
    raise AssertionError('unattached session admitted')
public = config.public_config
account = config.read_only_config
for changed in [dict(mutation_budget_ms=0), dict(mutation_budget_ms=60001),
                dict(receive_window_ms=0), dict(receive_window_ms=60001)]:
    bad = dict(kwargs)
    bad.update(changed)
    try:
        m.BackpackLoopbackExecutionClientConfig(account, public, **bad)
    except ValueError:
        pass
    else:
        raise AssertionError(changed)
real_economics = m.BackpackInstrumentEconomics('0.1', '0.05', '0', '0.005', 'Configured', 'caller')
non_synthetic = m.BackpackDataClientConfig(['BTC_USDC_PERP'], {'BTC_USDC_PERP': real_economics},
    base_url_http=http, base_url_ws=ws)
other_peer = m.BackpackDataClientConfig(['BTC_USDC_PERP'], {'BTC_USDC_PERP': economics},
    base_url_http='http://127.0.0.1:12347', base_url_ws=ws)
for public_bad in [non_synthetic, other_peer]:
    try:
        m.BackpackLoopbackExecutionClientConfig(account, public_bad, **kwargs)
    except ValueError:
        pass
    else:
        raise AssertionError('wrong peer/provenance admitted')
production_credential = m.BackpackCredential(seed)
production_account = m.BackpackExecutionClientConfig(['BTC_USDC_PERP'], production_credential,
    'BACKPACK-PRODUCTION-LABEL', 'production-label', directory, quota=quota)
production_public = m.BackpackDataClientConfig(['BTC_USDC_PERP'], {'BTC_USDC_PERP': economics})
try:
    m.BackpackLoopbackExecutionClientConfig(production_account, production_public, **kwargs)
except ValueError as e:
    assert seed not in str(e)
else:
    raise AssertionError('production write path')
", Some(&l), None).unwrap();
        assert!(!root.path().join("identity").exists());
    });
}

#[tokio::test(flavor = "current_thread")]
async fn native_builder_validates_actual_scope_before_identity_and_weak_control_drops_owner() {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{cache::Cache, enums::Environment};
    use nautilus_live::node::builder::LiveNodeBuilder;
    use nautilus_model::identifiers::TraderId;
    use nautilus_system::get_global_pyo3_registry;
    Python::initialize();
    Python::attach(|py| {
        let root = tempfile::TempDir::new().unwrap();
        let l = peer_locals(py, &root);
        let module = l.get_item("m").unwrap().unwrap();
        nautilus_backpack::python::backpack(module.cast::<pyo3::types::PyModule>().unwrap())
            .unwrap();
        let registry = get_global_pyo3_registry();
        let config = l.get_item("config").unwrap().unwrap();
        let factory = registry
            .extract_exec_factory(py, l.get_item("factory").unwrap().unwrap().unbind())
            .unwrap();
        let extracted = registry
            .extract_config(py, config.clone().unbind())
            .unwrap();
        assert!(
            factory
                .create(
                    TraderId::from("TRADER-001"),
                    "BACKPACK",
                    extracted.as_ref(),
                    Rc::new(RefCell::new(Cache::default())).into()
                )
                .is_err()
        );
        assert!(!root.path().join("identity").exists());
        py.run(c"public = config.public_config\npublic_factory = m.BackpackDataClientFactory(quota=quota)", Some(&l), None).unwrap();
        let data_config = registry
            .extract_config(py, l.get_item("public").unwrap().unwrap().unbind())
            .unwrap();
        let data_factory = registry
            .extract_factory(py, l.get_item("public_factory").unwrap().unwrap().unbind())
            .unwrap();
        let node = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .add_data_client(None, data_factory, data_config)
            .unwrap()
            .add_exec_client(None, factory, extracted)
            .unwrap()
            .build()
            .unwrap();
        assert!(root.path().join("identity").exists());
        py.run(
            c"
health = json.loads(control.telemetry_snapshot_json())
assert health['run_id'] is not None and health['transport_connected'] is False
assert health['private_subscription_confirmed'] is False
assert json.loads(control.pending_fills_json()) == []
try:
    control.begin_session()
except RuntimeError:
    pass
else:
    raise AssertionError('unconnected public/private owner admitted')
old_public = config.public_config
old_account = config.read_only_config
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
    raise AssertionError('old weak attachment survived disposal')
assert json.loads(control.shutdown_report_json())['dirty'] is True
fresh_config = fresh()
fresh_factory = m.BackpackLoopbackExecutionClientFactory()
fresh_public = fresh_config.public_config
fresh_public_factory = m.BackpackDataClientFactory(quota=quota)
",
            Some(&l),
            None,
        )
        .unwrap();
        let data_config = registry
            .extract_config(py, l.get_item("fresh_public").unwrap().unwrap().unbind())
            .unwrap();
        let data_factory = registry
            .extract_factory(
                py,
                l.get_item("fresh_public_factory")
                    .unwrap()
                    .unwrap()
                    .unbind(),
            )
            .unwrap();
        let exec_config = registry
            .extract_config(py, l.get_item("fresh_config").unwrap().unwrap().unbind())
            .unwrap();
        let exec_factory = registry
            .extract_exec_factory(py, l.get_item("fresh_factory").unwrap().unwrap().unbind())
            .unwrap();
        let replacement = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .add_data_client(None, data_factory, data_config)
            .unwrap()
            .add_exec_client(None, exec_factory, exec_config)
            .unwrap()
            .build()
            .unwrap();
        py.run(
            c"
assert json.loads(fresh_config.control.telemetry_snapshot_json())['run_id'] != health['run_id']
try:
    control.begin_session()
except RuntimeError:
    pass
else:
    raise AssertionError('old control redirected to replacement owner')
",
            Some(&l),
            None,
        )
        .unwrap();
        drop(replacement);
    });
}
