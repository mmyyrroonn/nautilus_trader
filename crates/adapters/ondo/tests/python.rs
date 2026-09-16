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

//! Offline tests of the Ondo Python surface.
//!
//! Every test here runs in-process against the registered `pyo3` module: no wheel, no venue and no
//! credential. They are what proves the exported names, the configuration defaults and the absence
//! of any secret parameter are the ones the plan fixes, rather than something asserted only once a
//! wheel exists.
//!
//! Exactly one test calls [`nautilus_ondo::python::ondo`], because the module registers `ONDO`
//! extractors in a process-global registry that rejects a second registration; the other tests
//! build the same classes onto a scratch module, which is how they stay independent of that
//! registry.

#![cfg(feature = "python")]

use std::{cell::RefCell, rc::Rc, sync::Arc, task::Poll, time::Duration};

use futures_util::poll;
use nautilus_common::{
    cache::Cache, clock::TestClock, factories::DataClientFactory,
    live::runner::replace_data_event_sender, messages::DataEvent,
};
use nautilus_model::identifiers::{ClientId, InstrumentId, Venue};
use nautilus_ondo::{
    common::{
        consts::{ONDO, ONDO_VENUE},
        enums::OndoEnvironment,
    },
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
    factories::{OndoDataClientFactory, OndoExecutionClientFactory},
    http::{client::OndoHttpClient, rate_limit::shared_rest_budget},
    websocket::private::PrivateStreamMode,
};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{
    Bound, Py, Python,
    exceptions::PyTypeError,
    types::{PyAny, PyAnyMethods, PyDict, PyModule, PyModuleMethods},
};
use rstest::rstest;

/// A scratch module holding the configuration, the environment and the HTTP client, without going
/// through the process-global extractor registry.
fn scratch_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "ondo").expect("the ondo scratch module should be created");

    module
        .add_class::<OndoEnvironment>()
        .expect("the environment should register");
    module
        .add_class::<OndoDataClientConfig>()
        .expect("the data configuration should register");
    module
        .add_class::<OndoExecutionClientConfig>()
        .expect("the execution configuration should register");
    module
        .add_class::<OndoDataClientFactory>()
        .expect("the data factory should register");
    module
        .add_class::<OndoExecutionClientFactory>()
        .expect("the execution factory should register");
    module
        .add_class::<OndoHttpClient>()
        .expect("the HTTP client should register");

    module
}

fn setup_data_event_sender() {
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(sender);
}

fn sandbox<'py>(module: &Bound<'py, PyModule>) -> Bound<'py, PyAny> {
    module
        .getattr("OndoEnvironment")
        .expect("the environment should be registered")
        .getattr("SANDBOX")
        .expect("SANDBOX should be exported")
}

#[rstest]
fn test_the_python_module_registers_the_documented_surface() {
    setup_data_event_sender();
    Python::initialize();

    Python::attach(|py| {
        let module = PyModule::new(py, "ondo").expect("the ondo module should be created");
        nautilus_ondo::python::ondo(&module).expect("the ondo Python module should register");

        assert_eq!(
            module.getattr("ONDO").unwrap().extract::<String>().unwrap(),
            ONDO
        );
        assert_eq!(
            module
                .getattr("ONDO_VENUE")
                .unwrap()
                .extract::<Venue>()
                .unwrap(),
            *ONDO_VENUE,
            "ONDO_VENUE is the Ondo Perps venue, not the ONDO asset ticker",
        );
        assert_eq!(
            module
                .getattr("ONDO_CLIENT_ID")
                .unwrap()
                .extract::<ClientId>()
                .unwrap()
                .as_str(),
            ONDO,
        );

        let environment = module.getattr("OndoEnvironment").unwrap();

        for variant in ["PRODUCTION", "SANDBOX"] {
            assert!(
                environment.hasattr(variant).unwrap(),
                "OndoEnvironment.{variant} must be exported",
            );
        }

        // §4.1's public client list, complete as of the execution phase.
        for name in [
            "OndoDataClientConfig",
            "OndoDataClientFactory",
            "OndoExecutionClientConfig",
            "OndoExecutionClientFactory",
            "OndoHttpClient",
        ] {
            assert!(
                module.hasattr(name).unwrap(),
                "{name} must be exported to Python",
            );
        }

        // The rate budget is a runtime object the factories own rather than a Python surface, and
        // the credential type is never exported: signing stays behind the HTTP client.
        for absent in ["OndoRateBudget", "OndoCredential"] {
            assert!(
                !module.hasattr(absent).unwrap(),
                "{absent} must not be exported to Python",
            );
        }

        // The execution configuration is the one surface carrying a credential pair, and neither
        // half of it is readable from Python.
        let exec_config = module.getattr("OndoExecutionClientConfig").unwrap();

        for secret in ["api_key", "api_secret", "key_id", "secret"] {
            assert!(
                !exec_config.hasattr(secret).unwrap(),
                "OndoExecutionClientConfig must not expose `{secret}` to Python",
            );
        }

        let exec_config = exec_config
            .call0()
            .expect("the execution config should construct with no arguments")
            .extract::<OndoExecutionClientConfig>()
            .expect("the execution config should extract back into Rust");

        assert_eq!(
            exec_config.environment,
            OndoEnvironment::Sandbox,
            "the authenticated surface defaults to sandbox, never to production",
        );

        // The factory this module registered is the one the client registry extracts with `ONDO`.
        let factory = Py::new(py, OndoDataClientFactory::new())
            .expect("the factory should convert to a Python object")
            .into_any();
        let config = Py::new(
            py,
            OndoDataClientConfig::builder()
                .load_ids(vec![InstrumentId::from("NVDA-USD-PERP.ONDO")])
                .build(),
        )
        .expect("the config should convert to a Python object")
        .into_any();

        let registry = get_global_pyo3_registry();
        let extracted_factory = registry
            .extract_factory(py, factory)
            .expect("the data factory should extract from a Python object");
        let extracted_config = registry
            .extract_config(py, config)
            .expect("the data config should extract from a Python object");

        assert_eq!(extracted_factory.name(), ONDO);
        assert_eq!(extracted_factory.config_type(), "OndoDataClientConfig");

        let ondo_config = extracted_config
            .as_any()
            .downcast_ref::<OndoDataClientConfig>()
            .expect("the config should downcast to the crate's own type");

        assert_eq!(ondo_config.load_ids.len(), 1);

        let client = extracted_factory
            .create(
                "ONDO-DATA-EXTRACTED",
                extracted_config.as_ref(),
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .expect("the extracted factory should create a data client");

        assert_eq!(client.client_id(), ClientId::from("ONDO-DATA-EXTRACTED"));
        assert_eq!(client.venue(), Some(*ONDO_VENUE));
        assert!(
            !client.is_connected(),
            "the factory builds a client, it does not connect one",
        );
    });
}

/// The two factories a live node builds in Python draw on one REST budget per environment, and the
/// client one of them creates is paced by it (plan §4.4, finding F13).
///
/// Both factories are constructed here **through the Python surface** - `OndoDataClientFactory()`
/// and `OndoExecutionClientFactory()`, which is what the node's registration does - and nothing
/// hands either of them a budget. What makes them share one is the environment the configuration
/// names, resolved at `create`.
#[tokio::test(start_paused = true)]
async fn test_the_python_factories_share_one_environment_budget_under_concurrency() {
    setup_data_event_sender();
    Python::initialize();

    let (data_factory, exec_factory) = Python::attach(|py| {
        let module = scratch_module(py);
        let data_factory = module
            .getattr("OndoDataClientFactory")
            .expect("the data factory is exported")
            .call0()
            .expect("the data factory constructs with no arguments")
            .extract::<OndoDataClientFactory>()
            .expect("the Python factory extracts back into the Rust type the node uses");
        let exec_factory = module
            .getattr("OndoExecutionClientFactory")
            .expect("the execution factory is exported")
            .call0()
            .expect("the execution factory constructs with no arguments")
            .extract::<OndoExecutionClientFactory>()
            .expect("the Python factory extracts back into the Rust type the node uses");

        (data_factory, exec_factory)
    });

    // What both Python constructors resolved: the default environment's process-wide bucket, and
    // therefore one instance for both surfaces rather than one each.
    assert!(
        Arc::ptr_eq(
            data_factory.budget().limiter(),
            exec_factory.budget().limiter()
        ),
        "two factories of one environment are one budget, without a caller passing one"
    );
    assert!(Arc::ptr_eq(
        data_factory.budget().limiter(),
        shared_rest_budget(OndoEnvironment::Production).limiter()
    ));

    // Concurrency across the two factories: the slot one of them spent is the slot the other waits
    // for. Neither handle was passed to the test - each came from its own factory - and the second
    // acquisition is driven by hand rather than spawned, so the assertion is about the budget
    // rather than about how the runtime happened to schedule a task.
    data_factory
        .budget()
        .acquire(nautilus_ondo::http::rate_limit::OndoRequestPriority::Normal)
        .await;

    let exec_budget = exec_factory.budget().clone();
    let mut waiting =
        Box::pin(exec_budget.acquire(nautilus_ondo::http::rate_limit::OndoRequestPriority::Normal));
    for _ in 0..16 {
        assert!(
            poll!(waiting.as_mut()).is_pending(),
            "the second factory waits on the slot the first one spent"
        );
        tokio::task::yield_now().await;
    }

    // Two seconds rather than one: this bucket is process-wide, so the reference instant its
    // limiter counts from was taken by whichever test in this binary created it - possibly a
    // runtime whose paused clock is a millisecond or so away from this one's. What is asserted
    // exactly is that the second factory *waits*; the release allows for that skew rather than
    // pretending two runtimes share one clock.
    tokio::time::advance(Duration::from_secs(2)).await;

    let mut released = false;
    for _ in 0..16 {
        if poll!(waiting.as_mut()).is_ready() {
            released = true;

            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        released,
        "one request per second, however many factories share the environment"
    );

    // And the client one of them creates is paced by the bucket of the environment its own
    // configuration names - here the other one, which is what the isolation key buys.
    let config = Python::attach(|py| {
        let module = scratch_module(py);
        let kwargs = PyDict::new(py);
        kwargs.set_item("environment", sandbox(&module)).unwrap();
        kwargs
            .set_item("load_ids", vec![InstrumentId::from("NVDA-USD-PERP.ONDO")])
            .unwrap();
        // Nothing is listening on the loopback port, so a read that gets past the budget fails at
        // the transport instead of reaching a venue.
        kwargs
            .set_item("base_url_http", "http://127.0.0.1:9")
            .unwrap();

        module
            .getattr("OndoDataClientConfig")
            .expect("the data configuration is exported")
            .call((), Some(&kwargs))
            .expect("the configuration constructs from keywords")
            .extract::<OndoDataClientConfig>()
            .expect("the configuration extracts back into Rust")
    });

    // This bucket is created here, by this test's own runtime, and the client's read is the only
    // thing that draws on it - so the cell it spends is the cell the assertion below finds gone.
    let sandbox_budget = shared_rest_budget(OndoEnvironment::Sandbox);
    assert!(
        sandbox_budget
            .limiter()
            .check_key(&ustr::Ustr::from(
                nautilus_ondo::http::rate_limit::ONDO_REST_BUCKET
            ))
            .is_ok(),
        "the sandbox environment's bucket holds its one cell before the read"
    );

    let mut client = data_factory
        .create(
            "ONDO-PY-DATA",
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .expect("the Python-built factory creates the client the node asks it for");

    let mut connect = Box::pin(client.connect());
    for _ in 0..16 {
        assert!(
            poll!(connect.as_mut()).is_pending(),
            "the sandbox configuration waits on the sandbox bucket, not on the factory's own"
        );
        tokio::task::yield_now().await;
    }

    assert!(
        sandbox_budget
            .limiter()
            .check_key(&ustr::Ustr::from(
                nautilus_ondo::http::rate_limit::ONDO_REST_BUCKET
            ))
            .is_err(),
        "the client created through the Python factory drew the sandbox environment's cell"
    );

    tokio::time::advance(Duration::from_secs(1)).await;

    let mut connected = None;
    for _ in 0..16 {
        match poll!(connect.as_mut()) {
            Poll::Ready(result) => {
                connected = Some(result);

                break;
            }
            Poll::Pending => {}
        }
        tokio::task::yield_now().await;
    }
    let connected = match connected {
        Some(result) => result,
        None => connect.await,
    };
    assert!(
        connected.is_err(),
        "the read went out and nothing answers on the local port"
    );
}

#[rstest]
fn test_the_data_client_config_defaults_match_the_plan() {
    Python::initialize();

    Python::attach(|py| {
        let module = scratch_module(py);

        let config = module
            .getattr("OndoDataClientConfig")
            .unwrap()
            .call0()
            .expect("the config should construct with no arguments")
            .extract::<OndoDataClientConfig>()
            .expect("the config should extract back into Rust");

        assert_eq!(config.environment, OndoEnvironment::Production);
        assert!(config.load_ids.is_empty());
        assert_eq!(config.http_timeout_secs, 15);
        assert_eq!(config.ws_heartbeat_secs, 20);
        assert_eq!(config.book_limit, 100);
        assert!(config.base_url_http.is_none());
        assert!(config.base_url_ws.is_none());
        assert!(config.raw_md_path.is_none());

        let kwargs = PyDict::new(py);
        kwargs.set_item("environment", sandbox(&module)).unwrap();
        kwargs
            .set_item(
                "load_ids",
                vec![
                    InstrumentId::from("NVDA-USD-PERP.ONDO"),
                    InstrumentId::from("TSLA-USD-PERP.ONDO"),
                ],
            )
            .unwrap();

        let configured = module
            .getattr("OndoDataClientConfig")
            .unwrap()
            .call((), Some(&kwargs))
            .expect("the config should construct from keywords")
            .extract::<OndoDataClientConfig>()
            .expect("the configured config should extract back into Rust");

        assert_eq!(configured.environment, OndoEnvironment::Sandbox);
        assert_eq!(
            configured.load_ids,
            vec![
                InstrumentId::from("NVDA-USD-PERP.ONDO"),
                InstrumentId::from("TSLA-USD-PERP.ONDO"),
            ],
            "load_ids must reach Rust as the requested instruments",
        );
    });
}

#[rstest]
fn test_the_data_client_config_and_the_http_client_take_no_secret() {
    Python::initialize();

    Python::attach(|py| {
        let module = scratch_module(py);
        let config_class = module.getattr("OndoDataClientConfig").unwrap();

        for credential in [
            "api_key",
            "api_secret",
            "secret",
            "private_key",
            "passphrase",
            "token",
        ] {
            assert!(
                !config_class.hasattr(credential).unwrap(),
                "the data configuration must not expose `{credential}`",
            );

            let kwargs = PyDict::new(py);
            kwargs.set_item(credential, "x").unwrap();

            let error = config_class
                .call((), Some(&kwargs))
                .expect_err("an unknown field must not be accepted silently");

            assert!(
                error.is_instance_of::<PyTypeError>(py),
                "`{credential}` must be rejected as an unknown keyword, was {error}",
            );
        }

        let http_class = module.getattr("OndoHttpClient").unwrap();

        for credential in ["api_key", "api_secret", "secret", "token"] {
            assert!(
                !http_class.hasattr(credential).unwrap(),
                "the public HTTP client must not expose `{credential}`",
            );

            let kwargs = PyDict::new(py);
            kwargs.set_item(credential, "x").unwrap();

            let error = http_class
                .call((sandbox(&module),), Some(&kwargs))
                .expect_err("the HTTP client must not accept a credential either");

            assert!(
                error.is_instance_of::<PyTypeError>(py),
                "`{credential}` must be rejected by OndoHttpClient, was {error}",
            );
        }
    });
}

#[rstest]
fn test_the_http_client_constructs_offline_and_exposes_the_four_reads() {
    Python::initialize();

    Python::attach(|py| {
        let module = scratch_module(py);

        // No venue is reachable in this test and no request is made: construction only builds the
        // connection pool, so this must return a usable client. The default timeout is pinned by
        // omitting it, and the explicit form is constructed too.
        let client = module
            .getattr("OndoHttpClient")
            .unwrap()
            .call1((sandbox(&module),))
            .expect("constructing OndoHttpClient must perform no I/O");

        let with_timeout = module
            .getattr("OndoHttpClient")
            .unwrap()
            .call1((sandbox(&module), 30_u64))
            .expect("the timeout is a positional argument after the environment");

        for name in [
            "get_status",
            "get_markets",
            "get_contracts",
            "load_instrument_definitions",
        ] {
            assert!(
                client.getattr(name).unwrap().is_callable(),
                "OndoHttpClient.{name} must be callable",
            );
            assert!(
                with_timeout.getattr(name).unwrap().is_callable(),
                "OndoHttpClient.{name} must be callable with an explicit timeout",
            );
        }

        for absent in ["post_raw", "get_raw", "submit_order", "revoke_integrator"] {
            assert!(
                !client.hasattr(absent).unwrap(),
                "{absent} must not be on the public Python surface",
            );
        }
    });
}

/// The private account session's two configuration members reach Python, and the credential still
/// does not.
///
/// The execution configuration is the one surface that takes an API key pair, so every addition to
/// it is a place a secret could start being readable. `base_url_ws` and `account_read_only` are
/// settings, not credentials: they say where the account session comes up and whether it may place
/// an order, and the assertion that neither half of the key pair is readable is made again here,
/// next to them, rather than only where the surface was first pinned (plan §R3.1).
#[rstest]
fn test_the_private_session_is_configurable_from_python_and_carries_no_secret() {
    setup_data_event_sender();
    Python::initialize();

    Python::attach(|py| {
        // The scratch module, not the process-global registration: the factory extractor is
        // registered once per process and another test already owns that.
        let module = scratch_module(py);

        let config_type = module
            .getattr("OndoExecutionClientConfig")
            .expect("the execution config is exported");

        let kwargs = PyDict::new(py);
        kwargs
            .set_item("base_url_ws", "ws://127.0.0.1:8080/ws")
            .expect("the keyword is settable");
        kwargs
            .set_item("account_read_only", true)
            .expect("the keyword is settable");

        let instance = config_type
            .call((), Some(&kwargs))
            .expect("the execution config accepts the private session's settings");

        let config: OndoExecutionClientConfig = instance
            .extract()
            .expect("the config extracts back into Rust");

        assert_eq!(
            config.base_url_ws.as_deref(),
            Some("ws://127.0.0.1:8080/ws"),
            "the private endpoint is a configuration member",
        );
        assert!(config.account_read_only);
        assert_eq!(config.ws_url(), "ws://127.0.0.1:8080/ws");
        assert_eq!(config.stream_mode(), PrivateStreamMode::ReadOnly);

        assert_eq!(
            instance
                .getattr("base_url_ws")
                .expect("the private endpoint is readable")
                .extract::<String>()
                .expect("it is a string"),
            "ws://127.0.0.1:8080/ws",
        );
        assert!(
            instance
                .getattr("account_read_only")
                .expect("the read-only flag is readable")
                .extract::<bool>()
                .expect("it is a bool"),
        );

        // The rendering names the session and never a credential, and neither half of the key pair
        // has become readable by being next to it.
        let rendered = instance.repr().expect("the config renders").to_string();

        assert!(rendered.contains("account_read_only"), "{rendered}");
        assert!(rendered.contains("base_url_ws"), "{rendered}");

        for secret in ["api_key", "api_secret", "key_id", "secret"] {
            assert!(
                !instance.hasattr(secret).unwrap(),
                "OndoExecutionClientConfig must not expose `{secret}` to Python",
            );
        }
    });
}
