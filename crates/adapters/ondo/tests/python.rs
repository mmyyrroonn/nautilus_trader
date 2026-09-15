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

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache, clock::TestClock, live::runner::replace_data_event_sender, messages::DataEvent,
};
use nautilus_model::identifiers::{ClientId, InstrumentId, Venue};
use nautilus_ondo::{
    common::{
        consts::{ONDO, ONDO_VENUE},
        enums::OndoEnvironment,
    },
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
    factories::{OndoDataClientFactory, OndoExecutionClientFactory},
    http::client::OndoHttpClient,
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
