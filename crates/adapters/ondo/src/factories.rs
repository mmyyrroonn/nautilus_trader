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

//! Factories for creating the Ondo Perps clients.
//!
//! [`OndoDataClientFactory`] and [`OndoExecutionClientFactory`] are the registration seams the live
//! node's client registry and the Python projection consume: they build an [`OndoDataClient`] from
//! an [`OndoDataClientConfig`] and an [`OndoExecutionClient`] from an
//! [`OndoExecutionClientConfig`], and each hands every client it creates the one REST budget that
//! client's environment draws on.
//!
//! # The shared REST budget (plan §4.4)
//!
//! The budget is per adapter **environment**, not per client, so a metadata read and a cancel cannot
//! each hold half of the venue's limit. The resolution happens where the environment is known -
//! `create`, from the configuration the registry hands the factory - and it is
//! [`shared_rest_budget`] that answers: a factory built with `new` is bound to the default
//! environment and its clients draw on that environment's process-wide bucket, while a configuration
//! naming the other environment draws on the other one. That is what makes the two surfaces of one
//! environment share one budget through the live node's *actual* wiring - `OndoDataClientFactory()`
//! and `OndoExecutionClientFactory()` in Python, each handed its own configuration - rather than
//! through a caller remembering to pass one instance to both.
//!
//! [`OndoDataClientFactory::with_budget`] and [`OndoExecutionClientFactory::with_budget`] remain the
//! explicit injection points: a budget handed in there is the instance every client of that factory
//! draws on, whatever environment its configuration names, which is how a test or a Rust embedder
//! pins one bucket of its own.
//!
//! # Credentials
//!
//! The data factory carries none, and its configuration has no member that could hold one. The
//! execution factory carries none either: the credential is resolved inside
//! [`OndoExecutionClient::new`], from the configuration's own pair or from the process environment
//! through the environment gate, and never travels through a factory.

use std::{any::Any, cell::RefCell, rc::Rc, sync::Arc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};
use parking_lot::RwLock;

use crate::{
    common::{
        consts::{ONDO, ONDO_VENUE},
        enums::OndoEnvironment,
    },
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
    data::OndoDataClient,
    diagnostics::OndoReadOnlyDiagnostics,
    execution::OndoExecutionClient,
    http::rate_limit::{OndoRateBudget, shared_rest_budget},
};

/// Makes the data client configuration usable through the factory's generic configuration surface.
impl ClientConfig for OndoDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Makes the execution client configuration usable through the factory's generic surface.
impl ClientConfig for OndoExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Ondo Perps data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object,)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoDataClientFactory {
    budget: OndoRateBudget,
    environment: Option<OndoEnvironment>,
}

impl Default for OndoDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoDataClientFactory {
    /// Creates a factory bound to the default environment's process-wide REST budget.
    ///
    /// This is the constructor the Python registration path uses, and it binds
    /// [`OndoEnvironment::default`] - production - because a factory is built before any
    /// configuration reaches it. A configuration naming the other environment resolves *that*
    /// environment's bucket at [`DataClientFactory::create`], so a data client and an execution
    /// client of one environment share one budget (plan §4.4) and two environments never do.
    #[must_use]
    pub fn new() -> Self {
        Self::bound_to(OndoEnvironment::default())
    }

    /// Creates a factory whose clients draw on `budget`, whatever environment they name.
    ///
    /// A budget handed in here is used as-is: it is the instance every client this factory creates
    /// draws on, which is how a test or an embedder pins one bucket of its own. The environment of
    /// the configuration is then not an isolation key, so an injected budget must never be shared
    /// between two environments.
    #[must_use]
    pub fn with_budget(budget: OndoRateBudget) -> Self {
        Self {
            budget,
            environment: None,
        }
    }

    /// Returns the budget this factory was constructed with.
    ///
    /// For a factory built with [`Self::with_budget`] this is the exact instance every client it
    /// creates draws on. For one built with [`Self::new`] it is the default environment's
    /// process-wide budget, which is what its clients draw on for that environment; a configuration
    /// naming the other environment resolves the other bucket at `create` (see
    /// [`shared_rest_budget`]), and the two are then never the same instance.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        &self.budget
    }

    fn bound_to(environment: OndoEnvironment) -> Self {
        Self {
            budget: shared_rest_budget(environment),
            environment: Some(environment),
        }
    }

    /// The budget a client created for `environment` draws on.
    fn budget_for(&self, environment: OndoEnvironment) -> OndoRateBudget {
        match self.environment {
            Some(bound) if bound == environment => self.budget.clone(),
            Some(_) => shared_rest_budget(environment),
            None => self.budget.clone(),
        }
    }
}

impl DataClientFactory for OndoDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        // The data client owns its metadata store and never reads the platform cache during
        // construction, so the cache view and the node clock are not threaded any further. The
        // client's own timestamps come from the atomic realtime clock it subscribes to in `new`.
        let ondo_config = config
            .as_any()
            .downcast_ref::<OndoDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for OndoDataClientFactory. Expected OndoDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let budget = self.budget_for(ondo_config.environment);
        let client = OndoDataClient::new(ClientId::from(name), ondo_config, budget)?;

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ONDO
    }

    fn config_type(&self) -> &'static str {
        "OndoDataClientConfig"
    }
}

/// Factory for creating Ondo Perps execution clients.
///
/// The client it builds is the native Rust [`OndoExecutionClient`] behind the
/// [`ExecutionClient`](nautilus_common::clients::ExecutionClient) trait, exactly as Aster's and
/// Lighter's are: a Python `LiveNode` wires the adapter through this factory and never through a
/// second execution implementation.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object,)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoExecutionClientFactory {
    budget: OndoRateBudget,
    environment: Option<OndoEnvironment>,
    /// The most recent execution client's read-only diagnostics, when one has been created.
    ///
    /// One factory is built per node and one execution client per node, so this is the current
    /// run's handle rather than a cross-run accumulator. The Python surface reads a snapshot from
    /// it and has no way to write it.
    read_only_diagnostics: Arc<RwLock<Option<OndoReadOnlyDiagnostics>>>,
    production: Arc<RwLock<Option<Arc<crate::production::ProductionAuthority>>>>,
}

impl Default for OndoExecutionClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoExecutionClientFactory {
    /// Creates a factory bound to the default environment's process-wide REST budget.
    ///
    /// This is the constructor the Python registration path uses. The execution configuration
    /// defaults to the *sandbox* environment, so a factory built here and handed a configuration
    /// that names sandbox resolves the sandbox bucket at [`ExecutionClientFactory::create`] - the
    /// same one a data client configured for sandbox draws on, and never the production one.
    #[must_use]
    pub fn new() -> Self {
        Self::bound_to(OndoEnvironment::default())
    }

    /// Creates a factory whose clients draw on `budget`, whatever environment they name.
    ///
    /// Pass a clone of the environment's budget - the same instance the data factory holds - so one
    /// process has one budget rather than one per surface (plan §4.4). An injected budget is used
    /// as-is, so it must never be shared between two environments.
    #[must_use]
    pub fn with_budget(budget: OndoRateBudget) -> Self {
        Self {
            budget,
            environment: None,
            read_only_diagnostics: Arc::new(RwLock::new(None)),
            production: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns the budget this factory was constructed with.
    ///
    /// See [`OndoDataClientFactory::budget`]: an injected instance is the one every client draws on,
    /// while a factory built with [`Self::new`] reports the default environment's process-wide
    /// budget and resolves another environment's at `create`.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        &self.budget
    }

    /// Returns a sanitized snapshot of the most recent execution client's diagnostics.
    ///
    /// [`None`] when this factory has not created an execution client yet. The snapshot carries
    /// counters, fixed channel labels, the run state, the account identity and the owned shutdown
    /// status only; it cannot carry a frame, a credential, an account or order id, or an amount.
    #[must_use]
    pub fn read_only_snapshot(&self) -> Option<crate::diagnostics::OndoReadOnlySnapshot> {
        self.read_only_diagnostics
            .read()
            .as_ref()
            .map(OndoReadOnlyDiagnostics::snapshot)
    }

    /// Returns the current run's native production evidence, or no completed snapshot.
    #[must_use]
    pub fn production_trade_snapshot(&self) -> Option<serde_json::Value> {
        self.production
            .read()
            .as_ref()
            .and_then(|guard| guard.snapshot())
    }

    fn bound_to(environment: OndoEnvironment) -> Self {
        Self {
            budget: shared_rest_budget(environment),
            environment: Some(environment),
            read_only_diagnostics: Arc::new(RwLock::new(None)),
            production: Arc::new(RwLock::new(None)),
        }
    }

    /// The budget a client created for `environment` draws on.
    fn budget_for(&self, environment: OndoEnvironment) -> OndoRateBudget {
        match self.environment {
            Some(bound) if bound == environment => self.budget.clone(),
            Some(_) => shared_rest_budget(environment),
            None => self.budget.clone(),
        }
    }
}

impl ExecutionClientFactory for OndoExecutionClientFactory {
    /// Builds the execution client from its configuration.
    ///
    /// The account id is required and refused here rather than defaulted: a report attributed to
    /// the wrong account is worse than no report. The environment gate and the credential
    /// resolution run inside [`OndoExecutionClient::new`], before any socket exists.
    ///
    /// # Errors
    ///
    /// Returns an error for a configuration of another type, for one with no account id, and for
    /// everything [`OndoExecutionClient::new`] refuses - production, `allow_production_orders`, and
    /// a missing credential.
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let ondo_config = config
            .as_any()
            .downcast_ref::<OndoExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for OndoExecutionClientFactory. Expected OndoExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();

        let account_id = ondo_config.account_id.ok_or_else(|| {
            anyhow::anyhow!(
                "OndoExecutionClientConfig requires an `account_id`: an execution client reports \
                 for exactly one account",
            )
        })?;

        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *ONDO_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None, // base_currency: the venue settles in USDC, which the account state carries
            cache,
        );

        let budget = self.budget_for(ondo_config.environment);
        let client = OndoExecutionClient::with_credential(
            core,
            ondo_config,
            None, // credential: resolved by the client, behind the environment gate
            Some(budget),
        )?;

        self.read_only_diagnostics
            .write()
            .replace(client.read_only_diagnostics());
        *self.production.write() = client.production_authority();

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ONDO
    }

    fn config_type(&self) -> &'static str {
        "OndoExecutionClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, task::Poll, time::Duration};

    use futures_util::poll;
    use nautilus_common::{
        cache::Cache, clock::TestClock, live::runner::replace_data_event_sender,
        messages::DataEvent,
    };
    use nautilus_model::identifiers::{AccountId, InstrumentId};
    use rstest::rstest;

    use super::*;
    use crate::{
        common::{consts::ONDO_VENUE, enums::OndoEnvironment},
        http::rate_limit::{ONDO_REST_BUCKET, shared_rest_budget},
    };

    /// A configuration type this factory does not own, which is what a mis-wired node hands it.
    #[derive(Debug)]
    struct ForeignConfig;

    impl ClientConfig for ForeignConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// Installs the process data event channel the data client publishes into.
    ///
    /// `OndoDataClient::new` reads the channel at construction, exactly as it does inside a running
    /// node; a test builds the client only after this returns, and keeps the receiver alive for the
    /// client's lifetime.
    fn install_data_event_channel() -> tokio::sync::mpsc::UnboundedReceiver<DataEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);

        rx
    }

    #[rstest]
    fn test_the_factory_names_the_venue_and_the_configuration_it_accepts() {
        let factory = OndoDataClientFactory::new();

        assert_eq!(factory.name(), ONDO);
        assert_eq!(factory.config_type(), "OndoDataClientConfig");
        assert_eq!(ONDO, ONDO_VENUE.as_str());
    }

    #[rstest]
    fn test_the_configuration_is_reachable_through_the_generic_client_config_surface() {
        let boxed: Box<dyn ClientConfig> = Box::new(OndoDataClientConfig::default());

        assert!(
            boxed
                .as_any()
                .downcast_ref::<OndoDataClientConfig>()
                .is_some(),
            "the factory's downcast is the one the registry relies on"
        );
    }

    #[rstest]
    fn test_the_factory_constructs_a_client_from_its_configuration() {
        let _events = install_data_event_channel();
        let config = OndoDataClientConfig::builder()
            .load_ids(vec![InstrumentId::from("NVDA-USD-PERP.ONDO")])
            .build();

        let client = OndoDataClientFactory::new()
            .create(
                "ONDO-TEST",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .expect("the factory builds a data client from its own configuration type");

        assert_eq!(client.client_id(), ClientId::from("ONDO-TEST"));
        assert_eq!(client.venue(), Some(*ONDO_VENUE));
        assert!(
            !client.is_connected(),
            "the factory constructs a client, it does not connect one"
        );
    }

    #[rstest]
    fn test_the_factory_rejects_a_configuration_it_does_not_own() {
        let _events = install_data_event_channel();
        let created = OndoDataClientFactory::new().create(
            "ONDO-TEST",
            &ForeignConfig,
            Rc::new(RefCell::new(Cache::default())).into(),
            Rc::new(RefCell::new(TestClock::new())),
        );

        let error = match created {
            Ok(_) => panic!("a foreign configuration is a wiring error, not a client"),
            Err(e) => e,
        };

        assert!(
            error.to_string().contains("Invalid config type"),
            "was `{error}`"
        );
        assert!(
            error.to_string().contains("OndoDataClientConfig"),
            "the error names what it expected, was `{error}`"
        );
    }

    /// Whether the shared bucket would admit one more request right now, without waiting for it.
    ///
    /// `check_key` is the same decision `OndoRateBudget::acquire` makes before it waits, so this
    /// reads the bucket's state rather than changing it.
    fn slot_is_free(budget: &OndoRateBudget) -> bool {
        budget
            .limiter()
            .check_key(&ustr::Ustr::from(ONDO_REST_BUCKET))
            .is_ok()
    }

    /// A data configuration naming `environment` whose base URL nothing is listening on.
    ///
    /// The read this client makes at `connect` therefore fails at the transport, which is what lets
    /// a test tell "waited for the budget" from "went out": nothing here reaches the venue.
    fn data_config_for(environment: OndoEnvironment) -> OndoDataClientConfig {
        OndoDataClientConfig::builder()
            .environment(environment)
            .load_ids(vec![InstrumentId::from("NVDA-USD-PERP.ONDO")])
            .base_url_http("http://127.0.0.1:9".to_string())
            .build()
    }

    /// An execution configuration that builds without touching the process environment.
    ///
    /// The credential is the plan's own fixture pair - the same fake key the signing tests use -
    /// and the base URL is a local host, so nothing here can reach the venue even if a request
    /// were made (none is: constructing a client opens no socket).
    fn exec_config() -> OndoExecutionClientConfig {
        OndoExecutionClientConfig {
            environment: OndoEnvironment::Sandbox,
            account_id: Some(AccountId::from("ONDO-SANDBOX-001")),
            api_key: Some("ondoKeyId_UNIT_TEST_ONLY".to_string()),
            api_secret: Some("ondoApiSecret_UNIT_TEST_ONLY".to_string()),
            base_url_http: Some("http://127.0.0.1:9".to_string()),
            ..Default::default()
        }
    }

    #[rstest]
    fn test_the_execution_factory_names_the_venue_and_the_configuration_it_accepts() {
        let factory = OndoExecutionClientFactory::new();

        assert_eq!(factory.name(), ONDO);
        assert_eq!(factory.config_type(), "OndoExecutionClientConfig");
    }

    #[rstest]
    fn test_the_execution_factory_constructs_the_native_client_from_its_configuration() {
        let client = OndoExecutionClientFactory::new()
            .create(
                TraderId::from("TESTER-001"),
                "ONDO-EXEC",
                &exec_config(),
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .expect("the factory builds an execution client from its own configuration type");

        assert_eq!(client.client_id(), ClientId::from("ONDO-EXEC"));
        assert_eq!(client.venue(), *ONDO_VENUE);
        assert_eq!(client.oms_type(), OmsType::Netting);
        assert_eq!(client.account_id(), AccountId::from("ONDO-SANDBOX-001"));
        assert!(
            !client.is_connected(),
            "the factory constructs a client, it does not connect one",
        );
        assert!(
            !client.provides_bulk_position_coverage(InstrumentId::from("NVDA-USD-PERP.ONDO")),
            "an absent position report is not evidence of a flat account before Task 8",
        );
    }

    /// The factory retains a sanitized snapshot of the client it created. It starts from the safe
    /// defaults: the configured run token, no accepted login, `disconnected`, `unknown` identity
    /// and `not_attempted` shutdown.
    #[rstest]
    fn test_the_execution_factory_retains_a_sanitized_snapshot_of_the_client_it_created() {
        let factory = OndoExecutionClientFactory::new();

        assert!(
            factory.read_only_snapshot().is_none(),
            "a factory that has created no client has no snapshot",
        );

        let config = OndoExecutionClientConfig {
            account_read_only: true,
            diagnostics_run_id: Some("run-alpha".to_string()),
            ..exec_config()
        };
        let _client = factory
            .create(
                TraderId::from("TESTER-001"),
                "ONDO-EXEC",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .expect("the factory builds the native client");

        let snapshot = factory
            .read_only_snapshot()
            .expect("the factory retained the client's diagnostics handle");

        assert_eq!(snapshot.run_id, "run-alpha");
        assert!(!snapshot.logged_in, "no login has been accepted");
        assert!(snapshot.subscriptions_acked.is_empty());
        assert_eq!(snapshot.run_state, "disconnected");
        assert_eq!(snapshot.reconnects, 0);
        assert_eq!(snapshot.recoveries, 0);
        assert_eq!(snapshot.account_state_events, 0);
        assert_eq!(snapshot.identity_match, "unknown");
        assert_eq!(snapshot.shutdown_status, "not_attempted");
    }

    /// A factory clone shares the diagnostics store with the original, so the handle the registry's
    /// copy retains is the handle the node's client reports through.
    #[rstest]
    fn test_a_cloned_execution_factory_reads_the_same_diagnostics_store() {
        let factory = OndoExecutionClientFactory::new();
        let clone = factory.clone();

        let config = OndoExecutionClientConfig {
            account_read_only: true,
            diagnostics_run_id: Some("run-clone".to_string()),
            ..exec_config()
        };
        let _client = clone
            .create(
                TraderId::from("TESTER-001"),
                "ONDO-EXEC",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .expect("the clone builds the native client");

        let snapshot = factory
            .read_only_snapshot()
            .expect("the original sees the clone's client");

        assert_eq!(snapshot.run_id, "run-clone");
    }

    /// Two runs through one factory are isolated: the second client replaces the first run's store,
    /// so a snapshot is the current run's and its counters do not accumulate.
    #[rstest]
    fn test_two_runs_through_one_factory_are_isolated() {
        let factory = OndoExecutionClientFactory::new();

        let first_config = OndoExecutionClientConfig {
            account_read_only: true,
            diagnostics_run_id: Some("run-first".to_string()),
            ..exec_config()
        };
        let first = factory
            .create(
                TraderId::from("TESTER-001"),
                "ONDO-EXEC",
                &first_config,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .expect("the first run builds");
        drop(first);

        assert_eq!(
            factory.read_only_snapshot().unwrap().run_id,
            "run-first",
            "the snapshot survives the client object",
        );

        let second_config = OndoExecutionClientConfig {
            account_read_only: true,
            diagnostics_run_id: Some("run-second".to_string()),
            ..exec_config()
        };
        let _second = factory
            .create(
                TraderId::from("TESTER-001"),
                "ONDO-EXEC",
                &second_config,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .expect("the second run builds");

        let snapshot = factory.read_only_snapshot().unwrap();

        assert_eq!(snapshot.run_id, "run-second");
        assert!(
            !snapshot.logged_in && snapshot.reconnects == 0 && snapshot.recoveries == 0,
            "the second run does not inherit the first run's counters",
        );
    }

    #[rstest]
    fn test_the_execution_factory_refuses_a_configuration_without_an_account() {
        let config = OndoExecutionClientConfig {
            account_id: None,
            ..exec_config()
        };

        let created = OndoExecutionClientFactory::new().create(
            TraderId::from("TESTER-001"),
            "ONDO-EXEC",
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
        );
        let error = match created {
            Ok(_) => panic!("an execution client reports for exactly one account"),
            Err(e) => e,
        };

        assert!(error.to_string().contains("account_id"), "was `{error}`");
    }

    #[rstest]
    fn test_the_execution_factory_refuses_production_order_entry() {
        let config = OndoExecutionClientConfig {
            allow_production_orders: true,
            ..exec_config()
        };

        let created = OndoExecutionClientFactory::new().create(
            TraderId::from("TESTER-001"),
            "ONDO-EXEC",
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
        );
        let error = match created {
            Ok(_) => panic!("there is no production write branch to open"),
            Err(e) => e,
        };

        assert!(
            error.to_string().contains("allow_production_orders"),
            "was `{error}`",
        );
    }

    #[rstest]
    fn test_the_execution_factory_refuses_a_configuration_it_does_not_own() {
        let created = OndoExecutionClientFactory::new().create(
            TraderId::from("TESTER-001"),
            "ONDO-EXEC",
            &ForeignConfig,
            Rc::new(RefCell::new(Cache::default())).into(),
        );

        let error = match created {
            Ok(_) => panic!("a foreign configuration is a wiring error, not a client"),
            Err(e) => e,
        };

        assert!(
            error.to_string().contains("OndoExecutionClientConfig"),
            "the error names what it expected, was `{error}`",
        );
    }

    #[rstest]
    fn test_the_execution_factory_hands_one_rest_budget_to_every_client_it_creates() {
        let budget = OndoRateBudget::new();
        let factory = OndoExecutionClientFactory::with_budget(budget.clone());

        assert!(
            Arc::ptr_eq(factory.budget().limiter(), budget.limiter()),
            "the injected budget is the instance the factory holds, not an equivalent one",
        );
        assert!(
            !Arc::ptr_eq(factory.budget().limiter(), OndoRateBudget::new().limiter()),
            "two environments - or two independent factories - never contend for one bucket",
        );
    }

    #[rstest]
    fn test_the_factory_hands_one_rest_budget_to_every_client_it_creates() {
        let _events = install_data_event_channel();
        let budget = OndoRateBudget::new();
        let factory = OndoDataClientFactory::with_budget(budget.clone());

        assert!(
            Arc::ptr_eq(factory.budget().limiter(), budget.limiter()),
            "the injected budget is the instance the factory holds, not an equivalent one"
        );
        assert!(
            Arc::ptr_eq(
                factory.budget().limiter(),
                factory.clone().budget().limiter()
            ),
            "a cloned factory (the registry's copy) still spends from the same bucket"
        );
        assert!(
            !Arc::ptr_eq(factory.budget().limiter(), OndoRateBudget::new().limiter()),
            "two environments - or two independent factories - never contend for one bucket"
        );
    }

    // --------------------------------------------------------------------------------------------
    // The Python registration path: two factories, two configurations, one environment budget
    // --------------------------------------------------------------------------------------------

    /// The resolution `create` performs, from both factories, for the environment they are handed.
    ///
    /// This is the F13 seam: the live node builds `OndoDataClientFactory()` and
    /// `OndoExecutionClientFactory()` in Python, and each is then handed its own configuration. The
    /// environment is a member of *that configuration*, so it is what decides the bucket - and both
    /// surfaces of one environment must land on the same instance without a caller passing one.
    #[rstest]
    fn test_both_factories_resolve_one_budget_per_environment() {
        let data_factory = OndoDataClientFactory::new();
        let exec_factory = OndoExecutionClientFactory::new();

        for environment in [OndoEnvironment::Production, OndoEnvironment::Sandbox] {
            let from_data = data_factory.budget_for(environment);
            let from_exec = exec_factory.budget_for(environment);

            assert!(
                Arc::ptr_eq(from_data.limiter(), from_exec.limiter()),
                "one environment is one budget, whichever factory a configuration reaches"
            );
            assert!(
                Arc::ptr_eq(
                    from_data.limiter(),
                    shared_rest_budget(environment).limiter()
                ),
                "the budget is the environment's process-wide one, not one the factory minted"
            );
        }

        assert!(
            !Arc::ptr_eq(
                data_factory.budget_for(OndoEnvironment::Sandbox).limiter(),
                data_factory
                    .budget_for(OndoEnvironment::Production)
                    .limiter(),
            ),
            "two environments never contend for one bucket"
        );
        assert!(
            Arc::ptr_eq(
                data_factory
                    .budget_for(OndoEnvironment::Production)
                    .limiter(),
                data_factory.budget().limiter()
            ),
            "a factory built with `new` is bound to the default environment, which is production"
        );
    }

    /// An injected budget is the instance every configuration uses, whatever environment it names.
    #[rstest]
    fn test_an_injected_budget_outranks_the_environment_a_configuration_names() {
        let budget = OndoRateBudget::new();
        let data_factory = OndoDataClientFactory::with_budget(budget.clone());
        let exec_factory = OndoExecutionClientFactory::with_budget(budget.clone());

        for environment in [OndoEnvironment::Production, OndoEnvironment::Sandbox] {
            assert!(Arc::ptr_eq(
                data_factory.budget_for(environment).limiter(),
                budget.limiter()
            ));
            assert!(Arc::ptr_eq(
                exec_factory.budget_for(environment).limiter(),
                budget.limiter()
            ));
        }
    }

    /// The client a factory actually built is paced by the bucket of the environment it named.
    ///
    /// Nothing here hands the client a budget. Both factories are constructed the way the Python
    /// registration path constructs them (`new`, with no argument), the configuration names the
    /// environment they were *not* bound to, and the client's read is the only thing in this test
    /// that draws on that environment's bucket - so the cell it spends is the cell this asserts is
    /// gone. A client that had been given a budget of its own, or the production one its factory is
    /// bound to, would leave the sandbox bucket untouched and go out instead.
    #[tokio::test(start_paused = true)]
    async fn test_the_client_a_factory_built_waits_on_the_bucket_of_the_environment_it_named() {
        let _events = install_data_event_channel();
        let data_factory = OndoDataClientFactory::new();
        let exec_factory = OndoExecutionClientFactory::new();
        let sandbox = OndoEnvironment::Sandbox;

        let mut client = data_factory
            .create(
                "ONDO-TEST",
                &data_config_for(sandbox),
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .expect("the factory builds a data client from its own configuration type");

        let sandbox_budget = exec_factory.budget_for(sandbox);
        assert!(
            slot_is_free(&sandbox_budget),
            "the environment's bucket holds its one cell before the read"
        );

        // The connect's metadata read is what takes that cell, and the future is driven by hand - it
        // is not spawned - so these polls are what move it. `poll` rather than a timeout keeps the
        // clock out of the assertion: with time paused, a timeout would need a timer that cannot
        // fire.
        let mut connect = Box::pin(client.connect());
        for _ in 0..16 {
            assert!(
                poll!(connect.as_mut()).is_pending(),
                "the metadata read waits for the environment's bucket instead of running ahead of it"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            !slot_is_free(&sandbox_budget),
            "the read drew the environment's cell, not one of its own"
        );

        // With the cell back, the read goes out - and nothing answers on the local port.
        tokio::time::advance(Duration::from_secs(1)).await;

        let mut refused = None;
        for _ in 0..16 {
            match poll!(connect.as_mut()) {
                Poll::Ready(result) => {
                    refused = Some(result);

                    break;
                }
                Poll::Pending => {}
            }
            tokio::task::yield_now().await;
        }
        let refused = match refused {
            Some(result) => result,
            None => connect.await,
        };
        assert!(
            refused.is_err(),
            "the read went out and nothing answers on the local port"
        );

        // The other environment's bucket was never touched by any of it.
        assert!(
            slot_is_free(&data_factory.budget_for(OndoEnvironment::Production)),
            "the factory's own environment keeps its cell: the read was paced by the sandbox one"
        );
    }
}
