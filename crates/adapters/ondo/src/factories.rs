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
//! [`OndoExecutionClientConfig`], and each owns the one REST budget every client it creates draws
//! on.
//!
//! # The shared REST budget (plan §4.4)
//!
//! The budget is per adapter **environment**, not per client. One process creates one
//! [`OndoRateBudget`] and hands a clone to the data factory and to the execution factory of the same
//! environment, so a metadata read and a cancel cannot each hold half of the venue's limit.
//! [`OndoDataClientFactory::with_budget`] and [`OndoExecutionClientFactory::with_budget`] are those
//! injection points; a factory built with `new` owns an independent one-second budget and shares it
//! with every client it creates.
//!
//! # Credentials
//!
//! The data factory carries none, and its configuration has no member that could hold one. The
//! execution factory carries none either: the credential is resolved inside
//! [`OndoExecutionClient::new`], from the configuration's own pair or from the process environment
//! through the environment gate, and never travels through a factory.

use std::{any::Any, cell::RefCell, rc::Rc};

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

use crate::{
    common::consts::{ONDO, ONDO_VENUE},
    config::{OndoDataClientConfig, OndoExecutionClientConfig},
    data::OndoDataClient,
    execution::OndoExecutionClient,
    http::rate_limit::OndoRateBudget,
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
}

impl Default for OndoDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoDataClientFactory {
    /// Creates a factory whose clients draw on one independent default REST budget.
    #[must_use]
    pub fn new() -> Self {
        Self {
            budget: OndoRateBudget::new(),
        }
    }

    /// Creates a factory whose clients draw on `budget`.
    ///
    /// Pass a clone of the environment's budget (see the module documentation) so every client of
    /// the same environment paces against the same bucket.
    #[must_use]
    pub fn with_budget(budget: OndoRateBudget) -> Self {
        Self { budget }
    }

    /// Returns the budget every client this factory creates draws on.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        &self.budget
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

        let client = OndoDataClient::new(ClientId::from(name), ondo_config, self.budget.clone())?;

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
}

impl Default for OndoExecutionClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoExecutionClientFactory {
    /// Creates a factory whose clients draw on one independent default REST budget.
    #[must_use]
    pub fn new() -> Self {
        Self {
            budget: OndoRateBudget::new(),
        }
    }

    /// Creates a factory whose clients draw on `budget`.
    ///
    /// Pass a clone of the environment's budget - the same instance the data factory holds - so one
    /// process has one budget rather than one per surface (plan §4.4).
    #[must_use]
    pub fn with_budget(budget: OndoRateBudget) -> Self {
        Self { budget }
    }

    /// Returns the budget every client this factory creates draws on.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        &self.budget
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

        let client = OndoExecutionClient::with_credential(
            core,
            ondo_config,
            None, // credential: resolved by the client, behind the environment gate
            Some(self.budget.clone()),
        )?;

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
    use std::sync::Arc;

    use nautilus_common::{
        cache::Cache, clock::TestClock, live::runner::replace_data_event_sender,
        messages::DataEvent,
    };
    use nautilus_model::identifiers::{AccountId, InstrumentId};
    use rstest::rstest;

    use super::*;
    use crate::common::{consts::ONDO_VENUE, enums::OndoEnvironment};

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
}
