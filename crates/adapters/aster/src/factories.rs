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

//! Factory functions for creating Aster clients and components.

use std::{cell::RefCell, rc::Rc};

use nautilus_binance::{
    common::enums::BinanceProductType, futures::data::BinanceFuturesDataClient,
};
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::consts::ASTER, config::AsterDataClientConfig};

/// Factory for creating Aster data clients.
///
/// Aster reuses the Binance USD-M Futures data client with Aster endpoints and the
/// `ASTER` venue; no Aster-specific protocol code is involved.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.aster", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.aster")
)]
pub struct AsterDataClientFactory;

impl AsterDataClientFactory {
    /// Creates a new [`AsterDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for AsterDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let aster_config = config
            .as_any()
            .downcast_ref::<AsterDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AsterDataClientFactory. Expected AsterDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let binance_config = aster_config.to_binance();
        binance_config.validate()?;

        let client =
            BinanceFuturesDataClient::new(client_id, binance_config, BinanceProductType::UsdM)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ASTER
    }

    fn config_type(&self) -> &'static str {
        stringify!(AsterDataClientConfig)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_binance::config::BinanceInstrumentProviderConfig;
    use nautilus_common::{
        cache::Cache, clock::TestClock, live::runner::replace_data_event_sender,
        messages::DataEvent,
    };
    use nautilus_model::identifiers::Venue;
    use rstest::rstest;

    use super::*;
    use crate::common::consts::ASTER_VENUE;

    fn data_config() -> AsterDataClientConfig {
        AsterDataClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec!["BTCUSDT-PERP.ASTER".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[rstest]
    fn test_aster_data_client_factory_creation() {
        let factory = AsterDataClientFactory::new();
        assert_eq!(factory.name(), ASTER);
        assert_eq!(factory.config_type(), "AsterDataClientConfig");
    }

    #[rstest]
    fn test_aster_data_client_factory_rejects_wrong_config_type() {
        let factory = AsterDataClientFactory::new();
        let wrong_config = nautilus_binance::config::BinanceDataClientConfig::default();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result = factory.create("ASTER-TEST", &wrong_config, cache.into(), clock);

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Invalid config type")
        );
    }

    #[rstest]
    fn test_aster_data_client_uses_aster_venue() {
        let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(data_tx);

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let client = AsterDataClientFactory::new()
            .create("ASTER-DATA", &data_config(), cache.into(), clock)
            .expect("expected data client to construct");

        assert_eq!(client.client_id(), ClientId::from("ASTER-DATA"));
        assert_eq!(client.venue(), Some(*ASTER_VENUE));
        assert!(!client.is_connected());
    }

    #[rstest]
    fn test_aster_data_client_honours_venue_override() {
        let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(data_tx);

        let venue = Venue::from("ASTER_CUSTOM");
        let config = AsterDataClientConfig {
            instrument_provider: BinanceInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec!["BTCUSDT-PERP.ASTER_CUSTOM".to_string()]),
                ..Default::default()
            },
            venue: Some(venue),
            ..Default::default()
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let client = AsterDataClientFactory::new()
            .create("ASTER-CUSTOM", &config, cache.into(), clock)
            .expect("expected data client to construct");

        assert_eq!(client.venue(), Some(venue));
    }
}
