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

//! Offline public replay projection using the accepted native parser and domain data.

use nautilus_core::{UnixNanos, python::to_pyvalue_err};
use nautilus_model::{instruments::CryptoPerpetual, python::data::data_to_pyobject};
use pyo3::{prelude::*, types::PyBytes};

use super::config::PyBackpackDataClientConfig;
use crate::{models::BackpackMarket, parsing::parse_market, replay::BackpackPublicReplay};

/// Offline replay for one recorded allowlisted market; historical data is never live readiness.
#[pyclass(
    name = "BackpackPublicReplay",
    module = "nautilus_trader.adapters.backpack"
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.backpack")]
#[derive(Debug)]
pub struct PyBackpackPublicReplay {
    inner: BackpackPublicReplay,
    instrument: CryptoPerpetual,
}

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyBackpackPublicReplay {
    /// Validates recorded public market JSON and exact config economics without any I/O.
    /// Metadata receipt time and stream generation are supplied by the recorded session.
    #[new]
    #[pyo3(signature = (config, market_json, metadata_received_at_ns, *, generation=1))]
    fn py_new(
        config: &PyBackpackDataClientConfig,
        market_json: &str,
        metadata_received_at_ns: u64,
        generation: u64,
    ) -> PyResult<Self> {
        if market_json.len() > 1_048_576 {
            return Err(to_pyvalue_err(
                "Backpack replay metadata exceeds byte bound",
            ));
        }
        let market: BackpackMarket = serde_json::from_str(market_json)
            .map_err(|_| to_pyvalue_err("invalid Backpack replay metadata"))?;
        let metadata = parse_market(
            &market,
            config.inner.scope(),
            UnixNanos::from(metadata_received_at_ns),
        )
        .map_err(|_| to_pyvalue_err("invalid Backpack replay metadata"))?;
        let economics = config.inner.economics().get(metadata.raw_symbol.as_str());
        let instrument = metadata.to_instrument(economics).map_err(to_pyvalue_err)?;
        let inner = BackpackPublicReplay::new_checked(
            metadata,
            generation,
            config.inner.lifecycle().clone(),
        )
        .map_err(to_pyvalue_err)?;
        Ok(Self { inner, instrument })
    }

    /// Returns the actual native perpetual instrument with explicit economic provenance.
    #[getter]
    fn instrument(&self) -> CryptoPerpetual {
        self.instrument.clone()
    }

    /// Applies one bounded native JSONL record, retaining original event and receipt times.
    /// Duplicates, unavailable quotes and old generations return None. Invalid data raises.
    /// The caller must bound total records, elapsed work and output; this owner opens no file.
    #[gen_stub(override_return_type(
        type_repr = "nautilus_trader.model.QuoteTick | nautilus_trader.model.TradeTick | nautilus_trader.model.MarkPriceUpdate | nautilus_trader.model.OrderBookDeltas | None",
        imports = ("nautilus_trader.model",),
    ))]
    fn apply_record(
        &mut self,
        py: Python<'_>,
        record: &Bound<'_, PyBytes>,
    ) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .apply_record(record.as_bytes())
            .map_err(to_pyvalue_err)?
            .map(|data| data_to_pyobject(py, data))
            .transpose()
    }

    fn __repr__(&self) -> &'static str {
        "BackpackPublicReplay(offline)"
    }
}
