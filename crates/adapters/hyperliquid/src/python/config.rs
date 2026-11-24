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

//! Python bindings for Hyperliquid configuration.

use std::str::FromStr;

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::{
    data::QuoteTick,
    identifiers::{AccountId, InstrumentId},
};
use nautilus_network::websocket::TransportBackend;
use pyo3::prelude::*;

use crate::{
    common::enums::HyperliquidEnvironment,
    config::{HyperliquidDataClientConfig, HyperliquidExecutionClientConfig},
};

fn validated_ws_selection(
    candidates: Option<usize>,
    probe_ms: Option<u64>,
    default_candidates: usize,
    default_probe_ms: u64,
) -> PyResult<(usize, u64)> {
    let candidates = candidates.unwrap_or(default_candidates);
    if !(1..=12).contains(&candidates) {
        return Err(to_pyvalue_err("ws_select_candidates must be in 1..=12"));
    }
    let probe_ms = probe_ms.unwrap_or(default_probe_ms);
    if !(100..=15_000).contains(&probe_ms) {
        return Err(to_pyvalue_err("ws_select_probe_ms must be in 100..=15_000"));
    }
    Ok((candidates, probe_ms))
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl HyperliquidDataClientConfig {
    /// Configuration for the Hyperliquid data client.
    ///
    /// The `stale_stream_*` options control the stream health monitor. With recovery
    /// enabled, a stale stream is warned about first, targeted-resubscribed once per
    /// recovery cooldown (preserving its original `l2Book` options), and escalated to
    /// a full WebSocket reconnect after `stale_stream_max_targeted_resubscribes`
    /// failed attempts; fresh data resets the ladder. See the Hyperliquid integration
    /// guide ("Stream health and recovery") for details.
    #[new]
    #[pyo3(signature = (
        environment = None,
        private_key = None,
        base_url_ws = None,
        base_url_http = None,
        proxy_url = None,
        http_timeout_secs = None,
        ws_timeout_secs = None,
        update_instruments_interval_mins = None,
        transport_backend = None,
        stale_stream_receive_timeout_secs = None,
        stream_health_check_interval_secs = None,
        stale_stream_warning_cooldown_secs = None,
        stale_stream_recovery_enabled = None,
        stale_stream_recovery_cooldown_secs = None,
        stale_stream_max_targeted_resubscribes = None,
        bbo_price_changes_only = None,
        bbo_cache_only = None,
        ws_select_candidates = None,
        ws_select_probe_ms = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<HyperliquidEnvironment>,
        private_key: Option<String>,
        base_url_ws: Option<String>,
        base_url_http: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_timeout_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
        transport_backend: Option<TransportBackend>,
        stale_stream_receive_timeout_secs: Option<u64>,
        stream_health_check_interval_secs: Option<u64>,
        stale_stream_warning_cooldown_secs: Option<u64>,
        stale_stream_recovery_enabled: Option<bool>,
        stale_stream_recovery_cooldown_secs: Option<u64>,
        stale_stream_max_targeted_resubscribes: Option<u32>,
        bbo_price_changes_only: Option<bool>,
        bbo_cache_only: Option<bool>,
        ws_select_candidates: Option<usize>,
        ws_select_probe_ms: Option<u64>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let (ws_select_candidates, ws_select_probe_ms) = validated_ws_selection(
            ws_select_candidates,
            ws_select_probe_ms,
            defaults.ws_select_candidates,
            defaults.ws_select_probe_ms,
        )?;
        Ok(Self {
            private_key,
            base_url_ws,
            base_url_http,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_timeout_secs: ws_timeout_secs.unwrap_or(defaults.ws_timeout_secs),
            stale_stream_receive_timeout_secs: stale_stream_receive_timeout_secs
                .unwrap_or(defaults.stale_stream_receive_timeout_secs),
            stream_health_check_interval_secs: stream_health_check_interval_secs
                .unwrap_or(defaults.stream_health_check_interval_secs),
            stale_stream_warning_cooldown_secs: stale_stream_warning_cooldown_secs
                .unwrap_or(defaults.stale_stream_warning_cooldown_secs),
            stale_stream_recovery_enabled: stale_stream_recovery_enabled
                .unwrap_or(defaults.stale_stream_recovery_enabled),
            stale_stream_recovery_cooldown_secs: stale_stream_recovery_cooldown_secs
                .unwrap_or(defaults.stale_stream_recovery_cooldown_secs),
            stale_stream_max_targeted_resubscribes: stale_stream_max_targeted_resubscribes
                .unwrap_or(defaults.stale_stream_max_targeted_resubscribes),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
            transport_backend: transport_backend.unwrap_or(defaults.transport_backend),
            bbo_price_changes_only: bbo_price_changes_only
                .unwrap_or(defaults.bbo_price_changes_only),
            bbo_cache_only: bbo_cache_only.unwrap_or(defaults.bbo_cache_only),
            latest_bbo_cache: defaults.latest_bbo_cache,
            ws_select_candidates,
            ws_select_probe_ms,
        })
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    #[pyo3(name = "latest_bbo")]
    fn py_latest_bbo(&self, instrument_id: &Bound<'_, PyAny>) -> PyResult<Option<QuoteTick>> {
        let parsed = if let Ok(id) = instrument_id.extract::<InstrumentId>() {
            id
        } else {
            let raw: &str = instrument_id.extract()?;
            InstrumentId::from_str(raw).map_err(to_pyvalue_err)?
        };
        Ok(self.latest_bbo(parsed))
    }

    #[pyo3(name = "latest_bbo_len")]
    fn py_latest_bbo_len(&self) -> usize {
        self.latest_bbo_len()
    }

    fn __repr__(&self) -> String {
        stringify!(HyperliquidDataClientConfig).to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl HyperliquidExecutionClientConfig {
    /// Configuration for the Hyperliquid execution client.
    #[new]
    #[pyo3(signature = (
        account_id = None,
        private_key = None,
        vault_address = None,
        account_address = None,
        environment = None,
        base_url_ws = None,
        base_url_http = None,
        base_url_exchange = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        normalize_prices = None,
        market_order_slippage_bps = None,
        include_builder_attribution = None,
        ws_post_timeout_secs = None,
        transport_backend = None,
        ws_select_candidates = None,
        ws_select_probe_ms = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        account_id: Option<AccountId>,
        private_key: Option<String>,
        vault_address: Option<String>,
        account_address: Option<String>,
        environment: Option<HyperliquidEnvironment>,
        base_url_ws: Option<String>,
        base_url_http: Option<String>,
        base_url_exchange: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        normalize_prices: Option<bool>,
        market_order_slippage_bps: Option<u32>,
        include_builder_attribution: Option<bool>,
        ws_post_timeout_secs: Option<u64>,
        transport_backend: Option<TransportBackend>,
        ws_select_candidates: Option<usize>,
        ws_select_probe_ms: Option<u64>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let (ws_select_candidates, ws_select_probe_ms) = validated_ws_selection(
            ws_select_candidates,
            ws_select_probe_ms,
            defaults.ws_select_candidates,
            defaults.ws_select_probe_ms,
        )?;
        Ok(Self {
            account_id: account_id.unwrap_or(defaults.account_id),
            private_key,
            vault_address,
            account_address,
            base_url_ws,
            base_url_http,
            base_url_exchange,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            retry_delay_initial_ms: retry_delay_initial_ms
                .unwrap_or(defaults.retry_delay_initial_ms),
            retry_delay_max_ms: retry_delay_max_ms.unwrap_or(defaults.retry_delay_max_ms),
            normalize_prices: normalize_prices.unwrap_or(defaults.normalize_prices),
            market_order_slippage_bps: market_order_slippage_bps
                .unwrap_or(defaults.market_order_slippage_bps),
            include_builder_attribution: include_builder_attribution
                .unwrap_or(defaults.include_builder_attribution),
            ws_post_timeout_secs: ws_post_timeout_secs.unwrap_or(defaults.ws_post_timeout_secs),
            transport_backend: transport_backend.unwrap_or(defaults.transport_backend),
            outcome_settlement_poll_secs: defaults.outcome_settlement_poll_secs,
            ws_select_candidates,
            ws_select_probe_ms,
        })
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    fn __repr__(&self) -> String {
        stringify!(HyperliquidExecutionClientConfig).to_string()
    }
}
