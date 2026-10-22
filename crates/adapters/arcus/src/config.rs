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

//! Arcus connection and execution configuration.

use std::{any::Any, fmt::Debug, sync::Arc};

use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{consts::ARCUS_VENUE, signing::ArcusSigner};

/// Selects the Arcus deployment and its separate credentials.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.arcus",
        frozen,
        eq,
        eq_int,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.arcus")
)]
pub enum ArcusEnvironment {
    /// Production deployment.
    #[default]
    Mainnet,
    /// Testnet deployment with separate API keys and paper collateral.
    Testnet,
}

impl ArcusEnvironment {
    /// Returns the REST origin.
    #[must_use]
    pub const fn http_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.arcus.xyz",
            Self::Testnet => "https://api.testnet.arcus.xyz",
        }
    }

    /// Returns the streaming URL.
    #[must_use]
    pub const fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.arcus.xyz/v1/ws",
            Self::Testnet => "wss://api.testnet.arcus.xyz/v1/ws",
        }
    }

    fn env_prefix(self) -> &'static str {
        match self {
            Self::Mainnet => "ARCUS",
            Self::Testnet => "ARCUS_TESTNET",
        }
    }
}

/// Configuration for public Arcus perpetual market data.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.arcus", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.arcus")
)]
pub struct ArcusDataClientConfig {
    /// Deployment to connect to.
    pub environment: ArcusEnvironment,
    /// Instruments to load; an empty list loads the available catalogue.
    pub load_ids: Vec<InstrumentId>,
    /// Optional HTTP origin override.
    pub base_url_http: Option<String>,
    /// Optional streaming URL override.
    pub base_url_ws: Option<String>,
    /// Deadline for an individual HTTP request in seconds.
    pub http_timeout_secs: u64,
    /// WebSocket idle deadline in seconds.
    pub ws_idle_timeout_secs: u64,
    /// Maximum number of queued internal messages per connection.
    pub queue_capacity: usize,
}

impl Default for ArcusDataClientConfig {
    fn default() -> Self {
        Self {
            environment: ArcusEnvironment::Mainnet,
            load_ids: Vec::new(),
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: 10,
            ws_idle_timeout_secs: 30,
            queue_capacity: 4096,
        }
    }
}

impl ArcusDataClientConfig {
    /// Validates endpoint, queue, timeout, and instrument configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, credentials in URLs, or foreign instruments.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_connection(
            &self.resolved_http_url(),
            &self.resolved_ws_url(),
            self.http_timeout_secs,
            self.ws_idle_timeout_secs,
            self.queue_capacity,
            &self.load_ids,
        )
    }

    /// Returns the configured HTTP origin.
    #[must_use]
    pub fn resolved_http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| self.environment.http_url().to_owned())
    }

    /// Returns the configured WebSocket URL.
    #[must_use]
    pub fn resolved_ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| self.environment.ws_url().to_owned())
    }
}

impl ClientConfig for ArcusDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Configuration for Arcus order execution and account reconciliation.
///
/// Secrets can be supplied explicitly or through `ARCUS_API_SECRET` and
/// `ARCUS_API_KEY`. Testnet uses `ARCUS_TESTNET_` variables exclusively.
/// The secret is an Ed25519 seed, not an Ethereum wallet private key.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.arcus", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.arcus")
)]
pub struct ArcusExecutionClientConfig {
    /// Nautilus margin account identifier.
    pub account_id: AccountId,
    /// Deployment to connect to.
    pub environment: ArcusEnvironment,
    /// Wallet address owning the registered API key; otherwise read from the environment.
    pub address: Option<String>,
    /// Arcus subaccount number.
    pub subaccount_id: u8,
    /// Optional expected Ed25519 public key, checked against the supplied seed.
    #[serde(skip_serializing)]
    pub api_key: Option<String>,
    /// Ed25519 seed in hexadecimal; omitted from serialization and debug output.
    #[serde(skip_serializing)]
    pub api_secret: Option<String>,
    /// Instruments to load; an empty list loads the available catalogue.
    pub load_ids: Vec<InstrumentId>,
    /// Optional HTTP origin override.
    pub base_url_http: Option<String>,
    /// Optional streaming URL override.
    pub base_url_ws: Option<String>,
    /// Deadline for an individual HTTP request in seconds.
    pub http_timeout_secs: u64,
    /// WebSocket idle deadline in seconds.
    pub ws_idle_timeout_secs: u64,
    /// Maximum number of queued internal messages per connection.
    pub queue_capacity: usize,
    /// Period for refreshing account and order evidence in seconds.
    pub reconciliation_interval_secs: u64,
    /// Maximum market-order price deviation from the reference price in basis points.
    pub market_slippage_bps: u32,
}

impl Default for ArcusExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("ARCUS-001"),
            environment: ArcusEnvironment::Mainnet,
            address: None,
            subaccount_id: 0,
            api_key: None,
            api_secret: None,
            load_ids: Vec::new(),
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: 10,
            ws_idle_timeout_secs: 30,
            queue_capacity: 4096,
            reconciliation_interval_secs: 30,
            market_slippage_bps: 50,
        }
    }
}

impl Debug for ArcusExecutionClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ArcusExecutionClientConfig))
            .field("account_id", &self.account_id)
            .field("environment", &self.environment)
            .field("subaccount_id", &self.subaccount_id)
            .field("load_ids", &self.load_ids)
            .field("credentials", &"<redacted>")
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_idle_timeout_secs", &self.ws_idle_timeout_secs)
            .field("queue_capacity", &self.queue_capacity)
            .field(
                "reconciliation_interval_secs",
                &self.reconciliation_interval_secs,
            )
            .field("market_slippage_bps", &self.market_slippage_bps)
            .finish_non_exhaustive()
    }
}

impl ArcusExecutionClientConfig {
    /// Validates the connection and execution limits without loading credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if endpoints, instruments, or limits are invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_connection(
            &self.resolved_http_url(),
            &self.resolved_ws_url(),
            self.http_timeout_secs,
            self.ws_idle_timeout_secs,
            self.queue_capacity,
            &self.load_ids,
        )?;
        anyhow::ensure!(
            (1..=3600).contains(&self.reconciliation_interval_secs),
            "reconciliation_interval_secs must be between 1 and 3600"
        );
        anyhow::ensure!(
            (1..=1000).contains(&self.market_slippage_bps),
            "market_slippage_bps must be between 1 and 1000"
        );
        anyhow::ensure!(
            self.subaccount_id <= 9,
            "subaccount_id must be between 0 and 9"
        );

        if let Some(address) = &self.address {
            validate_address(address)?;
        }
        Ok(())
    }

    /// Returns the configured HTTP origin.
    #[must_use]
    pub fn resolved_http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| self.environment.http_url().to_owned())
    }

    /// Returns the configured WebSocket URL.
    #[must_use]
    pub fn resolved_ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| self.environment.ws_url().to_owned())
    }

    /// Loads and validates the account address for the selected environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the address is absent or malformed.
    pub fn resolved_address(&self) -> anyhow::Result<String> {
        let name = format!("{}_ADDRESS", self.environment.env_prefix());
        let address = self
            .address
            .clone()
            .or_else(|| std::env::var(&name).ok())
            .ok_or_else(|| anyhow::anyhow!("Arcus address is required ({name})"))?;
        validate_address(&address)?;
        Ok(address.to_ascii_lowercase())
    }

    /// Loads the signer and checks any explicitly supplied public key.
    ///
    /// # Errors
    ///
    /// Returns an error for missing or malformed secrets or a mismatched public key.
    pub fn resolved_signer(&self) -> anyhow::Result<Arc<ArcusSigner>> {
        let name = format!("{}_API_SECRET", self.environment.env_prefix());
        let seed = Zeroizing::new(
            self.api_secret
                .clone()
                .or_else(|| std::env::var(&name).ok())
                .ok_or_else(|| anyhow::anyhow!("Arcus Ed25519 API secret is required ({name})"))?,
        );
        let signer = ArcusSigner::from_seed_hex(&seed)?;
        let key_name = format!("{}_API_KEY", self.environment.env_prefix());

        if let Some(key) = self
            .api_key
            .clone()
            .or_else(|| std::env::var(&key_name).ok())
        {
            anyhow::ensure!(
                key.trim_start_matches("0x")
                    .eq_ignore_ascii_case(signer.api_key().trim_start_matches("0x")),
                "Arcus API key does not match the Ed25519 seed"
            );
        }
        Ok(Arc::new(signer))
    }
}

impl ClientConfig for ArcusExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ArcusDataClientConfig {
    environment: ArcusEnvironment,
    load_ids: Vec<InstrumentId>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_idle_timeout_secs: u64,
    queue_capacity: usize,
});

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ArcusExecutionClientConfig {
    account_id: AccountId,
    environment: ArcusEnvironment,
    subaccount_id: u8,
    load_ids: Vec<InstrumentId>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_idle_timeout_secs: u64,
    queue_capacity: usize,
    reconciliation_interval_secs: u64,
    market_slippage_bps: u32,
});

fn validate_address(address: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        address.starts_with("0x")
            && address.len() == 42
            && address[2..].bytes().all(|c| c.is_ascii_hexdigit()),
        "Arcus address must be a 20-byte hexadecimal Ethereum address"
    );
    Ok(())
}

fn validate_connection(
    http: &str,
    ws: &str,
    timeout: u64,
    idle: u64,
    capacity: usize,
    instruments: &[InstrumentId],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=120).contains(&timeout),
        "http_timeout_secs must be between 1 and 120"
    );
    anyhow::ensure!(
        (5..=300).contains(&idle),
        "ws_idle_timeout_secs must be between 5 and 300"
    );
    anyhow::ensure!(
        (16..=65536).contains(&capacity),
        "queue_capacity must be between 16 and 65536"
    );

    for (value, secure, plain) in [(http, "https", "http"), (ws, "wss", "ws")] {
        let url =
            url::Url::parse(value).map_err(|_| anyhow::anyhow!("Invalid Arcus endpoint URL"))?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        anyhow::ensure!(
            url.scheme() == secure || (url.scheme() == plain && loopback),
            "Arcus endpoints require TLS except for loopback testing"
        );
        anyhow::ensure!(
            url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Arcus endpoint URLs must have a host and contain no credentials, query, or fragment"
        );
    }

    for id in instruments {
        anyhow::ensure!(
            id.venue == *ARCUS_VENUE && id.symbol.as_str().ends_with("-PERP"),
            "Arcus load_ids must use SYMBOL-PERP.ARCUS identifiers"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn rejects_unbounded_or_insecure_configuration() {
        let config = ArcusDataClientConfig {
            queue_capacity: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        let config = ArcusDataClientConfig {
            base_url_ws: Some("ws://api.arcus.xyz/v1/ws".into()),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        let config = ArcusDataClientConfig {
            base_url_http: Some("https://user:password@api.arcus.xyz".into()),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[rstest]
    fn secrets_are_redacted_and_public_key_must_match() {
        let config = ArcusExecutionClientConfig {
            api_secret: Some("11".repeat(32)),
            api_key: Some("22".repeat(32)),
            ..Default::default()
        };
        assert!(!format!("{config:?}").contains(&"11".repeat(32)));
        let json = serde_json::to_value(&config).unwrap();
        assert!(json.get("api_secret").is_none());
        assert!(json.get("api_key").is_none());
        assert!(config.resolved_signer().is_err());
    }

    #[rstest]
    fn environment_and_instrument_validation() {
        let config = ArcusDataClientConfig {
            environment: ArcusEnvironment::Testnet,
            load_ids: vec![InstrumentId::from("BTC-USD-PERP.ARCUS")],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.resolved_http_url(), "https://api.testnet.arcus.xyz");
        assert_eq!(ArcusEnvironment::Testnet.env_prefix(), "ARCUS_TESTNET");
        let foreign = ArcusDataClientConfig {
            load_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            ..config
        };
        assert!(foreign.validate().is_err());
    }
}
