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

//! Configuration for the Ondo Perps clients.
//!
//! The public data client holds no credentials and never reads `.env`: it reads public market
//! data only. Environment resolution for the endpoints lives in
//! [`crate::common::consts`], so an environment choice and an explicit override cannot mix live
//! and sandbox endpoints silently.
//!
//! The execution client's configuration ([`OndoExecutionClientConfig`]) defaults to
//! [`OndoEnvironment::Sandbox`] and has no production write branch to open: production is refused
//! by [`crate::common::credential::validate_authenticated_environment`] and
//! [`OndoExecutionClientConfig::allow_production_orders`] is refused by
//! [`OndoExecutionClientConfig::validate`], both before a socket is touched (plan §4.1, §1).

use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use serde::{Deserialize, Serialize};

use crate::common::{
    consts::{
        ONDO_BOOK_LIMIT, ONDO_HTTP_TIMEOUT_SECS, ONDO_WS_HEARTBEAT_SECS, http_base_url, ws_url,
    },
    enums::OndoEnvironment,
};

/// Default dead man's switch timeout, in seconds (plan §6.4).
pub const ONDO_DMS_TIMEOUT_SECS: u64 = 30;

/// Default interval between account reconciliations, in seconds (plan §6.4).
pub const ONDO_RECONCILE_INTERVAL_SECS: u64 = 30;

/// Configuration for the Ondo Perps data client.
#[derive(Clone, Debug, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object,)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoDataClientConfig {
    /// The target environment.
    #[builder(default)]
    pub environment: OndoEnvironment,
    /// The instrument IDs to load.
    ///
    /// A non-empty list narrows the published set to those instruments. An empty list loads
    /// every market the venue publishes.
    #[builder(default)]
    pub load_ids: Vec<InstrumentId>,
    /// Override for the REST base URL.
    ///
    /// The public transport carries no credential, so it is not held to the signed endpoint
    /// allowlist: production public data is readable by design (plan §1). Use the official host of
    /// the same environment, or an explicit local test server. The *signed* endpoints are a
    /// different matter - [`OndoExecutionClientConfig::base_url_http`] is judged by
    /// [`crate::common::credential::validate_authenticated_environment`], which refuses everything
    /// that is not the sandbox authority or a loopback test service.
    pub base_url_http: Option<String>,
    /// Override for the WebSocket URL, with the same restriction as [`Self::base_url_http`].
    pub base_url_ws: Option<String>,
    /// The HTTP request timeout in seconds.
    #[builder(default = ONDO_HTTP_TIMEOUT_SECS)]
    pub http_timeout_secs: u64,
    /// The application-level WebSocket heartbeat interval in seconds.
    #[builder(default = ONDO_WS_HEARTBEAT_SECS)]
    pub ws_heartbeat_secs: u64,
    /// The maximum number of order book levels requested per market.
    #[builder(default = ONDO_BOOK_LIMIT)]
    pub book_limit: u32,
    /// Optional directory for raw public market-data frames.
    pub raw_md_path: Option<String>,
    /// The run id the raw recording must carry.
    ///
    /// The application sets this to its process stamp - the same string every tape record of the run
    /// carries - so a run's raw public frames and its tape join by construction even when
    /// `raw_md_path`'s parent directory is not named after the run (the default `--out reports/stage1`
    /// is the case that matters). Without it the recorder derives the run id from that parent name and
    /// the run header says `run_id_source: "derived_from_path"`, which joins the tape only when the run
    /// directory is itself named after the run.
    pub raw_md_run_id: Option<String>,
}

impl Default for OndoDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

// Exposes every member of §4.1's configuration surface as a read-only Python property.
//
// The list is the configuration itself, not a subset: `raw_md_path` is carried unconsumed today
// (the recorder is a later task) but is still readable, and no credential member can appear here
// because the Rust type has none.
#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OndoDataClientConfig {
    environment: OndoEnvironment,
    load_ids: Vec<InstrumentId>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_heartbeat_secs: u64,
    book_limit: u32,
    raw_md_path: Option<String>,
    raw_md_run_id: Option<String>,
});

impl OndoDataClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the REST base URL this configuration resolves to.
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        self.base_url_http
            .as_deref()
            .unwrap_or_else(|| http_base_url(self.environment))
    }

    /// Returns the WebSocket URL this configuration resolves to.
    #[must_use]
    pub fn ws_url(&self) -> &str {
        self.base_url_ws
            .as_deref()
            .unwrap_or_else(|| ws_url(self.environment))
    }
}

/// Why an execution configuration cannot be used.
///
/// Both are decided before the client exists and therefore before any request does: a refused
/// configuration has no object to send from.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OndoExecutionConfigError {
    /// Production order entry is not a capability this phase implements, whatever the flag says.
    #[error(
        "`allow_production_orders` is not a supported capability: this phase refuses production \
         order entry unconditionally (plan §4.1), and there is no configuration value that opens \
         a production write branch"
    )]
    ProductionOrdersUnsupported,
    /// No account was named.
    #[error("the Ondo Perps execution client requires an `account_id`")]
    MissingAccountId,
}

/// Configuration for the Ondo Perps execution client.
///
/// # The environment defaults to sandbox, and production is not reachable
///
/// [`Self::environment`] defaults to [`OndoEnvironment::Sandbox`], unlike the data client's
/// production default: the authenticated surface may only ever be sandbox
/// ([`crate::common::credential::validate_authenticated_environment`]), and a production
/// configuration is refused there before a credential is read. The same gate admits the endpoint
/// the session signs for - see [`Self::base_url_http`] - so a configuration cannot aim a credential
/// at an authority this adapter does not own.
/// [`Self::allow_production_orders`] exists so that asking for production order entry can be
/// *refused by name* ([`OndoExecutionConfigError::ProductionOrdersUnsupported`]) rather than
/// silently ignored; setting it does not enable anything.
///
/// # Credentials
///
/// [`Self::api_key`] and [`Self::api_secret`] are optional explicit overrides. When either is
/// absent the client resolves the credential from the process environment through
/// [`crate::common::credential::resolve_credential`] - the same gate-first path Task 6 built - and
/// a missing credential is an error naming the variable, never a fallback to another account or
/// environment. The secret is never rendered: [`std::fmt::Debug`] for this type masks it with
/// [`REDACTED`], exactly as the Aster execution configuration does for its signer key.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object,)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
pub struct OndoExecutionClientConfig {
    /// The target environment. Defaults to [`OndoEnvironment::Sandbox`].
    #[builder(default = OndoEnvironment::Sandbox)]
    pub environment: OndoEnvironment,
    /// The Nautilus account id this client reports for. Required.
    pub account_id: Option<AccountId>,
    /// The API key id, `ondoKeyId_` prefix included. Falls back to the environment variable.
    pub api_key: Option<String>,
    /// The API secret, `ondoApiSecret_` prefix included. Falls back to the environment variable.
    pub api_secret: Option<String>,
    /// Override for the REST base URL.
    ///
    /// The signed endpoint is an allowlist, not a preference. Two authorities are admitted: the
    /// official host of [`Self::environment`] on `https` and the scheme's default port, and a
    /// loopback test service (`127.0.0.0/8` or `::1`) for the offline tests and a local mock.
    /// Everything else - another remote host, a host that merely resembles the official one,
    /// userinfo, a plaintext remote service, an unreadable URL - is refused by
    /// [`crate::common::credential::validate_authenticated_environment`] before a credential is
    /// read and before a client exists, and the URL is judged with the parser the transport itself
    /// uses rather than as a string (plan §R0.3). The default resolves to the official sandbox
    /// host, which is never loopback, so a test service is always an explicit override.
    pub base_url_http: Option<String>,
    /// Override for the private WebSocket URL.
    ///
    /// Judged by the **same** policy as [`Self::base_url_http`], for the WebSocket scheme family:
    /// the official host of [`Self::environment`] on `wss` and the scheme's default port, or a
    /// loopback test service on `ws`/`wss`
    /// ([`crate::common::credential::validate_authenticated_websocket_environment`]). The default
    /// resolves to [`crate::common::consts::ws_url`] of the configured environment, which is never
    /// loopback, so the production host is not reachable by omitting a value and a test service is
    /// always an explicit override.
    pub base_url_ws: Option<String>,
    /// Whether this client is an account **read-only** session.
    ///
    /// A read-only session reads the account over the private stream and never places an order:
    /// [`crate::reconciliation::NewRiskRefusal::AccountIsReadOnly`] refuses every submission
    /// whatever the account reads, and the private transport never subscribes to the account's dead
    /// man's switch, whose arm cancels resting orders (plan §0). Defaults to `false`.
    #[builder(default)]
    pub account_read_only: bool,
    /// The HTTP request timeout in seconds.
    #[builder(default = ONDO_HTTP_TIMEOUT_SECS)]
    pub http_timeout_secs: u64,
    /// The dead man's switch timeout in seconds (plan §6.4). The private transport arms it and
    /// renews it at half this interval.
    #[builder(default = ONDO_DMS_TIMEOUT_SECS)]
    pub dms_timeout_secs: u64,
    /// The interval between account reconciliations, in seconds (plan §6.4). The private
    /// transport's run loop is the caller.
    #[builder(default = ONDO_RECONCILE_INTERVAL_SECS)]
    pub reconcile_interval_secs: u64,
    /// Whether production order entry was requested. Refused, always (see the type documentation).
    #[builder(default)]
    pub allow_production_orders: bool,
}

impl Default for OndoExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

// The secret is deliberately absent: `api_secret` is the one member that must not cross into
// Python, and a getter is the only way it could.
#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OndoExecutionClientConfig {
    environment: OndoEnvironment,
    account_id: Option<AccountId>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    account_read_only: bool,
    http_timeout_secs: u64,
    dms_timeout_secs: u64,
    reconcile_interval_secs: u64,
    allow_production_orders: bool,
});

impl std::fmt::Debug for OndoExecutionClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(OndoExecutionClientConfig))
            .field("environment", &self.environment)
            .field("account_id", &self.account_id)
            .field("api_key", &self.api_key.as_ref().map(|_| REDACTED))
            .field("api_secret", &self.api_secret.as_ref().map(|_| REDACTED))
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("account_read_only", &self.account_read_only)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("dms_timeout_secs", &self.dms_timeout_secs)
            .field("reconcile_interval_secs", &self.reconcile_interval_secs)
            .field("allow_production_orders", &self.allow_production_orders)
            .finish()
    }
}

impl OndoExecutionClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the REST base URL this configuration resolves to.
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        self.base_url_http
            .as_deref()
            .unwrap_or_else(|| http_base_url(self.environment))
    }

    /// Returns the private WebSocket URL this configuration resolves to.
    #[must_use]
    pub fn ws_url(&self) -> &str {
        self.base_url_ws
            .as_deref()
            .unwrap_or_else(|| ws_url(self.environment))
    }

    /// Returns the stream mode this configuration asks for.
    ///
    /// One decision, taken from one member: a read-only account session never subscribes to the
    /// switch, and the mode is what carries that to the transport.
    #[must_use]
    pub const fn stream_mode(&self) -> crate::websocket::private::PrivateStreamMode {
        if self.account_read_only {
            crate::websocket::private::PrivateStreamMode::ReadOnly
        } else {
            crate::websocket::private::PrivateStreamMode::Trading
        }
    }

    /// Returns whether this configuration carries an explicit credential pair.
    ///
    /// A blank value does not count: an empty override is not a credential, and the client falls
    /// back to the environment rather than sending an empty key.
    #[must_use]
    pub fn has_explicit_credentials(&self) -> bool {
        let present = |value: &Option<String>| {
            value
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
        };

        present(&self.api_key) && present(&self.api_secret)
    }

    /// Validates the configuration, before any client or socket exists.
    ///
    /// # Errors
    ///
    /// Returns [`OndoExecutionConfigError::ProductionOrdersUnsupported`] when
    /// [`Self::allow_production_orders`] is set - there is no production write branch to open -
    /// and [`OndoExecutionConfigError::MissingAccountId`] when no account was named. The
    /// environment and base URL are judged separately, by
    /// [`crate::common::credential::validate_authenticated_environment`].
    pub fn validate(&self) -> Result<(), OndoExecutionConfigError> {
        if self.allow_production_orders {
            return Err(OndoExecutionConfigError::ProductionOrdersUnsupported);
        }

        if self.account_id.is_none() {
            return Err(OndoExecutionConfigError::MissingAccountId);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::{credential::validate_authenticated_environment, endpoint::OndoEndpoint};

    #[rstest]
    fn test_defaults() {
        let config = OndoDataClientConfig::default();

        assert_eq!(config.environment, OndoEnvironment::Production);
        assert!(config.load_ids.is_empty());
        assert_eq!(config.http_timeout_secs, 15);
        assert_eq!(config.ws_heartbeat_secs, 20);
        assert_eq!(config.book_limit, 100);
        assert!(config.base_url_http.is_none());
        assert!(config.base_url_ws.is_none());
        assert!(config.raw_md_path.is_none());
        assert!(
            config.raw_md_run_id.is_none(),
            "no run id configured: the recorder falls back to the directory-derived one"
        );
    }

    #[rstest]
    fn test_endpoints_follow_the_environment() {
        let production = OndoDataClientConfig::default();
        let sandbox = OndoDataClientConfig {
            environment: OndoEnvironment::Sandbox,
            ..Default::default()
        };

        assert_eq!(production.http_base_url(), "https://api.ondoperps.xyz");
        assert_eq!(production.ws_url(), "wss://api.ondoperps.xyz/ws");
        assert_eq!(sandbox.http_base_url(), "https://api.ondoperps-sandbox.xyz");
        assert_eq!(sandbox.ws_url(), "wss://api.ondoperps-sandbox.xyz/ws");
    }

    #[rstest]
    fn test_explicit_overrides_win() {
        let config = OndoDataClientConfig {
            base_url_http: Some("http://127.0.0.1:8080".to_string()),
            base_url_ws: Some("ws://127.0.0.1:8080/ws".to_string()),
            ..Default::default()
        };

        assert_eq!(config.http_base_url(), "http://127.0.0.1:8080");
        assert_eq!(config.ws_url(), "ws://127.0.0.1:8080/ws");
    }

    #[rstest]
    fn test_deserializes_with_defaults_and_rejects_unknown_fields() {
        let config: OndoDataClientConfig = serde_json::from_str(
            r#"{"environment": "sandbox", "load_ids": ["NVDA-USD-PERP.ONDO"]}"#,
        )
        .unwrap();

        assert_eq!(config.environment, OndoEnvironment::Sandbox);
        assert_eq!(config.load_ids.len(), 1);
        assert_eq!(config.load_ids[0].to_string(), "NVDA-USD-PERP.ONDO");
        assert_eq!(config.book_limit, 100);

        assert!(
            serde_json::from_str::<OndoDataClientConfig>(r#"{"load_idz": []}"#).is_err(),
            "an unknown configuration field is rejected"
        );
    }

    #[rstest]
    fn test_the_raw_recording_takes_an_explicit_run_id() {
        let config: OndoDataClientConfig = serde_json::from_str(
            r#"{"raw_md_path": "reports/stage1/raw_ondo", "raw_md_run_id": "20260914T000000Z"}"#,
        )
        .unwrap();

        assert_eq!(
            config.raw_md_run_id.as_deref(),
            Some("20260914T000000Z"),
            "the application's process stamp travels as the recording's run id"
        );
    }

    #[rstest]
    fn test_the_execution_config_defaults_to_sandbox_with_production_orders_off() {
        let config = OndoExecutionClientConfig::default();

        assert_eq!(config.environment, OndoEnvironment::Sandbox);
        assert!(config.account_id.is_none());
        assert!(config.api_key.is_none());
        assert!(config.api_secret.is_none());
        assert_eq!(config.dms_timeout_secs, 30);
        assert_eq!(config.reconcile_interval_secs, 30);
        assert!(!config.allow_production_orders);
        assert_eq!(
            config.http_base_url(),
            "https://api.ondoperps-sandbox.xyz",
            "a default execution configuration resolves to the sandbox host",
        );
    }

    #[rstest]
    fn test_the_execution_config_never_renders_the_secret_or_the_key() {
        let config = OndoExecutionClientConfig {
            api_key: Some("ondoKeyId_UNIT_TEST_ONLY".to_string()),
            api_secret: Some("ondoApiSecret_UNIT_TEST_ONLY".to_string()),
            ..Default::default()
        };

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("UNIT_TEST_ONLY"), "{rendered}");
        assert_eq!(rendered.matches(REDACTED).count(), 2, "{rendered}");
        assert!(config.has_explicit_credentials());
    }

    #[rstest]
    fn test_a_blank_override_is_not_a_credential() {
        let config = OndoExecutionClientConfig {
            api_key: Some("   ".to_string()),
            api_secret: Some("ondoApiSecret_UNIT_TEST_ONLY".to_string()),
            ..Default::default()
        };

        assert!(
            !config.has_explicit_credentials(),
            "an empty override falls back to the environment rather than sending a blank key",
        );
    }

    #[rstest]
    fn test_production_order_entry_is_refused_by_name_whatever_the_flag_says() {
        let requested = OndoExecutionClientConfig {
            account_id: Some(AccountId::from("ONDO-SANDBOX-001")),
            allow_production_orders: true,
            ..Default::default()
        };

        assert_eq!(
            requested
                .validate()
                .expect_err("no production write branch exists"),
            OndoExecutionConfigError::ProductionOrdersUnsupported,
        );

        let unconfigured = OndoExecutionClientConfig {
            allow_production_orders: true,
            ..Default::default()
        };
        assert_eq!(
            unconfigured
                .validate()
                .expect_err("the flag is refused first"),
            OndoExecutionConfigError::ProductionOrdersUnsupported,
        );
    }

    #[rstest]
    fn test_an_execution_config_without_an_account_is_refused() {
        assert_eq!(
            OndoExecutionClientConfig::default()
                .validate()
                .expect_err("an execution client reports for an account"),
            OndoExecutionConfigError::MissingAccountId,
        );

        let named = OndoExecutionClientConfig {
            account_id: Some(AccountId::from("ONDO-SANDBOX-001")),
            ..Default::default()
        };
        assert_eq!(named.validate(), Ok(()));
    }

    #[rstest]
    fn test_the_execution_config_takes_an_explicit_local_base_url_override() {
        let config = OndoExecutionClientConfig {
            base_url_http: Some("http://127.0.0.1:8080".to_string()),
            ..Default::default()
        };

        assert_eq!(config.http_base_url(), "http://127.0.0.1:8080");
    }

    /// What the configuration resolves to and what the gate admits are one decision: a default
    /// configuration can only ever name the environment's own host, a loopback mock is always an
    /// explicit override, and an arbitrary remote host is not an endpoint this configuration can
    /// aim a credential at.
    #[rstest]
    fn test_a_configuration_can_only_resolve_to_an_endpoint_it_may_sign_for() {
        let default = OndoExecutionClientConfig::default();

        assert_eq!(default.http_base_url(), "https://api.ondoperps-sandbox.xyz");
        assert_eq!(
            validate_authenticated_environment(OndoEnvironment::Sandbox, default.http_base_url()),
            Ok(OndoEndpoint::Official),
            "the default resolves to the official sandbox host, never a test service",
        );

        let local = OndoExecutionClientConfig {
            base_url_http: Some("http://127.0.0.1:8080".to_string()),
            ..Default::default()
        };
        assert_eq!(
            validate_authenticated_environment(OndoEnvironment::Sandbox, local.http_base_url()),
            Ok(OndoEndpoint::LoopbackTestService),
            "a loopback mock is the explicit test service, classified as itself",
        );

        for url in ["https://evil.example", "https://api.ondoperps.xyz"] {
            let remote = OndoExecutionClientConfig {
                base_url_http: Some(url.to_string()),
                ..Default::default()
            };
            assert!(
                validate_authenticated_environment(
                    OndoEnvironment::Sandbox,
                    remote.http_base_url()
                )
                .is_err(),
                "`{url}` is not an endpoint this configuration may sign for",
            );
        }
    }

    #[rstest]
    fn test_the_execution_config_deserializes_with_defaults_and_rejects_unknown_fields() {
        let config: OndoExecutionClientConfig =
            serde_json::from_str(r#"{"account_id": "ONDO-SANDBOX-001"}"#).unwrap();

        assert_eq!(config.environment, OndoEnvironment::Sandbox);
        assert_eq!(config.dms_timeout_secs, 30);
        assert!(
            serde_json::from_str::<OndoExecutionClientConfig>(r#"{"account_idz": "x"}"#).is_err(),
            "an unknown configuration field is rejected",
        );
    }
}
