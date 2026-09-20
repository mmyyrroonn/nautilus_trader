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
//! Execution defaults to sandbox. Production reads require `account_read_only`; production writes
//! additionally require explicit opt-in, a complete immutable execution envelope, raw account
//! identity, a run token and a durable journal. The native transport independently enforces the
//! bounded authority, and a read-only configuration never permits an order, cancel or DMS write.

use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use serde::{Deserialize, Serialize};

use crate::common::{
    consts::{
        ONDO_BOOK_LIMIT, ONDO_HTTP_TIMEOUT_SECS, ONDO_WS_HEARTBEAT_SECS, http_base_url, ws_url,
    },
    enums::{OndoAuthenticationScope, OndoEnvironment},
};

/// Default dead man's switch timeout, in seconds (plan §6.4).
pub const ONDO_DMS_TIMEOUT_SECS: u64 = 30;

/// The default bound on consecutive failed switch renewals (plan §R3.3).
///
/// It lives in the crate that owns the switch
/// ([`crate::reconciliation::ONDO_DMS_MAX_FAILED_RENEWALS`]) and is re-exported here so a caller
/// configuring the client does not have to reach into the state machine for the number.
pub const ONDO_DMS_MAX_FAILED_RENEWALS: u32 = crate::reconciliation::ONDO_DMS_MAX_FAILED_RENEWALS;

/// The most a configuration may ask that bound to be: the ceiling this client caps it at
/// (plan §R3.3).
///
/// A configuration may tighten the bound, never widen it past this
/// ([`OndoExecutionClientConfig::capped_dms_max_failed_renewals`]), so no configuration file can
/// loosen the one bound that decides when this client stops trusting an unrenewed switch.
pub const ONDO_DMS_MAX_FAILED_RENEWALS_CEILING: u32 =
    crate::reconciliation::ONDO_DMS_MAX_FAILED_RENEWALS_CEILING;

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
    /// Production without `account_read_only` is not a supported combination.
    #[error(
        "the production environment is read-only in this phase: set `account_read_only = true`, \
         or use the sandbox environment for order entry (plan §4.1); there is no production write \
         branch"
    )]
    ProductionWritesUnsupported,
    /// No account was named.
    #[error("the Ondo Perps execution client requires an `account_id`")]
    MissingAccountId,
    /// The approved production envelope or its required run binding is invalid.
    #[error("invalid production execution configuration: {0}")]
    InvalidProductionEnvelope(String),
}

/// Configuration for the Ondo Perps execution client.
///
/// # Production authorization
///
/// Production writes are disabled by default. An explicit opt-in requires the complete immutable
/// execution envelope and its identity, journal and run-token bindings. Read-only mode cannot be
/// combined with that opt-in. Endpoint and credential environments must agree.
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
    /// The venue account identifier the authenticated account must match.
    ///
    /// This is the venue's own `accountID` (for example a numeric string), **not** the Nautilus
    /// [`AccountId`] above: the two are different identifiers and are never compared to each
    /// other. When set, the client reads `GET /v1/account` once while connecting and compares the
    /// documented `accountID` member; a mismatch refuses the connection, and an answer that carries
    /// no comparable identifier is recorded as `unknown` rather than assumed to match. When unset,
    /// the identity is `unknown` and no account read is made for it. The value is never logged.
    pub expected_venue_account_id: Option<String>,
    /// The application's run token, echoed by the read-only diagnostics snapshot.
    ///
    /// It is the application's own per-run identifier (`run_id`), not a secret and not a venue
    /// value. [`crate::diagnostics::OndoReadOnlySnapshot::run_id`] carries it verbatim, so an
    /// application can reject a snapshot that belongs to another run rather than merge cross-run
    /// state. The execution factory retains the latest client's diagnostics handle, so a later run
    /// reports its own token and counts.
    pub diagnostics_run_id: Option<String>,
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
    /// How many switch renewals in a row may fail to be written before this client stops trusting
    /// the switch and refuses new orders (plan §R3.3).
    ///
    /// The failures counted are **send** failures - a frame this process could not put on the
    /// socket - and they are counted consecutively: one frame that is written clears the run. There
    /// is no acknowledgement to wait for, because the frozen material documents the subscribe frame
    /// and the timeout and says nothing about which message renews an armed switch, so "written" is
    /// the strongest statement this adapter can make about a renewal and it is not a confirmation.
    ///
    /// The default is [`ONDO_DMS_MAX_FAILED_RENEWALS`] and the **ceiling** is
    /// [`ONDO_DMS_MAX_FAILED_RENEWALS_CEILING`]: a value above it is capped rather than honoured, so
    /// a configuration can make the bound tighter and can never make it looser.
    #[builder(default = ONDO_DMS_MAX_FAILED_RENEWALS)]
    pub dms_max_failed_renewals: u32,
    /// The interval between account reconciliations, in seconds (plan §6.4). The private
    /// transport's run loop is the caller.
    #[builder(default = ONDO_RECONCILE_INTERVAL_SECS)]
    pub reconcile_interval_secs: u64,
    /// Path of the durable ledger journal (plan §R3.2).
    ///
    /// The journal is what lets a restart know which fills it has already applied, which order a
    /// venue order id belongs to, and which writes it left unsettled. It is written whole to a
    /// sibling temporary file and renamed onto this path, so a reader sees one complete journal or
    /// the previous one, and the parent directory is created when it is missing.
    ///
    /// **A [`None`] path is a supported mode and not a quiet one.** Nothing is written, the
    /// ledger, the order index and the unsettled writes live for this process only, and the run
    /// says so where it can be read ([`crate::execution::OndoAccountRuntime::journal_status`]
    /// answers [`crate::reconciliation::JournalStatus::NotConfigured`]) and in the log. What it
    /// must never be is a run that believed it was durable — and a configured path that stops
    /// accepting writes is the other way to become one, so that run does not report itself as
    /// restored either: once a checkpoint has failed, `journal_status` answers
    /// [`crate::reconciliation::JournalStatus::Degraded`], naming the instant of the last write
    /// that did reach the disk
    /// ([`crate::execution::OndoAccountRuntime::journal_write_failures`] is the count alone). A
    /// degraded journal states the lapse and refuses no order: losing durability is not losing
    /// memory, and a crash from there replays under trade ids the engine dedupes.
    ///
    /// The path is a plain filesystem path and carries no credential; it is the one configuration
    /// member besides the endpoints that names something outside this process.
    pub journal_path: Option<String>,
    /// Whether the separately bounded production execution capability was explicitly requested.
    #[builder(default)]
    pub allow_production_orders: bool,
    /// The separately approved immutable production execution limits.
    pub execution_envelope: Option<crate::production::OndoExecutionEnvelopeConfig>,
}

impl Default for OndoExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

// The secret is deliberately absent: `api_secret` is the one member that must not cross into
// Python, and a getter is the only way it could. `expected_venue_account_id` is present so an
// application can confirm the identity it configured without re-reading its own environment.
#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OndoExecutionClientConfig {
    environment: OndoEnvironment,
    account_id: Option<AccountId>,
    expected_venue_account_id: Option<String>,
    diagnostics_run_id: Option<String>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    account_read_only: bool,
    http_timeout_secs: u64,
    dms_timeout_secs: u64,
    dms_max_failed_renewals: u32,
    reconcile_interval_secs: u64,
    journal_path: Option<String>,
    allow_production_orders: bool,
});

impl std::fmt::Debug for OndoExecutionClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(OndoExecutionClientConfig))
            .field("environment", &self.environment)
            .field("account_id", &self.account_id)
            .field("expected_venue_account_id", &self.expected_venue_account_id)
            .field("diagnostics_run_id", &self.diagnostics_run_id)
            .field("api_key", &self.api_key.as_ref().map(|_| REDACTED))
            .field("api_secret", &self.api_secret.as_ref().map(|_| REDACTED))
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("account_read_only", &self.account_read_only)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("dms_timeout_secs", &self.dms_timeout_secs)
            .field("dms_max_failed_renewals", &self.dms_max_failed_renewals)
            .field("reconcile_interval_secs", &self.reconcile_interval_secs)
            .field("journal_path", &self.journal_path)
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

    /// Returns the switch's consecutive-renewal-failure bound, capped at the crate's ceiling.
    ///
    /// [`Self::dms_max_failed_renewals`] is what was configured; this is what the client will
    /// actually arm. The two are the same number until a configuration asks for more than
    /// [`ONDO_DMS_MAX_FAILED_RENEWALS_CEILING`], and the client reads this one - so a configuration
    /// file that asked for a bound the crate does not allow cannot widen it by being believed.
    #[must_use]
    pub const fn capped_dms_max_failed_renewals(&self) -> u32 {
        if self.dms_max_failed_renewals > ONDO_DMS_MAX_FAILED_RENEWALS_CEILING {
            ONDO_DMS_MAX_FAILED_RENEWALS_CEILING
        } else {
            self.dms_max_failed_renewals
        }
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

    /// Returns the authorization scope this configuration asks for.
    ///
    /// The scope pairs [`Self::environment`] with [`Self::account_read_only`]. Production with
    /// `account_read_only = false` has no scope: it is refused with
    /// [`OndoExecutionConfigError::ProductionWritesUnsupported`].
    ///
    /// # Errors
    ///
    /// Returns [`OndoExecutionConfigError::ProductionWritesUnsupported`] for a production
    /// configuration that does not set `account_read_only`.
    pub fn authentication_scope(
        &self,
    ) -> Result<OndoAuthenticationScope, OndoExecutionConfigError> {
        match (self.environment, self.account_read_only) {
            (OndoEnvironment::Sandbox, false) => Ok(OndoAuthenticationScope::SandboxTrading),
            (OndoEnvironment::Sandbox, true) => Ok(OndoAuthenticationScope::SandboxReadOnly),
            (OndoEnvironment::Production, true) => Ok(OndoAuthenticationScope::ProductionReadOnly),
            (OndoEnvironment::Production, false) => {
                if self.allow_production_orders && self.execution_envelope.is_some() {
                    self.validate_production_envelope()?;
                    Ok(OndoAuthenticationScope::ProductionTrading)
                } else {
                    Err(OndoExecutionConfigError::ProductionWritesUnsupported)
                }
            }
        }
    }

    fn validate_production_envelope(&self) -> Result<(), OndoExecutionConfigError> {
        let fail =
            |reason: &str| OndoExecutionConfigError::InvalidProductionEnvelope(reason.to_string());
        if self.environment != OndoEnvironment::Production
            || self.account_read_only
            || !self.allow_production_orders
        {
            return Err(fail(
                "production envelope requires explicit production write opt-in",
            ));
        }
        if self
            .expected_venue_account_id
            .as_ref()
            .is_none_or(|v| v.trim().is_empty())
            || self
                .diagnostics_run_id
                .as_ref()
                .is_none_or(|v| v.is_empty() || v.len() > 128)
            || self
                .journal_path
                .as_ref()
                .is_none_or(|v| v.trim().is_empty())
            || !(1..=30).contains(&self.dms_timeout_secs)
            || self.reconcile_interval_secs != 1
        {
            return Err(fail(
                "production requires identity, run token, journal, bounded 1-30-second DMS and one-second reconciliation",
            ));
        }
        self.execution_envelope
            .as_ref()
            .ok_or_else(|| fail("missing envelope"))?
            .validate(
                nautilus_core::time::get_atomic_clock_realtime()
                    .get_time_ns()
                    .as_u64(),
            )
            .map_err(OndoExecutionConfigError::InvalidProductionEnvelope)
    }

    /// Validates the configuration, before any client or socket exists.
    ///
    /// # Errors
    ///
    /// Returns [`OndoExecutionConfigError::ProductionOrdersUnsupported`] when
    /// [`Self::allow_production_orders`] is set - there is no production write branch to open -
    /// [`OndoExecutionConfigError::MissingAccountId`] when no account was named, and
    /// [`OndoExecutionConfigError::ProductionWritesUnsupported`] when production is configured
    /// without `account_read_only`. The environment and base URL are judged separately, by
    /// [`crate::common::credential::validate_authenticated_environment`].
    pub fn validate(&self) -> Result<(), OndoExecutionConfigError> {
        if self.allow_production_orders
            && (self.environment != OndoEnvironment::Production
                || self.account_read_only
                || self.execution_envelope.is_none())
        {
            return Err(OndoExecutionConfigError::ProductionOrdersUnsupported);
        }
        if self.execution_envelope.is_some() {
            self.validate_production_envelope()?;
        }

        if self.account_id.is_none() {
            return Err(OndoExecutionConfigError::MissingAccountId);
        }

        self.authentication_scope()?;

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
        assert!(
            config.journal_path.is_none(),
            "no journal is configured by default, and the run says so rather than writing one              somewhere it was not asked to",
        );
        assert!(!config.allow_production_orders);
        assert!(
            config.expected_venue_account_id.is_none(),
            "no venue account id is configured by default, so identity is unknown until one is",
        );
        assert_eq!(
            config.authentication_scope(),
            Ok(OndoAuthenticationScope::SandboxTrading),
            "a default execution configuration is a sandbox trading session",
        );
        assert_eq!(
            config.http_base_url(),
            "https://api.ondoperps-sandbox.xyz",
            "a default execution configuration resolves to the sandbox host",
        );
    }

    /// The scope is the one pair of members that decides the capability, and production has no
    /// trading variant: a production configuration without `account_read_only` is refused before a
    /// credential is read.
    #[rstest]
    fn test_the_authentication_scope_is_derived_from_the_environment_and_the_read_only_flag() {
        let sandbox_trading = OndoExecutionClientConfig {
            environment: OndoEnvironment::Sandbox,
            account_read_only: false,
            ..Default::default()
        };
        assert_eq!(
            sandbox_trading.authentication_scope(),
            Ok(OndoAuthenticationScope::SandboxTrading),
        );

        let sandbox_read_only = OndoExecutionClientConfig {
            environment: OndoEnvironment::Sandbox,
            account_read_only: true,
            ..Default::default()
        };
        assert_eq!(
            sandbox_read_only.authentication_scope(),
            Ok(OndoAuthenticationScope::SandboxReadOnly),
        );

        let production_read_only = OndoExecutionClientConfig {
            environment: OndoEnvironment::Production,
            account_read_only: true,
            ..Default::default()
        };
        assert_eq!(
            production_read_only.authentication_scope(),
            Ok(OndoAuthenticationScope::ProductionReadOnly),
        );

        let production_writes = OndoExecutionClientConfig {
            environment: OndoEnvironment::Production,
            account_read_only: false,
            account_id: Some(AccountId::from("ONDO-MAINNET-001")),
            ..Default::default()
        };
        assert_eq!(
            production_writes
                .validate()
                .expect_err("production has no write scope"),
            OndoExecutionConfigError::ProductionWritesUnsupported,
        );
        assert_eq!(
            production_writes.authentication_scope(),
            Err(OndoExecutionConfigError::ProductionWritesUnsupported),
        );

        // A production read-only configuration with an account validates.
        let mut named = production_read_only;
        named.account_id = Some(AccountId::from("ONDO-MAINNET-001"));
        assert_eq!(named.validate(), Ok(()));
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

    /// The expected venue account id is a plain optional member: it is carried so the client can
    /// verify identity, and never derived from the Nautilus account id by string splitting.
    #[rstest]
    fn test_the_execution_config_carries_the_expected_venue_account_id_verbatim() {
        let config: OndoExecutionClientConfig = serde_json::from_str(
            r#"{"account_id": "ONDO-MAINNET-001", "expected_venue_account_id": "10458932786832481"}"#,
        )
        .unwrap();

        assert_eq!(
            config.expected_venue_account_id.as_deref(),
            Some("10458932786832481"),
            "the venue id is carried verbatim, with no prefix guessing",
        );
        assert_eq!(
            config.account_id,
            Some(AccountId::from("ONDO-MAINNET-001")),
            "the Nautilus account id is a different identifier and stays as it was",
        );
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
            validate_authenticated_environment(
                OndoAuthenticationScope::SandboxTrading,
                default.http_base_url()
            ),
            Ok(OndoEndpoint::Official),
            "the default resolves to the official sandbox host, never a test service",
        );

        let local = OndoExecutionClientConfig {
            base_url_http: Some("http://127.0.0.1:8080".to_string()),
            ..Default::default()
        };
        assert_eq!(
            validate_authenticated_environment(
                OndoAuthenticationScope::SandboxTrading,
                local.http_base_url()
            ),
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
                    OndoAuthenticationScope::SandboxTrading,
                    remote.http_base_url()
                )
                .is_err(),
                "`{url}` is not an endpoint this configuration may sign for",
            );
        }
    }

    /// The production read-only configuration resolves to the production host and is admitted by
    /// its own scope; the sandbox host is refused for it, and a sandbox session refuses the
    /// production host. Cross-environment mixing is never admitted.
    #[rstest]
    fn test_a_production_read_only_configuration_cannot_resolve_to_the_sandbox_authority() {
        let production = OndoExecutionClientConfig {
            environment: OndoEnvironment::Production,
            account_read_only: true,
            account_id: Some(AccountId::from("ONDO-MAINNET-001")),
            ..Default::default()
        };
        let scope = production.authentication_scope().unwrap();

        assert_eq!(production.http_base_url(), "https://api.ondoperps.xyz");
        assert_eq!(
            validate_authenticated_environment(scope, production.http_base_url()),
            Ok(OndoEndpoint::Official),
        );
        assert!(
            validate_authenticated_environment(scope, "https://api.ondoperps-sandbox.xyz").is_err(),
            "a production session never signs for the sandbox authority",
        );
    }

    #[rstest]
    fn test_the_execution_config_deserializes_with_defaults_and_rejects_unknown_fields() {
        let config: OndoExecutionClientConfig =
            serde_json::from_str(r#"{"account_id": "ONDO-SANDBOX-001"}"#).unwrap();

        assert_eq!(config.environment, OndoEnvironment::Sandbox);
        assert_eq!(config.dms_timeout_secs, 30);
        assert!(config.journal_path.is_none());

        let with_journal: OndoExecutionClientConfig = serde_json::from_str(
            r#"{"account_id": "ONDO-SANDBOX-001", "journal_path": "reports/ondo-journal.json"}"#,
        )
        .unwrap();

        assert_eq!(
            with_journal.journal_path.as_deref(),
            Some("reports/ondo-journal.json"),
            "the durable journal's path is part of the configuration surface",
        );
        assert!(
            serde_json::from_str::<OndoExecutionClientConfig>(r#"{"account_idz": "x"}"#).is_err(),
            "an unknown configuration field is rejected",
        );
    }
}
