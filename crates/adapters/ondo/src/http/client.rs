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

//! The Ondo Perps REST client.
//!
//! This is the **public** REST transport: it takes no credentials, never reads an API key and never
//! loads a `.env` file. Every method returns the schema data the venue sent, through the single
//! parsing boundary in [`crate::http::models`] or as untouched JSON text, so a decimal string is
//! never re-serialized and never routed through an `f64`.
//!
//! # What the client owns, and what it does not
//!
//! - Transport, timeout, retry policy, error classification and the shared rate budget
//!   ([`crate::http::rate_limit::OndoRateBudget`]).
//! - The request target ([`OndoRequestTarget`]) is the exact path-and-query byte string that a
//!   later signature will be computed over; nothing here re-encodes it.
//! - It does **not** own WebSocket state. The public feed is [`crate::websocket`]; this module never
//!   touches a socket that carries market data.
//! - It does **not** own order semantics. [`Self::post_raw`] is single-shot on purpose: an error
//!   from it does not say whether the venue applied the request, and deciding that is Task 7's
//!   "result unknown" handling, not a retry this client may take.
//!
//! # Retries
//!
//! A **read** is retried within the configured policy: transport failures, timeouts, 5xx responses
//! and 429 refusals. A 429's `Retry-After` is honoured as the minimum wait when the venue sent it as
//! a whole number of seconds; when it is absent (or in its HTTP-date form, which this adapter cannot
//! verify) the policy's exponential backoff applies, and the attempt count is bounded by the policy.
//! A **POST** is never replayed. A 4xx rejection is terminal and is never retried in a loop.
//!
//! # The new-risk guard
//!
//! A signed `POST` **is** new risk: the venue's only signed POSTs create orders. So the send point
//! carries one more gate than the caller's own checks ([`OndoNewRiskGuard`]), and it is the last one
//! there is - after the shared budget has been acquired and before the request is built. Between the
//! caller's decision and the wire there is a wait for that budget, and the account can stop
//! admitting new risk inside it; this is where that is caught. The gate is a re-check of the *same*
//! run generation the caller was admitted under ([`NewRiskPermit`]), not a fresh reading of the
//! account, so a permit that has been superseded cannot be re-admitted by a later state.
//!
//! A refusal is not an unknown outcome: the request was never built, so nothing reached the venue,
//! and it is reported as [`OndoNewRiskSendError::Refused`] rather than as a transport failure.
//!
//! A signed `DELETE` carries **no** such gate. A cancel reduces risk, and the plan requires that
//! "cannot place new orders" never blocks the cleanup that follows from an unsettled one; a gate
//! here would refuse exactly the cancels an unknown outcome makes necessary.
//!
//! # The authenticated surface
//!
//! A client built with a [`OndoCredential`] signs every private request with that credential
//! ([`crate::signing`]) and reads the private endpoints ([`Self::get_account`],
//! [`Self::get_orders`], [`Self::get_fills`], [`Self::get_positions`], [`Self::get_balance`], and
//! the raw [`Self::get_signed`] / [`Self::post_signed_raw`] / [`Self::delete_signed_raw`] seams).
//!
//! It also carries the **order write surface**: [`Self::create_order`],
//! [`Self::create_orders_batch`], the two lookups ([`Self::get_order`],
//! [`Self::get_client_order`]) and the three cancels ([`Self::cancel_order`],
//! [`Self::cancel_client_order`], [`Self::cancel_market_orders`]). Every one of them takes its
//! target and its body from [`crate::http::orders`] rather than building either here, so the bytes
//! the signature covers are the bytes [`OndoRequestTarget::as_str`] and the body `Vec<u8>` put on
//! the wire (plan §6.1). The three cancel endpoints are `DELETE` - the frozen REST spec has no
//! `POST .../cancel` - which is what [`Self::delete_signed_raw`] exists for.
//!
//! - The environment gate ([`crate::common::credential::validate_authenticated_environment`]) runs
//!   in the constructor, before the client exists: a sandbox credential with a base URL outside the
//!   endpoint allowlist ([`crate::common::endpoint::OndoEndpointPolicy`] - the environment's own
//!   host, or a loopback test service) is a build error, not a request the venue gets to refuse.
//!   The URL is judged by the parser the transport itself uses, so it is the *authority* that is
//!   admitted and not a string that resembles it.
//! - An authenticated client refuses redirects outright ([`HttpRedirectPolicy::Reject`]): the
//!   signature headers travel as this adapter's own names, which the transport's cross-host
//!   sensitive-header stripping does not cover, so a followed hop would carry a valid signature to
//!   whichever authority the answer named. The public transport keeps the default policy.
//! - Sign-then-send shares the *same* [`OndoRateBudget`] as the public reads: a signed request
//!   acquires a slot through [`OndoRateBudget::acquire`] exactly where a public read does, so the
//!   in-process budget is one budget, not one per surface (§4.4). The priority class travels with
//!   the request ([`OndoRequestPriority`]): Task 7/8's cancel, unknown-order and reconciliation
//!   traffic will pass [`OndoRequestPriority::High`] through these same seams, and §4.4's reserved
//!   slot attaches in [`OndoRateBudget::acquire`] rather than in a second path here.
//! - The bytes signed are the bytes sent: the signature covers `target.as_str()` and the URL is
//!   `base_url + target.as_str()`, with no second encoding step.
//! - A read may retry; a signed POST and a signed DELETE never do
//!   ([`Self::post_signed_raw`] and [`Self::delete_signed_raw`] are single-shot). A cancel is a
//!   state-changing request like a submission: replaying one after a timeout could cancel twice.
//! - A clock the venue's own `Date` header shows to be outside the signature tolerance refuses to
//!   sign at all ([`crate::signing::check_clock_skew`]).

use std::{
    collections::HashMap,
    str,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use nautilus_network::{
    http::{HttpClient, HttpClientError, HttpRedirectPolicy, HttpResponse, Method},
    retry::{RetryConfig, RetryError, RetryManager, create_http_retry_manager},
};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    common::{
        consts::ONDO_HTTP_TIMEOUT_SECS,
        credential::{OndoCredential, validate_authenticated_environment},
        endpoint::OndoEndpoint,
    },
    http::{
        error::{
            OndoHttpError, OndoHttpResult, classify_auth_rejection, extract_error_code,
            should_retry_ondo_http_error,
        },
        models::{MarketsResponse, parse_markets},
        orders::{
            OndoApiOrder, OndoBatchAddOrderResponse, OndoBatchError, OndoOrderCommand, batch_body,
            batch_create_target, client_order_lookup_target, create_order_target,
            market_cancel_target, order_lookup_target,
        },
        private::{
            ACCOUNT_PATH, BALANCE_PATH, FUNDING_FEES_PATH, OndoPrivateReadQuery,
            OndoPrivateResponse, POSITIONS_PATH,
        },
        query::{
            CONTRACTS_PATH, FILLS_PATH, MARKETS_PATH, ORDERS_PATH, OndoRequestTarget, STATUS_PATH,
        },
        rate_limit::{OndoRateBudget, OndoRequestPriority},
    },
    recording::RAW_MD_HEADER_WHITELIST,
    signing::{
        check_clock_skew, http_date_offset_secs, now_millis, now_secs, sign_rest, signed_headers,
    },
};

/// What a signed `POST` is refused with when the client holds no new-risk guard.
///
/// An execution client always installs one; a client built without one is a client whose write
/// surface cannot say whether the account admits new risk, and this adapter does not send from one.
const NO_NEW_RISK_GUARD: &str =
    "this client has no new-risk guard, so a signed write cannot be admitted";

/// The response header a 429 carries its requested wait in.
///
/// It is also the one name in [`RAW_MD_HEADER_WHITELIST`] the retry policy reads; the transport
/// retains exactly the whitelist, so a response header outside it is neither read nor recordable.
const RETRY_AFTER_HEADER: &str = "retry-after";

/// The response header the venue's own clock is read from.
///
/// It is in [`RAW_MD_HEADER_WHITELIST`], so the transport already retains it. Only the *offset* it
/// implies is ever used, and only to refuse a signature: see [`OndoAuth`].
const SERVER_DATE_HEADER: &str = "date";

/// The offset a client that has not yet seen a `Date` header uses.
///
/// A sentinel rather than `0`: "no evidence" and "the clocks agree" are different facts, and only
/// the second is worth representing as zero.
const NO_SERVER_OFFSET: i64 = i64::MIN;

/// The credential a client signs with, and the clock evidence it has gathered.
///
/// The credential is held once and shared through an [`Arc`] - it is deliberately not `Clone` - so
/// cloning the client never copies the secret. The same handle is what the client's constructor is
/// handed, which is how one credential signs both the REST requests and the private WebSocket's
/// login frame without either surface holding a second copy of the secret.
#[derive(Debug)]
struct OndoAuth {
    credential: Arc<OndoCredential>,
    /// Local time minus venue time, in whole seconds, from the last `Date` header the venue sent.
    server_offset_secs: AtomicI64,
}

impl OndoAuth {
    fn new(credential: Arc<OndoCredential>) -> Self {
        Self {
            credential,
            server_offset_secs: AtomicI64::new(NO_SERVER_OFFSET),
        }
    }

    /// Records the clock offset the venue's `Date` header implies.
    ///
    /// An unreadable header leaves the previous evidence in place rather than replacing it with
    /// nothing: the adapter's clock does not change because a header did.
    fn observe(&self, headers: &HashMap<String, String>) {
        let Some(date) = header_value(headers, SERVER_DATE_HEADER) else {
            return;
        };

        let Ok(local_secs) = now_secs() else {
            return;
        };

        if let Some(offset) = http_date_offset_secs(local_secs, date) {
            self.server_offset_secs.store(offset, Ordering::Relaxed);
            log::debug!("Observed an Ondo Perps server clock offset of {offset} s");
        }
    }

    /// Refuses a clock the venue's own header has already shown to be out of tolerance.
    ///
    /// Before the first `Date` header there is no evidence, so the request is signed; the venue's
    /// answer is then the evidence, and it is recorded by [`Self::observe`] whichever way it went.
    ///
    /// # Errors
    ///
    /// Returns [`OndoSigningError::ClockSkew`] when the last observed offset cannot be signed from.
    fn check_clock(&self) -> Result<(), crate::signing::OndoSigningError> {
        match self.server_offset_secs.load(Ordering::Relaxed) {
            NO_SERVER_OFFSET => Ok(()),
            offset => check_clock_skew(offset),
        }
    }
}

/// Returns a response header case-insensitively, as the transport's retained headers are keyed.
fn header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _value)| key.eq_ignore_ascii_case(name))
        .map(|(_key, value)| value.as_str())
}

/// The admission a new-risk write was granted under, presented again at the send point.
///
/// A permit is the *claim*, not the licence: the caller took it from the account's admission
/// decision, and [`OndoNewRiskGuard::revalidate`] is where the same claim is re-checked against the
/// account's current state. Carrying the generation is what makes that a re-check: a guard that only
/// reads the state again would admit a permit the account has since superseded, whenever the account
/// happens to admit new risk again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NewRiskPermit(u64);

impl NewRiskPermit {
    /// Wraps the run generation an admission decision was granted under.
    #[must_use]
    pub const fn new(generation: u64) -> Self {
        Self(generation)
    }

    /// Returns the run generation this permit was granted under.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.0
    }
}

/// The account's own say on whether a new-risk write may leave this process.
///
/// The transport holds one when it is built with a credential, and consults it at the one point
/// where the question is still answerable: inside the signed `POST`, **after** the shared rate
/// budget has been acquired and **before** a single byte of the request exists. Everything earlier
/// is a decision the caller took on a state that a wait can invalidate; everything later is a
/// request that has already been sent.
///
/// It is deliberately not consulted on the cancel path: see the module docs, "The new-risk guard".
pub trait OndoNewRiskGuard: Send + Sync + std::fmt::Debug {
    /// Re-checks `permit` against the account as it stands now.
    ///
    /// # Errors
    ///
    /// Returns the account's own reason for refusing the write. A refusal is a decision, not an
    /// unknown outcome: no request was built, so nothing reached the venue.
    fn revalidate(&self, permit: NewRiskPermit) -> Result<(), String>;
}

/// A successful public read: its status, the response headers this client retains, and its body.
///
/// The headers are the ones named in [`RAW_MD_HEADER_WHITELIST`]: the transport is configured with
/// that set, so a header the venue sends that is not in it never reaches this type, and the recorder
/// that is handed one of these keeps the whitelist applied a second time on its own side.
#[derive(Clone, Debug)]
pub struct OndoRawResponse {
    /// The HTTP status the venue answered with.
    pub status: u16,
    /// The retained response headers, by name as the transport reported them.
    pub headers: HashMap<String, String>,
    /// The response body, exactly as it was received.
    pub body: Vec<u8>,
}

/// A REST client for the Ondo Perps API.
///
/// Without a credential the client is the **public** transport: it reads public market data, never
/// reads an API key and never loads a `.env` file. With one it additionally signs the private read
/// surface. The client is cheap to clone (the underlying connection pool, the rate budget and the
/// credential handle are shared) and constructing it performs no I/O: the environment gate is a
/// pure decision, and no socket is opened until a request is sent.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.ondo", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.ondo")
)]
#[derive(Debug, Clone)]
pub struct OndoHttpClient {
    base_url: String,
    client: HttpClient,
    retry_manager: RetryManager<OndoHttpError>,
    budget: OndoRateBudget,
    auth: Option<Arc<OndoAuth>>,
    /// The authority class this client's signed requests go to, or [`None`] for a public client.
    endpoint: Option<OndoEndpoint>,
    /// The account's say on new-risk writes, or [`None`] for a client that has none.
    ///
    /// An authenticated client without one refuses every signed `POST`: a write surface that cannot
    /// say whether the account admits new risk is not a write surface this adapter sends from.
    new_risk_guard: Option<Arc<dyn OndoNewRiskGuard>>,
}

#[bon::bon]
impl OndoHttpClient {
    /// Creates a client for `base_url`.
    ///
    /// `timeout_secs` is the per-request timeout that applies to the whole request, and defaults to
    /// [`ONDO_HTTP_TIMEOUT_SECS`]. `budget` is the shared REST budget: pass the *same* instance to
    /// the data client and the execution client so one process has one budget, not one per client
    /// (plan §4.4). `retry_config` overrides the bounded retry policy; the default is the
    /// framework's HTTP policy (four attempts overall).
    ///
    /// `credential` turns the client into the authenticated transport. Resolve it with
    /// [`crate::common::credential::resolve_credential`], which applies the environment gate before
    /// it reads anything; this constructor applies the same gate again, so an authenticated client
    /// cannot exist for an environment or a host the gate refuses, whatever the caller passed.
    ///
    /// The rate limiter is deliberately *not* installed inside the HTTP transport: every request
    /// goes through [`OndoRateBudget::acquire`] in this client, which is the one place a priority
    /// request will reserve its slot.
    ///
    /// `new_risk_guard` is the account's say on new-risk writes, consulted at the send point of
    /// every signed `POST` ([`Self::post_signed_raw`]). An authenticated client built without one
    /// refuses every such write rather than sending it unverified, so whatever builds the execution
    /// client is obliged to hand one in.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::Network`] if the underlying HTTP client cannot be built, or
    /// [`OndoHttpError::Environment`] if a credential was given for an environment or a base URL
    /// the endpoint policy refuses: the base URL is judged as a URL (scheme, host, port, userinfo),
    /// not as a string, and a base URL the policy cannot place is a construction error rather than
    /// a request the venue gets to answer.
    #[builder]
    pub fn new(
        base_url: String,
        #[builder(default = ONDO_HTTP_TIMEOUT_SECS)] timeout_secs: u64,
        budget: Option<OndoRateBudget>,
        retry_config: Option<RetryConfig>,
        credential: Option<Arc<OndoCredential>>,
        new_risk_guard: Option<Arc<dyn OndoNewRiskGuard>>,
    ) -> OndoHttpResult<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();

        // The gate runs before the client holds the credential at all, so a refused session has no
        // object to send from. It also decides the redirect policy below, which is why the
        // transport is built after it rather than before.
        let (auth, endpoint) = match credential {
            Some(credential) => {
                let endpoint =
                    validate_authenticated_environment(credential.environment(), &base_url)?;

                if endpoint == OndoEndpoint::LoopbackTestService {
                    log::warn!(
                        "The authenticated Ondo Perps transport is pointed at a loopback test \
                         service ({base_url}): this session cannot reach the venue",
                    );
                }

                (Some(Arc::new(OndoAuth::new(credential))), Some(endpoint))
            }
            None => (None, None),
        };

        let client = HttpClient::builder()
            .header_keys(
                RAW_MD_HEADER_WHITELIST
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect(),
            )
            .timeout_secs(timeout_secs)
            .rate_limiters(Vec::new())
            // An authenticated request is never replayed through a redirect. The transport follows
            // up to ten hops by default and re-sends the caller's headers on each one; the headers
            // that carry this adapter's signature are its own names, so the stripping the transport
            // applies to the standard credential headers on a cross-host hop does not reach them.
            // `Reject` is the only setting that makes such a hop impossible: the transport exposes
            // no per-hop hook, and following a hop and stripping the headers afterwards is not
            // something it offers. A public read carries no credential and keeps the default
            // policy.
            .redirect_policy(if auth.is_some() {
                HttpRedirectPolicy::Reject
            } else {
                HttpRedirectPolicy::Follow
            })
            .build()
            .map_err(|error| OndoHttpError::Network(error.to_string()))?;

        let retry_manager: RetryManager<OndoHttpError> = match retry_config {
            Some(config) => RetryManager::new(config),
            None => create_http_retry_manager(),
        };

        Ok(Self {
            base_url,
            client,
            retry_manager,
            budget: budget.unwrap_or_default(),
            auth,
            endpoint,
            new_risk_guard,
        })
    }
}

impl OndoHttpClient {
    /// Returns the configured REST base URL, without a trailing slash.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the shared REST budget this client draws on.
    ///
    /// Task 7 injects this same budget into the execution client so cancels, unknown-order queries
    /// and reconciliation pace against the metadata reads instead of a second budget.
    #[must_use]
    pub fn budget(&self) -> &OndoRateBudget {
        &self.budget
    }

    /// Calls `GET /status` and returns the unwrapped response body.
    ///
    /// **The envelope cannot be confirmed in either direction, and this function is the reason.**
    /// The frozen spec documents the answer as `GenericResponse` + `StatusResult`, and every
    /// `/status` call this project has ever made has gone through this function, whose reader strips
    /// a `result` member when one is present. A wire body that carried the envelope and one that did
    /// not therefore produce byte-identical results here, so no *raw* `/status` body has ever been
    /// retained and the capture cannot settle the question.
    ///
    /// Do not read the older "the host refused it" note as still current. A single P0 probe at
    /// 2026-09-14T11:50:22Z did answer HTTP 403 (Cloudflare `error code: 1010`), but `/status` has
    /// answered **200 on every call since** - 2026-09-14T15:15:02Z, 2026-09-14T16:27:21Z and
    /// 2026-09-15T03:31:08Z - each time returning `marketStatus: open` and nothing else. That is a
    /// successful read, not a verified shape.
    ///
    /// A body whose object carries a `result` member is unwrapped, including the `success: false`
    /// check; anything else is returned as it arrived rather than being forced into an envelope it
    /// may not have. This is diagnostics only: no member of it is ever read as a price, quantity or
    /// fee.
    ///
    /// # Errors
    ///
    /// Returns a transport or classification error, [`OndoHttpError::Decode`] if the body is not
    /// UTF-8 JSON, or [`OndoHttpError::Unsuccessful`] if the envelope reports failure.
    pub async fn get_status(&self) -> OndoHttpResult<serde_json::Value> {
        let body = self.get_body(&OndoRequestTarget::new(STATUS_PATH)).await?;

        decode_status(&text_body(STATUS_PATH, &body)?)
    }

    /// Calls `GET /v1/markets` and returns the response schema.
    ///
    /// The response is parsed by [`parse_markets`], so every increment and fee stays the decimal
    /// string the venue sent and the fail-closed rules described in [`crate::http::models`] apply.
    ///
    /// # Errors
    ///
    /// Returns a transport or classification error, or the schema error `parse_markets` produces
    /// for a body that is not market metadata.
    pub async fn get_markets(&self) -> OndoHttpResult<MarketsResponse> {
        let body = self.get_body(&OndoRequestTarget::new(MARKETS_PATH)).await?;

        parse_markets(&text_body(MARKETS_PATH, &body)?)
    }

    /// Calls `GET /v1/perps/contracts` and returns one entry per contract.
    ///
    /// Each entry is the **exact JSON text** of one `Contract`, so a decimal member the venue sends
    /// as a number cannot be routed through an `f64` by a [`serde_json::Value`]. The contract schema
    /// was sampled live twice on 2026-09-14 (`reports/ondo-acceptance/`: `rest-capture/contracts.json`
    /// and `preflight/contracts.json`); nothing here parses against a schema, though - only the
    /// documented field names are relied on.
    ///
    /// # Errors
    ///
    /// Returns a transport or classification error, [`OndoHttpError::Decode`] if the body is not
    /// UTF-8 JSON of the expected envelope shape, [`OndoHttpError::Unsuccessful`] if the envelope
    /// reports failure, [`OndoHttpError::MissingField`] if `result` is absent, or
    /// [`OndoHttpError::EmptyResult`] if the venue publishes no contracts at all.
    pub async fn get_contracts(&self) -> OndoHttpResult<Vec<Box<RawValue>>> {
        let body = self
            .get_body(&OndoRequestTarget::new(CONTRACTS_PATH))
            .await?;

        decode_contracts(&text_body(CONTRACTS_PATH, &body)?)
    }

    /// Sends a single `GET` and returns the raw response body.
    ///
    /// This is the raw read the typed endpoints are built on; later tasks (fills, orders, account
    /// queries) read through it. The request is retried within the client's retry policy.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout, classification or pagination error.
    pub async fn get_raw(&self, target: &OndoRequestTarget) -> OndoHttpResult<Vec<u8>> {
        self.get_body(target).await
    }

    /// Sends a single `GET` and returns its status, retained headers and raw body.
    ///
    /// This is the read a metadata snapshot is taken from: the status and the headers are what make
    /// the body attributable, and the body is returned exactly as it arrived. The headers are the
    /// whitelist the client's transport retains ([`RAW_MD_HEADER_WHITELIST`]), so nothing outside it
    /// can be carried into a snapshot. The request is retried within the client's retry policy.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout, classification or pagination error.
    pub async fn get_raw_response(
        &self,
        target: &OndoRequestTarget,
    ) -> OndoHttpResult<OndoRawResponse> {
        self.get_response(target).await
    }

    /// Sends a single-shot `POST` and returns the raw response body.
    ///
    /// The request is sent **exactly once**: a POST is never replayed automatically, because a
    /// timeout, a 5xx or a lost acknowledgement leaves the outcome unknown, and a retry could apply
    /// the request twice. Nothing here decides that outcome; the seam for it is the error this
    /// returns, whose kind (transport, timeout, HTTP status) is what Task 7's "result unknown"
    /// handling - query by client order id, or reconcile - reads.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout or classification error. A 4xx rejection is terminal.
    pub async fn post_raw(
        &self,
        target: &OndoRequestTarget,
        body: Vec<u8>,
    ) -> OndoHttpResult<Vec<u8>> {
        let url = self.url(target);

        self.budget.acquire(OndoRequestPriority::Normal).await;

        let response = self
            .client
            .request(Method::POST, url, None, None, Some(body), None, None)
            .await
            .map_err(transport_error)?;

        check_response(response).map(|response| response.body)
    }

    // --------------------------------------------------------------------------------------------
    // The authenticated surface
    // --------------------------------------------------------------------------------------------

    /// Returns `true` when this client holds a credential and can sign private requests.
    #[must_use]
    pub const fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }

    /// Returns the authority class this client signs for, or [`None`] when it is the public
    /// transport, which carries no credential and is not gated.
    ///
    /// The class is the endpoint policy's decision, made once in the constructor:
    /// [`OndoEndpoint::Official`] is the environment's own host, and
    /// [`OndoEndpoint::LoopbackTestService`] is the explicit local test service a session cannot
    /// reach the venue from.
    #[must_use]
    pub const fn endpoint_kind(&self) -> Option<OndoEndpoint> {
        self.endpoint
    }

    /// Calls `GET /v1/account` and returns the envelope it answered with.
    ///
    /// **The path is documented but unverified**: the frozen REST spec declares `GET /v1/account`
    /// (`summary: "Get Account"`), and no authenticated request has ever been made, so no host has
    /// answered it. Documented is not the same fact as verified - see [`crate::http::private`].
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::NotAuthenticated`] when the client has no credential, the named
    /// auth rejection for a 401/403, a transport or classification error, or a schema error for an
    /// answer that is not the documented envelope.
    pub async fn get_account(&self) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(
            &OndoRequestTarget::new(ACCOUNT_PATH),
            OndoRequestPriority::Normal,
        )
        .await
    }

    /// Calls `GET /v1/perps/orders` and returns one page of orders.
    ///
    /// Each item is the exact JSON text the venue sent, so the venue's own order status string is
    /// preserved verbatim inside it (plan §6.3); this read classifies nothing.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`].
    pub async fn get_orders(
        &self,
        query: &OndoPrivateReadQuery,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(&query.target(ORDERS_PATH), OndoRequestPriority::Normal)
            .await
    }

    /// Calls `GET /v1/perps/fills` and returns one page of fills.
    ///
    /// The response's [`OndoPrivateResponse::fills`] decodes the documented `ApiFill` members, with
    /// every decimal left as the string the venue sent.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`].
    pub async fn get_fills(
        &self,
        query: &OndoPrivateReadQuery,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(&query.target(FILLS_PATH), OndoRequestPriority::Normal)
            .await
    }

    /// Calls `GET /v1/perps/positions` and returns one page of positions.
    ///
    /// **The path is documented but unverified**: the frozen REST spec declares it (`summary: "Get
    /// Positions"`) and documents it as taking no query parameters - which
    /// [`OndoPrivateReadQuery::target`] records as an open item rather than silently changing.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`].
    pub async fn get_positions(
        &self,
        query: &OndoPrivateReadQuery,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(&query.target(POSITIONS_PATH), OndoRequestPriority::Normal)
            .await
    }

    /// Calls `GET /v1/perps/funding_fees` and returns one page of funding payments.
    ///
    /// **The path is documented but unverified**: the frozen REST spec declares it (`summary: "Get
    /// Funding Fee Payments"`) and documents every member of a record as required. The response's
    /// [`OndoPrivateResponse::funding_fees`] reads them, with every decimal left as the string the
    /// venue sent.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`].
    pub async fn get_funding_fees(
        &self,
        query: &OndoPrivateReadQuery,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(
            &query.target(FUNDING_FEES_PATH),
            OndoRequestPriority::Normal,
        )
        .await
    }

    /// Calls `GET /v1/perps/balance` and returns the account's balances.
    ///
    /// **The path is documented but unverified**: the frozen REST spec declares it (`summary: "Get
    /// Balance"`) and documents it as taking no query parameters. The documented balance members
    /// (`walletBalance`, `marginBalance`, `usedMargin`, `availableMargin`, `withdrawableMargin`)
    /// stay in the raw result, distinct, and none of them is a Nautilus `AccountState` until Task 8
    /// maps them.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`].
    pub async fn get_balance(&self) -> OndoHttpResult<OndoPrivateResponse> {
        self.get_signed(
            &OndoRequestTarget::new(BALANCE_PATH),
            OndoRequestPriority::Normal,
        )
        .await
    }

    /// Sends a signed `GET` and returns the envelope it answered with.
    ///
    /// This is the seam the typed private reads are built on and the one Task 7/8's unknown-order
    /// queries and reconciliation reads use. `priority` is §4.4's traffic class: pass
    /// [`OndoRequestPriority::High`] for a query that must not queue behind metadata reads. The
    /// request is retried within the client's retry policy, and every attempt is signed afresh (the
    /// timestamp moves) over the same target bytes.
    ///
    /// # Errors
    ///
    /// Returns [`OndoHttpError::NotAuthenticated`] when the client has no credential,
    /// [`OndoHttpError::Signing`] when a skewed clock refuses to sign,
    /// [`OndoHttpError::AuthRejected`] for a 401/403, or a transport, classification or schema
    /// error.
    pub async fn get_signed(
        &self,
        target: &OndoRequestTarget,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        let auth = self.auth(target)?;
        let url = self.url(target);

        self.retry_manager
            .execute_with_retry_with_delay(
                target.as_str(),
                || {
                    let url = url.clone();

                    async move {
                        self.budget.acquire(priority).await;

                        // Sign, then send the same bytes: `url` is `base_url + target.as_str()`.
                        let headers = signed_request_headers(auth, "GET", target, &[])?;
                        let response = self
                            .client
                            .request(Method::GET, url, None, Some(headers), None, None, None)
                            .await
                            .map_err(transport_error)?;

                        auth.observe(&response.headers);

                        check_private_response(response, auth)
                    }
                },
                should_retry_ondo_http_error,
                retry_delay,
                retry_error_to_ondo_http_error,
            )
            .await
    }

    /// Sends a **single-shot** signed `POST` and returns the envelope it answered with.
    ///
    /// Like [`Self::post_raw`] this is never replayed: the signed path is what an order submission
    /// or a cancel runs on, and a timeout, a 5xx or a lost acknowledgement leaves the outcome
    /// unknown, so a retry could apply the request twice. `priority` is §4.4's traffic class:
    /// Task 7/8 pass [`OndoRequestPriority::High`] for a cancel and
    /// [`OndoRequestPriority::Normal`] for a submission. The body handed here is the body signed
    /// and the body sent - the same `Vec<u8>`, moved, never re-encoded.
    ///
    /// `permit` is the admission the caller was granted for this write, and **this is where it
    /// stops being a claim**: the guard is consulted after the budget wait and before the request
    /// is built, so a wait that outlives the admission cannot produce a request. A signed `POST` is
    /// an order creation - the venue has no other - so every one of them passes this gate, and a
    /// client that has no guard refuses rather than sends.
    ///
    /// # Errors
    ///
    /// Returns [`OndoNewRiskSendError::Refused`] when the guard refuses (or when there is no guard
    /// to admit the write): nothing was sent, and the reason is the account's. Otherwise see
    /// [`Self::get_signed`]; a 4xx rejection is terminal.
    pub async fn post_signed_raw(
        &self,
        target: &OndoRequestTarget,
        body: Vec<u8>,
        priority: OndoRequestPriority,
        permit: NewRiskPermit,
    ) -> Result<OndoPrivateResponse, OndoNewRiskSendError> {
        let auth = self.auth(target)?;
        let url = self.url(target);

        self.budget.acquire(priority).await;

        self.admit_new_risk(permit)?;

        let headers = signed_request_headers(auth, "POST", target, &body)?;
        let response = self
            .client
            .request(
                Method::POST,
                url,
                None,
                Some(headers),
                Some(body),
                None,
                None,
            )
            .await
            .map_err(transport_error)?;

        auth.observe(&response.headers);

        check_private_response(response, auth).map_err(OndoNewRiskSendError::from)
    }

    /// Sends a **single-shot** signed `DELETE` and returns the envelope it answered with.
    ///
    /// This is [`Self::post_signed_raw`]'s sibling and exists because the venue has no
    /// `POST .../cancel`: all three cancel endpoints (`DELETE /v1/perps/orders/{orderID}`,
    /// `DELETE /v1/perps/orders/batch` and `DELETE /v1/perps/orders?market=...`) are `DELETE`
    /// (plan §6.2). It is single-shot for the same reason a submission is: a cancel is a
    /// state-changing request, so a timeout, a 5xx or a lost acknowledgement leaves the outcome
    /// unknown, and a replay could cancel twice. Everything else is deliberately identical to
    /// [`Self::post_signed_raw`] - the same budget acquisition, the same clock observation, the
    /// same private-response classification - so the two write paths cannot drift apart.
    ///
    /// The one thing it does **not** carry is the new-risk guard, and that is the point of it: a
    /// cancel reduces risk, and an account that has stopped admitting new risk is exactly an account
    /// whose resting orders have to be cleaned up. Gating this path would refuse the cancels an
    /// unknown outcome makes necessary.
    ///
    /// The body is empty and is signed as empty: the cancel endpoints carry their parameters in
    /// the path and query, which [`OndoRequestTarget::as_str`] has already serialized once.
    ///
    /// This is the strict reader: a successful answer must carry the endpoint's `result`, because
    /// for a read a missing `result` is a truncated answer. Of the endpoints this adapter writes,
    /// exactly one is documented *without* a `result` - `DELETE /v1/perps/orders`, the market-wide
    /// cancel - and it is read by its own reader (`read_cancel_all`, below) through the same send
    /// rather than by a relaxed version of this.
    ///
    /// # Errors
    ///
    /// See [`Self::get_signed`]; a 4xx rejection is terminal and its `code` is what
    /// [`crate::http::orders::OndoCancelRejection::from_code`] classifies.
    pub async fn delete_signed_raw(
        &self,
        target: &OndoRequestTarget,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoPrivateResponse> {
        self.delete_signed_with(target, priority, OndoPrivateResponse::decode)
            .await
    }

    /// The one `DELETE` send, and the only difference there is between reading its answers.
    ///
    /// `decode` reads a 2xx body; everything else about the request - one attempt with no retry
    /// wrapper, an empty body signed as empty, the same budget acquisition, the same clock
    /// observation, the same classification of a non-2xx - is identical whichever reader is passed,
    /// which is what keeps the cancel paths from drifting apart.
    async fn delete_signed_with<T>(
        &self,
        target: &OndoRequestTarget,
        priority: OndoRequestPriority,
        decode: fn(u16, &[u8]) -> OndoHttpResult<T>,
    ) -> OndoHttpResult<T> {
        let auth = self.auth(target)?;
        let url = self.url(target);

        self.budget.acquire(priority).await;

        let headers = signed_request_headers(auth, "DELETE", target, &[])?;
        let response = self
            .client
            .request(Method::DELETE, url, None, Some(headers), None, None, None)
            .await
            .map_err(transport_error)?;

        auth.observe(&response.headers);

        check_private_response_with(response, auth, decode)
    }

    // --------------------------------------------------------------------------------------------
    // The order write surface (plan §6.2)
    // --------------------------------------------------------------------------------------------

    /// Calls `POST /v1/perps/orders` and returns the order the venue created.
    ///
    /// The body is [`OndoOrderCommand::body`] - the exact bytes that were signed - and the target
    /// is [`create_order_target`], so nothing is re-encoded between the signature and the wire. The
    /// command is validated before this is called ([`OndoOrderCommand::from_init`]); an order this
    /// adapter cannot express never reaches here.
    ///
    /// # Errors
    ///
    /// Returns [`OndoNewRiskSendError::Refused`] when the account stops admitting new risk while
    /// the request waits for the budget: no request was built, so no order was created. Otherwise a
    /// transport error or a classified rejection - a 400 carrying
    /// [`crate::http::orders::ONDO_POST_ONLY_HAS_MATCH`] is the one the caller must report as a
    /// post-only rejection - or [`OndoHttpError::Decode`] when the answer is not the documented
    /// `ApiOrder` payload. A decode failure after a 2xx is **not** evidence the order was not
    /// created, which is why the caller treats every failure here as an unknown outcome rather
    /// than as a refusal.
    pub async fn create_order(
        &self,
        command: &OndoOrderCommand,
        priority: OndoRequestPriority,
        permit: NewRiskPermit,
    ) -> Result<OndoApiOrder, OndoNewRiskSendError> {
        let response = self
            .post_signed_raw(&create_order_target(), command.body(), priority, permit)
            .await?;

        Ok(OndoApiOrder::from_text(response.raw_result())?)
    }

    /// Calls `POST /v1/perps/orders/batch` and returns the per-item answer.
    ///
    /// A 2xx answer carrying both `addedOrders` and `failedOrders` is the normal shape: HTTP 2xx
    /// does **not** mean every item succeeded (plan §6.2), which is why this returns the parsed
    /// [`OndoBatchAddOrderResponse`] rather than a count.
    ///
    /// # Errors
    ///
    /// Returns [`OndoNewRiskSendError::Local`] when the batch is one this adapter refuses to send
    /// (empty, past the venue's item cap, or carrying an item that is itself refused),
    /// [`OndoNewRiskSendError::Refused`] when the account stops admitting new risk while the batch
    /// waits for the budget, and [`OndoNewRiskSendError::Http`] for the transport and decode
    /// failures [`Self::create_order`] documents.
    pub async fn create_orders_batch(
        &self,
        commands: &[OndoOrderCommand],
        priority: OndoRequestPriority,
        permit: NewRiskPermit,
    ) -> Result<OndoBatchAddOrderResponse, OndoNewRiskSendError> {
        let body = batch_body(commands)?;

        let response = self
            .post_signed_raw(&batch_create_target(), body, priority, permit)
            .await?;

        Ok(OndoBatchAddOrderResponse::from_text(response.raw_result())?)
    }

    /// Calls `GET /v1/perps/orders/{orderID}` and returns the order.
    ///
    /// `order_ref` is a venue order id, or the `client:{clientOrderId}` form
    /// ([`crate::http::orders::client_lookup_value`]) - the lookup is a path segment, so it is
    /// percent-encoded by [`order_lookup_target`] exactly as it is signed.
    ///
    /// # Errors
    ///
    /// See [`Self::get_account`], and [`OndoHttpError::Decode`] for an answer that is not an
    /// `ApiOrder`.
    pub async fn get_order(
        &self,
        order_ref: &str,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoApiOrder> {
        let response = self
            .get_signed(&order_lookup_target(order_ref), priority)
            .await?;

        OndoApiOrder::from_text(response.raw_result())
    }

    /// Calls `GET /v1/perps/orders/client%3A{clientOrderId}` and returns the order.
    ///
    /// The client-order-id form is what recovers an order whose venue id this session never
    /// observed (plan §6.3: query by the original client order id, never by a new one).
    ///
    /// # Errors
    ///
    /// See [`Self::get_order`].
    pub async fn get_client_order(
        &self,
        client_order_id: &str,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoApiOrder> {
        let response = self
            .get_signed(&client_order_lookup_target(client_order_id), priority)
            .await?;

        OndoApiOrder::from_text(response.raw_result())
    }

    /// Calls `DELETE /v1/perps/orders/{orderID}`.
    ///
    /// # Errors
    ///
    /// See [`Self::delete_signed_raw`]. A rejection's `code` is classified by
    /// [`crate::http::orders::OndoCancelRejection::from_code`], and every code that classifies as
    /// requiring a query obliges the caller to confirm the order's state before reporting anything
    /// about it (plan §6.3).
    pub async fn cancel_order(
        &self,
        order_ref: &str,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoCancelAnswer> {
        self.cancel_target(&order_lookup_target(order_ref), priority)
            .await
    }

    /// Calls `DELETE /v1/perps/orders/client%3A{clientOrderId}`.
    ///
    /// # Errors
    ///
    /// See [`Self::cancel_order`].
    pub async fn cancel_client_order(
        &self,
        client_order_id: &str,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoCancelAnswer> {
        self.cancel_target(&client_order_lookup_target(client_order_id), priority)
            .await
    }

    /// Calls `DELETE /v1/perps/orders?market=...` and cancels every order on that market.
    ///
    /// This is the one cancel endpoint the frozen REST spec documents with a **bare**
    /// `GenericResponse`: `required: ["success"]`, and no `result` member in its 200 schema at all.
    /// Reading it with the strict [`Self::delete_signed_raw`] contract turned a cancel the venue
    /// had performed into a schema error, which is the worst answer available here - the orders are
    /// gone at the venue while this adapter still believes they rest, and no confirming read ever
    /// runs. It is read through [`read_cancel_all`] instead, which maps that documented success to
    /// [`OndoCancelAnswer::Unconfirmed`]: the cancel API having answered still is not the orders
    /// being cancelled (plan §6.3), so the caller owes the confirming read either way.
    ///
    /// # Errors
    ///
    /// See [`Self::cancel_order`]. A 4xx is classified exactly as the strict path classifies it.
    pub async fn cancel_market_orders(
        &self,
        market: &str,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoCancelAnswer> {
        self.delete_signed_with(&market_cancel_target(market), priority, read_cancel_all)
            .await
    }

    /// Sends one of the two by-id cancel endpoints and reads what it answered.
    ///
    /// # Errors
    ///
    /// See [`Self::delete_signed_raw`].
    async fn cancel_target(
        &self,
        target: &OndoRequestTarget,
        priority: OndoRequestPriority,
    ) -> OndoHttpResult<OndoCancelAnswer> {
        let response = self.delete_signed_raw(target, priority).await?;

        Ok(read_cancel_answer(response.raw_result()))
    }

    /// Asks the account whether `permit` still admits a new-risk write.
    ///
    /// This is the last gate before the wire and the only one the caller cannot be late for: it
    /// runs after the shared budget has been taken, so it sees the state the request would be built
    /// in rather than the state the command was accepted in.
    ///
    /// # Errors
    ///
    /// Returns [`OndoNewRiskSendError::Refused`] when the guard refuses the permit, and the same
    /// when there is no guard at all: an authenticated client that cannot answer this question does
    /// not send.
    fn admit_new_risk(&self, permit: NewRiskPermit) -> Result<(), OndoNewRiskSendError> {
        let Some(guard) = self.new_risk_guard.as_ref() else {
            return Err(OndoNewRiskSendError::Refused {
                reason: NO_NEW_RISK_GUARD.to_string(),
            });
        };

        guard
            .revalidate(permit)
            .map_err(|reason| OndoNewRiskSendError::Refused { reason })
    }

    /// Returns the credential this client signs with.
    fn auth(&self, target: &OndoRequestTarget) -> OndoHttpResult<&OndoAuth> {
        self.auth
            .as_deref()
            .ok_or_else(|| OndoHttpError::NotAuthenticated {
                target: target.as_str().to_string(),
            })
    }

    /// Builds the full URL for a target.
    ///
    /// The target is used verbatim: this is where the bytes a signature covers become the bytes on
    /// the wire, with no second encoding step.
    fn url(&self, target: &OndoRequestTarget) -> String {
        format!("{}{}", self.base_url, target.as_str())
    }

    /// Sends a `GET` through the retry policy and returns the body of a successful response.
    async fn get_body(&self, target: &OndoRequestTarget) -> OndoHttpResult<Vec<u8>> {
        self.get_response(target)
            .await
            .map(|response| response.body)
    }

    /// Sends a `GET` through the retry policy and returns the successful response itself.
    async fn get_response(&self, target: &OndoRequestTarget) -> OndoHttpResult<OndoRawResponse> {
        let url = self.url(target);

        self.retry_manager
            .execute_with_retry_with_delay(
                target.as_str(),
                || {
                    let url = url.clone();

                    async move {
                        self.budget.acquire(OndoRequestPriority::Normal).await;

                        let response = self
                            .client
                            .request(Method::GET, url, None, None, None, None, None)
                            .await
                            .map_err(transport_error)?;

                        check_response(response)
                    }
                },
                should_retry_ondo_http_error,
                retry_delay,
                retry_error_to_ondo_http_error,
            )
            .await
    }
}

/// Reads a cancel answer that carried a payload: the order the venue sent after the cancel.
///
/// A body that is not an `ApiOrder` is kept verbatim in [`OndoCancelAnswer::Unconfirmed`] rather
/// than guessed at: the cancel API having been called is not a terminal state (plan §6.3), and a
/// body this adapter cannot read is not evidence that an order is gone.
fn read_cancel_answer(result: &str) -> OndoCancelAnswer {
    match OndoApiOrder::from_text(result) {
        Ok(order) => OndoCancelAnswer::Order(Box::new(order)),
        Err(_unreadable) => OndoCancelAnswer::Unconfirmed {
            raw: result.to_string(),
        },
    }
}

/// Reads the one cancel answer the spec documents without a `result`: `DELETE /v1/perps/orders`.
///
/// Its 200 is a bare `GenericResponse` - `required: ["success"]` - so "success, no payload" is the
/// documented success shape here and not a schema failure. It answers
/// [`OndoCancelAnswer::Unconfirmed`] with the envelope's own text, which is the same answer a
/// success carrying an unreadable payload gets: both say the cancel API was called and nothing
/// more, which is all either of them says (plan §6.3).
fn read_cancel_all(status: u16, body: &[u8]) -> OndoHttpResult<OndoCancelAnswer> {
    match OndoPrivateResponse::decode_optional_result(status, body)? {
        Some(response) => Ok(read_cancel_answer(response.raw_result())),
        // `decode_optional_result` already read this body as UTF-8 JSON, so the lossy conversion
        // cannot lose anything; it is used because it is total, and the text is kept as it arrived.
        None => Ok(OndoCancelAnswer::Unconfirmed {
            raw: String::from_utf8_lossy(body).into_owned(),
        }),
    }
}

/// What one of the three cancel endpoints answered.
///
/// Neither variant is a cancellation. The venue's post-cancel order is what confirms a terminal
/// state, and an answer that carried no readable order confirms nothing (plan §6.3: the cancel API
/// being called is not the same fact as the order being cancelled).
#[derive(Clone, Debug)]
pub enum OndoCancelAnswer {
    /// The answer carried the order's own payload: the venue's own post-cancel state for it.
    Order(Box<OndoApiOrder>),
    /// The venue answered success without an order payload.
    ///
    /// The raw body is kept, because a sandbox session is what has to read it first.
    Unconfirmed {
        /// The `result` payload, exactly as the venue sent it.
        raw: String,
    },
}

/// Why a new-risk write did not go out, and what happened when it did.
///
/// The three cases are deliberately distinct. The first two never became a request, so they say
/// nothing about the venue: a [`Self::Refused`] write was admitted when it was accepted and the
/// account stopped admitting it before the request existed, and a [`Self::Local`] one was refused
/// by this adapter for what it is. Only [`Self::Http`] leaves the question open, and it is the one
/// the caller owes a probe to.
#[derive(Debug, thiserror::Error)]
pub enum OndoNewRiskSendError {
    /// The guard refused the write at the send point, after the budget wait and before the request
    /// was built.
    ///
    /// Nothing was sent, so this is a decision rather than an unknown outcome: the caller reports
    /// the refusal and registers no uncertainty. `reason` is the account's own, and is what a
    /// strategy is told.
    #[error("the new-risk write was refused before a request existed: {reason}")]
    Refused {
        /// Why the account refuses new risk, as the guard reported it.
        reason: String,
    },
    /// The write was refused by this adapter, before any request existed.
    #[error(transparent)]
    Local(#[from] OndoBatchError),
    /// The request was made and its answer could not be used.
    #[error(transparent)]
    Http(#[from] OndoHttpError),
}

/// The `GET /v1/perps/contracts` envelope: a `GenericResponse` whose `result` is kept as raw text.
#[derive(Deserialize)]
struct ContractsEnvelope<'a> {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default, borrow)]
    result: Option<&'a RawValue>,
}

/// Decodes a response body as UTF-8, naming the endpoint that carried it.
fn text_body(path: &str, body: &[u8]) -> OndoHttpResult<String> {
    String::from_utf8(body.to_vec())
        .map_err(|error| OndoHttpError::Decode(format!("{path} response is not UTF-8: {error}")))
}

/// Decodes a `GET /status` body.
fn decode_status(body: &str) -> OndoHttpResult<serde_json::Value> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|error| OndoHttpError::Decode(error.to_string()))?;

    if value.get("success").and_then(serde_json::Value::as_bool) == Some(false) {
        return Err(OndoHttpError::Unsuccessful);
    }

    Ok(value.get("result").cloned().unwrap_or(value))
}

/// Decodes a `GET /v1/perps/contracts` body into the exact JSON text of each contract.
fn decode_contracts(body: &str) -> OndoHttpResult<Vec<Box<RawValue>>> {
    let envelope: ContractsEnvelope<'_> =
        serde_json::from_str(body).map_err(|error| OndoHttpError::Decode(error.to_string()))?;

    if envelope.success == Some(false) {
        return Err(OndoHttpError::Unsuccessful);
    }

    let Some(result) = envelope.result else {
        return Err(OndoHttpError::MissingField {
            context: format!("{CONTRACTS_PATH} response"),
            field: "result",
        });
    };

    let contracts: Vec<Box<RawValue>> = serde_json::from_str(result.get())
        .map_err(|error| OndoHttpError::Decode(error.to_string()))?;

    if contracts.is_empty() {
        return Err(OndoHttpError::EmptyResult);
    }

    Ok(contracts)
}

/// Returns a successful response's status, retained headers and body, or the classified failure.
fn check_response(response: HttpResponse) -> OndoHttpResult<OndoRawResponse> {
    if response.status.is_success() {
        return Ok(OndoRawResponse {
            status: response.status.as_u16(),
            headers: response.headers,
            body: response.body.to_vec(),
        });
    }

    let status = response.status.as_u16();
    let body = String::from_utf8_lossy(&response.body).to_string();

    Err(classify_status(status, &response.headers, body))
}

/// Signs one authenticated request and returns the headers that carry the signature.
///
/// The message is `timestamp + UPPERCASE_METHOD + target.as_str() + body`
/// ([`crate::signing::sign_rest`]), so what the signature covers is the target the caller will also
/// put on the wire. A clock the venue's own `Date` header has shown to be beyond the ±30 s
/// tolerance refuses here, before a socket is touched.
///
/// # Errors
///
/// Returns [`OndoHttpError::Signing`] when the clock refuses to sign or the system clock predates
/// the Unix epoch.
fn signed_request_headers(
    auth: &OndoAuth,
    method: &str,
    target: &OndoRequestTarget,
    body: &[u8],
) -> OndoHttpResult<HashMap<String, String>> {
    auth.check_clock()?;

    let timestamp_ms = now_millis()?;
    let signature = sign_rest(
        &auth.credential,
        timestamp_ms,
        method,
        target.as_str(),
        body,
    );

    Ok(signed_headers(&auth.credential, timestamp_ms, &signature))
}

/// Classifies a non-success answer to an authenticated request.
///
/// A 401 or a 403 becomes the venue's *named* auth failure, with the credential redacted out of the
/// body before it is kept: the venue's own messages echo the key id they were sent. Every other
/// status keeps the public path's classification, so a 429 stays a rate-limit refusal and a 5xx
/// stays retryable.
fn classify_private_status(
    status: u16,
    headers: &HashMap<String, String>,
    body: String,
) -> OndoHttpError {
    match status {
        401 | 403 => classify_auth_rejection(status, body),
        other => classify_status(other, headers, body),
    }
}

/// Decodes a successful answer to an authenticated request, or classifies the failure.
fn check_private_response(
    response: HttpResponse,
    auth: &OndoAuth,
) -> OndoHttpResult<OndoPrivateResponse> {
    check_private_response_with(response, auth, OndoPrivateResponse::decode)
}

/// Classifies an answer to an authenticated request, and reads a successful body with `decode`.
///
/// How a *failure* is classified is the same question whoever asked - a 401 is a 401, a 429 carries
/// the same `Retry-After` - so it is answered once, here. Only the success body's reader belongs to
/// the caller, because a success may or may not carry a `result`: 17 of the spec's 72 operations
/// answer 200 with something else, 10 of them a bare `GenericResponse`. Among the endpoints this
/// adapter writes only `DELETE /v1/perps/orders` does, and [`read_cancel_all`] is its reader.
fn check_private_response_with<T>(
    response: HttpResponse,
    auth: &OndoAuth,
    decode: fn(u16, &[u8]) -> OndoHttpResult<T>,
) -> OndoHttpResult<T> {
    let status = response.status.as_u16();

    if response.status.is_success() {
        return decode(status, &response.body);
    }

    let body = auth
        .credential
        .redact(&String::from_utf8_lossy(&response.body));

    Err(classify_private_status(status, &response.headers, body))
}

/// Classifies a non-success HTTP status into the error a caller acts on.
///
/// Status families are what decide retryability: 429 is a rate-limit refusal that carries the
/// venue's requested wait, every other 4xx is a terminal rejection that additionally carries the
/// body's error code when the body has one, and 5xx is a definitive server-side failure that
/// [`should_retry_ondo_http_error`] treats as transient.
fn classify_status(status: u16, headers: &HashMap<String, String>, body: String) -> OndoHttpError {
    if status == 429 {
        return OndoHttpError::RateLimited {
            retry_after_secs: retry_after_secs(headers),
            body,
        };
    }

    if (400..500).contains(&status) {
        return OndoHttpError::RequestRejected {
            status,
            code: extract_error_code(&body),
            message: body,
        };
    }

    OndoHttpError::Http { status, body }
}

/// Reads the `Retry-After` header as a whole number of seconds.
///
/// Only the delay-seconds form is honoured. The header's HTTP-date form would require trusting the
/// venue's clock, which this adapter cannot verify, so it is left to the exponential backoff.
fn retry_after_secs(headers: &HashMap<String, String>) -> Option<u64> {
    headers.get(RETRY_AFTER_HEADER)?.trim().parse::<u64>().ok()
}

/// The minimum wait an error asks for before the retry, when it asks for one.
fn retry_delay(error: &OndoHttpError) -> Option<Duration> {
    match error {
        OndoHttpError::RateLimited {
            retry_after_secs: Some(secs),
            ..
        } => Some(Duration::from_secs(*secs)),
        _ => None,
    }
}

/// Classifies a failure of the HTTP transport itself.
fn transport_error(error: HttpClientError) -> OndoHttpError {
    match error {
        HttpClientError::TimeoutError(message) => OndoHttpError::Timeout(message),
        other => OndoHttpError::Network(other.to_string()),
    }
}

/// Maps a synthesized retry failure into the HTTP taxonomy.
fn retry_error_to_ondo_http_error(error: RetryError) -> OndoHttpError {
    match error {
        RetryError::OperationTimeout { timeout_ms } => {
            OndoHttpError::Timeout(format!("no answer within {timeout_ms}ms"))
        }
        other => OndoHttpError::Network(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_retry_after_accepts_only_whole_seconds() {
        let header =
            |value: &str| HashMap::from([(RETRY_AFTER_HEADER.to_string(), value.to_string())]);

        assert_eq!(retry_after_secs(&header("1")), Some(1));
        assert_eq!(retry_after_secs(&header(" 30 ")), Some(30));
        assert_eq!(
            retry_after_secs(&header("Wed, 21 Oct 2026 07:28:00 GMT")),
            None
        );
        assert_eq!(retry_after_secs(&header("soon")), None);
        assert_eq!(retry_after_secs(&HashMap::new()), None);
    }

    #[rstest]
    fn test_status_families_are_classified_for_retryability() {
        let none = HashMap::new();

        let rate_limited = classify_status(429, &none, "slow down".to_string());
        assert!(matches!(
            rate_limited,
            OndoHttpError::RateLimited {
                retry_after_secs: None,
                ..
            }
        ));
        assert!(should_retry_ondo_http_error(&rate_limited));

        let forbidden = classify_status(403, &none, "error code: 1010".to_string());
        assert!(matches!(
            &forbidden,
            OndoHttpError::RequestRejected {
                status: 403,
                code: Some(code),
                ..
            } if code == "1010"
        ));
        assert!(!should_retry_ondo_http_error(&forbidden));

        let unauthorized = classify_status(401, &none, r#"{"code":"UNAUTHORIZED"}"#.to_string());
        assert!(matches!(
            unauthorized,
            OndoHttpError::RequestRejected { status: 401, .. }
        ));
        assert!(!should_retry_ondo_http_error(&unauthorized));

        let missing = classify_status(404, &none, "unknown market".to_string());
        assert!(matches!(
            missing,
            OndoHttpError::RequestRejected { status: 404, .. }
        ));
        assert!(!should_retry_ondo_http_error(&missing));

        let server_error = classify_status(503, &none, "busy".to_string());
        assert!(matches!(
            server_error,
            OndoHttpError::Http { status: 503, .. }
        ));
        assert!(should_retry_ondo_http_error(&server_error));
    }

    #[rstest]
    fn test_a_zero_second_retry_after_is_the_shortest_honoured_wait() {
        let error = OndoHttpError::RateLimited {
            retry_after_secs: Some(0),
            body: "slow down".to_string(),
        };

        assert_eq!(retry_delay(&error), Some(Duration::from_secs(0)));
        assert_eq!(
            retry_delay(&OndoHttpError::Network("lost".to_string())),
            None
        );
    }

    #[rstest]
    fn test_the_client_trims_a_trailing_slash_from_its_base_url() {
        let client = OndoHttpClient::builder()
            .base_url("https://api.ondoperps.xyz/".to_string())
            .build()
            .expect("the client builds");

        assert_eq!(client.base_url(), "https://api.ondoperps.xyz");
        assert_eq!(
            client.url(&OndoRequestTarget::new(MARKETS_PATH)),
            "https://api.ondoperps.xyz/v1/markets",
        );
    }

    #[rstest]
    fn test_the_default_client_timeout_is_the_documented_one() {
        assert_eq!(ONDO_HTTP_TIMEOUT_SECS, 15);
    }

    #[rstest]
    fn test_decode_status_unwraps_an_envelope_and_passes_anything_else_through() {
        let unwrapped = decode_status(r#"{"success":true,"result":{"status":"ok"}}"#).unwrap();
        assert_eq!(unwrapped["status"], "ok");

        let bare = decode_status(r#"{"status":"ok"}"#).unwrap();
        assert_eq!(bare["status"], "ok");

        assert!(matches!(
            decode_status(r#"{"success":false,"result":null}"#),
            Err(OndoHttpError::Unsuccessful)
        ));
        assert!(matches!(
            decode_status("not json"),
            Err(OndoHttpError::Decode(_))
        ));
    }

    #[rstest]
    fn test_decode_contracts_keeps_each_contract_as_its_exact_json_text() {
        let body = r#"{"success":true,"result":[{"market":"NVDA-USD.P","makerFee":"0.0001"},{"market":"TSLA-USD.P","takerFee":"0.00025"}]}"#;

        let contracts = decode_contracts(body).unwrap();

        assert_eq!(contracts.len(), 2);
        assert_eq!(
            contracts[0].get(),
            r#"{"market":"NVDA-USD.P","makerFee":"0.0001"}"#,
        );
        assert_eq!(
            contracts[1].get(),
            r#"{"market":"TSLA-USD.P","takerFee":"0.00025"}"#,
        );
    }

    #[rstest]
    fn test_decode_contracts_fails_closed() {
        assert!(matches!(
            decode_contracts(r#"{"success":true,"result":[]}"#),
            Err(OndoHttpError::EmptyResult)
        ));
        assert!(matches!(
            decode_contracts(r#"{"success":true}"#),
            Err(OndoHttpError::MissingField {
                field: "result",
                ..
            })
        ));
        assert!(matches!(
            decode_contracts(r#"{"success":false,"result":[]}"#),
            Err(OndoHttpError::Unsuccessful)
        ));
        assert!(matches!(
            decode_contracts("not json"),
            Err(OndoHttpError::Decode(_))
        ));
    }

    #[rstest]
    fn test_a_transport_timeout_is_classified_as_a_retryable_timeout() {
        let timeout = transport_error(HttpClientError::TimeoutError("no answer".to_string()));

        assert!(matches!(timeout, OndoHttpError::Timeout(_)));
        assert!(should_retry_ondo_http_error(&timeout));

        let network = transport_error(HttpClientError::Error("connection reset".to_string()));
        assert!(matches!(network, OndoHttpError::Network(_)));
        assert!(should_retry_ondo_http_error(&network));
    }
}
