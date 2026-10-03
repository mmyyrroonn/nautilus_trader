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

//! Single-attempt mutation owner, durable intent/binding records, and sticky shutdown evidence.

use std::{
    collections::BTreeMap,
    fmt, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_model::identifiers::{ClientOrderId, InstrumentId, VenueOrderId};
use nautilus_network::{
    dst::time::Instant,
    http::{
        HttpClient, HttpClientError, HttpRedirectPolicy, HttpResponse, Method, PreparedHttpRequest,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{
    BackpackExecutionError, BackpackExecutionErrorKind, BackpackMutationReceipt,
    BackpackMutationStatus,
    command::BackpackOrderSpec,
    guard::{
        AdmissionFence, BackpackExecutionAuthority, BackpackExecutionGuard,
        BackpackLoopbackAccountFacts, BackpackLoopbackMarketFacts,
    },
};
use crate::{
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    config::BackpackConfig,
    http::{
        client::BackpackClock,
        error::{BackpackHttpError, BackpackHttpErrorKind, BackpackRequestOutcome},
        quota::BackpackQuota,
    },
    identity::{BackpackClientIdNamespace, BackpackClientIdStore, BackpackSubmissionIntent},
    signing::{BackpackParameters, BackpackReceiveWindow, BackpackScalar, canonical_rest},
};

const CHECKPOINT_NAME: &str = "execution.json";
const MAX_CHECKPOINT_BYTES: u64 = 16_777_216;
const MAX_RESPONSE_BYTES: usize = 1_048_576;

/// Restricted write policy. Budgets include quota, disk admission and the sole transport attempt.
#[derive(Clone, Copy, Debug)]
pub struct BackpackMutationPolicy {
    pub window: BackpackReceiveWindow,
    pub budget: Duration,
}

/// A closed loopback-only order owner. Clones of its guard never expose raw dispatch.
pub struct BackpackOrderOwner {
    guard: BackpackExecutionGuard,
    endpoints: BackpackEndpoints,
    credential: BackpackCredential,
    transport: HttpClient,
    clock: Arc<dyn BackpackClock>,
    policy: BackpackMutationPolicy,
    quota: BackpackQuota,
}
impl fmt::Debug for BackpackOrderOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackOrderOwner").finish_non_exhaustive()
    }
}

/// Construction inputs keep identities, endpoint audience and finite authority explicit.
pub struct BackpackOrderOwnerConfig {
    pub config: BackpackConfig,
    pub endpoints: BackpackEndpoints,
    pub credential: BackpackCredential,
    pub quota: BackpackQuota,
    pub clock: Arc<dyn BackpackClock>,
    pub policy: BackpackMutationPolicy,
    pub identities: BackpackClientIdStore,
    pub namespace: BackpackClientIdNamespace,
    pub authority: BackpackExecutionAuthority,
}
impl fmt::Debug for BackpackOrderOwnerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackOrderOwnerConfig")
            .finish_non_exhaustive()
    }
}

impl BackpackOrderOwner {
    /// Opens a guarded local peer owner, restoring capacity and original bindings conservatively.
    ///
    /// # Errors
    /// Returns an explicit unsupported error for production, audience/namespace mismatch,
    /// invalid finite limits, changed/corrupt durable state, or transport configuration failure.
    pub fn new_checked(input: BackpackOrderOwnerConfig) -> Result<Self, BackpackExecutionError> {
        Self::open_with_checkpoint(input, Arc::new(FileCheckpoint))
    }
    fn open_with_checkpoint(
        input: BackpackOrderOwnerConfig,
        checkpoint_store: Arc<dyn ExecutionCheckpoint>,
    ) -> Result<Self, BackpackExecutionError> {
        if !input.endpoints.is_loopback() {
            return Err(BackpackExecutionErrorKind::UnsupportedProduction.into());
        }
        if input.config.endpoints() != &input.endpoints
            || !input.namespace.matches_loopback_peer(&input.endpoints)
        {
            return Err(BackpackExecutionErrorKind::Identity.into());
        }
        input.credential.check_audience(&input.endpoints)?;
        input.authority.validate()?;
        if !input.identities.namespace_matches(&input.namespace) {
            return Err(BackpackExecutionErrorKind::Identity.into());
        }
        if input.policy.budget.is_zero() || input.policy.budget > Duration::from_secs(60) {
            return Err(BackpackExecutionErrorKind::Validation.into());
        }
        let transport = HttpClient::builder()
            .rate_limiters(vec![input.quota.clone().into_limiter()])
            .redirect_policy(HttpRedirectPolicy::Reject)
            .use_system_proxy(false)
            .build()
            .map_err(BackpackHttpError::from)?;
        let checkpoint = input.identities.execution_directory().join(CHECKPOINT_NAME);
        let mut state = OwnerState {
            identities: input.identities,
            namespace: input.namespace,
            config: input.config,
            authority: input.authority,
            generation: 0,
            session_active: false,
            stopped: false,
            poisoned: false,
            dirty_sticky: false,
            account: None,
            markets: BTreeMap::new(),
            public: None,
            records: BTreeMap::new(),
            checkpoint,
            checkpoint_bytes: None,
            checkpoint_store,
        };
        state.restore()?;
        state.persist()?;
        Ok(Self {
            guard: BackpackExecutionGuard {
                state: Arc::new(Mutex::new(state)),
                fence: Arc::new(AdmissionFence::default()),
            },
            endpoints: input.endpoints,
            credential: input.credential,
            transport,
            clock: input.clock,
            policy: input.policy,
            quota: input.quota,
        })
    }
    /// Returns the admission control handle for the native lifecycle owner.
    #[must_use]
    pub fn guard(&self) -> BackpackExecutionGuard {
        self.guard.clone()
    }

    // Internal report conversion reads one immutable view while retaining the identity lock.
    // The closure is adapter-owned parsing, never a caller admission/transport callback.
    pub(crate) fn with_report_identity<T>(
        &self,
        parse: impl FnOnce(
            &BackpackClientIdStore,
            &crate::account::reports::BackpackOrderBindings,
        ) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let state = self.guard.lock()?;
        let mut bindings = crate::account::reports::BackpackOrderBindings::default();
        for (id, record) in &state.records {
            if let Some(venue) = &record.venue_order_id {
                bindings.confirm_acknowledged(
                    *id,
                    VenueOrderId::new_checked(venue)?,
                    record.spec.instrument_id,
                    &state.identities,
                )?;
            }
        }
        parse(&state.identities, &bindings)
    }

    /// Commits one immutable create intent before quota and dispatches at most once.
    ///
    /// # Errors
    /// Returns a local refusal without bytes for invalid admission/intent, or a classified
    /// HTTP failure. Uncertain transmission retains original identity and capacity forever
    /// until independent economic reconciliation; repeated calls never send another creation.
    pub async fn submit(
        &self,
        spec: BackpackOrderSpec,
        cancel: &CancellationToken,
    ) -> Result<BackpackMutationReceipt, BackpackExecutionError> {
        self.submit_with_deadline(spec, cancel, self.begin_deadline()?)
            .await
    }

    pub(crate) fn begin_deadline(&self) -> Result<Instant, BackpackExecutionError> {
        Instant::now()
            .checked_add(self.policy.budget)
            .ok_or_else(|| BackpackExecutionErrorKind::Validation.into())
    }

    pub(crate) async fn submit_with_deadline(
        &self,
        spec: BackpackOrderSpec,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<BackpackMutationReceipt, BackpackExecutionError> {
        if deadline <= Instant::now() {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission).into());
        }
        let now = self.clock.timestamp_ms()?;
        let id = spec.client_order_id;
        let generation;
        let revision;
        {
            let mut state = self.guard.lock()?;
            revision = self.guard.fence.snapshot()?;
            if state.identities.venue_id(&id).is_some() {
                return Err(BackpackExecutionErrorKind::Duplicate.into());
            }
            if cancel.is_cancelled() {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Cancelled).into());
            }
            let (notional, margin) = state.reservation(&spec, now, None)?;
            let symbol = state
                .markets
                .get(&spec.instrument_id)
                .ok_or(BackpackExecutionErrorKind::Readiness)?
                .metadata
                .raw_symbol
                .to_string();
            // Force wire scalar validation before a durable identity can be burned.
            spec.parameters(&symbol, None)?;
            let intent = StoredIntent {
                spec,
                symbol,
                notional,
                margin,
            };
            let encoded = serde_json::to_string(&intent)
                .map_err(|_| BackpackExecutionErrorKind::Validation)?;
            let durable = BackpackSubmissionIntent::new_checked(id, encoded)
                .map_err(|_| BackpackExecutionErrorKind::Identity)?;
            let client_id = match state.identities.reserve_intent(durable) {
                Ok(value) => value,
                Err(_) => {
                    state.poisoned = true;
                    state.dirty_sticky = true;
                    return Err(BackpackExecutionErrorKind::Identity.into());
                }
            };
            generation = state.generation;
            state.records.insert(
                id,
                OrderRecord {
                    spec: intent.spec,
                    symbol: intent.symbol,
                    notional,
                    margin,
                    client_id,
                    status: CreateStatus::Unsent,
                    venue_order_id: None,
                    cancel: CancelStatus::NeverAttempted,
                    observed_cumulative: Decimal::ZERO,
                    economic_ack_reference: None,
                },
            );
            state.persist()?;
            if cancel.is_cancelled()
                || deadline <= Instant::now()
                || !self.guard.fence.matches(revision)
            {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission).into());
            }
        }
        self.dispatch(id, generation, false, cancel, deadline).await
    }

    /// Cancels one independently bound owned venue order with exactly one DELETE attempt.
    ///
    /// # Errors
    /// Returns a local refusal for external/unbound identity, prior cancellation, or absent
    /// current cancellation authority. DELETE 202 stays pending; no order event is generated.
    pub async fn cancel_owned(
        &self,
        id: ClientOrderId,
        cancel: &CancellationToken,
    ) -> Result<BackpackMutationReceipt, BackpackExecutionError> {
        let deadline = Instant::now()
            .checked_add(self.policy.budget)
            .ok_or(BackpackExecutionErrorKind::Validation)?;
        let now = self.clock.timestamp_ms()?;
        let generation;
        let revision;
        {
            let mut state = self.guard.lock()?;
            revision = self.guard.fence.snapshot()?;
            state.check_cancel(id, now)?;
            if cancel.is_cancelled() {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Cancelled).into());
            }
            let record = state
                .records
                .get_mut(&id)
                .ok_or(BackpackExecutionErrorKind::Ownership)?;
            if record.cancel != CancelStatus::NeverAttempted {
                return Err(BackpackExecutionErrorKind::Duplicate.into());
            }
            record.cancel = CancelStatus::Unsent;
            generation = state.generation;
            state.persist()?;
            if cancel.is_cancelled()
                || deadline <= Instant::now()
                || !self.guard.fence.matches(revision)
            {
                return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission).into());
            }
        }
        self.dispatch(id, generation, true, cancel, deadline).await
    }

    pub(crate) fn observe_owned_order(
        &self,
        id: ClientOrderId,
        raw: &crate::account::models::BackpackOrder,
    ) -> Result<(), BackpackExecutionError> {
        let mut state = self.guard.lock()?;
        state.account = None;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        let expected = record
            .spec
            .parameters(&record.symbol, Some(record.client_id))?
            .json_body();
        let cumulative = raw.executed_quantity.as_ref().map(|value| value.0);
        let matched = record.venue_order_id.as_deref() == Some(raw.id.as_str())
            && raw.symbol == record.symbol
            && raw.client_id.is_none_or(|value| value == record.client_id)
            && raw.side == expected["side"]
            && raw.order_type == expected["orderType"]
            && raw.time_in_force == expected["timeInForce"]
            && raw.quantity.as_ref().map(|value| value.0) == Some(record.spec.quantity)
            && raw.price.as_ref().map(|value| value.0) == record.spec.price
            && raw.post_only == Some(record.spec.post_only)
            && raw.reduce_only == Some(record.spec.reduce_only)
            && cumulative
                .is_some_and(|value| value >= Decimal::ZERO && value <= record.spec.quantity);
        if !matched {
            record.status = CreateStatus::Unknown;
            state.dirty_sticky = true;
            state.persist()?;
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        let cumulative = cumulative.ok_or(BackpackExecutionErrorKind::Validation)?;
        if cumulative > record.observed_cumulative {
            let reopened = record.status == CreateStatus::Reconciled;
            if reopened {
                record.status = CreateStatus::Unknown;
            }
            record.observed_cumulative = cumulative;
            if reopened {
                state.dirty_sticky = true;
            }
            state.persist()?;
        }
        Ok(())
    }

    /// Returns only a durable binding established by the original matched POST response.
    #[must_use]
    pub fn confirmed_binding(&self, id: ClientOrderId) -> Option<(VenueOrderId, InstrumentId)> {
        let state = self.guard.state.lock().ok()?;
        if state.poisoned {
            return None;
        }
        let record = state.records.get(&id)?;
        Some((
            VenueOrderId::new_checked(record.venue_order_id.as_ref()?).ok()?,
            record.spec.instrument_id,
        ))
    }

    pub(crate) fn restore_native_binding(
        &self,
        spec: &BackpackOrderSpec,
        venue: VenueOrderId,
    ) -> Result<(), BackpackExecutionError> {
        let state = self.guard.lock()?;
        let original = state
            .records
            .get(&spec.client_order_id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        if original.venue_order_id.as_deref() != Some(venue.as_str())
            || serde_json::to_vec(&original.spec)
                .map_err(|_| BackpackExecutionErrorKind::Storage)?
                != serde_json::to_vec(spec).map_err(|_| BackpackExecutionErrorKind::Storage)?
        {
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        Ok(())
    }

    /// Acknowledges independent terminal and already-applied true fill evidence from the local peer.
    ///
    /// # Errors
    /// Returns an error unless original binding/generation agree, fill quantity exactly equals
    /// cumulative quantity within the original order size, and durable economic ACK is identified.
    /// This is a local peer attestation boundary, never a production readiness certificate.
    pub fn acknowledge_reconciled_terminal(
        &self,
        evidence: &BackpackLoopbackTerminalEvidence,
    ) -> Result<(), BackpackExecutionError> {
        let mut state = self.guard.lock()?;
        if state.poisoned
            || evidence.generation != state.generation
            || evidence.economic_ack_reference.trim().is_empty()
            || evidence.cumulative_quantity < Decimal::ZERO
            || evidence.cumulative_quantity != evidence.applied_fill_quantity
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        let record = state
            .records
            .get_mut(&evidence.client_order_id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        if record.spec.instrument_id != evidence.instrument_id
            || record.venue_order_id.as_deref() != Some(evidence.venue_order_id.as_str())
            || evidence.cumulative_quantity > record.spec.quantity
            || evidence.cumulative_quantity < record.observed_cumulative
        {
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        record.economic_ack_reference = Some(evidence.economic_ack_reference.clone());
        record.observed_cumulative = evidence.cumulative_quantity;
        record.status = CreateStatus::Reconciled;
        record.cancel = CancelStatus::Reconciled;
        // Reconciliation changes account economics; fresh accepted account facts are required.
        state.account = None;
        state.persist()
    }

    /// Stops new dispatch and durably records unresolved work. Repetition cannot clean a dirty stop.
    ///
    /// # Errors
    /// Returns an error on checkpoint failure; the in-memory owner remains stopped and dirty.
    pub fn stop(&self) -> Result<BackpackShutdownReport, BackpackExecutionError> {
        let _change = self.guard.fence.change();
        let now = self.clock.timestamp_ms().ok();
        let mut state = self.guard.lock()?;
        state.stopped = true;
        state.session_active = false;
        let report = state.shutdown_report(now);
        state.dirty_sticky |= report.dirty;
        state.persist()?;
        Ok(state.shutdown_report(now))
    }

    async fn dispatch(
        &self,
        id: ClientOrderId,
        generation: u64,
        deleting: bool,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<BackpackMutationReceipt, BackpackExecutionError> {
        let transmitted = std::sync::atomic::AtomicBool::new(false);
        let mut quota_wait = self.quota.start_wait();
        let operation = self.transport.request_with_url_redacted_prepared_request(
            if deleting {
                Method::DELETE
            } else {
                Method::POST
            },
            Some(BackpackQuota::keys(false)),
            Some(deadline),
            || {
                quota_wait.admitted();
                let timestamp = self.clock.timestamp_ms()?;
                self.credential.check_audience(&self.endpoints)?;
                let mut state = self
                    .guard
                    .lock()
                    .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                let revision = self
                    .guard
                    .fence
                    .snapshot()
                    .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                if cancel.is_cancelled()
                    || state.generation != generation
                    || !self.guard.fence.matches(revision)
                {
                    return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                }
                if deleting {
                    state
                        .check_cancel(id, timestamp)
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                } else {
                    let record = state.records.get(&id).ok_or_else(|| {
                        BackpackHttpError::local(BackpackHttpErrorKind::Admission)
                    })?;
                    let current = state
                        .reservation(&record.spec, timestamp, Some(record))
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                    if current != (record.notional, record.margin) {
                        return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                    }
                }
                let record = state
                    .records
                    .get_mut(&id)
                    .ok_or_else(|| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                let parameters = if deleting {
                    let mut values = BackpackParameters::default();
                    values.insert("symbol", Some(BackpackScalar::Token(record.symbol.clone())))?;
                    values.insert(
                        "orderId",
                        Some(BackpackScalar::Token(
                            record.venue_order_id.clone().ok_or_else(|| {
                                BackpackHttpError::local(BackpackHttpErrorKind::Admission)
                            })?,
                        )),
                    )?;
                    record.cancel = CancelStatus::Unknown;
                    values
                } else {
                    let values = record
                        .spec
                        .parameters(&record.symbol, Some(record.client_id))
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
                    record.status = CreateStatus::Unknown;
                    values
                };
                // Persist the attempt before the shared transport can emit its first byte.
                state
                    .persist()
                    .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
                // Disk synchronization may be slow: refresh clock and admission again after it.
                let timestamp = self.clock.timestamp_ms()?;
                if cancel.is_cancelled()
                    || state.generation != generation
                    || !self.guard.fence.matches(revision)
                {
                    return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                }
                if deleting {
                    state
                        .check_cancel(id, timestamp)
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?;
                } else {
                    let record = state.records.get(&id).ok_or_else(|| {
                        BackpackHttpError::local(BackpackHttpErrorKind::Admission)
                    })?;
                    if state
                        .reservation(&record.spec, timestamp, Some(record))
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Admission))?
                        != (record.notional, record.margin)
                    {
                        return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                    }
                }
                if deadline <= Instant::now() || !self.guard.fence.matches(revision) {
                    return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                }
                let canonical = canonical_rest(
                    if deleting {
                        "orderCancel"
                    } else {
                        "orderExecute"
                    },
                    &parameters,
                    timestamp,
                    self.policy.window,
                );
                let mut headers =
                    self.credential
                        .headers(&canonical, timestamp, self.policy.window)?;
                headers.insert("Content-Type".into(), "application/json".into());
                let body = serde_json::to_vec(&parameters.json_body())
                    .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
                if cancel.is_cancelled()
                    || deadline <= Instant::now()
                    || !self.guard.fence.matches(revision)
                {
                    return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                }
                transmitted.store(true, std::sync::atomic::Ordering::Release);
                Ok(PreparedHttpRequest {
                    url: format!(
                        "{}/api/v1/order",
                        self.endpoints.rest_url().trim_end_matches('/')
                    ),
                    headers: Some(headers),
                    body: Some(body),
                })
            },
        );
        let result: Result<HttpResponse, BackpackHttpError> = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(BackpackHttpError::local(BackpackHttpErrorKind::Cancelled)),
            result = operation => result,
        };
        drop(quota_wait);
        // AdmissionDenied proves no transport entry, even if synchronous preparation completed.
        let result = result.map_err(|error| {
            if error.outcome() == BackpackRequestOutcome::NotSent
                && error.kind() == BackpackHttpErrorKind::Admission
            {
                error
            } else {
                error.preserve_transmission(transmitted.load(std::sync::atomic::Ordering::Acquire))
            }
        });
        self.finish(id, generation, deleting, result)
    }

    fn finish(
        &self,
        id: ClientOrderId,
        generation: u64,
        deleting: bool,
        response: Result<HttpResponse, BackpackHttpError>,
    ) -> Result<BackpackMutationReceipt, BackpackExecutionError> {
        let mut state = self.guard.lock()?;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        let already_reconciled = record.status == CreateStatus::Reconciled;
        let mut result = response.and_then(|response| decode_response(record, deleting, &response));
        let observed_binding = state
            .records
            .get(&id)
            .and_then(|record| record.venue_order_id.clone());
        if !deleting
            && observed_binding.as_ref().is_some_and(|venue_id| {
                state.records.iter().any(|(other_id, record)| {
                    *other_id != id && record.venue_order_id.as_ref() == Some(venue_id)
                })
            })
        {
            result = Err(BackpackHttpError::unknown(BackpackHttpErrorKind::Decode));
            state
                .records
                .get_mut(&id)
                .ok_or(BackpackExecutionErrorKind::Ownership)?
                .venue_order_id = None;
        }
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        match &result {
            _ if deleting && already_reconciled => {}
            Ok(receipt) if deleting => {
                record.cancel = if receipt.status == BackpackMutationStatus::CancelPending {
                    CancelStatus::Pending
                } else {
                    CancelStatus::ResponseObserved
                }
            }
            Ok(_) => record.status = CreateStatus::ResponseObserved,
            Err(error) if deleting => {
                record.cancel = match error.outcome() {
                    BackpackRequestOutcome::NotSent => CancelStatus::Unsent,
                    BackpackRequestOutcome::VenueRejected => CancelStatus::Rejected,
                    BackpackRequestOutcome::Unknown => CancelStatus::Unknown,
                }
            }
            Err(error) => {
                record.status = match error.outcome() {
                    BackpackRequestOutcome::NotSent => CreateStatus::Unsent,
                    BackpackRequestOutcome::VenueRejected => CreateStatus::Rejected,
                    BackpackRequestOutcome::Unknown => CreateStatus::Unknown,
                }
            }
        }
        if result
            .as_ref()
            .is_err_and(|error| error.outcome() == BackpackRequestOutcome::Unknown)
            || generation != state.generation
        {
            state.account = None;
        }
        if state.persist().is_err() {
            return Err(BackpackHttpError::unknown(BackpackHttpErrorKind::Validation).into());
        }
        result.map_err(BackpackExecutionError::from)
    }
}

/// Independent terminal/economic acknowledgement from the explicit local peer integration.
#[derive(Clone, Debug)]
pub struct BackpackLoopbackTerminalEvidence {
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub instrument_id: InstrumentId,
    pub generation: u64,
    pub cumulative_quantity: Decimal,
    pub applied_fill_quantity: Decimal,
    pub economic_ack_reference: String,
}

/// Durable shutdown evidence, without manufactured cancellation or flatness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackShutdownReport {
    pub dirty: bool,
    pub unsent: usize,
    pub unknown: usize,
    pub observed_unreconciled: usize,
    pub pending_cancellations: usize,
    pub positions_unknown_or_nonzero: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredIntent {
    spec: BackpackOrderSpec,
    symbol: String,
    notional: Decimal,
    margin: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OrderRecord {
    pub(crate) spec: BackpackOrderSpec,
    symbol: String,
    pub(crate) notional: Decimal,
    pub(crate) margin: Decimal,
    client_id: u32,
    status: CreateStatus,
    venue_order_id: Option<String>,
    cancel: CancelStatus,
    observed_cumulative: Decimal,
    economic_ack_reference: Option<String>,
}
impl OrderRecord {
    pub(crate) fn holds_capacity(&self) -> bool {
        !matches!(
            self.status,
            CreateStatus::Rejected | CreateStatus::Reconciled
        )
    }
    pub(crate) fn is_unknown(&self) -> bool {
        self.status == CreateStatus::Unknown
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum CreateStatus {
    Unsent,
    Unknown,
    ResponseObserved,
    Rejected,
    Reconciled,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum CancelStatus {
    NeverAttempted,
    Unsent,
    Unknown,
    Pending,
    ResponseObserved,
    Rejected,
    Reconciled,
}

#[derive(Debug)]
pub(crate) struct OwnerState {
    identities: BackpackClientIdStore,
    pub(crate) namespace: BackpackClientIdNamespace,
    pub(crate) config: BackpackConfig,
    pub(crate) authority: BackpackExecutionAuthority,
    pub(crate) generation: u64,
    pub(crate) session_active: bool,
    pub(crate) stopped: bool,
    pub(crate) poisoned: bool,
    dirty_sticky: bool,
    pub(crate) account: Option<BackpackLoopbackAccountFacts>,
    pub(crate) markets: BTreeMap<InstrumentId, BackpackLoopbackMarketFacts>,
    pub(crate) public: Option<super::guard::PublicAdmission>,
    pub(crate) records: BTreeMap<ClientOrderId, OrderRecord>,
    checkpoint: PathBuf,
    checkpoint_bytes: Option<Vec<u8>>,
    checkpoint_store: Arc<dyn ExecutionCheckpoint>,
}
impl OwnerState {
    fn check_cancel(&self, id: ClientOrderId, now: u64) -> Result<(), BackpackExecutionError> {
        self.check_session(now)?;
        let record = self
            .records
            .get(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        if !self.authority.allow_owned_cancel
            || record.venue_order_id.is_none()
            || !record.holds_capacity()
            || self.identities.venue_id(&id) != Some(record.client_id)
        {
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        Ok(())
    }
    fn shutdown_report(&self, now_ms: Option<u64>) -> BackpackShutdownReport {
        let positions_unknown_or_nonzero = self.account.as_ref().is_none_or(|account| {
            account.generation != self.generation
                || now_ms.is_none_or(|now| {
                    now.checked_sub(account.observed_at_ms)
                        .is_none_or(|age| age > self.authority.max_account_age_ms)
                })
                || account.net_positions.values().any(|q| *q != Decimal::ZERO)
        });
        let unsent = self
            .records
            .values()
            .filter(|r| r.status == CreateStatus::Unsent)
            .count();
        let unknown = self
            .records
            .values()
            .filter(|r| r.status == CreateStatus::Unknown)
            .count();
        let observed_unreconciled = self
            .records
            .values()
            .filter(|r| r.status == CreateStatus::ResponseObserved)
            .count();
        let pending_cancellations = self
            .records
            .values()
            .filter(|r| {
                matches!(
                    r.cancel,
                    CancelStatus::Unknown | CancelStatus::Pending | CancelStatus::ResponseObserved
                )
            })
            .count();
        BackpackShutdownReport {
            dirty: self.dirty_sticky
                || unsent + unknown + observed_unreconciled + pending_cancellations > 0
                || positions_unknown_or_nonzero,
            unsent,
            unknown,
            observed_unreconciled,
            pending_cancellations,
            positions_unknown_or_nonzero,
        }
    }
    fn restore(&mut self) -> Result<(), BackpackExecutionError> {
        if self.checkpoint.exists() {
            reject_symlink(&self.checkpoint)?;
            if fs::metadata(&self.checkpoint)
                .map_err(|_| BackpackExecutionErrorKind::Storage)?
                .len()
                > MAX_CHECKPOINT_BYTES
            {
                return Err(BackpackExecutionErrorKind::Storage.into());
            }
            let bytes =
                fs::read(&self.checkpoint).map_err(|_| BackpackExecutionErrorKind::Storage)?;
            let stored: Checkpoint =
                serde_json::from_slice(&bytes).map_err(|_| BackpackExecutionErrorKind::Storage)?;
            let payload = serde_json::to_vec(&stored.payload)
                .map_err(|_| BackpackExecutionErrorKind::Storage)?;
            if stored.version != 1
                || stored.checksum != blake3::hash(&payload).to_hex().as_str()
                || stored.payload.namespace != self.namespace
            {
                return Err(BackpackExecutionErrorKind::Storage.into());
            }
            self.records = stored.payload.records;
            self.generation = stored.payload.generation;
            self.dirty_sticky = stored.payload.dirty_sticky;
            self.checkpoint_bytes = Some(bytes);
        }
        for (id, client_id) in self.identities.reserved_ids() {
            let encoded = self
                .identities
                .unsigned_intent(&id)
                .ok_or(BackpackExecutionErrorKind::Storage)?;
            let intent: StoredIntent =
                serde_json::from_str(encoded).map_err(|_| BackpackExecutionErrorKind::Storage)?;
            if intent.spec.client_order_id != id
                || intent.notional < Decimal::ZERO
                || intent.margin < Decimal::ZERO
            {
                return Err(BackpackExecutionErrorKind::Storage.into());
            }
            if let Some(record) = self.records.get(&id) {
                if record.client_id != client_id
                    || serde_json::to_string(&StoredIntent {
                        spec: record.spec.clone(),
                        symbol: record.symbol.clone(),
                        notional: record.notional,
                        margin: record.margin,
                    })
                    .map_err(|_| BackpackExecutionErrorKind::Storage)?
                        != encoded
                {
                    return Err(BackpackExecutionErrorKind::Storage.into());
                }
                if let Some(venue_id) = &record.venue_order_id {
                    let mut token = BackpackParameters::default();
                    token.insert("orderId", Some(BackpackScalar::Token(venue_id.clone())))?;
                }
            } else {
                self.records.insert(
                    id,
                    OrderRecord {
                        spec: intent.spec,
                        symbol: intent.symbol,
                        notional: intent.notional,
                        margin: intent.margin,
                        client_id,
                        status: CreateStatus::Unknown,
                        venue_order_id: None,
                        cancel: CancelStatus::NeverAttempted,
                        observed_cumulative: Decimal::ZERO,
                        economic_ack_reference: None,
                    },
                );
            }
        }
        if self
            .records
            .iter()
            .any(|(id, r)| self.identities.venue_id(id) != Some(r.client_id))
        {
            return Err(BackpackExecutionErrorKind::Storage.into());
        }
        // Process termination gives no proof about in-flight or observed-but-unapplied economics.
        for record in self.records.values_mut() {
            if matches!(
                record.status,
                CreateStatus::Unsent | CreateStatus::ResponseObserved
            ) {
                record.status = CreateStatus::Unknown;
            }
            if record.cancel == CancelStatus::Unsent {
                record.cancel = CancelStatus::Unknown;
            }
        }
        Ok(())
    }
    fn persist(&mut self) -> Result<(), BackpackExecutionError> {
        let result = self.persist_checked();
        if result.is_err() {
            self.poisoned = true;
            self.dirty_sticky = true;
            self.session_active = false;
        }
        result
    }
    fn persist_checked(&mut self) -> Result<(), BackpackExecutionError> {
        reject_symlink(&self.checkpoint)?;
        let current = if self.checkpoint.exists() {
            Some(fs::read(&self.checkpoint).map_err(|_| BackpackExecutionErrorKind::Storage)?)
        } else {
            None
        };
        if current != self.checkpoint_bytes {
            return Err(BackpackExecutionErrorKind::Storage.into());
        }
        let payload = CheckpointPayload {
            namespace: self.namespace.clone(),
            records: self.records.clone(),
            generation: self.generation,
            dirty_sticky: self.dirty_sticky,
        };
        let canonical =
            serde_json::to_vec(&payload).map_err(|_| BackpackExecutionErrorKind::Storage)?;
        let value = Checkpoint {
            version: 1,
            checksum: blake3::hash(&canonical).to_hex().to_string(),
            payload,
        };
        let bytes = serde_json::to_vec(&value).map_err(|_| BackpackExecutionErrorKind::Storage)?;
        if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
            return Err(BackpackExecutionErrorKind::Storage.into());
        }
        self.checkpoint_store
            .replace(&self.checkpoint, &bytes)
            .map_err(|_| BackpackExecutionErrorKind::Storage)?;
        self.checkpoint_bytes = Some(bytes);
        Ok(())
    }
}
/// Durable replacement boundary; success includes file and directory synchronization.
trait ExecutionCheckpoint: fmt::Debug + Send + Sync {
    fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;
}
#[derive(Debug)]
struct FileCheckpoint;
impl ExecutionCheckpoint for FileCheckpoint {
    fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let directory = path
            .parent()
            .ok_or_else(|| io::Error::other("missing checkpoint directory"))?;
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        let file = temporary.persist(path).map_err(|e| e.error)?;
        file.sync_all()?;
        #[cfg(unix)]
        fs::File::open(directory)?.sync_all()?;
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u32,
    checksum: String,
    payload: CheckpointPayload,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckpointPayload {
    namespace: BackpackClientIdNamespace,
    records: BTreeMap<ClientOrderId, OrderRecord>,
    generation: u64,
    dirty_sticky: bool,
}
fn reject_symlink(path: &Path) -> Result<(), BackpackExecutionError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            Err(BackpackExecutionErrorKind::Storage.into())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(BackpackExecutionErrorKind::Storage.into()),
    }
}
impl From<HttpClientError> for BackpackExecutionError {
    fn from(value: HttpClientError) -> Self {
        Self::Http(BackpackHttpError::from(value))
    }
}

fn decode_response(
    record: &mut OrderRecord,
    deleting: bool,
    response: &HttpResponse,
) -> Result<BackpackMutationReceipt, BackpackHttpError> {
    let status = response.status.as_u16();
    if response.body.len() > MAX_RESPONSE_BYTES {
        return Err(BackpackHttpError::unknown(BackpackHttpErrorKind::Decode));
    }
    if deleting && status == 202 {
        return Ok(BackpackMutationReceipt {
            status: BackpackMutationStatus::CancelPending,
            body: response.body.to_vec(),
        });
    }
    if status != 200 {
        let code = serde_json::from_slice::<serde_json::Value>(&response.body)
            .ok()
            .and_then(|value| {
                value
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|code| {
                !code.is_empty()
                    && code.len() <= 64
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            });
        let definitive = [400, 401, 403].contains(&status)
            && matches!(
                code.as_deref(),
                Some("INVALID_CLIENT_REQUEST" | "ACCOUNT_DEACTIVATED")
            );
        return Err(BackpackHttpError {
            kind: BackpackHttpErrorKind::Venue,
            outcome: if definitive {
                BackpackRequestOutcome::VenueRejected
            } else {
                BackpackRequestOutcome::Unknown
            },
            status: Some(status),
            code,
            retry_after: None,
        });
    }
    let wire: OrderAcknowledgement = serde_json::from_slice(&response.body)
        .map_err(|_| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?;
    let expected = record
        .spec
        .parameters(&record.symbol, Some(record.client_id))
        .map_err(|_| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?
        .json_body();
    let unsupported_semantics = [
        &wire.system_order_type,
        &wire.quote_quantity,
        &wire.trigger_price,
        &wire.trigger_quantity,
        &wire.trigger_by,
        &wire.stop_loss_trigger_price,
        &wire.stop_loss_limit_price,
        &wire.stop_loss_trigger_by,
        &wire.take_profit_trigger_price,
        &wire.take_profit_limit_price,
        &wire.take_profit_trigger_by,
        &wire.related_order_id,
        &wire.strategy_id,
    ]
    .iter()
    .any(|value| value.as_ref().is_some_and(|value| !value.is_null()))
        || [
            &wire.auto_borrow,
            &wire.auto_lend,
            &wire.auto_lend_redeem,
            &wire.auto_borrow_repay,
        ]
        .iter()
        .any(|value| {
            value
                .as_ref()
                .is_some_and(|value| !value.is_null() && value.as_bool() != Some(false))
        });
    let valid = !unsupported_semantics
        && wire.client_id == record.client_id
        && wire.symbol == record.symbol
        && wire.side == expected["side"]
        && wire.order_type == expected["orderType"]
        && wire.time_in_force == expected["timeInForce"]
        && wire.post_only == record.spec.post_only
        && wire.reduce_only == record.spec.reduce_only
        && parse_decimal(&wire.quantity) == Some(record.spec.quantity)
        && wire.price.as_deref().and_then(parse_decimal) == record.spec.price
        && parse_decimal(&wire.executed_quantity)
            .is_some_and(|qty| qty >= record.observed_cumulative && qty <= record.spec.quantity)
        && !wire.status.is_empty();
    let mut token = BackpackParameters::default();
    token
        .insert("orderId", Some(BackpackScalar::Token(wire.id.clone())))
        .map_err(|_| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?;
    if !valid
        || record
            .venue_order_id
            .as_ref()
            .is_some_and(|id| id != &wire.id)
    {
        return Err(BackpackHttpError::unknown(BackpackHttpErrorKind::Decode));
    }
    record.observed_cumulative = parse_decimal(&wire.executed_quantity)
        .ok_or_else(|| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?;
    if !deleting {
        record.venue_order_id = Some(wire.id);
    }
    Ok(BackpackMutationReceipt {
        status: BackpackMutationStatus::ResponseObserved,
        body: response.body.to_vec(),
    })
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderAcknowledgement {
    id: String,
    client_id: u32,
    symbol: String,
    side: String,
    order_type: String,
    time_in_force: String,
    quantity: String,
    price: Option<String>,
    post_only: bool,
    reduce_only: bool,
    executed_quantity: String,
    status: String,
    system_order_type: Option<Value>,
    quote_quantity: Option<Value>,
    trigger_price: Option<Value>,
    trigger_quantity: Option<Value>,
    trigger_by: Option<Value>,
    stop_loss_trigger_price: Option<Value>,
    stop_loss_limit_price: Option<Value>,
    stop_loss_trigger_by: Option<Value>,
    take_profit_trigger_price: Option<Value>,
    take_profit_limit_price: Option<Value>,
    take_profit_trigger_by: Option<Value>,
    related_order_id: Option<Value>,
    strategy_id: Option<Value>,
    auto_borrow: Option<Value>,
    auto_lend: Option<Value>,
    auto_lend_redeem: Option<Value>,
    auto_borrow_repay: Option<Value>,
    #[serde(flatten)]
    _extra: BTreeMap<String, Value>,
}
fn parse_decimal(value: &str) -> Option<Decimal> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return None;
    }
    Decimal::from_str_exact(value).ok()
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::{models::BackpackMarket, parsing::parse_market};
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::{Request, State},
        response::Response,
        routing::any,
    };
    use base64::{Engine, engine::general_purpose::STANDARD};
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::QuoteTick,
        enums::{OrderSide, OrderType, TimeInForce},
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use tokio::net::TcpListener;

    #[derive(Debug)]
    struct Clock(std::sync::atomic::AtomicU64);
    impl BackpackClock for Clock {
        fn timestamp_ms(&self) -> Result<u64, BackpackHttpError> {
            Ok(self.0.load(Ordering::Acquire))
        }
    }
    #[derive(Debug)]
    struct BlockingCheckpoint {
        calls: AtomicUsize,
        block_call: usize,
        arrived: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl ExecutionCheckpoint for BlockingCheckpoint {
        fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
            FileCheckpoint.replace(path, bytes)?;
            if self.calls.fetch_add(1, Ordering::AcqRel) == self.block_call {
                self.arrived.send(()).map_err(io::Error::other)?;
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(io::Error::other)?;
            }
            Ok(())
        }
    }
    fn authority() -> BackpackExecutionAuthority {
        BackpackExecutionAuthority {
            expires_at_ms: 100_000,
            max_account_age_ms: 10_000,
            max_market_age_ms: 10_000,
            max_order_notional: Decimal::from(1000),
            max_reserved_notional: Decimal::from(2000),
            max_reserved_margin: Decimal::from(500),
            max_unsettled_orders: 4,
            allow_new_risk: true,
            allow_reduction: true,
            allow_owned_cancel: true,
        }
    }
    fn instrument() -> InstrumentId {
        InstrumentId::from("BTC_USDC_PERP.BACKPACK")
    }
    fn account(namespace: &BackpackClientIdNamespace) -> BackpackLoopbackAccountFacts {
        BackpackLoopbackAccountFacts {
            namespace: namespace.clone(),
            generation: 1,
            observed_at_ms: 1000,
            available_margin: Decimal::from(500),
            margin_per_notional: Decimal::new(1, 1),
            fee_buffer_per_notional: Decimal::new(1, 3),
            economics_reference: "synthetic-peer".into(),
            net_positions: BTreeMap::from([(instrument(), Decimal::ONE)]),
            auto_borrow: false,
            auto_lend: false,
            auto_repay: false,
            liquidating: false,
            complete: true,
        }
    }
    fn market(config: &BackpackConfig) -> BackpackLoopbackMarketFacts {
        let raw: BackpackMarket =
            serde_json::from_str(include_str!("../../test_data/btc_usdc_perp.json")).unwrap();
        let time = UnixNanos::from(1_000_000_000);
        BackpackLoopbackMarketFacts {
            generation: 1,
            metadata: parse_market(&raw, config, time).unwrap(),
            quote: QuoteTick::new_checked(
                instrument(),
                Price::from("99.9"),
                Price::from("100.1"),
                Quantity::from("1"),
                Quantity::from("1"),
                time,
                time,
            )
            .unwrap(),
        }
    }
    fn spec() -> BackpackOrderSpec {
        BackpackOrderSpec {
            client_order_id: ClientOrderId::from("blocked-checkpoint"),
            instrument_id: instrument(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            quantity: Decimal::new(1, 1),
            price: Some(Decimal::from(100)),
            post_only: false,
            reduce_only: false,
        }
    }
    async fn handler(State(bytes): State<Arc<AtomicUsize>>, request: Request) -> Response {
        let body = to_bytes(request.into_body(), 8192).await.unwrap();
        bytes.fetch_add(body.len(), Ordering::AcqRel);
        let mut ack: Value = serde_json::from_slice(&body).unwrap();
        ack["id"] = json!("venue-1");
        ack["executedQuantity"] = json!("0");
        ack["status"] = json!("New");
        Response::builder()
            .status(200)
            .body(Body::from(ack.to_string()))
            .unwrap()
    }
    #[rstest]
    #[case("none")]
    #[case("deadline")]
    #[case("cancel")]
    #[case("session")]
    #[case("generation")]
    #[case("stop")]
    #[case("authority")]
    #[case("readiness")]
    #[case("freshness")]
    #[case("expiry")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_checkpoint_block_cannot_hide_deadline_or_admission_revocation(
        #[case] change: &str,
        #[values(1, 2)] checkpoint_call: usize,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let bytes = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(any(handler))
            .with_state(bytes.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoints = BackpackEndpoints::loopback_override(
            &format!("http://{address}"),
            &format!("ws://{address}"),
        )
        .unwrap();
        let namespace =
            BackpackClientIdNamespace::loopback_peer(&endpoints, "synthetic-checkpoint", None)
                .unwrap();
        let config =
            BackpackConfig::with_endpoints_checked(vec!["BTC_USDC_PERP".into()], endpoints.clone())
                .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(Clock(std::sync::atomic::AtomicU64::new(1000)));
        let (arrived_tx, arrived_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let checkpoint = Arc::new(BlockingCheckpoint {
            calls: AtomicUsize::new(0),
            block_call: checkpoint_call,
            arrived: arrived_tx,
            release: Mutex::new(release_rx),
        });
        let owner = Arc::new(
            BackpackOrderOwner::open_with_checkpoint(
                BackpackOrderOwnerConfig {
                    config: config.clone(),
                    endpoints: endpoints.clone(),
                    credential: BackpackCredential::loopback_peer(
                        &STANDARD.encode([7; 32]),
                        &endpoints,
                    )
                    .unwrap(),
                    quota: BackpackQuota::default(),
                    clock: clock.clone(),
                    policy: BackpackMutationPolicy {
                        window: BackpackReceiveWindow::default(),
                        budget: Duration::from_millis(500),
                    },
                    identities: BackpackClientIdStore::open(directory.path(), &namespace).unwrap(),
                    namespace: namespace.clone(),
                    authority: authority(),
                },
                checkpoint,
            )
            .unwrap(),
        );
        owner.guard.begin_session(&namespace, 1).unwrap();
        owner
            .guard
            .update_account(account(&namespace), 1000)
            .unwrap();
        owner.guard.update_market(market(&config), 1000).unwrap();
        let cancel = CancellationToken::new();
        let task_owner = owner.clone();
        let task_cancel = cancel.clone();
        let submit = tokio::spawn(async move { task_owner.submit(spec(), &task_cancel).await });
        tokio::task::spawn_blocking(move || {
            arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        })
        .await
        .unwrap();
        assert_eq!(bytes.load(Ordering::Acquire), 0);
        let updater = match change {
            "session" | "generation" | "stop" | "authority" | "readiness" => {
                let update_owner = owner.clone();
                let update_namespace = namespace.clone();
                let change = change.to_owned();
                Some(std::thread::spawn(move || match change.as_str() {
                    "session" => update_owner.guard.invalidate(),
                    "generation" => update_owner
                        .guard
                        .begin_session(&update_namespace, 2)
                        .unwrap(),
                    "stop" => {
                        update_owner.stop().unwrap();
                    }
                    "authority" => {
                        let mut value = authority();
                        value.allow_new_risk = false;
                        update_owner.guard.update_authority(value).unwrap();
                    }
                    "readiness" => {
                        let mut value = account(&update_namespace);
                        value.complete = false;
                        assert!(update_owner.guard.update_account(value, 1000).is_err());
                    }
                    _ => unreachable!(),
                }))
            }
            "deadline" => {
                tokio::time::sleep(Duration::from_millis(550)).await;
                None
            }
            "cancel" => {
                cancel.cancel();
                None
            }
            "freshness" => {
                clock.0.store(11_001, Ordering::Release);
                None
            }
            "expiry" => {
                clock.0.store(100_000, Ordering::Release);
                None
            }
            "none" => None,
            _ => unreachable!(),
        };
        if updater.is_some() {
            tokio::time::timeout(Duration::from_secs(2), async {
                while owner.guard.fence.snapshot().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        release_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), submit)
            .await
            .unwrap()
            .unwrap();
        if let Some(updater) = updater {
            updater.join().unwrap();
        }
        if change == "none" {
            assert!(result.is_ok());
            assert!(bytes.load(Ordering::Acquire) > 0);
        } else {
            match result.unwrap_err() {
                BackpackExecutionError::Http(value) => {
                    assert_eq!(value.outcome(), BackpackRequestOutcome::NotSent);
                }
                value => panic!("unexpected refusal: {value:?}"),
            }
            assert_eq!(bytes.load(Ordering::Acquire), 0);
            assert_eq!(
                owner
                    .guard
                    .lock()
                    .unwrap()
                    .identities
                    .venue_id(&spec().client_order_id),
                Some(1)
            );
            assert!(matches!(
                owner.submit(spec(), &CancellationToken::new()).await,
                Err(BackpackExecutionError::Local(
                    BackpackExecutionErrorKind::Duplicate | BackpackExecutionErrorKind::Readiness
                ))
            ));
        }
        drop(owner);
        let restored = BackpackClientIdStore::open(directory.path(), &namespace).unwrap();
        assert_eq!(restored.venue_id(&spec().client_order_id), Some(1));
        assert_eq!(restored.next_client_id(), 2);
        assert!(restored.unsigned_intent(&spec().client_order_id).is_some());
        server.abort();
    }
}
