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
        if !input.endpoints.is_loopback() {
            return Err(BackpackExecutionErrorKind::UnsupportedProduction.into());
        }
        Self::open_with_checkpoint(input, Arc::new(FileCheckpoint))
    }
    /// Opens the native production-protocol owner with explicit, bounded authority.
    /// Transport audience and credential-bound identity are checked before opening the owner.
    pub(crate) fn new_production(
        input: BackpackOrderOwnerConfig,
    ) -> Result<Self, BackpackExecutionError> {
        if !input
            .namespace
            .matches_credential_peer(&input.credential, &input.endpoints)
        {
            return Err(BackpackExecutionErrorKind::Identity.into());
        }
        let owner = Self::open_with_checkpoint(input, Arc::new(FileCheckpoint))?;
        owner.guard.lock()?.production = true;
        Ok(owner)
    }
    fn open_with_checkpoint(
        input: BackpackOrderOwnerConfig,
        checkpoint_store: Arc<dyn ExecutionCheckpoint>,
    ) -> Result<Self, BackpackExecutionError> {
        if input.config.endpoints() != &input.endpoints
            || (input.endpoints.is_loopback()
                && !input.namespace.matches_loopback_peer(&input.endpoints))
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
        let anchor_initialization_allowed = input.identities.initialized_this_open()
            && !checkpoint.exists()
            && input.identities.reserved_ids().next().is_none();
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
            shutdown_account: None,
            markets: BTreeMap::new(),
            public: None,
            records: BTreeMap::new(),
            checkpoint,
            checkpoint_bytes: None,
            checkpoint_store,
            production: false,
            recovery_start_ms: None,
            anchor_initialization_allowed,
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
    /// Pins an optional session boundary before any account recovery or dispatch.
    /// Legacy/default journals cannot acquire a later boundary; an existing boundary
    /// cannot be changed or removed, including by switching back to the default.
    pub(crate) fn bind_recovery_start_ms(
        &self,
        start: Option<u64>,
    ) -> Result<(), BackpackExecutionError> {
        let now = self.clock.timestamp_ms()?;
        let mut state = self.guard.lock()?;
        if !state.production
            || start.is_some_and(|value| value == 0 || value > u64::MAX / 1_000_000 || value > now)
            || (state.recovery_start_ms != start
                && !(state.anchor_initialization_allowed
                    && state.records.is_empty()
                    && state.generation == 0
                    && !state.session_active))
        {
            return Err(BackpackExecutionErrorKind::Identity.into());
        }
        state.recovery_start_ms = start;
        state.anchor_initialization_allowed = false;
        state.persist()
    }
    /// Returns the admission control handle for the native lifecycle owner.
    #[must_use]
    pub fn guard(&self) -> BackpackExecutionGuard {
        self.guard.clone()
    }
    pub(crate) fn owns_observation(&self, raw: &crate::account::models::BackpackOrder) -> bool {
        self.owned_observation_id(raw).is_some()
    }
    pub(crate) fn owned_observation_id(
        &self,
        raw: &crate::account::models::BackpackOrder,
    ) -> Option<ClientOrderId> {
        self.guard.lock().ok().and_then(|state| {
            state.records.iter().find_map(|(id, record)| {
                (record.venue_order_id.as_deref() == Some(raw.id.as_str())
                    && record.symbol == raw.symbol
                    && raw
                        .client_id
                        .is_none_or(|numeric| numeric == record.client_id))
                .then_some(*id)
            })
        })
    }
    /// Recovers an uncertain production creation only from a complete authenticated order
    /// observation matching every durable original command field, never from clientId alone.
    pub(crate) fn recover_production_binding(
        &self,
        raw: &crate::account::models::BackpackOrder,
    ) -> Result<Option<ClientOrderId>, BackpackExecutionError> {
        let mut state = self.guard.lock()?;
        if !state.production {
            return Ok(None);
        }
        let Some(id) = state.records.iter().find_map(|(id, record)| {
            (record.venue_order_id.as_deref() == Some(raw.id.as_str())).then_some(*id)
        }) else {
            // A numeric clientId, even with an identical specification, cannot
            // create a binding when the original POST acknowledgement is absent.
            return Ok(None);
        };
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        // Market responses can omit this optional intent field. A supplied value
        // must still match the independently established durable venue binding.
        if raw
            .client_id
            .is_some_and(|numeric| numeric != record.client_id)
        {
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        let view = record.spec.observation_view(raw)?;
        let raw = &view;
        let expected = record
            .spec
            .parameters(&record.symbol, Some(record.client_id))?
            .json_body();
        let matches = raw.symbol == record.symbol
            && raw.side == expected["side"]
            && raw.order_type == expected["orderType"]
            && raw.time_in_force == expected["timeInForce"]
            && raw.quantity.map(|value| value.0) == Some(record.spec.quantity)
            && raw.price.map(|value| value.0) == record.spec.price
            && raw.post_only == Some(record.spec.post_only)
            && raw.reduce_only == Some(record.spec.reduce_only)
            && raw.quote_quantity.is_none()
            && raw
                .system_order_type
                .as_deref()
                .is_none_or(|value| value == "User")
            && raw
                .executed_quantity
                .is_some_and(|value| value.0 >= Decimal::ZERO && value.0 <= record.spec.quantity)
            && raw.extra.iter().all(|(key, value)| {
                value.is_null() || crate::account::models::disabled_spot_order_flag(key, value)
            });
        if !matches {
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        VenueOrderId::new_checked(&raw.id).map_err(|_| BackpackExecutionErrorKind::Validation)?;
        Ok(Some(id))
    }
    pub(crate) fn reserved_reductions(
        &self,
        instrument: InstrumentId,
        side: nautilus_model::enums::OrderSide,
    ) -> Result<Vec<(ClientOrderId, Decimal)>, BackpackExecutionError> {
        Ok(self
            .guard
            .lock()?
            .records
            .values()
            .filter(|record| {
                record.holds_capacity()
                    && record.spec.reduce_only
                    && record.spec.instrument_id == instrument
                    && record.spec.side == side
            })
            .map(|record| (record.spec.client_order_id, record.spec.quantity))
            .collect())
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
                if state.production {
                    bindings
                        .original_specs
                        .insert(VenueOrderId::new_checked(venue)?, record.spec.clone());
                }
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
                    local_refusal_confirmed: false,
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
                if state.production {
                    // No dispatch call has occurred: retain the burned identity,
                    // but durably release capacity for this definite local refusal.
                    state
                        .records
                        .get_mut(&id)
                        .ok_or(BackpackExecutionErrorKind::Ownership)?
                        .local_refusal_confirmed = true;
                    state.persist()?;
                }
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
        if !state.production {
            state.account = None;
        }
        let production = state.production;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        let view = if production {
            Some(record.spec.observation_view(raw)?)
        } else {
            None
        };
        let raw = view.as_ref().unwrap_or(raw);
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
            let _change = self.guard.fence.change();
            record.status = CreateStatus::Unknown;
            state.account = None;
            state.dirty_sticky = true;
            state.persist()?;
            return Err(BackpackExecutionErrorKind::Ownership.into());
        }
        let cumulative = cumulative.ok_or(BackpackExecutionErrorKind::Validation)?;
        if cumulative > record.observed_cumulative {
            let _change = self.guard.fence.change();
            let reopened = record.status == CreateStatus::Reconciled;
            if reopened {
                record.status = CreateStatus::Unknown;
            }
            record.observed_cumulative = cumulative;
            if reopened {
                state.dirty_sticky = true;
            }
            state.account = None;
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
        state.stop(now)
    }

    /// Stops with a final account observation fenced by the native private owner.
    /// This observation never grants financial admission or requires public market readiness.
    pub(crate) fn stop_with_account(
        &self,
        proof: Option<BackpackShutdownAccountProof>,
    ) -> Result<BackpackShutdownReport, BackpackExecutionError> {
        let _change = self.guard.fence.change();
        let now = self.clock.timestamp_ms().ok();
        let mut state = self.guard.lock()?;
        if !state.stopped {
            // A failed final observation must not fall back to an invalidated admission snapshot.
            state.account = None;
            state.shutdown_account = proof;
        }
        state.stop(now)
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
                let production = state.production;
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
                    let mut values = record
                        .spec
                        .parameters(&record.symbol, Some(record.client_id))
                        .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
                    if production {
                        for flag in [
                            "autoBorrow",
                            "autoBorrowRepay",
                            "autoLend",
                            "autoLendRedeem",
                        ] {
                            values.insert(flag, Some(BackpackScalar::Boolean(false)))?;
                        }
                    }
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
        let production = state.production;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(BackpackExecutionErrorKind::Ownership)?;
        let already_reconciled = record.status == CreateStatus::Reconciled;
        let mut result =
            response.and_then(|response| decode_response(record, deleting, production, &response));
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
                record.local_refusal_confirmed =
                    production && error.outcome() == BackpackRequestOutcome::NotSent;
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
    #[serde(default, skip_serializing_if = "refusal_unconfirmed")]
    local_refusal_confirmed: bool,
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
fn refusal_unconfirmed(confirmed: &bool) -> bool {
    !*confirmed
}
impl OrderRecord {
    pub(crate) fn holds_capacity(&self) -> bool {
        !self.local_refusal_confirmed
            && !matches!(
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
pub(crate) struct BackpackShutdownAccountProof {
    pub(crate) observed_at_ms: u64,
    pub(crate) positions_flat: bool,
}

#[derive(Debug)]
pub(crate) struct OwnerState {
    pub(crate) production: bool,
    recovery_start_ms: Option<u64>,
    anchor_initialization_allowed: bool,
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
    shutdown_account: Option<BackpackShutdownAccountProof>,
    pub(crate) markets: BTreeMap<InstrumentId, BackpackLoopbackMarketFacts>,
    pub(crate) public: Option<super::guard::PublicAdmission>,
    pub(crate) records: BTreeMap<ClientOrderId, OrderRecord>,
    checkpoint: PathBuf,
    checkpoint_bytes: Option<Vec<u8>>,
    checkpoint_store: Arc<dyn ExecutionCheckpoint>,
}
impl OwnerState {
    fn stop(&mut self, now: Option<u64>) -> Result<BackpackShutdownReport, BackpackExecutionError> {
        self.stopped = true;
        self.session_active = false;
        self.dirty_sticky |= self.shutdown_report(now).dirty;
        self.persist()?;
        Ok(self.shutdown_report(now))
    }
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
        let positions_unknown_or_nonzero = if let Some(proof) = &self.shutdown_account {
            !proof.positions_flat
                || now_ms.is_none_or(|now| {
                    now.checked_sub(proof.observed_at_ms)
                        .is_none_or(|age| age > self.authority.max_account_age_ms)
                })
        } else {
            self.account.as_ref().is_none_or(|account| {
                account.generation != self.generation
                    || now_ms.is_none_or(|now| {
                        now.checked_sub(account.observed_at_ms)
                            .is_none_or(|age| age > self.authority.max_account_age_ms)
                    })
                    || account.net_positions.values().any(|q| *q != Decimal::ZERO)
            })
        };
        let unsent = self
            .records
            .values()
            .filter(|r| r.status == CreateStatus::Unsent)
            .filter(|r| !r.local_refusal_confirmed)
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
            self.recovery_start_ms = stored.payload.recovery_start_ms;
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
                        local_refusal_confirmed: false,
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
            if record.local_refusal_confirmed
                && (record.status != CreateStatus::Unsent
                    || record.venue_order_id.is_some()
                    || record.observed_cumulative != Decimal::ZERO
                    || record.economic_ack_reference.is_some()
                    || record.cancel != CancelStatus::NeverAttempted)
            {
                return Err(BackpackExecutionErrorKind::Storage.into());
            }
            if matches!(
                record.status,
                CreateStatus::Unsent | CreateStatus::ResponseObserved
            ) && !record.local_refusal_confirmed
            {
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
            recovery_start_ms: self.recovery_start_ms,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery_start_ms: Option<u64>,
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
    production: bool,
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
    let market = production && record.spec.order_type == nautilus_model::enums::OrderType::Market;
    let returned_quote = wire
        .quote_quantity
        .as_ref()
        .filter(|value| !value.is_null());
    let unsupported_quote = if market {
        returned_quote.is_some_and(|value| {
            wire.quantity.is_none()
                || !value
                    .as_str()
                    .and_then(parse_decimal)
                    .is_some_and(|value| value >= Decimal::ZERO)
        })
    } else {
        returned_quote.is_some()
    };
    let expected = record
        .spec
        .parameters(&record.symbol, Some(record.client_id))
        .map_err(|_| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?
        .json_body();
    let unsupported_semantics = [
        &wire.system_order_type,
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
        || unsupported_quote
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
        && wire
            .client_id
            .map_or(market, |client| client == record.client_id)
        && wire.symbol == record.symbol
        && wire.side == expected["side"]
        && wire.order_type == expected["orderType"]
        && wire.time_in_force == expected["timeInForce"]
        && wire
            .post_only
            .map_or(market, |flag| flag == record.spec.post_only)
        && wire
            .reduce_only
            .map_or(market, |flag| flag == record.spec.reduce_only)
        && wire.quantity.as_deref().map_or(market, |quantity| {
            parse_decimal(quantity) == Some(record.spec.quantity)
        })
        && if market {
            wire.price.is_none()
        } else {
            wire.price.as_deref().and_then(parse_decimal) == record.spec.price
        }
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
    client_id: Option<u32>,
    symbol: String,
    side: String,
    order_type: String,
    time_in_force: String,
    quantity: Option<String>,
    price: Option<String>,
    post_only: Option<bool>,
    reduce_only: Option<bool>,
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

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
    use tokio::net::TcpListener;

    use super::*;
    use crate::{models::BackpackMarket, parsing::parse_market};

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
    fn recovery_anchor_owner(directory: &Path) -> BackpackOrderOwner {
        let endpoints =
            BackpackEndpoints::loopback_override("http://127.0.0.1:8123", "ws://127.0.0.1:8123")
                .unwrap();
        let credential =
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &endpoints).unwrap();
        let namespace =
            BackpackClientIdNamespace::credential_scope(&credential, &endpoints, None).unwrap();
        BackpackOrderOwner::new_production(BackpackOrderOwnerConfig {
            config: BackpackConfig::with_endpoints_checked(
                vec!["BTC_USDC_PERP".into()],
                endpoints.clone(),
            )
            .unwrap(),
            endpoints,
            credential,
            quota: BackpackQuota::default(),
            clock: Arc::new(Clock(std::sync::atomic::AtomicU64::new(1000))),
            policy: BackpackMutationPolicy {
                window: Default::default(),
                budget: Duration::from_secs(1),
            },
            identities: BackpackClientIdStore::open(directory, &namespace).unwrap(),
            namespace,
            authority: authority(),
        })
        .unwrap()
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn production_public_recovery_requires_full_scope_and_preserves_execution_state(
        #[case] new_epoch: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let owner = recovery_anchor_owner(directory.path());
        let guard = owner.guard();
        let now = crate::runtime::now();
        let now_ms = now.as_u64() / 1_000_000;
        let config = BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".into(), "SOL_USDC_PERP".into()],
            owner.endpoints.clone(),
        )
        .unwrap();
        let namespace = {
            let mut state = guard.lock().unwrap();
            state.config = config.clone();
            state.authority.expires_at_ms = now_ms + 100_000;
            state.namespace.clone()
        };
        guard.begin_session(&namespace, 1).unwrap();
        let mut facts = account(&namespace);
        facts.observed_at_ms = now_ms;
        facts
            .net_positions
            .insert(InstrumentId::from("SOL_USDC_PERP.BACKPACK"), Decimal::ZERO);
        guard.update_account(facts, now_ms).unwrap();
        for symbol in ["BTC_USDC_PERP", "SOL_USDC_PERP"] {
            let raw: BackpackMarket = serde_json::from_str(if symbol == "BTC_USDC_PERP" {
                include_str!("../../test_data/btc_usdc_perp.json")
            } else {
                include_str!("../../test_data/sol_usdc_perp.json")
            })
            .unwrap();
            let metadata = parse_market(&raw, &config, now).unwrap();
            let mut quote = market(&config).quote;
            quote.instrument_id = metadata.instrument_id;
            quote.ts_event = now;
            quote.ts_init = now;
            guard
                .update_market(
                    BackpackLoopbackMarketFacts {
                        generation: 1,
                        metadata,
                        quote,
                    },
                    now_ms,
                )
                .unwrap();
        }
        let telemetry = crate::telemetry::BackpackPublicTelemetry::default();
        let public_gate = telemetry
            .claim(&owner.endpoints, &BackpackQuota::default(), 30, 3000)
            .unwrap();
        {
            let mut gate = public_gate.lock();
            gate.running = true;
            gate.connected = true;
            gate.metadata_ready = true;
            gate.owner = 1;
            gate.epoch = 1;
            for symbol in ["BTC_USDC_PERP", "SOL_USDC_PERP"] {
                gate.quote_receipts.insert(symbol.into(), (now, now));
            }
        }
        guard.bind_public(telemetry.clone()).unwrap();
        let retained = {
            let mut state = guard.lock().unwrap();
            let mut retained_spec = spec();
            retained_spec.client_order_id = ClientOrderId::from("already-in-flight");
            state.records.insert(
                retained_spec.client_order_id,
                OrderRecord {
                    local_refusal_confirmed: false,
                    spec: retained_spec,
                    symbol: "BTC_USDC_PERP".into(),
                    notional: Decimal::from(10),
                    margin: Decimal::ONE,
                    client_id: 123,
                    status: CreateStatus::ResponseObserved,
                    venue_order_id: Some("venue-existing".into()),
                    cancel: CancelStatus::NeverAttempted,
                    observed_cumulative: Decimal::new(1, 2),
                    economic_ack_reference: Some("durable-existing-receipt".into()),
                },
            );
            serde_json::to_value(&state.records).unwrap()
        };
        guard.refresh_production_public(now_ms, None).unwrap();
        {
            let mut gate = public_gate.lock();
            gate.invalidate("synthetic public reconnect");
            if new_epoch {
                gate.epoch += 1;
            }
            gate.connected = true;
            gate.metadata_ready = true;
            gate.stale = None;
            gate.quote_receipts
                .insert("BTC_USDC_PERP".into(), (now, now));
        }
        assert!(
            guard
                .lock()
                .unwrap()
                .reservation(&spec(), now_ms, None)
                .is_err()
        );
        assert!(guard.refresh_production_public(now_ms, None).is_err());
        let target = instrument();
        let unrelated = InstrumentId::from("SOL_USDC_PERP.BACKPACK");
        assert!(
            guard
                .refresh_production_public(now_ms, Some(unrelated))
                .is_err()
        );
        assert!(
            guard
                .refresh_production_public(
                    now_ms,
                    Some(InstrumentId::from("ETH_USDC_PERP.BACKPACK"))
                )
                .is_err()
        );
        guard
            .refresh_production_public(now_ms, Some(target))
            .unwrap();
        let mut reduction = spec();
        reduction.side = OrderSide::Sell;
        reduction.reduce_only = true;
        assert!(
            guard
                .lock()
                .unwrap()
                .reservation(&reduction, now_ms, None)
                .is_ok()
        );
        assert!(guard.refresh_production_public(now_ms, None).is_err());
        public_gate.lock().quote_receipts.remove("BTC_USDC_PERP");
        assert!(
            guard
                .refresh_production_public(now_ms, Some(target))
                .is_err()
        );
        assert!(
            guard
                .lock()
                .unwrap()
                .reservation(&reduction, now_ms, None)
                .is_err()
        );
        public_gate
            .lock()
            .quote_receipts
            .insert("BTC_USDC_PERP".into(), (now, now));
        let target_facts = guard.lock().unwrap().markets.remove(&target).unwrap();
        assert!(
            guard
                .refresh_production_public(now_ms, Some(target))
                .is_err()
        );
        guard.lock().unwrap().markets.insert(target, target_facts);
        public_gate
            .lock()
            .quote_receipts
            .insert("SOL_USDC_PERP".into(), (now, now));
        guard.refresh_production_public(now_ms, None).unwrap();
        {
            let state = guard.lock().unwrap();
            assert_eq!(state.generation, 1);
            assert!(state.session_active && state.account.is_some());
            assert_eq!(serde_json::to_value(&state.records).unwrap(), retained);
            assert_eq!(state.markets.len(), 2);
            assert!(state.reservation(&spec(), now_ms, None).is_ok());
        }
        let mut account = guard.lock().unwrap().account.take().unwrap();
        assert!(guard.refresh_production_public(now_ms, None).is_err());
        account.observed_at_ms = now_ms - 10_001;
        guard.lock().unwrap().account = Some(account.clone());
        assert!(guard.refresh_production_public(now_ms, None).is_err());
        account.observed_at_ms = now_ms;
        guard.lock().unwrap().account = Some(account);
        guard.refresh_production_public(now_ms, None).unwrap();
        public_gate.lock().invalidate("synthetic late public fault");
        assert!(
            guard
                .lock()
                .unwrap()
                .reservation(&spec(), now_ms, None)
                .is_err()
        );
    }

    #[rstest]
    #[case(Some(1000), true, false)]
    #[case(Some(899), true, true)]
    #[case(Some(1001), true, true)]
    #[case(Some(1000), false, true)]
    #[case(None, true, true)]
    fn shutdown_account_is_independent_of_admission_but_requires_fresh_flat_proof(
        #[case] stamp: Option<u64>,
        #[case] flat: bool,
        #[case] dirty: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let owner = recovery_anchor_owner(directory.path());
        let guard = owner.guard();
        {
            let mut state = guard.lock().unwrap();
            state.authority.max_account_age_ms = 100;
            state.authority.expires_at_ms = 999;
            assert!(!state.session_active && state.public.is_none());
        }
        let proof = stamp.map(|observed_at_ms| BackpackShutdownAccountProof {
            observed_at_ms,
            positions_flat: flat,
        });
        let report = owner.stop_with_account(proof).unwrap();
        assert_eq!(report.dirty, dirty);
        assert_eq!(report.positions_unknown_or_nonzero, dirty);
        assert!(guard.lock().unwrap().check_session(1000).is_err());
        let repeated = owner
            .stop_with_account(Some(BackpackShutdownAccountProof {
                observed_at_ms: 1000,
                positions_flat: true,
            }))
            .unwrap();
        assert_eq!(repeated.dirty, dirty);
        assert_eq!(repeated.positions_unknown_or_nonzero, dirty);
    }

    #[rstest]
    fn authority_renewal_preserves_scope_limits_and_permissions() {
        let directory = tempfile::tempdir().unwrap();
        let owner = recovery_anchor_owner(directory.path());
        let guard = owner.guard();
        let before = guard.lock().unwrap().authority.clone();
        guard.renew_authority(301_000, 1000).unwrap();
        let after = guard.lock().unwrap().authority.clone();
        assert_eq!(after.expires_at_ms, 301_000);
        assert_eq!(after.max_order_notional, before.max_order_notional);
        assert_eq!(after.max_reserved_notional, before.max_reserved_notional);
        assert_eq!(after.max_reserved_margin, before.max_reserved_margin);
        assert_eq!(after.max_account_age_ms, before.max_account_age_ms);
        assert_eq!(after.max_market_age_ms, before.max_market_age_ms);
        assert_eq!(after.max_unsettled_orders, before.max_unsettled_orders);
        assert_eq!(after.allow_new_risk, before.allow_new_risk);
        assert_eq!(after.allow_reduction, before.allow_reduction);
        assert_eq!(after.allow_owned_cancel, before.allow_owned_cancel);
        assert!(guard.renew_authority(301_000, 1000).is_err());
        assert!(guard.renew_authority(301_001, 1000).is_err());
        owner.stop().unwrap();
        assert!(guard.renew_authority(302_000, 2000).is_err());
    }
    #[rstest]
    fn recovery_anchor_is_pinned_across_restart_and_cannot_change_or_disappear() {
        let directory = tempfile::tempdir().unwrap();
        let owner = recovery_anchor_owner(directory.path());
        owner.bind_recovery_start_ms(Some(500)).unwrap();
        assert!(owner.bind_recovery_start_ms(Some(501)).is_err());
        assert!(owner.bind_recovery_start_ms(None).is_err());
        drop(owner);
        let owner = recovery_anchor_owner(directory.path());
        owner.bind_recovery_start_ms(Some(500)).unwrap();
        assert!(owner.bind_recovery_start_ms(Some(499)).is_err());
        assert!(owner.bind_recovery_start_ms(None).is_err());
    }
    #[rstest]
    fn recovery_anchor_cannot_be_introduced_into_existing_default_namespace() {
        let directory = tempfile::tempdir().unwrap();
        let owner = recovery_anchor_owner(directory.path());
        owner.bind_recovery_start_ms(None).unwrap();
        assert!(owner.bind_recovery_start_ms(Some(500)).is_err());
        drop(owner);
        let owner = recovery_anchor_owner(directory.path());
        owner.bind_recovery_start_ms(None).unwrap();
        assert!(owner.bind_recovery_start_ms(Some(500)).is_err());
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
