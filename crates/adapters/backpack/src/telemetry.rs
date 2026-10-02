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

//! Read-only telemetry tied to one actual native public client owner.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use parking_lot::Mutex;

use crate::{
    data_error::BackpackDataError,
    runtime::{BackpackPublicHealth, Gate},
};
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);
#[derive(Default)]
struct Inner {
    gate: Mutex<Arc<Mutex<Gate>>>,
    claimed: AtomicBool,
    scope: Mutex<
        Option<(
            crate::common::endpoints::BackpackEndpoints,
            crate::http::quota::BackpackQuota,
        )>,
    >,
}
/// Explicitly shared observation handle; a configuration cannot silently host two clients.
#[derive(Clone, Default)]
pub struct BackpackPublicTelemetry {
    inner: Arc<Inner>,
}
/// Internal identity captured atomically with public observations, never caller-created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackpackPublicAdmissionToken {
    run: u64,
    generation: u64,
    epoch: u64,
    fault_serial: u64,
}
impl std::fmt::Debug for BackpackPublicTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpackPublicTelemetry")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}
impl BackpackPublicTelemetry {
    /// Returns a read-only observation of the actual owner, including original receipt age.
    #[must_use]
    pub fn snapshot(&self) -> BackpackPublicHealth {
        self.admission_snapshot().0
    }
    pub(crate) fn admission_snapshot(
        &self,
    ) -> (BackpackPublicHealth, BackpackPublicAdmissionToken) {
        let current = self.inner.gate.lock().clone();
        let gate = current.lock();
        (
            gate.health(gate.idle_secs),
            BackpackPublicAdmissionToken {
                run: gate.run_id,
                generation: gate.owner,
                epoch: gate.epoch,
                fault_serial: gate.fault_serial,
            },
        )
    }
    pub(crate) fn claim(
        &self,
        endpoints: &crate::common::endpoints::BackpackEndpoints,
        quota: &crate::http::quota::BackpackQuota,
        idle_secs: u64,
        quote_stale_after_ms: u64,
    ) -> Result<Arc<Mutex<Gate>>, BackpackDataError> {
        if self
            .inner
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(BackpackDataError::Configuration(
                "telemetry already owned by another client",
            ));
        }
        let run_id = NEXT_RUN
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(1))
            .map_err(|_| {
                self.release();
                BackpackDataError::Lifecycle("run identity exhausted")
            })?;
        let gate = Arc::new(Mutex::new(Gate {
            run_id,
            idle_secs,
            quote_stale_after_ms,
            ..Gate::default()
        }));
        *self.inner.scope.lock() = Some((endpoints.clone(), quota.clone()));
        *self.inner.gate.lock() = gate.clone();
        Ok(gate)
    }

    pub(crate) fn owns_scope(
        &self,
        endpoints: &crate::common::endpoints::BackpackEndpoints,
        quota: &crate::http::quota::BackpackQuota,
    ) -> bool {
        self.inner.claimed.load(Ordering::Acquire)
            && self
                .inner
                .scope
                .lock()
                .as_ref()
                .is_some_and(|(actual, limiter)| actual == endpoints && limiter.shares_scope(quota))
    }

    pub(crate) fn release(&self) {
        self.inner.claimed.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use rstest::rstest;

    use super::*;
    #[rstest]
    fn fresh_claim_cannot_inherit_old_gate_or_future_receipts() {
        let telemetry = BackpackPublicTelemetry::default();
        let old = telemetry
            .claim(
                &crate::common::endpoints::BackpackEndpoints::production(),
                &crate::http::quota::BackpackQuota::default(),
                30,
                3000,
            )
            .unwrap();
        let old_id = telemetry.snapshot().run_id;
        {
            let mut gate = old.lock();
            gate.running = true;
            gate.connected = true;
            gate.metadata_ready = true;
            gate.owner = 7;
            let future = UnixNanos::from(crate::runtime::now().as_u64() + 1_000_000_000);
            gate.quote_receipts
                .insert("BTC_USDC_PERP".into(), (future, future));
        }
        assert!(!telemetry.snapshot().quotes_fresh["BTC_USDC_PERP"]);
        telemetry.release();
        let new = telemetry
            .claim(
                &crate::common::endpoints::BackpackEndpoints::production(),
                &crate::http::quota::BackpackQuota::default(),
                30,
                3000,
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&old, &new));
        old.lock().connected = true;
        old.lock().metadata_ready = true;
        let snapshot = telemetry.snapshot();
        assert_ne!(snapshot.run_id, old_id);
        assert_eq!(snapshot.generation, 0);
        assert!(!snapshot.connected && !snapshot.metadata_ready);
        assert!(snapshot.quotes_fresh.is_empty() && snapshot.books.is_empty());
    }
    #[rstest]
    fn actual_native_scope_requires_claimed_exact_origin_and_same_limiter() {
        let telemetry = BackpackPublicTelemetry::default();
        let endpoints = crate::common::endpoints::BackpackEndpoints::loopback_override(
            "http://127.0.0.1:8123",
            "ws://127.0.0.1:8123",
        )
        .unwrap();
        let quota = crate::http::quota::BackpackQuota::default();
        assert!(!telemetry.owns_scope(&endpoints, &quota));
        telemetry.claim(&endpoints, &quota, 30, 3000).unwrap();
        assert!(telemetry.owns_scope(&endpoints, &quota));
        assert!(!telemetry.owns_scope(&endpoints, &crate::http::quota::BackpackQuota::default()));
        assert!(!telemetry.owns_scope(
            &crate::common::endpoints::BackpackEndpoints::production(),
            &quota
        ));
        telemetry.release();
        assert!(!telemetry.owns_scope(&endpoints, &quota));
    }
    #[rstest]
    fn same_epoch_recovery_cannot_reuse_pre_fault_admission() {
        let telemetry = BackpackPublicTelemetry::default();
        let gate = telemetry
            .claim(
                &crate::common::endpoints::BackpackEndpoints::production(),
                &crate::http::quota::BackpackQuota::default(),
                30,
                3000,
            )
            .unwrap();
        {
            let mut state = gate.lock();
            state.running = true;
            state.owner = 1;
            state.epoch = 1;
            state.connected = true;
            state.metadata_ready = true;
        }
        let (_, before) = telemetry.admission_snapshot();
        {
            let mut state = gate.lock();
            state.invalidate("synthetic fault");
            state.connected = true;
            state.metadata_ready = true;
            state.stale = None;
        }
        let (health, after) = telemetry.admission_snapshot();
        assert_eq!(before.run, after.run);
        assert_eq!(before.generation, after.generation);
        assert_eq!(before.epoch, after.epoch);
        assert!(health.connected);
        assert_ne!(before, after);
    }
}
