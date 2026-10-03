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

//! A caller-owned quota scope shared across public and private clients.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_network::dst::time::Instant;

use nautilus_network::ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota};
use ustr::Ustr;

use super::error::{BackpackHttpError, BackpackHttpErrorKind};

/// Shared subaccount budget, including public reads and recovery pagination.
///
/// Reuse clones for every client consuming one venue scope. Separate processes and
/// external callers require additional coordination; this in-memory limiter cannot
/// account for their traffic. Defaults pace below the official 2000/min and 30/min
/// ceilings with burst capacity one because the venue's exact window is unconfirmed.
#[derive(Clone, Debug)]
pub struct BackpackQuota {
    limiter: Arc<RateLimiter<Ustr, MonotonicClock>>,
    diagnostics: Arc<Mutex<BackpackQuotaDiagnostics>>,
}
impl Default for BackpackQuota {
    fn default() -> Self {
        Self::with_periods(Duration::from_millis(32), Duration::from_millis(2100))
            .expect("nonzero conservative quota periods")
    }
}
impl BackpackQuota {
    /// Creates a shared scope with equal or slower pacing than the published ceilings.
    ///
    /// # Errors
    ///
    /// Returns an error for a standard period below30ms or historical period below2s.
    pub fn with_periods(
        standard: Duration,
        historical_market: Duration,
    ) -> Result<Self, BackpackHttpError> {
        if standard < Duration::from_millis(30) || historical_market < Duration::from_secs(2) {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
        }
        let standard = Quota::with_period(standard)
            .ok_or_else(|| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
        let historical = Quota::with_period(historical_market)
            .ok_or_else(|| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
        Ok(Self {
            diagnostics: Arc::new(Mutex::new(BackpackQuotaDiagnostics::default())),
            limiter: Arc::new(RateLimiter::new_with_quota(
                None,
                vec![
                    (Ustr::from("standard"), standard),
                    (Ustr::from("historical-market"), historical),
                ],
            )),
        })
    }
    /// Returns whether these handles consume the same caller-owned REST limiter.
    #[must_use]
    pub fn shares_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.limiter, &other.limiter)
    }
    /// Returns measured in-process quota waits shared by every clone of this scope.
    ///
    /// Nanoseconds measure monotonic elapsed time from entering the shared HTTP primitive
    /// until preparation starts, or until a queued attempt is refused/dropped. Disk,
    /// signing, response time and retry backoff are excluded. Counters saturate.
    #[must_use]
    pub fn diagnostics(&self) -> BackpackQuotaDiagnostics {
        *self.diagnostics.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(crate) fn start_wait(&self) -> BackpackQuotaWait {
        BackpackQuotaWait {
            quota: self.clone(),
            started: Instant::now(),
            completed: false,
        }
    }
    fn record_wait(&self, elapsed: Duration, admitted: bool) {
        let nanoseconds = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let mut value = self.diagnostics.lock().unwrap_or_else(|e| e.into_inner());
        value.observed_waits = value.observed_waits.saturating_add(1);
        if admitted {
            value.admitted_waits = value.admitted_waits.saturating_add(1);
        } else {
            value.refused_waits = value.refused_waits.saturating_add(1);
        }
        value.last_queue_wait_ns = nanoseconds;
        value.total_queue_wait_ns = value.total_queue_wait_ns.saturating_add(nanoseconds);
        value.last_admitted = Some(admitted);
    }
    pub(crate) fn into_limiter(self) -> Arc<RateLimiter<Ustr, MonotonicClock>> {
        self.limiter
    }
    pub(crate) fn keys(historical_market: bool) -> Vec<String> {
        let mut keys = vec!["standard".into()];
        if historical_market {
            keys.push("historical-market".into());
        }
        keys
    }
}

/// Sanitized quota diagnostics for one caller-owned in-process limiter scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpackQuotaDiagnostics {
    pub schema_version: u8,
    pub observed_waits: u64,
    pub admitted_waits: u64,
    pub refused_waits: u64,
    pub last_queue_wait_ns: u64,
    pub total_queue_wait_ns: u64,
    pub last_admitted: Option<bool>,
}
impl Default for BackpackQuotaDiagnostics {
    fn default() -> Self {
        Self {
            schema_version: 1,
            observed_waits: 0,
            admitted_waits: 0,
            refused_waits: 0,
            last_queue_wait_ns: 0,
            total_queue_wait_ns: 0,
            last_admitted: None,
        }
    }
}
pub(crate) struct BackpackQuotaWait {
    quota: BackpackQuota,
    started: Instant,
    completed: bool,
}
impl BackpackQuotaWait {
    pub(crate) fn admitted(&mut self) {
        self.quota.record_wait(self.started.elapsed(), true);
        self.completed = true;
    }
}
impl Drop for BackpackQuotaWait {
    fn drop(&mut self) {
        if !self.completed {
            self.quota.record_wait(self.started.elapsed(), false);
        }
    }
}
