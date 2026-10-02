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

use std::{sync::Arc, time::Duration};

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
            limiter: Arc::new(RateLimiter::new_with_quota(
                None,
                vec![
                    (Ustr::from("standard"), standard),
                    (Ustr::from("historical-market"), historical),
                ],
            )),
        })
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
