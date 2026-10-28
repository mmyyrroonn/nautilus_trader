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

//! Shared REST admission with cancel-first priority and reactive venue cooldowns.
//!
//! The historical one-request-per-second default was a local integration precaution,
//! not a venue limit. Production defaults to no fixed pacing. Explicit quotas remain
//! available to callers. HTTP 429 cooldowns apply to all clients of one environment.
//! Cancel requests precede reductions/reconciliation, which precede ordinary requests;
//! priority does not bypass a venue cooldown or a caller-specified quota.

use std::{
    num::NonZeroU32,
    sync::{
        Arc, LazyLock, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use nautilus_network::ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use ustr::Ustr;

use crate::common::enums::OndoEnvironment;

/// The single rate-limit bucket every Ondo Perps REST request draws on.
///
/// One bucket for the whole adapter is the point: a per-endpoint limiter would defeat the shared
/// budget §4.4 asks for.
pub const ONDO_REST_BUCKET: &str = "ondo:rest";

/// Historical opt-in quota, retained for callers that explicitly choose conservative pacing.
pub const ONDO_REST_REQUESTS_PER_SECOND: u32 = 1;

/// Historical opt-in REST quota with a single-request burst.
pub static ONDO_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(ONDO_REST_REQUESTS_PER_SECOND).expect("non-zero"))
        .expect("one request per second has a non-zero replenish interval")
});

/// The process-wide REST budgets, one per adapter environment.
///
/// The key is the environment and nothing else: one process that reads production market data and
/// talks to a sandbox account holds two buckets, and two clients of one environment hold one. The
/// map is never cleared - a bucket is a plain allocation with no state worth resetting - so a
/// factory, a re-created client and a reconnecting client all resolve the same instance.
static ENVIRONMENT_BUDGETS: LazyLock<Mutex<AHashMap<OndoEnvironment, OndoRateBudget>>> =
    LazyLock::new(|| Mutex::new(AHashMap::new()));

/// Returns the REST budget the whole process shares for `environment` (plan §4.4).
///
/// This is the resolution the factories use at `create`, from the environment the configuration
/// names: two clients built for one environment - the data client whose refresh reads the market
/// metadata and the execution client whose account calls share the venue's request limit - pace
/// against one bucket, and a client built for another environment never does. The answer is a clone
/// of a shared handle, so callers hold the bucket rather than the registry.
#[must_use]
pub fn shared_rest_budget(environment: OndoEnvironment) -> OndoRateBudget {
    let mut budgets = ENVIRONMENT_BUDGETS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    budgets.entry(environment).or_default().clone()
}

/// The class of traffic a slot in the shared budget is being taken for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OndoRequestPriority {
    /// Ordinary market metadata and history reads.
    Normal,
    /// Reduce-only close, unknown-order query and reconciliation traffic.
    ///
    /// It reserves the next unconsumed slot ahead of ordinary reads while drawing on the same
    /// budget as [`Self::Normal`].
    High,
    /// Cancellations precede all other queued traffic.
    Cancel,
}

/// The shared REST budget for one adapter environment.
///
/// Cloning shares the budget; constructing a second one creates an independent budget. A
/// process that needs one budget for its data client and its execution client creates one instance
/// and passes a clone to each.
#[derive(Clone, Debug)]
pub struct OndoRateBudget {
    limiter: Arc<RateLimiter<Ustr, MonotonicClock>>,
    /// FIFO turnstile for waiters on the single shared bucket.
    ///
    /// The limiter preserves the rate but does not promise which awakened waiter wins the next
    /// slot. Without one queue, a periodic reconciliation pass can repeatedly overtake an order
    /// that was already waiting and starve it indefinitely.
    waiters: Arc<AsyncMutex<()>>,
    priority_waiters: Arc<AtomicUsize>,
    priority_changed: Arc<Notify>,
    cancel_waiters: Arc<AtomicUsize>,
    cooldown: Arc<Mutex<Option<tokio::time::Instant>>>,
    paced: bool,
}

impl Default for OndoRateBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoRateBudget {
    /// Creates an independent unpaced budget with shared reactive cooldowns.
    #[must_use]
    pub fn new() -> Self {
        let mut budget = Self::with_quota(*ONDO_REST_QUOTA);
        budget.paced = false;
        budget
    }

    /// Creates an independent budget with an explicit quota.
    ///
    /// All clients sharing this budget obey the supplied quota as well as venue cooldowns.
    #[must_use]
    pub fn with_quota(quota: Quota) -> Self {
        Self {
            limiter: Arc::new(RateLimiter::new_with_quota(
                None,
                vec![(Ustr::from(ONDO_REST_BUCKET), quota)],
            )),
            waiters: Arc::new(AsyncMutex::new(())),
            priority_waiters: Arc::new(AtomicUsize::new(0)),
            priority_changed: Arc::new(Notify::new()),
            cancel_waiters: Arc::new(AtomicUsize::new(0)),
            cooldown: Arc::new(Mutex::new(None)),
            paced: true,
        }
    }

    /// Returns the shared limiter instance.
    ///
    /// Two budgets are the same budget exactly when this returns the same [`Arc`]. The handle is
    /// exposed for a transport that meters its own requests and for tests that need to prove the
    /// sharing; every request this adapter makes still goes through [`Self::acquire`].
    #[must_use]
    pub fn limiter(&self) -> &Arc<RateLimiter<Ustr, MonotonicClock>> {
        &self.limiter
    }

    /// Extends the shared cooldown after a venue rate-limit response.
    pub fn defer(&self, duration: Duration) {
        let until = tokio::time::Instant::now() + duration;
        let mut current = self.cooldown.lock().unwrap_or_else(PoisonError::into_inner);
        *current = Some(current.map_or(until, |old| old.max(until)));
        self.priority_changed.notify_waiters();
    }

    fn higher_waiting(&self, priority: OndoRequestPriority) -> bool {
        (priority != OndoRequestPriority::Cancel
            && self.cancel_waiters.load(Ordering::Acquire) != 0)
            || (priority == OndoRequestPriority::Normal
                && self.priority_waiters.load(Ordering::Acquire) != 0)
    }

    async fn ready(&self) {
        loop {
            let until = *self.cooldown.lock().unwrap_or_else(PoisonError::into_inner);
            match until {
                Some(until) if until > tokio::time::Instant::now() => {
                    tokio::time::sleep_until(until).await;
                }
                _ => break,
            }
        }
        if self.paced {
            self.limiter
                .await_keys_ready(Some(&[Ustr::from(ONDO_REST_BUCKET)]))
                .await;
        }
    }

    /// Waits for shared cooldown/quota availability, allowing cancels to preempt waiters.
    pub async fn acquire(&self, priority: OndoRequestPriority) {
        let count = match priority {
            OndoRequestPriority::Normal => None,
            OndoRequestPriority::High => Some(&self.priority_waiters),
            OndoRequestPriority::Cancel => Some(&self.cancel_waiters),
        };
        let _registration = count.map(|count| {
            count.fetch_add(1, Ordering::AcqRel);
            self.priority_changed.notify_waiters();
            PriorityRegistration {
                count: Arc::clone(count),
                changed: Arc::clone(&self.priority_changed),
            }
        });
        loop {
            let changed = self.priority_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.higher_waiting(priority) {
                changed.await;
                continue;
            }
            let turn = self.waiters.lock().await;
            if self.higher_waiting(priority) {
                drop(turn);
                continue;
            }
            tokio::select! {
                biased;
                () = &mut changed => { drop(turn); }
                () = self.ready() => { return; }
            }
        }
    }
}

struct PriorityRegistration {
    count: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl Drop for PriorityRegistration {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use rstest::rstest;

    use super::*;
    use crate::http::client::OndoHttpClient;

    /// A client that holds `budget`. Its base URL is never dialled by these tests.
    fn client_on(budget: &OndoRateBudget) -> OndoHttpClient {
        OndoHttpClient::builder()
            .base_url("http://127.0.0.1:1".to_string())
            .budget(budget.clone())
            .build()
            .expect("the client builds")
    }

    #[rstest]
    fn test_the_explicit_historical_quota_is_one_request_per_second() {
        assert_eq!(ONDO_REST_REQUESTS_PER_SECOND, 1);
        assert_eq!(ONDO_REST_BUCKET, "ondo:rest");
        assert_eq!(ONDO_REST_QUOTA.burst_size().get(), 1);
        assert_eq!(ONDO_REST_QUOTA.replenish_interval(), Duration::from_secs(1));
    }

    /// §4.4's isolation key: the environment. Two resolutions of one environment are one bucket,
    /// and the two environments this adapter knows never contend for one another's tokens.
    #[rstest]
    fn test_the_process_shares_one_budget_per_environment_and_never_across_environments() {
        let production = shared_rest_budget(OndoEnvironment::Production);
        let again = shared_rest_budget(OndoEnvironment::Production);
        let sandbox = shared_rest_budget(OndoEnvironment::Sandbox);

        assert!(
            Arc::ptr_eq(production.limiter(), again.limiter()),
            "the same environment resolves the same bucket, however many times it is asked"
        );
        assert!(
            !Arc::ptr_eq(production.limiter(), sandbox.limiter()),
            "two environments must not contend for one bucket"
        );
        assert!(
            !Arc::ptr_eq(production.limiter(), OndoRateBudget::new().limiter()),
            "the shared budget is not the independent one a caller builds for itself"
        );
    }

    #[rstest]
    fn test_clones_share_one_budget_and_independent_budgets_do_not() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        let clone = budget.clone();
        let independent = OndoRateBudget::new();

        assert!(Arc::ptr_eq(budget.limiter(), clone.limiter()));
        assert!(Arc::ptr_eq(&budget.waiters, &clone.waiters));
        assert!(!Arc::ptr_eq(budget.limiter(), independent.limiter()));
        assert!(!Arc::ptr_eq(&budget.waiters, &independent.waiters));
    }

    /// A later periodic read must not repeatedly take the slot an older order already awaits.
    #[tokio::test(start_paused = true)]
    async fn test_waiters_take_replenished_slots_in_arrival_order() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        budget.acquire(OndoRequestPriority::Normal).await;

        for label in ["order", "later-reconciliation"] {
            let budget = budget.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                budget.acquire(OndoRequestPriority::Normal).await;
                tx.send(label).expect("the receiver remains open");
            });
            tokio::task::yield_now().await;
        }

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(rx.try_recv(), Ok("order"));

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(rx.try_recv(), Ok("later-reconciliation"));
    }

    /// A safety close reserves the next shared slot even when a history read is already waiting.
    #[tokio::test(start_paused = true)]
    async fn test_safety_priority_preempts_waiting_history_without_bypassing_quota() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        budget.acquire(OndoRequestPriority::Normal).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for (label, priority) in [
            ("history", OndoRequestPriority::Normal),
            ("close", OndoRequestPriority::High),
        ] {
            let budget = budget.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                budget.acquire(priority).await;
                tx.send(label).unwrap();
            });
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        assert!(rx.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(rx.try_recv(), Ok("close"));
        assert!(
            rx.try_recv().is_err(),
            "priority does not create a second slot"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(rx.try_recv(), Ok("history"));
    }

    /// A cancelled priority request must not consume capacity or strand ordinary traffic.
    #[tokio::test(start_paused = true)]
    async fn test_cancelled_safety_wait_releases_reservation_without_consuming_slot() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        budget.acquire(OndoRequestPriority::Normal).await;
        let high_budget = budget.clone();
        let high = tokio::spawn(async move {
            high_budget.acquire(OndoRequestPriority::High).await;
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(budget.priority_waiters.load(Ordering::Acquire), 1);
        high.abort();
        assert!(high.await.unwrap_err().is_cancelled());
        assert_eq!(budget.priority_waiters.load(Ordering::Acquire), 0);
        let normal_budget = budget.clone();
        let normal = tokio::spawn(async move {
            normal_budget.acquire(OndoRequestPriority::Normal).await;
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!normal.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(normal.is_finished());
        normal.await.unwrap();
    }

    /// An explicit quota is the quota the limiter actually enforces, not a decoration on the
    /// default. On the virtual clock a 1-per-millisecond budget paces five acquisitions in a few
    /// milliseconds, where the 1-per-second default would take four seconds of clock.
    #[tokio::test(start_paused = true)]
    async fn test_an_explicit_quota_is_the_quota_that_is_enforced() {
        let quota = Quota::per_second(NonZeroU32::new(1_000).unwrap()).unwrap();
        let budget = OndoRateBudget::with_quota(quota);
        let started = tokio::time::Instant::now();

        for _ in 0..5 {
            budget.acquire(OndoRequestPriority::Normal).await;
        }

        let elapsed = tokio::time::Instant::now().duration_since(started);
        assert!(
            elapsed < Duration::from_millis(100),
            "a 1-per-millisecond quota paces in milliseconds, got {elapsed:?}",
        );
    }

    /// Two clients, one budget, one request per second. The clock is the runtime's virtual clock,
    /// so the assertion costs no real time.
    #[tokio::test(start_paused = true)]
    async fn test_two_clients_sharing_one_budget_cannot_exceed_one_request_per_second() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        let data_client = client_on(&budget);
        let exec_client = client_on(&budget);
        let done = Arc::new(AtomicUsize::new(0));

        // The first slot is free, and is spent through the data client.
        data_client
            .budget()
            .acquire(OndoRequestPriority::Normal)
            .await;

        for client in [data_client.clone(), exec_client.clone()] {
            let done = Arc::clone(&done);
            tokio::spawn(async move {
                client.budget().acquire(OndoRequestPriority::Normal).await;
                done.fetch_add(1, Ordering::SeqCst);
            });
        }

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            done.load(Ordering::SeqCst),
            0,
            "the only slot in the first second is already spent",
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            done.load(Ordering::SeqCst),
            1,
            "one request per second, however many clients share the budget",
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(done.load(Ordering::SeqCst), 2);
    }

    /// §4.4: priority reserves a slot, it does not bypass the limit.
    #[tokio::test(start_paused = true)]
    async fn test_a_priority_request_draws_on_the_same_budget_instead_of_bypassing_it() {
        let budget = OndoRateBudget::with_quota(*ONDO_REST_QUOTA);
        let done = Arc::new(AtomicUsize::new(0));

        budget.acquire(OndoRequestPriority::Normal).await;

        let priority_budget = budget.clone();
        let priority_done = Arc::clone(&done);
        tokio::spawn(async move {
            priority_budget.acquire(OndoRequestPriority::High).await;
            priority_done.fetch_add(1, Ordering::SeqCst);
        });

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            done.load(Ordering::SeqCst),
            0,
            "priority traffic waits for the shared budget like anything else",
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_has_no_fixed_pacing() {
        let budget = OndoRateBudget::new();
        let start = tokio::time::Instant::now();
        for _ in 0..100 {
            budget.acquire(OndoRequestPriority::Normal).await;
        }
        assert_eq!(tokio::time::Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn test_shared_cooldown_releases_cancels_before_close_and_reads() {
        let budget = OndoRateBudget::new();
        budget.defer(Duration::from_secs(2));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for priority in [
            OndoRequestPriority::Normal,
            OndoRequestPriority::High,
            OndoRequestPriority::Cancel,
        ] {
            let b = budget.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                b.acquire(priority).await;
                tx.send(priority).unwrap();
            });
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        assert!(rx.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(rx.try_recv(), Ok(OndoRequestPriority::Cancel));
        assert_eq!(rx.try_recv(), Ok(OndoRequestPriority::High));
        assert_eq!(rx.try_recv(), Ok(OndoRequestPriority::Normal));
    }
}
