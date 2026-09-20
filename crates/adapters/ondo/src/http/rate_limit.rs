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

//! The shared REST request budget for one Ondo Perps adapter environment.
//!
//! Plan §4.4 sets the REST limit conservatively at **one request per second per adapter per
//! environment**. That is deliberately *one budget*, not one per endpoint and not one per client:
//! [`OndoRateBudget`] is injectable, and every client that talks to the same environment is built
//! from a clone of the same instance, so a data client and an execution client in one process draw
//! on the same tokens.
//!
//! # How two clients end up on one budget
//!
//! A factory is constructed before any configuration reaches it - `OndoDataClientFactory()` in
//! Python, then `create(name, config, ..)` once the node has one - and the environment is a member
//! of *that configuration*, not of the factory. So the shared instance is resolved at `create`:
//! [`shared_rest_budget`] answers the process-wide budget of the environment a configuration names,
//! which is what makes the data and execution clients of one environment contend for one bucket
//! without either surface having to be wired to the other. The registry is keyed by the environment
//! itself, so a production client and a sandbox client in one process never share a bucket - and
//! [`OndoRateBudget::new`] is still the way a test or a Rust embedder takes an independent one.
//!
//! # Priority
//!
//! §4.4 also requires cancels, unknown-order queries and reconciliation to take precedence over
//! ordinary metadata and history reads *within the same budget*, by reserving a request slot rather
//! than bypassing the limit. [`OndoRequestPriority`] is that hook: [`OndoRateBudget::acquire`]
//! takes it, so every caller already names the class of traffic it is. Both classes await the same
//! single bucket today, which is what makes "a priority request can never exceed the budget" true
//! now. A FIFO turnstile prevents a later periodic read from starving an older waiter; it is not
//! the reserved-slot mechanism. That later mechanism still attaches inside `acquire`, where a
//! `High` request can take the next reserved slot without bypassing the shared limit.

use std::{
    num::NonZeroU32,
    sync::{Arc, LazyLock, Mutex, PoisonError},
};

use ahash::AHashMap;
use nautilus_network::ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota};
use tokio::sync::Mutex as AsyncMutex;
use ustr::Ustr;

use crate::common::enums::OndoEnvironment;

/// The single rate-limit bucket every Ondo Perps REST request draws on.
///
/// One bucket for the whole adapter is the point: a per-endpoint limiter would defeat the shared
/// budget §4.4 asks for.
pub const ONDO_REST_BUCKET: &str = "ondo:rest";

/// The conservative REST budget: one request per second for the whole adapter (§4.4).
pub const ONDO_REST_REQUESTS_PER_SECOND: u32 = 1;

/// The default REST quota: one request per second, with a single-request burst.
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
    /// Cancel, unknown-order query and reconciliation traffic (§4.4).
    ///
    /// It draws on the same budget as [`Self::Normal`]; the reserved-slot mechanism that lets it
    /// take the next slot ahead of ordinary reads attaches in [`OndoRateBudget::acquire`].
    High,
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
}

impl Default for OndoRateBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl OndoRateBudget {
    /// Creates an independent budget with the default one-request-per-second quota.
    #[must_use]
    pub fn new() -> Self {
        Self::with_quota(*ONDO_REST_QUOTA)
    }

    /// Creates an independent budget with an explicit quota.
    ///
    /// The plan's one request per second is a conservative *default*, not a venue constant: it is a
    /// historical value P0 recorded for re-checking against the live host. Nothing else about the
    /// budget changes - it is still one bucket every request draws on, whatever the quota is.
    #[must_use]
    pub fn with_quota(quota: Quota) -> Self {
        Self {
            limiter: Arc::new(RateLimiter::new_with_quota(
                None,
                vec![(Ustr::from(ONDO_REST_BUCKET), quota)],
            )),
            waiters: Arc::new(AsyncMutex::new(())),
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

    /// Awaits one slot in the shared budget, for traffic of the given priority.
    ///
    /// A request that cannot be admitted waits here rather than being sent: the budget reserves a
    /// slot, it never bypasses the limit. The wait is on the caller's clock, so a client parks in
    /// its own task and does not sleep mid-loop.
    pub async fn acquire(&self, priority: OndoRequestPriority) {
        log::trace!("Awaiting the shared Ondo REST budget for {priority:?} traffic");
        // Tokio's async mutex grants locks in call order. Hold only the wait for a rate-limit
        // slot, never the network request, so an older waiter cannot be overtaken while the
        // request itself still releases the turn immediately after admission.
        let _turn = self.waiters.lock().await;
        self.limiter
            .await_keys_ready(Some(&[Ustr::from(ONDO_REST_BUCKET)]))
            .await;
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
    fn test_the_default_budget_is_one_request_per_second() {
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
        let budget = OndoRateBudget::new();
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
        let budget = OndoRateBudget::new();
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
        let budget = OndoRateBudget::new();
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
        let budget = OndoRateBudget::new();
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
}
