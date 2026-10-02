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

//! Exclusive, serializable per-run evidence. Transport connection never proves readiness.
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BackpackAccountState {
    Stopped,
    Connecting,
    Degraded,
}
/// Safe run observations; excludes account IDs, private payloads and credentials.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackpackAccountHealth {
    pub schema_version: u32,
    pub run_id: Option<u64>,
    pub generation: u64,
    pub connection_epoch: Option<u64>,
    pub state: BackpackAccountState,
    pub transport_connected: bool,
    pub private_subscription_confirmed: bool,
    pub rest_snapshot_observed: bool,
    pub observed_topics: BTreeSet<String>,
    pub evidence_gaps: BTreeSet<String>,
    pub pending_fills: usize,
    pub recovery_count: u64,
    pub parse_failures: u64,
    pub last_observed_ns: Option<u64>,
}
impl Default for BackpackAccountHealth {
    fn default() -> Self {
        Self {
            schema_version: 1,
            run_id: None,
            generation: 0,
            connection_epoch: None,
            state: BackpackAccountState::Stopped,
            transport_connected: false,
            private_subscription_confirmed: false,
            rest_snapshot_observed: false,
            observed_topics: BTreeSet::new(),
            evidence_gaps: BTreeSet::from([
                "AccountIdentityUnverified".into(),
                "PrivateSubscriptionUnconfirmed".into(),
                "RetentionOrReplicationUnknown".into(),
            ]),
            pending_fills: 0,
            recovery_count: 0,
            parse_failures: 0,
            last_observed_ns: None,
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct Gate {
    pub health: BackpackAccountHealth,
    pub claimed: bool,
    pub running: bool,
    pub revision: u64,
    pub pending_frames: usize,
}
impl Gate {
    pub(crate) fn current(&self, run: u64, generation: u64, epoch: u64) -> bool {
        self.running
            && self.health.run_id == Some(run)
            && self.health.generation == generation
            && self.health.connection_epoch == Some(epoch)
            && self.health.transport_connected
    }
    pub(crate) fn fault(&mut self, reason: &str) {
        self.revision = self.revision.saturating_add(1);
        self.health.rest_snapshot_observed = false;
        self.health.state = BackpackAccountState::Degraded;
        self.health.evidence_gaps.insert(reason.into());
    }
}
#[derive(Clone, Debug, Default)]
pub struct BackpackAccountTelemetry {
    gate: Arc<Mutex<Arc<Mutex<Gate>>>>,
}
impl BackpackAccountTelemetry {
    #[must_use]
    pub fn snapshot(&self) -> BackpackAccountHealth {
        self.gate.lock().lock().health.clone()
    }
    pub(crate) fn claim(&self) -> anyhow::Result<(u64, Arc<Mutex<Gate>>)> {
        let mut holder = self.gate.lock();
        anyhow::ensure!(
            !holder.lock().claimed,
            "account telemetry already has an owner"
        );
        let replacement = Arc::new(Mutex::new(Gate::default()));
        let mut gate = replacement.lock();
        anyhow::ensure!(!gate.claimed, "account telemetry already has an owner");
        let run = NEXT_RUN
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| anyhow::anyhow!("account run ID exhausted"))?;
        gate.claimed = true;
        gate.health = BackpackAccountHealth {
            run_id: Some(run),
            ..Default::default()
        };
        drop(gate);
        *holder = replacement.clone();
        Ok((run, replacement))
    }
    pub(crate) fn release(&self, run: u64) {
        let holder = self.gate.lock();
        let mut g = holder.lock();
        if g.health.run_id == Some(run) {
            g.running = false;
            g.claimed = false;
            g.health.transport_connected = false;
            g.health.state = BackpackAccountState::Stopped;
        }
    }
}
