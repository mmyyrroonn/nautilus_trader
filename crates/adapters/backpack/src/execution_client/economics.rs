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

//! Synthetic-only durable consumer snapshots, independent of transport delivery.
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use nautilus_common::{cache::Cache, clients::ExecutionCacheRecovery};
use nautilus_model::{
    accounts::AccountAny,
    events::{OrderEventAny, OrderFilled},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    reports::FillReport,
};
use serde::{Deserialize, Serialize};

use crate::{
    account::reconciliation::{BackpackAppliedFill, BackpackFillKey, BackpackFillReconciler},
    identity::BackpackClientIdNamespace,
};

const JOURNAL: &str = "economics.json";
const MARKER: &str = "economics.initialized";
const MARKER_BYTES: &[u8] = b"backpack-loopback-economics-v1\n";
const MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EconomicFill {
    report: FillReport,
    receipt: BackpackAppliedFill,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EconomicSnapshot {
    symbols: Vec<String>,
    namespace: BackpackClientIdNamespace,
    identity_directory: PathBuf,
    account_id: AccountId,
    trader_id: TraderId,
    client_id: ClientId,
    revision: u64,
    account: Option<AccountAny>,
    instruments: Vec<InstrumentAny>,
    orders: Vec<OrderAny>,
    positions: Vec<Position>,
    fills: Vec<EconomicFill>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema_version: u32,
    checksum: String,
    state: EconomicSnapshot,
}

/// Retains exclusive consumer ownership even across differing identity directories.
#[derive(Debug)]
pub(super) struct EconomicStore {
    directory: PathBuf,
    _lock: File,
    state: EconomicSnapshot,
    poisoned: bool,
}

impl EconomicSnapshot {
    pub(super) fn receipts(&self) -> Vec<BackpackAppliedFill> {
        self.fills.iter().map(|f| f.receipt.clone()).collect()
    }
    pub(super) fn reports(&self) -> Vec<FillReport> {
        self.fills.iter().map(|f| f.report.clone()).collect()
    }
    pub(super) fn reference(&self) -> anyhow::Result<String> {
        checksum(self)
    }
    pub(super) fn verify_cache(&self, cache: &Cache) -> anyhow::Result<()> {
        if let Some(account) = &self.account {
            let restored = cache
                .account(&self.account_id)
                .ok_or_else(|| anyhow::anyhow!("durable account was not restored"))?;
            anyhow::ensure!(
                serde_json::to_value(&*restored)? == serde_json::to_value(account)?,
                "durable account was not restored"
            );
        }
        for order in &self.orders {
            anyhow::ensure!(
                cache.client_id(&order.client_order_id()) == Some(&self.client_id),
                "durable order client routing was not restored"
            );
            let restored = cache
                .order(&order.client_order_id())
                .ok_or_else(|| anyhow::anyhow!("durable order was not restored"))?;
            anyhow::ensure!(
                serde_json::to_value(&*restored)? == serde_json::to_value(order)?,
                "durable order was not restored"
            );
        }
        for position in &self.positions {
            let restored = cache
                .position(&position.id)
                .ok_or_else(|| anyhow::anyhow!("durable position was not restored"))?;
            anyhow::ensure!(
                serde_json::to_value(&*restored)? == serde_json::to_value(position)?,
                "durable position was not restored"
            );
        }
        Ok(())
    }
    pub(super) fn orders(&self) -> &[OrderAny] {
        &self.orders
    }
    pub(super) fn recovery(&self) -> Option<ExecutionCacheRecovery> {
        self.account.as_ref().map(|account| ExecutionCacheRecovery {
            account: account.clone(),
            instruments: self.instruments.clone(),
            orders: self.orders.clone(),
            positions: self.positions.clone(),
        })
    }
    fn validate(&self, capacity: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.fills.len() <= capacity,
            "economic receipt capacity exhausted"
        );
        let mut reconciler = BackpackFillReconciler::from_applied(capacity, [])?;
        for fill in &self.fills {
            reconciler.stage(fill.report.clone())?;
            anyhow::ensure!(
                reconciler.pending_acknowledgement(fill.receipt.key)? == fill.receipt,
                "economic receipt fingerprint mismatch"
            );
            anyhow::ensure!(
                self.consumed(&fill.report)?,
                "durable native consumption missing"
            );
            reconciler.acknowledge_committed(&fill.receipt)?;
        }
        if let Some(account) = &self.account {
            anyhow::ensure!(account.id() == self.account_id, "economic account mismatch");
        } else {
            anyhow::ensure!(
                self.orders.is_empty() && self.positions.is_empty() && self.fills.is_empty(),
                "economic account missing"
            );
        }
        let instrument_ids: std::collections::BTreeSet<_> =
            self.instruments.iter().map(|i| i.id()).collect();
        for instrument in &self.instruments {
            anyhow::ensure!(
                instrument.id().venue == Venue::from("BACKPACK")
                    && self
                        .symbols
                        .iter()
                        .any(|symbol| symbol == instrument.id().symbol.as_str()),
                "economic instrument outside configured scope"
            );
        }
        for order in &self.orders {
            anyhow::ensure!(
                self.symbols
                    .iter()
                    .any(|symbol| symbol == order.instrument_id().symbol.as_str())
                    && instrument_ids.contains(&order.instrument_id()),
                "economic order outside configured scope"
            );
            anyhow::ensure!(
                order.trader_id() == self.trader_id
                    && order.instrument_id().venue == Venue::from("BACKPACK")
                    && order.account_id().is_none_or(|id| id == self.account_id),
                "economic order scope mismatch"
            );
            for event in order.events() {
                if let OrderEventAny::Filled(fill) = event {
                    anyhow::ensure!(
                        self.fills.iter().any(|f| matches_fill(fill, &f.report)),
                        "unreceipted native order economics"
                    );
                }
            }
        }
        for position in &self.positions {
            anyhow::ensure!(
                self.symbols
                    .iter()
                    .any(|symbol| symbol == position.instrument_id.symbol.as_str())
                    && instrument_ids.contains(&position.instrument_id),
                "economic position outside configured scope"
            );
            anyhow::ensure!(
                self.orders
                    .iter()
                    .any(|order| order.client_order_id() == position.opening_order_id)
                    && position.closing_order_id.is_none_or(|id| self
                        .orders
                        .iter()
                        .any(|order| order.client_order_id() == id)),
                "economic position order missing"
            );
            anyhow::ensure!(
                position.trader_id == self.trader_id
                    && position.account_id == self.account_id
                    && position.instrument_id.venue == Venue::from("BACKPACK"),
                "economic position scope mismatch"
            );
            for fill in &position.events {
                anyhow::ensure!(
                    self.fills.iter().any(|f| matches_fill(fill, &f.report)),
                    "unreceipted native position economics"
                );
            }
        }
        Ok(())
    }
    fn consumed(&self, report: &FillReport) -> anyhow::Result<bool> {
        anyhow::ensure!(
            report.account_id == self.account_id,
            "economic report account mismatch"
        );
        let Some(id) = report.client_order_id else {
            return Ok(false);
        };
        let Some(order) = self.orders.iter().find(|o| o.client_order_id() == id) else {
            return Ok(false);
        };
        let order_fill = order.events().into_iter().find_map(|event| match event {
            OrderEventAny::Filled(fill) if fill.trade_id == report.trade_id => Some(fill),
            _ => None,
        });
        let Some(fill) = order_fill else {
            return Ok(false);
        };
        anyhow::ensure!(
            matches_fill(fill, report),
            "native order fill economics conflict"
        );
        let Some(fill) = self
            .positions
            .iter()
            .flat_map(|p| &p.events)
            .find(|f| f.client_order_id == id && f.trade_id == report.trade_id)
        else {
            return Ok(false);
        };
        anyhow::ensure!(
            matches_fill(fill, report),
            "native position fill economics conflict"
        );
        Ok(true)
    }
}
fn matches_fill(fill: &OrderFilled, report: &FillReport) -> bool {
    report.client_order_id == Some(fill.client_order_id)
        && fill.account_id == report.account_id
        && fill.instrument_id == report.instrument_id
        && fill.venue_order_id == report.venue_order_id
        && fill.trade_id == report.trade_id
        && fill.order_side == report.order_side
        && fill.last_qty == report.last_qty
        && fill.last_px == report.last_px
        && fill.commission == Some(report.commission)
        && fill.liquidity_side == report.liquidity_side
        && fill.ts_event == report.ts_event
}
impl EconomicStore {
    #[expect(
        clippy::too_many_arguments,
        reason = "durable consumer scope binds every native identity"
    )]
    pub(super) fn open(
        directory: &Path,
        identity_directory: &Path,
        namespace: &BackpackClientIdNamespace,
        account_id: AccountId,
        trader_id: TraderId,
        capacity: usize,
        symbols: &[String],
        client_id: ClientId,
    ) -> anyhow::Result<Self> {
        let mut symbols = symbols.to_vec();
        symbols.sort();
        reject_symlink(directory)?;
        fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        let identity_directory = identity_directory.canonicalize()?;
        for path in [
            directory.join("economics.lock"),
            directory.join(JOURNAL),
            directory.join(MARKER),
        ] {
            reject_symlink(&path)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("economics.lock"))?;
        lock.try_lock()
            .map_err(|_| anyhow::anyhow!("economic consumer already owned"))?;
        let journal = directory.join(JOURNAL);
        let marker = directory.join(MARKER);
        let state = if journal.exists() {
            anyhow::ensure!(
                fs::metadata(&journal)?.len() <= MAX_BYTES,
                "economic checkpoint too large"
            );
            let envelope: Envelope = serde_json::from_slice(&fs::read(&journal)?)?;
            anyhow::ensure!(
                envelope.schema_version == 1 && envelope.checksum == checksum(&envelope.state)?,
                "economic checkpoint corrupt"
            );
            envelope.state
        } else {
            anyhow::ensure!(
                !marker.exists(),
                "economic checkpoint missing after initialization"
            );
            EconomicSnapshot {
                symbols: symbols.clone(),
                namespace: namespace.clone(),
                identity_directory: identity_directory.clone(),
                account_id,
                trader_id,
                client_id,
                revision: 0,
                account: None,
                instruments: vec![],
                orders: vec![],
                positions: vec![],
                fills: vec![],
            }
        };
        anyhow::ensure!(
            state.symbols == symbols
                && state.namespace == *namespace
                && state.identity_directory == identity_directory
                && state.account_id == account_id
                && state.trader_id == trader_id
                && state.client_id == client_id,
            "economic checkpoint scope mismatch"
        );
        state.validate(capacity)?;
        let store = Self {
            directory,
            _lock: lock,
            state,
            poisoned: false,
        };
        if !journal.exists() {
            store.write(&store.state)?;
        }
        if marker.exists() {
            anyhow::ensure!(fs::read(marker)? == MARKER_BYTES, "economic marker corrupt");
        } else {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(marker)?;
            file.write_all(MARKER_BYTES)?;
            file.sync_all()?;
            sync_directory(&store.directory)?;
        }
        Ok(store)
    }
    pub(super) fn state(&self) -> &EconomicSnapshot {
        &self.state
    }
    pub(super) fn prepare(
        &self,
        cache: &Cache,
        pending: Vec<(FillReport, BackpackAppliedFill)>,
        capacity: usize,
        seed: Option<ClientOrderId>,
    ) -> anyhow::Result<(EconomicSnapshot, Vec<BackpackFillKey>)> {
        anyhow::ensure!(!self.poisoned, "economic storage is poisoned");
        let mut state = self.state.clone();
        let account = cache
            .account(&state.account_id)
            .ok_or_else(|| anyhow::anyhow!("native consumer account missing"))?;
        state.account = Some((*account).clone());
        state.orders = cache
            .orders(Some(&Venue::from("BACKPACK")), None, None, None, None)
            .into_iter()
            .filter(|o| {
                o.trader_id() == state.trader_id
                    && (cache.client_id(&o.client_order_id()) == Some(&state.client_id)
                        || (seed == Some(o.client_order_id())
                            && cache.client_id(&o.client_order_id()).is_none()))
                    && (o.account_id() == Some(state.account_id)
                        || (o.account_id().is_none()
                            && o.status() == nautilus_model::enums::OrderStatus::Initialized))
            })
            .map(|o| (*o).clone())
            .collect();
        // A later local refusal cannot erase the durable original replay seed.
        // Retain that seed conservatively even when no accepted lifecycle was delivered.
        for original in &self.state.orders {
            if original.account_id().is_none()
                && original.status() == nautilus_model::enums::OrderStatus::Initialized
                && !state
                    .orders
                    .iter()
                    .any(|order| order.client_order_id() == original.client_order_id())
            {
                let current = cache
                    .order(&original.client_order_id())
                    .ok_or_else(|| anyhow::anyhow!("durable native seed missing"))?;
                anyhow::ensure!(
                    current.trade_ids().is_empty()
                        && current.venue_order_id().is_none()
                        && cache
                            .client_id(&original.client_order_id())
                            .is_none_or(|id| *id == state.client_id),
                    "durable native seed changed ownership or economics"
                );
                state.orders.push(original.clone());
            }
        }
        state.positions = cache
            .positions(None, None, None, Some(&state.account_id), None)
            .into_iter()
            .map(|p| (*p).clone())
            .collect();
        state.orders.sort_by_key(|order| order.client_order_id());
        state.positions.sort_by_key(|position| position.id);
        let ids: std::collections::BTreeSet<InstrumentId> = state
            .orders
            .iter()
            .map(|o| o.instrument_id())
            .chain(state.positions.iter().map(|p| p.instrument_id))
            .collect();
        state.instruments = ids
            .iter()
            .map(|id| {
                cache
                    .instrument(id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("native consumer instrument missing"))
            })
            .collect::<anyhow::Result<_>>()?;
        let mut records: BTreeMap<_, _> = state
            .fills
            .iter()
            .cloned()
            .map(|f| (f.receipt.key, f))
            .collect();
        let mut keys = vec![];
        for (report, receipt) in pending {
            if state.consumed(&report)? {
                keys.push(receipt.key);
                if let Some(previous) = records.get(&receipt.key) {
                    anyhow::ensure!(previous.receipt == receipt, "economic retry conflict");
                } else {
                    records.insert(receipt.key, EconomicFill { report, receipt });
                }
            }
        }
        state.fills = records.into_values().collect();
        if checksum(&state)? != checksum(&self.state)? {
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("economic revision exhausted"))?;
        }
        state.validate(capacity)?;
        Ok((state, keys))
    }
    pub(super) fn commit(&mut self, state: EconomicSnapshot) -> anyhow::Result<()> {
        anyhow::ensure!(!self.poisoned, "economic storage is poisoned");
        if checksum(&state)? == checksum(&self.state)? {
            return Ok(());
        }
        if let Err(e) = self.write(&state) {
            self.poisoned = true;
            return Err(e);
        }
        self.state = state;
        Ok(())
    }
    pub(super) fn confirm(&self, receipt: &BackpackAppliedFill) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.poisoned && self.state.fills.iter().any(|f| &f.receipt == receipt),
            "economic receipt is not durable"
        );
        Ok(())
    }
    pub(super) fn summary(&self, acknowledged: usize, pending: usize) -> serde_json::Value {
        serde_json::json!({"schema_version":1,"newly_acknowledged_fills":acknowledged,"durable_receipts":self.state.fills.len(),"pending_fills":pending,"checkpoint_revision":self.state.revision})
    }
    fn write(&self, state: &EconomicSnapshot) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(&Envelope {
            schema_version: 1,
            checksum: checksum(state)?,
            state: state.clone(),
        })?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "economic checkpoint too large"
        );
        let mut file = tempfile::Builder::new()
            .prefix(".economics-")
            .suffix(".tmp")
            .tempfile_in(&self.directory)?;
        file.write_all(&bytes)?;
        file.as_file().sync_all()?;
        let file = file.persist(self.directory.join(JOURNAL))?;
        file.sync_all()?;
        sync_directory(&self.directory)
    }
}
fn checksum(state: &EconomicSnapshot) -> anyhow::Result<String> {
    Ok(
        blake3::hash(&serde_json::to_vec(&serde_json::to_value(state)?)?)
            .to_hex()
            .to_string(),
    )
}
fn reject_symlink(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) => anyhow::ensure!(
            !m.file_type().is_symlink(),
            "economic storage symlink refused"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
#[cfg(unix)]
fn sync_directory(directory: &Path) -> anyhow::Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}
#[cfg(not(unix))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "Windows lacks a portable directory barrier; crash recovery excludes power loss"
)]
fn sync_directory(_: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    fn scope() -> (AccountId, TraderId, ClientId, BackpackClientIdNamespace) {
        (
            AccountId::from("BACKPACK-SYNTHETIC"),
            TraderId::from("TRADER-001"),
            ClientId::from("BACKPACK"),
            BackpackClientIdNamespace::new_checked("loopback", "peer-account", None).unwrap(),
        )
    }
    #[rstest]
    fn economic_checkpoint_binds_complete_allowlist_and_client() {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        fs::create_dir(&identity).unwrap();
        let directory = root.path().join("consumer");
        let (account, trader, client, namespace) = scope();
        let symbols = vec!["SOL_USDC_PERP".to_string(), "BTC_USDC_PERP".to_string()];
        let store = EconomicStore::open(
            &directory, &identity, &namespace, account, trader, 100, &symbols, client,
        )
        .unwrap();
        assert!(
            EconomicStore::open(
                &directory, &identity, &namespace, account, trader, 100, &symbols, client
            )
            .is_err()
        );
        drop(store);
        let original = fs::read(directory.join(JOURNAL)).unwrap();
        assert!(
            EconomicStore::open(
                &directory,
                &identity,
                &namespace,
                account,
                trader,
                100,
                &symbols[1..],
                client
            )
            .is_err()
        );
        assert!(
            EconomicStore::open(
                &directory,
                &identity,
                &namespace,
                account,
                trader,
                100,
                &symbols,
                ClientId::from("OTHER-BACKPACK")
            )
            .is_err()
        );
        assert_eq!(fs::read(directory.join(JOURNAL)).unwrap(), original);
        assert!(
            EconomicStore::open(
                &directory, &identity, &namespace, account, trader, 100, &symbols, client
            )
            .is_ok()
        );
    }
    #[rstest]
    #[case("checksum")]
    #[case("marker")]
    #[case("missing")]
    fn initialized_economic_checkpoints_fail_closed(#[case] fault: &str) {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        fs::create_dir(&identity).unwrap();
        let directory = root.path().join("consumer");
        let (account, trader, client, namespace) = scope();
        let symbols = vec!["BTC_USDC_PERP".to_string()];
        drop(
            EconomicStore::open(
                &directory, &identity, &namespace, account, trader, 100, &symbols, client,
            )
            .unwrap(),
        );
        match fault {
            "checksum" => fs::write(
                directory.join(JOURNAL),
                b"{\"schema_version\":1,\"checksum\":\"forged\"}",
            )
            .unwrap(),
            "marker" => fs::write(directory.join(MARKER), b"forged marker").unwrap(),
            "missing" => fs::remove_file(directory.join(JOURNAL)).unwrap(),
            _ => unreachable!(),
        }
        let bytes = fs::read(directory.join(JOURNAL)).ok();
        assert!(
            EconomicStore::open(
                &directory, &identity, &namespace, account, trader, 100, &symbols, client
            )
            .is_err()
        );
        assert_eq!(fs::read(directory.join(JOURNAL)).ok(), bytes);
    }
}
