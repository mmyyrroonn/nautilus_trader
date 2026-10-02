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

//! Durable local ownership of Backpack's uint32 client order identifiers.
//!
//! A reservation commits both identity and unsigned submission intent before returning.
//! Reading a restored intent never authorizes retransmission. External venue orders are
//! not adopted. A stable, retained OS file lock excludes other handles and processes.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::{self, File, OpenOptions, TryLockError},
    io::Write,
    path::{Path, PathBuf},
};

use nautilus_model::identifiers::ClientOrderId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const SCHEMA_VERSION: u32 = 1;
const MARKER_CONTENT: &[u8] = b"backpack-client-identity-v1\n";
const JOURNAL_NAME: &str = "identity.json";
const LOCK_NAME: &str = "identity.lock";
const MARKER_NAME: &str = "identity.initialized";
const EXHAUSTED_SEQUENCE: u64 = 1_u64 << u32::BITS;

/// Exact environment, authenticated account and optional subaccount storage scope.
///
/// Values are never normalized or inferred from a filename. The caller supplies the
/// authenticated venue identity, not a transient API key or a display nickname.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpackClientIdNamespace {
    environment: String,
    account: String,
    subaccount: Option<String>,
}

impl BackpackClientIdNamespace {
    /// Constructs an exact namespace without reading credentials or environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, non-ASCII, control-containing or padded components.
    pub fn new_checked(
        environment: &str,
        account: &str,
        subaccount: Option<&str>,
    ) -> Result<Self, BackpackIdentityError> {
        let namespace = Self {
            environment: environment.to_string(),
            account: account.to_string(),
            subaccount: subaccount.map(str::to_string),
        };
        namespace.validate()?;
        Ok(namespace)
    }

    fn validate(&self) -> Result<(), BackpackIdentityError> {
        if !valid_component(&self.environment)
            || !valid_component(&self.account)
            || self
                .subaccount
                .as_deref()
                .is_some_and(|v| !valid_component(v))
        {
            return Err(BackpackIdentityError::InvalidNamespace);
        }
        Ok(())
    }
}

impl Debug for BackpackClientIdNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpackClientIdNamespace")
            .finish_non_exhaustive()
    }
}

/// Immutable creation intent associated with one locally owned Nautilus order.
///
/// The payload is the caller's exact unsigned command encoding, before inserting
/// the allocated venue clientId. It must contain no credentials, authentication
/// headers or signature. Encoding and subsequent wire validation remain the native
/// execution client's responsibility; this store does not parse or send orders.
#[derive(Clone, Eq, PartialEq)]
pub struct BackpackSubmissionIntent {
    client_order_id: ClientOrderId,
    unsigned_payload: String,
}

impl BackpackSubmissionIntent {
    /// Constructs a creation intent without reserving an identifier or sending anything.
    ///
    /// # Errors
    ///
    /// Returns an error for the external-order sentinel or an empty unsigned payload.
    pub fn new_checked(
        client_order_id: ClientOrderId,
        unsigned_payload: String,
    ) -> Result<Self, BackpackIdentityError> {
        if client_order_id.is_external() {
            return Err(BackpackIdentityError::ExternalOrder);
        }

        if unsigned_payload.trim().is_empty() {
            return Err(BackpackIdentityError::InvalidIntent);
        }
        Ok(Self {
            client_order_id,
            unsigned_payload,
        })
    }

    /// Returns the original Nautilus order identifier.
    #[must_use]
    pub const fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }

    /// Returns the original unsigned payload, for reconciliation rather than automatic resend.
    #[must_use]
    pub fn unsigned_payload(&self) -> &str {
        &self.unsigned_payload
    }
}

impl Debug for BackpackSubmissionIntent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpackSubmissionIntent")
            .finish_non_exhaustive()
    }
}

/// Exclusive owner of a durable clientId namespace.
///
/// IDs start at one; zero is conservatively reserved. Every successful reservation
/// is permanently burned, including an intent never sent or an order later canceled.
/// There is no release, adoption or retransmission method.
///
/// The directory must be on a local filesystem with OS file locking and atomic
/// replacement. The execution layer must configure one stable directory for each
/// authenticated namespace throughout its lifetime; choosing a new directory is
/// not recovery and cannot prove that the account has never used clientIds.
/// First use and recovery must separately check venue-side external ID conflicts.
/// Keep it private to this owner: deleting the lock file, rolling back
/// the journal, or deleting all namespace files violates the storage contract.
/// A retained marker detects a missing initialized journal, not deletion of all state.
///
/// Each snapshot is synchronized before replacement and the replacement file is
/// synchronized again. Unix also synchronizes the containing directory. Windows
/// guarantees here cover process termination on a functioning filesystem, not power
/// loss: std does not supply a portable directory durability barrier there. Hardware,
/// filesystem or whole-directory rollback cannot be disproved by a local checksum.
/// Any reported commit failure poisons this handle, even if replacement may have
/// completed. Reopen and reconcile stored intents; never retry creation on a guess.
/// The checksum detects accidental corruption, not malicious authenticated edits.
pub struct BackpackClientIdStore {
    directory: PathBuf,
    // Retained for the owner's lifetime; never clone, unlink, replace or unlock early.
    _owner: File,
    state: JournalState,
    by_order: BTreeMap<ClientOrderId, usize>,
    by_venue: BTreeMap<u32, ClientOrderId>,
    poisoned: bool,
}

impl BackpackClientIdStore {
    /// Opens or initializes a namespace and acquires exclusive process ownership.
    ///
    /// A first initialization writes its permanent marker before the empty checkpoint.
    /// A crash between those steps refuses subsequent open rather than resetting IDs.
    /// No temporary file is used as evidence or guessed to be a recoverable checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for competing ownership, missing initialized state, incomplete
    /// initialization, corrupt or unsupported state, scope mismatch or any file error.
    pub fn open(
        directory: &Path,
        namespace: &BackpackClientIdNamespace,
    ) -> Result<Self, BackpackIdentityError> {
        namespace.validate()?;
        fs::create_dir_all(directory).map_err(|e| io_error("create namespace directory", e))?;
        let directory =
            fs::canonicalize(directory).map_err(|e| io_error("resolve namespace directory", e))?;
        let lock_path = directory.join(LOCK_NAME);
        reject_symlink(&lock_path)?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| io_error("open ownership lock", e))?;

        match owner.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(BackpackIdentityError::OwnershipConflict),
            Err(TryLockError::Error(e)) => return Err(io_error("acquire ownership lock", e)),
        }

        let journal = directory.join(JOURNAL_NAME);
        let marker = directory.join(MARKER_NAME);
        reject_symlink(&journal)?;
        reject_symlink(&marker)?;
        let marker_exists = marker
            .try_exists()
            .map_err(|e| io_error("inspect initialization marker", e))?;
        let journal_exists = journal
            .try_exists()
            .map_err(|e| io_error("inspect checkpoint", e))?;
        let state = match (marker_exists, journal_exists) {
            (true, true) => {
                verify_marker(&directory)?;
                read_checkpoint(&directory)?
            }
            (true, false) => return Err(BackpackIdentityError::MissingJournal),
            (false, true) => return Err(BackpackIdentityError::IncompleteInitialization),
            (false, false) => {
                let mut marker = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(marker)
                    .map_err(|e| io_error("create initialization marker", e))?;
                marker
                    .write_all(MARKER_CONTENT)
                    .map_err(|e| io_error("write initialization marker", e))?;
                marker
                    .sync_all()
                    .map_err(|e| io_error("sync initialization marker", e))?;
                sync_directory(&directory)?;
                let state = JournalState {
                    namespace: namespace.clone(),
                    next_client_id: 1,
                    intents: Vec::new(),
                };
                write_checkpoint(&directory, &state)?;
                state
            }
        };

        if state.namespace != *namespace {
            return Err(BackpackIdentityError::NamespaceMismatch);
        }
        let (by_order, by_venue) = validate_state(&state)?;
        Ok(Self {
            directory,
            _owner: owner,
            state,
            by_order,
            by_venue,
            poisoned: false,
        })
    }

    /// Atomically commits a new mapping and creation intent before returning its clientId.
    ///
    /// Repeating an intent is refused, including after restart or cancellation. The
    /// returned integer is identity evidence, not authority to send an order. Native
    /// execution must still check its current session, risk and endpoint admission.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate or external identity, exhausted uint32 range,
    /// a poisoned owner, changed storage, or any checkpoint/synchronization failure.
    /// A storage failure permanently closes this handle to further reservations.
    pub fn reserve_intent(
        &mut self,
        intent: BackpackSubmissionIntent,
    ) -> Result<u32, BackpackIdentityError> {
        if self.poisoned {
            return Err(BackpackIdentityError::Unavailable);
        }

        if self.by_order.contains_key(&intent.client_order_id) {
            return Err(BackpackIdentityError::DuplicateIntent);
        }
        let client_id = u32::try_from(self.state.next_client_id)
            .map_err(|_| BackpackIdentityError::Exhausted)?;
        let mut next = self.state.clone();
        next.next_client_id += 1;
        next.intents.push(StoredIntent {
            client_order_id: intent.client_order_id.to_string(),
            client_id,
            unsigned_payload: intent.unsigned_payload,
        });
        let commit = || {
            verify_marker(&self.directory)?;
            let current = read_checkpoint(&self.directory)?;

            if current != self.state {
                return Err(BackpackIdentityError::ChangedCheckpoint);
            }
            write_checkpoint(&self.directory, &next)
        };

        if let Err(e) = commit() {
            self.poisoned = true;
            return Err(e);
        }
        let index = self.state.intents.len();
        self.state = next;
        self.by_order.insert(intent.client_order_id, index);
        self.by_venue.insert(client_id, intent.client_order_id);
        Ok(client_id)
    }

    /// Looks up locally persisted identity; this does not reserve or authorize retransmission.
    #[must_use]
    pub fn venue_id(&self, client_order_id: &ClientOrderId) -> Option<u32> {
        self.by_order
            .get(client_order_id)
            .map(|i| self.state.intents[*i].client_id)
    }

    /// Resolves only locally persisted venue identifiers, leaving external orders unowned.
    #[must_use]
    pub fn client_order_id(&self, client_id: u32) -> Option<ClientOrderId> {
        self.by_venue.get(&client_id).copied()
    }

    /// Reads the original unsigned intent for recovery, without authorizing a resend.
    #[must_use]
    pub fn unsigned_intent(&self, client_order_id: &ClientOrderId) -> Option<&str> {
        self.by_order
            .get(client_order_id)
            .map(|i| self.state.intents[*i].unsigned_payload.as_str())
    }

    /// Enumerates every permanently reserved local identity for recovery.
    ///
    /// These include never-sent, unknown, canceled and settled orders. Presence
    /// proves only a durable local intent; it does not prove venue acceptance or
    /// authorize another transmission. Resolve each original unsigned payload
    /// through `unsigned_intent` and use venue facts to reconcile its outcome.
    pub fn reserved_ids(&self) -> impl Iterator<Item = (ClientOrderId, u32)> + '_ {
        self.by_venue
            .iter()
            .map(|(venue_id, order_id)| (*order_id, *venue_id))
    }
    /// Returns the monotonic high-water mark; u32::MAX + 1 means exhaustion.
    #[must_use]
    pub const fn next_client_id(&self) -> u64 {
        self.state.next_client_id
    }
}

impl Debug for BackpackClientIdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpackClientIdStore")
            .field("reserved_ids", &self.by_order.len())
            .field("next_client_id", &self.state.next_client_id)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

/// A refusal to claim durable order identity.
#[derive(Debug, Error)]
pub enum BackpackIdentityError {
    /// Invalid exact namespace components.
    #[error("invalid Backpack clientId namespace")]
    InvalidNamespace,
    /// Empty creation intent.
    #[error("unsigned Backpack submission intent is empty")]
    InvalidIntent,
    /// External-order sentinel cannot become a local owned order.
    #[error("external orders cannot reserve Backpack clientId ownership")]
    ExternalOrder,
    /// Another handle/process retains the namespace lock.
    #[error("Backpack clientId namespace is already owned")]
    OwnershipConflict,
    /// Restored scope differs from the requested authenticated identity.
    #[error("Backpack clientId namespace does not match stored identity")]
    NamespaceMismatch,
    /// An initialized namespace has lost its authoritative checkpoint.
    #[error("initialized Backpack clientId namespace has no checkpoint")]
    MissingJournal,
    /// A journal exists without initialization proof.
    #[error("Backpack clientId namespace initialization is incomplete")]
    IncompleteInitialization,
    /// Unsupported persistent schema.
    #[error("unsupported Backpack clientId journal schema")]
    UnsupportedSchema,
    /// Malformed JSON, checksum, mapping or high-water invariant.
    #[error("corrupt Backpack clientId journal or initialization marker")]
    CorruptJournal,
    /// Local state no longer agrees with the authoritative checkpoint.
    #[error("Backpack clientId checkpoint changed outside its owner")]
    ChangedCheckpoint,
    /// No uint32 values remain.
    #[error("Backpack uint32 clientId range is exhausted")]
    Exhausted,
    /// One creation intent already permanently owns this Nautilus ID.
    #[error("Backpack creation intent already exists; reconcile its original clientId")]
    DuplicateIntent,
    /// A previous storage error makes the current owner's commit state uncertain.
    #[error("Backpack clientId owner is unavailable after a storage failure")]
    Unavailable,
    /// A file operation failed; no identity commit is reported successful.
    #[error("Backpack clientId storage operation failed: {operation}")]
    Io {
        /// Bounded operation label, never an account, order or payload.
        operation: &'static str,
        /// Underlying operating-system failure.
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalState {
    namespace: BackpackClientIdNamespace,
    next_client_id: u64,
    intents: Vec<StoredIntent>,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIntent {
    client_order_id: String,
    client_id: u32,
    unsigned_payload: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalEnvelope {
    schema_version: u32,
    checksum: String,
    state: JournalState,
}

type IdentityIndexes = (BTreeMap<ClientOrderId, usize>, BTreeMap<u32, ClientOrderId>);

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && value.trim() == value
        && !value.bytes().any(|b| b.is_ascii_control())
}

fn validate_state(state: &JournalState) -> Result<IdentityIndexes, BackpackIdentityError> {
    state
        .namespace
        .validate()
        .map_err(|_| BackpackIdentityError::CorruptJournal)?;

    if !(1..=EXHAUSTED_SEQUENCE).contains(&state.next_client_id) {
        return Err(BackpackIdentityError::CorruptJournal);
    }
    let mut by_order = BTreeMap::new();
    let mut by_venue = BTreeMap::new();

    for (index, intent) in state.intents.iter().enumerate() {
        let id = ClientOrderId::new_checked(&intent.client_order_id)
            .map_err(|_| BackpackIdentityError::CorruptJournal)?;

        if id.is_external()
            || intent.unsigned_payload.trim().is_empty()
            || intent.client_id == 0
            || u64::from(intent.client_id) >= state.next_client_id
            || by_order.insert(id, index).is_some()
            || by_venue.insert(intent.client_id, id).is_some()
        {
            return Err(BackpackIdentityError::CorruptJournal);
        }
    }
    Ok((by_order, by_venue))
}

fn checksum(state: &JournalState) -> Result<String, BackpackIdentityError> {
    let bytes = serde_json::to_vec(state).map_err(|_| BackpackIdentityError::CorruptJournal)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn read_checkpoint(directory: &Path) -> Result<JournalState, BackpackIdentityError> {
    let journal = directory.join(JOURNAL_NAME);
    reject_symlink(&journal)?;
    let bytes = match fs::read(journal) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(BackpackIdentityError::MissingJournal);
        }
        Err(e) => return Err(io_error("read checkpoint", e)),
    };
    let envelope: JournalEnvelope =
        serde_json::from_slice(&bytes).map_err(|_| BackpackIdentityError::CorruptJournal)?;

    if envelope.schema_version != SCHEMA_VERSION {
        return Err(BackpackIdentityError::UnsupportedSchema);
    }

    if envelope.checksum != checksum(&envelope.state)? {
        return Err(BackpackIdentityError::CorruptJournal);
    }
    validate_state(&envelope.state)?;
    Ok(envelope.state)
}

fn write_checkpoint(directory: &Path, state: &JournalState) -> Result<(), BackpackIdentityError> {
    let envelope = JournalEnvelope {
        schema_version: SCHEMA_VERSION,
        checksum: checksum(state)?,
        state: state.clone(),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| BackpackIdentityError::CorruptJournal)?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".identity-")
        .suffix(".tmp")
        .tempfile_in(directory)
        .map_err(|e| io_error("create checkpoint temporary", e))?;
    temporary
        .write_all(&bytes)
        .map_err(|e| io_error("write checkpoint temporary", e))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|e| io_error("sync checkpoint temporary", e))?;
    let file = temporary
        .persist(directory.join(JOURNAL_NAME))
        .map_err(|e| io_error("replace checkpoint", e.error))?;
    file.sync_all()
        .map_err(|e| io_error("sync replaced checkpoint", e))?;
    sync_directory(directory)
}

fn verify_marker(directory: &Path) -> Result<(), BackpackIdentityError> {
    let marker = directory.join(MARKER_NAME);
    reject_symlink(&marker)?;
    let bytes = fs::read(marker).map_err(|e| io_error("read initialization marker", e))?;

    if bytes != MARKER_CONTENT {
        return Err(BackpackIdentityError::CorruptJournal);
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), BackpackIdentityError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(BackpackIdentityError::CorruptJournal)
        }
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_error("inspect storage entry", e)),
    }
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<(), BackpackIdentityError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|e| io_error("sync namespace directory", e))
}

#[cfg(not(unix))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "shared platform signature propagates Unix directory synchronization failures"
)]
fn sync_directory(_directory: &Path) -> Result<(), BackpackIdentityError> {
    // Directory durability has no portable std barrier on Windows; the public contract
    // explicitly excludes power loss there. Every available file barrier remains required.
    Ok(())
}

fn io_error(operation: &'static str, source: std::io::Error) -> BackpackIdentityError {
    BackpackIdentityError::Io { operation, source }
}
#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn namespace() -> BackpackClientIdNamespace {
        BackpackClientIdNamespace::new_checked("loopback", "synthetic-account", Some("0")).unwrap()
    }

    fn intent(id: &str) -> BackpackSubmissionIntent {
        BackpackSubmissionIntent::new_checked(
            ClientOrderId::new(id),
            "{\"symbol\":\"BTC_USDC_PERP\",\"quantity\":\"0.1\"}".to_string(),
        )
        .unwrap()
    }

    fn fixture(directory: &Path, state: &JournalState) {
        fs::write(directory.join(MARKER_NAME), MARKER_CONTENT).unwrap();
        write_checkpoint(directory, state).unwrap();
    }

    #[rstest]
    #[case("", "account", None)]
    #[case(" loopback", "account", None)]
    #[case("loopback", "account ", None)]
    #[case("loopback", "account", Some(""))]
    #[case("loopback", "account", Some("0\n1"))]
    #[case("loopback", "a\u{e9}", None)]
    fn invalid_namespaces_are_refused(
        #[case] environment: &str,
        #[case] account: &str,
        #[case] subaccount: Option<&str>,
    ) {
        assert!(matches!(
            BackpackClientIdNamespace::new_checked(environment, account, subaccount),
            Err(BackpackIdentityError::InvalidNamespace)
        ));
    }

    #[rstest]
    fn committed_intents_survive_restart_and_never_release_their_ids() {
        let directory = tempfile::tempdir().unwrap();
        let id = ClientOrderId::new("owned-1");
        let original = intent("owned-1");
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(store.reserve_intent(original.clone()).unwrap(), 1);
        assert_eq!(store.venue_id(&id), Some(1));
        assert_eq!(store.client_order_id(1), Some(id));
        assert_eq!(
            store.unsigned_intent(&id),
            Some(original.unsigned_payload())
        );
        drop(store);
        let mut restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(restored.venue_id(&id), Some(1));
        assert_eq!(restored.client_order_id(1), Some(id));
        assert_eq!(
            restored.unsigned_intent(&id),
            Some(original.unsigned_payload())
        );
        assert!(matches!(
            restored.reserve_intent(original),
            Err(BackpackIdentityError::DuplicateIntent)
        ));
        assert_eq!(restored.reserve_intent(intent("owned-2")).unwrap(), 2);
        assert_eq!(restored.client_order_id(0), None);
        assert_eq!(restored.client_order_id(1000), None);
        assert_eq!(restored.venue_id(&ClientOrderId::external()), None);
    }

    #[rstest]
    fn multiple_handles_contend_and_drop_releases_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let owner = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::OwnershipConflict)
        ));
        drop(owner);
        let _next_owner = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert!(directory.path().join(LOCK_NAME).is_file());
    }

    #[rstest]
    #[case("production", "synthetic-account", Some("0"))]
    #[case("loopback", "other-account", Some("0"))]
    #[case("loopback", "synthetic-account", Some("1"))]
    #[case("loopback", "synthetic-account", None)]
    fn scope_mismatch_never_restores_or_resets(
        #[case] environment: &str,
        #[case] account: &str,
        #[case] subaccount: Option<&str>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        store.reserve_intent(intent("owned")).unwrap();
        drop(store);
        let scope =
            BackpackClientIdNamespace::new_checked(environment, account, subaccount).unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &scope),
            Err(BackpackIdentityError::NamespaceMismatch)
        ));
        let restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(restored.venue_id(&ClientOrderId::new("owned")), Some(1));
    }

    #[rstest]
    fn independent_namespaces_keep_independent_mapping() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let mut first = BackpackClientIdStore::open(a.path(), &namespace()).unwrap();
        let mut second = BackpackClientIdStore::open(
            b.path(),
            &BackpackClientIdNamespace::new_checked("loopback", "other-account", None).unwrap(),
        )
        .unwrap();
        assert_eq!(first.reserve_intent(intent("first")).unwrap(), 1);
        assert_eq!(second.reserve_intent(intent("second")).unwrap(), 1);
        assert_eq!(first.venue_id(&ClientOrderId::new("second")), None);
        assert_eq!(second.venue_id(&ClientOrderId::new("first")), None);
    }

    #[rstest]
    #[case(0)]
    #[case(EXHAUSTED_SEQUENCE + 1)]
    fn invalid_high_water_is_refused(#[case] next: u64) {
        let directory = tempfile::tempdir().unwrap();
        fixture(
            directory.path(),
            &JournalState {
                namespace: namespace(),
                next_client_id: next,
                intents: Vec::new(),
            },
        );
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::CorruptJournal)
        ));
    }

    #[rstest]
    fn last_uint32_is_committed_once_then_exhaustion_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        // A synthetic high-water fixture represents already burned lower IDs without
        // allocating billions of records. No production seeding/recovery bypass exists.
        fixture(
            directory.path(),
            &JournalState {
                namespace: namespace(),
                next_client_id: u64::from(u32::MAX) - 1,
                intents: Vec::new(),
            },
        );
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(
            store.reserve_intent(intent("penultimate")).unwrap(),
            u32::MAX - 1
        );
        assert_eq!(store.reserve_intent(intent("last")).unwrap(), u32::MAX);
        assert_eq!(store.next_client_id(), EXHAUSTED_SEQUENCE);
        assert!(matches!(
            store.reserve_intent(intent("overflow")),
            Err(BackpackIdentityError::Exhausted)
        ));
        drop(store);
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(
            store.client_order_id(u32::MAX),
            Some(ClientOrderId::new("last"))
        );
        assert!(matches!(
            store.reserve_intent(intent("overflow")),
            Err(BackpackIdentityError::Exhausted)
        ));
    }

    #[rstest]
    #[case("duplicate-order")]
    #[case("duplicate-venue")]
    #[case("zero")]
    #[case("above-high-water")]
    #[case("external")]
    #[case("empty-intent")]
    fn invalid_mapping_is_refused_even_with_a_valid_checksum(#[case] corruption: &str) {
        let directory = tempfile::tempdir().unwrap();
        let entry = StoredIntent {
            client_order_id: "owned".to_string(),
            client_id: 1,
            unsigned_payload: "{}".to_string(),
        };
        let mut state = JournalState {
            namespace: namespace(),
            next_client_id: 3,
            intents: vec![entry.clone()],
        };

        match corruption {
            "duplicate-order" => state.intents.push(StoredIntent {
                client_id: 2,
                ..entry
            }),
            "duplicate-venue" => state.intents.push(StoredIntent {
                client_order_id: "other".to_string(),
                ..entry
            }),
            "zero" => state.intents[0].client_id = 0,
            "above-high-water" => state.intents[0].client_id = 3,
            "external" => state.intents[0].client_order_id = "EXTERNAL".to_string(),
            "empty-intent" => state.intents[0].unsigned_payload = " ".to_string(),
            _ => unreachable!(),
        }
        fixture(directory.path(), &state);
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::CorruptJournal)
        ));
    }

    #[rstest]
    #[case("truncated")]
    #[case("checksum")]
    #[case("unknown-field")]
    #[case("unsupported-schema")]
    fn malformed_checkpoints_are_never_reinitialized(#[case] corruption: &str) {
        let directory = tempfile::tempdir().unwrap();
        let store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        drop(store);
        let path = directory.path().join(JOURNAL_NAME);
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        match corruption {
            "truncated" => {
                fs::write(&path, b"{\"schema_version\":1,").unwrap();
            }
            "checksum" => {
                envelope["state"]["next_client_id"] = 100.into();
                fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            }
            "unknown-field" => {
                envelope["unexpected"] = true.into();
                fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            }
            "unsupported-schema" => {
                envelope["schema_version"] = 2.into();
                fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            }
            _ => unreachable!(),
        }
        let result = BackpackClientIdStore::open(directory.path(), &namespace());
        assert!(matches!(
            result,
            Err(BackpackIdentityError::CorruptJournal | BackpackIdentityError::UnsupportedSchema)
        ));
    }

    #[rstest]
    fn initialized_missing_checkpoint_and_interrupted_initialization_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join(MARKER_NAME), MARKER_CONTENT).unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::MissingJournal)
        ));
        let missing = tempfile::tempdir().unwrap();
        let store = BackpackClientIdStore::open(missing.path(), &namespace()).unwrap();
        drop(store);
        fs::remove_file(missing.path().join(JOURNAL_NAME)).unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(missing.path(), &namespace()),
            Err(BackpackIdentityError::MissingJournal)
        ));
    }

    #[rstest]
    fn missing_or_corrupt_marker_never_recreates_initialization() {
        let directory = tempfile::tempdir().unwrap();
        let store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        drop(store);
        fs::remove_file(directory.path().join(MARKER_NAME)).unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::IncompleteInitialization)
        ));
        fs::write(directory.path().join(MARKER_NAME), b"truncated").unwrap();
        assert!(matches!(
            BackpackClientIdStore::open(directory.path(), &namespace()),
            Err(BackpackIdentityError::CorruptJournal)
        ));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn orphan_pre_replace_temporary_is_never_adopted(#[case] complete: bool) {
        let directory = tempfile::tempdir().unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        store.reserve_intent(intent("committed")).unwrap();
        let mut newer = store.state.clone();
        newer.next_client_id = 3;
        newer.intents.push(StoredIntent {
            client_order_id: "uncommitted".to_string(),
            client_id: 2,
            unsigned_payload: "{}".to_string(),
        });
        let envelope = JournalEnvelope {
            schema_version: SCHEMA_VERSION,
            checksum: checksum(&newer).unwrap(),
            state: newer,
        };
        let bytes = if complete {
            serde_json::to_vec(&envelope).unwrap()
        } else {
            b"{\"schema_version\":".to_vec()
        };
        fs::write(directory.path().join(".identity-crash.tmp"), bytes).unwrap();
        drop(store);
        let mut restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(restored.venue_id(&ClientOrderId::new("committed")), Some(1));
        assert_eq!(restored.venue_id(&ClientOrderId::new("uncommitted")), None);
        assert_eq!(restored.reserve_intent(intent("next")).unwrap(), 2);
    }

    #[rstest]
    fn a_valid_but_changed_checkpoint_cannot_be_overwritten_by_stale_memory() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        store.reserve_intent(intent("original")).unwrap();
        let mut newer = store.state.clone();
        newer.next_client_id = 3;
        newer.intents.push(StoredIntent {
            client_order_id: "outside-write".to_string(),
            client_id: 2,
            unsigned_payload: "{}".to_string(),
        });
        write_checkpoint(directory.path(), &newer).unwrap();
        assert!(matches!(
            store.reserve_intent(intent("stale")),
            Err(BackpackIdentityError::ChangedCheckpoint)
        ));
        assert!(matches!(
            store.reserve_intent(intent("another")),
            Err(BackpackIdentityError::Unavailable)
        ));
        drop(store);
        let restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(
            restored.reserved_ids().collect::<Vec<_>>(),
            vec![
                (ClientOrderId::new("original"), 1),
                (ClientOrderId::new("outside-write"), 2)
            ]
        );
        assert_eq!(restored.venue_id(&ClientOrderId::new("stale")), None);
        assert_eq!(restored.next_client_id(), 3);
    }
    #[rstest]
    fn storage_failure_poisoning_prevents_a_second_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        store.reserve_intent(intent("committed")).unwrap();
        fs::remove_file(directory.path().join(JOURNAL_NAME)).unwrap();
        assert!(matches!(
            store.reserve_intent(intent("not-committed")),
            Err(BackpackIdentityError::MissingJournal)
        ));
        assert_eq!(store.venue_id(&ClientOrderId::new("not-committed")), None);
        assert_eq!(store.next_client_id(), 2);
        assert!(matches!(
            store.reserve_intent(intent("another")),
            Err(BackpackIdentityError::Unavailable)
        ));
        assert_eq!(store.venue_id(&ClientOrderId::new("committed")), Some(1));
    }

    #[rstest]
    fn external_identity_empty_payload_and_private_debug_are_refused_or_hidden() {
        assert!(matches!(
            BackpackSubmissionIntent::new_checked(ClientOrderId::external(), "{}".to_string()),
            Err(BackpackIdentityError::ExternalOrder)
        ));
        assert!(matches!(
            BackpackSubmissionIntent::new_checked(ClientOrderId::new("owned"), " \n".to_string()),
            Err(BackpackIdentityError::InvalidIntent)
        ));
        assert!(!format!("{:?}", namespace()).contains("synthetic-account"));
        assert!(!format!("{:?}", intent("private-order")).contains("private-order"));
    }

    #[cfg(windows)]
    #[rstest]
    fn failed_windows_replacement_returns_no_id_and_poisons_the_owner() {
        use std::os::windows::fs::OpenOptionsExt;

        let directory = tempfile::tempdir().unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        // Permit reads while denying delete/replace, using a real OS failure instead
        // of a test-only persistence hook in the production allocator.
        let blocker = OpenOptions::new()
            .read(true)
            .share_mode(1)
            .open(directory.path().join(JOURNAL_NAME))
            .unwrap();
        assert!(matches!(
            store.reserve_intent(intent("blocked")),
            Err(BackpackIdentityError::Io {
                operation: "replace checkpoint",
                ..
            })
        ));
        assert_eq!(store.venue_id(&ClientOrderId::new("blocked")), None);
        assert_eq!(store.next_client_id(), 1);
        drop(blocker);
        assert!(matches!(
            store.reserve_intent(intent("second")),
            Err(BackpackIdentityError::Unavailable)
        ));
        drop(store);
        let restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
        assert_eq!(restored.venue_id(&ClientOrderId::new("blocked")), None);
        assert_eq!(restored.next_client_id(), 1);
    }
}
