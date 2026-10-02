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

//! Real process ownership and crash-after-commit tests for durable order identity.

use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use nautilus_backpack::identity::{
    BackpackClientIdNamespace, BackpackClientIdStore, BackpackIdentityError,
    BackpackSubmissionIntent,
};
use nautilus_model::identifiers::ClientOrderId;
use rstest::rstest;

fn namespace() -> BackpackClientIdNamespace {
    BackpackClientIdNamespace::new_checked("loopback", "synthetic-process-account", Some("0"))
        .unwrap()
}

fn intent(id: &str) -> BackpackSubmissionIntent {
    BackpackSubmissionIntent::new_checked(
        ClientOrderId::new(id),
        "{\"symbol\":\"BTC_USDC_PERP\"}".to_string(),
    )
    .unwrap()
}

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn worker(directory: &Path, action: &str) -> Worker {
    Worker(
        Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "subprocess_worker", "--nocapture"])
            .env("BACKPACK_ID_TEST_DIR", directory)
            .env("BACKPACK_ID_TEST_ACTION", action)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

fn wait_ready(worker: &mut Worker, directory: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while !directory.join("worker-ready").is_file() {
        assert!(
            worker.0.try_wait().unwrap().is_none(),
            "worker exited before retaining ownership"
        );
        assert!(Instant::now() < deadline, "worker did not become ready");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[rstest]
fn another_process_cannot_allocate_in_an_owned_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
    assert_eq!(store.reserve_intent(intent("parent-owned")).unwrap(), 1);
    let mut probe = worker(directory.path(), "probe-conflict");
    assert!(probe.0.wait().unwrap().success());
    assert_eq!(store.venue_id(&ClientOrderId::new("parent-owned")), Some(1));
    drop(store);
    let restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
    assert_eq!(restored.next_client_id(), 2);
}

#[rstest]
fn killing_an_owner_releases_the_os_lock_without_deleting_the_lock_file() {
    let directory = tempfile::tempdir().unwrap();
    let mut owner = worker(directory.path(), "hold");
    wait_ready(&mut owner, directory.path());
    assert!(matches!(
        BackpackClientIdStore::open(directory.path(), &namespace()),
        Err(BackpackIdentityError::OwnershipConflict)
    ));
    owner.0.kill().unwrap();
    owner.0.wait().unwrap();
    assert!(directory.path().join("identity.lock").is_file());
    let _restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
}

#[rstest]
fn process_death_after_commit_recovers_original_id_without_creating_another_intent() {
    let directory = tempfile::tempdir().unwrap();
    let mut owner = worker(directory.path(), "commit-and-hold");
    wait_ready(&mut owner, directory.path());
    owner.0.kill().unwrap();
    owner.0.wait().unwrap();
    let mut restored = BackpackClientIdStore::open(directory.path(), &namespace()).unwrap();
    let id = ClientOrderId::new("child-committed");
    assert_eq!(restored.venue_id(&id), Some(1));
    assert_eq!(restored.client_order_id(1), Some(id));
    assert_eq!(
        restored.unsigned_intent(&id),
        Some(intent("child-committed").unsigned_payload())
    );
    assert!(matches!(
        restored.reserve_intent(intent("child-committed")),
        Err(BackpackIdentityError::DuplicateIntent)
    ));
    assert_eq!(restored.reserve_intent(intent("later-command")).unwrap(), 2);
}

#[rstest]
#[ignore = "invoked only by the parent process tests with synthetic identities"]
fn subprocess_worker() {
    let directory = std::env::var_os("BACKPACK_ID_TEST_DIR").expect("worker directory");
    let directory = Path::new(&directory);
    let action = std::env::var("BACKPACK_ID_TEST_ACTION").expect("worker action");

    if action == "probe-conflict" {
        assert!(matches!(
            BackpackClientIdStore::open(directory, &namespace()),
            Err(BackpackIdentityError::OwnershipConflict)
        ));
        return;
    }
    let mut store = BackpackClientIdStore::open(directory, &namespace()).unwrap();

    if action == "commit-and-hold" {
        assert_eq!(store.reserve_intent(intent("child-committed")).unwrap(), 1);
    } else {
        assert_eq!(action, "hold");
    }
    fs::write(directory.join("worker-ready"), b"ready").unwrap();

    loop {
        std::thread::park();
    }
}
