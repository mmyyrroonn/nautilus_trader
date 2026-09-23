# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""Shared repository identity capture for the adapter evidence scripts (issue #11)."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

EVIDENCE_SCHEMA_VERSION = 1


def repo_root() -> Path:
    """Returns the repository root, resolved from this file's location."""
    return Path(__file__).resolve().parents[2]


def _git_bytes(root: Path, *args: str) -> bytes:
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, check=False)
    return result.stdout if result.returncode == 0 else b""


def _git(root: Path, *args: str) -> str:
    return _git_bytes(root, *args).decode(errors="replace").strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _untracked_digest(root: Path) -> str:
    """Hashes the paths and contents of every untracked file.

    `git status` alone cannot tell two different untracked sources apart, and an untracked
    source file is exactly the local experiment this tool exists to name.
    """
    digest = hashlib.sha256()
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    for line in sorted(status.splitlines()):
        if not line.startswith("?? "):
            continue
        relative = line[3:].strip()
        digest.update(relative.encode())
        digest.update(b"\0")
        path = root / relative
        if path.is_file():
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def capture_identity(root: Path | None = None) -> dict[str, Any]:
    """Captures the commit, tree and a content identity of every local modification.

    A file status is not a content identity: the same `M source.rs` line names two different
    sources. `tracked_diff_sha256` hashes the binary diff against `HEAD`, and
    `untracked_sha256` hashes untracked paths and contents, so two checkouts that would
    otherwise look identical can be told apart.
    """
    root = root or repo_root()
    lock = root / "Cargo.lock"
    dirty = [line for line in _git(root, "status", "--porcelain").splitlines() if line]
    tracked_diff = _git_bytes(root, "diff", "--binary", "HEAD")
    return {
        "commit": _git(root, "rev-parse", "HEAD"),
        "tree": _git(root, "rev-parse", "HEAD^{tree}"),
        "dirty": dirty,
        "dirty_count": len(dirty),
        "tracked_diff_sha256": hashlib.sha256(tracked_diff).hexdigest(),
        "untracked_sha256": _untracked_digest(root),
        "cargo_lock_sha256": sha256_file(lock) if lock.is_file() else None,
    }


def fingerprint(identity: dict[str, Any]) -> str:
    """Returns a stable digest of the identity fields that name the source content."""
    canonical = {
        "commit": identity.get("commit"),
        "tree": identity.get("tree"),
        "tracked_diff_sha256": identity.get("tracked_diff_sha256"),
        "untracked_sha256": identity.get("untracked_sha256"),
        "cargo_lock_sha256": identity.get("cargo_lock_sha256"),
    }
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
