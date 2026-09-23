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
import subprocess
from pathlib import Path
from typing import Any

EVIDENCE_SCHEMA_VERSION = 1


def repo_root() -> Path:
    """Returns the repository root, resolved from this file's location."""
    return Path(__file__).resolve().parents[2]


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def capture_identity(root: Path | None = None) -> dict[str, Any]:
    """Captures the commit, tree, dirty manifest and lock hash of the checkout."""
    root = root or repo_root()
    lock = root / "Cargo.lock"
    dirty = [line for line in _git(root, "status", "--porcelain").splitlines() if line]
    return {
        "commit": _git(root, "rev-parse", "HEAD"),
        "tree": _git(root, "rev-parse", "HEAD^{tree}"),
        "dirty": dirty,
        "dirty_count": len(dirty),
        "cargo_lock_sha256": sha256_file(lock) if lock.is_file() else None,
    }
