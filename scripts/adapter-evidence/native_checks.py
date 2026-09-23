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

"""Runs the native adapter checks and writes one evidence manifest (issue #11).

This is the fork's no-secrets entry point for the Aster and Ondo adapters: the same
commands the adapter CI workflow runs, recorded together with the repository identity
they ran against. A claim about an adapter change that is not backed by a manifest from
this script is a claim about a checkout nobody can name.

Every check's stdout and stderr are written to separate log files next to the manifest,
and the manifest carries their hashes and tails. Merging the two streams into one tail
loses whichever stream was not last, which is usually the one carrying the failure.

Blocking checks: ``fmt`` and ``test``. Their failure fails this script. ``clippy`` is
recorded but not blocking while the pre-existing lint debt tracked under #11 is open;
see ``README.md`` for the exact exception.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from _identity import EVIDENCE_SCHEMA_VERSION, capture_identity, fingerprint, repo_root

DEFAULT_CRATES = ("nautilus-aster", "nautilus-ondo")
TAIL_CHARS = 2_000


def _toolchain(root: Path) -> dict[str, Any]:
    def version(args: list[str]) -> str:
        result = subprocess.run(args, cwd=root, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else "unavailable"

    return {
        "rustc": version(["rustc", "-Vv"]),
        "cargo": version(["cargo", "-V"]),
        "os": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
    }


def _tail(text: str) -> str:
    if len(text) <= TAIL_CHARS:
        return text
    return f"...[{len(text) - TAIL_CHARS} chars omitted]...\n" + text[-TAIL_CHARS:]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode(errors="replace")).hexdigest()


def _write_log(logs_dir: Path, name: str, stream: str, text: str) -> dict[str, Any]:
    path = logs_dir / f"{name}.{stream}.log"
    path.write_text(text, encoding="utf-8", errors="replace")
    return {
        "path": str(path),
        "sha256": _sha256(text),
        "bytes": len(text.encode(errors="replace")),
        "tail": _tail(text),
    }


def _run_check(
    root: Path,
    logs_dir: Path,
    name: str,
    command: list[str],
    *,
    blocking: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    duration_ms = int((time.monotonic() - started) * 1_000)

    return {
        "name": name,
        "command": command,
        "blocking": blocking,
        "exit_code": result.returncode,
        "duration_ms": duration_ms,
        "stdout": _write_log(logs_dir, name, "stdout", result.stdout or ""),
        "stderr": _write_log(logs_dir, name, "stderr", result.stderr or ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output",
        default="adapter-native-evidence.json",
        help="where to write the evidence manifest (default: adapter-native-evidence.json)",
    )
    parser.add_argument(
        "--logs",
        help="directory for the per-check stdout/stderr logs (default: <output stem>-logs)",
    )
    parser.add_argument(
        "--crates",
        nargs="+",
        default=list(DEFAULT_CRATES),
        help="cargo package names to check (default: nautilus-aster nautilus-ondo)",
    )
    parser.add_argument(
        "--skip-clippy",
        action="store_true",
        help="do not run the recorded clippy check",
    )
    args = parser.parse_args(argv)

    root = repo_root()
    package_args: list[str] = []
    for crate in args.crates:
        package_args += ["-p", crate]

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    logs_dir = Path(args.logs) if args.logs else output_path.with_name(output_path.stem + "-logs")
    if not logs_dir.is_absolute():
        logs_dir = Path.cwd() / logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)

    before = capture_identity(root)

    checks = [
        _run_check(
            root,
            logs_dir,
            "fmt",
            ["cargo", "fmt", *package_args, "--", "--check"],
            blocking=True,
        ),
        _run_check(
            root,
            logs_dir,
            "test",
            ["cargo", "test", *package_args],
            blocking=True,
        ),
    ]
    if not args.skip_clippy:
        checks.append(
            _run_check(
                root,
                logs_dir,
                "clippy",
                ["cargo", "clippy", *package_args, "--all-targets", "--", "-D", "warnings"],
                blocking=False,
            )
        )

    after = capture_identity(root)
    manifest = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "purpose": "native adapter checks and the checkout they ran against (issue #11)",
        "native": before,
        "identity_before_fingerprint": fingerprint(before),
        "identity_after_fingerprint": fingerprint(after),
        "identity_changed_during_checks": fingerprint(before) != fingerprint(after),
        "toolchain": _toolchain(root),
        "crates": list(args.crates),
        "logs_dir": str(logs_dir),
        "checks": checks,
    }

    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")
    print(f"wrote logs to {logs_dir}")

    failed = [check["name"] for check in checks if check["blocking"] and check["exit_code"] != 0]
    if failed:
        print(f"blocking checks failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    print("blocking checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
