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

Blocking checks: ``fmt`` and ``test``. Their failure fails this script. ``clippy`` is
recorded but not blocking while the pre-existing lint debt tracked under #11 is open;
see ``README.md`` for the exact exception.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from _identity import EVIDENCE_SCHEMA_VERSION, capture_identity, repo_root

DEFAULT_CRATES = ("nautilus-aster", "nautilus-ondo")
OUTPUT_TAIL_CHARS = 4_000


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
    if len(text) <= OUTPUT_TAIL_CHARS:
        return text
    return f"...[{len(text) - OUTPUT_TAIL_CHARS} chars omitted]...\n" + text[-OUTPUT_TAIL_CHARS:]


def _run_check(
    root: Path,
    name: str,
    command: list[str],
    *,
    blocking: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    duration_ms = int((time.monotonic() - started) * 1_000)
    output = (result.stdout or "") + (result.stderr or "")

    return {
        "name": name,
        "command": command,
        "blocking": blocking,
        "exit_code": result.returncode,
        "duration_ms": duration_ms,
        "output_tail": _tail(output),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output",
        default="adapter-native-evidence.json",
        help="where to write the evidence manifest (default: adapter-native-evidence.json)",
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

    checks = [
        _run_check(
            root,
            "fmt",
            ["cargo", "fmt", *package_args, "--", "--check"],
            blocking=True,
        ),
        _run_check(
            root,
            "test",
            ["cargo", "test", *package_args],
            blocking=True,
        ),
    ]
    if not args.skip_clippy:
        checks.append(
            _run_check(
                root,
                "clippy",
                ["cargo", "clippy", *package_args, "--all-targets", "--", "-D", "warnings"],
                blocking=False,
            )
        )

    manifest = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "purpose": "native adapter checks and the checkout they ran against (issue #11)",
        "native": capture_identity(root),
        "toolchain": _toolchain(root),
        "crates": list(args.crates),
        "checks": checks,
    }

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")

    failed = [check["name"] for check in checks if check["blocking"] and check["exit_code"] != 0]
    if failed:
        print(f"blocking checks failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    print("blocking checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
