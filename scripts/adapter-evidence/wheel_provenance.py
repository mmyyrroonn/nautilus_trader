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

"""Writes a provenance manifest for a built adapter wheel (issue #11).

A wheel version number is not a binary: two builds from different sources can carry the
same version. This records what a specific wheel actually is - its hash, the native
objects inside it, the stubs it was generated with, and the checkout it was built from -
so an installed binary can be matched back to a source tree instead of trusted by name.

Pair it with ``native_checks.py``: run the checks first, then build the wheel, then pass
``--native-evidence`` here so both records name the same checkout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from _identity import EVIDENCE_SCHEMA_VERSION, capture_identity, repo_root, sha256_file

NATIVE_SUFFIXES = (".pyd", ".so", ".dylib", ".dll")


def _hash_zip_entry(archive: zipfile.ZipFile, name: str) -> str:
    digest = hashlib.sha256()
    with archive.open(name) as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stub_hashes(paths: list[Path]) -> list[dict[str, str]]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.pyi")))
        elif path.is_file():
            files.append(path)
        else:
            raise SystemExit(f"stub path does not exist: {path}")

    return [
        {
            "path": str(path),
            "sha256": sha256_file(path),
        }
        for path in files
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--wheel", required=True, help="path to the built .whl")
    parser.add_argument(
        "--output",
        default="adapter-wheel-provenance.json",
        help="where to write the provenance manifest",
    )
    parser.add_argument(
        "--native-evidence",
        help="native evidence JSON from native_checks.py; its identity is copied verbatim",
    )
    parser.add_argument(
        "--features",
        default="",
        help="cargo features the wheel was built with (recorded verbatim)",
    )
    parser.add_argument(
        "--profile",
        default="release",
        help="cargo profile the wheel was built with",
    )
    parser.add_argument(
        "--stubs",
        nargs="*",
        default=[],
        help="stub files or directories to hash (directories are searched for *.pyi)",
    )
    args = parser.parse_args(argv)

    wheel = Path(args.wheel).resolve()
    if not wheel.is_file():
        raise SystemExit(f"wheel does not exist: {wheel}")

    if args.native_evidence:
        native = json.loads(Path(args.native_evidence).read_text(encoding="utf-8"))["native"]
    else:
        native = capture_identity(repo_root())

    with zipfile.ZipFile(wheel) as archive:
        embedded = [
            {
                "path": info.filename,
                "sha256": _hash_zip_entry(archive, info.filename),
                "size": info.file_size,
            }
            for info in sorted(archive.infolist(), key=lambda entry: entry.filename)
            if info.filename.endswith(NATIVE_SUFFIXES) and not info.is_dir()
        ]

    manifest = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "purpose": "name the binary an installed wheel actually is (issue #11)",
        "native": native,
        "wheel": {
            "path": str(wheel),
            "filename": wheel.name,
            "sha256": sha256_file(wheel),
            "size": wheel.stat().st_size,
        },
        "build": {
            "features": args.features,
            "profile": args.profile,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "embedded_native_objects": embedded,
        "stubs": _stub_hashes([Path(p) for p in args.stubs]),
    }

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
