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
objects inside it, the stubs it carries, and the source identity it is *declared* to come
from.

It is deliberately an inventory, not a proof of origin. Passing ``--native-evidence``
copies the identity that evidence recorded, but nothing here can show that the wheel was
built from that tree: only a controlled build record that names both the source
fingerprint and the artifact digest can say that, and this manifest marks such a binding
as ``unknown``. The wheel's own tags are read from its metadata; the host running this
script is recorded separately as the collector and is not the build platform.
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
ADAPTER_STUB_PREFIX = "nautilus_trader/adapters/"


def _hash_zip_entry(archive: zipfile.ZipFile, name: str) -> str:
    digest = hashlib.sha256()
    with archive.open(name) as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_wheel_metadata(archive: zipfile.ZipFile) -> dict[str, Any]:
    """Reads the wheel's own tags and name/version from its metadata files."""
    tags: list[str] = []
    name = None
    version = None

    for entry in archive.namelist():
        if entry.endswith(".dist-info/WHEEL"):
            for line in archive.read(entry).decode(errors="replace").splitlines():
                if line.startswith("Tag:"):
                    tags.append(line.removeprefix("Tag:").strip())
        elif entry.endswith(".dist-info/METADATA"):
            for line in archive.read(entry).decode(errors="replace").splitlines():
                if line.startswith("Name:"):
                    name = line.removeprefix("Name:").strip()
                elif line.startswith("Version:"):
                    version = line.removeprefix("Version:").strip()
                if name and version:
                    break

    return {"name": name, "version": version, "tags": tags}


def _stub_hashes(paths: list[Path]) -> list[dict[str, Any]]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.pyi")))
        elif path.is_file():
            files.append(path)
        else:
            raise SystemExit(f"stub path does not exist: {path}")

    return [{"path": str(path), "sha256": sha256_file(path)} for path in files]


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
        help="native evidence JSON whose identity is declared for this wheel",
    )
    parser.add_argument(
        "--features",
        default="",
        help="cargo features the wheel was declared built with (recorded verbatim)",
    )
    parser.add_argument(
        "--profile",
        default="release",
        help="cargo profile the wheel was declared built with",
    )
    parser.add_argument(
        "--stubs",
        nargs="*",
        default=[],
        help="reference stub files or directories to hash and compare with the wheel",
    )
    args = parser.parse_args(argv)

    wheel = Path(args.wheel).resolve()
    if not wheel.is_file():
        raise SystemExit(f"wheel does not exist: {wheel}")

    declared_by = "collector_checkout"
    declared_evidence = None
    if args.native_evidence:
        evidence_path = Path(args.native_evidence)
        declared = json.loads(evidence_path.read_text(encoding="utf-8"))["native"]
        declared_by = "evidence_file"
        declared_evidence = {
            "path": str(evidence_path),
            "sha256": sha256_file(evidence_path),
        }
    else:
        declared = capture_identity(repo_root())

    with zipfile.ZipFile(wheel) as archive:
        metadata = _read_wheel_metadata(archive)
        embedded = [
            {
                "path": info.filename,
                "sha256": _hash_zip_entry(archive, info.filename),
                "size": info.file_size,
            }
            for info in sorted(archive.infolist(), key=lambda entry: entry.filename)
            if info.filename.endswith(NATIVE_SUFFIXES) and not info.is_dir()
        ]
        embedded_stubs = {
            info.filename: _hash_zip_entry(archive, info.filename)
            for info in sorted(archive.infolist(), key=lambda entry: entry.filename)
            if info.filename.startswith(ADAPTER_STUB_PREFIX)
            and info.filename.endswith(".pyi")
            and not info.is_dir()
        }

    reference_stubs = []
    for stub in _stub_hashes([Path(p) for p in args.stubs]):
        wheel_path = stub["path"].replace("\\", "/").removeprefix("python/")
        embedded_hash = embedded_stubs.get(wheel_path)
        reference_stubs.append(
            {
                **stub,
                "wheel_path": wheel_path,
                "matches_embedded": embedded_hash == stub["sha256"]
                if embedded_hash
                else None,
            }
        )

    manifest = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "purpose": "name the binary an installed wheel actually is (issue #11)",
        "source_binding": "unknown",
        "source_binding_note": (
            "the declared identity names what the wheel is said to come from; only a "
            "controlled build record with a source fingerprint and artifact digest can "
            "verify it"
        ),
        "declared_by": declared_by,
        "declared_native": declared,
        "declared_evidence": declared_evidence,
        "wheel": {
            "path": str(wheel),
            "filename": wheel.name,
            "sha256": sha256_file(wheel),
            "size": wheel.stat().st_size,
            **metadata,
        },
        "declared_build": {
            "features": args.features,
            "profile": args.profile,
        },
        "collector": {
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "embedded_native_objects": embedded,
        "embedded_adapter_stubs": [
            {"path": path, "sha256": digest} for path, digest in embedded_stubs.items()
        ],
        "reference_stubs": reference_stubs,
    }

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
