"""Build one adapter wheel and bind its digest to an unchanged source checkout."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import tomllib
from _identity import capture_identity, fingerprint, repo_root, sha256_file
from wheel_provenance import main as write_provenance

UV_VERSION = "0.12.6"
RUST_VERSION = "1.98.0"
MATURIN_VERSION = "1.15.0"


def version(command: list[str]) -> str:
    return subprocess.check_output(command, text=True).strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--python", type=Path, required=True, help="native build venv interpreter")
    parser.add_argument("--native-root", type=Path, default=repo_root())
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=("release", "nextest"), default="release")
    parser.add_argument("--online", action="store_true", help="allow Cargo to fetch locked dependencies")
    args = parser.parse_args()

    root = args.native_root.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    python = Path(os.path.abspath(args.python))
    if not python.is_file():
        parser.error("native interpreter is missing")
    maturin = python.with_name("maturin.exe" if os.name == "nt" else "maturin")
    if not maturin.is_file():
        parser.error("maturin is missing from the selected native environment")

    py = json.loads(version([str(python), "-c", "import json,sys; print(json.dumps(list(sys.version_info[:3])))"]))
    if tuple(py) != (3, 12, 9):
        parser.error("native build requires CPython 3.12.9")
    tools = {
        "uv": version(["uv", "--version"]),
        "rustc": version(["rustc", "--version"]),
        "cargo": version(["cargo", "--version"]),
        "maturin": version([str(maturin), "--version"]),
        "python": version([str(python), "--version"]),
    }
    if not tools["uv"].startswith(f"uv {UV_VERSION} "):
        parser.error(f"uv {UV_VERSION} is required")
    if not tools["rustc"].startswith(f"rustc {RUST_VERSION} "):
        parser.error(f"rustc {RUST_VERSION} is required")
    if tools["maturin"] != f"maturin {MATURIN_VERSION}":
        parser.error(f"maturin {MATURIN_VERSION} is required")

    before = capture_identity(root)
    if not before["commit"] or not before["tree"] or not before["cargo_lock_sha256"]:
        parser.error("native Git checkout or Cargo.lock identity is unavailable")
    if list(output.glob("nautilus_trader-*.whl")):
        parser.error("output directory already contains a wheel")
    command = [str(maturin), "build", "--profile", args.profile, "--locked",
               "--interpreter", str(python), "--out", str(output)]
    if not args.online:
        command.append("--offline")
    env = os.environ.copy()
    for name in tuple(env):
        if name.startswith("CONDA_") or name in {"PYTHONPATH", "PYTHONHOME"}:
            env.pop(name, None)
    env["PYTHONUTF8"] = "1"
    if os.name == "nt":
        # MSVC emits an import-library notice which linker_messages ignores -Dwarnings.
        # Scope this Cargo exception to this one build invocation.
        env["CARGO_BUILD_WARNINGS"] = "allow"
        env["PATH"] = os.pathsep.join(
            part for part in env.get("PATH", "").split(os.pathsep)
            if not any(token in part.lower() for token in ("conda", "anaconda", "miniconda"))
        )
    subprocess.run(command, cwd=root / "python", env=env, check=True)
    after = capture_identity(root)
    if fingerprint(before) != fingerprint(after):
        raise SystemExit("native source changed during build; wheel provenance refused")
    wheels = sorted(output.glob("nautilus_trader-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel in {output}; found {len(wheels)}")
    manifest = output / "native-provenance.json"
    evidence = output / "native-build-input.json"
    evidence.write_text(json.dumps({"native": before}, sort_keys=True) + "\n", encoding="utf-8")
    config = tomllib.loads((root / "python" / "pyproject.toml").read_text(encoding="utf-8"))
    features = ",".join(config["tool"]["maturin"]["features"])
    write_provenance(["--wheel", str(wheels[0]), "--output", str(manifest),
                      "--profile", args.profile, "--features", features,
                      "--native-evidence", str(evidence)])
    record = json.loads(manifest.read_text(encoding="utf-8"))
    if fingerprint(record["declared_native"]) != fingerprint(before):
        raise SystemExit("provenance source differs from pre-build source")
    record["source_binding"] = "verified"
    record["source_binding_note"] = "single controlled build from matching pre/post source fingerprints"
    record["source_fingerprint_sha256"] = fingerprint(before)
    record["build"] = {
        "started_source": before,
        "finished_source": after,
        "command": command[1:],
        "build_warnings_override": env.get("CARGO_BUILD_WARNINGS"),
        "cargo_target_dir": env.get("CARGO_TARGET_DIR"),
        "conda_path_entries_removed": os.name == "nt",
        "tools": tools,
        "interpreter": str(python),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cargo_lock_sha256": sha256_file(root / "Cargo.lock"),
        "python_uv_lock_sha256": sha256_file(root / "python" / "uv.lock"),
        "maturin_config_sha256": sha256_file(root / "python" / "pyproject.toml"),
        "builder_sha256": sha256_file(Path(__file__)),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    manifest.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"source-bound wheel: {wheels[0]}")
    print(f"wheel SHA-256: {record['wheel']['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
