# Adapter evidence tooling

This directory is the fork's minimal, no-secrets way to answer one question: **which
source tree did the adapter binary that just ran actually come from?** It is the first
layer of issue
[mmyyrroonn/nautilus_trader#11](https://github.com/mmyyrroonn/nautilus_trader/issues/11).

A version number is not a binary. `nautilus_trader-2.0.0rc4` can be built from any commit,
so acceptance evidence binds results to a named commit, tree, dirty manifest, lock file,
toolchain, platform, features and artifact hash instead of to a version string.

## Run the native checks

```sh
# From the repository root (any Python 3.11+):
python scripts/adapter-evidence/native_checks.py --output adapter-native-evidence.json
```

The script runs the same commands CI runs, records each exit code and a bounded output
tail, captures the checkout identity and the toolchain, and writes one JSON manifest. It
exits non-zero when a blocking check fails; `clippy` is recorded but not blocking while
the pre-existing lint debt below is open. It never reads credentials and never touches an
account.

| Check | Blocking | Command |
|---|---|---|
| `fmt` | yes | `cargo fmt -p nautilus-aster -p nautilus-ondo -- --check` |
| `test` | yes | `cargo test -p nautilus-aster -p nautilus-ondo` |
| `clippy` | no | `cargo clippy -p nautilus-aster -p nautilus-ondo --all-targets -- -D warnings` |

Options: `--crates` to override the package list, `--skip-clippy`, `--output`. The
manifest a run leaves behind is the record; it is not copied into the repository, where it
would only go stale.

## Record a built wheel

```sh
python scripts/adapter-evidence/wheel_provenance.py \
  --wheel path/to/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl \
  --native-evidence adapter-native-evidence.json \
  --features python \
  --profile release \
  --stubs python/nautilus_trader/adapters/aster python/nautilus_trader/adapters/ondo \
  --output adapter-wheel-provenance.json
```

The script hashes the wheel, hashes every native object (`.pyd`/`.so`/`.dylib`/`.dll`)
inside it, and hashes the stub files it is pointed at. Passing `--native-evidence` copies
the already-recorded checkout identity verbatim, so the checks and the wheel cannot name
different trees without the manifest saying so. Omitting it captures the identity of the
current checkout instead.

## CI

[`.github/workflows/nautilus-adapter-checks.yml`](../../.github/workflows/nautilus-adapter-checks.yml)
runs the blocking checks on every pull request and `main` push that touches the Aster or
Ondo adapters, uploads the evidence manifest as an artifact, and records clippy without
blocking. The upstream `test.yml` workflow is not usable here: it is pinned to upstream's
self-hosted runners and does not attach results to fork pull requests.

To reproduce the CI result locally, run `native_checks.py` - it is the same entry point.

## Known exceptions (explicit, not hidden)

- **Clippy in `nautilus-ondo`**: with the pinned toolchain, `cargo clippy -p nautilus-ondo
  --all-targets -- -D warnings` reports pre-existing lints (for example
  `clippy::redundant_clone` in `src/websocket/parse.rs`, `clippy::collapsible_if` in
  `src/execution.rs`, and workspace `missing_panics_doc` errors). `nautilus-aster` is
  clean. The debt is tracked under #11 and is cleared in its own change; the convention
  is to fix the lints, not to add `#[allow]` or drop `-D warnings`.
- **Wheel builds are not automated here yet**: `wheel_provenance.py` records a wheel that
  was built with the documented repository build steps; wiring the build itself into CI is
  the next layer of #11.

## Still open in #11

The full issue also asks for: a wheel-building CI leg with consumable artifacts, the
cross-repo workflow that checks out Nautilus-Perps and installs the same candidate wheel,
a fixture manifest (observed / official-example / synthetic with source and hash), a
versioned capability matrix, and README pointers to the current evidence entry points.
Until those land, this directory is the authoritative "can this be re-run" entry for the
two adapters.
