# Adapter evidence tooling

This directory is the fork's minimal, no-secrets way to answer one question: **which
source tree did the adapter binary that just ran actually come from?** It is the first
layer of issue
[mmyyrroonn/nautilus_trader#11](https://github.com/mmyyrroonn/nautilus_trader/issues/11).

A version number is not a binary. `nautilus_trader-2.0.0rc4` can be built from any commit,
so acceptance evidence binds results to a named commit, tree, content fingerprint, lock
file, toolchain, platform, features and artifact hash instead of to a version string.

## Run the native checks

```sh
# From the repository root (any Python 3.11+):
python scripts/adapter-evidence/native_checks.py --output adapter-native-evidence.json
```

The script runs the same commands CI runs, records each exit code, and writes every
check's stdout and stderr to separate files under `adapter-native-evidence-logs/` (or
`--logs`). The manifest carries each log's path, byte count, sha256 and a bounded tail,
so a failure assertion on stdout is not lost because stderr happened to be longer. It
never reads credentials and never touches an account.

The test entry is `cargo nextest`, the repository's standard runner, so the retry policy
applies to the execution-client suites that starve under heavy parallelism. The
repository's `.config/nextest.toml` scopes its `retries = 3` override as
`test(exec_client)`, which matches test *names*; this repository's test binary is *named*
`exec_client`, so the entry applies `--retries 3 --no-fail-fast` explicitly instead of
silently getting no retries. `--profile ci` limits parallelism the same way CI does.
Doctests run under the libtest harness and are recorded separately, as the repository's
own `make cargo-test` does. Install the pinned tool with
`cargo install cargo-nextest --version "$(bash scripts/cargo-tool-version.sh cargo-nextest)" --locked`
(the repository's `make install-tools` does it too).

The identity recorded is a *content* identity, not a file-status one: `dirty` lists the
paths, `tracked_diff_sha256` hashes the binary diff against `HEAD`, and
`untracked_sha256` hashes untracked paths and contents. Two checkouts with the same
`M source.rs` status and different contents therefore get different fingerprints. The
identity is captured before and after the checks; `identity_changed_during_checks` is
`true` when a check changed the source or the lock file, and such a manifest must not be
used as a candidate record.

It exits non-zero when a blocking check fails; `clippy` is recorded but not blocking while
the pre-existing lint debt below is open.

| Check | Blocking | Command |
|---|---|---|
| `fmt` | yes | `cargo fmt -p nautilus-aster -p nautilus-ondo -- --check` |
| `test` | yes | `cargo nextest run -p nautilus-aster -p nautilus-ondo --profile ci --retries 3 --no-fail-fast` |
| `doctest` | yes | `cargo test --doc -p nautilus-aster -p nautilus-ondo` |
| `clippy` | no | `cargo clippy -p nautilus-aster -p nautilus-ondo --all-targets -- -D warnings` |

Options: `--crates` to override the package list, `--skip-clippy`, `--output`, `--logs`.

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

This is an **inventory, not a proof of origin**. It hashes the wheel, the native objects
inside it, and the adapter stubs it carries; it reads the wheel's own name, version and
tags from its metadata; and it records the host that ran the recorder as `collector`,
which is not the build platform. The identity passed with `--native-evidence` is recorded
as `declared_native` with `declared_by: evidence_file` (or the current checkout with
`declared_by: collector_checkout`), and `source_binding` stays `unknown`: nothing here can
show that the wheel was built from that tree. Only a controlled build record that names
both a source fingerprint and the artifact digest can set a verified binding. The evidence
file itself is hashed so the declaration is traceable.

`--stubs` are recorded as `reference_stubs` and compared against the matching
`embedded_adapter_stubs` entry (path mapping strips a leading `python/`), so a stub can be
checked against what the wheel actually carries instead of being trusted by name.

## CI

[`.github/workflows/nautilus-adapter-checks.yml`](../../.github/workflows/nautilus-adapter-checks.yml)
runs the blocking checks on every pull request and `main` push that touches the Aster or
Ondo adapters, installs `rustfmt`, `clippy` and the pinned `cargo-nextest` explicitly (the
toolchain file names no components), and uploads the manifest and the per-check logs as an
artifact. The upstream `test.yml` workflow is not usable here: it is pinned to upstream's
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
