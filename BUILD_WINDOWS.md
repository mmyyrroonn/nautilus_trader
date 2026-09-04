# Building NautilusTrader v2.0.0rc4 from source on Windows 11

Result: **SUCCESS**. Upstream sources were built UNMODIFIED (no `.rs` / `.py` /
`pyproject.toml` edits). Every problem was solved with environment variables only.

- Repo: `E:\nautilus_trader`, tag `v2.0.0rc4`, commit `a0400251110653b6d8ae6a9b5b89c4543fa85a2d`
- Local branch: `aster` (nothing committed)
- Date: 2026-09-05

## Toolchain versions

| Tool | Version |
|---|---|
| OS | Windows 11 Pro 10.0.26200 |
| rustup | 1.28.1 |
| rustc / cargo | 1.98.0 (88d9e12ae 2026-08-18) / 1.98.0 (797e8a9bc 2026-08-05) - auto-installed by rustup from `rust-toolchain.toml` |
| clang | 20.1.3 (`C:\Program Files\LLVM\bin`, already on PATH) |
| MSVC | VS 2022 Build Tools 17.11.5, Windows Kit 10.0.22621 |
| uv | **before 0.6.10 -> after 0.12.6** (repo requires `>=0.12,<0.13`; pinned value from `scripts/tool-version.sh uv` is 0.12.6) |
| maturin | 1.15.0 (from the `python/pyproject.toml` dev group) |
| Python | uv-managed CPython 3.12.9 in `E:\nautilus_trader\.venv` |
| git | 2.54.0.windows.1 |

`make` is not installed on this machine; the `sync`, `py-stubs` and `build-debug`
Makefile targets were transcribed and run by hand (see below).

## Environment variables used

Exported for the build steps (mirrors the Makefile plus two host-specific fixes):

```sh
export UV_PROJECT_ENVIRONMENT="E:/nautilus_trader/.venv"   # Makefile
export VIRTUAL_ENV=                                        # Makefile
export CC=clang                                            # Makefile
export CXX=clang++                                         # Makefile
export CARGO_TARGET_DIR="E:/nautilus_trader/target"        # Makefile TARGET_DIR
export NAUTILUS_STUB_PROFILE=nextest                       # Makefile CARGO_CI_PROFILE (stub step only)

# Host-specific fixes (NOT in the Makefile):
export PYTHONUTF8=1                                        # fix 4 - see Problems
unset CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL \
      CONDA_PROMPT_MODIFIER CONDA_EXE CONDA_PYTHON_EXE     # fix 5 - see Problems
```

`PYO3_PYTHON` was NOT needed: `uv run --no-sync` sets `VIRTUAL_ENV` to
`UV_PROJECT_ENVIRONMENT`, which PyO3 picks up.

No Visual Studio dev shell (`vcvars64.bat` / `VsDevCmd.bat`) was needed - rustc
locates MSVC `link.exe` on its own, even though `C:\Program Files\Git\usr\bin\link.exe`
shadows it on PATH. Verified up front with a throwaway hello-world `cargo run`.

## Command sequence that worked

```sh
# 0. Clone (a full clone timed out at 300s; --depth 1 works)
cd /e
git clone --depth 1 --branch v2.0.0rc4 \
  https://github.com/nautechsystems/nautilus_trader.git nautilus_trader
cd /e/nautilus_trader
git switch -c aster

# 1. Toolchain
rustup toolchain install 1.98.0-x86_64-pc-windows-msvc

# 2. uv upgrade (0.6.10 -> 0.12.6); uv was pip-installed, so `uv self update` refuses
python -m pip install --user --upgrade "uv==0.12.6"

# 3. Makefile target `sync`
cd /e/nautilus_trader/python
UV_PROJECT_ENVIRONMENT="E:/nautilus_trader/.venv" VIRTUAL_ENV= \
  uv sync --all-groups --all-extras --no-install-package nautilus-trader \
          --inexact --managed-python --python 3.12

# 4. Makefile target `py-stubs` (the big cargo build: ~630 crates)
cd /e/nautilus_trader/python
UV_PROJECT_ENVIRONMENT="E:/nautilus_trader/.venv" VIRTUAL_ENV= \
  CC=clang CXX=clang++ PYTHONUTF8=1 \
  NAUTILUS_STUB_PROFILE=nextest CARGO_TARGET_DIR="E:/nautilus_trader/target" \
  uv run --no-sync python generate_stubs.py

# 5. Makefile target `build-debug`
cd /e/nautilus_trader/python
# CONDA_* must be unset here (see Problems)
UV_PROJECT_ENVIRONMENT="E:/nautilus_trader/.venv" VIRTUAL_ENV= \
  CC=clang CXX=clang++ PYTHONUTF8=1 \
  CARGO_TARGET_DIR="E:/nautilus_trader/target" \
  uv run --no-sync maturin develop --profile nextest
```

## Wall-clock times and sizes

| Step | Wall clock |
|---|---|
| `git clone --depth 1` | ~1 min (27 MB `.git`) |
| `rustup toolchain install 1.98.0` | ~2 min |
| `uv sync` (managed CPython) | < 1 min |
| `generate_stubs.py` (~630 crates, profile `nextest`) | **5 min 48 s** (00:13:36 -> 00:19:24) |
| `maturin develop --profile nextest` | **1 min 46 s** (00:20:45 -> 00:22:31); cargo itself reported `Finished nextest profile [unoptimized] target(s) in 1m 38s` |
| Total build (steps 4 + 5) | **~7.5 min** |

- `E:\nautilus_trader\target` after both steps: **26 GB**
- `python/nautilus_trader/_libnautilus.cp312-win_amd64.pyd`: 372,517,888 bytes (355 MB, debug profile, unstripped)
- `python/nautilus_trader/nautilus_pyo3.pdb`: 422,203,392 bytes (403 MB) - untracked, `*.pdb` is not in `.gitignore`

## Build-config changes

**None.** `python/pyproject.toml` was not touched; the default maturin feature list
(`extension-module, arrow, betfair, high-precision, mimalloc, redis, postgres,
defi, hypersync, tracing-bridge`) built as-is.

Notably `aws-lc-sys 0.44.0` compiled successfully **without** `cmake` or `nasm`
on PATH (neither is installed on this machine).

## Problems hit and how they were resolved

### 1. Full `git clone` times out

`git clone` without `--depth` died with `Connection timed out after 300048 ms`, while
`curl https://github.com` and `git ls-remote` both worked. There is no proxy
(`netsh winhttp show proxy` reports direct access; `ProxyEnable=0`). Fixed by cloning
shallow: `git clone --depth 1 --branch v2.0.0rc4`. This also matches the upstream docs.

### 2. `uv self update` refuses to run

uv was pip-installed into `C:\Users\myron\AppData\Roaming\Python\Python312\Scripts`.
`uv self update` prints *"Self-update is only available for uv binaries installed via
the standalone installation scripts."* Resolved with
`python -m pip install --user --upgrade "uv==0.12.6"` (0.6.10 -> 0.12.6).

### 3. `uv sync` picked conda's Python by default

The first `uv sync` created `.venv` on top of conda base's CPython 3.12.4
(`C:\ProgramData\miniconda3`). The venv was deleted and recreated with
`--managed-python --python 3.12` so its base interpreter is the uv-managed
CPython 3.12.9 under `%APPDATA%\uv\python\`, keeping the fork independent of conda.

### 4. `generate_docstrings.py` crashes with `UnicodeDecodeError: 'gbk' codec`

This machine's system locale is Chinese, so CPython's default text encoding is GBK.
`generate_docstrings.py` (invoked by `generate_stubs.py`) calls `Path.read_text()`
on `.rs` files without an explicit encoding:

```
File "E:\nautilus_trader\python\generate_docstrings.py", line 135, in collect_source_docs
    lines = rs_file.read_text().splitlines()
UnicodeDecodeError: 'gbk' codec can't decode byte 0xa6 in position 27133: illegal multibyte sequence
```

Fixed **without editing sources** by setting `PYTHONUTF8=1` (Python UTF-8 mode).
Any Windows host whose default locale is not UTF-8 will need this.

### 5. `maturin develop` refuses to run under conda

```
maturin failed
  Caused by: Both VIRTUAL_ENV and CONDA_PREFIX are set. Please unset one of them
```

The shell auto-activates conda base, and `uv run` sets `VIRTUAL_ENV`. Fixed by
`unset CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL CONDA_PROMPT_MODIFIER CONDA_EXE CONDA_PYTHON_EXE`
before invoking maturin.

### 6. Stub generation rewrites all 41 `.pyi` files with CRLF

After `generate_stubs.py`, `git status` shows 41 modified tracked `.pyi` files.
The content is **byte-identical apart from line endings** - `git diff --ignore-cr-at-eol --stat`
is empty. The generator writes text files in Windows text mode while the checkout is LF
(`core.autocrlf` is not enabled). They were restored with
`git checkout -- python/nautilus_trader`; the extension is unaffected (`.pyi` are type
stubs only). Expect this to reappear after every stub regeneration on Windows.

## Verification

```
$ E:\nautilus_trader\.venv\Scripts\python.exe -c "import nautilus_trader, sys; print(sys.version); print(nautilus_trader.__version__); from nautilus_trader.adapters.lighter import LighterDataClientFactory; from nautilus_trader.adapters.binance import BinanceDataClientFactory, BinanceDataClientConfig; from nautilus_trader.adapters.hyperliquid import HyperliquidDataClientFactory; from nautilus_trader.live import LiveNode; print('import ok')"
3.12.9 (main, Mar 17 2025, 21:06:20) [MSC v.1943 64 bit (AMD64)]
2.0.0rc4
import ok

$ cd E:\nautilus_trader && rustc --version && cargo --version && git status --short
rustc 1.98.0 (88d9e12ae 2026-08-18)
cargo 1.98.0 (797e8a9bc 2026-08-05)
?? python/nautilus_trader/nautilus_pyo3.pdb
```

`target/` and `.venv/` are gitignored (`*target/`, `.venv*/`); `build-logs/*.log` is
covered by the `*.log` rule, so `build-logs/` does not show up either.
`nautilus_pyo3.pdb` is a maturin debug-symbol artifact (`*.pdb` is not gitignored upstream).

## Logs

- `build-logs/01-uv-sync.log` - first (conda-based) sync, superseded
- `build-logs/01b-uv-sync-managed.log` - the sync that was kept
- `build-logs/02-py-stubs.log` - `generate_stubs.py`
- `build-logs/03-maturin-develop.log` - `maturin develop --profile nextest`
