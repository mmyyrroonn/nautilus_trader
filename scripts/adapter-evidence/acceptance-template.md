# Dual-repo acceptance record

Copy this template into the run's report directory and fill every field. `unknown` and
`not run` are valid results; blank is not. One `overall: true` hides a failed dimension,
so each dimension carries its own verdict, and an entry fill never counts as acceptance
for a clean shutdown.

## Candidate identity

| Field | Value |
|---|---|
| Native repo | `mmyyrroonn/nautilus_trader` |
| Native commit | |
| Native tree | |
| Native dirty manifest | `adapter-native-evidence.json` (`dirty_count`) |
| Cargo.lock sha256 | |
| Wheel filename | |
| Wheel sha256 | |
| Embedded native object sha256 | |
| Stub sha256 | |
| rustc / cargo | |
| Platform / ABI | |
| Cargo features / profile | |
| App repo | `mmyyrroonn/Nautilus-Perps` |
| App commit | |
| Config sha256 | |

## Commands and artifacts

| Command | Exit | Artifact / evidence |
|---|---|---|
| `python scripts/adapter-evidence/native_checks.py --output ...` | | |
| `python scripts/adapter-evidence/wheel_provenance.py --wheel ... --native-evidence ...` | | |
| App-side offline checks | | |
| Venue-facing run (only when separately authorized) | | |

## Results by dimension

Mark each `applied` / `verified` / `unknown` / `not run`. Use the evidence path that
proves the verdict.

| Dimension | Verdict | Evidence / notes |
|---|---|---|
| Entry (request, fill, order state) | | |
| Close (reduce-only, partial fills, residual) | | |
| Flat (position and open orders) | | |
| Clean shutdown | | |
| DMS / protection release | | |
| Economics (fees, funding, balances) | | |
| Read-only recovery (no writes) | | |

## Residual risk and open items

- Unconfirmed facts (for example DMS release not acknowledged) and what would confirm them:
- Known limitations in force for this run:
- Authorization reference for any venue write:
