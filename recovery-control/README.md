# Central Restart Authority

CRA evaluates a managed FFmpeg child's TCP stall on the delivery host under a
policy signed by the central service. The central service publishes policy and
verifies the append-only audit. The existing Stream Effect Executor owns the
physical effect, exact-target checks, and durable generation fence.

The current implementation is **17 Python files, 2,535 lines** under `src/cra/`
(including comments and blank lines). The operated release remains in
`dry_run`: it records decisions and sends no restart requests. Publication,
local tests, and elapsed runtime do not grant production authority.

## Start here

- [Architecture](docs/architecture.md): two CRA services and the Stream boundary.
- [Current contract](docs/local-reflex-authority.md): restart criterion, 90-second
  engine grace, finite limits, unknown outcomes, and signed audit acknowledgements.
- [Operations](docs/local-reflex-operations.md): inspect, evaluate, stop and roll back.
- [Why CRA exists](docs/why-cra.md): duplicate attempts and the simpler authority split.
- [Test boundaries](docs/harness-trust.md) and [dated evidence](docs/local-reflex-records.md).
- [Historical design](docs/history.md): the previous three-host implementation,
  soak machinery, harness, and their exact retained Git revision.

## Repository map

| Path | Responsibility |
| --- | --- |
| `src/cra/` | Local decision, persistent limits, Executor client, policy and audit verification. |
| `tests/reflex/` | Current unit and real-Executor integration tests with test-owned resources. |
| `tools/replay_tcp_samples.py` | Offline replay of retained TCP samples. |
| `tools/executor_authority.py` | Explicit authority inspection and handover; no automatic invocation. |
| `ops/systemd/`, `policy/` | Inert service and `dry_run` configuration examples. |
| `docs/` | Current contract, operations, evidence limits and historical pointers. |

The Stream runtime boundary lives in [`../src/runtime_boundary/`](../src/runtime_boundary/),
with its tests in [`../tests/runtime_boundary/`](../tests/runtime_boundary/).
The target observer and the small shared identity/projection modules also belong
to Stream. They are not part of the CRA distribution. Their source was moved
without changing behavior; they are required by the delivery runtime and by CRA's
real-Executor compatibility checks. The older central authority, fallback agent,
soak system and general chaos harness are available through Git history rather
than installed alongside the current CRA.

## Install and validate

Use Python 3.11 or later. From `recovery-control/` in this repository:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q tests
.venv/bin/ruff check src/cra tests tools
.venv/bin/ruff format --check src/cra tests tools
.venv/bin/mypy
.venv/bin/python tools/validate_public_snapshot.py
.venv/bin/cra-agent --help
.venv/bin/cra-central --help
```

The integration tests load the companion Stream source from `../src`; no private
checkout, production database, pinned SQLite build or live credentials are needed.
Only the test extra needs the Stream boundary's `jsonschema` and `rfc8785`
dependencies. The installed CRA package itself depends on `cryptography`.
To run the Stream suite, follow the [top-level guide](../docs/README.md).

## Public boundary

Examples retain `dry_run`. Enabling effects needs an explicit authority handover,
compatible observations, finite persistent limits, a real Executor socket, and
operating permission; see the current contract. These files contain no production
keys, databases, logs, packet captures or private host installation workflow.
The [publication record](../publication/cra-layout-20261026.json) records this
source-only reorganization; no service, policy, live ledger or delivery was changed.

MIT. See [LICENSE](LICENSE) and the [documentation index](docs/00_INDEX.md).
