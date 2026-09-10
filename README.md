# Central Restart Authority

This repository is a sanitized, repository-only snapshot of the recovery
control plane behind a self-built 24/7 YouTube Live system. It separates
monitoring facts, central authorization, and the physical FFmpeg effect across
three failure domains.

The public snapshot is designed for architecture and reliability review. It
does not contain production credentials, runtime state, databases, host logs,
packet captures, or an enabled production action path.

## System shape

```text
Dell delivery host
  signed target, transport, recovery, and maintenance observations
       |
       | read-only mTLS GET
       v
arena-server monitoring plane
  PostgreSQL read-only adapter -> facts-only signed projection
       |
       | read-only mTLS GET
       v
cra-01 central authority
  policy + budget + cooldown + command lifecycle + final verification
       |
       | production delivery is disabled in this snapshot
       v
Dell Recovery Agent
  authority lease + exact target + durable effect fence
       v
stream-engine Effect Executor -> exact FFmpeg child
```

The monitoring plane cannot authorize an action. The central authority cannot
signal a process. The Dell Agent cannot redefine central policy. Only the
Effect Executor owns the physical effect boundary.

## Current source posture

The included policy declares:

```text
status = PROPOSED_LOCAL_VERIFICATION
production_behavior_change_authorized = false
```

The no-action runtime can record `WOULD_AUTHORIZE` decisions, but it cannot
materialize an executable authorization or deliver a command. Source presence
and test success must not be read as production activation.

## Reliability mechanisms

- RFC 8785 canonical JSON, SHA-256, Ed25519, and TLS 1.3 mutual TLS;
- signed, source-bound, release-bound, sequence-bound evidence projections;
- facts-only arena payloads with recursive control-field rejection;
- host-local SQLite/WAL with ordered migrations and transactional outboxes;
- request idempotency plus logical-generation and exact-target effect fences;
- `OUTCOME_UNKNOWN` without automatic retry;
- append-only reconciliation after delayed or ambiguous outcomes;
- separate Central, Dell Agent, and runtime EffectLedger databases;
- an independent oracle, negative controls, evidence completeness checks, and
  production-isolation counters in the harness.

The design does not claim distributed ACID across hosts. It closes uncertainty
with durable local transactions, identifiers, fencing, signed evidence, and
reconciliation.

## Repository map

| Path | Purpose |
| --- | --- |
| `src/cra_authority/` | Central policy, command, outbox, and verification logic. |
| `src/monitoring_projection/` | arena-server read-only facts adapter and signed projection. |
| `src/dell_recovery_agent/` | Dell observation, authority, lease, and reconciliation boundary. |
| `src/runtime_boundary/` | Exact FFmpeg target and at-most-once effect ledger. |
| `src/cra_no_action_soak/` | No-action status, parity, and soak gates. |
| `src/cra_harness/` | Fault injection, independent checks, and evidence gates. |
| `contracts/` | Versioned JSON schemas. |
| `migrations/` | Ordered Central, Dell, and maintenance SQLite migrations. |
| `policy/` | Explicit recovery policy and disabled production posture. |
| `ops/` | Inert deployment examples; public CI does not install them. |
| `tests/` | Unit, integration, chaos, release-boundary, and negative-control tests. |

## Local validation

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
tools/sqlite_runtime/build.sh
tools/sqlite_runtime/run-fixed.sh .venv/bin/python tools/run_public_test_suite.py
python3 tools/validate_public_snapshot.py
```

The public runner discovers every test file and applies the explicit exclusions
in `tests/public_ci_exclusions.json`. Those exclusions require private evidence,
private workflow contracts, immutable release artifacts, a matching sibling
`stream_v3` candidate, or an extended resource-bound replay. They remain visible
for review but are not counted as public CI coverage.

The tests require SQLite 3.51.3. The build script downloads the upstream source
archive, verifies its SHA-256 and `sqlite3.c` SHA3-256, and installs it only
under the ignored `.runtime/` directory. `tools/run_full_regression.sh` retains
the private full-regression entrypoint, but running it does not restore excluded
private evidence or cross-repository candidates.

Start with [`docs/architecture.md`](docs/architecture.md), then read
[`docs/safety-model.md`](docs/safety-model.md) and
[`docs/harness-trust.md`](docs/harness-trust.md). The current repository-only
design record is split into [harness engineering](docs/harness-engineering-draft.md),
[failure injection](docs/failure-injection-draft.md), and
[chaos testing](docs/chaos-testing-draft.md) drafts.

## Public boundary

The snapshot was derived from private source commit
`fb7ffa3d96c4cf23637d72a5cbc6f65fac4d008c`. The private Git history,
operational records, live evidence, generated artifacts, and uncommitted source
changes were not copied. See [`docs/public-release.md`](docs/public-release.md)
for the complete boundary.

## License

MIT. See [`LICENSE`](LICENSE).
