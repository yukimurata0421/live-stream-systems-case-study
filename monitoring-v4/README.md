# Monitoring v4

Monitoring v4 is an isolated observation-and-evidence subsystem extracted from a
24/7 ADS-B YouTube Live operation. It turns read-only observations into typed
current state, incident transitions, and notification intents while keeping
all production credentials and runtime mutation authority disabled. Recovery
policy, command authorization, delivery, and final verification belong to the
sibling [Central Restart Authority](../recovery-control/README.md), not the
Monitoring v4 package.

This directory is the Monitoring v4 subproject of the public engineering case
study. It was imported from sanitized public snapshot
`5e61e2b7dbbd09f014746c35e50c406ead755561`; it is not the production source
of truth or a supported monitoring product.

## OCI And Twitch Source Update

The 2026-10-13 source port adds configured read-only relay/API/reception
collectors and separate public SLI projections. See
[relay and provider evidence](docs/relay-provider-evidence.md).
The 2026-10-21 update adds fixed-cadence API collection, failure/quota diagnostics,
and OCI-aware Twitch notification decisions with verified-observation recovery
gates. The public examples retain configured account/revision bindings.
The milestone measurements below predate this update; they are historical
validation results, not current deployment or elapsed-time qualification.

## Current public milestone

The repository now contains the R0-R6 credential-free shadow path, including
its PostgreSQL/k3s subsystem:

- a machine-readable ownership inventory and forbidden-import checks;
- versioned contracts with strict UTC timestamps and stable identifiers;
- an isolated SQLite ledger with schema migration, WAL, backup, and restore;
- read-only YouTube lifecycle, input-quality, and delivery adapters;
- sanitized runtime-lifecycle and verified planned-rollout evidence;
- deterministic current reduction and replayable incident transitions;
- notification intents and credential-free recording/scripted providers;
- seven-domain read-only adapters and a versioned SLI projection contract;
- build/source-revision-pinned source coverage and semantic parity ledgers;
- read-only compatibility metrics and a fixed-field public-safe projection;
- an all-or-nothing, network-isolated safe-input generation with a digest manifest;
- PostgreSQL repositories, scoped database roles, leases, and SQLite import;
- a schema-v6 durable ledger that reconciles DB commits with file publication;
- separate k3s workloads for input, core, report, export, database, and backup;
- separately gated database/file retention, dual-device backup, and full-restore evidence;
- default-deny network policy and a detection-only host sentinel;
- role-specific repository ports and split reporting, sentinel, safe-input,
  observation, storage, outbox, and shadow-cycle modules; and
- a read-only, build/source-revision-bound soak checkpoint evaluator that never
  counts evidence before its declared epoch.

The shadow path uses recording/scripted notification providers. A separate
compatibility notifier supports operator-configured Slack and Discord webhooks;
credentials and enabled production writers are excluded. This package contains
no Dell runtime executor or Raspberry Pi internals. Passing tests does not
complete the seven-day source-coverage gate or the fourteen-day semantic-parity
gate.

The committed private implementation lineage through
`8f2c1aa03b8d3805d8a4c1d51a3c432b0e59657f` is represented by the sanitized
public history through `997863c`. The later public soak-checkpoint commit
`e37fb06` is bound to an explicitly recorded private worktree candidate rather
than being presented as a private commit. The portable public test
suite recorded for that earlier milestone was `391 tests / 385 passed / 6 skipped`; the six skips require explicitly
supplied disposable PostgreSQL DSNs. These are source validation results, not
evidence that a public commit was deployed.

SQLite remains part of the contract tests, migration path, forensics, and
rollback boundary. PostgreSQL was introduced when the live shadow was split
into separate k3s workloads: a single SQLite file would otherwise bind writer,
readers, shared storage, and lock ownership to one application failure domain.
PostgreSQL solves that Pod-level coordination problem; it does not make a
single-node k3s cluster highly available.

## Authority boundary

- The arena monitoring host owns observation, current state, incident and
  notification evidence, and facts-only projection.
- The CRA host owns recovery policy, budget, cooldown, authorization, command
  lifecycle, and final verification.
- The Dell delivery host may become a narrowly scoped R7 executor, but it does
  not decide incidents, SLOs, routes, or recovery scope.
- The Raspberry Pi remains publish-only. It does not import Monitoring v4,
  read its internal database, or feed control decisions back to the arena.

## Run the tests

```bash
python3 -m pip install -e '.[test]'
python3 -m pytest -q tests
```

The public tests are self-contained. A real migration-source repository is
required only when running the optional inventory or shadow commands against
an external read-only source.

## Read next

- [Architecture and safety boundaries](docs/architecture.md)
- [Responsibility refactor and failure hardening](docs/hardening.md)
- [Failure-mode mitigation record](docs/failure-mode-mitigation-record.md)
- [Harness engineering, failure injection, and chaos draft](docs/harness-engineering-draft.md)
- [Preserved stateful harness evidence](docs/stateful-harness-evidence.md)
- [Revision-bound soak checkpoints](docs/soak-checkpoints.md)
- [Why PostgreSQL and how the k3s subsystem is split](docs/postgresql-k3s.md)
- [Implemented and pending gates](docs/status.md)
- [Reconstructed history and provenance](docs/history.md)
- [Public release boundary and milestone mapping](docs/public-release.md)

## License

MIT. See [LICENSE](LICENSE).
