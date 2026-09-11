# Public release boundary and provenance

Recorded: 2026-08-14 JST

Status: sanitized public snapshot integrated under `monitoring-v4/` in the
`live-stream-systems-case-study` repository. This integration does not create a
deployment, service restart, credential operation, or production action.

The integrated subtree was copied from standalone sanitized public commit
`5e61e2b7dbbd09f014746c35e50c406ead755561`. Its private implementation lineage
and public-only adaptations remain recorded below.

## What this repository is

This repository is a reviewed, secret-free public mirror of the credential-free
Monitoring v4 subsystem. It preserves meaningful implementation milestones
without copying the private `.git` directory or high-resolution operational
logs.

It is not the production source of truth, a live-health endpoint, or proof that
an elapsed evidence gate has completed. The machine-readable phase snapshot is
historical provenance assessed in the private environment on 2026-08-14 JST.

## Milestone mapping

| Public commit | Private source | Meaning |
| --- | --- | --- |
| `679223e60a7c59f026209df2cc2a7edb0a9a40da` | `8047d911ec9c6c6deb2bd553cdc42ac8c8eb90c4` | isolated R0-R5 contracts, SQLite, adapters, incidents, intents, and credential-free providers |
| `b6dec7fb6157bb547954ce11a40f5f48361f30ba` | `de10bde7693881bec4574158c26d00c13a073c39` through `2b53a839eeb50565a30e2074441771f66189caa2` | revision-pinned R2 coverage, R3 parity, R6 projections, and wall-clock service examples |
| `074bdb571fe73fadc1794a62b199398db6cfc4f3` | `832f4482268809dfa28c4b736d601d9c65b3e091` | safe-input, PostgreSQL repository, k3s workload split, migration, and detection-only sentinel |
| `1ab42730fe32b2f18716f5a279ff9a4ad9ac3b65` | public-only changes | portable paths/tests, release documentation, CI, and full-tree validation |
| `139ca84f84de6df73f1a13ba584abebc36dadabd` | private behavioral identity `73aae2f9eb4db9e9ddbab79823fa5ea2d52fb3f9` | responsibility refactor, schema-v6 durable publication, lifecycle/rollout evidence, backup/restore/retention hardening, and fault-injection coverage |
| `610ef8e` through `997863c` | private commits `eaefcd9` through `8f2c1aa` | source-aware parity, semantic-current replacement, stateful authority harness, historical replay, and RC evidence-lineage snapshots |
| `57cbb93` | public-only change | removes a campaign-controller harness whose private package dependencies were not published |
| `0a3c105` | public-only change | records the public failure-mode mitigation boundary |
| `e37fb06` | private worktree candidates identified by file SHA-256 in `docs/soak-checkpoints.md` | read-only, revision-bound soak checkpoint evaluation |

The private deployment content identity
`ef2f0b0d5ed9e0f5fefac6d4ef1f72d508ae2afa` is intentionally recorded as a
different identifier. It covered deployment inputs and was compared with the
private live release; it is not a Git commit and does not mean a public commit
was deployed.

The newer `73aae2f...` identifier has the same boundary: it is a deterministic
content identity over reviewed private behavior and deployment inputs. The
private source tree was dirty, so it must not be presented as a private Git
commit. The public `139ca84...` tree differs by portable paths, explicit
external-source arguments, self-contained fixtures, and release sanitation.

## Included

- versioned contracts, implementation, deterministic fixtures, and unit tests;
- generic credential-free systemd and k3s examples;
- PostgreSQL schema/repository, SQLite import, role bootstrap source, safe-input
  generation boundary, backup/restore/retention Jobs, and sentinel source;
- a historical machine-readable phase snapshot and an example ownership
  inventory;
- public architecture, status, database rationale, provenance, CI, and
  snapshot validation, including a failure-mode register that explicitly
  records unresolved and not-yet-proven boundaries;
- a current repository-only harness-engineering draft that separates
  implemented stateful evidence, proposed failure injection and chaos work,
  and unchanged-revision production gates.

## Excluded

- private Git metadata and remote configuration;
- raw state, database files, WAL, dumps, backups, logs, captures, caches,
  build output, virtual environments, and private ops-log bodies;
- credential values, tokens, webhooks, passwords, and private keys;
- private home/archive paths, private network addresses, and raw live unit
  dumps;
- a real notification provider, notifier Pod, production credential handoff,
  Dell runtime executor, Raspberry Pi internals, or automatic k3s restart.

Kubernetes `secretKeyRef` names and credential environment *names* are part of
the deployment contract; no corresponding values are present.

## Validation gate

The assembled history is accepted only after all of the following succeed:

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`;
- `python3 ops/scripts/validate_public_snapshot.py`;
- `python3 -m compileall -q src tests ops/scripts`;
- `git diff --check` and `git fsck --strict`;
- `gitleaks detect --redact --source .` over the full public Git history;
- author/committer identity review for every public commit;
- confirmation that the integrated source tree passes the parent repository's
  non-mutating CI and secret scan before push.

For the `139ca84...` implementation milestone, the gate additionally recorded:

- `242 collected / 236 passed / 6 skipped` on the host;
- the same result in a fixed Python 3.13, network-disabled, read-only audit
  container with Git added only to that temporary audit image;
- all six skipped integration tests passing on a disposable PostgreSQL 17
  database after schema versions 1 through 6 were migrated;
- a production-image build and `pip check` success;
- k3s rendering to 19 resources and 1,277 lines;
- a 228-file public snapshot scan with zero findings and a no-Git worktree
  secret scan with no leaks.

## Known incomplete gates

- R2 still requires seven days of source coverage for one fixed revision pair.
- R3 still requires fourteen days of classified semantic parity for that pair.
- R5 has no real provider, credential handoff, production single-writer cutover,
  or required soak.
- R6 has no production exporter/public-source cutover.
- R7 typed runtime command transport/executor and R8 production authority
  retirement are not implemented here.
- PostgreSQL is one replica on the same single-node k3s failure domain; this is
  not database or node high availability.
- The current dual-device backup is still same-host protection. Off-host
  recovery, another node, and site-level RPO/RTO remain outside this milestone.
