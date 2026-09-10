# Public Release Boundary

Recorded: 2026-09-10 JST

Status: local sanitized snapshot; no remote, push, tag, deployment, service
restart, credential operation, or production action was created by this
publication work.

## Provenance

The snapshot was exported from private source commit
`fb7ffa3d96c4cf23637d72a5cbc6f65fac4d008c` without its Git history. Exported
areas were then reviewed and sanitized in a new repository history.

The private source working tree was clean when this provenance was refreshed.
Later local changes, if any, are outside this exact source identity and cannot
be inferred from the public commit.

## Included

- authority, monitoring projection, Dell Agent, runtime boundary, no-action
  soak, maintenance, snapshot projection, and harness packages;
- versioned JSON schemas and ordered SQLite migrations;
- the explicit disabled-production recovery policy;
- deterministic unit, integration, chaos, negative-control, and release tests,
  with a machine-readable public-CI exclusion manifest;
- inert systemd, PostgreSQL, Kubernetes, logrotate, and needrestart examples;
- build, validation, replay, and harness tools; and
- curated design-rationale, architecture, and safety documentation, including
  repository-only harness-engineering, failure-injection, and chaos-testing
  drafts.

## Excluded

- private Git metadata and remote configuration;
- operational records, incident bodies, raw evidence, and soak artifacts;
- databases, WAL/SHM, logs, captures, caches, build products, and virtual
  environments;
- certificates, private keys, tokens, webhooks, credentials, and real
  credential paths;
- private home paths, internal endpoint addresses, and exact live release
  directories; and
- deployment claims or authorization to enable production behavior.

RFC 1918 addresses that remain under tests are synthetic fixtures used to
exercise private-listener validation. They are not retained live endpoints.

## Public adaptations

- private workspace and release paths were replaced with portable `/srv`,
  `/opt`, and `/var/lib` examples;
- host-contract examples use a repository-specific generic path;
- high-resolution private documentation was replaced with this curated set;
- generated package metadata was excluded; and
- public validation rejects private workspace paths and common secret or
  runtime-artifact classes.

## Verification

The public snapshot is accepted only after Python compilation, public snapshot
validation, the selected repository-only test surface under the hash-pinned
SQLite 3.51.3 runtime, diff checks, Git object checks, and redacted secret
scanning succeed. The retained but excluded tests
are listed with reasons in `tests/public_ci_exclusions.json`; none is converted
into evidence for a missing private environment, artifact, workflow contract,
or sibling-repository candidate.

## Operational warning

Files under `ops/` and several tools construct production-shaped commands.
They are review examples, not an installation request. A real deployment must
reacquire host identity, live source, running release, failure domain,
authorization, rollback, and stop conditions for every target host.
