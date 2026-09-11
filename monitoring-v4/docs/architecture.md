# Architecture and safety boundaries

## Decision ownership

Monitoring v4 keeps observation and incident evidence on the arena monitoring
host. Read-only adapters create versioned observations; pure reducers calculate
current state; the incident engine creates episode transitions; and routing
creates durable notification intents. Recovery policy and command authority
belong to the separate CRA host. Delivery and runtime execution are separate
authority boundaries and are not enabled by this baseline.

The host split is intentional:

| Host role | Owns | Must not own |
| --- | --- | --- |
| Arena monitoring | observations, current state, incident/parity evidence, and facts-only projection | recovery authorization, final recovery verdict, or arbitrary remote shell execution |
| CRA authority | recovery policy, budget, cooldown, authorization, command lifecycle, and final verification | raw-source ownership or direct process signaling |
| Dell delivery | future typed, scoped command execution only | incident, SLO, route, or action selection |
| Raspberry Pi publisher | publication of an already allowlisted artifact | Monitoring v4 packages, internal state, judgment, or control feedback |

## Layering rules

Contracts cannot import implementation packages. Pure domains cannot import
adapters, storage, notification delivery, filesystem, network, or subprocess
APIs. Read-only adapters cannot import incident delivery or runtime mutation.
The credential-free R5 providers cannot import network clients. These rules are
enforced by source-tree tests, not only documented as conventions.

Within the arena subsystem, responsibility is split by behavior rather than by
adding more network services. Reporting is divided into read model,
accumulator, coverage, parity, policy, and builder modules. Sentinel collection,
storage checks, continuity, evidence decoding, assessment, and payload creation
are separate. Safe-input I/O, sanitation, lifecycle/rollout projection,
generation, and publication are separate. Observer worker supervision,
scheduling, result normalization, and persistence are also separate. Storage
services consume role-specific repository protocols, while SQLite and
PostgreSQL remain distinct backend roots.

## Input-quality authority

The raw OAuth watchdog is the current authority for YouTube input quality.
Prometheus and rolling values produced from the same watchdog are diagnostic
or historical projections. Their sampling disagreement cannot open an
incident by itself. Explicit raw warning/error can remain actionable; `noData`
and unrecognized values do not become current incidents. Independent
coverage/freshness failure and multi-window burn remain separate evidence.

## Side-effect boundary

The shadow writes only to an explicitly supplied v4-owned SQLite or PostgreSQL
backend, separate v4-owned artifact paths, and credential-free test providers.
It does not modify the migration source, production monitoring state, streaming
runtime, public publisher, or external notification systems.

For PostgreSQL cycles, schema v6 commits the cycle and a pending artifact intent
together. File publication is a retryable exact-byte reconciliation step. A
missing file, a crash before acknowledgement, or a transient filesystem error
therefore remains visible and recoverable without pretending the database and
filesystem form one atomic transaction.

## Revision-pinned evidence

Coverage and semantic parity are partitioned by both the Monitoring v4 build
revision and the effective migration-source revision. Unknown or unversioned
identity fails closed instead of extending an earlier soak. The scheduler uses
wall-clock anchors, so repeated oneshot execution does not accumulate relative
timer drift.

Formal five-objective SLI projections and the faster YouTube input-quality
feedback remain separate scopes. A non-formal projection cannot assert formal
compliance, and every projection carries `no_automatic_recovery`. The public-
safe artifact has a fixed field allowlist and omits internal identifiers and
compliance claims.

## k3s subsystem boundary

The credential-free shadow is split into a safe-input projector, a single
evidence writer, a reporter, two read-only exporters, a PostgreSQL StatefulSet,
and scheduled backup, restore-verification, retention, and migration jobs. The
projector is the only application workload that mounts the external raw state.
It has no database
configuration, service-account token, or allowed network egress. The core sees
only the fixed twelve-file projection, its versioned digest manifest, and
PostgreSQL. The projector publishes one complete generation at a time; the core
revalidates every member under the shared lock before reading adapters.

The decision writer uses one replica, `Recreate`, and a database lease. The
namespace is default-deny; PostgreSQL and exporter ports are opened only to
declared workload labels. No notifier or runtime executor is present. A host
sentinel outside k3s detects cluster and evidence freshness, but it cannot
restart k3s, send notifications, or mutate the delivery runtime.

See [PostgreSQL and k3s subsystem](postgresql-k3s.md) for the database decision,
failure-domain limit, migration order, and rollback boundary.

See [Responsibility refactor and failure hardening](hardening.md) for the module
split, crash-window contracts, fault controls, and validation evidence.

See [Failure-mode mitigation record](failure-mode-mitigation-record.md) for the
fact-by-fact distinction between prevention, fail-closed behavior,
reconciliation, detection-only controls, and unresolved boundaries.
