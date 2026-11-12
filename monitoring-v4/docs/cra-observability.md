# CRA observation, audit logs and notifications

This compatibility path exports fixed read-only CRA facts. It does not become
a recovery decider, grant authority or complete the Monitoring v4 phase gates.
The [dated checkpoint](../../docs/v3/cra-observability-closure.md) separates
operated evidence from this public source port.

| Owner | Input and responsibility |
| --- | --- |
| Delivery host | Existing CRA DB/status, runtime observation and Pod API observer status; fixed `monitoring_facts.py dell` projection |
| Central host | Existing policy/audit DB and latest verifier report; fixed `monitoring_facts.py central` projection |
| Monitoring host | Fixed SSH/helper reads, Prometheus metrics, diagnostic log and notification ledger |
| Public publisher | Public-safe aggregate only; no internal DB, v4 core or control feedback |
| Relay host | Existing media/fallback facts; no CRA policy or incident ownership |

The helper opens SQLite with `mode=ro` and `query_only`, bounds its response,
allowlists audit payload fields and omits credentials and free-form errors.
It reads the existing host identity contract but does not infer authorization
from it. Generic public identities are `delivery-host` and `authority-host`.
An installation must review and pin the actual identities and source hashes.

## Source layout and staging

From the repository root, stage these files into an owned empty directory:

```sh
mkdir -p /tmp/cra-monitoring-stage
cp monitoring-v4/src/stream_monitoring_v4/compatibility/cra_*.py /tmp/cra-monitoring-stage/
cp recovery-control/tools/monitoring_facts.py /tmp/cra-monitoring-stage/
cp ops/selfheal/pod_api_observer.py /tmp/cra-monitoring-stage/
cp src/stream_core/common/notification_gateway_client.py /tmp/cra-monitoring-stage/
sha256sum /tmp/cra-monitoring-stage/*.py
```

The delivery/central helper release needs `monitoring_facts.py`, with
`pod_api_observer.py` beside it on the delivery host. The monitoring release
needs the three `cra_*.py` files and the shared gateway client beside them.
The shared client owns the UNIX HTTP boundary; this port adds no webhook
implementation or credential. Keep a reviewed manifest and immutable directory
for each staged release; translated public hashes differ from private releases.

`deploy/cra_observation.config.example.json` is deliberately incomplete. Replace
zero helper hashes, confirm the agent instance and initial sequence against the
existing log/cursor, configure fixed SSH known hosts and narrowly scoped helper
permission, and prepare owned state/log directories. Do not accept arbitrary
remote commands or reset an existing cursor, ledger or UNKNOWN to bootstrap.
The helper's fixed config/status paths must match the installation.

The systemd templates are inert. Review the actual UID, directory permissions,
SSH access, gateway socket and `@RELEASE@`/`@USER@` substitutions before any
separately authorized installation. No CI job installs or enables them.
The rules describe an installed enforce path; pointing them at the public
`dry_run` defaults reports an authority mismatch rather than activating effects.

## Data and alert semantics

Collection and scraping run about every 15 seconds. Source freshness is 60 seconds;
agent/target timestamps have 45 seconds for collection/scrape delay. These do not
change CRA's own 10-second freshness or the observer's 4-second API / 5-second
socket deadlines. Central verification becomes stale after 480 seconds.
Epoch serialization retains 17 significant digits; rounded epoch timestamps once
caused false stale alerts and are covered by a round-trip regression.

`cra_monitoring_issue=0` needs fresh source facts, the matching agent instance,
policy/authority consistency, no unresolved outcomes, current central audit,
Loki ingestion and a fresh notifier. PID, mode or an old successful snapshot
alone is insufficient. Missing facts never become delivery-failure evidence.

The Prometheus files define 11 rules. Ten carry
`notification_owner=cra_monitoring` and are skipped by the general notifier.
`CRANotificationMonitorUnavailable` belongs to the general notifier, which
provides independent coverage of the dedicated worker. Review this ownership
together with the existing route configuration to avoid duplicate alerts.

The dedicated notifier uses a durable route-specific SQLite ledger. It records
send admission before I/O, converts interrupted sends to UNKNOWN on restart,
does not retry UNKNOWN, and retries clear temporary rejection at most 5 times.
Missing, broken or stale rules cannot resolve an episode. A resolved alert
means a new normal Prometheus evaluation; it does not establish media recovery.
Tests use injected senders or local fake servers, never real providers.

Loki reads `{app="cra",source="cra_monitoring"}` through the existing Alloy
pipeline. `ts_utc` is monitoring ingest time; `source_ts_utc` is the source audit
event time. The log is fsynced before cursor advancement; replay may repeat an
event_id, so count audit records by instance/seq rather than raw line count.
The diagnostic copy never replaces the source DB. Rotation bounds local log
payload to about 80 MiB; preserve the cursor and delivery ledger on rollback.

The Alloy fragment assumes `/var/log` is mounted read-only at `/host/var/log`
and an existing `loki.process.jsonl` receiver. The Grafana dashboard and public
CRA aggregate are examples, not a new monitoring or recovery authority.

## Local verification

```sh
cd monitoring-v4
PYTHONPATH=src python3 -m pytest -q tests/test_cra_monitoring.py tests/test_twitch_alert_notify.py
```

From `recovery-control`, run
`python3 -m pytest -q tests/reflex/test_monitoring_facts.py` to check read-only DB
access and payload filtering. The root observer suite exercises real local
TLS/API and UNIX socket boundaries. No live SSH, provider delivery, Kubernetes
mutation or stream restart is part of these checks.
