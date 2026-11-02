# Observation Contracts and Connected Delivery

The observation contract separates evidence validity from service health. Its
current deployment checkpoint and verification limits are in
[current status](../current-status.md). The [independent upload sampler](upload-measurement-source.md)
remains the source for current measurements and historical SLI.

## Meaning and ownership

| Dimension | Values | Meaning |
| --- | --- | --- |
| Observation validity | `VALID`, `STALE`, `UNAVAILABLE` | Whether the evidence is usable now |
| Target state | `OK`, `FAIL`, `UNKNOWN` | What it proves about the declared scope |
| Expected activity | `ACTIVE`, `STANDBY`, `MAINTENANCE`, `UNSPECIFIED` | Whether progress is expected |

Invalid observations cannot assert target OK or FAIL. A heartbeat proves that
collection ran; an ACK proves progress on that connection. Neither proves
provider playback. Generic fresh snapshots retain target UNKNOWN unless their
source contract defines a narrower health verdict.

Source identity, generations and original age are checked at the collection
boundary. Existing boot-bound clock and upload contracts are reused. Scraping or
copying a file does not renew its age. UTC remains useful for history and diagnosis;
boottimes from different hosts are never subtracted directly. Asynchronous
cross-host publication retains nonce, machine, boot and full-round-trip checks.

Recovery, veto, incident policy and notification routing retain their owners.
The common observation library grants no action, authorization or recovery timer.
Direct fallback facts remain separate from dashboards and publication.

## Connected paths

```text
sender sampler -> local adapter -> dedicated WireGuard -> owner Prometheus
sender journal -> authenticated history replica -> raw recovery proof and SLI
owner snapshots -> common adapters -> local Prometheus
owner sources and Prometheus -> sanitized bundle -> publish-only host -> public objects
existing incident owners -> common delivery gateway -> Slack / Discord
independent local notification health -> existing direct provider delivery
external heartbeat -> independent absence detector (activation pending)
```

The sender adapter has no control methods, outbound fetches or credentials. Its
service uses a dedicated dynamic identity, read-only source access, resource limits
and a scraper address restriction. A live HTTP endpoint can report UNAVAILABLE:
Prometheus `up` is not evidence that the underlying measurement is fresh.
A valid zero rate remains a measurement; missing data never becomes zero.

The common owner adapter covers YouTube watchdog, map runtime, viewer synthetic,
reliability rollup/burn, external blackbox, ADS-B, subsystem and clock-mesh sources.
It preserves their individual cadences. ADS-B source heartbeat and retained probe
snapshot have different expiry bounds. Clock synchronization failure is scoped to
clock synchronization, not stream failure.

Build metrics contain a startup manifest digest and effective configuration hash.
Expected-build rules detect mismatches, failed scrapes and missing targets. Loaded
processes and mounts are checked separately during deployment; a self-reported
hash alone is not deployment proof. Releases remain immutable after activation.

## Measurement warnings and history

Current-source selection is explicit. Missing, malformed or mismatched selection
is UNKNOWN; legacy behavior requires the exact `mode=legacy` selector. The reader
never silently falls back to a different Prometheus job.

Recording rules and the reader add elapsed time to the original source age and
reject frozen scrapes, frozen rules, mismatched evaluations and wrong builds.
Current measurement health and raw history replication are reported independently.
Their combined recovery proof still requires both sources to satisfy the existing
freshness and sustained-recovery bounds. SLO intervals, denominators and targets
are unchanged.

Measurement warnings use the receiver's boot-bound elapsed time. Short gaps and
initial collection do not immediately open an incident. Repeated bad observations
accumulate within a bounded window; stable good evidence clears that admission
history. An already open incident closes only after sustained recovery. The last
bad observation remains the incident's last bad timestamp while fresh data builds
the recovery window. History-replication and formal historical-coverage incidents
have separate identities; a recovered live measurement cannot erase past missing
SLI intervals. Exact constants and operated evidence belong in current status and
dated records rather than repeated summaries.

## Public projection

Sanitization and source queries run on the observation owner. The publisher fetches
one allowlisted bundle and validates each artifact's schema and original age.
It rewrites presentation time from the proved age on its own clock, preserving
historical event times. Partial failure retains the previous artifact with its
previous age. Reading a bundle again does not make it new.

The Reliability projection preserves the operated v6 schema and calculation
semantics. Its ledger reader uses SQLite query-only access. A checkpointed database
without WAL sidecars can be read as an immutable image only while its file identity
is unchanged and no WAL appears; active WAL data is never silently ignored.
This avoids granting the publisher database initialization or write access.

The publisher remains publish-only. SSH remains for this authenticated bundle and
for historical raw replication; it no longer distributes multiple query and
classification implementations across the publication host.

## Shared notification delivery

Incident owners keep severity, repeat, recovery and outbox policy. A Unix-socket
gateway is the common transport for the native and standalone notifiers. Peer
credentials restrict callers; fixed configured HTTPS destinations prevent arbitrary
forwarding. Success means the provider accepted the request, not merely that a
queue accepted it.

A durable receipt is written before dispatch. Accepted repeats are deduplicated;
explicit rejection may be retried. A timeout or crash after dispatch is UNKNOWN
and is held for reconciliation instead of blind resend. Receipts are bounded and
do not store message content or webhook secrets. An independent local health
process reports unavailable delivery or unresolved outcomes through its existing
direct route. That exception prevents the gateway from being its own only alarm.

Alertmanager was not inserted as a transport shim: doing so would change the
existing provider-acknowledgment and fallback semantics. Shared delivery is
implemented; incident ownership has not been collapsed into a new policy engine.

The external heartbeat checks fresh, same-boot gateway and natural notification
cycles plus Prometheus readiness. Its prepared Cloud Monitoring absence policy
requires an explicit project-bound notification channel. A local dry run is not an
external deadman qualification. Activation requires a baseline point, independent
absence detection and observed provider delivery. The
[Cloud Monitoring API](https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/create)
accepts heartbeat points; the
[absence condition](https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies)
is evaluated outside the monitored host. Credential, network and clock failures
can also stop heartbeats; the resulting alert is about monitoring availability.

## Retirement and rollback

The duplicate Kubernetes Prometheus and Grafana were retired only after checking
actual consumers, routes and repair entries. The watchdog was first redirected to
the existing host services and observed naturally. Replica counts were then set
to zero with UID/version checks; storage and configuration were retained. The old
LAN-only upload endpoint was retired after the dedicated tunnel qualified.
The multi-purpose legacy exporter and raw replica still have consumers and remain.

Rollback restores consumer selectors, loaded entrypoints and publication first,
then removes only the added scrape targets or services. Keep the old data and
configuration. Do not restore old incident/outbox/database snapshots, clear UNKNOWN
receipts, reset budgets or restart media to roll back observation code.

## Verification boundary

Tests cover the real isolated HTTP/Prometheus/projection path, stale sources,
exporter loss/restart, epochs, bad selectors, warning admission, read-only ledgers,
public nonce/replay/partial failure, delivery receipts and deployment alerts.
Operated evidence separately records natural consumers, sustained raw/Prometheus
freshness, publication cycles and real provider HTTP acceptance.

The public browser probe received and advanced video, then showed a player error.
It does not qualify continuous reception or audio, and does not establish that
an internal service caused the player error. No production failure was injected.
Full external deadman delivery and long-duration receiver qualification remain
separate gates, not implied by source or unit-test success.
