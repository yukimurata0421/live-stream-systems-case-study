# Twitch Reliability Collection and Missing Data

Recorded: 2026-10-14 JST. Status: live collection and public display recovered;
historical SLO assessment remains COLLECTING.

The Twitch API observer continued reporting HTTP 200 and LIVE while the
separate reliability collector exited at 06:31:35 JST. Publication retained its
last JSON. After the 180-second freshness limit, the UI stopped rendering the
cards and history. This was a reliability-observation failure; current provider
state and historical measurement availability are separate evidence.

The deployed parser also raised `AttributeError` when a relay observation had a
null timestamp. That behavior was reproduced with a pure-function fixture.
The incident's exception output had been discarded, so the fixture does not
establish the original exit's exact cause.

## Repair

- Reject missing, malformed, naive, and invalid timestamp types without terminating collection.
- Continue eligible API observations when the relay source is unavailable.
- Commit each history update as a transaction; roll back partial writes on a failed cycle.
- Log source-state changes and safe failure metadata, without source payloads or exception messages.
- Clear the in-memory projection on a fatal failure. Publish readiness and watchdog notifications only after completed cycles.
- Keep four named cards visible as Unknown when data is stale or unavailable; suppress old percentages and history bars.

The service example uses journal output, a 90-second watchdog, and failure
restarts after 30 seconds, capped at three starts within 900 seconds. Reaching
the limit leaves the service stopped for operator review. This confines recovery
to the observation collector. It adds no provider writes or delivery actions.

The public implementation retains explicit account/revision binding supplied
by configuration. Tests use a synthetic account. The unit's reader group and
binding path are examples that require operator configuration.

## Evidence and Limits

Regression checks cover malformed observations, source loss and recovery,
transaction rollback, retained history, safe diagnostics, and completed-cycle
watchdog notification. A local Chromium fixture checks eleven UI transitions,
including fresh data, stale data, failed retrieval, recovery, and window changes.
The service example passes static unit verification.

Preparation was held after a managed host-contract change. A fresh task verified
the target contracts and live identities before applying the private collector
at 15:27 JST and the display asset at 15:29 JST. The public collector remains a
configurable source example; it was not used as the live account configuration.

Live verification at 15:33–15:36 JST observed advancing collector and metrics
timestamps, new minute rows, and retained history. All 2,026 previous minute rows,
the original start time, and the failed daily-renewal outcome were preserved.
The minute count reached 2,056. Ordinary publication updated the origin and
domain, with JSON age below the 180-second limit. Desktop and mobile Chromium
showed four cards, 24 hourly bars, and working 24-hour/7-day controls.

The existing provider observers and delivery processes were preserved. Both
provider observations reported LIVE, and their separate relay output counters
advanced. This does not establish continuous viewer reception. Native watchdog
expiry, restart-limit behavior, failure-triggered restarts, and long-term soak
remain unobserved; no production failure injection was performed.

The repair preserves source timestamps, duplicate suppression, unknown and
missing minutes, daily-renewal failures, measurement windows, and coverage
gates. Restarting collection must not erase the outage or turn missing history
into successful delivery. Display recovery and SLO qualification remain separate.

See [OCI delivery evidence](oci-relay-and-twitch.md),
[route observation](route-network-observation.md), and the
[public language policy](../public-language-policy.md).
