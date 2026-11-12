# CRA migration: observation and four-host closure

Evidence cutoff: November 13, 2026, 02:27 JST. This is a dated operated checkpoint
reported from retained private evidence, not a live health endpoint. The public
source port reproduces implementation and isolated checks; it does not deploy
the operated configuration or grant its authority.

The operated minimal CRA path uses the existing typed v2 Executor boundary.
Centrally signed policy v3 is `enforce`; the delivery host evaluates it locally,
persists limits and audit, and requests at most one effect per FFmpeg generation.
The policy owner, durable fence and unresolved-outcome rules remain unchanged.
The successor runtime/v4 request adoption remains frozen. Public policy examples
remain `dry_run` and production-disabled.

## What changed and why

Pod API observation moved to a separate K3s observer using a naturally rotating
projected token and a restricted UNIX socket. The host collector retains process
identity and double target reads. The observer health probe does not import API
or TLS code, and one API request cannot consume all health-check connection slots.
The [source and render guide](../../ops/kubernetes/cra-pod-observer/README.md)
describes its bounds and portable deployment prerequisites.

The latest resource change raised the observer CPU limit from 100m to 250m with
the same image, 96MiB memory, RBAC, credentials and API/socket deadlines. An
isolated same-image comparison, with real cgroup quota and concurrent API plus
two probes, passed 8/8 trials at each limit. Maximum response was 3.510 seconds
at 100m and 1.212 seconds at 250m, about 65% lower. Neither condition failed in
that comparison, so it does not quantify a production failure-rate improvement.

The monitoring host now pulls fixed facts from delivery and central hosts,
exports Prometheus metrics, ships diagnostic audit copies through Alloy/Loki,
and owns a dedicated dual-route notification ledger. The
[monitoring guide](../../monitoring-v4/docs/cra-observability.md) covers source
identity, freshness, cursor durability, UNKNOWN handling and alert ownership.
The public aggregate adds CRA observation/audit status and labels the old
Fast Recovery counter as legacy; old records remain available.

## Retained failures

The first exporter rounded epoch values to six significant digits. Three false
stale alert types generated six firing deliveries across two routes. Seventeen
significant digits, a round-trip test and collection/scrape-aware freshness
corrected it; six resolutions and a correction on each route were accepted.
The episode, receipts and failed release were retained.

In the final 15-minute observation, three of 31 samples included pending states:
two audit-replication delays and one unavailable Loki query. They cleared before
firing. A separate Loki readiness 503 had no captured response body; its cause
remains unknown. Later ingestion and contiguous audit sequences establish the
later state without proving that the earlier condition did not occur.

Labelled isolated notification input produced firing and resolution acceptance
on both real routes in the earlier operated validation; repeating the same input
created no extra delivery. Slack messages were visible in the channel. Discord
HTTP 204 establishes provider acceptance only; channel visibility was not observed.
This public port sends no notifications.

## Four-host follow-up

| Host | Verified checkpoint and bounded result |
| --- | --- |
| Delivery | CRA enforce/HEALTHY, same main Pod/FFmpeg generation and Executor authority. ACK progressed; no new enforce or UNKNOWN action in the observed window. |
| Monitoring | Ten targets up, 63 alert rules and 3 recording rules healthy, current alerts 0. CRA audit seq 3426–3451 was contiguous; legacy logs were fresh. Source audit matched 29 producers / 22 entrypoints. |
| Public publisher | Three normal publish cycles succeeded; immutable index and 8 file readbacks verified. External HTTP reported fresh sources, 24/24 normal metrics and CRA issue 0. No v4 core or control feedback was added. |
| OCI relay | MediaMTX, keeper, YouTube copy and read-only facts retained process identity. Input and both destination output counters advanced. An obsolete route transport was disabled. |

The obsolete relay-side transport had refused startup with a host/binding
mismatch since October 31 but remained enabled. Dependency review found no
current media consumer. It was disabled once at 02:16:35 JST; unit, wrapper,
configuration and failure history were preserved. The initial verifier expected
inactive, while systemd retained the old failed marker. Read-only reconciliation
confirmed disabled/PID0 and unchanged media processes; the operation was not
resent and the failure marker was not cleared.

Five post-change samples per host covered about 121 seconds. Relay input rose
64,281,668 bytes, YouTube output 62,987,455 bytes, Twitch output 63,555,802 bytes,
and delivery ACK 63,332,462 bytes. YouTube video/audio packet counts increased
600 / 5,625. Counters use different sampling/buffering boundaries and are not
expected to match byte-for-byte. These are transport/forwarding observations,
not public-player AV continuity.

The publisher retained 832 historical failures, 24 immutable UNKNOWN attempts
and 5 older receipts. Its latest failure preceded the successful cycles; those
historical object outcomes were not all reconciled by this checkpoint. Existing
shadow compatibility history also remained intact. No media restart, Pod
replacement, network restart, production fault injection or authority/budget
change was needed for the four-host follow-up.

## Completion boundary

This closes the operated CRA migration integration described here. It does not
complete Monitoring v4's separate time/authority phases, the frozen successor
runtime, every historical UNKNOWN, a real production CRA recovery intervention
or uninterrupted public-player reception. The
[public port manifest](../../publication/cra-observability-port-20261113.json)
records the independently sanitized source and validation scope.
