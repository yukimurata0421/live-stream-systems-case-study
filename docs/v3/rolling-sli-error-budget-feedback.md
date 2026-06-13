# Rolling SLI And Error-Budget Feedback

This page is a public-safe summary of a point-in-time rolling SLI review from
2026-06-13 JST. It is included because it shows how `stream_v3` reads current
dashboard windows without confusing them with the historical 14-day and 28-day
case-study claims.

The source review used private Prometheus/report evidence. This public version
keeps the window sizes, denominators, burn numbers, and decisions, but excludes
raw queries, exact live video identifiers, private paths, and host-specific
state payloads.

## Why This Matters

The useful SRE signal is not that a dashboard can show many ratios. The useful
signal is that each ratio is tied to a different decision boundary:

- same-watch-URL preservation is a production invariant;
- YouTube availability and input quality are product-health SLIs;
- upload p95 and raw over-budget seconds are guardrails;
- visual and audio checks are sampled correctness evidence;
- dashboard metric gaps are not automatically delivery outages.

Rolling windows are feedback. They help the operator decide what to inspect
next. They do not replace the public 14-day SLI baseline or the 28-day same-URL
case study.

In this snapshot, rolling 24h, 7d, and available 30d windows were read as
feedback windows with explicit retention limits.

## Feedback Snapshot

Point-in-time review: `2026-06-13 JST`.

| Signal | Feedback window | Burn / result | Reading |
| --- | --- | ---: | --- |
| YouTube availability | rolling 7d feedback | `7.0 / 100.8 min` burned | Within the 99.0% budget for the feedback window. The recent burn aligned with a known WAN/session/route recurrence rather than a YouTube lifecycle mutation. |
| Same URL preservation | rolling 7d feedback plus replacement evidence | metric-zero `19.0 min`; actual URL burn `0`; replacement count `0` | Metric-zero samples were treated as stale/unknown observability evidence because resolver/watchdog identity and replacement evidence did not show URL loss. |
| Upload ceiling | rolling 24h feedback | p95-above-5.0 `0.0 min`; raw over-budget `195 sec` | The steady-state tuning signal stayed inside the p95 guardrail; raw spikes remain correlation evidence, not automatic restart or bitrate-change authority. |
| YouTube input quality | rolling 7d feedback | `84.0 / 100.8 min` burned | Still within budget, but high enough to watch closely before accepting any lower-upload encoder trade-off. |
| Visual correctness | sampled 7d checks | `0 / 1342` bad samples | Sampled display evidence stayed healthy; this does not prove every delivered frame. |
| Audio correctness | rolling 7d feedback | `0.0 / 50.4 min` burned | Pulse/audio energy checks stayed inside the feedback budget. |

The 24-hour view was read as fast feedback, not as a long-window reliability
claim. The available 30-day trend was also treated as trend-only when the
retention coverage did not span a complete 30 days.

## Decision Rules

### Same URL Metric-Zero Is Not Enough

Same URL budget burn requires actual URL identity evidence:

```text
expected/current video identity agreement
candidate-new-URL evidence
replacement action selection
replacement action allowance
public/API/OAuth live evidence
```

If only the dashboard metric is zero while the identity evidence still agrees,
the correct first classification is stale or unknown observability evidence.
That is handled through the observability-plane self-check path, not by creating
new delivery authority. Put plainly: not by creating a replacement broadcast or
restarting delivery.

### Upload Uses Two Numbers

Upload is read with two separate signals:

- p95 against the 5.0 Mbps warning ceiling;
- raw over-budget seconds for short spikes.

The p95 signal protects the steady-state shared-line budget. Raw over-budget
seconds are useful for correlation with TCP stalls, reconnects, or encoder
changes, but they do not independently authorize a runtime restart. This is the
same rule used by scoped recovery: upload pressure alone is not executor-owned
recovery authority.

### Input Quality Can Block Upload-Only Tuning

Lower upload is not automatically better. If YouTube input-quality burn is
recently high, encoder changes should not chase upload efficiency without
checking whether the lower-upload profile makes YouTube classify the input as
unhealthy.

This is the operational reason the public encoder case study keeps nominal
bitrate, measured RTMPS send, YouTube warnings, and visual quality as separate
evidence.

### WAN Recurrences Stay In The Fault Layer

The reviewed burn included a short WAN/session/route recurrence. The response
is to read the TCP stall evidence stack:

```text
persistent anchors
fresh TCP anchors
RTMPS socket state
route/address events
same-URL and YouTube lifecycle evidence
```

It is not evidence to redefine the SLO, replace the YouTube broadcast, or lower
bitrate by default.

## Public Boundary

This page intentionally does not publish:

- raw Prometheus queries or time-series payloads;
- exact current YouTube video identifiers;
- private Grafana/Loki links;
- private host paths or generated state files;
- public IP values from WAN observers;
- audio/capture artifacts.

The public value is the reasoning contract: measured windows, denominators,
budget burn, and the decisions that the numbers did and did not authorize.

## Review Signal

A reviewer should evaluate whether the system keeps the objective function
coherent under mixed evidence:

- URL identity is not averaged into availability.
- same-URL metric-zero samples are challenged against raw identity evidence.
- Upload spikes remain guardrail evidence until correlated with a fault.
- YouTube input quality can veto lower-upload tuning.
- Rolling feedback is labeled separately from long-window public SLI claims.
