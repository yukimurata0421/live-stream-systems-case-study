# Notification Diagnostic Boundary

This article explains what the `stream_v3` notification stream can diagnose by
itself, and where it intentionally stops. It is public-safe: it describes the
evidence model and message shape without publishing raw incident logs, private
paths, live video identifiers, hostnames, IP addresses, or generated state
payloads.

## Problem

The early notification feed was useful for paging, but too coarse for later
analysis. A repeated status message could say that an incident was still active
and show a short evidence string such as:

```text
current_fail=true
network_down
latest report missing
```

That was enough to know that the operator should look, but not enough to answer
the questions that matter during review:

- was this a delivery fault or an observability/report gap;
- did YouTube lifecycle state change, or was the same watch URL preserved;
- did RTMPS stop sending bytes, or did only a public probe fail;
- did recovery actually restore send throughput;
- was ADS-B overlay data missing, stale, or only temporarily unreachable;
- what evidence is still needed before assigning cause to the CPE, carrier, or
  YouTube ingest edge.

The notification layer now treats the Discord message as the first diagnostic
summary, not merely as an alert title.

## Message Model

Each active incident keeps the original incident fields:

```text
component
severity
window
issue
recovery_type
evidence
follow_up
```

It also carries a compact diagnostic context when the source evidence can
support it:

```text
context=pass=... current_fail=... historical_degraded=...
        fast_recovery_1h=... fast_recovery_24h=...
        fr_triggers={...}
        upload_p95=... upload_max=...
        public_probe=... api_report=...
```

For fast-recovery auto-recovered events, the context is transport-specific:

```text
context=pre=-60s mbps=... delta=... lastsnd=... notsent=... unacked=... pid=...
        post=+60s mbps=... delta=... lastsnd=... notsent=... unacked=... pid=...
        recovery_lag_sec=...
```

That keeps the notification small while preserving the decision-grade facts:
what was bad, what changed, and what evidence proved recovery.

## What The Notification Can Separate

| Question | Notification evidence | What it can decide |
| --- | --- | --- |
| Is this a current delivery fault? | `current_fail`, YouTube judgment, pulse/audio status, current pass state | Separates active stream failure from stale history. |
| Is this a report or dashboard problem? | report state, age, stale threshold, `api_report`, public probe judgment | Separates observability gaps from delivery mutations. |
| Did RTMPS transport stall? | pre-restart `mbps`, `bytes_sent_delta`, `lastsnd`, `notsent`, `unacked` | Shows whether the sender stopped moving bytes or only carried a warning label. |
| Did recovery restore send throughput? | post-restart TCP sample and `recovery_lag_sec` | Proves restart completion plus delivery recovery at sample granularity. |
| Is YouTube lifecycle mutation justified? | same-URL policy context, public/API/OAuth/local split, recovery type | Keeps broadcast replacement out of ambiguous transport or probe incidents. |
| Is ADS-B overlay data bad or missing? | report-only judgment, report age, warning list, position/message deltas | Separates missing overlay fetches from source-data movement problems. |
| Is API quota evidence usable? | open-day/closed-day freshness and timer activity | Separates API report warmup/staleness from delivery health. |

This is enough for first-pass triage: choose the right investigation lane
without touching the delivery runtime unnecessarily.

## What It Still Cannot Prove

The notification is deliberately not a root-cause oracle.

| Boundary | Why the notification cannot prove it alone | Required deeper evidence |
| --- | --- | --- |
| CPE versus carrier ownership | A WAN/session event can make RTMPS, fresh anchors, and persistent anchors fail together from the stream system's point of view. | CPE logs, WAN settings, session events, carrier-side timing, and retained WAN observer history. |
| YouTube ingest edge fault | A YouTube warning is not enough if non-YouTube anchors also fail. | Comparison between RTMPS, Cloudflare/Google anchors, API/OAuth state, and public watch evidence. |
| Exact recovery second | TCP send samples are periodic and may be coarser than the actual restart decision. | Higher-cadence burst samples, process lifecycle events, and runtime logs. |
| Every delivered frame and audio sample | Notifications summarize health signals; they do not audit every frame or audio buffer. | Capture evidence, audio probe artifacts, and SLI sampling review. |
| Short report-only blips | A report timer can miss a very short overlay or upstream interruption. | Report JSONL around the window and, when needed, upstream service logs. |
| Public probe truth | Public watch probes can be rate-limited or bot-classified. | Authoritative OAuth/Data API/local ingest evidence and same-URL identity state. |

The correct operational posture is: notifications route the investigation;
raw evidence assigns final ownership.

## Failure-Layer Reading

The notification can classify the incident into a practical fault layer:

```text
delivery current_fail
RTMPS transport restart
report-only ADS-B overlay/upstream warning
YouTube public probe noise
API report freshness/timer issue
runtime or container lifecycle event
```

That layer is enough to decide what not to do. For example:

- do not replace the YouTube broadcast from a transport stall alone;
- do not lower encoder quality from a short upload-pressure event alone;
- do not restart the delivery runtime for a stale API report;
- do not treat a public probe cluster as viewer-facing outage while
  authoritative live evidence remains healthy;
- do not treat report-only overlay warnings as delivery authority unless fresh
  stream evidence also fails.

## Timing Recurrences

The same diagnostic boundary applies to recurring transport windows. When a
daily-like RTMPS issue shifts from one local-time window to another, the
notification can show:

- the trigger label, such as `network_down`, `tcp_stall`, or
  `low_upload_pressure`;
- the pre-restart TCP state;
- whether a post-restart sample recovered;
- whether the event aligned with other current incidents.

It cannot, by itself, prove whether the schedule moved because of a CPE setting,
carrier session policy, or an unrelated upstream maintenance window. That
assignment still needs raw WAN/session evidence.

## Implementation Hooks

The public repository keeps the portable parts:

- `src/stream_core/notifications/incidents.py`
  builds incident summaries and compact diagnostic context.
- `src/stream_core/notifications/status_loop.py`
  reads nearby fast-recovery TCP samples for auto-recovered restart messages.
- `src/stream_core/notifications/renderer.py`
  renders `context=` and non-empty recovery evidence into the notification.
- `tests/test_cli_ops_commands.py`
  verifies that fast-recovery notifications include pre/post TCP context and
  that stream-health recovery messages include current recovery evidence.
- `tests/test_critical_helper_contracts.py`
  verifies that a very recent fast-recovery event can wait briefly for a
  recovery sample before emitting an informational message.

The implementation is read-only with respect to delivery control. It improves
the evidence attached to notifications; it does not change restart authority,
YouTube lifecycle authority, encoder policy, or report-only recovery policy.

## Review Signal

A reviewer should expect the notification feed to answer:

```text
What failed?
Which layer is currently implicated?
What evidence proved recovery?
What should not be mutated yet?
What raw evidence is still needed for ownership?
```

That is the intended diagnostic boundary. Notifications should be precise
enough to prevent the wrong response, but modest enough not to pretend that a
small message can replace raw incident evidence.
