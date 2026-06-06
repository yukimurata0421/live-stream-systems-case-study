# 28-Day Same-URL SLI Case Study

This page is the public English version of a private June 2026 SLI review. It is
included because it shows the operating method behind `stream_v3`: define the
objective, measure the denominator, separate invariants from availability, and
publish the unresolved risks instead of hiding them behind a single uptime
number.

## What Was Ported

The public version intentionally selects only the material that helps a technical
reviewer evaluate the system:

| Source material | Public treatment |
| --- | --- |
| 14-day v2 SLI observation | Kept as the historical baseline in [`sli-methodology.md`](sli-methodology.md). |
| 28-day same-URL observation | Translated and summarized on this page. |
| v3 SLI and dashboard contract | Linked through [`v3/sli-and-dashboard.md`](v3/sli-and-dashboard.md). |
| v3 routine checks around network and encoder behavior | Condensed into the comparison, risk, and follow-up sections. |
| Raw operational logs and environment-specific paths | Not published. Only sanitized windows, denominators, and conclusions are kept. |

## Review Objective

The question was not "did a process stay up for 28 days?" The production
question was:

```text
Did the public YouTube Live identity survive 28 days without creating a
replacement broadcast?
```

For this project, the public watch URL is part of the product identity. A short
local restart can be acceptable if viewers, bookmarks, embeds, and external
links keep pointing at the same live broadcast. Creating a replacement broadcast
may keep "a stream" online while still damaging the actual product.

For that reason, same-watch-URL continuity is a production invariant. It is
measured next to availability, but it is not averaged into availability.

## Measurement Window

Requested window:

```text
2026-05-06 17:59:54 JST -> 2026-06-03 17:59:54 JST
```

The system changed shape during this window:

- early evidence came from the v2 single-host runtime and its archived SLI
  snapshot;
- mid-window evidence came from v2 runtime logs and monitoring backfill;
- later evidence came from the v3 split-plane observability monitor and Prometheus
  series.

That means the case study does not claim one perfect 28-day ratio from one
unchanged schema. It makes the safer claim: each regime had explicit evidence,
and the evidence agrees that the public URL identity was preserved.

## Headline Result

```text
Same-URL decision:
  pass

Replacement broadcasts:
  observed selected replacement actions: 0
  observed allowed replacement decisions: 0
  observed candidate-new-URL evidence: 0

Current state at the review time:
  expected video id matched the resolver-selected video id
  YouTube public watch evidence was live
  Data API evidence was live
  OAuth broadcast lifecycle was live
  OAuth stream status was active and healthy
```

The important reading is not "every sample was green." Some samples were
degraded or recoverable. The important reading is that none of those samples
justified abandoning the public URL.

## Summary Table

| Signal | Observation window | Result | How to read it |
| --- | --- | ---: | --- |
| 14-day archived same-watch-URL continuity | 2026-05-06 to 2026-05-20 JST | `3561 / 3563`, `99.944%`; local replacement count `0` | Two resolver mismatches recovered without replacement. |
| v2 strict same-URL state | 2026-05-16 to 2026-05-28 JST | `27486 / 27626`, `99.493%` | Strict `same_url_live` samples only. |
| v2 preserved-ish same-URL state | 2026-05-16 to 2026-05-28 JST | `27526 / 27626`, `99.638%` | Includes local-degraded samples where the URL was still live. |
| v3 strict same-URL state | 2026-05-29 to 2026-06-03 JST | `6558 / 6568`, `99.848%` | Strict `same_url_live` samples only. |
| v3 preserved-ish same-URL state | 2026-05-29 to 2026-06-03 JST | `6560 / 6568`, `99.878%` | Includes local-degraded samples where the URL was still live. |
| v2 YouTube watchdog OK | 2026-05-16 to 2026-05-28 JST | `3093 / 3114`, `99.326%` | Primary SLI, separate from URL identity. |
| v3 YouTube watchdog OK | 2026-05-29 to 2026-06-03 JST | `1305 / 1310`, `99.618%` | Primary SLI, separate from URL identity. |
| v2 upload within 5 Mbps | 2026-05-16 to 2026-05-28 JST | `99.964%`, p95 `4.767 Mbps` | Guardrail. |
| v3 upload within 5 Mbps | 2026-05-29 to 2026-06-03 JST | `99.694%`, p95 `4.910 Mbps` | Guardrail; less headroom after the v3/NVENC move. |
| v2 ADS-B map-source report-only OK | 2026-05-16 to 2026-05-28 JST | `1112 / 1113`, `99.910%` | Primary/report-only evidence, not a replacement trigger. |
| v3 ADS-B map-source report-only OK | 2026-05-29 to 2026-06-03 JST | `467 / 467`, `100.000%` | Primary/report-only evidence. |
| v2 Discord delivery observation | 2026-05-16 to 2026-05-28 JST | `85 / 89`, `95.506%` | Secondary SLI; degraded versus the 14-day baseline. |
| v3 Discord delivery observation | 2026-05-31 to 2026-06-03 JST | `8 / 8`, `100.000%` | Secondary SLI recovered in the v3 window. |
| v2 API quota-exceeded event rate | 2026-05-16 to 2026-05-28 JST | `10 / 21538`, `0.046%` | Guardrail; not the same as delivery health. |
| v3 API quota-exceeded event rate | 2026-05-29 to 2026-06-03 JST | `0 / 8319`, `0.000%` | Guardrail. |

## What Got Worse Versus The 14-Day Baseline

The 28-day review is valuable because it does not only celebrate the pass. It
also records the weaker areas.

### Same-URL Sample Ratio

The 14-day archived same-watch-URL ratio was `99.944%`. Later strict sample
ratios were lower:

- v2 strict same-URL state: `99.493%`
- v3 strict same-URL state: `99.848%`

This is a real degradation in sample cleanliness, but it is not evidence of an
actual URL change. The deciding evidence remained:

```text
candidate-new-URL evidence: 0
replacement allowed decisions: 0
selected replacement actions: 0
```

The lower ratios came from recoverable or local-degraded samples. Those should
be investigated, but they should not be reported as lost URL identity.

### Upload Headroom

The v3/NVENC window had less upload headroom:

```text
14-day baseline:
  within 5 Mbps = 99.819%

v2 later window:
  within 5 Mbps = 99.964%
  p95 = 4.767 Mbps

v3 window:
  within 5 Mbps = 99.694%
  p95 = 4.910 Mbps
```

The v3 encoder remained inside the guardrail most of the time, but it ran closer
to the 5 Mbps ceiling. This is an operational follow-up, not a same-URL failure.

### Notification Delivery

Discord delivery was lower in the later v2 raw window:

```text
14-day baseline: 97.230%
later v2 raw:    95.506%
v3 raw:          100.000%
```

Notification delivery is a secondary SLI. It matters because operators need
reliable incident visibility, but it is not proof that the live stream identity
or delivery path failed.

## What Improved Or Stayed Stable

- YouTube watchdog OK improved from the 14-day baseline `98.687%` to `99.326%`
  in the later v2 raw window and `99.618%` in the v3 raw window.
- ADS-B map-source report-only checks stayed at or above the baseline:
  `99.894%` baseline, `99.910%` v2 raw, `100.000%` v3 raw.
- API quota pressure was not the active failure driver in the v3 window.
- Recovery events occurred, but they did not lead to a replacement broadcast.

## Remaining Gaps

The review deliberately kept the following unknowns visible:

- There was no single exact 28-day ratio from one unchanged log schema.
- Viewer-visible interruption seconds were still not measured directly through
  YouTube player behavior.
- Full YouTube broadcast inventory audit was not included in this review.
- The v3 upload guardrail had less headroom and needed continued 30-day
  monitoring.
- The next SLO view should use rolling 30 days and keep replacement count,
  candidate-new-URL evidence, and current video-id agreement as the primary URL
  identity checks.

## Engineering Takeaway

The strongest result is not that the dashboard looked green. The strongest
result is that the system had enough classified evidence to say:

```text
The live URL identity survived the review window.
Availability, upload, visual source health, notification delivery, and API quota
were measured separately.
Known weak points were recorded instead of hidden.
```

That is the reliability lesson this repository is meant to show.
