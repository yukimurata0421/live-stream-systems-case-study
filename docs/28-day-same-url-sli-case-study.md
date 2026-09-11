# Historical 28-Day Same-URL SLI And Current Continuity Checkpoint

This page has two deliberately separate evidence layers. The fixed June 2026
review preserves the original 28-day SLI windows and denominators. The later
checkpoint extends only the exact-video-identity claim through a retained
September ledger endpoint. It does not recompute the historical ratios or turn
them into a moving uptime number.

Together they show the operating method behind `stream_v3`: define the
objective, measure the denominator, separate invariants from availability, and
publish unresolved risks instead of hiding them behind one percentage.

## At-Least-127-Day Continuity Checkpoint

Retained evidence from `2026-05-06 10:36:17 JST` through
`2026-09-11 00:04:12 JST` covers `127 days, 13 hours, 27 minutes, 55 seconds`.
The same public YouTube Live identity was selected at the start, across the
production cutover from the v2 single-host runtime to the v3 k3s split-plane
architecture, and at the endpoint.

| Evidence item | Observed value | Interpretation boundary |
| --- | --- | --- |
| Measurement start | `2026-05-06 10:36:17 JST` | Start of the retained 14-day same-URL observation. |
| Measurement endpoint | `2026-09-11 00:04:12 JST` | Latest retained arena-server daily ledger checkpoint reviewed for this publication; this is not an uninterrupted-frame denominator. |
| Expected video ID | `OpMzOBFwM7M` | The configured identity contract. |
| Selected video ID at endpoint | `OpMzOBFwM7M` | The ledger's selected and expected ID hashes both match the SHA-256 of this public ID. |
| Daily ledger coverage | `107 / 107` JST days from `2026-05-28` through `2026-09-11` | One baseline plus 106 daily checkpoints; this daily ledger begins at the V3 handoff evidence, not the May 6 measurement start. |
| Ledger URL transitions / candidate-new-URL / force-live | `0 / 0 / 0` | All 107 rows retain one selected ID hash, one expected ID hash, and one live URL hash. |
| Selected replacement actions | `0` observed across retained review windows | This is not a full YouTube broadcast inventory audit. |
| Candidate-new-URL samples | `2` transient samples in the initial 14-day window | Both samples came from one short resolver mismatch interval and recovered without selection. |
| V2 stopped | `2026-05-28 22:29:43 JST` | Final V2 runtime state was `stopped`; its resolver selected `OpMzOBFwM7M`. |
| First retained V3 production-send evidence | `2026-05-28 22:41:31 JST` | A TCP send sample measured `4.900 Mbps`; the exact authority handoff was not separately logged. |
| First V3 public identity evidence | `2026-05-28 22:57:22 JST` | Expected and selected IDs were both `OpMzOBFwM7M`; public-watch evidence was live. |
| First fully healthy V3 identity sample | `2026-05-28 22:59:53 JST` | Local ingest, public evidence, and the same video ID were all healthy. |

The defensible claim is URL identity preservation without a selected replacement
broadcast. It is not a zero-downtime or frame-continuity claim. The retained
V2-stop to first V3-send evidence gap is `11 minutes, 48 seconds`, so exact
viewer-visible interruption during the handoff remains unknown.

The arena-server ledger retains hashes rather than the plain watch URL. At the
endpoint, its selected ID hash and expected ID hash both equal the SHA-256 of
`OpMzOBFwM7M`, and its URL hash equals the SHA-256 of
`https://youtube.com/watch?v=OpMzOBFwM7M`. Database integrity was `ok` in the
read-only `2026-09-11 JST` evidence query. The endpoint remains a retained
evidence cutoff, not an automatically moving current-state claim.

## Current Specification Mapping

The system contract evolved after the June review. The historical values above
remain unchanged; the current public source interprets them through these
boundaries:

| Concern | Current contract | Effect on this report |
| --- | --- | --- |
| Same-watch-URL identity | A production invariant, evaluated separately from availability and input quality. | The zero-transition identity ledger is the deciding continuity evidence; degraded samples are not silently converted into URL loss. |
| Rolling feedback | Rolling 24h, 7d, and available-30d windows require explicit coverage, freshness, and source identity. | Rolling values are operational feedback, not replacements for the fixed 14-day, 28-day, or 127-day evidence windows. |
| YouTube lifecycle evidence | Raw OAuth/Data API, local delivery, and public retrieval evidence keep distinct freshness and authority. | A cached wrapper timestamp, public-page result, or metric-zero sample cannot establish current identity loss by itself. |
| Monitoring v4 | `arena-server` owns observations, current state, incidents, parity, notification intents, and a facts-only projection. | Monitoring can explain a mismatch but cannot authorize a restart or replacement broadcast. |
| Central Restart Authority | `cra-01` owns policy, budget, cooldown, authorization, command lifecycle, and final verification. | Historical `replacement allowed` fields are evidence from the earlier decision path, not current CRA authority. |
| Dell effect boundary | Dell may execute one exact, fenced FFmpeg-child effect only after target identity agrees. | Missing or ambiguous results become `OUTCOME_UNKNOWN` and require reconciliation; they do not authorize a second attempt. |
| Connectivity loss | Physical connectivity failure suppresses repeated child launches and broader restart escalation. | A network outage is not treated as evidence that the YouTube identity should change. |
| Upload pressure | The 5.0 Mbps ceiling remains a guardrail; 5fps/3400k is the normal encoder contract and 2500k is bounded emergency behavior. | Upload values remain separate from URL identity and cannot authorize restart or bitrate changes alone. |
| Public and external evidence | Public snapshots and external probes are supporting, read-only evidence. | They cannot independently burn the formal SLO or grant recovery authority. |

The current contracts are
[`v3/sli-and-dashboard.md`](v3/sli-and-dashboard.md),
[`v3/operational-reliability-and-external-evidence.md`](v3/operational-reliability-and-external-evidence.md),
[`v3/scoped-recovery-authority.md`](v3/scoped-recovery-authority.md),
[`Monitoring v4 architecture`](../monitoring-v4/docs/architecture.md), and
[`why CRA exists`](../recovery-control/docs/why-cra.md). The public CRA policy
remains production-disabled; source and Harness tests are not deployment or
live-effect evidence.

## What Was Ported

The public version intentionally selects only the material that helps a technical
reviewer evaluate the system:

| Source material | Public treatment |
| --- | --- |
| 14-day v2 SLI observation | Kept as the historical baseline in [`sli-methodology.md`](sli-methodology.md). |
| 28-day same-URL observation | Translated and summarized on this page. |
| Current SLI, Monitoring v4, and CRA contracts | Mapped to the historical evidence without retroactively rewriting it. |
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
  initial 14-day candidate-new-URL samples: 2 transient samples, not selected
  later v2/v3 raw extraction: 0 candidate-new-URL samples

State at the historical review endpoint:
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
initial 14-day candidate-new-URL samples: 2 transient samples, not selected
later v2/v3 raw extraction: 0 candidate-new-URL samples
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
to the 5 Mbps ceiling. This was an operational follow-up, not a same-URL
failure. A later July correction restored the normal 5fps/3400k profile after a
temporary 2500k value had been left active; that later incident does not alter
the May-June measurements on this page.

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
- The May-June v3 upload window had less headroom. Later encoder corrections do
  not retroactively change that measured window.
- Current operator feedback uses rolling 24h, 7d, and available-30d windows
  with coverage, freshness, and revision gates. Those windows do not replace
  the fixed historical review or exact-identity ledger endpoint.
- The public source does not prove that Monitoring v4 or CRA is the deployed
  production authority, that a CRA soak completed, or that a live effect
  occurred.

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
