# Hiring Reviewer Guide

Use this page only to choose a review path. The top-level
[`README.md`](../README.md) already contains the short summary, evidence
snapshot, architecture, decisions, and claim limits.

## Choose One Path

### Non-Technical Interviewer

Read:

1. the top-level [`README.md`](../README.md);
2. the [`operational scorecard`](operational-scorecard.md);
3. the [`executive summary`](executive-summary.md).

Evaluate whether the case study:

- attaches limits to its measured results;
- separates a real production invariant from a broad uptime claim;
- shows automated recovery without hiding operator responsibility;
- calibrates its single-operator, three-home-host scale honestly.

### Backend Or Infrastructure Reviewer

Read:

1. the [`implementation review map`](implementation-review-map.md);
2. the [`runtime contract`](runtime-contract.md);
3. the [`physical topology`](physical-topology.md);
4. the [`public status boundary`](v3/public-status-snapshot.md).

Evaluate whether:

- k3s ownership is explicit for delivery and observability workloads;
- the Airspy/readsb/modified-tar1090 source chain is separated from rendering;
- private Prometheus, Loki, and Grafana stay outside the public path;
- monitoring cannot directly take ownership of FFmpeg;
- public tests map to the claimed safety contracts.

### SRE Or Platform Reviewer

Read:

1. the [`SLI and dashboard model`](v3/sli-and-dashboard.md);
2. the [`rolling SLI feedback rules`](v3/rolling-sli-error-budget-feedback.md);
3. the [`TCP stall case study`](v3/tcp-stall-case-study.md);
4. the [`diagnostic resolution boundary`](v3/tcp-stall-resolution-depth.md);
5. the [`scoped recovery authority`](v3/scoped-recovery-authority.md);
6. the [`notification evidence model`](v3/notification-and-auto-recovery.md).

Evaluate whether:

- same-watch-URL continuity remains distinct from availability ratios;
- transport, WAN/session, YouTube, upload, source, visual, and audio evidence
  remain separate;
- recovery authority is blocked by stale or ambiguous evidence;
- restart observation is not mislabeled as confirmed send recovery;
- MTTR, dashboard sampling, and viewer-visible impact are not conflated;
- unresolved ownership remains explicit when public evidence is insufficient.

## Evaluation Rubric

| Area | Strong signal | Warning sign |
| --- | --- | --- |
| Evidence | Measurement window, denominator, freshness, and limitation are stated. | A dashboard label is treated as root cause. |
| Recovery | Authority, guard, and non-actions are explicit. | A monitor can mutate YouTube lifecycle or FFmpeg from one weak signal. |
| Architecture | Delivery, observation, source, and public publication have named owners. | Public status reads traverse private monitoring or home ingress. |
| Reliability | Historical SLI, rolling feedback, and current incident state stay separate. | A short drill is generalized into a broad uptime promise. |
| Operations | Known unknowns and required deeper evidence are recorded. | Missing evidence is silently converted into success. |

The complete reference catalog is in [`docs/00_INDEX.md`](00_INDEX.md). Most
reviewers should not need to read every document.
