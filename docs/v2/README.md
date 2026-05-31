# stream_v2 Historical Summary

`stream_v2` was the refactored single-host runtime. It introduced the subsystem
model that later made the v3 split possible.

## What v2 Added

- clearer runtime ownership
- watchdogs and recovery policy
- SLI summaries
- YouTube API cost guards
- report-only observability paths
- restart budgets
- same-URL preservation gates
- YouTube OAuth/channel mutation guards
- current-vs-historical incident classification
- contract tests
- runbook-driven operations
- low-bandwidth media tuning from 5fps/3500k/audio192k to
  4fps/3400k/audio192k as upload and YouTube-health evidence accumulated

## Why v2 Was Not Enough

The delivery path and observability path still shared one host and one process
ownership model. That made it harder to reason about resource contention,
recovery blast radius, and stale monitoring evidence.

v3 keeps v2 as historical context and moved the current production shape toward
a k3s delivery plane with a separate observability plane.

The migration rule was conservative: a new v3 workload being healthy did not by
itself transfer production authority. Authority moved only when the runtime,
state root, CLI/supervisor path, metrics namespace, alert path, and recovery
gates all had explicit cutover evidence. That rule is why the public v3 design
documents still mention v2 decisions: they are inherited safety constraints, not
a claim that v2 is the current production owner.
