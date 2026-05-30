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
- contract tests
- runbook-driven operations

## Why v2 Was Not Enough

The delivery path and observability path still shared one host and one process
ownership model. That made it harder to reason about resource contention,
recovery blast radius, and stale monitoring evidence.

v3 keeps v2 as historical context and moves the current production shape toward
a k3s delivery plane with a separate observability plane.
