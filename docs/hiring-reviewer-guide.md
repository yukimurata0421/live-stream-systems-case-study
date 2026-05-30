# Hiring Reviewer Guide

This repository is meant to be reviewed as an operational case study, not as a
plug-and-play streaming package.

## This Is Not

- a generic OSS starter;
- a supported YouTube streaming product;
- a commercial SaaS project;
- an installer for another operator's environment.

## This Is

- a reliability engineering case study;
- a 24/7 media delivery system operated under real hardware constraints;
- an example of separating delivery ownership from monitoring ownership;
- a record of safety decisions around recovery, stale evidence, and API quota.

## Key Design Decisions

1. The delivery plane and observability plane are separated.
2. Monitors do not directly own FFmpeg or the live RTMPS process.
3. Recovery is staged through guards before destructive actions.
4. API quota exhaustion is treated as degraded evidence, not immediate stream
   failure.
5. Shadow mode exists before destructive cutover.
6. Public validation excludes secrets, live YouTube mutation, and production
   k3s apply.

## Suggested Review Path

1. `README.md`
2. `docs/design-decisions-for-review.md`
3. `docs/v3/decisions.md`
4. `docs/runtime-contract.md`
5. `docs/v3/runtime-state-and-evidence.md`
6. `src/stream_v2/recovery_orchestrator/gate.py`
7. `ops/scripts/v3_shadow_acceptance.py`
8. `tests/test_v3_shadow_acceptance.py`
9. `tests/test_youtube_video_id_resolver_cache_freshness.py`
10. `.github/workflows/public-snapshot-check.yml`

## What To Evaluate

- Whether the failure domains are named clearly.
- Whether recovery actions are blocked when evidence is stale, ambiguous, or in
  shadow mode.
- Whether public validation proves the snapshot boundary without requiring
  credentials or live production mutation.
- Whether the system shows operational judgment rather than only code volume.
