# YouTube Lifecycle Safety

`stream_v3` treats YouTube lifecycle mutation as a high-risk operation. A
working live stream can be harmed by the wrong recovery action: replacing a
broadcast, rebinding a stream, or trusting stale cached state can break the
public URL even when local delivery is recoverable.

## Safety Goal

The safety goal is:

```text
preserve same watch URL when recoverable
avoid destructive YouTube mutation from stale or ambiguous evidence
separate delivery failure from control-plane evidence failure
spend API quota deliberately during unhealthy windows
```

This is why same-URL preservation is a production invariant rather than just
another availability percentage.

## Evidence Sources

The YouTube control plane uses several evidence sources, each with different
failure modes.

| Source | What it can prove | What it cannot prove alone |
| --- | --- | --- |
| Local FFmpeg / RTMPS | the delivery process is trying to send bytes | that viewers see the live page |
| Public watch page | viewer-facing URL state | OAuth authority or broadcast ownership |
| YouTube Data API | broadcast and stream metadata | freshness if quota or cache state is degraded |
| OAuth probe | channel and mutation authority | delivery health |
| Resolver state | current video identity and same-URL continuity | final truth if cached timestamps are stale |
| Watchdog state | health classification and warning context | safe mutation without identity and freshness gates |

The system requires these signals to agree before destructive lifecycle actions
are allowed.

## Cache Freshness Bug

An external reviewer found a stats reuse bug in the resolver/watchdog path.
The bug pattern was:

```text
top-level stats timestamp looked fresh
per-probe OAuth or Data API result was older
cached evidence could be reused as if it had just been checked
```

The fix was to prefer per-probe checked timestamps over the top-level stats
timestamp. If an OAuth or Data API probe was not checked recently, its cached
result is not treated as fresh merely because the containing stats file was
rewritten.

Public tests cover this behavior:

- `tests/test_youtube_video_id_resolver_cache_freshness.py`
- `tests/test_youtube_watchdog_cache_freshness.py`
- `tests/test_youtube_watchdog_checked_timestamps.py`

## Quota Guard

YouTube API quota exhaustion is treated as degraded evidence, not delivery
failure.

If API quota pressure is high:

- public watch-page evidence can still describe viewer-facing state;
- local RTMPS and fast-recovery evidence can still drive delivery recovery;
- expensive API calls are deferred or gated;
- quota errors do not automatically authorize broadcast replacement.

The dashboard separates:

```text
API PT day actual units
projected units/day
closed-day historical units
quota-exceeded events
```

This prevents an operator from mistaking projection for actual PT-day usage.

## Mutation Gate

Destructive actions require explicit permission and fresh identity evidence.

Examples of blocked or constrained actions:

- replacing a broadcast before the URL-preservation window is satisfied;
- replacing a persistent scheduled broadcast when it can be transitioned live;
- deleting a live source broadcast during cleanup;
- mutating when OAuth channel evidence mismatches the expected channel;
- acting on stale remote-ended evidence while public/live evidence disagrees;
- promoting a candidate video ID to expected video ID without resolver policy.

The public tests exercise these cases in `tests/test_youtube_broadcast_selection.py`,
`tests/test_youtube_evidence_decision.py`, and
`tests/test_youtube_monitor_e2e.py`.

## Operational Rule

The core rule is:

```text
delivery recovery may preserve a live stream;
YouTube lifecycle mutation may destroy its identity.
```

Therefore, the delivery plane can restart FFmpeg or the local runtime when
fresh local evidence justifies it, but YouTube lifecycle mutation must pass
identity, ownership, freshness, quota, and explicit action gates.

## Public Implementation Hooks

- `src/watchers/youtube_video_id_resolver.py` resolves current video identity.
- `src/watchers/video_resolver/*` contains cache, identity, session, and policy
  helpers.
- `src/watchers/youtube_watchdog.py` records YouTube health and lifecycle
  evidence.
- `src/watchers/youtube_api.py` and `src/watchers/youtube_api_lib/*` implement
  API and broadcast operations.
- `src/watchers/evidence/*` and `src/watchers/decision/*` keep evidence and
  action gates separate.
- `src/stream_v2/recovery_orchestrator/gate.py` blocks unsafe staged recovery.

The public repo includes tests for destructive-action prevention without
requiring real credentials or live YouTube mutation.

## Review Signal

For review, the important behavior is restraint. The code is designed so that a
dashboard warning, API quota error, stale cache, or transient local fault does
not automatically become a YouTube lifecycle mutation.
