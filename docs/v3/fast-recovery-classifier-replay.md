# Fast-Recovery Classifier Replay

Date: 2026-06-06

This note documents a remediation to the shadow recovery evidence model. It is
not a claim that the executor had already performed production restarts. It is a
counterfactual replay over retained fast-recovery restart events using the
current local-delivery classifier.

## Problem

The public recovery-boundary view showed historical production restarts without
matching shadow recovery intent:

```text
last_7d:
  production_action_counts.restart_stream = 5
  shadow_recovery_intent_action_count = 0
  shadow_vs_production_disagreement_by_reason.production_without_shadow = 5
```

Those events were real production fast-recovery restarts. The review question
was whether the current orchestrator could classify the same evidence, without
rewriting historical orchestrator logs to make the past look cleaner.

## Boundary

Two fields must stay separate:

```text
shadow_vs_production_disagreement_by_reason.production_without_shadow
```

This is historical comparison against the orchestrator JSONL that existed at
the time. It is not backfilled.

```text
current_classifier_replay
```

This is the current classifier replaying historical fast-recovery restart
events. It proves current classifier coverage for those retained events, not
past executor intent.

## Implementation

The source reader now keeps both the latest fast-recovery event and the latest
fast-recovery restart event. This matters because a restart can be followed by a
new healthy `tcp_send_sample`; the newer sample should not erase recent restart
evidence before the classifier can see it.

The local-delivery classifier maps recent fast-recovery `kind=restart` events
to stream-level recovery evidence for these triggers:

| Trigger | Evidence name | Recommended action |
| --- | --- | --- |
| `tcp_stall` | `fast_recovery_stream_restart_tcp_stall` | `restart_stream` |
| `network_down` | `fast_recovery_stream_restart_network_down` | `restart_stream` |
| `low_upload_pressure` | `fast_recovery_stream_restart_low_upload_pressure` | `restart_stream` |
| `remote_warning` | `fast_recovery_stream_restart_remote_warning` | `restart_stream` |

A raw `kind=tcp_stall` sample remains scoped local recovery and recommends
`restart_ffmpeg`. Only a production restart event is promoted to stream restart
evidence.

The action proposer uses the subsystem recommendation to order candidates. If
local delivery recommends `restart_stream`, the stream restart candidate is
proposed before `restart_ffmpeg`; otherwise the narrower FFmpeg action remains
first.

## Replay Result

Retained replay on 2026-06-06:

| Window | Eligible production restarts | Covered by current classifier | Uncovered | Covered triggers |
| --- | ---: | ---: | ---: | --- |
| 7d | 5 | 5 | 0 | `network_down=4`, `low_upload_pressure=1` |
| 30d | 6 | 6 | 0 | `network_down=5`, `low_upload_pressure=1` |

This is exposed as:

```json
{
  "current_classifier_replay": {
    "classifier": "local_delivery_fast_recovery_stream_restart_v1",
    "target_action": "restart_stream",
    "basis": "current classifier replay over historical fast_recovery restart events; historical orchestrator JSONL is not backfilled",
    "eligible_count": 5,
    "covered_count": 5,
    "uncovered_count": 0,
    "coverage_ratio": 1.0
  }
}
```

## What This Proves

- Historical disagreement is still visible; it was not hidden or backfilled.
- The current classifier covers the retained fast-recovery restart cases.
- A healthy current `tcp_send_sample` after a restart does not erase recent
  restart evidence.
- Shadow SLI comparison now uses executable recovery intent, not every
  report-only selected action.

## What This Does Not Prove

- It does not prove that the executor has already performed production
  `restart_stream` actions.
- It does not prove future restart correctness for unseen trigger classes.
- It does not authorize YouTube broadcast replacement; local-delivery failures
  still block lifecycle mutation.

## Code And Tests

- `src/stream_v2/source_reader.py`
- `src/stream_v2/subsystems/local_delivery/`
- `src/stream_v2/recovery_orchestrator/proposer.py`
- `src/stream_v2/sli.py`
- `tests/test_subsystems.py`
- `tests/test_orchestrator.py`
- `tests/test_sli_pipeline_rotation.py`
