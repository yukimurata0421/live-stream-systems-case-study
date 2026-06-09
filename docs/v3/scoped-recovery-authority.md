# Scoped Recovery Authority

`stream_v3` gives the observability plane limited recovery authority only where
the action can preserve the current YouTube URL. If the system cannot preserve
the URL, it should fail closed instead of creating a replacement broadcast.

## Policy

The recovery policy is intentionally narrow:

| Decision | Policy |
| --- | --- |
| Same-URL preserving runtime restart | Allowed when the runtime workload is inactive and cooldown permits it. |
| `restart_dj` | Allowed only as an Auto DJ container restart; it must not roll the shared runtime Deployment. |
| `restart_ffmpeg` | Allowed only as an RTMPS FFmpeg child restart inside `stream-engine`; fallback is limited to the `stream-engine` container. |
| Upload pressure / 5 Mbps budget breach | Recorded for SLI and diagnosis, but not a restart cause. |
| `create_replacement_broadcast` | Hard blocked by the same-URL contract. |

This separates observability evidence from mutation authority. A warning can be
useful for diagnosis without being a valid reason to restart production.

## Implementation

`ops/scripts/stream_v3_scoped_recovery.py` owns the local recovery primitive.
It uses a namespace-scoped `kubectl` path and checks the target before it
mutates anything:

- `restart-dj` selects exactly one Running `stream-v3-runtime` Pod, sends
  `TERM` to PID 1 in the `auto-dj` container, waits for only that container to
  change, and fails if `stream-engine` changes.
- `restart-ffmpeg` inspects `pgrep -a ffmpeg` inside `stream-engine`, requires
  exactly one RTMP/RTMPS child, terminates that child, and waits for a new
  RTMPS FFmpeg PID.
- If no RTMPS FFmpeg child exists, the helper can restart only the
  `stream-engine` container. It still fails if `auto-dj` changes.
- Upload-related reasons are blocked before the helper reads Pod state.

`src/stream_v2/recovery_orchestrator/executor.py` renders k3s action plans to
that scoped helper:

```text
restart_dj     -> python3 ops/scripts/stream_v3_scoped_recovery.py restart-dj
restart_ffmpeg -> python3 ops/scripts/stream_v3_scoped_recovery.py restart-ffmpeg
```

The same executor keeps `create_replacement_broadcast` blocked with
`same_url_required_absolute` and `replacement_broadcast_disabled`.

## Action-Plan Consumption

`ops/scripts/stream_v3_remote_recovery.py` still handles the coarse case where
the runtime workload is inactive. After that check is healthy, it may consume
the current `recovery_action_plan.json`, but only through an allowlist:

```text
restart_dj
restart_ffmpeg
```

The action plan must be executable, recent, outside cooldown, not already
executed for the same event ID, and blocked only by `shadow_mode`. The reason,
action, blockers, and step descriptions must not contain upload-pressure terms.

The public systemd unit keeps the repo path and state path in
`/etc/default/stream-v3-remote-recovery`, so deployments can point it at a
local checkout without changing the committed unit. The committed defaults are
fail-closed: workload and action-plan mutation require explicit live-host
opt-in through `STREAM_V3_REMOTE_RECOVERY_APPLY=1` and
`STREAM_V3_REMOTE_RECOVERY_APPLY_ACTION_PLAN=1`.

## What This Proves

This design proves that the public recovery path is not just a broad
`kubectl rollout restart` wrapper:

- music-only recovery stays in the Auto DJ container;
- FFmpeg recovery stays in the RTMPS child or, if missing, the stream-engine
  container;
- upload budget signals remain observability inputs, not control inputs;
- URL replacement remains outside executor authority.

Public tests cover the policy and command rendering. They do not execute a live
DJ or FFmpeg restart against the production cluster.
