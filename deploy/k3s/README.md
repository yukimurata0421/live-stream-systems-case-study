# stream_v3 k3s deployment skeleton

This directory is the first concrete build target for v3. It is intentionally shadow-first:

- `stream_v2` remains the production owner until an explicit cutover.
- `v3-runtime` starts with `TEST_MODE=1`; it captures locally instead of publishing to YouTube.
- `v3-runtime` starts PulseAudio inside the `stream-engine` container and exposes it to `auto-dj` through `/run/stream-pulse/native` on a shared `emptyDir`.
- `v3-control` runs `python3 -m stream_v3.control_loop`, reads v2 state through a read-only mount or sync path, and writes only v3 state.
- `v3-observer` exports v3 state for Prometheus-style scraping.
- `v2-state-mirror` is a suspended CronJob skeleton for rsyncing the current v2 `.state` into a PVC that `v3-control` mounts read-only.
- `v3-reports` contains suspended CronJobs for API cost and stream1090/upstream report-only jobs. Unsuspend them after URLs, secrets, and state PVC writes are validated.

## Validate manifests locally

This validation does not need a live k3s cluster:

```bash
cd /home/yuki/projects/stream_v3
python3 ops/scripts/validate_k3s_manifests.py
```

It checks the shadow overlay resources, ConfigMap safety flags, PVC wiring, repo-local command paths, GPU/NVENC prerequisites, and the supervisor target map.

To check whether the current host can build and apply the shadow stack:

```bash
python3 ops/scripts/v3_k3s_preflight.py
```

On the current v2 production host this is expected to report missing cluster/build prerequisites. On the new server it should pass before running the apply command.

To prove the repo-local shadow path is safe before a real cluster exists:

```bash
python3 ops/scripts/v3_shadow_acceptance.py
```

This runs manifest validation, one v3 shadow control-loop pass, and confirms the generated action plan is not executable in shadow mode.
The root `.dockerignore` is part of the preflight contract so `.state`, local env snapshots, virtualenvs, logs, and local music payloads are not copied into the image build context.

## GPU / NVENC prerequisites

v3 runtime is not CPU-encode-first. The `stream-engine` container is configured with `VIDEO_ENCODER=h264_nvenc`, `VIDEO_NVENC_PRESET=p5`, YouTube-friendly NVENC CBR (`VIDEO_NVENC_RC=cbr`, `VIDEO_NVENC_MULTIPASS=fullres`, `VIDEO_NVENC_RC_LOOKAHEAD=20`, `VIDEO_NVENC_SPATIAL_AQ=1`, `VIDEO_NVENC_TEMPORAL_AQ=1`, `VIDEO_NVENC_BFRAMES=2`, `VIDEO_NVENC_B_REF_MODE=middle`, `FRAME_RATE=30`, `VIDEO_BITRATE=3300k`, `VIDEO_MAXRATE=3300k`), `NVIDIA_DRIVER_CAPABILITIES=video,utility`, and `nvidia.com/gpu: "1"` so the GTX 1070 NVENC block does the H.264 encode work.

Before applying the runtime on the new server, prove these three points:

```bash
nvidia-smi
ffmpeg -hide_banner -encoders | grep h264_nvenc
kubectl describe node | grep -F nvidia.com/gpu
```

The k3s node needs the NVIDIA driver plus a container runtime/device-plugin path that exposes `nvidia.com/gpu` to Pods. If `v3_k3s_preflight.py` fails on `nvidia-smi` or `ffmpeg:h264_nvenc`, do not move to runtime TEST_MODE yet.

## Build image

```bash
cd /home/yuki/projects/stream_v3
sudo nerdctl -n k8s.io build -f deploy/k3s/Containerfile -t stream-v3:local .
```

Use `docker build` or `podman build` if the new server is not using containerd directly.

## Prepare music

Local v3 uses `/home/yuki/projects/stream_v3/ncs_music/time_tags`.
The k3s runtime mounts the same asset shape at `/music/time_tags` through `stream-v3-music`.
Keep the audio files out of Git and provision the PVC from the copied `ncs_music/` tree before enabling AutoDJ in a real Pod.

## Prepare secrets

Copy `base/secret.example.yaml` to a local untracked file and fill real values there. Do not commit real keys.

Required before production-like tests:

- `STREAM_KEY`
- `YTW_API_KEY`
- `YTW_OAUTH_CLIENT_ID`
- `YTW_OAUTH_CLIENT_SECRET`
- `YTW_OAUTH_REFRESH_TOKEN`
- `STREAM_NOTIFY_DISCORD_WEBHOOK_URL` if notification dry-runs should reach Discord

The committed manifests reference `stream-v3-secrets` as an optional Secret so shadow TEST_MODE can run before real keys are provisioned. Create the real Secret on the cluster before production-like lifecycle or notification tests.

## Apply shadow stack

```bash
kubectl apply -k deploy/k3s/shadow
```

`base/configmap-shadow.yaml` keeps `TEST_MODE=1`. Do not switch it off until the v3 runtime contract in `docs/v3/10_current/2026-05-29_01_current_runtime_contract.md`, the ownership decision in `docs/v3/25_decisions/2026-05-29_02_streaming_monitoring_ownership_split.md`, and the k3s cutover history in `docs/v3/50_ops_logs/2026-05-29_01_k3s_streaming_monitoring_encoder_tuning.md` are satisfied.

The shadow ConfigMap also keeps `STREAM_RUNTIME_SUPERVISOR=k8s` and `STREAM_K8S_DRY_RUN=1`, so recovery code can exercise k8s command mapping without deleting or restarting workloads.

## Mirror v2 state

`deploy/k3s/v2-state-mirror/cronjob.yaml` is committed with `suspend: true`.

Before enabling it:

1. Set `STREAM_V2_MIRROR_SOURCE` in `base/configmap-shadow.yaml` to the current v2 host and `.state/adsb-streamnew-v2/` path.
2. Create a local secret from `deploy/k3s/v2-state-mirror/secret.example.yaml` with a read-only SSH key and known_hosts entry.
3. Unsuspend the CronJob only after the first manual rsync succeeds.

`v3-control` mounts the resulting `stream-v2-state-mirror` PVC at `/source-v2-readonly` as read-only.

## Cutover gate

Production mutation requires both of these to be true:

- k3s manifests have been changed away from shadow mode.
- launcher environment includes `STREAM_V3_CUTOVER_ENABLE=1`.

The repo launcher blocks mutating systemd commands without that flag, even if the old v2 CLI is called through this tree.
`stream_v3.control_loop --mode cutover` has the same guard. Without `STREAM_V3_CUTOVER_ENABLE=1`, it exits before starting the fast recovery / watchdog / resolver / notify task set.

For this streaming host, use the streaming-only overlay:

```bash
python3 ops/scripts/validate_k3s_manifests.py --overlay streaming
python3 ops/scripts/v3_k3s_preflight.py --overlay streaming
kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k3s/streaming | kubectl apply -f -
```

The streaming overlay keeps this host limited to the delivery plane: `stream-v3-runtime` renders readsb/custom tar1090, plays AutoDJ, captures audio/video, sends RTMP, and runs the local fast recovery sidecar at `V3_FAST_RECOVERY_INTERVAL_SEC=10`. It switches `STREAM_V3_MODE=streaming`, `STREAM_V3_CUTOVER_ENABLE=1`, `STREAM_K8S_DRY_RUN=0`, and `TEST_MODE=0`. Create the real `stream-v3-secrets` Secret with `STREAM_KEY` before applying it; otherwise the stream engine cannot resolve a production RTMP URL.

Arena-server owns the monitoring plane. Install `ops/systemd/stream-v3-arena-monitor.service` there for `youtube_video_resolver`, `youtube_monitor`, `stream_watchdog`, `notify_status`, `subsystems_status`, `recovery_orchestrator`, and `shadow_sli`. Install `ops/systemd/stream-v3-remote-recovery.timer` there with `STREAM_V3_RECOVERY_WORKLOADS=deployment/stream-v3-runtime` so arena-server can request runtime restarts on this host through the namespace-scoped k8s token. Manual staged requests can use:

```bash
python3 ops/scripts/stream_v3_staged_restart.py --reason "arena manual recovery"
python3 ops/scripts/stream_v3_staged_restart.py --hard --reason "arena escalated recovery"
```
