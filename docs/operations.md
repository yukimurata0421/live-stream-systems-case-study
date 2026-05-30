# Operations

This repository is safe to validate locally without publishing to YouTube.

## Local Checks

```bash
python3 ops/scripts/validate_k3s_manifests.py
python3 ops/scripts/v3_shadow_acceptance.py
```

For focused Python tests:

```bash
pytest tests/test_v3_k3s_preflight.py tests/test_stream_v3_control_loop.py
pytest tests/test_stream_v3_prometheus_exporter.py
```

## Shadow Mode

Shadow mode validates command mapping, state writes, recovery planning, and SLI
summaries without live publication.

Expected shadow settings:

```text
STREAM_V3_MODE=shadow
TEST_MODE=1
STREAM_K8S_DRY_RUN=1
STREAM_V3_CUTOVER_ENABLE=0
```

## Production-like Streaming

Production-like streaming requires a host with GPU/NVENC support, PulseAudio
runtime support, source map access, and local secrets.

Before live use, validate:

```bash
nvidia-smi
ffmpeg -hide_banner -encoders | grep h264_nvenc
kubectl describe node | grep -F nvidia.com/gpu
```

## Rollback Thinking

The v3 design keeps the v2 single-host runtime as a conceptual rollback
baseline. The public repository does not ship production state or private
rollback artifacts, but the architecture keeps migration and rollback boundaries
explicit.
