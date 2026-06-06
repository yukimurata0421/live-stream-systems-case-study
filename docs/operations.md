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

The public test boundary is documented in
`test-strategy-and-safety-boundary.md`. Public validation must remain
non-mutating: no stream key, no live YouTube mutation, no production k3s apply,
and no PVC deletion.

## Production-like Streaming

Production-like streaming requires a host with GPU/NVENC support, PulseAudio
runtime support, source map access, and local secrets.

Before live use, validate:

```bash
nvidia-smi
ffmpeg -hide_banner -encoders | grep h264_nvenc
kubectl describe node | grep -F nvidia.com/gpu
```

For v3-impacting runtime, encoder, recovery, or cutover-authority changes, use
a 24-hour smoke test before treating the change as ordinary production
behavior. The rationale is documented in
`v3/migration-cutover-case-study.md`: v2 supplied the stable behavior baseline,
while v3 must still prove the migrated ownership model across one daily cycle.

## Rollback Thinking

The v3 design keeps the v2 single-host runtime as a conceptual rollback
baseline. The public repository does not ship production state or private
rollback artifacts, but the architecture keeps migration and rollback boundaries
explicit.
