# Encoder And Upload Budget Case Study

This case study explains why the v3 encoder contract is not a simple copy of
the older single-host stream. The short version is that `stream_v3` moved to
NVIDIA NVENC for the production host, kept YouTube-friendly CBR, and accepted a
higher measured upload envelope because lower-upload alternatives damaged
YouTube input health.

## Current Contract

The current public v3 encoder contract is:

```text
VIDEO_ENCODER=h264_nvenc
VIDEO_NVENC_PRESET=p4
VIDEO_NVENC_RC=cbr
VIDEO_NVENC_MULTIPASS=
VIDEO_NVENC_RC_LOOKAHEAD=0
VIDEO_NVENC_SPATIAL_AQ=0
VIDEO_NVENC_TEMPORAL_AQ=0
VIDEO_NVENC_BFRAMES=0
FRAME_RATE=5
VIDEO_BITRATE=3400k
VIDEO_MAXRATE=3400k
VIDEO_BUFSIZE=6800k
AUDIO_BITRATE=192k
```

The target is not "minimum upload at any cost." The target is same-URL 24/7
delivery with stable YouTube input health, visual correctness, audio health,
and upload below the 5.0 Mbps warning ceiling.

## What Changed

The v2 lineage ended as a low-bandwidth CPU-encoded profile:

```text
libx264
4fps
3400k CBR video
192k audio
```

The v3 production host has GPU capability and runs the delivery workload inside
k3s, so the production target moved to NVENC:

```text
h264_nvenc
CBR
GPU-backed encode
```

This is an encoder and rate-control change, not only an FPS change. At the same
nominal video bitrate, NVENC CBR produced a higher measured RTMPS send envelope
than the older v2 CPU path.

## Measurement Summary

| Profile | Observed upload | YouTube input health | Decision |
| --- | --- | --- | --- |
| v2 `libx264` 4fps/3400k | about p50 4.38 Mbps, p95 4.47 Mbps | acceptable in the v2 window | historical baseline |
| v3 `h264_nvenc` CBR 3400k | about p50 4.87 Mbps, p95 4.93 Mbps | acceptable, but much closer to the 5 Mbps ceiling | accepted with guardrails |
| v3 `h264_nvenc` VBR/CQ trial | about 3.0 Mbps | YouTube low-bitrate / not-enough-video warnings | rejected |
| v3 4fps/3400k CBR | p95 near 4.92 Mbps in trial | good, but cadence was lower | replaced by 5fps |
| v3 5fps/3400k CBR | p95 near 4.91 Mbps in trial | good | current contract |
| v3 10fps/3400k CBR | p95 near 4.94 Mbps in trial | good in short window, but lower per-frame budget | rejected as current target |

The important lesson is that a lower upload number was not automatically a
better production setting. VBR/CQ reduced upload, but YouTube classified the
input as worse. The accepted contract spends more upload headroom to preserve
YouTube input quality.

## Why Upload Increased

The nominal bitrate in an FFmpeg command is not the full wire-rate promise. The
measured RTMPS send path includes encoder rate-control behavior, muxing, audio,
transport overhead, and recovery-profile state.

In this system, the practical difference was:

- v2 `libx264` at 3400k stayed around the mid-4 Mbps range;
- v3 `h264_nvenc` CBR at 3400k stayed closer to the high-4 Mbps range;
- VBR/CQ could lower upload but created YouTube input-quality warnings;
- increasing FPS at fixed video CBR did not materially lower upload; it divided
  the same video budget across more frames.

That is why the v3 decision uses measured upload samples and YouTube health
together instead of trusting nominal bitrate alone.

## Guardrail

The upload budget is a guardrail, not the product outcome.

```text
normal target: stay below 5.0 Mbps p95 warning ceiling
primary outcome: YouTube availability and same-URL continuity
quality outcome: no persistent YouTube input warnings, visual correctness, audio health
```

If the budget and input quality conflict, availability and same-URL preservation
come first, then YouTube input health, then visual/audio correctness, then
upload efficiency. Upload reduction is not accepted if it makes YouTube classify
the stream as unhealthy.

## Operational Decision

The accepted production contract is `h264_nvenc p4 CBR 5fps/3400k/audio192k`.

The rejected options remain useful historical evidence:

- `libx264` is a fallback/debug path, not the production target.
- `VBR/CQ` is not accepted for production because it lowered upload while
  worsening YouTube health.
- 10fps/3400k is not current because it leaves only half the per-frame budget
  of 5fps.
- 30fps/3300k with more NVENC quality knobs was a trial history, not the current
  env-synced contract.

## Public Implementation Hooks

- `docs/v3/encoder-fps-tuning-2026-05-31.md` records the 4fps/5fps/10fps trial.
- `docs/runtime-contract.md` records the current public contract.
- `deploy/k3s/base/configmap-shadow.yaml` and `configs/*.env.example` keep the
  public env examples aligned with the contract.
- `src/stream_core/engine/ffmpeg_args.py` builds the FFmpeg arguments.
- `tests/test_docs_structure.py` checks that the documented encoder contract
  matches public config examples.

The public repository keeps summarized measurements and contract tests, not raw
RTMPS logs or private runtime state.
