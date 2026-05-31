# Encoder Fps Tuning 2026-05-31

Status: accepted
Scope: v3 production encoder fps contract

## Question

YouTube reported intermittent input-quality warnings while the stream was using
4fps/3400k video CBR. The operator constraint was to keep total upload below the
5.0 Mbps warning ceiling while preserving the same public watch URL.

The comparison held these settings constant:

```text
VIDEO_ENCODER=h264_nvenc
VIDEO_NVENC_PRESET=p4
VIDEO_NVENC_RC=cbr
VIDEO_BITRATE=3400k
VIDEO_MAXRATE=3400k
VIDEO_BUFSIZE=6800k
AUDIO_BITRATE=192k
```

## Trial Summary

| setting | upload avg | upload p95 | upload max | over 5 Mbps | YouTube health | sampled HLS total |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| 4fps/3400k | 4.878 Mbps | 4.921 Mbps | 4.938 Mbps | 0s | good | 785 kbps |
| 5fps/3400k | 4.875 Mbps | 4.905 Mbps | 4.910 Mbps | 0s | good | 779 kbps |
| 10fps/3400k | 4.884 Mbps | 4.943 Mbps | 4.954 Mbps | 0s | good | 776 kbps |

The HLS sample is viewer-side output evidence, not a direct ingest-fps
measurement. It was useful as a sanity check that the public player surface was
still producing the expected 1080p variant during each trial.

## Frame Budget

At fixed 3400k video CBR, increasing fps does not materially change the upload
target; it divides the same video budget across more frames.

| fps | kbit/frame | retained budget vs 4fps | budget drop vs 4fps |
| --- | ---: | ---: | ---: |
| 4 | 850 | 100% | 0% |
| 5 | 680 | 80% | 20% |
| 10 | 340 | 40% | 60% |

## Decision

Adopt 5fps/3400k/audio192k as the current v3 encoder contract.

5fps is the middle point: it improves cadence over 4fps, keeps the short-window
upload samples under the ceiling, and avoids the much larger per-frame quality
trade-off of 10fps. 10fps remains a trial result, not the current contract.

## Public Retention Boundary

Private runtime state, stream keys, raw watcher caches, and unsanitized live
logs are not committed. The public record keeps the decision, summarized
measurements, and the operational interpretation.
