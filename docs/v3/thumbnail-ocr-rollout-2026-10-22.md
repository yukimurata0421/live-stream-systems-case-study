# Operated Thumbnail OCR Counter Rollout

On 2026-10-22 at 11:51 JST, the monitoring host activated the remaining OCR
accounting source from `c27f0ee0`. The public source was independently ported in
`3c1db55`; its ten selected source/test digests were checked against that private
commit and the public port manifest before adding this record.

The [thumbnail contract](twitch-thumbnail-monitor.md) owns the semantics. This
page records the operated result, not deployment of the public checkout.

## Scope And Result

Only the dedicated thumbnail worker and the exporter thumbnail module needed
updating. Stream-change ETag handling, startup grace, fourteen-minute image age,
missing-series warnings, the unit and notification extension already matched
the operated source. The original exporter baseline stayed hash-pinned, and
paused viewer-clock wiring was not added to it.

| Check | Observed result | Limit |
| --- | --- | --- |
| Worker/source parity | Eight source/unit files and the fifteen-entry observer manifest matched. | File parity does not prove future observation quality. |
| Live thumbnail window, 11:51-11:56 JST | Six cycles, one actual OCR observation and one successful binary retry. Five cached cycles did not increment either counter. | One retry is not a representative retry-rate distribution. |
| Metrics | Prometheus ingested the persistent retry counter with value one. | No warning receiver-side delivery test was performed. |
| Resources | Observed worker memory peak about 58.4 MiB, swap limit zero; ordinary image files zero. | A short normal window, not worst-case or 24-hour performance. |
| Protected processes | API collector, existing notifier and readsb PIDs continued. | No stopped-monitor baseline or controlled RF comparison. |
| Source tests | 104 related private tests passed before activation. | Synthetic/mocked tests remain separate from live image verification. |

The counters measure actual OCR observations and binary retries, including
unsuccessful attempts. They are not reconstructed from older history. Paired
string identity, confidence and timezone-offset requirements were unchanged.
Old releases and exporter-module preimages remain available for rollback.

## Independent Quality Observation

The return-relay observer briefly raised a clock-quality warning: some UTC OCR
confidences fell below the existing threshold while frames and native sender
bytes continued. Later accepted observations cleared the warning. That analyzer
was unchanged by this rollout; the thumbnail counters did not repair it. Normal
capture or sender progress alone is not public-player reception proof.

The public source transfer detected the other port's new commit before any
write, then reused that reviewed source. Four independent publisher/API SLI
working files were preserved. Their pre-existing source-map mismatches are
outside the selected clean-snapshot validation, not silently accepted changes.

Detailed checks and bounded operated observations are retained in
[`publication/thumbnail-ocr-rollout-20261022.json`](../../publication/thumbnail-ocr-rollout-20261022.json).
No credentials, channel/stream identifiers, images, raw logs or runtime databases
are included. Daily production rollover, long-term OCR quality and player/audio
E2E correctness remain unverified.
