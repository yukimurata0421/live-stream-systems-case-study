# Fault Switching And Player Continuity: Automated Validation

Dated record, October 31, 2026, run against step 3 of the
[revision history](fallback-policy-revisions.md). The detection rule has since
changed; see the [fallback decision contract](fallback-decision-contract.md).
The measured reception gap below still applies, because the cold backup and the
30 s failure window are unchanged.

On October 31, 2026, automated validation preserved the production stream and
injected faults only into isolated, test-owned processes. No production fault,
new YouTube resource or provider lifecycle mutation was used.

| Gate | Evidence | Result |
| --- | --- | --- |
| Clock fault only | Discontinuous clock envelope for 35 s, unchanged real clocks and advancing media | Backup remained off |
| Resident collector, actual input loss | Native collector/controller; 75 s after stopping the owned primary | No switch: error facts were discarded |
| Fixed collector, local receiver | Backup started after 45.402 s; same video element/Hls instance received BACKUP frames and decoded 660 Hz audio | Switch and AV resumption observed |
| Continuous local reception | Presentation gap 48.633 s; decoded recording silence 47.942 s | FAIL |
| Public YouTube player | Actual production frames initially visible; later media HTTP 403 in both 120 s and 75 s observations | Continuity FAIL in this observer; audio unverified |

The local receiver is unmodified MediaMTX/HLS with Chromium/Hls.js, not YouTube.
The test uses H.264/AAC synthetic media at 320x180/10fps, a local-output copy lab
variant, and frozen operated Python source. Only OS membership, localhost
placement and the isolated replica effect adapter are mapped. The real nonce
exchange, collector checks, controller main and 15 s/30 s timing execute.
No production credential or state is supplied to the media namespace. This is
not formal route qualification, a production-resolution performance test or a
provider-selection proof.

The native run found a producer/consumer gap: the collector accepted current,
process/boot/destination-bound copy facts, then discarded them when the bridge
was idle or had a last error. The controller consequently saw observation loss
instead of fresh evidence of input loss. The [collector](../../ops/selfheal/route_facts_http.py)
now preserves those bound diagnostics. Stale heartbeat, wrong process or
destination, and missing/duplicate bridge rejection remain intact. Boundary
regressions reproduce the old failure and check the corrected path. The fix was
applied to the operated read-only facts service after a real DynamicUser read
and a bound consumer read; media processes and provider configuration were
preserved. The related public suite passed 130 tests and 77 subtests.

The first HLS run used extended retries and started the backup without resuming
that same receiver. It remains failed evidence. A later run using normal loader
retry behavior and root-manifest recovery received the backup pattern and tone.
Both runs failed continuity. Saved WebM was decoded to verify the long silent
interval independently of browser counters. A progressing media clock alone
is not sufficient: decoded-frame progress is required as well.

The 30 s failure window, polling wait, sender startup and receiver recovery
produce a visible interruption in the measured cold-backup setup. No-gap
switching was not achieved. Pre-sending a backup or absorbing the delay in the
receiver would require a separate design and bandwidth decision; this validation
did not change that policy.

The public observation used ordinary anonymous watch-page playback, an ephemeral
profile, screenshots, DOM media counters and classified HTTP status only.
Signed media URLs were not extracted, access checks were not bypassed and the
403 cause was not established. An ingest/API success does not repair this missing
player evidence. Authenticated user reception, Twitch public-player continuity,
real WAN failure and actual YouTube backup selection remain unverified.

All owned children exited. Inputs, failed setup attempts, screenshots and AV
remain in bounded private HDD evidence. A five-second production guard observed
unchanged FFmpeg/connection generations and advancing ACKs during both native
windows; the operated backup remained off.
