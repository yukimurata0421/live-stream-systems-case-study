# Current Status

Last updated: 2026-10-22 JST.

This is the only page that records state expected to change soon: what was
changed recently, what has not been observed yet, and what is deliberately
disabled. Other pages describe designs and dated evidence and link here instead
of repeating these items.

## Production Route

Since 2026-10-20 08:43 JST, Dell uploads one feed to the OCI relay, which
forwards it to YouTube and Twitch. A Dell controller starts a direct YouTube
fallback upload when relay health is lost or unobservable and stops it after
300 seconds of continuous relay health. The [route record](v3/self-healing-oci-fanout.md)
defines the components and thresholds.

## Changes Since The Route Went Live (2026-10-20 JST)

| Time | Change | Verified |
| --- | --- | --- |
| 08:55-09:03 | Relay-stop drill. | Fallback started after 52 s and stopped after 300 s of relay health; the broadcast did not end. |
| 09:10 | Replacement-broadcast creation disabled in the YouTube watchdog. A broadcast that really ends is recreated by hand. | Configuration applied and monitor restarted. |
| 11:33 | arena-server observes the relay through the private tunnel instead of the home public address. | Public status relay values returned to measured data. |
| 11:56-12:21 | Main sender switched to RTMPS on the relay; readiness and TCP-sampling port lists updated. | Upload samples of 3.57 and 3.63 Mbps returned. |
| 14:16 | The YouTube watchdog counts an established fallback-Pod connection as local delivery while the main Pod is not sending. | After restart, `local_ok=true` and no incident with the fallback at zero replicas. Not yet observed during a real fallback. The code change is not part of this public snapshot. |

## Video Observation (2026-10-22 JST)

The operated system now runs two clock layers beside the native forward counters
and provider APIs. [Observability](observability.md#video-observation-layers)
defines what each layer can show; the
[clock observation case study](v3/clock-observation-case-study.md) explains how
they were reached.

| Layer | Operated state | Last checked | Open item |
| --- | --- | --- | --- |
| Return-feed clock | Continuous reader, paired JST/UTC OCR every 10 s, probe fixed after a multi-hour blind period. | 09:44-09:47: 18/18 cycles confirmed after binding. | Low UTC confidence raised the quality warning around noon; this analyzer has no binary retry. |
| Fallback-sender clock | Deployed; idle because the fallback was not running. | Standby only. | Not observed during a real fallback. |
| Observation warnings | Gap and observer-unavailable warnings on the existing notification loop; no Critical or recovery threshold. | Rule tests and offline replay of the blind period. | Delivery to the notification channel not yet observed. |
| Twitch thumbnail clock | Source `c27f0ee0af1f` with stream-change handling, 840 s stale warning, missing-metric rule, binary OCR retry and OCR counters. | 12:12: capture and OCR succeeded; counters at three observations and two retries. | First real session renewal under this worker is due 2026-10-23 00:00 JST. |

None of these layers is public-player E2E evidence. Dated measurements are in the
[return-feed port](v3/public-source-port-2026-10-22.md), the
[thumbnail port](v3/public-source-port-2026-10-22-thumbnail.md) and its
[review follow-up](v3/public-source-port-2026-10-22-thumbnail-review.md), and the
[operated OCR counter rollout](v3/thumbnail-ocr-rollout-2026-10-22.md). The public
examples were not deployed; OCI restore assets and monitor images were retained
privately.

## Not Yet Observed

- YouTube continuity across a keeper-driven Twitch renewal. The first renewal,
  at 2026-10-21 00:00 JST, did nothing: the keeper sent its forward change as
  POST, which MediaMTX's config API rejects with 404, and it recorded the date
  anyway. The keeper now sends PATCH. At the 2026-10-22 renewal it removed the
  Twitch forward at 00:00:01 JST and restored it at 00:02:02 JST, and the Twitch
  API reported a new stream from 00:02:05 JST. YouTube delivery during that
  window was not separately checked.
- Delivery of a real Twitch alert to Discord.
- A prolonged relay-host stop that releases the cloud host allocation, and
  provider-side failures. A stopped relay service, tunnel loss, and a
  relay-host reset were exercised on 2026-10-20 and 2026-10-21.
- Whether MediaMTX reconnects a dropped forward without the keeper.
- Why the relayed stream drew YouTube `bitrateLow` warnings at an unchanged
  media rate. Captures on 2026-10-21 showed media of about 3.6 Mbps on both
  routes (the earlier 4.88 Mbps included about 34% TLS framing). Raising the
  video bitrate to 4000k stopped the warnings: 11 in the 24 hours before, 0 in
  the 24 hours after.
- Long-term soak of the current route.
- Twenty-four-hour clock/OCR quality, worst-case two-input performance within
  the observer limit, and the updated observer during a real fallback.
- A fix for low UTC OCR confidence on the return feed. It raised the observation
  warning on 2026-10-22 while frames and forward bytes continued.
- Twenty-four-hour thumbnail freshness, OCR quality and binary-retry rate, the
  thumbnail worker across a real session renewal, and actual delivery of the
  new observation-quality warnings.
- A new-VM bootstrap and live relay recovery from the saved OCI bundle.
- Public-player frame continuity or end-to-end platform delay from returned
  relay/fallback clocks. Public-player automated acquisition remains on hold
  pending applicable terms or permission. The selected operated update connects
  observation-quality warnings to the existing notification route; it adds no
  clock-derived Critical or recovery threshold. Thumbnail checks do not require
  automated player acquisition.
- Critical and recovery delivery through the OCI-aware Twitch notifier. Its
  public source tests use synthetic inputs and mocked webhooks.

## Twitch Observation Source Update (2026-10-21 JST)

The public Monitoring v4 example now includes fixed-cadence API collection,
failure/quota diagnostics, and notification decisions that combine OCI input,
Twitch output, and API observations. Critical path failures use Slack; observation
warnings use Discord. Recovery waits for two distinct fresh path/LIVE
observations. Historical API SLO failures and missing minutes remain intact.
See the [implementation notes](../monitoring-v4/docs/relay-provider-evidence.md)
and [source-port validation record](../publication/twitch-observation-port-20261021.json).
This port applies no public example to production and sends no notifications.

## Deliberately Disabled

- CRA production authority (`production_behavior_change_authorized=false`).
- Automatic replacement-broadcast creation by the YouTube watchdog.
- Runtime and recovery authority in Monitoring v4. Its relay observer and
  public projection run in production as read-only components.

## Latest Evidence Checkpoints

| Evidence | Latest checkpoint | Record |
| --- | --- | --- |
| Same YouTube URL | 2026-10-14 00:04:57 JST, `140 / 140` ledger dates | [Continuity evidence](same-url-continuity-evidence.md) |
| Automatic fallback and return | 2026-10-20 drill | [Route record](v3/self-healing-oci-fanout.md#drill-stopping-the-relay-2026-10-20-0855-to-0903-jst) |

## Longer-Term Gaps

These are known gaps, not scheduled or authorized work:

- Keep Monitoring v4 runtime/recovery authority and CRA production authority
  disabled until a host, immutable revision, target, rollback, formal soak, and
  authorization review is completed outside public CI. Monitoring v4's read-only
  relay observation and public projection are already running in production.
- Complete the Monitoring v4 phases still marked unimplemented in its
  [status record](../monitoring-v4/docs/status.md).
- Measure viewer buffering and frame continuity before claiming more than the
  same-URL evidence supports.
- Audit the full YouTube broadcast inventory before strengthening the
  zero-replacement claim beyond ledger evidence.
- Add a portable Kind or k3d validation path for contributors without k3s.
- Publish more dashboard examples only when labels and generated evidence can
  be sanitized.
