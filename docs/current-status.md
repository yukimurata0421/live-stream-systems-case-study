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

## Clock Monitor And Backup Source Update (2026-10-22 JST)

The operated return-feed observer was updated to continuous reception and
ten-second JST/UTC verification. A dated normal window confirmed ten of ten
cycles, and stopped/blocked monitor reception passed an isolated primary-output
exercise. The [source-port record](v3/public-source-port-2026-10-22.md) gives the
measurements, failed attempts, adaptations and local checks.

The public example adds no production deployment, alert policy, recovery action
or public-player acquisition. Its native relay and provider evidence remains
separate from returned-frame health. OCI restore assets and the updated monitor
image/source were retained privately; only portable code and procedures are
included here.

## Thumbnail Observation Snapshot (2026-10-22 JST)

The [initial source port](v3/public-source-port-2026-10-22-thumbnail.md) covered a
short operated window with four clock progressions across five provider images.
The [review follow-up](v3/public-source-port-2026-10-22-thumbnail-review.md) now
selects source `c27f0ee0af1f`: stream-change cache invalidation, a bounded
twelve-minute startup allowance, a fourteen-minute stale-image warning, missing
publication/metric checks, explicit waits and a conservative OCR retry. The
[thumbnail contract](v3/twitch-thumbnail-monitor.md) owns the current semantics.

Operated capture/OCR and warning state were checked at 11:22 JST. The public
example has not been deployed, and the next real daily stream rollover is still
unobserved. This source port neither changes independent publisher/SLI work nor
turns provider thumbnails into public-player E2E evidence.

## Not Yet Observed

- A completed keeper-driven Twitch renewal. The first one, at 2026-10-21
  00:00 JST, did nothing: the keeper sent its forward change as POST, which
  MediaMTX's config API rejects with 404, and it recorded the date anyway. The
  keeper now sends PATCH and confirms changes by reading the configuration
  back. The next renewal is due 2026-10-22 00:00 JST.
- Delivery of a real Twitch alert to Discord.
- A prolonged relay-host stop that releases the cloud host allocation, and
  provider-side failures. A stopped relay service, tunnel loss, and a
  relay-host reset were exercised on 2026-10-20 and 2026-10-21.
- Whether MediaMTX reconnects a dropped forward without the keeper.
- Why YouTube has intermittently reported `bitrateLow` warnings since the
  route change. Captures on 2026-10-21 showed that delivered media did not
  drop: the earlier 4.88 Mbps included about 34% TLS framing on the direct
  connection, and media is about 3.6 Mbps on both routes.
- Long-term soak of the current route.
- Twenty-four-hour clock/OCR quality, worst-case two-input performance within
  the observer limit, and the updated observer during a real fallback.
- Twenty-four-hour thumbnail freshness/OCR quality, daily stream rollover and
  actual delivery of the new observation-quality warnings.
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
