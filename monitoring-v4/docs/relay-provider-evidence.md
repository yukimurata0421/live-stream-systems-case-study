# Relay And Provider Evidence

The 2026-10-13 source update adds read-only OCI relay input/output, Twitch
API, public reception, and SLI projections. They have independent source times
and freshness gates. Byte progress, provider LIVE state, and decoded media are
separate evidence planes; none grants recovery authority.

`compatibility/relay_observe.py` collects fixed local sources and allowlisted
GET endpoints. `relay_monitor.py` reduces bound raw evidence to transport and
resource facts. `collectors/twitch_api.py` produces read-only official provider
evidence; `twitch_public.py`, `twitch_sli.py`, and the public projection modules
retain UNKNOWN on stale or unbound input. Broadcaster identity and collector
revision come from explicit operator bindings, not a baked-in private account.
Synthetic tests use fixture identities.

A planned Twitch break is recognized only with fresh bounded daily-owner
state. It cannot suppress a YouTube output issue. Counter, boot, or handler
changes invalidate rates. Output projections exclude forwarding destinations,
raw provider errors, credentials, boot identity, and machine bindings.

Account metadata administration and credential installers are excluded.
CLI collection needs reviewed source paths and credentials held outside this
repository. The 2026-10-13 update had no enabled notification provider, runtime
executor, route policy, or public-site deployment; the 2026-10-20 update below
adds one read-only notification path.

Focused tests cover relay reduction, API identity/freshness, public projection,
SLI uncertainty, and recovery verification. They use synthetic/mocked inputs.
They do not complete a seven-day coverage gate, fourteen-day parity gate,
provider reception soak, or native failure qualification. Earlier milestone
counts in historical documents retain their original validation window.

## Self-healing relay update: 2026-10-20 JST

The relay route moved to a MediaMTX fan-out with a keeper and a fallback
controller; see the integrated repository's self-healing fan-out document.
`relay_observe.py` now reads the MediaMTX API and the keeper state. While a
Twitch renewal is in progress it reports that renewal's 00:00 JST as
`next_renewal_at`, so a planned break is still recognized within its 360-second
window; otherwise it reports the next 00:00 JST.

`relay_public.py` adds `fallback`: whether the direct YouTube fallback is
desired and ready, and, while it runs, its upload rate. Since 2026-10-21 it
samples the fallback FFmpeg's acknowledged bytes itself and compares them with
the previous call; a Pod change, counter reset, or interval outside 5 to 600
seconds gives null. Missing values are null; the relay artifact is still
printed.

`twitch_alert_notify.py` reads firing Twitch alerts from the local Prometheus
API. Its OCI-aware decisions and channel routing are described below. It ignores
pending and unrelated alerts, keeps its state when Prometheus is unreachable,
and changes no media-delivery or recovery-control state.

Tests cover the renewal time and notification decisions with synthetic inputs.
Delivery to a real webhook and the first keeper-driven renewal are not
exercised by these tests.

## API cadence and diagnostics: 2026-10-21 JST

The API collector starts probes on a fixed monotonic schedule, centered in each
minute at the default 60-second interval. Waiting 60 seconds after each response
had added request latency to every interval, leaving empty calendar-minute
slots between successful probes. Overruns skip missed slots without catch-up
requests. Failure backoff and provider Retry-After limits still apply.

The existing API SLI remains successful observation minutes divided by all
expected minutes. Failed, unknown, and missing minutes and the history start
are retained. A separate `stream_v4_twitch_api_observed_success_percent` uses
only known minutes; read it with collection coverage and failed/missing minute
counts. It does not replace the original SLO or erase historical degradation.

Since 2026-10-22, an isolated failure keeps the next scheduled polling slot.
Backoff starts with the second consecutive failure; provider Retry-After is
always respected. There is no catch-up burst or retry within a probe. The public
publisher accepts the current and previous reviewed API indicator wording, and
a contract test connects the actual SLI producer to that consumer. History,
targets, and the default five-second request timeout are retained.

The collector now records fixed failure categories, request stage and duration,
HTTP status, schedule lag, and numeric rate-limit metadata. The existing SLI
`/metrics` endpoint exposes these diagnostics after validating the configured
account/revision binding, boot, freshness, and matching JSON/text timestamps:

- `stream_v4_twitch_api_probes_total{result}` separates timeouts, network errors,
  authentication, rate limiting, upstream errors, and other fixed categories.
- `stream_v4_twitch_api_requests_total{stage}` and the
  `stream_v4_twitch_api_request_duration_seconds` summary retain request counts
  and elapsed time by OAuth, validation, user lookup, and stream lookup stage.
- `stream_v4_twitch_api_probe_duration_seconds`,
  `stream_v4_twitch_api_schedule_lag_seconds`, and
  `stream_v4_twitch_api_http_status` describe the latest probe.
- `stream_v4_twitch_api_rate_limit_remaining`,
  `stream_v4_twitch_api_rate_limit_limit`, and
  `stream_v4_twitch_api_rate_limit_reset_unixtime` retain provider quota facts.
  Unknown quota is omitted, not reported as zero.
- `stream_v4_twitch_api_diagnostics_available` reports whether those diagnostics
  passed validation. The legacy `stream_v4_twitch_api_requests` gauge still means
  requests in the latest probe, not a cumulative count.

Counters reset with the collector process; use `increase()` or `rate()` for
windows. Logs omit credentials, provider bodies, URLs, and exception messages.
The stage-duration summary covers a whole HTTP request; the transport metrics
added below separate the underlying phases when they are observable.
Set `collector_revision` in the existing binding file to the reviewed collector
file's SHA-256. The public example requires that explicit pin; it does not ship
the private deployment's account or rolling-release allowlist.

## Request deadline and transport diagnostics: 2026-10-27 JST

A socket timeout did not bound a whole urllib request: DNS resolution, connection
candidates, TLS and response-header waits could exceed the five-second setting.
The selected transport now starts a root-owned `/usr/bin/curl` child for each
request and applies `--max-time` to the whole transfer. The parent waits at most
the configured timeout plus 0.5 seconds before killing its child, and reaps it
with a bounded wait. This is not a real-time scheduling guarantee. The change
also avoids repeatedly constructing Python SSL contexts in the long-lived
collector, which had reproduced memory growth under a fixed resource limit.

The transport keeps TLS certificate verification enabled, rejects redirects,
disables inherited proxies and curl configuration, bounds response/diagnostic
output, and performs no transfer retry. Credentials and request bodies enter
the child through stdin rather than arguments or temporary files. Verbose
headers exist only in bounded process memory: the parent retains fixed phase
enums and discards the raw text. No URL, credential, provider body or raw header
is added to the journal.

- `stream_v4_twitch_api_transport_failures_total{stage,phase}` counts failures by
  API stage and DNS, connect, TLS, response headers/body, whole-request deadline,
  HTTP result or unknown phase.
- `stream_v4_twitch_api_transport_phase_seconds{stage,phase}` reports observable
  phase times from requests in the latest probe. Missing end timestamps are not
  fabricated as completed zero-duration phases.

Final curl timing fields can omit a completed TCP connection when TLS stalls,
so phase classification also reads fixed progress events in memory. If the
evidence is insufficient, the result remains unknown or deadline-limited.
Historical timeouts cannot be assigned a phase retroactively. Failed requests
remain UNKNOWN and never reuse a previous LIVE observation.

The SLI endpoint accepts multiple fixed labels for these diagnostics while
preserving the account/revision/boot/freshness checks, timestamp match, size
limit, allowed metric prefix and numeric validation. Public consumers still
require the operator's exact `collector_revision` binding. Review the new file
hash and update the binding when upgrading; no private rolling-release pins are
included. Relay and public projector behavior is unchanged.

The 60-second monotonic grid, five-second default, Retry-After floor, consecutive
failure backoff, no catch-up and no inline retry remain intact. This port changes
no SLO target, history denominator, renewal decision, notification policy or
media route. The [source-transfer record](../../publication/twitch-api-deadline-20261027.json)
lists local source tests separately from any operated private deployment. The
local TLS fixtures exercise DNS/TCP/TLS/header/body stalls, certificate rejection,
redirects, rate limiting, oversized responses and child cleanup without calling
a provider. They do not establish long-term timeout reduction or decoded media
reception.

## OCI-aware notification decisions

Twitch has a required OCI path in this topology. The relay observer combines
fresh relay-process, input-counter, Twitch-output-counter, and provider facts
into `stream_v4_twitch_delivery_state{state}`. Notifications and Prometheus
rules use that assessment. Raw counters and API observations remain available
for diagnosis.

| Evidence | Assessment and notification |
| --- | --- |
| OCI input/output progress, API unavailable or too far apart in time | API observation degradation; Discord warning |
| OCI input/output progress, fresh API OFFLINE | Provider/transport disagreement; Discord warning |
| Explicit OCI relay, input, or Twitch-output interruption | Critical after three minutes; Slack |
| Stale OCI observations or unestablished counter deltas | Unassessed delivery; Discord warning |
| Bounded daily renewal with relay and input healthy | Planned Twitch break; relay/input failures remain detectable |

Source facts expire after 180 seconds; OCI/API timestamps must be within 90
seconds to establish a combined healthy observation. Counter resets, connection
changes, missing identities, and malformed fields cannot establish an outage or
successful delivery. The existing `/public` JSON shape is preserved; the
notification reader uses the separate local `/twitch-delivery` endpoint.

`stream_v4_twitch_delivery_path_interrupted` groups confirmed path failures
under one alert, so a change of failing leg does not reset its three-minute
confirmation. `stream_v4_twitch_delivery_recovery_confirmed` marks a currently
healthy combined observation. The notifier waits for two such observations with
different OCI timestamps at least 45 seconds apart before resolving a path
incident. A vanished series, planned break, or unavailable API cannot close it.
Critical starts and recovery follow-ups use the same Slack route; other messages
use Discord. Each alert advances its delivery state only after its own channel
acknowledges delivery, avoiding an unpaired recovery or replay of another
channel's successful notification.

Historical SLO warnings send an initial message, a message when severity or the
combined OCI/API assessment changes, and a resolution message. While the same
state continues, reminders are sent every 24 hours after the last notification
(`--slo-repeat-sec 86400`). Changing timestamps, rates, or SLI values alone do
not trigger a state-change message. Existing notification times and historical
failed/missing minutes are retained across an upgrade.

Current OCI-path interruptions and API-observation failures remain independent:
initial notification follows the existing three-minute confirmation and normal
30-second notification loop, with hourly reminders (`--repeat-sec 3600`).
Historical API SLO warnings include current OCI relay, input, output, and API
context. Combined HEALTHY means these observations
agree; decoded video/audio reception still requires independent evidence.

Example rules are in `deploy/prometheus/`. The Prometheus fixture can be run from
this subproject with `promtool test rules
tests/fixtures/monitoring_v4/twitch_delivery_alerts.test.yml`.
The [source-port record](../../publication/twitch-observation-port-20261021.json)
separates public source validation from private deployment and real webhook
delivery. No credentials, runtime state, or deployment receipts are included.
