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
desired and ready, and, while it runs, its upload rate from a Deployment
annotation no older than 120 seconds. Missing or stale values are null; the
relay artifact is still printed.

`twitch_alert_notify.py` reads firing Twitch alerts from the local Prometheus
API and posts one Discord message when an alert starts, one per hour while it
continues, and one when it resolves. It ignores pending and unrelated alerts,
keeps its state when Prometheus is unreachable, and only advances state after a
successful delivery. It changes no delivery or recovery state.

Tests cover the renewal time and notification decisions with synthetic inputs.
Delivery to a real webhook and the first keeper-driven renewal are not
exercised by these tests.
