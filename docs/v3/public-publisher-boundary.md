# Raspberry Pi Public Publisher Boundary

The operated site moved to private R2 on 2026-10-27. The
[dated migration and freshness record](r2-publication-and-freshness-case-study.md)
describes its checks and a later clock-skew failure. The November 3
[R2 source port](r2-publication-transport.md) adds the uploader, Worker,
durable receipts, IPv6 preference and isolated resolver examples. The earlier
GCS example remains available. The collectors' timestamp validation from
the dated record is ported in `public_snapshot_time.py`.

The 2026-10-13 public source includes separate relay, Twitch API/reception, and
Twitch SLI collectors plus updated static layout assets. Their public fields
remain allowlisted. This source port does not run the collectors, upload the
site, change the publisher service, or include generated live JSON. See the
[port record](public-source-port-2026-10-13.md).

The Raspberry Pi is a static snapshot publisher. It is not a k3s node,
Prometheus backend, Grafana instance, formal SLI authority, or recovery
executor.

The [2026-10-14 Pi source port](pi-publisher-source-port-2026-10-14.md) adds
reliability schema v5, separate Viewer/ADS-B evidence, Full-HD sample facts,
and a field-filtered active-incident section. Notification backlog now counts
pending delivery only. The [source guide](../../ops/public-publisher/site/README.md)
describes portable setup and offline validation. Live configuration, generated
snapshots, and the older machine-specific Japanese operations notes are excluded.

The later [observation contract](observation-standardization.md#public-projection)
moves source queries, sanitization and JSON generation to arena-server. The
current bundle preserves Reliability v6; the dated v5 port and individual
collector sources remain historical references.

## Data Flow

```text
arena-server: private evidence -> source queries and sanitization
  -> allowlisted projection bundle with original-age proof
  -> Pi-initiated read-only fetch, schema and age validation
  -> static site tree on Pi
  -> outbound private R2 upload (or the retained GCS example)
  -> Cloudflare Worker
  -> public browser
```

Public browsers never need a route to the home network or private monitoring
services. Arena-server owns evidence generation and sanitization; the Pi owns
bundle validation, presentation timestamps derived from proved age, static
assembly and upload. Changes must reach the source revision loaded by the
affected owner. Editing a checkout or a generated artifact alone does not change
the corresponding recurring job.

## Canonical Source

The public source is grouped under `ops/public-publisher/`:

- `status/` provides Prometheus/Loki sanitizers reused by the owner-side
  projection and static HTML views; its earlier standalone collection path is
  retained;
- `site/scripts/collect_observation_bundle.py` fetches the owner-generated
  bundle and validates schemas and original age;
- `site/scripts/build_public.py` invokes that fetch and assembles static views;
  uploader scripts send allowlisted objects to the selected store;
- `site/public/` contains static HTML, CSS, and JavaScript;
- `site/systemd/` contains an inert unit example and external environment
  template; and
- `site/cloudflare-worker/` contains the public edge Worker source.

Generated JSON, local state, logs, backups, credentials, `node_modules`, and
Worker local state are not source artifacts and are excluded.
The owner-side builder is `ops/scripts/stream_v3_public_projection.py`; it uses
the projection and validation helpers under `site/scripts/`. Individual
collector entrypoints remain available but are not invoked by the current
publisher build. The [source guide](../../ops/public-publisher/site/README.md#bundle-source-binding)
lists the bundle reader and external machine binding required for installation.

## Sanitization Contract

Owner-side sanitizers, bundle validation and upload checks enforce these public
boundaries:

- metric labels use an allowlist rather than copying arbitrary labels;
- event and error text redacts secret assignments, bearer values, URLs, and IP
  addresses;
- raw Loki queries are not published;
- storage destinations and credentials come from external configuration; the
  bundle's fixed SSH alias, source paths and external machine fingerprint must
  be reviewed for the installation;
- no credential value is printed as part of validation; and
- the reliability card is presented as measured evidence with reference lines,
  not a pass/fail guarantee.

If the reliability source refresh fails, the owner retains any prior artifact
with its original generation receipt. With no prior artifact, the bundle omits
that observation. The Pi retains its last accepted file; it does not turn a
missing source into a newly healthy snapshot. Publisher success and each
observation's freshness therefore remain separate claims.

The retained GCS upload step freezes only the fixed public allowlist into a private
temporary directory. It rejects links, non-regular or oversized files,
duplicate JSON keys, non-finite timestamps, stale/future JSON, wildcard or
root-only GCS destinations, and public-tree status paths. A bounded deadline
kills only the invocation-owned process group. Upload completion is recorded
durably but does not claim that an external reader has verified the mirror.

This hardening was ported from an uncommitted private worktree candidate whose
`push_to_gcs.py` SHA-256 was
`5378112b4043be6f281279db02402b1274006d86cfc333800807423b9160c5d6`.
The source hash identifies the candidate; it is not deployment or external
visibility evidence.

The R2 uploader uses its own manifest for the packaged public assets. It
checks each observation independently, so one stale supporting feed does not
prevent fresh live-status feeds from advancing. A partially successful cycle
is still recorded as `PARTIAL`. PUT acceptance requires readback; uncertain
outcomes remain in a destination-bound private receipt until GET reconciliation.
Connection fallback occurs before HTTP transmission, never by repeating an
uncertain PUT. See the [transport contract](r2-publication-transport.md).

## Freshness And Cache Boundaries

The reference timer starts 90 seconds after boot and then runs approximately
every 60 seconds with 15-second accuracy. JSON uses a 60-second public cache;
static assets use 300 seconds. A public snapshot becomes warning-level after
180 seconds and bad after 300 seconds.

These clocks describe publication freshness. They do not describe ADS-B source
age, YouTube evidence age, viewer-probe cadence, or formal SLI coverage.

## Repository Validation

The public tests cover collector redaction, label allowlisting, last-good
behavior, build inputs, GCS command construction, environment separation,
generated-file exclusions, and the unit/timer contract:

- `tests/test_public_map_collector.py`
- `tests/test_public_reliability_collector.py`
- `tests/test_public_publisher_build.py`

No test uploads a real object. The committed unit is a proposal and is not
installed or enabled by public CI.

R2 transport, receipt and publication tests live in
`ops/public-publisher/site/tests/`; public integration checks are in
`tests/test_public_r2_port.py`. Worker tests use an in-memory bucket under
`ops/public-publisher/site/cloudflare-worker/tests/`. These checks do not
qualify a live resolver, cloud deployment or external receiver.
