# Static Operations Publisher

This directory contains the public source for a Raspberry Pi publisher. It
fetches an arena-generated observation bundle, validates schemas and original
age, builds static artifacts, and uploads an allowlisted tree. The shipped unit
uploads to a configured GCS prefix; the operated site uses a private R2 prefix,
which is an explicit installation choice. The publisher does not control the
stream, select recovery actions, or store a monitoring database.

The [publisher boundary](../../../docs/v3/public-publisher-boundary.md) defines
the data and upload limits. The [source-port record](../../../docs/v3/pi-publisher-source-port-2026-10-14.md)
explains the differences imported from the publisher working tree.
The [R2 transport guide](../../../docs/v3/r2-publication-transport.md) covers
the later uploader, receipt reconciliation, IPv6 preference and private DNS.

## Source Layout

| Path | Purpose |
| --- | --- |
| `../status/` | Prometheus/Loki sanitizers reused by the owner-side projection, static views, and a retained standalone collector |
| `../../scripts/stream_v3_public_projection.py` | Arena-side source queries, sanitization, bundle build and read command |
| `scripts/collect_observation_bundle.py` | Current Pi JSON entrypoint: allowlisted bundle fetch, schema checks and boot-bound age proof |
| `scripts/reliability_projection.py` | Current Reliability v6 projection used by the arena-side builder |
| `scripts/collect_reliability.py` | Retained standalone v5 collector; not run by the current build |
| `scripts/collect_twitch_public.py` | Twitch validation shared with the bundle; standalone collection retained |
| `scripts/collect_relay_public.py` | Relay validation shared with the bundle; standalone collection retained |
| `scripts/collect_twitch_sli.py` | Twitch history validation shared with the bundle; standalone collection retained |
| `scripts/build_public.py` | Invokes the bundle fetch and copies optional static HTML views; does not run standalone JSON collectors |
| `scripts/push_to_gcs.py` | Frozen upload allowlist, deadlines, and private publication status |
| `scripts/push_to_r2.py` | Per-object readback, durable uncertain outcomes, independent feed publication |
| `scripts/r2_network.py` | Two persistent HTTPS connections, IPv6 first and IPv4 fallback before HTTP |
| `scripts/render_r2_resolver.py` | Publisher-only resolver configuration; no host DNS mutation |
| `public/` | Static HTML, CSS, and JavaScript; generated live JSON is excluded |
| `systemd/` | Inert service/timer and external-environment examples |
| `cloudflare-worker/` | Edge source and an example routing configuration |

## Validate Without Live Services

Run these commands from the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q tests/test_public_*.py ops/public-publisher/status/tests ops/public-publisher/site/tests
node --test ops/public-publisher/site/cloudflare-worker/tests/r2-worker.test.mjs
.venv/bin/python ops/scripts/validate_public_port.py
```

Use Node.js 22 for the Worker tests. The publisher tests use synthetic data,
mocked transports and local TLS/DNS fixtures. They do not
upload objects, contact providers, or install a service. The HTML fixtures in
`tests/` exercise missing measurements and display recovery locally.

## Configure an Installation

Review the [bundle source binding](#bundle-source-binding) before configuring
either uploader. Use `systemd/stream-v3-public-publisher.env.example` as the
starting point for an external environment file. `STATUS_DIR` selects the
directory containing the optional static HTML views; it does not select the
JSON source. The retained `STREAM_V3_RELIABILITY_*` and `STREAM_V3_CLOCK_SSH_TARGET`
settings belong to standalone collectors and do not configure the bundle fetch.
Keep credentials and actual host configuration outside this repository. For the
GCS example, set an explicit bucket **and prefix**. For R2, merge
`systemd/stream-v3-r2-publisher.env.example` into the external environment and
follow the [R2 configuration guide](../../../docs/v3/r2-publication-transport.md#installation-example).
The base service still selects GCS; R2 is an explicit installation choice.

`scripts/build_public.py` performs a real SSH bundle fetch when invoked without
mocks.
`scripts/push_to_gcs.py` and `scripts/push_to_r2.py` perform cloud uploads.
Review the unit examples,
destination, credentials, and read-only source binding before either command
is used. Public CI does not perform these operations.

The retained GCS Worker uses an example origin and the routes in
`cloudflare-worker/wrangler.toml.example`. The R2 Worker uses `src/r2.js` and
`wrangler.r2.toml.example`, with an explicit private bucket binding and matching
`public/` prefix instead of an origin fallback. Neither example establishes a
live Worker configuration.

### Bundle Source Binding

`scripts/collect_observation_bundle.py` currently fixes the following bindings
in source; the older collector environment variables do not override them:

| Binding | Required installation input |
| --- | --- |
| SSH target | `arena-server`, resolved through the publisher user's external SSH configuration, with authenticated read-only access to the bundle reader |
| Remote reader | `python3 -I -B /opt/stream-v3-public-projection/current/ops/scripts/stream_v3_public_projection.py read` |
| Remote bundle | `--output /var/lib/stream-v3-public-projection/bundle.json`; each request also supplies a fresh `--nonce` |
| Peer identity | An external `scripts/clock-peers.json` beside the collector, containing `arena_machine_sha256` for the intended observation host |

The observation owner must already build the bundle from its local sources;
the read command does not generate missing observations. The peer fingerprint
must match that owner. Actual fingerprints, SSH keys, generated bundles and
host-specific configuration are excluded from this checkout. A deployment with
different paths requires an explicit review of the fixed source binding; setting
`STREAM_V3_EGRESS_SSH_TARGET` changes only the retained standalone egress collector.
Missing or mismatched binding retains old artifacts and their original age;
a zero exit status from the fetch alone does not establish fresh publication.

## Interpret the Evidence

The current bundle requires Reliability schema `stream-v3-reliability-public.v6`,
generated by the owner-side `reliability_projection.py`. The retained standalone
`collect_reliability.py` emits v5 and is not used by `build_public.py`.
Viewer evidence remains separate from ADS-B source availability. Full-HD counts
describe recorded sample dimensions; unavailable or older low-resolution evidence does not establish
current visual correctness. Quiet aircraft activity is compatible with a
healthy, fresh source heartbeat.

`active_incidents` contains only identifiers, severity, recovery type, and
observation time. A failed incident read produces an unavailable count, not a
healthy zero. Notification backlog counts pending delivery only; it does not
count unresolved incidents. A formal rolling SLO breach can remain active after
the current stream has recovered.

Failed collection retains the last artifact and its original age. Publication
success, freshness, provider state, historical SLO assessment, and viewer
reception are separate results. Generated JSON, logs, caches, databases,
backups, and private installation files are not included in this source tree.
