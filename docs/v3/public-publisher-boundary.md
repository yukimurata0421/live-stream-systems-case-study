# Raspberry Pi Public Publisher Boundary

The Raspberry Pi is a static snapshot publisher. It is not a k3s node,
Prometheus backend, Grafana instance, formal SLI authority, or recovery
executor.

## Data Flow

```text
private observability evidence
  -> publisher-initiated collection
  -> allowlisted and redacted JSON
  -> static site tree
  -> outbound GCS synchronization
  -> Cloudflare
  -> public browser
```

Public browsers never need a route to the home network or private monitoring
services. The publisher owns the final transformation and static upload, so a
change made only on another host can be overwritten by the next publisher
cycle if the canonical publisher source is not updated as well.

## Canonical Source

The public source is grouped under `ops/public-publisher/`:

- `status/` collects reduced Prometheus and Loki views;
- `site/scripts/` builds the static tree, creates reliability indicators, and
  synchronizes to GCS;
- `site/public/` contains static HTML, CSS, and JavaScript;
- `site/systemd/` contains an inert unit example and external environment
  template; and
- `site/cloudflare-worker/` contains the public edge Worker source.

Generated JSON, local state, logs, backups, credentials, `node_modules`, and
Worker local state are not source artifacts and are excluded.

## Sanitization Contract

The collectors enforce these public boundaries:

- metric labels use an allowlist rather than copying arbitrary labels;
- event and error text redacts secret assignments, bearer values, URLs, and IP
  addresses;
- raw Loki queries are not published;
- GCS destination and monitoring host values must come from an external
  environment file;
- no credential value is printed as part of validation; and
- the reliability card is presented as measured evidence with reference lines,
  not a pass/fail guarantee.

If the reliability source refresh fails, an existing snapshot is retained as
last-good data. Only a first-run failure with no prior snapshot emits an empty
`measurement unavailable` payload. Publisher success and each collected
section's success therefore remain separate claims.

The upload step now freezes only the fixed public allowlist into a private
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
