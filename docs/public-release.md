# Public Release Notes

This repository is a sanitized public snapshot of a personal 24/7 ADS-B
streaming system. The goal is to show the architecture, code, tests, deployment
contracts, and operational decisions without publishing runtime data or secrets.

## What Was Kept

- stream_v3 delivery-plane code: custom MapLibre rendering, processed
  analysis-only precipitation, PulseAudio, AutoDJ, FFmpeg/NVENC, runtime/GPU
  guards, and k3s entrypoints.
- observability-plane code: YouTube resolver, watchdogs, recovery orchestrator,
  SLI summaries, Prometheus exporter, `ops/monitoring` Prometheus/Loki/Grafana
  config, and observability monitor systemd units.
- v2 historical context and runbooks that explain why v3 exists.
- k3s manifests for shadow, streaming, observer, reports, and cutover gates.
- Tests for config contracts, recovery policy, watchdog behavior, and k3s
  manifest validation.
- The integrated `recovery-control/` subproject: CRA intent handling,
  arena-server facts projection, Dell admission, exact-target effect fencing,
  schemas, migrations, isolated tests, and public design records.
- The integrated `monitoring-v4/` subproject: the sanitized arena observation,
  current, incident, notification-intent, parity, storage, and Harness surfaces
  from public snapshot `5e61e2b7dbbd09f014746c35e50c406ead755561`.
- Read-only map-runtime and public-viewer synthetic probes, Prometheus mappings
  and rules, and Discord/Slack routing policy.
- Durable operational reliability rollups, external public-video evidence,
  and the single-writer notification policy.
- Public-safe Raspberry Pi publisher source, static assets, GCS command
  construction, and Cloudflare Worker source under `ops/public-publisher/`.
- Host-maintenance decision logic and inert unit examples for GPU startup,
  postboot readiness, and read-only NVIDIA package observation.
- Connectivity-aware FFmpeg launch suppression, browser-only recovery, and
  arena-side incident correlation without live host configuration or state.
- Exact-target runtime-boundary integration, local effect auditing, redacted
  rotating FFmpeg stderr capture, and correlated exit/restart evidence, with
  production authority still disabled in the public policy.
- An explicitly disabled-by-default FFmpeg network writer timeout with bounded
  configuration validation and no claim of production activation.
- Boot-bound, durable network episode classification from the existing
  credential-free persistent anchor observer; generated episode state remains
  excluded.
- OAuth evidence timestamps that distinguish a real API attempt from response
  cache reuse, plus an inert arena-server schedule example.
- Public review docs for executive summary, operational scorecard, test safety
  boundary, incident review, and migration cutover reasoning.
- Repository-level and recovery-control failure-injection and chaos-testing
  drafts that keep component tests, proposed campaigns, and production
  authorization separate.
- A sanitized renderer-cutover case study with failure, rollback, repair, and
  accepted-window aggregates while raw operational artifacts remain excluded.
- A mobile-legibility source proposal and measured aggregate luma values; raw
  captures and live deployment claims remain excluded.
- Sanitized prodesk monitoring extracts in `ops/prodesk-monitoring/`.

## What Was Excluded

- `.state/` runtime state, precipitation generations, probe history, incident
  snapshots, local logs, screenshots, and viewer/local capture outputs.
- Packet captures and generated packet-metadata artifacts.
- `ncs_music/` and other local media payloads.
- Virtual environments, Python caches, and generated runtime directories.
- Real YouTube stream keys, OAuth tokens, Discord webhooks, SSH keys, and
  environment files from production state.

The `monitoring-v4/` import preserves the standalone public snapshot's
portable paths and test fixtures. It does not copy private Monitoring v4
operational records, generated soak artifacts, databases, or live deployment
identity.

## Safety Rules

- Treat every `*.env.example` as a template only.
- Keep production-like values in local untracked files or Kubernetes Secrets.
- Run the secret scan before pushing a public branch.
- Keep public CI non-mutating. Live YouTube mutation and production k3s apply
  belong to explicit local operations, not the public snapshot workflow.
- Do not install or enable the host-maintenance unit examples from CI.
