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
- Read-only map-runtime and public-viewer synthetic probes, Prometheus mappings
  and rules, and Discord/Slack routing policy.
- Host-maintenance decision logic and inert unit examples for GPU startup,
  postboot readiness, and read-only NVIDIA package observation.
- Public review docs for executive summary, operational scorecard, test safety
  boundary, incident review, and migration cutover reasoning.
- A sanitized renderer-cutover case study with failure, rollback, repair, and
  accepted-window aggregates while raw operational artifacts remain excluded.
- Sanitized prodesk monitoring extracts in `ops/prodesk-monitoring/`.

## What Was Excluded

- `.state/` runtime state, precipitation generations, probe history, incident
  snapshots, local logs, screenshots, and viewer/local capture outputs.
- Packet captures and generated packet-metadata artifacts.
- `ncs_music/` and other local media payloads.
- Virtual environments, Python caches, and generated runtime directories.
- Real YouTube stream keys, OAuth tokens, Discord webhooks, SSH keys, and
  environment files from production state.

## Safety Rules

- Treat every `*.env.example` as a template only.
- Keep production-like values in local untracked files or Kubernetes Secrets.
- Run the secret scan before pushing a public branch.
- Keep public CI non-mutating. Live YouTube mutation and production k3s apply
  belong to explicit local operations, not the public snapshot workflow.
- Do not install or enable the host-maintenance unit examples from CI.
